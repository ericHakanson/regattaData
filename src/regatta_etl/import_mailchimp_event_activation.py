"""regatta_etl.import_mailchimp_event_activation

Mailchimp event-registration activation pipeline (v1).

Segments:
  upcoming_registrants — participants already registered for events starting
                         within the configured window.
  likely_registrants   — historically active participants not yet registered
                         for upcoming events.
  all                  — union of both segments, deduped by normalized email.

Delivery modes:
  csv — write a ready-to-import CSV (default).
  api — upsert members and apply segment tags via Mailchimp Marketing API
        (optional; requires mailchimp-marketing package).

Spec: docs/requirements/mailchimp-event-registration-activation-spec.md
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg

from regatta_etl.shared import RunCounters

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REQUIRED_TABLES = [
    "canonical_participant",
    "canonical_event",
    "canonical_registration",
    "mailchimp_audience_row",
    "mailchimp_contact_state",
    # Audit tables — created by migration 0019; fail fast if missing
    "mailchimp_activation_run",
    "mailchimp_activation_row",
    # Identity policy tables — created by migration 0020
    "participant_mailchimp_identity",
    "mailchimp_identity_review_queue",
]

_SUPPRESSED_STATUSES = {"unsubscribed", "cleaned"}

_CSV_FIELDS = [
    "email",
    "first_name",
    "last_name",
    "display_name",
    "segment_types",
    "upcoming_event_count",
    "historical_registration_count",
    "suppression_status",
    "source_participant_id",
    "generated_at",
    "club_name",
    "yacht_name",
    "last_registered_event_name",
]


# ---------------------------------------------------------------------------
# Internal row representations
# ---------------------------------------------------------------------------

@dataclass
class _CandidateRow:
    """Raw query result, one per participant, before email-level dedupe."""
    participant_id: str
    email_normalized: str
    first_name: Optional[str]
    last_name: Optional[str]
    display_name: Optional[str]
    confidence_score: float
    updated_at: Optional[datetime]
    upcoming_event_count: int
    historical_registration_count: int
    segment_types: list[str]
    yacht_name: Optional[str]
    last_registered_event_name: Optional[str]
    # Populated after email-level dedupe on the winner row
    contributing_participant_ids: list[str] = field(default_factory=list)


@dataclass
class _AudienceRow:
    """Post-dedupe, post-suppression row ready for export and audit."""
    email_normalized: str
    participant_id: str
    first_name: Optional[str]
    last_name: Optional[str]
    display_name: Optional[str]
    segment_types: list[str]
    upcoming_event_count: int
    historical_registration_count: int
    is_suppressed: bool
    suppression_reason: Optional[str]
    yacht_name: Optional[str]
    last_registered_event_name: Optional[str]
    generated_at: str
    contributing_participant_ids: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------

def _check_dependencies(conn: psycopg.Connection) -> None:
    """Fail fast with a clear message if any required table is absent."""
    with conn.cursor() as cur:
        for table in _REQUIRED_TABLES:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            if not cur.fetchone():
                raise RuntimeError(
                    f"Required table '{table}' not found. "
                    "Run all prerequisite migrations before using mailchimp_event_activation."
                )


# ---------------------------------------------------------------------------
# Suppression map
# ---------------------------------------------------------------------------

def _load_suppression_map(conn: psycopg.Connection) -> dict[str, str]:
    """Return {email_normalized -> audience_status} for suppressed emails.

    Uses DISTINCT ON to pick the latest known Mailchimp state per email
    (ordered by status_at DESC NULLS LAST).  Only emails whose latest state
    is 'unsubscribed' or 'cleaned' appear in the result dict.
    Emails with no history are absent and treated as eligible.
    """
    sql = """
        SELECT DISTINCT ON (email_normalized)
            email_normalized,
            audience_status
        FROM mailchimp_contact_state
        WHERE email_normalized IS NOT NULL
        ORDER BY email_normalized, status_at DESC NULLS LAST
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return {
            row[0]: row[1]
            for row in cur.fetchall()
            if row[1] in _SUPPRESSED_STATUSES
        }


# ---------------------------------------------------------------------------
# Segment queries
# ---------------------------------------------------------------------------

def _query_upcoming_registrants(
    conn: psycopg.Connection,
    window_days: int,
) -> list[_CandidateRow]:
    """Segment A: participants registered for events starting within window."""
    sql = """
        WITH upcoming_events AS (
            SELECT id, event_name
            FROM canonical_event
            WHERE start_date BETWEEN CURRENT_DATE
                                 AND CURRENT_DATE + (%(days)s * INTERVAL '1 day')
        ),
        upcoming_regs AS (
            SELECT
                cr.canonical_primary_participant_id                AS pid,
                COUNT(DISTINCT cr.canonical_event_id)              AS upcoming_event_count,
                MAX(cy.name)                                       AS yacht_name,
                MAX(ue.event_name)                                 AS last_registered_event_name
            FROM canonical_registration cr
            JOIN upcoming_events ue ON ue.id = cr.canonical_event_id
            LEFT JOIN canonical_yacht cy ON cy.id = cr.canonical_yacht_id
            WHERE cr.canonical_primary_participant_id IS NOT NULL
            GROUP BY cr.canonical_primary_participant_id
        ),
        hist_regs AS (
            SELECT
                canonical_primary_participant_id                   AS pid,
                COUNT(*)                                           AS historical_registration_count
            FROM canonical_registration
            WHERE canonical_primary_participant_id IS NOT NULL
            GROUP BY canonical_primary_participant_id
        )
        SELECT
            cp.id::text                                            AS participant_id,
            cp.best_email                                         AS email_normalized,
            cp.first_name,
            cp.last_name,
            cp.display_name,
            COALESCE(cp.canonical_confidence_score, 0)            AS confidence_score,
            cp.updated_at,
            ur.upcoming_event_count::int,
            COALESCE(hr.historical_registration_count, 0)::int    AS historical_registration_count,
            ur.yacht_name,
            ur.last_registered_event_name
        FROM upcoming_regs ur
        JOIN canonical_participant cp ON cp.id = ur.pid
        LEFT JOIN hist_regs hr ON hr.pid = cp.id
        WHERE cp.best_email IS NOT NULL
        ORDER BY cp.best_email,
                 COALESCE(cp.canonical_confidence_score, 0) DESC,
                 cp.updated_at DESC NULLS LAST,
                 cp.id ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"days": window_days})
        return [
            _CandidateRow(
                participant_id=r[0],
                email_normalized=r[1],
                first_name=r[2],
                last_name=r[3],
                display_name=r[4],
                confidence_score=float(r[5]) if r[5] is not None else 0.0,
                updated_at=r[6],
                upcoming_event_count=int(r[7]),
                historical_registration_count=int(r[8]),
                segment_types=["upcoming_registrants"],
                yacht_name=r[9],
                last_registered_event_name=r[10],
            )
            for r in cur.fetchall()
        ]


def _query_likely_registrants(
    conn: psycopg.Connection,
    window_days: int,
    lookback_seasons: int = 3,
) -> list[_CandidateRow]:
    """Segment B: historically active participants not registered for upcoming events."""
    sql = """
        WITH upcoming_events AS (
            SELECT id
            FROM canonical_event
            WHERE start_date BETWEEN CURRENT_DATE
                                 AND CURRENT_DATE + (%(days)s * INTERVAL '1 day')
        ),
        upcoming_pids AS (
            SELECT DISTINCT canonical_primary_participant_id AS pid
            FROM canonical_registration cr
            JOIN upcoming_events ue ON ue.id = cr.canonical_event_id
            WHERE cr.canonical_primary_participant_id IS NOT NULL
        ),
        historical_regs AS (
            SELECT
                cr.canonical_primary_participant_id            AS pid,
                COUNT(*)                                       AS historical_registration_count,
                MAX(ce.event_name)                             AS last_registered_event_name
            FROM canonical_registration cr
            JOIN canonical_event ce ON ce.id = cr.canonical_event_id
            WHERE cr.canonical_primary_participant_id IS NOT NULL
              AND ce.start_date < CURRENT_DATE
              AND ce.season_year >= EXTRACT(YEAR FROM now())::int - %(lookback)s
              AND ce.season_year <= EXTRACT(YEAR FROM now())::int
            GROUP BY cr.canonical_primary_participant_id
        )
        SELECT
            cp.id::text                                            AS participant_id,
            cp.best_email                                         AS email_normalized,
            cp.first_name,
            cp.last_name,
            cp.display_name,
            COALESCE(cp.canonical_confidence_score, 0)            AS confidence_score,
            cp.updated_at,
            0::int                                                 AS upcoming_event_count,
            hr.historical_registration_count::int,
            NULL::text                                             AS yacht_name,
            hr.last_registered_event_name
        FROM historical_regs hr
        JOIN canonical_participant cp ON cp.id = hr.pid
        WHERE cp.best_email IS NOT NULL
          AND hr.pid NOT IN (SELECT pid FROM upcoming_pids)
        ORDER BY cp.best_email,
                 COALESCE(cp.canonical_confidence_score, 0) DESC,
                 cp.updated_at DESC NULLS LAST,
                 cp.id ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"days": window_days, "lookback": lookback_seasons})
        return [
            _CandidateRow(
                participant_id=r[0],
                email_normalized=r[1],
                first_name=r[2],
                last_name=r[3],
                display_name=r[4],
                confidence_score=float(r[5]) if r[5] is not None else 0.0,
                updated_at=r[6],
                upcoming_event_count=int(r[7]),
                historical_registration_count=int(r[8]),
                segment_types=["likely_registrants"],
                yacht_name=r[9],
                last_registered_event_name=r[10],
            )
            for r in cur.fetchall()
        ]


# ---------------------------------------------------------------------------
# Merge + dedupe
# ---------------------------------------------------------------------------

def _merge_segment_rows(
    rows_a: list[_CandidateRow],
    rows_b: list[_CandidateRow],
) -> list[_CandidateRow]:
    """Combine segment A and B rows keyed by participant_id.

    If the same participant appears in both segments, their segment_types are
    merged.  Different participants sharing an email are NOT collapsed here —
    that happens in _dedupe_by_email.
    """
    by_pid: dict[str, _CandidateRow] = {}
    for row in rows_a + rows_b:
        pid = row.participant_id
        if pid not in by_pid:
            by_pid[pid] = _CandidateRow(
                participant_id=row.participant_id,
                email_normalized=row.email_normalized,
                first_name=row.first_name,
                last_name=row.last_name,
                display_name=row.display_name,
                confidence_score=row.confidence_score,
                updated_at=row.updated_at,
                upcoming_event_count=row.upcoming_event_count,
                historical_registration_count=row.historical_registration_count,
                segment_types=list(row.segment_types),
                yacht_name=row.yacht_name,
                last_registered_event_name=row.last_registered_event_name,
            )
        else:
            existing = by_pid[pid]
            for st in row.segment_types:
                if st not in existing.segment_types:
                    existing.segment_types.append(st)
            if row.upcoming_event_count > existing.upcoming_event_count:
                existing.upcoming_event_count = row.upcoming_event_count
            if row.historical_registration_count > existing.historical_registration_count:
                existing.historical_registration_count = row.historical_registration_count
    return list(by_pid.values())


def _dedupe_by_email(rows: list[_CandidateRow]) -> tuple[list[_CandidateRow], int]:
    """Collapse to one winner per normalized email; return (winners, deduped_out).

    Winner selection (deterministic):
      1. Highest canonical_confidence_score (desc).
      2. Most recently updated (updated_at desc, None last).
      3. Lowest UUID string (asc) — lexical tie-breaker.

    The winner's contributing_participant_ids is populated with ALL participant
    IDs that shared the email (including the winner itself) for audit purposes.
    The winner's segment_types is the union across all candidates for that email.
    """
    by_email: dict[str, list[_CandidateRow]] = {}
    for row in rows:
        by_email.setdefault(row.email_normalized, []).append(row)

    winners: list[_CandidateRow] = []
    deduped_out = 0
    for _email, candidates in sorted(by_email.items()):
        candidates.sort(
            key=lambda r: (
                -r.confidence_score,
                -(r.updated_at.timestamp() if r.updated_at else 0.0),
                r.participant_id,  # UUID str, lexical ascending
            )
        )
        winner = candidates[0]

        # Merge segment_types across all candidates for this email
        merged_segments: list[str] = list(winner.segment_types)
        for c in candidates[1:]:
            for st in c.segment_types:
                if st not in merged_segments:
                    merged_segments.append(st)
        winner.segment_types = merged_segments

        # Record all participant IDs for audit
        winner.contributing_participant_ids = [c.participant_id for c in candidates]

        winners.append(winner)
        deduped_out += len(candidates) - 1

    return winners, deduped_out


# ---------------------------------------------------------------------------
# Apply suppression
# ---------------------------------------------------------------------------

def _apply_suppression(
    rows: list[_CandidateRow],
    suppression_map: dict[str, str],
    ctrs: RunCounters,
    generated_at: str,
) -> list[_AudienceRow]:
    """Tag each row as suppressed or eligible; update counters."""
    audience_rows: list[_AudienceRow] = []
    for row in rows:
        reason = suppression_map.get(row.email_normalized)
        is_suppressed = reason is not None
        if is_suppressed:
            if reason == "unsubscribed":
                ctrs.activation_rows_suppressed_unsubscribed += 1
            elif reason == "cleaned":
                ctrs.activation_rows_suppressed_cleaned += 1
        else:
            ctrs.activation_rows_eligible += 1

        audience_rows.append(_AudienceRow(
            email_normalized=row.email_normalized,
            participant_id=row.participant_id,
            first_name=row.first_name,
            last_name=row.last_name,
            display_name=row.display_name,
            segment_types=sorted(row.segment_types),
            upcoming_event_count=row.upcoming_event_count,
            historical_registration_count=row.historical_registration_count,
            is_suppressed=is_suppressed,
            suppression_reason=reason,
            yacht_name=row.yacht_name,
            last_registered_event_name=row.last_registered_event_name,
            generated_at=generated_at,
            contributing_participant_ids=row.contributing_participant_ids,
        ))
    return audience_rows


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def _write_csv(rows: list[_AudienceRow], output_path: str) -> int:
    """Write eligible rows to CSV ordered by email. Returns count written."""
    eligible = sorted(
        (r for r in rows if not r.is_suppressed),
        key=lambda r: r.email_normalized,
    )
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
        writer.writeheader()
        for row in eligible:
            writer.writerow({
                "email": row.email_normalized,
                "first_name": row.first_name or "",
                "last_name": row.last_name or "",
                "display_name": row.display_name or "",
                "segment_types": ",".join(row.segment_types),
                "upcoming_event_count": row.upcoming_event_count,
                "historical_registration_count": row.historical_registration_count,
                "suppression_status": "eligible",
                "source_participant_id": row.participant_id,
                "generated_at": row.generated_at,
                "club_name": "",
                "yacht_name": row.yacht_name or "",
                "last_registered_event_name": row.last_registered_event_name or "",
            })
    return len(eligible)


# ---------------------------------------------------------------------------
# Audit persistence
# ---------------------------------------------------------------------------

def _insert_activation_run(
    conn: psycopg.Connection,
    run_id: str,
    mode: str,
    segment_type: str,
    event_window_days: int,
    created_by: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO mailchimp_activation_run
                (id, mode, segment_type, event_window_days, status, created_by)
            VALUES (%s, %s, %s, %s, 'running', %s)
            """,
            (run_id, mode, segment_type, event_window_days, created_by),
        )


def _update_activation_run(
    conn: psycopg.Connection,
    run_id: str,
    status: str,
    ctrs: RunCounters,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mailchimp_activation_run
            SET finished_at = now(), status = %s, counters = %s
            WHERE id = %s
            """,
            (status, json.dumps(_activation_counters_dict(ctrs)), run_id),
        )


def _insert_activation_rows(
    conn: psycopg.Connection,
    run_id: str,
    rows: list[_AudienceRow],
) -> None:
    with conn.cursor() as cur:
        for row in rows:
            payload = {
                "first_name": row.first_name,
                "last_name": row.last_name,
                "display_name": row.display_name,
                "segment_types": row.segment_types,
                "upcoming_event_count": row.upcoming_event_count,
                "historical_registration_count": row.historical_registration_count,
                "yacht_name": row.yacht_name,
                "last_registered_event_name": row.last_registered_event_name,
                "contributing_participant_ids": row.contributing_participant_ids,
            }
            cur.execute(
                """
                INSERT INTO mailchimp_activation_row
                    (run_id, email_normalized, participant_id, segment_types,
                     is_suppressed, suppression_reason, payload)
                VALUES (%s, %s, %s::uuid, %s, %s, %s, %s)
                ON CONFLICT (run_id, email_normalized) DO NOTHING
                """,
                (
                    run_id,
                    row.email_normalized,
                    row.participant_id,
                    row.segment_types,
                    row.is_suppressed,
                    row.suppression_reason,
                    json.dumps(payload),
                ),
            )


# ---------------------------------------------------------------------------
# API mode (optional)
# ---------------------------------------------------------------------------

def _persist_api_contact_id(
    conn: psycopg.Connection,
    participant_id: str,
    email_normalized: str,
    mailchimp_contact_id: str,
    subscriber_hash: str,
    ctrs: RunCounters,
) -> bool:
    """Persist the Mailchimp contact ID returned by the API.

    Returns True on success, False if a cross-participant conflict is detected
    (in which case a review-queue row is inserted and ctrs are updated).
    """
    # Check for existing row with this contact_id.
    # Three cases (mirrors the LEID/EUID logic in _upsert_mailchimp_identity):
    #   1. Different participant → cross-participant conflict: queue + return False.
    #   2. Same participant, same email → ON CONFLICT below handles it (no action here).
    #   3. Same participant, different email → contact_id already captured on that email's
    #      row; suppress it on this row to avoid hitting the partial unique index.
    contact_id_for_upsert = mailchimp_contact_id
    existing = conn.execute(
        """
        SELECT participant_id::text, email_normalized
        FROM participant_mailchimp_identity
        WHERE mailchimp_contact_id = %s
        """,
        (mailchimp_contact_id,),
    ).fetchone()
    if existing:
        if existing[0] != participant_id:
            # Cross-participant conflict
            conn.execute(
                """
                INSERT INTO mailchimp_identity_review_queue
                    (source_file_name, email_normalized, candidate_participant_id,
                     reason_code, reason_detail, raw_payload)
                VALUES (%s, %s, %s::uuid, %s, %s, %s::jsonb)
                """,
                (
                    "mailchimp_event_activation",
                    email_normalized,
                    participant_id,
                    "mailchimp_contact_id_conflict",
                    f"contact_id={mailchimp_contact_id!r} already linked to "
                    f"participant {existing[0]}",
                    json.dumps({"mailchimp_contact_id": mailchimp_contact_id,
                                "participant_id": participant_id}),
                ),
            )
            ctrs.mailchimp_contact_id_conflicts += 1
            ctrs.mailchimp_identity_conflicts += 1
            return False
        elif existing[1] != email_normalized:
            # Same participant, different email — contact_id already recorded; suppress
            # it here to avoid a unique-index violation on the new email's row.
            contact_id_for_upsert = None

    # Upsert identity link with contact ID and subscriber hash
    result = conn.execute(
        """
        INSERT INTO participant_mailchimp_identity
            (participant_id, mailchimp_contact_id, subscriber_hash, email_normalized,
             source_system, source_file_name)
        VALUES (%s::uuid, %s, %s, %s, %s, %s)
        ON CONFLICT (participant_id, email_normalized) DO UPDATE SET
            last_seen_at         = now(),
            mailchimp_contact_id = COALESCE(participant_mailchimp_identity.mailchimp_contact_id,
                                            EXCLUDED.mailchimp_contact_id),
            subscriber_hash      = COALESCE(participant_mailchimp_identity.subscriber_hash,
                                            EXCLUDED.subscriber_hash)
        RETURNING (xmax = 0) AS was_inserted
        """,
        (participant_id, contact_id_for_upsert, subscriber_hash, email_normalized,
         "mailchimp_event_activation", "mailchimp_event_activation"),
    ).fetchone()
    if result and result[0]:
        ctrs.mailchimp_identity_links_inserted += 1
    elif result:
        ctrs.mailchimp_identity_links_updated += 1
    return True


def _api_upsert(
    rows: list[_AudienceRow],
    list_id: str,
    api_key: str,
    ctrs: RunCounters,
    conn: Optional[psycopg.Connection] = None,
) -> None:
    """Upsert eligible rows into a Mailchimp list and apply segment tags.

    Never subscribes suppressed contacts.  Retries on rate-limit (429) with
    exponential backoff up to 3 attempts.  When conn is provided, persists
    returned Mailchimp contact IDs into participant_mailchimp_identity and
    fails closed on cross-participant contact-ID conflicts.
    """
    try:
        import mailchimp_marketing as mc
        from mailchimp_marketing.api_client import ApiClientError
    except ImportError:
        raise RuntimeError(
            "mailchimp-marketing package is required for API mode. "
            "Install with: pip install mailchimp-marketing"
        )

    parts = api_key.rsplit("-", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Cannot parse Mailchimp datacenter suffix from API key. "
            "Expected format: '<key>-<datacenter>' e.g. 'abc123-us6'."
        )
    server = parts[1]

    client = mc.Client()
    client.set_config({"api_key": api_key, "server": server})

    eligible = [r for r in rows if not r.is_suppressed]
    for row in eligible:
        subscriber_hash = hashlib.md5(row.email_normalized.encode()).hexdigest()
        member_body = {
            "email_address": row.email_normalized,
            "status_if_new": "subscribed",
            "merge_fields": {
                "FNAME": row.first_name or "",
                "LNAME": row.last_name or "",
            },
            "tags": [f"segment:{st}" for st in row.segment_types] + ["source:regattadata_cdp"],
        }
        api_success = False
        for attempt in range(3):
            try:
                response = client.lists.set_list_member(list_id, subscriber_hash, member_body)
                api_success = True
                break
            except ApiClientError as exc:
                status_code = getattr(exc, "status_code", None)
                if status_code == 429:
                    wait = 2 ** attempt
                    ctrs.warnings.append(
                        f"Rate limited for {row.email_normalized}, "
                        f"retrying in {wait}s (attempt {attempt + 1}/3)"
                    )
                    time.sleep(wait)
                    continue
                ctrs.activation_rows_api_failed += 1
                ctrs.warnings.append(
                    f"API error for {row.email_normalized}: {exc}"
                )
                break
        else:
            ctrs.activation_rows_api_failed += 1
            ctrs.warnings.append(
                f"Max retries exceeded for {row.email_normalized}"
            )

        if not api_success:
            continue

        # Persist returned contact ID when conn is available.
        # mailchimp_marketing returns plain dicts, so use .get() for dicts
        # and fall back to getattr() for any object-style response.
        if conn is not None:
            if isinstance(response, dict):
                mailchimp_contact_id = response.get("unique_email_id")
            else:
                mailchimp_contact_id = getattr(response, "unique_email_id", None)
            if mailchimp_contact_id:
                ok = _persist_api_contact_id(
                    conn, row.participant_id, row.email_normalized,
                    mailchimp_contact_id, subscriber_hash, ctrs,
                )
                if not ok:
                    # Cross-participant conflict — treat this row as failed
                    ctrs.activation_rows_api_failed += 1
                    ctrs.warnings.append(
                        f"Contact-ID conflict for {row.email_normalized}: "
                        f"contact_id={mailchimp_contact_id!r} already linked to "
                        "a different participant"
                    )
                    continue

        ctrs.activation_rows_api_upserted += 1


# ---------------------------------------------------------------------------
# Counters helper
# ---------------------------------------------------------------------------

def _activation_counters_dict(ctrs: RunCounters) -> dict:
    return {
        "rows_considered": ctrs.activation_rows_considered,
        "rows_eligible": ctrs.activation_rows_eligible,
        "rows_suppressed_unsubscribed": ctrs.activation_rows_suppressed_unsubscribed,
        "rows_suppressed_cleaned": ctrs.activation_rows_suppressed_cleaned,
        "rows_deduped_out": ctrs.activation_rows_deduped_out,
        "rows_exported_csv": ctrs.activation_rows_exported_csv,
        "rows_api_upserted": ctrs.activation_rows_api_upserted,
        "rows_api_failed": ctrs.activation_rows_api_failed,
        "identity_links_inserted": ctrs.mailchimp_identity_links_inserted,
        "identity_links_updated": ctrs.mailchimp_identity_links_updated,
        "identity_conflicts": ctrs.mailchimp_identity_conflicts,
        "contact_id_conflicts": ctrs.mailchimp_contact_id_conflicts,
        "db_errors": ctrs.db_phase_errors,
        "warnings": ctrs.warnings,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_mailchimp_event_activation(
    run_id: str,
    started_at: str,
    db_dsn: str,
    ctrs: RunCounters,
    *,
    event_window_days: int = 45,
    segment_type: str = "all",
    delivery_mode: str = "csv",
    output_path: Optional[str] = None,
    mailchimp_list_id: Optional[str] = None,
    mailchimp_api_key: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """Main activation pipeline.

    Connects to the DB, builds audience segments, applies suppression and
    email-level dedupe, persists audit rows, and delivers via CSV or API.

    Dry-run: performs full selection/suppression logic; writes no CSV, makes
    no API calls, and rolls back all DB writes.
    """
    if delivery_mode == "csv" and not output_path and not dry_run:
        raise ValueError("output_path is required for csv delivery mode")
    if delivery_mode == "api":
        if not mailchimp_list_id:
            raise ValueError("mailchimp_list_id is required for api delivery mode")
        if not mailchimp_api_key:
            raise ValueError(
                "Mailchimp API key env var is not set; "
                "cannot run api delivery mode"
            )

    generated_at = datetime.now(timezone.utc).isoformat()

    conn = psycopg.connect(db_dsn, autocommit=False)
    try:
        # 1. Dependency check — fail fast if canonical tables are missing
        _check_dependencies(conn)

        # 2. Load suppression map (latest Mailchimp state per email)
        suppression_map = _load_suppression_map(conn)

        # 3. Query segments
        rows_a: list[_CandidateRow] = []
        rows_b: list[_CandidateRow] = []
        if segment_type in ("upcoming_registrants", "all"):
            rows_a = _query_upcoming_registrants(conn, event_window_days)
        if segment_type in ("likely_registrants", "all"):
            rows_b = _query_likely_registrants(conn, event_window_days)

        # 4. Merge (same participant in A+B) then dedupe (same email, different participants)
        merged = _merge_segment_rows(rows_a, rows_b)
        ctrs.activation_rows_considered = len(merged)
        deduped, deduped_out = _dedupe_by_email(merged)
        ctrs.activation_rows_deduped_out = deduped_out

        # 5. Apply suppression
        audience_rows = _apply_suppression(deduped, suppression_map, ctrs, generated_at)

        if not dry_run:
            # 6. Persist audit tables
            _insert_activation_run(
                conn, run_id, delivery_mode, segment_type,
                event_window_days, created_by="regatta-import",
            )
            _insert_activation_rows(conn, run_id, audience_rows)

            # 7. Deliver
            if delivery_mode == "csv":
                assert output_path is not None
                n_written = _write_csv(audience_rows, output_path)
                ctrs.activation_rows_exported_csv = n_written
            elif delivery_mode == "api":
                assert mailchimp_api_key is not None
                assert mailchimp_list_id is not None
                _api_upsert(audience_rows, mailchimp_list_id, mailchimp_api_key, ctrs,
                            conn=conn)

            # Finalize run record — DB errors OR any API delivery failures → failed
            final_status = (
                "failed"
                if ctrs.db_phase_errors > 0 or ctrs.activation_rows_api_failed > 0
                else "ok"
            )
            _update_activation_run(conn, run_id, final_status, ctrs)
            conn.commit()
        else:
            # Dry run: count hypothetical export but write nothing
            eligible_count = sum(1 for r in audience_rows if not r.is_suppressed)
            ctrs.activation_rows_exported_csv = eligible_count
            conn.rollback()

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        ctrs.db_phase_errors += 1
        raise
    finally:
        conn.close()


def build_activation_report(ctrs: RunCounters, dry_run: bool = False) -> str:
    """Return a human-readable summary of the activation run."""
    prefix = "[DRY RUN] " if dry_run else ""
    lines = [
        f"{prefix}mailchimp_event_activation report:",
        f"  rows_considered:                {ctrs.activation_rows_considered}",
        f"  rows_eligible:                  {ctrs.activation_rows_eligible}",
        f"  rows_suppressed_unsubscribed:   {ctrs.activation_rows_suppressed_unsubscribed}",
        f"  rows_suppressed_cleaned:        {ctrs.activation_rows_suppressed_cleaned}",
        f"  rows_deduped_out:               {ctrs.activation_rows_deduped_out}",
        f"  rows_exported_csv:              {ctrs.activation_rows_exported_csv}",
        f"  rows_api_upserted:              {ctrs.activation_rows_api_upserted}",
        f"  rows_api_failed:                {ctrs.activation_rows_api_failed}",
        f"  identity_links_inserted:        {ctrs.mailchimp_identity_links_inserted}",
        f"  identity_links_updated:         {ctrs.mailchimp_identity_links_updated}",
        f"  identity_conflicts:             {ctrs.mailchimp_identity_conflicts}",
        f"  contact_id_conflicts:           {ctrs.mailchimp_contact_id_conflicts}",
        f"  db_errors:                      {ctrs.db_phase_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"  warnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:10]:
            lines.append(f"    - {w}")
    return "\n".join(lines)
