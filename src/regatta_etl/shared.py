"""regatta_etl.shared

Shared utilities used by both private_export and public_scrape modes.
Includes RejectWriter, base RunCounters, common DB helpers, and
report-writing support.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg

from regatta_etl.normalize import (
    normalize_name,
    parse_name_parts,
    slug_name,
    trim,
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class AmbiguousMatchError(Exception):
    """Raised when a contact-value lookup returns multiple participant matches."""


# ---------------------------------------------------------------------------
# RejectWriter
# ---------------------------------------------------------------------------

class RejectWriter:
    """Lazy-open CSV writer for rejected rows."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh = None
        self._writer = None

    def write(self, row: dict[str, str], reason: str) -> None:
        if self._fh is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self._path, "w", newline="", encoding="utf-8")
            fieldnames = list(row.keys()) + ["_reject_reason"]
            self._writer = csv.DictWriter(
                self._fh, fieldnames=fieldnames, extrasaction="ignore"
            )
            self._writer.writeheader()
        out = dict(row)
        out["_reject_reason"] = reason
        self._writer.writerow(out)
        self._fh.flush()

    def close(self) -> None:
        if self._fh:
            self._fh.close()


# ---------------------------------------------------------------------------
# RunCounters
# ---------------------------------------------------------------------------

@dataclass
class RunCounters:
    # Common counters
    rows_read: int = 0
    rows_rejected: int = 0
    db_phase_errors: int = 0
    participants_inserted: int = 0
    participants_matched_existing: int = 0
    yachts_inserted: int = 0
    entries_upserted: int = 0
    owner_links_inserted: int = 0
    memberships_inserted: int = 0
    contact_points_inserted: int = 0
    addresses_inserted: int = 0
    warnings: list[str] = field(default_factory=list)
    # Public-scrape-specific counters
    events_upserted: int = 0
    unmatched_entry_urls: int = 0
    synthesized_events: int = 0
    events_skipped_no_race_id: int = 0
    # Jotform-waiver-specific counters
    raw_rows_inserted: int = 0
    curated_rows_processed: int = 0
    document_status_upserted: int = 0
    unresolved_event_links: int = 0
    participant_related_contacts_inserted: int = 0
    # Mailchimp-audience-specific counters
    mailchimp_status_rows_inserted: int = 0
    mailchimp_tags_inserted: int = 0
    # Airtable-copy-specific counters (totals)
    airtable_rows_raw_inserted: int = 0
    airtable_rows_curated_processed: int = 0
    airtable_rows_curated_rejected: int = 0
    airtable_xref_inserted: int = 0
    airtable_warnings_unknown_source_type: int = 0
    # Airtable per-asset counters
    airtable_clubs_read: int = 0
    airtable_clubs_inserted: int = 0
    airtable_clubs_rejected: int = 0
    airtable_events_read: int = 0
    airtable_events_inserted: int = 0
    airtable_events_rejected: int = 0
    airtable_yachts_read: int = 0
    airtable_yachts_inserted: int = 0
    airtable_yachts_rejected: int = 0
    airtable_owners_read: int = 0
    airtable_owners_inserted: int = 0
    airtable_owners_rejected: int = 0
    airtable_participants_read: int = 0
    airtable_participants_inserted: int = 0
    airtable_participants_rejected: int = 0
    airtable_entries_read: int = 0
    airtable_entries_inserted: int = 0
    airtable_entries_rejected: int = 0
    # Yacht-scoring-specific counters
    yacht_scoring_rows_raw_inserted: int = 0
    yacht_scoring_rows_curated_processed: int = 0
    yacht_scoring_rows_curated_rejected: int = 0
    yacht_scoring_events_upserted: int = 0
    yacht_scoring_entries_upserted: int = 0
    yacht_scoring_yachts_upserted: int = 0
    yacht_scoring_participants_upserted: int = 0
    yacht_scoring_xref_inserted: int = 0
    yacht_scoring_unknown_schema_rows: int = 0
    # BHYC member directory-specific counters
    bhyc_pages_discovered: int = 0
    bhyc_pages_fetched: int = 0
    bhyc_pages_archived: int = 0
    bhyc_pages_parsed: int = 0
    bhyc_pages_rejected: int = 0
    bhyc_members_processed: int = 0
    bhyc_members_skipped_checkpoint: int = 0
    bhyc_household_members_created: int = 0
    bhyc_raw_rows_inserted: int = 0
    bhyc_xref_inserted: int = 0
    bhyc_candidate_created: int = 0
    bhyc_candidate_enriched: int = 0
    bhyc_candidate_links_inserted: int = 0
    bhyc_auth_errors: int = 0
    bhyc_network_errors: int = 0
    bhyc_parse_errors: int = 0
    bhyc_rate_limit_hits: int = 0
    bhyc_safe_stop_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "rows_rejected": self.rows_rejected,
            "db_phase_errors": self.db_phase_errors,
            "participants_inserted": self.participants_inserted,
            "participants_matched_existing": self.participants_matched_existing,
            "yachts_inserted": self.yachts_inserted,
            "entries_upserted": self.entries_upserted,
            "owner_links_inserted": self.owner_links_inserted,
            "memberships_inserted": self.memberships_inserted,
            "contact_points_inserted": self.contact_points_inserted,
            "addresses_inserted": self.addresses_inserted,
            "events_upserted": self.events_upserted,
            "unmatched_entry_urls": self.unmatched_entry_urls,
            "synthesized_events": self.synthesized_events,
            "events_skipped_no_race_id": self.events_skipped_no_race_id,
            "raw_rows_inserted": self.raw_rows_inserted,
            "curated_rows_processed": self.curated_rows_processed,
            "document_status_upserted": self.document_status_upserted,
            "unresolved_event_links": self.unresolved_event_links,
            "participant_related_contacts_inserted": self.participant_related_contacts_inserted,
            "mailchimp_status_rows_inserted": self.mailchimp_status_rows_inserted,
            "mailchimp_tags_inserted": self.mailchimp_tags_inserted,
            "airtable_rows_raw_inserted": self.airtable_rows_raw_inserted,
            "airtable_rows_curated_processed": self.airtable_rows_curated_processed,
            "airtable_rows_curated_rejected": self.airtable_rows_curated_rejected,
            "airtable_xref_inserted": self.airtable_xref_inserted,
            "airtable_warnings_unknown_source_type": self.airtable_warnings_unknown_source_type,
            "airtable_clubs_read": self.airtable_clubs_read,
            "airtable_clubs_inserted": self.airtable_clubs_inserted,
            "airtable_clubs_rejected": self.airtable_clubs_rejected,
            "airtable_events_read": self.airtable_events_read,
            "airtable_events_inserted": self.airtable_events_inserted,
            "airtable_events_rejected": self.airtable_events_rejected,
            "airtable_yachts_read": self.airtable_yachts_read,
            "airtable_yachts_inserted": self.airtable_yachts_inserted,
            "airtable_yachts_rejected": self.airtable_yachts_rejected,
            "airtable_owners_read": self.airtable_owners_read,
            "airtable_owners_inserted": self.airtable_owners_inserted,
            "airtable_owners_rejected": self.airtable_owners_rejected,
            "airtable_participants_read": self.airtable_participants_read,
            "airtable_participants_inserted": self.airtable_participants_inserted,
            "airtable_participants_rejected": self.airtable_participants_rejected,
            "airtable_entries_read": self.airtable_entries_read,
            "airtable_entries_inserted": self.airtable_entries_inserted,
            "airtable_entries_rejected": self.airtable_entries_rejected,
            "yacht_scoring_rows_raw_inserted": self.yacht_scoring_rows_raw_inserted,
            "yacht_scoring_rows_curated_processed": self.yacht_scoring_rows_curated_processed,
            "yacht_scoring_rows_curated_rejected": self.yacht_scoring_rows_curated_rejected,
            "yacht_scoring_events_upserted": self.yacht_scoring_events_upserted,
            "yacht_scoring_entries_upserted": self.yacht_scoring_entries_upserted,
            "yacht_scoring_yachts_upserted": self.yacht_scoring_yachts_upserted,
            "yacht_scoring_participants_upserted": self.yacht_scoring_participants_upserted,
            "yacht_scoring_xref_inserted": self.yacht_scoring_xref_inserted,
            "yacht_scoring_unknown_schema_rows": self.yacht_scoring_unknown_schema_rows,
            "bhyc_pages_discovered": self.bhyc_pages_discovered,
            "bhyc_pages_fetched": self.bhyc_pages_fetched,
            "bhyc_pages_archived": self.bhyc_pages_archived,
            "bhyc_pages_parsed": self.bhyc_pages_parsed,
            "bhyc_pages_rejected": self.bhyc_pages_rejected,
            "bhyc_members_processed": self.bhyc_members_processed,
            "bhyc_members_skipped_checkpoint": self.bhyc_members_skipped_checkpoint,
            "bhyc_household_members_created": self.bhyc_household_members_created,
            "bhyc_raw_rows_inserted": self.bhyc_raw_rows_inserted,
            "bhyc_xref_inserted": self.bhyc_xref_inserted,
            "bhyc_candidate_created": self.bhyc_candidate_created,
            "bhyc_candidate_enriched": self.bhyc_candidate_enriched,
            "bhyc_candidate_links_inserted": self.bhyc_candidate_links_inserted,
            "bhyc_auth_errors": self.bhyc_auth_errors,
            "bhyc_network_errors": self.bhyc_network_errors,
            "bhyc_parse_errors": self.bhyc_parse_errors,
            "bhyc_rate_limit_hits": self.bhyc_rate_limit_hits,
            "bhyc_safe_stop_reason": self.bhyc_safe_stop_reason,
            "warnings": self.warnings,
        }


# ---------------------------------------------------------------------------
# Header normalization
# ---------------------------------------------------------------------------

def normalize_headers(raw: dict[str, str]) -> dict[str, str]:
    """Return a new dict with header keys whitespace-stripped."""
    return {k.strip(): v for k, v in raw.items()}


# ---------------------------------------------------------------------------
# Shared DB helpers — participant
# ---------------------------------------------------------------------------

def resolve_participant_by_name(
    conn: psycopg.Connection,
    name_norm: str,
) -> str | None:
    row = conn.execute(
        "SELECT id FROM participant WHERE normalized_full_name = %s ORDER BY id ASC LIMIT 1",
        (name_norm,),
    ).fetchone()
    return str(row[0]) if row else None


def insert_participant(
    conn: psycopg.Connection,
    full_name: str,
) -> str:
    first, last = parse_name_parts(full_name)
    norm = normalize_name(full_name)
    row = conn.execute(
        """
        INSERT INTO participant (full_name, normalized_full_name, first_name, last_name)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (full_name, norm, first, last),
    ).fetchone()
    return str(row[0])


def resolve_or_insert_coowner_participant(
    conn: psycopg.Connection,
    full_name: str,
    counters: RunCounters,
) -> str:
    """Resolve co-owner by normalized name only, inserting if absent."""
    name_norm = normalize_name(full_name)
    if name_norm:
        pid = resolve_participant_by_name(conn, name_norm)
        if pid:
            counters.participants_matched_existing += 1
            return pid
    pid = insert_participant(conn, full_name)
    counters.participants_inserted += 1
    return pid


# ---------------------------------------------------------------------------
# Shared DB helpers — club / membership
# ---------------------------------------------------------------------------

def upsert_affiliate_club(
    conn: psycopg.Connection,
    org_name: str,
) -> str:
    """Upsert a yacht club with vitality_status='unknown' (DO NOTHING on conflict)."""
    norm = slug_name(org_name)
    conn.execute(
        """
        INSERT INTO yacht_club (name, normalized_name, vitality_status)
        VALUES (%s, %s, 'unknown')
        ON CONFLICT (normalized_name) DO NOTHING
        """,
        (org_name, norm),
    )
    row = conn.execute(
        "SELECT id FROM yacht_club WHERE normalized_name = %s",
        (norm,),
    ).fetchone()
    return str(row[0])


def upsert_membership(
    conn: psycopg.Connection,
    participant_id: str,
    club_id: str,
    effective_start: date | None,
    source_system: str,
    counters: RunCounters,
) -> None:
    existing = conn.execute(
        """
        SELECT id FROM club_membership
        WHERE participant_id = %s
          AND yacht_club_id = %s
          AND effective_end IS NULL
        """,
        (participant_id, club_id),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO club_membership
          (participant_id, yacht_club_id, membership_role,
           effective_start, source_system)
        VALUES (%s, %s, 'member', %s, %s)
        """,
        (participant_id, club_id, effective_start, source_system),
    )
    counters.memberships_inserted += 1


def upsert_ownership(
    conn: psycopg.Connection,
    participant_id: str,
    yacht_id: str,
    role: str,
    is_primary_contact: bool,
    effective_start: date,
    source_system: str,
    counters: RunCounters,
) -> None:
    existing = conn.execute(
        """
        SELECT id FROM yacht_ownership
        WHERE participant_id = %s
          AND yacht_id = %s
          AND role = %s
          AND effective_end IS NULL
        """,
        (participant_id, yacht_id, role),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO yacht_ownership
          (participant_id, yacht_id, role, is_primary_contact,
           effective_start, source_system)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (participant_id, yacht_id, role, is_primary_contact,
         effective_start, source_system),
    )
    counters.owner_links_inserted += 1


# ---------------------------------------------------------------------------
# Shared DB helpers — yacht
# ---------------------------------------------------------------------------

def resolve_or_insert_yacht(
    conn: psycopg.Connection,
    yacht_name: str,
    boat_type: str | None,
    length_feet: Any | None,
    counters: RunCounters,
) -> str:
    """Resolve yacht by (normalized_name, length_feet) or normalized_name alone."""
    norm = slug_name(yacht_name)

    if length_feet is not None:
        row = conn.execute(
            "SELECT id FROM yacht WHERE normalized_name = %s AND length_feet = %s ORDER BY id ASC LIMIT 1",
            (norm, length_feet),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id FROM yacht WHERE normalized_name = %s ORDER BY id ASC LIMIT 1",
            (norm,),
        ).fetchone()

    if row:
        return str(row[0])

    model = trim(boat_type)
    new_row = conn.execute(
        """
        INSERT INTO yacht (name, normalized_name, model, length_feet)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (yacht_name, norm, model, length_feet),
    ).fetchone()
    counters.yachts_inserted += 1
    return str(new_row[0])


def resolve_or_insert_yacht_with_sail(
    conn: psycopg.Connection,
    yacht_name: str,
    sail_num: str | None,
    boat_type: str | None,
    event_instance_id: str,
    registration_external_id: str,
    counters: RunCounters,
) -> str:
    """Resolve/insert yacht for public-scrape rows.

    Sail present: exact match on (normalized_name, normalized_sail_number).
                  Raises AmbiguousMatchError if multiple records found.
                  INSERTs a new record if no match.

    Sail absent:  looks up a prior event_entry for this
                  (event_instance_id, registration_external_id) and reuses
                  its yacht_id — guaranteeing the same ID across reruns.
                  INSERTs a new yacht record on the first run.
    """
    norm = slug_name(yacht_name)
    sail_norm = slug_name(sail_num) if sail_num else None

    if sail_norm:
        rows = conn.execute(
            """
            SELECT id FROM yacht
            WHERE normalized_name = %s AND normalized_sail_number = %s
            """,
            (norm, sail_norm),
        ).fetchall()
        if len(rows) > 1:
            raise AmbiguousMatchError(
                f"ambiguous_yacht_match: name={norm!r} sail={sail_norm!r} "
                f"({len(rows)} records)"
            )
        if len(rows) == 1:
            yacht_id = str(rows[0][0])
            if boat_type:
                conn.execute(
                    "UPDATE yacht SET model = COALESCE(model, %s) WHERE id = %s",
                    (trim(boat_type), yacht_id),
                )
            return yacht_id
        # No match → INSERT below

    else:
        # No sail discriminator: reuse the yacht_id already linked to this
        # entry in a prior run (stable via registration_external_id) so that
        # reruns produce the same (event_instance_id, yacht_id) key and the
        # event_entry ON CONFLICT fires correctly.
        prior = conn.execute(
            """
            SELECT yacht_id
            FROM event_entry
            WHERE event_instance_id = %s
              AND registration_external_id = %s
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """,
            (event_instance_id, registration_external_id),
        ).fetchone()
        if prior:
            return str(prior[0])
        # First run for this entry → INSERT a new yacht below

    model = trim(boat_type)
    new_row = conn.execute(
        """
        INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number, model)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (yacht_name, norm, sail_num, sail_norm, model),
    ).fetchone()
    counters.yachts_inserted += 1
    return str(new_row[0])


# ---------------------------------------------------------------------------
# Shared DB helpers — event entry + participant link
# ---------------------------------------------------------------------------

def upsert_event_entry(
    conn: psycopg.Connection,
    event_instance_id: str,
    yacht_id: str,
    entry_status: str,
    registration_source: str,
    registration_external_id: str | None,
    registered_at: datetime | None,
    counters: RunCounters,
) -> str:
    row = conn.execute(
        """
        INSERT INTO event_entry
          (event_instance_id, yacht_id, entry_status,
           registration_source, registration_external_id, registered_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_instance_id, yacht_id) DO UPDATE SET
          entry_status = CASE
            WHEN 'confirmed' IN (event_entry.entry_status, EXCLUDED.entry_status)
              THEN 'confirmed'
            WHEN 'submitted' IN (event_entry.entry_status, EXCLUDED.entry_status)
              THEN 'submitted'
            ELSE event_entry.entry_status
          END,
          registered_at = CASE
            WHEN event_entry.registered_at IS NULL
              THEN EXCLUDED.registered_at
            WHEN EXCLUDED.registered_at IS NULL
              THEN event_entry.registered_at
            ELSE LEAST(event_entry.registered_at, EXCLUDED.registered_at)
          END,
          registration_external_id =
            COALESCE(event_entry.registration_external_id,
                     EXCLUDED.registration_external_id)
        RETURNING id
        """,
        (event_instance_id, yacht_id, entry_status,
         registration_source, registration_external_id, registered_at),
    ).fetchone()
    counters.entries_upserted += 1
    return str(row[0])


def upsert_entry_participant(
    conn: psycopg.Connection,
    event_entry_id: str,
    participant_id: str,
    role: str,
    participation_state: str,
    source_system: str,
) -> None:
    conn.execute(
        """
        INSERT INTO event_entry_participant
          (event_entry_id, participant_id, role,
           participation_state, source_system)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (event_entry_id, participant_id, role) DO NOTHING
        """,
        (event_entry_id, participant_id, role, participation_state, source_system),
    )


# ---------------------------------------------------------------------------
# Shared DB helpers — event context
# ---------------------------------------------------------------------------

def upsert_event_context(
    conn: psycopg.Connection,
    host_club_name: str,
    host_club_normalized: str,
    event_series_name: str,
    event_series_normalized: str,
    event_display_name: str,
    season_year: int,
    registration_open_at: datetime | None,
) -> tuple[str, str, str]:
    """Upsert host club, event series, event instance.

    Returns (host_club_id, event_series_id, event_instance_id).
    Caller manages transaction.
    """
    club_row = conn.execute(
        """
        INSERT INTO yacht_club (name, normalized_name, vitality_status)
        VALUES (%s, %s, 'active')
        ON CONFLICT (normalized_name) DO UPDATE SET
          name = EXCLUDED.name,
          vitality_status = 'active'
        RETURNING id
        """,
        (host_club_name, host_club_normalized),
    ).fetchone()
    host_club_id = str(club_row[0])

    conn.execute(
        """
        INSERT INTO event_series (yacht_club_id, name, normalized_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (yacht_club_id, normalized_name) DO NOTHING
        """,
        (host_club_id, event_series_name, event_series_normalized),
    )
    series_row = conn.execute(
        """
        SELECT id FROM event_series
        WHERE yacht_club_id = %s AND normalized_name = %s
        """,
        (host_club_id, event_series_normalized),
    ).fetchone()
    event_series_id = str(series_row[0])

    instance_row = conn.execute(
        """
        INSERT INTO event_instance
          (event_series_id, display_name, season_year, registration_open_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_series_id, season_year) DO UPDATE SET
          display_name = EXCLUDED.display_name,
          registration_open_at = CASE
            WHEN event_instance.registration_open_at IS NULL
              THEN EXCLUDED.registration_open_at
            WHEN EXCLUDED.registration_open_at IS NULL
              THEN event_instance.registration_open_at
            ELSE LEAST(event_instance.registration_open_at,
                       EXCLUDED.registration_open_at)
          END
        RETURNING id
        """,
        (event_series_id, event_display_name, season_year, registration_open_at),
    ).fetchone()
    event_instance_id = str(instance_row[0])

    return host_club_id, event_series_id, event_instance_id


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def write_run_report(
    run_id: str,
    started_at: str,
    mode: str,
    dry_run: bool,
    source_paths: dict[str, str],
    counters: RunCounters,
) -> Path:
    report = {
        "run_id": run_id,
        "mode": mode,
        "started_at": started_at,
        "finished_at": datetime.utcnow().isoformat(),
        "dry_run": dry_run,
        **source_paths,
        "counters": counters.to_dict(),
    }
    report_path = Path(f"./artifacts/reports/{run_id}.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))
    return report_path
