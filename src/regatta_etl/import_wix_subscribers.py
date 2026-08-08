"""Wix website-subscriber ingestion (FOR-886).

Ingests a Wix contacts CSV export (website subscribers) into Neon with:
  1. Lossless raw capture into `wix_subscriber_row` (all columns, idempotent).
  2. Reconcile-by-email into the canonical participant layer:
       - email already known  -> link (add a `wix` provenance contact point).
       - email net-new         -> create a participant + `wix` email contact point.
     Names are taken from First/Last only. Wix is email-only for most rows, and
     an email is NEVER stored as a participant name (avoids the email-as-name
     defect that FOR-884 cleaned up).
  3. Subscriber status (Subscribed / Never subscribed) captured on the raw row.

Idempotent: re-running against the same export adds no duplicate participants or
contact points (email-keyed reconcile + raw row_hash uniqueness). Reconciles
cleanly with the one-time SQL insert that seeded Wix data (source_system='wix').

Reuses the shared ingestion architecture (shared RunCounters/RejectWriter, no
standalone pipeline).
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

import click
import psycopg

from regatta_etl.normalize import (
    looks_like_email,
    normalize_email,
    normalize_name,
    normalize_space,
    trim,
)
from regatta_etl.shared import RejectWriter, RunCounters, normalize_headers

REQUIRED_HEADERS = {"Email 1", "Email subscriber status"}

SOURCE_SYSTEM = "wix"


def _resolve_participant_by_email(conn: psycopg.Connection, email_norm: str) -> str | None:
    """Return the participant id owning this email, preferring a real-named record
    over an email-as-name/blank one (so Wix links to the good twin, not the placeholder)."""
    row = conn.execute(
        """
        SELECT c.participant_id
        FROM participant_contact_point c
        JOIN participant p ON p.id = c.participant_id
        WHERE c.contact_type = 'email'
          AND lower(c.contact_value_normalized) = %s
        ORDER BY (p.full_name NOT LIKE '%%@%%' AND nullif(trim(p.full_name), '') IS NOT NULL) DESC,
                 p.updated_at DESC
        LIMIT 1
        """,
        (email_norm,),
    ).fetchone()
    return str(row[0]) if row else None


def _insert_participant_no_email_name(
    conn: psycopg.Connection, first: str | None, last: str | None
) -> str:
    """Create a participant from First/Last only. Never stores an email as a name."""
    full_name = normalize_space(" ".join(p for p in [first, last] if p)) or ""
    if full_name and looks_like_email(full_name):
        full_name = ""  # guard: refuse email-as-name
    normalized = normalize_name(full_name) if full_name else ""
    row = conn.execute(
        """
        INSERT INTO participant (full_name, normalized_full_name, first_name, last_name)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (full_name, normalized or "", first or None, last or None),
    ).fetchone()
    return str(row[0])


def _add_wix_contact_point(
    conn: psycopg.Connection,
    participant_id: str,
    email_raw: str,
    email_norm: str,
    is_primary: bool,
    subtype: str,
) -> bool:
    """Add a wix-sourced email contact point if this participant has no wix row for
    this email yet. Returns True if inserted."""
    existing = conn.execute(
        """
        SELECT 1 FROM participant_contact_point
        WHERE participant_id = %s AND contact_type = 'email'
          AND lower(contact_value_normalized) = %s AND source_system = %s
        LIMIT 1
        """,
        (participant_id, email_norm, SOURCE_SYSTEM),
    ).fetchone()
    if existing:
        return False
    conn.execute(
        """
        INSERT INTO participant_contact_point
            (participant_id, contact_type, contact_subtype, contact_value_raw,
             contact_value_normalized, is_primary, source_system)
        VALUES (%s, 'email', %s, %s, %s, %s, %s)
        """,
        (participant_id, subtype, email_raw, email_norm, is_primary, SOURCE_SYSTEM),
    )
    return True


def _run_wix_subscribers(
    run_id: str,
    started_at: str,
    db_dsn: str,
    counters: RunCounters,
    rejects: RejectWriter,
    csv_path: str,
    dry_run: bool,
) -> None:
    csv_file = Path(csv_path)
    csv_file_name = csv_file.name

    with open(csv_file, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        raw_fieldnames = reader.fieldnames or []
        normalized_hdr = {k.strip(): k for k in raw_fieldnames}
        missing = REQUIRED_HEADERS - set(normalized_hdr.keys())
        if missing:
            click.echo(
                f"[{run_id}] FATAL: wix export missing required headers: {sorted(missing)}",
                err=True,
            )
            sys.exit(1)
        rows = list(reader)

    click.echo(f"[{run_id}] Pre-scan: {len(rows)} rows read")

    status_subscribed = status_never = status_other = provenance_points = 0

    with psycopg.connect(db_dsn, autocommit=False) as conn:
        for raw in rows:
            row = normalize_headers(raw)
            counters.rows_read += 1

            email_raw = trim(row.get("Email 1"))
            email_norm = normalize_email(email_raw) if email_raw else None
            if not email_norm:
                rejects.write(row, "missing_or_invalid_email")
                counters.rows_rejected += 1
                continue

            status = (trim(row.get("Email subscriber status")) or "").lower()
            if status == "subscribed":
                status_subscribed += 1
            elif status.startswith("never"):
                status_never += 1
            else:
                status_other += 1

            # 1. Lossless raw capture
            raw_payload = json.dumps(raw)
            row_hash = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()
            inserted_raw = conn.execute(
                """
                INSERT INTO wix_subscriber_row
                    (source_file_name, source_email_raw, source_email_normalized,
                     subscriber_status, labels, wix_source, created_at_raw, language,
                     raw_payload, row_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_system, source_email_normalized, row_hash) DO NOTHING
                RETURNING id
                """,
                (
                    csv_file_name, email_raw, email_norm,
                    trim(row.get("Email subscriber status")),
                    trim(row.get("Labels")), trim(row.get("Source")),
                    trim(row.get("Created At (UTC+0)")), trim(row.get("Language")),
                    raw_payload, row_hash,
                ),
            ).fetchone()
            if inserted_raw:
                counters.raw_rows_inserted += 1

            # 2. Reconcile-by-email into the participant layer
            participant_id = _resolve_participant_by_email(conn, email_norm)
            if participant_id:
                counters.participants_matched_existing += 1
                if _add_wix_contact_point(
                    conn, participant_id, email_raw, email_norm,
                    is_primary=False, subtype="wix_provenance",
                ):
                    provenance_points += 1
            else:
                participant_id = _insert_participant_no_email_name(
                    conn, trim(row.get("First Name")) or None, trim(row.get("Last Name")) or None
                )
                counters.participants_inserted += 1
                if _add_wix_contact_point(
                    conn, participant_id, email_raw, email_norm,
                    is_primary=True, subtype="primary",
                ):
                    counters.contact_points_inserted += 1

        if dry_run:
            conn.rollback()
            click.echo(f"[{run_id}] [dry-run] All changes rolled back.")
        else:
            conn.commit()
            click.echo(f"[{run_id}] Committed.")

    counters.warnings.append(
        f"wix: subscribed={status_subscribed}, never={status_never}, "
        f"other_status={status_other}, provenance_points={provenance_points}"
    )
    click.echo(
        f"[{run_id}] wix_subscribers: raw={counters.raw_rows_inserted}, "
        f"new_participants={counters.participants_inserted}, "
        f"matched={counters.participants_matched_existing}, "
        f"primary_emails={counters.contact_points_inserted}, provenance={provenance_points}, "
        f"status[subscribed={status_subscribed} never={status_never}]"
    )
