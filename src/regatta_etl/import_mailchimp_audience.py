"""regatta_etl.import_mailchimp_audience

Mailchimp audience contact ingestion pipeline.

Consumes three audience export CSV files:
  - subscribed_email_audience_export_*.csv   (audience_status='subscribed')
  - unsubscribed_email_audience_export_*.csv (audience_status='unsubscribed')
  - cleaned_email_audience_export_*.csv      (audience_status='cleaned')

Produces:
  - mailchimp_audience_row    — lossless raw capture (every source row, including rejects)
  - mailchimp_contact_state   — append-only status history per participant
  - mailchimp_contact_tag     — tag bridge table per participant
  - participant / participant_contact_point / participant_address — curated projection
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import click
import psycopg

from regatta_etl.normalize import (
    normalize_email,
    normalize_name,
    normalize_phone,
    parse_ts,
    trim,
)
from regatta_etl.shared import (
    AmbiguousMatchError,
    RejectWriter,
    RunCounters,
    insert_participant,
    normalize_headers,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "mailchimp_audience_csv"

# Required headers per file type (subset — only those used for validation/parsing)
REQUIRED_HEADERS_SUBSCRIBED = {"Email Address", "LAST_CHANGED"}
REQUIRED_HEADERS_UNSUBSCRIBED = {"Email Address", "UNSUB_TIME"}
REQUIRED_HEADERS_CLEANED = {"Email Address", "CLEAN_TIME"}

FILE_TYPES: dict[str, dict[str, Any]] = {
    "subscribed":   {"required": REQUIRED_HEADERS_SUBSCRIBED,   "status_col": "LAST_CHANGED"},
    "unsubscribed": {"required": REQUIRED_HEADERS_UNSUBSCRIBED,  "status_col": "UNSUB_TIME"},
    "cleaned":      {"required": REQUIRED_HEADERS_CLEANED,       "status_col": "CLEAN_TIME"},
}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_tags(tags_raw: str | None) -> list[str]:
    """Parse Mailchimp TAGS field into a list of tag strings.

    Mailchimp stores tags as a double-quoted, comma-delimited string within the
    CSV field value.  After DictReader unescapes the outer CSV layer the value
    looks like: '"tag one","tag two"'.  We run a second csv.reader pass to split
    and unquote the tags.
    """
    v = trim(tags_raw)
    if not v:
        return []
    reader = csv.reader(io.StringIO(v))
    try:
        tokens = next(reader)
    except StopIteration:
        return []
    return [t.strip() for t in tokens if t.strip()]


def _validate_email_format(email_norm: str | None) -> bool:
    """Return True if the normalized email has the minimal expected shape."""
    if not email_norm:
        return False
    at = email_norm.find("@")
    return at > 0 and at < len(email_norm) - 1 and "." in email_norm[at:]


def _parse_member_rating(value: str | None) -> int | None:
    v = trim(value)
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Participant resolution
# ---------------------------------------------------------------------------

def _resolve_participant_by_email(
    conn: psycopg.Connection,
    email_norm: str,
) -> str | None:
    """Return participant_id for a unique email match, raise on ambiguity."""
    rows = conn.execute(
        """
        SELECT DISTINCT participant_id
        FROM participant_contact_point
        WHERE contact_type = 'email' AND contact_value_normalized = %s
        """,
        (email_norm,),
    ).fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        raise AmbiguousMatchError(f"ambiguous_email_match: email={email_norm!r}")
    return str(rows[0][0])


def _resolve_participant_by_name_strict(
    conn: psycopg.Connection,
    name_norm: str,
) -> str | None:
    """Return participant_id for a unique name match, raise on ambiguity."""
    rows = conn.execute(
        "SELECT id FROM participant WHERE normalized_full_name = %s",
        (name_norm,),
    ).fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        raise AmbiguousMatchError(f"ambiguous_name_match: name_norm={name_norm!r}")
    return str(rows[0][0])


def _build_full_name(first_raw: str | None, last_raw: str | None) -> str | None:
    first = trim(first_raw)
    last = trim(last_raw)
    parts = [p for p in [first, last] if p]
    return " ".join(parts) if parts else None


def _get_participant_corroboration_data(
    conn: psycopg.Connection,
    participant_id: str,
) -> tuple[str | None, str | None, str | None]:
    """Return (normalized_full_name, phone_norm, address_raw) for a participant."""
    name_row = conn.execute(
        "SELECT normalized_full_name FROM participant WHERE id = %s",
        (participant_id,),
    ).fetchone()
    phone_row = conn.execute(
        """
        SELECT contact_value_normalized FROM participant_contact_point
        WHERE participant_id = %s AND contact_type = 'phone' LIMIT 1
        """,
        (participant_id,),
    ).fetchone()
    addr_row = conn.execute(
        "SELECT address_raw FROM participant_address WHERE participant_id = %s LIMIT 1",
        (participant_id,),
    ).fetchone()
    return (
        name_row[0] if name_row else None,
        phone_row[0] if phone_row else None,
        addr_row[0] if addr_row else None,
    )


def _insert_identity_review_queue(
    conn: psycopg.Connection,
    source_file_name: str,
    email_norm: str | None,
    candidate_participant_id: str | None,
    reason_code: str,
    reason_detail: str | None,
    raw_payload: str,
) -> None:
    """Insert a quarantine row into mailchimp_identity_review_queue."""
    conn.execute(
        """
        INSERT INTO mailchimp_identity_review_queue
            (source_file_name, email_normalized, candidate_participant_id,
             reason_code, reason_detail, raw_payload)
        VALUES (%s, %s, %s::uuid, %s, %s, %s::jsonb)
        """,
        (source_file_name, email_norm, candidate_participant_id,
         reason_code, reason_detail, raw_payload),
    )


def _strict_resolve_participant(
    conn: psycopg.Connection,
    row: dict[str, str],
    email_norm: str,
    source_file_name: str,
    audience_status: str,
    raw_payload: str,
    counters: RunCounters,
    rejects: RejectWriter,
) -> str | None:
    """Resolve participant per Policy v2 strict identity rules.

    Returns participant_id on success, or None if the row was quarantined or
    rejected.  On quarantine: inserts a review-queue row, writes to the reject
    file, and increments the quarantine / rejection counters.  On
    name-fallback ambiguous match: writes to reject file only (not a review
    queue case).
    """
    def _quarantine(pid: str | None, reason_code: str, reason_detail: str | None) -> None:
        _insert_identity_review_queue(
            conn, source_file_name, email_norm, pid,
            reason_code, reason_detail, raw_payload,
        )
        rejects.write(
            {**row, "_source_file": source_file_name,
             "_audience_status": audience_status},
            reason_code,
        )
        counters.mailchimp_identity_rows_quarantined += 1
        counters.rows_rejected += 1

    # ------------------------------------------------------------------ A
    # Email lookup
    # ------------------------------------------------------------------ A
    try:
        pid = _resolve_participant_by_email(conn, email_norm)
    except AmbiguousMatchError:
        _quarantine(None, "ambiguous_email_match",
                    f"email {email_norm!r} maps to multiple participants")
        return None

    if pid is not None:
        # -------------------------------------------------------------- B
        # Name corroboration (required when email matched)
        # -------------------------------------------------------------- B
        target_name, target_phone, target_address = _get_participant_corroboration_data(
            conn, pid
        )
        source_first = trim(row.get("First Name"))
        source_last = trim(row.get("Last Name"))
        source_full = _build_full_name(source_first, source_last)
        source_name_norm = normalize_name(source_full) if source_full else None

        if source_name_norm is None or target_name is None:
            _quarantine(
                pid, "missing_name_for_email_match",
                f"source_has_name={source_name_norm is not None}, "
                f"target_has_name={target_name is not None}",
            )
            return None

        if source_name_norm != target_name:
            _quarantine(
                pid, "email_name_mismatch",
                f"source={source_name_norm!r}, target={target_name!r}",
            )
            return None

        # -------------------------------------------------------------- C
        # Optional corroboration: phone
        # -------------------------------------------------------------- C
        source_phone_raw = trim(row.get("Phone Number"))
        if source_phone_raw and target_phone:
            source_phone_norm = normalize_phone(source_phone_raw)
            if source_phone_norm and source_phone_norm != target_phone:
                _quarantine(
                    pid, "email_phone_mismatch",
                    f"source={source_phone_norm!r}, target={target_phone!r}",
                )
                return None

        # -------------------------------------------------------------- D
        # Optional corroboration: address
        # -------------------------------------------------------------- D
        source_address = trim(row.get("Address"))
        if source_address and target_address:
            # Normalize casing and whitespace before comparing to avoid
            # over-quarantining benign formatting differences.
            src_addr_norm = " ".join(source_address.lower().split())
            tgt_addr_norm = " ".join(target_address.lower().split())
            if src_addr_norm != tgt_addr_norm:
                _quarantine(
                    pid, "email_address_mismatch",
                    f"source={source_address!r}, target={target_address!r}",
                )
                return None

        # All checks passed — link to existing participant
        counters.participants_matched_existing += 1
        return pid

    # ------------------------------------------------------------------ E
    # No email match — fall through to name lookup or create new
    # ------------------------------------------------------------------ E
    source_first = trim(row.get("First Name"))
    source_last = trim(row.get("Last Name"))
    full_name = _build_full_name(source_first, source_last)

    if full_name:
        name_norm = normalize_name(full_name)
        if name_norm:
            try:
                name_pid = _resolve_participant_by_name_strict(conn, name_norm)
            except AmbiguousMatchError:
                # Name-only ambiguity: row-level reject only (not a review-queue case)
                rejects.write(
                    {**row, "_source_file": source_file_name,
                     "_audience_status": audience_status},
                    "ambiguous_name_match",
                )
                counters.rows_rejected += 1
                return None
            if name_pid is not None:
                counters.participants_matched_existing += 1
                return name_pid

    display_name = full_name or email_norm  # email as fallback for name-absent rows
    new_pid = insert_participant(conn, display_name)
    counters.participants_inserted += 1
    return new_pid


def _upsert_mailchimp_identity(
    conn: psycopg.Connection,
    participant_id: str,
    email_norm: str,
    leid: str | None,
    euid: str | None,
    source_file_name: str,
    raw_payload: str,
    counters: RunCounters,
) -> None:
    """Upsert participant_mailchimp_identity when LEID or EUID is present.

    Cross-participant conflict behaviour (LEID/EUID already belong to a different
    participant): the conflict is queued for manual review and this function returns
    without writing an identity row.  Curated participant/state/tag writes for the
    current row are NOT blocked — LEID/EUID are auxiliary IDs, not primary identity
    evidence.  The strict corroboration gate (_strict_resolve_participant) already
    governs whether the row may be linked at all; this function only manages the
    auxiliary identity bookkeeping after that gate has passed.

    Same-participant/different-email case (LEID or EUID already captured on a
    different email row for the same participant): the conflicting auxiliary ID is
    suppressed (set to NULL) on the new row to avoid hitting the partial unique
    index.  No conflict is queued.
    """
    if not leid and not euid:
        return

    # Check LEID uniqueness.
    # Three cases when a matching LEID row already exists:
    #   1. Different participant → true cross-participant conflict: queue + abort.
    #   2. Same participant, same email → ON CONFLICT update below handles it.
    #   3. Same participant, different email → LEID is already recorded; suppress
    #      it on this new email's row to avoid hitting the partial unique index.
    if leid:
        existing = conn.execute(
            """
            SELECT participant_id::text, email_normalized
            FROM participant_mailchimp_identity
            WHERE leid = %s
            """,
            (leid,),
        ).fetchone()
        if existing:
            if existing[0] != participant_id:
                # Cross-participant conflict
                _insert_identity_review_queue(
                    conn, source_file_name, email_norm, participant_id,
                    "leid_conflict",
                    f"leid={leid!r} already linked to participant {existing[0]}",
                    raw_payload,
                )
                counters.mailchimp_identity_conflicts += 1
                return
            elif existing[1] != email_norm:
                # Same participant, different email — LEID already captured; don't
                # duplicate it on the new email row (would violate partial unique index).
                leid = None

    # Check EUID uniqueness (same three-case logic as LEID above).
    if euid:
        existing = conn.execute(
            """
            SELECT participant_id::text, email_normalized
            FROM participant_mailchimp_identity
            WHERE euid = %s
            """,
            (euid,),
        ).fetchone()
        if existing:
            if existing[0] != participant_id:
                # Cross-participant conflict
                _insert_identity_review_queue(
                    conn, source_file_name, email_norm, participant_id,
                    "euid_conflict",
                    f"euid={euid!r} already linked to participant {existing[0]}",
                    raw_payload,
                )
                counters.mailchimp_identity_conflicts += 1
                return
            elif existing[1] != email_norm:
                # Same participant, different email — EUID already captured.
                euid = None

    import hashlib as _hashlib
    subscriber_hash = _hashlib.md5(email_norm.encode()).hexdigest()

    result = conn.execute(
        """
        INSERT INTO participant_mailchimp_identity
            (participant_id, leid, euid, subscriber_hash, email_normalized,
             source_system, source_file_name)
        VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (participant_id, email_normalized) DO UPDATE SET
            last_seen_at    = now(),
            leid            = COALESCE(participant_mailchimp_identity.leid, EXCLUDED.leid),
            euid            = COALESCE(participant_mailchimp_identity.euid, EXCLUDED.euid),
            subscriber_hash = COALESCE(participant_mailchimp_identity.subscriber_hash,
                                       EXCLUDED.subscriber_hash)
        RETURNING (xmax = 0) AS was_inserted
        """,
        (participant_id, leid, euid, subscriber_hash, email_norm,
         SOURCE_SYSTEM, source_file_name),
    ).fetchone()
    if result and result[0]:
        counters.mailchimp_identity_links_inserted += 1
    elif result:
        counters.mailchimp_identity_links_updated += 1


# ---------------------------------------------------------------------------
# Curated projection helpers
# ---------------------------------------------------------------------------

def _upsert_email_contact_point(
    conn: psycopg.Connection,
    participant_id: str,
    email_raw: str,
    email_norm: str,
    counters: RunCounters,
) -> None:
    existing = conn.execute(
        """
        SELECT id FROM participant_contact_point
        WHERE participant_id = %s
          AND contact_type = 'email'
          AND contact_value_normalized = %s
        """,
        (participant_id, email_norm),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO participant_contact_point
          (participant_id, contact_type, contact_subtype,
           contact_value_raw, contact_value_normalized, is_primary, source_system)
        VALUES (%s, 'email', 'primary', %s, %s, true, %s)
        """,
        (participant_id, email_raw, email_norm, SOURCE_SYSTEM),
    )
    counters.contact_points_inserted += 1


def _upsert_phone_contact_point(
    conn: psycopg.Connection,
    participant_id: str,
    phone_raw: str,
    phone_norm: str,
    counters: RunCounters,
) -> None:
    existing = conn.execute(
        """
        SELECT id FROM participant_contact_point
        WHERE participant_id = %s
          AND contact_type = 'phone'
          AND contact_value_normalized = %s
        """,
        (participant_id, phone_norm),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO participant_contact_point
          (participant_id, contact_type, contact_subtype,
           contact_value_raw, contact_value_normalized, is_primary, source_system)
        VALUES (%s, 'phone', 'primary', %s, %s, false, %s)
        """,
        (participant_id, phone_raw, phone_norm, SOURCE_SYSTEM),
    )
    counters.contact_points_inserted += 1


def _upsert_address(
    conn: psycopg.Connection,
    participant_id: str,
    address_raw: str,
    counters: RunCounters,
) -> None:
    existing = conn.execute(
        """
        SELECT id FROM participant_address
        WHERE participant_id = %s AND address_raw = %s
        """,
        (participant_id, address_raw),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO participant_address
          (participant_id, address_type, address_raw, is_primary, source_system)
        VALUES (%s, 'mailing', %s, true, %s)
        """,
        (participant_id, address_raw, SOURCE_SYSTEM),
    )
    counters.addresses_inserted += 1


def _insert_mailchimp_state(
    conn: psycopg.Connection,
    participant_id: str,
    email_norm: str,
    audience_status: str,
    row: dict[str, str],
    status_at: datetime | None,
    source_file_name: str,
    row_hash: str,
    counters: RunCounters,
) -> None:
    result = conn.execute(
        """
        INSERT INTO mailchimp_contact_state (
            participant_id, email_normalized, audience_status, status_at,
            last_changed, optin_time, confirm_time, optin_ip, confirm_ip,
            unsub_campaign_title, unsub_campaign_id, unsub_reason, unsub_reason_other,
            clean_campaign_title, clean_campaign_id,
            leid, euid, member_rating, gmtoff, dstoff, timezone, cc, region, notes,
            source_file_name, row_hash
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (participant_id, source_file_name, row_hash) DO NOTHING
        RETURNING id
        """,
        (
            participant_id, email_norm, audience_status, status_at,
            # subscribe-specific
            parse_ts(row.get("LAST_CHANGED")),
            parse_ts(row.get("OPTIN_TIME")),
            parse_ts(row.get("CONFIRM_TIME")),
            trim(row.get("OPTIN_IP")),
            trim(row.get("CONFIRM_IP")),
            # unsubscribe-specific
            trim(row.get("UNSUB_CAMPAIGN_TITLE")),
            trim(row.get("UNSUB_CAMPAIGN_ID")),
            trim(row.get("UNSUB_REASON")),
            trim(row.get("UNSUB_REASON_OTHER")),
            # clean-specific
            trim(row.get("CLEAN_CAMPAIGN_TITLE")),
            trim(row.get("CLEAN_CAMPAIGN_ID")),
            # shared
            trim(row.get("LEID")),
            trim(row.get("EUID")),
            _parse_member_rating(row.get("MEMBER_RATING")),
            trim(row.get("GMTOFF")),
            trim(row.get("DSTOFF")),
            trim(row.get("TIMEZONE")),
            trim(row.get("CC")),
            trim(row.get("REGION")),
            trim(row.get("NOTES")),
            source_file_name, row_hash,
        ),
    ).fetchone()
    if result:
        counters.mailchimp_status_rows_inserted += 1


def _insert_mailchimp_tags(
    conn: psycopg.Connection,
    participant_id: str,
    email_norm: str,
    tags: list[str],
    source_file_name: str,
    observed_at: datetime | None,
    counters: RunCounters,
) -> None:
    for tag in tags:
        result = conn.execute(
            """
            INSERT INTO mailchimp_contact_tag
              (participant_id, email_normalized, tag_value, source_file_name, observed_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (participant_id, email_normalized, tag_value, source_file_name)
            DO NOTHING
            RETURNING id
            """,
            (participant_id, email_norm, tag, source_file_name, observed_at),
        ).fetchone()
        if result:
            counters.mailchimp_tags_inserted += 1


# ---------------------------------------------------------------------------
# Per-row processing
# ---------------------------------------------------------------------------

def _process_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    audience_status: str,
    status_col: str,
    source_file_name: str,
    counters: RunCounters,
    rejects: RejectWriter,
) -> None:
    """Process one row.  Caller manages savepoint.

    Row ordering:
      1. Compute row_hash + raw_payload.
      2. Lossless raw capture — always, even for rows that will be rejected.
      3. Email presence + format validation (reject if invalid).
      4. Status datetime validation (reject if present but unparseable).
      5. Strict identity resolution (Policy v2): quarantine on mismatch.
      6. Upsert participant_mailchimp_identity if LEID/EUID present.
      7-11. Contact points, address, state, tags.
    """
    counters.rows_read += 1

    # Step 1: compute row hash + payload before any validation
    raw_payload = json.dumps(row, ensure_ascii=False)
    row_hash = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

    # Step 2: lossless raw capture — always, before any early-return reject
    email_raw = trim(row.get("Email Address"))
    email_norm = normalize_email(email_raw) if email_raw else None

    raw_result = conn.execute(
        """
        INSERT INTO mailchimp_audience_row
          (source_file_name, audience_status,
           source_email_raw, source_email_normalized, row_hash, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (source_system, source_file_name, row_hash) DO NOTHING
        RETURNING id
        """,
        (source_file_name, audience_status, email_raw, email_norm,
         row_hash, raw_payload),
    ).fetchone()
    if raw_result:
        counters.raw_rows_inserted += 1

    # Step 3: email validation (after raw capture so the row is always preserved)
    if not email_raw:
        rejects.write(
            {**row, "_source_file": source_file_name, "_audience_status": audience_status},
            "missing_email_address",
        )
        counters.rows_rejected += 1
        return

    if not _validate_email_format(email_norm):
        rejects.write(
            {**row, "_source_file": source_file_name, "_audience_status": audience_status},
            "malformed_email",
        )
        counters.rows_rejected += 1
        return

    # Step 4: status datetime validation
    status_at_raw = trim(row.get(status_col))
    status_at = parse_ts(status_at_raw)
    if status_at_raw is not None and status_at is None:
        # Field is present but did not parse — reject
        rejects.write(
            {**row, "_source_file": source_file_name, "_audience_status": audience_status},
            "unparseable_status_datetime",
        )
        counters.rows_rejected += 1
        return

    # Step 5: strict identity resolution per Policy v2
    # On quarantine: review queue row inserted + reject written + counter incremented;
    # returns None so we skip curated writes.
    participant_id = _strict_resolve_participant(
        conn, row,
        email_norm,  # type: ignore[arg-type]  # validated non-null above
        source_file_name, audience_status, raw_payload, counters, rejects,
    )
    if participant_id is None:
        return  # quarantined or ambiguous-name reject; raw capture persists

    # Step 6: upsert identity link when LEID or EUID is present
    leid = trim(row.get("LEID"))
    euid = trim(row.get("EUID"))
    if leid or euid:
        _upsert_mailchimp_identity(
            conn, participant_id, email_norm,  # type: ignore[arg-type]
            leid, euid, source_file_name, raw_payload, counters,
        )

    # Step 7: contact point — email
    _upsert_email_contact_point(conn, participant_id, email_raw, email_norm, counters)  # type: ignore[arg-type]

    # Step 8: contact point — phone (optional)
    phone_raw = trim(row.get("Phone Number"))
    if phone_raw:
        phone_norm = normalize_phone(phone_raw)
        if phone_norm:
            _upsert_phone_contact_point(conn, participant_id, phone_raw, phone_norm, counters)

    # Step 9: address (raw only, optional)
    address_raw = trim(row.get("Address"))
    if address_raw:
        _upsert_address(conn, participant_id, address_raw, counters)

    # Step 10: mailchimp_contact_state (append-only, idempotent by row_hash)
    _insert_mailchimp_state(
        conn, participant_id, email_norm, audience_status,  # type: ignore[arg-type]
        row, status_at, source_file_name, row_hash, counters,
    )

    # Step 11: tags
    tags = parse_tags(row.get("TAGS"))
    if tags:
        _insert_mailchimp_tags(
            conn, participant_id, email_norm, tags,  # type: ignore[arg-type]
            source_file_name, status_at, counters,
        )

    counters.curated_rows_processed += 1


# ---------------------------------------------------------------------------
# Header-only pre-scan (no row loading)
# ---------------------------------------------------------------------------

def _validate_file_headers(
    csv_path: Path,
    audience_status: str,
    run_id: str,
) -> None:
    """Open the file just far enough to read the header row and validate it."""
    required = FILE_TYPES[audience_status]["required"]
    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        _ = reader.fieldnames  # advance to header line
        header_set = {k.strip() for k in (reader.fieldnames or [])}
        missing = required - header_set
        if missing:
            click.echo(
                f"[{run_id}] FATAL: {csv_path.name} missing required headers: "
                f"{sorted(missing)}",
                err=True,
            )
            sys.exit(1)


# ---------------------------------------------------------------------------
# Streaming DB processing for one file
# ---------------------------------------------------------------------------

def _stream_process_file(
    conn: psycopg.Connection,
    csv_path: Path,
    audience_status: str,
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
) -> None:
    """Open file and stream rows directly into the DB phase with per-row savepoints.

    The file is not loaded into memory — rows are processed one at a time.
    """
    status_col = FILE_TYPES[audience_status]["status_col"]
    source_file_name = csv_path.name

    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for idx, raw_row in enumerate(reader):
            row = normalize_headers(raw_row)
            sp_name = f"{audience_status}_{idx}"
            conn.execute(f"SAVEPOINT {sp_name}")
            try:
                _process_row(
                    conn, row, audience_status, status_col,
                    source_file_name, counters, rejects,
                )
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            except AmbiguousMatchError as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                rejects.write(
                    {**row, "_source_file": source_file_name,
                     "_audience_status": audience_status},
                    str(exc),
                )
                counters.rows_rejected += 1
                # AmbiguousMatchError is a row-level reject, not a DB error
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                rejects.write(
                    {**row, "_source_file": source_file_name,
                     "_audience_status": audience_status},
                    "db_constraint_error",
                )
                counters.warnings.append(
                    f"[{run_id}] {audience_status} row {idx} "
                    f"{type(exc).__name__}: {exc}"
                )
                counters.rows_rejected += 1
                counters.db_phase_errors += 1


# ---------------------------------------------------------------------------
# Main run entry point
# ---------------------------------------------------------------------------

def _run_mailchimp_audience(
    run_id: str,
    started_at: str,
    db_dsn: str,
    counters: RunCounters,
    rejects: RejectWriter,
    subscribed_path: str,
    unsubscribed_path: str,
    cleaned_path: str,
    max_reject_rate: float,
    dry_run: bool,
) -> None:
    files = [
        (Path(subscribed_path),   "subscribed"),
        (Path(unsubscribed_path), "unsubscribed"),
        (Path(cleaned_path),      "cleaned"),
    ]

    # Pre-scan: validate headers only; no rows loaded into memory
    for csv_path, audience_status in files:
        _validate_file_headers(csv_path, audience_status, run_id)

    click.echo(f"[{run_id}] Header validation passed for all 3 files — starting DB phase")

    # DB phase: stream rows from each file
    conn = psycopg.connect(db_dsn, autocommit=False)
    try:
        if dry_run:
            try:
                for csv_path, audience_status in files:
                    _stream_process_file(
                        conn, csv_path, audience_status,
                        run_id, counters, rejects,
                    )
            finally:
                conn.rollback()
                click.echo(f"[{run_id}] [dry-run] All changes rolled back.")

            if counters.db_phase_errors > 0:
                click.echo(
                    f"[{run_id}] [dry-run] {counters.db_phase_errors} DB error(s) — "
                    "exiting non-zero",
                    err=True,
                )
                sys.exit(1)

            total_read = counters.rows_read
            if total_read > 0:
                reject_rate = counters.rows_rejected / total_read
                if reject_rate > max_reject_rate:
                    click.echo(
                        f"[{run_id}] [dry-run] reject rate {reject_rate:.2%} exceeds "
                        f"threshold {max_reject_rate:.2%} — exiting non-zero",
                        err=True,
                    )
                    sys.exit(1)
        else:
            try:
                for csv_path, audience_status in files:
                    _stream_process_file(
                        conn, csv_path, audience_status,
                        run_id, counters, rejects,
                    )
            except Exception as exc:
                conn.rollback()
                click.echo(
                    f"[{run_id}] FATAL: unexpected error during DB phase: {exc}",
                    err=True,
                )
                sys.exit(1)

            if counters.db_phase_errors > 0:
                conn.rollback()
                click.echo(
                    f"[{run_id}] FATAL: {counters.db_phase_errors} DB error(s); "
                    "rolling back",
                    err=True,
                )
                sys.exit(1)

            total_read = counters.rows_read
            if total_read > 0:
                reject_rate = counters.rows_rejected / total_read
                if reject_rate > max_reject_rate:
                    conn.rollback()
                    click.echo(
                        f"[{run_id}] FATAL: reject rate {reject_rate:.2%} exceeds "
                        f"threshold {max_reject_rate:.2%}; rolling back",
                        err=True,
                    )
                    sys.exit(1)

            conn.commit()

    finally:
        conn.close()
        rejects.close()

    click.echo(
        f"[{run_id}] Done: {counters.rows_read} rows read, "
        f"{counters.rows_rejected} rejected, "
        f"{counters.mailchimp_identity_rows_quarantined} identity-quarantined, "
        f"{counters.mailchimp_identity_conflicts} identity-conflicts, "
        f"{counters.participants_inserted} participants inserted, "
        f"{counters.participants_matched_existing} matched, "
        f"{counters.mailchimp_identity_links_inserted} identity-links-inserted, "
        f"{counters.mailchimp_identity_links_updated} identity-links-updated, "
        f"{counters.mailchimp_status_rows_inserted} state rows inserted, "
        f"{counters.mailchimp_tags_inserted} tags inserted"
    )
