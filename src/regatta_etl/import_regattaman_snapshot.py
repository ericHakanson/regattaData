"""regatta_etl.import_regattaman_snapshot

Regattaman owner+yacht snapshot ingestion pipeline (--mode regattaman_snapshot).

Source file: finalMergedAllRegattaman - unique.csv
Columns: nameLower, yachtNameLower, boatTypeLower, Sail, Rating

Two-layer model:
  Raw layer:     append-only lossless capture (regattaman_snapshot_row).
                 Every source row is captured before curated validation.
  Curated layer: enrichment-only projection into participant, yacht,
                 yacht_ownership, and yacht_rating tables.

Rating constants (fixed for this source):
  RATING_SYSTEM   = 'PHRF'
  RATING_CATEGORY = 'phrf_main'

  'phrf_main' denotes the main PHRF time-on-distance offshore rating for
  this fleet. It is a single fixed category for all rows in this snapshot
  since the source does not distinguish PHRF sub-categories.

Per-row processing:
  1. Raw capture (SAVEPOINT 1): store raw payload, always persists.
  2. Curated projection (SAVEPOINT 2):
     a. Parse nameLower → split on '&' / ' and ' → one or more owner names.
     b. For each owner: resolve participant by normalized name; insert if absent.
        First owner gets role='owner'; additional owners get role='co_owner'.
     c. Resolve yacht by (normalized_name, normalized_sail_number);
        insert if absent; fill-null model if yacht exists without one.
     d. Upsert yacht_ownership for each owner ↔ yacht pair.
     e. Write PHRF rating to yacht_rating if Rating is non-blank and parses
        as an integer. 0 is a valid PHRF rating and is written.

Name-only participant resolution (no email/phone in source):
  normalize_name(owner_name) → lookup; insert on miss.

Effective start date for ownership rows:
  Provided via --effective-start (YYYY-MM-DD); defaults to the load date
  (CURRENT_DATE) if not specified. Providing an explicit date makes the
  operational load reproducible and auditable.

Idempotency:
  Re-running the same file inserts no duplicate raw rows (row_hash unique).
  Re-running the same file performs fill-null-only enrichment on curated
  entities. PHRF rating is upserted (UPDATE if value changes, INSERT if new).
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path

import click
import psycopg

from regatta_etl.normalize import (
    normalize_name,
    normalize_space,
    slug_name,
    trim,
)
from regatta_etl.shared import (
    AmbiguousMatchError,
    RejectWriter,
    RunCounters,
    insert_participant,
    normalize_headers,
    resolve_participant_by_name,
    upsert_ownership,
    write_run_report,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "regattaman_snapshot_csv"

REQUIRED_HEADERS = {"nameLower", "yachtNameLower", "boatTypeLower", "Sail", "Rating"}

# PHRF rating identifiers — fixed for this source.
# 'phrf_main' is the single PHRF time-on-distance offshore rating category;
# the source CSV does not distinguish sub-categories.
RATING_SYSTEM = "PHRF"
RATING_CATEGORY = "phrf_main"

# Pattern for splitting co-owners: '&' with optional spaces, or ' and ' (case-insensitive)
_CO_OWNER_SPLIT_RE = re.compile(r"\s*&\s*|\s+and\s+", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Header validation
# ---------------------------------------------------------------------------

def _validate_headers(csv_path: Path, run_id: str) -> None:
    """Exit non-zero if required headers are missing."""
    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        actual = set(reader.fieldnames or [])
    missing = REQUIRED_HEADERS - {h.strip() for h in actual}
    if missing:
        click.echo(
            f"[{run_id}] FATAL: {csv_path.name} missing required headers: {missing}",
            err=True,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Owner name splitting
# ---------------------------------------------------------------------------

def _split_owner_names(name_lower: str) -> list[str]:
    """Split a potentially compound owner name on '&' or ' and ' (case-insensitive).

    'cagle, nate'               → ['cagle, nate']
    'clarkson, deanna & toby'   → ['clarkson, deanna', 'toby']
    'baker, chip and parker'    → ['baker, chip', 'parker']
    'smith, alice AND jones'    → ['smith, alice', 'jones']

    Each token is stripped; blank tokens are discarded.
    """
    parts = _CO_OWNER_SPLIT_RE.split(name_lower)
    return [p.strip() for p in parts if p.strip()]


# ---------------------------------------------------------------------------
# Rating parsing
# ---------------------------------------------------------------------------

def _parse_rating(raw: str | None) -> int | None:
    """Parse Rating field.

    Returns:
        int — parsed value (may be negative; may be 0)
        None — blank, '-', or non-numeric (caller decides how to handle 0)
    """
    v = trim(raw)
    if not v or v == "-":
        return None
    try:
        return int(v)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Raw capture helpers
# ---------------------------------------------------------------------------

def _row_primary_id(name_lower: str, yacht_name_lower: str, sail: str) -> str:
    """Stable SHA-256 identifier for a snapshot row (no natural ID in source)."""
    key = f"{name_lower}|{yacht_name_lower}|{sail}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _row_hash(payload_str: str) -> str:
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()


def _insert_raw_row(
    conn: psycopg.Connection,
    source_file_name: str,
    ordinal: int,
    source_primary_id: str,
    row_hash: str,
    raw_payload_str: str,
    counters: RunCounters,
) -> None:
    cur = conn.execute(
        """
        INSERT INTO regattaman_snapshot_row
          (source_file_name, source_row_ordinal, source_primary_id, row_hash, raw_payload)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source_system, source_file_name, row_hash) DO NOTHING
        """,
        (source_file_name, ordinal, source_primary_id, row_hash, raw_payload_str),
    )
    # rowcount == 0 → conflict/duplicate (idempotent skip); 1 → actually inserted
    counters.regattaman_snapshot_rows_raw_inserted += max(0, cur.rowcount)


# ---------------------------------------------------------------------------
# Xref helpers
# ---------------------------------------------------------------------------

def _xref_lookup_participant(
    conn: psycopg.Connection,
    source_primary_id: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT participant_id FROM regattaman_snapshot_xref_participant
        WHERE source_system = %s AND source_primary_id = %s
        """,
        (SOURCE_SYSTEM, source_primary_id),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_participant(
    conn: psycopg.Connection,
    source_primary_id: str,
    participant_id: str,
    is_new: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO regattaman_snapshot_xref_participant
          (source_system, source_primary_id, participant_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_system, source_primary_id)
          DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, source_primary_id, participant_id),
    )
    if is_new:
        counters.regattaman_snapshot_xref_inserted += 1


def _xref_lookup_yacht(
    conn: psycopg.Connection,
    source_primary_id: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT yacht_id FROM regattaman_snapshot_xref_yacht
        WHERE source_system = %s AND source_primary_id = %s
        """,
        (SOURCE_SYSTEM, source_primary_id),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_yacht(
    conn: psycopg.Connection,
    source_primary_id: str,
    yacht_id: str,
    is_new: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO regattaman_snapshot_xref_yacht
          (source_system, source_primary_id, yacht_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_system, source_primary_id)
          DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, source_primary_id, yacht_id),
    )
    if is_new:
        counters.regattaman_snapshot_xref_inserted += 1


# ---------------------------------------------------------------------------
# Participant resolution
# ---------------------------------------------------------------------------

def _resolve_or_insert_participant(
    conn: psycopg.Connection,
    owner_name: str,
    counters: RunCounters,
) -> str:
    """Resolve participant by normalized name; insert if absent.

    Name-only resolution — no email/phone in snapshot source.
    Raises AmbiguousMatchError if normalized name matches multiple participants.
    """
    norm = normalize_name(owner_name)
    if norm:
        rows = conn.execute(
            "SELECT id FROM participant WHERE normalized_full_name = %s",
            (norm,),
        ).fetchall()
        if len(rows) > 1:
            raise AmbiguousMatchError(
                f"ambiguous_participant_match: name={norm!r} ({len(rows)} matches)"
            )
        if len(rows) == 1:
            counters.participants_matched_existing += 1
            return str(rows[0][0])
    pid = insert_participant(conn, owner_name)
    counters.participants_inserted += 1
    return pid


# ---------------------------------------------------------------------------
# Yacht resolution
# ---------------------------------------------------------------------------

def _normalize_sail(sail_raw: str | None) -> str | None:
    """Normalize sail number: trim whitespace; return None if blank."""
    v = trim(sail_raw)
    if not v:
        return None
    # Remove leading zeros for normalization; keep as string
    return v.lstrip("0") or v  # keep "0" if all zeros


def _resolve_or_insert_yacht(
    conn: psycopg.Connection,
    yacht_name: str,
    sail_raw: str | None,
    model: str | None,
    counters: RunCounters,
) -> str:
    """Resolve yacht by (normalized_name, normalized_sail_number) or name alone.

    Sail present:
      - Exact (normalized_name, normalized_sail_number) match.
      - >1 match → AmbiguousMatchError.
      - 0 matches → INSERT new yacht.

    Sail absent:
      - Name-only match.
      - >1 match → AmbiguousMatchError.
      - 0 matches → INSERT new yacht.

    Fill-null enrichment: if yacht exists without model and we have one, fill it.
    """
    norm_name = slug_name(yacht_name)
    sail_norm = _normalize_sail(sail_raw)

    if sail_norm:
        rows = conn.execute(
            """
            SELECT id, model FROM yacht
            WHERE normalized_name = %s
              AND normalized_sail_number = %s
            """,
            (norm_name, sail_norm),
        ).fetchall()
        if len(rows) > 1:
            raise AmbiguousMatchError(
                f"ambiguous_yacht_match: name={norm_name!r} sail={sail_norm!r} "
                f"({len(rows)} matches)"
            )
        if len(rows) == 1:
            yacht_id = str(rows[0][0])
            existing_model = rows[0][1]
            if model and not existing_model:
                conn.execute(
                    "UPDATE yacht SET model = %s WHERE id = %s",
                    (model, yacht_id),
                )
            return yacht_id
    else:
        rows = conn.execute(
            "SELECT id, model FROM yacht WHERE normalized_name = %s",
            (norm_name,),
        ).fetchall()
        if len(rows) > 1:
            raise AmbiguousMatchError(
                f"ambiguous_yacht_match: name={norm_name!r} (no sail, {len(rows)} matches)"
            )
        if len(rows) == 1:
            yacht_id = str(rows[0][0])
            existing_model = rows[0][1]
            if model and not existing_model:
                conn.execute(
                    "UPDATE yacht SET model = %s WHERE id = %s",
                    (model, yacht_id),
                )
            return yacht_id

    # INSERT new yacht
    new_row = conn.execute(
        """
        INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number, model)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (yacht_name, norm_name, sail_raw if sail_norm else None, sail_norm, model),
    ).fetchone()
    counters.yachts_inserted += 1
    return str(new_row[0])


# ---------------------------------------------------------------------------
# PHRF rating upsert
# ---------------------------------------------------------------------------

def _upsert_phrf_rating(
    conn: psycopg.Connection,
    yacht_id: str,
    rating_value: int,
    counters: RunCounters,
) -> None:
    """Upsert a PHRF/phrf_main rating with NULL effective_start.

    Uses the partial unique index idx_yacht_rating_unique_null_start
    (yacht_id, rating_system, rating_category) WHERE effective_start IS NULL.
    """
    conn.execute(
        """
        INSERT INTO yacht_rating
          (yacht_id, rating_system, rating_category, rating_value, effective_start, source_system)
        VALUES (%s, %s, %s, %s, NULL, %s)
        ON CONFLICT (yacht_id, rating_system, rating_category)
          WHERE effective_start IS NULL
          DO UPDATE SET
            rating_value = EXCLUDED.rating_value,
            updated_at   = now()
        """,
        (yacht_id, RATING_SYSTEM, RATING_CATEGORY, str(rating_value), SOURCE_SYSTEM),
    )
    counters.regattaman_snapshot_ratings_written += 1


# ---------------------------------------------------------------------------
# Curated row processor
# ---------------------------------------------------------------------------

def _process_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_name: str,
    ordinal: int,
    row_primary_id: str,
    effective_start: date | None,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    """Project one snapshot row into curated domain tables."""
    name_lower = trim(row.get("nameLower"))
    yacht_name_lower = trim(row.get("yachtNameLower"))
    boat_type_lower = trim(row.get("boatTypeLower"))
    sail_raw = trim(row.get("Sail"))
    rating_raw = trim(row.get("Rating"))

    # ── Validate required fields ──
    if not name_lower:
        rejects.write(row, "missing_required_column:nameLower")
        counters.rows_rejected += 1
        counters.regattaman_snapshot_rows_curated_rejected += 1
        return
    if not yacht_name_lower:
        rejects.write(row, "missing_required_column:yachtNameLower")
        counters.rows_rejected += 1
        counters.regattaman_snapshot_rows_curated_rejected += 1
        return

    # Reject pure-punctuation yacht names (slug_name → empty string)
    if not slug_name(yacht_name_lower):
        rejects.write(row, "invalid_yacht_name")
        counters.rows_rejected += 1
        counters.regattaman_snapshot_rows_curated_rejected += 1
        return

    # ── Parse owner names ──
    owner_names = _split_owner_names(name_lower)
    if not owner_names:
        rejects.write(row, "missing_required_column:nameLower")
        counters.rows_rejected += 1
        counters.regattaman_snapshot_rows_curated_rejected += 1
        return

    # Validate at least the first owner has a resolvable normalized name
    first_norm = normalize_name(owner_names[0])
    if not first_norm:
        rejects.write(row, "missing_required_column:nameLower")
        counters.rows_rejected += 1
        counters.regattaman_snapshot_rows_curated_rejected += 1
        return

    # ── Parse rating ──
    rating_int = _parse_rating(rating_raw)
    if rating_int is None and rating_raw and rating_raw not in ("-", ""):
        counters.regattaman_snapshot_ratings_skipped_invalid += 1
        counters.warnings.append(
            f"[{run_id}] row {ordinal}: unparseable Rating={rating_raw!r} — skipping"
        )

    # ── Model: title-case the boatTypeLower for storage ──
    model = normalize_space(boat_type_lower) if boat_type_lower else None

    # ── Resolve / insert yacht (xref lookup first) ──
    yacht_xref_id = _xref_lookup_yacht(conn, row_primary_id)
    if yacht_xref_id:
        yacht_id = yacht_xref_id
        # Still apply fill-null model enrichment on re-run
        if model:
            conn.execute(
                "UPDATE yacht SET model = COALESCE(model, %s) WHERE id = %s",
                (model, yacht_id),
            )
    else:
        yacht_id = _resolve_or_insert_yacht(conn, yacht_name_lower, sail_raw, model, counters)
        _xref_upsert_yacht(conn, row_primary_id, yacht_id, True, counters)

    # ── Resolve / insert each owner and upsert ownership ──
    for owner_idx, owner_name in enumerate(owner_names):
        owner_norm = normalize_name(owner_name)
        if not owner_norm:
            counters.warnings.append(
                f"[{run_id}] row {ordinal}: owner[{owner_idx}] "
                f"normalizes to blank ({owner_name!r}) — skipping owner"
            )
            continue

        participant_xref_id = f"{row_primary_id}:{owner_idx}"
        cached_pid = _xref_lookup_participant(conn, participant_xref_id)
        if cached_pid:
            participant_id = cached_pid
        else:
            participant_id = _resolve_or_insert_participant(conn, owner_name, counters)
            is_new_xref = True
            _xref_upsert_participant(conn, participant_xref_id, participant_id, is_new_xref, counters)

        # First owner is primary; additional owners are co-owners
        ownership_role = "owner" if owner_idx == 0 else "co_owner"
        is_primary = owner_idx == 0

        # Upsert ownership link
        existing_ownership = conn.execute(
            """
            SELECT id FROM yacht_ownership
            WHERE yacht_id = %s AND participant_id = %s
              AND role = %s AND effective_end IS NULL
            """,
            (yacht_id, participant_id, ownership_role),
        ).fetchone()
        if not existing_ownership:
            conn.execute(
                """
                INSERT INTO yacht_ownership
                  (yacht_id, participant_id, role, is_primary_contact,
                   effective_start, source_system)
                VALUES (%s, %s, %s, %s, COALESCE(%s::date, CURRENT_DATE), %s)
                """,
                (yacht_id, participant_id, ownership_role, is_primary,
                 effective_start.isoformat() if effective_start else None,
                 SOURCE_SYSTEM),
            )
            counters.regattaman_snapshot_ownerships_inserted += 1
            counters.owner_links_inserted += 1

    # ── Write PHRF rating (all non-blank integers including 0 are valid) ──
    if rating_int is None:
        # blank, '-', or unparseable → no rating to write
        pass
    else:
        _upsert_phrf_rating(conn, yacht_id, rating_int, counters)

    counters.regattaman_snapshot_rows_curated_processed += 1


# ---------------------------------------------------------------------------
# Main streaming loop
# ---------------------------------------------------------------------------

def _stream_snapshot(
    conn: psycopg.Connection,
    csv_path: Path,
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
    effective_start: date | None,
) -> None:
    """Stream snapshot CSV with two-savepoint pattern per row.

    Savepoint 1 (raw): captures raw row — persists even if curated fails.
    Savepoint 2 (cur): curated projection — rolled back on any error.
    """
    source_file_name = csv_path.name

    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for idx, raw_row in enumerate(reader):
            row = normalize_headers(raw_row)
            counters.rows_read += 1

            # Compute stable row identifiers
            name_lower = trim(row.get("nameLower")) or ""
            yacht_name_lower = trim(row.get("yachtNameLower")) or ""
            sail = trim(row.get("Sail")) or ""
            row_primary_id = _row_primary_id(name_lower, yacht_name_lower, sail)
            payload_str = json.dumps(row, ensure_ascii=False, sort_keys=True)
            rhash = _row_hash(payload_str)

            # ── Savepoint 1: raw capture ──
            sp_raw = f"snap_{idx}_r"
            conn.execute(f"SAVEPOINT {sp_raw}")
            try:
                _insert_raw_row(
                    conn, source_file_name, idx + 1,
                    row_primary_id, rhash, payload_str, counters,
                )
                conn.execute(f"RELEASE SAVEPOINT {sp_raw}")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_raw}")
                counters.warnings.append(
                    f"[{run_id}] snap[{idx}] raw capture failed: {exc}"
                )
                counters.db_phase_errors += 1
                continue  # skip curated if raw capture failed

            # ── Savepoint 2: curated projection ──
            sp_cur = f"snap_{idx}_c"
            conn.execute(f"SAVEPOINT {sp_cur}")
            try:
                _process_row(
                    conn, row, source_file_name, idx + 1,
                    row_primary_id, effective_start, counters, rejects, run_id,
                )
                conn.execute(f"RELEASE SAVEPOINT {sp_cur}")
            except AmbiguousMatchError as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_cur}")
                rejects.write(
                    {**row, "_reject_context": str(exc)},
                    "ambiguous_match",
                )
                counters.rows_rejected += 1
                counters.regattaman_snapshot_rows_curated_rejected += 1
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_cur}")
                rejects.write(
                    {**row, "_reject_context": str(exc)},
                    "db_constraint_error",
                )
                counters.warnings.append(
                    f"[{run_id}] snap[{idx}]: {type(exc).__name__}: {exc}"
                )
                counters.rows_rejected += 1
                counters.regattaman_snapshot_rows_curated_rejected += 1
                counters.db_phase_errors += 1


# ---------------------------------------------------------------------------
# Main run entry point
# ---------------------------------------------------------------------------

def _run_regattaman_snapshot(
    run_id: str,
    started_at: str,
    db_dsn: str,
    counters: RunCounters,
    rejects: RejectWriter,
    snapshot_path: str,
    effective_start: date | None,
    dry_run: bool,
) -> None:
    csv_path = Path(snapshot_path)

    if not csv_path.exists():
        click.echo(
            f"[{run_id}] FATAL: snapshot file not found: {csv_path}",
            err=True,
        )
        sys.exit(1)

    _validate_headers(csv_path, run_id)
    click.echo(
        f"[{run_id}] Header validation passed — starting DB phase "
        f"(effective_start={effective_start or 'CURRENT_DATE'})"
    )

    conn = psycopg.connect(db_dsn, autocommit=False)
    try:
        if dry_run:
            try:
                _stream_snapshot(conn, csv_path, run_id, counters, rejects, effective_start)
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
        else:
            try:
                _stream_snapshot(conn, csv_path, run_id, counters, rejects, effective_start)
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

            conn.commit()
            click.echo(f"[{run_id}] Committed.")
    finally:
        conn.close()

    click.echo(
        f"[{run_id}] regattaman_snapshot complete — "
        f"rows_read={counters.rows_read} "
        f"raw_inserted={counters.regattaman_snapshot_rows_raw_inserted} "
        f"curated_processed={counters.regattaman_snapshot_rows_curated_processed} "
        f"curated_rejected={counters.regattaman_snapshot_rows_curated_rejected} "
        f"participants_inserted={counters.participants_inserted} "
        f"yachts_inserted={counters.yachts_inserted} "
        f"ownerships_inserted={counters.regattaman_snapshot_ownerships_inserted} "
        f"ratings_written={counters.regattaman_snapshot_ratings_written}"
    )
