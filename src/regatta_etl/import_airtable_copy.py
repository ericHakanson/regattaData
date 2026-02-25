"""regatta_etl.import_airtable_copy

Airtable copy multi-asset ingestion pipeline (--mode airtable_copy).

Processes 6 CSV files in dependency order:
  clubs → events → yachts → owners → participants → entries

Two-layer model:
  Raw layer:     append-only lossless capture (airtable_copy_row).
                 Every source row is captured before curated validation.
  Curated layer: enrichment-only projection into existing domain tables
                 using conservative, strict matching (fill-nulls-only).

Raw capture uses a separate SAVEPOINT per row so it persists even when
the curated projection is rejected or rolled back.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Callable

import click
import psycopg

from regatta_etl.normalize import (
    normalize_email,
    normalize_name,
    normalize_phone,
    normalize_space,
    parse_numeric,
    parse_race_url,
    parse_name_parts,
    slug_name,
    trim,
)
from regatta_etl.shared import (
    AmbiguousMatchError,
    RejectWriter,
    RunCounters,
    insert_participant,
    normalize_headers,
    upsert_entry_participant,
    upsert_event_entry,
    upsert_ownership,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "airtable_copy_csv"

ASSET_NAMES = frozenset({"clubs", "events", "entries", "yachts", "owners", "participants"})

# Required headers per asset — minimum needed for validation + identity
REQUIRED_HEADERS: dict[str, set[str]] = {
    "clubs":        {"Name", "club_global_id"},
    "events":       {"Event Name", "source"},
    "entries":      {"entries_global_id", "Name", "Yacht Name", "entries_url", "eventUuid"},
    "yachts":       {"yachtName", "yacht_global_id"},
    "owners":       {"ownerName", "owner_global_id"},
    "participants": {"participant_global_id", "competitorE", "name"},
}

# Which column holds the Airtable source primary ID per asset
_PRIMARY_ID_FIELD: dict[str, str] = {
    "clubs":        "club_global_id",
    "events":       "event_global_id",
    "entries":      "entries_global_id",
    "yachts":       "yacht_global_id",
    "owners":       "owner_global_id",
    "participants": "participant_global_id",
}

# Which column holds the source type (e.g. regattaman_2021), or None
_SOURCE_TYPE_FIELD: dict[str, str | None] = {
    "clubs":        None,
    "events":       "source",
    "entries":      "source",
    "yachts":       None,
    "owners":       None,
    "participants": None,
}

# Known source type pattern (warn on unknown, don't reject)
_KNOWN_SOURCE_RE = re.compile(r"^(regattaman_\d{4}|yachtScoring(_\d{4})?)$")

# Processing order (dependency order)
ASSET_ORDER = ["clubs", "events", "yachts", "owners", "participants", "entries"]

# File name → asset name
_FILE_NAMES: dict[str, str] = {
    "clubs":        "clubs-Grid view.csv",
    "events":       "events-Grid view.csv",
    "yachts":       "yachts-Grid view.csv",
    "owners":       "owners-Grid view.csv",
    "participants": "participants-Grid view.csv",
    "entries":      "entries-Grid view.csv",
}


# ---------------------------------------------------------------------------
# Source type helpers
# ---------------------------------------------------------------------------

def _extract_year_from_source(source: str | None) -> int | None:
    """Return the 4-digit year embedded in a source tag like 'regattaman_2021'."""
    v = trim(source)
    if not v:
        return None
    m = re.search(r"(?<![0-9])(20\d{2})(?![0-9])", v)
    return int(m.group(1)) if m else None


def _check_source_type(
    source_type: str | None,
    counters: RunCounters,
    asset_name: str,
    run_id: str,
) -> None:
    """Increment warning counter if source_type is non-null and unrecognized."""
    if source_type and not _KNOWN_SOURCE_RE.match(source_type):
        counters.airtable_warnings_unknown_source_type += 1
        counters.warnings.append(
            f"[{run_id}] {asset_name}: unknown source_type={source_type!r}"
        )


# ---------------------------------------------------------------------------
# Event global ID helpers
# ---------------------------------------------------------------------------

def _parse_event_global_id(raw: str | None) -> tuple[str | None, int | None]:
    """Parse race_id and yr from an event_global_id JSON string.

    Input: '{"race_id":"537", "yr":"2021"}' → ("537", 2021)
    Returns (None, None) on parse failure or blank input.
    """
    v = trim(raw)
    if not v:
        return None, None
    try:
        data = json.loads(v)
        race_id = trim(str(data.get("race_id") or ""))
        yr_str = trim(str(data.get("yr") or ""))
        race_id = race_id or None
        yr = int(yr_str) if yr_str and yr_str.isdigit() else None
        return race_id, yr
    except (json.JSONDecodeError, ValueError, TypeError):
        return None, None


def _canonical_race_key(race_id: str | None, yr: int | None) -> str | None:
    """Return stable xref key 'race:{race_id}:yr:{yr}' or None."""
    if race_id and yr:
        return f"race:{race_id}:yr:{yr}"
    return None


def _race_key_from_url(url: str | None) -> str | None:
    """Parse a canonical race key from an entries_url or Event URL."""
    race_id, yr = parse_race_url(url)
    return _canonical_race_key(race_id, yr)


# ---------------------------------------------------------------------------
# Raw capture helper
# ---------------------------------------------------------------------------

def _extract_row_metadata(
    row: dict[str, str],
    asset_name: str,
) -> tuple[str, str | None, str | None]:
    """Return (row_hash, source_primary_id, source_type) for a row."""
    raw_payload = json.dumps(row, ensure_ascii=False, sort_keys=True)
    row_hash = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

    pid_field = _PRIMARY_ID_FIELD[asset_name]
    source_primary_id = trim(row.get(pid_field))

    st_field = _SOURCE_TYPE_FIELD[asset_name]
    source_type = trim(row.get(st_field)) if st_field else None

    return row_hash, source_primary_id, source_type


def _insert_raw_row(
    conn: psycopg.Connection,
    asset_name: str,
    source_file_name: str,
    ordinal: int,
    source_primary_id: str | None,
    source_type: str | None,
    row_hash: str,
    raw_payload_str: str,
    counters: RunCounters,
) -> None:
    result = conn.execute(
        """
        INSERT INTO airtable_copy_row
          (source_system, asset_name, source_file_name, source_row_ordinal,
           source_primary_id, source_type, row_hash, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (source_system, asset_name, source_file_name, row_hash)
        DO NOTHING
        RETURNING id
        """,
        (SOURCE_SYSTEM, asset_name, source_file_name, ordinal,
         source_primary_id, source_type, row_hash, raw_payload_str),
    ).fetchone()
    if result:
        counters.airtable_rows_raw_inserted += 1


# ---------------------------------------------------------------------------
# Xref helpers
# ---------------------------------------------------------------------------

def _xref_lookup_participant(
    conn: psycopg.Connection,
    source_primary_id: str,
    asset_name: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT participant_id FROM airtable_xref_participant
        WHERE source_system = %s AND asset_name = %s AND source_primary_id = %s
        """,
        (SOURCE_SYSTEM, asset_name, source_primary_id),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_participant(
    conn: psycopg.Connection,
    source_primary_id: str,
    asset_name: str,
    participant_id: str,
    is_new_xref: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO airtable_xref_participant
          (source_system, asset_name, source_primary_id, participant_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_system, asset_name, source_primary_id)
        DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, asset_name, source_primary_id, participant_id),
    )
    if is_new_xref:
        counters.airtable_xref_inserted += 1


def _xref_lookup_yacht(
    conn: psycopg.Connection,
    source_primary_id: str,
    asset_name: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT yacht_id FROM airtable_xref_yacht
        WHERE source_system = %s AND asset_name = %s AND source_primary_id = %s
        """,
        (SOURCE_SYSTEM, asset_name, source_primary_id),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_yacht(
    conn: psycopg.Connection,
    source_primary_id: str,
    asset_name: str,
    yacht_id: str,
    is_new_xref: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO airtable_xref_yacht
          (source_system, asset_name, source_primary_id, yacht_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_system, asset_name, source_primary_id)
        DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, asset_name, source_primary_id, yacht_id),
    )
    if is_new_xref:
        counters.airtable_xref_inserted += 1


def _xref_lookup_club(
    conn: psycopg.Connection,
    source_primary_id: str,
    asset_name: str = "clubs",
) -> str | None:
    row = conn.execute(
        """
        SELECT yacht_club_id FROM airtable_xref_club
        WHERE source_system = %s AND asset_name = %s AND source_primary_id = %s
        """,
        (SOURCE_SYSTEM, asset_name, source_primary_id),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_club(
    conn: psycopg.Connection,
    source_primary_id: str,
    asset_name: str,
    yacht_club_id: str,
    is_new_xref: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO airtable_xref_club
          (source_system, asset_name, source_primary_id, yacht_club_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_system, asset_name, source_primary_id)
        DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, asset_name, source_primary_id, yacht_club_id),
    )
    if is_new_xref:
        counters.airtable_xref_inserted += 1


def _xref_lookup_event(
    conn: psycopg.Connection,
    canonical_key: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT event_instance_id FROM airtable_xref_event
        WHERE source_system = %s AND asset_name = 'events' AND source_primary_id = %s
        """,
        (SOURCE_SYSTEM, canonical_key),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_event(
    conn: psycopg.Connection,
    canonical_key: str,
    event_instance_id: str,
    is_new_xref: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO airtable_xref_event
          (source_system, asset_name, source_primary_id, event_instance_id)
        VALUES (%s, 'events', %s, %s)
        ON CONFLICT (source_system, asset_name, source_primary_id)
        DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, canonical_key, event_instance_id),
    )
    if is_new_xref:
        counters.airtable_xref_inserted += 1


# ---------------------------------------------------------------------------
# Participant resolution (strict — raise AmbiguousMatchError on ambiguity)
# ---------------------------------------------------------------------------

def _resolve_by_email_strict(
    conn: psycopg.Connection,
    email_norm: str,
) -> str | None:
    rows = conn.execute(
        """
        SELECT DISTINCT participant_id
        FROM participant_contact_point
        WHERE contact_type = 'email' AND contact_value_normalized = %s
        """,
        (email_norm,),
    ).fetchall()
    if len(rows) > 1:
        raise AmbiguousMatchError(
            f"ambiguous_participant_match: email={email_norm!r} ({len(rows)} participants)"
        )
    return str(rows[0][0]) if rows else None


def _resolve_by_phone_strict(
    conn: psycopg.Connection,
    phone_norm: str,
) -> str | None:
    rows = conn.execute(
        """
        SELECT DISTINCT participant_id
        FROM participant_contact_point
        WHERE contact_type = 'phone' AND contact_value_normalized = %s
        """,
        (phone_norm,),
    ).fetchall()
    if len(rows) > 1:
        raise AmbiguousMatchError(
            f"ambiguous_participant_match: phone={phone_norm!r} ({len(rows)} participants)"
        )
    return str(rows[0][0]) if rows else None


def _resolve_by_name_strict(
    conn: psycopg.Connection,
    name_norm: str,
) -> str | None:
    rows = conn.execute(
        "SELECT id FROM participant WHERE normalized_full_name = %s",
        (name_norm,),
    ).fetchall()
    if len(rows) > 1:
        raise AmbiguousMatchError(
            f"ambiguous_participant_match: name={name_norm!r} ({len(rows)} participants)"
        )
    return str(rows[0][0]) if rows else None


def _resolve_or_insert_participant_strict(
    conn: psycopg.Connection,
    full_name: str | None,
    email_norm: str | None,
    phone_norm: str | None,
    counters: RunCounters,
) -> str:
    """Resolve via email → phone → name (strict); insert new if no match.

    Raises AmbiguousMatchError if any step finds multiple candidates.
    Raises ValueError if full_name is None and identity cannot be inserted.
    """
    pid: str | None = None

    if email_norm:
        pid = _resolve_by_email_strict(conn, email_norm)
    if pid is None and phone_norm:
        pid = _resolve_by_phone_strict(conn, phone_norm)
    if pid is None and full_name:
        name_norm = normalize_name(full_name)
        if name_norm:
            pid = _resolve_by_name_strict(conn, name_norm)

    if pid is not None:
        counters.participants_matched_existing += 1
        return pid

    if not full_name:
        raise ValueError("missing_required_identity: no name or resolvable contact")

    pid = insert_participant(conn, full_name)
    counters.participants_inserted += 1
    return pid


def _resolve_or_insert_participant_name_only(
    conn: psycopg.Connection,
    full_name: str,
    counters: RunCounters,
) -> str:
    """Resolve by normalized name only; raise on ambiguity; insert on no match."""
    name_norm = normalize_name(full_name)
    if name_norm:
        pid = _resolve_by_name_strict(conn, name_norm)
        if pid is not None:
            counters.participants_matched_existing += 1
            return pid
    pid = insert_participant(conn, full_name)
    counters.participants_inserted += 1
    return pid


# ---------------------------------------------------------------------------
# Yacht resolution (strict — raise AmbiguousMatchError on ambiguity)
# ---------------------------------------------------------------------------

def _resolve_or_insert_yacht_strict(
    conn: psycopg.Connection,
    yacht_name: str,
    sail_num: str | None,
    boat_type: str | None,
    loa: str | None,
    counters: RunCounters,
) -> str:
    """Resolve yacht strictly per spec rules.

    Sail present: (normalized_name, normalized_sail_number) exact match.
                  Raises AmbiguousMatchError if >1 match; INSERTs on 0.
    Sail absent:  (normalized_name, length_feet) if length present, else
                  name-only — raises AmbiguousMatchError if >1 match; INSERTs on 0.
    """
    norm = slug_name(yacht_name)
    sail_norm = slug_name(sail_num) if sail_num else None
    length = parse_numeric(loa)
    model = trim(boat_type)

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
                f"ambiguous_yacht_match: name={norm!r} sail={sail_norm!r}"
            )
        if len(rows) == 1:
            yacht_id = str(rows[0][0])
            # Fill-nulls-only: enrich model if missing
            if model:
                conn.execute(
                    "UPDATE yacht SET model = COALESCE(model, %s) WHERE id = %s",
                    (model, yacht_id),
                )
            if length is not None:
                conn.execute(
                    "UPDATE yacht SET length_feet = COALESCE(length_feet, %s) WHERE id = %s",
                    (length, yacht_id),
                )
            return yacht_id
    elif length is not None:
        rows = conn.execute(
            "SELECT id FROM yacht WHERE normalized_name = %s AND length_feet = %s",
            (norm, length),
        ).fetchall()
        if len(rows) > 1:
            raise AmbiguousMatchError(
                f"ambiguous_yacht_match: name={norm!r} length={length}"
            )
        if len(rows) == 1:
            yacht_id = str(rows[0][0])
            if model:
                conn.execute(
                    "UPDATE yacht SET model = COALESCE(model, %s) WHERE id = %s",
                    (model, yacht_id),
                )
            return yacht_id
    else:
        rows = conn.execute(
            "SELECT id FROM yacht WHERE normalized_name = %s",
            (norm,),
        ).fetchall()
        if len(rows) > 1:
            raise AmbiguousMatchError(
                f"ambiguous_yacht_match: name={norm!r} (no sail/length discriminator)"
            )
        if len(rows) == 1:
            yacht_id = str(rows[0][0])
            if model:
                conn.execute(
                    "UPDATE yacht SET model = COALESCE(model, %s) WHERE id = %s",
                    (model, yacht_id),
                )
            return yacht_id

    # INSERT new yacht
    new_row = conn.execute(
        """
        INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number, model, length_feet)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (yacht_name, norm, sail_num, sail_norm, model, length),
    ).fetchone()
    counters.yachts_inserted += 1
    return str(new_row[0])


# ---------------------------------------------------------------------------
# Contact / address upsert helpers
# ---------------------------------------------------------------------------

def _upsert_contact_point(
    conn: psycopg.Connection,
    participant_id: str,
    contact_type: str,
    contact_subtype: str,
    raw_value: str,
    norm_value: str | None,
    is_primary: bool,
    counters: RunCounters,
) -> None:
    if not norm_value:
        return
    existing = conn.execute(
        """
        SELECT id FROM participant_contact_point
        WHERE participant_id = %s
          AND contact_type = %s
          AND contact_value_normalized = %s
        """,
        (participant_id, contact_type, norm_value),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO participant_contact_point
          (participant_id, contact_type, contact_subtype,
           contact_value_raw, contact_value_normalized, is_primary, source_system)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (participant_id, contact_type, contact_subtype,
         raw_value, norm_value, is_primary, SOURCE_SYSTEM),
    )
    counters.contact_points_inserted += 1


def _upsert_address_raw(
    conn: psycopg.Connection,
    participant_id: str,
    address_raw: str,
    counters: RunCounters,
) -> None:
    """Upsert a raw address string (no structured parsing)."""
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


def _upsert_address_structured(
    conn: psycopg.Connection,
    participant_id: str,
    line1: str | None,
    line2: str | None,
    city: str | None,
    state: str | None,
    postal: str | None,
    counters: RunCounters,
) -> None:
    """Upsert a structured address into participant_address."""
    parts = [p for p in [line1, line2, city, state, postal] if p]
    if not parts:
        return
    address_raw = " | ".join(parts)
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
          (participant_id, address_type, line1, city, state,
           postal_code, address_raw, is_primary, source_system)
        VALUES (%s, 'mailing', %s, %s, %s, %s, %s, true, %s)
        """,
        (participant_id, line1, city, state, postal, address_raw, SOURCE_SYSTEM),
    )
    counters.addresses_inserted += 1


# ---------------------------------------------------------------------------
# Club resolution (never raises AmbiguousMatchError — always inserts if unknown)
# ---------------------------------------------------------------------------

def _resolve_or_upsert_club(
    conn: psycopg.Connection,
    club_name: str,
    website_url: str | None,
    source_primary_id: str | None,
) -> str:
    """Resolve/upsert a club by normalized name. Returns yacht_club_id.

    Never raises — inserts with 'unknown' vitality if not found.
    Fill-nulls-only: does not overwrite existing website_url.
    """
    # 1. Try xref lookup
    if source_primary_id:
        club_id = _xref_lookup_club(conn, source_primary_id)
        if club_id:
            if website_url:
                conn.execute(
                    "UPDATE yacht_club SET website_url = COALESCE(website_url, %s) WHERE id = %s",
                    (website_url, club_id),
                )
            return club_id

    # 2. Name-based match
    norm = slug_name(club_name)
    row = conn.execute(
        "SELECT id FROM yacht_club WHERE normalized_name = %s",
        (norm,),
    ).fetchone()
    if row:
        club_id = str(row[0])
        if website_url:
            conn.execute(
                "UPDATE yacht_club SET website_url = COALESCE(website_url, %s) WHERE id = %s",
                (website_url, club_id),
            )
        return club_id

    # 3. Insert new club
    new_row = conn.execute(
        """
        INSERT INTO yacht_club (name, normalized_name, vitality_status, website_url)
        VALUES (%s, %s, 'unknown', %s)
        RETURNING id
        """,
        (club_name, norm, website_url),
    ).fetchone()
    return str(new_row[0])


# ---------------------------------------------------------------------------
# Asset processors (curated projection only — raw capture is done upstream)
# ---------------------------------------------------------------------------

def _process_club_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_name: str,
    ordinal: int,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    counters.airtable_clubs_read += 1

    name = normalize_space(row.get("Name"))
    if not name:
        rejects.write(
            {**row, "_asset_name": "clubs", "_reject_context": "blank_club_name"},
            "missing_required_identity",
        )
        counters.airtable_rows_curated_rejected += 1
        counters.airtable_clubs_rejected += 1
        return

    source_primary_id = trim(row.get("club_global_id"))
    website_url = trim(row.get("Website"))

    # Determine if xref exists (before resolution)
    xref_existed = bool(source_primary_id and _xref_lookup_club(conn, source_primary_id))

    club_id = _resolve_or_upsert_club(conn, name, website_url, source_primary_id)

    if source_primary_id:
        _xref_upsert_club(conn, source_primary_id, "clubs", club_id, not xref_existed, counters)

    counters.airtable_rows_curated_processed += 1
    counters.airtable_clubs_inserted += 1


def _process_event_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_name: str,
    ordinal: int,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    counters.airtable_events_read += 1

    event_name = normalize_space(row.get("Event Name"))
    if not event_name:
        rejects.write(
            {**row, "_asset_name": "events", "_reject_context": "blank_event_name"},
            "missing_required_identity",
        )
        counters.airtable_rows_curated_rejected += 1
        counters.airtable_events_rejected += 1
        return

    source_type = trim(row.get("source"))
    _check_source_type(source_type, counters, "events", run_id)

    # Determine season year: source tag → entries_url → Event URL
    season_year = (
        _extract_year_from_source(source_type)
        or _extract_year_from_source(trim(row.get("entries_url")))
    )
    if season_year is None:
        _, yr = parse_race_url(row.get("entries_url"))
        season_year = yr
    if season_year is None:
        _, yr = parse_race_url(row.get("Event URL"))
        season_year = yr
    if season_year is None:
        rejects.write(
            {**row, "_asset_name": "events", "_reject_context": "no_parseable_year"},
            "missing_season_year",
        )
        counters.airtable_rows_curated_rejected += 1
        counters.airtable_events_rejected += 1
        return

    # Canonical race key (for xref_event)
    canonical_key = (
        _race_key_from_url(row.get("entries_url"))
        or _race_key_from_url(row.get("Event URL"))
    )
    if canonical_key is None:
        race_id, yr = _parse_event_global_id(row.get("event_global_id"))
        canonical_key = _canonical_race_key(race_id, yr)

    # If xref already maps this race key → just refresh last_seen_at and return
    if canonical_key:
        existing_event_id = _xref_lookup_event(conn, canonical_key)
        if existing_event_id:
            _xref_upsert_event(conn, canonical_key, existing_event_id, False, counters)
            counters.airtable_rows_curated_processed += 1
            counters.airtable_events_inserted += 1
            return

    # Resolve host club
    host_club_name = normalize_space(row.get("Host Club"))
    if not host_club_name:
        rejects.write(
            {**row, "_asset_name": "events", "_reject_context": "blank_host_club"},
            "missing_required_identity",
        )
        counters.airtable_rows_curated_rejected += 1
        counters.airtable_events_rejected += 1
        return

    club_global_id_lookup = trim(row.get("club_global_id_lookup"))
    club_id = _resolve_or_upsert_club(conn, host_club_name, None, club_global_id_lookup)

    # Upsert event_series
    series_norm = slug_name(event_name)
    conn.execute(
        """
        INSERT INTO event_series (yacht_club_id, name, normalized_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (yacht_club_id, normalized_name) DO NOTHING
        """,
        (club_id, event_name, series_norm),
    )
    series_row = conn.execute(
        "SELECT id FROM event_series WHERE yacht_club_id = %s AND normalized_name = %s",
        (club_id, series_norm),
    ).fetchone()
    event_series_id = str(series_row[0])

    # Upsert event_instance
    instance_result = conn.execute(
        """
        INSERT INTO event_instance (event_series_id, display_name, season_year)
        VALUES (%s, %s, %s)
        ON CONFLICT (event_series_id, season_year) DO NOTHING
        RETURNING id
        """,
        (event_series_id, event_name, season_year),
    ).fetchone()

    if instance_result:
        event_instance_id = str(instance_result[0])
    else:
        existing_inst = conn.execute(
            "SELECT id FROM event_instance WHERE event_series_id = %s AND season_year = %s",
            (event_series_id, season_year),
        ).fetchone()
        event_instance_id = str(existing_inst[0])

    # Upsert xref_event
    if canonical_key:
        _xref_upsert_event(conn, canonical_key, event_instance_id, True, counters)

    counters.airtable_rows_curated_processed += 1
    counters.airtable_events_inserted += 1


def _process_yacht_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_name: str,
    ordinal: int,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    counters.airtable_yachts_read += 1

    yacht_name = normalize_space(row.get("yachtName"))
    if not yacht_name:
        rejects.write(
            {**row, "_asset_name": "yachts", "_reject_context": "blank_yacht_name"},
            "missing_required_identity",
        )
        counters.airtable_rows_curated_rejected += 1
        counters.airtable_yachts_rejected += 1
        return

    source_primary_id = trim(row.get("yacht_global_id"))

    # Xref lookup first
    xref_existed = False
    yacht_id: str | None = None
    if source_primary_id:
        yacht_id = _xref_lookup_yacht(conn, source_primary_id, "yachts")
        xref_existed = yacht_id is not None

    if yacht_id is None:
        sail_num = trim(row.get("sailNumber"))
        boat_type = trim(row.get("yachtType"))
        loa = trim(row.get("yachtLoa"))
        # May raise AmbiguousMatchError (caller handles via savepoint)
        yacht_id = _resolve_or_insert_yacht_strict(
            conn, yacht_name, sail_num, boat_type, loa, counters
        )

    if source_primary_id:
        _xref_upsert_yacht(conn, source_primary_id, "yachts", yacht_id, not xref_existed, counters)

    counters.airtable_rows_curated_processed += 1
    counters.airtable_yachts_inserted += 1


def _process_owner_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_name: str,
    ordinal: int,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    counters.airtable_owners_read += 1

    owner_name = normalize_space(row.get("ownerName"))
    source_primary_id = trim(row.get("owner_global_id"))

    # Xref lookup first
    xref_existed = False
    participant_id: str | None = None
    if source_primary_id:
        participant_id = _xref_lookup_participant(conn, source_primary_id, "owners")
        xref_existed = participant_id is not None

    if participant_id is None:
        # Resolve via email → phone → name
        email_raw = trim(row.get("ownerEmail"))
        email_norm = normalize_email(email_raw)
        hphone_raw = trim(row.get("ownerHomePhone"))
        hphone_norm = normalize_phone(hphone_raw)
        cphone_raw = trim(row.get("ownerCellPhone"))
        cphone_norm = normalize_phone(cphone_raw)

        full_name = owner_name or trim(row.get("skipperName"))
        if not full_name and not email_norm:
            rejects.write(
                {**row, "_asset_name": "owners", "_reject_context": "no_name_or_email"},
                "missing_required_identity",
            )
            counters.airtable_rows_curated_rejected += 1
            counters.airtable_owners_rejected += 1
            return

        # May raise AmbiguousMatchError
        participant_id = _resolve_or_insert_participant_strict(
            conn, full_name, email_norm, hphone_norm or cphone_norm, counters
        )

    if source_primary_id:
        _xref_upsert_participant(
            conn, source_primary_id, "owners", participant_id, not xref_existed, counters
        )

    # Fill contact points (fill-nulls-only — don't overwrite existing)
    email_raw = trim(row.get("ownerEmail"))
    if email_raw:
        email_norm = normalize_email(email_raw)
        _upsert_contact_point(
            conn, participant_id, "email", "primary", email_raw, email_norm, True, counters
        )
    hphone_raw = trim(row.get("ownerHomePhone"))
    if hphone_raw:
        hphone_norm = normalize_phone(hphone_raw)
        _upsert_contact_point(
            conn, participant_id, "phone", "home", hphone_raw, hphone_norm, False, counters
        )
    cphone_raw = trim(row.get("ownerCellPhone"))
    if cphone_raw:
        cphone_norm = normalize_phone(cphone_raw)
        _upsert_contact_point(
            conn, participant_id, "phone", "mobile", cphone_raw, cphone_norm, False, counters
        )

    # Fill address (structured, fill-nulls-only)
    _upsert_address_structured(
        conn, participant_id,
        trim(row.get("ownerAddressLine1")),
        trim(row.get("ownerAddressLine2")),
        trim(row.get("ownerCity")),
        trim(row.get("ownerState")),
        trim(row.get("ownerPostal")),
        counters,
    )

    # Club affiliation
    affiliation = normalize_space(row.get("ownerAffiliation"))
    club_global_id_lookup = trim(row.get("club_global_id_lookup"))
    if affiliation:
        club_id = _resolve_or_upsert_club(conn, affiliation, None, club_global_id_lookup)
        # Upsert club membership (open-ended)
        existing_membership = conn.execute(
            """
            SELECT id FROM club_membership
            WHERE participant_id = %s AND yacht_club_id = %s AND effective_end IS NULL
            """,
            (participant_id, club_id),
        ).fetchone()
        if not existing_membership:
            conn.execute(
                """
                INSERT INTO club_membership
                  (participant_id, yacht_club_id, membership_role, source_system)
                VALUES (%s, %s, 'member', %s)
                """,
                (participant_id, club_id, SOURCE_SYSTEM),
            )
            counters.memberships_inserted += 1

    # Yacht ownership link (via yacht_global_id_lookup xref)
    yacht_global_id_lookup = trim(row.get("yacht_global_id_lookup"))
    if yacht_global_id_lookup:
        linked_yacht_id = _xref_lookup_yacht(conn, yacht_global_id_lookup, "yachts")
        if linked_yacht_id:
            upsert_ownership(
                conn, participant_id, linked_yacht_id, "owner",
                is_primary_contact=True,
                effective_start=date.today(),
                source_system=SOURCE_SYSTEM,
                counters=counters,
            )

    counters.airtable_rows_curated_processed += 1
    counters.airtable_owners_inserted += 1


def _process_participant_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_name: str,
    ordinal: int,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    counters.airtable_participants_read += 1

    source_primary_id = trim(row.get("participant_global_id"))
    email_raw = trim(row.get("competitorE"))
    email_norm = normalize_email(email_raw)
    full_name = normalize_space(row.get("name"))

    # Xref lookup first
    xref_existed = False
    participant_id: str | None = None
    if source_primary_id:
        participant_id = _xref_lookup_participant(conn, source_primary_id, "participants")
        xref_existed = participant_id is not None

    if participant_id is None:
        phone_raw = trim(row.get("numbersOnly"))
        phone_norm = normalize_phone(phone_raw)

        if not full_name and not email_norm:
            rejects.write(
                {**row, "_asset_name": "participants", "_reject_context": "no_name_or_email"},
                "missing_required_identity",
            )
            counters.airtable_rows_curated_rejected += 1
            counters.airtable_participants_rejected += 1
            return

        # Use email as name fallback (same pattern as mailchimp)
        display_name = full_name or email_raw

        # May raise AmbiguousMatchError
        participant_id = _resolve_or_insert_participant_strict(
            conn, display_name, email_norm, phone_norm, counters
        )

    if source_primary_id:
        _xref_upsert_participant(
            conn, source_primary_id, "participants", participant_id, not xref_existed, counters
        )

    # Fill contact points
    if email_raw and email_norm:
        _upsert_contact_point(
            conn, participant_id, "email", "primary", email_raw, email_norm, True, counters
        )
    phone_raw = trim(row.get("numbersOnly"))
    if phone_raw:
        phone_norm = normalize_phone(phone_raw)
        _upsert_contact_point(
            conn, participant_id, "phone", "primary", phone_raw, phone_norm, False, counters
        )

    # Fill address (raw, combined from address + postalCode)
    addr = trim(row.get("address"))
    postal = trim(row.get("postalCode"))
    if addr:
        full_addr = f"{addr} {postal}".strip() if postal else addr
        _upsert_address_raw(conn, participant_id, full_addr, counters)

    counters.airtable_rows_curated_processed += 1
    counters.airtable_participants_inserted += 1


def _process_entry_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_name: str,
    ordinal: int,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    counters.airtable_entries_read += 1

    source_type = trim(row.get("source"))
    _check_source_type(source_type, counters, "entries", run_id)

    # Resolve event_instance via eventUuid canonical key
    event_uuid_raw = trim(row.get("eventUuid"))
    race_id, yr = _parse_event_global_id(event_uuid_raw)
    canonical_key = _canonical_race_key(race_id, yr)

    if canonical_key is None:
        # Fallback: try entries_url
        canonical_key = _race_key_from_url(row.get("entries_url"))

    if canonical_key is None:
        rejects.write(
            {**row, "_asset_name": "entries", "_reject_context": "no_parseable_event_key"},
            "unresolved_event_link",
        )
        counters.airtable_rows_curated_rejected += 1
        counters.airtable_entries_rejected += 1
        return

    event_instance_id = _xref_lookup_event(conn, canonical_key)
    if event_instance_id is None:
        rejects.write(
            {**row, "_asset_name": "entries",
             "_reject_context": f"event_not_in_xref:{canonical_key}"},
            "unresolved_event_link",
        )
        counters.airtable_rows_curated_rejected += 1
        counters.airtable_entries_rejected += 1
        return

    # Resolve yacht (name + sail, strict)
    yacht_name = normalize_space(row.get("Yacht Name"))
    if not yacht_name:
        rejects.write(
            {**row, "_asset_name": "entries", "_reject_context": "blank_yacht_name"},
            "missing_required_identity",
        )
        counters.airtable_rows_curated_rejected += 1
        counters.airtable_entries_rejected += 1
        return

    sail_num = trim(row.get("Sail Num"))
    boat_type = trim(row.get("Boat Type"))
    # May raise AmbiguousMatchError
    yacht_id = _resolve_or_insert_yacht_strict(
        conn, yacht_name, sail_num, boat_type, None, counters
    )

    # Resolve skipper (name-only from entries Name field — regattaman/yachtScoring Name = helm)
    skipper_name = normalize_space(row.get("Name"))
    participant_id: str | None = None
    if skipper_name:
        # May raise AmbiguousMatchError
        participant_id = _resolve_or_insert_participant_name_only(
            conn, skipper_name, counters
        )

    # Upsert event_entry
    entries_sku = trim(row.get("entriesSku"))
    event_entry_id = upsert_event_entry(
        conn,
        event_instance_id,
        yacht_id,
        entry_status="submitted",
        registration_source=SOURCE_SYSTEM,
        registration_external_id=entries_sku,
        registered_at=None,
        counters=counters,
    )

    # Upsert event_entry_participant (skipper link)
    if participant_id:
        upsert_entry_participant(
            conn,
            event_entry_id,
            participant_id,
            role="skipper",
            participation_state="participating",
            source_system=SOURCE_SYSTEM,
        )

    counters.airtable_rows_curated_processed += 1
    counters.airtable_entries_inserted += 1


# ---------------------------------------------------------------------------
# Asset processor dispatch table
# ---------------------------------------------------------------------------

_ASSET_PROCESSORS: dict[str, Callable] = {
    "clubs":        _process_club_row,
    "events":       _process_event_row,
    "yachts":       _process_yacht_row,
    "owners":       _process_owner_row,
    "participants": _process_participant_row,
    "entries":      _process_entry_row,
}


# ---------------------------------------------------------------------------
# Header validation
# ---------------------------------------------------------------------------

def _validate_asset_headers(
    csv_path: Path,
    asset_name: str,
    run_id: str,
) -> None:
    required = REQUIRED_HEADERS[asset_name]
    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        _ = reader.fieldnames
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
# Per-asset counter increment helpers
# ---------------------------------------------------------------------------

def _inc_asset_rejected(counters: RunCounters, asset_name: str) -> None:
    attr = f"airtable_{asset_name}_rejected"
    setattr(counters, attr, getattr(counters, attr) + 1)


# ---------------------------------------------------------------------------
# Streaming processor for one asset file
# ---------------------------------------------------------------------------

def _stream_asset(
    conn: psycopg.Connection,
    csv_path: Path,
    asset_name: str,
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
) -> None:
    """Stream one asset CSV file with two-savepoint pattern per row.

    Savepoint 1 (raw): captures raw row — persists even if curated fails.
    Savepoint 2 (cur): curated projection — rolled back on any error.
    """
    processor = _ASSET_PROCESSORS[asset_name]
    source_file_name = csv_path.name

    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for idx, raw_row in enumerate(reader):
            row = normalize_headers(raw_row)
            counters.rows_read += 1

            # Compute row metadata once (used in both savepoints)
            row_hash, source_primary_id, source_type = _extract_row_metadata(row, asset_name)
            raw_payload_str = json.dumps(row, ensure_ascii=False, sort_keys=True)

            # ── Savepoint 1: raw capture (must persist regardless of curated outcome) ──
            sp_raw = f"{asset_name}_{idx}_r"
            conn.execute(f"SAVEPOINT {sp_raw}")
            try:
                _insert_raw_row(
                    conn, asset_name, source_file_name, idx + 1,
                    source_primary_id, source_type, row_hash, raw_payload_str, counters,
                )
                conn.execute(f"RELEASE SAVEPOINT {sp_raw}")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_raw}")
                counters.warnings.append(
                    f"[{run_id}] {asset_name}[{idx}] raw capture failed: {exc}"
                )
                counters.db_phase_errors += 1
                continue  # skip curated if raw capture failed

            # ── Savepoint 2: curated projection ──
            sp_cur = f"{asset_name}_{idx}_c"
            conn.execute(f"SAVEPOINT {sp_cur}")
            try:
                processor(conn, row, source_file_name, idx + 1, counters, rejects, run_id)
                conn.execute(f"RELEASE SAVEPOINT {sp_cur}")
            except AmbiguousMatchError as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_cur}")
                rejects.write(
                    {**row, "_asset_name": asset_name, "_reject_context": str(exc)},
                    "ambiguous_match",
                )
                counters.rows_rejected += 1
                counters.airtable_rows_curated_rejected += 1
                _inc_asset_rejected(counters, asset_name)
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_cur}")
                rejects.write(
                    {**row, "_asset_name": asset_name, "_reject_context": str(exc)},
                    "db_constraint_error",
                )
                counters.warnings.append(
                    f"[{run_id}] {asset_name}[{idx}]: {type(exc).__name__}: {exc}"
                )
                counters.rows_rejected += 1
                counters.airtable_rows_curated_rejected += 1
                counters.db_phase_errors += 1
                _inc_asset_rejected(counters, asset_name)


# ---------------------------------------------------------------------------
# Main run entry point
# ---------------------------------------------------------------------------

def _run_airtable_copy(
    run_id: str,
    started_at: str,
    db_dsn: str,
    counters: RunCounters,
    rejects: RejectWriter,
    airtable_dir: str,
    dry_run: bool,
) -> None:
    dir_path = Path(airtable_dir)

    # Build ordered asset list (dependency order)
    assets: list[tuple[str, Path]] = [
        (asset_name, dir_path / _FILE_NAMES[asset_name])
        for asset_name in ASSET_ORDER
    ]

    # Pre-scan: verify files exist and headers match contract
    for asset_name, csv_path in assets:
        if not csv_path.exists():
            click.echo(
                f"[{run_id}] FATAL: missing required file: {csv_path}",
                err=True,
            )
            sys.exit(1)
        _validate_asset_headers(csv_path, asset_name, run_id)

    click.echo(
        f"[{run_id}] Header validation passed for all {len(assets)} assets — starting DB phase"
    )

    # DB phase: stream all assets
    conn = psycopg.connect(db_dsn, autocommit=False)
    try:
        if dry_run:
            try:
                for asset_name, csv_path in assets:
                    click.echo(f"[{run_id}] [dry-run] Processing {asset_name}...")
                    _stream_asset(conn, csv_path, asset_name, run_id, counters, rejects)
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
                for asset_name, csv_path in assets:
                    click.echo(f"[{run_id}] Processing {asset_name}...")
                    _stream_asset(conn, csv_path, asset_name, run_id, counters, rejects)
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

    finally:
        conn.close()
        rejects.close()

    click.echo(
        f"[{run_id}] Done: "
        f"raw={counters.airtable_rows_raw_inserted} "
        f"curated={counters.airtable_rows_curated_processed} "
        f"rejected={counters.airtable_rows_curated_rejected} "
        f"xref={counters.airtable_xref_inserted} "
        f"warnings={counters.airtable_warnings_unknown_source_type}"
    )
