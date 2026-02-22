"""regatta_etl.import_yacht_scoring

Yacht Scoring multi-file ingestion pipeline (--mode yacht_scoring).

Directory structure under --yacht-scoring-dir:
  scrapedEvents/*.csv      → scraped_event_listing
  scrapedEntries/*.csv     → scraped_entry_listing
  deduplicated_entries.csv → deduplicated_entry
  unique_yachts.csv        → unique_yacht

Two-layer model:
  Raw layer:     append-only lossless capture (yacht_scoring_raw_row).
                 Every source row is captured before curated validation.
  Curated layer: conservative projection into domain tables using
                 strict xref + ambiguity-safe matching.

Processing order (source precedence):
  1. scrapedEvents/*       — populates event xrefs first
  2. deduplicated_entries  — highest-confidence entries
  3. unique_yachts         — yacht-level enrichment only
  4. scrapedEntries/*      — fallback entry capture
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Callable

import click
import psycopg

from regatta_etl.normalize import (
    normalize_name,
    normalize_space,
    parse_ys_boatdetail_url,
    parse_ys_emenu_url,
    parse_ys_entries_url,
    slug_name,
    trim,
)
from regatta_etl.shared import (
    AmbiguousMatchError,
    RejectWriter,
    RunCounters,
    insert_participant,
    normalize_headers,
    upsert_affiliate_club,
    upsert_entry_participant,
    upsert_event_entry,
    upsert_membership,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "yacht_scoring_csv"

# Minimum required headers per asset type (for schema validation)
_REQUIRED_HEADERS: dict[str, frozenset[str]] = {
    "scraped_event_listing": frozenset({"title-small href", "w-[20%]"}),
    "scraped_entry_listing": frozenset({"title-small href", "flex", "title-small 2"}),
    "deduplicated_entry":    frozenset({"sailNumber", "entryUrl", "yachtName", "ownerName", "yachtType", "eventUrl"}),
    "unique_yacht":          frozenset({"sailNumber", "entryUrl", "yachtName", "ownerName", "yachtType", "eventUrl"}),
}

# Synthetic club used for events scraped from Yacht Scoring without a known host
_YS_CLUB_NAME = "Yacht Scoring"
_YS_CLUB_NORM = "yacht-scoring"

_LEADING_YEAR_RE = re.compile(r"^\s*\d{4}\s*")
_YEAR_IN_TEXT_RE = re.compile(r"\b(20\d{2})\b")


# ---------------------------------------------------------------------------
# File discovery and classification
# ---------------------------------------------------------------------------

def _discover_files(root: Path) -> list[tuple[Path, str]]:
    """Return (file_path, asset_type) tuples in processing order.

    Order:
      1. scrapedEvents/*.csv  — populate event xrefs first
      2. deduplicated_entries.csv
      3. unique_yachts.csv
      4. scrapedEntries/*.csv — fallback entry capture
    """
    result: list[tuple[Path, str]] = []

    scraped_events_dir = root / "scrapedEvents"
    if scraped_events_dir.is_dir():
        for p in sorted(scraped_events_dir.rglob("*.csv")):
            result.append((p, "scraped_event_listing"))

    dedup = root / "deduplicated_entries.csv"
    if dedup.is_file():
        result.append((dedup, "deduplicated_entry"))

    unique_yachts = root / "unique_yachts.csv"
    if unique_yachts.is_file():
        result.append((unique_yachts, "unique_yacht"))

    scraped_entries_dir = root / "scrapedEntries"
    if scraped_entries_dir.is_dir():
        for p in sorted(scraped_entries_dir.rglob("*.csv")):
            result.append((p, "scraped_entry_listing"))

    return result


def _check_headers(actual_headers: set[str], asset_type: str) -> bool:
    """Return True if actual headers satisfy the minimum contract for asset_type."""
    required = _REQUIRED_HEADERS.get(asset_type, frozenset())
    return required.issubset(actual_headers)


# ---------------------------------------------------------------------------
# Stable key builders (for xref tables)
# ---------------------------------------------------------------------------

def _build_yacht_source_key(
    yacht_name: str | None,
    sail_num: str | None,
) -> str | None:
    """Return a stable xref key for a yacht.

    Key: "n:{name_slug}:s:{sail_slug}"  (sail_slug empty when sail is absent)
    Returns None when yacht_name normalizes to None.
    """
    name_norm = slug_name(yacht_name)
    if not name_norm:
        return None
    sail_norm = slug_name(sail_num) if sail_num else ""
    return f"n:{name_norm}:s:{sail_norm}"


def _build_participant_source_key(
    owner_name: str | None,
    affiliation: str | None,
    location: str | None,
) -> str | None:
    """Return a stable xref key for a participant.

    Key: "{name_norm}|{affiliation_slug}|{location_slug}"
    Returns None when owner_name normalizes to None.
    """
    name_norm = normalize_name(owner_name)
    if not name_norm:
        return None
    aff_slug = slug_name(affiliation) or ""
    loc_slug = slug_name(location) or ""
    return f"{name_norm}|{aff_slug}|{loc_slug}"


# ---------------------------------------------------------------------------
# Raw capture helper
# ---------------------------------------------------------------------------

def _extract_ids_for_raw(
    row: dict[str, str],
    asset_type: str,
) -> tuple[str | None, str | None]:
    """Parse (source_event_id, source_entry_id) from a row for raw metadata."""
    if asset_type == "scraped_event_listing":
        event_id = parse_ys_emenu_url(row.get("title-small href"))
        return event_id, None
    if asset_type == "scraped_entry_listing":
        event_id, entry_id = parse_ys_boatdetail_url(row.get("title-small href"))
        return event_id, entry_id
    if asset_type in ("deduplicated_entry", "unique_yacht"):
        event_id, entry_id = parse_ys_boatdetail_url(row.get("entryUrl"))
        if not event_id:
            event_id = parse_ys_entries_url(row.get("eventUrl"))
        return event_id, entry_id
    return None, None


def _insert_raw_row(
    conn: psycopg.Connection,
    asset_type: str,
    source_file_name: str,
    source_file_path: str,
    ordinal: int,
    source_event_id: str | None,
    source_entry_id: str | None,
    row_hash: str,
    raw_payload_str: str,
    counters: RunCounters,
) -> None:
    result = conn.execute(
        """
        INSERT INTO yacht_scoring_raw_row
          (source_system, asset_type, source_file_name, source_file_path,
           source_row_ordinal, source_event_id, source_entry_id,
           row_hash, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (source_system, source_file_path, row_hash) DO NOTHING
        RETURNING id
        """,
        (SOURCE_SYSTEM, asset_type, source_file_name, source_file_path,
         ordinal, source_event_id, source_entry_id, row_hash, raw_payload_str),
    ).fetchone()
    if result:
        counters.yacht_scoring_rows_raw_inserted += 1


# ---------------------------------------------------------------------------
# Xref helpers — event
# ---------------------------------------------------------------------------

def _xref_lookup_event(conn: psycopg.Connection, source_event_id: str) -> str | None:
    row = conn.execute(
        """
        SELECT event_instance_id FROM yacht_scoring_xref_event
        WHERE source_system = %s AND source_event_id = %s
        """,
        (SOURCE_SYSTEM, source_event_id),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_event(
    conn: psycopg.Connection,
    source_event_id: str,
    event_instance_id: str,
    is_new: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO yacht_scoring_xref_event
          (source_system, source_event_id, event_instance_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_system, source_event_id)
        DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, source_event_id, event_instance_id),
    )
    if is_new:
        counters.yacht_scoring_xref_inserted += 1


# ---------------------------------------------------------------------------
# Xref helpers — entry
# ---------------------------------------------------------------------------

def _xref_lookup_entry(
    conn: psycopg.Connection,
    source_event_id: str,
    source_entry_id: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT event_entry_id FROM yacht_scoring_xref_entry
        WHERE source_system = %s AND source_event_id = %s AND source_entry_id = %s
        """,
        (SOURCE_SYSTEM, source_event_id, source_entry_id),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_entry(
    conn: psycopg.Connection,
    source_event_id: str,
    source_entry_id: str,
    event_entry_id: str,
    is_new: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO yacht_scoring_xref_entry
          (source_system, source_event_id, source_entry_id, event_entry_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_system, source_event_id, source_entry_id)
        DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, source_event_id, source_entry_id, event_entry_id),
    )
    if is_new:
        counters.yacht_scoring_xref_inserted += 1


# ---------------------------------------------------------------------------
# Xref helpers — yacht
# ---------------------------------------------------------------------------

def _xref_lookup_yacht(conn: psycopg.Connection, source_yacht_key: str) -> str | None:
    row = conn.execute(
        """
        SELECT yacht_id FROM yacht_scoring_xref_yacht
        WHERE source_system = %s AND source_yacht_key = %s
        """,
        (SOURCE_SYSTEM, source_yacht_key),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_yacht(
    conn: psycopg.Connection,
    source_yacht_key: str,
    yacht_id: str,
    is_new: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO yacht_scoring_xref_yacht
          (source_system, source_yacht_key, yacht_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_system, source_yacht_key)
        DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, source_yacht_key, yacht_id),
    )
    if is_new:
        counters.yacht_scoring_xref_inserted += 1


# ---------------------------------------------------------------------------
# Xref helpers — participant
# ---------------------------------------------------------------------------

def _xref_lookup_participant(conn: psycopg.Connection, source_key: str) -> str | None:
    row = conn.execute(
        """
        SELECT participant_id FROM yacht_scoring_xref_participant
        WHERE source_system = %s AND source_participant_key = %s
        """,
        (SOURCE_SYSTEM, source_key),
    ).fetchone()
    return str(row[0]) if row else None


def _xref_upsert_participant(
    conn: psycopg.Connection,
    source_key: str,
    participant_id: str,
    is_new: bool,
    counters: RunCounters,
) -> None:
    conn.execute(
        """
        INSERT INTO yacht_scoring_xref_participant
          (source_system, source_participant_key, participant_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_system, source_participant_key)
        DO UPDATE SET last_seen_at = now()
        """,
        (SOURCE_SYSTEM, source_key, participant_id),
    )
    if is_new:
        counters.yacht_scoring_xref_inserted += 1


# ---------------------------------------------------------------------------
# Domain helpers
# ---------------------------------------------------------------------------

def _parse_year_from_text(text: str | None) -> int | None:
    """Extract the first 4-digit year in [2000, 2099] from a text string."""
    v = trim(text)
    if not v:
        return None
    years = _YEAR_IN_TEXT_RE.findall(v)
    return int(years[0]) if years else None


def _strip_leading_year(title: str) -> str:
    """Remove a leading 4-digit year + space prefix from a title string."""
    return _LEADING_YEAR_RE.sub("", title).strip()


def _resolve_or_insert_yacht_ys(
    conn: psycopg.Connection,
    yacht_name: str,
    sail_num: str | None,
    boat_type: str | None,
    counters: RunCounters,
) -> str:
    """Resolve/insert a yacht for yacht_scoring data (conservative).

    Sail present: exact match on (normalized_name, normalized_sail_number).
                  Raises AmbiguousMatchError if >1 match.
    Sail absent:  match by (normalized_name, model) when model known,
                  else normalized_name only.
                  Raises AmbiguousMatchError if >1 match.
    Inserts a new yacht record when 0 matches.
    """
    norm = slug_name(yacht_name)
    sail_norm = slug_name(sail_num) if sail_num else None
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
                f"ambiguous_yacht_match: name={norm!r} sail={sail_norm!r} ({len(rows)} records)"
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
        if model:
            rows = conn.execute(
                "SELECT id FROM yacht WHERE normalized_name = %s AND model = %s",
                (norm, model),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id FROM yacht WHERE normalized_name = %s",
                (norm,),
            ).fetchall()
        if len(rows) > 1:
            raise AmbiguousMatchError(
                f"ambiguous_yacht_match: name={norm!r} (no sail; {len(rows)} records)"
            )
        if len(rows) == 1:
            yacht_id = str(rows[0][0])
            if model:
                conn.execute(
                    "UPDATE yacht SET model = COALESCE(model, %s) WHERE id = %s",
                    (model, yacht_id),
                )
            return yacht_id

    # No match → insert new yacht
    new_row = conn.execute(
        """
        INSERT INTO yacht
          (name, normalized_name, sail_number, normalized_sail_number, model)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (yacht_name, norm, sail_num, sail_norm, model),
    ).fetchone()
    counters.yachts_inserted += 1
    counters.yacht_scoring_yachts_upserted += 1
    return str(new_row[0])


def _resolve_or_insert_participant_ys(
    conn: psycopg.Connection,
    full_name: str,
    counters: RunCounters,
) -> str:
    """Resolve participant by normalized name only; raise on ambiguity; insert on no match."""
    name_norm = normalize_name(full_name)
    if name_norm:
        rows = conn.execute(
            "SELECT id FROM participant WHERE normalized_full_name = %s",
            (name_norm,),
        ).fetchall()
        if len(rows) > 1:
            raise AmbiguousMatchError(
                f"ambiguous_participant_match: name={name_norm!r} ({len(rows)} participants)"
            )
        if len(rows) == 1:
            counters.participants_matched_existing += 1
            return str(rows[0][0])

    pid = insert_participant(conn, full_name)
    counters.participants_inserted += 1
    counters.yacht_scoring_participants_upserted += 1
    return pid


# ---------------------------------------------------------------------------
# Asset processors (curated projection)
# ---------------------------------------------------------------------------

def _process_scraped_event_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_path: str,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    event_url = row.get("title-small href") or ""
    event_id = parse_ys_emenu_url(event_url)
    if not event_id:
        rejects.write(
            {**row,
             "_asset_type": "scraped_event_listing",
             "_source_file_path": source_file_path,
             "_reject_context": f"url={event_url!r}"},
            "unparseable_event_url",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    # Xref check — refresh last_seen_at and return if already mapped
    existing_instance_id = _xref_lookup_event(conn, event_id)
    if existing_instance_id:
        _xref_upsert_event(conn, event_id, existing_instance_id, False, counters)
        counters.yacht_scoring_rows_curated_processed += 1
        counters.yacht_scoring_events_upserted += 1
        return

    # Parse event attributes
    event_title = normalize_space(row.get("title-small")) or f"Event {event_id}"
    date_range = trim(row.get("w-[20%]"))
    season_year = _parse_year_from_text(date_range) or _parse_year_from_text(event_title)
    if season_year is None:
        rejects.write(
            {**row,
             "_asset_type": "scraped_event_listing",
             "_source_file_path": source_file_path,
             "_reject_context": f"event_id={event_id} date_range={date_range!r}"},
            "missing_season_year",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    # Upsert synthetic host club
    conn.execute(
        """
        INSERT INTO yacht_club (name, normalized_name, vitality_status)
        VALUES (%s, %s, 'unknown')
        ON CONFLICT (normalized_name) DO NOTHING
        """,
        (_YS_CLUB_NAME, _YS_CLUB_NORM),
    )
    club_row = conn.execute(
        "SELECT id FROM yacht_club WHERE normalized_name = %s",
        (_YS_CLUB_NORM,),
    ).fetchone()
    club_id = str(club_row[0])

    # Upsert event_series (strip leading year from title for series name)
    series_name = _strip_leading_year(event_title) or event_title
    series_norm = slug_name(series_name)
    conn.execute(
        """
        INSERT INTO event_series (yacht_club_id, name, normalized_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (yacht_club_id, normalized_name) DO NOTHING
        """,
        (club_id, series_name, series_norm),
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
        (event_series_id, event_title, season_year),
    ).fetchone()
    if instance_result:
        event_instance_id = str(instance_result[0])
    else:
        existing_inst = conn.execute(
            "SELECT id FROM event_instance WHERE event_series_id = %s AND season_year = %s",
            (event_series_id, season_year),
        ).fetchone()
        event_instance_id = str(existing_inst[0])

    _xref_upsert_event(conn, event_id, event_instance_id, True, counters)
    counters.yacht_scoring_rows_curated_processed += 1
    counters.yacht_scoring_events_upserted += 1


def _process_dedup_entry_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_path: str,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    # Parse event_id and entry_id from entryUrl
    entry_url = trim(row.get("entryUrl"))
    event_id, entry_id = parse_ys_boatdetail_url(entry_url)

    # Fallback: parse event_id from eventUrl
    if not event_id:
        event_url = trim(row.get("eventUrl"))
        event_id = parse_ys_entries_url(event_url)

    if not event_id:
        rejects.write(
            {**row,
             "_asset_type": "deduplicated_entry",
             "_source_file_path": source_file_path,
             "_reject_context": f"entryUrl={entry_url!r}"},
            "unparseable_event_url",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    # Resolve event_instance via xref
    event_instance_id = _xref_lookup_event(conn, event_id)
    if event_instance_id is None:
        rejects.write(
            {**row,
             "_asset_type": "deduplicated_entry",
             "_source_file_path": source_file_path,
             "_reject_context": f"event_id={event_id}"},
            "unresolved_event_link",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    # Idempotency: if entry xref already exists, refresh and skip
    if entry_id:
        existing_entry_id = _xref_lookup_entry(conn, event_id, entry_id)
        if existing_entry_id:
            _xref_upsert_entry(conn, event_id, entry_id, existing_entry_id, False, counters)
            counters.yacht_scoring_rows_curated_processed += 1
            counters.yacht_scoring_entries_upserted += 1
            return

    # Yacht resolution
    yacht_name = normalize_space(row.get("yachtName"))
    if not yacht_name:
        rejects.write(
            {**row,
             "_asset_type": "deduplicated_entry",
             "_source_file_path": source_file_path,
             "_reject_context": "blank_yacht_name"},
            "missing_required_identity",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return
    if not slug_name(yacht_name):
        rejects.write(
            {**row,
             "_asset_type": "deduplicated_entry",
             "_source_file_path": source_file_path,
             "_reject_context": f"unusable_yacht_name={yacht_name!r}"},
            "missing_required_identity",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    sail_num = trim(row.get("sailNumber"))
    boat_type = trim(row.get("yachtType"))

    yacht_source_key = _build_yacht_source_key(yacht_name, sail_num)
    yacht_id: str | None = None
    xref_yacht_existed = False

    if yacht_source_key:
        yacht_id = _xref_lookup_yacht(conn, yacht_source_key)
        xref_yacht_existed = yacht_id is not None

    if yacht_id is None:
        # May raise AmbiguousMatchError
        yacht_id = _resolve_or_insert_yacht_ys(conn, yacht_name, sail_num, boat_type, counters)

    if yacht_source_key:
        _xref_upsert_yacht(conn, yacht_source_key, yacht_id, not xref_yacht_existed, counters)

    # Participant resolution
    owner_name = normalize_space(row.get("ownerName"))
    participant_id: str | None = None
    if owner_name:
        affiliation = trim(row.get("ownerAffiliation"))
        location = trim(row.get("ownerLocation"))
        p_source_key = _build_participant_source_key(owner_name, affiliation, location)
        xref_p_existed = False

        if p_source_key:
            participant_id = _xref_lookup_participant(conn, p_source_key)
            xref_p_existed = participant_id is not None

        if participant_id is None:
            # May raise AmbiguousMatchError
            participant_id = _resolve_or_insert_participant_ys(conn, owner_name, counters)

        if p_source_key:
            _xref_upsert_participant(conn, p_source_key, participant_id, not xref_p_existed, counters)

        # Club affiliation → membership
        if affiliation and slug_name(affiliation):
            club_id = upsert_affiliate_club(conn, affiliation)
            upsert_membership(conn, participant_id, club_id, None, SOURCE_SYSTEM, counters)

    # Upsert event_entry
    event_entry_id = upsert_event_entry(
        conn,
        event_instance_id,
        yacht_id,
        entry_status="submitted",
        registration_source=SOURCE_SYSTEM,
        registration_external_id=entry_id,
        registered_at=None,
        counters=counters,
    )
    counters.yacht_scoring_entries_upserted += 1

    if entry_id:
        _xref_upsert_entry(conn, event_id, entry_id, event_entry_id, True, counters)

    # Entry participant link
    if participant_id:
        upsert_entry_participant(
            conn,
            event_entry_id,
            participant_id,
            role="owner_contact",
            participation_state="non_participating_contact",
            source_system=SOURCE_SYSTEM,
        )

    counters.yacht_scoring_rows_curated_processed += 1


def _process_unique_yacht_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_path: str,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    """Yacht enrichment only — resolves/creates yacht, does not create event entries."""
    yacht_name = normalize_space(row.get("yachtName"))
    if not yacht_name:
        rejects.write(
            {**row,
             "_asset_type": "unique_yacht",
             "_source_file_path": source_file_path,
             "_reject_context": "blank_yacht_name"},
            "missing_required_identity",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return
    if not slug_name(yacht_name):
        rejects.write(
            {**row,
             "_asset_type": "unique_yacht",
             "_source_file_path": source_file_path,
             "_reject_context": f"unusable_yacht_name={yacht_name!r}"},
            "missing_required_identity",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    sail_num = trim(row.get("sailNumber"))
    boat_type = trim(row.get("yachtType"))

    yacht_source_key = _build_yacht_source_key(yacht_name, sail_num)
    yacht_id: str | None = None
    xref_existed = False

    if yacht_source_key:
        yacht_id = _xref_lookup_yacht(conn, yacht_source_key)
        xref_existed = yacht_id is not None

    if yacht_id is None:
        # May raise AmbiguousMatchError
        yacht_id = _resolve_or_insert_yacht_ys(conn, yacht_name, sail_num, boat_type, counters)
    else:
        # Fill nulls only on existing yacht
        model = trim(boat_type)
        if model:
            conn.execute(
                "UPDATE yacht SET model = COALESCE(model, %s) WHERE id = %s",
                (model, yacht_id),
            )

    if yacht_source_key:
        _xref_upsert_yacht(conn, yacht_source_key, yacht_id, not xref_existed, counters)

    counters.yacht_scoring_rows_curated_processed += 1


def _process_scraped_entry_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    source_file_path: str,
    counters: RunCounters,
    rejects: RejectWriter,
    run_id: str,
) -> None:
    entry_url = trim(row.get("title-small href"))
    event_id, entry_id = parse_ys_boatdetail_url(entry_url)

    if not event_id:
        rejects.write(
            {**row,
             "_asset_type": "scraped_entry_listing",
             "_source_file_path": source_file_path,
             "_reject_context": f"url={entry_url!r}"},
            "unparseable_entry_url",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    # Resolve event_instance via xref
    event_instance_id = _xref_lookup_event(conn, event_id)
    if event_instance_id is None:
        rejects.write(
            {**row,
             "_asset_type": "scraped_entry_listing",
             "_source_file_path": source_file_path,
             "_reject_context": f"event_id={event_id}"},
            "unresolved_event_link",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    # Idempotency: if entry xref already exists (ingested via deduplicated_entries), refresh and skip
    if entry_id:
        existing_entry_id = _xref_lookup_entry(conn, event_id, entry_id)
        if existing_entry_id:
            _xref_upsert_entry(conn, event_id, entry_id, existing_entry_id, False, counters)
            counters.yacht_scoring_rows_curated_processed += 1
            counters.yacht_scoring_entries_upserted += 1
            return

    # New entry — resolve yacht
    # scraped_entry_listing field mapping:
    #   title-small       → sail number
    #   title-small 2     → yacht name
    #   flex              → owner name
    #   tablescraper-selected-row   → affiliation
    #   tablescraper-selected-row 2 → location
    #   tablescraper-selected-row 3 → yacht type
    yacht_name = normalize_space(row.get("title-small 2"))
    sail_num = trim(row.get("title-small"))
    boat_type = trim(row.get("tablescraper-selected-row 3"))

    if not yacht_name:
        rejects.write(
            {**row,
             "_asset_type": "scraped_entry_listing",
             "_source_file_path": source_file_path,
             "_reject_context": f"event_id={event_id} entry_id={entry_id}"},
            "missing_required_identity",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return
    if not slug_name(yacht_name):
        rejects.write(
            {**row,
             "_asset_type": "scraped_entry_listing",
             "_source_file_path": source_file_path,
             "_reject_context": f"unusable_yacht_name={yacht_name!r} event_id={event_id} entry_id={entry_id}"},
            "missing_required_identity",
        )
        counters.rows_rejected += 1
        counters.yacht_scoring_rows_curated_rejected += 1
        return

    yacht_source_key = _build_yacht_source_key(yacht_name, sail_num)
    yacht_id: str | None = None
    xref_yacht_existed = False

    if yacht_source_key:
        yacht_id = _xref_lookup_yacht(conn, yacht_source_key)
        xref_yacht_existed = yacht_id is not None

    if yacht_id is None:
        # May raise AmbiguousMatchError
        yacht_id = _resolve_or_insert_yacht_ys(conn, yacht_name, sail_num, boat_type, counters)

    if yacht_source_key:
        _xref_upsert_yacht(conn, yacht_source_key, yacht_id, not xref_yacht_existed, counters)

    # Participant resolution
    owner_name = normalize_space(row.get("flex"))
    participant_id: str | None = None

    if owner_name:
        affiliation = trim(row.get("tablescraper-selected-row"))
        location = trim(row.get("tablescraper-selected-row 2"))
        p_source_key = _build_participant_source_key(owner_name, affiliation, location)
        xref_p_existed = False

        if p_source_key:
            participant_id = _xref_lookup_participant(conn, p_source_key)
            xref_p_existed = participant_id is not None

        if participant_id is None:
            # May raise AmbiguousMatchError
            participant_id = _resolve_or_insert_participant_ys(conn, owner_name, counters)

        if p_source_key:
            _xref_upsert_participant(conn, p_source_key, participant_id, not xref_p_existed, counters)

        if affiliation and slug_name(affiliation):
            club_id = upsert_affiliate_club(conn, affiliation)
            upsert_membership(conn, participant_id, club_id, None, SOURCE_SYSTEM, counters)

    # Upsert event_entry
    event_entry_id = upsert_event_entry(
        conn,
        event_instance_id,
        yacht_id,
        entry_status="submitted",
        registration_source=SOURCE_SYSTEM,
        registration_external_id=entry_id,
        registered_at=None,
        counters=counters,
    )
    counters.yacht_scoring_entries_upserted += 1

    if entry_id:
        _xref_upsert_entry(conn, event_id, entry_id, event_entry_id, True, counters)

    if participant_id:
        upsert_entry_participant(
            conn,
            event_entry_id,
            participant_id,
            role="owner_contact",
            participation_state="non_participating_contact",
            source_system=SOURCE_SYSTEM,
        )

    counters.yacht_scoring_rows_curated_processed += 1


# ---------------------------------------------------------------------------
# Asset processor dispatch
# ---------------------------------------------------------------------------

_ASSET_PROCESSORS: dict[str, Callable] = {
    "scraped_event_listing": _process_scraped_event_row,
    "scraped_entry_listing": _process_scraped_entry_row,
    "deduplicated_entry":    _process_dedup_entry_row,
    "unique_yacht":          _process_unique_yacht_row,
}


# ---------------------------------------------------------------------------
# File streaming with two-savepoint pattern
# ---------------------------------------------------------------------------

def _stream_file(
    conn: psycopg.Connection,
    file_path: Path,
    asset_type: str,
    root: Path,
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
) -> None:
    """Stream one CSV file with two-savepoint pattern per row.

    Savepoint 1 (raw): captures raw row — persists even if curated fails.
    Savepoint 2 (cur): curated projection — rolled back on any error.
    """
    source_file_name = file_path.name
    source_file_path = str(file_path.relative_to(root))

    with file_path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        headers = {k.strip() for k in (reader.fieldnames or [])}

        # Validate headers; fall back to unknown if mismatch
        effective_asset_type = asset_type
        if not _check_headers(headers, asset_type):
            if asset_type != "unknown":
                counters.warnings.append(
                    f"[{run_id}] {source_file_name}: header mismatch for {asset_type!r}, treating as unknown"
                )
                effective_asset_type = "unknown"

        processor = _ASSET_PROCESSORS.get(effective_asset_type)

        for idx, raw_row in enumerate(reader):
            row = normalize_headers(raw_row)
            counters.rows_read += 1

            raw_payload_str = json.dumps(row, ensure_ascii=False, sort_keys=True)
            row_hash = hashlib.sha256(raw_payload_str.encode("utf-8")).hexdigest()
            source_event_id, source_entry_id = _extract_ids_for_raw(row, effective_asset_type)

            if effective_asset_type == "unknown":
                counters.yacht_scoring_unknown_schema_rows += 1

            # ── Savepoint 1: raw capture (persists regardless of curated outcome) ──
            sp_raw = f"ys_{idx}_r"
            conn.execute(f"SAVEPOINT {sp_raw}")
            try:
                _insert_raw_row(
                    conn, effective_asset_type, source_file_name, source_file_path,
                    idx + 1, source_event_id, source_entry_id, row_hash, raw_payload_str,
                    counters,
                )
                conn.execute(f"RELEASE SAVEPOINT {sp_raw}")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_raw}")
                counters.warnings.append(
                    f"[{run_id}] {source_file_name}[{idx}] raw capture failed: {exc}"
                )
                counters.db_phase_errors += 1
                continue  # skip curated if raw capture failed

            if processor is None:
                continue  # unknown schema — raw only

            # ── Savepoint 2: curated projection ──
            sp_cur = f"ys_{idx}_c"
            conn.execute(f"SAVEPOINT {sp_cur}")
            try:
                processor(conn, row, source_file_path, counters, rejects, run_id)
                conn.execute(f"RELEASE SAVEPOINT {sp_cur}")
            except AmbiguousMatchError as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_cur}")
                rejects.write(
                    {**row,
                     "_asset_type": effective_asset_type,
                     "_source_file_path": source_file_path,
                     "_reject_context": str(exc)},
                    "ambiguous_match",
                )
                counters.rows_rejected += 1
                counters.yacht_scoring_rows_curated_rejected += 1
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_cur}")
                rejects.write(
                    {**row,
                     "_asset_type": effective_asset_type,
                     "_source_file_path": source_file_path,
                     "_reject_context": str(exc)},
                    "db_constraint_error",
                )
                counters.warnings.append(
                    f"[{run_id}] {source_file_name}[{idx}]: {type(exc).__name__}: {exc}"
                )
                counters.rows_rejected += 1
                counters.yacht_scoring_rows_curated_rejected += 1
                counters.db_phase_errors += 1


# ---------------------------------------------------------------------------
# Main run entry point
# ---------------------------------------------------------------------------

def _run_yacht_scoring(
    run_id: str,
    started_at: str,
    db_dsn: str,
    counters: RunCounters,
    rejects: RejectWriter,
    yacht_scoring_dir: str,
    max_reject_rate: float,
    dry_run: bool,
) -> None:
    root = Path(yacht_scoring_dir)

    if not root.is_dir():
        click.echo(
            f"[{run_id}] FATAL: --yacht-scoring-dir {str(root)!r} does not exist or is not a directory",
            err=True,
        )
        sys.exit(1)

    ordered_files = _discover_files(root)
    if not ordered_files:
        click.echo(
            f"[{run_id}] FATAL: no CSV files found under {str(root)!r}",
            err=True,
        )
        sys.exit(1)

    click.echo(
        f"[{run_id}] Discovered {len(ordered_files)} CSV files under {root.name!r}"
    )

    conn = psycopg.connect(db_dsn, autocommit=False)
    try:
        if dry_run:
            try:
                for file_path, asset_type in ordered_files:
                    rel = file_path.relative_to(root)
                    click.echo(f"[{run_id}] [dry-run] Processing {rel} ({asset_type})")
                    _stream_file(conn, file_path, asset_type, root, run_id, counters, rejects)
            finally:
                conn.rollback()
                click.echo(f"[{run_id}] [dry-run] All changes rolled back.")

            if counters.db_phase_errors > 0:
                click.echo(
                    f"[{run_id}] [dry-run] {counters.db_phase_errors} DB error(s) — exiting non-zero",
                    err=True,
                )
                sys.exit(1)

            if counters.rows_read > 0:
                reject_rate = counters.yacht_scoring_rows_curated_rejected / counters.rows_read
                if reject_rate > max_reject_rate:
                    click.echo(
                        f"[{run_id}] [dry-run] Curated reject rate {reject_rate:.1%} exceeds maximum {max_reject_rate:.1%}",
                        err=True,
                    )
                    sys.exit(1)
        else:
            try:
                for file_path, asset_type in ordered_files:
                    rel = file_path.relative_to(root)
                    click.echo(f"[{run_id}] Processing {rel} ({asset_type})")
                    _stream_file(conn, file_path, asset_type, root, run_id, counters, rejects)
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
                    f"[{run_id}] FATAL: {counters.db_phase_errors} DB error(s); rolling back",
                    err=True,
                )
                sys.exit(1)

            # Reject rate gate — before commit so exceeding the threshold is fail-closed
            if counters.rows_read > 0:
                reject_rate = counters.yacht_scoring_rows_curated_rejected / counters.rows_read
                if reject_rate > max_reject_rate:
                    conn.rollback()
                    click.echo(
                        f"[{run_id}] Curated reject rate {reject_rate:.1%} exceeds maximum {max_reject_rate:.1%}; rolled back",
                        err=True,
                    )
                    sys.exit(1)

            conn.commit()
    finally:
        conn.close()
        rejects.close()

    click.echo(
        f"[{run_id}] Done: "
        f"raw={counters.yacht_scoring_rows_raw_inserted} "
        f"curated={counters.yacht_scoring_rows_curated_processed} "
        f"rejected={counters.yacht_scoring_rows_curated_rejected} "
        f"events={counters.yacht_scoring_events_upserted} "
        f"entries={counters.yacht_scoring_entries_upserted} "
        f"yachts={counters.yacht_scoring_yachts_upserted} "
        f"participants={counters.yacht_scoring_participants_upserted} "
        f"xref={counters.yacht_scoring_xref_inserted}"
    )
