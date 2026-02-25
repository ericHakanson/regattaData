"""regatta_etl.public_scrape

Adapter for ingesting public Regattaman scrape CSV files:
  - consolidated_regattaman_events_2021_2025_with_event_url.csv
  - all_regattaman_entries_2021_2025.csv

Design decisions (confirmed before implementation):
  - Unmatched entries_url  → synthesize_event (create minimal event_instance)
  - Existing participant/yacht attributes → fill nulls only
  - event_entry_participant.role → 'registrant'
  - Reject-rate threshold → 5% (configurable via --max-reject-rate)
"""

from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import click
import psycopg

from regatta_etl.normalize import (
    build_entry_hash,
    canonical_entries_url,
    extract_sku_from_hist,
    normalize_name,
    normalize_space,
    parse_race_url,
    slug_name,
    trim,
)
from regatta_etl.shared import (
    AmbiguousMatchError,
    RejectWriter,
    RunCounters,
    insert_participant,
    normalize_headers,
    resolve_or_insert_coowner_participant,
    resolve_or_insert_yacht_with_sail,
    resolve_participant_by_name,
    upsert_affiliate_club,
    upsert_entry_participant,
    upsert_event_context,
    upsert_event_entry,
)

# ---------------------------------------------------------------------------
# Column contracts
# ---------------------------------------------------------------------------

REQUIRED_EVENT_COLS = {"source", "Event Name", "entries_url", "Host Club"}
REQUIRED_ENTRY_COLS = {"source", "Event Name", "entries_url", "Fleet", "Name", "Yacht Name"}

REGISTRATION_SOURCE = "regattaman_public_scrape"

# ---------------------------------------------------------------------------
# Staging dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ScrapeEventRecord:
    source: str
    season_year: int
    event_name: str
    canonical_url: str       # race_id + yr only
    race_id: str
    yr: int
    host_club_name: str
    host_club_normalized: str
    event_url: str | None
    date_str: str | None


@dataclass
class ScrapeEntryRecord:
    source: str
    season_year: int
    event_name: str
    canonical_url: str
    race_id: str
    yr: int
    fleet: str
    full_name: str
    yacht_name: str
    city: str | None
    sail_num: str | None
    boat_type: str | None
    registration_external_id: str
    external_id_type: str  # 'sku' | 'hash'


# ---------------------------------------------------------------------------
# Row parsers (staging layer)
# ---------------------------------------------------------------------------

def _parse_source_year(source: str) -> int | None:
    """Extract year int from source tag like 'regattaman_2021'."""
    parts = source.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return None


def parse_event_row(
    row: dict[str, str],
    rejects: RejectWriter,
    counters: RunCounters,
) -> ScrapeEventRecord | None:
    """Parse and validate one row from the events CSV.

    Returns None and writes to rejects on validation failure.
    """
    source = trim(row.get("source")) or ""
    event_name = normalize_space(row.get("Event Name"))
    entries_url_raw = trim(row.get("entries_url"))
    host_club = normalize_space(row.get("Host Club"))

    # Required column check
    for col, val in [
        ("source", source),
        ("Event Name", event_name),
        ("entries_url", entries_url_raw),
        ("Host Club", host_club),
    ]:
        if not val:
            counters.rows_rejected += 1
            rejects.write(row, f"missing_required_column:{col}")
            return None

    # Parse race_id / yr from entries_url
    race_id, yr = parse_race_url(entries_url_raw)
    canon_url = canonical_entries_url(entries_url_raw)

    if race_id is None or yr is None or canon_url is None:
        # Non-race_id URLs (soc_id events) — skip; no entries will match them
        counters.events_skipped_no_race_id += 1
        return None

    # Validate season_year from source tag, and consistency with URL yr
    season_year = _parse_source_year(source)
    if season_year is None:
        counters.rows_rejected += 1
        rejects.write(row, f"invalid_year_or_race_id:source={source!r}")
        return None
    if season_year != yr:
        counters.rows_rejected += 1
        rejects.write(row, f"invalid_year_or_race_id:source_year={season_year},url_yr={yr}")
        return None

    return ScrapeEventRecord(
        source=source,
        season_year=season_year,
        event_name=event_name,  # type: ignore[arg-type]
        canonical_url=canon_url,
        race_id=race_id,
        yr=yr,
        host_club_name=host_club,  # type: ignore[arg-type]
        host_club_normalized=slug_name(host_club) or "",  # type: ignore[arg-type]
        event_url=trim(row.get("Event URL")),
        date_str=trim(row.get("Date")),
    )


def parse_entry_row(
    row: dict[str, str],
    rejects: RejectWriter,
    counters: RunCounters,
) -> ScrapeEntryRecord | None:
    """Parse and validate one row from the entries CSV."""
    source = trim(row.get("source")) or ""
    event_name = normalize_space(row.get("Event Name"))
    entries_url_raw = trim(row.get("entries_url"))
    fleet = normalize_space(row.get("Fleet"))
    full_name = normalize_space(row.get("Name"))
    yacht_name = normalize_space(row.get("Yacht Name"))

    # Required column check
    for col, val in [
        ("source", source),
        ("Event Name", event_name),
        ("entries_url", entries_url_raw),
        ("Name", full_name),
    ]:
        if not val:
            counters.rows_rejected += 1
            rejects.write(row, f"missing_required_column:{col}")
            return None

    # Blank yacht name → reject
    if not yacht_name:
        counters.rows_rejected += 1
        rejects.write(row, "missing_required_column:Yacht Name")
        return None

    # Yacht name must produce a non-null slug (rejects names like "-", "……/)…")
    if not slug_name(yacht_name):
        counters.rows_rejected += 1
        rejects.write(row, "invalid_yacht_name")
        return None

    # Name must produce a non-null normalized form (rejects pure-punctuation values)
    if not normalize_name(full_name):
        counters.rows_rejected += 1
        rejects.write(row, "missing_required_column:Name")
        return None

    # Parse race_id / yr
    race_id, yr = parse_race_url(entries_url_raw)
    canon_url = canonical_entries_url(entries_url_raw)

    if race_id is None or yr is None or canon_url is None:
        counters.rows_rejected += 1
        rejects.write(row, f"invalid_year_or_race_id:entries_url={entries_url_raw!r}")
        return None

    season_year = _parse_source_year(source)
    if season_year is None:
        counters.rows_rejected += 1
        rejects.write(row, f"invalid_year_or_race_id:source={source!r}")
        return None
    if season_year != yr:
        counters.rows_rejected += 1
        rejects.write(row, f"invalid_year_or_race_id:source_year={season_year},url_yr={yr}")
        return None

    # Entry identity
    sku = extract_sku_from_hist(row.get("Hist"))
    if sku:
        reg_ext_id = sku
        ext_id_type = "sku"
    else:
        reg_ext_id = build_entry_hash(
            source,
            canon_url,
            fleet or "",
            full_name or "",  # type: ignore[arg-type]
            yacht_name or "",  # type: ignore[arg-type]
            trim(row.get("Sail Num")) or "",
        )
        ext_id_type = "hash"

    return ScrapeEntryRecord(
        source=source,
        season_year=season_year,
        event_name=event_name,  # type: ignore[arg-type]
        canonical_url=canon_url,
        race_id=race_id,
        yr=yr,
        fleet=fleet or "",
        full_name=full_name,  # type: ignore[arg-type]
        yacht_name=yacht_name,  # type: ignore[arg-type]
        city=trim(row.get("City")),
        sail_num=trim(row.get("Sail Num")),
        boat_type=trim(row.get("Boat Type")),
        registration_external_id=reg_ext_id,
        external_id_type=ext_id_type,
    )


# ---------------------------------------------------------------------------
# Load phases
# ---------------------------------------------------------------------------

def load_events(
    path: Path,
    rejects: RejectWriter,
    counters: RunCounters,
) -> tuple[list[ScrapeEventRecord], dict[str, ScrapeEventRecord]]:
    """Parse events file; return (records, {canonical_url: record}).

    Only rows with parseable race_id/yr are included (soc_id events are skipped).
    """
    records: list[ScrapeEventRecord] = []
    url_map: dict[str, ScrapeEventRecord] = {}

    with path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        raw_fields = reader.fieldnames or []
        norm_fields = {k.strip() for k in raw_fields}

        missing = REQUIRED_EVENT_COLS - norm_fields
        if missing:
            click.echo(
                f"FATAL: events file missing required columns: {sorted(missing)}",
                err=True,
            )
            sys.exit(1)

        for raw_row in reader:
            row = normalize_headers(raw_row)
            counters.rows_read += 1
            rec = parse_event_row(row, rejects, counters)
            if rec is None:
                continue
            records.append(rec)
            url_map[rec.canonical_url] = rec

    return records, url_map


def load_entries(
    path: Path,
    event_url_map: dict[str, ScrapeEventRecord],
    rejects: RejectWriter,
    counters: RunCounters,
    synthesize_events: bool,
) -> tuple[list[ScrapeEntryRecord], dict[str, ScrapeEventRecord]]:
    """Parse entries file.

    Returns (valid_entry_records, updated_event_url_map).
    Unmatched entry URLs either synthesize a minimal event record or are rejected.
    Collision detection: if the same registration_external_id appears twice, the
    second occurrence is rejected with 'entry_identity_collision'.
    """
    records: list[ScrapeEntryRecord] = []
    seen_ext_ids: dict[str, int] = {}   # ext_id → first occurrence row index
    synthesized: dict[str, ScrapeEventRecord] = {}

    with path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        raw_fields = reader.fieldnames or []
        norm_fields = {k.strip() for k in raw_fields}

        missing = REQUIRED_ENTRY_COLS - norm_fields
        if missing:
            click.echo(
                f"FATAL: entries file missing required columns: {sorted(missing)}",
                err=True,
            )
            sys.exit(1)

        for raw_row in reader:
            row = normalize_headers(raw_row)
            counters.rows_read += 1
            rec = parse_entry_row(row, rejects, counters)
            if rec is None:
                continue

            # Collision detection on registration_external_id
            if rec.registration_external_id in seen_ext_ids:
                counters.rows_rejected += 1
                rejects.write(row, f"entry_identity_collision:{rec.registration_external_id}")
                continue
            seen_ext_ids[rec.registration_external_id] = len(records)

            # Event linkage
            if rec.canonical_url not in event_url_map:
                counters.unmatched_entry_urls += 1
                if not synthesize_events:
                    counters.rows_rejected += 1
                    rejects.write(row, f"event_link_not_found:{rec.canonical_url}")
                    continue
                # Synthesize minimal event record
                if rec.canonical_url not in synthesized:
                    synth = ScrapeEventRecord(
                        source=rec.source,
                        season_year=rec.season_year,
                        event_name=rec.event_name,
                        canonical_url=rec.canonical_url,
                        race_id=rec.race_id,
                        yr=rec.yr,
                        host_club_name="Unknown Club",
                        host_club_normalized="unknown-club",
                        event_url=None,
                        date_str=None,
                    )
                    synthesized[rec.canonical_url] = synth
                    event_url_map[rec.canonical_url] = synth

            records.append(rec)

    # Report synthesized events
    if synthesized:
        click.echo(
            f"  Synthesized {len(synthesized)} event record(s) for unmatched entry URLs."
        )

    return records, event_url_map


# ---------------------------------------------------------------------------
# DB apply layer — events
# ---------------------------------------------------------------------------

def _upsert_scrape_event(
    conn: psycopg.Connection,
    rec: ScrapeEventRecord,
    source_system: str,
    counters: RunCounters,
) -> str:
    """Upsert host club, event_series, event_instance for a scrape event.

    Returns event_instance_id.
    """
    _, _, instance_id = upsert_event_context(
        conn,
        rec.host_club_name,
        rec.host_club_normalized,
        rec.event_name,
        slug_name(rec.event_name) or rec.event_name.lower().replace(" ", "-"),
        rec.event_name,  # display_name = event_name for public scrape
        rec.season_year,
        None,            # no registration_open_at in public scrape
    )
    counters.events_upserted += 1
    return instance_id


# ---------------------------------------------------------------------------
# DB apply layer — entries
# ---------------------------------------------------------------------------

def _process_entry(
    conn: psycopg.Connection,
    rec: ScrapeEntryRecord,
    event_instance_id: str,
    source_system: str,
    counters: RunCounters,
) -> None:
    """Upsert participant, yacht, event_entry, event_entry_participant for one entry."""

    # Participant — name-only resolution for public scrape
    name_norm = normalize_name(rec.full_name)
    pid: str | None = None
    if name_norm:
        pid = resolve_participant_by_name(conn, name_norm)

    if pid is not None:
        counters.participants_matched_existing += 1
    else:
        pid = insert_participant(conn, rec.full_name)
        counters.participants_inserted += 1

    # Yacht
    yacht_id = resolve_or_insert_yacht_with_sail(
        conn,
        rec.yacht_name,
        rec.sail_num,
        rec.boat_type,
        event_instance_id,
        rec.registration_external_id,
        counters,
    )

    # Event entry
    event_entry_id = upsert_event_entry(
        conn,
        event_instance_id,
        yacht_id,
        "submitted",   # public scrape has no payment signal → submitted
        REGISTRATION_SOURCE,
        rec.registration_external_id,
        None,          # no registered_at timestamp in public scrape
        counters,
    )

    # Event entry participant (role=registrant per design decision)
    upsert_entry_participant(
        conn,
        event_entry_id,
        pid,
        role="registrant",
        participation_state="non_participating_contact",
        source_system=source_system,
    )


# ---------------------------------------------------------------------------
# Top-level run function
# ---------------------------------------------------------------------------

def run_public_scrape(
    conn: psycopg.Connection,
    entries_path: Path,
    events_path: Path,
    source_system: str,
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
    dry_run: bool,
    max_reject_rate: float,
    synthesize_events: bool,
) -> None:
    """End-to-end public scrape ingestion.

    Phase 1: Load + validate events file → build canonical_url → event record map.
    Phase 2: Load + validate entries file → link to events.
    Phase 3: Upsert events (batch transaction).
    Phase 4: Upsert entries (per-row transactions).
    """
    click.echo(f"[{run_id}] Loading events file: {events_path.name}")
    event_records, event_url_map = load_events(events_path, rejects, counters)
    click.echo(f"[{run_id}]   {len(event_records)} parseable event rows")

    click.echo(f"[{run_id}] Loading entries file: {entries_path.name}")
    entry_records, event_url_map = load_entries(
        entries_path, event_url_map, rejects, counters, synthesize_events
    )
    click.echo(
        f"[{run_id}]   {len(entry_records)} valid entry rows, "
        f"{counters.rows_rejected} rejected so far"
    )

    # Reject-rate guard
    total_rows = counters.rows_read
    if total_rows > 0 and (counters.rows_rejected / total_rows) > max_reject_rate:
        click.echo(
            f"[{run_id}] FATAL: reject rate "
            f"{counters.rows_rejected}/{total_rows} exceeds threshold {max_reject_rate:.1%}",
            err=True,
        )
        sys.exit(1)

    click.echo(f"[{run_id}] Starting DB phase (dry_run={dry_run})")

    if dry_run:
        _run_dry_scrape(
            conn, event_records, entry_records, event_url_map,
            source_system, run_id, counters, rejects,
        )
    else:
        _run_real_scrape(
            conn, event_records, entry_records, event_url_map,
            source_system, run_id, counters, rejects,
        )


def _run_real_scrape(
    conn: psycopg.Connection,
    event_records: list[ScrapeEventRecord],
    entry_records: list[ScrapeEntryRecord],
    event_url_map: dict[str, ScrapeEventRecord],
    source_system: str,
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
) -> None:
    # Upsert all event contexts in one batch transaction
    url_to_instance_id: dict[str, str] = {}
    with conn.transaction():
        for ev_rec in event_records:
            instance_id = _upsert_scrape_event(conn, ev_rec, source_system, counters)
            url_to_instance_id[ev_rec.canonical_url] = instance_id

    click.echo(f"[{run_id}] Events upserted: {counters.events_upserted}")

    # Per-entry transactions
    for entry_rec in entry_records:
        event_instance_id = url_to_instance_id.get(entry_rec.canonical_url)
        if event_instance_id is None:
            # Synthesized event: upsert now
            with conn.transaction():
                synth_ev = event_url_map[entry_rec.canonical_url]
                event_instance_id = _upsert_scrape_event(
                    conn, synth_ev, source_system, counters
                )
                url_to_instance_id[entry_rec.canonical_url] = event_instance_id

        try:
            with conn.transaction():
                _process_entry(
                    conn, entry_rec, event_instance_id, source_system, counters
                )
        except Exception as exc:
            rejects.write(
                _entry_to_row(entry_rec),
                f"db_constraint_error:{exc}",
            )
            counters.rows_rejected += 1
            counters.db_phase_errors += 1

    click.echo(
        f"[{run_id}] Entries processed: {counters.entries_upserted} upserted, "
        f"{counters.db_phase_errors} DB errors"
    )


def _run_dry_scrape(
    conn: psycopg.Connection,
    event_records: list[ScrapeEventRecord],
    entry_records: list[ScrapeEntryRecord],
    event_url_map: dict[str, ScrapeEventRecord],
    source_system: str,
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
) -> None:
    """All writes inside one outer transaction; rolled back at end."""
    conn.autocommit = False
    try:
        url_to_instance_id: dict[str, str] = {}

        # Events batch
        sp = "events_batch"
        conn.execute(f"SAVEPOINT {sp}")
        try:
            for ev_rec in event_records:
                instance_id = _upsert_scrape_event(
                    conn, ev_rec, source_system, counters
                )
                url_to_instance_id[ev_rec.canonical_url] = instance_id
            conn.execute(f"RELEASE SAVEPOINT {sp}")
            click.echo(f"[{run_id}] [dry-run] Events batch valid: {counters.events_upserted}")
        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            click.echo(f"[{run_id}] [dry-run] FATAL: events batch failed: {exc}", err=True)
            counters.db_phase_errors += 1
            return

        # Per-entry savepoints
        for idx, entry_rec in enumerate(entry_records):
            event_instance_id = url_to_instance_id.get(entry_rec.canonical_url)
            if event_instance_id is None:
                sp2 = f"synth_{idx}"
                conn.execute(f"SAVEPOINT {sp2}")
                try:
                    synth_ev = event_url_map[entry_rec.canonical_url]
                    event_instance_id = _upsert_scrape_event(
                        conn, synth_ev, source_system, counters
                    )
                    url_to_instance_id[entry_rec.canonical_url] = event_instance_id
                    conn.execute(f"RELEASE SAVEPOINT {sp2}")
                except Exception as exc:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {sp2}")
                    rejects.write(
                        _entry_to_row(entry_rec),
                        f"db_constraint_error:{exc}",
                    )
                    counters.rows_rejected += 1
                    counters.db_phase_errors += 1
                    continue

            sp_name = f"entry_{idx}"
            conn.execute(f"SAVEPOINT {sp_name}")
            try:
                _process_entry(
                    conn, entry_rec, event_instance_id, source_system, counters
                )
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                rejects.write(
                    _entry_to_row(entry_rec),
                    f"db_constraint_error:{exc}",
                )
                counters.rows_rejected += 1
                counters.db_phase_errors += 1
                counters.warnings.append(f"[dry-run] entry {idx}: {exc}")

    finally:
        conn.rollback()
        click.echo(f"[{run_id}] [dry-run] All changes rolled back.")


def _entry_to_row(rec: ScrapeEntryRecord) -> dict[str, str]:
    """Convert a ScrapeEntryRecord back to a dict for reject writing."""
    return {
        "source": rec.source,
        "Event Name": rec.event_name,
        "entries_url": rec.canonical_url,
        "Fleet": rec.fleet,
        "Name": rec.full_name,
        "Yacht Name": rec.yacht_name,
        "City": rec.city or "",
        "Sail Num": rec.sail_num or "",
        "Boat Type": rec.boat_type or "",
        "registration_external_id": rec.registration_external_id,
        "external_id_type": rec.external_id_type,
    }
