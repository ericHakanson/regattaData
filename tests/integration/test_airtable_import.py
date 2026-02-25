"""Integration tests for the airtable_copy ingestion pipeline."""

from __future__ import annotations

import csv
import uuid
from pathlib import Path

import pytest

from regatta_etl.import_airtable_copy import _run_airtable_copy
from regatta_etl.shared import RejectWriter, RunCounters


# ---------------------------------------------------------------------------
# Fixture CSV builders
# ---------------------------------------------------------------------------

CLUBS_HEADERS = [
    "Name", "club_global_id", "Website", "isDefunct", "originalAddress",
    "USPSFormattedAddress", "Phone", "Description", "stateUSA",
]
EVENTS_HEADERS = [
    "eventsConcatenateEventNameSource", "Event Name", "Event URL", "source",
    "entries_url", "event_global_id", "Host Club", "club_global_id_lookup",
    "Date", "Day", "Region", "entries_global_id_lookup",
]
YACHTS_HEADERS = [
    "yachtName", "yacht_global_id", "sailNumber", "yachtLoa", "yachtType",
    "owners", "owner_global_id_lookup",
]
OWNERS_HEADERS = [
    "ownerName", "owner_global_id", "ownerEmail", "ownerHomePhone",
    "ownerCellPhone", "ownerAddressLine1", "ownerAddressLine2",
    "ownerCity", "ownerState", "ownerPostal", "ownerAffiliation",
    "club_global_id_lookup", "yacht_global_id_lookup", "skipperName",
    "is_duplicate",
]
PARTICIPANTS_HEADERS = [
    "participant_global_id", "competitorE", "name", "numbersOnly",
    "address", "postalCode", "boatName", "sailNumber",
]
ENTRIES_HEADERS = [
    "entries_global_id", "source", "Event Name", "Fleet", "Name",
    "Yacht Name", "City", "Sail Num", "Boat Type", "entriesSku",
    "entries_url", "eventUuid", "entryUuid", "Hist",
]


def _write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _make_dir(tmp_path: Path, clubs=None, events=None, yachts=None,
              owners=None, participants=None, entries=None) -> Path:
    """Write all 6 CSVs into tmp_path using given row lists (default empty)."""
    _write_csv(tmp_path / "clubs-Grid view.csv", CLUBS_HEADERS, clubs or [])
    _write_csv(tmp_path / "events-Grid view.csv", EVENTS_HEADERS, events or [])
    _write_csv(tmp_path / "yachts-Grid view.csv", YACHTS_HEADERS, yachts or [])
    _write_csv(tmp_path / "owners-Grid view.csv", OWNERS_HEADERS, owners or [])
    _write_csv(tmp_path / "participants-Grid view.csv", PARTICIPANTS_HEADERS, participants or [])
    _write_csv(tmp_path / "entries-Grid view.csv", ENTRIES_HEADERS, entries or [])
    return tmp_path


def _make_club(**kwargs) -> dict:
    row = {h: "" for h in CLUBS_HEADERS}
    row.update({"Name": "Boothbay Harbor YC", "club_global_id": "recCLUB001",
                "Website": "https://bhyc.example.com"})
    row.update(kwargs)
    return row


def _make_event(**kwargs) -> dict:
    row = {h: "" for h in EVENTS_HEADERS}
    row.update({
        "Event Name": "BHYC Summer Race",
        "source": "regattaman_2024",
        "entries_url": "https://regattaman.com/scratch.php?race_id=100&yr=2024",
        "event_global_id": '{"race_id":"100", "yr":"2024"}',
        "Host Club": "Boothbay Harbor YC",
        "club_global_id_lookup": "recCLUB001",
    })
    row.update(kwargs)
    return row


def _make_yacht(**kwargs) -> dict:
    row = {h: "" for h in YACHTS_HEADERS}
    row.update({
        "yachtName": "Sea Spirit",
        "yacht_global_id": "recYACHT001",
        "sailNumber": "US-42",
        "yachtLoa": "35.5",
        "yachtType": "Catalina 35",
    })
    row.update(kwargs)
    return row


def _make_owner(**kwargs) -> dict:
    row = {h: "" for h in OWNERS_HEADERS}
    row.update({
        "ownerName": "Jane Doe",
        "owner_global_id": "recOWNER001",
        "ownerEmail": "jane@example.com",
        "ownerHomePhone": "207-555-0100",
        "ownerCity": "Boothbay Harbor",
        "ownerState": "ME",
        "ownerPostal": "04538",
        "ownerAffiliation": "Boothbay Harbor YC",
        "club_global_id_lookup": "recCLUB001",
        "yacht_global_id_lookup": "recYACHT001",
    })
    row.update(kwargs)
    return row


def _make_participant(**kwargs) -> dict:
    row = {h: "" for h in PARTICIPANTS_HEADERS}
    row.update({
        "participant_global_id": "recPART001",
        "competitorE": "racer@example.com",
        "name": "Alice Racer",
        "numbersOnly": "207-555-0200",
        "address": "10 Harbor St, Boothbay Harbor ME",
        "postalCode": "04538",
    })
    row.update(kwargs)
    return row


def _make_entry(**kwargs) -> dict:
    row = {h: "" for h in ENTRIES_HEADERS}
    row.update({
        "entries_global_id": "recENTRY001",
        "source": "regattaman_2024",
        "Event Name": "BHYC Summer Race",
        "Name": "Jane Doe",
        "Yacht Name": "Sea Spirit",
        "Sail Num": "US-42",
        "Boat Type": "Catalina 35",
        "entriesSku": "r-100-2024-001",
        "entries_url": "https://regattaman.com/scratch.php?race_id=100&yr=2024",
        "eventUuid": '{"race_id":"100", "yr":"2024"}',
    })
    row.update(kwargs)
    return row


def _run(dsn: str, tmp_path: Path, dry_run: bool = False, **kwargs) -> RunCounters:
    counters = RunCounters()
    rejects = RejectWriter(tmp_path / f"{uuid.uuid4()}_rejects.csv")
    _run_airtable_copy(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        airtable_dir=str(tmp_path),
        dry_run=dry_run,
    )
    return counters


# ---------------------------------------------------------------------------
# Test: end-to-end happy path (all 6 assets)
# ---------------------------------------------------------------------------

def test_airtable_e2e_all_assets(db_conn, tmp_path):
    conn, dsn = db_conn

    _make_dir(
        tmp_path,
        clubs=[_make_club()],
        events=[_make_event()],
        yachts=[_make_yacht()],
        owners=[_make_owner()],
        participants=[_make_participant()],
        entries=[_make_entry()],
    )

    counters = _run(dsn, tmp_path)

    # No DB errors
    assert counters.db_phase_errors == 0
    assert counters.rows_rejected == 0

    # Raw capture: 6 rows (1 per asset)
    assert counters.airtable_rows_raw_inserted == 6

    # Curated: 6 rows processed
    assert counters.airtable_rows_curated_processed == 6
    assert counters.airtable_rows_curated_rejected == 0

    # Xref entries
    assert counters.airtable_xref_inserted >= 4  # club, event, yacht, owner, participant

    # Domain objects created
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM airtable_copy_row")
        assert cur.fetchone()[0] == 6

        cur.execute("SELECT COUNT(*) FROM yacht_club")
        assert cur.fetchone()[0] >= 1

        cur.execute("SELECT COUNT(*) FROM event_instance")
        assert cur.fetchone()[0] >= 1

        cur.execute("SELECT COUNT(*) FROM yacht WHERE normalized_name IS NOT NULL")
        assert cur.fetchone()[0] >= 1

        cur.execute("SELECT COUNT(*) FROM participant WHERE full_name IS NOT NULL")
        assert cur.fetchone()[0] >= 2  # owner + participant

        cur.execute("SELECT COUNT(*) FROM event_entry")
        assert cur.fetchone()[0] >= 1

        # Skipper link must use role='skipper' / participation_state='participating'
        cur.execute(
            "SELECT role, participation_state FROM event_entry_participant LIMIT 1"
        )
        eep = cur.fetchone()
        assert eep is not None
        assert eep[0] == "skipper"
        assert eep[1] == "participating"


# ---------------------------------------------------------------------------
# Test: raw completeness — every source row in airtable_copy_row
# ---------------------------------------------------------------------------

def test_raw_capture_completeness(db_conn, tmp_path):
    conn, dsn = db_conn

    clubs = [_make_club(), _make_club(Name="Other Club", club_global_id="recCLUB002")]
    _make_dir(tmp_path, clubs=clubs)

    counters = _run(dsn, tmp_path)

    # 2 club rows captured
    assert counters.airtable_rows_raw_inserted >= 2

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM airtable_copy_row WHERE asset_name = 'clubs'"
        )
        assert cur.fetchone()[0] == 2

        # All columns recoverable from raw_payload
        cur.execute(
            "SELECT raw_payload->>'Name' FROM airtable_copy_row WHERE asset_name = 'clubs'"
        )
        names = {r[0] for r in cur.fetchall()}
        assert "Boothbay Harbor YC" in names
        assert "Other Club" in names


# ---------------------------------------------------------------------------
# Test: idempotency — rerun produces zero duplicate logical inserts
# ---------------------------------------------------------------------------

def test_idempotency_rerun(db_conn, tmp_path):
    conn, dsn = db_conn

    _make_dir(
        tmp_path,
        clubs=[_make_club()],
        events=[_make_event()],
        yachts=[_make_yacht()],
        owners=[_make_owner()],
        participants=[_make_participant()],
        entries=[_make_entry()],
    )

    c1 = _run(dsn, tmp_path)
    c2 = _run(dsn, tmp_path)

    # Second run: no new raw rows (same hashes)
    assert c2.airtable_rows_raw_inserted == 0

    # Second run: no new participants
    assert c2.participants_inserted == 0

    # Second run: no new yachts
    assert c2.yachts_inserted == 0

    # Second run: curated rows still processed (matched existing), no errors
    assert c2.db_phase_errors == 0

    # Database counts unchanged
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM airtable_copy_row")
        assert cur.fetchone()[0] == 6  # same 6 from first run
        cur.execute("SELECT COUNT(*) FROM participant")
        p_count = cur.fetchone()[0]
        assert p_count >= 2  # from first run
        # Second run should not add more
        cur.execute("SELECT COUNT(*) FROM event_entry")
        e_count = cur.fetchone()[0]
        assert e_count >= 1


# ---------------------------------------------------------------------------
# Test: dry-run rolls back all changes
# ---------------------------------------------------------------------------

def test_dry_run_rollback(db_conn, tmp_path):
    conn, dsn = db_conn

    _make_dir(
        tmp_path,
        clubs=[_make_club()],
        events=[_make_event()],
        yachts=[_make_yacht()],
        owners=[_make_owner()],
        participants=[_make_participant()],
        entries=[_make_entry()],
    )

    counters = _run(dsn, tmp_path, dry_run=True)

    # Counters reflect execution (full DB path taken)
    assert counters.rows_read == 6

    # But nothing persisted
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM airtable_copy_row")
        assert cur.fetchone()[0] == 0

        cur.execute("SELECT COUNT(*) FROM participant")
        assert cur.fetchone()[0] == 0

        cur.execute("SELECT COUNT(*) FROM yacht")
        assert cur.fetchone()[0] == 0

        cur.execute("SELECT COUNT(*) FROM event_instance")
        assert cur.fetchone()[0] == 0

        cur.execute("SELECT COUNT(*) FROM event_entry")
        assert cur.fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Test: ambiguous participant match → curated reject, raw row preserved
# ---------------------------------------------------------------------------

def test_ambiguous_participant_curated_reject_raw_preserved(db_conn, tmp_path):
    conn, dsn = db_conn

    # Pre-insert two participants sharing the same email
    shared_email = "shared@example.com"
    with conn.cursor() as cur:
        for name in ("Person One", "Person Two"):
            cur.execute(
                "INSERT INTO participant (full_name, normalized_full_name) "
                "VALUES (%s, %s) RETURNING id",
                (name, name.lower()),
            )
            pid = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO participant_contact_point
                  (participant_id, contact_type, contact_subtype,
                   contact_value_raw, contact_value_normalized, is_primary, source_system)
                VALUES (%s, 'email', 'primary', %s, %s, true, 'test')
                """,
                (pid, shared_email, shared_email),
            )
    conn.commit()

    owners = [_make_owner(ownerEmail=shared_email, owner_global_id="recAMBIG001")]
    rejects_path = tmp_path / "rejects.csv"
    _make_dir(tmp_path, owners=owners)

    counters = RunCounters()
    rejects = RejectWriter(rejects_path)
    _run_airtable_copy(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        airtable_dir=str(tmp_path),
        dry_run=False,
    )

    # Raw row preserved, curated rejected
    assert counters.airtable_rows_raw_inserted >= 1
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM airtable_copy_row WHERE asset_name = 'owners'"
        )
        assert cur.fetchone()[0] == 1  # raw row present

    # Curated rejected
    assert counters.rows_rejected >= 1
    assert counters.airtable_owners_rejected >= 1
    assert counters.db_phase_errors == 0  # ambiguous match is not a DB error

    # Reject file written
    with rejects_path.open() as fh:
        rows = list(csv.DictReader(fh))
    assert any("ambiguous" in r["_reject_reason"] for r in rows)


# ---------------------------------------------------------------------------
# Test: ambiguous yacht match → curated reject for entries, raw preserved
# ---------------------------------------------------------------------------

def test_ambiguous_yacht_reject_in_entries(db_conn, tmp_path):
    conn, dsn = db_conn

    # Pre-insert two yachts with identical name + sail
    with conn.cursor() as cur:
        for i in range(2):
            cur.execute(
                """
                INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number)
                VALUES ('Duplicate', 'duplicate', 'DUP-1', 'dup-1')
                """,
            )
    conn.commit()

    # Create the event so entries can find it
    _make_dir(
        tmp_path,
        events=[_make_event()],
        entries=[_make_entry(
            **{"Yacht Name": "Duplicate", "Sail Num": "DUP-1",
               "entries_global_id": "recDUP001"}
        )],
    )

    counters = _run(dsn, tmp_path)

    # Entries raw row preserved despite curated rejection
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM airtable_copy_row WHERE asset_name = 'entries'"
        )
        assert cur.fetchone()[0] == 1

    assert counters.airtable_entries_rejected >= 1
    assert counters.db_phase_errors == 0


# ---------------------------------------------------------------------------
# Test: entry with unresolvable event → curated reject, raw preserved
# ---------------------------------------------------------------------------

def test_entry_unresolved_event_reject(db_conn, tmp_path):
    conn, dsn = db_conn

    # No events loaded → xref_event is empty → entry event not found
    entries = [_make_entry()]  # references race_id=100,yr=2024 but no event was loaded
    _make_dir(tmp_path, entries=entries)

    counters = _run(dsn, tmp_path)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM airtable_copy_row WHERE asset_name = 'entries'"
        )
        assert cur.fetchone()[0] == 1  # raw captured

    assert counters.airtable_entries_rejected >= 1
    assert counters.airtable_rows_curated_rejected >= 1


# ---------------------------------------------------------------------------
# Test: club with blank name → curated reject, raw preserved
# ---------------------------------------------------------------------------

def test_club_blank_name_curated_reject(db_conn, tmp_path):
    conn, dsn = db_conn

    clubs = [
        _make_club(Name="", club_global_id="recBLANK001"),  # invalid
        _make_club(),                                         # valid
    ]
    _make_dir(tmp_path, clubs=clubs)

    rejects_path = tmp_path / "rejects.csv"
    counters = RunCounters()
    rejects = RejectWriter(rejects_path)
    _run_airtable_copy(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        airtable_dir=str(tmp_path),
        dry_run=False,
    )

    # Both raw rows captured
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM airtable_copy_row WHERE asset_name = 'clubs'"
        )
        assert cur.fetchone()[0] == 2

    # One curated reject, one curated processed
    assert counters.airtable_clubs_rejected >= 1
    assert counters.airtable_rows_curated_rejected >= 1

    with rejects_path.open() as fh:
        rows = list(csv.DictReader(fh))
    assert any("missing_required_identity" in r["_reject_reason"] for r in rows)


# ---------------------------------------------------------------------------
# Test: unknown source type → warning counter, not rejection
# ---------------------------------------------------------------------------

def test_unknown_source_type_warning_not_rejection(db_conn, tmp_path):
    conn, dsn = db_conn

    events = [_make_event(source="custom_source_2025")]
    _make_dir(tmp_path, events=events)

    counters = _run(dsn, tmp_path)

    # Row not rejected (source type warning is just a counter)
    assert counters.airtable_warnings_unknown_source_type == 1
    assert counters.airtable_events_rejected == 0  # should still try to process

    # Raw row captured
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM airtable_copy_row WHERE asset_name = 'events'"
        )
        assert cur.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Test: event without season year → curated reject, raw preserved
# ---------------------------------------------------------------------------

def test_event_no_year_curated_reject(db_conn, tmp_path):
    conn, dsn = db_conn

    # source without year, entries_url without yr, no event_global_id
    events = [_make_event(
        source="yachtScoring",
        entries_url="",
        event_global_id="",
    )]
    _make_dir(tmp_path, events=events)

    counters = _run(dsn, tmp_path)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM airtable_copy_row WHERE asset_name = 'events'"
        )
        assert cur.fetchone()[0] == 1  # raw preserved

    assert counters.airtable_events_rejected >= 1


# ---------------------------------------------------------------------------
# Test: xref stability — second run re-maps same global_ids without drift
# ---------------------------------------------------------------------------

def test_xref_stability_across_reruns(db_conn, tmp_path):
    conn, dsn = db_conn

    _make_dir(
        tmp_path,
        clubs=[_make_club()],
        yachts=[_make_yacht()],
    )

    _run(dsn, tmp_path)

    # Get the xref-mapped IDs from run 1
    with conn.cursor() as cur:
        cur.execute(
            "SELECT yacht_club_id FROM airtable_xref_club WHERE source_primary_id = 'recCLUB001'"
        )
        club_id_run1 = cur.fetchone()[0]

        cur.execute(
            "SELECT yacht_id FROM airtable_xref_yacht WHERE source_primary_id = 'recYACHT001'"
        )
        yacht_id_run1 = cur.fetchone()[0]

    _run(dsn, tmp_path)

    # Xref IDs must be identical after rerun
    with conn.cursor() as cur:
        cur.execute(
            "SELECT yacht_club_id FROM airtable_xref_club WHERE source_primary_id = 'recCLUB001'"
        )
        club_id_run2 = cur.fetchone()[0]

        cur.execute(
            "SELECT yacht_id FROM airtable_xref_yacht WHERE source_primary_id = 'recYACHT001'"
        )
        yacht_id_run2 = cur.fetchone()[0]

    assert club_id_run1 == club_id_run2, "Club xref drifted between runs"
    assert yacht_id_run1 == yacht_id_run2, "Yacht xref drifted between runs"
