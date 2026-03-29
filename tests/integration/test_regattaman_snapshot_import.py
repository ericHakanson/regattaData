"""Integration tests for the regattaman_snapshot ingestion pipeline."""

from __future__ import annotations

import csv
import uuid
from datetime import date
from pathlib import Path

import pytest

from regatta_etl.import_regattaman_snapshot import _run_regattaman_snapshot
from regatta_etl.shared import RejectWriter, RunCounters


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

HEADERS = ["nameLower", "yachtNameLower", "boatTypeLower", "Sail", "Rating"]
_EFFECTIVE_START = date(2025, 1, 1)


def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADERS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _make_row(**kwargs) -> dict:
    row = {h: "" for h in HEADERS}
    row.update({
        "nameLower": "cagle, nate",
        "yachtNameLower": "mumtaz",
        "boatTypeLower": "bhod",
        "Sail": "52",
        "Rating": "75",
    })
    row.update(kwargs)
    return row


def _run(
    dsn: str,
    csv_path: Path,
    dry_run: bool = False,
    effective_start: date | None = _EFFECTIVE_START,
) -> RunCounters:
    counters = RunCounters()
    rejects = RejectWriter(csv_path.parent / f"{uuid.uuid4()}_rejects.csv")
    _run_regattaman_snapshot(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        snapshot_path=str(csv_path),
        effective_start=effective_start,
        dry_run=dry_run,
    )
    return counters


# ---------------------------------------------------------------------------
# Test: happy-path single owner, non-zero rating
# ---------------------------------------------------------------------------

def test_snapshot_single_owner_with_rating(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row()])

    counters = _run(dsn, csv_path)

    assert counters.db_phase_errors == 0
    assert counters.rows_rejected == 0
    assert counters.regattaman_snapshot_rows_raw_inserted == 1
    assert counters.regattaman_snapshot_rows_curated_processed == 1
    assert counters.regattaman_snapshot_rows_curated_rejected == 0
    assert counters.participants_inserted == 1
    assert counters.yachts_inserted == 1
    assert counters.regattaman_snapshot_ownerships_inserted == 1
    assert counters.regattaman_snapshot_ratings_written == 1
    assert counters.regattaman_snapshot_xref_inserted == 2  # 1 yacht + 1 participant

    # Verify yacht_rating DB state
    yacht_row = conn.execute(
        "SELECT id, model FROM yacht WHERE normalized_name = 'mumtaz'"
    ).fetchone()
    assert yacht_row is not None
    assert yacht_row[1] == "bhod"

    rating_row = conn.execute(
        """
        SELECT rating_value, rating_system, rating_category
        FROM yacht_rating WHERE yacht_id = %s
        """,
        (str(yacht_row[0]),),
    ).fetchone()
    assert rating_row is not None
    assert rating_row[0] == "75"
    assert rating_row[1] == "PHRF"
    assert rating_row[2] == "phrf_main"

    # Verify ownership effective_start was recorded
    own_row = conn.execute(
        "SELECT role, effective_start FROM yacht_ownership WHERE yacht_id = %s",
        (str(yacht_row[0]),),
    ).fetchone()
    assert own_row[0] == "owner"
    assert own_row[1] == _EFFECTIVE_START


# ---------------------------------------------------------------------------
# Test: Rating = 0 → written to yacht_rating (0 is a valid PHRF rating)
# ---------------------------------------------------------------------------

def test_snapshot_rating_zero_written(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(Rating="0")])

    counters = _run(dsn, csv_path)

    assert counters.regattaman_snapshot_ratings_written == 1
    assert counters.db_phase_errors == 0

    yacht_row = conn.execute(
        "SELECT id FROM yacht WHERE normalized_name = 'mumtaz'"
    ).fetchone()
    rating_row = conn.execute(
        "SELECT rating_value FROM yacht_rating WHERE yacht_id = %s",
        (str(yacht_row[0]),),
    ).fetchone()
    assert rating_row is not None
    assert rating_row[0] == "0"


# ---------------------------------------------------------------------------
# Test: Rating = '-' → no rating written (absent marker)
# ---------------------------------------------------------------------------

def test_snapshot_rating_dash_skipped(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(Rating="-")])

    counters = _run(dsn, csv_path)

    assert counters.regattaman_snapshot_ratings_written == 0
    assert counters.regattaman_snapshot_rows_curated_processed == 1
    assert counters.db_phase_errors == 0


# ---------------------------------------------------------------------------
# Test: negative rating (valid PHRF) → written
# ---------------------------------------------------------------------------

def test_snapshot_negative_rating_written(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(Rating="-31")])

    counters = _run(dsn, csv_path)

    assert counters.regattaman_snapshot_ratings_written == 1
    yacht_row = conn.execute(
        "SELECT id FROM yacht WHERE normalized_name = 'mumtaz'"
    ).fetchone()
    rating_row = conn.execute(
        "SELECT rating_value FROM yacht_rating WHERE yacht_id = %s",
        (str(yacht_row[0]),),
    ).fetchone()
    assert rating_row[0] == "-31"


# ---------------------------------------------------------------------------
# Test: co-owner row — first owner gets role='owner', second gets role='co_owner'
# ---------------------------------------------------------------------------

def test_snapshot_co_owner_role_semantics(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(
        nameLower="clarkson, deanna & toby",
        yachtNameLower="osprey",
        boatTypeLower="bhod",
        Sail="26",
        Rating="0",
    )])

    counters = _run(dsn, csv_path)

    assert counters.db_phase_errors == 0
    assert counters.participants_inserted == 2
    assert counters.regattaman_snapshot_ownerships_inserted == 2
    assert counters.regattaman_snapshot_xref_inserted == 3  # 1 yacht + 2 participants

    yacht_row = conn.execute(
        "SELECT id FROM yacht WHERE normalized_name = 'osprey'"
    ).fetchone()
    assert yacht_row is not None

    ownerships = conn.execute(
        "SELECT role, is_primary_contact FROM yacht_ownership "
        "WHERE yacht_id = %s ORDER BY role",
        (str(yacht_row[0]),),
    ).fetchall()
    by_role = {r[0]: r[1] for r in ownerships}
    assert set(by_role.keys()) == {"owner", "co_owner"}
    assert by_role["owner"] is True
    assert by_role["co_owner"] is False


# ---------------------------------------------------------------------------
# Test: co-owner split via ' and ' (case-insensitive)
# ---------------------------------------------------------------------------

def test_snapshot_co_owner_and_separator(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(
        nameLower="smith, alice and jones, bob",
        yachtNameLower="windward",
        Sail="99",
        Rating="42",
    )])

    counters = _run(dsn, csv_path)

    assert counters.db_phase_errors == 0
    assert counters.participants_inserted == 2
    assert counters.regattaman_snapshot_ownerships_inserted == 2

    yacht_row = conn.execute(
        "SELECT id FROM yacht WHERE normalized_name = 'windward'"
    ).fetchone()
    ownerships = conn.execute(
        "SELECT role FROM yacht_ownership WHERE yacht_id = %s ORDER BY role",
        (str(yacht_row[0]),),
    ).fetchall()
    roles = sorted(r[0] for r in ownerships)
    assert roles == ["co_owner", "owner"]


# ---------------------------------------------------------------------------
# Test: idempotency — re-running same file inserts no duplicates
# ---------------------------------------------------------------------------

def test_snapshot_idempotent(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row()])

    _run(dsn, csv_path)
    counters2 = _run(dsn, csv_path)

    assert counters2.db_phase_errors == 0
    # Raw row not re-inserted (ON CONFLICT DO NOTHING)
    assert counters2.regattaman_snapshot_rows_raw_inserted == 0
    # Curated path still runs (xref hit → fill-null-only)
    assert counters2.regattaman_snapshot_rows_curated_processed == 1
    assert counters2.participants_inserted == 0
    assert counters2.yachts_inserted == 0

    assert conn.execute("SELECT COUNT(*) FROM participant").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM yacht").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM yacht_ownership").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM yacht_rating").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Test: rating upsert — re-run with updated rating updates the stored value
# ---------------------------------------------------------------------------

def test_snapshot_rating_upsert_updates_value(db_conn, tmp_path):
    conn, dsn = db_conn

    csv_path = tmp_path / "snapshot_v1.csv"
    _write_csv(csv_path, [_make_row(Rating="75")])
    _run(dsn, csv_path)

    csv_path2 = tmp_path / "snapshot_v2.csv"
    _write_csv(csv_path2, [_make_row(Rating="84")])
    _run(dsn, csv_path2)

    yacht_row = conn.execute(
        "SELECT id FROM yacht WHERE normalized_name = 'mumtaz'"
    ).fetchone()
    rating_row = conn.execute(
        "SELECT rating_value FROM yacht_rating WHERE yacht_id = %s",
        (str(yacht_row[0]),),
    ).fetchone()
    assert rating_row[0] == "84"


# ---------------------------------------------------------------------------
# Test: missing nameLower → reject (raw still captured)
# ---------------------------------------------------------------------------

def test_snapshot_missing_name_rejected(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(nameLower="")])

    counters = _run(dsn, csv_path)

    assert counters.rows_rejected == 1
    assert counters.regattaman_snapshot_rows_curated_rejected == 1
    assert counters.regattaman_snapshot_rows_raw_inserted == 1


# ---------------------------------------------------------------------------
# Test: invalid yacht name (pure punctuation) → reject
# ---------------------------------------------------------------------------

def test_snapshot_invalid_yacht_name_rejected(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(yachtNameLower="-")])

    counters = _run(dsn, csv_path)

    assert counters.rows_rejected == 1
    assert counters.regattaman_snapshot_rows_curated_rejected == 1


# ---------------------------------------------------------------------------
# Test: dry-run rolls back all changes
# ---------------------------------------------------------------------------

def test_snapshot_dry_run_no_persist(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(), _make_row(
        nameLower="hakanson, eric",
        yachtNameLower="charisma",
        Sail="25",
        Rating="42",
    )])

    counters = _run(dsn, csv_path, dry_run=True)

    assert counters.db_phase_errors == 0
    assert counters.rows_read == 2
    assert counters.regattaman_snapshot_rows_curated_processed == 2

    assert conn.execute("SELECT COUNT(*) FROM participant").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM yacht").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM yacht_rating").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM regattaman_snapshot_row").fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Test: multiple rows — batch happy path (0 ratings now written)
# ---------------------------------------------------------------------------

def test_snapshot_multiple_rows(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [
        _make_row(nameLower="cagle, nate", yachtNameLower="mumtaz", Sail="52", Rating="75"),
        _make_row(nameLower="hakanson, eric", yachtNameLower="charisma", Sail="25", Rating="0"),
        _make_row(nameLower="colburn, kenneth", yachtNameLower="gho2t", boatTypeLower="j/80",
                  Sail="1534", Rating="108"),
    ])

    counters = _run(dsn, csv_path)

    assert counters.db_phase_errors == 0
    assert counters.rows_rejected == 0
    assert counters.rows_read == 3
    assert counters.regattaman_snapshot_rows_raw_inserted == 3
    assert counters.participants_inserted == 3
    assert counters.yachts_inserted == 3
    # All three ratings written (including Rating="0")
    assert counters.regattaman_snapshot_ratings_written == 3


# ---------------------------------------------------------------------------
# Test: explicit effective_start is recorded on ownership rows
# ---------------------------------------------------------------------------

def test_snapshot_effective_start_recorded(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row()])

    fixed_date = date(2024, 6, 15)
    counters = _run(dsn, csv_path, effective_start=fixed_date)

    assert counters.db_phase_errors == 0
    own_row = conn.execute(
        "SELECT effective_start FROM yacht_ownership"
    ).fetchone()
    assert own_row[0] == fixed_date


# ---------------------------------------------------------------------------
# Test: no effective_start → ownership row still created (CURRENT_DATE used)
# ---------------------------------------------------------------------------

def test_snapshot_no_effective_start_defaults_to_today(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row()])

    counters = _run(dsn, csv_path, effective_start=None)

    assert counters.db_phase_errors == 0
    assert conn.execute("SELECT COUNT(*) FROM yacht_ownership").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Test: participant name-only resolution reuses existing participant
# ---------------------------------------------------------------------------

def test_snapshot_reuses_existing_participant(db_conn, tmp_path):
    conn, dsn = db_conn

    from regatta_etl.normalize import normalize_name, parse_name_parts
    full_name = "cagle, nate"
    norm = normalize_name(full_name)
    first, last = parse_name_parts(full_name)
    conn.execute(
        "INSERT INTO participant (full_name, normalized_full_name, first_name, last_name) "
        "VALUES (%s,%s,%s,%s)",
        (full_name, norm, first, last),
    )
    conn.commit()

    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row()])
    counters = _run(dsn, csv_path)

    assert counters.participants_inserted == 0
    assert counters.participants_matched_existing == 1
    assert conn.execute("SELECT COUNT(*) FROM participant").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Test: fill-null model enrichment on existing yacht
# ---------------------------------------------------------------------------

def test_snapshot_fill_null_model_on_existing_yacht(db_conn, tmp_path):
    conn, dsn = db_conn

    from regatta_etl.normalize import slug_name
    conn.execute(
        "INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number) "
        "VALUES ('mumtaz', %s, '52', '52')",
        (slug_name("mumtaz"),),
    )
    conn.commit()

    csv_path = tmp_path / "snapshot.csv"
    _write_csv(csv_path, [_make_row(boatTypeLower="bhod")])
    counters = _run(dsn, csv_path)

    assert counters.yachts_inserted == 0
    model = conn.execute(
        "SELECT model FROM yacht WHERE normalized_name = 'mumtaz'"
    ).fetchone()[0]
    assert model == "bhod"
