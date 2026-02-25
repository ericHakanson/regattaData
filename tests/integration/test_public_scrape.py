"""Integration tests for public_scrape ingestion.

Tests run against an ephemeral PostgreSQL database with the full schema
applied via the db_conn fixture in conftest.py.
"""

from __future__ import annotations

import csv
from pathlib import Path
from textwrap import dedent

import pytest

from regatta_etl.public_scrape import (
    ScrapeEventRecord,
    ScrapeEntryRecord,
    _process_entry,
    _upsert_scrape_event,
    load_entries,
    load_events,
    run_public_scrape,
)
from regatta_etl.shared import RejectWriter, RunCounters

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent.parent
REAL_ENTRIES_CSV = PROJECT_ROOT / "rawEvidence" / "all_regattaman_entries_2021_2025.csv"
REAL_EVENTS_CSV = (
    PROJECT_ROOT
    / "rawEvidence"
    / "consolidated_regattaman_events_2021_2025_with_event_url.csv"
)

CANON_URL_537 = "https://regattaman.com/scratch.php?race_id=537&yr=2021"

# ---------------------------------------------------------------------------
# Synthetic fixtures
# ---------------------------------------------------------------------------

SYNTH_EVENT = ScrapeEventRecord(
    source="regattaman_2021",
    season_year=2021,
    event_name="Jack Roberts Memorial NYD Race",
    canonical_url=CANON_URL_537,
    race_id="537",
    yr=2021,
    host_club_name="Constitution YC",
    host_club_normalized="constitution-yc",
    event_url=None,
    date_str="Jan 1",
)

SYNTH_ENTRY = ScrapeEntryRecord(
    source="regattaman_2021",
    season_year=2021,
    event_name="Jack Roberts Memorial NYD Race",
    canonical_url=CANON_URL_537,
    race_id="537",
    yr=2021,
    fleet="SPIN",
    full_name="Chuang, John",
    yacht_name="Twist",
    city="Brookline",
    sail_num="32801",
    boat_type="Beneteau First 42 SD",
    registration_external_id="r-537-2021-554-558-0",
    external_id_type="sku",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rejects(tmp_path: Path) -> RejectWriter:
    return RejectWriter(tmp_path / "rejects.csv")


def _count(conn, table: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------------------
# Test: event upsert
# ---------------------------------------------------------------------------

class TestScrapeEventUpsert:
    def test_creates_club_series_instance(self, db_conn):
        conn, _ = db_conn
        counters = RunCounters()
        with conn.transaction():
            iid = _upsert_scrape_event(conn, SYNTH_EVENT, "regattaman_public_scrape_csv", counters)
        assert iid is not None
        assert _count(conn, "yacht_club") == 1
        assert _count(conn, "event_series") == 1
        assert _count(conn, "event_instance") == 1
        assert counters.events_upserted == 1

    def test_idempotent_second_upsert(self, db_conn):
        conn, _ = db_conn
        counters = RunCounters()
        with conn.transaction():
            id1 = _upsert_scrape_event(conn, SYNTH_EVENT, "regattaman_public_scrape_csv", counters)
        with conn.transaction():
            id2 = _upsert_scrape_event(conn, SYNTH_EVENT, "regattaman_public_scrape_csv", counters)
        assert id1 == id2
        assert _count(conn, "event_instance") == 1


# ---------------------------------------------------------------------------
# Test: entry upsert
# ---------------------------------------------------------------------------

class TestScrapeEntryUpsert:
    def _setup(self, conn):
        counters = RunCounters()
        with conn.transaction():
            iid = _upsert_scrape_event(
                conn, SYNTH_EVENT, "regattaman_public_scrape_csv", counters
            )
        conn.commit()
        return iid, counters

    def test_creates_participant_yacht_entry(self, db_conn):
        conn, _ = db_conn
        iid, counters = self._setup(conn)
        with conn.transaction():
            _process_entry(
                conn, SYNTH_ENTRY, iid, "regattaman_public_scrape_csv", counters
            )
        assert _count(conn, "participant") == 1
        assert _count(conn, "yacht") == 1
        assert _count(conn, "event_entry") == 1
        assert _count(conn, "event_entry_participant") == 1

    def test_entry_participant_role_is_registrant(self, db_conn):
        conn, _ = db_conn
        iid, counters = self._setup(conn)
        with conn.transaction():
            _process_entry(
                conn, SYNTH_ENTRY, iid, "regattaman_public_scrape_csv", counters
            )
        row = conn.execute("SELECT role FROM event_entry_participant").fetchone()
        assert row[0] == "registrant"

    def test_entry_status_is_submitted(self, db_conn):
        conn, _ = db_conn
        iid, counters = self._setup(conn)
        with conn.transaction():
            _process_entry(
                conn, SYNTH_ENTRY, iid, "regattaman_public_scrape_csv", counters
            )
        row = conn.execute("SELECT entry_status FROM event_entry").fetchone()
        assert row[0] == "submitted"

    def test_idempotent_second_process(self, db_conn):
        conn, _ = db_conn
        iid, counters1 = self._setup(conn)
        with conn.transaction():
            _process_entry(
                conn, SYNTH_ENTRY, iid, "regattaman_public_scrape_csv", counters1
            )
        counters2 = RunCounters()
        with conn.transaction():
            _process_entry(
                conn, SYNTH_ENTRY, iid, "regattaman_public_scrape_csv", counters2
            )
        assert counters2.participants_inserted == 0
        assert counters2.yachts_inserted == 0
        assert _count(conn, "event_entry") == 1
        assert _count(conn, "event_entry_participant") == 1

    def test_sail_number_stored_on_yacht(self, db_conn):
        conn, _ = db_conn
        iid, counters = self._setup(conn)
        with conn.transaction():
            _process_entry(
                conn, SYNTH_ENTRY, iid, "regattaman_public_scrape_csv", counters
            )
        row = conn.execute("SELECT sail_number FROM yacht").fetchone()
        assert row[0] == "32801"

    def test_sail_present_inserts_new_yacht_when_no_exact_match(self, db_conn):
        """When a yacht with the same name but no sail_number exists in the DB,
        a sail-present entry creates a new yacht record (no fill-null fallback)."""
        conn, _ = db_conn
        iid, counters = self._setup(conn)
        # Insert a no-sail yacht (simulating a prior partial record)
        conn.execute(
            "INSERT INTO yacht (name, normalized_name) VALUES ('Twist', 'twist')"
        )
        conn.commit()
        with conn.transaction():
            _process_entry(
                conn, SYNTH_ENTRY, iid, "regattaman_public_scrape_csv", counters
            )
        # A new yacht is inserted (sail-present lookup found no exact match)
        assert counters.yachts_inserted == 1
        assert _count(conn, "yacht") == 2
        # The newly inserted yacht has sail_number set
        row = conn.execute(
            "SELECT sail_number FROM yacht WHERE normalized_sail_number = '32801'"
        ).fetchone()
        assert row[0] == "32801"

    def test_no_sail_entry_is_deterministic_across_runs(self, db_conn):
        """A no-sail entry is idempotent: the second run reuses the yacht_id
        already stored in event_entry for this registration_external_id,
        so event_entry count is unchanged."""
        conn, _ = db_conn
        iid, counters = self._setup(conn)

        # Pre-populate two yachts with the same name but different sail numbers
        conn.execute(
            "INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number) "
            "VALUES ('Twist', 'twist', '32801', '32801')"
        )
        conn.execute(
            "INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number) "
            "VALUES ('Twist', 'twist', '99999', '99999')"
        )
        conn.commit()

        no_sail_entry = ScrapeEntryRecord(
            source="regattaman_2021",
            season_year=2021,
            event_name="Jack Roberts Memorial NYD Race",
            canonical_url=CANON_URL_537,
            race_id="537",
            yr=2021,
            fleet="SPIN",
            full_name="Smith, Bob",
            yacht_name="Twist",
            city=None,
            sail_num=None,
            boat_type=None,
            registration_external_id="hash-test-no-sail-determinism",
            external_id_type="hash",
        )

        # First run: no prior event_entry → inserts a new no-sail yacht record
        with conn.transaction():
            _process_entry(conn, no_sail_entry, iid, "regattaman_public_scrape_csv", counters)

        assert _count(conn, "event_entry") == 1
        yacht_id_run1 = conn.execute("SELECT yacht_id FROM event_entry").fetchone()[0]

        # Second run: finds prior event_entry → reuses same yacht_id → ON CONFLICT
        counters2 = RunCounters()
        with conn.transaction():
            _process_entry(conn, no_sail_entry, iid, "regattaman_public_scrape_csv", counters2)

        assert _count(conn, "event_entry") == 1  # unchanged
        assert counters2.yachts_inserted == 0    # no new yacht inserted
        yacht_id_run2 = conn.execute("SELECT yacht_id FROM event_entry").fetchone()[0]
        assert yacht_id_run2 == yacht_id_run1    # same yacht on both runs


# ---------------------------------------------------------------------------
# Test: source precedence — private export not overwritten
# ---------------------------------------------------------------------------

class TestSourcePrecedence:
    def test_confirmed_entry_not_downgraded(self, db_conn):
        """If private export loaded a 'confirmed' entry, public scrape must not change it."""
        conn, _ = db_conn
        counters = RunCounters()
        with conn.transaction():
            iid = _upsert_scrape_event(
                conn, SYNTH_EVENT, "regattaman_public_scrape_csv", counters
            )
        # Insert confirmed entry as if from private export
        conn.execute(
            "INSERT INTO yacht (name, normalized_name) VALUES ('Twist', 'twist')"
        )
        yacht_id = conn.execute(
            "SELECT id FROM yacht WHERE normalized_name = 'twist'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO event_entry
              (event_instance_id, yacht_id, entry_status, registration_source,
               registration_external_id)
            VALUES (%s, %s, 'confirmed', 'regattaman', 'r-537-2021-554-558-0')
            """,
            (iid, yacht_id),
        )
        conn.commit()

        # Public scrape processes the same entry
        with conn.transaction():
            _process_entry(
                conn, SYNTH_ENTRY, iid, "regattaman_public_scrape_csv", counters
            )

        # entry_status must remain 'confirmed', not downgraded to 'submitted'
        row = conn.execute(
            "SELECT entry_status, registration_source FROM event_entry"
        ).fetchone()
        assert row[0] == "confirmed"
        assert row[1] == "regattaman"  # source preserved, not overwritten


# ---------------------------------------------------------------------------
# Test: CSV loading utilities
# ---------------------------------------------------------------------------

class TestLoadEvents:
    def _write_events_csv(self, path: Path, rows: list[dict]) -> None:
        fieldnames = [
            "source", "Event Name", "entries_url", "Host Club",
            "Date", "Day", "When", "Region", "Event URL", "Info URL", "Results URL",
        ]
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def test_loads_valid_event(self, tmp_path, db_conn):
        conn, _ = db_conn
        p = tmp_path / "events.csv"
        self._write_events_csv(p, [{
            "source": "regattaman_2021",
            "Event Name": "Test Race",
            "entries_url": (
                "https://regattaman.com/scratch.php"
                "?race_id=999&yr=2021&sort=0"
            ),
            "Host Club": "Test YC",
        }])
        rejects = _rejects(tmp_path)
        counters = RunCounters()
        records, url_map = load_events(p, rejects, counters)
        assert len(records) == 1
        assert "https://regattaman.com/scratch.php?race_id=999&yr=2021" in url_map

    def test_skips_soc_id_events_silently(self, tmp_path, db_conn):
        conn, _ = db_conn
        p = tmp_path / "events.csv"
        self._write_events_csv(p, [
            {
                "source": "regattaman_2021",
                "Event Name": "Meeting",
                "entries_url": "https://regattaman.com/def_soc_page.php?soc_id=70&yr=2021",
                "Host Club": "Test YC",
            },
            {
                "source": "regattaman_2021",
                "Event Name": "Real Race",
                "entries_url": (
                    "https://regattaman.com/scratch.php?race_id=100&yr=2021"
                ),
                "Host Club": "Test YC",
            },
        ])
        rejects = _rejects(tmp_path)
        counters = RunCounters()
        records, url_map = load_events(p, rejects, counters)
        assert len(records) == 1
        assert counters.rows_rejected == 0


class TestLoadEntries:
    def _write_entries_csv(self, path: Path, rows: list[dict]) -> None:
        fieldnames = [
            "source", "Event Name", "entries_url", "Fleet", "Name",
            "Yacht Name", "City", "Sail Num", "Boat Type",
            "Split Rating", "Race Rat", "Cru Rat", "R/C Cfg", "Hist",
        ]
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _base_row(self, **overrides) -> dict:
        row = {
            "source": "regattaman_2021",
            "Event Name": "Jack Roberts Memorial NYD Race",
            "entries_url": (
                "https://regattaman.com/scratch.php"
                "?race_id=537&yr=2021&sort=0"
            ),
            "Fleet": "SPIN",
            "Name": "Chuang, John",
            "Yacht Name": "Twist",
            "City": "Brookline",
            "Sail Num": "32801",
            "Boat Type": "Beneteau First 42 SD",
            "Split Rating": "",
            "Race Rat": "",
            "Cru Rat": "",
            "R/C Cfg": "",
            "Hist": "https://regattaman.com/get_race_hist.php?sku=r-537-2021-554-558-0",
        }
        row.update(overrides)
        return row

    def test_collision_rejected(self, tmp_path):
        """Two rows with same sku produce first accepted, second rejected."""
        p = tmp_path / "entries.csv"
        row = self._base_row()
        self._write_entries_csv(p, [row, row])  # identical rows = same sku
        event_url_map = {
            "https://regattaman.com/scratch.php?race_id=537&yr=2021": SYNTH_EVENT
        }
        rejects = _rejects(tmp_path)
        counters = RunCounters()
        records, _ = load_entries(p, event_url_map, rejects, counters, synthesize_events=True)
        assert len(records) == 1
        assert counters.rows_rejected == 1

    def test_synthesize_event_for_unmatched_url(self, tmp_path):
        """Unmatched entries_url → synthesize minimal event record."""
        p = tmp_path / "entries.csv"
        self._write_entries_csv(p, [self._base_row(entries_url=(
            "https://regattaman.com/scratch.php?race_id=9999&yr=2021"
        ))])
        rejects = _rejects(tmp_path)
        counters = RunCounters()
        records, event_map = load_entries(p, {}, rejects, counters, synthesize_events=True)
        assert len(records) == 1
        assert counters.unmatched_entry_urls == 1
        canon = "https://regattaman.com/scratch.php?race_id=9999&yr=2021"
        assert canon in event_map
        assert event_map[canon].host_club_name == "Unknown Club"

    def test_reject_if_no_synthesize(self, tmp_path):
        """Unmatched entries_url → reject when synthesize_events=False."""
        p = tmp_path / "entries.csv"
        self._write_entries_csv(p, [self._base_row(entries_url=(
            "https://regattaman.com/scratch.php?race_id=9999&yr=2021"
        ))])
        rejects = _rejects(tmp_path)
        counters = RunCounters()
        records, _ = load_entries(p, {}, rejects, counters, synthesize_events=False)
        assert len(records) == 0
        assert counters.rows_rejected == 1


# ---------------------------------------------------------------------------
# Test: full run_public_scrape with real files (skipped if absent)
# ---------------------------------------------------------------------------

class TestFullPublicScrapeRun:
    @pytest.mark.skipif(
        not (REAL_ENTRIES_CSV.exists() and REAL_EVENTS_CSV.exists()),
        reason="Real CSV files not available",
    )
    def test_dry_run_commits_nothing(self, db_conn, tmp_path):
        conn, dsn = db_conn
        counters = RunCounters()
        rejects = RejectWriter(tmp_path / "rejects.csv")

        run_public_scrape(
            conn=conn,
            entries_path=REAL_ENTRIES_CSV,
            events_path=REAL_EVENTS_CSV,
            source_system="regattaman_public_scrape_csv",
            run_id="test-dry-run-scrape",
            counters=counters,
            rejects=rejects,
            dry_run=True,
            max_reject_rate=0.10,
            synthesize_events=True,
        )
        rejects.close()

        for table in ["participant", "yacht", "event_entry", "event_instance"]:
            assert _count(conn, table) == 0, f"Expected 0 rows in {table} after dry-run"

    @pytest.mark.skipif(
        not (REAL_ENTRIES_CSV.exists() and REAL_EVENTS_CSV.exists()),
        reason="Real CSV files not available",
    )
    def test_real_run_produces_expected_counts(self, db_conn, tmp_path):
        conn, dsn = db_conn
        counters = RunCounters()
        rejects = RejectWriter(tmp_path / "rejects.csv")

        run_public_scrape(
            conn=conn,
            entries_path=REAL_ENTRIES_CSV,
            events_path=REAL_EVENTS_CSV,
            source_system="regattaman_public_scrape_csv",
            run_id="test-real-scrape",
            counters=counters,
            rejects=rejects,
            dry_run=False,
            max_reject_rate=0.10,
            synthesize_events=True,
        )
        rejects.close()

        # At least one event_instance created
        assert _count(conn, "event_instance") >= 1
        # Entries should be in DB (15075 - rejects)
        assert _count(conn, "event_entry") >= 14000

        # Capture counts before second run for idempotency check
        count_before_second = _count(conn, "event_entry")

        # Idempotency: second run
        counters2 = RunCounters()
        run_public_scrape(
            conn=conn,
            entries_path=REAL_ENTRIES_CSV,
            events_path=REAL_EVENTS_CSV,
            source_system="regattaman_public_scrape_csv",
            run_id="test-real-scrape-2",
            counters=counters2,
            rejects=RejectWriter(tmp_path / "rejects2.csv"),
            dry_run=False,
            max_reject_rate=0.10,
            synthesize_events=True,
        )

        # Second run adds no new participants, yachts, or entries beyond first
        assert counters2.participants_inserted == 0
        assert counters2.yachts_inserted == 0
        # event_entry count unchanged after second run
        assert _count(conn, "event_entry") == count_before_second


# ---------------------------------------------------------------------------
# Test: regression — private_export mode unchanged
# ---------------------------------------------------------------------------

class TestPrivateExportRegression:
    """Verify existing private-export test scenarios still work after refactor."""

    ANDRUS_ROW = {
        "oid": "7113", "bid": "5285",
        "date_entered": "2025-07-09 8:00:45", "cid": "0",
        "sku": "r-858-2025-7113-5285-0", "parent_sku": "r-858-2025-7113-5285-0",
        "date_paid": "2025-06-24 10:16:16", "date_updated": "2025-07-09 8:00:45",
        "Paid Type": "rms", "Discounts": "",
        "ownername": "Andrus, Justin",
        "Name": "Andrus, Justin & McCoig, Kathryn",
        "owner_address": "PO Box 310", "City": "Bath",
        "owner_state": "ME", "ccode": "US", "owner_zip": "04530",
        "owner_hphone": "2078318338", "owner_cphone": "2078318338",
        "Email": "justinandrus@protonmail.com",
        "org_name": "Regatta Management Solutions",
        "Club": "-", "org_abbrev": "RMS",
        "Yacht Name": "Ingalina", "Boat  Type": "Sabre 34-2",
        "LOA": "34.17", "Moor Depth": "6",
    }

    def _setup(self, conn):
        from regatta_etl.shared import upsert_event_context
        _, _, iid = upsert_event_context(
            conn,
            "Boothbay Harbor Yacht Club", "boothbay-harbor-yacht-club",
            "BHYC Regatta", "bhyc-regatta",
            "BHYC Regatta 2025", 2025, None,
        )
        conn.commit()
        return iid

    def test_private_export_two_participants(self, db_conn):
        conn, _ = db_conn
        iid = self._setup(conn)
        from regatta_etl.import_regattaman_csv import _process_row
        from regatta_etl.shared import RunCounters
        counters = RunCounters()
        with conn.transaction():
            _process_row(conn, self.ANDRUS_ROW, iid, "regattaman",
                         "regattaman_csv_export", "test-run", counters)
        assert _count(conn, "participant") == 2

    def test_private_export_entry_participant_role_owner_contact(self, db_conn):
        conn, _ = db_conn
        iid = self._setup(conn)
        from regatta_etl.import_regattaman_csv import _process_row
        from regatta_etl.shared import RunCounters
        counters = RunCounters()
        with conn.transaction():
            _process_row(conn, self.ANDRUS_ROW, iid, "regattaman",
                         "regattaman_csv_export", "test-run", counters)
        roles = [r[0] for r in conn.execute(
            "SELECT role FROM event_entry_participant"
        ).fetchall()]
        assert all(r == "owner_contact" for r in roles)
