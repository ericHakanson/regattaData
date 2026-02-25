"""Integration tests for import_regattaman_csv.

These tests run against an ephemeral PostgreSQL database with the full
schema applied via the db_conn fixture in conftest.py.
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path
from unittest.mock import patch

import psycopg
import pytest

from regatta_etl.import_regattaman_csv import (
    _RejectWriter,
    _derive_entry_status,
    _process_row,
    _upsert_event_context,
    RunCounters,
)
from regatta_etl.normalize import parse_ts

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent.parent
REAL_CSV = PROJECT_ROOT / "rawEvidence" / "export2025bhycRegatta - export.csv"

CLI_KWARGS = dict(
    host_club_name="Boothbay Harbor Yacht Club",
    host_club_normalized="boothbay-harbor-yacht-club",
    event_series_name="BHYC Regatta",
    event_series_normalized="bhyc-regatta",
    event_display_name="BHYC Regatta 2025",
    season_year=2025,
    registration_source="regattaman",
    source_system="regattaman_csv_export",
)


def _setup_event_context(conn, registration_open_at=None):
    """Helper: upsert event context and return event_instance_id."""
    _, _, instance_id = _upsert_event_context(
        conn,
        CLI_KWARGS["host_club_name"],
        CLI_KWARGS["host_club_normalized"],
        CLI_KWARGS["event_series_name"],
        CLI_KWARGS["event_series_normalized"],
        CLI_KWARGS["event_display_name"],
        CLI_KWARGS["season_year"],
        registration_open_at,
    )
    conn.commit()
    return instance_id


# ---------------------------------------------------------------------------
# Test: event context upsert
# ---------------------------------------------------------------------------

class TestEventContextUpsert:
    def test_creates_host_club_series_instance(self, db_conn):
        conn, _ = db_conn
        instance_id = _setup_event_context(conn)
        assert instance_id is not None

        row = conn.execute(
            "SELECT count(*) FROM event_instance WHERE season_year = 2025"
        ).fetchone()
        assert row[0] == 1

    def test_idempotent_second_upsert(self, db_conn):
        conn, _ = db_conn
        id1 = _setup_event_context(conn)
        id2 = _setup_event_context(conn)
        assert id1 == id2

        row = conn.execute("SELECT count(*) FROM event_instance").fetchone()
        assert row[0] == 1

    def test_display_name_updated_on_conflict(self, db_conn):
        conn, _ = db_conn
        _setup_event_context(conn)
        # Re-upsert with a different display name
        _upsert_event_context(
            conn,
            CLI_KWARGS["host_club_name"],
            CLI_KWARGS["host_club_normalized"],
            CLI_KWARGS["event_series_name"],
            CLI_KWARGS["event_series_normalized"],
            "BHYC Regatta 2025 Updated",
            CLI_KWARGS["season_year"],
            None,
        )
        conn.commit()
        row = conn.execute(
            "SELECT display_name FROM event_instance WHERE season_year = 2025"
        ).fetchone()
        assert row[0] == "BHYC Regatta 2025 Updated"

    def test_registration_open_at_takes_earlier_value(self, db_conn):
        conn, _ = db_conn
        from datetime import datetime
        ts1 = datetime(2025, 5, 12, 16, 0, 37)
        ts2 = datetime(2025, 4, 1, 0, 0, 0)
        _upsert_event_context(
            conn,
            CLI_KWARGS["host_club_name"],
            CLI_KWARGS["host_club_normalized"],
            CLI_KWARGS["event_series_name"],
            CLI_KWARGS["event_series_normalized"],
            CLI_KWARGS["event_display_name"],
            CLI_KWARGS["season_year"],
            ts1,
        )
        conn.commit()
        _upsert_event_context(
            conn,
            CLI_KWARGS["host_club_name"],
            CLI_KWARGS["host_club_normalized"],
            CLI_KWARGS["event_series_name"],
            CLI_KWARGS["event_series_normalized"],
            CLI_KWARGS["event_display_name"],
            CLI_KWARGS["season_year"],
            ts2,
        )
        conn.commit()
        row = conn.execute(
            "SELECT registration_open_at FROM event_instance WHERE season_year = 2025"
        ).fetchone()
        assert row[0].replace(tzinfo=None) == ts2


# ---------------------------------------------------------------------------
# Test: co-owner row processing (Andrus / McCoig)
# ---------------------------------------------------------------------------

class TestCoOwnerRow:
    ANDRUS_ROW = {
        "oid": "7113",
        "bid": "5285",
        "date_entered": "2025-07-09 8:00:45",
        "cid": "0",
        "sku": "r-858-2025-7113-5285-0",
        "parent_sku": "r-858-2025-7113-5285-0",
        "date_paid": "2025-06-24 10:16:16",
        "date_updated": "2025-07-09 8:00:45",
        "Paid Type": "rms",
        "Discounts": "",
        "ownername": "Andrus, Justin",
        "Name": "Andrus, Justin & McCoig, Kathryn",
        "owner_address": "PO Box 310",
        "City": "Bath",
        "owner_state": "ME",
        "ccode": "US",
        "owner_zip": "04530",
        "owner_hphone": "2078318338",
        "owner_cphone": "2078318338",
        "Email": "justinandrus@protonmail.com",
        "org_name": "Regatta Management Solutions",
        "Club": "-",
        "org_abbrev": "RMS",
        "Yacht Name": "Ingalina",
        "Boat  Type": "Sabre 34-2",
        "LOA": "34.17",
        "Moor Depth": "6",
    }

    def test_two_participants_inserted(self, db_conn):
        conn, _ = db_conn
        instance_id = _setup_event_context(conn)
        counters = RunCounters()

        _process_row(conn, self.ANDRUS_ROW, instance_id,
                     "regattaman", "regattaman_csv_export", "test-run", counters)
        conn.commit()

        row = conn.execute("SELECT count(*) FROM participant").fetchone()
        assert row[0] == 2

    def test_two_ownership_rows_inserted(self, db_conn):
        conn, _ = db_conn
        instance_id = _setup_event_context(conn)
        counters = RunCounters()

        _process_row(conn, self.ANDRUS_ROW, instance_id,
                     "regattaman", "regattaman_csv_export", "test-run", counters)
        conn.commit()

        row = conn.execute("SELECT count(*) FROM yacht_ownership").fetchone()
        assert row[0] == 2

    def test_two_event_entry_participant_rows(self, db_conn):
        conn, _ = db_conn
        instance_id = _setup_event_context(conn)
        counters = RunCounters()

        _process_row(conn, self.ANDRUS_ROW, instance_id,
                     "regattaman", "regattaman_csv_export", "test-run", counters)
        conn.commit()

        row = conn.execute("SELECT count(*) FROM event_entry_participant").fetchone()
        assert row[0] == 2

    def test_rms_club_not_inserted(self, db_conn):
        conn, _ = db_conn
        instance_id = _setup_event_context(conn)
        counters = RunCounters()

        _process_row(conn, self.ANDRUS_ROW, instance_id,
                     "regattaman", "regattaman_csv_export", "test-run", counters)
        conn.commit()

        # Only the host club should be present (from event context)
        row = conn.execute("SELECT count(*) FROM yacht_club").fetchone()
        assert row[0] == 1  # only host club

    def test_processing_is_idempotent(self, db_conn):
        conn, _ = db_conn
        instance_id = _setup_event_context(conn)
        counters1 = RunCounters()

        _process_row(conn, self.ANDRUS_ROW, instance_id,
                     "regattaman", "regattaman_csv_export", "test-run", counters1)
        conn.commit()

        counters2 = RunCounters()
        _process_row(conn, self.ANDRUS_ROW, instance_id,
                     "regattaman", "regattaman_csv_export", "test-run", counters2)
        conn.commit()

        # Second run should not insert new rows
        assert counters2.participants_inserted == 0
        assert counters2.yachts_inserted == 0
        assert counters2.owner_links_inserted == 0
        assert counters2.contact_points_inserted == 0
        assert counters2.addresses_inserted == 0


# ---------------------------------------------------------------------------
# Test: entry_status on an actual row
# ---------------------------------------------------------------------------

class TestEntryStatusIntegration:
    PAID_ROW = {
        "oid": "16166", "bid": "15828",
        "date_entered": "2025-05-21 10:54:49", "cid": "0",
        "sku": "r-858-2025-16166-15828-0", "parent_sku": "r-858-2025-16166-15828-0",
        "date_paid": "2025-05-21 10:55:57", "date_updated": "0000-00-00 00:00:00",
        "Paid Type": "rms", "Discounts": "USSAIL",
        "ownername": "Amthor, Henry", "Name": "Amthor, Henry",
        "owner_address": "31 Kings Point dr", "City": "Hampton",
        "owner_state": "VA", "ccode": "US", "owner_zip": "23669",
        "owner_hphone": "7574351543", "owner_cphone": "",
        "Email": "henrypamthor@gmail.com",
        "org_name": "Hampton Yacht Club", "Club": "Hampton YC", "org_abbrev": "HYC",
        "Yacht Name": "E+A2", "Boat  Type": "Viper 640", "LOA": "21", "Moor Depth": "5",
    }

    def test_confirmed_entry_created(self, db_conn):
        conn, _ = db_conn
        instance_id = _setup_event_context(conn)
        counters = RunCounters()

        _process_row(conn, self.PAID_ROW, instance_id,
                     "regattaman", "regattaman_csv_export", "test-run", counters)
        conn.commit()

        row = conn.execute(
            "SELECT entry_status FROM event_entry"
        ).fetchone()
        assert row[0] == "confirmed"


# ---------------------------------------------------------------------------
# Test: real CSV full import (CLI-level smoke)
# ---------------------------------------------------------------------------

class TestFullImport:
    """Smoke test using the real CSV file via CLI invocation."""

    @pytest.mark.skipif(
        not REAL_CSV.exists(),
        reason="Real CSV not available in test environment",
    )
    def test_dry_run_produces_no_db_rows(self, db_conn, tmp_path):
        conn, dsn = db_conn
        rejects_file = tmp_path / "rejects.csv"

        from click.testing import CliRunner
        from regatta_etl.import_regattaman_csv import main

        runner = CliRunner()
        result = runner.invoke(main, [
            "--db-dsn", dsn,
            "--csv-path", str(REAL_CSV),
            "--host-club-name", "Boothbay Harbor Yacht Club",
            "--host-club-normalized", "boothbay-harbor-yacht-club",
            "--event-series-name", "BHYC Regatta",
            "--event-series-normalized", "bhyc-regatta",
            "--event-display-name", "BHYC Regatta 2025",
            "--season-year", "2025",
            "--registration-source", "regattaman",
            "--source-system", "regattaman_csv_export",
            "--asset-type", "regattaman_csv_export",
            "--dry-run",
            "--rejects-path", str(rejects_file),
            "--run-id", "test-dry-run",
        ])

        # Expected validation rejects (blank Yacht Name) must NOT cause non-zero exit.
        assert result.exit_code == 0, (
            f"Dry-run exited {result.exit_code} — expected 0 for validation-only rejects.\n"
            f"Output:\n{result.output}"
        )

        # Dry-run must roll back — no rows in any table
        for table in ["participant", "yacht", "event_entry", "event_instance"]:
            row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
            assert row[0] == 0, f"Expected 0 rows in {table} after dry-run"

    @pytest.mark.skipif(
        not REAL_CSV.exists(),
        reason="Real CSV not available in test environment",
    )
    def test_real_run_produces_expected_counts(self, db_conn, tmp_path):
        conn, dsn = db_conn
        rejects_file = tmp_path / "rejects.csv"

        from click.testing import CliRunner
        from regatta_etl.import_regattaman_csv import main

        runner = CliRunner()

        def invoke():
            return runner.invoke(main, [
                "--db-dsn", dsn,
                "--csv-path", str(REAL_CSV),
                "--host-club-name", "Boothbay Harbor Yacht Club",
                "--host-club-normalized", "boothbay-harbor-yacht-club",
                "--event-series-name", "BHYC Regatta",
                "--event-series-normalized", "bhyc-regatta",
                "--event-display-name", "BHYC Regatta 2025",
                "--season-year", "2025",
                "--registration-source", "regattaman",
                "--source-system", "regattaman_csv_export",
                "--asset-type", "regattaman_csv_export",
                "--rejects-path", str(rejects_file),
                "--run-id", "test-real-run",
            ])

        result = invoke()
        assert result.exit_code == 0, f"CLI failed:\n{result.output}"

        # Exactly 1 event_instance for 2025
        row = conn.execute(
            "SELECT count(*) FROM event_instance WHERE season_year = 2025"
        ).fetchone()
        assert row[0] == 1

        # 64 valid rows → 64 event_entries (1 blank-yacht reject)
        row = conn.execute("SELECT count(*) FROM event_entry").fetchone()
        assert row[0] == 64

        # Rejects file exists and contains the blank-yacht row
        assert rejects_file.exists()
        with rejects_file.open() as f:
            reader = csv.DictReader(f)
            reject_rows = list(reader)
        assert len(reject_rows) >= 1
        reasons = [r["_reject_reason"] for r in reject_rows]
        assert any("blank_yacht_name" in r for r in reasons)

        # Capture counts after first run, before second run
        participant_count_after_first = conn.execute(
            "SELECT count(*) FROM participant"
        ).fetchone()[0]

        # Idempotency: second run produces same counts
        result2 = invoke()
        assert result2.exit_code == 0

        assert conn.execute("SELECT count(*) FROM event_entry").fetchone()[0] == 64
        assert conn.execute("SELECT count(*) FROM participant").fetchone()[0] == participant_count_after_first
