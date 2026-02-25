"""Integration tests for the yacht_scoring ingestion pipeline."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from regatta_etl.import_yacht_scoring import _run_yacht_scoring
from regatta_etl.shared import RejectWriter, RunCounters


# ---------------------------------------------------------------------------
# Fixture CSV builders
# ---------------------------------------------------------------------------

SCRAPED_EVENT_HEADERS = ["ant-image-img src", "title-small", "title-small href", "w-[10%]", "w-[20%]"]
SCRAPED_ENTRY_HEADERS = [
    "font-bold", "title-small", "title-small href", "flex",
    "tablescraper-selected-row", "tablescraper-selected-row 2",
    "tablescraper-selected-row 3", "tablescraper-selected-row 4",
    "title-small 2",
]
DEDUP_ENTRY_HEADERS = [
    "sailNumber", "entryUrl", "yachtName", "ownerName",
    "ownerAffiliation", "ownerLocation", "yachtNameDuplicate",
    "yachtType", "eventUrl",
]
UNIQUE_YACHT_HEADERS = DEDUP_ENTRY_HEADERS + ["concatenate"]


def _write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _make_scraped_event(**kwargs) -> dict:
    row = {h: "" for h in SCRAPED_EVENT_HEADERS}
    row.update({
        "title-small": "2024 Orange Bowl International",
        "title-small href": "https://www.yachtscoring.com/emenu/99901",
        "w-[10%]": "Miami USA",
        "w-[20%]": "December 28 - 30, 2024",
    })
    row.update(kwargs)
    return row


def _make_scraped_entry(**kwargs) -> dict:
    row = {h: "" for h in SCRAPED_ENTRY_HEADERS}
    row.update({
        "font-bold": "1.",
        "title-small": "USA-42",
        "title-small href": "https://www.yachtscoring.com/boatdetail/99901/111",
        "flex": "Alice Smith",
        "tablescraper-selected-row": "Boothbay YC",
        "tablescraper-selected-row 2": "Maine USA",
        "tablescraper-selected-row 3": "J/24",
        "tablescraper-selected-row 4": "24",
        "title-small 2": "Sea Spirit",
    })
    row.update(kwargs)
    return row


def _make_dedup_entry(**kwargs) -> dict:
    row = {h: "" for h in DEDUP_ENTRY_HEADERS}
    row.update({
        "sailNumber": "USA-42",
        "entryUrl": "https://www.yachtscoring.com/boatdetail/99901/111",
        "yachtName": "Sea Spirit",
        "ownerName": "Alice Smith",
        "ownerAffiliation": "Boothbay YC",
        "ownerLocation": "Maine USA",
        "yachtNameDuplicate": "Sea Spirit",
        "yachtType": "J/24",
        "eventUrl": "https://www.yachtscoring.com/current_event_entries/99901",
    })
    row.update(kwargs)
    return row


def _make_unique_yacht(**kwargs) -> dict:
    row = {h: "" for h in UNIQUE_YACHT_HEADERS}
    row.update({
        "sailNumber": "USA-42",
        "entryUrl": "https://www.yachtscoring.com/boatdetail/99901/111",
        "yachtName": "Sea Spirit",
        "ownerName": "Alice Smith",
        "ownerAffiliation": "Boothbay YC",
        "ownerLocation": "Maine USA",
        "yachtNameDuplicate": "Sea Spirit",
        "yachtType": "J/24",
        "eventUrl": "https://www.yachtscoring.com/current_event_entries/99901",
        "concatenate": "USA-42Sea SpiritJ/24",
    })
    row.update(kwargs)
    return row


def _make_dir(
    tmp_path: Path,
    scraped_events: list[dict] | None = None,
    scraped_entries: list[dict] | None = None,
    dedup_entries: list[dict] | None = None,
    unique_yachts: list[dict] | None = None,
) -> Path:
    """Build a minimal yacht scoring directory fixture."""
    if scraped_events is not None:
        _write_csv(
            tmp_path / "scrapedEvents" / "events.csv",
            SCRAPED_EVENT_HEADERS,
            scraped_events,
        )
    if scraped_entries is not None:
        _write_csv(
            tmp_path / "scrapedEntries" / "entries.csv",
            SCRAPED_ENTRY_HEADERS,
            scraped_entries,
        )
    if dedup_entries is not None:
        _write_csv(
            tmp_path / "deduplicated_entries.csv",
            DEDUP_ENTRY_HEADERS,
            dedup_entries,
        )
    if unique_yachts is not None:
        _write_csv(
            tmp_path / "unique_yachts.csv",
            UNIQUE_YACHT_HEADERS,
            unique_yachts,
        )
    return tmp_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_counters() -> RunCounters:
    return RunCounters()


def _make_rejects(tmp_path: Path) -> RejectWriter:
    return RejectWriter(tmp_path / "rejects.csv")


# ---------------------------------------------------------------------------
# Test: end-to-end happy path (scraped event + dedup entry)
# ---------------------------------------------------------------------------

class TestEndToEndHappyPath:
    def test_event_and_entry_ingested(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry()],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        # Raw rows captured
        assert counters.yacht_scoring_rows_raw_inserted == 2
        # Event upserted
        assert counters.yacht_scoring_events_upserted == 1
        # Entry upserted
        assert counters.yacht_scoring_entries_upserted == 1
        # No DB errors
        assert counters.db_phase_errors == 0
        assert counters.yacht_scoring_rows_curated_rejected == 0

    def test_xref_event_populated(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(tmp_path, scraped_events=[_make_scraped_event()])
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        conn.autocommit = True
        row = conn.execute(
            "SELECT source_event_id FROM yacht_scoring_xref_event WHERE source_event_id = '99901'"
        ).fetchone()
        assert row is not None, "xref_event not populated"

    def test_xref_entry_populated(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry()],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        conn.autocommit = True
        row = conn.execute(
            """
            SELECT source_entry_id FROM yacht_scoring_xref_entry
            WHERE source_event_id = '99901' AND source_entry_id = '111'
            """
        ).fetchone()
        assert row is not None, "xref_entry not populated"


# ---------------------------------------------------------------------------
# Test: raw completeness (all input rows captured)
# ---------------------------------------------------------------------------

class TestRawCompleteness:
    def test_all_rows_in_raw_table(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[
                _make_dedup_entry(),
                _make_dedup_entry(
                    sailNumber="USA-100",
                    yachtName="Spirit II",
                    entryUrl="https://www.yachtscoring.com/boatdetail/99901/222",
                ),
            ],
            unique_yachts=[_make_unique_yacht()],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        # 1 scraped_event + 2 dedup + 1 unique_yacht = 4 rows
        assert counters.rows_read == 4
        assert counters.yacht_scoring_rows_raw_inserted == 4

    def test_raw_payload_contains_all_columns(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(tmp_path, scraped_events=[_make_scraped_event()])
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        conn.autocommit = True
        row = conn.execute(
            "SELECT raw_payload FROM yacht_scoring_raw_row WHERE asset_type = 'scraped_event_listing'"
        ).fetchone()
        assert row is not None
        payload = row[0]
        assert "title-small href" in payload
        assert "w-[20%]" in payload


# ---------------------------------------------------------------------------
# Test: idempotency (rerun produces stable counts)
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_rerun_no_duplicate_raw_rows(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry()],
        )

        for _ in range(2):
            counters = _make_counters()
            rejects = _make_rejects(tmp_path)
            _run_yacht_scoring(
                "test-run", "2024-01-01", dsn, counters, rejects,
                yacht_scoring_dir=str(root),
                max_reject_rate=0.5,
                dry_run=False,
            )

        conn.autocommit = True
        count = conn.execute("SELECT COUNT(*) FROM yacht_scoring_raw_row").fetchone()[0]
        assert count == 2  # 1 event + 1 entry, no duplicates

    def test_rerun_no_duplicate_event_instances(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(tmp_path, scraped_events=[_make_scraped_event()])

        for _ in range(2):
            counters = _make_counters()
            rejects = _make_rejects(tmp_path)
            _run_yacht_scoring(
                "test-run", "2024-01-01", dsn, counters, rejects,
                yacht_scoring_dir=str(root),
                max_reject_rate=0.5,
                dry_run=False,
            )

        conn.autocommit = True
        count = conn.execute("SELECT COUNT(*) FROM event_instance").fetchone()[0]
        assert count == 1

    def test_rerun_no_duplicate_entries(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry()],
        )

        for _ in range(2):
            counters = _make_counters()
            rejects = _make_rejects(tmp_path)
            _run_yacht_scoring(
                "test-run", "2024-01-01", dsn, counters, rejects,
                yacht_scoring_dir=str(root),
                max_reject_rate=0.5,
                dry_run=False,
            )

        conn.autocommit = True
        count = conn.execute("SELECT COUNT(*) FROM event_entry").fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Test: ambiguous participant reject preserves raw row
# ---------------------------------------------------------------------------

class TestAmbiguousParticipantReject:
    def test_ambiguous_participant_raw_preserved(self, db_conn, tmp_path):
        conn, dsn = db_conn

        # Pre-insert two participants with the same normalized name
        conn.autocommit = True
        conn.execute(
            "INSERT INTO participant (full_name, normalized_full_name) VALUES ('Alice Smith', 'alice smith')"
        )
        conn.execute(
            "INSERT INTO participant (full_name, normalized_full_name) VALUES ('Alice Smith', 'alice smith')"
        )
        conn.autocommit = False

        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry(ownerName="Alice Smith")],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.9,
            dry_run=False,
        )

        # Raw still captured (2 rows: 1 event + 1 entry)
        conn.autocommit = True
        raw_count = conn.execute("SELECT COUNT(*) FROM yacht_scoring_raw_row").fetchone()[0]
        assert raw_count == 2

        # Curated entry reject counted
        assert counters.yacht_scoring_rows_curated_rejected >= 1

    def test_ambiguous_yacht_reject_raw_preserved(self, db_conn, tmp_path):
        conn, dsn = db_conn

        # Pre-insert two yachts with same normalized name and no sail
        conn.autocommit = True
        conn.execute(
            "INSERT INTO yacht (name, normalized_name) VALUES ('Sea Spirit', 'sea-spirit')"
        )
        conn.execute(
            "INSERT INTO yacht (name, normalized_name) VALUES ('Sea Spirit', 'sea-spirit')"
        )
        conn.autocommit = False

        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry(sailNumber="", yachtName="Sea Spirit", yachtType="")],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.9,
            dry_run=False,
        )

        conn.autocommit = True
        raw_count = conn.execute("SELECT COUNT(*) FROM yacht_scoring_raw_row").fetchone()[0]
        assert raw_count == 2  # raw always captured
        assert counters.yacht_scoring_rows_curated_rejected >= 1


# ---------------------------------------------------------------------------
# Test: dry-run rolls back all changes
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_no_raw_rows_persisted(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry()],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=True,
        )

        conn.autocommit = True
        raw_count = conn.execute("SELECT COUNT(*) FROM yacht_scoring_raw_row").fetchone()[0]
        assert raw_count == 0  # all rolled back

    def test_dry_run_no_event_instances_persisted(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(tmp_path, scraped_events=[_make_scraped_event()])
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=True,
        )

        conn.autocommit = True
        count = conn.execute("SELECT COUNT(*) FROM event_instance").fetchone()[0]
        assert count == 0

    def test_dry_run_counters_reflect_processing(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry()],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=True,
        )

        # Rows were processed (counters updated) even though DB rolled back
        assert counters.rows_read == 2
        assert counters.yacht_scoring_rows_raw_inserted == 2


# ---------------------------------------------------------------------------
# Test: unknown schema treated as raw-only
# ---------------------------------------------------------------------------

class TestUnknownSchema:
    def test_file_with_bad_headers_ingested_raw_only(self, db_conn, tmp_path):
        conn, dsn = db_conn

        # Create a scraped events file but with completely wrong headers
        bad_file = tmp_path / "scrapedEvents" / "bad.csv"
        bad_file.parent.mkdir(parents=True)
        bad_file.write_text("col1,col2\nval1,val2\n")

        root = tmp_path
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        # 1 row read
        assert counters.rows_read == 1
        # Treated as unknown schema
        assert counters.yacht_scoring_unknown_schema_rows == 1
        # Still inserted into raw table (as 'unknown' asset_type)
        assert counters.yacht_scoring_rows_raw_inserted == 1
        # No curated processing attempted
        assert counters.yacht_scoring_rows_curated_processed == 0
        assert counters.db_phase_errors == 0


# ---------------------------------------------------------------------------
# Test: scraped entry reuses xref from deduplicated entry
# ---------------------------------------------------------------------------

class TestScrapedEntryXrefReuse:
    def test_scraped_entry_reuses_entry_from_dedup(self, db_conn, tmp_path):
        conn, dsn = db_conn

        # Same event_id/entry_id in both dedup and scraped entry
        root = _make_dir(
            tmp_path,
            scraped_events=[_make_scraped_event()],
            dedup_entries=[_make_dedup_entry()],
            scraped_entries=[_make_scraped_entry()],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        # Should have exactly 1 event_entry (scraped entry reused xref from dedup)
        conn.autocommit = True
        count = conn.execute("SELECT COUNT(*) FROM event_entry").fetchone()[0]
        assert count == 1

        # 3 raw rows: 1 event + 1 dedup + 1 scraped entry
        assert counters.yacht_scoring_rows_raw_inserted == 3
        assert counters.db_phase_errors == 0


# ---------------------------------------------------------------------------
# Test: entry with unresolved event link → curated reject, raw kept
# ---------------------------------------------------------------------------

class TestUnresolvedEventLink:
    def test_dedup_entry_without_event_in_xref(self, db_conn, tmp_path):
        conn, dsn = db_conn

        # No scraped events → xref_event is empty
        root = _make_dir(
            tmp_path,
            dedup_entries=[_make_dedup_entry()],
        )
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=1.0,
            dry_run=False,
        )

        # Raw row still captured
        conn.autocommit = True
        raw_count = conn.execute("SELECT COUNT(*) FROM yacht_scoring_raw_row").fetchone()[0]
        assert raw_count == 1

        # Curated rejected
        assert counters.yacht_scoring_rows_curated_rejected == 1

        # No event_entry created
        entry_count = conn.execute("SELECT COUNT(*) FROM event_entry").fetchone()[0]
        assert entry_count == 0


# ---------------------------------------------------------------------------
# Test: unique yacht enrichment
# ---------------------------------------------------------------------------

class TestUniqueYachtEnrichment:
    def test_unique_yacht_creates_yacht_if_absent(self, db_conn, tmp_path):
        conn, dsn = db_conn
        root = _make_dir(tmp_path, unique_yachts=[_make_unique_yacht()])
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        conn.autocommit = True
        count = conn.execute("SELECT COUNT(*) FROM yacht").fetchone()[0]
        assert count == 1
        assert counters.yacht_scoring_yachts_upserted == 1

    def test_unique_yacht_fills_null_model(self, db_conn, tmp_path):
        conn, dsn = db_conn

        # Pre-insert yacht without model
        conn.autocommit = True
        conn.execute(
            """
            INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number)
            VALUES ('Sea Spirit', 'sea-spirit', 'USA-42', 'usa-42')
            """
        )
        conn.autocommit = False

        root = _make_dir(tmp_path, unique_yachts=[_make_unique_yacht(yachtType="J/24")])
        counters = _make_counters()
        rejects = _make_rejects(tmp_path)

        _run_yacht_scoring(
            "test-run", "2024-01-01", dsn, counters, rejects,
            yacht_scoring_dir=str(root),
            max_reject_rate=0.5,
            dry_run=False,
        )

        conn.autocommit = True
        model = conn.execute("SELECT model FROM yacht WHERE normalized_name = 'sea-spirit'").fetchone()[0]
        assert model == "J/24"
