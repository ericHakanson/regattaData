"""Unit tests for airtable_copy parsing helpers."""

from __future__ import annotations

import pytest

from regatta_etl.import_airtable_copy import (
    _canonical_race_key,
    _check_source_type,
    _extract_row_metadata,
    _extract_year_from_source,
    _parse_event_global_id,
    _race_key_from_url,
    REQUIRED_HEADERS,
    SOURCE_SYSTEM,
)
from regatta_etl.shared import RunCounters


# ---------------------------------------------------------------------------
# _extract_year_from_source
# ---------------------------------------------------------------------------

class TestExtractYearFromSource:
    def test_regattaman_year(self):
        assert _extract_year_from_source("regattaman_2023") == 2023

    def test_yachtscoring_year(self):
        assert _extract_year_from_source("yachtScoring_2022") == 2022

    def test_bare_year_string(self):
        assert _extract_year_from_source("2021") == 2021

    def test_no_year(self):
        assert _extract_year_from_source("yachtScoring") is None

    def test_none_input(self):
        assert _extract_year_from_source(None) is None

    def test_empty_string(self):
        assert _extract_year_from_source("") is None

    def test_year_in_middle(self):
        assert _extract_year_from_source("foo_2024_bar") == 2024


# ---------------------------------------------------------------------------
# _parse_event_global_id
# ---------------------------------------------------------------------------

class TestParseEventGlobalId:
    def test_standard_json(self):
        raw = '{"race_id":"537", "yr":"2021"}'
        race_id, yr = _parse_event_global_id(raw)
        assert race_id == "537"
        assert yr == 2021

    def test_integer_values(self):
        raw = '{"race_id": 99, "yr": 2023}'
        race_id, yr = _parse_event_global_id(raw)
        assert race_id == "99"
        assert yr == 2023

    def test_blank_event_global_id(self):
        race_id, yr = _parse_event_global_id("")
        assert race_id is None
        assert yr is None

    def test_none_input(self):
        race_id, yr = _parse_event_global_id(None)
        assert race_id is None
        assert yr is None

    def test_invalid_json(self):
        race_id, yr = _parse_event_global_id("not-json")
        assert race_id is None
        assert yr is None

    def test_missing_yr(self):
        raw = '{"race_id":"100"}'
        race_id, yr = _parse_event_global_id(raw)
        assert race_id == "100"
        assert yr is None

    def test_missing_race_id(self):
        raw = '{"yr":"2025"}'
        race_id, yr = _parse_event_global_id(raw)
        assert race_id is None
        assert yr == 2025

    def test_non_numeric_yr(self):
        raw = '{"race_id":"5", "yr":"abc"}'
        race_id, yr = _parse_event_global_id(raw)
        assert race_id == "5"
        assert yr is None


# ---------------------------------------------------------------------------
# _canonical_race_key
# ---------------------------------------------------------------------------

class TestCanonicalRaceKey:
    def test_full_key(self):
        assert _canonical_race_key("537", 2021) == "race:537:yr:2021"

    def test_none_race_id(self):
        assert _canonical_race_key(None, 2021) is None

    def test_none_yr(self):
        assert _canonical_race_key("537", None) is None

    def test_both_none(self):
        assert _canonical_race_key(None, None) is None


# ---------------------------------------------------------------------------
# _race_key_from_url
# ---------------------------------------------------------------------------

class TestRaceKeyFromUrl:
    def test_entries_url(self):
        url = "https://regattaman.com/scratch.php?race_id=537&yr=2021&sort=0"
        assert _race_key_from_url(url) == "race:537:yr:2021"

    def test_event_url(self):
        url = "https://regattaman.com/new_event_page.php?race_id=100&yr=2024"
        assert _race_key_from_url(url) == "race:100:yr:2024"

    def test_no_yr_param(self):
        url = "https://regattaman.com/scratch.php?race_id=537"
        assert _race_key_from_url(url) is None

    def test_none_input(self):
        assert _race_key_from_url(None) is None

    def test_blank_string(self):
        assert _race_key_from_url("") is None

    def test_non_regattaman_url(self):
        assert _race_key_from_url("https://example.com/results") is None


# ---------------------------------------------------------------------------
# _check_source_type
# ---------------------------------------------------------------------------

class TestCheckSourceType:
    def test_known_regattaman_types(self):
        counters = RunCounters()
        for src in ["regattaman_2021", "regattaman_2022", "regattaman_2025"]:
            _check_source_type(src, counters, "events", "test-run")
        assert counters.airtable_warnings_unknown_source_type == 0
        assert counters.warnings == []

    def test_known_yachtscoring_types(self):
        counters = RunCounters()
        for src in ["yachtScoring", "yachtScoring_2021", "yachtScoring_2023"]:
            _check_source_type(src, counters, "events", "test-run")
        assert counters.airtable_warnings_unknown_source_type == 0

    def test_unknown_source_type_increments_counter(self):
        counters = RunCounters()
        _check_source_type("custom_source_2025", counters, "entries", "test-run")
        assert counters.airtable_warnings_unknown_source_type == 1
        assert len(counters.warnings) == 1
        assert "custom_source_2025" in counters.warnings[0]

    def test_none_source_type_no_warning(self):
        counters = RunCounters()
        _check_source_type(None, counters, "clubs", "test-run")
        assert counters.airtable_warnings_unknown_source_type == 0

    def test_empty_string_no_warning(self):
        counters = RunCounters()
        _check_source_type("", counters, "clubs", "test-run")
        assert counters.airtable_warnings_unknown_source_type == 0

    def test_multiple_unknown_types_accumulate(self):
        counters = RunCounters()
        _check_source_type("unknown_a", counters, "events", "r1")
        _check_source_type("unknown_b", counters, "events", "r1")
        assert counters.airtable_warnings_unknown_source_type == 2
        assert len(counters.warnings) == 2


# ---------------------------------------------------------------------------
# _extract_row_metadata — source_primary_id and source_type extraction
# ---------------------------------------------------------------------------

class TestExtractRowMetadata:
    def test_clubs_extracts_club_global_id(self):
        row = {"Name": "Test Club", "club_global_id": "recABC123", "Website": ""}
        row_hash, source_primary_id, source_type = _extract_row_metadata(row, "clubs")
        assert source_primary_id == "recABC123"
        assert source_type is None
        assert len(row_hash) == 64  # SHA-256 hex

    def test_events_extracts_event_global_id_and_source(self):
        row = {
            "Event Name": "Summer Race",
            "event_global_id": '{"race_id":"5","yr":"2022"}',
            "source": "regattaman_2022",
        }
        row_hash, source_primary_id, source_type = _extract_row_metadata(row, "events")
        assert source_primary_id == '{"race_id":"5","yr":"2022"}'
        assert source_type == "regattaman_2022"

    def test_entries_extracts_entries_global_id_and_source(self):
        row = {"entries_global_id": "recXYZ789", "source": "yachtScoring_2023", "Name": "Doe"}
        row_hash, source_primary_id, source_type = _extract_row_metadata(row, "entries")
        assert source_primary_id == "recXYZ789"
        assert source_type == "yachtScoring_2023"

    def test_yachts_no_source_type(self):
        row = {"yachtName": "Wind", "yacht_global_id": "recYacht1"}
        row_hash, source_primary_id, source_type = _extract_row_metadata(row, "yachts")
        assert source_primary_id == "recYacht1"
        assert source_type is None

    def test_blank_primary_id_returns_none(self):
        row = {"yachtName": "Wind", "yacht_global_id": ""}
        _, source_primary_id, _ = _extract_row_metadata(row, "yachts")
        assert source_primary_id is None

    def test_hash_is_deterministic(self):
        row = {"Name": "Club A", "club_global_id": "rec001"}
        h1, _, _ = _extract_row_metadata(row, "clubs")
        h2, _, _ = _extract_row_metadata(row, "clubs")
        assert h1 == h2

    def test_different_rows_produce_different_hashes(self):
        row_a = {"Name": "Club A", "club_global_id": "rec001"}
        row_b = {"Name": "Club B", "club_global_id": "rec002"}
        h_a, _, _ = _extract_row_metadata(row_a, "clubs")
        h_b, _, _ = _extract_row_metadata(row_b, "clubs")
        assert h_a != h_b


# ---------------------------------------------------------------------------
# REQUIRED_HEADERS — contract validation
# ---------------------------------------------------------------------------

class TestRequiredHeaders:
    def test_all_six_assets_defined(self):
        for asset in ("clubs", "events", "entries", "yachts", "owners", "participants"):
            assert asset in REQUIRED_HEADERS
            assert len(REQUIRED_HEADERS[asset]) > 0

    def test_clubs_requires_name(self):
        assert "Name" in REQUIRED_HEADERS["clubs"]

    def test_events_requires_event_name(self):
        assert "Event Name" in REQUIRED_HEADERS["events"]

    def test_entries_requires_eventUuid(self):
        assert "eventUuid" in REQUIRED_HEADERS["entries"]

    def test_participants_requires_email(self):
        assert "competitorE" in REQUIRED_HEADERS["participants"]


# ---------------------------------------------------------------------------
# SOURCE_SYSTEM constant
# ---------------------------------------------------------------------------

def test_source_system_constant():
    assert SOURCE_SYSTEM == "airtable_copy_csv"
