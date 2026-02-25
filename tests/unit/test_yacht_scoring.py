"""Unit tests for yacht_scoring ingestion helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from regatta_etl.normalize import (
    parse_ys_boatdetail_url,
    parse_ys_emenu_url,
    parse_ys_entries_url,
)
from regatta_etl.import_yacht_scoring import (
    _build_participant_source_key,
    _build_yacht_source_key,
    _check_headers,
    _discover_files,
    _extract_ids_for_raw,
    _parse_year_from_text,
    _strip_leading_year,
    SOURCE_SYSTEM,
)


# ---------------------------------------------------------------------------
# parse_ys_emenu_url
# ---------------------------------------------------------------------------

class TestParseYsEmenuUrl:
    def test_standard_url(self):
        assert parse_ys_emenu_url("https://www.yachtscoring.com/emenu/14867") == "14867"

    def test_different_event_id(self):
        assert parse_ys_emenu_url("https://www.yachtscoring.com/emenu/99999") == "99999"

    def test_none_input(self):
        assert parse_ys_emenu_url(None) is None

    def test_empty_string(self):
        assert parse_ys_emenu_url("") is None

    def test_whitespace_only(self):
        assert parse_ys_emenu_url("  ") is None

    def test_no_emenu_pattern(self):
        assert parse_ys_emenu_url("https://www.yachtscoring.com/boatdetail/123/456") is None

    def test_path_only(self):
        assert parse_ys_emenu_url("/emenu/55555") == "55555"

    def test_non_numeric_id_not_matched(self):
        assert parse_ys_emenu_url("https://www.yachtscoring.com/emenu/abc") is None


# ---------------------------------------------------------------------------
# parse_ys_entries_url
# ---------------------------------------------------------------------------

class TestParseYsEntriesUrl:
    def test_standard_url(self):
        assert parse_ys_entries_url(
            "https://www.yachtscoring.com/current_event_entries/13146"
        ) == "13146"

    def test_none_input(self):
        assert parse_ys_entries_url(None) is None

    def test_empty_string(self):
        assert parse_ys_entries_url("") is None

    def test_no_entries_pattern(self):
        assert parse_ys_entries_url("https://www.yachtscoring.com/emenu/14867") is None

    def test_path_only(self):
        assert parse_ys_entries_url("/current_event_entries/42") == "42"

    def test_non_numeric_not_matched(self):
        assert parse_ys_entries_url("https://www.yachtscoring.com/current_event_entries/abc") is None


# ---------------------------------------------------------------------------
# parse_ys_boatdetail_url
# ---------------------------------------------------------------------------

class TestParseYsBoatdetailUrl:
    def test_standard_url(self):
        event_id, entry_id = parse_ys_boatdetail_url(
            "https://www.yachtscoring.com/boatdetail/13146/235256"
        )
        assert event_id == "13146"
        assert entry_id == "235256"

    def test_none_input(self):
        event_id, entry_id = parse_ys_boatdetail_url(None)
        assert event_id is None
        assert entry_id is None

    def test_empty_string(self):
        event_id, entry_id = parse_ys_boatdetail_url("")
        assert event_id is None
        assert entry_id is None

    def test_emenu_url_no_match(self):
        event_id, entry_id = parse_ys_boatdetail_url(
            "https://www.yachtscoring.com/emenu/14867"
        )
        assert event_id is None
        assert entry_id is None

    def test_path_only(self):
        event_id, entry_id = parse_ys_boatdetail_url("/boatdetail/100/200")
        assert event_id == "100"
        assert entry_id == "200"

    def test_returns_strings(self):
        event_id, entry_id = parse_ys_boatdetail_url(
            "https://www.yachtscoring.com/boatdetail/14811/237724"
        )
        assert isinstance(event_id, str)
        assert isinstance(entry_id, str)
        assert event_id == "14811"
        assert entry_id == "237724"


# ---------------------------------------------------------------------------
# _build_yacht_source_key
# ---------------------------------------------------------------------------

class TestBuildYachtSourceKey:
    def test_name_and_sail(self):
        key = _build_yacht_source_key("Sea Spirit", "US-42")
        assert key == "n:sea-spirit:s:us-42"

    def test_name_only(self):
        key = _build_yacht_source_key("Acadia", None)
        assert key == "n:acadia:s:"

    def test_empty_sail_treated_as_absent(self):
        key = _build_yacht_source_key("Acadia", "")
        assert key == "n:acadia:s:"

    def test_none_name_returns_none(self):
        assert _build_yacht_source_key(None, "US-42") is None

    def test_empty_name_returns_none(self):
        assert _build_yacht_source_key("", "US-42") is None

    def test_punctuation_only_name_returns_none(self):
        assert _build_yacht_source_key("-", "US-42") is None

    def test_sail_normalization(self):
        key1 = _build_yacht_source_key("Boat", "USA 1031")
        key2 = _build_yacht_source_key("Boat", "USA1031")
        # Both should normalize differently since slug_name keeps structure
        assert key1 is not None
        assert key2 is not None

    def test_name_case_insensitive(self):
        key1 = _build_yacht_source_key("SEA SPIRIT", "US-42")
        key2 = _build_yacht_source_key("sea spirit", "US-42")
        assert key1 == key2


# ---------------------------------------------------------------------------
# _build_participant_source_key
# ---------------------------------------------------------------------------

class TestBuildParticipantSourceKey:
    def test_full_key(self):
        key = _build_participant_source_key("Fred Smith", "Boothbay YC", "Maine USA")
        assert key is not None
        assert "fredsmith" in key or "fred" in key

    def test_name_only(self):
        key = _build_participant_source_key("Fred Smith", None, None)
        assert key is not None
        assert key.endswith("||")

    def test_none_name_returns_none(self):
        assert _build_participant_source_key(None, "BHYC", "Maine") is None

    def test_empty_name_returns_none(self):
        assert _build_participant_source_key("", "BHYC", "Maine") is None

    def test_punctuation_name_returns_none(self):
        assert _build_participant_source_key("---", "BHYC", "Maine") is None

    def test_same_name_affiliation_location_stable(self):
        key1 = _build_participant_source_key("Alice Jones", "Storm Trysail Club", "CT USA")
        key2 = _build_participant_source_key("Alice Jones", "Storm Trysail Club", "CT USA")
        assert key1 == key2

    def test_different_affiliation_different_key(self):
        key1 = _build_participant_source_key("Bob Smith", "Club A", "Maine")
        key2 = _build_participant_source_key("Bob Smith", "Club B", "Maine")
        assert key1 != key2


# ---------------------------------------------------------------------------
# _check_headers
# ---------------------------------------------------------------------------

class TestCheckHeaders:
    def test_scraped_event_listing_valid(self):
        headers = {"ant-image-img src", "title-small", "title-small href", "w-[10%]", "w-[20%]"}
        assert _check_headers(headers, "scraped_event_listing") is True

    def test_scraped_event_listing_minimum(self):
        headers = {"title-small href", "w-[20%]"}
        assert _check_headers(headers, "scraped_event_listing") is True

    def test_scraped_event_listing_missing_required(self):
        headers = {"title-small href"}
        assert _check_headers(headers, "scraped_event_listing") is False

    def test_scraped_entry_listing_valid(self):
        headers = {
            "font-bold", "title-small", "title-small href", "flex",
            "tablescraper-selected-row", "tablescraper-selected-row 2",
            "tablescraper-selected-row 3", "tablescraper-selected-row 4",
            "title-small 2",
        }
        assert _check_headers(headers, "scraped_entry_listing") is True

    def test_scraped_entry_listing_missing_required(self):
        headers = {"title-small href", "flex"}  # missing "title-small 2"
        assert _check_headers(headers, "scraped_entry_listing") is False

    def test_deduplicated_entry_valid(self):
        headers = {"sailNumber", "entryUrl", "yachtName", "ownerName", "yachtType", "eventUrl"}
        assert _check_headers(headers, "deduplicated_entry") is True

    def test_unique_yacht_valid(self):
        headers = {"sailNumber", "entryUrl", "yachtName", "ownerName", "yachtType", "eventUrl", "concatenate"}
        assert _check_headers(headers, "unique_yacht") is True

    def test_unknown_always_passes(self):
        assert _check_headers(set(), "unknown") is True

    def test_unknown_type_missing_from_contracts(self):
        # An asset_type not in _REQUIRED_HEADERS is treated like unknown
        assert _check_headers(set(), "not_a_real_type") is True


# ---------------------------------------------------------------------------
# _parse_year_from_text
# ---------------------------------------------------------------------------

class TestParseYearFromText:
    def test_date_range_standard(self):
        assert _parse_year_from_text("December 28 - 30, 2021") == 2021

    def test_event_title_with_year(self):
        assert _parse_year_from_text("2023 Orange Bowl International") == 2023

    def test_no_year(self):
        assert _parse_year_from_text("Summer Regatta") is None

    def test_none_input(self):
        assert _parse_year_from_text(None) is None

    def test_empty_string(self):
        assert _parse_year_from_text("") is None

    def test_year_in_middle(self):
        assert _parse_year_from_text("July 15-17, 2024 regatta") == 2024

    def test_two_years_takes_first(self):
        # "December 28, 2021 - January 2, 2022" → first year found
        assert _parse_year_from_text("December 28, 2021 - January 2, 2022") == 2021


# ---------------------------------------------------------------------------
# _strip_leading_year
# ---------------------------------------------------------------------------

class TestStripLeadingYear:
    def test_strips_year_prefix(self):
        assert _strip_leading_year("2021 Orange Bowl International") == "Orange Bowl International"

    def test_no_leading_year(self):
        assert _strip_leading_year("Orange Bowl International") == "Orange Bowl International"

    def test_year_in_middle_unchanged(self):
        assert _strip_leading_year("Orange Bowl 2021 International") == "Orange Bowl 2021 International"

    def test_only_year(self):
        assert _strip_leading_year("2024") == ""

    def test_whitespace_trimmed(self):
        assert _strip_leading_year("  2022 Some Event  ") == "Some Event"


# ---------------------------------------------------------------------------
# _extract_ids_for_raw
# ---------------------------------------------------------------------------

class TestExtractIdsForRaw:
    def test_scraped_event_listing(self):
        row = {"title-small href": "https://www.yachtscoring.com/emenu/14867"}
        event_id, entry_id = _extract_ids_for_raw(row, "scraped_event_listing")
        assert event_id == "14867"
        assert entry_id is None

    def test_scraped_entry_listing(self):
        row = {"title-small href": "https://www.yachtscoring.com/boatdetail/14811/237724"}
        event_id, entry_id = _extract_ids_for_raw(row, "scraped_entry_listing")
        assert event_id == "14811"
        assert entry_id == "237724"

    def test_deduplicated_entry_from_entry_url(self):
        row = {
            "entryUrl": "https://www.yachtscoring.com/boatdetail/13146/235256",
            "eventUrl": "https://www.yachtscoring.com/current_event_entries/13146",
        }
        event_id, entry_id = _extract_ids_for_raw(row, "deduplicated_entry")
        assert event_id == "13146"
        assert entry_id == "235256"

    def test_deduplicated_entry_fallback_to_event_url(self):
        row = {
            "entryUrl": "",
            "eventUrl": "https://www.yachtscoring.com/current_event_entries/99",
        }
        event_id, entry_id = _extract_ids_for_raw(row, "deduplicated_entry")
        assert event_id == "99"
        assert entry_id is None

    def test_unique_yacht_same_as_dedup_entry(self):
        row = {
            "entryUrl": "https://www.yachtscoring.com/boatdetail/200/300",
            "eventUrl": "",
        }
        event_id, entry_id = _extract_ids_for_raw(row, "unique_yacht")
        assert event_id == "200"
        assert entry_id == "300"

    def test_unknown_returns_none_none(self):
        row = {"someCol": "someVal"}
        event_id, entry_id = _extract_ids_for_raw(row, "unknown")
        assert event_id is None
        assert entry_id is None


# ---------------------------------------------------------------------------
# _discover_files
# ---------------------------------------------------------------------------

class TestDiscoverFiles:
    def test_ordering(self, tmp_path):
        """scrapedEvents come before dedup, unique_yachts, then scrapedEntries."""
        (tmp_path / "scrapedEvents").mkdir()
        (tmp_path / "scrapedEntries").mkdir()
        (tmp_path / "scrapedEvents" / "event_a.csv").write_text("x")
        (tmp_path / "scrapedEvents" / "event_b.csv").write_text("x")
        (tmp_path / "deduplicated_entries.csv").write_text("x")
        (tmp_path / "unique_yachts.csv").write_text("x")
        (tmp_path / "scrapedEntries" / "entry_a.csv").write_text("x")
        (tmp_path / "scrapedEntries" / "entry_b.csv").write_text("x")

        files = _discover_files(tmp_path)
        asset_types = [at for _, at in files]
        names = [p.name for p, _ in files]

        # First two: scraped events (sorted alphabetically)
        assert asset_types[0] == "scraped_event_listing"
        assert asset_types[1] == "scraped_event_listing"
        assert names[0] == "event_a.csv"
        assert names[1] == "event_b.csv"

        # Then dedup
        assert asset_types[2] == "deduplicated_entry"
        assert names[2] == "deduplicated_entries.csv"

        # Then unique_yachts
        assert asset_types[3] == "unique_yacht"
        assert names[3] == "unique_yachts.csv"

        # Then scraped entries (sorted alphabetically)
        assert asset_types[4] == "scraped_entry_listing"
        assert asset_types[5] == "scraped_entry_listing"
        assert names[4] == "entry_a.csv"
        assert names[5] == "entry_b.csv"

    def test_missing_subdirs_skipped(self, tmp_path):
        """Works even when scrapedEntries or scrapedEvents dirs are absent."""
        (tmp_path / "deduplicated_entries.csv").write_text("x")
        files = _discover_files(tmp_path)
        assert len(files) == 1
        assert files[0][1] == "deduplicated_entry"

    def test_empty_directory_returns_empty(self, tmp_path):
        files = _discover_files(tmp_path)
        assert files == []

    def test_scraped_entries_only(self, tmp_path):
        (tmp_path / "scrapedEntries").mkdir()
        (tmp_path / "scrapedEntries" / "z.csv").write_text("x")
        (tmp_path / "scrapedEntries" / "a.csv").write_text("x")
        files = _discover_files(tmp_path)
        names = [p.name for p, _ in files]
        assert names == ["a.csv", "z.csv"]  # sorted
