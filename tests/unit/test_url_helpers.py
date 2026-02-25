"""Unit tests for URL normalization helpers and entry-hash builder."""

import pytest

from regatta_etl.normalize import (
    build_entry_hash,
    canonical_entries_url,
    extract_sku_from_hist,
    parse_race_url,
)


# ---------------------------------------------------------------------------
# parse_race_url
# ---------------------------------------------------------------------------

class TestParseRaceUrl:
    FULL_URL = (
        "https://regattaman.com/scratch.php"
        "?race_id=537&yr=2021&sort=0&ssort=0&sdir=true&ssdir=true"
    )

    def test_extracts_race_id_and_yr(self):
        race_id, yr = parse_race_url(self.FULL_URL)
        assert race_id == "537"
        assert yr == 2021

    def test_returns_none_none_for_none_input(self):
        assert parse_race_url(None) == (None, None)

    def test_returns_none_none_for_empty_string(self):
        assert parse_race_url("") == (None, None)

    def test_returns_none_for_soc_id_url(self):
        url = (
            "https://regattaman.com/def_soc_page.php"
            "?soc_id=70&yr=2021&sort=0&ssort=0&sdir=true&ssdir=true"
        )
        race_id, yr = parse_race_url(url)
        assert race_id is None
        assert yr == 2021  # yr is still parseable

    def test_returns_none_yr_when_yr_missing(self):
        url = "https://regattaman.com/scratch.php?race_id=537"
        race_id, yr = parse_race_url(url)
        assert race_id == "537"
        assert yr is None

    def test_strips_whitespace_from_url(self):
        race_id, yr = parse_race_url("  " + self.FULL_URL + "  ")
        assert race_id == "537"

    def test_minimal_url(self):
        url = "https://regattaman.com/scratch.php?race_id=893&yr=2022"
        race_id, yr = parse_race_url(url)
        assert race_id == "893"
        assert yr == 2022


# ---------------------------------------------------------------------------
# canonical_entries_url
# ---------------------------------------------------------------------------

class TestCanonicalEntriesUrl:
    def test_produces_stable_canonical_form(self):
        full = (
            "https://regattaman.com/scratch.php"
            "?race_id=537&yr=2021&sort=0&ssort=0&sdir=true&ssdir=true"
        )
        canon = canonical_entries_url(full)
        assert canon == "https://regattaman.com/scratch.php?race_id=537&yr=2021"

    def test_same_result_for_different_sort_params(self):
        url1 = "https://regattaman.com/scratch.php?race_id=100&yr=2023&sort=1"
        url2 = "https://regattaman.com/scratch.php?race_id=100&yr=2023&sort=0&sdir=false"
        assert canonical_entries_url(url1) == canonical_entries_url(url2)

    def test_returns_none_for_soc_id_url(self):
        url = "https://regattaman.com/def_soc_page.php?soc_id=70&yr=2021"
        assert canonical_entries_url(url) is None

    def test_returns_none_for_none_input(self):
        assert canonical_entries_url(None) is None

    def test_returns_none_when_yr_missing(self):
        url = "https://regattaman.com/scratch.php?race_id=537"
        assert canonical_entries_url(url) is None


# ---------------------------------------------------------------------------
# extract_sku_from_hist
# ---------------------------------------------------------------------------

class TestExtractSkuFromHist:
    def test_extracts_sku(self):
        hist = "https://regattaman.com/get_race_hist.php?sku=r-537-2021-554-558-0"
        assert extract_sku_from_hist(hist) == "r-537-2021-554-558-0"

    def test_returns_none_for_none(self):
        assert extract_sku_from_hist(None) is None

    def test_returns_none_for_empty(self):
        assert extract_sku_from_hist("") is None

    def test_returns_none_when_no_sku_param(self):
        hist = "https://regattaman.com/get_race_hist.php?race_id=537"
        assert extract_sku_from_hist(hist) is None

    def test_handles_whitespace(self):
        hist = "  https://regattaman.com/get_race_hist.php?sku=r-893-2021-9095-6434-0  "
        assert extract_sku_from_hist(hist) == "r-893-2021-9095-6434-0"


# ---------------------------------------------------------------------------
# build_entry_hash
# ---------------------------------------------------------------------------

class TestBuildEntryHash:
    def test_returns_32_hex_chars(self):
        h = build_entry_hash("regattaman_2021", "https://x.com/s?race_id=1&yr=2021",
                             "SPIN", "Smith, John", "Foobar", "32801")
        assert len(h) == 32
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic_for_same_inputs(self):
        args = ("regattaman_2021", "https://x.com/s?race_id=1&yr=2021",
                "SPIN", "Smith, John", "Foobar", "32801")
        assert build_entry_hash(*args) == build_entry_hash(*args)

    def test_differs_for_different_yacht(self):
        base = ("regattaman_2021", "https://x.com/s?race_id=1&yr=2021",
                "SPIN", "Smith, John")
        h1 = build_entry_hash(*base, "Foobar", "32801")
        h2 = build_entry_hash(*base, "OtherBoat", "32801")
        assert h1 != h2

    def test_differs_for_different_sail_num(self):
        base = ("regattaman_2021", "https://x.com/s?race_id=1&yr=2021",
                "SPIN", "Smith, John", "Foobar")
        assert build_entry_hash(*base, "111") != build_entry_hash(*base, "222")

    def test_empty_sail_num_handled(self):
        h = build_entry_hash("regattaman_2022", "https://x.com/s?race_id=2&yr=2022",
                             "J70", "Jones, Amy", "Swift", "")
        assert len(h) == 32
