"""Unit tests for public_scrape staging layer (parse_event_row, parse_entry_row)."""

from __future__ import annotations

import io
import csv
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from regatta_etl.public_scrape import (
    _parse_source_year,
    parse_event_row,
    parse_entry_row,
)
from regatta_etl.shared import RejectWriter, RunCounters


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rejects(tmp_path: Path) -> RejectWriter:
    return RejectWriter(tmp_path / "rejects.csv")


def _counters() -> RunCounters:
    return RunCounters()


# ---------------------------------------------------------------------------
# _parse_source_year
# ---------------------------------------------------------------------------

class TestParseSourceYear:
    def test_extracts_year(self):
        assert _parse_source_year("regattaman_2021") == 2021
        assert _parse_source_year("regattaman_2025") == 2025

    def test_returns_none_for_bad_format(self):
        assert _parse_source_year("regattaman") is None
        assert _parse_source_year("") is None
        assert _parse_source_year("regattaman_abc") is None

    def test_returns_none_for_no_underscore(self):
        assert _parse_source_year("2021") is None


# ---------------------------------------------------------------------------
# parse_event_row
# ---------------------------------------------------------------------------

VALID_EVENT_ROW = {
    "source": "regattaman_2021",
    "Event Name": "Jack Roberts Memorial NYD Race",
    "entries_url": (
        "https://regattaman.com/scratch.php"
        "?race_id=537&yr=2021&sort=0&ssort=0&sdir=true&ssdir=true"
    ),
    "Host Club": "Constitution YC",
    "Date": "Jan 1",
    "Day": "Fri",
    "When": "Day",
    "Region": "MBSA-S",
    "Event URL": "https://regattaman.com/new_event_page.php?race_id=537",
    "Info URL": "",
    "Results URL": "",
}


class TestParseEventRow:
    def test_parses_valid_row(self, tmp_path):
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_event_row(VALID_EVENT_ROW, rejects, counters)
        assert rec is not None
        assert rec.race_id == "537"
        assert rec.yr == 2021
        assert rec.season_year == 2021
        assert rec.host_club_name == "Constitution YC"
        assert rec.host_club_normalized == "constitution-yc"
        assert rec.event_name == "Jack Roberts Memorial NYD Race"
        assert "race_id=537" in rec.canonical_url
        assert counters.rows_rejected == 0

    def test_soc_id_url_returns_none_tracked(self, tmp_path):
        """Social/meeting events with soc_id in URL are skipped with counter increment."""
        row = dict(VALID_EVENT_ROW)
        row["entries_url"] = "https://regattaman.com/def_soc_page.php?soc_id=70&yr=2021"
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_event_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 0  # not a reject, just a skip
        assert counters.events_skipped_no_race_id == 1

    def test_missing_host_club_rejects(self, tmp_path):
        row = dict(VALID_EVENT_ROW)
        row["Host Club"] = ""
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_event_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 1

    def test_missing_event_name_rejects(self, tmp_path):
        row = dict(VALID_EVENT_ROW)
        row["Event Name"] = "   "
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_event_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 1

    def test_bad_source_year_rejects(self, tmp_path):
        row = dict(VALID_EVENT_ROW)
        row["source"] = "regattaman_xyz"
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_event_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 1

    def test_year_mismatch_rejects(self, tmp_path):
        """source year (regattaman_2022) != URL yr (2021) → reject."""
        row = dict(VALID_EVENT_ROW)
        row["source"] = "regattaman_2022"  # yr in URL is still 2021
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_event_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 1

    def test_event_url_is_optional(self, tmp_path):
        row = dict(VALID_EVENT_ROW)
        row["Event URL"] = ""
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_event_row(row, rejects, counters)
        assert rec is not None
        assert rec.event_url is None


# ---------------------------------------------------------------------------
# parse_entry_row
# ---------------------------------------------------------------------------

VALID_ENTRY_ROW = {
    "source": "regattaman_2021",
    "Event Name": "Jack Roberts Memorial NYD Race",
    "entries_url": (
        "https://regattaman.com/scratch.php"
        "?race_id=537&yr=2021&sort=0&ssort=0&sdir=true&ssdir=true"
    ),
    "Fleet": "SPIN",
    "Name": "Chuang, John",
    "Yacht Name": "Twist",
    "City": "Brookline",
    "Sail Num": "32801",
    "Boat Type": "Beneteau First 42 SD",
    "Split Rating": "-",
    "Race Rat": "807.8",
    "Cru Rat": "845.9",
    "R/C Cfg": "SPIN",
    "Hist": "https://regattaman.com/get_race_hist.php?sku=r-537-2021-554-558-0",
}


class TestParseEntryRow:
    def test_parses_valid_row_with_hist(self, tmp_path):
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_entry_row(VALID_ENTRY_ROW, rejects, counters)
        assert rec is not None
        assert rec.full_name == "Chuang, John"
        assert rec.yacht_name == "Twist"
        assert rec.sail_num == "32801"
        assert rec.registration_external_id == "r-537-2021-554-558-0"
        assert rec.external_id_type == "sku"
        assert rec.season_year == 2021
        assert counters.rows_rejected == 0

    def test_parses_row_without_hist_uses_hash(self, tmp_path):
        row = dict(VALID_ENTRY_ROW)
        row["Hist"] = ""
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_entry_row(row, rejects, counters)
        assert rec is not None
        assert rec.external_id_type == "hash"
        assert len(rec.registration_external_id) == 32

    def test_hash_is_deterministic(self, tmp_path):
        row = dict(VALID_ENTRY_ROW)
        row["Hist"] = ""
        r1 = parse_entry_row(row, _make_rejects(tmp_path), _counters())
        r2 = parse_entry_row(row, _make_rejects(tmp_path), _counters())
        assert r1 is not None and r2 is not None
        assert r1.registration_external_id == r2.registration_external_id

    def test_blank_yacht_name_rejects(self, tmp_path):
        row = dict(VALID_ENTRY_ROW)
        row["Yacht Name"] = ""
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_entry_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 1

    def test_blank_name_rejects(self, tmp_path):
        row = dict(VALID_ENTRY_ROW)
        row["Name"] = ""
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_entry_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 1

    def test_invalid_race_id_url_rejects(self, tmp_path):
        row = dict(VALID_ENTRY_ROW)
        row["entries_url"] = "https://regattaman.com/def_soc_page.php?soc_id=70&yr=2021"
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_entry_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 1

    def test_year_mismatch_rejects(self, tmp_path):
        """source year (regattaman_2022) != URL yr (2021) → reject."""
        row = dict(VALID_ENTRY_ROW)
        row["source"] = "regattaman_2022"  # yr in URL is still 2021
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_entry_row(row, rejects, counters)
        assert rec is None
        assert counters.rows_rejected == 1

    def test_optional_fields_can_be_empty(self, tmp_path):
        row = dict(VALID_ENTRY_ROW)
        row["City"] = ""
        row["Boat Type"] = ""
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_entry_row(row, rejects, counters)
        assert rec is not None
        assert rec.city is None
        assert rec.boat_type is None

    def test_canonical_url_strips_sort_params(self, tmp_path):
        rejects = _make_rejects(tmp_path)
        counters = _counters()
        rec = parse_entry_row(VALID_ENTRY_ROW, rejects, counters)
        assert rec is not None
        assert rec.canonical_url == "https://regattaman.com/scratch.php?race_id=537&yr=2021"
