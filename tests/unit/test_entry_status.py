"""Unit tests for entry_status derivation logic."""

import pytest

from regatta_etl.import_regattaman_csv import _derive_entry_status


class TestDeriveEntryStatus:
    def test_paid_date_gives_confirmed(self):
        row = {
            "date_paid": "2025-05-21 10:55:57",
            "Paid Type": "rms",
            "date_entered": "2025-05-21 10:54:49",
        }
        assert _derive_entry_status(row) == "confirmed"

    def test_paid_type_free_gives_confirmed(self):
        row = {
            "date_paid": "0000-00-00 00:00:00",
            "Paid Type": "free",
            "date_entered": "2025-05-21 10:54:49",
        }
        assert _derive_entry_status(row) == "confirmed"

    def test_free_case_insensitive(self):
        row = {
            "date_paid": "",
            "Paid Type": "FREE",
            "date_entered": "2025-06-01 09:00:00",
        }
        assert _derive_entry_status(row) == "confirmed"

    def test_unpaid_non_free_with_date_gives_submitted(self):
        row = {
            "date_paid": "0000-00-00 00:00:00",
            "Paid Type": "rms",
            "date_entered": "2025-05-21 10:54:49",
        }
        assert _derive_entry_status(row) == "submitted"

    def test_unpaid_non_free_no_date_gives_unknown(self):
        row = {
            "date_paid": "0000-00-00 00:00:00",
            "Paid Type": "rms",
            "date_entered": "0000-00-00 00:00:00",
        }
        assert _derive_entry_status(row) == "unknown"

    def test_all_blank_gives_unknown(self):
        row = {
            "date_paid": "",
            "Paid Type": "",
            "date_entered": "",
        }
        assert _derive_entry_status(row) == "unknown"

    def test_paid_type_clb_with_no_date_paid_gives_submitted(self):
        row = {
            "date_paid": "0000-00-00 00:00:00",
            "Paid Type": "clb",
            "date_entered": "2025-06-08 08:57:39",
        }
        assert _derive_entry_status(row) == "submitted"

    def test_valid_date_paid_takes_priority_over_type(self):
        # Even if Paid Type is 'free', a valid date_paid still → confirmed
        row = {
            "date_paid": "2025-06-24 10:16:16",
            "Paid Type": "free",
            "date_entered": "2025-07-09 08:00:45",
        }
        assert _derive_entry_status(row) == "confirmed"
