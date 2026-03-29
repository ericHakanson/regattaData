"""Unit tests for regattaman_snapshot helpers (no DB required)."""

from __future__ import annotations

import pytest

from regatta_etl.import_regattaman_snapshot import (
    _parse_rating,
    _row_primary_id,
    _split_owner_names,
)


# ---------------------------------------------------------------------------
# _split_owner_names
# ---------------------------------------------------------------------------

class TestSplitOwnerNames:
    def test_single_owner(self):
        assert _split_owner_names("cagle, nate") == ["cagle, nate"]

    def test_two_owners_ampersand(self):
        result = _split_owner_names("clarkson, deanna & toby")
        assert result == ["clarkson, deanna", "toby"]

    def test_two_owners_tight_ampersand(self):
        result = _split_owner_names("baker, chip&parker")
        assert result == ["baker, chip", "parker"]

    def test_three_owners(self):
        result = _split_owner_names("smith, john & jones, alice & doe, bob")
        assert result == ["smith, john", "jones, alice", "doe, bob"]

    def test_trailing_spaces_stripped(self):
        result = _split_owner_names("  cagle, nate  ")
        assert result == ["cagle, nate"]

    def test_blank_tokens_discarded(self):
        # Double & produces empty token that should be discarded
        result = _split_owner_names("a && b")
        assert result == ["a", "b"]

    def test_empty_string(self):
        assert _split_owner_names("") == []

    def test_only_ampersand(self):
        assert _split_owner_names("&") == []

    def test_lalumiere_todd_and_todd(self):
        result = _split_owner_names("lalumiere, todd & todd")
        assert result == ["lalumiere, todd", "todd"]

    # ── ' and ' separator (case-insensitive) ──

    def test_and_lowercase(self):
        result = _split_owner_names("smith, alice and jones, bob")
        assert result == ["smith, alice", "jones, bob"]

    def test_and_uppercase(self):
        result = _split_owner_names("smith, alice AND jones, bob")
        assert result == ["smith, alice", "jones, bob"]

    def test_and_mixed_case(self):
        result = _split_owner_names("smith, alice And jones, bob")
        assert result == ["smith, alice", "jones, bob"]

    def test_and_with_extra_spaces(self):
        result = _split_owner_names("smith, alice  and  jones, bob")
        assert result == ["smith, alice", "jones, bob"]

    def test_ampersand_takes_priority_over_and(self):
        # Both delimiters present — ampersand splits first, then the remaining token
        # may still contain ' and '; both produce the same result
        result = _split_owner_names("a & b and c")
        assert result == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# _parse_rating
# ---------------------------------------------------------------------------

class TestParseRating:
    def test_positive_integer(self):
        assert _parse_rating("75") == 75

    def test_negative_integer(self):
        assert _parse_rating("-31") == -31

    def test_zero(self):
        assert _parse_rating("0") == 0

    def test_blank(self):
        assert _parse_rating("") is None

    def test_none_input(self):
        assert _parse_rating(None) is None

    def test_dash(self):
        assert _parse_rating("-") is None

    def test_non_numeric(self):
        assert _parse_rating("abc") is None

    def test_whitespace_only(self):
        assert _parse_rating("   ") is None

    def test_integer_with_spaces(self):
        assert _parse_rating("  84  ") == 84

    def test_large_rating(self):
        assert _parse_rating("126") == 126

    def test_high_negative(self):
        assert _parse_rating("-32") == -32


# ---------------------------------------------------------------------------
# _row_primary_id
# ---------------------------------------------------------------------------

class TestRowPrimaryId:
    def test_deterministic(self):
        a = _row_primary_id("cagle, nate", "mumtaz", "52")
        b = _row_primary_id("cagle, nate", "mumtaz", "52")
        assert a == b

    def test_different_inputs_differ(self):
        a = _row_primary_id("cagle, nate", "mumtaz", "52")
        b = _row_primary_id("hakanson, eric", "charisma", "25")
        assert a != b

    def test_sail_changes_id(self):
        a = _row_primary_id("cagle, nate", "mumtaz", "52")
        b = _row_primary_id("cagle, nate", "mumtaz", "99")
        assert a != b

    def test_returns_hex_string(self):
        result = _row_primary_id("a", "b", "c")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_empty_sail(self):
        # Empty sail still produces a stable id
        a = _row_primary_id("cagle, nate", "mumtaz", "")
        b = _row_primary_id("cagle, nate", "mumtaz", "")
        assert a == b
