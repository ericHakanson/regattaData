"""Unit tests for mailchimp_audience parsing helpers."""

from __future__ import annotations

import pytest

from regatta_etl.import_mailchimp_audience import (
    _parse_member_rating,
    _validate_email_format,
    parse_tags,
)
from regatta_etl.normalize import normalize_email, normalize_phone, parse_ts


# ---------------------------------------------------------------------------
# parse_tags
# ---------------------------------------------------------------------------

class TestParseTags:
    def test_quoted_csv_format(self):
        # Mailchimp stores tags as a quoted CSV string inside the field value
        raw = '"2025 signed waivers","Contact Import December 13 2025 10:04 pm"'
        assert parse_tags(raw) == [
            "2025 signed waivers",
            "Contact Import December 13 2025 10:04 pm",
        ]

    def test_simple_unquoted_tags(self):
        assert parse_tags("tag1,tag2,tag3") == ["tag1", "tag2", "tag3"]

    def test_single_quoted_tag(self):
        assert parse_tags('"2023 Jot Form"') == ["2023 Jot Form"]

    def test_empty_string(self):
        assert parse_tags("") == []

    def test_whitespace_only(self):
        assert parse_tags("   ") == []

    def test_none(self):
        assert parse_tags(None) == []

    def test_trims_whitespace_from_each_tag(self):
        # Mailchimp format: no space between quoted elements; trim applies inside each token
        assert parse_tags('"  tag one  ","  tag two  "') == ["tag one", "tag two"]

    def test_drops_empty_tokens(self):
        assert parse_tags('"tag one",,,"tag two"') == ["tag one", "tag two"]

    def test_single_unquoted_tag(self):
        assert parse_tags("member") == ["member"]

    def test_tags_with_commas_inside_quotes(self):
        # A tag that internally contains a comma (edge case)
        raw = '"Tag, with comma","plain tag"'
        result = parse_tags(raw)
        assert result == ["Tag, with comma", "plain tag"]


# ---------------------------------------------------------------------------
# _validate_email_format
# ---------------------------------------------------------------------------

class TestValidateEmailFormat:
    def test_valid_email(self):
        assert _validate_email_format("user@example.com") is True

    def test_valid_email_subdomain(self):
        assert _validate_email_format("user@mail.example.co.uk") is True

    def test_missing_at(self):
        assert _validate_email_format("userexample.com") is False

    def test_at_at_start(self):
        assert _validate_email_format("@example.com") is False

    def test_at_at_end(self):
        assert _validate_email_format("user@") is False

    def test_no_dot_after_at(self):
        assert _validate_email_format("user@example") is False

    def test_none(self):
        assert _validate_email_format(None) is False

    def test_empty_string(self):
        assert _validate_email_format("") is False


# ---------------------------------------------------------------------------
# _parse_member_rating
# ---------------------------------------------------------------------------

class TestParseMemberRating:
    def test_valid_integer(self):
        assert _parse_member_rating("2") == 2

    def test_valid_zero(self):
        assert _parse_member_rating("0") == 0

    def test_none(self):
        assert _parse_member_rating(None) is None

    def test_empty(self):
        assert _parse_member_rating("") is None

    def test_non_numeric(self):
        assert _parse_member_rating("abc") is None

    def test_float_string(self):
        assert _parse_member_rating("2.5") is None


# ---------------------------------------------------------------------------
# Mailchimp datetime parsing (reuses shared parse_ts)
# ---------------------------------------------------------------------------

class TestMailchimpDatetimeParsing:
    def test_standard_format(self):
        from datetime import datetime
        result = parse_ts("2025-07-30 12:23:18")
        assert result == datetime(2025, 7, 30, 12, 23, 18)

    def test_none(self):
        assert parse_ts(None) is None

    def test_empty(self):
        assert parse_ts("") is None

    def test_invalid_format(self):
        assert parse_ts("07/30/2025") is None

    def test_sentinel_value(self):
        assert parse_ts("0000-00-00 00:00:00") is None


# ---------------------------------------------------------------------------
# normalize_email edge cases (Mailchimp-specific)
# ---------------------------------------------------------------------------

class TestNormalizeEmailMailchimp:
    def test_lowercase_and_trim(self):
        assert normalize_email("  TEST@EXAMPLE.COM  ") == "test@example.com"

    def test_none(self):
        assert normalize_email(None) is None

    def test_already_lowercase(self):
        assert normalize_email("user@example.com") == "user@example.com"


# ---------------------------------------------------------------------------
# normalize_phone edge cases (Mailchimp-specific)
# ---------------------------------------------------------------------------

class TestNormalizePhoneMailchimp:
    def test_us_with_dashes(self):
        assert normalize_phone("251-656-4243") == "+12516564243"

    def test_us_with_parentheses_spaces(self):
        assert normalize_phone("(251) 656-4243") == "+12516564243"

    def test_none(self):
        assert normalize_phone(None) is None

    def test_too_short(self):
        assert normalize_phone("123") is None

    def test_eleven_digit_us(self):
        assert normalize_phone("12516564243") == "+12516564243"
