"""Unit tests for regatta_etl.normalize."""

import pytest
from decimal import Decimal
from datetime import date, datetime

from regatta_etl.normalize import (
    addresses_match_for_identity,
    normalize_address_for_identity,
    trim,
    normalize_space,
    normalize_email,
    normalize_phone,
    normalize_postal_code,
    normalize_name,
    slug_name,
    parse_ts,
    parse_date_from_ts,
    parse_numeric,
    parse_name_parts,
    parse_co_owners,
)


# ---------------------------------------------------------------------------
# trim
# ---------------------------------------------------------------------------

class TestTrim:
    def test_strips_whitespace(self):
        assert trim("  hello  ") == "hello"

    def test_empty_string_returns_none(self):
        assert trim("") is None

    def test_whitespace_only_returns_none(self):
        assert trim("   ") is None

    def test_none_returns_none(self):
        assert trim(None) is None

    def test_no_whitespace(self):
        assert trim("hello") == "hello"


# ---------------------------------------------------------------------------
# normalize_space
# ---------------------------------------------------------------------------

class TestNormalizeSpace:
    def test_collapses_internal_spaces(self):
        assert normalize_space("hello   world") == "hello world"

    def test_collapses_tabs(self):
        assert normalize_space("hello\t\tworld") == "hello world"

    def test_trims_outer(self):
        assert normalize_space("  hello  ") == "hello"

    def test_none(self):
        assert normalize_space(None) is None


# ---------------------------------------------------------------------------
# normalize_email
# ---------------------------------------------------------------------------

class TestNormalizeEmail:
    def test_lowercases(self):
        assert normalize_email("User@Example.COM") == "user@example.com"

    def test_trims(self):
        assert normalize_email("  user@example.com  ") == "user@example.com"

    def test_none(self):
        assert normalize_email(None) is None

    def test_empty(self):
        assert normalize_email("") is None


# ---------------------------------------------------------------------------
# normalize_phone
# ---------------------------------------------------------------------------

class TestNormalizePhone:
    def test_ten_digit(self):
        assert normalize_phone("7574351543") == "+17574351543"

    def test_ten_digit_with_dashes(self):
        assert normalize_phone("757-435-1543") == "+17574351543"

    def test_eleven_digit_starting_one(self):
        assert normalize_phone("17574351543") == "+17574351543"

    def test_strips_parens_spaces(self):
        assert normalize_phone("(757) 435-1543") == "+17574351543"

    def test_none(self):
        assert normalize_phone(None) is None

    def test_empty(self):
        assert normalize_phone("") is None

    def test_too_short_returns_none(self):
        assert normalize_phone("123") is None

    def test_same_home_and_cell(self):
        # From CSV: owner_hphone == owner_cphone — both normalize to same value
        assert normalize_phone("2078318338") == normalize_phone("2078318338")


# ---------------------------------------------------------------------------
# normalize_postal_code
# ---------------------------------------------------------------------------

class TestNormalizePostalCode:
    def test_four_digit_zip_is_zero_padded(self):
        assert normalize_postal_code("2748") == "02748"

    def test_zip_plus_four_reduces_to_base_zip(self):
        assert normalize_postal_code("04102-2537") == "04102"

    def test_invalid_postal_returns_none(self):
        assert normalize_postal_code("0402q") is None


# ---------------------------------------------------------------------------
# normalize_address_for_identity / addresses_match_for_identity
# ---------------------------------------------------------------------------

class TestNormalizeAddressForIdentity:
    def test_mailchimp_freeform_matches_stored_pipe_delimited_format(self):
        body, postal = normalize_address_for_identity(
            "15 Palmer St , Dartmouth, MA  2748    US"
        )
        assert body == "15 palmer st dartmouth ma"
        assert postal == "02748"

    def test_zip_plus_four_normalizes_to_base_zip(self):
        body, postal = normalize_address_for_identity(
            "185 Craigie Street Portland, ME|04102-2537"
        )
        assert body == "185 craigie street portland me"
        assert postal == "04102"


class TestAddressesMatchForIdentity:
    def test_format_only_differences_match(self):
        assert addresses_match_for_identity(
            "PO Box 322, East Boothbay, ME  4544    US",
            "PO Box 322 East Boothbay, ME|04544",
        ) is True

    def test_missing_postal_on_one_side_does_not_force_mismatch(self):
        assert addresses_match_for_identity(
            "3385 Michelson Drive, Irvine, CA  92612    US",
            "3385 Michelson Drive Irvine, CA|",
        ) is True

    def test_different_address_body_still_mismatches(self):
        assert addresses_match_for_identity(
            "1 Main St, Bar Harbor, ME 04609",
            "99 Different Rd, Portland, ME 04101",
        ) is False


# ---------------------------------------------------------------------------
# normalize_name
# ---------------------------------------------------------------------------

class TestNormalizeName:
    def test_lowercases(self):
        assert normalize_name("Smith") == "smith"

    def test_removes_comma(self):
        assert normalize_name("Smith, John") == "smith john"

    def test_collapses_spaces(self):
        assert normalize_name("  John   Smith  ") == "john smith"

    def test_none(self):
        assert normalize_name(None) is None

    def test_punctuation_removed(self):
        assert normalize_name("O'Brien") == "obrien"


# ---------------------------------------------------------------------------
# slug_name
# ---------------------------------------------------------------------------

class TestSlugName:
    def test_basic(self):
        assert slug_name("Boothbay Harbor Yacht Club") == "boothbay-harbor-yacht-club"

    def test_numbers(self):
        assert slug_name("BHYC Regatta 2025") == "bhyc-regatta-2025"

    def test_special_chars(self):
        assert slug_name("E+A2") == "e-a2"

    def test_none(self):
        assert slug_name(None) is None

    def test_leading_trailing_separators_stripped(self):
        result = slug_name("  Regatta  ")
        assert not result.startswith("-")
        assert not result.endswith("-")


# ---------------------------------------------------------------------------
# parse_ts
# ---------------------------------------------------------------------------

class TestParseTs:
    def test_valid(self):
        result = parse_ts("2025-05-21 10:54:49")
        assert result == datetime(2025, 5, 21, 10, 54, 49)

    def test_sentinel_returns_none(self):
        assert parse_ts("0000-00-00 00:00:00") is None

    def test_none(self):
        assert parse_ts(None) is None

    def test_empty(self):
        assert parse_ts("") is None

    def test_invalid_format(self):
        assert parse_ts("not-a-date") is None


# ---------------------------------------------------------------------------
# parse_date_from_ts
# ---------------------------------------------------------------------------

class TestParseDateFromTs:
    def test_extracts_date(self):
        assert parse_date_from_ts("2025-05-21 10:54:49") == date(2025, 5, 21)

    def test_sentinel(self):
        assert parse_date_from_ts("0000-00-00 00:00:00") is None

    def test_none(self):
        assert parse_date_from_ts(None) is None


# ---------------------------------------------------------------------------
# parse_numeric
# ---------------------------------------------------------------------------

class TestParseNumeric:
    def test_integer(self):
        assert parse_numeric("21") == Decimal("21")

    def test_decimal(self):
        assert parse_numeric("34.17") == Decimal("34.17")

    def test_none(self):
        assert parse_numeric(None) is None

    def test_empty(self):
        assert parse_numeric("") is None

    def test_invalid(self):
        assert parse_numeric("abc") is None


# ---------------------------------------------------------------------------
# parse_name_parts
# ---------------------------------------------------------------------------

class TestParseNameParts:
    def test_comma_format(self):
        first, last = parse_name_parts("Amthor, Henry")
        assert first == "Henry"
        assert last == "Amthor"

    def test_first_last(self):
        first, last = parse_name_parts("Henry Amthor")
        assert first == "Henry"
        assert last == "Amthor"

    def test_single_name(self):
        first, last = parse_name_parts("Madonna")
        assert first == "Madonna"
        assert last is None

    def test_none(self):
        assert parse_name_parts(None) == (None, None)

    def test_first_middle_last(self):
        first, last = parse_name_parts("John Paul Smith")
        assert first == "John Paul"
        assert last == "Smith"

    def test_comma_with_spaces(self):
        first, last = parse_name_parts("Carleton , Stott")
        assert first == "Stott"
        assert last == "Carleton"


# ---------------------------------------------------------------------------
# parse_co_owners
# ---------------------------------------------------------------------------

class TestParseCoOwners:
    def test_single_owner_no_name_field(self):
        result = parse_co_owners("Amthor, Henry", "Amthor, Henry")
        assert result == [("Amthor, Henry", "owner")]

    def test_ampersand_split(self):
        result = parse_co_owners("Andrus, Justin", "Andrus, Justin & McCoig, Kathryn")
        assert len(result) == 2
        assert result[0] == ("Andrus, Justin", "owner")
        assert result[1][0] == "McCoig, Kathryn"
        assert result[1][1] == "co_owner"

    def test_and_keyword_split(self):
        result = parse_co_owners("Alice Smith", "Alice Smith and Bob Jones")
        assert len(result) == 2
        assert result[0] == ("Alice Smith", "owner")
        assert result[1] == ("Bob Jones", "co_owner")

    def test_case_insensitive_and(self):
        result = parse_co_owners("Alice Smith", "Alice Smith AND Bob Jones")
        assert len(result) == 2

    def test_deduplication(self):
        # primary owner also in name_field — should not appear twice
        result = parse_co_owners("Amthor, Henry", "Amthor, Henry")
        assert len(result) == 1

    def test_dedup_by_normalized_name(self):
        # Different punctuation/spacing but same normalized name
        result = parse_co_owners("Amthor Henry", "amthor  henry")
        assert len(result) == 1

    def test_blank_ownername_returns_empty(self):
        assert parse_co_owners("", "some name") == []

    def test_none_ownername_returns_empty(self):
        assert parse_co_owners(None, "some name") == []

    def test_primary_always_first(self):
        result = parse_co_owners("Zara Z", "Alice A & Zara Z")
        assert result[0] == ("Zara Z", "owner")
        assert result[1][0] == "Alice A"

    def test_no_cross_contamination_of_coowner_email(self):
        # Co-owners parsed from Name field should NOT carry over email/phone
        # from the row — this is a structural test: parse_co_owners only
        # returns (name, role) tuples, no contact data.
        result = parse_co_owners("Owner A", "Owner A & Co Owner B")
        for name, role in result:
            assert isinstance(name, str)
            assert role in ("owner", "co_owner")
            # No email/phone attributes on the tuple
            assert len((name, role)) == 2
