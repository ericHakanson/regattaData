"""Unit tests for participant_hold_geo_prepare — normalization, classification,
best-hint selection, conflict detection, and junk detection.

No database required.
"""

from __future__ import annotations

import pytest

from regatta_etl.participant_hold_geo_prepare import (
    GeoHint,
    _classify_quality,
    _is_junk,
    _normalize_tokens,
    _select_best_hint,
    normalize_geo_hint,
)


# ---------------------------------------------------------------------------
# _is_junk
# ---------------------------------------------------------------------------

class TestIsJunk:
    def test_exact_boat_class_laser(self):
        assert _is_junk("Laser") is True

    def test_exact_boat_class_optimist(self):
        assert _is_junk("Optimist") is True

    def test_exact_boat_class_case_insensitive(self):
        assert _is_junk("OPTI") is True
        assert _is_junk("opti") is True

    def test_exact_boat_class_j24(self):
        assert _is_junk("J24") is True

    def test_club_name_substring(self):
        assert _is_junk("Boothbay Harbor Yacht Club") is True

    def test_sailing_club_substring(self):
        assert _is_junk("Portland Sailing Club") is True

    def test_marina_substring(self):
        assert _is_junk("Rockland Marina") is True

    def test_corinthian_substring(self):
        assert _is_junk("Corinthian Yacht Club of Portland") is True

    def test_real_city_not_junk(self):
        assert _is_junk("Portland ME USA") is False

    def test_empty_string_not_junk(self):
        assert _is_junk("") is False

    def test_country_only_not_junk(self):
        assert _is_junk("USA") is False

    def test_lightning_exact(self):
        assert _is_junk("Lightning") is True

    def test_partial_match_not_junk(self):
        # Exact junk match requires the WHOLE string to equal a junk entry.
        # "Laser City" does not match the exact token "LASER", so it's not junk.
        assert _is_junk("Laser City") is False
        # But a plain "420" is an exact match for the boat class "420".
        assert _is_junk("420") is True


# ---------------------------------------------------------------------------
# _normalize_tokens
# ---------------------------------------------------------------------------

class TestNormalizeTokens:
    def test_city_state_country(self):
        city, state, country = _normalize_tokens("Portland ME USA")
        assert city == "Portland"
        assert state == "ME"
        assert country == "USA"

    def test_city_country_no_state(self):
        city, state, country = _normalize_tokens("London GBR")
        assert city == "London"
        assert state is None
        assert country == "GBR"

    def test_country_only(self):
        city, state, country = _normalize_tokens("USA")
        assert city is None
        assert state is None
        assert country == "USA"

    def test_state_country_no_city(self):
        city, state, country = _normalize_tokens("ME USA")
        assert city is None
        assert state == "ME"
        assert country == "USA"

    def test_multi_word_country(self):
        city, state, country = _normalize_tokens("Auckland New Zealand")
        assert city == "Auckland"
        assert state is None
        assert country == "NZL"

    def test_multi_word_us_state(self):
        city, state, country = _normalize_tokens("Manchester New Hampshire USA")
        assert city == "Manchester"
        assert state == "NH"
        assert country == "USA"

    def test_comma_separated(self):
        city, state, country = _normalize_tokens("Portland, ME, USA")
        assert city == "Portland"
        assert state == "ME"
        assert country == "USA"

    def test_united_states_of_america_full(self):
        city, state, country = _normalize_tokens("Boston MA United States of America")
        assert city == "Boston"
        assert state == "MA"
        assert country == "USA"

    def test_empty_string(self):
        city, state, country = _normalize_tokens("")
        assert city is None
        assert state is None
        assert country is None

    def test_case_insensitive(self):
        city, state, country = _normalize_tokens("portland me usa")
        assert city == "Portland"
        assert state == "ME"
        assert country == "USA"

    def test_canada(self):
        city, state, country = _normalize_tokens("Vancouver CAN")
        assert city == "Vancouver"
        assert state is None
        assert country == "CAN"

    def test_multi_word_state_rhode_island(self):
        city, state, country = _normalize_tokens("Newport Rhode Island USA")
        assert city == "Newport"
        assert state == "RI"
        assert country == "USA"


# ---------------------------------------------------------------------------
# _classify_quality
# ---------------------------------------------------------------------------

class TestClassifyQuality:
    def test_city_state_country(self):
        assert _classify_quality("Portland", "ME", "USA") == "city_state_country"

    def test_city_country(self):
        assert _classify_quality("London", None, "GBR") == "city_country"

    def test_state_country(self):
        assert _classify_quality(None, "ME", "USA") == "state_country"

    def test_country_only(self):
        assert _classify_quality(None, None, "USA") == "country_only"

    def test_city_only(self):
        # city without country → freeform_partial
        assert _classify_quality("Portland", None, None) == "freeform_partial"

    def test_state_only(self):
        assert _classify_quality(None, "ME", None) == "freeform_partial"

    def test_all_none(self):
        assert _classify_quality(None, None, None) == "freeform_partial"


# ---------------------------------------------------------------------------
# normalize_geo_hint
# ---------------------------------------------------------------------------

class TestNormalizeGeoHint:
    def test_full_city_state_country(self):
        hint = normalize_geo_hint("Portland ME USA")
        assert hint.quality_class == "city_state_country"
        assert hint.city == "Portland"
        assert hint.state_region == "ME"
        assert hint.country_code == "USA"
        assert hint.normalized_hint == "Portland ME USA"

    def test_empty_string(self):
        hint = normalize_geo_hint("")
        assert hint.quality_class == "empty_or_null"
        assert hint.normalized_hint is None

    def test_whitespace_only(self):
        hint = normalize_geo_hint("   ")
        assert hint.quality_class == "empty_or_null"

    def test_junk_returns_non_address_junk(self):
        hint = normalize_geo_hint("Laser")
        assert hint.quality_class == "non_address_junk"
        assert hint.city is None
        assert hint.country_code is None

    def test_yacht_club_junk(self):
        hint = normalize_geo_hint("Boothbay Harbor Yacht Club")
        assert hint.quality_class == "non_address_junk"

    def test_country_only(self):
        hint = normalize_geo_hint("USA")
        assert hint.quality_class == "country_only"
        assert hint.country_code == "USA"
        assert hint.normalized_hint == "USA"

    def test_source_address_row_id_preserved(self):
        hint = normalize_geo_hint("Portland ME USA", source_address_row_id="abc-123")
        assert hint.source_address_row_id == "abc-123"

    def test_address_raw_preserved(self):
        hint = normalize_geo_hint("Portland ME USA")
        assert hint.address_raw == "Portland ME USA"

    def test_variants_normalize_to_same_hint(self):
        h1 = normalize_geo_hint("Portland, ME, USA")
        h2 = normalize_geo_hint("Portland ME USA")
        assert h1.city == h2.city
        assert h1.state_region == h2.state_region
        assert h1.country_code == h2.country_code
        assert h1.normalized_hint == h2.normalized_hint

    def test_freeform_partial_preserves_cleaned_raw(self):
        # Something like a street address that doesn't parse
        hint = normalize_geo_hint("123 Main Street")
        assert hint.quality_class == "freeform_partial"
        assert hint.normalized_hint is not None  # should have something


# ---------------------------------------------------------------------------
# _select_best_hint
# ---------------------------------------------------------------------------

def _make_hint(quality_class: str, city=None, state=None, country=None) -> GeoHint:
    return GeoHint(
        address_raw="raw",
        source_address_row_id=None,
        normalized_hint=None,
        city=city,
        state_region=state,
        country_code=country,
        quality_class=quality_class,
    )


class TestSelectBestHint:
    def test_empty_list_returns_none_no_conflict(self):
        best, is_conflict = _select_best_hint([])
        assert best is None
        assert is_conflict is False

    def test_single_city_state_country(self):
        h = _make_hint("city_state_country", city="Portland", state="ME", country="USA")
        best, is_conflict = _select_best_hint([h])
        assert best is h
        assert is_conflict is False

    def test_prefers_city_state_country_over_country_only(self):
        h1 = _make_hint("city_state_country", city="Portland", state="ME", country="USA")
        h2 = _make_hint("country_only", country="USA")
        best, is_conflict = _select_best_hint([h1, h2])
        assert best is h1
        assert is_conflict is False

    def test_conflict_two_different_cities(self):
        h1 = _make_hint("city_state_country", city="Portland", state="ME", country="USA")
        h2 = _make_hint("city_state_country", city="Boston", state="MA", country="USA")
        best, is_conflict = _select_best_hint([h1, h2])
        assert best is None
        assert is_conflict is True

    def test_no_conflict_same_city_different_raw(self):
        h1 = _make_hint("city_state_country", city="Portland", state="ME", country="USA")
        h2 = _make_hint("city_state_country", city="Portland", state="ME", country="USA")
        best, is_conflict = _select_best_hint([h1, h2])
        assert best is not None
        assert is_conflict is False

    def test_country_only_excluded_by_default(self):
        h = _make_hint("country_only", country="USA")
        best, is_conflict = _select_best_hint([h])
        assert best is None
        assert is_conflict is False

    def test_country_only_included_when_flag_set(self):
        h = _make_hint("country_only", country="USA")
        best, is_conflict = _select_best_hint([h], include_country_only=True)
        assert best is h
        assert is_conflict is False

    def test_freeform_partial_excluded_by_default(self):
        h = _make_hint("freeform_partial")
        best, is_conflict = _select_best_hint([h])
        assert best is None
        assert is_conflict is False

    def test_freeform_partial_included_when_flag_set(self):
        h = _make_hint("freeform_partial")
        best, is_conflict = _select_best_hint([h], allow_freeform_partial=True)
        assert best is h
        assert is_conflict is False

    def test_junk_never_selected(self):
        h = _make_hint("non_address_junk")
        best, is_conflict = _select_best_hint([h])
        assert best is None
        assert is_conflict is False

    def test_city_country_beats_state_country(self):
        h1 = _make_hint("city_country", city="London", country="GBR")
        h2 = _make_hint("state_country", state="ME", country="USA")
        best, is_conflict = _select_best_hint([h1, h2])
        assert best is h1
        assert is_conflict is False

    def test_conflict_at_city_country_level(self):
        h1 = _make_hint("city_country", city="London", country="GBR")
        h2 = _make_hint("city_country", city="Paris", country="FRA")
        best, is_conflict = _select_best_hint([h1, h2])
        assert best is None
        assert is_conflict is True

    def test_falls_through_to_next_quality(self):
        # No city_state_country, has city_country
        h1 = _make_hint("city_country", city="London", country="GBR")
        h2 = _make_hint("country_only", country="USA")
        best, is_conflict = _select_best_hint([h1, h2])
        assert best is h1
        assert is_conflict is False
