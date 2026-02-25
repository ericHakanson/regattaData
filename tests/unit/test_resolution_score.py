"""Unit tests for resolution_score.py — feature extractors and ScoreCounters."""

from __future__ import annotations

import pytest

from regatta_etl.resolution_score import (
    ScoreCounters,
    _features_club,
    _features_event,
    _features_participant,
    _features_registration,
    _features_yacht,
)


# ---------------------------------------------------------------------------
# Feature extractor helpers
# ---------------------------------------------------------------------------

def _part(
    normalized_name=None,
    best_email=None,
    best_phone=None,
    date_of_birth=None,
    **_,
) -> dict:
    return {
        "id": "x",
        "normalized_name": normalized_name,
        "best_email": best_email,
        "best_phone": best_phone,
        "date_of_birth": date_of_birth,
        "quality_score": 0,
    }


def _yacht(
    normalized_name=None,
    normalized_sail_number=None,
    yacht_type=None,
    length_feet=None,
    **_,
) -> dict:
    return {
        "id": "x",
        "normalized_name": normalized_name,
        "normalized_sail_number": normalized_sail_number,
        "yacht_type": yacht_type,
        "length_feet": length_feet,
        "quality_score": 0,
    }


def _club(
    normalized_name=None,
    website=None,
    state_usa=None,
    phone=None,
    **_,
) -> dict:
    return {
        "id": "x",
        "normalized_name": normalized_name,
        "website": website,
        "state_usa": state_usa,
        "phone": phone,
        "quality_score": 0,
    }


def _event(
    normalized_event_name=None,
    event_external_id=None,
    season_year=None,
    start_date=None,
    end_date=None,
    **_,
) -> dict:
    return {
        "id": "x",
        "normalized_event_name": normalized_event_name,
        "event_external_id": event_external_id,
        "season_year": season_year,
        "start_date": start_date,
        "end_date": end_date,
        "quality_score": 0,
    }


def _reg(
    registration_external_id=None,
    candidate_event_id=None,
    candidate_yacht_id=None,
    candidate_primary_participant_id=None,
    **_,
) -> dict:
    return {
        "id": "x",
        "registration_external_id": registration_external_id,
        "candidate_event_id": candidate_event_id,
        "candidate_yacht_id": candidate_yacht_id,
        "candidate_primary_participant_id": candidate_primary_participant_id,
        "quality_score": 0,
    }


# ---------------------------------------------------------------------------
# Participant feature extractor
# ---------------------------------------------------------------------------

class TestParticipantFeatures:
    def test_all_present(self):
        feats = _features_participant(_part(
            normalized_name="john-doe",
            best_email="john@example.com",
            best_phone="+12075551234",
            date_of_birth="1990-01-01",
        ))
        assert feats == {
            "email_exact": True,
            "phone_exact": True,
            "dob_exact": True,
            "normalized_name_exact": True,
        }

    def test_none_present(self):
        feats = _features_participant(_part())
        assert all(not v for v in feats.values())

    def test_email_only(self):
        feats = _features_participant(_part(best_email="a@b.com"))
        assert feats["email_exact"] is True
        assert feats["phone_exact"] is False
        assert feats["dob_exact"] is False
        assert feats["normalized_name_exact"] is False

    def test_empty_string_treated_as_false(self):
        feats = _features_participant(_part(best_email="", normalized_name=""))
        assert feats["email_exact"] is False
        assert feats["normalized_name_exact"] is False


# ---------------------------------------------------------------------------
# Yacht feature extractor
# ---------------------------------------------------------------------------

class TestYachtFeatures:
    def test_all_present(self):
        feats = _features_yacht(_yacht(
            normalized_name="sea-legs",
            normalized_sail_number="usa-1234",
            yacht_type="J/24",
            length_feet=24.5,
        ))
        assert feats == {
            "sail_number_exact": True,
            "name_normalized": True,
            "yacht_type_present": True,
            "length_feet_present": True,
        }

    def test_none_present(self):
        feats = _features_yacht(_yacht())
        assert all(not v for v in feats.values())

    def test_sail_only(self):
        feats = _features_yacht(_yacht(normalized_sail_number="usa-99"))
        assert feats["sail_number_exact"] is True
        assert feats["name_normalized"] is False


# ---------------------------------------------------------------------------
# Club feature extractor
# ---------------------------------------------------------------------------

class TestClubFeatures:
    def test_all_present(self):
        feats = _features_club(_club(
            normalized_name="boothbay-harbor-yc",
            website="https://bhyc.org",
            state_usa="ME",
            phone="+12075551234",
        ))
        assert all(feats.values())

    def test_name_only(self):
        feats = _features_club(_club(normalized_name="bhyc"))
        assert feats["name_normalized"] is True
        assert feats["website_present"] is False
        assert feats["state_usa_present"] is False
        assert feats["phone_present"] is False


# ---------------------------------------------------------------------------
# Event feature extractor
# ---------------------------------------------------------------------------

class TestEventFeatures:
    def test_all_present(self):
        feats = _features_event(_event(
            normalized_event_name="bhyc-regatta",
            event_external_id="race-537",
            season_year=2024,
            start_date="2024-07-04",
        ))
        assert feats == {
            "external_id_present": True,
            "season_year_present": True,
            "name_normalized": True,
            "dates_present": True,
        }

    def test_dates_present_with_only_end_date(self):
        feats = _features_event(_event(end_date="2024-07-06"))
        assert feats["dates_present"] is True

    def test_season_year_zero_is_not_falsy(self):
        # season_year=0 is technically not None, so it should be True
        feats = _features_event(_event(season_year=0))
        # 0 is not None → season_year_present should be True (year 0 is unusual
        # but the check is "IS NOT NULL", not truthiness)
        assert feats["season_year_present"] is True

    def test_none_present(self):
        feats = _features_event(_event())
        assert all(not v for v in feats.values())


# ---------------------------------------------------------------------------
# Registration feature extractor
# ---------------------------------------------------------------------------

class TestRegistrationFeatures:
    def test_all_present(self):
        feats = _features_registration(_reg(
            registration_external_id="sku-123",
            candidate_event_id="event-uuid",
            candidate_yacht_id="yacht-uuid",
            candidate_primary_participant_id="part-uuid",
        ))
        assert all(feats.values())

    def test_external_id_only(self):
        feats = _features_registration(_reg(registration_external_id="sku-1"))
        assert feats["external_id_present"] is True
        assert feats["event_resolved"] is False
        assert feats["yacht_resolved"] is False
        assert feats["participant_resolved"] is False

    def test_none_present(self):
        feats = _features_registration(_reg())
        assert all(not v for v in feats.values())


# ---------------------------------------------------------------------------
# ScoreCounters.to_dict
# ---------------------------------------------------------------------------

class TestScoreCounters:
    def test_to_dict_keys(self):
        ctrs = ScoreCounters(
            candidates_scored=5,
            candidates_auto_promote=2,
            candidates_review=1,
            candidates_hold=1,
            candidates_rejected=1,
            nbas_written=3,
        )
        d = ctrs.to_dict()
        assert d["candidates_scored"] == 5
        assert d["candidates_auto_promote"] == 2
        assert d["nbas_written"] == 3
        assert "warnings" in d

    def test_warnings_truncated_to_50(self):
        ctrs = ScoreCounters(warnings=[f"w{i}" for i in range(100)])
        d = ctrs.to_dict()
        assert len(d["warnings"]) == 50

    def test_nbas_written_default_zero(self):
        ctrs = ScoreCounters()
        assert ctrs.nbas_written == 0
        assert ctrs.to_dict()["nbas_written"] == 0
