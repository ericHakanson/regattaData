"""Unit tests for import_mailchimp_event_activation pure functions.

Tests: suppression resolver, email-level dedupe, segment merging, CSV ordering.
No DB connection required.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from regatta_etl.import_mailchimp_event_activation import (
    _AudienceRow,
    _CandidateRow,
    _apply_suppression,
    _dedupe_by_email,
    _merge_segment_rows,
    _write_csv,
)
from regatta_etl.shared import RunCounters


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cand(
    pid: str,
    email: str,
    segments: list[str],
    confidence: float = 0.5,
    updated_at: datetime | None = None,
    upcoming: int = 0,
    historical: int = 1,
    yacht: str | None = None,
    last_event: str | None = None,
) -> _CandidateRow:
    return _CandidateRow(
        participant_id=pid,
        email_normalized=email,
        first_name="Alice",
        last_name="Smith",
        display_name="Alice Smith",
        confidence_score=confidence,
        updated_at=updated_at,
        upcoming_event_count=upcoming,
        historical_registration_count=historical,
        segment_types=list(segments),
        yacht_name=yacht,
        last_registered_event_name=last_event,
    )


def _ts(year: int, month: int = 1, day: int = 1) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _merge_segment_rows
# ---------------------------------------------------------------------------

class TestMergeSegmentRows:
    def test_distinct_participants_preserved(self):
        a = [_cand("p1", "a@x.com", ["upcoming_registrants"])]
        b = [_cand("p2", "b@x.com", ["likely_registrants"])]
        result = _merge_segment_rows(a, b)
        assert len(result) == 2

    def test_same_participant_in_both_segments_merged(self):
        p1_a = _cand("p1", "a@x.com", ["upcoming_registrants"], upcoming=2, historical=3)
        p1_b = _cand("p1", "a@x.com", ["likely_registrants"], upcoming=0, historical=5)
        result = _merge_segment_rows([p1_a], [p1_b])
        assert len(result) == 1
        r = result[0]
        assert set(r.segment_types) == {"upcoming_registrants", "likely_registrants"}
        # upcoming_event_count takes the max
        assert r.upcoming_event_count == 2
        # historical_registration_count takes the max
        assert r.historical_registration_count == 5

    def test_segment_types_not_duplicated(self):
        p1_a = _cand("p1", "a@x.com", ["upcoming_registrants"])
        p1_b = _cand("p1", "a@x.com", ["upcoming_registrants"])
        result = _merge_segment_rows([p1_a], [p1_b])
        assert result[0].segment_types.count("upcoming_registrants") == 1

    def test_empty_inputs(self):
        assert _merge_segment_rows([], []) == []

    def test_only_a_segment(self):
        a = [_cand("p1", "a@x.com", ["upcoming_registrants"])]
        result = _merge_segment_rows(a, [])
        assert len(result) == 1
        assert result[0].segment_types == ["upcoming_registrants"]


# ---------------------------------------------------------------------------
# _dedupe_by_email — tie-breaking
# ---------------------------------------------------------------------------

class TestDedupeByEmail:
    def test_single_participant_per_email_no_dedupe(self):
        rows = [_cand("p1", "a@x.com", ["upcoming_registrants"])]
        winners, deduped_out = _dedupe_by_email(rows)
        assert len(winners) == 1
        assert deduped_out == 0

    def test_higher_confidence_wins(self):
        low = _cand("p1", "a@x.com", ["upcoming_registrants"], confidence=0.3)
        high = _cand("p2", "a@x.com", ["upcoming_registrants"], confidence=0.9)
        winners, deduped_out = _dedupe_by_email([low, high])
        assert len(winners) == 1
        assert winners[0].participant_id == "p2"
        assert deduped_out == 1

    def test_most_recent_updated_at_wins_on_equal_confidence(self):
        older = _cand("p1", "a@x.com", ["upcoming_registrants"],
                      confidence=0.5, updated_at=_ts(2023))
        newer = _cand("p2", "a@x.com", ["upcoming_registrants"],
                      confidence=0.5, updated_at=_ts(2024))
        winners, _ = _dedupe_by_email([older, newer])
        assert winners[0].participant_id == "p2"

    def test_lexical_uuid_tiebreaker_on_equal_confidence_and_timestamp(self):
        # Use UUIDs where "aaa..." < "bbb..." lexically
        p_a = _cand("aaaaaaaa-0000-0000-0000-000000000001", "a@x.com",
                    ["upcoming_registrants"], confidence=0.5, updated_at=_ts(2024))
        p_b = _cand("bbbbbbbb-0000-0000-0000-000000000002", "a@x.com",
                    ["upcoming_registrants"], confidence=0.5, updated_at=_ts(2024))
        winners, _ = _dedupe_by_email([p_b, p_a])
        # Lowest UUID lexically = "aaa..." wins
        assert winners[0].participant_id == "aaaaaaaa-0000-0000-0000-000000000001"

    def test_segment_types_union_across_all_candidates(self):
        p1 = _cand("p1", "a@x.com", ["upcoming_registrants"], confidence=0.9)
        p2 = _cand("p2", "a@x.com", ["likely_registrants"], confidence=0.1)
        winners, _ = _dedupe_by_email([p1, p2])
        assert set(winners[0].segment_types) == {"upcoming_registrants", "likely_registrants"}

    def test_contributing_participant_ids_captured(self):
        p1 = _cand("p1", "a@x.com", ["upcoming_registrants"], confidence=0.9)
        p2 = _cand("p2", "a@x.com", ["likely_registrants"], confidence=0.1)
        winners, _ = _dedupe_by_email([p1, p2])
        assert set(winners[0].contributing_participant_ids) == {"p1", "p2"}

    def test_multiple_distinct_emails_all_preserved(self):
        rows = [
            _cand("p1", "a@x.com", ["upcoming_registrants"]),
            _cand("p2", "b@x.com", ["likely_registrants"]),
            _cand("p3", "c@x.com", ["upcoming_registrants"]),
        ]
        winners, deduped_out = _dedupe_by_email(rows)
        assert len(winners) == 3
        assert deduped_out == 0

    def test_null_updated_at_treated_as_oldest(self):
        no_ts = _cand("p1", "a@x.com", ["upcoming_registrants"],
                      confidence=0.5, updated_at=None)
        has_ts = _cand("p2", "a@x.com", ["upcoming_registrants"],
                       confidence=0.5, updated_at=_ts(2020))
        winners, _ = _dedupe_by_email([no_ts, has_ts])
        assert winners[0].participant_id == "p2"


# ---------------------------------------------------------------------------
# _apply_suppression
# ---------------------------------------------------------------------------

class TestApplySuppressionResolver:
    def _make_audience_rows(
        self,
        candidates: list[_CandidateRow],
        suppression_map: dict[str, str],
    ) -> tuple[list[_AudienceRow], RunCounters]:
        ctrs = RunCounters()
        rows = _apply_suppression(candidates, suppression_map, ctrs, "2026-01-01T00:00:00Z")
        return rows, ctrs

    def test_unsubscribed_email_suppressed(self):
        cand = _cand("p1", "a@x.com", ["upcoming_registrants"])
        rows, ctrs = self._make_audience_rows([cand], {"a@x.com": "unsubscribed"})
        assert rows[0].is_suppressed is True
        assert rows[0].suppression_reason == "unsubscribed"
        assert ctrs.activation_rows_suppressed_unsubscribed == 1
        assert ctrs.activation_rows_eligible == 0

    def test_cleaned_email_suppressed(self):
        cand = _cand("p1", "b@x.com", ["likely_registrants"])
        rows, ctrs = self._make_audience_rows([cand], {"b@x.com": "cleaned"})
        assert rows[0].is_suppressed is True
        assert rows[0].suppression_reason == "cleaned"
        assert ctrs.activation_rows_suppressed_cleaned == 1

    def test_no_mailchimp_history_eligible(self):
        cand = _cand("p1", "c@x.com", ["upcoming_registrants"])
        rows, ctrs = self._make_audience_rows([cand], {})
        assert rows[0].is_suppressed is False
        assert ctrs.activation_rows_eligible == 1

    def test_subscribed_status_not_suppressed(self):
        # subscribed status should NOT appear in suppression_map (only unsubscribed/cleaned do)
        cand = _cand("p1", "d@x.com", ["upcoming_registrants"])
        rows, ctrs = self._make_audience_rows([cand], {})
        assert rows[0].is_suppressed is False

    def test_mixed_eligible_and_suppressed(self):
        candidates = [
            _cand("p1", "ok@x.com", ["upcoming_registrants"]),
            _cand("p2", "bad@x.com", ["likely_registrants"]),
        ]
        rows, ctrs = self._make_audience_rows(
            candidates, {"bad@x.com": "unsubscribed"}
        )
        assert ctrs.activation_rows_eligible == 1
        assert ctrs.activation_rows_suppressed_unsubscribed == 1
        eligible = [r for r in rows if not r.is_suppressed]
        assert len(eligible) == 1
        assert eligible[0].email_normalized == "ok@x.com"

    def test_segment_types_sorted_on_audience_row(self):
        cand = _cand("p1", "a@x.com", ["likely_registrants", "upcoming_registrants"])
        rows, _ = self._make_audience_rows([cand], {})
        assert rows[0].segment_types == ["likely_registrants", "upcoming_registrants"]


# ---------------------------------------------------------------------------
# _write_csv — schema and ordering
# ---------------------------------------------------------------------------

class TestWriteCsv:
    def test_only_eligible_rows_written(self, tmp_path):
        rows = [
            _AudienceRow(
                email_normalized="a@x.com",
                participant_id="p1",
                first_name="Alice",
                last_name="Smith",
                display_name="Alice Smith",
                segment_types=["upcoming_registrants"],
                upcoming_event_count=1,
                historical_registration_count=2,
                is_suppressed=False,
                suppression_reason=None,
                yacht_name="Seabird",
                last_registered_event_name="BHYC Regatta",
                generated_at="2026-03-01T00:00:00Z",
            ),
            _AudienceRow(
                email_normalized="b@x.com",
                participant_id="p2",
                first_name="Bob",
                last_name="Jones",
                display_name="Bob Jones",
                segment_types=["likely_registrants"],
                upcoming_event_count=0,
                historical_registration_count=3,
                is_suppressed=True,
                suppression_reason="unsubscribed",
                yacht_name=None,
                last_registered_event_name=None,
                generated_at="2026-03-01T00:00:00Z",
            ),
        ]
        out = tmp_path / "activation.csv"
        n = _write_csv(rows, str(out))
        assert n == 1
        import csv as csv_mod
        with out.open() as fh:
            reader = csv_mod.DictReader(fh)
            written = list(reader)
        assert len(written) == 1
        assert written[0]["email"] == "a@x.com"
        assert written[0]["suppression_status"] == "eligible"

    def test_csv_ordered_by_email(self, tmp_path):
        emails = ["charlie@x.com", "alice@x.com", "bob@x.com"]
        rows = [
            _AudienceRow(
                email_normalized=e,
                participant_id=f"p{i}",
                first_name=None,
                last_name=None,
                display_name=None,
                segment_types=["upcoming_registrants"],
                upcoming_event_count=0,
                historical_registration_count=1,
                is_suppressed=False,
                suppression_reason=None,
                yacht_name=None,
                last_registered_event_name=None,
                generated_at="2026-03-01T00:00:00Z",
            )
            for i, e in enumerate(emails)
        ]
        out = tmp_path / "activation.csv"
        _write_csv(rows, str(out))
        import csv as csv_mod
        with out.open() as fh:
            written = [r["email"] for r in csv_mod.DictReader(fh)]
        assert written == sorted(emails)

    def test_csv_has_all_required_columns(self, tmp_path):
        row = _AudienceRow(
            email_normalized="a@x.com",
            participant_id="p1",
            first_name="Alice",
            last_name="Smith",
            display_name="Alice Smith",
            segment_types=["upcoming_registrants", "likely_registrants"],
            upcoming_event_count=2,
            historical_registration_count=5,
            is_suppressed=False,
            suppression_reason=None,
            yacht_name="Windward",
            last_registered_event_name="Fall Series",
            generated_at="2026-03-01T00:00:00Z",
        )
        out = tmp_path / "activation.csv"
        _write_csv([row], str(out))
        import csv as csv_mod
        with out.open() as fh:
            reader = csv_mod.DictReader(fh)
            assert set(reader.fieldnames or []) >= {
                "email", "first_name", "last_name", "display_name",
                "segment_types", "upcoming_event_count",
                "historical_registration_count", "suppression_status",
                "source_participant_id", "generated_at",
                "club_name", "yacht_name", "last_registered_event_name",
            }

    def test_segment_types_comma_delimited_in_csv(self, tmp_path):
        row = _AudienceRow(
            email_normalized="a@x.com",
            participant_id="p1",
            first_name=None,
            last_name=None,
            display_name=None,
            segment_types=["likely_registrants", "upcoming_registrants"],
            upcoming_event_count=1,
            historical_registration_count=2,
            is_suppressed=False,
            suppression_reason=None,
            yacht_name=None,
            last_registered_event_name=None,
            generated_at="2026-03-01T00:00:00Z",
        )
        out = tmp_path / "activation.csv"
        _write_csv([row], str(out))
        import csv as csv_mod
        with out.open() as fh:
            written = list(csv_mod.DictReader(fh))
        assert "," in written[0]["segment_types"]

    def test_empty_rows_writes_header_only(self, tmp_path):
        out = tmp_path / "activation.csv"
        n = _write_csv([], str(out))
        assert n == 0
        import csv as csv_mod
        with out.open() as fh:
            reader = csv_mod.DictReader(fh)
            rows_out = list(reader)
        assert rows_out == []
        assert reader.fieldnames  # header present
