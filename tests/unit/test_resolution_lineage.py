"""Unit tests for resolution_lineage — result dataclass and report builder."""

from __future__ import annotations

import pytest

from regatta_etl.resolution_lineage import (
    LineageCoverageResult,
    build_lineage_report,
)


# ---------------------------------------------------------------------------
# LineageCoverageResult
# ---------------------------------------------------------------------------

class TestLineageCoverageResult:
    def _make(self, **kwargs) -> LineageCoverageResult:
        defaults = {
            "entity_type": "participant",
            "candidates_total": 100,
            "candidates_promoted": 80,
            "pct_candidate_to_canonical": 80.0,
            "source_rows_in_link_table": None,
            "source_rows_with_candidate": None,
            "pct_source_to_candidate": None,
            "unresolved_critical_deps": 0,
            "thresholds_passed": False,
            "threshold_canonical_pct": 90.0,
            "threshold_source_pct": 90.0,
        }
        defaults.update(kwargs)
        return LineageCoverageResult(**defaults)

    def test_defaults_thresholds_passed_false(self):
        r = self._make(pct_candidate_to_canonical=85.0, thresholds_passed=False)
        assert r.thresholds_passed is False

    def test_thresholds_passed_true_when_set(self):
        r = self._make(pct_candidate_to_canonical=95.0, thresholds_passed=True)
        assert r.thresholds_passed is True

    def test_null_pct_stored_as_none(self):
        r = self._make(pct_candidate_to_canonical=None)
        assert r.pct_candidate_to_canonical is None

    def test_notes_default_empty(self):
        r = self._make()
        assert r.notes == []

    def test_notes_stored(self):
        r = self._make(notes=["no candidates found"])
        assert r.notes == ["no candidates found"]

    def test_entity_types_accepted(self):
        for et in ["participant", "yacht", "club", "event", "registration"]:
            r = self._make(entity_type=et)
            assert r.entity_type == et


# ---------------------------------------------------------------------------
# build_lineage_report
# ---------------------------------------------------------------------------

class TestBuildLineageReport:
    def _pass_result(self, entity_type: str = "participant") -> LineageCoverageResult:
        return LineageCoverageResult(
            entity_type=entity_type,
            candidates_total=100,
            candidates_promoted=100,
            pct_candidate_to_canonical=100.0,
            source_rows_in_link_table=None,
            source_rows_with_candidate=None,
            pct_source_to_candidate=None,
            unresolved_critical_deps=0,
            thresholds_passed=True,
            threshold_canonical_pct=90.0,
            threshold_source_pct=90.0,
        )

    def _fail_result(self, entity_type: str = "participant") -> LineageCoverageResult:
        return LineageCoverageResult(
            entity_type=entity_type,
            candidates_total=100,
            candidates_promoted=50,
            pct_candidate_to_canonical=50.0,
            source_rows_in_link_table=None,
            source_rows_with_candidate=None,
            pct_source_to_candidate=None,
            unresolved_critical_deps=0,
            thresholds_passed=False,
            threshold_canonical_pct=90.0,
            threshold_source_pct=90.0,
        )

    def test_pass_overall(self):
        results = [self._pass_result("participant"), self._pass_result("yacht")]
        report = build_lineage_report(results)
        assert "Overall: PASS" in report

    def test_fail_overall_when_any_fail(self):
        results = [self._pass_result("participant"), self._fail_result("yacht")]
        report = build_lineage_report(results)
        assert "Overall: FAIL" in report

    def test_entity_type_shown(self):
        results = [self._pass_result("club")]
        report = build_lineage_report(results)
        assert "club" in report

    def test_pct_shown(self):
        results = [self._pass_result()]
        report = build_lineage_report(results)
        assert "100.00%" in report

    def test_dry_run_label(self):
        results = [self._pass_result()]
        report = build_lineage_report(results, dry_run=True)
        assert "dry_run: True" in report

    def test_null_source_coverage_shown_as_not_measurable(self):
        results = [self._fail_result()]
        report = build_lineage_report(results)
        assert "not measurable" in report

    def test_notes_shown_in_report(self):
        r = self._pass_result()
        r.notes = ["some important note"]
        report = build_lineage_report([r])
        assert "some important note" in report
