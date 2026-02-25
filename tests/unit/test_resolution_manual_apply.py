"""Unit tests for resolution_manual_apply — counter dataclass and CSV validation."""

from __future__ import annotations

import csv
import io

import pytest

from regatta_etl.resolution_manual_apply import ManualApplyCounters


# ---------------------------------------------------------------------------
# ManualApplyCounters
# ---------------------------------------------------------------------------

class TestManualApplyCounters:
    def test_defaults(self):
        ctrs = ManualApplyCounters()
        assert ctrs.rows_read == 0
        assert ctrs.rows_applied == 0
        assert ctrs.rows_skipped_already_promoted == 0
        assert ctrs.rows_skipped_missing_dep == 0
        assert ctrs.rows_invalid == 0
        assert ctrs.db_errors == 0
        assert ctrs.warnings == []

    def test_to_dict_keys(self):
        ctrs = ManualApplyCounters(
            rows_read=10,
            rows_applied=7,
            rows_skipped_already_promoted=1,
            rows_skipped_missing_dep=1,
            rows_invalid=1,
            db_errors=0,
        )
        d = ctrs.to_dict()
        assert d["rows_read"] == 10
        assert d["rows_applied"] == 7
        assert d["rows_skipped_already_promoted"] == 1
        assert d["rows_skipped_missing_dep"] == 1
        assert d["rows_invalid"] == 1
        assert d["db_errors"] == 0
        assert "warnings" in d

    def test_warnings_truncated_to_50(self):
        ctrs = ManualApplyCounters(warnings=[f"w{i}" for i in range(100)])
        d = ctrs.to_dict()
        assert len(d["warnings"]) == 50


# ---------------------------------------------------------------------------
# CSV format validation (via run_manual_apply with a tmp file)
# ---------------------------------------------------------------------------

class TestCSVValidation:
    """Test that run_manual_apply raises ValueError for bad CSV headers."""

    def _write_csv(self, tmp_path, rows: list[dict], fieldnames: list[str]) -> str:
        p = tmp_path / "decisions.csv"
        with p.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            for row in rows:
                w.writerow(row)
        return str(p)

    def test_missing_required_column_raises(self, tmp_path):
        # Missing 'actor' column
        path = self._write_csv(
            tmp_path,
            [{"candidate_entity_type": "participant", "candidate_entity_id": "x", "action": "promote"}],
            fieldnames=["candidate_entity_type", "candidate_entity_id", "action"],
        )
        from regatta_etl.resolution_manual_apply import run_manual_apply

        class FakeConn:
            """Minimal psycopg-like conn for header-only test."""
            def execute(self, *a, **kw): ...
            def __enter__(self): return self
            def __exit__(self, *a): ...

        with pytest.raises(ValueError, match="missing required columns"):
            run_manual_apply(FakeConn(), decisions_path=path)  # type: ignore[arg-type]

    def test_empty_csv_raises(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("")
        from regatta_etl.resolution_manual_apply import run_manual_apply

        class FakeConn:
            def execute(self, *a, **kw): ...

        with pytest.raises(ValueError, match="empty or has no header"):
            run_manual_apply(FakeConn(), decisions_path=str(p))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

class TestBuildManualApplyReport:
    def test_basic_report(self):
        from regatta_etl.resolution_manual_apply import build_manual_apply_report
        ctrs = ManualApplyCounters(rows_read=5, rows_applied=3, rows_invalid=2)
        report = build_manual_apply_report(ctrs, dry_run=False)
        assert "rows read" in report
        assert "5" in report
        assert "rows applied" in report
        assert "3" in report
        assert "Note: run --mode resolution_score" in report

    def test_dry_run_report_no_rescore_note(self):
        from regatta_etl.resolution_manual_apply import build_manual_apply_report
        ctrs = ManualApplyCounters(rows_read=1, rows_applied=1)
        report = build_manual_apply_report(ctrs, dry_run=True)
        assert "dry_run: True" in report
        # Dry run: no re-score note
        assert "Note: run --mode resolution_score" not in report

    def test_warnings_appear_in_report(self):
        from regatta_etl.resolution_manual_apply import build_manual_apply_report
        ctrs = ManualApplyCounters(warnings=["warn1", "warn2"])
        report = build_manual_apply_report(ctrs, dry_run=False)
        assert "warn1" in report
        assert "warn2" in report

    def test_excess_warnings_truncated(self):
        from regatta_etl.resolution_manual_apply import build_manual_apply_report
        ctrs = ManualApplyCounters(warnings=[f"w{i}" for i in range(30)])
        report = build_manual_apply_report(ctrs, dry_run=False)
        assert "... and 10 more" in report
