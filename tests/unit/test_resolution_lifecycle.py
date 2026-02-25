"""Unit tests for resolution_lifecycle — counters, CSV validation, and report builder."""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from regatta_etl.resolution_lifecycle import (
    LifecycleCounters,
    _REQUIRED_COLS_BY_OP,
    _VALID_ENTITY_TYPES,
    build_lifecycle_report,
    run_lifecycle,
)


# ---------------------------------------------------------------------------
# LifecycleCounters
# ---------------------------------------------------------------------------

class TestLifecycleCounters:
    def test_defaults(self):
        ctrs = LifecycleCounters()
        assert ctrs.rows_read == 0
        assert ctrs.rows_applied == 0
        assert ctrs.rows_invalid == 0
        assert ctrs.rows_skipped == 0
        assert ctrs.db_errors == 0
        assert ctrs.warnings == []

    def test_to_dict_keys(self):
        ctrs = LifecycleCounters(
            rows_read=5,
            rows_applied=3,
            rows_invalid=1,
            rows_skipped=0,
            db_errors=1,
            warnings=["w1", "w2"],
        )
        d = ctrs.to_dict()
        assert d["rows_read"] == 5
        assert d["rows_applied"] == 3
        assert d["rows_invalid"] == 1
        assert d["rows_skipped"] == 0
        assert d["db_errors"] == 1
        assert d["warnings"] == ["w1", "w2"]

    def test_warnings_truncated_at_50(self):
        ctrs = LifecycleCounters(warnings=[f"w{i}" for i in range(60)])
        d = ctrs.to_dict()
        assert len(d["warnings"]) == 50

    def test_warnings_under_50_not_truncated(self):
        ctrs = LifecycleCounters(warnings=["x"] * 30)
        d = ctrs.to_dict()
        assert len(d["warnings"]) == 30


# ---------------------------------------------------------------------------
# _REQUIRED_COLS_BY_OP
# ---------------------------------------------------------------------------

class TestRequiredColsByOp:
    def test_merge_cols(self):
        assert "canonical_entity_type" in _REQUIRED_COLS_BY_OP["merge"]
        assert "keep_canonical_id" in _REQUIRED_COLS_BY_OP["merge"]
        assert "merge_canonical_id" in _REQUIRED_COLS_BY_OP["merge"]
        assert "actor" in _REQUIRED_COLS_BY_OP["merge"]

    def test_demote_cols(self):
        assert "candidate_entity_type" in _REQUIRED_COLS_BY_OP["demote"]
        assert "candidate_entity_id" in _REQUIRED_COLS_BY_OP["demote"]
        assert "actor" in _REQUIRED_COLS_BY_OP["demote"]

    def test_unlink_cols(self):
        assert _REQUIRED_COLS_BY_OP["unlink"] == _REQUIRED_COLS_BY_OP["demote"]

    def test_split_cols(self):
        assert "canonical_entity_type" in _REQUIRED_COLS_BY_OP["split"]
        assert "old_canonical_id" in _REQUIRED_COLS_BY_OP["split"]
        assert "candidate_entity_id" in _REQUIRED_COLS_BY_OP["split"]
        assert "actor" in _REQUIRED_COLS_BY_OP["split"]


# ---------------------------------------------------------------------------
# CSV header validation via run_lifecycle
# ---------------------------------------------------------------------------

def _write_csv(tmp_path: Path, rows: list[dict], name: str = "test.csv") -> Path:
    p = tmp_path / name
    if rows:
        fieldnames = list(rows[0].keys())
        with p.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        p.write_text("")
    return p


class TestMergeCSVValidation:
    def test_missing_required_column_raises(self, tmp_path):
        p = tmp_path / "m.csv"
        p.write_text("canonical_entity_type,keep_canonical_id,actor\n")
        with pytest.raises(ValueError, match="missing required columns"):
            run_lifecycle(None, p, "merge")  # type: ignore[arg-type]

    def test_empty_file_raises(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("")
        with pytest.raises(ValueError, match="empty or has no header"):
            run_lifecycle(None, p, "merge")  # type: ignore[arg-type]


class TestDemoteCSVValidation:
    def test_missing_required_column_raises(self, tmp_path):
        p = tmp_path / "d.csv"
        p.write_text("candidate_entity_type,actor\n")
        with pytest.raises(ValueError, match="missing required columns"):
            run_lifecycle(None, p, "demote")  # type: ignore[arg-type]


class TestUnlinkCSVValidation:
    def test_missing_required_column_raises(self, tmp_path):
        p = tmp_path / "u.csv"
        p.write_text("candidate_entity_id,reason_code,actor\n")
        with pytest.raises(ValueError, match="missing required columns"):
            run_lifecycle(None, p, "unlink")  # type: ignore[arg-type]


class TestSplitCSVValidation:
    def test_missing_required_column_raises(self, tmp_path):
        p = tmp_path / "s.csv"
        p.write_text("canonical_entity_type,old_canonical_id,actor\n")
        with pytest.raises(ValueError, match="missing required columns"):
            run_lifecycle(None, p, "split")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# build_lifecycle_report
# ---------------------------------------------------------------------------

class TestBuildLifecycleReport:
    def test_basic_content(self):
        ctrs = LifecycleCounters(rows_read=10, rows_applied=8, rows_invalid=2)
        report = build_lifecycle_report(ctrs)
        assert "rows read" in report
        assert "10" in report
        assert "8" in report
        assert "2" in report

    def test_dry_run_label(self):
        ctrs = LifecycleCounters()
        report = build_lifecycle_report(ctrs, dry_run=True)
        assert "dry_run: True" in report

    def test_warnings_shown(self):
        ctrs = LifecycleCounters(warnings=["bad row 1", "bad row 2"])
        report = build_lifecycle_report(ctrs)
        assert "bad row 1" in report
        assert "bad row 2" in report

    def test_excess_warnings_truncated_in_report(self):
        ctrs = LifecycleCounters(warnings=[f"w{i}" for i in range(25)])
        report = build_lifecycle_report(ctrs)
        assert "and 5 more" in report

    def test_no_warnings_section_when_empty(self):
        ctrs = LifecycleCounters()
        report = build_lifecycle_report(ctrs)
        assert "Warnings" not in report
