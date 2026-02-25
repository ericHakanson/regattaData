"""Integration tests for resolution_lineage pipeline.

Tests lineage_report and purge_check against a live DB with various
promotion coverage percentages.
"""

from __future__ import annotations

import hashlib
import sys
import uuid

import psycopg
import pytest

from regatta_etl.resolution_lineage import (
    LineageCoverageResult,
    build_lineage_report,
    run_lineage_report,
    run_purge_check,
)
from regatta_etl.resolution_promote import run_promote


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fp(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _insert_auto_promote_participant(conn: psycopg.Connection, suffix: str = "") -> str:
    tag = suffix or str(uuid.uuid4())[:8]
    name_slug = f"lin-person-{tag}"
    em = f"lin-{tag}@example.test"
    fp = _fp(name_slug, em.lower())
    row = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name,
             best_email, resolution_state)
        VALUES (%s, %s, %s, %s, 'auto_promote')
        ON CONFLICT (stable_fingerprint) DO UPDATE
          SET normalized_name = EXCLUDED.normalized_name
        RETURNING id
        """,
        (fp, name_slug, name_slug, em),
    ).fetchone()
    return str(row[0])


def _insert_review_participant(conn: psycopg.Connection, suffix: str = "") -> str:
    """Insert a participant in review state (not promoted)."""
    tag = suffix or str(uuid.uuid4())[:8]
    name_slug = f"rev-person-{tag}"
    em = f"rev-{tag}@example.test"
    fp = _fp(name_slug, em.lower())
    row = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name,
             best_email, resolution_state)
        VALUES (%s, %s, %s, %s, 'review')
        ON CONFLICT (stable_fingerprint) DO UPDATE
          SET normalized_name = EXCLUDED.normalized_name
        RETURNING id
        """,
        (fp, name_slug, name_slug, em),
    ).fetchone()
    return str(row[0])


# ---------------------------------------------------------------------------
# lineage_report — basic coverage
# ---------------------------------------------------------------------------

class TestLineageReportBasic:
    def test_empty_db_all_zeros(self, db_conn):
        conn, _ = db_conn
        results = run_lineage_report(
            conn, entity_type="participant", dry_run=True
        )
        assert len(results) == 1
        r = results[0]
        assert r.candidates_total == 0
        assert r.candidates_promoted == 0
        assert r.pct_candidate_to_canonical is None
        assert r.unresolved_critical_deps == 0

    def test_empty_db_thresholds_not_passed(self, db_conn):
        conn, _ = db_conn
        results = run_lineage_report(
            conn, entity_type="participant",
            canonical_threshold_pct=90.0, dry_run=True
        )
        assert results[0].thresholds_passed is False

    def test_after_promote_pct_correct(self, db_conn):
        conn, _ = db_conn
        # 2 auto_promote + 1 review → 2/3 promoted = 66.67%
        _insert_auto_promote_participant(conn, suffix="lin-ap1")
        _insert_auto_promote_participant(conn, suffix="lin-ap2")
        _insert_review_participant(conn, suffix="lin-rev1")
        run_promote(conn, entity_type="participant")

        results = run_lineage_report(
            conn, entity_type="participant",
            canonical_threshold_pct=60.0, dry_run=True
        )
        r = results[0]
        assert r.candidates_total == 3
        assert r.candidates_promoted == 2
        assert r.pct_candidate_to_canonical is not None
        assert abs(r.pct_candidate_to_canonical - 66.67) < 0.1
        assert r.thresholds_passed is True  # 66.67 >= 60.0

    def test_after_promote_fails_high_threshold(self, db_conn):
        conn, _ = db_conn
        _insert_auto_promote_participant(conn, suffix="hth-ap")
        _insert_review_participant(conn, suffix="hth-rev")
        run_promote(conn, entity_type="participant")

        results = run_lineage_report(
            conn, entity_type="participant",
            canonical_threshold_pct=90.0, dry_run=True
        )
        assert results[0].thresholds_passed is False

    def test_100pct_promoted_passes_threshold(self, db_conn):
        conn, _ = db_conn
        _insert_auto_promote_participant(conn, suffix="all1")
        _insert_auto_promote_participant(conn, suffix="all2")
        run_promote(conn, entity_type="participant")

        results = run_lineage_report(
            conn, entity_type="participant",
            canonical_threshold_pct=95.0, dry_run=True
        )
        r = results[0]
        assert r.pct_candidate_to_canonical == 100.0
        assert r.thresholds_passed is True


# ---------------------------------------------------------------------------
# lineage_report — snapshot persistence
# ---------------------------------------------------------------------------

class TestLineageReportSnapshot:
    def test_snapshot_not_inserted_on_dry_run(self, db_conn):
        conn, _ = db_conn
        run_lineage_report(conn, entity_type="participant", dry_run=True)
        count = conn.execute(
            "SELECT COUNT(*) FROM lineage_coverage_snapshot WHERE entity_type = 'participant'"
        ).fetchone()[0]
        assert count == 0

    def test_snapshot_inserted_on_normal_run(self, db_conn):
        conn, _ = db_conn
        run_lineage_report(conn, entity_type="participant", dry_run=False)
        count = conn.execute(
            "SELECT COUNT(*) FROM lineage_coverage_snapshot WHERE entity_type = 'participant'"
        ).fetchone()[0]
        assert count == 1

    def test_multiple_runs_insert_multiple_snapshots(self, db_conn):
        conn, _ = db_conn
        run_lineage_report(conn, entity_type="participant", dry_run=False)
        run_lineage_report(conn, entity_type="participant", dry_run=False)
        count = conn.execute(
            "SELECT COUNT(*) FROM lineage_coverage_snapshot WHERE entity_type = 'participant'"
        ).fetchone()[0]
        assert count == 2


# ---------------------------------------------------------------------------
# lineage_report — all entity types
# ---------------------------------------------------------------------------

class TestLineageReportAllTypes:
    def test_all_entity_types_returns_five_results(self, db_conn):
        conn, _ = db_conn
        results = run_lineage_report(conn, entity_type="all", dry_run=True)
        entity_types = {r.entity_type for r in results}
        assert entity_types == {"participant", "yacht", "club", "event", "registration"}

    def test_all_types_inserts_five_snapshots(self, db_conn):
        conn, _ = db_conn
        run_lineage_report(conn, entity_type="all", dry_run=False)
        count = conn.execute(
            "SELECT COUNT(*) FROM lineage_coverage_snapshot"
        ).fetchone()[0]
        assert count == 5


# ---------------------------------------------------------------------------
# purge_check
# ---------------------------------------------------------------------------

class TestPurgeCheck:
    def test_purge_check_passes_at_100pct(self, db_conn):
        conn, _ = db_conn
        _insert_auto_promote_participant(conn, suffix="pc-all1")
        _insert_auto_promote_participant(conn, suffix="pc-all2")
        run_promote(conn, entity_type="participant")

        result = run_purge_check(
            conn,
            entity_type="participant",
            canonical_threshold_pct=95.0,
            source_threshold_pct=95.0,
        )
        assert result is True

    def test_purge_check_fails_below_threshold(self, db_conn):
        conn, _ = db_conn
        _insert_auto_promote_participant(conn, suffix="pc-fail-ap")
        _insert_review_participant(conn, suffix="pc-fail-rev")
        run_promote(conn, entity_type="participant")

        with pytest.raises(SystemExit) as exc_info:
            run_purge_check(
                conn,
                entity_type="participant",
                canonical_threshold_pct=95.0,
                source_threshold_pct=95.0,
            )
        assert exc_info.value.code == 1

    def test_purge_check_inserts_snapshots(self, db_conn):
        conn, _ = db_conn
        _insert_auto_promote_participant(conn, suffix="pc-snap1")
        _insert_auto_promote_participant(conn, suffix="pc-snap2")
        run_promote(conn, entity_type="participant")

        try:
            run_purge_check(
                conn, entity_type="participant",
                canonical_threshold_pct=95.0, source_threshold_pct=95.0,
            )
        except SystemExit:
            pass

        count = conn.execute(
            "SELECT COUNT(*) FROM lineage_coverage_snapshot WHERE entity_type = 'participant'"
        ).fetchone()[0]
        assert count >= 1


# ---------------------------------------------------------------------------
# unresolved critical deps
# ---------------------------------------------------------------------------

class TestUnresolvedCriticalDeps:
    def test_promoted_registration_with_unpromoted_event_counts_as_dep(self, db_conn, tmp_path):
        """A promoted registration whose event has been demoted back to review is a dep."""
        import csv as _csv
        conn, _ = db_conn

        # Insert event candidate in auto_promote state
        ev_fp = _fp("lin-event-dep", "2024", str(uuid.uuid4()))
        ev_row = conn.execute(
            """
            INSERT INTO candidate_event
                (stable_fingerprint, event_name, normalized_event_name,
                 season_year, resolution_state)
            VALUES (%s, 'Lin Event Dep', 'lin-event-dep', 2024, 'auto_promote')
            RETURNING id
            """,
            (ev_fp,),
        ).fetchone()
        ev_id = str(ev_row[0])

        # Insert registration candidate linked to that event in auto_promote state
        reg_fp = _fp(ev_id, str(uuid.uuid4()))
        conn.execute(
            """
            INSERT INTO candidate_registration
                (stable_fingerprint, candidate_event_id, resolution_state)
            VALUES (%s, %s, 'auto_promote')
            """,
            (reg_fp, ev_id),
        )

        # Promote event first, then registration (run_promote handles ordering)
        run_promote(conn, entity_type="event")
        run_promote(conn, entity_type="registration")

        # Now demote the event candidate back to review using run_lifecycle
        p = tmp_path / "demote_event.csv"
        with p.open("w", newline="", encoding="utf-8") as fh:
            w = _csv.DictWriter(
                fh,
                fieldnames=["candidate_entity_type", "candidate_entity_id", "reason_code", "actor"],
            )
            w.writeheader()
            w.writerow({
                "candidate_entity_type": "event",
                "candidate_entity_id": ev_id,
                "reason_code": "test",
                "actor": "test",
            })

        from regatta_etl.resolution_lifecycle import run_lifecycle as _run_lifecycle
        ctrs = _run_lifecycle(conn, p, "demote")
        assert ctrs.rows_applied == 1

        # registration still promoted, but event candidate no longer is
        results = run_lineage_report(
            conn, entity_type="registration", dry_run=True
        )
        r = results[0]
        assert r.unresolved_critical_deps >= 1
        assert r.thresholds_passed is False
