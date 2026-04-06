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
    candidate_id = str(row[0])
    conn.execute(
        """
        INSERT INTO candidate_source_link
            (candidate_entity_type, candidate_entity_id, source_table_name, source_row_pk)
        VALUES ('participant', %s::uuid, 'participant', %s)
        """,
        (candidate_id, f"lineage-auto-{tag}"),
    )
    return candidate_id


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
    candidate_id = str(row[0])
    conn.execute(
        """
        INSERT INTO candidate_source_link
            (candidate_entity_type, candidate_entity_id, source_table_name, source_row_pk)
        VALUES ('participant', %s::uuid, 'participant', %s)
        """,
        (candidate_id, f"lineage-review-{tag}"),
    )
    return candidate_id


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

    def test_sourceless_candidates_fail_thresholds(self, db_conn):
        conn, _ = db_conn
        fp = _fp("lin-sourceless", "sourceless@example.test")
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
            VALUES (%s, 'Sourceless Person', 'sourceless person', %s, 'review')
            """,
            (fp, "sourceless@example.test"),
        )

        results = run_lineage_report(
            conn, entity_type="participant", canonical_threshold_pct=0.0, dry_run=True
        )
        r = results[0]
        assert r.candidates_without_source_links == 1
        assert r.thresholds_passed is False
        assert any("zero source links" in note for note in r.notes)

    def test_participant_lineage_reports_email_like_name_count(self, db_conn):
        conn, _ = db_conn
        fp = _fp("lin-email-name", "lin-email-name@example.test")
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
            VALUES (%s, 'lin-email-name@example.test', 'lin-email-name@example.test', %s, 'review')
            """,
            (fp, "lin-email-name@example.test"),
        )

        results = run_lineage_report(
            conn, entity_type="participant", canonical_threshold_pct=0.0, dry_run=True
        )
        assert any(
            note == "participant candidates with email-like names: 1"
            for note in results[0].notes
        )

    def test_participant_lineage_reports_identity_gap_counts(self, db_conn):
        conn, _ = db_conn
        nameless_fp = _fp("lin-nameless", "lin-nameless@example.test")
        promoted_fp = _fp("lin-nameless-promoted", "lin-nameless-promoted@example.test")
        canonical_id = conn.execute(
            """
            INSERT INTO canonical_participant (display_name, canonical_confidence_score)
            VALUES ('Nameless Canonical', 0.9)
            RETURNING id
            """
        ).fetchone()[0]

        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
            VALUES (%s, NULL, NULL, %s, 'hold')
            """,
            (nameless_fp, "lin-nameless@example.test"),
        )
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email,
                 resolution_state, is_promoted, promoted_canonical_id)
            VALUES (%s, NULL, NULL, %s, 'auto_promote', true, %s::uuid)
            """,
            (promoted_fp, "lin-nameless-promoted@example.test", canonical_id),
        )

        results = run_lineage_report(
            conn, entity_type="participant", canonical_threshold_pct=0.0, dry_run=True
        )
        assert any(
            note == "participant candidates with no identity anchor: 2"
            for note in results[0].notes
        )
        assert any(
            note == "promoted participant candidates with no identity anchor: 1"
            for note in results[0].notes
        )

    def test_participant_lineage_reports_org_pattern_counts(self, db_conn):
        conn, _ = db_conn
        promoted_canonical_id = conn.execute(
            """
            INSERT INTO canonical_participant (display_name, canonical_confidence_score)
            VALUES ('Org Canonical', 0.9)
            RETURNING id
            """
        ).fetchone()[0]

        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
            VALUES (%s, 'Nantucket Yacht Club', 'nantucket yacht club', %s, 'hold')
            """,
            (_fp("lin-org-hold", "lin-org-hold@example.test"), "lin-org-hold@example.test"),
        )
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email,
                 resolution_state, is_promoted, promoted_canonical_id)
            VALUES (%s, 'Tenacious Holdings LLC', 'tenacious holdings llc', %s,
                    'auto_promote', true, %s::uuid)
            """,
            (
                _fp("lin-org-promoted", "lin-org-promoted@example.test"),
                "lin-org-promoted@example.test",
                promoted_canonical_id,
            ),
        )

        results = run_lineage_report(
            conn, entity_type="participant", canonical_threshold_pct=0.0, dry_run=True
        )
        assert any(
            note == "participant org-pattern candidates: 2"
            for note in results[0].notes
        )
        assert any(
            note == "promoted participant org-pattern candidates: 1"
            for note in results[0].notes
        )

    def test_participant_lineage_reports_invalid_phone_counts(self, db_conn):
        conn, _ = db_conn
        candidate_id = conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, best_phone, resolution_state)
            VALUES (%s, 'Phone Person', 'phone person', %s, '+8295056', 'review')
            RETURNING id
            """,
            (_fp("lin-invalid-phone", "lin-invalid-phone@example.test"), "lin-invalid-phone@example.test"),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO candidate_participant_contact
                (candidate_participant_id, contact_type, raw_value, normalized_value,
                 is_primary, source_table_name, source_row_pk)
            VALUES (%s, 'phone', '+8295056', '+8295056', true, 'test', 'row-1')
            """,
            (candidate_id,),
        )

        results = run_lineage_report(
            conn, entity_type="participant", canonical_threshold_pct=0.0, dry_run=True
        )
        assert any(
            note == "participant invalid phone contact rows: 1"
            for note in results[0].notes
        )
        assert any(
            note == "participant invalid best_phone values: 1"
            for note in results[0].notes
        )

    def test_participant_lineage_warns_when_mobile_phone_coverage_is_zero(self, db_conn):
        conn, _ = db_conn
        candidate_id = conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
            VALUES (%s, 'No Mobile Person', 'no mobile person', %s, 'review')
            RETURNING id
            """,
            (_fp("lin-no-mobile", "lin-no-mobile@example.test"), "lin-no-mobile@example.test"),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO candidate_participant_contact
                (candidate_participant_id, contact_type, contact_subtype, raw_value, normalized_value,
                 is_primary, source_table_name, source_row_pk)
            VALUES (%s, 'phone', 'home', '(207) 555-3333', '+12075553333', true, 'test', 'row-home')
            """,
            (candidate_id,),
        )

        results = run_lineage_report(
            conn, entity_type="participant", canonical_threshold_pct=0.0, dry_run=True
        )
        assert any(
            note == "participant mobile phone contact rows: 0"
            for note in results[0].notes
        )
        assert any(
            note == "warning: participant mobile phone coverage is 0% despite phone contact rows"
            for note in results[0].notes
        )


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

    def test_snapshot_persists_sourceless_count(self, db_conn):
        conn, _ = db_conn
        fp = _fp("lin-snap-sourceless", "snap-sourceless@example.test")
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
            VALUES (%s, 'Snapshot Sourceless', 'snapshot sourceless', %s, 'review')
            """,
            (fp, "snap-sourceless@example.test"),
        )

        run_lineage_report(conn, entity_type="participant", dry_run=False)
        count = conn.execute(
            """
            SELECT candidates_without_source_links
            FROM lineage_coverage_snapshot
            WHERE entity_type = 'participant'
            ORDER BY snapshot_at DESC
            LIMIT 1
            """
        ).fetchone()[0]
        assert count == 1


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

    def test_purge_check_fails_when_candidate_has_no_source_links(self, db_conn):
        conn, _ = db_conn
        fp = _fp("pc-nosrc", "pc-nosrc@example.test")
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
            VALUES (%s, 'No Source', 'no source', %s, 'review')
            """,
            (fp, "pc-nosrc@example.test"),
        )

        with pytest.raises(SystemExit) as exc_info:
            run_purge_check(
                conn,
                entity_type="participant",
                canonical_threshold_pct=0.0,
                source_threshold_pct=0.0,
            )
        assert exc_info.value.code == 1


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
