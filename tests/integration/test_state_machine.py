"""Integration tests for resolution state machine DB constraints.

Tests:
  - CHECK constraint ck_promoted_has_canonical (via INSERT/UPDATE)
  - BEFORE UPDATE trigger enforce_candidate_state_transition (Rule 1 + Rule 2)
  - Lifecycle ops correctly bypass Rule 1 via single UPDATE with is_promoted=false
  - Manual apply promote sets correct state
  - Idempotency regression tests
"""

from __future__ import annotations

import hashlib
import uuid

import psycopg
import pytest

from regatta_etl.resolution_lifecycle import run_lifecycle
from regatta_etl.resolution_promote import run_promote
from regatta_etl.resolution_manual_apply import run_manual_apply

import csv
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fp(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _insert_auto_promote_participant(conn: psycopg.Connection, suffix: str = "") -> str:
    tag = suffix or str(uuid.uuid4())[:8]
    name_slug = f"sm-person-{tag}"
    em = f"sm-{tag}@example.test"
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
    tag = suffix or str(uuid.uuid4())[:8]
    name_slug = f"sm-rev-{tag}"
    em = f"sm-rev-{tag}@example.test"
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


def _write_lifecycle_csv(
    tmp_path: Path, op: str, rows: list[dict], name: str = "lc.csv"
) -> Path:
    p = tmp_path / name
    if op == "merge":
        fieldnames = [
            "canonical_entity_type", "keep_canonical_id",
            "merge_canonical_id", "reason_code", "actor",
        ]
    elif op in ("demote", "unlink"):
        fieldnames = [
            "candidate_entity_type", "candidate_entity_id",
            "reason_code", "actor",
        ]
    else:
        fieldnames = [
            "canonical_entity_type", "old_canonical_id",
            "candidate_entity_id", "reason_code", "actor",
        ]
    with p.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return p


def _write_decisions_csv(tmp_path: Path, rows: list[dict], name: str = "dec.csv") -> Path:
    p = tmp_path / name
    fieldnames = [
        "candidate_entity_type", "candidate_entity_id", "action", "reason_code", "actor"
    ]
    with p.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return p


def _get_promoted_canonical_id(conn, cid, entity_type="participant"):
    row = conn.execute(
        """
        SELECT canonical_entity_id FROM candidate_canonical_link
        WHERE candidate_entity_type = %s AND candidate_entity_id = %s
        """,
        (entity_type, cid),
    ).fetchone()
    return str(row[0]) if row else None


# ---------------------------------------------------------------------------
# CHECK constraint: ck_promoted_has_canonical
# ---------------------------------------------------------------------------

class TestCheckConstraint:
    def test_insert_with_is_promoted_false_valid(self, db_conn):
        conn, _ = db_conn
        tag = str(uuid.uuid4())[:8]
        fp = _fp(f"ck-{tag}", "")
        # is_promoted defaults to false — no constraint violation
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, normalized_name, resolution_state)
            VALUES (%s, %s, 'auto_promote')
            """,
            (fp, f"ck-{tag}"),
        )

    def test_direct_update_to_promoted_without_canonical_raises(self, db_conn):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="ck-nocanon")
        with pytest.raises(Exception):
            # Setting is_promoted=true without promoted_canonical_id violates CHECK
            conn.execute(
                "UPDATE candidate_participant SET is_promoted = true WHERE id = %s",
                (cid,),
            )


# ---------------------------------------------------------------------------
# Trigger Rule 1: promoted candidate cannot change resolution_state while promoted
# ---------------------------------------------------------------------------

class TestTriggerRule1:
    def test_trigger_fires_on_promoted_state_change(self, db_conn):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="trig-r1")
        run_promote(conn, entity_type="participant")

        # Verify promoted
        state = conn.execute(
            "SELECT is_promoted FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert state is True

        with pytest.raises(Exception, match="auto_promote"):
            # Rule 1: cannot change state while is_promoted=true
            conn.execute(
                """
                UPDATE candidate_participant
                SET resolution_state = 'review'
                WHERE id = %s
                """,
                (cid,),
            )

    def test_trigger_allows_updating_non_state_columns_on_promoted(self, db_conn):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="trig-safe")
        run_promote(conn, entity_type="participant")

        # Updating a non-state column on a promoted candidate is fine
        conn.execute(
            "UPDATE candidate_participant SET display_name = 'Updated' WHERE id = %s",
            (cid,),
        )
        name = conn.execute(
            "SELECT display_name FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert name == "Updated"


# ---------------------------------------------------------------------------
# Trigger Rule 2: reject → auto_promote blocked
# ---------------------------------------------------------------------------

class TestTriggerRule2:
    def test_reject_to_auto_promote_blocked(self, db_conn):
        conn, _ = db_conn
        tag = str(uuid.uuid4())[:8]
        fp = _fp(f"r2-{tag}", "")
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, normalized_name, resolution_state)
            VALUES (%s, %s, 'reject')
            """,
            (fp, f"r2-{tag}"),
        )
        cid = conn.execute(
            "SELECT id FROM candidate_participant WHERE stable_fingerprint = %s", (fp,)
        ).fetchone()[0]

        with pytest.raises(Exception, match="reject"):
            conn.execute(
                """
                UPDATE candidate_participant
                SET resolution_state = 'auto_promote'
                WHERE id = %s
                """,
                (cid,),
            )

    def test_reject_to_review_allowed(self, db_conn):
        conn, _ = db_conn
        tag = str(uuid.uuid4())[:8]
        fp = _fp(f"r2-rev-{tag}", "")
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, normalized_name, resolution_state)
            VALUES (%s, %s, 'reject')
            """,
            (fp, f"r2-rev-{tag}"),
        )
        cid = conn.execute(
            "SELECT id FROM candidate_participant WHERE stable_fingerprint = %s", (fp,)
        ).fetchone()[0]

        # review is fine — Rule 2 only blocks reject → auto_promote
        conn.execute(
            "UPDATE candidate_participant SET resolution_state = 'review' WHERE id = %s",
            (cid,),
        )
        state = conn.execute(
            "SELECT resolution_state FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert state == "review"


# ---------------------------------------------------------------------------
# Lifecycle ops correctly bypass trigger
# ---------------------------------------------------------------------------

class TestLifecycleBypassesTrigger:
    def test_demote_bypasses_rule1(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="bypass-dem")
        run_promote(conn, entity_type="participant")

        csv_path = _write_lifecycle_csv(tmp_path, "demote", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "test",
            "actor": "test",
        }])
        # Should not raise — demote sets is_promoted=false + review in one UPDATE
        ctrs = run_lifecycle(conn, csv_path, "demote")
        assert ctrs.rows_applied == 1
        assert ctrs.db_errors == 0

    def test_unlink_bypasses_rule1(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="bypass-ul")
        run_promote(conn, entity_type="participant")

        csv_path = _write_lifecycle_csv(tmp_path, "unlink", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "unlink")
        assert ctrs.rows_applied == 1
        assert ctrs.db_errors == 0


# ---------------------------------------------------------------------------
# Manual apply: promote sets correct state
# ---------------------------------------------------------------------------

class TestManualApplyStateMachine:
    def test_manual_promote_sets_auto_promote_state(self, db_conn, tmp_path):
        conn, _ = db_conn
        # review candidate (email+phone gives score 0.85 → review state after scoring)
        tag = str(uuid.uuid4())[:8]
        fp = _fp(f"ma-{tag}", f"ma-{tag}@example.test")
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, normalized_name, best_email, resolution_state)
            VALUES (%s, %s, %s, 'review')
            """,
            (fp, f"ma-{tag}", f"ma-{tag}@example.test"),
        )
        cid = str(conn.execute(
            "SELECT id FROM candidate_participant WHERE stable_fingerprint = %s", (fp,)
        ).fetchone()[0])

        csv_path = _write_decisions_csv(tmp_path, [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "action": "promote",
            "reason_code": "manual",
            "actor": "tester",
        }])
        ctrs = run_manual_apply(conn, csv_path)
        assert ctrs.rows_applied == 1

        state = conn.execute(
            "SELECT is_promoted, resolution_state FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        assert state[0] is True  # is_promoted
        assert state[1] == "auto_promote"


# ---------------------------------------------------------------------------
# Idempotency regression tests
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_demote_twice_second_run_rows_invalid(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="idem-dem")
        run_promote(conn, entity_type="participant")

        csv_path = _write_lifecycle_csv(tmp_path, "demote", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs1 = run_lifecycle(conn, csv_path, "demote")
        assert ctrs1.rows_applied == 1

        # Second run: candidate is no longer promoted → invalid
        ctrs2 = run_lifecycle(conn, csv_path, "demote")
        assert ctrs2.rows_invalid == 1
        assert ctrs2.rows_applied == 0

    def test_lineage_report_twice_same_coverage(self, db_conn):
        from regatta_etl.resolution_lineage import run_lineage_report

        conn, _ = db_conn
        _insert_auto_promote_participant(conn, suffix="idem-lin1")
        _insert_auto_promote_participant(conn, suffix="idem-lin2")
        run_promote(conn, entity_type="participant")

        r1 = run_lineage_report(conn, entity_type="participant", dry_run=True)
        r2 = run_lineage_report(conn, entity_type="participant", dry_run=True)

        assert r1[0].candidates_total == r2[0].candidates_total
        assert r1[0].candidates_promoted == r2[0].candidates_promoted
        assert r1[0].pct_candidate_to_canonical == r2[0].pct_candidate_to_canonical
