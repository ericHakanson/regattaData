"""Integration tests for resolution_lifecycle pipeline.

Tests merge, demote, unlink, split operations end-to-end against a live DB.
Each test promotes candidate(s) to canonical first, then applies lifecycle ops.
"""

from __future__ import annotations

import csv
import hashlib
import uuid
from pathlib import Path

import psycopg
import pytest

from regatta_etl.resolution_lifecycle import (
    LifecycleCounters,
    _write_provenance,
    run_lifecycle,
)
from regatta_etl.resolution_promote import run_promote


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fp(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _insert_auto_promote_participant(
    conn: psycopg.Connection,
    *,
    suffix: str = "",
    email: str | None = None,
    phone: str | None = None,
    display_name: str | None = None,
    best_phone: str | None = None,
) -> str:
    """Insert a participant pre-set to auto_promote state."""
    tag = suffix or str(uuid.uuid4())[:8]
    name_slug = f"test-person-{tag}"
    em = email or f"test-{tag}@example.test"
    ph = phone or best_phone
    fp = _fp(name_slug, em.lower())
    row = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name,
             best_email, best_phone, resolution_state)
        VALUES (%s, %s, %s, %s, %s, 'auto_promote')
        ON CONFLICT (stable_fingerprint) DO UPDATE
          SET normalized_name = EXCLUDED.normalized_name
        RETURNING id
        """,
        (fp, display_name or name_slug, name_slug, em, ph),
    ).fetchone()
    return str(row[0])


def _insert_nba(
    conn: psycopg.Connection,
    entity_type: str,
    entity_id: str,
) -> str:
    # NBAs are written by resolution_score.py with target_entity_type = "candidate_{entity_type}"
    # (e.g. "candidate_participant").  Match that convention so dismissal tests work correctly.
    target_type = f"candidate_{entity_type}"
    row = conn.execute(
        """
        INSERT INTO next_best_action
            (action_type, target_entity_type, target_entity_id,
             priority_score, reason_code, reason_detail,
             recommended_channel, generated_at, rule_version, status)
        VALUES ('review', %s, %s, 1.0, 'test', 'test detail',
                'email', now(), '0.1', 'open')
        RETURNING id
        """,
        (target_type, entity_id),
    ).fetchone()
    return str(row[0])


def _write_lifecycle_csv(
    tmp_path: Path,
    op: str,
    rows: list[dict],
    name: str = "lifecycle.csv",
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
    else:  # split
        fieldnames = [
            "canonical_entity_type", "old_canonical_id",
            "candidate_entity_id", "reason_code", "actor",
        ]
    with p.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return p


def _get_promoted_canonical_id(
    conn: psycopg.Connection, candidate_id: str, entity_type: str
) -> str | None:
    row = conn.execute(
        """
        SELECT canonical_entity_id
        FROM candidate_canonical_link
        WHERE candidate_entity_type = %s AND candidate_entity_id = %s
        """,
        (entity_type, candidate_id),
    ).fetchone()
    return str(row[0]) if row else None


def _candidate_state(conn: psycopg.Connection, candidate_id: str) -> dict:
    row = conn.execute(
        """
        SELECT is_promoted, resolution_state, promoted_canonical_id
        FROM candidate_participant WHERE id = %s
        """,
        (candidate_id,),
    ).fetchone()
    return {
        "is_promoted": row[0],
        "resolution_state": row[1],
        "promoted_canonical_id": str(row[2]) if row[2] else None,
    }


# ---------------------------------------------------------------------------
# Merge tests
# ---------------------------------------------------------------------------

class TestMerge:
    def test_merge_relinks_candidates_to_keep(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid_a = _insert_auto_promote_participant(conn, suffix="merge-a")
        cid_b = _insert_auto_promote_participant(conn, suffix="merge-b")
        run_promote(conn, entity_type="participant")

        keep_id = _get_promoted_canonical_id(conn, cid_a, "participant")
        merge_id = _get_promoted_canonical_id(conn, cid_b, "participant")
        assert keep_id and merge_id and keep_id != merge_id

        csv_path = _write_lifecycle_csv(tmp_path, "merge", [{
            "canonical_entity_type": "participant",
            "keep_canonical_id": keep_id,
            "merge_canonical_id": merge_id,
            "reason_code": "duplicate",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "merge")

        assert ctrs.rows_applied == 1
        assert ctrs.rows_invalid == 0
        assert ctrs.db_errors == 0

        # merge_id canonical deleted
        assert conn.execute(
            "SELECT id FROM canonical_participant WHERE id = %s", (merge_id,)
        ).fetchone() is None

        # candidate B now points to keep_id
        new_link = conn.execute(
            """
            SELECT canonical_entity_id FROM candidate_canonical_link
            WHERE candidate_entity_type = 'participant' AND candidate_entity_id = %s
            """,
            (cid_b,),
        ).fetchone()
        assert new_link is not None
        assert str(new_link[0]) == keep_id

        # candidate B promoted_canonical_id updated
        state_b = _candidate_state(conn, cid_b)
        assert state_b["promoted_canonical_id"] == keep_id

        # audit log entry
        log = conn.execute(
            """
            SELECT action_type FROM resolution_manual_action_log
            WHERE canonical_entity_id = %s AND action_type = 'merge'
            """,
            (keep_id,),
        ).fetchone()
        assert log is not None

    def test_merge_fill_nulls_survivorship(self, db_conn, tmp_path):
        conn, _ = db_conn
        # A has display_name, no phone; B has phone but different display_name
        cid_a = _insert_auto_promote_participant(conn, suffix="fill-a", email="fill-a@x.test")
        cid_b = _insert_auto_promote_participant(conn, suffix="fill-b", email="fill-b@x.test",
                                                  phone="+19995551234")
        run_promote(conn, entity_type="participant")

        keep_id = _get_promoted_canonical_id(conn, cid_a, "participant")
        merge_id = _get_promoted_canonical_id(conn, cid_b, "participant")

        # Ensure keep canonical has no best_phone by clearing it
        conn.execute(
            "UPDATE canonical_participant SET best_phone = NULL WHERE id = %s", (keep_id,)
        )

        csv_path = _write_lifecycle_csv(tmp_path, "merge", [{
            "canonical_entity_type": "participant",
            "keep_canonical_id": keep_id,
            "merge_canonical_id": merge_id,
            "reason_code": "fill",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "merge")

        assert ctrs.rows_applied == 1

        # best_phone should be filled from merge canonical
        row = conn.execute(
            "SELECT best_phone FROM canonical_participant WHERE id = %s", (keep_id,)
        ).fetchone()
        assert row[0] == "+19995551234"

    def test_merge_same_id_invalid(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="same-id")
        run_promote(conn, entity_type="participant")
        keep_id = _get_promoted_canonical_id(conn, cid, "participant")

        csv_path = _write_lifecycle_csv(tmp_path, "merge", [{
            "canonical_entity_type": "participant",
            "keep_canonical_id": keep_id,
            "merge_canonical_id": keep_id,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "merge")
        assert ctrs.rows_invalid == 1
        assert ctrs.rows_applied == 0

    def test_merge_missing_keep_invalid(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="miss-keep")
        run_promote(conn, entity_type="participant")
        real_id = _get_promoted_canonical_id(conn, cid, "participant")
        fake_id = str(uuid.uuid4())

        csv_path = _write_lifecycle_csv(tmp_path, "merge", [{
            "canonical_entity_type": "participant",
            "keep_canonical_id": fake_id,
            "merge_canonical_id": real_id,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "merge")
        assert ctrs.rows_invalid == 1

    def test_merge_missing_merge_id_invalid(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="miss-merge")
        run_promote(conn, entity_type="participant")
        real_id = _get_promoted_canonical_id(conn, cid, "participant")
        fake_id = str(uuid.uuid4())

        csv_path = _write_lifecycle_csv(tmp_path, "merge", [{
            "canonical_entity_type": "participant",
            "keep_canonical_id": real_id,
            "merge_canonical_id": fake_id,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "merge")
        assert ctrs.rows_invalid == 1

    def test_merge_orphan_canonical_is_invalid(self, db_conn, tmp_path):
        """merge_canonical_id exists but has no linked candidates → rows_invalid, no DB error."""
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="orphan-keep")
        run_promote(conn, entity_type="participant")
        keep_id = _get_promoted_canonical_id(conn, cid, "participant")

        # Create a bare canonical_participant row with no candidate link (orphan).
        orphan_row = conn.execute(
            """
            INSERT INTO canonical_participant (normalized_name)
            VALUES ('orphan-canonical')
            RETURNING id
            """
        ).fetchone()
        orphan_id = str(orphan_row[0])

        csv_path = _write_lifecycle_csv(tmp_path, "merge", [{
            "canonical_entity_type": "participant",
            "keep_canonical_id": keep_id,
            "merge_canonical_id": orphan_id,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "merge")

        assert ctrs.rows_invalid == 1
        assert ctrs.db_errors == 0
        assert ctrs.rows_applied == 0
        # Orphan canonical must still exist (we returned before deleting it).
        assert conn.execute(
            "SELECT id FROM canonical_participant WHERE id = %s", (orphan_id,)
        ).fetchone() is not None

    def test_merge_dismisses_nbas_for_relinked_candidates(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid_a = _insert_auto_promote_participant(conn, suffix="nba-a")
        cid_b = _insert_auto_promote_participant(conn, suffix="nba-b")
        run_promote(conn, entity_type="participant")

        keep_id = _get_promoted_canonical_id(conn, cid_a, "participant")
        merge_id = _get_promoted_canonical_id(conn, cid_b, "participant")

        # Create open NBA for candidate B
        nba_id = _insert_nba(conn, "participant", cid_b)

        csv_path = _write_lifecycle_csv(tmp_path, "merge", [{
            "canonical_entity_type": "participant",
            "keep_canonical_id": keep_id,
            "merge_canonical_id": merge_id,
            "reason_code": "test",
            "actor": "test",
        }])
        run_lifecycle(conn, csv_path, "merge")

        nba_status = conn.execute(
            "SELECT status FROM next_best_action WHERE id = %s", (nba_id,)
        ).fetchone()[0]
        assert nba_status == "dismissed"


# ---------------------------------------------------------------------------
# Demote tests
# ---------------------------------------------------------------------------

class TestDemote:
    def test_demote_sole_candidate_deletes_canonical(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="demote-sole")
        run_promote(conn, entity_type="participant")
        canonical_id = _get_promoted_canonical_id(conn, cid, "participant")
        assert canonical_id is not None

        csv_path = _write_lifecycle_csv(tmp_path, "demote", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "error",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "demote")

        assert ctrs.rows_applied == 1
        assert ctrs.rows_invalid == 0

        # Candidate back in review
        state = _candidate_state(conn, cid)
        assert state["is_promoted"] is False
        assert state["resolution_state"] == "review"
        assert state["promoted_canonical_id"] is None

        # Canonical deleted
        assert conn.execute(
            "SELECT id FROM canonical_participant WHERE id = %s", (canonical_id,)
        ).fetchone() is None

        # Audit logged
        log = conn.execute(
            """
            SELECT action_type FROM resolution_manual_action_log
            WHERE candidate_entity_id = %s AND action_type = 'demote'
            """,
            (cid,),
        ).fetchone()
        assert log is not None

    def test_demote_one_of_two_preserves_canonical(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid_a = _insert_auto_promote_participant(conn, suffix="d2-a")
        cid_b = _insert_auto_promote_participant(conn, suffix="d2-b")
        run_promote(conn, entity_type="participant")

        keep_id = _get_promoted_canonical_id(conn, cid_a, "participant")
        merge_id = _get_promoted_canonical_id(conn, cid_b, "participant")

        # First merge so both candidates point to keep_id
        merge_csv = _write_lifecycle_csv(tmp_path, "merge", [{
            "canonical_entity_type": "participant",
            "keep_canonical_id": keep_id,
            "merge_canonical_id": merge_id,
            "reason_code": "dup",
            "actor": "test",
        }], name="merge.csv")
        run_lifecycle(conn, merge_csv, "merge")

        # Now demote candidate B
        demote_csv = _write_lifecycle_csv(tmp_path, "demote", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid_b,
            "reason_code": "error",
            "actor": "test",
        }], name="demote.csv")
        ctrs = run_lifecycle(conn, demote_csv, "demote")

        assert ctrs.rows_applied == 1

        # Canonical keep_id still exists (candidate A still linked)
        assert conn.execute(
            "SELECT id FROM canonical_participant WHERE id = %s", (keep_id,)
        ).fetchone() is not None

        # Candidate B back in review
        state_b = _candidate_state(conn, cid_b)
        assert state_b["is_promoted"] is False
        assert state_b["resolution_state"] == "review"

    def test_demote_unpromoted_candidate_invalid(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="unp-dem")
        # Do NOT promote — candidate stays in auto_promote state but is_promoted=false

        csv_path = _write_lifecycle_csv(tmp_path, "demote", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "demote")
        assert ctrs.rows_invalid == 1
        assert ctrs.rows_applied == 0

    def test_demote_dismisses_nbas(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="dem-nba")
        run_promote(conn, entity_type="participant")
        nba_id = _insert_nba(conn, "participant", cid)

        csv_path = _write_lifecycle_csv(tmp_path, "demote", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "test",
            "actor": "test",
        }])
        run_lifecycle(conn, csv_path, "demote")

        nba_status = conn.execute(
            "SELECT status FROM next_best_action WHERE id = %s", (nba_id,)
        ).fetchone()[0]
        assert nba_status == "dismissed"


# ---------------------------------------------------------------------------
# Unlink tests
# ---------------------------------------------------------------------------

class TestUnlink:
    def test_unlink_preserves_canonical_even_as_sole_candidate(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="unlink-sole")
        run_promote(conn, entity_type="participant")
        canonical_id = _get_promoted_canonical_id(conn, cid, "participant")

        csv_path = _write_lifecycle_csv(tmp_path, "unlink", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "reassign",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "unlink")

        assert ctrs.rows_applied == 1

        # Candidate back in review
        state = _candidate_state(conn, cid)
        assert state["is_promoted"] is False
        assert state["resolution_state"] == "review"

        # Canonical preserved (key difference from demote)
        assert conn.execute(
            "SELECT id FROM canonical_participant WHERE id = %s", (canonical_id,)
        ).fetchone() is not None

        # Audit logged as 'unlink'
        log = conn.execute(
            """
            SELECT action_type FROM resolution_manual_action_log
            WHERE candidate_entity_id = %s AND action_type = 'unlink'
            """,
            (cid,),
        ).fetchone()
        assert log is not None

    def test_unlink_does_not_dismiss_nbas(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="unlink-nba")
        run_promote(conn, entity_type="participant")
        nba_id = _insert_nba(conn, "participant", cid)

        csv_path = _write_lifecycle_csv(tmp_path, "unlink", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "test",
            "actor": "test",
        }])
        run_lifecycle(conn, csv_path, "unlink")

        # NBA should still be open (unlink preserves NBAs)
        nba_status = conn.execute(
            "SELECT status FROM next_best_action WHERE id = %s", (nba_id,)
        ).fetchone()[0]
        assert nba_status == "open"

    def test_unlink_unpromoted_candidate_invalid(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="unlink-unp")

        csv_path = _write_lifecycle_csv(tmp_path, "unlink", [{
            "candidate_entity_type": "participant",
            "candidate_entity_id": cid,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "unlink")
        assert ctrs.rows_invalid == 1


# ---------------------------------------------------------------------------
# Split tests
# ---------------------------------------------------------------------------

class TestSplit:
    def _promote_and_merge_three(
        self, conn: psycopg.Connection, tmp_path: Path, tag: str
    ) -> tuple[str, str, str, str]:
        """Return (cid_a, cid_b, cid_c, canonical_id) where all 3 candidates link to canonical_id."""
        cid_a = _insert_auto_promote_participant(conn, suffix=f"{tag}-a")
        cid_b = _insert_auto_promote_participant(conn, suffix=f"{tag}-b")
        cid_c = _insert_auto_promote_participant(conn, suffix=f"{tag}-c")
        run_promote(conn, entity_type="participant")

        keep_id = _get_promoted_canonical_id(conn, cid_a, "participant")
        merge_b_id = _get_promoted_canonical_id(conn, cid_b, "participant")
        merge_c_id = _get_promoted_canonical_id(conn, cid_c, "participant")

        # Merge B and C into A
        merge_rows = [
            {
                "canonical_entity_type": "participant",
                "keep_canonical_id": keep_id,
                "merge_canonical_id": merge_b_id,
                "reason_code": "dup",
                "actor": "test",
            },
            {
                "canonical_entity_type": "participant",
                "keep_canonical_id": keep_id,
                "merge_canonical_id": merge_c_id,
                "reason_code": "dup",
                "actor": "test",
            },
        ]
        merge_csv = _write_lifecycle_csv(
            tmp_path, "merge", merge_rows, name=f"merge_{tag}.csv"
        )
        run_lifecycle(conn, merge_csv, "merge")
        return cid_a, cid_b, cid_c, keep_id  # type: ignore[return-value]

    def test_split_creates_new_canonical_for_split_candidate(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid_a, cid_b, cid_c, old_id = self._promote_and_merge_three(conn, tmp_path, "sp1")

        csv_path = _write_lifecycle_csv(tmp_path, "split", [{
            "canonical_entity_type": "participant",
            "old_canonical_id": old_id,
            "candidate_entity_id": cid_c,
            "reason_code": "different_person",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "split")

        assert ctrs.rows_applied == 1
        assert ctrs.rows_invalid == 0

        # cid_c now linked to a new canonical (not old_id)
        new_link = conn.execute(
            """
            SELECT canonical_entity_id FROM candidate_canonical_link
            WHERE candidate_entity_type = 'participant' AND candidate_entity_id = %s
            """,
            (cid_c,),
        ).fetchone()
        assert new_link is not None
        new_canonical_id = str(new_link[0])
        assert new_canonical_id != old_id

        # Old canonical still exists
        assert conn.execute(
            "SELECT id FROM canonical_participant WHERE id = %s", (old_id,)
        ).fetchone() is not None

        # cid_a and cid_b still linked to old canonical
        for cid in (cid_a, cid_b):
            link = conn.execute(
                """
                SELECT canonical_entity_id FROM candidate_canonical_link
                WHERE candidate_entity_type = 'participant' AND candidate_entity_id = %s
                """,
                (cid,),
            ).fetchone()
            assert str(link[0]) == old_id

    def test_split_candidate_not_linked_invalid(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid_a = _insert_auto_promote_participant(conn, suffix="sp-bad")
        cid_unlinked = _insert_auto_promote_participant(conn, suffix="sp-bad-ul")
        run_promote(conn, entity_type="participant")

        old_id = _get_promoted_canonical_id(conn, cid_a, "participant")

        # cid_unlinked is promoted to a DIFFERENT canonical
        csv_path = _write_lifecycle_csv(tmp_path, "split", [{
            "canonical_entity_type": "participant",
            "old_canonical_id": old_id,
            "candidate_entity_id": cid_unlinked,
            "reason_code": "test",
            "actor": "test",
        }])
        ctrs = run_lifecycle(conn, csv_path, "split")
        assert ctrs.rows_invalid == 1
        assert ctrs.rows_applied == 0

    def test_split_audit_logged(self, db_conn, tmp_path):
        conn, _ = db_conn
        cid_a, cid_b, cid_c, old_id = self._promote_and_merge_three(conn, tmp_path, "sp-audit")

        csv_path = _write_lifecycle_csv(tmp_path, "split", [{
            "canonical_entity_type": "participant",
            "old_canonical_id": old_id,
            "candidate_entity_id": cid_b,
            "reason_code": "split",
            "actor": "tester",
        }])
        run_lifecycle(conn, csv_path, "split")

        log = conn.execute(
            """
            SELECT action_type FROM resolution_manual_action_log
            WHERE canonical_entity_id = %s AND action_type = 'split'
            """,
            (old_id,),
        ).fetchone()
        assert log is not None


# ---------------------------------------------------------------------------
# _write_provenance helper
# ---------------------------------------------------------------------------

class TestWriteProvenance:
    def test_write_provenance_inserts_rows(self, db_conn):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="prov")
        run_promote(conn, entity_type="participant")
        canonical_id = _get_promoted_canonical_id(conn, cid, "participant")

        # Provenance should have been written by run_promote → _write_provenance
        count = conn.execute(
            """
            SELECT COUNT(*) FROM canonical_attribute_provenance
            WHERE canonical_entity_type = 'participant'
              AND canonical_entity_id = %s
            """,
            (canonical_id,),
        ).fetchone()[0]
        assert count > 0

    def test_write_provenance_upserts_on_conflict(self, db_conn):
        conn, _ = db_conn
        cid = _insert_auto_promote_participant(conn, suffix="prov-upsert")
        run_promote(conn, entity_type="participant")
        canonical_id = _get_promoted_canonical_id(conn, cid, "participant")

        # Call _write_provenance again — should not raise, should upsert
        _write_provenance(
            conn,
            entity_type="participant",
            canonical_id=canonical_id,
            candidate_id=cid,
            candidate_score=0.9,
            rule_version="v2",
            decided_by="manual",
        )

        # Check decided_by updated to 'manual'
        row = conn.execute(
            """
            SELECT decided_by FROM canonical_attribute_provenance
            WHERE canonical_entity_type = 'participant'
              AND canonical_entity_id = %s
              AND attribute_name = 'best_email'
            """,
            (canonical_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "manual"
