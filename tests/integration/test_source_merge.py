"""Integration tests for the source-layer yacht merge pipeline."""

from __future__ import annotations

import csv
from pathlib import Path

import psycopg
import psycopg.rows

from regatta_etl.resolution_source_to_candidate import (
    event_fingerprint,
    participant_fingerprint,
    registration_fingerprint,
    yacht_fingerprint,
)
from regatta_etl.source_merge import run_source_merge


def _write_merge_csv(tmp_path: Path, rows: list[dict], name: str = "source_merge.csv") -> Path:
    path = tmp_path / name
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["entity_type", "keep_id", "drop_id", "reason", "actor", "qa_action"],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)
    return path


def _insert_source_yacht(
    conn: psycopg.Connection,
    *,
    name: str,
    normalized_name: str,
    sail_number: str | None = None,
    normalized_sail_number: str | None = None,
) -> str:
    row = conn.execute(
        """
        INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (name, normalized_name, sail_number, normalized_sail_number),
    ).fetchone()
    return str(row["id"])


def _insert_candidate_yacht(
    conn: psycopg.Connection,
    *,
    name: str,
    normalized_name: str,
    sail_number: str | None = None,
    normalized_sail_number: str | None = None,
    quality_score: float = 0.0,
    resolution_state: str = "review",
    is_promoted: bool = False,
    promoted_canonical_id: str | None = None,
) -> str:
    row = conn.execute(
        """
        INSERT INTO candidate_yacht
            (stable_fingerprint, name, normalized_name, sail_number, normalized_sail_number,
             quality_score, resolution_state, is_promoted, promoted_canonical_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            yacht_fingerprint(normalized_name, normalized_sail_number),
            name,
            normalized_name,
            sail_number,
            normalized_sail_number,
            quality_score,
            resolution_state,
            is_promoted,
            promoted_canonical_id,
        ),
    ).fetchone()
    return str(row["id"])


def _insert_candidate_source_link(
    conn: psycopg.Connection,
    *,
    candidate_id: str,
    source_row_pk: str,
) -> None:
    conn.execute(
        """
        INSERT INTO candidate_source_link
            (candidate_entity_type, candidate_entity_id, source_table_name, source_row_pk)
        VALUES ('yacht', %s, 'yacht', %s)
        """,
        (candidate_id, source_row_pk),
    )


def _insert_candidate_event(
    conn: psycopg.Connection,
    *,
    normalized_event_name: str = "source-merge-regatta",
    season_year: int = 2026,
    event_external_id: str | None = None,
) -> str:
    row = conn.execute(
        """
        INSERT INTO candidate_event
            (stable_fingerprint, event_name, normalized_event_name, season_year, event_external_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            event_fingerprint(normalized_event_name, season_year, event_external_id),
            normalized_event_name,
            normalized_event_name,
            season_year,
            event_external_id,
        ),
    ).fetchone()
    return str(row["id"])


def _insert_candidate_registration(
    conn: psycopg.Connection,
    *,
    candidate_event_id: str,
    candidate_yacht_id: str,
    registration_external_id: str,
) -> str:
    row = conn.execute(
        """
        INSERT INTO candidate_registration
            (stable_fingerprint, registration_external_id, candidate_event_id, candidate_yacht_id)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (
            registration_fingerprint(candidate_event_id, registration_external_id, candidate_yacht_id),
            registration_external_id,
            candidate_event_id,
            candidate_yacht_id,
        ),
    ).fetchone()
    return str(row["id"])


def _insert_candidate_participant(
    conn: psycopg.Connection,
    *,
    display_name: str,
    normalized_name: str,
    best_email: str | None = None,
) -> str:
    row = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name, best_email)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (
            participant_fingerprint(normalized_name, best_email),
            display_name,
            normalized_name,
            best_email,
        ),
    ).fetchone()
    return str(row["id"])


def _insert_candidate_role(
    conn: psycopg.Connection,
    *,
    candidate_participant_id: str,
    role: str,
    candidate_event_id: str | None = None,
    candidate_registration_id: str | None = None,
    source_context: str = "test_source_merge",
) -> str:
    row = conn.execute(
        """
        INSERT INTO candidate_participant_role_assignment
            (candidate_participant_id, role, candidate_event_id,
             candidate_registration_id, source_context)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            candidate_participant_id,
            role,
            candidate_event_id,
            candidate_registration_id,
            source_context,
        ),
    ).fetchone()
    return str(row["id"])


def _insert_canonical_yacht(
    conn: psycopg.Connection,
    *,
    name: str,
    normalized_name: str,
    sail_number: str | None = None,
    normalized_sail_number: str | None = None,
) -> str:
    row = conn.execute(
        """
        INSERT INTO canonical_yacht
            (name, normalized_name, sail_number, normalized_sail_number)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (name, normalized_name, sail_number, normalized_sail_number),
    ).fetchone()
    return str(row["id"])


def _insert_candidate_canonical_link(
    conn: psycopg.Connection,
    *,
    candidate_id: str,
    canonical_id: str,
    promotion_score: float = 0.9,
) -> None:
    conn.execute(
        """
        INSERT INTO candidate_canonical_link
            (candidate_entity_type, candidate_entity_id, canonical_entity_id,
             promotion_score, promotion_mode, promoted_by)
        VALUES ('yacht', %s, %s, %s, 'auto', 'test_source_merge')
        """,
        (candidate_id, canonical_id, promotion_score),
    )


class TestSourceMerge:
    def test_merge_repoints_registration_when_drop_candidate_becomes_sourceless(
        self, db_conn, tmp_path
    ):
        conn, _ = db_conn
        conn.row_factory = psycopg.rows.dict_row

        keep_source_id = _insert_source_yacht(
            conn, name="Sea Legs", normalized_name="sea-legs", sail_number="USA 1",
            normalized_sail_number="usa-1",
        )
        drop_source_id = _insert_source_yacht(
            conn, name="SEA LEGS", normalized_name="sea-legs", sail_number="USA 1",
            normalized_sail_number="usa-1",
        )
        keep_candidate_id = _insert_candidate_yacht(
            conn, name="Sea Legs", normalized_name="sea-legs-canonical",
            normalized_sail_number="usa-1", quality_score=0.95,
        )
        drop_candidate_id = _insert_candidate_yacht(
            conn, name="SEA LEGS", normalized_name="sea-legs-legacy",
            normalized_sail_number="usa-legacy-1", quality_score=0.25,
        )
        _insert_candidate_source_link(conn, candidate_id=keep_candidate_id, source_row_pk=keep_source_id)
        _insert_candidate_source_link(conn, candidate_id=drop_candidate_id, source_row_pk=drop_source_id)

        event_id = _insert_candidate_event(conn, normalized_event_name="merge-regatta-a")
        reg_id = _insert_candidate_registration(
            conn,
            candidate_event_id=event_id,
            candidate_yacht_id=drop_candidate_id,
            registration_external_id="REG-A",
        )

        csv_path = _write_merge_csv(tmp_path, [{
            "entity_type": "yacht",
            "keep_id": keep_source_id,
            "drop_id": drop_source_id,
            "reason": "duplicate_yacht",
            "actor": "test",
            "qa_action": "merge",
        }])
        ctrs = run_source_merge(conn, csv_path)

        assert ctrs.rows_merged == 1
        assert ctrs.db_errors == 0

        reg_row = conn.execute(
            """
            SELECT candidate_yacht_id, stable_fingerprint
            FROM candidate_registration
            WHERE id = %s::uuid
            """,
            (reg_id,),
        ).fetchone()
        assert str(reg_row["candidate_yacht_id"]) == keep_candidate_id
        assert reg_row["stable_fingerprint"] == registration_fingerprint(event_id, "REG-A", keep_candidate_id)

        drop_candidate = conn.execute(
            """
            SELECT resolution_state, is_promoted, promoted_canonical_id
            FROM candidate_yacht
            WHERE id = %s::uuid
            """,
            (drop_candidate_id,),
        ).fetchone()
        assert drop_candidate["resolution_state"] == "reject"
        assert drop_candidate["is_promoted"] is False
        assert drop_candidate["promoted_canonical_id"] is None

        assert conn.execute(
            "SELECT 1 FROM candidate_source_link WHERE candidate_entity_id = %s::uuid",
            (drop_candidate_id,),
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM yacht WHERE id = %s::uuid",
            (drop_source_id,),
        ).fetchone() is None

    def test_merge_registration_collision_repoints_and_dedupes_role_assignments(
        self, db_conn, tmp_path
    ):
        conn, _ = db_conn
        conn.row_factory = psycopg.rows.dict_row

        keep_source_id = _insert_source_yacht(
            conn, name="Blue Jay", normalized_name="blue-jay", sail_number="USA 22",
            normalized_sail_number="usa-22",
        )
        drop_source_id = _insert_source_yacht(
            conn, name="BLUE JAY", normalized_name="blue-jay", sail_number="USA 22",
            normalized_sail_number="usa-22",
        )
        keep_candidate_id = _insert_candidate_yacht(
            conn, name="Blue Jay", normalized_name="blue-jay-current",
            normalized_sail_number="usa-22", quality_score=0.9,
        )
        drop_candidate_id = _insert_candidate_yacht(
            conn, name="BLUE JAY", normalized_name="blue-jay-old",
            normalized_sail_number="usa-legacy-22", quality_score=0.3,
        )
        _insert_candidate_source_link(conn, candidate_id=keep_candidate_id, source_row_pk=keep_source_id)
        _insert_candidate_source_link(conn, candidate_id=drop_candidate_id, source_row_pk=drop_source_id)

        event_id = _insert_candidate_event(conn, normalized_event_name="merge-regatta-b")
        keep_reg_id = _insert_candidate_registration(
            conn,
            candidate_event_id=event_id,
            candidate_yacht_id=keep_candidate_id,
            registration_external_id="REG-COLLIDE",
        )
        drop_reg_id = _insert_candidate_registration(
            conn,
            candidate_event_id=event_id,
            candidate_yacht_id=drop_candidate_id,
            registration_external_id="REG-COLLIDE",
        )
        dup_participant_id = _insert_candidate_participant(
            conn,
            display_name="Pat Duplicate",
            normalized_name="pat-duplicate",
            best_email="dup@example.test",
        )
        unique_participant_id = _insert_candidate_participant(
            conn,
            display_name="Casey Crew",
            normalized_name="casey-crew",
            best_email="crew@example.test",
        )
        _insert_candidate_role(
            conn,
            candidate_participant_id=dup_participant_id,
            role="skipper",
            candidate_event_id=event_id,
            candidate_registration_id=keep_reg_id,
        )
        _insert_candidate_role(
            conn,
            candidate_participant_id=dup_participant_id,
            role="skipper",
            candidate_event_id=event_id,
            candidate_registration_id=drop_reg_id,
        )
        _insert_candidate_role(
            conn,
            candidate_participant_id=unique_participant_id,
            role="crew",
            candidate_event_id=event_id,
            candidate_registration_id=drop_reg_id,
        )

        csv_path = _write_merge_csv(tmp_path, [{
            "entity_type": "yacht",
            "keep_id": keep_source_id,
            "drop_id": drop_source_id,
            "reason": "duplicate_yacht",
            "actor": "test",
            "qa_action": "merge",
        }])
        ctrs = run_source_merge(conn, csv_path)

        assert ctrs.rows_merged == 1
        assert ctrs.db_errors == 0

        reg_count = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM candidate_registration
            WHERE candidate_event_id = %s::uuid
              AND registration_external_id = 'REG-COLLIDE'
            """,
            (event_id,),
        ).fetchone()
        assert reg_count["n"] == 1
        assert conn.execute(
            "SELECT 1 FROM candidate_registration WHERE id = %s::uuid",
            (drop_reg_id,),
        ).fetchone() is None

        roles = conn.execute(
            """
            SELECT candidate_participant_id, role, candidate_registration_id
            FROM candidate_participant_role_assignment
            WHERE candidate_registration_id = %s::uuid
            ORDER BY role, candidate_participant_id
            """,
            (keep_reg_id,),
        ).fetchall()
        assert len(roles) == 2
        assert {(str(r["candidate_participant_id"]), r["role"]) for r in roles} == {
            (dup_participant_id, "skipper"),
            (unique_participant_id, "crew"),
        }
        assert conn.execute(
            """
            SELECT 1
            FROM candidate_participant_role_assignment
            WHERE candidate_registration_id = %s::uuid
            """,
            (drop_reg_id,),
        ).fetchone() is None

    def test_merge_unlinks_promoted_stale_candidate_without_deleting_canonical(
        self, db_conn, tmp_path
    ):
        conn, _ = db_conn
        conn.row_factory = psycopg.rows.dict_row

        keep_source_id = _insert_source_yacht(
            conn, name="Night Watch", normalized_name="night-watch"
        )
        drop_source_id = _insert_source_yacht(
            conn, name="NIGHT WATCH", normalized_name="night-watch"
        )
        keep_candidate_id = _insert_candidate_yacht(
            conn, name="Night Watch", normalized_name="night-watch-keep",
            quality_score=0.85,
        )
        canonical_id = _insert_canonical_yacht(
            conn, name="Night Watch", normalized_name="night-watch-canonical"
        )
        drop_candidate_id = _insert_candidate_yacht(
            conn,
            name="NIGHT WATCH",
            normalized_name="night-watch-drop",
            quality_score=0.2,
            resolution_state="auto_promote",
            is_promoted=True,
            promoted_canonical_id=canonical_id,
        )
        _insert_candidate_source_link(conn, candidate_id=keep_candidate_id, source_row_pk=keep_source_id)
        _insert_candidate_source_link(conn, candidate_id=drop_candidate_id, source_row_pk=drop_source_id)
        _insert_candidate_canonical_link(
            conn, candidate_id=drop_candidate_id, canonical_id=canonical_id
        )

        csv_path = _write_merge_csv(tmp_path, [{
            "entity_type": "yacht",
            "keep_id": keep_source_id,
            "drop_id": drop_source_id,
            "reason": "duplicate_yacht",
            "actor": "test",
            "qa_action": "merge",
        }])
        ctrs = run_source_merge(conn, csv_path)

        assert ctrs.rows_merged == 1
        assert ctrs.db_errors == 0
        assert any("stale promoted candidate" in warning for warning in ctrs.warnings)

        drop_candidate = conn.execute(
            """
            SELECT is_promoted, promoted_canonical_id, resolution_state
            FROM candidate_yacht
            WHERE id = %s::uuid
            """,
            (drop_candidate_id,),
        ).fetchone()
        assert drop_candidate["is_promoted"] is False
        assert drop_candidate["promoted_canonical_id"] is None
        assert drop_candidate["resolution_state"] == "reject"

        assert conn.execute(
            """
            SELECT 1
            FROM candidate_canonical_link
            WHERE candidate_entity_type = 'yacht' AND candidate_entity_id = %s::uuid
            """,
            (drop_candidate_id,),
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM canonical_yacht WHERE id = %s::uuid",
            (canonical_id,),
        ).fetchone() is not None

        unlink_log = conn.execute(
            """
            SELECT action_type
            FROM resolution_manual_action_log
            WHERE candidate_entity_id = %s::uuid AND action_type = 'unlink'
            """,
            (drop_candidate_id,),
        ).fetchone()
        assert unlink_log is not None

    def test_merge_rehomes_drop_candidate_when_keep_row_has_none(
        self, db_conn, tmp_path
    ):
        conn, _ = db_conn
        conn.row_factory = psycopg.rows.dict_row

        keep_source_id = _insert_source_yacht(
            conn, name="Ghost", normalized_name="ghost"
        )
        drop_source_id = _insert_source_yacht(
            conn, name="GHOST", normalized_name="ghost"
        )
        survivor_candidate_id = _insert_candidate_yacht(
            conn, name="Ghost", normalized_name="ghost-best", quality_score=0.8
        )
        _insert_candidate_source_link(
            conn, candidate_id=survivor_candidate_id, source_row_pk=drop_source_id
        )

        csv_path = _write_merge_csv(tmp_path, [{
            "entity_type": "yacht",
            "keep_id": keep_source_id,
            "drop_id": drop_source_id,
            "reason": "duplicate_yacht",
            "actor": "test",
            "qa_action": "merge",
        }])
        ctrs = run_source_merge(conn, csv_path)

        assert ctrs.rows_merged == 1
        assert ctrs.db_errors == 0

        survivor_link = conn.execute(
            """
            SELECT source_row_pk
            FROM candidate_source_link
            WHERE candidate_entity_type = 'yacht' AND candidate_entity_id = %s::uuid
            """,
            (survivor_candidate_id,),
        ).fetchone()
        assert survivor_link is not None
        assert survivor_link["source_row_pk"] == keep_source_id
