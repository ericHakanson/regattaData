from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATION_0033 = PROJECT_ROOT / "migrations" / "0033_email_like_participant_display_name_repair.sql"


def test_migration_repairs_email_like_participant_candidate_and_canonical_names(db_conn):
    conn, _ = db_conn

    participant_id = conn.execute(
        """
        INSERT INTO participant (full_name, normalized_full_name, first_name, last_name)
        VALUES ('jen@example.com Baker', 'jenexamplecom baker', 'jen@example.com', 'Baker')
        RETURNING id
        """
    ).fetchone()[0]

    candidate_id = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
        VALUES
            ('for230-candidate', 'jen@example.com Baker', 'jenexamplecom baker',
             'jen@example.com', 'review')
        RETURNING id
        """
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO candidate_source_link
            (candidate_entity_type, candidate_entity_id, source_table_name,
             source_row_pk, source_system, link_score, link_reason)
        VALUES
            ('participant', %s::uuid, 'participant', %s::text,
             'operational_db', 1.0, '{}'::jsonb)
        """,
        (candidate_id, participant_id),
    )

    canonical_id = conn.execute(
        """
        INSERT INTO canonical_participant
            (display_name, normalized_name, first_name, last_name,
             canonical_confidence_score)
        VALUES
            ('jen@example.com Baker', 'jenexamplecom baker',
             'jen@example.com', 'Baker', 0.9)
        RETURNING id
        """
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO candidate_canonical_link
            (candidate_entity_type, candidate_entity_id, canonical_entity_id,
             promotion_score, promotion_mode, promoted_by)
        VALUES
            ('participant', %s::uuid, %s::uuid, 0.9, 'auto', 'pipeline')
        """,
        (candidate_id, canonical_id),
    )

    conn.commit()
    conn.autocommit = True
    try:
        conn.execute(MIGRATION_0033.read_text(encoding="utf-8"))
    finally:
        conn.autocommit = False

    participant_row = conn.execute(
        """
        SELECT full_name, normalized_full_name, first_name, last_name
        FROM participant
        WHERE id = %s
        """,
        (participant_id,),
    ).fetchone()
    assert participant_row == ("Baker", "baker", None, "Baker")

    candidate_row = conn.execute(
        """
        SELECT display_name, normalized_name
        FROM candidate_participant
        WHERE id = %s
        """,
        (candidate_id,),
    ).fetchone()
    assert candidate_row == ("Baker", "baker")

    canonical_row = conn.execute(
        """
        SELECT display_name, normalized_name, first_name, last_name
        FROM canonical_participant
        WHERE id = %s
        """,
        (canonical_id,),
    ).fetchone()
    assert canonical_row == ("Baker", "baker", "Baker", None)
