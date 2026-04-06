from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATION_0027 = PROJECT_ROOT / "migrations" / "0027_sourceless_candidate_repair.sql"


def test_migration_rejects_sourceless_candidates_and_unlinks_promoted_rows(db_conn):
    conn, _ = db_conn

    canonical_id = str(
        conn.execute(
            """
            INSERT INTO canonical_participant
                (display_name, canonical_confidence_score)
            VALUES ('FOR-229 Canonical', 0.9)
            RETURNING id
            """
        ).fetchone()[0]
    )
    promoted_candidate_id = str(
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email,
                 resolution_state, is_promoted, promoted_canonical_id)
            VALUES
                ('for229-promoted', 'Promoted Sourceless', 'promoted sourceless',
                 'promoted@example.test', 'auto_promote', true, %s::uuid)
            RETURNING id
            """,
            (canonical_id,),
        ).fetchone()[0]
    )
    conn.execute(
        """
        INSERT INTO candidate_canonical_link
            (candidate_entity_type, candidate_entity_id, canonical_entity_id,
             promotion_score, promotion_mode, promoted_by)
        VALUES
            ('participant', %s::uuid, %s::uuid, 0.9, 'auto', 'pipeline')
        """,
        (promoted_candidate_id, canonical_id),
    )

    review_candidate_id = str(
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
            VALUES
                ('for229-review', 'Review Sourceless', 'review sourceless',
                 'review@example.test', 'review')
            RETURNING id
            """
        ).fetchone()[0]
    )

    conn.commit()
    conn.autocommit = True
    try:
        conn.execute(MIGRATION_0027.read_text(encoding="utf-8"))
    finally:
        conn.autocommit = False

    promoted_row = conn.execute(
        """
        SELECT is_promoted, resolution_state, promoted_canonical_id
        FROM candidate_participant
        WHERE id = %s::uuid
        """,
        (promoted_candidate_id,),
    ).fetchone()
    assert promoted_row[0] is False
    assert promoted_row[1] == "reject"
    assert promoted_row[2] is None

    review_row = conn.execute(
        """
        SELECT is_promoted, resolution_state, promoted_canonical_id
        FROM candidate_participant
        WHERE id = %s::uuid
        """,
        (review_candidate_id,),
    ).fetchone()
    assert review_row[0] is False
    assert review_row[1] == "reject"
    assert review_row[2] is None

    assert conn.execute(
        """
        SELECT 1
        FROM candidate_canonical_link
        WHERE candidate_entity_type = 'participant'
          AND candidate_entity_id = %s::uuid
        """,
        (promoted_candidate_id,),
    ).fetchone() is None

    actions = conn.execute(
        """
        SELECT candidate_entity_id::text, action_type, canonical_entity_id::text, reason_code
        FROM resolution_manual_action_log
        WHERE reason_code = 'for_229_sourceless_candidate_repair'
        ORDER BY candidate_entity_id::text, action_type
        """
    ).fetchall()
    assert actions == [
        (
            promoted_candidate_id,
            "unlink",
            canonical_id,
            "for_229_sourceless_candidate_repair",
        ),
        (
            review_candidate_id,
            "reject",
            None,
            "for_229_sourceless_candidate_repair",
        ),
    ]

    conn.commit()
    conn.autocommit = True
    try:
        conn.execute(MIGRATION_0027.read_text(encoding="utf-8"))
    finally:
        conn.autocommit = False
    count = conn.execute(
        """
        SELECT COUNT(*)
        FROM resolution_manual_action_log
        WHERE reason_code = 'for_229_sourceless_candidate_repair'
        """
    ).fetchone()[0]
    assert count == 2
