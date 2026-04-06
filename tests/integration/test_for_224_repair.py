from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATION_0029 = PROJECT_ROOT / "migrations" / "0029_missing_identity_repair.sql"


def test_migration_demotes_promoted_nameless_candidate_and_holds_review_rows(db_conn):
    conn, _ = db_conn

    canonical_id = str(
        conn.execute(
            """
            INSERT INTO canonical_participant
                (display_name, normalized_name, first_name, last_name,
                 canonical_confidence_score)
            VALUES (NULL, NULL, NULL, NULL, 0.9)
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
                ('for224-promoted', NULL, NULL, 'promoted@example.test',
                 'auto_promote', true, %s::uuid)
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
    conn.execute(
        """
        INSERT INTO canonical_registration
            (registration_external_id, canonical_primary_participant_id)
        VALUES ('for224-reg', %s::uuid)
        """,
        (canonical_id,),
    )
    conn.execute(
        """
        INSERT INTO canonical_attribute_provenance
            (canonical_entity_type, canonical_entity_id, attribute_name,
             source_candidate_type, source_candidate_id, decided_by)
        VALUES
            ('participant', %s::uuid, 'display_name', 'participant',
             %s::uuid, 'auto_promote')
        """,
        (canonical_id, promoted_candidate_id),
    )

    review_candidate_id = str(
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email,
                 resolution_state)
            VALUES
                ('for224-review', NULL, NULL, 'review@example.test', 'review')
            RETURNING id
            """
        ).fetchone()[0]
    )

    conn.commit()
    conn.autocommit = True
    try:
        conn.execute(MIGRATION_0029.read_text(encoding="utf-8"))
    finally:
        conn.autocommit = False

    promoted_row = conn.execute(
        """
        SELECT is_promoted, resolution_state, promoted_canonical_id, confidence_reasons
        FROM candidate_participant
        WHERE id = %s::uuid
        """,
        (promoted_candidate_id,),
    ).fetchone()
    assert promoted_row[0] is False
    assert promoted_row[1] == "hold"
    assert promoted_row[2] is None
    assert "hard_block:missing_name" in promoted_row[3]

    review_row = conn.execute(
        """
        SELECT resolution_state, confidence_reasons
        FROM candidate_participant
        WHERE id = %s::uuid
        """,
        (review_candidate_id,),
    ).fetchone()
    assert review_row[0] == "hold"
    assert "hard_block:missing_name" in review_row[1]

    assert conn.execute(
        """
        SELECT 1
        FROM candidate_canonical_link
        WHERE candidate_entity_type = 'participant'
          AND candidate_entity_id = %s::uuid
        """,
        (promoted_candidate_id,),
    ).fetchone() is None

    assert conn.execute(
        "SELECT 1 FROM canonical_participant WHERE id = %s::uuid",
        (canonical_id,),
    ).fetchone() is None

    assert conn.execute(
        """
        SELECT canonical_primary_participant_id
        FROM canonical_registration
        WHERE registration_external_id = 'for224-reg'
        """
    ).fetchone()[0] is None

    assert conn.execute(
        """
        SELECT 1
        FROM canonical_attribute_provenance
        WHERE canonical_entity_type = 'participant'
          AND canonical_entity_id = %s::uuid
        """,
        (canonical_id,),
    ).fetchone() is None

    actions = conn.execute(
        """
        SELECT candidate_entity_id::text, action_type, canonical_entity_id::text, reason_code
        FROM resolution_manual_action_log
        WHERE reason_code = 'for_224_missing_identity_repair'
        ORDER BY candidate_entity_id::text, action_type
        """
    ).fetchall()
    assert sorted(actions) == sorted([
        (
            promoted_candidate_id,
            "demote",
            None,
            "for_224_missing_identity_repair",
        ),
        (
            review_candidate_id,
            "hold",
            None,
            "for_224_missing_identity_repair",
        ),
    ])
