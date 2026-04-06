from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATION_0030 = PROJECT_ROOT / "migrations" / "0030_organization_candidate_repair.sql"


def test_migration_rejects_org_candidates_and_demotes_promoted_orgs(db_conn):
    conn, _ = db_conn

    canonical_id = str(
        conn.execute(
            """
            INSERT INTO canonical_participant
                (display_name, normalized_name, first_name, last_name,
                 canonical_confidence_score)
            VALUES ('Tenacious Holdings LLC', 'tenacious holdings llc',
                    'Tenacious', 'Holdings LLC', 0.9)
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
                ('for222-promoted', 'Tenacious Holdings LLC', 'tenacious holdings llc',
                 'ops@tenacious.test', 'auto_promote', true, %s::uuid)
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

    hold_candidate_id = str(
        conn.execute(
            """
            INSERT INTO candidate_participant
                (stable_fingerprint, display_name, normalized_name, best_email,
                 resolution_state)
            VALUES
                ('for222-hold', 'Nantucket Yacht Club', 'nantucket yacht club',
                 'club@example.test', 'hold')
            RETURNING id
            """
        ).fetchone()[0]
    )

    conn.commit()
    conn.autocommit = True
    try:
        conn.execute(MIGRATION_0030.read_text(encoding="utf-8"))
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
    assert promoted_row[1] == "reject"
    assert promoted_row[2] is None
    assert "hard_block:organization_entity" in promoted_row[3]

    hold_row = conn.execute(
        """
        SELECT resolution_state, confidence_reasons
        FROM candidate_participant
        WHERE id = %s::uuid
        """,
        (hold_candidate_id,),
    ).fetchone()
    assert hold_row[0] == "reject"
    assert "hard_block:organization_entity" in hold_row[1]

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

    actions = conn.execute(
        """
        SELECT candidate_entity_id::text, action_type, canonical_entity_id::text, reason_code
        FROM resolution_manual_action_log
        WHERE reason_code = 'for_222_organization_candidate_repair'
        ORDER BY candidate_entity_id::text, action_type
        """
    ).fetchall()
    assert sorted(actions) == sorted([
        (
            promoted_candidate_id,
            "demote",
            None,
            "for_222_organization_candidate_repair",
        ),
        (
            hold_candidate_id,
            "reject",
            None,
            "for_222_organization_candidate_repair",
        ),
    ])
