from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATION_0032 = PROJECT_ROOT / "migrations" / "0032_mobile_contact_subtype_backfill.sql"


def test_migration_backfills_mobile_subtype_into_candidate_and_canonical_contacts(db_conn):
    conn, _ = db_conn

    participant_id = conn.execute(
        """
        INSERT INTO participant (full_name, normalized_full_name, first_name, last_name)
        VALUES ('Mobile Person', 'mobile person', 'Mobile', 'Person')
        RETURNING id
        """
    ).fetchone()[0]
    participant_contact_id = conn.execute(
        """
        INSERT INTO participant_contact_point
            (participant_id, contact_type, contact_subtype,
             contact_value_raw, contact_value_normalized, is_primary, source_system)
        VALUES (%s, 'phone', 'primary_mobile', '(207) 555-1212', '+12075551212', true, 'test')
        RETURNING id
        """,
        (participant_id,),
    ).fetchone()[0]

    candidate_id = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name, best_email, resolution_state)
        VALUES ('for219-candidate', 'Mobile Person', 'mobile person', 'mobile@example.test', 'review')
        RETURNING id
        """
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO candidate_participant_contact
            (candidate_participant_id, contact_type, contact_subtype, raw_value, normalized_value,
             is_primary, source_table_name, source_row_pk)
        VALUES (%s, 'phone', NULL, '(207) 555-1212', '+12075551212', true,
                'participant_contact_point', %s::text)
        """,
        (candidate_id, participant_contact_id),
    )

    canonical_id = conn.execute(
        """
        INSERT INTO canonical_participant
            (display_name, normalized_name, canonical_confidence_score)
        VALUES ('Mobile Person', 'mobile person', 0.9)
        RETURNING id
        """
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO candidate_canonical_link
            (candidate_entity_type, candidate_entity_id, canonical_entity_id,
             promotion_score, promotion_mode, promoted_by)
        VALUES ('participant', %s::uuid, %s::uuid, 0.9, 'auto', 'pipeline')
        """,
        (candidate_id, canonical_id),
    )
    conn.execute(
        """
        INSERT INTO canonical_participant_contact
            (canonical_participant_id, contact_type, contact_subtype, raw_value, normalized_value, is_primary)
        VALUES (%s, 'phone', NULL, '(207) 555-1212', '+12075551212', true)
        """,
        (canonical_id,),
    )

    conn.commit()
    conn.autocommit = True
    try:
        conn.execute(MIGRATION_0032.read_text(encoding="utf-8"))
    finally:
        conn.autocommit = False

    assert conn.execute(
        "SELECT contact_subtype FROM participant_contact_point WHERE id = %s",
        (participant_contact_id,),
    ).fetchone()[0] == "mobile"
    assert conn.execute(
        """
        SELECT contact_subtype
        FROM candidate_participant_contact
        WHERE candidate_participant_id = %s
        """,
        (candidate_id,),
    ).fetchone()[0] == "mobile"
    assert conn.execute(
        """
        SELECT contact_subtype
        FROM canonical_participant_contact
        WHERE canonical_participant_id = %s
        """,
        (canonical_id,),
    ).fetchone()[0] == "mobile"
