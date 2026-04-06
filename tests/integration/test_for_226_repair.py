from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATION_0031 = PROJECT_ROOT / "migrations" / "0031_invalid_phone_repair.sql"


def test_migration_clears_invalid_phone_values_across_layers(db_conn):
    conn, _ = db_conn

    participant_id = conn.execute(
        """
        INSERT INTO participant (full_name, normalized_full_name, first_name, last_name)
        VALUES ('Phone Repair Person', 'phone repair person', 'Phone', 'Repair')
        RETURNING id
        """
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO participant_contact_point
            (participant_id, contact_type, contact_subtype,
             contact_value_raw, contact_value_normalized, is_primary, source_system)
        VALUES (%s, 'phone', 'home', '8295056', '+8295056', true, 'test')
        """,
        (participant_id,),
    )

    candidate_id = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name, best_email, best_phone, resolution_state)
        VALUES ('for226-candidate', 'Phone Repair Person', 'phone repair person',
                'repair@example.test', '+8295056', 'review')
        RETURNING id
        """
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO candidate_participant_contact
            (candidate_participant_id, contact_type, raw_value, normalized_value,
             is_primary, source_table_name, source_row_pk)
        VALUES (%s, 'phone', '+8295056', '+8295056', true, 'test', 'candidate-row')
        """,
        (candidate_id,),
    )

    canonical_id = conn.execute(
        """
        INSERT INTO canonical_participant
            (display_name, normalized_name, best_phone, canonical_confidence_score)
        VALUES ('Phone Repair Person', 'phone repair person', '+8295056', 0.9)
        RETURNING id
        """
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO canonical_participant_contact
            (canonical_participant_id, contact_type, raw_value, normalized_value, is_primary)
        VALUES (%s, 'phone', '+8295056', '+8295056', true)
        """,
        (canonical_id,),
    )

    conn.commit()
    conn.autocommit = True
    try:
        conn.execute(MIGRATION_0031.read_text(encoding="utf-8"))
    finally:
        conn.autocommit = False

    assert conn.execute(
        """
        SELECT contact_value_normalized
        FROM participant_contact_point
        WHERE participant_id = %s
        """,
        (participant_id,),
    ).fetchone()[0] is None

    assert conn.execute(
        """
        SELECT COUNT(*)
        FROM candidate_participant_contact
        WHERE candidate_participant_id = %s
          AND contact_type = 'phone'
        """,
        (candidate_id,),
    ).fetchone()[0] == 0

    assert conn.execute(
        """
        SELECT COUNT(*)
        FROM canonical_participant_contact
        WHERE canonical_participant_id = %s
          AND contact_type = 'phone'
        """,
        (canonical_id,),
    ).fetchone()[0] == 0

    assert conn.execute(
        "SELECT best_phone FROM candidate_participant WHERE id = %s",
        (candidate_id,),
    ).fetchone()[0] is None

    assert conn.execute(
        "SELECT best_phone FROM canonical_participant WHERE id = %s",
        (canonical_id,),
    ).fetchone()[0] is None
