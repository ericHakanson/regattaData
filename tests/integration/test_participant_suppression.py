"""FOR-884: participant_suppression consent/suppression surface (migration 0038).

A first-class surface for "do not email this address" decisions, independent of the
Mailchimp mirror. Anchored to a participant, an email, or both; soft-deleted (never
hard-deleted) so a suppression can be lifted while keeping the audit trail. The
active_suppressed_email view resolves rows to concrete addresses for the send-audience
exclusion join, including expanding a participant-wide (email NULL) suppression to all
of that participant's emails.
"""

from __future__ import annotations

import psycopg
import pytest


def _seed_participant(conn: psycopg.Connection, full_name: str, email: str) -> str:
    pid = conn.execute(
        "INSERT INTO participant (full_name, normalized_full_name) VALUES (%s, %s) RETURNING id",
        (full_name, full_name.lower()),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO participant_contact_point "
        "(participant_id, contact_type, contact_value_raw, contact_value_normalized, is_primary, source_system) "
        "VALUES (%s, 'email', %s, %s, true, 'test')",
        (pid, email, email.lower()),
    )
    return str(pid)


def _run_tag_backfill(conn: psycopg.Connection) -> None:
    """Mirror the DO_NOT_EMAIL_MINOR -> participant_suppression backfill in 0038,
    verbatim (DISTINCT ON collapse + FK-safe LEFT JOIN guard + ON CONFLICT)."""
    conn.execute(
        """
        INSERT INTO participant_suppression
            (participant_id, email_normalized, reason_code, source_system, actor, notes, observed_at)
        SELECT DISTINCT ON (t.participant_id, lower(t.email_normalized))
            t.participant_id, lower(t.email_normalized), 'minor_guardian_email',
            t.source_system, 'dq884_migration', 'migrated', t.observed_at
        FROM mailchimp_contact_tag t
        LEFT JOIN participant p ON p.id = t.participant_id
        WHERE t.tag_value = 'DO_NOT_EMAIL_MINOR'
          AND (t.participant_id IS NULL OR p.id IS NOT NULL)
        ORDER BY t.participant_id, lower(t.email_normalized), t.observed_at DESC NULLS LAST
        ON CONFLICT DO NOTHING
        """
    )


def test_email_scoped_suppression_shows_in_view(db_conn):
    conn, _ = db_conn
    conn.execute(
        "INSERT INTO participant_suppression (email_normalized, reason_code, actor) "
        "VALUES (%s, %s, %s)",
        ("guardian@example.com", "minor_guardian_email", "tester"),
    )
    conn.commit()

    rows = conn.execute(
        "SELECT email_normalized, reason_code FROM active_suppressed_email"
    ).fetchall()
    assert ("guardian@example.com", "minor_guardian_email") in rows


def test_participant_wide_suppression_expands_to_all_emails(db_conn):
    conn, _ = db_conn
    pid = _seed_participant(conn, "Ruby Garland", "ruby.primary@example.com")
    # A second email on the same participant.
    conn.execute(
        "INSERT INTO participant_contact_point "
        "(participant_id, contact_type, contact_value_raw, contact_value_normalized, is_primary, source_system) "
        "VALUES (%s, 'email', %s, %s, false, 'test')",
        (pid, "ruby.alt@example.com", "ruby.alt@example.com"),
    )
    # Participant-wide suppression (email_normalized NULL).
    conn.execute(
        "INSERT INTO participant_suppression (participant_id, reason_code, actor) "
        "VALUES (%s, %s, %s)",
        (pid, "manual_optout", "tester"),
    )
    conn.commit()

    emails = {
        r[0]
        for r in conn.execute(
            "SELECT email_normalized FROM active_suppressed_email WHERE reason_code = 'manual_optout'"
        ).fetchall()
    }
    assert emails == {"ruby.primary@example.com", "ruby.alt@example.com"}


def test_anchorless_row_is_rejected(db_conn):
    conn, _ = db_conn
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO participant_suppression (reason_code, actor) VALUES (%s, %s)",
                ("manual_optout", "tester"),
            )


def test_duplicate_active_suppression_is_rejected(db_conn):
    conn, _ = db_conn
    conn.execute(
        "INSERT INTO participant_suppression (email_normalized, reason_code, actor) "
        "VALUES (%s, %s, %s)",
        ("dup@example.com", "manual_optout", "tester"),
    )
    conn.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO participant_suppression (email_normalized, reason_code, actor) "
                "VALUES (%s, %s, %s)",
                ("dup@example.com", "manual_optout", "tester2"),
            )


def test_soft_delete_hides_from_view_and_frees_the_unique_slot(db_conn):
    conn, _ = db_conn
    sid = conn.execute(
        "INSERT INTO participant_suppression (email_normalized, reason_code, actor) "
        "VALUES (%s, %s, %s) RETURNING id",
        ("lift@example.com", "hard_bounce", "tester"),
    ).fetchone()[0]
    conn.commit()

    # Lift it (soft-delete) — must vanish from the active view.
    conn.execute(
        "UPDATE participant_suppression SET deleted_at = now() WHERE id = %s", (sid,)
    )
    conn.commit()
    assert conn.execute(
        "SELECT count(*) FROM active_suppressed_email WHERE email_normalized = 'lift@example.com'"
    ).fetchone()[0] == 0

    # The unique slot is freed: the same (email, reason) can be re-added.
    conn.execute(
        "INSERT INTO participant_suppression (email_normalized, reason_code, actor) "
        "VALUES (%s, %s, %s)",
        ("lift@example.com", "hard_bounce", "tester"),
    )
    conn.commit()
    assert conn.execute(
        "SELECT count(*) FROM active_suppressed_email WHERE email_normalized = 'lift@example.com'"
    ).fetchone()[0] == 1


def test_do_not_email_minor_tag_promotes_onto_surface(db_conn):
    """Mirrors the 0038 data migration: a DO_NOT_EMAIL_MINOR tag in
    mailchimp_contact_tag promotes to a minor_guardian_email suppression."""
    conn, _ = db_conn
    pid = _seed_participant(conn, "Ruby Garland", "guardian.minor@example.com")
    conn.execute(
        "INSERT INTO mailchimp_contact_tag "
        "(participant_id, email_normalized, tag_value, source_system, source_file_name, observed_at) "
        "VALUES (%s, %s, 'DO_NOT_EMAIL_MINOR', 'manual_consent_dq884', 'f.csv', now())",
        (pid, "guardian.minor@example.com"),
    )
    conn.commit()

    # A second tag row for the SAME anchor (participant+email) — DISTINCT ON must
    # collapse it so the backfill never proposes two rows that collide on the
    # active-unique index.
    conn.execute(
        "INSERT INTO mailchimp_contact_tag "
        "(participant_id, email_normalized, tag_value, source_system, source_file_name, observed_at) "
        "VALUES (%s, %s, 'DO_NOT_EMAIL_MINOR', 'manual_consent_dq884', 'g.csv', now())",
        (pid, "guardian.minor@example.com"),
    )
    conn.commit()

    _run_tag_backfill(conn)

    row = conn.execute(
        "SELECT reason_code, source_system, actor FROM participant_suppression "
        "WHERE email_normalized = 'guardian.minor@example.com'"
    ).fetchone()
    assert row == ("minor_guardian_email", "manual_consent_dq884", "dq884_migration")
    assert conn.execute(
        "SELECT count(*) FROM active_suppressed_email WHERE email_normalized = 'guardian.minor@example.com'"
    ).fetchone()[0] == 1


def test_backfill_is_rerun_safe(db_conn):
    """Re-running the backfill is idempotent (ON CONFLICT DO NOTHING) — applying the
    migration statement twice must not raise or create a duplicate suppression."""
    conn, _ = db_conn
    pid = _seed_participant(conn, "Rerun Kid", "rerun@example.com")
    conn.execute(
        "INSERT INTO mailchimp_contact_tag "
        "(participant_id, email_normalized, tag_value, source_system, source_file_name, observed_at) "
        "VALUES (%s, %s, 'DO_NOT_EMAIL_MINOR', 'manual_consent_dq884', 'r.csv', now())",
        (pid, "rerun@example.com"),
    )
    conn.commit()

    _run_tag_backfill(conn)
    _run_tag_backfill(conn)  # rerun must not raise or duplicate
    conn.commit()

    rows = conn.execute(
        "SELECT participant_id::text, reason_code FROM participant_suppression "
        "WHERE email_normalized = 'rerun@example.com'"
    ).fetchall()
    assert rows == [(pid, "minor_guardian_email")]
