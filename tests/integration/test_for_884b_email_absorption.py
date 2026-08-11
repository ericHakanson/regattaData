"""FOR-884b: email-as-name absorption in source->candidate resolution.

A placeholder-named record (email-as-name or blank) that carries an email should fold
into the UNIQUE real-name candidate that owns that email, instead of forming an
email-as-name twin. Two distinct real names sharing an email (families) are never
merged, and a shared email owned by multiple real people is left ambiguous.

The real-name candidate must pre-exist (the map is loaded at run start, matching the
existing under-combination reuse mechanism and the real waiver->mailchimp/wix order),
so tests seed + run, then add the placeholder + run again.
"""

from __future__ import annotations

import psycopg

from regatta_etl.normalize import normalize_person_name_for_identity
from regatta_etl.resolution_source_to_candidate import run_source_to_candidate


def _seed_participant(conn: psycopg.Connection, full_name: str, email: str) -> str:
    norm = normalize_person_name_for_identity(full_name)
    pid = conn.execute(
        "INSERT INTO participant (full_name, normalized_full_name) VALUES (%s, %s) RETURNING id",
        (full_name, norm or full_name),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO participant_contact_point "
        "(participant_id, contact_type, contact_value_raw, contact_value_normalized, is_primary, source_system) "
        "VALUES (%s, 'email', %s, %s, true, 'test')",
        (pid, email, email.lower()),
    )
    return str(pid)


def _candidates_for_email(conn: psycopg.Connection, email: str) -> int:
    return conn.execute(
        "SELECT count(*) FROM candidate_participant WHERE lower(best_email) = %s",
        (email.lower(),),
    ).fetchone()[0]


def _candidate_for_source_pk(conn: psycopg.Connection, pk: str) -> str | None:
    row = conn.execute(
        "SELECT candidate_entity_id::text FROM candidate_source_link "
        "WHERE candidate_entity_type='participant' AND source_table_name='participant' AND source_row_pk=%s",
        (pk,),
    ).fetchone()
    return row[0] if row else None


def test_placeholder_absorbs_into_realname_candidate(db_conn, tmp_path):
    conn, _ = db_conn
    real_pid = _seed_participant(conn, "Paul Koch", "paul@example.com")
    conn.commit()
    run_source_to_candidate(conn, entity_type="participant")
    conn.commit()

    # Now an email-as-name record arrives for the same email (name IS the email).
    ph_pid = _seed_participant(conn, "paul@example.com", "paul@example.com")
    conn.commit()
    ctrs = run_source_to_candidate(conn, entity_type="participant")
    conn.commit()

    assert ctrs.participants_email_absorption_reused >= 1
    # Only ONE candidate owns the email — the placeholder folded in, no twin.
    assert _candidates_for_email(conn, "paul@example.com") == 1
    # Both source rows point at the same (real-name) candidate.
    assert _candidate_for_source_pk(conn, real_pid) == _candidate_for_source_pk(conn, ph_pid)


def test_single_run_realname_then_placeholder_absorbs(db_conn, tmp_path):
    """Both rows present before a SINGLE run: the real-name candidate is created earlier
    in the run and the placeholder processed later must still absorb (map is maintained
    incrementally within the loop, not just from a pre-run snapshot)."""
    conn, _ = db_conn
    _seed_participant(conn, "Paul Koch", "solo@example.com")       # created first in the run
    _seed_participant(conn, "solo@example.com", "solo@example.com")  # placeholder, same email
    conn.commit()

    ctrs = run_source_to_candidate(conn, entity_type="participant")
    conn.commit()

    assert ctrs.participants_email_absorption_reused >= 1
    assert _candidates_for_email(conn, "solo@example.com") == 1


def test_distinct_real_names_sharing_email_are_not_merged(db_conn, tmp_path):
    conn, _ = db_conn
    _seed_participant(conn, "Carrie Bridge", "family@example.com")
    conn.commit()
    run_source_to_candidate(conn, entity_type="participant")
    conn.commit()

    # A DIFFERENT real person shares the family email — must not absorb.
    _seed_participant(conn, "Greta Bridge", "family@example.com")
    conn.commit()
    ctrs = run_source_to_candidate(conn, entity_type="participant")
    conn.commit()

    assert ctrs.participants_email_absorption_reused == 0
    assert _candidates_for_email(conn, "family@example.com") == 2


def test_ambiguous_shared_email_leaves_placeholder_unabsorbed(db_conn, tmp_path):
    conn, _ = db_conn
    _seed_participant(conn, "Paul Koch", "shared@example.com")
    _seed_participant(conn, "Jane Doe", "shared@example.com")
    conn.commit()
    run_source_to_candidate(conn, entity_type="participant")
    conn.commit()

    # Two real people own this email -> absorption must not guess.
    _seed_participant(conn, "shared@example.com", "shared@example.com")
    conn.commit()
    ctrs = run_source_to_candidate(conn, entity_type="participant")
    conn.commit()

    assert ctrs.participants_email_absorption_ambiguous >= 1
    assert ctrs.participants_email_absorption_reused == 0
