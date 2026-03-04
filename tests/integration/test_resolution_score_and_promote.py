"""Integration tests for resolution_score + resolution_promote pipelines.

Seeds minimal candidate data, runs scoring, verifies scores/states, then
runs promotion and verifies canonical rows + links.
"""

from __future__ import annotations

import hashlib
import json
import uuid

import psycopg
import pytest

from regatta_etl.resolution_promote import PromoteCounters, run_promote
from regatta_etl.resolution_score import ScoreCounters, run_score


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fp(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _insert_candidate_participant(
    conn: psycopg.Connection,
    *,
    normalized_name: str | None = "john-doe",
    best_email: str | None = "john@example.com",
    best_phone: str | None = "+12075551234",
    date_of_birth: str | None = "1990-01-01",
) -> str:
    fp = _fp(normalized_name or "", (best_email or "").lower())
    row = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name,
             best_email, best_phone, date_of_birth)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (stable_fingerprint) DO UPDATE
          SET normalized_name = EXCLUDED.normalized_name
        RETURNING id
        """,
        (fp, normalized_name, normalized_name, best_email, best_phone, date_of_birth),
    ).fetchone()
    return str(row[0])


def _insert_candidate_yacht(
    conn: psycopg.Connection,
    *,
    normalized_name: str | None = "sea-legs",
    normalized_sail_number: str | None = "usa-1234",
    yacht_type: str | None = "J/24",
    length_feet=None,
) -> str:
    fp = _fp(normalized_name or "", normalized_sail_number or "")
    row = conn.execute(
        """
        INSERT INTO candidate_yacht
            (stable_fingerprint, name, normalized_name,
             sail_number, normalized_sail_number, yacht_type, length_feet)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stable_fingerprint) DO UPDATE
          SET normalized_name = EXCLUDED.normalized_name
        RETURNING id
        """,
        (fp, normalized_name, normalized_name,
         normalized_sail_number, normalized_sail_number,
         yacht_type, length_feet),
    ).fetchone()
    return str(row[0])


def _insert_candidate_club(
    conn: psycopg.Connection,
    *,
    normalized_name: str = "bhyc",
    website: str | None = "https://bhyc.org",
    state_usa: str | None = "ME",
    phone: str | None = None,
) -> str:
    fp = _fp(normalized_name)
    row = conn.execute(
        """
        INSERT INTO candidate_club
            (stable_fingerprint, name, normalized_name, website, state_usa, phone)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (stable_fingerprint) DO UPDATE
          SET normalized_name = EXCLUDED.normalized_name
        RETURNING id
        """,
        (fp, normalized_name, normalized_name, website, state_usa, phone),
    ).fetchone()
    return str(row[0])


def _insert_candidate_event(
    conn: psycopg.Connection,
    *,
    normalized_event_name: str = "bhyc-regatta",
    season_year: int = 2024,
    event_external_id: str | None = "race-537",
    start_date: str | None = "2024-07-04",
    end_date: str | None = None,
) -> str:
    fp = _fp(normalized_event_name, str(season_year), event_external_id or "")
    row = conn.execute(
        """
        INSERT INTO candidate_event
            (stable_fingerprint, event_name, normalized_event_name,
             season_year, event_external_id, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stable_fingerprint) DO UPDATE
          SET normalized_event_name = EXCLUDED.normalized_event_name
        RETURNING id
        """,
        (fp, normalized_event_name, normalized_event_name,
         season_year, event_external_id, start_date, end_date),
    ).fetchone()
    return str(row[0])


def _insert_candidate_registration(
    conn: psycopg.Connection,
    candidate_event_id: str,
    *,
    candidate_yacht_id: str | None = None,
    candidate_primary_participant_id: str | None = None,
    registration_external_id: str | None = "sku-001",
) -> str:
    fp = _fp(candidate_event_id, registration_external_id or "", candidate_yacht_id or "")
    row = conn.execute(
        """
        INSERT INTO candidate_registration
            (stable_fingerprint, registration_external_id,
             candidate_event_id, candidate_yacht_id,
             candidate_primary_participant_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (stable_fingerprint) DO UPDATE
          SET registration_external_id = EXCLUDED.registration_external_id
        RETURNING id
        """,
        (fp, registration_external_id, candidate_event_id,
         candidate_yacht_id, candidate_primary_participant_id),
    ).fetchone()
    return str(row[0])


# ---------------------------------------------------------------------------
# Scoring tests
# ---------------------------------------------------------------------------

class TestCandidateScoring:
    def test_fully_attributed_participant_scores_auto_promote(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        ctrs = run_score(conn, entity_type="participant")
        assert ctrs.db_errors == 0
        assert ctrs.candidates_scored >= 1
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        # email(0.55) + phone(0.20) + dob(0.15) + name(0.10) = 1.00 → auto_promote
        assert float(row[0]) == pytest.approx(1.0, abs=0.001)
        assert row[1] == "auto_promote"

    def test_name_only_participant_scores_hold_or_reject(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(
            conn,
            best_email=None,
            best_phone=None,
            date_of_birth=None,
        )
        ctrs = run_score(conn, entity_type="participant")
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        # name(0.10) - missing_email(0.10) - missing_phone(0.05) = -0.05 → clamp to 0 → reject
        assert float(row[0]) == 0.0
        assert row[1] == "reject"

    def test_fully_attributed_yacht_scores_auto_promote(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_yacht(conn, yacht_type="J/24", length_feet=24.5)
        ctrs = run_score(conn, entity_type="yacht")
        assert ctrs.db_errors == 0
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_yacht WHERE id = %s",
            (cid,),
        ).fetchone()
        # sail(0.50)+name(0.30)+type(0.10)+len(0.10) = 1.00 → auto_promote
        assert float(row[0]) == pytest.approx(1.0, abs=0.001)
        assert row[1] == "auto_promote"

    def test_yacht_without_sail_scores_review_after_penalty(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_yacht(conn, normalized_sail_number=None)
        ctrs = run_score(conn, entity_type="yacht")
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_yacht WHERE id = %s",
            (cid,),
        ).fetchone()
        # name(0.30)+type(0.10) - missing_sail(0.15) - missing_name_penalty NOT applied (name present)
        # = 0.40 - 0.15 = 0.25 → hold or reject (below 0.50)
        assert float(row[0]) < 0.50

    def test_fully_attributed_club_scores_auto_promote(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_club(conn, phone="+12075551234")
        ctrs = run_score(conn, entity_type="club")
        assert ctrs.db_errors == 0
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_club WHERE id = %s",
            (cid,),
        ).fetchone()
        # name(0.50)+website(0.25)+state(0.15)+phone(0.10) = 1.00 → auto_promote
        assert float(row[0]) == pytest.approx(1.0, abs=0.001)
        assert row[1] == "auto_promote"

    def test_fully_attributed_event_scores_auto_promote(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_event(conn)
        ctrs = run_score(conn, entity_type="event")
        assert ctrs.db_errors == 0
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_event WHERE id = %s",
            (cid,),
        ).fetchone()
        # ext_id(0.40)+year(0.25)+name(0.25)+dates(0.10) = 1.00 → auto_promote
        assert float(row[0]) == pytest.approx(1.0, abs=0.001)
        assert row[1] == "auto_promote"

    def test_fully_attributed_registration_scores_auto_promote(self, db_conn):
        conn, dsn = db_conn
        event_id  = _insert_candidate_event(conn)
        yacht_id  = _insert_candidate_yacht(conn)
        part_id   = _insert_candidate_participant(conn)
        cid = _insert_candidate_registration(
            conn, event_id,
            candidate_yacht_id=yacht_id,
            candidate_primary_participant_id=part_id,
        )
        ctrs = run_score(conn, entity_type="registration")
        assert ctrs.db_errors == 0
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_registration WHERE id = %s",
            (cid,),
        ).fetchone()
        # ext_id(0.40)+event(0.25)+yacht(0.25)+part(0.10) = 1.00 → auto_promote
        assert float(row[0]) == pytest.approx(1.0, abs=0.001)
        assert row[1] == "auto_promote"

    def test_score_run_sets_last_score_run_id(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        run_score(conn, entity_type="participant")
        row = conn.execute(
            "SELECT last_score_run_id FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        assert row[0] is not None, "last_score_run_id should be set after scoring"

    def test_run_all_scores_all_entity_types(self, db_conn):
        conn, dsn = db_conn
        _insert_candidate_participant(conn)
        _insert_candidate_yacht(conn)
        _insert_candidate_club(conn)
        event_id = _insert_candidate_event(conn)
        _insert_candidate_registration(conn, event_id)
        ctrs = run_score(conn, entity_type="all")
        assert ctrs.db_errors == 0
        assert ctrs.candidates_scored >= 5

    def test_dry_run_does_not_persist_scores(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        conn.commit()  # persist insert before testing rollback behavior
        run_score(conn, entity_type="participant", dry_run=True)
        conn.rollback()
        # After rollback the score should still be default 0
        row = conn.execute(
            "SELECT quality_score FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()
        assert float(row[0]) == 0.0

    def test_scoring_is_idempotent(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        run_score(conn, entity_type="participant")
        score1 = conn.execute(
            "SELECT quality_score FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        run_score(conn, entity_type="participant")
        score2 = conn.execute(
            "SELECT quality_score FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert float(score1) == float(score2)

    def test_nbas_written_for_review_candidate(self, db_conn):
        conn, dsn = db_conn
        # name-only participant: score < auto_promote threshold → gets NBAs
        cid = _insert_candidate_participant(
            conn,
            best_email=None,
            best_phone=None,
            date_of_birth=None,
        )
        ctrs = run_score(conn, entity_type="participant")
        assert ctrs.nbas_written >= 1
        count = conn.execute(
            """
            SELECT COUNT(*) FROM next_best_action
            WHERE target_entity_type = 'candidate_participant'
              AND target_entity_id = %s
              AND status = 'open'
              AND action_type = 'enrich_candidate'
            """,
            (cid,),
        ).fetchone()[0]
        assert count >= 1

    def test_nbas_not_written_for_auto_promote_candidate(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        run_score(conn, entity_type="participant")
        count = conn.execute(
            """
            SELECT COUNT(*) FROM next_best_action
            WHERE target_entity_type = 'candidate_participant'
              AND target_entity_id = %s
            """,
            (cid,),
        ).fetchone()[0]
        assert count == 0

    def test_nbas_replaced_on_rescore(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn, best_email=None, best_phone=None, date_of_birth=None)
        run_score(conn, entity_type="participant")
        count1 = conn.execute(
            "SELECT COUNT(*) FROM next_best_action WHERE target_entity_id = %s AND status='open'",
            (cid,),
        ).fetchone()[0]
        run_score(conn, entity_type="participant")
        count2 = conn.execute(
            "SELECT COUNT(*) FROM next_best_action WHERE target_entity_id = %s AND status='open'",
            (cid,),
        ).fetchone()[0]
        # Re-score should delete and re-insert the same set — count stays the same
        assert count2 == count1

    def test_rescore_does_not_downgrade_promoted_candidate(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        run_score(conn, entity_type="participant")
        run_promote(conn, entity_type="participant")
        # Now run score again — promoted candidate's resolution_state must stay auto_promote
        run_score(conn, entity_type="participant")
        row = conn.execute(
            "SELECT resolution_state, is_promoted FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        assert row[0] == "auto_promote"
        assert row[1] is True


# ---------------------------------------------------------------------------
# Promotion tests
# ---------------------------------------------------------------------------

class TestCandidatePromotion:
    def _score_and_promote(self, conn, entity_type="all"):
        run_score(conn, entity_type=entity_type)
        return run_promote(conn, entity_type=entity_type)

    def test_auto_promote_participant_creates_canonical_row(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        ctrs = self._score_and_promote(conn, "participant")
        assert ctrs.db_errors == 0
        assert ctrs.candidates_promoted >= 1
        # Check candidate updated
        row = conn.execute(
            "SELECT is_promoted, promoted_canonical_id FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        assert row[0] is True
        assert row[1] is not None
        canonical_id = str(row[1])
        # Check canonical row exists
        row2 = conn.execute(
            "SELECT best_email, normalized_name FROM canonical_participant WHERE id = %s",
            (canonical_id,),
        ).fetchone()
        assert row2 is not None
        assert row2[0] == "john@example.com"

    def test_candidate_canonical_link_created(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        self._score_and_promote(conn, "participant")
        link = conn.execute(
            """
            SELECT canonical_entity_id, promotion_mode
            FROM candidate_canonical_link
            WHERE candidate_entity_type = 'participant' AND candidate_entity_id = %s
            """,
            (cid,),
        ).fetchone()
        assert link is not None
        assert link[1] == "auto"

    def test_audit_log_entry_created(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        self._score_and_promote(conn, "participant")
        log = conn.execute(
            """
            SELECT action_type, source, actor
            FROM resolution_manual_action_log
            WHERE entity_type = 'participant' AND candidate_entity_id = %s
            """,
            (cid,),
        ).fetchone()
        assert log is not None
        assert log[0] == "promote"
        assert log[1] == "pipeline"
        assert log[2] == "pipeline"

    def test_review_candidate_not_promoted(self, db_conn):
        conn, dsn = db_conn
        # name-only participant: scores 0 → reject (not auto_promote)
        cid = _insert_candidate_participant(conn, best_email=None, best_phone=None, date_of_birth=None)
        self._score_and_promote(conn, "participant")
        row = conn.execute(
            "SELECT is_promoted FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()
        assert row[0] is False

    def test_promotion_idempotent_on_rerun(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        self._score_and_promote(conn, "participant")
        ctrs2 = run_promote(conn, entity_type="participant")
        # Second run: candidate already is_promoted=True, skipped entirely
        assert ctrs2.candidates_promoted == 0
        # Only one canonical_participant row should exist for this candidate
        count = conn.execute(
            """
            SELECT COUNT(*) FROM canonical_participant cp
            JOIN candidate_canonical_link ccl
              ON ccl.canonical_entity_id = cp.id
              AND ccl.candidate_entity_type = 'participant'
              AND ccl.candidate_entity_id = %s
            """,
            (cid,),
        ).fetchone()[0]
        assert count == 1

    def test_registration_not_promoted_when_event_not_promoted(self, db_conn):
        conn, dsn = db_conn
        event_id = _insert_candidate_event(conn)
        yacht_id  = _insert_candidate_yacht(conn, yacht_type="J/24", length_feet=24.5)
        part_id   = _insert_candidate_participant(conn)
        reg_id = _insert_candidate_registration(
            conn, event_id,
            candidate_yacht_id=yacht_id,
            candidate_primary_participant_id=part_id,
        )
        # Score all entities so the registration reaches auto_promote (1.0)
        run_score(conn, entity_type="all")
        # Promote yacht and participant but NOT event
        run_promote(conn, entity_type="yacht")
        run_promote(conn, entity_type="participant")
        ctrs = run_promote(conn, entity_type="registration")
        # Registration has auto_promote but event not yet promoted → skipped
        assert ctrs.candidates_skipped_missing_dep >= 1
        row = conn.execute(
            "SELECT is_promoted FROM candidate_registration WHERE id = %s", (reg_id,)
        ).fetchone()
        assert row[0] is False

    def test_full_pipeline_promotes_registration_after_deps_promoted(self, db_conn):
        conn, dsn = db_conn
        event_id = _insert_candidate_event(conn)
        yacht_id  = _insert_candidate_yacht(conn, yacht_type="J/24", length_feet=24.5)
        part_id   = _insert_candidate_participant(conn)
        reg_id = _insert_candidate_registration(
            conn, event_id,
            candidate_yacht_id=yacht_id,
            candidate_primary_participant_id=part_id,
        )
        # Run full pipeline
        run_score(conn, entity_type="all")
        ctrs = run_promote(conn, entity_type="all")
        assert ctrs.db_errors == 0
        assert ctrs.candidates_promoted >= 4  # event + yacht + participant + registration
        row = conn.execute(
            "SELECT is_promoted FROM candidate_registration WHERE id = %s", (reg_id,)
        ).fetchone()
        assert row[0] is True

    def test_canonical_registration_has_canonical_fk(self, db_conn):
        conn, dsn = db_conn
        event_id = _insert_candidate_event(conn)
        yacht_id  = _insert_candidate_yacht(conn, yacht_type="J/24", length_feet=24.5)
        part_id   = _insert_candidate_participant(conn)
        reg_id = _insert_candidate_registration(
            conn, event_id,
            candidate_yacht_id=yacht_id,
            candidate_primary_participant_id=part_id,
        )
        run_score(conn, entity_type="all")
        run_promote(conn, entity_type="all")
        # Get canonical_registration
        can_reg = conn.execute(
            """
            SELECT cr.canonical_event_id, cr.canonical_yacht_id,
                   cr.canonical_primary_participant_id
            FROM canonical_registration cr
            JOIN candidate_canonical_link ccl
              ON ccl.canonical_entity_id = cr.id
              AND ccl.candidate_entity_type = 'registration'
              AND ccl.candidate_entity_id = %s
            """,
            (reg_id,),
        ).fetchone()
        assert can_reg is not None
        assert can_reg[0] is not None  # canonical_event_id set
        assert can_reg[1] is not None  # canonical_yacht_id set
        assert can_reg[2] is not None  # canonical_primary_participant_id set

    def test_promote_dry_run_does_not_persist(self, db_conn):
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        run_score(conn, entity_type="participant")
        conn.commit()  # persist insert + score before testing rollback behavior
        run_promote(conn, entity_type="participant", dry_run=True)
        conn.rollback()
        row = conn.execute(
            "SELECT is_promoted FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()
        assert row[0] is False


# ---------------------------------------------------------------------------
# Staged-transition safety tests (§6.1) — reject → auto_promote without abort
# ---------------------------------------------------------------------------

class TestStagedTransitionSafety:
    """Verify that the scoring engine stages reject→review→auto_promote rather
    than attempting a single forbidden direct transition that would abort the
    whole transaction via the DB trigger enforce_candidate_state_transition().
    """

    def test_reject_to_auto_promote_staged_no_abort(self, db_conn):
        """A fully-attributed participant forcibly set to 'reject' must reach
        'auto_promote' after rescoring without any db_errors or transaction
        abort, and state_transitions_staged must be incremented.
        """
        conn, dsn = db_conn
        # Insert a fully-attributed participant (email+phone+dob+name → score 1.0)
        cid = _insert_candidate_participant(conn)
        # Default resolution_state is 'hold'; going to 'reject' is trigger-safe
        conn.execute(
            "UPDATE candidate_participant SET resolution_state = 'reject' WHERE id = %s",
            (cid,),
        )
        # Verify the forced state
        state_before = conn.execute(
            "SELECT resolution_state FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert state_before == "reject"

        # Run scoring — must not abort even though it targets 'auto_promote' from 'reject'
        ctrs = run_score(conn, entity_type="participant")
        assert ctrs.db_errors == 0, f"Expected no db_errors; got {ctrs.db_errors}: {ctrs.warnings}"
        assert ctrs.state_transitions_staged >= 1

        row = conn.execute(
            "SELECT resolution_state, quality_score FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        assert row[0] == "auto_promote"
        assert float(row[1]) == pytest.approx(1.0, abs=0.001)

    def test_non_reject_to_auto_promote_no_staging(self, db_conn):
        """A candidate starting from 'hold' or 'review' must reach 'auto_promote'
        without incrementing state_transitions_staged (no staging needed).
        """
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        # Default state is 'hold'; scoring should go directly to 'auto_promote'
        ctrs = run_score(conn, entity_type="participant")
        assert ctrs.db_errors == 0
        assert ctrs.state_transitions_staged == 0

        row = conn.execute(
            "SELECT resolution_state FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()
        assert row[0] == "auto_promote"

    def test_reject_staging_does_not_leave_candidate_in_review(self, db_conn):
        """After staged transition, final state must be 'auto_promote', not 'review'."""
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        conn.execute(
            "UPDATE candidate_participant SET resolution_state = 'reject' WHERE id = %s",
            (cid,),
        )
        run_score(conn, entity_type="participant")
        row = conn.execute(
            "SELECT resolution_state FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()
        assert row[0] == "auto_promote", "Staged transition must complete fully to auto_promote"

    def test_staged_candidate_promotes_to_canonical(self, db_conn):
        """A candidate that needed a staged transition must still be promotable."""
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn)
        conn.execute(
            "UPDATE candidate_participant SET resolution_state = 'reject' WHERE id = %s",
            (cid,),
        )
        run_score(conn, entity_type="participant")
        ctrs = run_promote(conn, entity_type="participant")
        assert ctrs.db_errors == 0
        assert ctrs.candidates_promoted >= 1
        row = conn.execute(
            "SELECT is_promoted FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()
        assert row[0] is True


# ---------------------------------------------------------------------------
# v1.1.0 YAML policy promotion tests (§6.2 + §5)
# Test boundary cases: name-only club, email+phone participant, sail+name yacht
# ---------------------------------------------------------------------------

class TestV110PolicyPromotion:
    """Verify that the updated v1.1.0 YAML thresholds enable auto_promote and
    canonical row creation for club, participant, and yacht.
    """

    def test_club_name_only_scores_auto_promote(self, db_conn):
        """Club with only normalized_name scores 0.45 (0.50 - 0.05 penalty)
        which meets the v1.1.0 auto_promote threshold of 0.45.
        """
        conn, dsn = db_conn
        # Insert club with name only (no website, state, phone)
        cid = _insert_candidate_club(conn, website=None, state_usa=None, phone=None)
        ctrs = run_score(conn, entity_type="club")
        assert ctrs.db_errors == 0
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_club WHERE id = %s",
            (cid,),
        ).fetchone()
        # name(0.50) - missing_website(0.05) = 0.45 → auto_promote at v1.1.0 threshold
        assert float(row[0]) == pytest.approx(0.45, abs=0.001)
        assert row[1] == "auto_promote"

    def test_club_name_only_promotes_to_canonical(self, db_conn):
        """Name-only club must create a canonical_club row under v1.1.0."""
        conn, dsn = db_conn
        cid = _insert_candidate_club(conn, website=None, state_usa=None, phone=None)
        run_score(conn, entity_type="club")
        ctrs = run_promote(conn, entity_type="club")
        assert ctrs.db_errors == 0
        assert ctrs.candidates_promoted >= 1
        row = conn.execute(
            "SELECT is_promoted, promoted_canonical_id FROM candidate_club WHERE id = %s",
            (cid,),
        ).fetchone()
        assert row[0] is True
        assert row[1] is not None
        # Canonical row exists
        can = conn.execute(
            "SELECT normalized_name FROM canonical_club WHERE id = %s",
            (str(row[1]),),
        ).fetchone()
        assert can is not None
        assert can[0] == "bhyc"

    def test_participant_email_phone_scores_auto_promote(self, db_conn):
        """Participant with email+phone+name (no DOB) scores 0.85 → auto_promote at v1.1.0."""
        conn, dsn = db_conn
        # Helper default includes normalized_name="john-doe", so we get name(0.10) too
        cid = _insert_candidate_participant(conn, date_of_birth=None)
        ctrs = run_score(conn, entity_type="participant")
        assert ctrs.db_errors == 0
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        # email(0.55) + phone(0.20) + name(0.10) = 0.85 → auto_promote at v1.1.0 threshold (0.75)
        assert float(row[0]) == pytest.approx(0.85, abs=0.001)
        assert row[1] == "auto_promote"

    def test_participant_email_phone_promotes_to_canonical(self, db_conn):
        """email+phone participant must create a canonical_participant row."""
        conn, dsn = db_conn
        cid = _insert_candidate_participant(conn, date_of_birth=None)
        run_score(conn, entity_type="participant")
        ctrs = run_promote(conn, entity_type="participant")
        assert ctrs.db_errors == 0
        assert ctrs.candidates_promoted >= 1
        row = conn.execute(
            "SELECT is_promoted, promoted_canonical_id FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        assert row[0] is True
        can = conn.execute(
            "SELECT best_email FROM canonical_participant WHERE id = %s",
            (str(row[1]),),
        ).fetchone()
        assert can is not None
        assert can[0] == "john@example.com"

    def test_yacht_sail_name_scores_auto_promote(self, db_conn):
        """Yacht with sail number + name (no type/length) scores 0.80 → auto_promote at v1.1.0."""
        conn, dsn = db_conn
        # No yacht_type, no length_feet
        cid = _insert_candidate_yacht(conn, yacht_type=None, length_feet=None)
        ctrs = run_score(conn, entity_type="yacht")
        assert ctrs.db_errors == 0
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_yacht WHERE id = %s",
            (cid,),
        ).fetchone()
        # sail(0.50) + name(0.30) = 0.80, no penalties (sail+name both present)
        assert float(row[0]) == pytest.approx(0.80, abs=0.001)
        assert row[1] == "auto_promote"

    def test_yacht_sail_name_promotes_to_canonical(self, db_conn):
        """sail+name yacht must create a canonical_yacht row."""
        conn, dsn = db_conn
        cid = _insert_candidate_yacht(conn, yacht_type=None, length_feet=None)
        run_score(conn, entity_type="yacht")
        ctrs = run_promote(conn, entity_type="yacht")
        assert ctrs.db_errors == 0
        assert ctrs.candidates_promoted >= 1
        row = conn.execute(
            "SELECT is_promoted, promoted_canonical_id FROM candidate_yacht WHERE id = %s",
            (cid,),
        ).fetchone()
        assert row[0] is True
        can = conn.execute(
            "SELECT normalized_name, normalized_sail_number FROM canonical_yacht WHERE id = %s",
            (str(row[1]),),
        ).fetchone()
        assert can is not None
        assert can[0] == "sea-legs"
        assert can[1] == "usa-1234"

    def test_new_auto_promote_counters_populated(self, db_conn):
        """new_auto_promote_* counters must reflect candidates newly entering auto_promote."""
        conn, dsn = db_conn
        _insert_candidate_club(conn, website=None, state_usa=None, phone=None)
        _insert_candidate_participant(conn, date_of_birth=None)
        _insert_candidate_yacht(conn, yacht_type=None, length_feet=None)
        ctrs = run_score(conn, entity_type="all")
        assert ctrs.db_errors == 0
        assert ctrs.new_auto_promote_club >= 1
        assert ctrs.new_auto_promote_participant >= 1
        assert ctrs.new_auto_promote_yacht >= 1

    def test_all_three_entity_types_promote_end_to_end(self, db_conn):
        """Full end-to-end: score + promote all three previously-blocked entity types."""
        conn, dsn = db_conn
        _insert_candidate_club(conn, website=None, state_usa=None, phone=None)
        _insert_candidate_participant(conn, date_of_birth=None)
        _insert_candidate_yacht(conn, yacht_type=None, length_feet=None)

        score_ctrs = run_score(conn, entity_type="all")
        assert score_ctrs.db_errors == 0

        promote_ctrs = run_promote(conn, entity_type="all")
        assert promote_ctrs.db_errors == 0
        assert promote_ctrs.candidates_promoted >= 3

        can_club = conn.execute("SELECT COUNT(*) FROM canonical_club").fetchone()[0]
        can_part = conn.execute("SELECT COUNT(*) FROM canonical_participant").fetchone()[0]
        can_yacht = conn.execute("SELECT COUNT(*) FROM canonical_yacht").fetchone()[0]
        assert can_club >= 1
        assert can_part >= 1
        assert can_yacht >= 1

    def test_participant_email_only_stays_review(self, db_conn):
        """Participant with email+name (no phone, no DOB) stays 'review', not auto_promote."""
        conn, dsn = db_conn
        # Helper default includes normalized_name="john-doe"
        cid = _insert_candidate_participant(conn, best_phone=None, date_of_birth=None)
        run_score(conn, entity_type="participant")
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        # email(0.55) + name(0.10) - missing_phone(0.05) = 0.60 → review at v1.1.0 thresholds
        assert float(row[0]) == pytest.approx(0.60, abs=0.001)
        assert row[1] == "review"

    def test_yacht_sail_only_stays_hold(self, db_conn):
        """Yacht with sail only (score 0.30 after missing_name penalty) stays 'hold'."""
        conn, dsn = db_conn
        cid = _insert_candidate_yacht(conn, normalized_name=None, yacht_type=None, length_feet=None)
        run_score(conn, entity_type="yacht")
        row = conn.execute(
            "SELECT quality_score, resolution_state FROM candidate_yacht WHERE id = %s",
            (cid,),
        ).fetchone()
        # sail(0.50) - missing_name(0.20) = 0.30 → hold at v1.1.0 thresholds
        assert float(row[0]) == pytest.approx(0.30, abs=0.001)
        assert row[1] == "hold"
