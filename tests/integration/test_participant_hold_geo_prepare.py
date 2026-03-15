"""Integration tests for participant_hold_geo_prepare pipeline.

Tests verify:
  - Address variants collapse to the same geo hint (deduplication).
  - Country-only (USA) → too_vague by default (no flag).
  - Junk address (Laser) → junk_location.
  - Conflicting city hints → multi_location_conflict.
  - Candidate with email → already_has_contact.
  - Candidate with good address → enrichment_ready.
  - Idempotency: double-run does not duplicate rows.
  - RocketReach filtered selection with require_geo_ready=True.
  - Raw candidate_participant_address rows are unchanged after run.

Requires a live PostgreSQL instance (via pytest-postgresql).
"""

from __future__ import annotations

import uuid

import psycopg
import pytest

from regatta_etl.import_rocketreach_enrichment import (
    RocketReachEnrichmentCounters,
    _select_candidates,
)
from regatta_etl.participant_hold_geo_prepare import (
    HoldGeoPrepareCounters,
    run_hold_geo_prepare,
)


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _make_uuid() -> str:
    return str(uuid.uuid4())


def _seed_candidate(
    conn: psycopg.Connection,
    *,
    resolution_state: str = "hold",
    normalized_name: str = "john smith",
    display_name: str = "John Smith",
    best_email: str | None = None,
    best_phone: str | None = None,
) -> str:
    """Insert a candidate_participant row and return its UUID string."""
    cid = _make_uuid()
    fingerprint = f"fp-{cid}"
    conn.execute(
        """
        INSERT INTO candidate_participant
            (id, stable_fingerprint, normalized_name, display_name,
             best_email, best_phone, resolution_state, is_promoted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, false)
        """,
        (cid, fingerprint, normalized_name, display_name,
         best_email, best_phone, resolution_state),
    )
    return cid


def _seed_address(
    conn: psycopg.Connection,
    cid: str,
    address_raw: str,
    source_table: str = "participant",
    source_pk: str | None = None,
) -> str:
    """Insert a candidate_participant_address row, return its UUID."""
    aid = _make_uuid()
    conn.execute(
        """
        INSERT INTO candidate_participant_address
            (id, candidate_participant_id, address_raw,
             source_table_name, source_row_pk)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (aid, cid, address_raw, source_table, source_pk or _make_uuid()),
    )
    return aid


def _seed_email_contact(conn: psycopg.Connection, cid: str, email: str) -> None:
    conn.execute(
        """
        INSERT INTO candidate_participant_contact
            (candidate_participant_id, contact_type, contact_subtype,
             raw_value, normalized_value, is_primary,
             source_table_name, source_row_pk)
        VALUES (%s, 'email', NULL, %s, %s, false, 'test', %s)
        ON CONFLICT (candidate_participant_id, contact_type, normalized_value)
            WHERE normalized_value IS NOT NULL
        DO NOTHING
        """,
        (cid, email, email, _make_uuid()),
    )


# ---------------------------------------------------------------------------
# Test: address variants collapse (deduplication)
# ---------------------------------------------------------------------------

class TestAddressVariantsCollapse:
    def test_variant_addresses_same_normalized_hint(self, db_conn):
        """Two address strings that normalize to the same hint produce one geo_hint row
        with is_best_hint=true (unique index on candidate_participant_id, address_raw
        is per-raw-string, but normalized_hint should match)."""
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "Portland ME USA")
        _seed_address(conn, cid, "Portland, ME, USA")  # comma variant
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])

        assert ctrs.hold_candidates_considered == 1
        assert ctrs.hold_candidates_with_address == 1
        assert ctrs.geo_hints_normalized == 2
        assert ctrs.geo_hints_deduplicated >= 1  # second is a dup of first

        # Both hint rows exist in DB (raw_address is the unique key)
        rows = conn.execute(
            "SELECT address_raw, normalized_hint, is_best_hint "
            "FROM candidate_participant_geo_hint WHERE candidate_participant_id = %s "
            "ORDER BY address_raw",
            (cid,),
        ).fetchall()
        assert len(rows) == 2
        # Both should share the same normalized_hint
        hints = {r[1] for r in rows}
        assert len(hints) == 1

        # Exactly one is_best_hint=true
        best_count = sum(1 for r in rows if r[2])
        assert best_count == 1

        # Readiness should be enrichment_ready (no email/phone, good address)
        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness is not None
        assert readiness[0] == "enrichment_ready"


# ---------------------------------------------------------------------------
# Test: country-only → too_vague
# ---------------------------------------------------------------------------

class TestCountryOnlyTooVague:
    def test_usa_only_address_is_too_vague(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "USA")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])

        assert ctrs.hold_candidates_considered == 1
        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness[0] == "too_vague"

    def test_usa_only_enrichment_ready_with_flag(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "USA")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"], include_country_only=True)

        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness[0] == "enrichment_ready"


# ---------------------------------------------------------------------------
# Test: junk address → junk_location
# ---------------------------------------------------------------------------

class TestJunkAddressJunkLocation:
    def test_laser_address_is_junk_location(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "Laser")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])

        assert ctrs.geo_candidates_junk_location == 1
        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness[0] == "junk_location"

    def test_yacht_club_is_junk_location(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "Boothbay Harbor Yacht Club")
        conn.commit()

        run_hold_geo_prepare(conn, states=["hold"])

        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness[0] == "junk_location"


# ---------------------------------------------------------------------------
# Test: conflicting city hints → multi_location_conflict
# ---------------------------------------------------------------------------

class TestMultiLocationConflict:
    def test_two_distinct_cities_produce_conflict(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "Portland ME USA")
        _seed_address(conn, cid, "Boston MA USA")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])

        assert ctrs.geo_candidates_multi_location_conflict == 1
        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness[0] == "multi_location_conflict"


# ---------------------------------------------------------------------------
# Test: candidate with email → already_has_contact
# ---------------------------------------------------------------------------

class TestAlreadyHasContact:
    def test_top_level_email_produces_already_has_contact(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn, best_email="john@example.com")
        _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])

        assert ctrs.geo_candidates_already_has_contact == 1
        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness[0] == "already_has_contact"

    def test_child_email_contact_produces_already_has_contact(self, db_conn):
        conn, _ = db_conn
        # No top-level email, but child contact row has one
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "Portland ME USA")
        _seed_email_contact(conn, cid, "child@example.com")
        conn.commit()

        run_hold_geo_prepare(conn, states=["hold"])

        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness[0] == "already_has_contact"


# ---------------------------------------------------------------------------
# Test: enrichment_ready candidate
# ---------------------------------------------------------------------------

class TestEnrichmentReady:
    def test_good_city_state_country_is_enrichment_ready(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])

        assert ctrs.geo_candidates_ready == 1

        hint_row = conn.execute(
            "SELECT city, state_region, country_code, is_best_hint "
            "FROM candidate_participant_geo_hint "
            "WHERE candidate_participant_id = %s AND is_best_hint = true",
            (cid,),
        ).fetchone()
        assert hint_row is not None
        assert hint_row[0] == "Portland"
        assert hint_row[1] == "ME"
        assert hint_row[2] == "USA"

    def test_not_hold_state_is_excluded(self, db_conn):
        conn, _ = db_conn
        # Candidate in 'reject' state should not be processed (default: only 'hold')
        cid = _seed_candidate(conn, resolution_state="reject")
        _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])

        assert ctrs.hold_candidates_considered == 0
        result = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert result[0] == 0

    def test_review_state_candidate_is_excluded(self, db_conn):
        conn, _ = db_conn
        # Candidate in 'review' state should not be processed (default: only 'hold')
        cid = _seed_candidate(conn, resolution_state="review")
        _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])

        assert ctrs.hold_candidates_considered == 0


# ---------------------------------------------------------------------------
# Test: idempotency
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_double_run_does_not_duplicate_rows(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        hint_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_geo_hint "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert hint_count == 1  # same raw address, upserted

        readiness_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert readiness_count == 1

    def test_updated_readiness_on_second_run(self, db_conn):
        """If an address changes status between runs, readiness is updated."""
        conn, _ = db_conn
        cid = _seed_candidate(conn, best_email="john@example.com")
        _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        # First run: has email → already_has_contact
        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        readiness_1 = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert readiness_1 == "already_has_contact"

        # Clear the email to simulate it being removed
        conn.execute(
            "UPDATE candidate_participant SET best_email = NULL WHERE id = %s", (cid,)
        )
        conn.commit()

        # Second run: no email → enrichment_ready
        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        readiness_2 = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert readiness_2 == "enrichment_ready"


# ---------------------------------------------------------------------------
# Test: raw address rows unchanged
# ---------------------------------------------------------------------------

class TestRawAddressUnchanged:
    def test_candidate_participant_address_rows_not_modified(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        aid = _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        # Capture raw before
        before = conn.execute(
            "SELECT address_raw, source_table_name FROM candidate_participant_address "
            "WHERE id = %s",
            (aid,),
        ).fetchone()

        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        # Capture raw after
        after = conn.execute(
            "SELECT address_raw, source_table_name FROM candidate_participant_address "
            "WHERE id = %s",
            (aid,),
        ).fetchone()

        assert before == after


# ---------------------------------------------------------------------------
# Test: RocketReach filtered selection (require_geo_ready)
# ---------------------------------------------------------------------------

class TestRocketReachGeoReadyFilter:
    def _run_rr_selection(
        self,
        conn: psycopg.Connection,
        require_geo_ready: bool,
    ) -> list[dict]:
        return _select_candidates(
            conn,
            candidate_states=["hold"],
            require_missing="none",
            cooldown_days=0,
            max_candidates=100,
            require_geo_ready=require_geo_ready,
        )

    def test_without_filter_includes_all_hold_candidates(self, db_conn):
        conn, _ = db_conn
        cid_ready = _seed_candidate(conn, normalized_name="alice jones")
        cid_vague = _seed_candidate(conn, normalized_name="bob brown")
        _seed_address(conn, cid_ready, "Portland ME USA")
        _seed_address(conn, cid_vague, "USA")
        conn.commit()

        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        candidates = self._run_rr_selection(conn, require_geo_ready=False)
        ids = {str(c["id"]) for c in candidates}
        assert cid_ready in ids
        assert cid_vague in ids

    def test_with_filter_includes_only_geo_ready(self, db_conn):
        conn, _ = db_conn
        cid_ready = _seed_candidate(conn, normalized_name="alice jones")
        cid_vague = _seed_candidate(conn, normalized_name="bob brown")
        _seed_address(conn, cid_ready, "Portland ME USA")
        _seed_address(conn, cid_vague, "USA")  # country-only → too_vague
        conn.commit()

        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        candidates = self._run_rr_selection(conn, require_geo_ready=True)
        ids = {str(c["id"]) for c in candidates}
        assert cid_ready in ids
        assert cid_vague not in ids

    def test_filter_excludes_candidate_with_no_readiness_row(self, db_conn):
        conn, _ = db_conn
        # Candidate with no address → geo_prepare never ran → no readiness row
        cid = _seed_candidate(conn)
        conn.commit()

        candidates = self._run_rr_selection(conn, require_geo_ready=True)
        ids = {str(c["id"]) for c in candidates}
        assert cid not in ids


# ---------------------------------------------------------------------------
# Test: stale readiness invalidation
# ---------------------------------------------------------------------------

class TestStaleReadinessInvalidation:
    def test_candidate_leaving_hold_gets_invalidated(self, db_conn):
        """Candidate previously enrichment_ready that moves to review state is
        invalidated to not_hold on the next run so RocketReach cannot select it."""
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        # Run 1: candidate is hold → enrichment_ready
        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        readiness_1 = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert readiness_1 == "enrichment_ready"

        # Move candidate out of hold
        conn.execute(
            "UPDATE candidate_participant SET resolution_state = 'review' WHERE id = %s",
            (cid,),
        )
        conn.commit()

        # Run 2: candidate is no longer hold → stale row should be invalidated
        ctrs = run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        assert ctrs.geo_candidates_invalidated >= 1

        readiness_2 = conn.execute(
            "SELECT readiness_status, reason_code "
            "FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert readiness_2[0] == "not_hold"
        assert readiness_2[1] == "invalidated_out_of_scope"

    def test_invalidated_candidate_excluded_from_rocketreach_filter(self, db_conn):
        """After invalidation, require_geo_ready=True must not select the candidate."""
        conn, _ = db_conn
        cid = _seed_candidate(conn, normalized_name="stale alice")
        _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        # Move out of hold
        conn.execute(
            "UPDATE candidate_participant SET resolution_state = 'review' WHERE id = %s",
            (cid,),
        )
        conn.commit()

        # Invalidation run
        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        from regatta_etl.import_rocketreach_enrichment import _select_candidates
        # RocketReach selects from 'review' state — but readiness is now not_hold
        candidates = _select_candidates(
            conn,
            candidate_states=["review"],
            require_missing="none",
            cooldown_days=0,
            max_candidates=100,
            require_geo_ready=True,
        )
        ids = {str(c["id"]) for c in candidates}
        assert cid not in ids

    def test_non_enrichment_ready_rows_not_touched_by_invalidation(self, db_conn):
        """Only enrichment_ready rows are subject to invalidation; too_vague etc. are preserved."""
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        _seed_address(conn, cid, "USA")  # → too_vague
        conn.commit()

        run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        # Move out of hold
        conn.execute(
            "UPDATE candidate_participant SET resolution_state = 'review' WHERE id = %s",
            (cid,),
        )
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"])
        conn.commit()

        # too_vague was not enrichment_ready, so invalidation counter should be 0
        assert ctrs.geo_candidates_invalidated == 0

        readiness = conn.execute(
            "SELECT readiness_status FROM candidate_participant_enrichment_readiness "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert readiness == "too_vague"  # unchanged


# ---------------------------------------------------------------------------
# Test: max_candidates limit
# ---------------------------------------------------------------------------

class TestMaxCandidatesLimit:
    def test_max_candidates_limits_processing(self, db_conn):
        conn, _ = db_conn
        for i in range(5):
            cid = _seed_candidate(conn, normalized_name=f"person {i}")
            _seed_address(conn, cid, "Portland ME USA")
        conn.commit()

        ctrs = run_hold_geo_prepare(conn, states=["hold"], max_candidates=2)

        assert ctrs.hold_candidates_considered == 2
        assert ctrs.geo_candidates_ready == 2
