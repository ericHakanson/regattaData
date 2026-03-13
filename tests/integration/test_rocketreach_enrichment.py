"""Integration tests for the RocketReach participant enrichment pipeline.

Tests verify:
  - Run and row audit tables are created on a non-dry run.
  - Dry run makes no persistent DB changes.
  - Fill-null-only enrichment applies email/phone when missing.
  - Existing non-null fields are NOT overwritten.
  - Ambiguous API response results in no field update.
  - Identity conflict blocks field application.
  - candidate_source_link is inserted for matched candidates.
  - Cooldown filter skips candidates enriched within the window.
  - No writes to canonical_participant tables.

Requires a live PostgreSQL instance (via pytest-postgresql).
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg
import pytest

from regatta_etl.import_rocketreach_enrichment import (
    LookupResult,
    RocketReachEnrichmentCounters,
    run_rocketreach_enrichment,
)


# ---------------------------------------------------------------------------
# Stub client helpers
# ---------------------------------------------------------------------------

class _StubClient:
    """Returns LookupResults from a pre-defined sequence."""

    def __init__(self, responses: list[LookupResult]) -> None:
        self._iter = iter(responses)

    def lookup(
        self,
        email: str | None,
        counters: RocketReachEnrichmentCounters,
    ) -> LookupResult:
        return next(self._iter)


def _matched(
    emails: list[str] | None = None,
    phones: list[str] | None = None,
    confidence: int = 80,
    person_id: str = "rr-42",
) -> LookupResult:
    """Build a successful 'complete' LookupResult with one profile."""
    profile: dict[str, Any] = {"confidence": confidence}
    if emails is not None:
        profile["emails"] = [{"email": e} for e in emails]
    if phones is not None:
        profile["phones"] = [{"number": p} for p in phones]
    return LookupResult(
        row_status="complete",
        profiles=[profile],
        person_id=person_id,
        credits_used=True,
        error_code=None,
    )


def _no_match() -> LookupResult:
    return LookupResult(
        row_status="complete", profiles=[], person_id=None,
        credits_used=True, error_code="rocketreach_no_match",
    )


def _ambiguous() -> LookupResult:
    return LookupResult(
        row_status="complete",
        profiles=[
            {"name": "Alice A", "confidence": 78},
            {"name": "Alice B", "confidence": 75},
        ],
        person_id="rr-99",
        credits_used=True,
        error_code=None,
    )


# ---------------------------------------------------------------------------
# DB seed helpers
# ---------------------------------------------------------------------------

def _seed_candidate(
    conn: psycopg.Connection,
    display_name: str = "Alice Smith",
    normalized_name: str | None = "alice smith",
    best_email: str | None = "alice@example.com",
    best_phone: str | None = None,
    resolution_state: str = "review",
    quality_score: float = 0.60,
) -> str:
    """Insert a candidate_participant and return its UUID string."""
    fp = f"fp-{uuid.uuid4()}"
    row = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name,
             best_email, best_phone, quality_score, resolution_state)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (fp, display_name, normalized_name,
         best_email, best_phone, quality_score, resolution_state),
    ).fetchone()
    return str(row[0])


def _run(
    conn: psycopg.Connection,
    client: _StubClient,
    *,
    candidate_states: list[str] | None = None,
    require_missing: str = "email_or_phone",
    cooldown_days: int = 30,
    max_candidates: int = 100,
    dry_run: bool = False,
) -> RocketReachEnrichmentCounters:
    counters = RocketReachEnrichmentCounters()
    run_rocketreach_enrichment(
        conn=conn,
        run_id=str(uuid.uuid4()),
        api_key="test-key",
        candidate_states=candidate_states or ["review", "hold", "reject"],
        require_missing=require_missing,
        cooldown_days=cooldown_days,
        max_candidates=max_candidates,
        source_system="rocketreach_api",
        timeout_seconds=20.0,
        max_retries=3,
        qps_limit=100.0,
        max_api_failure_rate=0.10,
        dry_run=dry_run,
        counters=counters,
        client=client,
    )
    return counters


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRunAndRowTablesCreated:
    """Non-dry-run creates audit rows in rocketreach_enrichment_run and _row."""

    def test_run_row_inserted(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client = _StubClient([_matched(emails=["alice@example.com"])])
        _run(conn, client)
        conn.commit()

        row = conn.execute(
            "SELECT status, dry_run FROM rocketreach_enrichment_run"
        ).fetchone()
        assert row is not None
        assert row[0] == "ok"
        assert row[1] is False

    def test_enrichment_row_inserted(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client = _StubClient([_matched(emails=["alice@example.com"])])
        _run(conn, client)
        conn.commit()

        row = conn.execute(
            "SELECT candidate_participant_id, status FROM rocketreach_enrichment_row"
        ).fetchone()
        assert row is not None
        assert str(row[0]) == cid
        assert row[1] == "matched"


class TestFillNullEnrichment:
    """Verify strict email policy + fill-null phone behavior."""

    def test_missing_email_is_skipped_without_api_call(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn, best_email=None)
        conn.commit()

        client = _StubClient([_matched(emails=["alice@example.com"])])
        ctrs = _run(conn, client)
        conn.commit()

        row = conn.execute(
            """
            SELECT status, error_code
            FROM rocketreach_enrichment_row
            WHERE candidate_participant_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (cid,),
        ).fetchone()
        assert row == ("skipped", "rocketreach_email_required")
        assert ctrs.rocketreach_candidates_called == 0
        assert ctrs.rocketreach_matches_applied == 0

    def test_fills_best_phone(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn, best_email="alice@example.com", best_phone=None)
        conn.commit()

        client = _StubClient([_matched(phones=["+15555550100"])])
        ctrs = _run(conn, client, require_missing="phone")
        conn.commit()

        phone = conn.execute(
            "SELECT best_phone FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert phone is not None  # normalized form stored
        assert ctrs.rocketreach_matches_applied == 1

    def test_does_not_overwrite_existing_email(self, db_conn):
        conn, _ = db_conn
        original_email = "original@example.com"
        cid = _seed_candidate(conn, best_email=original_email)
        conn.commit()

        # RocketReach returns a different email
        client = _StubClient([_matched(emails=["different@example.com"])])
        # require_missing='none' so candidate is still selected
        ctrs = _run(conn, client, require_missing="none")
        conn.commit()

        email = conn.execute(
            "SELECT best_email FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        # Original email must be preserved (fill-null-only)
        assert email == original_email


class TestDryRun:
    def test_dry_run_no_persistent_writes(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client = _StubClient([_matched(emails=["alice@example.com"])])
        ctrs = _run(conn, client, dry_run=True)
        conn.rollback()  # simulate CLI rollback on dry_run

        run_count = conn.execute(
            "SELECT COUNT(*) FROM rocketreach_enrichment_run"
        ).fetchone()[0]
        assert run_count == 0

        email = conn.execute(
            "SELECT best_email FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert email == "alice@example.com"


class TestAmbiguousNoApply:
    def test_ambiguous_result_no_field_update(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client = _StubClient([_ambiguous()])
        ctrs = _run(conn, client, require_missing="none")
        conn.commit()

        row = conn.execute(
            "SELECT status FROM rocketreach_enrichment_row WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert row[0] == "ambiguous"
        assert ctrs.rocketreach_ambiguous == 1
        assert ctrs.rocketreach_matches_applied == 0

        # No email or phone should have been written
        email = conn.execute(
            "SELECT best_email FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert email == "alice@example.com"


class TestIdentityConflictNoApply:
    def test_email_conflict_nothing_applied(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn, best_email="alice@example.com")
        conn.commit()

        # RocketReach returns a completely different email → identity conflict
        client = _StubClient([_matched(emails=["completely.different@otherdomain.org"])])
        ctrs = _run(conn, client, require_missing="none")
        conn.commit()

        row = conn.execute(
            "SELECT status, error_code, applied_field_mask "
            "FROM rocketreach_enrichment_row WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert row[0] == "matched"
        assert row[1] == "rocketreach_identity_conflict"
        assert row[2] == []  # nothing applied

        # Original email unchanged
        email = conn.execute(
            "SELECT best_email FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        assert email == "alice@example.com"


class TestSourceLinkInserted:
    def test_successful_match_inserts_source_link(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client = _StubClient([_matched(emails=["alice@example.com"], person_id="rr-777")])
        ctrs = _run(conn, client)
        conn.commit()

        link = conn.execute(
            """
            SELECT source_table_name, source_system, source_row_pk, link_reason
            FROM candidate_source_link
            WHERE candidate_entity_type = 'participant'
              AND candidate_entity_id = %s
            """,
            (cid,),
        ).fetchone()
        assert link is not None
        assert link[0] == "rocketreach_api"   # source_table_name keyed on provider (Fix 2)
        assert link[1] == "rocketreach_api"
        assert link[2] == "rr-777"            # source_row_pk = provider_person_id (Fix 2)
        assert "enrichment_row_id" in str(link[3])  # link_reason traces back to audit row
        assert ctrs.rocketreach_source_links_inserted == 1

    def test_no_match_no_source_link(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client = _StubClient([_no_match()])
        ctrs = _run(conn, client, require_missing="none")
        conn.commit()

        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_source_link WHERE candidate_entity_id = %s",
            (cid,),
        ).fetchone()[0]
        assert count == 0
        assert ctrs.rocketreach_source_links_inserted == 0


class TestCooldownSkip:
    def test_recently_enriched_candidate_skipped(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        # First run — matches
        client = _StubClient([_matched(emails=["alice@example.com"])])
        ctrs1 = _run(conn, client)
        conn.commit()
        assert ctrs1.rocketreach_candidates_considered == 1

        # Second run within cooldown — should not consider the same candidate
        client2 = _StubClient([])  # empty — would raise StopIteration if called
        ctrs2 = _run(conn, client2, cooldown_days=30)
        conn.commit()
        assert ctrs2.rocketreach_candidates_considered == 0


class TestNoCanonicalWrites:
    def test_enrichment_does_not_touch_canonical_participant(self, db_conn):
        conn, _ = db_conn
        _seed_candidate(conn)
        conn.commit()

        # canonical_participant should be empty (no promote was run)
        client = _StubClient([_matched(emails=["alice@example.com"])])
        _run(conn, client)
        conn.commit()

        count = conn.execute(
            "SELECT COUNT(*) FROM canonical_participant"
        ).fetchone()[0]
        assert count == 0


class TestContactRowsInserted:
    def test_contact_rows_created_for_enriched_emails(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client = _StubClient([_matched(emails=["alice@example.com", "a2@example.com"])])
        _run(conn, client)
        conn.commit()

        rows = conn.execute(
            """
            SELECT normalized_value FROM candidate_participant_contact
            WHERE candidate_participant_id = %s AND contact_type = 'email'
            ORDER BY normalized_value
            """,
            (cid,),
        ).fetchall()
        norms = {r[0] for r in rows}
        assert "alice@example.com" in norms

    def test_idempotent_second_run_no_duplicate_contacts(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        for _ in range(2):
            # New run_id each time, but cooldown_days=0 to allow re-enrichment
            client = _StubClient([_matched(emails=["alice@example.com"])])
            _run(conn, client, cooldown_days=0)
            conn.commit()

        count = conn.execute(
            """
            SELECT COUNT(*) FROM candidate_participant_contact
            WHERE candidate_participant_id = %s
              AND contact_type = 'email'
              AND normalized_value = 'alice@example.com'
            """,
            (cid,),
        ).fetchone()[0]
        assert count == 1  # idempotent — no duplicate


# ---------------------------------------------------------------------------
# Fix 2: source link idempotency across runs (keyed on provider_person_id)
# ---------------------------------------------------------------------------

class TestSourceLinkIdempotency:
    """Repeated runs for the same candidate+provider_person_id produce exactly
    one candidate_source_link row (ON CONFLICT DO NOTHING on provider key)."""

    def test_second_run_same_person_id_no_duplicate_link(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        # First run — inserts source link
        client1 = _StubClient([_matched(emails=["alice@example.com"], person_id="rr-42")])
        ctrs1 = _run(conn, client1, cooldown_days=0)
        conn.commit()
        assert ctrs1.rocketreach_source_links_inserted == 1

        # Second run — same provider_person_id, cooldown disabled
        client2 = _StubClient([_matched(emails=["alice@example.com"], person_id="rr-42")])
        ctrs2 = _run(conn, client2, cooldown_days=0)
        conn.commit()
        assert ctrs2.rocketreach_source_links_inserted == 0  # ON CONFLICT DO NOTHING

        # Exactly one source link row in the table
        count = conn.execute(
            """
            SELECT COUNT(*) FROM candidate_source_link
            WHERE candidate_entity_type = 'participant'
              AND candidate_entity_id = %s
            """,
            (cid,),
        ).fetchone()[0]
        assert count == 1

    def test_different_person_id_creates_new_link(self, db_conn):
        """A genuinely different provider_person_id creates a second link."""
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client1 = _StubClient([_matched(emails=["alice@example.com"], person_id="rr-1")])
        _run(conn, client1, cooldown_days=0)
        conn.commit()

        client2 = _StubClient([_matched(emails=["alice@example.com"], person_id="rr-2")])
        ctrs2 = _run(conn, client2, cooldown_days=0)
        conn.commit()
        assert ctrs2.rocketreach_source_links_inserted == 1  # genuinely new person_id

        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_source_link WHERE candidate_entity_id = %s",
            (cid,),
        ).fetchone()[0]
        assert count == 2


# ---------------------------------------------------------------------------
# Fix 3: applied_field_mask accuracy (RETURNING id, not just mapped.emails)
# ---------------------------------------------------------------------------

class TestAppliedFieldMaskAccuracy:
    """applied_field_mask only lists categories where rows were actually inserted
    (RETURNING id), not just where the API returned values."""

    def test_first_run_mask_includes_contact_emails(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        client = _StubClient([_matched(emails=["alice@example.com"], person_id="rr-1")])
        _run(conn, client, cooldown_days=0)
        conn.commit()

        mask = conn.execute(
            "SELECT applied_field_mask FROM rocketreach_enrichment_row "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert "contact_emails" in (mask or [])

    def test_second_run_all_do_nothing_mask_is_empty(self, db_conn):
        """When all contact INSERTs are DO NOTHING, mask must NOT include category."""
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        # First run inserts the contact rows
        client1 = _StubClient([_matched(emails=["alice@example.com"], person_id="rr-1")])
        _run(conn, client1, cooldown_days=0)
        conn.commit()

        # Second run — same email returned, all contact INSERTs are DO NOTHING
        client2 = _StubClient([_matched(emails=["alice@example.com"], person_id="rr-1")])
        _run(conn, client2, cooldown_days=0)
        conn.commit()

        rows = conn.execute(
            "SELECT applied_field_mask FROM rocketreach_enrichment_row "
            "WHERE candidate_participant_id = %s ORDER BY created_at",
            (cid,),
        ).fetchall()
        assert len(rows) == 2
        second_mask = rows[1][0]  # second enrichment row (chronological order)
        assert "contact_emails" not in (second_mask or [])


# ---------------------------------------------------------------------------
# Fix 5: cooldown suppresses skipped (nameless) candidates on re-run
# ---------------------------------------------------------------------------

class TestSkippedRowCooldown:
    """A skipped enrichment_row (status='skipped') must block re-selection of
    the same candidate within the cooldown window (Fix 5: removed
    'AND rer.status != skipped' from cooldown NOT EXISTS clause)."""

    def test_skipped_row_blocks_re_selection_within_cooldown(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        # First run with a client that returns skipped status
        skipped_result = LookupResult(
            row_status="skipped", profiles=[], person_id=None,
            credits_used=False, error_code=None,
        )
        client1 = _StubClient([skipped_result])
        ctrs1 = _run(conn, client1, require_missing="none")
        conn.commit()

        assert ctrs1.rocketreach_candidates_considered == 1

        # Verify a skipped row was persisted
        status = conn.execute(
            "SELECT status FROM rocketreach_enrichment_row "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert status == "skipped"

        # Second run within cooldown window — candidate must NOT be reselected
        client2 = _StubClient([])  # raises StopIteration if lookup() called
        ctrs2 = _run(conn, client2, cooldown_days=30, require_missing="none")
        conn.commit()
        assert ctrs2.rocketreach_candidates_considered == 0

    def test_zero_cooldown_allows_re_selection_after_skip(self, db_conn):
        """With cooldown_days=0 the skipped candidate is always eligible."""
        conn, _ = db_conn
        cid = _seed_candidate(conn)
        conn.commit()

        skipped_result = LookupResult(
            row_status="skipped", profiles=[], person_id=None,
            credits_used=False, error_code=None,
        )
        client1 = _StubClient([skipped_result])
        _run(conn, client1, cooldown_days=0, require_missing="none")
        conn.commit()

        # Second run with cooldown_days=0 — should still consider the candidate
        client2 = _StubClient([skipped_result])
        ctrs2 = _run(conn, client2, cooldown_days=0, require_missing="none")
        conn.commit()
        assert ctrs2.rocketreach_candidates_considered == 1


# ---------------------------------------------------------------------------
# Counter semantics: no double-counting, per-outcome (not per-retry)
# ---------------------------------------------------------------------------

class TestCounterSemantics:
    """Guards against regressions on the two counter bugs fixed in v1.0.1:
      - Issue 1: rocketreach_api_errors double-counted (client + _process_candidate).
      - Issue 2: rocketreach_rate_limited inflated by retry attempts.

    Counters are now incremented ONLY in _process_candidate (per-candidate-outcome).
    The transport client holds no counter state for these two fields.
    """

    def test_api_error_outcome_increments_exactly_once(self, db_conn):
        """A single api_error outcome must increment rocketreach_api_errors by 1."""
        conn, _ = db_conn
        _seed_candidate(conn)
        conn.commit()

        api_error = LookupResult(
            row_status="api_error", profiles=[], person_id=None,
            credits_used=True, error_code="rocketreach_api_error",
        )
        ctrs = _run(conn, _StubClient([api_error]), require_missing="none")
        conn.commit()

        assert ctrs.rocketreach_api_errors == 1, (
            "api_error outcome must increment exactly once, "
            "not be double-counted by transport client + _process_candidate"
        )
        assert ctrs.rocketreach_rate_limited == 0

    def test_rate_limited_outcome_increments_exactly_once(self, db_conn):
        """A terminal rate_limited outcome must increment rocketreach_rate_limited by 1."""
        conn, _ = db_conn
        _seed_candidate(conn)
        conn.commit()

        rl_result = LookupResult(
            row_status="rate_limited", profiles=[], person_id=None,
            credits_used=False, error_code="rocketreach_rate_limited",
        )
        ctrs = _run(conn, _StubClient([rl_result]), require_missing="none")
        conn.commit()

        assert ctrs.rocketreach_rate_limited == 1, (
            "rate_limited outcome must count per-candidate, not per-retry-attempt"
        )
        assert ctrs.rocketreach_api_errors == 0

    def test_failure_rate_bounded_by_candidates_called(self, db_conn):
        """Failure rate must never exceed 1.0, even with per-candidate-outcome counting."""
        conn, _ = db_conn
        fp_base = "fp-rate-test"
        for i in range(3):
            conn.execute(
                """
                INSERT INTO candidate_participant
                    (stable_fingerprint, display_name, normalized_name,
                     best_email, quality_score, resolution_state)
                VALUES (%s, %s, %s, %s, 0.60, 'review')
                """,
                (
                    f"{fp_base}-{i}",
                    f"Candidate {i}",
                    f"candidate {i}",
                    f"candidate{i}@example.com",
                ),
            )
        conn.commit()

        # 2 api_errors + 1 rate_limited out of 3 called
        results = [
            LookupResult(row_status="api_error", profiles=[], person_id=None,
                         credits_used=True, error_code="rocketreach_api_error"),
            LookupResult(row_status="api_error", profiles=[], person_id=None,
                         credits_used=True, error_code="rocketreach_api_error"),
            LookupResult(row_status="rate_limited", profiles=[], person_id=None,
                         credits_used=False, error_code="rocketreach_rate_limited"),
        ]
        ctrs = _run(conn, _StubClient(results), require_missing="none")
        conn.commit()

        called = ctrs.rocketreach_candidates_called
        failures = ctrs.rocketreach_api_errors + ctrs.rocketreach_rate_limited
        assert called == 3
        assert failures == 3
        assert failures / called <= 1.0  # failure rate is exactly 1.0 here, never > 1.0
