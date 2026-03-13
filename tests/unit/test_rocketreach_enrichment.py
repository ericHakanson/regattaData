"""Unit tests for regatta_etl.import_rocketreach_enrichment.

All tests are pure-Python: no database, no real HTTP calls.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock, patch

import requests
import pytest

from regatta_etl.import_rocketreach_enrichment import (
    LookupResult,
    RocketReachEnrichmentCounters,
    _MappedFields,
    _extract_profiles,
    _identity_gate,
    _map_profile,
    _missing_filter_clause,
    build_enrichment_report,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_counters() -> RocketReachEnrichmentCounters:
    return RocketReachEnrichmentCounters()


def _make_candidate(
    normalized_name: str | None = "john smith",
    best_email: str | None = "john@example.com",
    best_phone: str | None = None,
    display_name: str | None = "John Smith",
) -> dict:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "display_name": display_name,
        "normalized_name": normalized_name,
        "best_email": best_email,
        "best_phone": best_phone,
        "quality_score": 0.60,
        "resolution_state": "review",
    }


# ---------------------------------------------------------------------------
# 1. missing_filter_clause
# ---------------------------------------------------------------------------

class TestMissingFilterClause:
    def test_email(self):
        assert _missing_filter_clause("email") == "cp.best_email IS NULL"

    def test_phone(self):
        assert _missing_filter_clause("phone") == "cp.best_phone IS NULL"

    def test_email_or_phone(self):
        clause = _missing_filter_clause("email_or_phone")
        assert "best_email IS NULL" in clause
        assert "best_phone IS NULL" in clause
        assert "OR" in clause

    def test_none(self):
        assert _missing_filter_clause("none") == "TRUE"

    def test_unknown_defaults_to_true(self):
        # Any unrecognised value should default to TRUE (no filter)
        assert _missing_filter_clause("bogus") == "TRUE"


# ---------------------------------------------------------------------------
# 2. _extract_profiles
# ---------------------------------------------------------------------------

class TestExtractProfiles:
    def test_profiles_key_present(self):
        data = {"profiles": [{"name": "Alice"}, {"name": "Bob"}]}
        assert _extract_profiles(data) == [{"name": "Alice"}, {"name": "Bob"}]

    def test_top_level_name_field(self):
        data = {"name": "Alice", "status": "complete"}
        result = _extract_profiles(data)
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_empty_profiles_key(self):
        data = {"profiles": []}
        assert _extract_profiles(data) == []

    def test_no_usable_fields(self):
        data = {"status": "searching"}
        assert _extract_profiles(data) == []


# ---------------------------------------------------------------------------
# 3. _map_profile
# ---------------------------------------------------------------------------

class TestMapProfile:
    def test_email_dict_format(self):
        profile = {"emails": [{"email": "alice@example.com"}], "confidence": 85}
        mapped = _map_profile(profile)
        assert "alice@example.com" in mapped.emails
        assert mapped.confidence == 85

    def test_email_string_format(self):
        profile = {"emails": ["bob@example.com"], "confidence": 75}
        mapped = _map_profile(profile)
        assert "bob@example.com" in mapped.emails

    def test_phone_dict_format(self):
        profile = {"phones": [{"number": "+15555551234"}], "confidence": 80}
        mapped = _map_profile(profile)
        assert "+15555551234" in mapped.phones

    def test_phone_string_format(self):
        profile = {"phones": ["5555551234"], "confidence": 70}
        mapped = _map_profile(profile)
        assert "5555551234" in mapped.phones

    def test_linkedin_url(self):
        profile = {"linkedin_url": "https://linkedin.com/in/alice", "confidence": 90}
        mapped = _map_profile(profile)
        assert mapped.linkedin_url == "https://linkedin.com/in/alice"

    def test_empty_profile(self):
        mapped = _map_profile({})
        assert mapped.emails == []
        assert mapped.phones == []
        assert mapped.linkedin_url is None
        assert mapped.confidence == 0

    def test_null_entries_filtered(self):
        profile = {"emails": [{"email": ""}, {"email": "good@example.com"}], "confidence": 60}
        mapped = _map_profile(profile)
        assert "" not in mapped.emails
        assert "good@example.com" in mapped.emails


# ---------------------------------------------------------------------------
# 4. _identity_gate
# ---------------------------------------------------------------------------

class TestIdentityGate:
    def test_passes_with_name_and_no_email(self):
        candidate = _make_candidate(normalized_name="john smith", best_email=None)
        mapped = _MappedFields(
            emails=["john@example.com"], phones=[], linkedin_url=None, confidence=80
        )
        assert _identity_gate(candidate, mapped) is None

    def test_blocks_missing_normalized_name(self):
        candidate = _make_candidate(normalized_name=None)
        mapped = _MappedFields(
            emails=["john@example.com"], phones=[], linkedin_url=None, confidence=80
        )
        reason = _identity_gate(candidate, mapped)
        assert reason == "rocketreach_identity_conflict"

    def test_blocks_empty_normalized_name(self):
        candidate = _make_candidate(normalized_name="")
        mapped = _MappedFields(
            emails=["john@example.com"], phones=[], linkedin_url=None, confidence=80
        )
        reason = _identity_gate(candidate, mapped)
        assert reason == "rocketreach_identity_conflict"

    def test_email_conflict_blocked(self):
        candidate = _make_candidate(best_email="alice@example.com")
        # RocketReach returns completely different email
        mapped = _MappedFields(
            emails=["bob@differentdomain.com"], phones=[], linkedin_url=None, confidence=85
        )
        reason = _identity_gate(candidate, mapped)
        assert reason == "rocketreach_identity_conflict"

    def test_email_match_passes(self):
        candidate = _make_candidate(best_email="alice@example.com")
        mapped = _MappedFields(
            emails=["Alice@Example.COM"],  # different case — should normalise
            phones=[], linkedin_url=None, confidence=85
        )
        reason = _identity_gate(candidate, mapped)
        assert reason is None

    def test_existing_email_no_returned_emails_passes(self):
        """No returned emails → no email conflict to detect."""
        candidate = _make_candidate(best_email="alice@example.com")
        mapped = _MappedFields(
            emails=[], phones=["5551234567"], linkedin_url=None, confidence=80
        )
        assert _identity_gate(candidate, mapped) is None


# ---------------------------------------------------------------------------
# 5. Counter increments by outcome
# ---------------------------------------------------------------------------

class TestCounterIncrements:
    """Verify that _process_candidate mutates counters correctly per outcome.

    Uses a stub client that returns a preset LookupResult.
    """

    def _run(self, result: LookupResult, candidate: dict | None = None) -> RocketReachEnrichmentCounters:
        """Run _process_candidate with a stub client and real in-memory counters."""
        from regatta_etl.import_rocketreach_enrichment import _process_candidate

        if candidate is None:
            candidate = _make_candidate()

        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (
            "00000000-0000-0000-0000-000000000099",
        )

        class _Stub:
            def lookup(self, email, counters):
                return result

        counters = _make_counters()
        _process_candidate(conn, "run-1", candidate, _Stub(), "rocketreach_api", counters)
        return counters

    def test_no_match_increments_no_match(self):
        result = LookupResult(
            row_status="complete", profiles=[], person_id=None,
            credits_used=True, error_code=None,
        )
        ctrs = self._run(result)
        assert ctrs.rocketreach_no_match == 1
        assert ctrs.rocketreach_matches_applied == 0

    def test_ambiguous_increments_ambiguous(self):
        result = LookupResult(
            row_status="complete",
            profiles=[{"name": "Alice", "confidence": 80}, {"name": "Bob", "confidence": 78}],
            person_id=None, credits_used=True, error_code=None,
        )
        ctrs = self._run(result)
        assert ctrs.rocketreach_ambiguous == 1
        assert ctrs.rocketreach_matches_applied == 0

    def test_rate_limited_increments(self):
        result = LookupResult(
            row_status="rate_limited", profiles=[], person_id=None,
            credits_used=False, error_code="rocketreach_rate_limited",
        )
        ctrs = self._run(result)
        assert ctrs.rocketreach_matches_applied == 0
        assert ctrs.rocketreach_rate_limited == 1   # per-candidate-outcome (not per-retry)

    def test_api_error_increments_api_errors(self):
        result = LookupResult(
            row_status="api_error", profiles=[], person_id=None,
            credits_used=True, error_code="rocketreach_api_error",
        )
        ctrs = self._run(result)
        assert ctrs.rocketreach_api_errors == 1
        assert ctrs.rocketreach_matches_applied == 0

    def test_skipped_not_counted_as_called(self):
        """Candidates without email are skipped before calling API."""
        from regatta_etl.import_rocketreach_enrichment import _process_candidate

        candidate = _make_candidate(normalized_name=None, display_name=None, best_email=None)
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (
            "00000000-0000-0000-0000-000000000099",
        )

        class _NeverCalled:
            def lookup(self, email, counters):
                raise AssertionError("Should not call API without email")

        counters = _make_counters()
        _process_candidate(conn, "run-1", candidate, _NeverCalled(), "rocketreach_api", counters)
        assert counters.rocketreach_candidates_called == 0


# ---------------------------------------------------------------------------
# 6. Retry / backoff on 429
# ---------------------------------------------------------------------------

class TestRetryBackoff:
    def test_429_increments_rate_limited_and_retries(self):
        from regatta_etl.import_rocketreach_enrichment import RocketReachClient

        client = RocketReachClient(api_key="test", max_retries=2, qps_limit=1000.0)
        counters = _make_counters()

        resp_429 = MagicMock()
        resp_429.status_code = 429

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = {
            "status": "complete",
            "profiles": [{"name": "Alice", "confidence": 80}],
            "id": 42,
        }

        with patch("requests.get", side_effect=[resp_429, resp_200]), \
             patch("time.sleep"):
            result = client.lookup(email="alice@example.com", counters=counters)

        # rocketreach_rate_limited is a per-candidate-outcome counter (counted by
        # _process_candidate, not by the transport client).  A lookup that retried on
        # 429 but eventually succeeded must NOT increment the counter.
        assert counters.rocketreach_rate_limited == 0
        assert result.row_status == "complete"
        assert result.credits_used is True

    def test_429_exhausted_returns_rate_limited(self):
        from regatta_etl.import_rocketreach_enrichment import RocketReachClient

        client = RocketReachClient(api_key="test", max_retries=1, qps_limit=1000.0)
        counters = _make_counters()

        resp_429 = MagicMock()
        resp_429.status_code = 429

        with patch("requests.get", side_effect=[resp_429, resp_429]), \
             patch("time.sleep"):
            result = client.lookup(email="alice@example.com", counters=counters)

        assert result.row_status == "rate_limited"
        # Client does not increment the per-outcome counter; _process_candidate does.
        assert counters.rocketreach_rate_limited == 0

    def test_network_error_returns_api_error_not_rate_limited(self):
        """Fix 1: RequestException → row_status='api_error', NOT 'rate_limited'."""
        from regatta_etl.import_rocketreach_enrichment import RocketReachClient

        client = RocketReachClient(api_key="test", max_retries=0, qps_limit=1000.0)
        counters = _make_counters()

        with patch("requests.get", side_effect=requests.RequestException("timeout")), \
             patch("time.sleep"):
            result = client.lookup(email="alice@example.com", counters=counters)

        assert result.row_status == "api_error", (
            "network errors must not be misclassified as rate_limited"
        )
        assert result.error_code == "rocketreach_api_error"
        # Transport client no longer owns the api_errors counter; _process_candidate does.
        assert counters.rocketreach_api_errors == 0
        assert counters.rocketreach_rate_limited == 0

    def test_http_error_returns_api_error_not_rate_limited(self):
        """Fix 1: HTTP 5xx → row_status='api_error', NOT 'rate_limited'."""
        from regatta_etl.import_rocketreach_enrichment import RocketReachClient

        client = RocketReachClient(api_key="test", max_retries=0, qps_limit=1000.0)
        counters = _make_counters()

        resp_500 = MagicMock()
        resp_500.status_code = 500
        resp_500.text = "Internal Server Error"

        with patch("requests.get", return_value=resp_500), \
             patch("time.sleep"):
            result = client.lookup(email="alice@example.com", counters=counters)

        assert result.row_status == "api_error"
        # Transport client no longer owns the api_errors counter; _process_candidate does.
        assert counters.rocketreach_api_errors == 0
        assert counters.rocketreach_rate_limited == 0


# ---------------------------------------------------------------------------
# 7. Async polling — delayed-complete and timeout
# ---------------------------------------------------------------------------

class TestAsyncPolling:
    def test_searching_then_complete(self):
        from regatta_etl.import_rocketreach_enrichment import RocketReachClient

        client = RocketReachClient(
            api_key="test", max_retries=0, qps_limit=1000.0,
            timeout_seconds=60.0,
        )
        counters = _make_counters()

        resp_searching = MagicMock()
        resp_searching.status_code = 200
        resp_searching.json.return_value = {"status": "searching", "id": 99}

        resp_complete = MagicMock()
        resp_complete.status_code = 200
        resp_complete.json.return_value = {
            "status": "complete",
            "profiles": [{"name": "Alice", "confidence": 88, "emails": ["a@example.com"]}],
        }

        with patch("requests.get", side_effect=[resp_searching, resp_complete]), \
             patch("time.sleep"), \
             patch("time.monotonic", side_effect=[0.0, 1.0, 2.0]):
            result = client.lookup(email="alice@example.com", counters=counters)

        assert result.row_status == "complete"
        assert counters.rocketreach_status_polls == 1
        assert result.credits_used is True

    def test_polling_timeout_marks_non_terminal(self):
        from regatta_etl.import_rocketreach_enrichment import RocketReachClient

        client = RocketReachClient(
            api_key="test", max_retries=0, qps_limit=1000.0,
            timeout_seconds=5.0,
        )
        counters = _make_counters()

        resp_searching = MagicMock()
        resp_searching.status_code = 200
        resp_searching.json.return_value = {"status": "searching", "id": 77}

        resp_still_searching = MagicMock()
        resp_still_searching.status_code = 200
        resp_still_searching.json.return_value = {"status": "searching"}

        # monotonic values: lookup succeeds at t=0, then poll at t=6 > deadline (5.0)
        with patch("requests.get", side_effect=[resp_searching, resp_still_searching]), \
             patch("time.sleep"), \
             patch("time.monotonic", side_effect=[0.0, 6.0]):
            result = client.lookup(email="alice@example.com", counters=counters)

        assert result.row_status == "api_error"
        assert "timeout" in result.error_code
        assert counters.rocketreach_non_terminal_timeouts == 1


# ---------------------------------------------------------------------------
# 8. build_enrichment_report
# ---------------------------------------------------------------------------

class TestBuildReport:
    def test_dry_run_label(self):
        ctrs = _make_counters()
        ctrs.rocketreach_candidates_considered = 10
        ctrs.rocketreach_matches_applied = 5
        report = build_enrichment_report(ctrs, dry_run=True)
        assert "[DRY RUN]" in report
        assert "10" in report
        assert "5" in report

    def test_non_dry_run_no_label(self):
        ctrs = _make_counters()
        report = build_enrichment_report(ctrs, dry_run=False)
        assert "[DRY RUN]" not in report

    def test_warnings_included(self):
        ctrs = _make_counters()
        ctrs.warnings = ["w1", "w2"]
        report = build_enrichment_report(ctrs, dry_run=False)
        assert "w1" in report
