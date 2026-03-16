"""regatta_etl.import_rocketreach_enrichment

RocketReach participant enrichment pipeline (v1).

Mode: --mode participant_enrichment_rocketreach

Enriches candidate_participant rows with RocketReach API data (email, phone,
LinkedIn URL).  Does NOT write directly to canonical tables.  All promoted
changes must flow through the existing resolution_score → resolution_promote
pipeline.

API flow (async-capable):
  1. POST /v2/api/lookupProfile → may return 'searching' (async) or 'complete'.
  2. If 'searching': poll GET /v2/api/checkStatus at _POLL_INTERVAL_SECONDS
     until terminal state or timeout is reached.
  3. On HTTP 429: exponential backoff, retry up to max_retries times.

Selection policy:
  - candidate_participant only (is_promoted = false)
  - resolution_state IN configured states
  - missing field(s) per --require-missing
  - not enriched within --cooldown-days window
  - ordered: quality_score DESC, updated_at DESC, id ASC
  - LIMIT max_candidates

Spec: docs/requirements/rocketreach-participant-candidate-enrichment-spec.md
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import psycopg
import requests

from regatta_etl.normalize import normalize_email, normalize_phone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# RocketReach v2 API paths (matches official SDK behavior).
_API_LOOKUP_PATH = "/api/v2/person/lookup"
_API_STATUS_PATH = "/api/v2/person/checkStatus"
_POLL_INTERVAL_SECONDS = 2.0
_BACKOFF_BASE = 2.0
_BACKOFF_MAX = 64.0
# RocketReach confidence is 0–100; treat < 50 as no_match.
_MIN_CONFIDENCE = 50

# Person-name quality gate constants
_NAME_ORG_TOKENS: frozenset[str] = frozenset({
    "club", "team", "yacht", "sailing", "marina", "cruising",
    "corinthian", "association", "corps", "committee",
})
_MAX_NAME_TOKENS = 5


# ---------------------------------------------------------------------------
# LookupResult — returned by every client implementation
# ---------------------------------------------------------------------------

@dataclass
class LookupResult:
    """Normalised outcome of one RocketReach person lookup."""
    row_status: str          # matched|no_match|ambiguous|api_error|rate_limited|skipped
    profiles: list[dict]     # raw profile dicts from API (may be empty)
    person_id: str | None    # provider person id, if available
    credits_used: bool       # True when a credit-incurring lookup request was sent
    error_code: str | None   # reason code when not matched
    polls_made: int = 0      # number of status-poll requests made


# ---------------------------------------------------------------------------
# Client protocol — injectable for tests
# ---------------------------------------------------------------------------

def _is_plausible_person_name(name: str | None) -> bool:
    """Return True if name looks like a plausible individual person.

    Fails closed: rejects names that contain org-like tokens, ambiguous
    separators, or an improbably large number of tokens.
    """
    if not name:
        return False
    cleaned = name.strip()
    if len(cleaned) < 2:
        return False
    # Multi-person or list-like separators
    if "&" in cleaned or "/" in cleaned:
        return False
    tokens = cleaned.lower().split()
    # Org or program tokens
    if any(t in _NAME_ORG_TOKENS for t in tokens):
        return False
    # Too many tokens → likely concatenated labels
    if len(tokens) > _MAX_NAME_TOKENS:
        return False
    return True


class RocketReachClientProtocol(Protocol):
    def lookup(
        self,
        email: str | None,
        counters: "RocketReachEnrichmentCounters",
        name: str | None = None,
        city: str | None = None,
        state: str | None = None,
        country: str | None = None,
    ) -> LookupResult: ...


# ---------------------------------------------------------------------------
# Real HTTP client
# ---------------------------------------------------------------------------

@dataclass
class RocketReachClient:
    """Thin HTTP adapter around the RocketReach REST API.

    Designed to be injectable: replace with a stub in unit/integration tests.

    api_key:          RocketReach API key.
    base_url:         API base URL.
    timeout_seconds:  Per-request socket timeout AND total wall-clock budget
                      for the polling loop.
    max_retries:      Max 429 retry attempts before giving up.
    qps_limit:        Max lookup requests per second (enforced as min inter-call sleep).
    """

    api_key: str
    base_url: str = "https://api.rocketreach.co"
    timeout_seconds: float = 20.0
    max_retries: int = 3
    qps_limit: float = 2.0

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def lookup(
        self,
        email: str | None,
        counters: "RocketReachEnrichmentCounters",
        name: str | None = None,
        city: str | None = None,
        state: str | None = None,
        country: str | None = None,
    ) -> LookupResult:
        """Submit a person lookup and poll until terminal state or timeout.

        Returns a LookupResult.  counters are mutated in-place for API-level
        metrics (rate limits, polls, credits, errors).
        """
        url = self.base_url + _API_LOOKUP_PATH
        # Require at least email, OR name + at least one geo field
        if not email and not (name and (city or state or country)):
            return LookupResult(
                row_status="skipped",
                profiles=[],
                person_id=None,
                credits_used=False,
                error_code="rocketreach_insufficient_lookup_params",
            )
        params: dict[str, Any] = {}
        if email:
            params["email"] = email
        if name:
            params["name"] = name
        if city:
            params["city"] = city
        if state:
            params["state"] = state
        if country:
            params["country"] = country

        resp, lookup_error = self._lookup_with_retry(url, params, counters)
        if resp is None:
            # lookup_error is 'rate_limited' or 'api_error'
            error_code = (
                "rocketreach_rate_limited"
                if lookup_error == "rate_limited"
                else "rocketreach_api_error"
            )
            return LookupResult(
                row_status=lookup_error,  # type: ignore[arg-type]
                profiles=[],
                person_id=None,
                credits_used=False,
                error_code=error_code,
            )

        counters.rocketreach_lookup_credits_used += 1

        data = resp.json()
        status = data.get("status", "")

        if status == "complete":
            return LookupResult(
                row_status="complete",
                profiles=_extract_profiles(data),
                person_id=str(data.get("id")) if data.get("id") else None,
                credits_used=True,
                error_code=None,
            )

        if status in ("failed", "error", "not_found"):
            return LookupResult(
                row_status="api_error",
                profiles=[],
                person_id=str(data.get("id")) if data.get("id") else None,
                credits_used=True,
                error_code=data.get("message", "rocketreach_api_error"),
            )

        # Async path — status == 'searching'
        person_id = str(data["id"]) if data.get("id") else None
        if not person_id:
            return LookupResult(
                row_status="api_error",
                profiles=[],
                person_id=None,
                credits_used=True,
                error_code="missing_lookup_id",
            )

        return self._poll_until_terminal(person_id, counters)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {"Api-Key": self.api_key}

    def _min_delay(self) -> float:
        return 1.0 / max(self.qps_limit, 0.01)

    def _lookup_with_retry(
        self,
        url: str,
        params: dict[str, Any],
        counters: "RocketReachEnrichmentCounters",
    ) -> tuple[requests.Response | None, str | None]:
        """GET lookup with bounded exponential backoff on 429.

        Returns:
            (response, None)            on success
            (None, 'rate_limited')      when 429 retries exhausted
            (None, 'api_error')         on network error or non-429 HTTP error
        """
        backoff = _BACKOFF_BASE
        for attempt in range(self.max_retries + 1):
            time.sleep(self._min_delay())
            try:
                resp = requests.get(
                    url,
                    headers=self._headers(),
                    params=params,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                counters.warnings.append(f"RocketReach lookup GET error: {exc}")
                return None, "api_error"

            if resp.status_code == 429:
                if attempt >= self.max_retries:
                    return None, "rate_limited"
                time.sleep(min(backoff, _BACKOFF_MAX))
                backoff *= 2
                continue

            if resp.status_code >= 400:
                counters.warnings.append(
                    f"RocketReach lookup GET HTTP {resp.status_code}: {resp.text[:200]}"
                )
                return None, "api_error"

            return resp, None

        return None, "rate_limited"

    def _poll_until_terminal(
        self,
        person_id: str,
        counters: "RocketReachEnrichmentCounters",
    ) -> LookupResult:
        """Poll /api/v2/person/checkStatus until terminal state or timeout."""
        url = self.base_url + _API_STATUS_PATH
        deadline = time.monotonic() + self.timeout_seconds
        polls = 0

        while time.monotonic() < deadline:
            time.sleep(_POLL_INTERVAL_SECONDS)
            polls += 1
            counters.rocketreach_status_polls += 1

            try:
                resp = requests.get(
                    url,
                    headers=self._headers(),
                    params={"ids": person_id},
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                return LookupResult(
                    row_status="api_error",
                    profiles=[],
                    person_id=person_id,
                    credits_used=True,
                    error_code=f"poll_request_error: {exc}",
                    polls_made=polls,
                )

            if resp.status_code == 429:
                time.sleep(min(_BACKOFF_BASE, _BACKOFF_MAX))
                continue

            if resp.status_code >= 400:
                return LookupResult(
                    row_status="api_error",
                    profiles=[],
                    person_id=person_id,
                    credits_used=True,
                    error_code=f"poll_http_{resp.status_code}",
                    polls_made=polls,
                )

            data = resp.json()
            # checkStatus may return a list or a single object
            if isinstance(data, list):
                data = data[0] if data else {}

            status = data.get("status", "")
            if status == "complete":
                return LookupResult(
                    row_status="complete",
                    profiles=_extract_profiles(data),
                    person_id=person_id,
                    credits_used=True,
                    error_code=None,
                    polls_made=polls,
                )
            if status in ("failed", "error", "not_found"):
                return LookupResult(
                    row_status="api_error",
                    profiles=[],
                    person_id=person_id,
                    credits_used=True,
                    error_code=data.get("message", f"poll_status_{status}"),
                    polls_made=polls,
                )
            # still 'searching' — continue loop

        counters.rocketreach_non_terminal_timeouts += 1
        return LookupResult(
            row_status="api_error",
            profiles=[],
            person_id=person_id,
            credits_used=True,
            error_code="rocketreach_non_terminal_timeout",
            polls_made=polls,
        )


def _extract_profiles(data: dict) -> list[dict]:
    """Extract profiles list from a RocketReach response dict."""
    profiles = data.get("profiles")
    if profiles:
        return profiles
    # Some API shapes return fields directly on the top-level object
    if data.get("name") or data.get("emails"):
        return [data]
    return []


# ---------------------------------------------------------------------------
# Dry-run stub client
# ---------------------------------------------------------------------------

class _NullRocketReachClient:
    """Client stub used during dry runs — never makes real HTTP calls."""

    def lookup(
        self,
        email: str | None,
        counters: "RocketReachEnrichmentCounters",
        name: str | None = None,
        city: str | None = None,
        state: str | None = None,
        country: str | None = None,
    ) -> LookupResult:
        return LookupResult(
            row_status="skipped",
            profiles=[],
            person_id=None,
            credits_used=False,
            error_code=None,
        )


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class RocketReachEnrichmentCounters:
    rocketreach_candidates_considered: int = 0
    rocketreach_candidates_called: int = 0
    rocketreach_geo_ready_candidates_called: int = 0   # subset with geo context
    rocketreach_non_person_name: int = 0               # skipped by name-quality gate
    rocketreach_matches_applied: int = 0
    rocketreach_no_match: int = 0
    rocketreach_ambiguous: int = 0
    rocketreach_api_errors: int = 0
    rocketreach_rate_limited: int = 0
    rocketreach_fields_enriched: int = 0
    rocketreach_source_links_inserted: int = 0
    db_phase_errors: int = 0
    rocketreach_lookup_credits_used: int = 0
    rocketreach_status_polls: int = 0
    rocketreach_non_terminal_timeouts: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rocketreach_candidates_considered": self.rocketreach_candidates_considered,
            "rocketreach_candidates_called": self.rocketreach_candidates_called,
            "rocketreach_geo_ready_candidates_called": self.rocketreach_geo_ready_candidates_called,
            "rocketreach_non_person_name": self.rocketreach_non_person_name,
            "rocketreach_matches_applied": self.rocketreach_matches_applied,
            "rocketreach_no_match": self.rocketreach_no_match,
            "rocketreach_ambiguous": self.rocketreach_ambiguous,
            "rocketreach_api_errors": self.rocketreach_api_errors,
            "rocketreach_rate_limited": self.rocketreach_rate_limited,
            "rocketreach_fields_enriched": self.rocketreach_fields_enriched,
            "rocketreach_source_links_inserted": self.rocketreach_source_links_inserted,
            "db_phase_errors": self.db_phase_errors,
            "rocketreach_lookup_credits_used": self.rocketreach_lookup_credits_used,
            "rocketreach_status_polls": self.rocketreach_status_polls,
            "rocketreach_non_terminal_timeouts": self.rocketreach_non_terminal_timeouts,
            "warnings": self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Candidate selection
# ---------------------------------------------------------------------------

def _missing_filter_clause(require_missing: str) -> str:
    """Return a SQL boolean expression for the require-missing filter.

    Safe: value is always from a click.Choice enum, never raw user text.
    """
    if require_missing == "email":
        return "cp.best_email IS NULL"
    if require_missing == "phone":
        return "cp.best_phone IS NULL"
    if require_missing == "email_or_phone":
        return "(cp.best_email IS NULL OR cp.best_phone IS NULL)"
    return "TRUE"  # none


def _select_candidates(
    conn: psycopg.Connection,
    candidate_states: list[str],
    require_missing: str,
    cooldown_days: int,
    max_candidates: int,
    require_geo_ready: bool = False,
    geo_country: str | None = None,
    geo_states: list[str] | None = None,
) -> list[dict]:
    """Return up to max_candidates eligible candidate_participant rows.

    Always LEFT JOINs candidate_participant_enrichment_readiness so that
    geo hint columns (best_city / best_state_region / best_country_code) are
    available for every returned row (NULL when no readiness row exists).
    """
    missing_clause = _missing_filter_clause(require_missing)

    extra_conditions: list[str] = []
    extra_params: list = []

    if require_geo_ready:
        extra_conditions.append("cper.readiness_status = 'enrichment_ready'")
    if geo_country:
        extra_conditions.append("cper.best_country_code = %s")
        extra_params.append(geo_country)
    if geo_states:
        extra_conditions.append("cper.best_state_region = ANY(%s)")
        extra_params.append(geo_states)

    extra_where = "".join(f"\n          AND {c}" for c in extra_conditions)

    sql = f"""
        SELECT
            cp.id,
            cp.display_name,
            cp.normalized_name,
            cp.best_email,
            cp.best_phone,
            cp.quality_score,
            cp.resolution_state,
            cper.best_city,
            cper.best_state_region,
            cper.best_country_code
        FROM candidate_participant cp
        LEFT JOIN candidate_participant_enrichment_readiness cper
            ON cper.candidate_participant_id = cp.id
        WHERE cp.is_promoted = false
          AND cp.resolution_state = ANY(%s)
          AND {missing_clause}
          AND NOT EXISTS (
              SELECT 1
              FROM rocketreach_enrichment_row rer
              WHERE rer.candidate_participant_id = cp.id
                AND rer.created_at > NOW() - (%s * INTERVAL '1 day')
          ){extra_where}
        ORDER BY cp.quality_score DESC, cp.updated_at DESC, cp.id ASC
        LIMIT %s
    """
    params: list = [candidate_states, cooldown_days] + extra_params + [max_candidates]
    rows = conn.execute(sql, params).fetchall()
    cols = ["id", "display_name", "normalized_name", "best_email", "best_phone",
            "quality_score", "resolution_state",
            "best_city", "best_state_region", "best_country_code"]
    return [dict(zip(cols, row)) for row in rows]


# ---------------------------------------------------------------------------
# Field mapping from RocketReach profile
# ---------------------------------------------------------------------------

@dataclass
class _MappedFields:
    """Candidate field values extracted from a RocketReach profile."""
    emails: list[str]           # raw email strings
    phones: list[str]           # raw phone strings
    linkedin_url: str | None
    confidence: int             # 0–100


def _map_profile(profile: dict) -> _MappedFields:
    """Extract normalisation-friendly fields from a RocketReach profile dict."""
    emails: list[str] = []
    for e in profile.get("emails") or []:
        if isinstance(e, dict):
            val = e.get("email") or e.get("value") or ""
        else:
            val = str(e)
        if val:
            emails.append(val.strip())

    phones: list[str] = []
    for p in profile.get("phones") or []:
        if isinstance(p, dict):
            val = p.get("number") or p.get("value") or ""
        else:
            val = str(p)
        if val:
            phones.append(val.strip())

    linkedin_url: str | None = profile.get("linkedin_url") or None
    confidence: int = int(profile.get("confidence") or 0)

    return _MappedFields(
        emails=emails,
        phones=phones,
        linkedin_url=linkedin_url,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Identity safety gate
# ---------------------------------------------------------------------------

def _identity_gate(
    candidate: dict,
    mapped: _MappedFields,
) -> str | None:
    """Return a reason_code if enrichment should be blocked, else None.

    Checks (in order):
      1. Candidate has no normalized_name (no identity anchor).
      2. Returned emails conflict with existing non-null best_email.
    """
    if not candidate["normalized_name"]:
        return "rocketreach_identity_conflict"

    existing_email = candidate["best_email"]
    if existing_email and mapped.emails:
        existing_norm = normalize_email(existing_email)
        returned_norms = {normalize_email(e) for e in mapped.emails if e}
        returned_norms.discard(None)
        if existing_norm and returned_norms and existing_norm not in returned_norms:
            return "rocketreach_identity_conflict"

    return None


# ---------------------------------------------------------------------------
# Field application (fill-null-only)
# ---------------------------------------------------------------------------

def _apply_fields(
    conn: psycopg.Connection,
    candidate_id: str,
    enrichment_row_id: str,
    mapped: _MappedFields,
    source_system: str,
) -> list[str]:
    """Apply fill-null enrichment to candidate_participant and child contacts.

    Returns applied_field_mask: list of field category strings that were written.
    """
    applied: list[str] = []

    # -- Top-level candidate fields (fill-null-only) -----------------------
    top_updates: list[tuple[str, str | None]] = []

    if mapped.emails:
        best_norm = normalize_email(mapped.emails[0])
        if best_norm:
            top_updates.append(("best_email", best_norm))

    if mapped.phones:
        best_norm = normalize_phone(mapped.phones[0])
        if best_norm:
            top_updates.append(("best_phone", best_norm))

    for col, val in top_updates:
        result = conn.execute(
            f"UPDATE candidate_participant "
            f"SET {col} = COALESCE({col}, %s) "
            f"WHERE id = %s AND {col} IS NULL "
            f"RETURNING id",
            (val, candidate_id),
        ).fetchone()
        if result:
            applied.append(col)

    # -- candidate_participant_contact rows -----------------------------------
    email_contacts_inserted = 0
    for email_raw in mapped.emails:
        email_norm = normalize_email(email_raw)
        if not email_norm:
            continue
        row = conn.execute(
            """
            INSERT INTO candidate_participant_contact
                (candidate_participant_id, contact_type, contact_subtype,
                 raw_value, normalized_value, is_primary,
                 source_table_name, source_row_pk)
            VALUES (%s, 'email', 'rocketreach', %s, %s, false,
                    'rocketreach_enrichment_row', %s)
            ON CONFLICT (candidate_participant_id, contact_type, normalized_value)
                WHERE normalized_value IS NOT NULL
            DO NOTHING
            RETURNING id
            """,
            (candidate_id, email_raw, email_norm, enrichment_row_id),
        ).fetchone()
        if row:
            email_contacts_inserted += 1

    if email_contacts_inserted > 0:
        applied.append("contact_emails")

    phone_contacts_inserted = 0
    for phone_raw in mapped.phones:
        phone_norm = normalize_phone(phone_raw)
        if not phone_norm:
            continue
        row = conn.execute(
            """
            INSERT INTO candidate_participant_contact
                (candidate_participant_id, contact_type, contact_subtype,
                 raw_value, normalized_value, is_primary,
                 source_table_name, source_row_pk)
            VALUES (%s, 'phone', 'rocketreach', %s, %s, false,
                    'rocketreach_enrichment_row', %s)
            ON CONFLICT (candidate_participant_id, contact_type, normalized_value)
                WHERE normalized_value IS NOT NULL
            DO NOTHING
            RETURNING id
            """,
            (candidate_id, phone_raw, phone_norm, enrichment_row_id),
        ).fetchone()
        if row:
            phone_contacts_inserted += 1

    if phone_contacts_inserted > 0:
        applied.append("contact_phones")

    if mapped.linkedin_url:
        url_norm = mapped.linkedin_url.lower().rstrip("/")
        row = conn.execute(
            """
            INSERT INTO candidate_participant_contact
                (candidate_participant_id, contact_type, contact_subtype,
                 raw_value, normalized_value, is_primary,
                 source_table_name, source_row_pk)
            VALUES (%s, 'social', 'linkedin', %s, %s, false,
                    'rocketreach_enrichment_row', %s)
            ON CONFLICT (candidate_participant_id, contact_type, normalized_value)
                WHERE normalized_value IS NOT NULL
            DO NOTHING
            RETURNING id
            """,
            (candidate_id, mapped.linkedin_url, url_norm, enrichment_row_id),
        ).fetchone()
        if row:
            applied.append("linkedin_url")

    return applied


# ---------------------------------------------------------------------------
# Candidate source link
# ---------------------------------------------------------------------------

def _upsert_source_link(
    conn: psycopg.Connection,
    candidate_id: str,
    enrichment_row_id: str,
    provider_person_id: str | None,
    source_system: str,
    counters: RocketReachEnrichmentCounters,
) -> None:
    """Insert a candidate_source_link row (idempotent via ON CONFLICT DO NOTHING).

    Keyed on (candidate_id, 'rocketreach_api', provider_person_id) so that
    repeated runs for the same candidate + provider person do not create
    duplicate evidence links.  enrichment_row_id is stored in link_reason for
    back-traceability to the audit row.

    Skipped when provider_person_id is absent (no stable key to deduplicate on).
    """
    if not provider_person_id:
        return

    result = conn.execute(
        """
        INSERT INTO candidate_source_link
            (candidate_entity_type, candidate_entity_id,
             source_table_name, source_row_pk,
             source_system, link_reason)
        VALUES ('participant', %s,
                'rocketreach_api', %s,
                %s, %s)
        ON CONFLICT (candidate_entity_type, candidate_entity_id,
                     source_table_name, source_row_pk)
        DO NOTHING
        RETURNING id
        """,
        (
            candidate_id,
            provider_person_id,
            source_system,
            json.dumps({"enrichment_row_id": enrichment_row_id}),
        ),
    ).fetchone()
    if result:
        counters.rocketreach_source_links_inserted += 1


# ---------------------------------------------------------------------------
# Row-level processing
# ---------------------------------------------------------------------------

def _process_candidate(
    conn: psycopg.Connection,
    run_id: str,
    candidate: dict,
    client: RocketReachClientProtocol,
    source_system: str,
    counters: RocketReachEnrichmentCounters,
    require_person_name_quality: bool = True,
) -> None:
    """Process one candidate: lookup → gate → apply → audit row."""
    candidate_id = str(candidate["id"])
    # Prefer normalized_name for the outbound request — it is the same value that
    # _is_plausible_person_name() validates, so the gate and the payload are consistent.
    # Fall back to display_name only when normalized_name is absent.
    name = candidate["normalized_name"] or candidate["display_name"] or ""
    email = normalize_email(candidate["best_email"]) if candidate["best_email"] else None

    # Geo context from readiness join (None when no readiness row)
    geo_city: str | None = candidate.get("best_city")
    geo_state: str | None = candidate.get("best_state_region")
    geo_country: str | None = candidate.get("best_country_code")
    has_geo_context = bool(geo_city or geo_state or geo_country)

    # Person-name quality gate — runs before any billable API call
    if require_person_name_quality and not _is_plausible_person_name(
        candidate.get("normalized_name")
    ):
        counters.rocketreach_non_person_name += 1
        _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload={"name": name},
            response_payload={},
            provider_person_id=None,
            match_confidence=None,
            status="skipped",
            error_code="rocketreach_non_person_name",
            applied_field_mask=None,
        )
        return

    # Email required unless geo context provides a sufficient lookup anchor
    if not email and not has_geo_context:
        _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload={"email": None, "name": name},
            response_payload={},
            provider_person_id=None,
            match_confidence=None,
            status="skipped",
            error_code="rocketreach_email_required",
            applied_field_mask=None,
        )
        return

    counters.rocketreach_candidates_called += 1
    if has_geo_context:
        counters.rocketreach_geo_ready_candidates_called += 1

    # Build request payload — name always included; geo fields when available
    request_payload: dict[str, Any] = {"name": name}
    if email:
        request_payload["email"] = email
    if geo_city:
        request_payload["city"] = geo_city
    if geo_state:
        request_payload["state"] = geo_state
    if geo_country:
        request_payload["country"] = geo_country

    result = client.lookup(
        email=email,
        counters=counters,
        name=name if name else None,
        city=geo_city,
        state=geo_state,
        country=geo_country,
    )

    # Map raw row_status to enrichment_row.status
    if result.row_status == "rate_limited":
        counters.rocketreach_rate_limited += 1
        _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload=request_payload,
            response_payload={},
            provider_person_id=result.person_id,
            match_confidence=None,
            status="rate_limited",
            error_code=result.error_code,
            applied_field_mask=None,
        )
        return

    if result.row_status in ("api_error", "failed", "timeout"):
        counters.rocketreach_api_errors += 1
        _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload=request_payload,
            response_payload={"error_code": result.error_code},
            provider_person_id=result.person_id,
            match_confidence=None,
            status="api_error",
            error_code=result.error_code,
            applied_field_mask=None,
        )
        return

    if result.row_status == "skipped":
        _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload=request_payload,
            response_payload={},
            provider_person_id=None,
            match_confidence=None,
            status="skipped",
            error_code=result.error_code,
            applied_field_mask=None,
        )
        return

    # result.row_status == 'complete'
    profiles = result.profiles

    if len(profiles) == 0:
        counters.rocketreach_no_match += 1
        _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload=request_payload,
            response_payload={"profiles": profiles},
            provider_person_id=result.person_id,
            match_confidence=None,
            status="no_match",
            error_code="rocketreach_no_match",
            applied_field_mask=None,
        )
        return

    if len(profiles) > 1:
        counters.rocketreach_ambiguous += 1
        _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload=request_payload,
            response_payload={"profiles": profiles},
            provider_person_id=result.person_id,
            match_confidence=None,
            status="ambiguous",
            error_code="rocketreach_ambiguous_match",
            applied_field_mask=None,
        )
        return

    # Exactly one profile
    profile = profiles[0]
    mapped = _map_profile(profile)
    confidence_raw = mapped.confidence
    match_confidence = round(confidence_raw / 100.0, 4) if confidence_raw else None

    if confidence_raw < _MIN_CONFIDENCE:
        counters.rocketreach_no_match += 1
        _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload=request_payload,
            response_payload={"profiles": profiles},
            provider_person_id=result.person_id,
            match_confidence=match_confidence,
            status="no_match",
            error_code="rocketreach_no_match",
            applied_field_mask=None,
        )
        return

    # Identity safety gate
    conflict_reason = _identity_gate(candidate, mapped)
    if conflict_reason:
        # Row audited as matched (profile found) but nothing applied
        enrichment_row_id = _insert_enrichment_row(
            conn, run_id, candidate_id,
            request_payload=request_payload,
            response_payload={"profiles": profiles, "conflict_reason": conflict_reason},
            provider_person_id=result.person_id,
            match_confidence=match_confidence,
            status="matched",
            error_code=conflict_reason,
            applied_field_mask=[],
        )
        counters.warnings.append(
            f"Identity conflict for candidate {candidate_id}: {conflict_reason}"
        )
        return

    # Apply enrichment — inside the caller's per-row SAVEPOINT
    enrichment_row_id = _insert_enrichment_row(
        conn, run_id, candidate_id,
        request_payload=request_payload,
        response_payload={"profiles": profiles},
        provider_person_id=result.person_id,
        match_confidence=match_confidence,
        status="matched",
        error_code=None,
        applied_field_mask=None,  # filled in below after apply
    )

    applied_mask = _apply_fields(
        conn, candidate_id, enrichment_row_id, mapped, source_system
    )

    # Back-fill applied_field_mask on the row we just inserted
    conn.execute(
        "UPDATE rocketreach_enrichment_row SET applied_field_mask = %s WHERE id = %s",
        (applied_mask or None, enrichment_row_id),
    )

    counters.rocketreach_matches_applied += 1
    counters.rocketreach_fields_enriched += len(applied_mask)

    # Candidate source link
    _upsert_source_link(
        conn, candidate_id, enrichment_row_id,
        result.person_id, source_system, counters
    )


def _insert_enrichment_row(
    conn: psycopg.Connection,
    run_id: str,
    candidate_participant_id: str,
    request_payload: dict,
    response_payload: dict,
    provider_person_id: str | None,
    match_confidence: float | None,
    status: str,
    error_code: str | None,
    applied_field_mask: list[str] | None,
) -> str:
    """Insert rocketreach_enrichment_row and return its UUID."""
    row = conn.execute(
        """
        INSERT INTO rocketreach_enrichment_row
            (run_id, candidate_participant_id,
             request_payload, response_payload,
             provider_person_id, match_confidence,
             status, error_code, applied_field_mask)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            run_id,
            candidate_participant_id,
            json.dumps(request_payload),
            json.dumps(response_payload),
            provider_person_id,
            match_confidence,
            status,
            error_code,
            applied_field_mask,
        ),
    ).fetchone()
    return str(row[0])


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_rocketreach_enrichment(
    conn: psycopg.Connection,
    run_id: str,
    api_key: str,
    candidate_states: list[str],
    require_missing: str,
    cooldown_days: int,
    max_candidates: int,
    source_system: str,
    timeout_seconds: float,
    max_retries: int,
    qps_limit: float,
    max_api_failure_rate: float,
    dry_run: bool,
    counters: RocketReachEnrichmentCounters,
    client: RocketReachClientProtocol | None = None,
    require_geo_ready: bool = False,
    geo_country: str | None = None,
    geo_states: list[str] | None = None,
    require_person_name_quality: bool = True,
) -> RocketReachEnrichmentCounters:
    """Run the RocketReach enrichment pipeline.

    Args:
        conn:                 Active psycopg connection (autocommit=False).
        run_id:               UUID string for this run.
        api_key:              RocketReach API key.
        candidate_states:     List of resolution_state values to target.
        require_missing:      One of 'email'|'phone'|'email_or_phone'|'none'.
        cooldown_days:        Skip candidates enriched within this many days.
        max_candidates:       Max candidates to process per run.
        source_system:        Source-system label for provenance links.
        timeout_seconds:      Per-lookup wall-clock timeout (polling budget).
        max_retries:          Max 429 retry attempts.
        qps_limit:            Max POSTs per second.
        max_api_failure_rate: Warn when API errors exceed this fraction.
        dry_run:              If True, no DB writes are committed.
        counters:             Mutable counter accumulator.
        client:               Injectable RocketReach client (default: real or null on dry_run).

    Returns:
        The mutated counters.
    """
    if client is None:
        if dry_run:
            client = _NullRocketReachClient()
        else:
            client = RocketReachClient(
                api_key=api_key,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                qps_limit=qps_limit,
            )

    # Insert run audit row
    conn.execute(
        """
        INSERT INTO rocketreach_enrichment_run
            (id, dry_run, requested_max_candidates)
        VALUES (%s, %s, %s)
        """,
        (run_id, dry_run, max_candidates),
    )

    # Select candidates
    candidates = _select_candidates(
        conn, candidate_states, require_missing, cooldown_days, max_candidates,
        require_geo_ready=require_geo_ready,
        geo_country=geo_country,
        geo_states=geo_states,
    )
    counters.rocketreach_candidates_considered = len(candidates)

    # Process each candidate inside a per-row SAVEPOINT
    for idx, candidate in enumerate(candidates):
        sp_name = f"rr_enrich_{idx}"
        conn.execute(f"SAVEPOINT {sp_name}")
        try:
            _process_candidate(
                conn, run_id, candidate, client, source_system, counters,
                require_person_name_quality=require_person_name_quality,
            )
            conn.execute(f"RELEASE SAVEPOINT {sp_name}")
        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
            conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            counters.db_phase_errors += 1
            counters.warnings.append(
                f"DB error on candidate {candidate['id']}: {exc}"
            )

    # Check API failure rate threshold
    called = counters.rocketreach_candidates_called
    api_errors = counters.rocketreach_api_errors + counters.rocketreach_rate_limited
    if called > 0 and (api_errors / called) > max_api_failure_rate:
        counters.warnings.append(
            f"API failure rate {api_errors / called:.1%} exceeds threshold "
            f"{max_api_failure_rate:.1%} "
            f"({api_errors} failures / {called} calls)"
        )

    # Finalise run row
    final_status = "failed" if counters.db_phase_errors > 0 else "ok"
    if dry_run:
        final_status = "ok"

    conn.execute(
        """
        UPDATE rocketreach_enrichment_run
        SET finished_at = NOW(),
            status = %s,
            counters = %s,
            warnings = %s
        WHERE id = %s
        """,
        (
            final_status,
            json.dumps(counters.to_dict()),
            json.dumps(counters.warnings),
            run_id,
        ),
    )

    return counters


# ---------------------------------------------------------------------------
# Report formatter
# ---------------------------------------------------------------------------

def build_enrichment_report(
    counters: RocketReachEnrichmentCounters,
    dry_run: bool,
) -> str:
    tag = "[DRY RUN] " if dry_run else ""
    lines = [
        f"{tag}RocketReach Enrichment Report",
        f"  considered:      {counters.rocketreach_candidates_considered}",
        f"  called:          {counters.rocketreach_candidates_called}",
        f"  geo_ready_called:{counters.rocketreach_geo_ready_candidates_called}",
        f"  non_person_name: {counters.rocketreach_non_person_name}",
        f"  matched/applied: {counters.rocketreach_matches_applied}",
        f"  no_match:        {counters.rocketreach_no_match}",
        f"  ambiguous:       {counters.rocketreach_ambiguous}",
        f"  api_errors:      {counters.rocketreach_api_errors}",
        f"  rate_limited:    {counters.rocketreach_rate_limited}",
        f"  fields_enriched: {counters.rocketreach_fields_enriched}",
        f"  source_links:    {counters.rocketreach_source_links_inserted}",
        f"  credits_used:    {counters.rocketreach_lookup_credits_used}",
        f"  polls:           {counters.rocketreach_status_polls}",
        f"  timeouts:        {counters.rocketreach_non_terminal_timeouts}",
        f"  db_errors:       {counters.db_phase_errors}",
    ]
    if counters.warnings:
        lines.append(f"  warnings ({len(counters.warnings)}):")
        for w in counters.warnings[:10]:
            lines.append(f"    - {w}")
    return "\n".join(lines)
