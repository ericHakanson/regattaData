"""regatta_etl.participant_hold_geo_prepare

Hold-pool address normalization and RocketReach enrichment-readiness
classification (--mode participant_hold_geo_prepare).

For each hold candidate with candidate_participant_address child rows:
  1. Normalize each raw address string into a structured geographic hint.
  2. Classify the hint quality (city_state_country, country_only, junk, etc.).
  3. Deduplicate equivalent normalized hints per candidate.
  4. Select the single best hint for each candidate.
  5. Classify the candidate's enrichment readiness.
  6. Write candidate_participant_geo_hint and
     candidate_participant_enrichment_readiness rows (idempotent).

Ref: docs/requirements/participant-hold-pool-address-normalization-rocketreach-readiness-spec.md
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import psycopg


# ---------------------------------------------------------------------------
# Geographic token tables
# ---------------------------------------------------------------------------

# Multi-word phrases must be listed before single-word forms so the longest
# match wins when scanning from the right.
_COUNTRY_MULTI: dict[str, str] = {
    "UNITED STATES OF AMERICA": "USA",
    "UNITED STATES": "USA",
    "GREAT BRITAIN": "GBR",
    "UNITED KINGDOM": "GBR",
    "NEW ZEALAND": "NZL",
    "SOUTH AFRICA": "RSA",
}

_COUNTRY_SINGLE: dict[str, str] = {
    "USA": "USA",
    "US": "USA",
    "U.S.": "USA",
    "U.S.A.": "USA",
    "CAN": "CAN",
    "CANADA": "CAN",
    "IND": "IND",
    "INDIA": "IND",
    "GBR": "GBR",
    "UK": "GBR",
    "U.K.": "GBR",
    "AUS": "AUS",
    "AUSTRALIA": "AUS",
    "NZL": "NZL",
    "FRA": "FRA",
    "FRANCE": "FRA",
    "GER": "GER",
    "GERMANY": "GER",
    "ITA": "ITA",
    "ITALY": "ITA",
    "ESP": "ESP",
    "SPAIN": "ESP",
    "NED": "NED",
    "NETHERLANDS": "NED",
    "HOLLAND": "NED",
    "NOR": "NOR",
    "NORWAY": "NOR",
    "SWE": "SWE",
    "SWEDEN": "SWE",
    "DEN": "DEN",
    "DENMARK": "DEN",
    "RSA": "RSA",
    "BER": "BER",
    "BERMUDA": "BER",
    "BAH": "BAH",
    "BAHAMAS": "BAH",
    "JAM": "JAM",
    "JAMAICA": "JAM",
    "MEX": "MEX",
    "MEXICO": "MEX",
    "BRA": "BRA",
    "BRAZIL": "BRA",
    "ARG": "ARG",
    "ARGENTINA": "ARG",
    "CHI": "CHI",
    "CHILE": "CHI",
    "CHN": "CHN",
    "CHINA": "CHN",
    "JPN": "JPN",
    "JAPAN": "JPN",
}

# Multi-word US states first (longest match wins)
_US_STATE_MULTI: dict[str, str] = {
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "WEST VIRGINIA": "WV",
}

_US_STATE_SINGLE: dict[str, str] = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}

# All valid US state abbreviations (from the two-character abbrevs used above)
_US_STATE_ABBREVS: frozenset[str] = frozenset(
    list(_US_STATE_MULTI.values()) + list(_US_STATE_SINGLE.values())
)

# Exact-match junk values (uppercased) — boat classes, rigs, etc.
_JUNK_EXACT: frozenset[str] = frozenset({
    "LASER",
    "OPTIMIST",
    "OPTI",
    "SUNFISH",
    "420",
    "470",
    "29ER",
    "FJ",
    "FLYING JUNIOR",
    "MOTH",
    "RS AERO",
    "MELGES",
    "J24",
    "J/24",
    "J/22",
    "J/35",
    "THISTLE",
    "SNIPE",
    "LIGHTNING",
})

# Substrings that identify non-geographic strings (club names, etc.)
_JUNK_SUBSTRINGS: tuple[str, ...] = (
    "YACHT CLUB",
    "SAILING CLUB",
    "BOAT CLUB",
    "ROWING CLUB",
    "MARINA",
    "CRUISING CLUB",
    "CORINTHIAN",
)

# Quality classes ordered from best to worst for enrichment suitability
QUALITY_RANK: dict[str, int] = {
    "city_state_country": 1,
    "city_country": 2,
    "state_country": 3,
    "freeform_partial": 4,
    "country_only": 5,
    "non_address_junk": 99,
    "empty_or_null": 99,
}

# Readiness statuses
READINESS_ENRICHMENT_READY = "enrichment_ready"
READINESS_TOO_VAGUE = "too_vague"
READINESS_JUNK_LOCATION = "junk_location"
READINESS_MULTI_CONFLICT = "multi_location_conflict"
READINESS_ALREADY_HAS_CONTACT = "already_has_contact"
READINESS_NOT_HOLD = "not_hold"


# ---------------------------------------------------------------------------
# GeoHint dataclass
# ---------------------------------------------------------------------------

@dataclass
class GeoHint:
    address_raw: str
    source_address_row_id: str | None
    normalized_hint: str | None
    city: str | None
    state_region: str | None
    country_code: str | None
    quality_class: str


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def _clean_upper(raw: str) -> str:
    """Collapse whitespace, strip commas, return uppercase."""
    s = raw.strip().upper()
    s = s.replace(",", " ")
    return re.sub(r"\s+", " ", s).strip()


def _is_junk(raw: str) -> bool:
    """Return True if raw is clearly a non-geographic string (boat class, club name, etc.)."""
    upper = _clean_upper(raw)
    if not upper:
        return False
    if upper in _JUNK_EXACT:
        return True
    for sub in _JUNK_SUBSTRINGS:
        if sub in upper:
            return True
    return False


def _strip_country(tokens: list[str]) -> tuple[list[str], str | None]:
    """Strip a country token (or phrase) from the *right* of the token list.

    Returns (remaining_tokens, country_code | None).
    Checks 4-, 3-, 2-word phrases before single tokens.
    """
    for n in (4, 3, 2):
        if len(tokens) >= n:
            phrase = " ".join(tokens[-n:])
            if phrase in _COUNTRY_MULTI:
                return tokens[:-n], _COUNTRY_MULTI[phrase]
    if tokens:
        last = tokens[-1]
        if last in _COUNTRY_SINGLE:
            return tokens[:-1], _COUNTRY_SINGLE[last]
    return tokens, None


def _strip_us_state(tokens: list[str]) -> tuple[list[str], str | None]:
    """Strip a US state token or phrase from the *right* of the token list.

    Checks 2-word phrases first, then single abbreviations and full names.
    Returns (remaining_tokens, state_abbrev | None).
    """
    if len(tokens) >= 2:
        phrase = " ".join(tokens[-2:])
        if phrase in _US_STATE_MULTI:
            return tokens[:-2], _US_STATE_MULTI[phrase]
    if tokens:
        last = tokens[-1]
        if last in _US_STATE_ABBREVS:
            return tokens[:-1], last
        if last in _US_STATE_SINGLE:
            return tokens[:-1], _US_STATE_SINGLE[last]
    return tokens, None


def _normalize_tokens(raw: str) -> tuple[str | None, str | None, str | None]:
    """Parse raw address string into (city, state_region, country_code).

    Uses a right-to-left token parser:
      1. Strip country from the right.
      2. Strip US state from the right (only when country is US or unknown).
      3. Whatever remains is the city.

    Returns (None, None, None) if the string parses to nothing usable.
    """
    upper = _clean_upper(raw)
    if not upper:
        return None, None, None

    tokens = upper.split()

    # Step 1: strip country from right
    tokens, country_code = _strip_country(tokens)

    # Step 2: strip US state from right (for US addresses or ambiguous)
    state_region: str | None = None
    if country_code in (None, "USA"):
        tokens, state_region = _strip_us_state(tokens)

    # Step 3: remaining = city
    city_raw = " ".join(tokens).strip() if tokens else None
    city = city_raw.title() if city_raw else None

    return city or None, state_region, country_code


def _classify_quality(
    city: str | None,
    state_region: str | None,
    country_code: str | None,
) -> str:
    """Classify the geographic quality of parsed components."""
    if city and state_region and country_code:
        return "city_state_country"
    if city and country_code and not state_region:
        return "city_country"
    if state_region and country_code and not city:
        return "state_country"
    if country_code and not city and not state_region:
        return "country_only"
    # Has some content but doesn't fit structured patterns
    if city or state_region:
        return "freeform_partial"
    return "freeform_partial"


def normalize_geo_hint(
    address_raw: str,
    source_address_row_id: str | None = None,
) -> GeoHint:
    """Normalize a raw address string into a GeoHint.

    Junk detection runs before token parsing.  Produces a canonical
    normalized_hint string for deduplication and display.
    """
    raw = (address_raw or "").strip()

    if not raw:
        return GeoHint(
            address_raw=raw,
            source_address_row_id=source_address_row_id,
            normalized_hint=None,
            city=None,
            state_region=None,
            country_code=None,
            quality_class="empty_or_null",
        )

    if _is_junk(raw):
        return GeoHint(
            address_raw=raw,
            source_address_row_id=source_address_row_id,
            normalized_hint=None,
            city=None,
            state_region=None,
            country_code=None,
            quality_class="non_address_junk",
        )

    city, state_region, country_code = _normalize_tokens(raw)

    quality_class = _classify_quality(city, state_region, country_code)

    # Build canonical normalized hint
    if quality_class == "freeform_partial" and not city and not state_region and not country_code:
        # Preserve a cleaned version of the raw string as the hint
        normalized_hint = re.sub(r"\s+", " ", raw.replace(",", " ")).strip().title()
    else:
        parts = [p for p in [city, state_region, country_code] if p]
        normalized_hint = " ".join(parts) if parts else None

    return GeoHint(
        address_raw=raw,
        source_address_row_id=source_address_row_id,
        normalized_hint=normalized_hint,
        city=city,
        state_region=state_region,
        country_code=country_code,
        quality_class=quality_class,
    )


def _dedup_key(hint: GeoHint) -> tuple[str | None, str | None, str | None]:
    """Canonical key for deduplication: (city, state_region, country_code)."""
    return (hint.city, hint.state_region, hint.country_code)


def _select_best_hint(
    hints: list[GeoHint],
    include_country_only: bool = False,
    allow_freeform_partial: bool = False,
) -> tuple[GeoHint | None, bool]:
    """Select the best geographic hint from a list and detect conflicts.

    Returns (best_hint | None, is_conflict).

    Priority order (highest first):
      city_state_country → city_country → state_country
      → freeform_partial (if allow_freeform_partial)
      → country_only (if include_country_only)

    Conflict: multiple hints at the same best quality class with different
    (city, state_region) pairs.
    """
    preferred: list[str] = ["city_state_country", "city_country", "state_country"]
    if allow_freeform_partial:
        preferred.append("freeform_partial")
    if include_country_only:
        preferred.append("country_only")

    for cls in preferred:
        candidates = [h for h in hints if h.quality_class == cls]
        if not candidates:
            continue

        # Deduplicate within this quality class by (city, state_region, country_code)
        seen: dict[tuple, GeoHint] = {}
        for h in candidates:
            key = _dedup_key(h)
            if key not in seen:
                seen[key] = h

        unique_locations = set(
            (h.city, h.state_region) for h in seen.values()
        )
        if len(unique_locations) > 1:
            return None, True  # conflict

        return next(iter(seen.values())), False

    return None, False  # no usable hint found


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class HoldGeoPrepareCounters:
    hold_candidates_considered: int = 0
    hold_candidates_with_address: int = 0
    geo_hints_normalized: int = 0
    geo_hints_deduplicated: int = 0
    geo_candidates_ready: int = 0
    geo_candidates_too_vague: int = 0
    geo_candidates_junk_location: int = 0
    geo_candidates_multi_location_conflict: int = 0
    geo_candidates_already_has_contact: int = 0
    geo_candidates_invalidated: int = 0   # stale enrichment_ready rows cleared
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if k != "warnings"}
        d["warnings"] = self.warnings
        return d


# ---------------------------------------------------------------------------
# DB write helpers
# ---------------------------------------------------------------------------

def _upsert_geo_hint(conn: psycopg.Connection, cid: str, hint: GeoHint, is_best: bool) -> None:
    """Idempotent upsert of a candidate_participant_geo_hint row."""
    conn.execute(
        """
        INSERT INTO candidate_participant_geo_hint
            (candidate_participant_id, source_address_row_id, address_raw,
             normalized_hint, city, state_region, country_code,
             quality_class, is_best_hint)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (candidate_participant_id, address_raw)
        DO UPDATE SET
            source_address_row_id = EXCLUDED.source_address_row_id,
            normalized_hint       = EXCLUDED.normalized_hint,
            city                  = EXCLUDED.city,
            state_region          = EXCLUDED.state_region,
            country_code          = EXCLUDED.country_code,
            quality_class         = EXCLUDED.quality_class,
            is_best_hint          = EXCLUDED.is_best_hint
        """,
        (
            cid,
            hint.source_address_row_id,
            hint.address_raw,
            hint.normalized_hint,
            hint.city,
            hint.state_region,
            hint.country_code,
            hint.quality_class,
            is_best,
        ),
    )


def _upsert_readiness(
    conn: psycopg.Connection,
    cid: str,
    readiness_status: str,
    reason_code: str,
    best_hint: GeoHint | None,
) -> None:
    """Idempotent upsert of a candidate_participant_enrichment_readiness row."""
    conn.execute(
        """
        INSERT INTO candidate_participant_enrichment_readiness
            (candidate_participant_id, readiness_status, reason_code,
             best_hint_text, best_city, best_state_region, best_country_code,
             evaluated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (candidate_participant_id) DO UPDATE SET
            readiness_status  = EXCLUDED.readiness_status,
            reason_code       = EXCLUDED.reason_code,
            best_hint_text    = EXCLUDED.best_hint_text,
            best_city         = EXCLUDED.best_city,
            best_state_region = EXCLUDED.best_state_region,
            best_country_code = EXCLUDED.best_country_code,
            evaluated_at      = EXCLUDED.evaluated_at
        """,
        (
            cid,
            readiness_status,
            reason_code,
            best_hint.normalized_hint if best_hint else None,
            best_hint.city if best_hint else None,
            best_hint.state_region if best_hint else None,
            best_hint.country_code if best_hint else None,
        ),
    )


# ---------------------------------------------------------------------------
# Main pipeline runner
# ---------------------------------------------------------------------------

def run_hold_geo_prepare(
    conn: psycopg.Connection,
    max_candidates: int | None = None,
    states: list[str] | None = None,
    include_country_only: bool = False,
    allow_freeform_partial: bool = False,
    dry_run: bool = False,
) -> HoldGeoPrepareCounters:
    """Normalize hold-pool addresses and classify enrichment readiness.

    Args:
        conn:                   Active psycopg connection (autocommit=False).
        max_candidates:         Cap on candidates processed (None = all).
        states:                 Resolution states to process (default: ['hold']).
        include_country_only:   Allow country_only hints to qualify as best hint.
        allow_freeform_partial: Allow freeform_partial hints as best hint.
        dry_run:                Caller rolls back after this returns.

    Returns:
        HoldGeoPrepareCounters with run statistics.
    """
    ctrs = HoldGeoPrepareCounters()
    target_states = states or ["hold"]
    limit_sql = f"LIMIT {int(max_candidates)}" if max_candidates is not None else ""

    # Invalidate stale enrichment_ready rows for candidates that are no longer
    # in scope: wrong state, promoted, or lost all usable address rows.
    # This prevents RocketReach (require_geo_ready=True) from selecting
    # candidates whose readiness was computed under now-stale conditions.
    result = conn.execute(
        """
        UPDATE candidate_participant_enrichment_readiness cper
        SET readiness_status = 'not_hold',
            reason_code      = 'invalidated_out_of_scope',
            evaluated_at     = now()
        WHERE cper.readiness_status = 'enrichment_ready'
          AND NOT EXISTS (
              SELECT 1
              FROM candidate_participant cp
              JOIN candidate_participant_address cpa
                ON cpa.candidate_participant_id = cp.id
              WHERE cp.id = cper.candidate_participant_id
                AND cp.resolution_state = ANY(%s)
                AND cp.is_promoted = false
                AND cpa.address_raw IS NOT NULL
                AND cpa.address_raw <> ''
          )
        """,
        (target_states,),
    )
    ctrs.geo_candidates_invalidated = result.rowcount

    # Fetch hold candidates that have at least one address child row.
    # DISTINCT to avoid duplicate rows if a candidate has multiple addresses.
    candidate_rows = conn.execute(
        f"""
        SELECT DISTINCT cp.id::text, cp.normalized_name,
               cp.best_email, cp.best_phone, cp.resolution_state
        FROM candidate_participant cp
        JOIN candidate_participant_address cpa
          ON cpa.candidate_participant_id = cp.id
        WHERE cp.resolution_state = ANY(%s)
          AND cp.is_promoted = false
          AND cpa.address_raw IS NOT NULL
          AND cpa.address_raw <> ''
        ORDER BY cp.id::text
        {limit_sql}
        """,
        (target_states,),
    ).fetchall()

    for idx, crow in enumerate(candidate_rows):
        cid, norm_name, best_email, best_phone, res_state = (
            crow[0], crow[1], crow[2], crow[3], crow[4]
        )
        ctrs.hold_candidates_considered += 1

        # Check for effective email/phone evidence (top-level OR child contact)
        has_contact = bool(best_email or best_phone)
        if not has_contact:
            child = conn.execute(
                """
                SELECT 1 FROM candidate_participant_contact
                WHERE candidate_participant_id = %s
                  AND normalized_value IS NOT NULL
                  AND contact_type IN ('email', 'phone')
                LIMIT 1
                """,
                (cid,),
            ).fetchone()
            has_contact = child is not None

        # Load all address rows for this candidate
        addr_rows = conn.execute(
            """
            SELECT id::text, address_raw
            FROM candidate_participant_address
            WHERE candidate_participant_id = %s
              AND address_raw IS NOT NULL AND address_raw <> ''
            ORDER BY created_at ASC, id ASC
            """,
            (cid,),
        ).fetchall()

        if not addr_rows:
            continue  # no address evidence — shouldn't happen due to join above

        ctrs.hold_candidates_with_address += 1

        # Normalize all address rows
        all_hints: list[GeoHint] = []
        for addr_id, addr_raw in addr_rows:
            hint = normalize_geo_hint(addr_raw, source_address_row_id=str(addr_id))
            all_hints.append(hint)
            ctrs.geo_hints_normalized += 1

        # Deduplicate: detect how many hints share the same geo key
        seen_keys: set[tuple] = set()
        for hint in all_hints:
            if hint.quality_class in ("empty_or_null", "non_address_junk"):
                continue
            key = _dedup_key(hint)
            if key in seen_keys:
                ctrs.geo_hints_deduplicated += 1
            else:
                seen_keys.add(key)

        # Select best hint
        best_hint, is_conflict = _select_best_hint(
            all_hints,
            include_country_only=include_country_only,
            allow_freeform_partial=allow_freeform_partial,
        )

        # Determine readiness status
        if has_contact:
            readiness_status = READINESS_ALREADY_HAS_CONTACT
            reason_code = "effective_email_or_phone_present"
        elif is_conflict:
            readiness_status = READINESS_MULTI_CONFLICT
            reason_code = "multiple_distinct_city_state_hints"
        elif best_hint is None:
            # Determine whether hints are junk or just vague
            non_empty = [
                h for h in all_hints
                if h.quality_class not in ("empty_or_null",)
            ]
            if non_empty and all(h.quality_class == "non_address_junk" for h in non_empty):
                readiness_status = READINESS_JUNK_LOCATION
                reason_code = "all_hints_non_address_junk"
            else:
                readiness_status = READINESS_TOO_VAGUE
                reason_code = "no_usable_geographic_hint"
        elif not norm_name:
            readiness_status = READINESS_TOO_VAGUE
            reason_code = "missing_normalized_name"
        else:
            readiness_status = READINESS_ENRICHMENT_READY
            reason_code = f"best_hint_quality={best_hint.quality_class}"

        # Update counters
        if readiness_status == READINESS_ENRICHMENT_READY:
            ctrs.geo_candidates_ready += 1
        elif readiness_status == READINESS_TOO_VAGUE:
            ctrs.geo_candidates_too_vague += 1
        elif readiness_status == READINESS_JUNK_LOCATION:
            ctrs.geo_candidates_junk_location += 1
        elif readiness_status == READINESS_MULTI_CONFLICT:
            ctrs.geo_candidates_multi_location_conflict += 1
        elif readiness_status == READINESS_ALREADY_HAS_CONTACT:
            ctrs.geo_candidates_already_has_contact += 1

        # Write to DB inside a SAVEPOINT for per-candidate isolation
        sp = f"geo_prep_{idx}"
        conn.execute(f"SAVEPOINT {sp}")
        try:
            # Write geo_hint rows for all addresses
            for hint in all_hints:
                is_best = (best_hint is not None and hint is best_hint)
                _upsert_geo_hint(conn, cid, hint, is_best)

            # Write readiness row
            _upsert_readiness(conn, cid, readiness_status, reason_code, best_hint)

            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
            ctrs.db_errors += 1
            ctrs.warnings.append(f"geo_prepare cid={cid}: {exc}")

    return ctrs


# ---------------------------------------------------------------------------
# Report formatter
# ---------------------------------------------------------------------------

def build_geo_prepare_report(ctrs: HoldGeoPrepareCounters, dry_run: bool = False) -> str:
    lines = [
        "=" * 60,
        "Hold-Pool Geo Prepare Report",
        f"  dry_run: {dry_run}",
        "=" * 60,
        f"  hold candidates considered:   {ctrs.hold_candidates_considered}",
        f"  hold candidates with address: {ctrs.hold_candidates_with_address}",
        f"  geo hints normalized:         {ctrs.geo_hints_normalized}",
        f"  geo hints deduplicated:       {ctrs.geo_hints_deduplicated}",
        "Readiness classification:",
        f"  enrichment_ready:             {ctrs.geo_candidates_ready}",
        f"  too_vague:                    {ctrs.geo_candidates_too_vague}",
        f"  junk_location:                {ctrs.geo_candidates_junk_location}",
        f"  multi_location_conflict:      {ctrs.geo_candidates_multi_location_conflict}",
        f"  already_has_contact:          {ctrs.geo_candidates_already_has_contact}",
        f"  invalidated (out-of-scope):   {ctrs.geo_candidates_invalidated}",
        f"DB errors:                      {ctrs.db_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")
    lines.append("=" * 60)
    return "\n".join(lines)
