"""Normalization functions for regattaman CSV ingestion.

All functions accept str | None and return the appropriate type or None.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
import urllib.parse
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

_SENTINEL_TS = "0000-00-00 00:00:00"
_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
_US_COUNTRY_TOKEN_RE = re.compile(
    r"\b(?:united states(?: of america)?|usa|us)\b",
    re.IGNORECASE,
)
_TRAILING_POSTAL_RE = re.compile(
    r"(?:\||,|\s)(\d{5}(?:-\d{4})?|\d{4})"
    r"(?:\s*(?:united states(?: of america)?|usa|us))?\s*$",
    re.IGNORECASE,
)
_EMAIL_LIKE_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_CA_POSTAL_RE = re.compile(r"^[A-Z]\d[A-Z]\d[A-Z]\d$")
_NAME_TOKEN_CLEAN_RE = re.compile(r"^[\s,]+|[\s,]+$")
_NAME_INITIAL_RE = re.compile(r"^[A-Za-z]\.?$")
_ADDRESS_UNIT_RE = re.compile(
    r"^(?P<line1>.*?)(?:\s+(?P<line2>"
    r"(?:apt|apartment|unit|suite|ste|floor|fl|#)\s*[A-Za-z0-9\-\/]+.*"
    r"))$",
    re.IGNORECASE,
)
_UNIT_HEAD_TOKENS = {
    "apt",
    "apartment",
    "unit",
    "suite",
    "ste",
    "floor",
    "fl",
}
_STREET_MARKER_TOKENS = {
    "aly",
    "ave",
    "avenue",
    "blvd",
    "boulevard",
    "box",
    "cir",
    "circle",
    "court",
    "ct",
    "dr",
    "drive",
    "hwy",
    "highway",
    "ln",
    "lane",
    "pkwy",
    "parkway",
    "pl",
    "place",
    "point",
    "rd",
    "road",
    "st",
    "street",
    "ter",
    "terrace",
    "trail",
    "trl",
    "way",
}

_NAME_PREFIX_MAP: dict[str, str] = {
    "mr": "Mr",
    "mrs": "Mrs",
    "ms": "Ms",
    "miss": "Miss",
    "dr": "Dr",
    "prof": "Prof",
    "rev": "Rev",
    "fr": "Fr",
    "capt": "Capt",
    "captain": "Capt",
    "cmdr": "Cmdr",
    "lt": "Lt",
    "col": "Col",
    "gen": "Gen",
    "hon": "Hon",
    "sir": "Sir",
    "lady": "Lady",
}

_NAME_SUFFIX_MAP: dict[str, str] = {
    "jr": "Jr",
    "sr": "Sr",
    "ii": "II",
    "iii": "III",
    "iv": "IV",
    "v": "V",
    "esq": "Esq",
    "phd": "PhD",
    "md": "MD",
    "dds": "DDS",
    "dmd": "DMD",
    "jd": "JD",
}

_SURNAME_PARTICLES = {
    "al",
    "ap",
    "ben",
    "bin",
    "da",
    "dal",
    "de",
    "del",
    "della",
    "der",
    "di",
    "dos",
    "du",
    "ibn",
    "la",
    "le",
    "mac",
    "mc",
    "st",
    "van",
    "von",
}

_US_STATE_NAME_TO_CODE: dict[str, str] = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "districtofcolumbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "newhampshire": "NH",
    "newjersey": "NJ",
    "newmexico": "NM",
    "newyork": "NY",
    "northcarolina": "NC",
    "northdakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhodeisland": "RI",
    "southcarolina": "SC",
    "southdakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "westvirginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}

_CA_PROVINCE_NAME_TO_CODE: dict[str, str] = {
    "alberta": "AB",
    "britishcolumbia": "BC",
    "manitoba": "MB",
    "newbrunswick": "NB",
    "newfoundlandandlabrador": "NL",
    "northwestterritories": "NT",
    "novascotia": "NS",
    "nunavut": "NU",
    "ontario": "ON",
    "princeedwardisland": "PE",
    "quebec": "QC",
    "saskatchewan": "SK",
    "yukon": "YT",
}


@dataclass(frozen=True)
class NameParts:
    first_name: str | None
    middle_name: str | None
    last_name: str | None
    name_prefix: str | None
    name_suffix: str | None


@dataclass(frozen=True)
class AddressParts:
    line1: str | None
    line2: str | None
    city: str | None
    state: str | None
    postal_code: str | None
    country_code: str | None


# ---------------------------------------------------------------------------
# Rule 1: trim
# ---------------------------------------------------------------------------

def trim(value: str | None) -> str | None:
    """Strip leading/trailing whitespace; treat empty string as None."""
    if value is None:
        return None
    v = value.strip()
    return v if v else None


# ---------------------------------------------------------------------------
# Rule 2: normalize_space
# ---------------------------------------------------------------------------

def normalize_space(value: str | None) -> str | None:
    """Collapse internal runs of whitespace to single spaces, then trim."""
    v = trim(value)
    if v is None:
        return None
    return re.sub(r"\s+", " ", v)


# ---------------------------------------------------------------------------
# Rule 3: normalize_email
# ---------------------------------------------------------------------------

def normalize_email(value: str | None) -> str | None:
    """Lowercase and trim an email address."""
    v = trim(value)
    if v is None:
        return None
    return v.lower()


def looks_like_email(value: str | None) -> bool:
    """Return True when value has the minimal expected shape of an email."""
    v = trim(value)
    if v is None:
        return False
    return bool(_EMAIL_LIKE_RE.match(v))


# ---------------------------------------------------------------------------
# Rule 4: normalize_phone
# ---------------------------------------------------------------------------

def normalize_phone(value: str | None) -> str | None:
    """Return E.164-style phone or None.

    Keeps digits only.  10-digit → +1XXXXXXXXXX.
    11-digit starting with 1 → +1XXXXXXXXXX.
    Anything else → None (insufficient or non-standard digit count).

    FOR-226: Previously passed 7–9-digit values through as "+NNNNNNN…", producing
    malformed E.164.  These are almost always truncated or erroneous entries.
    """
    v = trim(value)
    if v is None:
        return None
    digits = re.sub(r"\D", "", v)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


# ---------------------------------------------------------------------------
# Org-entity detection  (FOR-222)
# ---------------------------------------------------------------------------

_ORG_PHRASE_RE = re.compile(
    r"\b("
    r"yacht\s+club|sailing\s+club|boat\s+club|cruising\s+club|racing\s+club|"
    r"coast\s+guard|yacht\s+squad|fleet|squadron|"
    r"foundation|association|assoc\.?|society|"
    r"team|committee|regatta|authority|district|"
    r"university|college|school|academy|"
    r"department|dept\.?|division|bureau|agency|"
    r"international|national|state\s+of|town\s+of|city\s+of|"
    r"charity|nonprofit|non-profit"
    r")\b",
    re.IGNORECASE,
)
_ORG_SUFFIX_RE = re.compile(
    r"(?:^|[\s,])("
    r"l\.?l\.?c\.?|inc\.?|corp\.?|ltd\.?|llp\.?|lp\.?|p\.?c\.?|"
    r"trust|estate"
    r")\.?$",
    re.IGNORECASE,
)


def is_likely_org_name(name: str | None) -> bool:
    """Return True when name looks like an organization rather than a person.

    Used to flag candidate_participant records that are orgs in disguise (FOR-222).
    Detection is intentionally conservative: a false positive (real person with a
    club-like token in their name) is less harmful than silently promoting an org as
    a canonical person. Downstream scoring/promotion can reject flagged candidates
    from the participant path.
    """
    if not name:
        return False
    return bool(_ORG_PHRASE_RE.search(name) or _ORG_SUFFIX_RE.search(name))


def normalize_country_code(value: str | None) -> str | None:
    """Normalize a country value to ISO-3166 alpha-2 when possible.

    Core requirement (direct-mail): emit/store two-letter codes, especially:
      - United States variants -> US
      - Canada variants        -> CA
    """
    v = trim(value)
    if v is None:
        return None

    token = re.sub(r"[^A-Za-z]", "", v).upper()
    if not token:
        return None

    if token in {"US", "USA", "UNITEDSTATES", "UNITEDSTATESOFAMERICA"}:
        return "US"
    if token in {"CA", "CAN", "CANADA"}:
        return "CA"
    if len(token) == 2:
        return token
    return None


def normalize_postal_code(value: str | None) -> str | None:
    """Normalize a postal code for identity comparisons.

    Current behavior is intentionally conservative and US-centric because the
    Mailchimp address corroboration issues we are resolving are dominated by
    leading-zero loss and ZIP+4 formatting differences.
    """
    v = trim(value)
    if v is None:
        return None
    if re.search(r"[a-z]", v, re.IGNORECASE):
        return None
    digits = re.sub(r"\D", "", v)
    if len(digits) == 9:
        return digits[:5]
    if len(digits) == 5:
        return digits
    if len(digits) == 4:
        return f"0{digits}"
    return None


def normalize_postal_code_for_storage(
    value: str | None,
    country_code: str | None = None,
) -> str | None:
    """Normalize postal code for durable storage.

    US/default:
      - 4-digit -> zero-padded 5-digit
      - 5-digit -> keep
      - ZIP+4   -> base 5-digit
    CA:
      - canonical A1A 1A1 spacing/casing when parseable
      - otherwise uppercase/trimmed raw value (no data loss)
    """
    v = trim(value)
    if v is None:
        return None

    cc = normalize_country_code(country_code)
    if cc == "CA":
        token = re.sub(r"\s+", "", v).upper()
        if _CA_POSTAL_RE.match(token):
            return f"{token[:3]} {token[3:]}"
        return token

    normalized_us = normalize_postal_code(v)
    if normalized_us is not None:
        return normalized_us
    return v


def split_address_line1_line2(value: str | None) -> tuple[str | None, str | None]:
    """Split a line1 string into (line1, line2) when a unit marker is present."""
    line = normalize_space(value)
    if not line:
        return (None, None)
    match = _ADDRESS_UNIT_RE.match(line)
    if not match:
        return (line, None)
    primary = normalize_space(match.group("line1"))
    secondary = normalize_space(match.group("line2"))
    if not primary or not secondary:
        return (line, None)
    return (primary, secondary)


def _normalize_state_or_province(
    value: str | None,
    country_code: str | None = None,
) -> str | None:
    token = normalize_space(value)
    if not token:
        return None
    compact = re.sub(r"[^A-Za-z]", "", token)
    if not compact:
        return None
    if len(compact) == 2:
        return compact.upper()
    key = compact.lower()
    cc = normalize_country_code(country_code)
    if cc == "CA":
        return _CA_PROVINCE_NAME_TO_CODE.get(key) or _US_STATE_NAME_TO_CODE.get(key)
    return _US_STATE_NAME_TO_CODE.get(key) or _CA_PROVINCE_NAME_TO_CODE.get(key)


def _looks_like_postal(value: str | None) -> bool:
    v = normalize_space(value)
    if not v:
        return False
    if normalize_postal_code(v) is not None:
        return True
    compact = re.sub(r"\s+", "", v).upper()
    return bool(_CA_POSTAL_RE.match(compact))


def _normalize_token_alnum_lower(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _normalize_token_alpha_upper(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^A-Za-z]", "", value).upper()


def _explicit_country_code(value: str | None) -> str | None:
    """Return country code only for explicit country tokens.

    Important: unlike normalize_country_code, this function intentionally does
    not treat arbitrary 2-letter codes as countries (to avoid misreading US
    state tokens like MA/NH as country codes while parsing addresses).
    """
    token = _normalize_token_alpha_upper(value)
    if token in {"US", "USA", "UNITEDSTATES", "UNITEDSTATESOFAMERICA"}:
        return "US"
    if token in {"CA", "CAN", "CANADA"}:
        return "CA"
    return None


def _looks_like_unit_prefix(value: str | None) -> bool:
    v = normalize_space(value)
    if not v:
        return False
    first = v.split()[0]
    normalized = _normalize_token_alnum_lower(first)
    return first.startswith("#") or normalized in _UNIT_HEAD_TOKENS


def _split_line1_and_city_from_overloaded_prefix(value: str | None) -> tuple[str | None, str | None]:
    """Split '<street [unit] city>' into line1/city when city was overloaded."""
    text = normalize_space(value)
    if not text:
        return (None, None)

    tokens = text.split()
    if len(tokens) < 2:
        return (None, None)

    cut: int | None = None

    # PO Box pattern: "PO Box <num> <city...>"
    if len(tokens) >= 4:
        t0 = _normalize_token_alnum_lower(tokens[0])
        t1 = _normalize_token_alnum_lower(tokens[1])
        if (t0, t1) == ("po", "box") or t0 == "pobox":
            cut = 3

    for idx, token in enumerate(tokens):
        normalized = _normalize_token_alnum_lower(token)
        if normalized in _STREET_MARKER_TOKENS:
            cut = max(cut or 0, idx + 1)
        if token.startswith("#"):
            cut = max(cut or 0, idx + 1)
            continue
        if normalized in _UNIT_HEAD_TOKENS and idx + 1 < len(tokens):
            cut = max(cut or 0, idx + 2)

    if cut is not None and 0 < cut < len(tokens):
        line1 = normalize_space(" ".join(tokens[:cut]).strip(", "))
        city = normalize_space(" ".join(tokens[cut:]).strip(", "))
        if line1 and city:
            return (line1, city)

    # Conservative fallback when no marker found.
    if len(tokens) >= 4:
        line1 = normalize_space(" ".join(tokens[:-2]).strip(", "))
        city = normalize_space(" ".join(tokens[-2:]).strip(", "))
        if line1 and city:
            return (line1, city)
    if len(tokens) >= 3:
        line1 = normalize_space(" ".join(tokens[:-1]).strip(", "))
        city = normalize_space(tokens[-1].strip(", "))
        if line1 and city:
            return (line1, city)

    return (None, None)


def _parse_overloaded_address_line(
    value: str | None,
    fallback_country_code: str | None = None,
) -> AddressParts | None:
    """Parse addresses like '43 Webb Road Edgecomb, Maine' (no ZIP)."""
    text = normalize_space(value)
    if not text:
        return None

    country = normalize_country_code(fallback_country_code)
    compact = re.sub(r"\s*,\s*", " ", text).strip()
    tokens = [tok for tok in compact.split() if tok]
    if len(tokens) < 3:
        return None

    # Remove explicit country suffix if present.
    for span in (4, 3, 2, 1):
        if len(tokens) >= span:
            maybe_country = " ".join(tokens[-span:])
            explicit_country = _explicit_country_code(maybe_country)
            if explicit_country:
                country = explicit_country
                tokens = tokens[:-span]
                break

    postal = None
    if len(tokens) >= 2:
        maybe_ca = f"{tokens[-2]} {tokens[-1]}"
        maybe_ca_norm = normalize_postal_code_for_storage(maybe_ca, "CA")
        if maybe_ca_norm and _CA_POSTAL_RE.match(maybe_ca_norm.replace(" ", "")):
            postal = maybe_ca_norm
            country = country or "CA"
            tokens = tokens[:-2]

    if postal is None and tokens:
        cc_hint = country
        if cc_hint is None:
            cc_hint = "CA" if re.search(r"[A-Za-z]", tokens[-1]) else "US"
        maybe_postal = normalize_postal_code_for_storage(tokens[-1], cc_hint)
        if maybe_postal and _looks_like_postal(maybe_postal):
            postal = maybe_postal
            if _CA_POSTAL_RE.match(maybe_postal.replace(" ", "")):
                country = country or "CA"
            elif re.fullmatch(r"\d{5}", maybe_postal):
                country = country or "US"
            tokens = tokens[:-1]

    state = None
    for span in (3, 2, 1):
        if len(tokens) >= span:
            maybe_state = " ".join(tokens[-span:])
            parsed_state = _normalize_state_or_province(maybe_state, country)
            if parsed_state:
                state = parsed_state
                tokens = tokens[:-span]
                break
    if not state:
        return None

    pre = normalize_space(" ".join(tokens))
    line1, city = _split_line1_and_city_from_overloaded_prefix(pre)
    if not line1 or not city:
        return None

    line1_split, line2 = split_address_line1_line2(line1)
    return AddressParts(
        line1=line1_split or line1,
        line2=line2,
        city=city,
        state=state,
        postal_code=normalize_postal_code_for_storage(postal, country),
        country_code=normalize_country_code(country),
    )


def _parse_city_state_postal_country(
    value: str | None,
    fallback_country_code: str | None = None,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Parse '<city> <state/province> <postal> [country]' tail strings."""
    text = normalize_space(value)
    if not text:
        return (None, None, None, normalize_country_code(fallback_country_code))

    tokens = text.split()
    country = normalize_country_code(fallback_country_code)
    if tokens:
        inferred = _explicit_country_code(tokens[-1])
        if inferred:
            country = inferred
            tokens = tokens[:-1]

    postal = None
    if len(tokens) >= 2:
        maybe_ca = f"{tokens[-2]} {tokens[-1]}"
        maybe_ca_norm = normalize_postal_code_for_storage(maybe_ca, "CA")
        if maybe_ca_norm and _CA_POSTAL_RE.match(maybe_ca_norm.replace(" ", "")):
            postal = maybe_ca_norm
            country = country or "CA"
            tokens = tokens[:-2]

    if postal is None and tokens:
        cc_hint = country
        if cc_hint is None:
            cc_hint = "CA" if re.search(r"[A-Za-z]", tokens[-1]) else "US"
        maybe_norm = normalize_postal_code_for_storage(tokens[-1], cc_hint)
        if maybe_norm and _looks_like_postal(maybe_norm):
            postal = maybe_norm
            if _CA_POSTAL_RE.match(maybe_norm.replace(" ", "")):
                country = country or "CA"
            elif re.fullmatch(r"\d{5}", maybe_norm):
                country = country or "US"
            tokens = tokens[:-1]

    state = None
    if tokens:
        state = _normalize_state_or_province(tokens[-1], country)
        if state:
            tokens = tokens[:-1]

    city = normalize_space(" ".join(tokens))
    return (city, state, postal, normalize_country_code(country))


def parse_mailing_address_components(
    value: str | None,
    *,
    fallback_country_code: str | None = None,
) -> AddressParts:
    """Best-effort parser for common US/CA freeform mailing addresses."""
    text = normalize_space(value)
    if not text:
        return AddressParts(None, None, None, None, None, normalize_country_code(fallback_country_code))

    segments = [normalize_space(part) for part in re.split(r"[|,]", text) if normalize_space(part)]
    if not segments:
        return AddressParts(None, None, None, None, None, normalize_country_code(fallback_country_code))

    line1_raw = segments[0]
    line1, line2 = split_address_line1_line2(line1_raw)
    country = normalize_country_code(fallback_country_code)
    city = None
    state = None
    postal = None

    if len(segments) >= 3:
        city = segments[1]
        _, state, postal, country = _parse_city_state_postal_country(
            " ".join(segments[2:]),
            country,
        )
        if _looks_like_unit_prefix(city):
            city_tokens = city.split()
            first_token = city_tokens[0] if city_tokens else ""
            first_norm = _normalize_token_alnum_lower(first_token)
            if first_token.startswith("#") and len(city_tokens) >= 2:
                merged_line1 = normalize_space(f"{line1} {first_token}")
                line1, line2 = split_address_line1_line2(merged_line1)
                city = normalize_space(" ".join(city_tokens[1:]))
            elif first_norm in _UNIT_HEAD_TOKENS and len(city_tokens) >= 3:
                merged_line1 = normalize_space(f"{line1} {' '.join(city_tokens[:2])}")
                line1, line2 = split_address_line1_line2(merged_line1)
                city = normalize_space(" ".join(city_tokens[2:]))
    elif len(segments) == 2:
        city, state, postal, country = _parse_city_state_postal_country(
            segments[1],
            country,
        )

    if not line1:
        line1 = line1_raw

    needs_fallback = (
        not city
        or not state
        or _looks_like_unit_prefix(city)
    )
    if needs_fallback:
        overloaded = _parse_overloaded_address_line(text, fallback_country_code=country)
        if overloaded:
            line1 = overloaded.line1 or line1
            if not city or _looks_like_unit_prefix(city):
                city = overloaded.city or city
            if overloaded.line2:
                if not line2:
                    line2 = overloaded.line2
                elif city and normalize_space(line2).lower().endswith(normalize_space(city).lower()):
                    line2 = overloaded.line2
            state = overloaded.state or state
            postal = overloaded.postal_code or postal
            country = overloaded.country_code or country

    postal = normalize_postal_code_for_storage(postal, country)
    return AddressParts(
        line1=line1,
        line2=line2,
        city=normalize_space(city),
        state=state,
        postal_code=postal,
        country_code=normalize_country_code(country),
    )


def normalize_address_for_identity(value: str | None) -> tuple[str | None, str | None]:
    """Return a canonical (address_body, postal_code) pair for identity checks.

    The body comparison is strict once formatting noise is removed:
    punctuation, separators, casing, and trailing US country tokens are ignored.
    Postal codes are normalized separately so ZIP+4 and dropped leading zeros
    don't create false mismatches.
    """
    v = trim(value)
    if v is None:
        return (None, None)

    v = unicodedata.normalize("NFKD", v)
    v = "".join(c for c in v if not unicodedata.combining(c))
    v = v.lower()

    postal_code = None
    trailing_postal = _TRAILING_POSTAL_RE.search(v)
    if trailing_postal:
        postal_code = normalize_postal_code(trailing_postal.group(1))
        v = f"{v[:trailing_postal.start()]} {v[trailing_postal.end():]}"

    v = _US_COUNTRY_TOKEN_RE.sub(" ", v)
    v = v.replace("|", " ")
    v = re.sub(r"[^a-z0-9]+", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return (v or None, postal_code)


def addresses_match_for_identity(source: str | None, target: str | None) -> bool:
    """Return True when two addresses agree for strict identity corroboration.

    The street/city/state body must match after removing formatting noise.
    Postal codes are enforced only when both sides provide a parseable value,
    which prevents incomplete stored addresses from creating false negatives.
    """
    source_body, source_postal = normalize_address_for_identity(source)
    target_body, target_postal = normalize_address_for_identity(target)

    if source_body != target_body:
        return False
    if source_postal and target_postal and source_postal != target_postal:
        return False
    return True


# ---------------------------------------------------------------------------
# Rule 5: normalize_name  (for participant lookup/matching)
# ---------------------------------------------------------------------------

def normalize_name(value: str | None) -> str | None:
    """Lowercase, remove punctuation except spaces, collapse spaces.

    Used for participant resolution lookups — not stored directly as
    normalized_name in clubs/events/yachts (use slug_name for those).
    """
    v = trim(value)
    if v is None:
        return None
    # Decompose unicode (e.g. accented chars) then drop combining marks
    v = unicodedata.normalize("NFKD", v)
    v = "".join(c for c in v if not unicodedata.combining(c))
    v = v.lower()
    # Remove punctuation except spaces
    v = re.sub(r"[^\w\s]", "", v)
    # Collapse whitespace
    v = re.sub(r"\s+", " ", v).strip()
    return v if v else None


def normalize_person_name_for_identity(value: str | None) -> str | None:
    """Canonical person-name normalizer for participant identity matching.

    Unlike normalize_name(), this helper canonicalizes comma-order variants:
      - "Smith, John" -> "john smith"
      - "John Smith"  -> "john smith"

    For non-comma names, token order is preserved.
    """
    v = normalize_space(value)
    if v is None:
        return None

    first, last = parse_name_parts(v)
    canonical = " ".join(part for part in (first, last) if part)
    if not canonical:
        if looks_like_email(v):
            return None
        canonical = v
    return normalize_name(canonical)


def participant_name_lookup_keys(value: str | None) -> tuple[str, ...]:
    """Return preferred + legacy participant-name lookup keys.

    Key order:
    1) normalize_person_name_for_identity(value)  (preferred; comma-order aware)
    2) normalize_name(value)                      (legacy fallback)
    """
    keys: list[str] = []
    preferred = normalize_person_name_for_identity(value)
    if preferred:
        keys.append(preferred)
    legacy = normalize_name(value)
    if legacy and legacy not in keys:
        keys.append(legacy)
    return tuple(keys)


def participant_legacy_comma_lookup_key(value: str | None) -> str | None:
    """Return legacy comma-order key for matching pre-FOR-205 participant rows.

    Example:
      - "John Smith"  -> "smith john"
      - "Smith, John" -> "smith john"
    """
    v = normalize_space(value)
    if not v:
        return None
    first, last = parse_name_parts(v)
    if not first or not last:
        return None
    return normalize_name(f"{last} {first}")


# ---------------------------------------------------------------------------
# Rule 6: slug_name  (for normalized_name DB fields on clubs/events/yachts)
# ---------------------------------------------------------------------------

def slug_name(value: str | None) -> str | None:
    """Lowercase alnum with '-' separators.

    Used for normalized_name columns in yacht_club, event_series,
    event_instance, and yacht tables.
    """
    v = trim(value)
    if v is None:
        return None
    v = unicodedata.normalize("NFKD", v)
    v = "".join(c for c in v if not unicodedata.combining(c))
    v = v.lower()
    v = re.sub(r"[^a-z0-9]+", "-", v)
    v = v.strip("-")
    return v if v else None


# ---------------------------------------------------------------------------
# Rule 7: parse_ts
# ---------------------------------------------------------------------------

def parse_ts(value: str | None) -> datetime | None:
    """Parse '%Y-%m-%d %H:%M:%S'.  Sentinel '0000-00-00 00:00:00' → None."""
    v = trim(value)
    if v is None or v == _SENTINEL_TS:
        return None
    try:
        return datetime.strptime(v, _TS_FORMAT)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Rule 8: parse_date_from_ts
# ---------------------------------------------------------------------------

def parse_date_from_ts(value: str | None) -> date | None:
    """Return the date portion of a parsed timestamp, or None."""
    ts = parse_ts(value)
    return ts.date() if ts is not None else None


def parse_date(value: str | None) -> date | None:
    """Parse '%b %d, %Y'. e.g. 'Jul 23, 2025' → date(2025, 7, 23)."""
    v = trim(value)
    if v is None:
        return None
    try:
        return datetime.strptime(v, "%b %d, %Y").date()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Rule 9: parse_numeric
# ---------------------------------------------------------------------------

def parse_numeric(value: str | None) -> Decimal | None:
    """Parse a decimal number from a string, returning None on failure."""
    v = trim(value)
    if v is None:
        return None
    try:
        return Decimal(v)
    except InvalidOperation:
        return None


def split_signed_document_urls(value: str | None) -> list[str]:
    """Split a multiline string of URLs into a list of URLs."""
    v = trim(value)
    if v is None:
        return []
    return [line.strip() for line in v.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Helper: parse_name_parts
# ---------------------------------------------------------------------------

def _clean_name_token(value: str | None) -> str | None:
    token = _NAME_TOKEN_CLEAN_RE.sub("", value or "")
    token = normalize_space(token)
    return token


def _name_token_key(value: str | None) -> str:
    token = _clean_name_token(value)
    if not token:
        return ""
    return token.lower().replace(".", "")


def _normalize_middle_tokens(tokens: list[str]) -> list[str]:
    normalized: list[str] = []
    for token in tokens:
        clean = _clean_name_token(token)
        if not clean:
            continue
        key = _name_token_key(clean)
        if len(key) == 1 and key.isalpha():
            normalized.append(key.upper())
        else:
            normalized.append(clean)
    return normalized


def _pop_name_prefix(tokens: list[str]) -> tuple[str | None, list[str]]:
    prefix_tokens: list[str] = []
    remaining = list(tokens)
    while remaining:
        key = _name_token_key(remaining[0])
        canonical = _NAME_PREFIX_MAP.get(key)
        if canonical is None:
            break
        prefix_tokens.append(canonical)
        remaining.pop(0)
    prefix = " ".join(prefix_tokens) if prefix_tokens else None
    return prefix, remaining


def _pop_name_suffix(tokens: list[str]) -> tuple[str | None, list[str]]:
    suffix_tokens: list[str] = []
    remaining = list(tokens)
    while remaining:
        key = _name_token_key(remaining[-1])
        canonical = _NAME_SUFFIX_MAP.get(key)
        if canonical is None:
            break
        suffix_tokens.insert(0, canonical)
        remaining.pop()
    suffix = " ".join(suffix_tokens) if suffix_tokens else None
    return suffix, remaining


def _split_given_and_last(tokens: list[str]) -> tuple[list[str], list[str]]:
    if not tokens:
        return [], []
    if len(tokens) == 1:
        return [tokens[0]], []

    # Start with the right-most token as the surname, then absorb recognized
    # surname particles from right-to-left (e.g., "de la Cruz", "van der Meer").
    last_start = len(tokens) - 1
    idx = last_start - 1
    while idx >= 1:
        key = _name_token_key(tokens[idx])
        if key in _SURNAME_PARTICLES:
            last_start = idx
            idx -= 1
            continue
        break

    return tokens[:last_start], tokens[last_start:]


def parse_person_name_parts(full_name: str | None) -> NameParts:
    """Parse a person name into structured components.

    Business rules:
    - Handles honorific prefixes (Mr/Dr/etc.) and suffixes (Jr/III/etc.).
    - Preserves hyphenated surnames.
    - Preserves multi-token surnames with particles (de, van, von, etc.).
    - Preserves multiple middle initials/tokens in middle_name.
    """
    v = normalize_space(full_name)
    if not v or looks_like_email(v):
        return NameParts(None, None, None, None, None)

    if "," in v:
        left, right = v.split(",", 1)
        last_tokens = [_clean_name_token(t) for t in left.split()]
        last_tokens = [t for t in last_tokens if t]

        given_tokens = [_clean_name_token(t) for t in right.split()]
        given_tokens = [t for t in given_tokens if t]
        prefix, given_tokens = _pop_name_prefix(given_tokens)
        suffix, given_tokens = _pop_name_suffix(given_tokens)

        first_name = given_tokens[0] if given_tokens else None
        middle_tokens = _normalize_middle_tokens(given_tokens[1:]) if len(given_tokens) > 1 else []
        middle_name = " ".join(middle_tokens) if middle_tokens else None
        last_name = " ".join(last_tokens) if last_tokens else None

        if first_name and looks_like_email(first_name):
            first_name = None
        if last_name and looks_like_email(last_name):
            last_name = None
        return NameParts(first_name, middle_name, last_name, prefix, suffix)

    tokens = [_clean_name_token(t) for t in v.split()]
    tokens = [t for t in tokens if t]
    prefix, tokens = _pop_name_prefix(tokens)
    suffix, tokens = _pop_name_suffix(tokens)

    if not tokens:
        return NameParts(None, None, None, prefix, suffix)
    if len(tokens) == 1:
        first_name = None if looks_like_email(tokens[0]) else tokens[0]
        return NameParts(first_name, None, None, prefix, suffix)

    given_tokens, last_tokens = _split_given_and_last(tokens)
    if not given_tokens:
        given_tokens = [last_tokens[0]]
        last_tokens = last_tokens[1:]

    first_name = given_tokens[0] if given_tokens else None
    middle_tokens = _normalize_middle_tokens(given_tokens[1:]) if len(given_tokens) > 1 else []
    middle_name = " ".join(middle_tokens) if middle_tokens else None
    last_name = " ".join(last_tokens) if last_tokens else None

    if first_name and looks_like_email(first_name):
        first_name = None
    if last_name and looks_like_email(last_name):
        last_name = None
    return NameParts(first_name, middle_name, last_name, prefix, suffix)


def parse_name_parts(full_name: str | None) -> tuple[str | None, str | None]:
    """Split a full name into (given_name, last_name).

    given_name preserves middle tokens (for backward compatibility), while
    parse_person_name_parts() exposes first/middle/prefix/suffix separately.
    """
    parts = parse_person_name_parts(full_name)
    given_tokens = [p for p in (parts.first_name, parts.middle_name) if p]
    given_name = " ".join(given_tokens) if given_tokens else None
    return given_name, parts.last_name


def build_person_display_name(first_name: str | None, last_name: str | None) -> str | None:
    """Return a display-ready name from non-email first/last parts."""
    first = trim(first_name)
    last = trim(last_name)
    parts = [p for p in (first, last) if p and not looks_like_email(p)]
    if not parts:
        return None
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Helper: parse_co_owners
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# URL helpers for public scrape
# ---------------------------------------------------------------------------

def parse_race_url(url: str | None) -> tuple[str | None, int | None]:
    """Return (race_id, yr) parsed from a regattaman entries URL, or (None, None)."""
    v = trim(url)
    if not v:
        return None, None
    try:
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(v).query)
    except Exception:
        return None, None
    race_id = qs.get("race_id", [None])[0]
    yr_str = qs.get("yr", [None])[0]
    yr = int(yr_str) if yr_str and yr_str.isdigit() else None
    return race_id, yr


def canonical_entries_url(url: str | None) -> str | None:
    """Return canonical entries URL retaining only race_id and yr params.

    Returns None when race_id or yr cannot be parsed (e.g. soc_id URLs).
    """
    race_id, yr = parse_race_url(url)
    if race_id is None or yr is None:
        return None
    return f"https://regattaman.com/scratch.php?race_id={race_id}&yr={yr}"


def extract_sku_from_hist(hist: str | None) -> str | None:
    """Extract sku query param from a regattaman get_race_hist.php URL."""
    v = trim(hist)
    if not v:
        return None
    try:
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(v).query)
    except Exception:
        return None
    return qs.get("sku", [None])[0]


def build_entry_hash(
    source: str,
    entries_url: str,
    fleet: str,
    name: str,
    yacht_name: str,
    sail_num: str,
) -> str:
    """Return a deterministic 32-hex hash key for an entry row lacking a sku.

    Used as registration_external_id when Hist is absent.
    """
    key = f"{source}|{entries_url}|{fleet}|{name}|{yacht_name}|{sail_num}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# URL helpers for Yacht Scoring scrape
# ---------------------------------------------------------------------------

_YS_EMENU_RE = re.compile(r"/emenu/(\d+)")
_YS_ENTRIES_RE = re.compile(r"/current_event_entries/(\d+)")
_YS_BOATDETAIL_RE = re.compile(r"/boatdetail/(\d+)/(\d+)")


def parse_ys_emenu_url(url: str | None) -> str | None:
    """Parse event_id from a /emenu/{event_id} Yacht Scoring URL."""
    v = trim(url)
    if not v:
        return None
    m = _YS_EMENU_RE.search(v)
    return m.group(1) if m else None


def parse_ys_entries_url(url: str | None) -> str | None:
    """Parse event_id from a /current_event_entries/{event_id} Yacht Scoring URL."""
    v = trim(url)
    if not v:
        return None
    m = _YS_ENTRIES_RE.search(v)
    return m.group(1) if m else None


def parse_ys_boatdetail_url(url: str | None) -> tuple[str | None, str | None]:
    """Parse (event_id, entry_id) from a /boatdetail/{event_id}/{entry_id} Yacht Scoring URL."""
    v = trim(url)
    if not v:
        return None, None
    m = _YS_BOATDETAIL_RE.search(v)
    if m:
        return m.group(1), m.group(2)
    return None, None


_AND_PATTERN = re.compile(r"\s+and\s+", re.IGNORECASE)


def parse_co_owners(
    ownername: str | None,
    name_field: str | None,
) -> list[tuple[str, str]]:
    """Return ordered list of (full_name, role) tuples for a CSV row.

    Resolution:
    1. Primary owner is always ownername (role='owner'), first in list.
    2. name_field is split on '&' and case-insensitive ' and '.
    3. Each token is trimmed; duplicates removed by normalize_name.
    4. Tokens not matching the primary owner become role='co_owner'.

    Returns [(full_name, role), ...] with at least one entry when ownername
    is non-null.  Returns [] when ownername is null/blank.
    """
    primary = normalize_space(ownername)
    if not primary:
        return []

    primary_norm = normalize_name(primary)

    # Split name_field on '&' then on ' and '
    raw_tokens: list[str] = []
    if name_field:
        for chunk in re.split(r"&", name_field):
            for part in _AND_PATTERN.split(chunk):
                t = normalize_space(part)
                if t:
                    raw_tokens.append(t)

    # Build deduped list: primary owner first
    seen: set[str | None] = {primary_norm}
    result: list[tuple[str, str]] = [(primary, "owner")]

    for token in raw_tokens:
        norm = normalize_name(token)
        if norm in seen:
            continue
        seen.add(norm)
        result.append((token, "co_owner"))

    return result
