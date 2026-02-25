"""Normalization functions for regattaman CSV ingestion.

All functions accept str | None and return the appropriate type or None.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
import urllib.parse
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

_SENTINEL_TS = "0000-00-00 00:00:00"
_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


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


# ---------------------------------------------------------------------------
# Rule 4: normalize_phone
# ---------------------------------------------------------------------------

def normalize_phone(value: str | None) -> str | None:
    """Return E.164-style phone or None.

    Keeps digits only.  10-digit → +1XXXXXXXXXX.
    11-digit starting with 1 → +1XXXXXXXXXX.
    Anything else → return as-is with '+' prefix if no prefix present,
    or None if fewer than 7 digits (likely a data error).
    """
    v = trim(value)
    if v is None:
        return None
    digits = re.sub(r"\D", "", v)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if len(digits) >= 7:
        return f"+{digits}"
    return None


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

def parse_name_parts(full_name: str | None) -> tuple[str | None, str | None]:
    """Split a full name into (first_name, last_name).

    Supports:
    - "Last, First Middle" → ("First Middle", "Last")
    - "First Last"         → ("First", "Last")
    - Single token         → (token, None)
    """
    v = normalize_space(full_name)
    if not v:
        return (None, None)
    if "," in v:
        parts = v.split(",", 1)
        last = trim(parts[0])
        first = trim(parts[1])
        return (first, last)
    tokens = v.split()
    if len(tokens) == 1:
        return (tokens[0], None)
    return (" ".join(tokens[:-1]), tokens[-1])


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
