"""regatta_etl.import_bhyc_member_directory

BHYC Member Directory scrape + ingestion pipeline.

Design principles:
  - Conservative / polite: exactly one in-flight request, 10s base delay, ±3s jitter.
  - Authenticated: session-cookie login using ASP.NET WebForms form discovery.
  - Resumable: JSON checkpoint file tracks completed member_ids across runs.
  - GCS archival: every fetched page is stored to GCS (or locally in test mode).
  - Two-layer fetch: profile HTML + vCard per member.
  - Idempotent: bhyc_member_raw_row unique on (source_system, member_id, page_type, run_id).
  - Dry-run: crawl + archive + parse happen; all DB entity writes are rolled back.

Processing order per profile:
  1.  Fetch + archive directory listing page(s)
  2.  For each member_id:
      a.  Fetch + archive profile HTML           → SAVEPOINT raw_html_{idx}
      b.  Fetch + archive vCard                  → SAVEPOINT raw_vcard_{idx}
      c.  Parse profile HTML + vCard             → parsed_json
      d.  SAVEPOINT curated_{idx}
          i.   Upsert operational participant (email → phone → name)
          ii.  Upsert participant contacts, addresses, boat + ownership, membership
          iii. Upsert household members (operational + candidate)
          iv.  Upsert candidate_participant + candidate child rows
          v.   Insert candidate_source_link
          vi.  Upsert bhyc_member_xref_participant
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin, urlparse

import psycopg
import requests
from bs4 import BeautifulSoup

from regatta_etl.normalize import (
    normalize_email,
    normalize_name,
    normalize_phone,
    normalize_space,
    slug_name,
    trim,
)
from regatta_etl.resolution_source_to_candidate import (
    _link_source,
    _upsert_address as _upsert_candidate_address,
    _upsert_candidate,
    _upsert_contact,
    _upsert_role,
    participant_fingerprint,
)
from regatta_etl.shared import (
    AmbiguousMatchError,
    insert_participant,
    resolve_participant_by_name,
    upsert_affiliate_club,
    upsert_membership,
    upsert_ownership,
)

log = logging.getLogger(__name__)

SOURCE_SYSTEM = "bhyc_member_directory"
SOURCE_TABLE = "bhyc_member_raw_row"
BHYC_CLUB_NAME = "Boothbay Harbor Yacht Club"
BHYC_CLUB_NORMALIZED = "boothbay-harbor-yacht-club"

# Sentinel for vCard MIME type
_VCARD_MIME_TYPES = ("text/vcard", "text/x-vcard", "text/plain")


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class BhycRunCounters:
    # Crawl / fetch
    pages_discovered: int = 0
    pages_fetched: int = 0
    pages_archived: int = 0
    pages_parsed: int = 0
    pages_rejected: int = 0
    members_processed: int = 0
    members_skipped_checkpoint: int = 0
    # Entity creation (operational layer)
    participants_inserted: int = 0
    participants_matched_existing: int = 0
    household_members_created: int = 0
    contact_points_inserted: int = 0
    addresses_inserted: int = 0
    yachts_inserted: int = 0
    owner_links_inserted: int = 0
    memberships_inserted: int = 0
    # Source / candidate layer
    raw_rows_inserted: int = 0
    xref_inserted: int = 0
    candidate_created: int = 0
    candidate_enriched: int = 0
    candidate_links_inserted: int = 0
    # Error buckets
    auth_errors: int = 0
    network_errors: int = 0
    parse_errors: int = 0
    db_errors: int = 0
    rate_limit_hits: int = 0
    # Safe stop
    safe_stop_reason: str | None = None
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if k != "warnings"}
        d["warnings"] = self.warnings[:50]
        return d


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

@dataclass
class RateLimiter:
    """Polite single-thread rate limiter with jitter and exponential backoff."""

    base_delay: float = 10.0
    jitter: float = 3.0
    max_consecutive_failures: int = 5
    _consecutive_failures: int = field(default=0, init=False, repr=False)
    _backoff_mult: float = field(default=1.0, init=False, repr=False)

    def sleep(self) -> None:
        """Block for base_delay * backoff_mult ± jitter seconds (min 1s)."""
        delay = self.base_delay * self._backoff_mult
        delay += random.uniform(-self.jitter, self.jitter)
        time.sleep(max(1.0, delay))

    def on_success(self) -> None:
        self._consecutive_failures = 0
        self._backoff_mult = 1.0

    def on_failure(self, reason: str = "") -> bool:
        """Record a failure. Returns True if the safe-stop threshold is reached."""
        self._consecutive_failures += 1
        self._backoff_mult = min(self._backoff_mult * 2.0, 32.0)
        return self._consecutive_failures >= self.max_consecutive_failures

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

class Checkpoint:
    """Persist completed member_ids to a JSON file so crawls can resume."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._completed: set[str] = set()

    def load(self) -> None:
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                self._completed = set(data.get("completed_member_ids", []))
            except Exception as exc:  # noqa: BLE001
                log.warning("Checkpoint load failed (%s); starting fresh.", exc)

    def is_done(self, member_id: str) -> bool:
        return member_id in self._completed

    def mark_done(self, member_id: str) -> None:
        self._completed.add(member_id)
        self._save()

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(
                {"completed_member_ids": sorted(self._completed)},
                indent=2,
            ),
            encoding="utf-8",
        )

    def __len__(self) -> int:
        return len(self._completed)


# ---------------------------------------------------------------------------
# GCS archiver (real GCS or local filesystem for tests)
# ---------------------------------------------------------------------------

class Archiver(Protocol):
    def archive(
        self,
        run_id: str,
        member_id: str,
        page_type: str,
        content: bytes,
        content_type: str,
    ) -> str:
        """Return the GCS object path (or local path)."""
        ...


@dataclass
class GcsArchiver:
    """Upload raw HTML/vCard bytes to GCS. Bucket + prefix are set at construction."""

    bucket_name: str
    prefix: str

    def archive(
        self,
        run_id: str,
        member_id: str,
        page_type: str,
        content: bytes,
        content_type: str,
    ) -> str:
        from google.cloud import storage  # type: ignore[import-untyped]

        ext = "vcf" if page_type == "vcard" else "html"
        obj_path = f"{self.prefix}/{run_id}/{page_type}/{member_id}.{ext}"
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(obj_path)
        blob.upload_from_string(content, content_type=content_type)
        return obj_path


@dataclass
class LocalArchiver:
    """Write raw content to a local directory (used in tests / dry-run without GCS)."""

    base_dir: Path

    def archive(
        self,
        run_id: str,
        member_id: str,
        page_type: str,
        content: bytes,
        content_type: str,
    ) -> str:
        ext = "vcf" if page_type == "vcard" else "html"
        dest = self.base_dir / run_id / page_type / f"{member_id}.{ext}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return str(dest)


@dataclass
class NullArchiver:
    """No-op archiver for unit tests."""

    def archive(
        self,
        run_id: str,
        member_id: str,
        page_type: str,
        content: bytes,
        content_type: str,
    ) -> str:
        return f"null://{page_type}/{member_id}"


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def _is_logged_in_response(resp: requests.Response) -> bool:
    """Heuristic: directory content implies authenticated session."""
    text_lower = resp.text.lower()
    # Logged-in pages typically contain member profile links or logout links
    return (
        "memprofile" in text_lower
        or ("logout" in text_lower and "directory" in text_lower)
    )


def _detect_auth_failure(resp: requests.Response) -> bool:
    """Detect expired/failed session from response."""
    if resp.status_code in (401, 403):
        return True
    url_lower = resp.url.lower()
    text_lower = resp.text.lower()
    # Redirected to login page and no authenticated content visible
    if "login" in url_lower and "memprofile" not in text_lower:
        return True
    return False


def login_session(
    session: requests.Session,
    start_url: str,
    username: str,
    password: str,
    timeout: int = 30,
) -> bool:
    """Perform ASP.NET WebForms login.

    1. GET start_url (follows redirects).
    2. If already authenticated, return True.
    3. Parse the login form, inject credentials into form fields.
    4. POST to the form action.
    5. Return True if the response looks authenticated.

    The form field names are discovered dynamically from the HTML so this
    works across different ASP.NET control ID naming schemes.
    """
    try:
        resp = session.get(start_url, timeout=timeout)
    except requests.RequestException as exc:
        log.error("Login GET failed: %s", exc)
        return False

    if resp.status_code != 200:
        log.error("Login GET returned status %s", resp.status_code)
        return False

    if _is_logged_in_response(resp):
        return True  # Already authenticated

    soup = BeautifulSoup(resp.text, "html.parser")
    form = soup.find("form")
    if not form:
        log.error("No <form> found on login page %s", resp.url)
        return False

    # Collect all input fields from the form (preserves __VIEWSTATE etc.)
    form_data: dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name") or ""
        value = inp.get("value") or ""
        if name:
            form_data[name] = value

    # Inject username: try known ASP.NET field name patterns
    username_injected = False
    for fname in (
        "ctl00$cBody$tbUserName", "ctl00$tbUserName",
        "UserName", "tbUserName", "username", "user",
    ):
        if fname in form_data:
            form_data[fname] = username
            username_injected = True
            break
    # Fallback: find any input with type=text that looks like a username field
    if not username_injected:
        for inp in form.find_all("input", attrs={"type": re.compile(r"^text$", re.I)}):
            name = inp.get("name") or ""
            if "user" in name.lower() or "login" in name.lower():
                form_data[name] = username
                username_injected = True
                break

    # Inject password
    password_injected = False
    for fname in (
        "ctl00$cBody$tbPassword", "ctl00$tbPassword",
        "Password", "tbPassword", "password", "pass",
    ):
        if fname in form_data:
            form_data[fname] = password
            password_injected = True
            break
    if not password_injected:
        for inp in form.find_all("input", attrs={"type": re.compile(r"^password$", re.I)}):
            name = inp.get("name") or ""
            if name:
                form_data[name] = password
                password_injected = True
                break

    if not username_injected or not password_injected:
        log.warning(
            "Could not inject credentials into form (username=%s password=%s). "
            "Form inputs: %s",
            username_injected,
            password_injected,
            [inp.get("name") for inp in form.find_all("input")],
        )

    # Resolve the form action URL
    action = form.get("action") or resp.url
    if not action.startswith("http"):
        action = urljoin(resp.url, action)

    try:
        resp2 = session.post(action, data=form_data, timeout=timeout, allow_redirects=True)
    except requests.RequestException as exc:
        log.error("Login POST failed: %s", exc)
        return False

    return _is_logged_in_response(resp2)


# ---------------------------------------------------------------------------
# Discovery: collect member_ids from directory listing pages
# ---------------------------------------------------------------------------

_MEMBER_ID_RE = re.compile(r"[?&]id=(\d+)", re.IGNORECASE)
_MEMBER_PROFILE_HREF_RE = re.compile(r"memprofile", re.IGNORECASE)


def _find_next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    """Return the URL of the next directory page, or None if last page."""
    # Common patterns: "Next", ">", "»" link text; or rel="next"
    for a in soup.find_all("a", href=True):
        text = (a.get_text(strip=True) or "").lower()
        rel = (a.get("rel") or [])
        if text in ("next", ">", "»", "next »") or "next" in rel:
            return urljoin(current_url, a["href"])
    return None


def discover_member_ids(
    session: requests.Session,
    start_url: str,
    rate_limiter: RateLimiter,
    counters: BhycRunCounters,
    max_pages: int | None = None,
    timeout: int = 30,
) -> list[tuple[str, str]]:
    """Crawl directory pages and return de-duplicated (member_id, absolute_profile_url) pairs.

    Using the discovered absolute URL avoids re-synthesizing profile URLs and preserves
    any query parameters present in the listing page links.

    Stops early if:
    - An auth failure is detected (counters.safe_stop_reason set).
    - 5 consecutive failures occur (counters.safe_stop_reason set).
    - max_pages has been reached.
    """
    member_ids: list[tuple[str, str]] = []
    seen: set[str] = set()
    page_url: str | None = start_url
    page_count = 0

    while page_url and (max_pages is None or page_count < max_pages):
        rate_limiter.sleep()

        try:
            resp = session.get(page_url, timeout=timeout)
        except requests.RequestException as exc:
            counters.network_errors += 1
            counters.warnings.append(f"discover network error: {exc}")
            if rate_limiter.on_failure("transport"):
                counters.safe_stop_reason = "failed_safe_stop"
                break
            # Try next iteration (same page_url)
            continue

        if _detect_auth_failure(resp):
            counters.auth_errors += 1
            counters.safe_stop_reason = "auth_failed"
            break

        if resp.status_code in (429,) or resp.status_code >= 500:
            counters.rate_limit_hits += 1
            if rate_limiter.on_failure(str(resp.status_code)):
                counters.safe_stop_reason = "failed_safe_stop"
                break
            continue

        if resp.status_code != 200:
            counters.network_errors += 1
            if rate_limiter.on_failure(str(resp.status_code)):
                counters.safe_stop_reason = "failed_safe_stop"
                break
            continue

        rate_limiter.on_success()
        page_count += 1
        counters.pages_fetched += 1

        soup = BeautifulSoup(resp.text, "html.parser")

        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            if _MEMBER_PROFILE_HREF_RE.search(href):
                m = _MEMBER_ID_RE.search(href)
                if m:
                    mid = m.group(1)
                    if mid not in seen:
                        seen.add(mid)
                        abs_url = urljoin(page_url, href)
                        member_ids.append((mid, abs_url))
                        counters.pages_discovered += 1

        next_url = _find_next_page_url(soup, page_url)
        page_url = next_url  # None → exits loop

    return member_ids


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _fetch_with_retry(
    session: requests.Session,
    url: str,
    rate_limiter: RateLimiter,
    counters: BhycRunCounters,
    max_attempts: int = 3,
    timeout: int = 30,
) -> requests.Response | None:
    """GET url with polite delay + backoff. Returns None on auth failure / safe stop."""
    for attempt in range(max_attempts):
        if attempt > 0:
            rate_limiter.sleep()

        try:
            resp = session.get(url, timeout=timeout)
        except requests.RequestException as exc:
            counters.network_errors += 1
            counters.warnings.append(f"network error fetching {url}: {exc}")
            if rate_limiter.on_failure("transport"):
                counters.safe_stop_reason = "failed_safe_stop"
                return None
            continue

        if _detect_auth_failure(resp):
            counters.auth_errors += 1
            counters.safe_stop_reason = "auth_failed"
            return None

        if resp.status_code in (429,) or resp.status_code >= 500:
            counters.rate_limit_hits += 1
            if rate_limiter.on_failure(str(resp.status_code)):
                counters.safe_stop_reason = "failed_safe_stop"
                return None
            continue

        rate_limiter.on_success()
        counters.pages_fetched += 1
        return resp

    return None  # exhausted retries


def _profile_url(base_url: str, member_id: str) -> str:
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/Default.aspx?p=MemProfile&id={member_id}"


def _vcard_url(base_url: str, member_id: str) -> str:
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/GetVcard.aspx?id={member_id}"


_VCARD_HREF_RE = re.compile(r"vcard", re.IGNORECASE)


def _extract_vcard_url(soup: BeautifulSoup, profile_url: str, member_id: str) -> str:
    """Return the vCard download URL found in a profile page, or fall back to synthesized."""
    for a in soup.find_all("a", href=True):
        if _VCARD_HREF_RE.search(a["href"]):
            return urljoin(profile_url, a["href"])
    return _vcard_url(profile_url, member_id)


# ---------------------------------------------------------------------------
# Profile HTML parser
# ---------------------------------------------------------------------------

def parse_profile_html(html: str) -> tuple[dict[str, Any], list[str]]:
    """Extract member profile fields from BHYC profile page HTML.

    Returns:
        (parsed_data dict, list of parse warning strings)

    The parser is deliberately tolerant: missing sections produce None values
    rather than exceptions. All raw extracted text is preserved in parsed_data
    for future re-parsing without a re-fetch.
    """
    soup = BeautifulSoup(html, "html.parser")
    warnings: list[str] = []
    data: dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # Step 1: Build a flat label → value map from all <tr> pairs          #
    # ------------------------------------------------------------------ #
    field_map: dict[str, str] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) >= 2:
            raw_label = cells[0].get_text(separator=" ", strip=True).rstrip(":").strip()
            # BHYC profile rows are label / spacer / value — take the last non-empty cell.
            raw_value = ""
            for cell in reversed(cells[1:]):
                text = cell.get_text(separator=" ", strip=True)
                if text:
                    raw_value = text
                    break
            if raw_label and raw_value:
                field_map[raw_label] = raw_value

    # ------------------------------------------------------------------ #
    # Step 2: Identity fields                                              #
    # ------------------------------------------------------------------ #
    def _first(*keys: str) -> str | None:
        for k in keys:
            v = trim(field_map.get(k))
            if v:
                return v
        return None

    data["display_name"] = _first("Name", "Display Name", "Full Name", "Member Name")
    data["title"] = _first("Title", "Salutation", "Prefix")
    data["first_name"] = _first("First Name", "First")
    data["middle_name"] = _first("Middle Name", "Middle")
    data["last_name"] = _first("Last Name", "Last")
    data["suffix"] = _first("Suffix")

    # ------------------------------------------------------------------ #
    # Step 3: Membership fields                                            #
    # ------------------------------------------------------------------ #
    data["membership_type"] = _first("Membership Type", "Member Type", "Category")
    data["membership_begins"] = _first(
        "Member Since", "Membership Begins", "Membership Begin", "Join Date"
    )

    # ------------------------------------------------------------------ #
    # Step 4: Contact fields                                               #
    # ------------------------------------------------------------------ #
    data["primary_email"] = _first(
        "Email", "Primary Email", "Primary Email Address", "E-Mail", "Email Address"
    )
    data["secondary_email"] = _first(
        "Secondary Email", "Secondary Email Address", "Email 2", "Alt Email"
    )

    phones: list[dict[str, str]] = []
    for label, val in field_map.items():
        label_lower = label.lower()
        if "phone" in label_lower or "fax" in label_lower:
            if trim(val):
                phone_subtype = (
                    "mobile" if "cell" in label_lower or "mobile" in label_lower
                    else "fax" if "fax" in label_lower
                    else "home" if "home" in label_lower
                    else "work" if "work" in label_lower or "business" in label_lower
                    else "other"
                )
                phones.append({"label": label, "value": val, "subtype": phone_subtype})
    data["phones"] = phones

    # ------------------------------------------------------------------ #
    # Step 5: Address sections                                             #
    # ------------------------------------------------------------------ #
    data["addresses"] = _parse_address_sections(soup, field_map, warnings)

    # ------------------------------------------------------------------ #
    # Step 6: Household members                                            #
    # ------------------------------------------------------------------ #
    data["household"] = _parse_household_section(soup, warnings)

    # ------------------------------------------------------------------ #
    # Step 7: Boat fields                                                  #
    # ------------------------------------------------------------------ #
    data["boats"] = _parse_boat_section(soup, field_map, warnings)

    # ------------------------------------------------------------------ #
    # Step 8: Raw field_map preserved for debugging / re-parse             #
    # ------------------------------------------------------------------ #
    data["_raw_field_map"] = field_map

    return data, warnings


def _parse_address_sections(
    soup: BeautifulSoup,
    field_map: dict[str, str],
    warnings: list[str],
) -> list[dict[str, Any]]:
    """Extract address blocks from the profile page.

    BHYC profiles may have multiple address sections (summer physical/mailing,
    winter, year-round). We try two strategies:
      A) Look for section-header rows followed by address sub-fields.
      B) Fall back to flat field_map keys that mention address keywords.
    """
    addresses: list[dict[str, Any]] = []

    # Strategy A: section headers like "Summer Physical Address"
    _SECTION_PATTERNS = [
        ("summer_physical",  re.compile(r"summer.*(physical|street)", re.I)),
        ("summer_mailing",   re.compile(r"summer.*mail",              re.I)),
        ("winter_physical",  re.compile(r"winter.*(physical|street)", re.I)),
        ("winter_mailing",   re.compile(r"winter.*mail",              re.I)),
        ("year_round",       re.compile(r"year.?round",               re.I)),
        ("mailing",          re.compile(r"^mailing",                  re.I)),
        ("physical",         re.compile(r"^physical|^street",         re.I)),
    ]

    for section_key, section_re in _SECTION_PATTERNS:
        for label, val in field_map.items():
            if section_re.search(label):
                addr: dict[str, Any] = {
                    "address_type": section_key,
                    "raw": val,
                    "line1": None, "city": None, "state": None,
                    "postal_code": None, "country_code": None,
                }
                _try_parse_address_raw(val, addr)
                if addr["raw"]:
                    addresses.append(addr)
                break

    # Strategy B: single generic address block
    if not addresses:
        for label, val in field_map.items():
            if "address" in label.lower() and trim(val):
                addr = {
                    "address_type": "mailing",
                    "raw": val,
                    "line1": None, "city": None, "state": None,
                    "postal_code": None, "country_code": None,
                }
                _try_parse_address_raw(val, addr)
                addresses.append(addr)
                break

    return addresses


def _try_parse_address_raw(raw: str, addr: dict[str, Any]) -> None:
    """Best-effort split of 'Line1, City, ST  ZIP' into components."""
    parts = [p.strip() for p in re.split(r",|\|", raw) if p.strip()]
    if len(parts) >= 3:
        addr["line1"] = parts[0]
        addr["city"] = parts[1]
        # Last part may be "ME 04538" or "ME 04538 USA"
        tail = parts[-1].split()
        if len(tail) >= 2:
            addr["state"] = tail[0]
            addr["postal_code"] = tail[1]
            if len(tail) >= 3:
                addr["country_code"] = tail[2]
    elif len(parts) == 2:
        addr["line1"] = parts[0]
        addr["city"] = parts[1]
    elif len(parts) == 1:
        addr["line1"] = parts[0]


def _parse_household_section(
    soup: BeautifulSoup,
    warnings: list[str],
) -> list[dict[str, str]]:
    """Extract household members and their relationship labels.

    Two strategies handle the two BHYC table layouts:

    Strategy 0 — in-row (soup-native): "Other Members" is a label cell whose
        value cell in the same <tr> holds one or more member entries separated by
        <br> tags.  Segments are split on <br> BEFORE calling get_text so the
        boundary is preserved.  Each segment is parsed for both formats:
          • "Name - Relationship / MemberType"  (real BHYC production format)
          • "Name (Relationship)"               (alternate / vCard-derived)

    Strategy 1 — sibling table: A standalone header element precedes a separate
        <table> whose rows are one member per row.
    """
    household: list[dict[str, str]] = []
    _HH_HEADER_RE = re.compile(
        r"household|family|other members|additional members", re.I
    )
    _REL_RE = re.compile(
        r"\b(spouse|wife|husband|partner|child|son|daughter|dependent|guest)\b",
        re.I,
    )
    # Match " - " separator (en-dash / em-dash / plain hyphen) with optional spaces.
    # Requires whitespace on both sides so hyphenated names (e.g. "Mary-Jane Smith")
    # are never split.  Tight formats without spaces ("Alice Smith-Spouse") are not
    # supported; they fall back to Format B (paren) or plain name with rel="member".
    _HYPHEN_SEP_RE = re.compile(r"\s+[-–—]\s+")

    def _split_cell_on_br(cell) -> list[str]:
        """Return text segments from a cell, split on <br> boundaries."""
        segments: list[str] = []
        current: list[str] = []
        for child in cell.children:
            if getattr(child, "name", None) == "br":
                joined = " ".join(current).strip()
                if joined:
                    segments.append(joined)
                current = []
            elif hasattr(child, "get_text"):
                t = child.get_text(strip=True)
                if t:
                    current.append(t)
            else:
                t = str(child).strip()
                if t:
                    current.append(t)
        joined = " ".join(current).strip()
        if joined:
            segments.append(joined)
        return segments

    def _parse_member_segment(text: str) -> dict[str, str] | None:
        text = text.strip()
        if not trim(text):
            return None
        # Format A: "Name - Relationship / MemberType"
        hyphen_m = _HYPHEN_SEP_RE.search(text)
        if hyphen_m:
            name = text[: hyphen_m.start()].strip()
            rel_text = text[hyphen_m.end():]
            rel_m = _REL_RE.search(rel_text)
            rel = rel_m.group(1).lower() if rel_m else "member"
        else:
            # Format B: "Name (Relationship)" or plain "Name"
            rel_m = _REL_RE.search(text)
            rel = rel_m.group(1).lower() if rel_m else "member"
            name = re.sub(r"\s*\([^)]*\)\s*", "", text).strip()
        return {"name": name, "relationship": rel} if trim(name) else None

    # --- Strategy 0: scan <tr>s; split value cell on <br> before text extraction ---
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        for i, cell in enumerate(cells):
            if not (_HH_HEADER_RE.search(cell.get_text(strip=True))
                    and len(cell.get_text(strip=True)) < 60):
                continue
            # Parse all value cells to the right of the header cell.
            for val_cell in cells[i + 1:]:
                for segment in _split_cell_on_br(val_cell):
                    for part in re.split(r",", segment):
                        entry = _parse_member_segment(part)
                        if entry:
                            household.append(entry)
            # Only short-circuit if we actually found members; otherwise fall
            # through to Strategy 1 (the row may exist but its value cell is empty).
            if household:
                return household
            break

    # --- Strategy 1: standalone header element → next sibling table ---
    for tag in soup.find_all(True):
        if tag.name in ("table", "tbody", "tr", "td", "th"):
            continue
        text = tag.get_text(strip=True)
        if _HH_HEADER_RE.search(text) and len(text) < 60:
            sibling = tag.find_next("table")
            if sibling:
                for tr in sibling.find_all("tr"):
                    cells = tr.find_all(["td", "th"])
                    if not cells:
                        continue
                    row_text = " ".join(c.get_text(strip=True) for c in cells)
                    name = cells[0].get_text(strip=True)
                    rel_m = _REL_RE.search(row_text)
                    rel = rel_m.group(1).lower() if rel_m else "member"
                    if trim(name):
                        household.append({"name": name, "relationship": rel})
            break

    return household


def _parse_boat_section(
    soup: BeautifulSoup,
    field_map: dict[str, str],
    warnings: list[str],
) -> list[dict[str, str | None]]:
    """Extract boat records (name + type) from the profile page."""
    boats: list[dict[str, str | None]] = []
    _BOAT_HEADER_RE = re.compile(r"\bboat|vessel|yacht", re.I)

    # Strategy A: field_map keys like "Boat Name", "Boat Type"
    boat_name = trim(
        field_map.get("Boat Name")
        or field_map.get("Yacht Name")
        or field_map.get("Vessel Name")
    )
    boat_type = trim(
        field_map.get("Boat Type")
        or field_map.get("Yacht Type")
        or field_map.get("Vessel Type")
        or field_map.get("Class")
    )
    if boat_name:
        boats.append({"name": boat_name, "boat_type": boat_type})

    # Strategy B: scan for a boat table section if strategy A found nothing
    if not boats:
        for tag in soup.find_all(True):
            text = tag.get_text(strip=True)
            if _BOAT_HEADER_RE.search(text) and len(text) < 50:
                sibling = tag.find_next("table")
                if sibling:
                    for tr in sibling.find_all("tr"):
                        cells = tr.find_all(["td", "th"])
                        if len(cells) >= 1:
                            bname = trim(cells[0].get_text(strip=True))
                            btype = trim(cells[1].get_text(strip=True)) if len(cells) >= 2 else None
                            if bname:
                                boats.append({"name": bname, "boat_type": btype})
                break

    return boats


# ---------------------------------------------------------------------------
# vCard parser
# ---------------------------------------------------------------------------

def parse_vcard_text(vcard: str) -> tuple[dict[str, Any], list[str]]:
    """Parse vCard 3.0 text into a flat dict of extracted fields.

    Handles multi-line continuations (RFC 6350 folding).
    Returns (data dict, list of warning strings).
    """
    warnings: list[str] = []
    data: dict[str, Any] = {}

    if not vcard or "BEGIN:VCARD" not in vcard.upper():
        warnings.append("vcard_content_missing_or_invalid")
        return data, warnings

    # Unfold RFC 6350 line continuations
    unfolded = re.sub(r"\r?\n[ \t]", "", vcard)
    lines = [ln.rstrip("\r\n") for ln in unfolded.splitlines()]

    emails: list[str] = []
    phones: list[dict[str, str]] = []
    addresses: list[dict[str, str | None]] = []
    names: list[str] = []

    for line in lines:
        if not line or line.upper() in ("BEGIN:VCARD", "END:VCARD"):
            continue

        # Split property;parameters:value
        if ":" not in line:
            continue
        prop_params, _, value = line.partition(":")
        prop_parts = prop_params.upper().split(";")
        prop = prop_parts[0]
        params = {
            p.split("=")[0]: (p.split("=")[1] if "=" in p else p)
            for p in prop_parts[1:]
        }

        value = value.strip()

        if prop == "FN":
            data["vcard_fn"] = value
        elif prop == "N":
            # Structured: Family;Given;Additional;Prefix;Suffix
            parts = value.split(";")
            data["vcard_last"] = parts[0].strip() if len(parts) > 0 else None
            data["vcard_first"] = parts[1].strip() if len(parts) > 1 else None
            data["vcard_middle"] = parts[2].strip() if len(parts) > 2 else None
            data["vcard_prefix"] = parts[3].strip() if len(parts) > 3 else None
            data["vcard_suffix"] = parts[4].strip() if len(parts) > 4 else None
        elif prop == "EMAIL":
            emails.append(value)
        elif prop == "TEL":
            ptype = params.get("TYPE", "other").lower()
            phones.append({"value": value, "type": ptype})
        elif prop == "ADR":
            # Structured: PO Box;Extended;Street;City;State;Zip;Country
            parts = (value + ";;;;;").split(";")
            addr = {
                "line1": trim(parts[2]) or trim(parts[1]) or trim(parts[0]),
                "city": trim(parts[3]),
                "state": trim(parts[4]),
                "postal_code": trim(parts[5]),
                "country_code": trim(parts[6]),
                "address_type": params.get("TYPE", "other").lower(),
                "raw": value.replace(";", ", ").strip(", "),
            }
            if any(v for v in addr.values() if v and v != addr["address_type"]):
                addresses.append(addr)
        elif prop == "ORG":
            data["vcard_org"] = value
        elif prop == "TITLE":
            data["vcard_title"] = value
        elif prop == "NOTE":
            data["vcard_note"] = value
        elif prop == "BDAY":
            data["vcard_bday"] = value

    data["vcard_emails"] = emails
    data["vcard_phones"] = phones
    data["vcard_addresses"] = addresses

    return data, warnings


# ---------------------------------------------------------------------------
# Merge profile + vCard data into unified profile dict
# ---------------------------------------------------------------------------

def merge_profile_data(
    profile: dict[str, Any],
    vcard: dict[str, Any],
) -> dict[str, Any]:
    """Merge parsed profile HTML dict and parsed vCard dict into one unified dict.

    HTML profile takes precedence; vCard fills in missing fields.
    """
    merged = dict(profile)

    # Fill missing identity from vCard
    if not merged.get("display_name"):
        merged["display_name"] = vcard.get("vcard_fn")
    if not merged.get("first_name"):
        merged["first_name"] = vcard.get("vcard_first")
    if not merged.get("last_name"):
        merged["last_name"] = vcard.get("vcard_last")
    if not merged.get("middle_name"):
        merged["middle_name"] = vcard.get("vcard_middle")
    if not merged.get("suffix"):
        merged["suffix"] = vcard.get("vcard_suffix")
    if not merged.get("title"):
        merged["title"] = vcard.get("vcard_prefix") or vcard.get("vcard_title")

    # Emails: primary from HTML; fill secondary from vCard extras
    html_emails = [merged.get("primary_email"), merged.get("secondary_email")]
    vcard_emails: list[str] = vcard.get("vcard_emails", [])
    all_emails = [e for e in html_emails if e]
    for e in vcard_emails:
        if e and e not in all_emails:
            all_emails.append(e)
    merged["all_emails"] = all_emails

    # Phones: merge HTML phone list + vCard phones
    html_phones: list[dict] = merged.get("phones", [])
    vcard_phones: list[dict] = vcard.get("vcard_phones", [])
    # De-duplicate by normalized value
    seen_phones: set[str] = set()
    merged_phones: list[dict] = []
    for p in html_phones:
        norm = normalize_phone(p.get("value", "")) or p.get("value", "")
        if norm and norm not in seen_phones:
            seen_phones.add(norm)
            merged_phones.append(p)
    for p in vcard_phones:
        norm = normalize_phone(p.get("value", "")) or p.get("value", "")
        if norm and norm not in seen_phones:
            seen_phones.add(norm)
            merged_phones.append({"label": p.get("type", "other"), "value": p["value"], "subtype": p.get("type", "other")})
    merged["phones"] = merged_phones

    # Addresses: merge HTML + vCard (vCard may have structured data)
    html_addrs: list[dict] = merged.get("addresses", [])
    vcard_addrs: list[dict] = vcard.get("vcard_addresses", [])
    seen_raw_addrs: set[str] = {a.get("raw", "") for a in html_addrs if a.get("raw")}
    for va in vcard_addrs:
        if va.get("raw") and va["raw"] not in seen_raw_addrs:
            html_addrs.append(va)
            seen_raw_addrs.add(va["raw"])
    merged["addresses"] = html_addrs

    # vCard raw data stored for provenance
    merged["_vcard_raw"] = vcard

    return merged


# ---------------------------------------------------------------------------
# DB helpers — source row archival
# ---------------------------------------------------------------------------

def _insert_bhyc_raw_row(
    conn: psycopg.Connection,
    member_id: str,
    page_type: str,
    source_url: str,
    run_id: str,
    gcs_bucket: str | None,
    gcs_object: str | None,
    http_status: int | None,
    content_hash: str | None,
    fetched_at: datetime | None,
    parsed_json: dict | None,
    parse_warnings: list[str] | None,
) -> str | None:
    """Insert a bhyc_member_raw_row. Returns the new row id, or None if skipped (duplicate)."""
    row = conn.execute(
        """
        INSERT INTO bhyc_member_raw_row
            (source_system, member_id, page_type, source_url, run_id,
             gcs_bucket, gcs_object, http_status, content_hash,
             fetched_at, parsed_json, parse_warnings)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_system, member_id, page_type, run_id) DO NOTHING
        RETURNING id
        """,
        (
            SOURCE_SYSTEM, member_id, page_type, source_url, run_id,
            gcs_bucket, gcs_object, http_status, content_hash,
            fetched_at,
            json.dumps(parsed_json) if parsed_json else None,
            json.dumps(parse_warnings) if parse_warnings else None,
        ),
    ).fetchone()
    return str(row[0]) if row else None


def _upsert_bhyc_xref_participant(
    conn: psycopg.Connection,
    member_id: str,
    participant_id: str,
    relationship_label: str | None,
) -> bool:
    """Upsert bhyc_member_xref_participant. Returns True if newly inserted."""
    row = conn.execute(
        """
        INSERT INTO bhyc_member_xref_participant
            (source_system, member_id, relationship_label, participant_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_system, member_id, COALESCE(relationship_label, ''))
        DO UPDATE SET last_seen_at = now()
        RETURNING (xmax = 0) AS was_inserted
        """,
        (SOURCE_SYSTEM, member_id, relationship_label, participant_id),
    ).fetchone()
    return bool(row[0]) if row else False


# ---------------------------------------------------------------------------
# DB helpers — participant resolution (BHYC: email → phone → name)
# ---------------------------------------------------------------------------

def _resolve_participant_by_email(
    conn: psycopg.Connection,
    email_norm: str,
    counters: BhycRunCounters,
) -> str | None:
    rows = conn.execute(
        """
        SELECT DISTINCT participant_id
        FROM participant_contact_point
        WHERE contact_type = 'email' AND contact_value_normalized = %s
        """,
        (email_norm,),
    ).fetchall()
    if len(rows) == 1:
        return str(rows[0][0])
    if len(rows) > 1:
        raise AmbiguousMatchError(f"ambiguous_email: {email_norm!r}")
    return None


def _resolve_participant_by_phone(
    conn: psycopg.Connection,
    phone_norm: str,
    counters: BhycRunCounters,
) -> str | None:
    rows = conn.execute(
        """
        SELECT DISTINCT participant_id
        FROM participant_contact_point
        WHERE contact_type = 'phone' AND contact_value_normalized = %s
        """,
        (phone_norm,),
    ).fetchall()
    if len(rows) == 1:
        return str(rows[0][0])
    if len(rows) > 1:
        raise AmbiguousMatchError(f"ambiguous_phone: {phone_norm!r}")
    return None


def _resolve_or_insert_participant_for_profile(
    conn: psycopg.Connection,
    display_name: str | None,
    emails: list[str],
    phones: list[dict],
    counters: BhycRunCounters,
) -> str:
    """Resolve participant via email → phone → name, or insert new."""
    pid: str | None = None

    # Email lookups
    for email_raw in emails:
        email_norm = normalize_email(email_raw)
        if email_norm and pid is None:
            pid = _resolve_participant_by_email(conn, email_norm, counters)

    # Phone lookups
    if pid is None:
        for ph in phones:
            phone_norm = normalize_phone(ph.get("value"))
            if phone_norm and pid is None:
                pid = _resolve_participant_by_phone(conn, phone_norm, counters)

    # Name lookup
    if pid is None and display_name:
        name_norm = normalize_name(display_name)
        if name_norm:
            pid = resolve_participant_by_name(conn, name_norm)

    if pid is not None:
        counters.participants_matched_existing += 1
        return pid

    full_name = display_name or "Unknown"
    pid = insert_participant(conn, full_name)
    counters.participants_inserted += 1
    return pid


def _upsert_contact_point(
    conn: psycopg.Connection,
    participant_id: str,
    contact_type: str,
    contact_subtype: str,
    raw_value: str,
    norm_value: str | None,
    is_primary: bool,
    counters: BhycRunCounters,
) -> None:
    existing = conn.execute(
        """
        SELECT id FROM participant_contact_point
        WHERE participant_id = %s
          AND contact_type = %s
          AND contact_subtype = %s
          AND contact_value_normalized = %s
        """,
        (participant_id, contact_type, contact_subtype, norm_value),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO participant_contact_point
            (participant_id, contact_type, contact_subtype,
             contact_value_raw, contact_value_normalized,
             is_primary, source_system)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (participant_id, contact_type, contact_subtype,
         raw_value, norm_value, is_primary, SOURCE_SYSTEM),
    )
    counters.contact_points_inserted += 1


_ADDR_TYPE_MAP = {
    "summer_physical": "residential",
    "summer_mailing": "mailing",
    "winter_physical": "residential",
    "winter_mailing": "mailing",
    "year_round": "residential",
    "physical": "residential",
    "mailing": "mailing",
}


def _upsert_address_op(
    conn: psycopg.Connection,
    participant_id: str,
    addr: dict[str, Any],
    counters: BhycRunCounters,
) -> None:
    raw = trim(addr.get("raw"))
    if not raw:
        return
    existing = conn.execute(
        "SELECT id FROM participant_address WHERE participant_id = %s AND address_raw = %s",
        (participant_id, raw),
    ).fetchone()
    if existing:
        return
    raw_type = (addr.get("address_type") or "mailing").lower()
    db_type = _ADDR_TYPE_MAP.get(raw_type, "other")
    conn.execute(
        """
        INSERT INTO participant_address
            (participant_id, address_type, line1, city, state,
             postal_code, country_code, address_raw, is_primary, source_system)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, false, %s)
        """,
        (
            participant_id,
            db_type,
            trim(addr.get("line1")),
            trim(addr.get("city")),
            trim(addr.get("state")),
            trim(addr.get("postal_code")),
            trim(addr.get("country_code")),
            raw,
            SOURCE_SYSTEM,
        ),
    )
    counters.addresses_inserted += 1


# ---------------------------------------------------------------------------
# Per-profile ingestion
# ---------------------------------------------------------------------------

def _ingest_profile(
    conn: psycopg.Connection,
    member_id: str,
    profile_row_id: str,
    merged: dict[str, Any],
    run_id: str,
    counters: BhycRunCounters,
) -> None:
    """Ingest one parsed member profile into operational + candidate layers.

    Caller wraps this in a SAVEPOINT so errors roll back cleanly.
    """
    display_name = trim(merged.get("display_name"))
    all_emails: list[str] = merged.get("all_emails", [])
    phones: list[dict] = merged.get("phones", [])
    addresses: list[dict] = merged.get("addresses", [])
    boats: list[dict] = merged.get("boats", [])
    household: list[dict] = merged.get("household", [])

    # ------------------------------------------------------------------ #
    # 1. Resolve or insert primary participant                             #
    # ------------------------------------------------------------------ #
    pid = _resolve_or_insert_participant_for_profile(
        conn, display_name, all_emails, phones, counters
    )

    # ------------------------------------------------------------------ #
    # 2. Contact points                                                    #
    # ------------------------------------------------------------------ #
    for idx, email_raw in enumerate(all_emails):
        email_norm = normalize_email(email_raw)
        if email_raw and email_norm:
            _upsert_contact_point(
                conn, pid, "email", "primary" if idx == 0 else "secondary",
                email_raw, email_norm, idx == 0, counters,
            )

    for idx, ph in enumerate(phones):
        val = ph.get("value", "")
        norm = normalize_phone(val)
        if val and norm:
            _upsert_contact_point(
                conn, pid, "phone", ph.get("subtype", "other"),
                val, norm, False, counters,
            )

    # ------------------------------------------------------------------ #
    # 3. Addresses (operational layer)                                     #
    # ------------------------------------------------------------------ #
    for addr in addresses:
        _upsert_address_op(conn, pid, addr, counters)

    # ------------------------------------------------------------------ #
    # 4. BHYC club membership                                              #
    # ------------------------------------------------------------------ #
    club_id = upsert_affiliate_club(conn, BHYC_CLUB_NAME)
    membership_begins_raw = trim(merged.get("membership_begins"))
    membership_begins: date | None = None
    if membership_begins_raw:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%d-%b-%Y"):
            try:
                membership_begins = datetime.strptime(membership_begins_raw, fmt).date()
                break
            except ValueError:
                pass
    upsert_membership(conn, pid, club_id, membership_begins, SOURCE_SYSTEM, counters)

    # ------------------------------------------------------------------ #
    # 5. Boats + ownership                                                 #
    # ------------------------------------------------------------------ #
    today = date.today()
    for boat in boats:
        boat_name = trim(boat.get("name"))
        if not boat_name:
            continue
        boat_type = trim(boat.get("boat_type"))
        norm = slug_name(boat_name)
        existing_yacht = conn.execute(
            "SELECT id FROM yacht WHERE normalized_name = %s ORDER BY id ASC LIMIT 1",
            (norm,),
        ).fetchone()
        if existing_yacht:
            yacht_id = str(existing_yacht[0])
        else:
            row = conn.execute(
                """
                INSERT INTO yacht (name, normalized_name, model)
                VALUES (%s, %s, %s) RETURNING id
                """,
                (boat_name, norm, boat_type),
            ).fetchone()
            yacht_id = str(row[0])
            counters.yachts_inserted += 1
        upsert_ownership(
            conn, pid, yacht_id, "owner",
            is_primary_contact=True,
            effective_start=today,
            source_system=SOURCE_SYSTEM,
            counters=counters,
        )

    # ------------------------------------------------------------------ #
    # 6. Xref (primary member)                                            #
    # ------------------------------------------------------------------ #
    was_new = _upsert_bhyc_xref_participant(conn, member_id, pid, None)
    if was_new:
        counters.xref_inserted += 1

    # ------------------------------------------------------------------ #
    # 7. Candidate participant (primary)                                   #
    # ------------------------------------------------------------------ #
    name_norm = normalize_name(display_name or "")
    best_email = normalize_email(all_emails[0]) if all_emails else None
    fp = participant_fingerprint(name_norm, best_email)

    cand_id, was_inserted = _upsert_candidate(
        conn,
        "candidate_participant",
        fp,
        {
            "display_name": display_name,
            "normalized_name": name_norm,
            "best_email": best_email,
            "resolution_state": "review",
            "is_promoted": False,
        },
    )
    if was_inserted:
        counters.candidate_created += 1
    else:
        counters.candidate_enriched += 1

    # Candidate contacts
    for idx, email_raw in enumerate(all_emails):
        email_norm = normalize_email(email_raw)
        if email_raw and email_norm:
            _upsert_contact(
                conn, cand_id, "email", email_raw, email_norm,
                idx == 0, SOURCE_TABLE, profile_row_id,
            )
    for ph in phones:
        val = ph.get("value", "")
        norm = normalize_phone(val)
        if val and norm:
            _upsert_contact(
                conn, cand_id, "phone", val, norm,
                False, SOURCE_TABLE, profile_row_id,
            )

    # Candidate addresses
    for addr in addresses:
        raw = trim(addr.get("raw"))
        if raw:
            _upsert_candidate_address(
                conn, cand_id, raw, SOURCE_TABLE, profile_row_id,
                line1=trim(addr.get("line1")),
                city=trim(addr.get("city")),
                state=trim(addr.get("state")),
                postal_code=trim(addr.get("postal_code")),
                country_code=trim(addr.get("country_code")),
            )

    # Candidate role: registrant (closest valid role for a BHYC member)
    _upsert_role(conn, cand_id, "registrant")

    # Candidate source link
    content_hash = hashlib.sha256(json.dumps(merged, sort_keys=True, default=str).encode()).hexdigest()
    was_linked = _link_source(
        conn,
        entity_type="participant",
        candidate_id=cand_id,
        source_table=SOURCE_TABLE,
        source_pk=profile_row_id,
        source_system=SOURCE_SYSTEM,
        source_row_hash=content_hash,
        link_score=0.95,
        link_reason={"member_id": member_id},
    )
    if was_linked:
        counters.candidate_links_inserted += 1

    # ------------------------------------------------------------------ #
    # 8. Household members                                                 #
    # ------------------------------------------------------------------ #
    # Map household relationship labels → valid candidate_participant_role_assignment.role values
    _HH_ROLE_MAP = {
        "spouse": "other", "wife": "other", "husband": "other", "partner": "other",
        "child": "other", "son": "other", "daughter": "other", "dependent": "other",
        "parent": "parent", "guardian": "guardian",
        "guest": "other",
    }

    for hh_member in household:
        hh_name = trim(hh_member.get("name"))
        raw_rel = trim(hh_member.get("relationship")) or "other"
        hh_role = _HH_ROLE_MAP.get(raw_rel.lower(), "other")
        if not hh_name:
            continue

        hh_name_norm = normalize_name(hh_name)
        if not hh_name_norm:
            counters.warnings.append(
                f"household member {hh_name!r} for member_id={member_id} "
                "normalized to empty — skipped"
            )
            continue

        # Resolve or insert household participant (name-only)
        hh_pid = resolve_participant_by_name(conn, hh_name_norm)
        if hh_pid is None:
            hh_pid = insert_participant(conn, hh_name)
            counters.household_members_created += 1
        else:
            counters.participants_matched_existing += 1

        # BHYC membership for household member
        upsert_membership(conn, hh_pid, club_id, membership_begins, SOURCE_SYSTEM, counters)

        # Xref (store the raw relationship label for lineage)
        was_new_hh = _upsert_bhyc_xref_participant(conn, member_id, hh_pid, raw_rel)
        if was_new_hh:
            counters.xref_inserted += 1

        # Candidate for household member
        hh_fp = participant_fingerprint(hh_name_norm, None)
        hh_cand_id, hh_inserted = _upsert_candidate(
            conn,
            "candidate_participant",
            hh_fp,
            {
                "display_name": hh_name,
                "normalized_name": hh_name_norm,
                "best_email": None,
                "resolution_state": "review",
                "is_promoted": False,
            },
        )
        if hh_inserted:
            counters.candidate_created += 1
        else:
            counters.candidate_enriched += 1

        _upsert_role(conn, hh_cand_id, hh_role)

        hh_was_linked = _link_source(
            conn,
            entity_type="participant",
            candidate_id=hh_cand_id,
            source_table=SOURCE_TABLE,
            source_pk=profile_row_id,
            source_system=SOURCE_SYSTEM,
            source_row_hash=None,
            link_score=0.7,
            link_reason={"member_id": member_id, "relationship": raw_rel},
        )
        if hh_was_linked:
            counters.candidate_links_inserted += 1

    counters.members_processed += 1


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_bhyc_member_directory(
    run_id: str,
    db_dsn: str,
    start_url: str,
    username: str,
    password: str,
    archiver: Any,  # Archiver protocol
    gcs_bucket: str | None,
    checkpoint: Checkpoint,
    rate_limiter: RateLimiter,
    counters: BhycRunCounters,
    max_pages: int | None = None,
    dry_run: bool = False,
    timeout: int = 30,
) -> None:
    """Full BHYC member directory scrape + ingestion run.

    Phase 1: Auth + discovery
    Phase 2: Per-member fetch (HTML + vCard) + archive + parse + DB ingest
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "BHYC-DataPipeline/1.0 (authorized member access)"})

    # ------------------------------------------------------------------ #
    # Phase 1: Auth                                                        #
    # ------------------------------------------------------------------ #
    logged_in = login_session(session, start_url, username, password, timeout=timeout)
    if not logged_in:
        counters.auth_errors += 1
        counters.safe_stop_reason = "auth_failed"
        return

    # ------------------------------------------------------------------ #
    # Phase 1b: Discover member_ids                                        #
    # ------------------------------------------------------------------ #
    member_ids = discover_member_ids(
        session, start_url, rate_limiter, counters,
        max_pages=max_pages, timeout=timeout,
    )
    if counters.safe_stop_reason:
        return

    if not member_ids:
        counters.warnings.append("No member_ids discovered — check start_url and auth")
        return

    # ------------------------------------------------------------------ #
    # Phase 2: Per-member fetch + ingest                                   #
    # ------------------------------------------------------------------ #
    conn = psycopg.connect(db_dsn, autocommit=False)
    pending_checkpoint: list[str] = []
    try:
        for idx, (member_id, profile_url) in enumerate(member_ids):
            if counters.safe_stop_reason:
                break

            if checkpoint.is_done(member_id):
                counters.members_skipped_checkpoint += 1
                continue

            # ------ Fetch profile HTML ------ #
            rate_limiter.sleep()
            profile_resp = _fetch_with_retry(
                session, profile_url, rate_limiter, counters, timeout=timeout
            )
            if counters.safe_stop_reason:
                break
            if profile_resp is None:
                counters.pages_rejected += 1
                continue

            profile_html = profile_resp.text
            profile_bytes = profile_resp.content
            profile_hash = hashlib.sha256(profile_bytes).hexdigest()
            profile_fetched_at = datetime.utcnow()

            # Archive profile HTML
            try:
                gcs_obj_profile = archiver.archive(
                    run_id, member_id, "member_profile",
                    profile_bytes, "text/html",
                )
                counters.pages_archived += 1
            except Exception as exc:
                counters.warnings.append(f"GCS archive failed for member_id={member_id}: {exc}")
                gcs_obj_profile = None

            # ------ Fetch vCard ------ #
            # Prefer a vCard link discovered in the profile page; fall back to synthesized URL.
            _profile_soup_for_vcard = BeautifulSoup(profile_html, "html.parser")
            vcard_url = _extract_vcard_url(_profile_soup_for_vcard, profile_url, member_id)
            rate_limiter.sleep()
            vcard_resp = _fetch_with_retry(
                session, vcard_url, rate_limiter, counters, timeout=timeout
            )
            vcard_text = ""
            vcard_hash = None
            vcard_fetched_at = None
            gcs_obj_vcard = None
            if vcard_resp is not None and vcard_resp.status_code == 200:
                vcard_text = vcard_resp.text
                vcard_bytes = vcard_resp.content
                vcard_hash = hashlib.sha256(vcard_bytes).hexdigest()
                vcard_fetched_at = datetime.utcnow()
                try:
                    gcs_obj_vcard = archiver.archive(
                        run_id, member_id, "vcard",
                        vcard_bytes, "text/vcard",
                    )
                    counters.pages_archived += 1
                except Exception as exc:
                    counters.warnings.append(f"GCS vCard archive failed for member_id={member_id}: {exc}")

            # ------ Parse ------ #
            try:
                profile_data, profile_warnings = parse_profile_html(profile_html)
                vcard_data, vcard_warnings = parse_vcard_text(vcard_text)
                merged = merge_profile_data(profile_data, vcard_data)
                all_warnings = profile_warnings + vcard_warnings
                counters.pages_parsed += 1
            except Exception as exc:
                counters.parse_errors += 1
                counters.pages_rejected += 1
                counters.warnings.append(f"parse error member_id={member_id}: {exc}")
                continue

            # ------ DB ingest (raw rows, then curated) ------ #
            sp_raw = f"bhyc_raw_{idx}"
            conn.execute(f"SAVEPOINT {sp_raw}")
            try:
                profile_row_id = _insert_bhyc_raw_row(
                    conn,
                    member_id=member_id,
                    page_type="member_profile",
                    source_url=profile_url,
                    run_id=run_id,
                    gcs_bucket=gcs_bucket,
                    gcs_object=gcs_obj_profile,
                    http_status=profile_resp.status_code,
                    content_hash=profile_hash,
                    fetched_at=profile_fetched_at,
                    parsed_json=merged,
                    parse_warnings=all_warnings if all_warnings else None,
                )
                if profile_row_id:
                    counters.raw_rows_inserted += 1

                if vcard_resp and vcard_resp.status_code == 200:
                    _insert_bhyc_raw_row(
                        conn,
                        member_id=member_id,
                        page_type="vcard",
                        source_url=vcard_url,
                        run_id=run_id,
                        gcs_bucket=gcs_bucket,
                        gcs_object=gcs_obj_vcard,
                        http_status=vcard_resp.status_code,
                        content_hash=vcard_hash,
                        fetched_at=vcard_fetched_at,
                        parsed_json=None,
                        parse_warnings=None,
                    )

                conn.execute(f"RELEASE SAVEPOINT {sp_raw}")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_raw}")
                counters.db_errors += 1
                counters.warnings.append(f"raw row insert failed member_id={member_id}: {exc}")
                continue

            # Use the raw row id we got (may be None on duplicate run; fetch it)
            if profile_row_id is None:
                row = conn.execute(
                    """
                    SELECT id FROM bhyc_member_raw_row
                    WHERE source_system = %s AND member_id = %s
                      AND page_type = 'member_profile' AND run_id = %s
                    """,
                    (SOURCE_SYSTEM, member_id, run_id),
                ).fetchone()
                profile_row_id = str(row[0]) if row else None

            if not profile_row_id:
                counters.warnings.append(f"No raw_row_id for member_id={member_id}; skipping curated")
                pending_checkpoint.append(member_id)
                continue

            # Curated savepoint
            sp_cur = f"bhyc_cur_{idx}"
            conn.execute(f"SAVEPOINT {sp_cur}")
            try:
                _ingest_profile(conn, member_id, profile_row_id, merged, run_id, counters)
                conn.execute(f"RELEASE SAVEPOINT {sp_cur}")
            except AmbiguousMatchError as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_cur}")
                counters.warnings.append(f"ambiguous match member_id={member_id}: {exc}")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_cur}")
                counters.db_errors += 1
                counters.warnings.append(f"curated ingest failed member_id={member_id}: {exc}")

            pending_checkpoint.append(member_id)

        # ------ Commit or rollback ------ #
        if dry_run:
            conn.rollback()
            # Do NOT persist checkpoint: DB writes were rolled back, members must re-run.
        elif counters.db_errors > 0:
            conn.rollback()
            # Do NOT persist checkpoint: transaction was rolled back due to errors.
        else:
            conn.commit()
            for mid in pending_checkpoint:
                checkpoint.mark_done(mid)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_bhyc_report(counters: BhycRunCounters, dry_run: bool) -> str:
    lines = [
        "=== BHYC Member Directory Run Report ===",
        f"dry_run          : {dry_run}",
        "",
        "--- Crawl ---",
        f"pages_discovered : {counters.pages_discovered}",
        f"pages_fetched    : {counters.pages_fetched}",
        f"pages_archived   : {counters.pages_archived}",
        f"pages_parsed     : {counters.pages_parsed}",
        f"pages_rejected   : {counters.pages_rejected}",
        f"members_processed: {counters.members_processed}",
        f"skipped(ckpt)    : {counters.members_skipped_checkpoint}",
        "",
        "--- Entities ---",
        f"participants_inserted        : {counters.participants_inserted}",
        f"participants_matched         : {counters.participants_matched_existing}",
        f"household_members_created    : {counters.household_members_created}",
        f"contact_points_inserted      : {counters.contact_points_inserted}",
        f"addresses_inserted           : {counters.addresses_inserted}",
        f"yachts_inserted              : {counters.yachts_inserted}",
        f"owner_links_inserted         : {counters.owner_links_inserted}",
        f"memberships_inserted         : {counters.memberships_inserted}",
        "",
        "--- Source / Candidate ---",
        f"raw_rows_inserted            : {counters.raw_rows_inserted}",
        f"xref_inserted                : {counters.xref_inserted}",
        f"candidate_created            : {counters.candidate_created}",
        f"candidate_enriched           : {counters.candidate_enriched}",
        f"candidate_links_inserted     : {counters.candidate_links_inserted}",
        "",
        "--- Errors ---",
        f"auth_errors      : {counters.auth_errors}",
        f"network_errors   : {counters.network_errors}",
        f"parse_errors     : {counters.parse_errors}",
        f"db_errors        : {counters.db_errors}",
        f"rate_limit_hits  : {counters.rate_limit_hits}",
    ]
    if counters.safe_stop_reason:
        lines.append(f"safe_stop_reason : {counters.safe_stop_reason}")
    if counters.warnings:
        lines += ["", "--- Warnings (first 10) ---"]
        lines += [f"  {w}" for w in counters.warnings[:10]]
    return "\n".join(lines)
