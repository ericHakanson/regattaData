"""Unit tests for BHYC member directory scrape helpers.

No database or network access required.
"""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from regatta_etl.import_bhyc_member_directory import (
    Checkpoint,
    LocalArchiver,
    NullArchiver,
    RateLimiter,
    _detect_auth_failure,
    _find_next_page_url,
    _is_logged_in_response,
    _profile_url,
    _try_parse_address_raw,
    _vcard_url,
    merge_profile_data,
    parse_profile_html,
    parse_vcard_text,
)


# ---------------------------------------------------------------------------
# RateLimiter
# ---------------------------------------------------------------------------

class TestRateLimiter:
    def test_defaults(self):
        rl = RateLimiter()
        assert rl.base_delay == 10.0
        assert rl.jitter == 3.0
        assert rl.max_consecutive_failures == 5

    def test_on_success_resets(self):
        rl = RateLimiter(base_delay=0.0, jitter=0.0)
        rl.on_failure("x")
        rl.on_failure("x")
        assert rl.consecutive_failures == 2
        rl.on_success()
        assert rl.consecutive_failures == 0

    def test_on_failure_returns_true_at_threshold(self):
        rl = RateLimiter(max_consecutive_failures=3, base_delay=0.0, jitter=0.0)
        assert rl.on_failure("a") is False
        assert rl.on_failure("b") is False
        assert rl.on_failure("c") is True  # threshold hit

    def test_backoff_multiplier_caps_at_32(self):
        rl = RateLimiter(max_consecutive_failures=100, base_delay=0.0, jitter=0.0)
        for _ in range(10):
            rl.on_failure("x")
        # _backoff_mult caps at 32
        assert rl._backoff_mult == 32.0

    def test_sleep_respects_min_1s(self):
        rl = RateLimiter(base_delay=0.0, jitter=0.0)
        start = time.monotonic()
        rl.sleep()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.9  # allow small timer tolerance


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

class TestCheckpoint:
    def test_empty_checkpoint(self, tmp_path):
        cp = Checkpoint(tmp_path / "ckpt.json")
        cp.load()
        assert len(cp) == 0
        assert not cp.is_done("123")

    def test_mark_done_persists(self, tmp_path):
        path = tmp_path / "ckpt.json"
        cp = Checkpoint(path)
        cp.mark_done("42")
        cp.mark_done("99")
        assert cp.is_done("42")
        assert cp.is_done("99")
        assert not cp.is_done("0")
        # Load fresh instance from same file
        cp2 = Checkpoint(path)
        cp2.load()
        assert cp2.is_done("42")
        assert cp2.is_done("99")

    def test_load_nonexistent_file_is_noop(self, tmp_path):
        cp = Checkpoint(tmp_path / "missing.json")
        cp.load()  # should not raise
        assert len(cp) == 0

    def test_load_invalid_json_warns_and_continues(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("not json", encoding="utf-8")
        cp = Checkpoint(path)
        cp.load()  # should not raise
        assert len(cp) == 0

    def test_len(self, tmp_path):
        cp = Checkpoint(tmp_path / "ckpt.json")
        assert len(cp) == 0
        cp.mark_done("1")
        assert len(cp) == 1
        cp.mark_done("2")
        assert len(cp) == 2
        cp.mark_done("2")  # duplicate — no change
        assert len(cp) == 2


# ---------------------------------------------------------------------------
# Archivers
# ---------------------------------------------------------------------------

class TestLocalArchiver:
    def test_archives_html(self, tmp_path):
        archiver = LocalArchiver(base_dir=tmp_path)
        path = archiver.archive("run1", "member42", "member_profile", b"<html>", "text/html")
        assert "run1" in path
        assert "member42" in path
        dest = Path(path)
        assert dest.exists()
        assert dest.read_bytes() == b"<html>"

    def test_archives_vcard(self, tmp_path):
        archiver = LocalArchiver(base_dir=tmp_path)
        path = archiver.archive("run1", "member42", "vcard", b"BEGIN:VCARD", "text/vcard")
        assert path.endswith(".vcf")
        assert Path(path).read_bytes() == b"BEGIN:VCARD"

    def test_creates_parent_dirs(self, tmp_path):
        archiver = LocalArchiver(base_dir=tmp_path / "nested" / "deep")
        path = archiver.archive("r", "m", "member_profile", b"x", "text/html")
        assert Path(path).exists()


class TestNullArchiver:
    def test_returns_null_path(self):
        archiver = NullArchiver()
        result = archiver.archive("run1", "99", "member_profile", b"", "text/html")
        assert "99" in result

    def test_does_not_raise(self):
        archiver = NullArchiver()
        archiver.archive("r", "m", "vcard", b"BEGIN:VCARD", "text/vcard")


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

class TestUrlHelpers:
    def test_profile_url(self):
        url = _profile_url("https://www.bhyc.net/Default.aspx?p=v35Directory", "12345")
        assert "p=MemProfile" in url
        assert "id=12345" in url
        assert url.startswith("https://www.bhyc.net/")

    def test_vcard_url(self):
        url = _vcard_url("https://www.bhyc.net/Default.aspx?p=v35Directory", "12345")
        assert "GetVcard.aspx" in url
        assert "id=12345" in url

    def test_different_host_preserved(self):
        url = _profile_url("http://staging.bhyc.net/page", "7")
        assert "staging.bhyc.net" in url


# ---------------------------------------------------------------------------
# Auth detection helpers
# ---------------------------------------------------------------------------

class TestAuthDetection:
    def _make_resp(self, status: int, url: str, text: str) -> MagicMock:
        r = MagicMock()
        r.status_code = status
        r.url = url
        r.text = text
        return r

    def test_403_is_auth_failure(self):
        r = self._make_resp(403, "https://bhyc.net/", "")
        assert _detect_auth_failure(r) is True

    def test_401_is_auth_failure(self):
        r = self._make_resp(401, "https://bhyc.net/", "")
        assert _detect_auth_failure(r) is True

    def test_login_redirect_without_memprofile(self):
        r = self._make_resp(200, "https://bhyc.net/login.aspx", "Please log in")
        assert _detect_auth_failure(r) is True

    def test_ok_page_with_memprofile(self):
        r = self._make_resp(200, "https://bhyc.net/Default.aspx", "MemProfile&id=123")
        assert _detect_auth_failure(r) is False

    def test_is_logged_in_with_memprofile(self):
        r = self._make_resp(200, "https://bhyc.net/dir", "MemProfile&id=1 logout")
        assert _is_logged_in_response(r) is True

    def test_is_not_logged_in_plain_page(self):
        r = self._make_resp(200, "https://bhyc.net/dir", "Welcome, please sign in")
        assert _is_logged_in_response(r) is False


# ---------------------------------------------------------------------------
# Next-page discovery
# ---------------------------------------------------------------------------

class TestFindNextPageUrl:
    def _soup(self, html: str):
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "html.parser")

    def test_finds_next_link_by_text(self):
        soup = self._soup('<a href="/dir?page=2">Next</a>')
        result = _find_next_page_url(soup, "https://bhyc.net/dir")
        assert result is not None
        assert "page=2" in result

    def test_finds_chevron_link(self):
        soup = self._soup('<a href="/dir?page=3">»</a>')
        result = _find_next_page_url(soup, "https://bhyc.net/dir")
        assert result is not None

    def test_returns_none_when_no_next(self):
        soup = self._soup('<a href="/dir?page=1">Previous</a>')
        result = _find_next_page_url(soup, "https://bhyc.net/dir")
        assert result is None

    def test_empty_page(self):
        soup = self._soup("<html></html>")
        result = _find_next_page_url(soup, "https://bhyc.net/dir")
        assert result is None


# ---------------------------------------------------------------------------
# Address raw parser
# ---------------------------------------------------------------------------

class TestTryParseAddressRaw:
    def test_full_address(self):
        addr: dict = {"raw": "123 Main St, Boothbay Harbor, ME 04538"}
        _try_parse_address_raw("123 Main St, Boothbay Harbor, ME 04538", addr)
        assert addr["line1"] == "123 Main St"
        assert addr["city"] == "Boothbay Harbor"
        assert addr["state"] == "ME"
        assert addr["postal_code"] == "04538"

    def test_two_part_address(self):
        addr: dict = {}
        _try_parse_address_raw("456 Ocean Ave, Edgecomb", addr)
        assert addr["line1"] == "456 Ocean Ave"
        assert addr["city"] == "Edgecomb"

    def test_single_part(self):
        addr: dict = {}
        _try_parse_address_raw("PO Box 100", addr)
        assert addr["line1"] == "PO Box 100"

    def test_pipe_separator(self):
        addr: dict = {}
        _try_parse_address_raw("1 Harbor Dr | Boothbay | ME 04538", addr)
        assert addr["line1"] == "1 Harbor Dr"
        assert addr["state"] == "ME"


# ---------------------------------------------------------------------------
# parse_profile_html
# ---------------------------------------------------------------------------

_SAMPLE_PROFILE_HTML = """
<html><body>
<table>
  <tr><td>Name</td><td>John Doe</td></tr>
  <tr><td>Membership Type</td><td>Regular</td></tr>
  <tr><td>Member Since</td><td>2005-03-15</td></tr>
  <tr><td>Email</td><td>john@example.com</td></tr>
  <tr><td>Secondary Email</td><td>jdoe@work.com</td></tr>
  <tr><td>Home Phone</td><td>(207) 555-1234</td></tr>
  <tr><td>Cell Phone</td><td>(207) 555-5678</td></tr>
  <tr><td>Summer Physical Address</td><td>10 Bay St, Boothbay, ME 04538</td></tr>
  <tr><td>Boat Name</td><td>Sea Breeze</td></tr>
  <tr><td>Boat Type</td><td>Sloop</td></tr>
</table>
</body></html>
"""


class TestParseProfileHtml:
    def test_extracts_display_name(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML)
        assert data["display_name"] == "John Doe"

    def test_extracts_membership_type(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML)
        assert data["membership_type"] == "Regular"

    def test_extracts_membership_begins(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML)
        assert data["membership_begins"] == "2005-03-15"

    def test_extracts_primary_email(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML)
        assert data["primary_email"] == "john@example.com"

    def test_extracts_secondary_email(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML)
        assert data["secondary_email"] == "jdoe@work.com"

    def test_extracts_phones(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML)
        phones = data["phones"]
        assert len(phones) == 2
        subtypes = {p["subtype"] for p in phones}
        assert "home" in subtypes
        assert "mobile" in subtypes

    def test_extracts_address(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML)
        addrs = data["addresses"]
        assert len(addrs) >= 1
        assert any(a.get("city") == "Boothbay" for a in addrs)

    def test_extracts_boat(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML)
        boats = data["boats"]
        assert len(boats) == 1
        assert boats[0]["name"] == "Sea Breeze"
        assert boats[0]["boat_type"] == "Sloop"

    def test_empty_html_no_crash(self):
        data, warnings = parse_profile_html("<html></html>")
        assert data["display_name"] is None
        assert data["boats"] == []

    def test_missing_sections_produce_empty_not_error(self):
        data, _ = parse_profile_html("<html><body><table></table></body></html>")
        assert data["phones"] == []
        assert data["addresses"] == []
        assert data["household"] == []


# 3-column (label / spacer / value) BHYC layout — the real site pattern.
_SAMPLE_PROFILE_HTML_3COL = """
<html><body>
<table>
  <tr><td>Name</td><td>&nbsp;</td><td>Jane Doe</td></tr>
  <tr><td>Email</td><td>&nbsp;</td><td>jane@example.com</td></tr>
  <tr><td>Home Phone</td><td>&nbsp;</td><td>(207) 555-9999</td></tr>
  <tr><td>Membership Type</td><td>&nbsp;</td><td>Full</td></tr>
  <tr><td>Summer Physical Address</td><td>&nbsp;</td><td>7 Harbor View, Boothbay, ME 04538</td></tr>
</table>
</body></html>
"""


class TestParseProfileHtml3Col:
    """Parser must extract values from the last non-empty cell (real BHYC table layout)."""

    def test_extracts_name_from_3col_row(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML_3COL)
        assert data["display_name"] == "Jane Doe"

    def test_extracts_email_from_3col_row(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML_3COL)
        assert data["primary_email"] == "jane@example.com"

    def test_extracts_phone_from_3col_row(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML_3COL)
        phones = data["phones"]
        assert any(p.get("value") == "(207) 555-9999" for p in phones)

    def test_extracts_membership_type_from_3col_row(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML_3COL)
        assert data["membership_type"] == "Full"

    def test_extracts_address_from_3col_row(self):
        data, _ = parse_profile_html(_SAMPLE_PROFILE_HTML_3COL)
        addrs = data["addresses"]
        assert any(a.get("city") == "Boothbay" for a in addrs)

    def test_spacer_cell_not_used_as_value(self):
        """A row whose middle cell contains only &nbsp; must not return that as value."""
        html = "<html><body><table><tr><td>Name</td><td>&nbsp;</td><td>Real Name</td></tr></table></body></html>"
        data, _ = parse_profile_html(html)
        assert data["display_name"] == "Real Name"

    def test_primary_email_address_label(self):
        """Real BHYC label 'Primary Email Address' must be recognised."""
        html = (
            "<html><body><table>"
            "<tr><td>Primary Email Address</td><td>&nbsp;</td><td>user@bhyc.org</td></tr>"
            "</table></body></html>"
        )
        data, _ = parse_profile_html(html)
        assert data["primary_email"] == "user@bhyc.org"

    def test_household_in_row_same_table(self):
        """'Other Members' in the same <tr> as member data must produce household entries."""
        html = (
            "<html><body><table>"
            "<tr><td>Name</td><td>&nbsp;</td><td>John Member</td></tr>"
            "<tr><td>Other Members</td><td>&nbsp;</td><td>Jane Member (Spouse)</td></tr>"
            "</table></body></html>"
        )
        data, _ = parse_profile_html(html)
        hh = data["household"]
        assert len(hh) == 1
        assert hh[0]["name"] == "Jane Member"
        assert hh[0]["relationship"] == "spouse"

    def test_household_multiple_in_row(self):
        """Multiple comma-separated household members in one value cell are all extracted."""
        html = (
            "<html><body><table>"
            "<tr><td>Other Members</td><td>Bob Smith (Child), Carol Smith (Child)</td></tr>"
            "</table></body></html>"
        )
        data, _ = parse_profile_html(html)
        hh = data["household"]
        assert len(hh) == 2
        names = {m["name"] for m in hh}
        assert "Bob Smith" in names
        assert "Carol Smith" in names

    def test_household_anchor_hyphen_br_production_pattern(self):
        """Real BHYC layout: anchor text + '- Rel / Type' segments separated by <br>."""
        html = (
            "<html><body><table>"
            "<tr><td>Name</td><td>&nbsp;</td><td>Primary Person</td></tr>"
            "<tr><td>Other Members</td><td>&nbsp;</td><td>"
            '<a href="/profile?id=2">Jane Doe</a> - Spouse / Secondary'
            "<br>"
            '<a href="/profile?id=3">Tim Doe</a> - Child / Additional'
            "</td></tr>"
            "</table></body></html>"
        )
        data, _ = parse_profile_html(html)
        hh = data["household"]
        assert len(hh) == 2, f"expected 2 household members, got {len(hh)}: {hh}"
        by_name = {m["name"]: m for m in hh}
        assert "Jane Doe" in by_name
        assert by_name["Jane Doe"]["relationship"] == "spouse"
        assert "Tim Doe" in by_name
        assert by_name["Tim Doe"]["relationship"] == "child"

    def test_household_hyphen_format_no_anchor(self):
        """Hyphen format without anchor tags: 'Name - Relationship' plain text."""
        html = (
            "<html><body><table>"
            "<tr><td>Other Members</td><td>Alice Smith - Spouse</td></tr>"
            "</table></body></html>"
        )
        data, _ = parse_profile_html(html)
        hh = data["household"]
        assert len(hh) == 1
        assert hh[0]["name"] == "Alice Smith"
        assert hh[0]["relationship"] == "spouse"


# ---------------------------------------------------------------------------
# parse_vcard_text
# ---------------------------------------------------------------------------

_SAMPLE_VCARD = """\
BEGIN:VCARD
VERSION:3.0
FN:Jane Smith
N:Smith;Jane;Ann;;
EMAIL;TYPE=INTERNET:jane@example.com
EMAIL;TYPE=INTERNET:jane.work@example.com
TEL;TYPE=HOME:(207) 555-9876
TEL;TYPE=CELL:(207) 555-4321
ADR;TYPE=HOME:;;5 Oak Ave;Boothbay Harbor;ME;04538;US
ORG:Boothbay Harbor Yacht Club
END:VCARD
"""


class TestParseVcardText:
    def test_parses_fn(self):
        data, _ = parse_vcard_text(_SAMPLE_VCARD)
        assert data["vcard_fn"] == "Jane Smith"

    def test_parses_n_fields(self):
        data, _ = parse_vcard_text(_SAMPLE_VCARD)
        assert data["vcard_last"] == "Smith"
        assert data["vcard_first"] == "Jane"
        assert data["vcard_middle"] == "Ann"

    def test_parses_emails(self):
        data, _ = parse_vcard_text(_SAMPLE_VCARD)
        assert "jane@example.com" in data["vcard_emails"]
        assert "jane.work@example.com" in data["vcard_emails"]

    def test_parses_phones(self):
        data, _ = parse_vcard_text(_SAMPLE_VCARD)
        types = {p["type"] for p in data["vcard_phones"]}
        assert "home" in types
        assert "cell" in types

    def test_parses_address(self):
        data, _ = parse_vcard_text(_SAMPLE_VCARD)
        addrs = data["vcard_addresses"]
        assert len(addrs) == 1
        assert addrs[0]["city"] == "Boothbay Harbor"
        assert addrs[0]["state"] == "ME"
        assert addrs[0]["postal_code"] == "04538"
        assert addrs[0]["country_code"] == "US"

    def test_empty_string_returns_warning(self):
        data, warnings = parse_vcard_text("")
        assert any("missing_or_invalid" in w for w in warnings)

    def test_missing_begin_returns_warning(self):
        data, warnings = parse_vcard_text("not a vcard")
        assert any("missing_or_invalid" in w for w in warnings)

    def test_rfc_line_folding(self):
        vcard = "BEGIN:VCARD\nVERSION:3.0\nFN:Jo\n hn Doe\nEND:VCARD\n"
        data, _ = parse_vcard_text(vcard)
        assert data["vcard_fn"] == "John Doe"


# ---------------------------------------------------------------------------
# merge_profile_data
# ---------------------------------------------------------------------------

class TestMergeProfileData:
    def _base_profile(self) -> dict:
        return {
            "display_name": "John Doe",
            "first_name": "John",
            "last_name": "Doe",
            "primary_email": "john@example.com",
            "secondary_email": None,
            "phones": [{"label": "Home Phone", "value": "(207) 555-1111", "subtype": "home"}],
            "addresses": [{"raw": "1 Main St, Bar Harbor, ME 04609", "address_type": "summer_physical"}],
            "household": [],
            "boats": [],
            "membership_type": "Regular",
            "membership_begins": "2010-01-01",
            "title": None,
            "middle_name": None,
            "suffix": None,
        }

    def _base_vcard(self) -> dict:
        return {
            "vcard_fn": "John A. Doe",
            "vcard_first": "John",
            "vcard_last": "Doe",
            "vcard_middle": "A.",
            "vcard_prefix": "Capt.",
            "vcard_suffix": None,
            "vcard_emails": ["john@example.com", "j.doe@work.com"],
            "vcard_phones": [{"value": "(207) 555-2222", "type": "work"}],
            "vcard_addresses": [{"raw": "PO Box 5, Boothbay, ME 04537", "city": "Boothbay",
                                  "state": "ME", "postal_code": "04537", "country_code": None,
                                  "line1": "PO Box 5", "address_type": "home"}],
        }

    def test_display_name_preserved_from_profile(self):
        merged = merge_profile_data(self._base_profile(), self._base_vcard())
        assert merged["display_name"] == "John Doe"

    def test_vcard_fills_missing_middle_name(self):
        merged = merge_profile_data(self._base_profile(), self._base_vcard())
        assert merged["middle_name"] == "A."

    def test_vcard_fills_missing_title(self):
        merged = merge_profile_data(self._base_profile(), self._base_vcard())
        assert merged["title"] == "Capt."

    def test_all_emails_deduped(self):
        merged = merge_profile_data(self._base_profile(), self._base_vcard())
        emails = merged["all_emails"]
        assert "john@example.com" in emails
        assert "j.doe@work.com" in emails
        # No duplicates
        assert len(emails) == len(set(emails))

    def test_phones_merged_from_both(self):
        merged = merge_profile_data(self._base_profile(), self._base_vcard())
        subtypes = {p.get("subtype") or p.get("type") for p in merged["phones"]}
        assert "home" in subtypes
        assert "work" in subtypes

    def test_addresses_merged_no_duplicates(self):
        merged = merge_profile_data(self._base_profile(), self._base_vcard())
        raws = [a["raw"] for a in merged["addresses"]]
        assert len(raws) == len(set(raws))
        assert len(raws) == 2  # one from each source

    def test_vcard_raw_preserved(self):
        merged = merge_profile_data(self._base_profile(), self._base_vcard())
        assert "_vcard_raw" in merged

    def test_display_name_falls_back_to_vcard_fn(self):
        profile = self._base_profile()
        profile["display_name"] = None
        merged = merge_profile_data(profile, self._base_vcard())
        assert merged["display_name"] == "John A. Doe"
