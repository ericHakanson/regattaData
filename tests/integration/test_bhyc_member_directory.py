"""Integration tests for BHYC member directory ingestion.

Requires a real PostgreSQL database (via pytest-postgresql).
No live HTTP requests are made; session / HTTP calls are mocked.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from regatta_etl.import_bhyc_member_directory import (
    BhycRunCounters,
    Checkpoint,
    NullArchiver,
    RateLimiter,
    _insert_bhyc_raw_row,
    _ingest_profile,
    _upsert_bhyc_xref_participant,
    parse_profile_html,
    parse_vcard_text,
    merge_profile_data,
    run_bhyc_member_directory,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn(db_conn):
    connection, dsn = db_conn
    yield connection


@pytest.fixture()
def dsn(db_conn):
    _, dsn = db_conn
    yield dsn


def _make_merged_profile(
    display_name: str = "Alice Sailor",
    emails: list[str] | None = None,
    phones: list[dict] | None = None,
    addresses: list[dict] | None = None,
    boats: list[dict] | None = None,
    household: list[dict] | None = None,
    membership_begins: str | None = "2010-06-01",
) -> dict:
    return {
        "display_name": display_name,
        "first_name": display_name.split()[0],
        "last_name": display_name.split()[-1],
        "middle_name": None,
        "title": None,
        "suffix": None,
        "membership_type": "Regular",
        "membership_begins": membership_begins,
        "primary_email": (emails or ["alice@example.com"])[0],
        "secondary_email": None,
        "all_emails": emails or ["alice@example.com"],
        "phones": phones or [{"label": "Home", "value": "(207) 555-1234", "subtype": "home"}],
        "addresses": addresses or [{"raw": "1 Dock St, Boothbay, ME 04538",
                                     "address_type": "mailing",
                                     "line1": "1 Dock St", "city": "Boothbay",
                                     "state": "ME", "postal_code": "04538",
                                     "country_code": None}],
        "boats": boats or [],
        "household": household or [],
        "_raw_field_map": {},
        "_vcard_raw": {},
    }


# ---------------------------------------------------------------------------
# Migration smoke test
# ---------------------------------------------------------------------------

class TestMigration:
    def test_bhyc_member_raw_row_exists(self, conn):
        conn.execute("SELECT 1 FROM bhyc_member_raw_row LIMIT 0")

    def test_bhyc_member_xref_participant_exists(self, conn):
        conn.execute("SELECT 1 FROM bhyc_member_xref_participant LIMIT 0")

    def test_raw_row_unique_constraint(self, conn):
        """Re-inserting same (source_system, member_id, page_type, run_id) does nothing."""
        kwargs = dict(
            member_id="42", page_type="member_profile",
            source_url="https://example.com", run_id="run-1",
            gcs_bucket=None, gcs_object=None,
            http_status=200, content_hash="abc",
            fetched_at=None, parsed_json=None, parse_warnings=None,
        )
        id1 = _insert_bhyc_raw_row(conn, **kwargs)
        id2 = _insert_bhyc_raw_row(conn, **kwargs)  # duplicate → None
        assert id1 is not None
        assert id2 is None


# ---------------------------------------------------------------------------
# _insert_bhyc_raw_row
# ---------------------------------------------------------------------------

class TestInsertBhycRawRow:
    def test_inserts_profile_row(self, conn):
        row_id = _insert_bhyc_raw_row(
            conn,
            member_id="100",
            page_type="member_profile",
            source_url="https://bhyc.net/Default.aspx?p=MemProfile&id=100",
            run_id="run-abc",
            gcs_bucket="my-bucket",
            gcs_object="prefix/run-abc/member_profile/100.html",
            http_status=200,
            content_hash="sha256abc",
            fetched_at=None,
            parsed_json={"display_name": "Bob"},
            parse_warnings=["warn1"],
        )
        assert row_id is not None
        row = conn.execute(
            "SELECT member_id, page_type, gcs_bucket, parsed_json FROM bhyc_member_raw_row WHERE id = %s",
            (row_id,),
        ).fetchone()
        assert row[0] == "100"
        assert row[1] == "member_profile"
        assert row[2] == "my-bucket"
        assert row[3] == {"display_name": "Bob"}

    def test_inserts_vcard_row(self, conn):
        row_id = _insert_bhyc_raw_row(
            conn,
            member_id="101",
            page_type="vcard",
            source_url="https://bhyc.net/GetVcard.aspx?id=101",
            run_id="run-abc",
            gcs_bucket=None,
            gcs_object=None,
            http_status=200,
            content_hash=None,
            fetched_at=None,
            parsed_json=None,
            parse_warnings=None,
        )
        assert row_id is not None

    def test_page_type_check_constraint(self, conn):
        with pytest.raises(Exception, match="bhyc_member_raw_row"):
            conn.execute(
                """
                INSERT INTO bhyc_member_raw_row
                    (source_system, member_id, page_type, source_url, run_id)
                VALUES ('bhyc_member_directory', '1', 'invalid_type', 'url', 'r')
                """
            )


# ---------------------------------------------------------------------------
# _upsert_bhyc_xref_participant
# ---------------------------------------------------------------------------

class TestUpsertBhycXrefParticipant:
    def _insert_participant(self, conn, name="Test Member") -> str:
        from regatta_etl.shared import insert_participant
        return insert_participant(conn, name)

    def test_inserts_primary_member(self, conn):
        pid = self._insert_participant(conn)
        was_new = _upsert_bhyc_xref_participant(conn, "42", pid, None)
        assert was_new is True

    def test_idempotent_primary(self, conn):
        pid = self._insert_participant(conn)
        _upsert_bhyc_xref_participant(conn, "42", pid, None)
        was_new2 = _upsert_bhyc_xref_participant(conn, "42", pid, None)
        assert was_new2 is False

    def test_inserts_household_member(self, conn):
        pid = self._insert_participant(conn)
        was_new = _upsert_bhyc_xref_participant(conn, "42", pid, "spouse")
        assert was_new is True

    def test_primary_and_household_distinct(self, conn):
        pid1 = self._insert_participant(conn, "Primary Person")
        pid2 = self._insert_participant(conn, "Spouse Person")
        _upsert_bhyc_xref_participant(conn, "42", pid1, None)
        _upsert_bhyc_xref_participant(conn, "42", pid2, "spouse")
        count = conn.execute(
            "SELECT COUNT(*) FROM bhyc_member_xref_participant WHERE member_id = '42'"
        ).fetchone()[0]
        assert count == 2


# ---------------------------------------------------------------------------
# _ingest_profile
# ---------------------------------------------------------------------------

class TestIngestProfile:
    def _insert_raw_row(self, conn, member_id="1", run_id="run-x") -> str:
        return _insert_bhyc_raw_row(
            conn,
            member_id=member_id,
            page_type="member_profile",
            source_url=f"https://bhyc.net/profile/{member_id}",
            run_id=run_id,
            gcs_bucket=None,
            gcs_object=None,
            http_status=200,
            content_hash=None,
            fetched_at=None,
            parsed_json=None,
            parse_warnings=None,
        )

    def test_creates_participant(self, conn):
        merged = _make_merged_profile("Carol Captain")
        row_id = self._insert_raw_row(conn, "200")
        counters = BhycRunCounters()
        _ingest_profile(conn, "200", row_id, merged, "run1", counters)
        p = conn.execute(
            "SELECT full_name FROM participant WHERE normalized_full_name = 'carol captain'"
        ).fetchone()
        assert p is not None
        assert "Carol" in p[0]
        assert counters.participants_inserted == 1

    def test_creates_bhyc_membership(self, conn):
        merged = _make_merged_profile()
        row_id = self._insert_raw_row(conn, "201")
        counters = BhycRunCounters()
        _ingest_profile(conn, "201", row_id, merged, "run1", counters)
        count = conn.execute(
            """
            SELECT COUNT(*) FROM club_membership cm
            JOIN yacht_club yc ON yc.id = cm.yacht_club_id
            WHERE yc.normalized_name = 'boothbay-harbor-yacht-club'
            """
        ).fetchone()[0]
        assert count >= 1

    def test_creates_contact_points(self, conn):
        merged = _make_merged_profile(emails=["dave@example.com"])
        row_id = self._insert_raw_row(conn, "202")
        counters = BhycRunCounters()
        _ingest_profile(conn, "202", row_id, merged, "run1", counters)
        count = conn.execute(
            "SELECT COUNT(*) FROM participant_contact_point WHERE contact_type = 'email'"
        ).fetchone()[0]
        assert count >= 1
        assert counters.contact_points_inserted >= 1

    def test_creates_address(self, conn):
        merged = _make_merged_profile()
        row_id = self._insert_raw_row(conn, "203")
        counters = BhycRunCounters()
        _ingest_profile(conn, "203", row_id, merged, "run1", counters)
        assert counters.addresses_inserted >= 1

    def test_creates_candidate_participant(self, conn):
        merged = _make_merged_profile("Eva Evans", emails=["eva@example.com"])
        row_id = self._insert_raw_row(conn, "204")
        counters = BhycRunCounters()
        _ingest_profile(conn, "204", row_id, merged, "run1", counters)
        cand = conn.execute(
            "SELECT id FROM candidate_participant WHERE normalized_name = 'eva evans'"
        ).fetchone()
        assert cand is not None
        assert counters.candidate_created >= 1

    def test_creates_candidate_source_link(self, conn):
        merged = _make_merged_profile("Frank Fisher", emails=["frank@example.com"])
        row_id = self._insert_raw_row(conn, "205")
        counters = BhycRunCounters()
        _ingest_profile(conn, "205", row_id, merged, "run1", counters)
        link = conn.execute(
            "SELECT id FROM candidate_source_link WHERE source_table_name = 'bhyc_member_raw_row'"
        ).fetchone()
        assert link is not None
        assert counters.candidate_links_inserted >= 1

    def test_creates_xref(self, conn):
        merged = _make_merged_profile("Gail Gardner")
        row_id = self._insert_raw_row(conn, "206")
        counters = BhycRunCounters()
        _ingest_profile(conn, "206", row_id, merged, "run1", counters)
        xref = conn.execute(
            "SELECT participant_id FROM bhyc_member_xref_participant WHERE member_id = '206'"
        ).fetchone()
        assert xref is not None
        assert counters.xref_inserted >= 1

    def test_ingest_with_boat(self, conn):
        merged = _make_merged_profile(
            "Harry Harris",
            boats=[{"name": "Wind Dancer", "boat_type": "Ketch"}],
        )
        row_id = self._insert_raw_row(conn, "207")
        counters = BhycRunCounters()
        _ingest_profile(conn, "207", row_id, merged, "run1", counters)
        yacht = conn.execute(
            "SELECT name FROM yacht WHERE normalized_name = 'wind-dancer'"
        ).fetchone()
        assert yacht is not None
        assert counters.yachts_inserted == 1

    def test_ingest_with_household_member(self, conn):
        merged = _make_merged_profile(
            "Iris Irving",
            household=[{"name": "Jack Irving", "relationship": "spouse"}],
        )
        row_id = self._insert_raw_row(conn, "208")
        counters = BhycRunCounters()
        _ingest_profile(conn, "208", row_id, merged, "run1", counters)
        # Household participant created
        hh = conn.execute(
            "SELECT id FROM participant WHERE normalized_full_name = 'jack irving'"
        ).fetchone()  # participant table uses normalized_full_name
        assert hh is not None
        assert counters.household_members_created == 1
        # Xref for household
        xrefs = conn.execute(
            "SELECT relationship_label FROM bhyc_member_xref_participant WHERE member_id = '208'"
        ).fetchall()
        labels = {r[0] for r in xrefs}
        assert None in labels   # primary member
        assert "spouse" in labels

    def test_idempotent_rerun(self, conn):
        """Running _ingest_profile twice for the same member must not duplicate entities."""
        merged = _make_merged_profile("Kate King", emails=["kate@example.com"])
        row_id = self._insert_raw_row(conn, "209")
        counters1 = BhycRunCounters()
        _ingest_profile(conn, "209", row_id, merged, "run1", counters1)

        row_id2 = _insert_bhyc_raw_row(
            conn, member_id="209", page_type="member_profile",
            source_url="https://bhyc.net/profile/209", run_id="run2",
            gcs_bucket=None, gcs_object=None, http_status=200,
            content_hash=None, fetched_at=None, parsed_json=None, parse_warnings=None,
        )
        counters2 = BhycRunCounters()
        _ingest_profile(conn, "209", row_id2, merged, "run2", counters2)

        # Only one participant should exist
        p_count = conn.execute(
            "SELECT COUNT(*) FROM participant WHERE normalized_full_name = 'kate king'"
        ).fetchone()[0]
        assert p_count == 1

        # Second run should match, not insert
        assert counters2.participants_matched_existing == 1
        assert counters2.participants_inserted == 0

    def test_membership_begins_parsed_from_date_string(self, conn):
        merged = _make_merged_profile("Larry Lee", membership_begins="2015-07-04")
        row_id = self._insert_raw_row(conn, "210")
        counters = BhycRunCounters()
        _ingest_profile(conn, "210", row_id, merged, "run1", counters)
        mem = conn.execute(
            """
            SELECT cm.effective_start FROM club_membership cm
            JOIN participant p ON p.id = cm.participant_id
            WHERE p.normalized_full_name = 'larry lee'
            """
        ).fetchone()
        assert mem is not None
        from datetime import date
        assert mem[0] == date(2015, 7, 4)


# ---------------------------------------------------------------------------
# run_bhyc_member_directory (mocked HTTP)
# ---------------------------------------------------------------------------

_DIRECTORY_HTML = """
<html><body>
<a href="/Default.aspx?p=MemProfile&id=1001">Member One</a>
<a href="/Default.aspx?p=MemProfile&id=1002">Member Two</a>
</body></html>
"""

_PROFILE_HTML_1001 = """
<html><body>
<table>
  <tr><td>Name</td><td>Member One</td></tr>
  <tr><td>Email</td><td>one@example.com</td></tr>
  <tr><td>Home Phone</td><td>(207) 555-0001</td></tr>
  <tr><td>Summer Physical Address</td><td>1 First St, Boothbay, ME 04538</td></tr>
</table>
</body></html>
"""

_PROFILE_HTML_1002 = """
<html><body>
<table>
  <tr><td>Name</td><td>Member Two</td></tr>
  <tr><td>Email</td><td>two@example.com</td></tr>
</table>
</body></html>
"""

_VCARD_1001 = "BEGIN:VCARD\nVERSION:3.0\nFN:Member One\nEMAIL:one@example.com\nEND:VCARD\n"
_VCARD_1002 = "BEGIN:VCARD\nVERSION:3.0\nFN:Member Two\nEMAIL:two@example.com\nEND:VCARD\n"


def _make_mock_session(start_url: str) -> MagicMock:
    """Build a requests.Session mock that simulates BHYC responses."""
    session = MagicMock()

    def get(url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.url = url
        if "MemProfile" in url and "id=1001" in url:
            resp.text = _PROFILE_HTML_1001
            resp.content = _PROFILE_HTML_1001.encode()
        elif "MemProfile" in url and "id=1002" in url:
            resp.text = _PROFILE_HTML_1002
            resp.content = _PROFILE_HTML_1002.encode()
        elif "GetVcard" in url and "id=1001" in url:
            resp.text = _VCARD_1001
            resp.content = _VCARD_1001.encode()
        elif "GetVcard" in url and "id=1002" in url:
            resp.text = _VCARD_1002
            resp.content = _VCARD_1002.encode()
        else:
            # Directory listing — returns member links
            resp.text = _DIRECTORY_HTML
            resp.content = _DIRECTORY_HTML.encode()
        return resp

    session.get.side_effect = get
    # Leave session.headers as MagicMock auto-attribute so .update() works
    return session


class TestRunBhycMemberDirectory:
    @patch("regatta_etl.import_bhyc_member_directory.login_session", return_value=True)
    @patch("regatta_etl.import_bhyc_member_directory.requests.Session")
    def test_full_run_two_members(self, mock_session_cls, mock_login, dsn):
        """End-to-end run with two mocked members; no auth or network needed."""
        start_url = "https://www.bhyc.net/Default.aspx?p=v35Directory&ssid=100073&vnf=1"
        session = _make_mock_session(start_url)
        mock_session_cls.return_value = session

        run_id = str(uuid.uuid4())
        counters = BhycRunCounters()
        checkpoint = Checkpoint(Path(f"/tmp/test_ckpt_{run_id}.json"))
        rate_limiter = RateLimiter(base_delay=0.0, jitter=0.0)
        archiver = NullArchiver()

        run_bhyc_member_directory(
            run_id=run_id,
            db_dsn=dsn,
            start_url=start_url,
            username="testuser",
            password="testpass",
            archiver=archiver,
            gcs_bucket=None,
            checkpoint=checkpoint,
            rate_limiter=rate_limiter,
            counters=counters,
            dry_run=False,
        )

        assert counters.safe_stop_reason is None
        assert counters.members_processed == 2
        assert counters.participants_inserted == 2
        assert counters.raw_rows_inserted >= 2  # at minimum profile rows

        # Verify DB state
        conn = psycopg.connect(dsn, autocommit=False)
        try:
            count = conn.execute("SELECT COUNT(*) FROM participant").fetchone()[0]
            assert count == 2
            cand_count = conn.execute("SELECT COUNT(*) FROM candidate_participant").fetchone()[0]
            assert cand_count == 2
            xref_count = conn.execute("SELECT COUNT(*) FROM bhyc_member_xref_participant").fetchone()[0]
            assert xref_count == 2
        finally:
            conn.close()

    @patch("regatta_etl.import_bhyc_member_directory.login_session", return_value=True)
    @patch("regatta_etl.import_bhyc_member_directory.requests.Session")
    def test_dry_run_rolls_back_db(self, mock_session_cls, mock_login, dsn):
        """Dry run: archive + parse happen but no DB rows are committed."""
        start_url = "https://www.bhyc.net/Default.aspx?p=v35Directory"
        session = _make_mock_session(start_url)
        mock_session_cls.return_value = session

        run_id = str(uuid.uuid4())
        counters = BhycRunCounters()
        checkpoint = Checkpoint(Path(f"/tmp/test_ckpt_dry_{run_id}.json"))
        rate_limiter = RateLimiter(base_delay=0.0, jitter=0.0)
        archiver = NullArchiver()

        run_bhyc_member_directory(
            run_id=run_id,
            db_dsn=dsn,
            start_url=start_url,
            username="u",
            password="p",
            archiver=archiver,
            gcs_bucket=None,
            checkpoint=checkpoint,
            rate_limiter=rate_limiter,
            counters=counters,
            dry_run=True,
        )

        # Dry run: counters reflect processing but DB is rolled back
        assert counters.members_processed == 2

        conn = psycopg.connect(dsn, autocommit=False)
        try:
            count = conn.execute("SELECT COUNT(*) FROM participant").fetchone()[0]
            assert count == 0  # rolled back
        finally:
            conn.close()

    @patch("regatta_etl.import_bhyc_member_directory.login_session", return_value=False)
    @patch("regatta_etl.import_bhyc_member_directory.requests.Session")
    def test_auth_failure_stops_safely(self, mock_session_cls, mock_login, dsn):
        """Auth failure is detected and stops without processing members."""
        start_url = "https://www.bhyc.net/Default.aspx?p=v35Directory"
        mock_session_cls.return_value = MagicMock()

        run_id = str(uuid.uuid4())
        counters = BhycRunCounters()
        checkpoint = Checkpoint(Path(f"/tmp/test_ckpt_auth_{run_id}.json"))
        rate_limiter = RateLimiter(base_delay=0.0, jitter=0.0)

        run_bhyc_member_directory(
            run_id=run_id,
            db_dsn=dsn,
            start_url=start_url,
            username="wrong",
            password="wrong",
            archiver=NullArchiver(),
            gcs_bucket=None,
            checkpoint=checkpoint,
            rate_limiter=rate_limiter,
            counters=counters,
            dry_run=False,
        )

        assert counters.safe_stop_reason == "auth_failed"
        assert counters.members_processed == 0
        assert counters.auth_errors == 1

    @patch("regatta_etl.import_bhyc_member_directory.login_session", return_value=True)
    @patch("regatta_etl.import_bhyc_member_directory.requests.Session")
    def test_dry_run_checkpoint_not_persisted(self, mock_session_cls, mock_login, dsn, tmp_path):
        """Dry run must NOT write member_ids to the checkpoint file."""
        start_url = "https://www.bhyc.net/Default.aspx?p=v35Directory"
        session = _make_mock_session(start_url)
        mock_session_cls.return_value = session

        run_id = str(uuid.uuid4())
        cp_path = tmp_path / f"ckpt_dry_{run_id}.json"
        checkpoint = Checkpoint(cp_path)
        rate_limiter = RateLimiter(base_delay=0.0, jitter=0.0)
        counters = BhycRunCounters()

        run_bhyc_member_directory(
            run_id=run_id,
            db_dsn=dsn,
            start_url=start_url,
            username="u",
            password="p",
            archiver=NullArchiver(),
            gcs_bucket=None,
            checkpoint=checkpoint,
            rate_limiter=rate_limiter,
            counters=counters,
            dry_run=True,
        )

        assert counters.members_processed == 2
        # Checkpoint file must not have been written; in-memory set must be empty.
        assert len(checkpoint) == 0, "dry_run must not persist member_ids to checkpoint"
        if cp_path.exists():
            data = json.loads(cp_path.read_text())
            assert data.get("completed_member_ids", []) == [], (
                "checkpoint file must have empty completed_member_ids after dry run"
            )

    @patch("regatta_etl.import_bhyc_member_directory.login_session", return_value=True)
    @patch("regatta_etl.import_bhyc_member_directory.requests.Session")
    def test_checkpoint_skips_completed_members(self, mock_session_cls, mock_login, dsn):
        """Members already in the checkpoint are skipped."""
        start_url = "https://www.bhyc.net/Default.aspx?p=v35Directory"
        session = _make_mock_session(start_url)
        mock_session_cls.return_value = session

        run_id = str(uuid.uuid4())
        counters = BhycRunCounters()
        cp_path = Path(f"/tmp/test_ckpt_skip_{run_id}.json")
        checkpoint = Checkpoint(cp_path)
        checkpoint.mark_done("1001")  # pre-complete member 1001
        checkpoint.mark_done("1002")  # pre-complete member 1002

        run_bhyc_member_directory(
            run_id=run_id,
            db_dsn=dsn,
            start_url=start_url,
            username="u",
            password="p",
            archiver=NullArchiver(),
            gcs_bucket=None,
            checkpoint=checkpoint,
            rate_limiter=RateLimiter(base_delay=0.0, jitter=0.0),
            counters=counters,
            dry_run=False,
        )

        assert counters.members_skipped_checkpoint == 2
        assert counters.members_processed == 0

    @patch("regatta_etl.import_bhyc_member_directory.login_session", return_value=True)
    @patch("regatta_etl.import_bhyc_member_directory.requests.Session")
    def test_idempotent_rerun_no_duplicates(self, mock_session_cls, mock_login, dsn):
        """Running twice with the same data produces no duplicate DB rows."""
        start_url = "https://www.bhyc.net/Default.aspx?p=v35Directory"

        for run_n in range(2):
            session = _make_mock_session(start_url)
            mock_session_cls.return_value = session

            run_id = f"run-{run_n}"
            counters = BhycRunCounters()
            checkpoint = Checkpoint(Path(f"/tmp/test_ckpt_idem_{run_n}.json"))
            rate_limiter = RateLimiter(base_delay=0.0, jitter=0.0)

            run_bhyc_member_directory(
                run_id=run_id,
                db_dsn=dsn,
                start_url=start_url,
                username="u",
                password="p",
                archiver=NullArchiver(),
                gcs_bucket=None,
                checkpoint=checkpoint,
                rate_limiter=rate_limiter,
                counters=counters,
                dry_run=False,
            )

        conn = psycopg.connect(dsn, autocommit=False)
        try:
            p_count = conn.execute("SELECT COUNT(*) FROM participant").fetchone()[0]
            assert p_count == 2  # no duplicates across two runs
            cand_count = conn.execute(
                "SELECT COUNT(*) FROM candidate_participant"
            ).fetchone()[0]
            assert cand_count == 2
        finally:
            conn.close()
