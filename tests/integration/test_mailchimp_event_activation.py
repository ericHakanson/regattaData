"""Integration tests for mailchimp_event_activation pipeline.

Requires a live PostgreSQL instance (via pytest-postgresql).
The db_conn fixture applies all migrations (including 0019, 0020) before each test.
"""

from __future__ import annotations

import csv
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
import psycopg

import sys
from unittest.mock import MagicMock, patch

from regatta_etl.import_mailchimp_event_activation import (
    _AudienceRow,
    _api_upsert,
    _check_dependencies,
    _insert_activation_rows,
    _insert_activation_run,
    _load_suppression_map,
    _query_likely_registrants,
    _query_upcoming_registrants,
    _update_activation_run,
    run_mailchimp_event_activation,
)
from regatta_etl.shared import RunCounters


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _seed_participant(conn, *, email: str, name: str = "Test User",
                      confidence: float = 0.85) -> str:
    """Insert a canonical_participant and return its UUID string."""
    pid = str(uuid.uuid4())
    first, *rest = name.split()
    last = rest[-1] if rest else ""
    conn.execute(
        """
        INSERT INTO canonical_participant
            (id, display_name, first_name, last_name, best_email,
             canonical_confidence_score)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (pid, name, first, last, email, confidence),
    )
    return pid


def _seed_event(conn, *, name: str, start_date: date,
                season_year: int | None = None) -> str:
    eid = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO canonical_event
            (id, event_name, start_date, season_year)
        VALUES (%s, %s, %s, %s)
        """,
        (eid, name, start_date, season_year),
    )
    return eid


def _seed_registration(conn, *, participant_id: str, event_id: str,
                       yacht_id: str | None = None) -> str:
    rid = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO canonical_registration
            (id, canonical_event_id, canonical_primary_participant_id,
             canonical_yacht_id)
        VALUES (%s, %s, %s, %s)
        """,
        (rid, event_id, participant_id, yacht_id),
    )
    return rid


def _seed_yacht(conn, *, name: str) -> str:
    yid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO canonical_yacht (id, name) VALUES (%s, %s)",
        (yid, name),
    )
    return yid


def _seed_suppression(conn, *, email: str, status: str,
                      status_at: datetime | None = None) -> None:
    """Seed a mailchimp_contact_state row (references operational participant table)."""
    # We need an operational participant to satisfy the FK
    op_pid = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO participant (id, full_name, normalized_full_name)
        VALUES (%s, %s, %s)
        """,
        (op_pid, "Suppressed User", "suppressed-user"),
    )
    conn.execute(
        """
        INSERT INTO mailchimp_contact_state
            (participant_id, email_normalized, audience_status, status_at,
             source_file_name, row_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            op_pid, email, status,
            status_at or datetime(2025, 1, 1, tzinfo=timezone.utc),
            "test_file.csv",
            str(uuid.uuid4()),
        ),
    )


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------

def test_check_dependencies_passes_with_full_schema(db_conn):
    conn, _dsn = db_conn
    # Should not raise
    _check_dependencies(conn)


# ---------------------------------------------------------------------------
# Suppression map
# ---------------------------------------------------------------------------

class TestLoadSuppressionMap:
    def test_empty_table_returns_empty_map(self, db_conn):
        conn, _ = db_conn
        result = _load_suppression_map(conn)
        assert result == {}

    def test_unsubscribed_included(self, db_conn):
        conn, _ = db_conn
        _seed_suppression(conn, email="bad@x.com", status="unsubscribed")
        result = _load_suppression_map(conn)
        assert "bad@x.com" in result
        assert result["bad@x.com"] == "unsubscribed"

    def test_cleaned_included(self, db_conn):
        conn, _ = db_conn
        _seed_suppression(conn, email="clean@x.com", status="cleaned")
        result = _load_suppression_map(conn)
        assert "clean@x.com" in result

    def test_subscribed_not_included(self, db_conn):
        conn, _ = db_conn
        _seed_suppression(conn, email="good@x.com", status="subscribed")
        result = _load_suppression_map(conn)
        assert "good@x.com" not in result

    def test_latest_state_wins(self, db_conn):
        conn, _ = db_conn
        # Older: unsubscribed; newer: subscribed → should be eligible
        _seed_suppression(conn, email="flip@x.com", status="unsubscribed",
                          status_at=datetime(2024, 1, 1, tzinfo=timezone.utc))
        _seed_suppression(conn, email="flip@x.com", status="subscribed",
                          status_at=datetime(2025, 1, 1, tzinfo=timezone.utc))
        result = _load_suppression_map(conn)
        assert "flip@x.com" not in result


# ---------------------------------------------------------------------------
# Segment queries
# ---------------------------------------------------------------------------

class TestQueryUpcomingRegistrants:
    def test_participant_with_upcoming_event_included(self, db_conn):
        conn, _ = db_conn
        pid = _seed_participant(conn, email="racer@x.com", name="Alice Race")
        eid = _seed_event(conn, name="Spring Series",
                          start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid, event_id=eid)

        rows = _query_upcoming_registrants(conn, window_days=45)
        pids = [r.participant_id for r in rows]
        assert pid in pids

    def test_participant_with_past_event_excluded(self, db_conn):
        conn, _ = db_conn
        pid = _seed_participant(conn, email="old@x.com", name="Bob Old")
        eid = _seed_event(conn, name="Old Regatta",
                          start_date=date(2020, 6, 1), season_year=2020)
        _seed_registration(conn, participant_id=pid, event_id=eid)

        rows = _query_upcoming_registrants(conn, window_days=45)
        pids = [r.participant_id for r in rows]
        assert pid not in pids

    def test_participant_without_email_excluded(self, db_conn):
        conn, _ = db_conn
        noemail_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO canonical_participant (id, display_name) VALUES (%s, %s)",
            (noemail_id, "No Email"),
        )
        eid = _seed_event(conn, name="Open Race",
                          start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=noemail_id, event_id=eid)

        rows = _query_upcoming_registrants(conn, window_days=45)
        pids = [r.participant_id for r in rows]
        assert noemail_id not in pids

    def test_upcoming_event_count_correct(self, db_conn):
        conn, _ = db_conn
        pid = _seed_participant(conn, email="multi@x.com", name="Multi Event")
        for i in range(3):
            eid = _seed_event(conn, name=f"Race {i}",
                              start_date=date.today(), season_year=2026)
            _seed_registration(conn, participant_id=pid, event_id=eid)

        rows = _query_upcoming_registrants(conn, window_days=45)
        row = next(r for r in rows if r.participant_id == pid)
        assert row.upcoming_event_count == 3


class TestQueryLikelyRegistrants:
    def test_historical_participant_not_registered_upcoming_included(self, db_conn):
        conn, _ = db_conn
        pid = _seed_participant(conn, email="veteran@x.com", name="Veteran Sailor")
        past_event = _seed_event(conn, name="2024 Race",
                                 start_date=date(2024, 6, 1), season_year=2024)
        _seed_registration(conn, participant_id=pid, event_id=past_event)

        rows = _query_likely_registrants(conn, window_days=45, lookback_seasons=3)
        pids = [r.participant_id for r in rows]
        assert pid in pids

    def test_participant_already_registered_for_upcoming_excluded(self, db_conn):
        conn, _ = db_conn
        pid = _seed_participant(conn, email="already@x.com", name="Already Registered")
        past_event = _seed_event(conn, name="Past Race",
                                 start_date=date(2024, 6, 1), season_year=2024)
        _seed_registration(conn, participant_id=pid, event_id=past_event)
        upcoming_event = _seed_event(conn, name="Upcoming Race",
                                     start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid, event_id=upcoming_event)

        rows = _query_likely_registrants(conn, window_days=45, lookback_seasons=3)
        pids = [r.participant_id for r in rows]
        assert pid not in pids

    def test_participant_outside_lookback_excluded(self, db_conn):
        conn, _ = db_conn
        pid = _seed_participant(conn, email="old_sailor@x.com", name="Old Sailor")
        old_event = _seed_event(conn, name="Ancient Race",
                                start_date=date(2018, 6, 1), season_year=2018)
        _seed_registration(conn, participant_id=pid, event_id=old_event)

        rows = _query_likely_registrants(conn, window_days=45, lookback_seasons=3)
        pids = [r.participant_id for r in rows]
        assert pid not in pids


# ---------------------------------------------------------------------------
# End-to-end run_mailchimp_event_activation
# ---------------------------------------------------------------------------

class TestRunMailchimpEventActivation:
    def test_e2e_csv_happy_path(self, db_conn, tmp_path):
        conn, dsn = db_conn
        # Seed two participants: one upcoming, one likely
        pid_upcoming = _seed_participant(conn, email="upcoming@x.com",
                                        name="Up Coming")
        upcoming_event = _seed_event(conn, name="Spring Race",
                                     start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid_upcoming, event_id=upcoming_event)

        pid_likely = _seed_participant(conn, email="likely@x.com",
                                      name="Likely Sailor")
        past_event = _seed_event(conn, name="Past Race",
                                 start_date=date(2024, 6, 1), season_year=2024)
        _seed_registration(conn, participant_id=pid_likely, event_id=past_event)
        conn.commit()

        out = tmp_path / "activation.csv"
        ctrs = RunCounters()
        run_mailchimp_event_activation(
            str(uuid.uuid4()), "2026-01-01T00:00:00",
            dsn, ctrs,
            event_window_days=45,
            segment_type="all",
            delivery_mode="csv",
            output_path=str(out),
            dry_run=False,
        )

        assert ctrs.activation_rows_eligible >= 2
        assert ctrs.activation_rows_exported_csv >= 2
        assert out.exists()

        with out.open() as fh:
            rows = list(csv.DictReader(fh))
        emails = {r["email"] for r in rows}
        assert "upcoming@x.com" in emails
        assert "likely@x.com" in emails

    def test_suppressed_contacts_excluded_from_csv(self, db_conn, tmp_path):
        conn, dsn = db_conn
        pid = _seed_participant(conn, email="suppress@x.com", name="Bad Email")
        eid = _seed_event(conn, name="Any Race",
                          start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid, event_id=eid)
        _seed_suppression(conn, email="suppress@x.com", status="unsubscribed")
        conn.commit()

        out = tmp_path / "activation.csv"
        ctrs = RunCounters()
        run_mailchimp_event_activation(
            str(uuid.uuid4()), "2026-01-01T00:00:00",
            dsn, ctrs,
            delivery_mode="csv",
            output_path=str(out),
            dry_run=False,
        )

        assert ctrs.activation_rows_suppressed_unsubscribed >= 1
        with out.open() as fh:
            rows = list(csv.DictReader(fh))
        assert all(r["email"] != "suppress@x.com" for r in rows)

    def test_duplicate_email_collapses_to_one_csv_row(self, db_conn, tmp_path):
        conn, dsn = db_conn
        # Two participants sharing the same email — only one should appear in CSV
        pid_a = _seed_participant(conn, email="shared@x.com", name="Alice A",
                                  confidence=0.9)
        pid_b = _seed_participant(conn, email="shared@x.com", name="Bob B",
                                  confidence=0.5)
        eid = _seed_event(conn, name="Shared Event",
                          start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid_a, event_id=eid)
        _seed_registration(conn, participant_id=pid_b, event_id=eid)
        conn.commit()

        out = tmp_path / "activation.csv"
        ctrs = RunCounters()
        run_mailchimp_event_activation(
            str(uuid.uuid4()), "2026-01-01T00:00:00",
            dsn, ctrs,
            delivery_mode="csv",
            output_path=str(out),
            dry_run=False,
        )

        assert ctrs.activation_rows_deduped_out >= 1
        with out.open() as fh:
            rows = list(csv.DictReader(fh))
        shared_rows = [r for r in rows if r["email"] == "shared@x.com"]
        assert len(shared_rows) == 1

    def test_audit_tables_populated_on_real_run(self, db_conn, tmp_path):
        conn, dsn = db_conn
        pid = _seed_participant(conn, email="audit@x.com", name="Audit User")
        eid = _seed_event(conn, name="Audit Race",
                          start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid, event_id=eid)
        conn.commit()

        run_id = str(uuid.uuid4())
        out = tmp_path / "audit.csv"
        ctrs = RunCounters()
        run_mailchimp_event_activation(
            run_id, "2026-01-01T00:00:00",
            dsn, ctrs,
            delivery_mode="csv",
            output_path=str(out),
            dry_run=False,
        )

        # Read back from audit tables via a fresh connection
        check_conn = psycopg.connect(dsn)
        try:
            run_row = check_conn.execute(
                "SELECT status, mode, segment_type FROM mailchimp_activation_run WHERE id = %s",
                (run_id,),
            ).fetchone()
            assert run_row is not None
            assert run_row[0] == "ok"
            assert run_row[1] == "csv"

            audit_rows = check_conn.execute(
                "SELECT COUNT(*) FROM mailchimp_activation_row WHERE run_id = %s",
                (run_id,),
            ).fetchone()
            assert audit_rows[0] >= 1
        finally:
            check_conn.close()

    def test_dry_run_no_csv_written_and_no_db_writes(self, db_conn, tmp_path):
        conn, dsn = db_conn
        pid = _seed_participant(conn, email="dryrun@x.com", name="Dry Runner")
        eid = _seed_event(conn, name="Dry Race",
                          start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid, event_id=eid)
        conn.commit()

        out = tmp_path / "dryrun.csv"
        ctrs = RunCounters()
        run_mailchimp_event_activation(
            str(uuid.uuid4()), "2026-01-01T00:00:00",
            dsn, ctrs,
            delivery_mode="csv",
            output_path=str(out),
            dry_run=True,
        )

        # CSV must not be written in dry run
        assert not out.exists()

        # Counters show what WOULD have been exported
        assert ctrs.activation_rows_exported_csv >= 1

        # Audit table must be empty (no writes committed)
        check_conn = psycopg.connect(dsn)
        try:
            count = check_conn.execute(
                "SELECT COUNT(*) FROM mailchimp_activation_run"
            ).fetchone()[0]
            assert count == 0
        finally:
            check_conn.close()

    def test_idempotent_rerun_produces_stable_output(self, db_conn, tmp_path):
        conn, dsn = db_conn
        pid = _seed_participant(conn, email="idempotent@x.com", name="Idem Sailor")
        eid = _seed_event(conn, name="Idem Race",
                          start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid, event_id=eid)
        conn.commit()

        out1 = tmp_path / "run1.csv"
        out2 = tmp_path / "run2.csv"

        for out in [out1, out2]:
            ctrs = RunCounters()
            run_mailchimp_event_activation(
                str(uuid.uuid4()), "2026-01-01T00:00:00",
                dsn, ctrs,
                delivery_mode="csv",
                output_path=str(out),
                dry_run=False,
            )

        # Both runs produce stable row selection (generated_at differs per run by design)
        def _load_rows_sans_timestamp(path: Path) -> list[dict]:
            with path.open() as fh:
                rows = list(csv.DictReader(fh))
            for r in rows:
                r.pop("generated_at", None)
            return rows

        rows1 = _load_rows_sans_timestamp(out1)
        rows2 = _load_rows_sans_timestamp(out2)
        assert rows1 == rows2

    def test_segment_type_upcoming_only(self, db_conn, tmp_path):
        conn, dsn = db_conn
        # Only upcoming participant
        pid = _seed_participant(conn, email="upcoming_only@x.com", name="Up Only")
        eid = _seed_event(conn, name="Up Race",
                          start_date=date.today(), season_year=2026)
        _seed_registration(conn, participant_id=pid, event_id=eid)
        # Likely-only participant (past registration, no upcoming)
        pid_likely = _seed_participant(conn, email="likely_only@x.com", name="Likely")
        past_eid = _seed_event(conn, name="Past",
                               start_date=date(2024, 1, 1), season_year=2024)
        _seed_registration(conn, participant_id=pid_likely, event_id=past_eid)
        conn.commit()

        out = tmp_path / "seg.csv"
        ctrs = RunCounters()
        run_mailchimp_event_activation(
            str(uuid.uuid4()), "2026-01-01T00:00:00",
            dsn, ctrs,
            segment_type="upcoming_registrants",
            delivery_mode="csv",
            output_path=str(out),
            dry_run=False,
        )

        with out.open() as fh:
            rows = list(csv.DictReader(fh))
        emails = {r["email"] for r in rows}
        assert "upcoming_only@x.com" in emails
        assert "likely_only@x.com" not in emails


# ---------------------------------------------------------------------------
# API identity persistence integration tests
# ---------------------------------------------------------------------------

def _make_audience_row(pid: str, email: str) -> _AudienceRow:
    return _AudienceRow(
        email_normalized=email,
        participant_id=pid,
        first_name="Test",
        last_name="Sailor",
        display_name="Test Sailor",
        segment_types=["upcoming_registrants"],
        upcoming_event_count=1,
        historical_registration_count=1,
        is_suppressed=False,
        suppression_reason=None,
        yacht_name=None,
        last_registered_event_name=None,
        generated_at="2026-01-01T00:00:00+00:00",
    )


def _build_mock_mc(unique_email_id: str = "UID-001", *, as_dict: bool = True):
    """Return a sys.modules patch dict that mocks mailchimp_marketing.

    ``as_dict=True`` (default) returns a plain dict response, matching the
    actual mailchimp_marketing library behaviour.  ``as_dict=False`` uses a
    MagicMock object to exercise the attribute-access fallback path.
    """
    if as_dict:
        mock_response = {"unique_email_id": unique_email_id, "id": unique_email_id}
    else:
        mock_response = MagicMock()
        mock_response.unique_email_id = unique_email_id

    mock_lists = MagicMock()
    mock_lists.set_list_member.return_value = mock_response

    mock_client_instance = MagicMock()
    mock_client_instance.lists = mock_lists

    mock_mc = MagicMock()
    mock_mc.Client.return_value = mock_client_instance

    mock_api_client = MagicMock()
    mock_api_client.ApiClientError = Exception

    return {"mailchimp_marketing": mock_mc, "mailchimp_marketing.api_client": mock_api_client}


class TestApiIdentityPersistence:
    """Identity link persistence for API-mode activation."""

    def test_api_contact_id_persisted_on_success(self, db_conn):
        """Successful API call persists unique_email_id into participant_mailchimp_identity."""
        conn, _dsn = db_conn

        pid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO participant (id, full_name, normalized_full_name) "
            "VALUES (%s, 'Api Sailor', 'api sailor')",
            (pid,),
        )
        conn.commit()

        row = _make_audience_row(pid, "api@example.com")
        ctrs = RunCounters()

        # as_dict=True mirrors actual mailchimp_marketing dict response
        with patch.dict(sys.modules, _build_mock_mc("CONTACT-ABC", as_dict=True)):
            _api_upsert([row], "list-id", "fake-api-key-us6", ctrs, conn=conn)

        assert ctrs.activation_rows_api_upserted == 1
        assert ctrs.activation_rows_api_failed == 0
        assert ctrs.mailchimp_identity_links_inserted == 1

        links = conn.execute(
            "SELECT mailchimp_contact_id FROM participant_mailchimp_identity "
            "WHERE participant_id = %s::uuid",
            (pid,),
        ).fetchall()
        assert len(links) == 1
        assert links[0][0] == "CONTACT-ABC"

    def test_api_contact_id_persisted_object_response(self, db_conn):
        """Attribute-style response (as_dict=False) also persists contact ID."""
        conn, _dsn = db_conn

        pid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO participant (id, full_name, normalized_full_name) "
            "VALUES (%s, 'Attr Sailor', 'attr sailor')",
            (pid,),
        )
        conn.commit()

        row = _make_audience_row(pid, "attr@example.com")
        ctrs = RunCounters()

        with patch.dict(sys.modules, _build_mock_mc("CONTACT-OBJ", as_dict=False)):
            _api_upsert([row], "list-id", "fake-api-key-us6", ctrs, conn=conn)

        assert ctrs.activation_rows_api_upserted == 1
        assert ctrs.mailchimp_identity_links_inserted == 1

        row_db = conn.execute(
            "SELECT mailchimp_contact_id FROM participant_mailchimp_identity "
            "WHERE participant_id = %s::uuid",
            (pid,),
        ).fetchone()
        assert row_db[0] == "CONTACT-OBJ"

    def test_api_contact_id_conflict_fails_row(self, db_conn):
        """contact_id already linked to P1; API returns same ID for P2 → row fails, queue row."""
        conn, _dsn = db_conn

        pid1 = str(uuid.uuid4())
        pid2 = str(uuid.uuid4())
        for pid, name in [(pid1, "Sailor One"), (pid2, "Sailor Two")]:
            conn.execute(
                "INSERT INTO participant (id, full_name, normalized_full_name) "
                "VALUES (%s, %s, %s)",
                (pid, name, name.lower()),
            )
        # Pre-link CONTACT-SHARED to pid1
        conn.execute(
            """
            INSERT INTO participant_mailchimp_identity
                (participant_id, mailchimp_contact_id, email_normalized, source_system)
            VALUES (%s::uuid, %s, %s, 'test')
            """,
            (pid1, "CONTACT-SHARED", "sailor1@example.com"),
        )
        conn.commit()

        row = _make_audience_row(pid2, "sailor2@example.com")
        ctrs = RunCounters()

        with patch.dict(sys.modules, _build_mock_mc("CONTACT-SHARED")):
            _api_upsert([row], "list-id", "fake-api-key-us6", ctrs, conn=conn)

        assert ctrs.activation_rows_api_upserted == 0
        assert ctrs.activation_rows_api_failed == 1
        assert ctrs.mailchimp_contact_id_conflicts == 1
        assert ctrs.mailchimp_identity_conflicts == 1

        # Review queue row inserted
        queue = conn.execute(
            "SELECT reason_code FROM mailchimp_identity_review_queue"
        ).fetchall()
        assert len(queue) == 1
        assert queue[0][0] == "mailchimp_contact_id_conflict"

    def test_api_contact_id_same_participant_second_email_no_index_crash(self, db_conn):
        """Same participant processed under a second email with the same contact ID.

        mailchimp_contact_id is globally unique (partial index).  If the same
        participant already has the contact ID on email1's row, processing email2
        must not crash: the contact ID is suppressed on the second row.
        """
        conn, _dsn = db_conn

        pid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO participant (id, full_name, normalized_full_name) "
            "VALUES (%s, 'Dual Email', 'dual email')",
            (pid,),
        )
        # Pre-link contact ID to pid / email1
        conn.execute(
            """
            INSERT INTO participant_mailchimp_identity
                (participant_id, mailchimp_contact_id, email_normalized, source_system)
            VALUES (%s::uuid, %s, %s, 'test')
            """,
            (pid, "CONTACT-DUAL", "dual1@example.com"),
        )
        conn.commit()

        # Activate pid under email2 — API returns the same contact ID
        row = _make_audience_row(pid, "dual2@example.com")
        ctrs = RunCounters()

        with patch.dict(sys.modules, _build_mock_mc("CONTACT-DUAL", as_dict=True)):
            _api_upsert([row], "list-id", "fake-api-key-us6", ctrs, conn=conn)

        # Row succeeds — no crash, no conflict queue row
        assert ctrs.activation_rows_api_upserted == 1
        assert ctrs.activation_rows_api_failed == 0
        assert ctrs.mailchimp_contact_id_conflicts == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM mailchimp_identity_review_queue"
        ).fetchone()[0] == 0

        # email2 row exists; contact_id is NULL (suppressed) to avoid index violation
        row_db = conn.execute(
            "SELECT mailchimp_contact_id FROM participant_mailchimp_identity "
            "WHERE participant_id = %s::uuid AND email_normalized = %s",
            (pid, "dual2@example.com"),
        ).fetchone()
        assert row_db is not None
        assert row_db[0] is None  # suppressed
