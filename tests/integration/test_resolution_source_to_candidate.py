"""Integration tests for regatta_etl.resolution_source_to_candidate.

Tests run against a real PostgreSQL instance spun up by pytest-postgresql,
with the full migration chain applied (0001–0012).

Each test is isolated (function-scope fixture) and seeds the minimal data
it needs before running the pipeline.
"""

from __future__ import annotations

import json
import uuid
from datetime import date

import psycopg
import pytest

from regatta_etl.resolution_source_to_candidate import (
    _SKIPPED_TABLES,
    club_fingerprint,
    event_fingerprint,
    participant_fingerprint,
    registration_fingerprint,
    run_source_to_candidate,
    yacht_fingerprint,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_uuid() -> str:
    return str(uuid.uuid4())


def _seed_yacht_club(conn: psycopg.Connection, name: str = "Test Club") -> str:
    """Insert a yacht_club and return its id."""
    norm = name.lower().replace(" ", "-")
    row = conn.execute(
        """
        INSERT INTO yacht_club (name, normalized_name, vitality_status)
        VALUES (%s, %s, 'active') RETURNING id
        """,
        (name, norm),
    ).fetchone()
    return str(row[0])


def _seed_event_series(conn: psycopg.Connection, club_id: str, name: str = "Test Series") -> str:
    norm = name.lower().replace(" ", "-")
    row = conn.execute(
        """
        INSERT INTO event_series (yacht_club_id, name, normalized_name)
        VALUES (%s, %s, %s) RETURNING id
        """,
        (club_id, name, norm),
    ).fetchone()
    return str(row[0])


def _seed_event_instance(
    conn: psycopg.Connection,
    series_id: str,
    display_name: str = "Test Regatta 2025",
    season_year: int = 2025,
) -> str:
    row = conn.execute(
        """
        INSERT INTO event_instance (event_series_id, display_name, season_year)
        VALUES (%s, %s, %s) RETURNING id
        """,
        (series_id, display_name, season_year),
    ).fetchone()
    return str(row[0])


def _seed_participant(
    conn: psycopg.Connection,
    full_name: str = "Alice Smith",
    email: str | None = None,
) -> str:
    from regatta_etl.normalize import normalize_name
    norm = normalize_name(full_name)
    row = conn.execute(
        """
        INSERT INTO participant (full_name, normalized_full_name)
        VALUES (%s, %s) RETURNING id
        """,
        (full_name, norm or full_name),
    ).fetchone()
    pid = str(row[0])
    if email:
        conn.execute(
            """
            INSERT INTO participant_contact_point
                (participant_id, contact_type, contact_value_raw,
                 contact_value_normalized, is_primary, source_system)
            VALUES (%s, 'email', %s, %s, true, 'test')
            """,
            (pid, email, email.lower()),
        )
    return pid


def _seed_yacht(
    conn: psycopg.Connection,
    name: str = "Fast Boat",
    sail: str | None = "US-42",
) -> str:
    from regatta_etl.normalize import normalize_name
    norm_name = normalize_name(name)
    norm_sail = sail.upper().replace(" ", "") if sail else None
    row = conn.execute(
        """
        INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number)
        VALUES (%s, %s, %s, %s) RETURNING id
        """,
        (name, norm_name, sail, norm_sail),
    ).fetchone()
    return str(row[0])


def _seed_event_entry(
    conn: psycopg.Connection,
    event_instance_id: str,
    yacht_id: str,
    ext_id: str = "EXT-001",
) -> str:
    row = conn.execute(
        """
        INSERT INTO event_entry
            (event_instance_id, yacht_id, entry_status,
             registration_source, registration_external_id)
        VALUES (%s, %s, 'confirmed', 'test', %s)
        RETURNING id
        """,
        (event_instance_id, yacht_id, ext_id),
    ).fetchone()
    return str(row[0])


# ---------------------------------------------------------------------------
# Club ingestion tests
# ---------------------------------------------------------------------------

class TestClubIngestion:
    def test_yacht_club_creates_candidate(self, db_conn):
        conn, _ = db_conn
        _seed_yacht_club(conn, "Boothbay Harbor Yacht Club")
        ctrs = run_source_to_candidate(conn, entity_type="club")

        assert ctrs.clubs_ingested == 1
        assert ctrs.clubs_candidate_created == 1

        row = conn.execute(
            "SELECT normalized_name FROM candidate_club WHERE normalized_name = 'boothbay-harbor-yacht-club'"
        ).fetchone()
        assert row is not None

    def test_club_source_link_inserted(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn, "Harbor Club")
        run_source_to_candidate(conn, entity_type="club")

        link = conn.execute(
            """
            SELECT candidate_entity_type, source_table_name, source_row_pk
            FROM candidate_source_link
            WHERE source_table_name = 'yacht_club' AND source_row_pk = %s
            """,
            (club_id,),
        ).fetchone()
        assert link is not None
        assert link[0] == "club"

    def test_club_ingestion_is_idempotent(self, db_conn):
        conn, _ = db_conn
        _seed_yacht_club(conn)
        run_source_to_candidate(conn, entity_type="club")
        run_source_to_candidate(conn, entity_type="club")

        count = conn.execute("SELECT COUNT(*) FROM candidate_club").fetchone()[0]
        assert count == 1  # No duplicates after re-run

    def test_source_link_idempotent(self, db_conn):
        conn, _ = db_conn
        _seed_yacht_club(conn)
        run_source_to_candidate(conn, entity_type="club")
        run_source_to_candidate(conn, entity_type="club")

        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_source_link WHERE source_table_name = 'yacht_club'"
        ).fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Event ingestion tests
# ---------------------------------------------------------------------------

class TestEventIngestion:
    def test_event_instance_creates_candidate(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        _seed_event_instance(conn, series_id, "BHYC Regatta 2025", 2025)

        ctrs = run_source_to_candidate(conn, entity_type="event")
        assert ctrs.events_ingested == 1
        assert ctrs.events_candidate_created == 1

    def test_event_source_link(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        eid = _seed_event_instance(conn, series_id)
        run_source_to_candidate(conn, entity_type="event")

        link = conn.execute(
            "SELECT 1 FROM candidate_source_link WHERE source_table_name = 'event_instance' AND source_row_pk = %s",
            (eid,),
        ).fetchone()
        assert link is not None

    def test_event_candidate_stores_season_year(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        _seed_event_instance(conn, series_id, "BHYC Regatta 2024", 2024)
        run_source_to_candidate(conn, entity_type="event")

        row = conn.execute("SELECT season_year FROM candidate_event LIMIT 1").fetchone()
        assert row[0] == 2024

    def test_event_idempotent(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        _seed_event_instance(conn, series_id)
        run_source_to_candidate(conn, entity_type="event")
        run_source_to_candidate(conn, entity_type="event")

        assert conn.execute("SELECT COUNT(*) FROM candidate_event").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Yacht ingestion tests
# ---------------------------------------------------------------------------

class TestYachtIngestion:
    def test_yacht_creates_candidate(self, db_conn):
        conn, _ = db_conn
        _seed_yacht(conn, "Fast Boat", "US-42")
        ctrs = run_source_to_candidate(conn, entity_type="yacht")

        assert ctrs.yachts_ingested == 1
        assert ctrs.yachts_candidate_created == 1
        row = conn.execute("SELECT normalized_sail_number FROM candidate_yacht LIMIT 1").fetchone()
        assert row[0] == "US-42"

    def test_yacht_source_link(self, db_conn):
        conn, _ = db_conn
        yid = _seed_yacht(conn)
        run_source_to_candidate(conn, entity_type="yacht")

        link = conn.execute(
            "SELECT 1 FROM candidate_source_link WHERE source_table_name = 'yacht' AND source_row_pk = %s",
            (yid,),
        ).fetchone()
        assert link is not None

    def test_yacht_idempotent(self, db_conn):
        conn, _ = db_conn
        _seed_yacht(conn)
        run_source_to_candidate(conn, entity_type="yacht")
        run_source_to_candidate(conn, entity_type="yacht")

        assert conn.execute("SELECT COUNT(*) FROM candidate_yacht").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Participant ingestion tests (from participant table)
# ---------------------------------------------------------------------------

class TestParticipantIngestionFromParticipantTable:
    def test_participant_creates_candidate(self, db_conn):
        conn, _ = db_conn
        _seed_participant(conn, "Alice Smith", "alice@example.com")
        ctrs = run_source_to_candidate(conn, entity_type="participant")

        assert ctrs.participants_ingested >= 1
        assert ctrs.participants_candidate_created >= 1

    def test_participant_fingerprint_matches(self, db_conn):
        conn, _ = db_conn
        from regatta_etl.normalize import normalize_name
        _seed_participant(conn, "Alice Smith", "alice@example.com")
        run_source_to_candidate(conn, entity_type="participant")

        fp = participant_fingerprint(normalize_name("Alice Smith"), "alice@example.com")
        row = conn.execute(
            "SELECT id FROM candidate_participant WHERE stable_fingerprint = %s",
            (fp,),
        ).fetchone()
        assert row is not None

    def test_participant_best_email_stored(self, db_conn):
        conn, _ = db_conn
        _seed_participant(conn, "Alice Smith", "alice@example.com")
        run_source_to_candidate(conn, entity_type="participant")

        row = conn.execute("SELECT best_email FROM candidate_participant LIMIT 1").fetchone()
        assert row[0] == "alice@example.com"

    def test_participant_contact_linked(self, db_conn):
        conn, _ = db_conn
        _seed_participant(conn, "Alice Smith", "alice@example.com")
        run_source_to_candidate(conn, entity_type="participant")

        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_contact WHERE contact_type = 'email'"
        ).fetchone()[0]
        assert count >= 1

    def test_participant_source_link(self, db_conn):
        conn, _ = db_conn
        pid = _seed_participant(conn, "Alice Smith")
        run_source_to_candidate(conn, entity_type="participant")

        link = conn.execute(
            "SELECT 1 FROM candidate_source_link WHERE source_table_name = 'participant' AND source_row_pk = %s",
            (pid,),
        ).fetchone()
        assert link is not None

    def test_two_participants_same_email_collapse_to_one_candidate(self, db_conn):
        """Two participant rows with same normalized_name + email → same fingerprint → 1 candidate."""
        conn, _ = db_conn
        _seed_participant(conn, "Alice Smith", "alice@example.com")
        # Insert duplicate directly (different full_name capitalization still normalizes the same)
        _seed_participant(conn, "Alice Smith", "alice@example.com")

        run_source_to_candidate(conn, entity_type="participant")
        count = conn.execute("SELECT COUNT(*) FROM candidate_participant").fetchone()[0]
        # Same fingerprint → 1 candidate, but 2 source links
        assert count == 1
        link_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_source_link WHERE source_table_name = 'participant'"
        ).fetchone()[0]
        assert link_count == 2

    def test_participant_idempotent(self, db_conn):
        conn, _ = db_conn
        _seed_participant(conn, "Bob Jones", "bob@example.com")
        run_source_to_candidate(conn, entity_type="participant")
        run_source_to_candidate(conn, entity_type="participant")

        assert conn.execute("SELECT COUNT(*) FROM candidate_participant").fetchone()[0] == 1

    def test_participant_address_linked(self, db_conn):
        conn, _ = db_conn
        pid = _seed_participant(conn, "Carol White")
        conn.execute(
            """
            INSERT INTO participant_address
                (participant_id, address_type, address_raw, is_primary, source_system)
            VALUES (%s, 'mailing', '123 Main St, Boothbay, ME 04538', true, 'test')
            """,
            (pid,),
        )
        run_source_to_candidate(conn, entity_type="participant")

        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_address"
        ).fetchone()[0]
        assert count >= 1


# ---------------------------------------------------------------------------
# Participant ingestion from jotform
# ---------------------------------------------------------------------------

class TestParticipantIngestionFromJotform:
    def _seed_jotform(self, conn: psycopg.Connection, name: str, email: str) -> str:
        payload = {"Name": name, "Competitor E mail": email, "Submission ID": "SUB-1"}
        payload_json = json.dumps(payload)
        import hashlib
        row_hash = hashlib.sha256(payload_json.encode()).hexdigest()
        row = conn.execute(
            """
            INSERT INTO jotform_waiver_submission
                (source_file_name, source_submission_id, source_submitted_at_raw,
                 raw_payload, row_hash)
            VALUES ('test.csv', 'SUB-1', '2025-01-01', %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (payload_json, row_hash),
        ).fetchone()
        return str(row[0]) if row else ""

    def test_jotform_creates_participant_candidate(self, db_conn):
        conn, _ = db_conn
        self._seed_jotform(conn, "Dave Green", "dave@example.com")
        ctrs = run_source_to_candidate(conn, entity_type="participant")
        assert ctrs.participants_ingested >= 1

    def test_jotform_source_link(self, db_conn):
        conn, _ = db_conn
        self._seed_jotform(conn, "Dave Green", "dave@example.com")
        run_source_to_candidate(conn, entity_type="participant")

        link = conn.execute(
            "SELECT 1 FROM candidate_source_link WHERE source_table_name = 'jotform_waiver_submission'"
        ).fetchone()
        assert link is not None

    def test_jotform_and_participant_table_collapse_to_one_candidate(self, db_conn):
        """Same person in both participant table and jotform → one candidate, two source links."""
        conn, _ = db_conn
        _seed_participant(conn, "Eve Brown", "eve@example.com")
        payload = {"Name": "Eve Brown", "Competitor E mail": "eve@example.com"}
        pjson = json.dumps(payload)
        import hashlib
        row_hash = hashlib.sha256(pjson.encode()).hexdigest()
        conn.execute(
            """
            INSERT INTO jotform_waiver_submission
                (source_file_name, source_submission_id, source_submitted_at_raw,
                 raw_payload, row_hash)
            VALUES ('test.csv', 'SUB-2', '2025-01-01', %s, %s)
            """,
            (pjson, row_hash),
        )
        run_source_to_candidate(conn, entity_type="participant")

        count = conn.execute("SELECT COUNT(*) FROM candidate_participant").fetchone()[0]
        assert count == 1
        link_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_source_link WHERE candidate_entity_type = 'participant'"
        ).fetchone()[0]
        assert link_count == 2


# ---------------------------------------------------------------------------
# Participant ingestion from mailchimp
# ---------------------------------------------------------------------------

class TestParticipantIngestionFromMailchimp:
    def _seed_mailchimp(self, conn: psycopg.Connection, email: str, first: str = "Frank", last: str = "Blue") -> None:
        payload = {"Email Address": email, "First Name": first, "Last Name": last}
        pjson = json.dumps(payload)
        import hashlib
        row_hash = hashlib.sha256(pjson.encode()).hexdigest()
        conn.execute(
            """
            INSERT INTO mailchimp_audience_row
                (source_file_name, audience_status, source_email_raw,
                 source_email_normalized, raw_payload, row_hash)
            VALUES ('subscribed.csv', 'subscribed', %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (email, email.lower(), pjson, row_hash),
        )

    def test_mailchimp_creates_candidate(self, db_conn):
        conn, _ = db_conn
        self._seed_mailchimp(conn, "frank@example.com")
        ctrs = run_source_to_candidate(conn, entity_type="participant")
        assert ctrs.participants_ingested >= 1

    def test_mailchimp_source_link(self, db_conn):
        conn, _ = db_conn
        self._seed_mailchimp(conn, "frank@example.com")
        run_source_to_candidate(conn, entity_type="participant")

        link = conn.execute(
            "SELECT 1 FROM candidate_source_link WHERE source_table_name = 'mailchimp_audience_row'"
        ).fetchone()
        assert link is not None


# ---------------------------------------------------------------------------
# Registration ingestion tests
# ---------------------------------------------------------------------------

class TestRegistrationIngestion:
    def test_event_entry_creates_candidate_registration(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        ei_id = _seed_event_instance(conn, series_id)
        y_id = _seed_yacht(conn)
        _seed_event_entry(conn, ei_id, y_id)

        # Must run clubs + events + yachts first
        run_source_to_candidate(conn, entity_type="all")

        count = conn.execute("SELECT COUNT(*) FROM candidate_registration").fetchone()[0]
        assert count == 1

    def test_registration_source_link(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        ei_id = _seed_event_instance(conn, series_id)
        y_id = _seed_yacht(conn)
        ee_id = _seed_event_entry(conn, ei_id, y_id)

        run_source_to_candidate(conn, entity_type="all")

        link = conn.execute(
            "SELECT 1 FROM candidate_source_link WHERE source_table_name = 'event_entry' AND source_row_pk = %s",
            (ee_id,),
        ).fetchone()
        assert link is not None

    def test_registration_links_candidate_event_and_yacht(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        ei_id = _seed_event_instance(conn, series_id)
        y_id = _seed_yacht(conn)
        _seed_event_entry(conn, ei_id, y_id)

        run_source_to_candidate(conn, entity_type="all")

        reg = conn.execute(
            """
            SELECT candidate_event_id, candidate_yacht_id
            FROM candidate_registration
            LIMIT 1
            """
        ).fetchone()
        assert reg[0] is not None  # candidate_event_id
        assert reg[1] is not None  # candidate_yacht_id

    def test_registration_idempotent(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        ei_id = _seed_event_instance(conn, series_id)
        y_id = _seed_yacht(conn)
        _seed_event_entry(conn, ei_id, y_id)

        run_source_to_candidate(conn, entity_type="all")
        run_source_to_candidate(conn, entity_type="all")

        count = conn.execute("SELECT COUNT(*) FROM candidate_registration").fetchone()[0]
        assert count == 1

    def test_registration_no_candidate_event_skips_gracefully(self, db_conn):
        """If event pipeline hasn't run, registration ingestion skips (no crash)."""
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        ei_id = _seed_event_instance(conn, series_id)
        y_id = _seed_yacht(conn)
        _seed_event_entry(conn, ei_id, y_id)

        # Run only yachts + registrations — skip events
        ctrs = run_source_to_candidate(conn, entity_type="yacht")
        run_source_to_candidate(conn, entity_type="registration")

        # Registration should warn but not crash
        count = conn.execute("SELECT COUNT(*) FROM candidate_registration").fetchone()[0]
        assert count == 0


# ---------------------------------------------------------------------------
# Full "all" entity type run
# ---------------------------------------------------------------------------

class TestFullRun:
    def test_all_entity_types_processed(self, db_conn):
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn, "Grand Regatta Club")
        series_id = _seed_event_series(conn, club_id, "Grand Series")
        ei_id = _seed_event_instance(conn, series_id, "Grand Regatta 2025", 2025)
        y_id = _seed_yacht(conn, "Speed Demon", "US-7")
        pid = _seed_participant(conn, "Helen Gray", "helen@example.com")
        ee_id = _seed_event_entry(conn, ei_id, y_id)

        ctrs = run_source_to_candidate(conn, entity_type="all")

        assert conn.execute("SELECT COUNT(*) FROM candidate_club").fetchone()[0] >= 1
        assert conn.execute("SELECT COUNT(*) FROM candidate_event").fetchone()[0] >= 1
        assert conn.execute("SELECT COUNT(*) FROM candidate_yacht").fetchone()[0] >= 1
        assert conn.execute("SELECT COUNT(*) FROM candidate_participant").fetchone()[0] >= 1
        assert conn.execute("SELECT COUNT(*) FROM candidate_registration").fetchone()[0] >= 1
        assert ctrs.db_errors == 0

    def test_source_lineage_complete(self, db_conn):
        """Every source row produces at least one candidate_source_link."""
        conn, _ = db_conn
        club_id = _seed_yacht_club(conn)
        series_id = _seed_event_series(conn, club_id)
        ei_id = _seed_event_instance(conn, series_id)
        y_id = _seed_yacht(conn)
        ee_id = _seed_event_entry(conn, ei_id, y_id)
        pid = _seed_participant(conn)

        run_source_to_candidate(conn, entity_type="all")

        # Each seeded source table should have at least one link
        for table, pk in [
            ("yacht_club", club_id),
            ("event_instance", ei_id),
            ("yacht", y_id),
            ("participant", pid),
            ("event_entry", ee_id),
        ]:
            link = conn.execute(
                "SELECT 1 FROM candidate_source_link WHERE source_table_name = %s AND source_row_pk = %s",
                (table, pk),
            ).fetchone()
            assert link is not None, f"No source link found for {table} pk={pk}"

    def test_enriched_view_returns_age_for_participant_with_dob(self, db_conn):
        """canonical_participant_enriched computes age_years and is_minor from DOB."""
        conn, _ = db_conn
        # Insert a canonical participant with a known DOB
        conn.execute(
            """
            INSERT INTO canonical_participant (display_name, normalized_name, date_of_birth)
            VALUES ('Young Sailor', 'young-sailor', '2010-06-15')
            """
        )
        row = conn.execute(
            "SELECT age_years, is_minor FROM canonical_participant_enriched LIMIT 1"
        ).fetchone()
        assert row[0] is not None
        assert isinstance(row[1], bool)
        assert row[1] == (row[0] < 18)

    def test_candidate_enriched_view_works(self, db_conn):
        """candidate_participant_enriched view is accessible."""
        conn, _ = db_conn
        _seed_participant(conn, "Old Salt")
        run_source_to_candidate(conn, entity_type="participant")

        row = conn.execute(
            "SELECT age_years, is_minor FROM candidate_participant_enriched LIMIT 1"
        ).fetchone()
        # DOB is null for this participant, so age_years and is_minor should be None
        assert row[0] is None
        assert row[1] is None
