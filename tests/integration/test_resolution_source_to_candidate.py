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
    run_under_combination_remediation,
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
    """Existing smoke tests updated to use resolved participant seeding."""

    def test_mailchimp_creates_candidate(self, db_conn):
        conn, _ = db_conn
        _seed_mailchimp_resolved(conn, "frank@example.com", "Frank Blue")
        ctrs = run_source_to_candidate(conn, entity_type="participant")
        assert ctrs.participants_ingested >= 1
        assert ctrs.db_errors == 0

    def test_mailchimp_source_link(self, db_conn):
        conn, _ = db_conn
        _seed_mailchimp_resolved(conn, "frank@example.com", "Frank Blue")
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


# ---------------------------------------------------------------------------
# Under-combination prevention tests
# ---------------------------------------------------------------------------

def _seed_ys_raw_row(conn: psycopg.Connection, owner_name: str) -> str:
    """Insert a minimal yacht_scoring_raw_row with an owner name and return its id."""
    import json as _json
    row = conn.execute(
        """
        INSERT INTO yacht_scoring_raw_row
            (asset_type, source_file_name, source_file_path, source_row_ordinal,
             raw_payload, row_hash, source_system)
        VALUES ('deduplicated_entry', 'test_file.csv', '/tmp/test_file.csv', 1,
                %s, %s, 'test')
        RETURNING id
        """,
        (_json.dumps({"ownerName": owner_name}), f"hash-{owner_name}"),
    ).fetchone()
    return str(row[0])


class TestUnderCombinationPrevention:
    """Prevention: name-only rows must reuse the existing email-bearing candidate."""

    def test_name_only_after_name_email_reuses_existing_candidate(self, db_conn):
        """Seed participant with email, then a name-only yacht-scoring row.
        Result: exactly one candidate_participant row.
        """
        conn, _ = db_conn
        from regatta_etl.normalize import normalize_email, normalize_name

        # Seed participant with email (creates name+email candidate)
        pid = _seed_participant(conn, "Adam Langerman", "adam@example.com")
        run_source_to_candidate(conn, entity_type="participant")

        # Verify one candidate exists with email
        count_before = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant WHERE normalized_name = %s",
            (normalize_name("Adam Langerman"),),
        ).fetchone()[0]
        assert count_before == 1

        # Seed yacht_scoring row with same name but no email
        _seed_ys_raw_row(conn, "Adam Langerman")
        ctrs = run_source_to_candidate(conn, entity_type="participant")

        # Still exactly one candidate
        count_after = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant WHERE normalized_name = %s",
            (normalize_name("Adam Langerman"),),
        ).fetchone()[0]
        assert count_after == 1, (
            f"Expected 1 candidate after name-only ingest, got {count_after}"
        )
        assert ctrs.participants_under_combination_reused >= 1
        assert ctrs.db_errors == 0

    def test_name_only_no_email_bearing_candidate_creates_new(self, db_conn):
        """Name-only row with no existing email-bearing candidate → normal new candidate."""
        conn, _ = db_conn
        from regatta_etl.normalize import normalize_name

        _seed_ys_raw_row(conn, "Unknown Sailor")
        ctrs = run_source_to_candidate(conn, entity_type="participant")

        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant WHERE normalized_name = %s",
            (normalize_name("Unknown Sailor"),),
        ).fetchone()[0]
        assert count == 1
        assert ctrs.participants_under_combination_reused == 0
        assert ctrs.participants_candidate_created >= 1
        assert ctrs.db_errors == 0

    def test_name_only_ambiguous_email_bearing_candidates_does_not_guess(self, db_conn):
        """Two email-bearing candidates with same name + name-only row → no forced merge,
        warning emitted, candidate count is 3 (2 email-bearing + 1 new name-only).
        """
        conn, _ = db_conn
        from regatta_etl.normalize import normalize_name

        norm = normalize_name("Common Name")

        # Directly insert two email-bearing candidates with same normalized_name
        for email in ("first@example.com", "second@example.com"):
            fp = participant_fingerprint(norm, email)
            conn.execute(
                """
                INSERT INTO candidate_participant
                    (stable_fingerprint, display_name, normalized_name, best_email)
                VALUES (%s, %s, %s, %s)
                """,
                (fp, "Common Name", norm, email),
            )

        _seed_ys_raw_row(conn, "Common Name")
        ctrs = run_source_to_candidate(conn, entity_type="participant")

        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant WHERE normalized_name = %s",
            (norm,),
        ).fetchone()[0]
        # Should be 3: 2 email-bearing + 1 new name-only (no forced merge)
        assert count == 3
        assert ctrs.participants_under_combination_ambiguous >= 1
        assert ctrs.participants_under_combination_reused == 0
        assert any("under_combination_ambiguous" in w for w in ctrs.warnings)
        assert ctrs.db_errors == 0

    def test_repeated_ingest_does_not_grow_candidates(self, db_conn):
        """Running participant ingest twice does not create extra candidates (idempotency)."""
        conn, _ = db_conn
        from regatta_etl.normalize import normalize_name

        pid = _seed_participant(conn, "Betty Regatta", "betty@example.com")
        _seed_ys_raw_row(conn, "Betty Regatta")

        run_source_to_candidate(conn, entity_type="participant")
        run_source_to_candidate(conn, entity_type="participant")

        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant WHERE normalized_name = %s",
            (normalize_name("Betty Regatta"),),
        ).fetchone()[0]
        assert count == 1

    def test_prevention_counter_in_report(self, db_conn):
        """build_pipeline_report includes under-combination lines."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import build_pipeline_report

        _seed_participant(conn, "Counter Test", "ct@example.com")
        _seed_ys_raw_row(conn, "Counter Test")
        ctrs = run_source_to_candidate(conn, entity_type="participant")

        report = build_pipeline_report(ctrs)
        assert "Under-Combination Prevention" in report
        assert "reused email-bearer" in report


# ---------------------------------------------------------------------------
# Under-combination remediation tests
# ---------------------------------------------------------------------------

def _insert_candidate_participant(
    conn: psycopg.Connection,
    display_name: str,
    norm_name: str,
    best_email: str | None,
    resolution_state: str = "review",
    is_promoted: bool = False,
    quality_score: float = 0.0,
) -> str:
    """Directly insert a candidate_participant row and return its id."""
    fp = participant_fingerprint(norm_name, best_email)
    row = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name, best_email,
             resolution_state, is_promoted, quality_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (fp, display_name, norm_name, best_email, resolution_state, is_promoted, quality_score),
    ).fetchone()
    return str(row[0])


def _insert_candidate_event(
    conn: psycopg.Connection,
    event_name: str = "Under Combination Event",
    normalized_event_name: str = "under-combination-event",
    season_year: int = 2025,
) -> str:
    """Directly insert a candidate_event row and return its id."""
    fp = event_fingerprint(normalized_event_name, season_year, None)
    row = conn.execute(
        """
        INSERT INTO candidate_event
            (stable_fingerprint, event_name, normalized_event_name, season_year, resolution_state)
        VALUES (%s, %s, %s, %s, 'review')
        RETURNING id
        """,
        (fp, event_name, normalized_event_name, season_year),
    ).fetchone()
    return str(row[0])


def _insert_candidate_registration(
    conn: psycopg.Connection,
    candidate_event_id: str,
    candidate_primary_participant_id: str | None,
    registration_external_id: str = "UC-REG-001",
) -> str:
    """Directly insert a candidate_registration row and return its id."""
    fp = registration_fingerprint(candidate_event_id, registration_external_id, None)
    row = conn.execute(
        """
        INSERT INTO candidate_registration
            (stable_fingerprint, registration_external_id, candidate_event_id,
             candidate_primary_participant_id, resolution_state)
        VALUES (%s, %s, %s, %s, 'review')
        RETURNING id
        """,
        (fp, registration_external_id, candidate_event_id, candidate_primary_participant_id),
    ).fetchone()
    return str(row[0])


def _insert_candidate_source_link(
    conn: psycopg.Connection,
    entity_type: str,
    candidate_id: str,
    source_table: str,
    source_pk: str,
) -> None:
    conn.execute(
        """
        INSERT INTO candidate_source_link
            (candidate_entity_type, candidate_entity_id, source_table_name, source_row_pk)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (entity_type, candidate_id, source_table, source_pk),
    )


class TestUnderCombinationRemediation:
    """One-time remediation: merge existing split pairs."""

    def _seed_split_pair(self, conn: psycopg.Connection):
        """Seed a winner (email-bearing, auto_promote) + loser (name-only, reject)."""
        from regatta_etl.normalize import normalize_name
        norm = normalize_name("Split Person")
        winner_id = _insert_candidate_participant(
            conn, "Split Person", norm, "split@example.com",
            resolution_state="auto_promote", quality_score=0.8,
        )
        loser_id = _insert_candidate_participant(
            conn, "Split Person", norm, None,
            resolution_state="reject",
        )
        # Attach a source link to the loser
        _insert_candidate_source_link(
            conn, "participant", loser_id,
            "yacht_scoring_raw_row", "fake-ys-pk-001",
        )
        return winner_id, loser_id

    def test_remediation_merges_existing_split_pair(self, db_conn):
        """Remediation deletes loser, retains winner, transfers source links."""
        conn, _ = db_conn
        winner_id, loser_id = self._seed_split_pair(conn)

        ctrs = run_under_combination_remediation(conn, dry_run=False)

        assert ctrs.groups_examined >= 1
        assert ctrs.groups_merged >= 1
        assert ctrs.loser_rows_deleted >= 1
        assert ctrs.links_transferred >= 1
        assert ctrs.db_errors == 0

        # Loser must be gone
        loser_row = conn.execute(
            "SELECT 1 FROM candidate_participant WHERE id = %s", (loser_id,)
        ).fetchone()
        assert loser_row is None

        # Winner must still exist
        winner_row = conn.execute(
            "SELECT 1 FROM candidate_participant WHERE id = %s", (winner_id,)
        ).fetchone()
        assert winner_row is not None

        # Source link transferred to winner
        link = conn.execute(
            """
            SELECT 1 FROM candidate_source_link
            WHERE candidate_entity_id = %s AND source_table_name = 'yacht_scoring_raw_row'
            """,
            (winner_id,),
        ).fetchone()
        assert link is not None

        # Loser's original source links must be deleted (no orphans)
        orphan = conn.execute(
            "SELECT 1 FROM candidate_source_link WHERE candidate_entity_id = %s",
            (loser_id,),
        ).fetchone()
        assert orphan is None

    def test_remediation_repoints_candidate_registration_fk_before_delete(self, db_conn):
        """Loser referenced by candidate_registration is repointed to winner then deleted."""
        conn, _ = db_conn
        winner_id, loser_id = self._seed_split_pair(conn)
        event_id = _insert_candidate_event(conn)
        registration_id = _insert_candidate_registration(
            conn,
            candidate_event_id=event_id,
            candidate_primary_participant_id=loser_id,
            registration_external_id="UC-REG-FK",
        )

        ctrs = run_under_combination_remediation(conn, dry_run=False)

        assert ctrs.db_errors == 0
        loser_row = conn.execute(
            "SELECT 1 FROM candidate_participant WHERE id = %s",
            (loser_id,),
        ).fetchone()
        assert loser_row is None

        reg_row = conn.execute(
            """
            SELECT candidate_primary_participant_id
            FROM candidate_registration
            WHERE id = %s
            """,
            (registration_id,),
        ).fetchone()
        assert reg_row is not None
        assert str(reg_row[0]) == winner_id

    def test_remediation_dry_run_no_writes(self, db_conn):
        """Dry-run reports planned merges but leaves DB unchanged."""
        conn, _ = db_conn
        winner_id, loser_id = self._seed_split_pair(conn)

        ctrs = run_under_combination_remediation(conn, dry_run=True)

        assert ctrs.groups_examined >= 1
        assert ctrs.loser_rows_deleted >= 1  # planned
        assert ctrs.db_errors == 0

        # DB must be unchanged
        loser_row = conn.execute(
            "SELECT 1 FROM candidate_participant WHERE id = %s", (loser_id,)
        ).fetchone()
        assert loser_row is not None, "Dry run must not delete loser"

    def test_remediation_skips_ineligible_loser(self, db_conn):
        """Loser that is not in 'reject' state must not be deleted."""
        conn, _ = db_conn
        from regatta_etl.normalize import normalize_name
        norm = normalize_name("Ineligible Person")
        _insert_candidate_participant(
            conn, "Ineligible Person", norm, "ip@example.com",
            resolution_state="auto_promote", quality_score=0.9,
        )
        # Null-email candidate in 'hold' state — ineligible (not 'reject')
        loser_id = _insert_candidate_participant(
            conn, "Ineligible Person", norm, None,
            resolution_state="hold", quality_score=0.0,
        )

        ctrs = run_under_combination_remediation(conn, dry_run=False)

        # Loser must still exist (ineligible because resolution_state != 'reject')
        loser_row = conn.execute(
            "SELECT 1 FROM candidate_participant WHERE id = %s", (loser_id,)
        ).fetchone()
        assert loser_row is not None
        # No losers were eligible, so groups_merged == 0 and conflicts_skipped >= 1
        assert ctrs.loser_rows_deleted == 0
        assert ctrs.conflicts_skipped >= 1

    def test_remediation_audit_log_written(self, db_conn):
        """Successful merge writes a resolution_manual_action_log entry."""
        conn, _ = db_conn
        winner_id, loser_id = self._seed_split_pair(conn)

        run_under_combination_remediation(conn, dry_run=False)

        log_row = conn.execute(
            """
            SELECT action_type, reason_code, actor, source
            FROM resolution_manual_action_log
            WHERE entity_type = 'participant'
              AND action_type = 'merge'
            LIMIT 1
            """
        ).fetchone()
        assert log_row is not None
        assert log_row[0] == "merge"
        assert log_row[1] == "under_combination_consolidation"
        assert log_row[2] == "pipeline_under_combination_fix"
        assert log_row[3] == "pipeline"

    def test_no_split_groups_returns_zero_examined(self, db_conn):
        """No split groups → zero groups_examined, no errors."""
        conn, _ = db_conn
        # Seed only a clean participant with email
        from regatta_etl.normalize import normalize_name
        norm = normalize_name("Clean Participant")
        _insert_candidate_participant(
            conn, "Clean Participant", norm, "clean@example.com",
            resolution_state="review",
        )

        ctrs = run_under_combination_remediation(conn, dry_run=False)

        assert ctrs.groups_examined == 0
        assert ctrs.loser_rows_deleted == 0
        assert ctrs.db_errors == 0


# ---------------------------------------------------------------------------
# Contact and address child evidence ingestion tests (spec T1–T5)
# ---------------------------------------------------------------------------

def _seed_mailchimp_resolved(
    conn: psycopg.Connection,
    email: str,
    full_name: str = "Frank Blue",
    phone: str | None = None,
    address_raw: str | None = None,
) -> tuple[str, str]:
    """Seed a participant, mailchimp_audience_row, and mailchimp_contact_state.

    Returns (participant_id, audience_row_id).
    Only rows with a matching mailchimp_contact_state are projected by the new
    Mailchimp candidate path (quarantined rows have no contact_state row).
    """
    from regatta_etl.normalize import normalize_name
    norm = normalize_name(full_name) or full_name
    pid = str(conn.execute(
        "INSERT INTO participant (full_name, normalized_full_name) VALUES (%s, %s) RETURNING id",
        (full_name, norm),
    ).fetchone()[0])

    conn.execute(
        """
        INSERT INTO participant_contact_point
            (participant_id, contact_type, contact_value_raw,
             contact_value_normalized, is_primary, source_system)
        VALUES (%s, 'email', %s, %s, true, 'test')
        """,
        (pid, email, email.lower()),
    )

    if phone:
        from regatta_etl.normalize import normalize_phone
        norm_phone = normalize_phone(phone)
        conn.execute(
            """
            INSERT INTO participant_contact_point
                (participant_id, contact_type, contact_value_raw,
                 contact_value_normalized, is_primary, source_system)
            VALUES (%s, 'phone', %s, %s, true, 'test')
            """,
            (pid, phone, norm_phone),
        )

    if address_raw:
        conn.execute(
            """
            INSERT INTO participant_address
                (participant_id, address_type, address_raw, is_primary, source_system)
            VALUES (%s, 'mailing', %s, true, 'test')
            """,
            (pid, address_raw),
        )

    # Build a mailchimp_audience_row for this participant
    first, *rest = full_name.split(" ", 1)
    last = rest[0] if rest else ""
    payload = {"Email Address": email, "First Name": first, "Last Name": last}
    pjson = json.dumps(payload)
    import hashlib as _hl
    row_hash = _hl.sha256(pjson.encode()).hexdigest()
    ar_id = str(conn.execute(
        """
        INSERT INTO mailchimp_audience_row
            (source_file_name, audience_status, source_email_raw,
             source_email_normalized, raw_payload, row_hash)
        VALUES ('subscribed.csv', 'subscribed', %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        (email, email.lower(), pjson, row_hash),
    ).fetchone()[0])

    # Link via mailchimp_contact_state (the identity-resolved anchor)
    conn.execute(
        """
        INSERT INTO mailchimp_contact_state
            (participant_id, email_normalized, audience_status,
             source_file_name, row_hash)
        VALUES (%s, %s, 'subscribed', 'subscribed.csv', %s)
        ON CONFLICT DO NOTHING
        """,
        (pid, email.lower(), row_hash),
    )

    return pid, ar_id


def _seed_participant_address(
    conn: psycopg.Connection,
    participant_id: str,
    address_raw: str = "123 Main St, Anytown, MA 01234",
) -> str:
    row = conn.execute(
        """
        INSERT INTO participant_address
            (participant_id, address_type, address_raw, line1, city, state,
             postal_code, country_code, is_primary, source_system)
        VALUES (%s, 'mailing', %s, %s, %s, %s, %s, %s, true, 'test')
        RETURNING id
        """,
        (participant_id, address_raw, "123 Main St", "Anytown", "MA", "01234", "US"),
    ).fetchone()
    return str(row[0])


def _seed_ys_raw_row_with_contact(
    conn: psycopg.Connection,
    owner_name: str,
    email: str | None = None,
    phone: str | None = None,
    location: str | None = None,
) -> str:
    import json as _json
    payload: dict = {"ownerName": owner_name}
    if email:
        payload["email"] = email
    if phone:
        payload["phone"] = phone
    if location:
        payload["ownerLocation"] = location
    row = conn.execute(
        """
        INSERT INTO yacht_scoring_raw_row
            (asset_type, source_file_name, source_file_path, source_row_ordinal,
             raw_payload, row_hash, source_system)
        VALUES ('deduplicated_entry', 'contact_test.csv', '/tmp/contact_test.csv', 1,
                %s, %s, 'test')
        RETURNING id
        """,
        (_json.dumps(payload), f"hash-contact-{owner_name}"),
    ).fetchone()
    return str(row[0])


class TestContactAddressChildEvidence:
    """Participant contact/address child evidence is materialised at the candidate layer."""

    # ------------------------------------------------------------------
    # T1: participant with email → candidate_participant_contact row
    # ------------------------------------------------------------------
    def test_participant_email_produces_contact_child_row(self, db_conn):
        """Participant with email creates a candidate_participant_contact row."""
        conn, _ = db_conn
        pid = _seed_participant(conn, "Email Person", "email.person@example.com")
        ctrs = run_source_to_candidate(conn, entity_type="participant")

        # Find the candidate for this participant source link
        cid = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'participant' AND source_row_pk = %s
            """,
            (pid,),
        ).fetchone()[0]

        contact = conn.execute(
            """
            SELECT contact_type, normalized_value
            FROM candidate_participant_contact
            WHERE candidate_participant_id = %s AND contact_type = 'email'
            """,
            (str(cid),),
        ).fetchone()
        assert contact is not None, "email contact child row should exist"
        assert contact[1] == "email.person@example.com"
        assert ctrs.participant_contacts_linked >= 1
        assert ctrs.db_errors == 0

    # ------------------------------------------------------------------
    # T2: participant with address → candidate_participant_address row
    # ------------------------------------------------------------------
    def test_participant_address_produces_address_child_row(self, db_conn):
        """Participant with a participant_address row creates a candidate_participant_address row."""
        conn, _ = db_conn
        pid = _seed_participant(conn, "Address Person", "addr.person@example.com")
        _seed_participant_address(conn, pid)
        ctrs = run_source_to_candidate(conn, entity_type="participant")

        cid = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'participant' AND source_row_pk = %s
            """,
            (pid,),
        ).fetchone()[0]

        addr = conn.execute(
            """
            SELECT address_raw FROM candidate_participant_address
            WHERE candidate_participant_id = %s
            """,
            (str(cid),),
        ).fetchone()
        assert addr is not None, "address child row should exist"
        assert "Main St" in addr[0]
        assert ctrs.participant_addresses_linked >= 1
        assert ctrs.db_errors == 0

    # ------------------------------------------------------------------
    # T3: reject candidate retains child evidence after scoring
    # ------------------------------------------------------------------
    def test_reject_candidate_retains_child_evidence_after_scoring(self, db_conn):
        """A name-only yacht_scoring row with ownerLocation has its address child row
        after scoring.  With v1.2.0 child-evidence scoring the address lifts the
        candidate from reject to hold (name+address-missing_email-missing_phone=0.30).
        The child row must survive scoring in either case.
        """
        conn, _ = db_conn
        from regatta_etl.resolution_score import run_score

        _seed_ys_raw_row_with_contact(
            conn, "Reject Sailor", location="Newport, RI"
        )
        run_source_to_candidate(conn, entity_type="participant")

        from regatta_etl.normalize import normalize_name
        norm = normalize_name("Reject Sailor")
        cid_row = conn.execute(
            "SELECT id, resolution_state FROM candidate_participant WHERE normalized_name = %s",
            (norm,),
        ).fetchone()
        assert cid_row is not None
        cid = str(cid_row[0])

        # Address must exist before scoring
        addr_before = conn.execute(
            "SELECT 1 FROM candidate_participant_address WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert addr_before is not None, "address child row must exist pre-score"

        # Score — with v1.2.0 child evidence scoring, name+address lands in 'hold'
        run_score(conn, entity_type="participant")

        state = conn.execute(
            "SELECT resolution_state FROM candidate_participant WHERE id = %s", (cid,)
        ).fetchone()[0]
        # name(0.10) + address(0.35) - missing_email(0.10) - missing_phone(0.05) = 0.30 → hold
        assert state in ("hold", "reject"), f"expected hold or reject, got {state!r}"

        # Address must still exist after scoring
        addr_after = conn.execute(
            "SELECT 1 FROM candidate_participant_address WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert addr_after is not None, "address child row must survive scoring"

    # ------------------------------------------------------------------
    # T4: yacht_scoring row with email/phone/location → child rows
    # ------------------------------------------------------------------
    def test_ys_row_with_contact_data_produces_child_rows(self, db_conn):
        """yacht_scoring_raw_row with email, phone, and ownerLocation creates
        candidate_participant_contact and candidate_participant_address child rows.
        """
        conn, _ = db_conn
        _seed_ys_raw_row_with_contact(
            conn, "YS Contact Person",
            email="ys.contact@example.com",
            phone="617-555-0100",
            location="Marblehead, MA",
        )
        ctrs = run_source_to_candidate(conn, entity_type="participant")

        from regatta_etl.normalize import normalize_name
        norm = normalize_name("YS Contact Person")
        cid_row = conn.execute(
            "SELECT id FROM candidate_participant WHERE normalized_name = %s", (norm,)
        ).fetchone()
        assert cid_row is not None
        cid = str(cid_row[0])

        email_row = conn.execute(
            "SELECT normalized_value FROM candidate_participant_contact "
            "WHERE candidate_participant_id = %s AND contact_type = 'email'",
            (cid,),
        ).fetchone()
        assert email_row is not None
        assert email_row[0] == "ys.contact@example.com"

        phone_row = conn.execute(
            "SELECT 1 FROM candidate_participant_contact "
            "WHERE candidate_participant_id = %s AND contact_type = 'phone'",
            (cid,),
        ).fetchone()
        assert phone_row is not None

        addr_row = conn.execute(
            "SELECT address_raw FROM candidate_participant_address "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert addr_row is not None
        assert "Marblehead" in addr_row[0]

        assert ctrs.participant_contacts_linked >= 2
        assert ctrs.participant_addresses_linked >= 1
        assert ctrs.db_errors == 0

    # ------------------------------------------------------------------
    # T5: idempotency — running twice does not duplicate child rows
    # ------------------------------------------------------------------
    def test_contact_address_child_rows_are_idempotent(self, db_conn):
        """Running run_source_to_candidate twice produces no duplicate child rows."""
        conn, _ = db_conn
        pid = _seed_participant(conn, "Idempotent Person", "idm@example.com")
        _seed_participant_address(conn, pid, "99 Yacht Lane, Rockport, ME 04856")

        run_source_to_candidate(conn, entity_type="participant")
        run_source_to_candidate(conn, entity_type="participant")

        cid = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'participant' AND source_row_pk = %s
            """,
            (pid,),
        ).fetchone()[0]

        email_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_contact "
            "WHERE candidate_participant_id = %s AND contact_type = 'email'",
            (str(cid),),
        ).fetchone()[0]
        assert email_count == 1, f"expected 1 email contact row, got {email_count}"

        addr_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_address "
            "WHERE candidate_participant_id = %s",
            (str(cid),),
        ).fetchone()[0]
        assert addr_count == 1, f"expected 1 address row, got {addr_count}"


# ---------------------------------------------------------------------------
# Mailchimp candidate evidence tests (spec T1–T6)
# ---------------------------------------------------------------------------

class TestMailchimpCandidateEvidence:
    """Mailchimp-resolved participant evidence must flow into candidate child tables."""

    def _get_candidate_id_for_participant(self, conn: psycopg.Connection, participant_id: str) -> str:
        row = conn.execute(
            """
            SELECT candidate_entity_id::text
            FROM candidate_source_link
            WHERE candidate_entity_type = 'participant'
              AND source_table_name = 'participant'
              AND source_row_pk = %s
            LIMIT 1
            """,
            (participant_id,),
        ).fetchone()
        assert row is not None, f"no candidate found for participant {participant_id}"
        return row[0]

    # ------------------------------------------------------------------
    # T1: Mailchimp-resolved participant enriches existing candidate, no duplicate
    # ------------------------------------------------------------------
    def test_mailchimp_enriches_existing_candidate_no_duplicate(self, db_conn):
        """Mailchimp evidence attaches to the participant-table candidate, not a duplicate."""
        conn, _ = db_conn
        pid, _ = _seed_mailchimp_resolved(conn, "alice@example.com", "Alice Smith")
        run_source_to_candidate(conn, entity_type="participant")

        candidate_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant"
        ).fetchone()[0]
        assert candidate_count == 1, (
            f"expected exactly 1 candidate (no Mailchimp duplicate), got {candidate_count}"
        )

        # The single candidate is linked from both sources
        links = conn.execute(
            "SELECT source_table_name FROM candidate_source_link "
            "WHERE candidate_entity_type = 'participant' ORDER BY source_table_name"
        ).fetchall()
        source_tables = {r[0] for r in links}
        assert "participant" in source_tables
        assert "mailchimp_audience_row" in source_tables

    # ------------------------------------------------------------------
    # T2: Mailchimp email produces candidate email child row on target candidate
    # ------------------------------------------------------------------
    def test_mailchimp_email_produces_candidate_contact_row(self, db_conn):
        """Mailchimp-resolved participant with email creates a candidate_participant_contact(email) row."""
        conn, _ = db_conn
        pid, _ = _seed_mailchimp_resolved(conn, "bob@example.com", "Bob Jones")
        run_source_to_candidate(conn, entity_type="participant")

        cid = self._get_candidate_id_for_participant(conn, pid)
        contact = conn.execute(
            """
            SELECT contact_type, normalized_value
            FROM candidate_participant_contact
            WHERE candidate_participant_id = %s AND contact_type = 'email'
            """,
            (cid,),
        ).fetchone()
        assert contact is not None, "Mailchimp email child row must exist on candidate"
        assert contact[1] == "bob@example.com"

    # ------------------------------------------------------------------
    # T3: Mailchimp phone produces candidate phone child row on target candidate
    # ------------------------------------------------------------------
    def test_mailchimp_phone_produces_candidate_contact_row(self, db_conn):
        """Mailchimp-resolved participant with phone creates a candidate_participant_contact(phone) row."""
        conn, _ = db_conn
        pid, _ = _seed_mailchimp_resolved(
            conn, "carol@example.com", "Carol White", phone="+12075551234"
        )
        run_source_to_candidate(conn, entity_type="participant")

        cid = self._get_candidate_id_for_participant(conn, pid)
        contact = conn.execute(
            """
            SELECT contact_type, normalized_value
            FROM candidate_participant_contact
            WHERE candidate_participant_id = %s AND contact_type = 'phone'
            """,
            (cid,),
        ).fetchone()
        assert contact is not None, "Mailchimp phone child row must exist on candidate"

    # ------------------------------------------------------------------
    # T4: Mailchimp address produces candidate address child row on target candidate
    # ------------------------------------------------------------------
    def test_mailchimp_address_produces_candidate_address_row(self, db_conn):
        """Mailchimp-resolved participant with address creates a candidate_participant_address row."""
        conn, _ = db_conn
        pid, _ = _seed_mailchimp_resolved(
            conn, "dave@example.com", "Dave Green",
            address_raw="456 Harbor Rd, Rockport, ME 04856",
        )
        run_source_to_candidate(conn, entity_type="participant")

        cid = self._get_candidate_id_for_participant(conn, pid)
        addr = conn.execute(
            """
            SELECT address_raw
            FROM candidate_participant_address
            WHERE candidate_participant_id = %s
            """,
            (cid,),
        ).fetchone()
        assert addr is not None, "Mailchimp address child row must exist on candidate"
        assert "Harbor Rd" in addr[0]

    # ------------------------------------------------------------------
    # T5: Quarantined Mailchimp row (no contact_state) does not project candidate evidence
    # ------------------------------------------------------------------
    def test_quarantined_mailchimp_row_does_not_project_evidence(self, db_conn):
        """A mailchimp_audience_row with no contact_state entry must not create candidate evidence."""
        conn, _ = db_conn
        # Seed raw audience row with no participant and no contact_state (quarantined)
        payload = {"Email Address": "quarantine@example.com", "First Name": "Q", "Last Name": "User"}
        pjson = json.dumps(payload)
        import hashlib as _hl
        row_hash = _hl.sha256(pjson.encode()).hexdigest()
        conn.execute(
            """
            INSERT INTO mailchimp_audience_row
                (source_file_name, audience_status, source_email_raw,
                 source_email_normalized, raw_payload, row_hash)
            VALUES ('subscribed.csv', 'subscribed', %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                "quarantine@example.com",
                "quarantine@example.com",
                pjson,
                row_hash,
            ),
        )
        # Deliberately no mailchimp_contact_state row — this simulates a quarantined identity

        ctrs = run_source_to_candidate(conn, entity_type="participant")
        assert ctrs.db_errors == 0

        # No candidate should have been created from this quarantined row
        link = conn.execute(
            """
            SELECT 1 FROM candidate_source_link
            WHERE source_table_name = 'mailchimp_audience_row'
            """,
        ).fetchone()
        assert link is None, "Quarantined Mailchimp row must not produce a source link"

    # ------------------------------------------------------------------
    # T6: Idempotency — two runs do not duplicate child evidence or source links
    # ------------------------------------------------------------------
    def test_mailchimp_ingestion_is_idempotent(self, db_conn):
        """Running source_to_candidate twice produces no duplicate Mailchimp child evidence."""
        conn, _ = db_conn
        pid, _ = _seed_mailchimp_resolved(
            conn, "eve@example.com", "Eve Black",
            phone="+12075559999",
            address_raw="789 Sail Ave, Portland, ME 04101",
        )

        run_source_to_candidate(conn, entity_type="participant")
        run_source_to_candidate(conn, entity_type="participant")

        cid = self._get_candidate_id_for_participant(conn, pid)

        email_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_contact "
            "WHERE candidate_participant_id = %s AND contact_type = 'email'",
            (cid,),
        ).fetchone()[0]
        assert email_count == 1, f"expected 1 email contact row, got {email_count}"

        phone_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_contact "
            "WHERE candidate_participant_id = %s AND contact_type = 'phone'",
            (cid,),
        ).fetchone()[0]
        assert phone_count == 1, f"expected 1 phone contact row, got {phone_count}"

        addr_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_address "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()[0]
        assert addr_count == 1, f"expected 1 address row, got {addr_count}"

        link_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_source_link "
            "WHERE source_table_name = 'mailchimp_audience_row'",
        ).fetchone()[0]
        assert link_count == 1, f"expected 1 mailchimp source link, got {link_count}"

    # ------------------------------------------------------------------
    # T7: Multiple distinct Mailchimp emails for same participant — all reach candidate
    # ------------------------------------------------------------------
    def test_multiple_mailchimp_emails_all_reach_candidate(self, db_conn):
        """Multiple mailchimp_contact_state rows with different emails produce one child row each."""
        conn, _ = db_conn
        from regatta_etl.normalize import normalize_name
        import hashlib as _hl

        full_name = "Multi Email Person"
        norm = normalize_name(full_name) or full_name
        pid = str(conn.execute(
            "INSERT INTO participant (full_name, normalized_full_name) VALUES (%s, %s) RETURNING id",
            (full_name, norm),
        ).fetchone()[0])

        # Two distinct Mailchimp rows for this participant, each with a different email
        for i, email in enumerate(["primary@example.com", "secondary@example.com"]):
            file_name = f"subscribed_{i}.csv"
            payload = {"Email Address": email, "First Name": "Multi", "Last Name": "Email"}
            pjson = json.dumps(payload)
            row_hash = _hl.sha256(pjson.encode()).hexdigest()
            conn.execute(
                """
                INSERT INTO mailchimp_audience_row
                    (source_file_name, audience_status, source_email_raw,
                     source_email_normalized, raw_payload, row_hash)
                VALUES (%s, 'subscribed', %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (file_name, email, email.lower(), pjson, row_hash),
            )
            conn.execute(
                """
                INSERT INTO mailchimp_contact_state
                    (participant_id, email_normalized, audience_status,
                     source_file_name, row_hash)
                VALUES (%s, %s, 'subscribed', %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (pid, email.lower(), file_name, row_hash),
            )

        run_source_to_candidate(conn, entity_type="participant")

        cid = self._get_candidate_id_for_participant(conn, pid)
        email_rows = conn.execute(
            """
            SELECT normalized_value FROM candidate_participant_contact
            WHERE candidate_participant_id = %s AND contact_type = 'email'
            ORDER BY normalized_value
            """,
            (cid,),
        ).fetchall()
        emails_found = {r[0] for r in email_rows}
        assert "primary@example.com" in emails_found, (
            f"primary email not projected; found: {emails_found}"
        )
        assert "secondary@example.com" in emails_found, (
            f"secondary email not projected; found: {emails_found}"
        )
        assert len(emails_found) == 2, (
            f"expected 2 distinct email child rows, got {len(emails_found)}: {emails_found}"
        )
