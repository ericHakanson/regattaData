
from __future__ import annotations

import csv
from pathlib import Path
import uuid
import pytest

from regatta_etl.import_jotform_waiver import _run_jotform_waiver, REQUIRED_HEADERS as JOTFORM_REQUIRED_HEADERS
from regatta_etl.shared import RunCounters, RejectWriter, upsert_event_context

def get_base_csv_data():
    return {
        "Submission ID": "12345",
        "Submission Date": "Jul 23, 2025",
        "Last Update Date": "",
        "Name": "John Doe",
        "Competitor E mail": "john.doe@example.com",
        "Numbers only, No dashes. Start with area code": "1234567890",
        "Address": "123 Main St",
        "Postal code": "12345",
        "Boat Name": "Test Boat",
        "Sail Number": "123",
        "I am the skipper (person in charge)": "Yes",
        "Please check box acknowledging above": "Yes",
        "I have read and understand the media waiver.": "Yes",
        "Signed Document": "https://example.com/signed.pdf",
        "Participant Signature": "https://example.com/signature.png",
        "I am age 18 or older": "Yes",
        "Name of Parent Or Guardian": "",
        "Parent or Guardian Phone:  Numbers only, No dashes. Start with area code": "",
        "Parent or Guardian  E mail": "",
        "Agreement to the above of Parent or Guardian for participant under 18": "",
        "Parent or Guardian Signature (if participant under 18)": "",
        "Name of your emergency contact": "Jane Doe",
        "Relationship (optional)": "Spouse",
        "Emergency phone contact": "0987654321",
        "Emergency email": "jane.doe@example.com",
    }

def test_jotform_import_e2e(db_conn, tmp_path):
    conn, dsn = db_conn
    
    # Setup event and yacht
    with conn.cursor() as cur:
        _, _, instance_id = upsert_event_context(
            cur,
            "Boothbay Harbor Yacht Club", "boothbay-harbor-yacht-club",
            "BHYC Regatta", "bhyc-regatta",
            "BHYC Regatta 2025", 2025, None
        )
        cur.execute(
            """
            INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number)
            VALUES ('Test Boat', 'test-boat', '123', '123')
            RETURNING id
            """
        )
        yacht_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO event_entry (event_instance_id, yacht_id, entry_status, registration_source)
            VALUES (%s, %s, 'submitted', 'test')
            """,
            (instance_id, yacht_id)
        )
    conn.commit()

    csv_data = [get_base_csv_data()]
    csv_path = tmp_path / "jotform.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
        writer.writeheader()
        writer.writerows(csv_data)

    run_id = str(uuid.uuid4())
    counters = RunCounters()
    rejects_path = tmp_path / "rejects.csv"
    rejects = RejectWriter(rejects_path)

    _run_jotform_waiver(
        run_id=run_id,
        started_at="2025-07-23T10:00:00Z",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        csv_path=str(csv_path),
        host_club_name="Boothbay Harbor Yacht Club",
        host_club_normalized="boothbay-harbor-yacht-club",
        event_series_name="BHYC Regatta",
        event_series_normalized="bhyc-regatta",
        event_display_name="BHYC Regatta 2025",
        season_year=2025,
        event_unresolved_link_max_reject_rate=0.1,
        dry_run=False,
    )

    # Assertions
    assert counters.rows_read == 1
    assert counters.raw_rows_inserted == 1
    assert counters.curated_rows_processed == 1
    assert counters.participants_inserted == 1
    assert counters.yachts_inserted == 0 # Should match existing
    assert counters.contact_points_inserted == 2
    assert counters.addresses_inserted == 1
    assert counters.participant_related_contacts_inserted == 1
    assert counters.unresolved_event_links == 0
    assert counters.document_status_upserted == 1

    # Check DB content
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM jotform_waiver_submission")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM participant")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM participant_contact_point")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT COUNT(*) FROM participant_address")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM participant_related_contact")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM document_status")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM event_entry_participant")
        assert cur.fetchone()[0] == 1

def test_jotform_import_idempotency(db_conn, tmp_path):
    conn, dsn = db_conn

    # Setup event and yacht
    with conn.cursor() as cur:
        _, _, instance_id = upsert_event_context(
            cur,
            "Boothbay Harbor Yacht Club", "boothbay-harbor-yacht-club",
            "BHYC Regatta", "bhyc-regatta",
            "BHYC Regatta 2025", 2025, None
        )
        cur.execute(
            """
            INSERT INTO yacht (name, normalized_name, sail_number, normalized_sail_number)
            VALUES ('Test Boat', 'test-boat', '123', '123')
            RETURNING id
            """
        )
        yacht_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO event_entry (event_instance_id, yacht_id, entry_status, registration_source)
            VALUES (%s, %s, 'submitted', 'test')
            """,
            (instance_id, yacht_id)
        )
    conn.commit()

    csv_data = [get_base_csv_data()]
    csv_path = tmp_path / "jotform.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
        writer.writeheader()
        writer.writerows(csv_data)

    # First run
    _run_jotform_waiver(
        run_id=str(uuid.uuid4()), started_at="2025-07-23T10:00:00Z", db_dsn=dsn,
        counters=RunCounters(), rejects=RejectWriter(tmp_path / "rejects1.csv"),
        csv_path=str(csv_path), host_club_name="Boothbay Harbor Yacht Club", host_club_normalized="boothbay-harbor-yacht-club",
        event_series_name="BHYC Regatta", event_series_normalized="bhyc-regatta",
        event_display_name="BHYC Regatta 2025", season_year=2025,
        event_unresolved_link_max_reject_rate=0.1, dry_run=False,
    )

    # Second run
    counters2 = RunCounters()
    _run_jotform_waiver(
        run_id=str(uuid.uuid4()), started_at="2025-07-23T11:00:00Z", db_dsn=dsn,
        counters=counters2, rejects=RejectWriter(tmp_path / "rejects2.csv"),
        csv_path=str(csv_path), host_club_name="Boothbay Harbor Yacht Club", host_club_normalized="boothbay-harbor-yacht-club",
        event_series_name="BHYC Regatta", event_series_normalized="bhyc-regatta",
        event_display_name="BHYC Regatta 2025", season_year=2025,
        event_unresolved_link_max_reject_rate=0.1, dry_run=False,
    )

    assert counters2.raw_rows_inserted == 0
    assert counters2.participants_inserted == 0
    assert counters2.contact_points_inserted == 0
    assert counters2.addresses_inserted == 0
    assert counters2.document_status_upserted == 0

def test_jotform_import_raw_capture_for_rejects(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_data = [get_base_csv_data()]
    csv_data[0]["Name"] = "" # Invalid row
    csv_path = tmp_path / "jotform.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
        writer.writeheader()
        writer.writerows(csv_data)

    counters = RunCounters()
    _run_jotform_waiver(
        run_id=str(uuid.uuid4()), started_at="2025-07-23T10:00:00Z", db_dsn=dsn,
        counters=counters, rejects=RejectWriter(tmp_path / "rejects.csv"),
        csv_path=str(csv_path), host_club_name="BHYC", host_club_normalized="bhyc",
        event_series_name="BHYC Regatta", event_series_normalized="bhyc-regatta",
        event_display_name="BHYC Regatta 2025", season_year=2025,
        event_unresolved_link_max_reject_rate=0.1, dry_run=False,
    )

    assert counters.rows_read == 1
    assert counters.raw_rows_inserted == 1
    assert counters.rows_rejected == 1
    assert counters.curated_rows_processed == 0

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM jotform_waiver_submission")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM participant")
        assert cur.fetchone()[0] == 0

def test_jotform_import_missing_submission_id(db_conn, tmp_path):
    """A row with no Submission ID must be rejected before the DB INSERT.

    Before the fix the NOT NULL constraint on jotform_waiver_submission.source_submission_id
    would cause a DB error, trip db_phase_errors, and roll back the entire run.
    After the fix it is a clean pre-INSERT validation reject: raw_rows_inserted=0,
    db_phase_errors=0, run exits 0, and no rows land in any table.
    """
    conn, dsn = db_conn
    csv_data = [get_base_csv_data()]
    csv_data[0]["Submission ID"] = ""
    csv_path = tmp_path / "jotform.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
        writer.writeheader()
        writer.writerows(csv_data)

    counters = RunCounters()
    _run_jotform_waiver(
        run_id=str(uuid.uuid4()), started_at="2025-07-23T10:00:00Z", db_dsn=dsn,
        counters=counters, rejects=RejectWriter(tmp_path / "rejects.csv"),
        csv_path=str(csv_path), host_club_name="BHYC", host_club_normalized="bhyc",
        event_series_name="BHYC Regatta", event_series_normalized="bhyc-regatta",
        event_display_name="BHYC Regatta 2025", season_year=2025,
        event_unresolved_link_max_reject_rate=0.1, dry_run=False,
    )

    assert counters.rows_read == 1
    assert counters.raw_rows_inserted == 0   # no DB write before validation
    assert counters.rows_rejected == 1
    assert counters.db_phase_errors == 0     # clean reject, not a DB error
    assert counters.curated_rows_processed == 0

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM jotform_waiver_submission")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM participant")
        assert cur.fetchone()[0] == 0


def test_jotform_import_db_error_is_fatal(db_conn, tmp_path):
    conn, dsn = db_conn
    
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE participant_related_contact ADD COLUMN new_col TEXT NOT NULL")
    conn.commit()

    csv_data = [get_base_csv_data()]
    csv_path = tmp_path / "jotform.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
        writer.writeheader()
        writer.writerows(csv_data)

    with pytest.raises(SystemExit):
        _run_jotform_waiver(
            run_id=str(uuid.uuid4()), started_at="2025-07-23T10:00:00Z", db_dsn=dsn,
            counters=RunCounters(), rejects=RejectWriter(tmp_path / "rejects.csv"),
            csv_path=str(csv_path), host_club_name="BHYC", host_club_normalized="bhyc",
            event_series_name="BHYC Regatta", event_series_normalized="bhyc-regatta",
            event_display_name="BHYC Regatta 2025", season_year=2025,
            event_unresolved_link_max_reject_rate=1.0, dry_run=False,
        )

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM jotform_waiver_submission")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM participant")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM participant_related_contact")
        assert cur.fetchone()[0] == 0
