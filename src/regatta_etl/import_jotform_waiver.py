
from __future__ import annotations

import csv
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

import click
import psycopg

from regatta_etl.normalize import (
    normalize_email,
    normalize_name,
    normalize_phone,
    normalize_space,
    parse_date,
    slug_name,
    trim,
)

from regatta_etl.shared import (
    AmbiguousMatchError,
    RejectWriter,
    RunCounters,
    insert_participant,
    normalize_headers,
    resolve_participant_by_name,
    upsert_event_context,
    upsert_entry_participant,
)

REQUIRED_HEADERS = {
    "Submission ID",
    "Submission Date",
    "Name",
    "Competitor E mail",
}

def _process_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    event_instance_id: str,
    event_yachts: dict,
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
    csv_file_name: str
):
    # Validate submission ID before any DB write — source_submission_id is NOT NULL
    source_submission_id = row.get("Submission ID")
    if not source_submission_id:
        rejects.write(row, "missing_submission_id")
        counters.rows_rejected += 1
        return

    # Lossless Source Capture
    raw_payload = json.dumps(row)
    row_hash = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

    inserted_raw = conn.execute(
        """
        INSERT INTO jotform_waiver_submission
          (source_file_name, source_submission_id, source_submitted_at_raw, source_last_update_at_raw, raw_payload, row_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_system, source_submission_id, row_hash) DO NOTHING
        RETURNING id
        """,
        (csv_file_name, source_submission_id, row.get("Submission Date"), row.get("Last Update Date"), raw_payload, row_hash),
    ).fetchone()
    if inserted_raw:
        counters.raw_rows_inserted += 1

    # Remaining validation for curated projection
    participant_name = normalize_space(row.get("Name"))
    if not participant_name:
        rejects.write(row, "missing_name")
        counters.rows_rejected += 1
        return
    if not row.get("Competitor E mail"):
        rejects.write(row, "missing_competitor_email")
        counters.rows_rejected += 1
        return
    submission_date = parse_date(row.get("Submission Date"))
    if not submission_date:
        rejects.write(row, "unparseable_submission_date")
        counters.rows_rejected += 1
        return

    # Canonical Projection
    # Participant
    participant_id = resolve_participant_by_name(conn, normalize_name(participant_name))
    if not participant_id:
        participant_id = insert_participant(conn, participant_name)
        counters.participants_inserted += 1
    else:
        counters.participants_matched_existing += 1
    
    # Contact Points
    email_raw = row.get("Competitor E mail")
    email = normalize_email(email_raw)
    phone_raw = row.get("Numbers only, No dashes. Start with area code")
    phone = normalize_phone(phone_raw)
    
    if email:
        existing_email = conn.execute("SELECT id FROM participant_contact_point WHERE participant_id = %s AND contact_value_normalized = %s", (participant_id, email)).fetchone()
        if not existing_email:
            conn.execute(
                """
                INSERT INTO participant_contact_point (participant_id, contact_type, contact_subtype, contact_value_raw, contact_value_normalized, is_primary, source_system)
                VALUES (%s, 'email', 'primary', %s, %s, true, 'jotform_csv_export')
                """,
                (participant_id, email_raw, email)
            )
            counters.contact_points_inserted +=1
    
    if phone:
        existing_phone = conn.execute("SELECT id FROM participant_contact_point WHERE participant_id = %s AND contact_value_normalized = %s", (participant_id, phone)).fetchone()
        if not existing_phone:
            conn.execute(
                """
                INSERT INTO participant_contact_point (participant_id, contact_type, contact_subtype, contact_value_raw, contact_value_normalized, is_primary, source_system)
                VALUES (%s, 'phone', 'primary_mobile', %s, %s, true, 'jotform_csv_export')
                """,
                (participant_id, phone_raw, phone)
            )
            counters.contact_points_inserted += 1

    # Address
    address_raw = row.get("Address")
    postal_code = row.get("Postal code")
    if address_raw or postal_code:
        address_composite = f"{address_raw or ''}|{postal_code or ''}".strip()
        existing_address = conn.execute("SELECT id FROM participant_address WHERE participant_id = %s AND address_raw = %s", (participant_id, address_composite)).fetchone()
        if not existing_address:
            conn.execute(
                """
                INSERT INTO participant_address (participant_id, address_type, line1, postal_code, address_raw, is_primary, source_system)
                VALUES (%s, 'mailing', %s, %s, %s, true, 'jotform_csv_export')
                """,
                (participant_id, address_raw, postal_code, address_composite)
            )
            counters.addresses_inserted += 1
    
    # Related Contacts
    guardian_name = row.get("Name of Parent Or Guardian")
    guardian_phone_raw = row.get("Parent or Guardian Phone:  Numbers only, No dashes. Start with area code")
    guardian_email_raw = row.get("Parent or Guardian  E mail")
    if guardian_name:
        existing_guardian = conn.execute("SELECT id FROM participant_related_contact WHERE participant_id = %s AND related_contact_type = 'guardian' AND related_full_name = %s", (participant_id, guardian_name)).fetchone()
        if not existing_guardian:
            conn.execute(
                """
                INSERT INTO participant_related_contact (participant_id, related_contact_type, related_full_name, phone_raw, phone_normalized, email_raw, email_normalized, source_submission_id, source_system)
                VALUES (%s, 'guardian', %s, %s, %s, %s, %s, %s, 'jotform_csv_export')
                """,
                (
                    participant_id,
                    guardian_name,
                    guardian_phone_raw,
                    normalize_phone(guardian_phone_raw),
                    guardian_email_raw,
                    normalize_email(guardian_email_raw),
                    source_submission_id,
                )
            )
            counters.participant_related_contacts_inserted += 1

    emergency_contact_name = row.get("Name of your emergency contact")
    emergency_phone_raw = row.get("Emergency phone contact")
    emergency_email_raw = row.get("Emergency email")
    if emergency_contact_name:
        existing_emergency = conn.execute("SELECT id FROM participant_related_contact WHERE participant_id = %s AND related_contact_type = 'emergency' AND related_full_name = %s", (participant_id, emergency_contact_name)).fetchone()
        if not existing_emergency:
            conn.execute(
                """
                INSERT INTO participant_related_contact (participant_id, related_contact_type, related_full_name, relationship_label, phone_raw, phone_normalized, email_raw, email_normalized, source_submission_id, source_system)
                VALUES (%s, 'emergency', %s, %s, %s, %s, %s, %s, %s, 'jotform_csv_export')
                """,
                (
                    participant_id,
                    emergency_contact_name,
                    row.get("Relationship (optional)"),
                    emergency_phone_raw,
                    normalize_phone(emergency_phone_raw),
                    emergency_email_raw,
                    normalize_email(emergency_email_raw),
                    source_submission_id,
                )
            )
            counters.participant_related_contacts_inserted += 1

    # Event Linkage
    yacht_name = normalize_space(row.get("Boat Name"))
    sail_number = normalize_space(row.get("Sail Number"))
    event_entry_id = None
    
    if sail_number:
        sail_slug = slug_name(sail_number)
        if sail_slug in event_yachts["by_sail_number"]:
            ids = event_yachts["by_sail_number"][sail_slug]
            if len(ids) == 1:
                event_entry_id = ids[0]
            else:
                counters.warnings.append(f"ambiguous sail number {sail_number} for submission {source_submission_id}")

    if not event_entry_id and yacht_name:
        name_slug = slug_name(yacht_name)
        if name_slug in event_yachts["by_name"]:
            ids = event_yachts["by_name"][name_slug]
            if len(ids) == 1:
                event_entry_id = ids[0]
            else:
                counters.warnings.append(f"ambiguous yacht name {yacht_name} for submission {source_submission_id}")


    if event_entry_id:
        role = "skipper" if row.get("I am the skipper (person in charge)") == "Yes" else "crew"
        upsert_entry_participant(conn, event_entry_id, participant_id, role, "participating", "jotform_csv_export")
    else:
        counters.unresolved_event_links += 1

    # Document Tracking
    # Ensure document_type and document_requirement exist
    doc_type_id = conn.execute("SELECT id FROM document_type WHERE normalized_name = 'bhyc-waiver-release' and scope = 'participant'").fetchone()
    if not doc_type_id:
        doc_type_id = conn.execute("""
            INSERT INTO document_type (name, normalized_name, scope) 
            VALUES ('BHYC Waiver/Release', 'bhyc-waiver-release', 'participant')
            RETURNING id
        """).fetchone()[0]
    else:
        doc_type_id = doc_type_id[0]
    
    doc_req_id = conn.execute("SELECT id FROM document_requirement WHERE event_instance_id = %s AND document_type_id = %s", (event_instance_id, doc_type_id)).fetchone()
    if not doc_req_id:
        doc_req_id = conn.execute("""
            INSERT INTO document_requirement (event_instance_id, document_type_id, is_mandatory)
            VALUES (%s, %s, true)
            RETURNING id
        """, (event_instance_id, doc_type_id)).fetchone()[0]
    else:
        doc_req_id = doc_req_id[0]
    
    existing_doc = conn.execute("SELECT id, status_at FROM document_status WHERE document_requirement_id = %s AND participant_id = %s", (doc_req_id, participant_id)).fetchone()
    if not existing_doc or (existing_doc and existing_doc[1].date() < submission_date):
        if existing_doc:
            conn.execute("DELETE FROM document_status WHERE id = %s", (existing_doc[0],))
        conn.execute(
            """
            INSERT INTO document_status (document_requirement_id, participant_id, status, status_at, evidence_ref, source_system)
            VALUES (%s, %s, 'received', %s, %s, 'jotform_csv_export')
            """,
            (doc_req_id, participant_id, submission_date, row.get("Signed Document"))
        )
        counters.document_status_upserted += 1

    counters.curated_rows_processed += 1


def _run_real(
    conn: psycopg.Connection,
    raw_rows: list[dict[str, str]],
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
    event_instance_id: str,
    event_yachts: dict,
    csv_file_name: str,
) -> None:
    for idx, row in enumerate(raw_rows):
        sp_name = f"row_{idx}"
        conn.execute(f"SAVEPOINT {sp_name}")
        try:
            _process_row(conn, row, event_instance_id, event_yachts, run_id, counters, rejects, csv_file_name)
            conn.execute(f"RELEASE SAVEPOINT {sp_name}")
        except Exception as e:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
            rejects.write(row, f"db_error: {e}")
            counters.rows_rejected += 1
            counters.db_phase_errors += 1



def _run_dry(
    conn: psycopg.Connection,
    raw_rows: list[dict[str, str]],
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
    event_instance_id: str,
    event_yachts: dict,
    csv_file_name: str
):
    try:
        for idx, row in enumerate(raw_rows):
            sp_name = f"row_{idx}"
            conn.execute(f"SAVEPOINT {sp_name}")
            try:
                _process_row(conn, row, event_instance_id, event_yachts, run_id, counters, rejects, csv_file_name)
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            except Exception as e:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                rejects.write(row, f"db_error: {e}")
                counters.rows_rejected += 1
                counters.db_phase_errors += 1
    finally:
        conn.rollback()
        click.echo(f"[{run_id}] [dry-run] All changes rolled back.")


def _run_jotform_waiver(
    run_id: str,
    started_at: str,
    db_dsn: str,
    counters: RunCounters,
    rejects: RejectWriter,
    csv_path: str,
    host_club_name: str,
    host_club_normalized: str,
    event_series_name: str,
    event_series_normalized: str,
    event_display_name: str,
    season_year: int,
    event_unresolved_link_max_reject_rate: float,
    dry_run: bool,
):
    # Pre-scan
    csv_file = Path(csv_path)
    raw_rows: list[dict[str, str]] = []

    with csv_file.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        raw_fieldnames = reader.fieldnames or []
        normalized_hdr = {k.strip(): k for k in raw_fieldnames}
        
        missing = REQUIRED_HEADERS - set(normalized_hdr.keys())
        if missing:
            click.echo(f"[{run_id}] FATAL: missing headers: {sorted(missing)}", err=True)
            sys.exit(1)

        for raw_row in reader:
            row = normalize_headers(raw_row)
            counters.rows_read += 1
            raw_rows.append(row)
    
    click.echo(f"[{run_id}] Pre-scan: {counters.rows_read} rows read, {len(raw_rows)} to process")

    # DB phase
    conn = psycopg.connect(db_dsn, autocommit=False)
    try:
        _, _, event_instance_id = upsert_event_context(
            conn,
            host_club_name, host_club_normalized,
            event_series_name, event_series_normalized,
            event_display_name, season_year, None,
        )

        event_yachts = {"by_sail_number": {}, "by_name": {}}
        yacht_entries = conn.execute("SELECT y.id, y.normalized_name, y.normalized_sail_number, ee.id as event_entry_id FROM yacht y JOIN event_entry ee ON y.id = ee.yacht_id WHERE ee.event_instance_id = %s", (event_instance_id,)).fetchall()
        for yacht_id, normalized_name, normalized_sail_number, event_entry_id in yacht_entries:
            if normalized_sail_number:
                if normalized_sail_number not in event_yachts["by_sail_number"]:
                    event_yachts["by_sail_number"][normalized_sail_number] = []
                event_yachts["by_sail_number"][normalized_sail_number].append(event_entry_id)
            if normalized_name:
                if normalized_name not in event_yachts["by_name"]:
                    event_yachts["by_name"][normalized_name] = []
                event_yachts["by_name"][normalized_name].append(event_entry_id)

        if dry_run:
            _run_dry(conn, raw_rows, run_id, counters, rejects, event_instance_id, event_yachts, csv_file.name)
            if counters.db_phase_errors > 0:
                click.echo(f"[{run_id}] [dry-run] {counters.db_phase_errors} row-level DB error(s) detected.", err=True)
                sys.exit(1)
            if counters.rows_read > 0 and (counters.unresolved_event_links / counters.rows_read) > event_unresolved_link_max_reject_rate:
                click.echo(f"[{run_id}] [dry-run] unresolved event link rate ({counters.unresolved_event_links / counters.rows_read:.2%}) exceeds threshold of {event_unresolved_link_max_reject_rate:.2%}.", err=True)
                sys.exit(1)
            return

        _run_real(conn, raw_rows, run_id, counters, rejects, event_instance_id, event_yachts, csv_file.name)

        if counters.db_phase_errors > 0:
            conn.rollback()
            click.echo(f"[{run_id}] FATAL: {counters.db_phase_errors} row-level DB error(s); rolling back.", err=True)
            sys.exit(1)

        if counters.rows_read > 0 and (counters.unresolved_event_links / counters.rows_read) > event_unresolved_link_max_reject_rate:
            conn.rollback()
            click.echo(f"[{run_id}] FATAL: unresolved event link rate ({counters.unresolved_event_links / counters.rows_read:.2%}) exceeds threshold of {event_unresolved_link_max_reject_rate:.2%}; rolling back.", err=True)
            sys.exit(1)

        conn.commit()
    except Exception as e:
        conn.rollback()
        click.echo(f"[{run_id}] FATAL: run failed with DB error: {e}", err=True)
        sys.exit(1)
    finally:
        conn.close()
        rejects.close()
