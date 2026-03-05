"""Integration tests for Mailchimp strict identity policy (Policy v2).

Tests verify:
  - Name corroboration is enforced when email matches an existing participant.
  - Phone / address hard-conflict checks quarantine rows correctly.
  - LEID/EUID identity-link upsert and cross-participant conflict detection.
  - No curated writes occur for quarantined rows.
  - Ambiguous email match routes to the review queue.

Requires a live PostgreSQL instance (via pytest-postgresql).
"""

from __future__ import annotations

import csv
import uuid
from pathlib import Path

import psycopg
import pytest

from regatta_etl.import_mailchimp_audience import _run_mailchimp_audience
from regatta_etl.shared import RunCounters, RejectWriter


# ---------------------------------------------------------------------------
# CSV helpers (re-used from test_mailchimp_import.py pattern)
# ---------------------------------------------------------------------------

SUBSCRIBED_HEADERS = [
    "Email Address", "First Name", "Last Name", "Address", "Phone Number",
    "Birthday", "MEMBER_RATING", "OPTIN_TIME", "OPTIN_IP", "CONFIRM_TIME",
    "CONFIRM_IP", "GMTOFF", "DSTOFF", "TIMEZONE", "CC", "REGION",
    "LAST_CHANGED", "LEID", "EUID", "NOTES", "TAGS",
]


def _make_row(
    email: str = "test@example.com",
    first: str = "Alice",
    last: str = "Smith",
    phone: str = "",
    address: str = "",
    last_changed: str = "2025-07-30 12:00:00",
    leid: str = "",
    euid: str = "",
    **kwargs,
) -> dict[str, str]:
    row = {h: "" for h in SUBSCRIBED_HEADERS}
    row.update({
        "Email Address": email,
        "First Name": first,
        "Last Name": last,
        "Phone Number": phone,
        "Address": address,
        "LAST_CHANGED": last_changed,
        "LEID": leid,
        "EUID": euid,
    })
    row.update(kwargs)
    return row


def _write_sub(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUBSCRIBED_HEADERS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_empty(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["Email Address", "UNSUB_TIME"],
                                extrasaction="ignore")
        writer.writeheader()


def _run(conn, dsn, tmp_path, rows, *, max_reject_rate=1.0):
    """Run the ingestion pipeline against a single set of subscribed rows."""
    sub = tmp_path / f"{uuid.uuid4()}_sub.csv"
    unsub = tmp_path / f"{uuid.uuid4()}_unsub.csv"
    clean = tmp_path / f"{uuid.uuid4()}_clean.csv"
    rejects_path = tmp_path / f"{uuid.uuid4()}_rejects.csv"

    _write_sub(sub, rows)

    # Write minimal unsub/clean headers (no rows)
    for p, ts_col in [(unsub, "UNSUB_TIME"), (clean, "CLEAN_TIME")]:
        with p.open("w", newline="", encoding="utf-8") as fh:
            cw = csv.DictWriter(fh, fieldnames=["Email Address", ts_col])
            cw.writeheader()

    ctrs = RunCounters()
    rejs = RejectWriter(rejects_path)

    _run_mailchimp_audience(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=ctrs,
        rejects=rejs,
        subscribed_path=str(sub),
        unsubscribed_path=str(unsub),
        cleaned_path=str(clean),
        max_reject_rate=max_reject_rate,
        dry_run=False,
    )
    return ctrs, rejects_path


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _seed_participant(conn, *, name: str, email: str,
                      phone: str | None = None, address: str | None = None) -> str:
    """Insert a participant with email contact point (and optional phone/address)."""
    pid = str(uuid.uuid4())
    from regatta_etl.normalize import normalize_name
    name_norm = normalize_name(name)
    conn.execute(
        "INSERT INTO participant (id, full_name, normalized_full_name) VALUES (%s, %s, %s)",
        (pid, name, name_norm),
    )
    conn.execute(
        """
        INSERT INTO participant_contact_point
            (participant_id, contact_type, contact_subtype,
             contact_value_raw, contact_value_normalized, is_primary, source_system)
        VALUES (%s, 'email', 'primary', %s, %s, true, 'test')
        """,
        (pid, email, email),
    )
    if phone:
        from regatta_etl.normalize import normalize_phone
        phone_norm = normalize_phone(phone) or phone
        conn.execute(
            """
            INSERT INTO participant_contact_point
                (participant_id, contact_type, contact_subtype,
                 contact_value_raw, contact_value_normalized, is_primary, source_system)
            VALUES (%s, 'phone', 'primary', %s, %s, false, 'test')
            """,
            (pid, phone, phone_norm),
        )
    if address:
        conn.execute(
            """
            INSERT INTO participant_address
                (participant_id, address_type, address_raw, is_primary, source_system)
            VALUES (%s, 'mailing', %s, true, 'test')
            """,
            (pid, address),
        )
    conn.commit()
    return pid



def _queue_rows(conn) -> list[dict]:
    rows = conn.execute(
        "SELECT reason_code, email_normalized, candidate_participant_id, reason_detail "
        "FROM mailchimp_identity_review_queue ORDER BY created_at"
    ).fetchall()
    return [{"reason_code": r[0], "email": r[1], "pid": r[2], "detail": r[3]} for r in rows]


def _identity_links(conn, participant_id: str | None = None) -> list[dict]:
    if participant_id:
        rows = conn.execute(
            "SELECT participant_id::text, email_normalized, leid, euid, mailchimp_contact_id "
            "FROM participant_mailchimp_identity WHERE participant_id = %s::uuid",
            (participant_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT participant_id::text, email_normalized, leid, euid, mailchimp_contact_id "
            "FROM participant_mailchimp_identity"
        ).fetchall()
    return [{"pid": r[0], "email": r[1], "leid": r[2], "euid": r[3], "contact_id": r[4]}
            for r in rows]


# ---------------------------------------------------------------------------
# Policy tests: name corroboration
# ---------------------------------------------------------------------------

def test_email_name_exact_match_accepted(db_conn, tmp_path):
    """Email match + matching name → participant linked, no quarantine."""
    conn, dsn = db_conn

    pid = _seed_participant(conn, name="Alice Smith", email="alice@example.com")

    ctrs, rejects_path = _run(conn, dsn, tmp_path, [
        _make_row(email="alice@example.com", first="Alice", last="Smith",
                  leid="L001", euid="E001"),
    ])

    assert ctrs.rows_rejected == 0
    assert ctrs.mailchimp_identity_rows_quarantined == 0
    assert ctrs.participants_matched_existing == 1
    assert ctrs.participants_inserted == 0

    # Identity link should be persisted
    links = _identity_links(conn, pid)
    assert len(links) == 1
    assert links[0]["leid"] == "L001"
    assert links[0]["euid"] == "E001"

    # No queue rows
    assert len(_queue_rows(conn)) == 0


def test_email_name_mismatch_quarantined(db_conn, tmp_path):
    """Email match but name mismatch → quarantine with email_name_mismatch."""
    conn, dsn = db_conn

    pid = _seed_participant(conn, name="Alice Smith", email="alice@example.com")

    ctrs, rejects_path = _run(conn, dsn, tmp_path, [
        _make_row(email="alice@example.com", first="Bob", last="Jones"),
    ])

    assert ctrs.rows_rejected == 1
    assert ctrs.mailchimp_identity_rows_quarantined == 1
    assert ctrs.participants_matched_existing == 0

    queue = _queue_rows(conn)
    assert len(queue) == 1
    assert queue[0]["reason_code"] == "email_name_mismatch"
    assert queue[0]["email"] == "alice@example.com"
    assert str(queue[0]["pid"]) == pid

    # No curated state write for the quarantined row
    ct = conn.execute("SELECT COUNT(*) FROM mailchimp_contact_state").fetchone()[0]
    assert ct == 0


def test_email_missing_source_name_quarantined(db_conn, tmp_path):
    """Email match but source row has no name → quarantine."""
    conn, dsn = db_conn

    _seed_participant(conn, name="Alice Smith", email="alice@example.com")

    ctrs, rejects_path = _run(conn, dsn, tmp_path, [
        _make_row(email="alice@example.com", first="", last=""),
    ])

    assert ctrs.rows_rejected == 1
    assert ctrs.mailchimp_identity_rows_quarantined == 1

    queue = _queue_rows(conn)
    assert len(queue) == 1
    assert queue[0]["reason_code"] == "missing_name_for_email_match"


# ---------------------------------------------------------------------------
# Policy tests: phone / address corroboration
# ---------------------------------------------------------------------------

def test_phone_mismatch_quarantined(db_conn, tmp_path):
    """Email + name match, but phone mismatch → quarantine."""
    conn, dsn = db_conn

    _seed_participant(conn, name="Alice Smith", email="alice@example.com",
                      phone="2075550101")

    ctrs, rejects_path = _run(conn, dsn, tmp_path, [
        _make_row(email="alice@example.com", first="Alice", last="Smith",
                  phone="9995551234"),
    ])

    assert ctrs.rows_rejected == 1
    assert ctrs.mailchimp_identity_rows_quarantined == 1

    queue = _queue_rows(conn)
    assert len(queue) == 1
    assert queue[0]["reason_code"] == "email_phone_mismatch"


def test_phone_absent_on_one_side_not_quarantined(db_conn, tmp_path):
    """Source has phone but target has no phone → no phone conflict."""
    conn, dsn = db_conn

    _seed_participant(conn, name="Alice Smith", email="alice@example.com")
    # No phone on target

    ctrs, rejects_path = _run(conn, dsn, tmp_path, [
        _make_row(email="alice@example.com", first="Alice", last="Smith",
                  phone="2075550101"),
    ])

    assert ctrs.rows_rejected == 0
    assert ctrs.mailchimp_identity_rows_quarantined == 0
    assert ctrs.participants_matched_existing == 1


def test_address_mismatch_quarantined(db_conn, tmp_path):
    """Email + name match, but address mismatch → quarantine."""
    conn, dsn = db_conn

    _seed_participant(conn, name="Alice Smith", email="alice@example.com",
                      address="1 Main St, Bar Harbor, ME 04609")

    ctrs, rejects_path = _run(conn, dsn, tmp_path, [
        _make_row(email="alice@example.com", first="Alice", last="Smith",
                  address="99 Different Rd, Portland, ME 04101"),
    ])

    assert ctrs.rows_rejected == 1
    assert ctrs.mailchimp_identity_rows_quarantined == 1

    queue = _queue_rows(conn)
    assert len(queue) == 1
    assert queue[0]["reason_code"] == "email_address_mismatch"


# ---------------------------------------------------------------------------
# Policy tests: ambiguous email match
# ---------------------------------------------------------------------------

def test_ambiguous_email_quarantined_with_queue_row(db_conn, tmp_path):
    """Two participants share an email → quarantine with review queue row."""
    conn, dsn = db_conn

    shared_email = "shared@example.com"
    for name in ("P One", "P Two"):
        pid = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO participant (id, full_name, normalized_full_name) "
            "VALUES (%s, %s, %s)",
            (pid, name, name.lower()),
        )
        conn.execute(
            """
            INSERT INTO participant_contact_point
                (participant_id, contact_type, contact_subtype,
                 contact_value_raw, contact_value_normalized, is_primary, source_system)
            VALUES (%s, 'email', 'primary', %s, %s, true, 'test')
            """,
            (pid, shared_email, shared_email),
        )
    conn.commit()

    ctrs, rejects_path = _run(conn, dsn, tmp_path, [
        _make_row(email=shared_email, first="P", last="One"),
    ])

    assert ctrs.rows_rejected == 1
    assert ctrs.mailchimp_identity_rows_quarantined == 1
    assert ctrs.db_phase_errors == 0

    queue = _queue_rows(conn)
    assert len(queue) == 1
    assert queue[0]["reason_code"] == "ambiguous_email_match"

    # Raw capture preserved despite quarantine
    ct = conn.execute("SELECT COUNT(*) FROM mailchimp_audience_row").fetchone()[0]
    assert ct == 1


# ---------------------------------------------------------------------------
# Policy tests: no curated writes on quarantine
# ---------------------------------------------------------------------------

def test_no_curated_writes_on_quarantine(db_conn, tmp_path):
    """Quarantined rows produce no contact_point, state, or tag writes."""
    conn, dsn = db_conn

    _seed_participant(conn, name="Alice Smith", email="alice@example.com")

    ctrs, _ = _run(conn, dsn, tmp_path, [
        _make_row(email="alice@example.com", first="Wrong", last="Name",
                  TAGS='"Sailing","Racing"'),
    ])

    assert ctrs.mailchimp_identity_rows_quarantined == 1
    assert conn.execute("SELECT COUNT(*) FROM mailchimp_contact_state").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM mailchimp_contact_tag").fetchone()[0] == 0
    # No new contact point (the existing email point stays from seed, but none added by pipeline)
    assert ctrs.contact_points_inserted == 0


# ---------------------------------------------------------------------------
# Identity link tests
# ---------------------------------------------------------------------------

def test_identity_link_persisted_with_leid_euid(db_conn, tmp_path):
    """New participant with LEID + EUID → participant_mailchimp_identity row inserted."""
    conn, dsn = db_conn

    ctrs, _ = _run(conn, dsn, tmp_path, [
        _make_row(email="new@example.com", first="New", last="Person",
                  leid="L999", euid="E999"),
    ])

    assert ctrs.participants_inserted == 1
    assert ctrs.mailchimp_identity_links_inserted == 1

    links = _identity_links(conn)
    assert len(links) == 1
    assert links[0]["leid"] == "L999"
    assert links[0]["euid"] == "E999"


def test_identity_link_updated_on_second_run(db_conn, tmp_path):
    """Second run with same email → identity link updated (last_seen_at refreshed)."""
    conn, dsn = db_conn

    for _ in range(2):
        ctrs, _ = _run(conn, dsn, tmp_path, [
            _make_row(email="repeat@example.com", first="Repeat", last="User",
                      leid="L111", euid="E111"),
        ])

    assert ctrs.mailchimp_identity_links_inserted == 0
    assert ctrs.mailchimp_identity_links_updated == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM participant_mailchimp_identity"
    ).fetchone()[0] == 1


def test_leid_conflict_queued_does_not_block_curated_writes(db_conn, tmp_path):
    """LEID linked to P1; ingesting P2 with same LEID → conflict queued, P2 curated still written."""
    conn, dsn = db_conn

    # Pre-insert P1 with LEID "CONFLICT_L"
    pid1 = _seed_participant(conn, name="Person One", email="p1@example.com")
    conn.execute(
        """
        INSERT INTO participant_mailchimp_identity
            (participant_id, leid, email_normalized, source_system)
        VALUES (%s::uuid, %s, %s, 'test')
        """,
        (pid1, "CONFLICT_L", "p1@example.com"),
    )
    conn.commit()

    # Ingest P2 with the same LEID
    ctrs, _ = _run(conn, dsn, tmp_path, [
        _make_row(email="p2@example.com", first="Person", last="Two",
                  leid="CONFLICT_L"),
    ])

    assert ctrs.rows_rejected == 0  # not a row-level reject
    assert ctrs.mailchimp_identity_rows_quarantined == 0
    assert ctrs.mailchimp_identity_conflicts == 1
    assert ctrs.participants_inserted == 1

    # Curated state still written for P2
    assert conn.execute("SELECT COUNT(*) FROM mailchimp_contact_state").fetchone()[0] == 1

    # Review queue has the conflict row
    queue = _queue_rows(conn)
    assert len(queue) == 1
    assert queue[0]["reason_code"] == "leid_conflict"


def test_no_identity_link_without_leid_or_euid(db_conn, tmp_path):
    """Row with no LEID/EUID → no participant_mailchimp_identity row inserted."""
    conn, dsn = db_conn

    ctrs, _ = _run(conn, dsn, tmp_path, [
        _make_row(email="nolink@example.com", first="No", last="Link",
                  leid="", euid=""),
    ])

    assert ctrs.participants_inserted == 1
    assert ctrs.mailchimp_identity_links_inserted == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM participant_mailchimp_identity"
    ).fetchone()[0] == 0
