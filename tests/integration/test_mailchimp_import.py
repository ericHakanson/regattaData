"""Integration tests for the mailchimp_audience ingestion pipeline."""

from __future__ import annotations

import csv
import json
import uuid
from pathlib import Path

import pytest

from regatta_etl.import_mailchimp_audience import _run_mailchimp_audience
from regatta_etl.shared import RunCounters, RejectWriter


# ---------------------------------------------------------------------------
# CSV fixture helpers
# ---------------------------------------------------------------------------

SUBSCRIBED_HEADERS = [
    "Email Address", "First Name", "Last Name", "Address", "Phone Number",
    "Birthday", "MEMBER_RATING", "OPTIN_TIME", "OPTIN_IP", "CONFIRM_TIME",
    "CONFIRM_IP", "GMTOFF", "DSTOFF", "TIMEZONE", "CC", "REGION",
    "LAST_CHANGED", "LEID", "EUID", "NOTES", "TAGS",
]

UNSUBSCRIBED_HEADERS = [
    "Email Address", "First Name", "Last Name", "Address", "Phone Number",
    "Birthday", "MEMBER_RATING", "OPTIN_TIME", "OPTIN_IP", "CONFIRM_TIME",
    "CONFIRM_IP", "GMTOFF", "DSTOFF", "TIMEZONE", "CC", "REGION",
    "UNSUB_TIME", "UNSUB_CAMPAIGN_TITLE", "UNSUB_CAMPAIGN_ID",
    "UNSUB_REASON", "UNSUB_REASON_OTHER", "LEID", "EUID", "NOTES", "TAGS",
]

CLEANED_HEADERS = [
    "Email Address", "First Name", "Last Name", "Address", "Phone Number",
    "Birthday", "MEMBER_RATING", "OPTIN_TIME", "OPTIN_IP", "CONFIRM_TIME",
    "CONFIRM_IP", "GMTOFF", "DSTOFF", "TIMEZONE", "CC", "REGION",
    "CLEAN_TIME", "CLEAN_CAMPAIGN_TITLE", "CLEAN_CAMPAIGN_ID",
    "LEID", "EUID", "NOTES", "TAGS",
]


def _make_subscribed_row(
    email: str = "alice@example.com",
    first: str = "Alice",
    last: str = "Smith",
    phone: str = "207-555-0101",
    address: str = "1 Main St, Bar Harbor, ME  04609  US",
    last_changed: str = "2025-07-30 12:00:00",
    tags: str = '"Member","2025 waiver"',
    **kwargs,
) -> dict[str, str]:
    row = {h: "" for h in SUBSCRIBED_HEADERS}
    row.update({
        "Email Address": email,
        "First Name": first,
        "Last Name": last,
        "Phone Number": phone,
        "Address": address,
        "MEMBER_RATING": "2",
        "OPTIN_TIME": "2025-01-01 00:00:00",
        "CONFIRM_TIME": "2025-01-01 00:00:00",
        "LAST_CHANGED": last_changed,
        "LEID": "123456",
        "EUID": "abcdef1234",
        "TAGS": tags,
    })
    row.update(kwargs)
    return row


def _make_unsubscribed_row(
    email: str = "bob@example.com",
    first: str = "Bob",
    last: str = "Jones",
    unsub_time: str = "2025-06-01 10:00:00",
    **kwargs,
) -> dict[str, str]:
    row = {h: "" for h in UNSUBSCRIBED_HEADERS}
    row.update({
        "Email Address": email,
        "First Name": first,
        "Last Name": last,
        "MEMBER_RATING": "1",
        "OPTIN_TIME": "2024-01-01 00:00:00",
        "UNSUB_TIME": unsub_time,
        "UNSUB_CAMPAIGN_TITLE": "Summer Newsletter",
        "UNSUB_CAMPAIGN_ID": "camp001",
        "UNSUB_REASON": "not_interested",
        "LEID": "999888",
        "EUID": "zzzzz99999",
    })
    row.update(kwargs)
    return row


def _make_cleaned_row(
    email: str = "carol@example.com",
    first: str = "Carol",
    last: str = "Lee",
    clean_time: str = "2025-05-15 09:00:00",
    **kwargs,
) -> dict[str, str]:
    row = {h: "" for h in CLEANED_HEADERS}
    row.update({
        "Email Address": email,
        "First Name": first,
        "Last Name": last,
        "MEMBER_RATING": "1",
        "OPTIN_TIME": "2023-06-01 00:00:00",
        "CLEAN_TIME": clean_time,
        "CLEAN_CAMPAIGN_TITLE": "Bounce Cleanup",
        "CLEAN_CAMPAIGN_ID": "clean001",
        "LEID": "777666",
        "EUID": "yyyyyyy777",
    })
    row.update(kwargs)
    return row


def _write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Test: end-to-end happy path
# ---------------------------------------------------------------------------

def test_mailchimp_e2e(db_conn, tmp_path):
    conn, dsn = db_conn

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"
    rejects_path = tmp_path / "rejects.csv"

    _write_csv(sub_path, SUBSCRIBED_HEADERS, [_make_subscribed_row()])
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, [_make_unsubscribed_row()])
    _write_csv(clean_path, CLEANED_HEADERS, [_make_cleaned_row()])

    run_id = str(uuid.uuid4())
    counters = RunCounters()
    rejects = RejectWriter(rejects_path)

    _run_mailchimp_audience(
        run_id=run_id,
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        subscribed_path=str(sub_path),
        unsubscribed_path=str(unsub_path),
        cleaned_path=str(clean_path),
        max_reject_rate=0.05,
        dry_run=False,
    )

    # 3 rows read, 0 rejected
    assert counters.rows_read == 3
    assert counters.rows_rejected == 0
    assert counters.db_phase_errors == 0

    # 3 participants created
    assert counters.participants_inserted == 3
    assert counters.participants_matched_existing == 0

    # 3 raw rows captured
    assert counters.raw_rows_inserted == 3

    # 3 state rows
    assert counters.mailchimp_status_rows_inserted == 3

    # 2 tags from subscribed row ("Member", "2025 waiver")
    assert counters.mailchimp_tags_inserted == 2

    # Verify raw table
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mailchimp_audience_row")
        assert cur.fetchone()[0] == 3

        cur.execute(
            "SELECT audience_status FROM mailchimp_audience_row ORDER BY audience_status"
        )
        statuses = [r[0] for r in cur.fetchall()]
        assert statuses == ["cleaned", "subscribed", "unsubscribed"]

    # Verify state table
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mailchimp_contact_state")
        assert cur.fetchone()[0] == 3

    # Verify tag table
    with conn.cursor() as cur:
        cur.execute("SELECT tag_value FROM mailchimp_contact_tag ORDER BY tag_value")
        tags = [r[0] for r in cur.fetchall()]
        assert "Member" in tags
        assert "2025 waiver" in tags


# ---------------------------------------------------------------------------
# Test: idempotency — rerun produces zero new logical inserts
# ---------------------------------------------------------------------------

def test_mailchimp_idempotency(db_conn, tmp_path):
    conn, dsn = db_conn

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"

    _write_csv(sub_path, SUBSCRIBED_HEADERS, [_make_subscribed_row()])
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, [_make_unsubscribed_row()])
    _write_csv(clean_path, CLEANED_HEADERS, [_make_cleaned_row()])

    def run():
        c = RunCounters()
        r = RejectWriter(tmp_path / f"{uuid.uuid4()}_rejects.csv")
        _run_mailchimp_audience(
            run_id=str(uuid.uuid4()),
            started_at="2026-01-01T00:00:00",
            db_dsn=dsn,
            counters=c,
            rejects=r,
            subscribed_path=str(sub_path),
            unsubscribed_path=str(unsub_path),
            cleaned_path=str(clean_path),
            max_reject_rate=0.05,
            dry_run=False,
        )
        return c

    c1 = run()
    c2 = run()

    # Second run: no new participants, no new raw rows, no new state rows, no new tags
    assert c2.participants_inserted == 0
    assert c2.participants_matched_existing == 3
    assert c2.raw_rows_inserted == 0
    assert c2.mailchimp_status_rows_inserted == 0
    assert c2.mailchimp_tags_inserted == 0
    assert c2.contact_points_inserted == 0
    assert c2.addresses_inserted == 0

    # Database counts unchanged
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mailchimp_audience_row")
        assert cur.fetchone()[0] == 3
        cur.execute("SELECT COUNT(*) FROM mailchimp_contact_state")
        assert cur.fetchone()[0] == 3
        cur.execute("SELECT COUNT(*) FROM mailchimp_contact_tag")
        assert cur.fetchone()[0] == 2  # same two tags from first run


# ---------------------------------------------------------------------------
# Test: ambiguous email match → reject row
# ---------------------------------------------------------------------------

def test_mailchimp_ambiguous_email_reject(db_conn, tmp_path):
    conn, dsn = db_conn

    # Pre-insert two participants that both have the same email contact point
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO participant (full_name, normalized_full_name) VALUES ('P One', 'p one') RETURNING id"
        )
        p1 = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO participant (full_name, normalized_full_name) VALUES ('P Two', 'p two') RETURNING id"
        )
        p2 = cur.fetchone()[0]
        shared_email = "shared@example.com"
        for pid in (p1, p2):
            cur.execute(
                """
                INSERT INTO participant_contact_point
                  (participant_id, contact_type, contact_subtype,
                   contact_value_raw, contact_value_normalized, is_primary, source_system)
                VALUES (%s, 'email', 'primary', %s, %s, true, 'test')
                """,
                (pid, shared_email, shared_email),
            )
    conn.commit()

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"
    rejects_path = tmp_path / "rejects.csv"

    _write_csv(
        sub_path, SUBSCRIBED_HEADERS,
        [_make_subscribed_row(email=shared_email)],
    )
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, [])
    _write_csv(clean_path, CLEANED_HEADERS, [])

    counters = RunCounters()
    rejects = RejectWriter(rejects_path)

    _run_mailchimp_audience(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        subscribed_path=str(sub_path),
        unsubscribed_path=str(unsub_path),
        cleaned_path=str(clean_path),
        max_reject_rate=1.0,  # allow all rejections so run doesn't abort
        dry_run=False,
    )

    assert counters.rows_rejected == 1
    assert counters.db_phase_errors == 0  # ambiguous match is a row reject, not a DB error

    # Verify reject file was written with the correct reason
    with rejects_path.open() as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    assert len(rows) == 1
    assert "ambiguous_email_match" in rows[0]["_reject_reason"]


# ---------------------------------------------------------------------------
# Test: raw capture completeness — every source row in mailchimp_audience_row
# ---------------------------------------------------------------------------

def test_mailchimp_raw_capture_completeness(db_conn, tmp_path):
    conn, dsn = db_conn

    subscribed_rows = [
        _make_subscribed_row(email="a@example.com"),
        _make_subscribed_row(email="b@example.com", first="Bob", last="Brown"),
    ]
    unsubscribed_rows = [_make_unsubscribed_row()]
    cleaned_rows = [_make_cleaned_row()]

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"

    _write_csv(sub_path, SUBSCRIBED_HEADERS, subscribed_rows)
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, unsubscribed_rows)
    _write_csv(clean_path, CLEANED_HEADERS, cleaned_rows)

    counters = RunCounters()
    rejects = RejectWriter(tmp_path / "rejects.csv")

    _run_mailchimp_audience(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        subscribed_path=str(sub_path),
        unsubscribed_path=str(unsub_path),
        cleaned_path=str(clean_path),
        max_reject_rate=0.05,
        dry_run=False,
    )

    total_source_rows = len(subscribed_rows) + len(unsubscribed_rows) + len(cleaned_rows)
    assert counters.rows_read == total_source_rows
    assert counters.raw_rows_inserted == total_source_rows

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mailchimp_audience_row")
        assert cur.fetchone()[0] == total_source_rows

        # All source email addresses are retrievable from raw_payload
        cur.execute(
            "SELECT raw_payload->>'Email Address' FROM mailchimp_audience_row"
        )
        captured_emails = {r[0] for r in cur.fetchall()}
    assert "a@example.com" in captured_emails
    assert "b@example.com" in captured_emails
    assert "bob@example.com" in captured_emails
    assert "carol@example.com" in captured_emails


# ---------------------------------------------------------------------------
# Test: dry-run rolls back all changes
# ---------------------------------------------------------------------------

def test_mailchimp_dry_run_rollback(db_conn, tmp_path):
    conn, dsn = db_conn

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"

    _write_csv(sub_path, SUBSCRIBED_HEADERS, [_make_subscribed_row()])
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, [_make_unsubscribed_row()])
    _write_csv(clean_path, CLEANED_HEADERS, [_make_cleaned_row()])

    counters = RunCounters()
    rejects = RejectWriter(tmp_path / "rejects.csv")

    _run_mailchimp_audience(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        subscribed_path=str(sub_path),
        unsubscribed_path=str(unsub_path),
        cleaned_path=str(clean_path),
        max_reject_rate=0.05,
        dry_run=True,
    )

    # Counters reflect execution (full DB path was taken)
    assert counters.rows_read == 3
    assert counters.rows_rejected == 0

    # But nothing persisted
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mailchimp_audience_row")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM mailchimp_contact_state")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM mailchimp_contact_tag")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM participant")
        assert cur.fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Test: missing email → row reject, not run abort
# ---------------------------------------------------------------------------

def test_mailchimp_missing_email_reject(db_conn, tmp_path):
    conn, dsn = db_conn

    good_row = _make_subscribed_row(email="good@example.com")
    bad_row = _make_subscribed_row(email="")  # blank email

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"
    rejects_path = tmp_path / "rejects.csv"

    _write_csv(sub_path, SUBSCRIBED_HEADERS, [good_row, bad_row])
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, [])
    _write_csv(clean_path, CLEANED_HEADERS, [])

    counters = RunCounters()
    rejects = RejectWriter(rejects_path)

    _run_mailchimp_audience(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        subscribed_path=str(sub_path),
        unsubscribed_path=str(unsub_path),
        cleaned_path=str(clean_path),
        max_reject_rate=1.0,
        dry_run=False,
    )

    assert counters.rows_read == 2
    assert counters.rows_rejected == 1
    assert counters.participants_inserted == 1  # only good_row

    # Both rows must be captured in the raw table — lossless even for rejects
    assert counters.raw_rows_inserted == 2
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mailchimp_audience_row")
        assert cur.fetchone()[0] == 2
        # Rejected row has NULL email columns
        cur.execute(
            "SELECT source_email_raw FROM mailchimp_audience_row WHERE source_email_raw IS NULL"
        )
        assert cur.fetchone() is not None

    with rejects_path.open() as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    assert rows[0]["_reject_reason"] == "missing_email_address"


# ---------------------------------------------------------------------------
# Test: name-absent contact uses email as participant display name
# ---------------------------------------------------------------------------

def test_mailchimp_name_absent_contact(db_conn, tmp_path):
    conn, dsn = db_conn

    no_name_row = _make_subscribed_row(
        email="noname@example.com", first="", last=""
    )

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"

    _write_csv(sub_path, SUBSCRIBED_HEADERS, [no_name_row])
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, [])
    _write_csv(clean_path, CLEANED_HEADERS, [])

    counters = RunCounters()
    rejects = RejectWriter(tmp_path / "rejects.csv")

    _run_mailchimp_audience(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        subscribed_path=str(sub_path),
        unsubscribed_path=str(unsub_path),
        cleaned_path=str(clean_path),
        max_reject_rate=0.05,
        dry_run=False,
    )

    assert counters.rows_rejected == 0
    assert counters.participants_inserted == 1

    with conn.cursor() as cur:
        cur.execute("SELECT full_name FROM participant LIMIT 1")
        full_name = cur.fetchone()[0]
    # fallback to email when no name provided
    assert full_name == "noname@example.com"


# ---------------------------------------------------------------------------
# Test: raw capture includes rows rejected for email issues
# ---------------------------------------------------------------------------

def test_mailchimp_raw_capture_includes_rejected_rows(db_conn, tmp_path):
    """Every source row lands in mailchimp_audience_row, even if curated projection is rejected."""
    conn, dsn = db_conn

    malformed_row = _make_subscribed_row(email="not-an-email")
    blank_email_row = _make_subscribed_row(email="", first="Nobody", last="Here")
    valid_row = _make_subscribed_row(email="valid@example.com")

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"

    _write_csv(sub_path, SUBSCRIBED_HEADERS, [malformed_row, blank_email_row, valid_row])
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, [])
    _write_csv(clean_path, CLEANED_HEADERS, [])

    counters = RunCounters()
    rejects = RejectWriter(tmp_path / "rejects.csv")

    _run_mailchimp_audience(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        subscribed_path=str(sub_path),
        unsubscribed_path=str(unsub_path),
        cleaned_path=str(clean_path),
        max_reject_rate=1.0,
        dry_run=False,
    )

    # 2 email-invalid rows rejected, 1 valid processed
    assert counters.rows_read == 3
    assert counters.rows_rejected == 2
    assert counters.participants_inserted == 1

    # All 3 source rows captured in raw table
    assert counters.raw_rows_inserted == 3
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mailchimp_audience_row")
        assert cur.fetchone()[0] == 3

        # Confirm blank-email row stored with NULL email columns
        cur.execute(
            "SELECT COUNT(*) FROM mailchimp_audience_row WHERE source_email_raw IS NULL"
        )
        assert cur.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Test: unparseable required status datetime → row reject
# ---------------------------------------------------------------------------

def test_mailchimp_unparseable_status_datetime(db_conn, tmp_path):
    """A row with a non-empty but unparseable LAST_CHANGED is rejected."""
    conn, dsn = db_conn

    bad_ts_row = _make_subscribed_row(
        email="badts@example.com", last_changed="not-a-date"
    )
    good_row = _make_subscribed_row(email="good@example.com")

    sub_path = tmp_path / "subscribed.csv"
    unsub_path = tmp_path / "unsubscribed.csv"
    clean_path = tmp_path / "cleaned.csv"
    rejects_path = tmp_path / "rejects.csv"

    _write_csv(sub_path, SUBSCRIBED_HEADERS, [bad_ts_row, good_row])
    _write_csv(unsub_path, UNSUBSCRIBED_HEADERS, [])
    _write_csv(clean_path, CLEANED_HEADERS, [])

    counters = RunCounters()
    rejects = RejectWriter(rejects_path)

    _run_mailchimp_audience(
        run_id=str(uuid.uuid4()),
        started_at="2026-01-01T00:00:00",
        db_dsn=dsn,
        counters=counters,
        rejects=rejects,
        subscribed_path=str(sub_path),
        unsubscribed_path=str(unsub_path),
        cleaned_path=str(clean_path),
        max_reject_rate=1.0,
        dry_run=False,
    )

    assert counters.rows_read == 2
    assert counters.rows_rejected == 1
    assert counters.participants_inserted == 1  # only good_row gets a participant

    # Bad-ts row is still in raw capture
    assert counters.raw_rows_inserted == 2

    with rejects_path.open() as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    assert rows[0]["_reject_reason"] == "unparseable_status_datetime"
