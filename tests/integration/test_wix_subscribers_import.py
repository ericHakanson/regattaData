"""Integration tests for the Wix subscribers importer (FOR-886).

Covers: reconcile-by-email (link existing vs create net-new), the never-store-an-
email-as-a-name guard, subscriber-status raw capture, and idempotent reruns.
"""

from __future__ import annotations

import csv
from pathlib import Path

from regatta_etl.import_wix_subscribers import _run_wix_subscribers
from regatta_etl.shared import RejectWriter, RunCounters

WIX_ROW_DEFAULTS = {
    "Email 1": "",
    "First Name": "",
    "Last Name": "",
    "Email subscriber status": "Subscribed",
    "Labels": "Subscriptions 7",
    "Source": "Form Submission",
    "Created At (UTC+0)": "2026-01-01 00:00:00",
    "Language": "en",
}


def _row(**overrides) -> dict:
    return {**WIX_ROW_DEFAULTS, **overrides}


def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _run(dsn: str, csv_path: Path, tmp_path: Path, tag: str) -> RunCounters:
    counters = RunCounters()
    rejects = RejectWriter(tmp_path / f"rejects_{tag}.csv")
    _run_wix_subscribers("test-run", "now", dsn, counters, rejects, str(csv_path), dry_run=False)
    rejects.close()
    return counters


def test_wix_reconcile_link_create_and_never_email_as_name(db_conn, tmp_path):
    conn, dsn = db_conn

    # Pre-seed an existing real-named participant owning a known email.
    existing_pid = conn.execute(
        "INSERT INTO participant (full_name, normalized_full_name) "
        "VALUES ('Existing Person', 'existing person') RETURNING id"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO participant_contact_point "
        "(participant_id, contact_type, contact_subtype, contact_value_raw, "
        " contact_value_normalized, is_primary, source_system) "
        "VALUES (%s, 'email', 'primary', 'known@example.com', 'known@example.com', true, 'seed')",
        (existing_pid,),
    )
    conn.commit()

    csv_path = tmp_path / "wix.csv"
    _write_csv(
        csv_path,
        [
            _row(**{"Email 1": "known@example.com"}),  # existing -> link
            _row(**{"Email 1": "newperson@example.com", "First Name": "New", "Last Name": "Person"}),  # net-new named
            _row(**{"Email 1": "nameless@example.com", "Email subscriber status": "Never subscribed"}),  # net-new nameless
        ],
    )

    counters = _run(dsn, csv_path, tmp_path, "main")
    conn.rollback()  # fresh snapshot of the importer's committed writes

    assert counters.participants_matched_existing == 1
    assert counters.participants_inserted == 2
    assert conn.execute("SELECT count(*) FROM wix_subscriber_row").fetchone()[0] == 3

    # Nameless row: participant created but its name must NOT be the email.
    nameless_name = conn.execute(
        "SELECT p.full_name FROM participant p "
        "JOIN participant_contact_point c ON c.participant_id = p.id "
        "WHERE lower(c.contact_value_normalized) = 'nameless@example.com' AND c.source_system = 'wix'"
    ).fetchone()
    assert nameless_name is not None
    assert "@" not in (nameless_name[0] or "")

    # Named net-new got the real name.
    named = conn.execute(
        "SELECT p.full_name FROM participant p "
        "JOIN participant_contact_point c ON c.participant_id = p.id "
        "WHERE lower(c.contact_value_normalized) = 'newperson@example.com' AND c.source_system = 'wix'"
    ).fetchone()[0]
    assert named == "New Person"

    # Existing participant got a single wix provenance point, no new participant.
    assert conn.execute(
        "SELECT count(*) FROM participant_contact_point WHERE participant_id = %s AND source_system = 'wix'",
        (existing_pid,),
    ).fetchone()[0] == 1


def test_wix_reimport_changed_content_keeps_one_participant(db_conn, tmp_path):
    """A later export changing a non-email field (new row_hash) is captured as a new
    raw snapshot, but reconcile-by-email still resolves the SAME participant — no
    duplicate participant is created (refutes the 'changed row -> dup participant' concern)."""
    conn, dsn = db_conn
    first = tmp_path / "wix_first.csv"
    second = tmp_path / "wix_second.csv"
    _write_csv(first, [_row(**{"Email 1": "x@example.com", "Email subscriber status": "Subscribed", "Labels": "A"})])
    _write_csv(second, [_row(**{"Email 1": "x@example.com", "Email subscriber status": "Never subscribed", "Labels": "B"})])

    _run(dsn, first, tmp_path, "r1")
    _run(dsn, second, tmp_path, "r2")
    conn.rollback()

    # Two raw snapshots (lossless history) ...
    assert conn.execute("SELECT count(*) FROM wix_subscriber_row").fetchone()[0] == 2
    # ... but exactly ONE participant owns the email.
    assert conn.execute(
        "SELECT count(DISTINCT c.participant_id) FROM participant_contact_point c "
        "WHERE c.contact_type = 'email' AND lower(c.contact_value_normalized) = 'x@example.com'"
    ).fetchone()[0] == 1


def test_wix_rejects_column_count_mismatch(db_conn, tmp_path):
    """A malformed row (cell count != header count) is rejected loudly, not silently
    truncated; the well-formed row is still captured."""
    conn, dsn = db_conn
    bad = tmp_path / "wix_bad.csv"
    bad.write_text(
        "Email 1,First Name,Email subscriber status\n"
        "good@example.com,Good,Subscribed\n"
        "bad@example.com,Bad,Subscribed,EXTRA_COLUMN\n"
    )
    counters = _run(dsn, bad, tmp_path, "bad")
    conn.rollback()

    assert counters.rows_read == 2
    assert counters.rows_rejected == 1
    assert conn.execute("SELECT count(*) FROM wix_subscriber_row").fetchone()[0] == 1
    assert conn.execute(
        "SELECT count(*) FROM participant_contact_point WHERE source_system = 'wix' "
        "AND lower(contact_value_normalized) = 'bad@example.com'"
    ).fetchone()[0] == 0


def test_wix_import_is_idempotent(db_conn, tmp_path):
    conn, dsn = db_conn
    csv_path = tmp_path / "wix.csv"
    _write_csv(csv_path, [_row(**{"Email 1": "a@example.com", "First Name": "A", "Last Name": "B"})])

    _run(dsn, csv_path, tmp_path, "first")
    _run(dsn, csv_path, tmp_path, "second")
    conn.rollback()

    assert conn.execute(
        "SELECT count(*) FROM participant_contact_point WHERE source_system = 'wix'"
    ).fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM wix_subscriber_row").fetchone()[0] == 1
