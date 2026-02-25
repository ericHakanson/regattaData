"""regatta_etl.import_regattaman_csv

Unified CLI entrypoint for Regattaman ingestion.

Modes (--mode):
  private_export  — import a regattaman.com private CSV export (default)
  public_scrape   — import public-scrape event + entries CSVs

Usage (private_export):
    python -m regatta_etl.import_regattaman_csv \\
        --mode private_export \\
        --db-dsn "$DB_DSN" \\
        --csv-path "rawEvidence/export2025bhycRegatta - export.csv" \\
        --host-club-name "Boothbay Harbor Yacht Club" \\
        --host-club-normalized "boothbay-harbor-yacht-club" \\
        --event-series-name "BHYC Regatta" \\
        --event-series-normalized "bhyc-regatta" \\
        --event-display-name "BHYC Regatta 2025" \\
        --season-year 2025 \\
        --registration-source "regattaman" \\
        --source-system "regattaman_csv_export" \\
        --asset-type "regattaman_csv_export"

Usage (public_scrape):
    python -m regatta_etl.import_regattaman_csv \\
        --mode public_scrape \\
        --db-dsn "$DB_DSN" \\
        --entries-path "rawEvidence/all_regattaman_entries_2021_2025.csv" \\
        --events-path "rawEvidence/consolidated_regattaman_events_2021_2025_with_event_url.csv" \\
        --rejects-path "artifacts/rejects/public_scrape_rejects.csv"
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import click
import psycopg

from regatta_etl.normalize import (
    normalize_email,
    normalize_name,
    normalize_phone,
    normalize_space,
    parse_co_owners,
    parse_date_from_ts,
    parse_name_parts,
    parse_numeric,
    parse_ts,
    slug_name,
    trim,
)
from regatta_etl.shared import (
    AmbiguousMatchError,
    RejectWriter,
    RunCounters,
    insert_participant,
    normalize_headers,
    resolve_or_insert_coowner_participant,
    resolve_or_insert_yacht,
    resolve_participant_by_name,
    upsert_affiliate_club,
    upsert_entry_participant,
    upsert_event_context,
    upsert_event_entry,
    upsert_membership,
    upsert_ownership,
    write_run_report,
)

# ---------------------------------------------------------------------------
# Private-export constants
# ---------------------------------------------------------------------------

REQUIRED_HEADERS = {
    "oid", "bid", "date_entered", "cid", "sku", "parent_sku",
    "date_paid", "date_updated", "Paid Type", "Discounts",
    "ownername", "Name", "owner_address", "City", "owner_state",
    "ccode", "owner_zip", "owner_hphone", "owner_cphone", "Email",
    "org_name", "Club", "org_abbrev", "Yacht Name", "Boat  Type",
    "LOA", "Moor Depth",
}

PLACEHOLDER_ORGS = {"-", "none/other", "regatta management solutions"}

STATUS_PRECEDENCE = {"confirmed": 3, "submitted": 2, "draft": 1, "unknown": 0}

# ---------------------------------------------------------------------------
# Backward-compat re-exports for existing tests and callers
# ---------------------------------------------------------------------------

_RejectWriter = RejectWriter
_AmbiguousMatchError = AmbiguousMatchError
_normalize_headers = normalize_headers
_upsert_event_context = upsert_event_context


# ---------------------------------------------------------------------------
# Row helpers (private_export)
# ---------------------------------------------------------------------------

def _derive_entry_status(row: dict[str, str]) -> str:
    date_paid = parse_ts(row.get("date_paid"))
    paid_type = (trim(row.get("Paid Type")) or "").lower()
    date_entered = parse_ts(row.get("date_entered"))

    if date_paid is not None:
        return "confirmed"
    if paid_type == "free":
        return "confirmed"
    if date_entered is not None:
        return "submitted"
    return "unknown"


def _is_placeholder_org(org_name: str | None, org_abbrev: str | None) -> bool:
    if org_abbrev and org_abbrev.strip().upper() == "RMS":
        return True
    if not org_name:
        return True
    return org_name.strip().lower() in PLACEHOLDER_ORGS


# ---------------------------------------------------------------------------
# DB helpers — participant resolution (private_export only)
# ---------------------------------------------------------------------------

def _resolve_participant_by_email(
    conn: psycopg.Connection,
    email_norm: str,
    run_id: str,
    counters: RunCounters,
) -> str | None:
    rows = conn.execute(
        """
        SELECT DISTINCT participant_id
        FROM participant_contact_point
        WHERE contact_type = 'email'
          AND contact_value_normalized = %s
        """,
        (email_norm,),
    ).fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        counters.warnings.append(
            f"run_id={run_id} ambiguous email match for {email_norm!r}: "
            f"{len(rows)} distinct participants"
        )
        raise AmbiguousMatchError(f"ambiguous_contact_match: email={email_norm!r}")
    return str(rows[0][0])


def _resolve_participant_by_phone(
    conn: psycopg.Connection,
    phone_norm: str,
    run_id: str,
    counters: RunCounters,
) -> str | None:
    rows = conn.execute(
        """
        SELECT DISTINCT participant_id
        FROM participant_contact_point
        WHERE contact_type = 'phone'
          AND contact_value_normalized = %s
        """,
        (phone_norm,),
    ).fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        counters.warnings.append(
            f"run_id={run_id} ambiguous phone match for {phone_norm!r}: "
            f"{len(rows)} distinct participants"
        )
        raise AmbiguousMatchError(f"ambiguous_contact_match: phone={phone_norm!r}")
    return str(rows[0][0])


def _resolve_or_insert_primary_participant(
    conn: psycopg.Connection,
    row: dict[str, str],
    run_id: str,
    counters: RunCounters,
) -> str:
    """Resolve primary owner via email → phone → name, or insert new."""
    email_norm = normalize_email(row.get("Email"))
    hphone_norm = normalize_phone(row.get("owner_hphone"))
    cphone_norm = normalize_phone(row.get("owner_cphone"))
    ownername = normalize_space(row.get("ownername")) or ""
    name_norm = normalize_name(ownername)

    pid: str | None = None

    if email_norm:
        pid = _resolve_participant_by_email(conn, email_norm, run_id, counters)
    if pid is None and hphone_norm:
        pid = _resolve_participant_by_phone(conn, hphone_norm, run_id, counters)
    if pid is None and cphone_norm:
        pid = _resolve_participant_by_phone(conn, cphone_norm, run_id, counters)
    if pid is None and name_norm:
        pid = resolve_participant_by_name(conn, name_norm)

    if pid is not None:
        counters.participants_matched_existing += 1
        return pid

    pid = insert_participant(conn, ownername)
    counters.participants_inserted += 1
    return pid


# ---------------------------------------------------------------------------
# DB helpers — contacts, address (private_export)
# ---------------------------------------------------------------------------

def _upsert_contact_point(
    conn: psycopg.Connection,
    participant_id: str,
    contact_type: str,
    contact_subtype: str,
    raw_value: str,
    norm_value: str | None,
    is_primary: bool,
    source_system: str,
    counters: RunCounters,
) -> None:
    existing = conn.execute(
        """
        SELECT id FROM participant_contact_point
        WHERE participant_id = %s
          AND contact_type = %s
          AND contact_subtype = %s
          AND contact_value_normalized = %s
        """,
        (participant_id, contact_type, contact_subtype, norm_value),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO participant_contact_point
          (participant_id, contact_type, contact_subtype,
           contact_value_raw, contact_value_normalized,
           is_primary, source_system)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (participant_id, contact_type, contact_subtype,
         raw_value, norm_value, is_primary, source_system),
    )
    counters.contact_points_inserted += 1


def _upsert_address(
    conn: psycopg.Connection,
    participant_id: str,
    row: dict[str, str],
    source_system: str,
    counters: RunCounters,
) -> None:
    line1 = trim(row.get("owner_address"))
    city = trim(row.get("City"))
    state = trim(row.get("owner_state"))
    postal_code = trim(row.get("owner_zip"))
    country_code = trim(row.get("ccode"))

    components = [c for c in [line1, city, state, postal_code, country_code] if c]
    if not components:
        return

    address_raw = " | ".join(components)

    existing = conn.execute(
        """
        SELECT id FROM participant_address
        WHERE participant_id = %s AND address_raw = %s
        """,
        (participant_id, address_raw),
    ).fetchone()
    if existing:
        return

    conn.execute(
        """
        INSERT INTO participant_address
          (participant_id, address_type, line1, city, state,
           postal_code, country_code, address_raw, is_primary, source_system)
        VALUES (%s, 'mailing', %s, %s, %s, %s, %s, %s, true, %s)
        """,
        (participant_id, line1, city, state, postal_code, country_code,
         address_raw, source_system),
    )
    counters.addresses_inserted += 1


# ---------------------------------------------------------------------------
# Per-row processing (private_export)
# ---------------------------------------------------------------------------

def _process_row(
    conn: psycopg.Connection,
    row: dict[str, str],
    event_instance_id: str,
    registration_source: str,
    source_system: str,
    run_id: str,
    counters: RunCounters,
) -> None:
    """Process one valid row.  Caller manages transaction/savepoint."""

    # Step 1: Parse all owners before any DB work
    owners = parse_co_owners(row.get("ownername"), row.get("Name"))

    # Step 2: Resolve/insert each participant
    participant_ids: list[tuple[str, str]] = []

    primary_pid = _resolve_or_insert_primary_participant(
        conn, row, run_id, counters
    )
    participant_ids.append((primary_pid, "owner"))

    for co_name, co_role in owners[1:]:
        co_pid = resolve_or_insert_coowner_participant(conn, co_name, counters)
        participant_ids.append((co_pid, co_role))

    primary_pid = participant_ids[0][0]

    # Step 3: Contact points (primary owner only)
    email_raw = trim(row.get("Email"))
    email_norm = normalize_email(email_raw)
    if email_raw and email_norm:
        _upsert_contact_point(
            conn, primary_pid, "email", "primary",
            email_raw, email_norm, True, source_system, counters,
        )

    hphone_raw = trim(row.get("owner_hphone"))
    hphone_norm = normalize_phone(hphone_raw)
    if hphone_raw and hphone_norm:
        _upsert_contact_point(
            conn, primary_pid, "phone", "home",
            hphone_raw, hphone_norm, False, source_system, counters,
        )

    cphone_raw = trim(row.get("owner_cphone"))
    cphone_norm = normalize_phone(cphone_raw)
    if cphone_raw and cphone_norm:
        _upsert_contact_point(
            conn, primary_pid, "phone", "mobile",
            cphone_raw, cphone_norm, False, source_system, counters,
        )

    # Step 4: Address (primary owner only)
    _upsert_address(conn, primary_pid, row, source_system, counters)

    # Step 5: Affiliate club + membership (primary owner only)
    org_name = normalize_space(row.get("org_name"))
    org_abbrev = trim(row.get("org_abbrev"))
    if not _is_placeholder_org(org_name, org_abbrev):
        club_id = upsert_affiliate_club(conn, org_name)  # type: ignore[arg-type]
        effective_start = parse_date_from_ts(row.get("date_entered"))
        upsert_membership(
            conn, primary_pid, club_id, effective_start, source_system, counters
        )

    # Step 6: Yacht
    yacht_name = normalize_space(row.get("Yacht Name")) or ""
    boat_type = trim(row.get("Boat  Type"))
    loa = row.get("LOA")
    length = parse_numeric(loa)
    yacht_id = resolve_or_insert_yacht(conn, yacht_name, boat_type, length, counters)

    # Step 7: Yacht ownership (all parsed owners)
    effective_start_date = parse_date_from_ts(row.get("date_entered")) or date.today()
    for idx, (pid, role) in enumerate(participant_ids):
        upsert_ownership(
            conn, pid, yacht_id, role,
            is_primary_contact=(idx == 0),
            effective_start=effective_start_date,
            source_system=source_system,
            counters=counters,
        )

    # Step 8: Event entry
    entry_status = _derive_entry_status(row)
    sku = trim(row.get("sku"))
    parent_sku = trim(row.get("parent_sku"))
    reg_ext_id = sku or parent_sku
    registered_at = parse_ts(row.get("date_entered"))

    event_entry_id = upsert_event_entry(
        conn, event_instance_id, yacht_id, entry_status,
        registration_source, reg_ext_id, registered_at, counters,
    )

    # Step 9: Event entry participant (all parsed owners)
    for pid, _role in participant_ids:
        upsert_entry_participant(
            conn, event_entry_id, pid,
            role="owner_contact",
            participation_state="non_participating_contact",
            source_system=source_system,
        )


# upsert_ownership is imported from regatta_etl.shared


# ---------------------------------------------------------------------------
# Unified CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option(
    "--mode",
    default="private_export",
    type=click.Choice([
        "private_export", "public_scrape", "jotform_waiver",
        "mailchimp_audience", "airtable_copy", "yacht_scoring",
        "bhyc_member_directory",
        "resolution_source_to_candidate", "resolution_score", "resolution_promote",
        "resolution_manual_apply", "resolution_lifecycle",
        "lineage_report", "purge_check",
    ]),
    show_default=True,
    help="Ingestion mode",
)
@click.option("--db-dsn", required=True, help="PostgreSQL DSN")
# private_export flags
@click.option("--csv-path", default=None, type=click.Path(), help="[private_export|jotform_waiver] Input CSV")
@click.option("--host-club-name", default=None)
@click.option("--host-club-normalized", default=None)
@click.option("--event-series-name", default=None)
@click.option("--event-series-normalized", default=None)
@click.option("--event-display-name", default=None)
@click.option("--season-year", default=None, type=int)
@click.option("--registration-source", default=None)
@click.option("--source-system", default=None)
@click.option("--asset-type", default=None)
@click.option("--gcs-bucket", default=None)
@click.option("--gcs-object", default=None)
@click.option("--gcs-prefix", default=None, help="[bhyc_member_directory] GCS object prefix for archived HTML")
# bhyc_member_directory flags
@click.option("--start-url", default=None, help="[bhyc_member_directory] Directory start URL")
@click.option("--member-user-env", default="BHYC_USERNAME", show_default=True, help="[bhyc_member_directory] Env var name holding BHYC username")
@click.option("--member-pass-env", default="BHYC_PASSWORD", show_default=True, help="[bhyc_member_directory] Env var name holding BHYC password")
@click.option("--checkpoint-path", default=None, type=click.Path(), help="[bhyc_member_directory] Path to JSON checkpoint file for resumable runs")
@click.option("--request-delay-seconds", default=10.0, type=float, show_default=True, help="[bhyc_member_directory] Base delay between requests in seconds")
@click.option("--request-jitter-seconds", default=3.0, type=float, show_default=True, help="[bhyc_member_directory] Random ±jitter added to each delay")
@click.option("--max-consecutive-failures", default=5, type=int, show_default=True, help="[bhyc_member_directory] Stop after this many consecutive fetch failures")
@click.option("--max-pages", default=None, type=int, help="[bhyc_member_directory] Limit total directory pages fetched (for testing)")
@click.option("--resume-from-checkpoint", is_flag=True, default=False, help="[bhyc_member_directory] Load existing checkpoint before crawling")
@click.option("--archive-local-dir", default=None, type=click.Path(), help="[bhyc_member_directory] Archive HTML to local dir instead of GCS (for testing)")
# public_scrape flags
@click.option("--entries-path", default=None, type=click.Path(), help="[public_scrape] Entries CSV")
@click.option("--events-path", default=None, type=click.Path(), help="[public_scrape] Events CSV")
@click.option(
    "--max-reject-rate",
    default=0.05,
    type=float,
    show_default=True,
    help="[public_scrape|mailchimp_audience] Fraction of rows that may be rejected before run fails",
)
# mailchimp_audience flags
@click.option("--airtable-dir", default=None, type=click.Path(), help="[airtable_copy] Directory containing all 6 Airtable CSV files")
@click.option("--yacht-scoring-dir", default=None, type=click.Path(), help="[yacht_scoring] Root directory of Yacht Scoring CSVs")
@click.option("--subscribed-path", default=None, type=click.Path(), help="[mailchimp_audience] Subscribed audience CSV")
@click.option("--unsubscribed-path", default=None, type=click.Path(), help="[mailchimp_audience] Unsubscribed audience CSV")
@click.option("--cleaned-path", default=None, type=click.Path(), help="[mailchimp_audience] Cleaned audience CSV")
@click.option(
    "--event-unresolved-link-max-reject-rate",
    default=0.05,
    type=float,
    show_default=True,
    help="[jotform_waiver] Fraction of rows that may have unresolved event links before run fails",
)
@click.option(
    "--synthesize-events/--no-synthesize-events",
    default=True,
    show_default=True,
    help="[public_scrape] Synthesize event records for unmatched entry URLs",
)
# shared flags
@click.option("--dry-run", is_flag=True, default=False)
@click.option(
    "--rejects-path",
    default="./artifacts/rejects/regattaman_rejects.csv",
    show_default=True,
)
@click.option("--run-id", default=None, help="Override UUID for log correlation")
@click.option(
    "--entity-type",
    default="all",
    type=click.Choice(["participant", "yacht", "event", "registration", "club", "all"]),
    show_default=True,
    help="[resolution_*] Entity type to process",
)
@click.option(
    "--rule-file",
    default=None,
    type=click.Path(),
    help="[resolution_score] Path to YAML scoring rule file — not consumed by resolution_source_to_candidate",
)
@click.option(
    "--decisions-path",
    default=None,
    type=click.Path(),
    help="[resolution_manual_apply] Path to CSV of manual review decisions",
)
@click.option(
    "--rescore-after-apply/--no-rescore-after-apply",
    default=False,
    show_default=True,
    help="[resolution_manual_apply] Re-score entity types with successful promotes after apply",
)
@click.option(
    "--decisions-validate-only/--no-decisions-validate-only",
    default=False,
    show_default=True,
    help="[resolution_manual_apply] Parse and validate CSV rows only; skip all DB operations",
)
@click.option(
    "--lifecycle-op",
    default=None,
    type=click.Choice(["merge", "demote", "unlink", "split"]),
    help="[resolution_lifecycle] Lifecycle operation type",
)
@click.option(
    "--canonical-threshold-pct",
    default=90.0,
    type=float,
    show_default=True,
    help="[lineage_report|purge_check] Minimum %% of candidates that must be promoted",
)
@click.option(
    "--source-threshold-pct",
    default=90.0,
    type=float,
    show_default=True,
    help="[lineage_report|purge_check] Minimum %% of source rows that must be linked",
)
def main(
    mode: str,
    db_dsn: str,
    # airtable_copy
    airtable_dir: str | None,
    # yacht_scoring
    yacht_scoring_dir: str | None,
    # private_export
    csv_path: str | None,
    host_club_name: str | None,
    host_club_normalized: str | None,
    event_series_name: str | None,
    event_series_normalized: str | None,
    event_display_name: str | None,
    season_year: int | None,
    registration_source: str | None,
    source_system: str | None,
    asset_type: str | None,
    gcs_bucket: str | None,
    gcs_object: str | None,
    gcs_prefix: str | None,
    # bhyc_member_directory
    start_url: str | None,
    member_user_env: str,
    member_pass_env: str,
    checkpoint_path: str | None,
    request_delay_seconds: float,
    request_jitter_seconds: float,
    max_consecutive_failures: int,
    max_pages: int | None,
    resume_from_checkpoint: bool,
    archive_local_dir: str | None,
    # public_scrape
    entries_path: str | None,
    events_path: str | None,
    max_reject_rate: float,
    event_unresolved_link_max_reject_rate: float,
    synthesize_events: bool,
    # mailchimp_audience
    subscribed_path: str | None,
    unsubscribed_path: str | None,
    cleaned_path: str | None,
    # shared
    dry_run: bool,
    rejects_path: str,
    run_id: str | None,
    # resolution modes
    entity_type: str,
    rule_file: str | None,
    decisions_path: str | None,
    rescore_after_apply: bool,
    decisions_validate_only: bool,
    lifecycle_op: str | None,
    canonical_threshold_pct: float,
    source_threshold_pct: float,
) -> None:
    """Unified Regattaman ingestion CLI."""
    run_id = run_id or str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat()
    counters = RunCounters()
    rejects = RejectWriter(Path(rejects_path))

    click.echo(f"[{run_id}] Starting {mode} run (dry_run={dry_run})")

    if mode == "bhyc_member_directory":
        _validate_bhyc_member_directory_flags(start_url, run_id)
        from regatta_etl.import_bhyc_member_directory import (
            BhycRunCounters,
            Checkpoint,
            GcsArchiver,
            LocalArchiver,
            NullArchiver,
            RateLimiter,
            build_bhyc_report,
            run_bhyc_member_directory,
        )
        # Read credentials from env — never from CLI args
        username = os.environ.get(member_user_env, "")
        password = os.environ.get(member_pass_env, "")
        if not username or not password:
            click.echo(
                f"[{run_id}] FATAL: env vars {member_user_env} and {member_pass_env} must be set",
                err=True,
            )
            sys.exit(1)

        # Archiver selection
        if archive_local_dir:
            archiver = LocalArchiver(base_dir=Path(archive_local_dir))
        elif gcs_bucket and gcs_prefix:
            archiver = GcsArchiver(bucket_name=gcs_bucket, prefix=gcs_prefix)
        else:
            click.echo(
                f"[{run_id}] ERROR: no archiver configured; "
                "provide --gcs-bucket + --gcs-prefix or --archive-local-dir.",
                err=True,
            )
            sys.exit(1)

        # Checkpoint
        cp_path = Path(checkpoint_path) if checkpoint_path else Path(f"./artifacts/checkpoints/bhyc_{run_id}.json")
        checkpoint = Checkpoint(cp_path)
        if resume_from_checkpoint:
            checkpoint.load()
            click.echo(f"[{run_id}] Checkpoint loaded: {len(checkpoint)} completed member_ids")

        bhyc_counters = BhycRunCounters()
        rate_limiter = RateLimiter(
            base_delay=request_delay_seconds,
            jitter=request_jitter_seconds,
            max_consecutive_failures=max_consecutive_failures,
        )

        click.echo(f"[{run_id}] bhyc_member_directory start_url={start_url} dry_run={dry_run}")
        run_bhyc_member_directory(
            run_id=run_id,
            db_dsn=db_dsn,
            start_url=start_url,  # type: ignore[arg-type]
            username=username,
            password=password,
            archiver=archiver,
            gcs_bucket=gcs_bucket,
            checkpoint=checkpoint,
            rate_limiter=rate_limiter,
            counters=bhyc_counters,
            max_pages=max_pages,
            dry_run=dry_run,
            timeout=30,
        )

        report = build_bhyc_report(bhyc_counters, dry_run=dry_run)
        click.echo(report)

        report_path = write_run_report(
            run_id, started_at, mode, dry_run,
            {"start_url": start_url, "checkpoint_path": str(cp_path)},
            bhyc_counters,  # type: ignore[arg-type]
        )
        click.echo(f"[{run_id}] Run report: {report_path}")

        if bhyc_counters.db_errors > 0 and not dry_run:
            click.echo(
                f"[{run_id}] {bhyc_counters.db_errors} DB errors — exiting non-zero",
                err=True,
            )
            sys.exit(1)
        if bhyc_counters.safe_stop_reason:
            click.echo(
                f"[{run_id}] Safe stop: {bhyc_counters.safe_stop_reason}",
                err=True,
            )
            sys.exit(1)
        return
    elif mode == "yacht_scoring":
        _validate_yacht_scoring_flags(yacht_scoring_dir, run_id)
        from regatta_etl.import_yacht_scoring import _run_yacht_scoring
        _run_yacht_scoring(
            run_id, started_at, db_dsn, counters, rejects,
            yacht_scoring_dir=yacht_scoring_dir,  # type: ignore[arg-type]
            max_reject_rate=max_reject_rate,
            dry_run=dry_run,
        )
    elif mode == "airtable_copy":
        _validate_airtable_copy_flags(airtable_dir, run_id)
        from regatta_etl.import_airtable_copy import _run_airtable_copy
        _run_airtable_copy(
            run_id, started_at, db_dsn, counters, rejects,
            airtable_dir=airtable_dir,  # type: ignore[arg-type]
            dry_run=dry_run,
        )
    elif mode == "private_export":
        _validate_private_export_flags(
            csv_path, host_club_name, host_club_normalized,
            event_series_name, event_series_normalized,
            event_display_name, season_year,
            registration_source, source_system, asset_type,
            run_id,
        )
        _run_private_export(
            run_id, started_at, db_dsn, counters, rejects,
            csv_path=csv_path,  # type: ignore[arg-type]
            host_club_name=host_club_name,  # type: ignore[arg-type]
            host_club_normalized=host_club_normalized,  # type: ignore[arg-type]
            event_series_name=event_series_name,  # type: ignore[arg-type]
            event_series_normalized=event_series_normalized,  # type: ignore[arg-type]
            event_display_name=event_display_name,  # type: ignore[arg-type]
            season_year=season_year,  # type: ignore[arg-type]
            registration_source=registration_source,  # type: ignore[arg-type]
            source_system=source_system,  # type: ignore[arg-type]
            asset_type=asset_type,  # type: ignore[arg-type]
            gcs_bucket=gcs_bucket,
            gcs_object=gcs_object,
            dry_run=dry_run,
        )
    elif mode == "mailchimp_audience":
        _validate_mailchimp_audience_flags(
            subscribed_path, unsubscribed_path, cleaned_path, run_id
        )
        from regatta_etl.import_mailchimp_audience import _run_mailchimp_audience
        _run_mailchimp_audience(
            run_id, started_at, db_dsn, counters, rejects,
            subscribed_path=subscribed_path,  # type: ignore[arg-type]
            unsubscribed_path=unsubscribed_path,  # type: ignore[arg-type]
            cleaned_path=cleaned_path,  # type: ignore[arg-type]
            max_reject_rate=max_reject_rate,
            dry_run=dry_run,
        )
    elif mode == "jotform_waiver":
        from regatta_etl.import_jotform_waiver import _run_jotform_waiver
        _validate_jotform_waiver_flags(
            csv_path,
            host_club_name,
            host_club_normalized,
            event_series_name,
            event_series_normalized,
            event_display_name,
            season_year,
            run_id
        )
        _run_jotform_waiver(
            run_id, started_at, db_dsn, counters, rejects,
            csv_path=csv_path,  # type: ignore[arg-type]
            host_club_name=host_club_name,  # type: ignore[arg-type]
            host_club_normalized=host_club_normalized,  # type: ignore[arg-type]
            event_series_name=event_series_name,  # type: ignore[arg-type]
            event_series_normalized=event_series_normalized,  # type: ignore[arg-type]
            event_display_name=event_display_name,  # type: ignore[arg-type]
            season_year=season_year,  # type: ignore[arg-type]
            event_unresolved_link_max_reject_rate=event_unresolved_link_max_reject_rate,
            dry_run=dry_run,
        )
    elif mode == "resolution_source_to_candidate":
        from regatta_etl.resolution_source_to_candidate import (
            _SKIPPED_TABLES,
            build_pipeline_report,
            run_source_to_candidate,
        )
        click.echo(f"[{run_id}] resolution_source_to_candidate entity_type={entity_type}")
        conn = psycopg.connect(db_dsn, autocommit=False)
        try:
            ctrs = run_source_to_candidate(conn, entity_type=entity_type, dry_run=dry_run)
            report = build_pipeline_report(ctrs, _SKIPPED_TABLES, dry_run=dry_run)
            click.echo(report)
            # Check errors BEFORE committing — rollback on any DB error
            if dry_run or ctrs.db_errors > 0:
                conn.rollback()
                if dry_run:
                    click.echo(f"[{run_id}] DRY RUN — rolled back.")
                else:
                    click.echo(
                        f"[{run_id}] {ctrs.db_errors} DB errors — rolled back.",
                        err=True,
                    )
                    sys.exit(1)
            else:
                conn.commit()
                click.echo(f"[{run_id}] Committed.")
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        # Write JSON run report (parity with other modes)
        report_path = write_run_report(
            run_id, started_at, mode, dry_run,
            {"entity_type": entity_type},
            ctrs,  # type: ignore[arg-type]  # SourceToCandidateCounters.to_dict() is compatible
        )
        click.echo(f"[{run_id}] Run report: {report_path}")
        return  # prevent fallthrough to the generic write_run_report below
    elif mode == "resolution_score":
        from regatta_etl.resolution_score import build_score_report, run_score
        click.echo(f"[{run_id}] resolution_score entity_type={entity_type}")
        rule_path = Path(rule_file) if rule_file else None
        conn = psycopg.connect(db_dsn, autocommit=False)
        try:
            ctrs = run_score(conn, entity_type=entity_type, rule_file=rule_path, dry_run=dry_run)
            report = build_score_report(ctrs, dry_run=dry_run)
            click.echo(report)
            if dry_run or ctrs.db_errors > 0:
                if not conn.closed:
                    conn.rollback()
                if dry_run:
                    click.echo(f"[{run_id}] DRY RUN — rolled back.")
                else:
                    if conn.closed:
                        click.echo(
                            f"[{run_id}] DB connection was lost before rollback; "
                            "treating run as failed.",
                            err=True,
                        )
                    click.echo(
                        f"[{run_id}] {ctrs.db_errors} DB errors — rolled back.",
                        err=True,
                    )
                    sys.exit(1)
            else:
                conn.commit()
                click.echo(f"[{run_id}] Committed.")
        except Exception:
            if not conn.closed:
                conn.rollback()
            raise
        finally:
            if not conn.closed:
                conn.close()
        report_path = write_run_report(
            run_id, started_at, mode, dry_run,
            {"entity_type": entity_type, "rule_file": rule_file},
            ctrs,  # type: ignore[arg-type]
        )
        click.echo(f"[{run_id}] Run report: {report_path}")
        return
    elif mode == "resolution_promote":
        from regatta_etl.resolution_promote import build_promote_report, run_promote
        click.echo(f"[{run_id}] resolution_promote entity_type={entity_type}")
        conn = psycopg.connect(db_dsn, autocommit=False)
        try:
            ctrs = run_promote(conn, entity_type=entity_type, dry_run=dry_run)
            report = build_promote_report(ctrs, dry_run=dry_run)
            click.echo(report)
            if dry_run or ctrs.db_errors > 0:
                conn.rollback()
                if dry_run:
                    click.echo(f"[{run_id}] DRY RUN — rolled back.")
                else:
                    click.echo(
                        f"[{run_id}] {ctrs.db_errors} DB errors — rolled back.",
                        err=True,
                    )
                    sys.exit(1)
            else:
                conn.commit()
                click.echo(f"[{run_id}] Committed.")
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        report_path = write_run_report(
            run_id, started_at, mode, dry_run,
            {"entity_type": entity_type},
            ctrs,  # type: ignore[arg-type]
        )
        click.echo(f"[{run_id}] Run report: {report_path}")
        return
    elif mode == "resolution_manual_apply":
        _validate_resolution_manual_apply_flags(decisions_path, run_id)
        from regatta_etl.resolution_manual_apply import (
            build_manual_apply_report,
            run_manual_apply,
        )
        click.echo(f"[{run_id}] resolution_manual_apply decisions_path={decisions_path}")
        if decisions_validate_only:
            # validate_only = pure CSV parsing; no DB connection needed.
            click.echo(f"[{run_id}] validate_only=True — skipping DB connection.")
            ctrs = run_manual_apply(
                None,  # type: ignore[arg-type]
                decisions_path=decisions_path,  # type: ignore[arg-type]
                rescore_after_apply=False,
                dry_run=False,
                validate_only=True,
            )
            report = build_manual_apply_report(ctrs, dry_run=False)
            click.echo(report)
        else:
            conn = psycopg.connect(db_dsn, autocommit=False)
            try:
                ctrs = run_manual_apply(
                    conn,
                    decisions_path=decisions_path,  # type: ignore[arg-type]
                    rescore_after_apply=rescore_after_apply,
                    dry_run=dry_run,
                    validate_only=False,
                )
                report = build_manual_apply_report(ctrs, dry_run=dry_run)
                click.echo(report)
                if dry_run or ctrs.db_errors > 0:
                    conn.rollback()
                    if dry_run:
                        click.echo(f"[{run_id}] DRY RUN — rolled back.")
                    else:
                        click.echo(
                            f"[{run_id}] {ctrs.db_errors} DB errors — rolled back.",
                            err=True,
                        )
                        sys.exit(1)
                else:
                    conn.commit()
                    click.echo(f"[{run_id}] Committed.")
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        report_path = write_run_report(
            run_id, started_at, mode, dry_run,
            {"decisions_path": decisions_path, "rescore_after_apply": rescore_after_apply},
            ctrs,  # type: ignore[arg-type]
        )
        click.echo(f"[{run_id}] Run report: {report_path}")
        return
    elif mode == "resolution_lifecycle":
        _validate_lifecycle_flags(lifecycle_op, decisions_path, run_id)
        from regatta_etl.resolution_lifecycle import build_lifecycle_report, run_lifecycle
        click.echo(
            f"[{run_id}] resolution_lifecycle lifecycle_op={lifecycle_op} "
            f"decisions_path={decisions_path}"
        )
        conn = psycopg.connect(db_dsn, autocommit=False)
        try:
            ctrs = run_lifecycle(
                conn,
                decisions_path=decisions_path,  # type: ignore[arg-type]
                lifecycle_op=lifecycle_op,  # type: ignore[arg-type]
                dry_run=dry_run,
            )
            report = build_lifecycle_report(ctrs, dry_run=dry_run)
            click.echo(report)
            if dry_run or ctrs.db_errors > 0:
                conn.rollback()
                if dry_run:
                    click.echo(f"[{run_id}] DRY RUN — rolled back.")
                else:
                    click.echo(
                        f"[{run_id}] {ctrs.db_errors} DB errors — rolled back.",
                        err=True,
                    )
                    sys.exit(1)
            else:
                conn.commit()
                click.echo(f"[{run_id}] Committed.")
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        report_path = write_run_report(
            run_id, started_at, mode, dry_run,
            {"lifecycle_op": lifecycle_op, "decisions_path": decisions_path},
            ctrs,  # type: ignore[arg-type]
        )
        click.echo(f"[{run_id}] Run report: {report_path}")
        return
    elif mode == "lineage_report":
        from regatta_etl.resolution_lineage import build_lineage_report, run_lineage_report
        click.echo(
            f"[{run_id}] lineage_report entity_type={entity_type} "
            f"canonical_threshold_pct={canonical_threshold_pct} "
            f"source_threshold_pct={source_threshold_pct}"
        )
        conn = psycopg.connect(db_dsn, autocommit=False)
        try:
            results = run_lineage_report(
                conn,
                entity_type=entity_type,
                canonical_threshold_pct=canonical_threshold_pct,
                source_threshold_pct=source_threshold_pct,
                dry_run=dry_run,
            )
            report = build_lineage_report(results, dry_run=dry_run)
            click.echo(report)
            if dry_run:
                conn.rollback()
                click.echo(f"[{run_id}] DRY RUN — rolled back.")
            else:
                conn.commit()
                click.echo(f"[{run_id}] Committed.")
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return
    elif mode == "purge_check":
        from regatta_etl.resolution_lineage import build_lineage_report, run_lineage_report
        click.echo(
            f"[{run_id}] purge_check entity_type={entity_type} "
            f"canonical_threshold_pct={canonical_threshold_pct} "
            f"source_threshold_pct={source_threshold_pct}"
        )
        conn = psycopg.connect(db_dsn, autocommit=False)
        try:
            results = run_lineage_report(
                conn,
                entity_type=entity_type,
                canonical_threshold_pct=canonical_threshold_pct,
                source_threshold_pct=source_threshold_pct,
                dry_run=False,
            )
            report = build_lineage_report(results, dry_run=False)
            click.echo(report)
            conn.commit()
            click.echo(f"[{run_id}] Committed snapshots.")
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        all_passed = all(r.thresholds_passed for r in results)
        if not all_passed:
            click.echo(f"[{run_id}] FAIL — thresholds not met.", err=True)
            sys.exit(1)
        click.echo(f"[{run_id}] PASS — all thresholds met.")
        return
    else:
        _validate_public_scrape_flags(entries_path, events_path, run_id)
        from regatta_etl.public_scrape import run_public_scrape
        conn = psycopg.connect(db_dsn, autocommit=False)
        try:
            run_public_scrape(
                conn=conn,
                entries_path=Path(entries_path),  # type: ignore[arg-type]
                events_path=Path(events_path),  # type: ignore[arg-type]
                source_system="regattaman_public_scrape_csv",
                run_id=run_id,
                counters=counters,
                rejects=rejects,
                dry_run=dry_run,
                max_reject_rate=max_reject_rate,
                synthesize_events=synthesize_events,
            )
        finally:
            conn.close()
            rejects.close()

        if counters.db_phase_errors > 0:
            report_path = write_run_report(
                run_id, started_at, mode, dry_run,
                {"csv_path": csv_path, "entries_path": entries_path, "events_path": events_path},
                counters,
            )
            click.echo(f"[{run_id}] Run report: {report_path}")
            click.echo(json.dumps(counters.to_dict(), indent=2, default=str))
            click.echo(
                f"[{run_id}] Run completed with {counters.db_phase_errors} DB error(s) — exiting non-zero",
                err=True,
            )
            sys.exit(1)

    report_path = write_run_report(
        run_id, started_at, mode, dry_run,
        {
            "csv_path": csv_path,
            "entries_path": entries_path,
            "events_path": events_path,
            "airtable_dir": airtable_dir,
            "yacht_scoring_dir": yacht_scoring_dir,
        },
        counters,
    )
    click.echo(f"[{run_id}] Run report: {report_path}")
    click.echo(json.dumps(counters.to_dict(), indent=2, default=str))
    click.echo(f"[{run_id}] Done.")


def _validate_private_export_flags(
    csv_path: str | None,
    host_club_name: str | None,
    host_club_normalized: str | None,
    event_series_name: str | None,
    event_series_normalized: str | None,
    event_display_name: str | None,
    season_year: int | None,
    registration_source: str | None,
    source_system: str | None,
    asset_type: str | None,
    run_id: str,
) -> None:
    required = {
        "--csv-path": csv_path,
        "--host-club-name": host_club_name,
        "--host-club-normalized": host_club_normalized,
        "--event-series-name": event_series_name,
        "--event-series-normalized": event_series_normalized,
        "--event-display-name": event_display_name,
        "--season-year": season_year,
        "--registration-source": registration_source,
        "--source-system": source_system,
        "--asset-type": asset_type,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        click.echo(
            f"[{run_id}] FATAL: private_export mode requires: {', '.join(missing)}",
            err=True,
        )
        sys.exit(1)


def _validate_jotform_waiver_flags(
    csv_path: str | None,
    host_club_name: str | None,
    host_club_normalized: str | None,
    event_series_name: str | None,
    event_series_normalized: str | None,
    event_display_name: str | None,
    season_year: int | None,
    run_id: str,
) -> None:
    required = {
        "--csv-path": csv_path,
        "--host-club-name": host_club_name,
        "--host-club-normalized": host_club_normalized,
        "--event-series-name": event_series_name,
        "--event-series-normalized": event_series_normalized,
        "--event-display_name": event_display_name,
        "--season-year": season_year,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        click.echo(
            f"[{run_id}] FATAL: jotform_waiver mode requires: {', '.join(missing)}",
            err=True,
        )
        sys.exit(1)


def _validate_public_scrape_flags(
    entries_path: str | None,
    events_path: str | None,
    run_id: str,
) -> None:
    required = {"--entries-path": entries_path, "--events-path": events_path}
    missing = [k for k, v in required.items() if v is None]
    if missing:
        click.echo(
            f"[{run_id}] FATAL: public_scrape mode requires: {', '.join(missing)}",
            err=True,
        )
        sys.exit(1)


def _validate_mailchimp_audience_flags(
    subscribed_path: str | None,
    unsubscribed_path: str | None,
    cleaned_path: str | None,
    run_id: str,
) -> None:
    required = {
        "--subscribed-path": subscribed_path,
        "--unsubscribed-path": unsubscribed_path,
        "--cleaned-path": cleaned_path,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        click.echo(
            f"[{run_id}] FATAL: mailchimp_audience mode requires: {', '.join(missing)}",
            err=True,
        )
        sys.exit(1)


def _validate_airtable_copy_flags(
    airtable_dir: str | None,
    run_id: str,
) -> None:
    if not airtable_dir:
        click.echo(
            f"[{run_id}] FATAL: airtable_copy mode requires: --airtable-dir",
            err=True,
        )
        sys.exit(1)


def _validate_yacht_scoring_flags(
    yacht_scoring_dir: str | None,
    run_id: str,
) -> None:
    if not yacht_scoring_dir:
        click.echo(
            f"[{run_id}] FATAL: yacht_scoring mode requires: --yacht-scoring-dir",
            err=True,
        )
        sys.exit(1)


def _validate_resolution_manual_apply_flags(
    decisions_path: str | None,
    run_id: str,
) -> None:
    if not decisions_path:
        click.echo(
            f"[{run_id}] FATAL: resolution_manual_apply mode requires: --decisions-path",
            err=True,
        )
        sys.exit(1)
    from pathlib import Path as _Path
    if not _Path(decisions_path).exists():
        click.echo(
            f"[{run_id}] FATAL: --decisions-path not found: {decisions_path}",
            err=True,
        )
        sys.exit(1)


def _validate_bhyc_member_directory_flags(
    start_url: str | None,
    run_id: str,
) -> None:
    if not start_url:
        click.echo(
            f"[{run_id}] FATAL: bhyc_member_directory mode requires: --start-url",
            err=True,
        )
        sys.exit(1)


def _validate_lifecycle_flags(
    lifecycle_op: str | None,
    decisions_path: str | None,
    run_id: str,
) -> None:
    if not lifecycle_op:
        click.echo(
            f"[{run_id}] FATAL: resolution_lifecycle mode requires: --lifecycle-op",
            err=True,
        )
        sys.exit(1)
    if not decisions_path:
        click.echo(
            f"[{run_id}] FATAL: resolution_lifecycle mode requires: --decisions-path",
            err=True,
        )
        sys.exit(1)
    from pathlib import Path as _Path
    if not _Path(decisions_path).exists():
        click.echo(
            f"[{run_id}] FATAL: --decisions-path not found: {decisions_path}",
            err=True,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Private export pipeline
# ---------------------------------------------------------------------------

def _run_private_export(
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
    registration_source: str,
    source_system: str,
    asset_type: str,
    gcs_bucket: str | None,
    gcs_object: str | None,
    dry_run: bool,
) -> None:
    # Phase 1: Pre-scan
    csv_file = Path(csv_path)
    raw_rows: list[dict[str, str]] = []

    with csv_file.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        raw_fieldnames = reader.fieldnames or []
        normalized_hdr = {k.strip(): k for k in raw_fieldnames}

        missing = REQUIRED_HEADERS - set(normalized_hdr.keys())
        if missing:
            click.echo(
                f"[{run_id}] FATAL: missing headers after trim: {sorted(missing)}",
                err=True,
            )
            sys.exit(1)

        for raw_row in reader:
            row = normalize_headers(raw_row)
            counters.rows_read += 1
            sku = trim(row.get("sku"))
            ownername = trim(row.get("ownername"))
            yacht_name = trim(row.get("Yacht Name"))

            if not sku:
                rejects.write(row, "blank_sku")
                counters.rows_rejected += 1
                continue
            if not ownername:
                rejects.write(row, "blank_ownername")
                counters.rows_rejected += 1
                continue
            if not yacht_name:
                rejects.write(row, "blank_yacht_name")
                counters.rows_rejected += 1
                continue

            raw_rows.append(row)

    click.echo(
        f"[{run_id}] Pre-scan: {counters.rows_read} rows read, "
        f"{counters.rows_rejected} rejected, {len(raw_rows)} valid"
    )

    registration_open_at: datetime | None = None
    for row in raw_rows:
        ts = parse_ts(row.get("date_entered"))
        if ts is not None:
            if registration_open_at is None or ts < registration_open_at:
                registration_open_at = ts

    # Phase 2: DB
    conn = psycopg.connect(db_dsn, autocommit=False)
    had_row_errors = False
    try:
        if dry_run:
            _run_dry(
                conn, raw_rows, run_id, counters, rejects,
                host_club_name, host_club_normalized,
                event_series_name, event_series_normalized,
                event_display_name, season_year, registration_open_at,
                registration_source, source_system, asset_type,
                gcs_bucket, gcs_object, csv_file,
            )
            had_row_errors = counters.db_phase_errors > 0
        else:
            _run_real(
                conn, raw_rows, run_id, counters, rejects,
                host_club_name, host_club_normalized,
                event_series_name, event_series_normalized,
                event_display_name, season_year, registration_open_at,
                registration_source, source_system, asset_type,
                gcs_bucket, gcs_object, csv_file,
            )
    finally:
        conn.close()
        rejects.close()

    if had_row_errors and dry_run:
        click.echo(
            f"[{run_id}] Dry-run completed with row-level errors — exiting non-zero",
            err=True,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Real run (private_export)
# ---------------------------------------------------------------------------

def _run_real(
    conn: psycopg.Connection,
    raw_rows: list[dict[str, str]],
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
    host_club_name: str,
    host_club_normalized: str,
    event_series_name: str,
    event_series_normalized: str,
    event_display_name: str,
    season_year: int,
    registration_open_at: datetime | None,
    registration_source: str,
    source_system: str,
    asset_type: str,
    gcs_bucket: str | None,
    gcs_object: str | None,
    csv_file: Path,
) -> None:
    with conn.transaction():
        _, _, event_instance_id = upsert_event_context(
            conn,
            host_club_name, host_club_normalized,
            event_series_name, event_series_normalized,
            event_display_name, season_year, registration_open_at,
        )
    click.echo(f"[{run_id}] Event context ready: instance_id={event_instance_id}")

    for row in raw_rows:
        try:
            with conn.transaction():
                _process_row(
                    conn, row, event_instance_id,
                    registration_source, source_system, run_id, counters,
                )
        except AmbiguousMatchError as exc:
            rejects.write(row, str(exc))
            counters.rows_rejected += 1
            counters.db_phase_errors += 1

    if gcs_bucket and gcs_object:
        content_hash = hashlib.sha256(csv_file.read_bytes()).hexdigest()
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO raw_asset
                  (source_system, asset_type, gcs_bucket, gcs_object,
                   content_hash, captured_at, retention_delete_after)
                VALUES (%s, %s, %s, %s, %s, now(),
                        current_date + interval '3 years')
                ON CONFLICT (gcs_bucket, gcs_object) DO NOTHING
                """,
                (source_system, asset_type, gcs_bucket, gcs_object, content_hash),
            )


# ---------------------------------------------------------------------------
# Dry run (private_export)
# ---------------------------------------------------------------------------

def _run_dry(
    conn: psycopg.Connection,
    raw_rows: list[dict[str, str]],
    run_id: str,
    counters: RunCounters,
    rejects: RejectWriter,
    host_club_name: str,
    host_club_normalized: str,
    event_series_name: str,
    event_series_normalized: str,
    event_display_name: str,
    season_year: int,
    registration_open_at: datetime | None,
    registration_source: str,
    source_system: str,
    asset_type: str,
    gcs_bucket: str | None,
    gcs_object: str | None,
    csv_file: Path,
) -> None:
    conn.autocommit = False

    try:
        try:
            _, _, event_instance_id = upsert_event_context(
                conn,
                host_club_name, host_club_normalized,
                event_series_name, event_series_normalized,
                event_display_name, season_year, registration_open_at,
            )
            click.echo(
                f"[{run_id}] [dry-run] Event context valid: instance_id={event_instance_id}"
            )
        except Exception as exc:
            click.echo(
                f"[{run_id}] [dry-run] FATAL: event context failed: {exc}", err=True
            )
            raise

        for idx, row in enumerate(raw_rows):
            sp_name = f"row_{idx}"
            conn.execute(f"SAVEPOINT {sp_name}")
            try:
                _process_row(
                    conn, row, event_instance_id,
                    registration_source, source_system, run_id, counters,
                )
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            except AmbiguousMatchError as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                rejects.write(row, str(exc))
                counters.rows_rejected += 1
                counters.db_phase_errors += 1
                counters.warnings.append(f"[dry-run] row {idx}: {exc}")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                counters.warnings.append(f"[dry-run] row {idx} error: {exc}")
                rejects.write(row, f"db_error: {exc}")
                counters.rows_rejected += 1
                counters.db_phase_errors += 1

        if gcs_bucket and gcs_object:
            content_hash = hashlib.sha256(csv_file.read_bytes()).hexdigest()
            sp_name = "raw_asset"
            conn.execute(f"SAVEPOINT {sp_name}")
            try:
                conn.execute(
                    """
                    INSERT INTO raw_asset
                      (source_system, asset_type, gcs_bucket, gcs_object,
                       content_hash, captured_at, retention_delete_after)
                    VALUES (%s, %s, %s, %s, %s, now(),
                            current_date + interval '3 years')
                    ON CONFLICT (gcs_bucket, gcs_object) DO NOTHING
                    """,
                    (source_system, asset_type, gcs_bucket, gcs_object, content_hash),
                )
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                counters.warnings.append(f"[dry-run] raw_asset error: {exc}")
                counters.db_phase_errors += 1

    finally:
        conn.rollback()
        click.echo(f"[{run_id}] [dry-run] All changes rolled back.")


if __name__ == "__main__":
    main()
