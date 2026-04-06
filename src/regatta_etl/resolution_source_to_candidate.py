"""regatta_etl.resolution_source_to_candidate

Source-to-Candidate pipeline (--mode resolution_source_to_candidate).

Reads every relevant table in the database and builds or enriches
candidate_* records, linking each source row to its candidate via
candidate_source_link.

Design principles:
  - Idempotent: re-running produces the same candidates (fingerprint-based upsert).
  - Fill-nulls-only: never overwrites an existing non-null candidate attribute.
  - No candidate rows are deleted by this pipeline.
  - Every source row is either linked to a candidate or logged as intentionally
    skipped with a reason code.

Entity processing order (dependency order):
  1. clubs       — no upstream candidate dependencies
  2. events      — no upstream candidate dependencies
  3. yachts      — no upstream candidate dependencies
  4. participants — no upstream candidate dependencies
  5. registrations — needs candidate_event + candidate_yacht + candidate_participant

Source tables ingested per entity type:
  clubs        : yacht_club
  events       : event_instance (+ event_series join)
  yachts       : yacht
  participants : participant (+ participant_contact_point),
                 jotform_waiver_submission (raw_payload),
                 mailchimp_audience_row (raw_payload),
                 airtable_copy_row[participants, owners],
                 yacht_scoring_raw_row[deduplicated_entry, scraped_entry_listing],
                 participant_related_contact (emergency/guardian contacts)
  registrations: event_entry,
                 airtable_copy_row[entries],
                 yacht_scoring_raw_row[scraped_entry_listing, deduplicated_entry]

Tables intentionally skipped (logged in pipeline report):
  event_series               — entities captured via event_instance join
  participant_contact_point  — used inline for participant best_email enrichment
  participant_address        — used inline for candidate_participant_address child rows
  club_membership            — relationship table; participant + club already ingested
  yacht_ownership            — used inline for role_assignment child rows
  yacht_rating               — yacht-attribute supplemental; not an entity
  document_type/requirement/status — meta/tracking tables
  identity_candidate_match   — resolution ops (pre-candidate system)
  identity_merge_action      — resolution ops
  next_best_action           — resolution ops
  raw_asset                  — GCS references only
  airtable_xref_*            — lookup/xref tables
  yacht_scoring_xref_*       — lookup/xref tables
  mailchimp_contact_state    — used inline as Mailchimp resolution anchor (participant path)
  mailchimp_contact_tag      — supplemental to mailchimp_audience_row
"""

from __future__ import annotations

from collections import defaultdict
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg import pq

from regatta_etl.normalize import (
    build_person_display_name,
    is_likely_org_name,
    looks_like_email,
    normalize_email,
    normalize_name,
    normalize_person_name_for_identity,
    normalize_phone,
    parse_race_url,
    slug_name,
    trim,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Tables intentionally skipped: (table_name, reason_code)
_SKIPPED_TABLES: list[tuple[str, str]] = [
    ("event_series",               "captured_via_event_instance_join"),
    ("participant_contact_point",  "used_inline_for_participant_enrichment"),
    ("participant_address",        "used_inline_for_candidate_address_child"),
    ("club_membership",            "relationship_table_entities_captured_separately"),
    ("yacht_ownership",            "used_inline_for_role_assignment_child"),
    ("yacht_rating",               "yacht_attribute_supplemental_not_an_entity"),
    ("document_type",              "meta_tracking_table"),
    ("document_requirement",       "meta_tracking_table"),
    ("document_status",            "meta_tracking_table"),
    ("identity_candidate_match",   "resolution_ops_pre_candidate_system"),
    ("identity_merge_action",      "resolution_ops_pre_candidate_system"),
    ("next_best_action",           "resolution_ops_pre_candidate_system"),
    ("raw_asset",                  "gcs_reference_only"),
    ("airtable_xref_participant",  "lookup_xref_table"),
    ("airtable_xref_yacht",        "lookup_xref_table"),
    ("airtable_xref_club",         "lookup_xref_table"),
    ("airtable_xref_event",        "lookup_xref_table"),
    ("yacht_scoring_xref_event",   "lookup_xref_table"),
    ("yacht_scoring_xref_entry",   "lookup_xref_table"),
    ("yacht_scoring_xref_yacht",   "lookup_xref_table"),
    ("yacht_scoring_xref_participant", "lookup_xref_table"),
    ("mailchimp_contact_state",    "used_inline_as_mailchimp_resolution_anchor"),
    ("mailchimp_contact_tag",      "supplemental_to_mailchimp_audience_row"),
    ("bhyc_member_raw_row",        "inline_candidate_linking_in_bhyc_pipeline"),
    ("bhyc_member_xref_participant", "lookup_xref_table"),
    ("bhyc_household_candidate_evidence", "bhyc_household_evidence_not_entity"),
    ("candidate_participant",      "is_candidate_layer_not_source"),
    ("candidate_yacht",            "is_candidate_layer_not_source"),
    ("candidate_club",             "is_candidate_layer_not_source"),
    ("candidate_event",            "is_candidate_layer_not_source"),
    ("candidate_registration",     "is_candidate_layer_not_source"),
    ("candidate_source_link",      "is_candidate_layer_not_source"),
    ("candidate_canonical_link",   "is_canonical_layer_not_source"),
    ("candidate_participant_contact",       "is_candidate_layer_not_source"),
    ("candidate_participant_address",       "is_candidate_layer_not_source"),
    ("candidate_participant_role_assignment","is_candidate_layer_not_source"),
    ("canonical_participant",      "is_canonical_layer_not_source"),
    ("canonical_yacht",            "is_canonical_layer_not_source"),
    ("canonical_club",             "is_canonical_layer_not_source"),
    ("canonical_event",            "is_canonical_layer_not_source"),
    ("canonical_registration",     "is_canonical_layer_not_source"),
    ("canonical_participant_contact",      "is_canonical_layer_not_source"),
    ("canonical_participant_address",      "is_canonical_layer_not_source"),
    ("canonical_participant_role_assignment","is_canonical_layer_not_source"),
    ("resolution_rule_set",        "governance_table"),
    ("resolution_score_run",       "governance_table"),
    ("resolution_manual_action_log","governance_table"),
]


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class SourceToCandidateCounters:
    # Clubs
    clubs_ingested: int = 0
    clubs_candidate_created: int = 0
    clubs_candidate_enriched: int = 0
    # Events
    events_ingested: int = 0
    events_candidate_created: int = 0
    events_candidate_enriched: int = 0
    # Yachts
    yachts_ingested: int = 0
    yachts_candidate_created: int = 0
    yachts_candidate_enriched: int = 0
    # Participants
    participants_ingested: int = 0
    participants_candidate_created: int = 0
    participants_candidate_enriched: int = 0
    participant_contacts_linked: int = 0
    participant_addresses_linked: int = 0
    participant_roles_linked: int = 0
    # Registrations
    registrations_ingested: int = 0
    registrations_candidate_created: int = 0
    registrations_candidate_enriched: int = 0
    # Source links
    source_links_inserted: int = 0
    source_links_skipped_duplicate: int = 0
    # Skipped rows (expected, not errors)
    rows_skipped_no_xref_link: int = 0    # raw row has no operational xref (pipeline not run)
    rows_skipped_no_owner_name: int = 0   # yacht_scoring entry row with no owner name
    # Under-combination prevention
    participants_under_combination_reused: int = 0
    participants_under_combination_ambiguous: int = 0
    # Errors
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if k != "warnings"}
        d["warnings"] = self.warnings
        return d


# ---------------------------------------------------------------------------
# Fingerprint helpers
# ---------------------------------------------------------------------------

def participant_fingerprint(normalized_name: str | None, best_email: str | None) -> str:
    """sha256(normalized_name|normalized_email_or_empty)"""
    key = f"{normalized_name or ''}|{(best_email or '').lower()}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def yacht_fingerprint(normalized_name: str | None, normalized_sail: str | None) -> str:
    """sha256(normalized_name|normalized_sail_or_empty)"""
    key = f"{normalized_name or ''}|{normalized_sail or ''}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def club_fingerprint(normalized_name: str | None) -> str:
    """sha256(normalized_name)"""
    return hashlib.sha256((normalized_name or "").encode("utf-8")).hexdigest()


def event_fingerprint(
    normalized_name: str | None,
    season_year: int | None,
    event_external_id: str | None,
) -> str:
    """sha256(normalized_name|season_year_or_empty|external_id_or_empty)"""
    key = f"{normalized_name or ''}|{season_year or ''}|{event_external_id or ''}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def registration_fingerprint(
    candidate_event_id: str,
    registration_external_id: str | None,
    candidate_yacht_id: str | None,
) -> str:
    """sha256(candidate_event_id|external_id_or_empty|candidate_yacht_id_or_empty)"""
    key = f"{candidate_event_id}|{registration_external_id or ''}|{candidate_yacht_id or ''}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Generic DB helpers
# ---------------------------------------------------------------------------

def _upsert_candidate(
    conn: psycopg.Connection,
    table: str,
    fingerprint: str,
    fields: dict[str, Any],
) -> tuple[str, bool]:
    """Insert or fill-nulls-only-update a candidate row by stable_fingerprint.

    On conflict (fingerprint already exists), COALESCE is used so only null
    columns in the existing row are filled from the incoming data.

    FOR-222: when table is 'candidate_participant', display_name is checked via
    is_likely_org_name(). If the name looks like an organization (yacht club, LLC,
    Coast Guard, etc.), resolution_state is forced to 'reject' on INSERT so the
    record never auto-promotes into a canonical person. Existing rows are not
    downgraded here (fill-nulls-only semantics); the backfill migration handles
    pre-existing rows.

    Returns:
        (uuid_str, was_inserted: bool)
    """
    if table == "candidate_participant" and is_likely_org_name(
        fields.get("display_name") or fields.get("normalized_name")
    ):
        fields = {**fields, "resolution_state": "reject"}

    cols = ["stable_fingerprint"] + list(fields.keys())
    vals = [fingerprint] + list(fields.values())
    placeholders = ", ".join(["%s"] * len(vals))
    col_list = ", ".join(cols)

    # Build fill-nulls-only UPDATE SET clause using COALESCE
    update_parts = [
        f"{col} = COALESCE({table}.{col}, EXCLUDED.{col})"
        for col in fields.keys()
        if col not in ("created_at",)
    ]
    update_parts.append("updated_at = now()")
    update_clause = ", ".join(update_parts)

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (stable_fingerprint) DO UPDATE
            SET {update_clause}
        RETURNING id, (xmax = 0) AS was_inserted
    """
    row = conn.execute(sql, vals).fetchone()
    return str(row[0]), bool(row[1])


def _link_source(
    conn: psycopg.Connection,
    entity_type: str,
    candidate_id: str,
    source_table: str,
    source_pk: str,
    source_system: str | None = None,
    source_row_hash: str | None = None,
    link_score: float = 1.0,
    link_reason: dict | None = None,
) -> bool:
    """Insert a candidate_source_link row; returns True if newly inserted."""
    result = conn.execute(
        """
        INSERT INTO candidate_source_link
            (candidate_entity_type, candidate_entity_id, source_table_name,
             source_row_pk, source_row_hash, source_system, link_score, link_reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (candidate_entity_type, source_table_name, source_row_pk)
        DO NOTHING
        RETURNING id
        """,
        (
            entity_type,
            candidate_id,
            source_table,
            source_pk,
            source_row_hash,
            source_system,
            link_score,
            json.dumps(link_reason or {}),
        ),
    ).fetchone()
    return result is not None


def _upsert_contact(
    conn: psycopg.Connection,
    candidate_participant_id: str,
    contact_type: str,
    raw_value: str,
    normalized_value: str | None,
    is_primary: bool,
    source_table: str,
    source_pk: str,
    contact_subtype: str | None = None,
) -> None:
    """Upsert a candidate_participant_contact row (idempotent via partial unique indexes).

    contact_subtype (e.g. 'mobile', 'home', 'work') is stored but is NOT part of the
    deduplication key — two phone numbers with identical normalized values but different
    subtypes are still the same contact point.  FOR-219: subtype was previously silently
    dropped; callers must now pass it for phone contacts.
    """
    # FOR-226: candidate-layer phone contacts must always be valid normalized
    # E.164-style values. Keep malformed/truncated raw numbers only in the
    # source-layer participant_contact_point table for provenance.
    if contact_type == "phone" and normalized_value is None:
        return

    if normalized_value is not None:
        # Conflict on (candidate_participant_id, contact_type, normalized_value)
        conn.execute(
            """
            INSERT INTO candidate_participant_contact
                (candidate_participant_id, contact_type, contact_subtype, raw_value,
                 normalized_value, is_primary, source_table_name, source_row_pk)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (candidate_participant_id, contact_type, normalized_value)
                WHERE normalized_value IS NOT NULL
            DO UPDATE SET
                contact_subtype = COALESCE(candidate_participant_contact.contact_subtype,
                                           EXCLUDED.contact_subtype),
                is_primary      = candidate_participant_contact.is_primary OR EXCLUDED.is_primary
            """,
            (
                candidate_participant_id,
                contact_type,
                contact_subtype,
                raw_value,
                normalized_value,
                is_primary,
                source_table,
                source_pk,
            ),
        )
    else:
        # Conflict on (candidate_participant_id, contact_type, raw_value)
        conn.execute(
            """
            INSERT INTO candidate_participant_contact
                (candidate_participant_id, contact_type, contact_subtype, raw_value,
                 normalized_value, is_primary, source_table_name, source_row_pk)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (candidate_participant_id, contact_type, raw_value)
                WHERE normalized_value IS NULL
            DO UPDATE SET
                contact_subtype = COALESCE(candidate_participant_contact.contact_subtype,
                                           EXCLUDED.contact_subtype),
                is_primary      = candidate_participant_contact.is_primary OR EXCLUDED.is_primary
            """,
            (
                candidate_participant_id,
                contact_type,
                contact_subtype,
                raw_value,
                None,
                is_primary,
                source_table,
                source_pk,
            ),
        )


def _upsert_address(
    conn: psycopg.Connection,
    candidate_participant_id: str,
    address_raw: str,
    source_table: str,
    source_pk: str,
    line1: str | None = None,
    city: str | None = None,
    state: str | None = None,
    postal_code: str | None = None,
    country_code: str | None = None,
    is_primary: bool = False,
) -> None:
    """Upsert a candidate_participant_address row (idempotent via unique index on address_raw)."""
    conn.execute(
        """
        INSERT INTO candidate_participant_address
            (candidate_participant_id, address_raw, line1, city, state,
             postal_code, country_code, is_primary, source_table_name, source_row_pk)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (candidate_participant_id, address_raw) DO NOTHING
        """,
        (
            candidate_participant_id,
            address_raw,
            line1, city, state, postal_code, country_code,
            is_primary,
            source_table,
            source_pk,
        ),
    )


def _upsert_role(
    conn: psycopg.Connection,
    candidate_participant_id: str,
    role: str,
    source_context: str | None = None,
    candidate_event_id: str | None = None,
    candidate_registration_id: str | None = None,
) -> None:
    """Insert a candidate_participant_role_assignment (skip exact duplicates)."""
    conn.execute(
        """
        INSERT INTO candidate_participant_role_assignment
            (candidate_participant_id, role, candidate_event_id,
             candidate_registration_id, source_context)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (
            candidate_participant_id,
            role,
            COALESCE(candidate_event_id::text, ''),
            COALESCE(candidate_registration_id::text, '')
        ) DO NOTHING
        """,
        (
            candidate_participant_id,
            role,
            candidate_event_id,
            candidate_registration_id,
            source_context,
        ),
    )


# ---------------------------------------------------------------------------
# Under-combination prevention helpers
# ---------------------------------------------------------------------------

_AMBIGUOUS = "ambiguous"


def _find_email_bearing_candidate_by_name(
    conn: psycopg.Connection,
    normalized_name: str,
) -> str | None:
    """Return the id of the unique email-bearing candidate_participant with this name.

    Returns:
        str UUID   — exactly one email-bearing candidate found; reuse it.
        _AMBIGUOUS — multiple email-bearing candidates found; do not guess.
        None       — zero email-bearing candidates found; use normal path.
    """
    rows = conn.execute(
        """
        SELECT id
        FROM candidate_participant
        WHERE normalized_name = %s
          AND best_email IS NOT NULL
        ORDER BY is_promoted DESC, quality_score DESC NULLS LAST, updated_at DESC, id ASC
        """,
        (normalized_name,),
    ).fetchall()

    if len(rows) == 1:
        return str(rows[0][0])
    if len(rows) > 1:
        return _AMBIGUOUS  # type: ignore[return-value]
    return None


def _enrich_candidate_by_id(
    conn: psycopg.Connection,
    table: str,
    candidate_id: str,
    fields: dict[str, Any],
) -> None:
    """Fill-nulls-only UPDATE for a candidate row we already hold the id for."""
    set_parts = [
        f"{col} = COALESCE({table}.{col}, %s)"
        for col in fields
        if col != "created_at"
    ]
    set_parts.append("updated_at = now()")
    sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE id = %s"
    vals = [v for col, v in fields.items() if col != "created_at"] + [candidate_id]
    conn.execute(sql, vals)


# ---------------------------------------------------------------------------
# Club ingestion
# ---------------------------------------------------------------------------

def _ingest_clubs_from_yacht_club(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest every yacht_club row → candidate_club."""
    rows = conn.execute(
        "SELECT id, name, normalized_name, website_url FROM yacht_club ORDER BY created_at"
    ).fetchall()

    for row in rows:
        pk, name, norm_name, website = str(row[0]), row[1], row[2], row[3]
        fp = club_fingerprint(norm_name)
        try:
            cid, created = _upsert_candidate(
                conn,
                "candidate_club",
                fp,
                {
                    "name": name,
                    "normalized_name": norm_name,
                    "website": website,
                },
            )
            if created:
                ctrs.clubs_candidate_created += 1
            else:
                ctrs.clubs_candidate_enriched += 1
            inserted = _link_source(conn, "club", cid, "yacht_club", pk, "operational_db")
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
            ctrs.clubs_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"clubs/yacht_club pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Event ingestion
# ---------------------------------------------------------------------------

def _ingest_events_from_event_instance(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest every event_instance row (joined with event_series) → candidate_event."""
    rows = conn.execute(
        """
        SELECT ei.id, ei.display_name, ei.season_year, ei.start_date, ei.end_date,
               es.normalized_name AS series_norm_name
        FROM event_instance ei
        JOIN event_series es ON es.id = ei.event_series_id
        ORDER BY ei.created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        display_name = row[1]
        season_year = row[2]
        start_date = row[3]
        end_date = row[4]
        series_norm = row[5]

        norm_event_name = normalize_name(display_name)
        fp = event_fingerprint(norm_event_name or series_norm, season_year, None)

        try:
            cid, created = _upsert_candidate(
                conn,
                "candidate_event",
                fp,
                {
                    "event_name": display_name,
                    "normalized_event_name": norm_event_name,
                    "season_year": season_year,
                    "start_date": str(start_date) if start_date else None,
                    "end_date": str(end_date) if end_date else None,
                },
            )
            if created:
                ctrs.events_candidate_created += 1
            else:
                ctrs.events_candidate_enriched += 1
            inserted = _link_source(conn, "event", cid, "event_instance", pk, "operational_db")
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
            ctrs.events_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"events/event_instance pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Yacht ingestion
# ---------------------------------------------------------------------------

def _ingest_yachts_from_yacht(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest every yacht row → candidate_yacht."""
    rows = conn.execute(
        """
        SELECT id, name, normalized_name, sail_number, normalized_sail_number,
               length_feet, model
        FROM yacht ORDER BY created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        name = row[1]
        norm_name = row[2]
        sail = row[3]
        norm_sail = row[4]
        length_feet = row[5]
        yacht_type = row[6]

        fp = yacht_fingerprint(norm_name, norm_sail)
        try:
            cid, created = _upsert_candidate(
                conn,
                "candidate_yacht",
                fp,
                {
                    "name": name,
                    "normalized_name": norm_name,
                    "sail_number": sail,
                    "normalized_sail_number": norm_sail,
                    "length_feet": float(length_feet) if length_feet is not None else None,
                    "yacht_type": yacht_type,
                },
            )
            if created:
                ctrs.yachts_candidate_created += 1
            else:
                ctrs.yachts_candidate_enriched += 1
            inserted = _link_source(conn, "yacht", cid, "yacht", pk, "operational_db")
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
            ctrs.yachts_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"yachts/yacht pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Participant ingestion — per source table
# ---------------------------------------------------------------------------

def _ingest_participants_from_participant_table(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest every participant row (+ best contact/address evidence).

    Live Cloud SQL runs pay a noticeable round-trip cost for each query. Loading
    child contact/address rows once and grouping them in memory avoids tens of
    thousands of per-participant lookups during a full participant rerun.
    """
    rows = conn.execute(
        """
        SELECT p.id, p.full_name, p.normalized_full_name, p.date_of_birth,
               p.first_name, p.last_name
        FROM participant p
        ORDER BY p.created_at
        """
    ).fetchall()

    contact_rows = conn.execute(
        """
        SELECT participant_id::text, id, contact_type, contact_subtype,
               contact_value_raw, contact_value_normalized, is_primary
        FROM participant_contact_point
        ORDER BY participant_id, is_primary DESC, created_at ASC, id ASC
        """
    ).fetchall()
    contacts_by_participant: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    best_contact_by_participant: dict[str, dict[str, str | None]] = defaultdict(dict)
    for contact_row in contact_rows:
        participant_id = str(contact_row[0])
        contacts_by_participant[participant_id].append(contact_row)
        contact_type = contact_row[2]
        if contact_type not in best_contact_by_participant[participant_id]:
            best_contact_by_participant[participant_id][contact_type] = contact_row[5]

    address_rows = conn.execute(
        """
        SELECT participant_id::text, id, address_raw, line1, city, state,
               postal_code, country_code, is_primary
        FROM participant_address
        ORDER BY participant_id, created_at ASC, id ASC
        """
    ).fetchall()
    addresses_by_participant: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for address_row in address_rows:
        addresses_by_participant[str(address_row[0])].append(address_row)

    for row in rows:
        pk = str(row[0])
        display_name = build_person_display_name(row[4], row[5])
        if display_name is None:
            raw_full_name = trim(row[1])
            if raw_full_name and "@" not in raw_full_name and not looks_like_email(raw_full_name):
                display_name = raw_full_name
        norm_name = normalize_person_name_for_identity(display_name)
        dob = row[3]
        best_email = best_contact_by_participant.get(pk, {}).get("email")
        best_phone = best_contact_by_participant.get(pk, {}).get("phone")

        fields = {
            "display_name": display_name,
            "normalized_name": norm_name,
            "date_of_birth": str(dob) if dob else None,
            "best_email": best_email,
            "best_phone": best_phone,
        }
        reuse_cid: str | None = None
        if best_email is None and norm_name:
            lookup = _find_email_bearing_candidate_by_name(conn, norm_name)
            if lookup == _AMBIGUOUS:
                ctrs.participants_under_combination_ambiguous += 1
                ctrs.warnings.append(
                    f"under_combination_ambiguous: participant pk={pk} norm_name={norm_name!r}"
                )
            elif lookup is not None:
                reuse_cid = lookup
                ctrs.participants_under_combination_reused += 1

        try:
            if reuse_cid is not None:
                cid = reuse_cid
                created = False
                _enrich_candidate_by_id(conn, "candidate_participant", cid, fields)
                ctrs.participants_candidate_enriched += 1
            else:
                fp = participant_fingerprint(norm_name, best_email)
                cid, created = _upsert_candidate(conn, "candidate_participant", fp, fields)
                if created:
                    ctrs.participants_candidate_created += 1
                else:
                    ctrs.participants_candidate_enriched += 1

            # Link source row
            inserted = _link_source(conn, "participant", cid, "participant", pk, "operational_db")
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1

            # Upsert contact child rows from participant_contact_point
            contacts = contacts_by_participant.get(pk, [])
            for c in contacts:
                _upsert_contact(
                    conn, cid,
                    contact_type=c[2],
                    raw_value=c[4],
                    normalized_value=c[5],
                    is_primary=bool(c[6]),
                    source_table="participant_contact_point",
                    source_pk=str(c[1]),
                    contact_subtype=c[3],  # FOR-219: was silently dropped
                )
                ctrs.participant_contacts_linked += 1

            # Upsert address child rows from participant_address
            addresses = addresses_by_participant.get(pk, [])
            for a in addresses:
                if a[2]:
                    _upsert_address(
                        conn, cid,
                        address_raw=a[2],
                        source_table="participant_address",
                        source_pk=str(a[1]),
                        line1=a[3], city=a[4], state=a[5],
                        postal_code=a[6], country_code=a[7],
                        is_primary=bool(a[8]),
                    )
                    ctrs.participant_addresses_linked += 1

            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"participants/participant pk={pk}: {exc}")


def _ingest_participants_from_jotform(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest jotform_waiver_submission rows → candidate_participant (submitter)."""
    rows = conn.execute(
        "SELECT id, raw_payload, row_hash, source_system FROM jotform_waiver_submission ORDER BY created_at"
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        payload: dict = row[1] if isinstance(row[1], dict) else json.loads(row[1])
        row_hash = row[2]
        source_system = row[3] or "jotform_csv_export"

        raw_name = trim(payload.get("Name") or payload.get("name", ""))
        raw_email = trim(payload.get("Competitor E mail") or payload.get("email", ""))

        if not raw_name:
            ctrs.warnings.append(f"jotform pk={pk}: skipped — missing name")
            continue

        norm_name = normalize_person_name_for_identity(raw_name)
        norm_email = normalize_email(raw_email) if raw_email else None

        jotform_fields = {
            "display_name": raw_name,
            "normalized_name": norm_name,
            "best_email": norm_email,
        }
        reuse_cid_jf: str | None = None
        if norm_email is None and norm_name:
            lookup = _find_email_bearing_candidate_by_name(conn, norm_name)
            if lookup == _AMBIGUOUS:
                ctrs.participants_under_combination_ambiguous += 1
                ctrs.warnings.append(
                    f"under_combination_ambiguous: jotform pk={pk} norm_name={norm_name!r}"
                )
            elif lookup is not None:
                reuse_cid_jf = lookup
                ctrs.participants_under_combination_reused += 1

        try:
            if reuse_cid_jf is not None:
                cid = reuse_cid_jf
                created = False
                _enrich_candidate_by_id(conn, "candidate_participant", cid, jotform_fields)
                ctrs.participants_candidate_enriched += 1
            else:
                fp = participant_fingerprint(norm_name, norm_email)
                cid, created = _upsert_candidate(
                    conn, "candidate_participant", fp, jotform_fields,
                )
                if created:
                    ctrs.participants_candidate_created += 1
                else:
                    ctrs.participants_candidate_enriched += 1

            inserted = _link_source(
                conn, "participant", cid,
                "jotform_waiver_submission", pk,
                source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1

            if norm_email:
                _upsert_contact(
                    conn, cid, "email", raw_email, norm_email, True,
                    "jotform_waiver_submission", pk,
                )
                ctrs.participant_contacts_linked += 1

            _upsert_role(conn, cid, "registrant", source_context="jotform_waiver")

            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"participants/jotform pk={pk}: {exc}")


def _ingest_participants_from_mailchimp(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest Mailchimp-resolved participants → candidate_participant child evidence.

    Uses mailchimp_contact_state.participant_id as the identity anchor — only rows
    that passed strict Mailchimp identity policy are projected.  Quarantined rows
    (no mailchimp_contact_state entry) are never projected into candidate evidence.

    For each resolved participant:
      - finds the existing candidate derived from the operational participant table
      - enriches candidate_participant_contact with email (from mcs.email_normalized)
        and phone (from participant_contact_point)
      - enriches candidate_participant_address from participant_address
      - links all contributing mailchimp_audience_row ids as source evidence
    """
    # All distinct resolved participant_ids (quarantined rows never reach contact_state).
    pid_rows = conn.execute(
        "SELECT DISTINCT participant_id::text FROM mailchimp_contact_state ORDER BY participant_id"
    ).fetchall()

    for (participant_id,) in pid_rows:
        # All audience rows that resolved to this participant (may span multiple files).
        mc_rows = conn.execute(
            """
            SELECT mcs.email_normalized,
                   mcs.source_system,
                   mar.id::text  AS audience_row_id,
                   mar.row_hash  AS audience_row_hash
            FROM mailchimp_contact_state mcs
            JOIN mailchimp_audience_row mar
              ON mar.source_file_name = mcs.source_file_name
             AND mar.row_hash         = mcs.row_hash
            WHERE mcs.participant_id = %s
            ORDER BY mcs.ingested_at ASC
            """,
            (participant_id,),
        ).fetchall()

        if not mc_rows:
            continue

        # Use the first row's email for the fallback fingerprint path only.
        first_email_normalized = mc_rows[0][0]
        source_system          = mc_rows[0][1] or "mailchimp_audience_csv"

        try:
            # ── Step 1: Resolve candidate ─────────────────────────────────────
            # Prefer the candidate already linked from the participant table path.
            link_row = conn.execute(
                """
                SELECT candidate_entity_id::text
                FROM candidate_source_link
                WHERE candidate_entity_type = 'participant'
                  AND source_table_name     = 'participant'
                  AND source_row_pk         = %s
                LIMIT 1
                """,
                (participant_id,),
            ).fetchone()

            if link_row:
                cid = str(link_row[0])
                ctrs.participants_candidate_enriched += 1
            else:
                # Fallback: participant table ingestion hasn't run yet.
                # Derive candidate from the participant record itself.
                p_rec = conn.execute(
                    """
                    SELECT p.full_name,
                           p.first_name,
                           p.last_name,
                           p.normalized_full_name,
                           (SELECT contact_value_normalized
                            FROM participant_contact_point
                            WHERE participant_id = p.id
                              AND contact_type = 'email'
                            ORDER BY is_primary DESC, created_at ASC
                            LIMIT 1) AS best_email
                    FROM participant p
                    WHERE p.id = %s
                    """,
                    (participant_id,),
                ).fetchone()
                if not p_rec:
                    ctrs.warnings.append(
                        f"participants/mailchimp participant_id={participant_id}: "
                        "participant row not found — skipped"
                    )
                    continue
                display_name = build_person_display_name(p_rec[1], p_rec[2])
                if display_name is None:
                    raw_full_name = trim(p_rec[0])
                    if raw_full_name and "@" not in raw_full_name and not looks_like_email(raw_full_name):
                        display_name = raw_full_name
                norm_name = normalize_person_name_for_identity(display_name)
                best_email = p_rec[4]
                fp = participant_fingerprint(norm_name, best_email or first_email_normalized)
                cid, created = _upsert_candidate(
                    conn,
                    "candidate_participant",
                    fp,
                    {
                        "display_name": display_name,
                        "normalized_name": norm_name,
                        "best_email": best_email or first_email_normalized,
                    },
                )
                if created:
                    ctrs.participants_candidate_created += 1
                else:
                    ctrs.participants_candidate_enriched += 1

            # ── Step 2: Email child evidence — all distinct emails ────────────
            # A participant may have multiple mailchimp_contact_state rows with
            # different email_normalized values (multi-email identity case).
            # Write a child contact row for each distinct email.
            seen_emails: set[str] = set()
            for mc_email, _, mc_audience_row_id, _ in mc_rows:
                if mc_email and mc_email not in seen_emails:
                    _upsert_contact(
                        conn, cid, "email",
                        raw_value=mc_email,
                        normalized_value=mc_email,
                        is_primary=False,
                        source_table="mailchimp_audience_row",
                        source_pk=mc_audience_row_id,
                    )
                    ctrs.participant_contacts_linked += 1
                    seen_emails.add(mc_email)

            # ── Step 3: Phone child evidence from participant_contact_point ───
            phone_rows = conn.execute(
                """
                SELECT id, contact_value_raw, contact_value_normalized, is_primary,
                       contact_subtype
                FROM participant_contact_point
                WHERE participant_id = %s AND contact_type = 'phone'
                ORDER BY is_primary DESC, created_at ASC
                """,
                (participant_id,),
            ).fetchall()
            for pr in phone_rows:
                if pr[2]:  # normalized_value required
                    _upsert_contact(
                        conn, cid, "phone",
                        raw_value=pr[1],
                        normalized_value=pr[2],
                        is_primary=bool(pr[3]),
                        source_table="participant_contact_point",
                        source_pk=str(pr[0]),
                        contact_subtype=pr[4],  # FOR-219
                    )
                    ctrs.participant_contacts_linked += 1

            # ── Step 4: Address child evidence from participant_address ────────
            addr_rows = conn.execute(
                """
                SELECT id, address_raw, line1, city, state, postal_code, country_code, is_primary
                FROM participant_address
                WHERE participant_id = %s
                ORDER BY created_at ASC
                """,
                (participant_id,),
            ).fetchall()
            for ar in addr_rows:
                if ar[1]:  # address_raw required
                    _upsert_address(
                        conn, cid,
                        address_raw=ar[1],
                        source_table="participant_address",
                        source_pk=str(ar[0]),
                        line1=ar[2], city=ar[3], state=ar[4],
                        postal_code=ar[5], country_code=ar[6],
                        is_primary=bool(ar[7]),
                    )
                    ctrs.participant_addresses_linked += 1

            # ── Step 5: Source links for each contributing audience_row ────────
            for _, row_source_system, audience_row_id, audience_row_hash in mc_rows:
                inserted = _link_source(
                    conn, "participant", cid,
                    "mailchimp_audience_row", audience_row_id,
                    row_source_system or "mailchimp_audience_csv",
                    audience_row_hash,
                )
                if inserted:
                    ctrs.source_links_inserted += 1
                else:
                    ctrs.source_links_skipped_duplicate += 1

            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(
                f"participants/mailchimp participant_id={participant_id}: {exc}"
            )


def _ingest_participants_from_airtable(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest airtable_copy_row[participants, owners] rows → candidate_participant."""
    rows = conn.execute(
        """
        SELECT id, asset_name, raw_payload, row_hash, source_system
        FROM airtable_copy_row
        WHERE asset_name IN ('participants', 'owners')
        ORDER BY ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        asset_name = row[1]
        payload: dict = row[2] if isinstance(row[2], dict) else json.loads(row[2])
        row_hash = row[3]
        source_system = row[4] or "airtable_copy_csv"

        if asset_name == "participants":
            raw_name = trim(payload.get("name") or payload.get("Name", ""))
            raw_email = trim(payload.get("competitorE") or payload.get("email", ""))
        else:  # owners
            raw_name = trim(payload.get("ownerName") or payload.get("name", ""))
            raw_email = trim(payload.get("email", ""))

        if not raw_name:
            ctrs.warnings.append(f"airtable/{asset_name} pk={pk}: skipped — missing name")
            continue

        norm_name = normalize_person_name_for_identity(raw_name)
        norm_email = normalize_email(raw_email) if raw_email else None
        fp = participant_fingerprint(norm_name, norm_email)

        role = "registrant" if asset_name == "participants" else "owner"
        # Map to valid candidate role
        candidate_role = "owner" if role == "owner" else "registrant"

        try:
            cid, created = _upsert_candidate(
                conn,
                "candidate_participant",
                fp,
                {
                    "display_name": raw_name,
                    "normalized_name": norm_name,
                    "best_email": norm_email,
                },
            )
            if created:
                ctrs.participants_candidate_created += 1
            else:
                ctrs.participants_candidate_enriched += 1

            inserted = _link_source(
                conn, "participant", cid,
                "airtable_copy_row", pk,
                source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1

            if norm_email:
                _upsert_contact(
                    conn, cid, "email", raw_email, norm_email, True,
                    "airtable_copy_row", pk,
                )
                ctrs.participant_contacts_linked += 1

            _upsert_role(conn, cid, candidate_role, source_context=f"airtable_{asset_name}")

            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"participants/airtable/{asset_name} pk={pk}: {exc}")


def _ingest_participants_from_yacht_scoring(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest yacht_scoring_raw_row entry rows (owner names) → candidate_participant."""
    rows = conn.execute(
        """
        SELECT id, asset_type, raw_payload, row_hash, source_system
        FROM yacht_scoring_raw_row
        WHERE asset_type IN ('deduplicated_entry', 'scraped_entry_listing')
        ORDER BY ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        asset_type = row[1]
        payload: dict = row[2] if isinstance(row[2], dict) else json.loads(row[2])
        row_hash = row[3]
        source_system = row[4] or "yacht_scoring_csv"

        raw_name = trim(payload.get("ownerName") or payload.get("Owner Name", ""))
        if not raw_name:
            ctrs.rows_skipped_no_owner_name += 1
            continue  # Many entries lack owner name — expected

        norm_name = normalize_person_name_for_identity(raw_name)
        if not norm_name:
            ctrs.rows_skipped_no_owner_name += 1
            continue

        # Contact/address evidence — schema-tolerant; most rows today have none
        ys_email_raw = trim(payload.get("email") or payload.get("ownerEmail") or "")
        ys_phone_raw = trim(payload.get("phone") or payload.get("ownerPhone") or "")
        ys_location_raw = trim(payload.get("ownerLocation") or payload.get("address") or "")
        ys_email_norm = normalize_email(ys_email_raw) if ys_email_raw else None
        ys_phone_norm = normalize_phone(ys_phone_raw) if ys_phone_raw else None

        ys_fields: dict = {"display_name": raw_name, "normalized_name": norm_name}
        if ys_email_norm:
            ys_fields["best_email"] = ys_email_norm
        if ys_phone_norm:
            ys_fields["best_phone"] = ys_phone_norm

        # Under-combination lookup only applies when THIS row has no email
        reuse_cid_ys: str | None = None
        if ys_email_norm is None:
            lookup = _find_email_bearing_candidate_by_name(conn, norm_name)
            if lookup == _AMBIGUOUS:
                ctrs.participants_under_combination_ambiguous += 1
                ctrs.warnings.append(
                    f"under_combination_ambiguous: yacht_scoring pk={pk} norm_name={norm_name!r}"
                )
            elif lookup is not None:
                reuse_cid_ys = lookup
                ctrs.participants_under_combination_reused += 1

        try:
            if reuse_cid_ys is not None:
                cid = reuse_cid_ys
                created = False
                _enrich_candidate_by_id(conn, "candidate_participant", cid, ys_fields)
                ctrs.participants_candidate_enriched += 1
            else:
                fp = participant_fingerprint(norm_name, ys_email_norm)
                cid, created = _upsert_candidate(
                    conn, "candidate_participant", fp, ys_fields,
                )
                if created:
                    ctrs.participants_candidate_created += 1
                else:
                    ctrs.participants_candidate_enriched += 1

            inserted = _link_source(
                conn, "participant", cid,
                "yacht_scoring_raw_row", pk,
                source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1

            _upsert_role(conn, cid, "owner", source_context=f"yacht_scoring_{asset_type}")

            # Attach contact/address child evidence when payload provides it
            if ys_email_norm:
                _upsert_contact(
                    conn, cid, "email", ys_email_raw, ys_email_norm, False,
                    "yacht_scoring_raw_row", pk,
                )
                ctrs.participant_contacts_linked += 1
            if ys_phone_norm:
                _upsert_contact(
                    conn, cid, "phone", ys_phone_raw, ys_phone_norm, False,
                    "yacht_scoring_raw_row", pk,
                )
                ctrs.participant_contacts_linked += 1
            if ys_location_raw:
                _upsert_address(
                    conn, cid, ys_location_raw,
                    "yacht_scoring_raw_row", pk,
                )
                ctrs.participant_addresses_linked += 1

            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"participants/yacht_scoring/{asset_type} pk={pk}: {exc}")


def _ingest_participants_from_related_contacts(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest participant_related_contact rows → candidate_participant (emergency/guardian)."""
    rows = conn.execute(
        """
        SELECT id, related_contact_type, related_full_name, phone_normalized, email_normalized
        FROM participant_related_contact
        ORDER BY created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        contact_type = row[1]  # 'emergency' or 'guardian'
        raw_name = trim(row[2] or "")
        phone_norm = row[3]
        email_norm = row[4]

        if not raw_name:
            ctrs.warnings.append(f"related_contacts pk={pk}: skipped — missing name")
            continue

        norm_name = normalize_person_name_for_identity(raw_name)

        rc_fields = {
            "display_name": raw_name,
            "normalized_name": norm_name,
            "best_email": email_norm,
            "best_phone": phone_norm,
        }
        reuse_cid_rc: str | None = None
        if email_norm is None and norm_name:
            lookup = _find_email_bearing_candidate_by_name(conn, norm_name)
            if lookup == _AMBIGUOUS:
                ctrs.participants_under_combination_ambiguous += 1
                ctrs.warnings.append(
                    f"under_combination_ambiguous: related_contact pk={pk} norm_name={norm_name!r}"
                )
            elif lookup is not None:
                reuse_cid_rc = lookup
                ctrs.participants_under_combination_reused += 1

        candidate_role = "emergency_contact" if contact_type == "emergency" else "guardian"
        try:
            if reuse_cid_rc is not None:
                cid = reuse_cid_rc
                created = False
                _enrich_candidate_by_id(conn, "candidate_participant", cid, rc_fields)
                ctrs.participants_candidate_enriched += 1
            else:
                fp = participant_fingerprint(norm_name, email_norm)
                cid, created = _upsert_candidate(
                    conn, "candidate_participant", fp, rc_fields,
                )
                if created:
                    ctrs.participants_candidate_created += 1
                else:
                    ctrs.participants_candidate_enriched += 1

            inserted = _link_source(
                conn, "participant", cid,
                "participant_related_contact", pk,
                "operational_db",
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1

            _upsert_role(conn, cid, candidate_role, source_context="jotform_waiver")

            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"participants/related_contacts pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Registration ingestion
# ---------------------------------------------------------------------------

def _lookup_candidate_event_for_event_instance(
    conn: psycopg.Connection,
    event_instance_id: str,
) -> str | None:
    """Return candidate_event.id for a given event_instance.id, or None."""
    row = conn.execute(
        """
        SELECT csl.candidate_entity_id
        FROM candidate_source_link csl
        WHERE csl.source_table_name = 'event_instance'
          AND csl.source_row_pk = %s
          AND csl.candidate_entity_type = 'event'
        LIMIT 1
        """,
        (event_instance_id,),
    ).fetchone()
    return str(row[0]) if row else None


def _lookup_candidate_yacht_for_yacht(
    conn: psycopg.Connection,
    yacht_id: str,
) -> str | None:
    """Return candidate_yacht.id for a given yacht.id, or None."""
    row = conn.execute(
        """
        SELECT csl.candidate_entity_id
        FROM candidate_source_link csl
        WHERE csl.source_table_name = 'yacht'
          AND csl.source_row_pk = %s
          AND csl.candidate_entity_type = 'yacht'
        LIMIT 1
        """,
        (yacht_id,),
    ).fetchone()
    return str(row[0]) if row else None


def _lookup_candidate_participant_for_participant(
    conn: psycopg.Connection,
    participant_id: str,
) -> str | None:
    """Return candidate_participant.id for a given participant.id, or None."""
    row = conn.execute(
        """
        SELECT csl.candidate_entity_id
        FROM candidate_source_link csl
        WHERE csl.source_table_name = 'participant'
          AND csl.source_row_pk = %s
          AND csl.candidate_entity_type = 'participant'
        LIMIT 1
        """,
        (participant_id,),
    ).fetchone()
    return str(row[0]) if row else None


def _ingest_registrations_from_event_entry(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest every event_entry row → candidate_registration."""
    rows = conn.execute(
        """
        SELECT ee.id, ee.event_instance_id, ee.yacht_id, ee.entry_status,
               ee.registration_external_id, ee.registered_at,
               -- primary participant: first skipper or owner_contact
               (SELECT eep.participant_id
                FROM event_entry_participant eep
                WHERE eep.event_entry_id = ee.id
                  AND eep.role IN ('skipper','owner_contact')
                ORDER BY CASE eep.role WHEN 'skipper' THEN 0 ELSE 1 END, eep.created_at
                LIMIT 1) AS primary_participant_id
        FROM event_entry ee
        ORDER BY ee.created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        event_instance_id = str(row[1])
        yacht_id = str(row[2])
        entry_status = row[3]
        ext_id = row[4]
        registered_at = row[5]
        primary_participant_id = str(row[6]) if row[6] else None

        # Look up candidate IDs via source links
        cand_event_id = _lookup_candidate_event_for_event_instance(conn, event_instance_id)
        cand_yacht_id = _lookup_candidate_yacht_for_yacht(conn, yacht_id)
        cand_part_id = (
            _lookup_candidate_participant_for_participant(conn, primary_participant_id)
            if primary_participant_id else None
        )

        if not cand_event_id:
            ctrs.warnings.append(
                f"registrations/event_entry pk={pk}: no candidate_event found for "
                f"event_instance_id={event_instance_id} — skipping"
            )
            continue

        fp = registration_fingerprint(cand_event_id, ext_id, cand_yacht_id)
        try:
            cid, created = _upsert_candidate(
                conn,
                "candidate_registration",
                fp,
                {
                    "registration_external_id": ext_id,
                    "candidate_event_id": cand_event_id,
                    "candidate_yacht_id": cand_yacht_id,
                    "candidate_primary_participant_id": cand_part_id,
                    "entry_status": entry_status,
                    "registered_at": str(registered_at) if registered_at else None,
                },
            )
            if created:
                ctrs.registrations_candidate_created += 1
            else:
                ctrs.registrations_candidate_enriched += 1

            inserted = _link_source(
                conn, "registration", cid,
                "event_entry", pk, "operational_db",
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1

            # Link participants from event_entry_participant as role assignments
            participants = conn.execute(
                """
                SELECT eep.participant_id, eep.role
                FROM event_entry_participant eep
                WHERE eep.event_entry_id = %s
                """,
                (pk,),
            ).fetchall()
            for pp in participants:
                p_cid = _lookup_candidate_participant_for_participant(conn, str(pp[0]))
                if p_cid:
                    src_role = pp[1]
                    # Map event_entry_participant roles to candidate roles
                    role_map = {
                        "skipper": "skipper",
                        "crew": "crew",
                        "owner_contact": "owner",
                        "registrant": "registrant",
                        "other": "other",
                    }
                    cand_role = role_map.get(src_role, "other")
                    _upsert_role(
                        conn, p_cid, cand_role,
                        source_context="event_entry_participant",
                        candidate_registration_id=cid,
                    )
                    ctrs.participant_roles_linked += 1

            ctrs.registrations_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"registrations/event_entry pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Raw-row → candidate linking helpers
# (link raw source rows to already-created candidates via xref tables)
# ---------------------------------------------------------------------------

def _link_airtable_raw_clubs_to_candidates(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Link airtable_copy_row[clubs] → candidate_club via airtable_xref_club."""
    rows = conn.execute(
        """
        SELECT acr.id, acr.row_hash, acr.source_system, axc.yacht_club_id
        FROM airtable_copy_row acr
        LEFT JOIN airtable_xref_club axc
          ON axc.source_primary_id = acr.source_primary_id
         AND axc.asset_name = 'clubs'
         AND axc.source_system = acr.source_system
        WHERE acr.asset_name = 'clubs'
        ORDER BY acr.ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        row_hash = row[1]
        source_system = row[2] or "airtable_copy_csv"
        yacht_club_id = str(row[3]) if row[3] else None

        if not yacht_club_id:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        c_row = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'yacht_club'
              AND source_row_pk = %s
              AND candidate_entity_type = 'club'
            LIMIT 1
            """,
            (yacht_club_id,),
        ).fetchone()

        if not c_row:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        try:
            inserted = _link_source(
                conn, "club", str(c_row[0]),
                "airtable_copy_row", pk, source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"airtable_clubs_link pk={pk}: {exc}")


def _link_airtable_raw_events_to_candidates(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Link airtable_copy_row[events] → candidate_event via airtable_xref_event."""
    rows = conn.execute(
        """
        SELECT id, raw_payload, row_hash, source_system
        FROM airtable_copy_row
        WHERE asset_name = 'events'
        ORDER BY ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        payload: dict = row[1] if isinstance(row[1], dict) else json.loads(row[1])
        row_hash = row[2]
        source_system = row[3] or "airtable_copy_csv"

        # Build canonical race key from event_global_id JSON field
        canonical_key: str | None = None
        egi_raw = payload.get("event_global_id")
        if egi_raw:
            try:
                egi = json.loads(egi_raw) if isinstance(egi_raw, str) else egi_raw
                race_id = egi.get("race_id")
                yr = egi.get("yr")
                if race_id and yr:
                    canonical_key = f"race:{race_id}:yr:{yr}"
            except Exception:
                pass

        if not canonical_key:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        xref = conn.execute(
            """
            SELECT event_instance_id FROM airtable_xref_event
            WHERE source_system = %s AND asset_name = 'events'
              AND source_primary_id = %s
            """,
            (source_system, canonical_key),
        ).fetchone()

        if not xref:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        c_row = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'event_instance'
              AND source_row_pk = %s
              AND candidate_entity_type = 'event'
            LIMIT 1
            """,
            (str(xref[0]),),
        ).fetchone()

        if not c_row:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        try:
            inserted = _link_source(
                conn, "event", str(c_row[0]),
                "airtable_copy_row", pk, source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"airtable_events_link pk={pk}: {exc}")


def _link_airtable_raw_yachts_to_candidates(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Link airtable_copy_row[yachts] → candidate_yacht via airtable_xref_yacht."""
    rows = conn.execute(
        """
        SELECT acr.id, acr.row_hash, acr.source_system, axy.yacht_id
        FROM airtable_copy_row acr
        LEFT JOIN airtable_xref_yacht axy
          ON axy.source_primary_id = acr.source_primary_id
         AND axy.asset_name = 'yachts'
         AND axy.source_system = acr.source_system
        WHERE acr.asset_name = 'yachts'
        ORDER BY acr.ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        row_hash = row[1]
        source_system = row[2] or "airtable_copy_csv"
        yacht_id = str(row[3]) if row[3] else None

        if not yacht_id:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        c_row = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'yacht'
              AND source_row_pk = %s
              AND candidate_entity_type = 'yacht'
            LIMIT 1
            """,
            (yacht_id,),
        ).fetchone()

        if not c_row:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        try:
            inserted = _link_source(
                conn, "yacht", str(c_row[0]),
                "airtable_copy_row", pk, source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"airtable_yachts_link pk={pk}: {exc}")


def _link_airtable_raw_entries_to_candidates(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Link airtable_copy_row[entries] → candidate_registration.

    Bridges: airtable_copy_row → airtable_xref_event → event_entry → candidate_registration.
    Requires registrations to have been ingested first (_ingest_registrations_from_event_entry).
    """
    rows = conn.execute(
        """
        SELECT id, raw_payload, row_hash, source_system
        FROM airtable_copy_row
        WHERE asset_name = 'entries'
        ORDER BY ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        payload: dict = row[1] if isinstance(row[1], dict) else json.loads(row[1])
        row_hash = row[2]
        source_system = row[3] or "airtable_copy_csv"

        # Build canonical race key from eventUuid JSON field (same logic as airtable pipeline)
        canonical_key: str | None = None
        event_uuid_raw = trim(payload.get("eventUuid"))
        if event_uuid_raw:
            try:
                egi = json.loads(event_uuid_raw) if isinstance(event_uuid_raw, str) else event_uuid_raw
                race_id = egi.get("race_id")
                yr = egi.get("yr")
                if race_id and yr:
                    canonical_key = f"race:{race_id}:yr:{yr}"
            except Exception:
                pass

        # Fallback: parse entries_url
        if not canonical_key:
            r_id, yr = parse_race_url(payload.get("entries_url"))
            if r_id and yr:
                canonical_key = f"race:{r_id}:yr:{yr}"

        if not canonical_key:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        xref = conn.execute(
            """
            SELECT event_instance_id FROM airtable_xref_event
            WHERE source_system = %s AND source_primary_id = %s
            LIMIT 1
            """,
            (source_system, canonical_key),
        ).fetchone()

        if not xref:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        event_instance_id = str(xref[0])
        entries_sku = trim(payload.get("entriesSku"))

        ee_row = conn.execute(
            """
            SELECT id FROM event_entry
            WHERE event_instance_id = %s
              AND registration_external_id = %s
            LIMIT 1
            """,
            (event_instance_id, entries_sku),
        ).fetchone()

        if not ee_row:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        c_row = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'event_entry'
              AND source_row_pk = %s
              AND candidate_entity_type = 'registration'
            LIMIT 1
            """,
            (str(ee_row[0]),),
        ).fetchone()

        if not c_row:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        try:
            inserted = _link_source(
                conn, "registration", str(c_row[0]),
                "airtable_copy_row", pk, source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"airtable_entries_link pk={pk}: {exc}")


def _link_ys_raw_events_to_candidates(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Link yacht_scoring_raw_row[scraped_event_listing] → candidate_event."""
    rows = conn.execute(
        """
        SELECT yrr.id, yrr.row_hash, yrr.source_system, yxe.event_instance_id
        FROM yacht_scoring_raw_row yrr
        LEFT JOIN yacht_scoring_xref_event yxe
          ON yxe.source_system = yrr.source_system
         AND yxe.source_event_id = yrr.source_event_id
        WHERE yrr.asset_type = 'scraped_event_listing'
        ORDER BY yrr.ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        row_hash = row[1]
        source_system = row[2] or "yacht_scoring_csv"
        event_instance_id = str(row[3]) if row[3] else None

        if not event_instance_id:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        c_row = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'event_instance'
              AND source_row_pk = %s
              AND candidate_entity_type = 'event'
            LIMIT 1
            """,
            (event_instance_id,),
        ).fetchone()

        if not c_row:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        try:
            inserted = _link_source(
                conn, "event", str(c_row[0]),
                "yacht_scoring_raw_row", pk, source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"ys_events_link pk={pk}: {exc}")


def _link_ys_raw_yachts_to_candidates(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Link yacht_scoring_raw_row[unique_yacht] → candidate_yacht via yacht_scoring_xref_yacht."""
    rows = conn.execute(
        """
        SELECT id, raw_payload, row_hash, source_system
        FROM yacht_scoring_raw_row
        WHERE asset_type = 'unique_yacht'
        ORDER BY ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        payload: dict = row[1] if isinstance(row[1], dict) else json.loads(row[1])
        row_hash = row[2]
        source_system = row[3] or "yacht_scoring_csv"

        yacht_name = trim(payload.get("yachtName"))
        sail_num = trim(payload.get("sailNumber"))
        name_norm = slug_name(yacht_name) if yacht_name else None

        if not name_norm:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        sail_norm = slug_name(sail_num) if sail_num else ""
        yacht_key = f"n:{name_norm}:s:{sail_norm}"

        xref = conn.execute(
            """
            SELECT yacht_id FROM yacht_scoring_xref_yacht
            WHERE source_system = %s AND source_yacht_key = %s
            """,
            (source_system, yacht_key),
        ).fetchone()

        if not xref:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        c_row = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'yacht'
              AND source_row_pk = %s
              AND candidate_entity_type = 'yacht'
            LIMIT 1
            """,
            (str(xref[0]),),
        ).fetchone()

        if not c_row:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        try:
            inserted = _link_source(
                conn, "yacht", str(c_row[0]),
                "yacht_scoring_raw_row", pk, source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"ys_yachts_link pk={pk}: {exc}")


def _link_ys_raw_entries_to_candidates(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Link yacht_scoring_raw_row[scraped_entry_listing/deduplicated_entry] → candidate_registration."""
    rows = conn.execute(
        """
        SELECT yrr.id, yrr.row_hash, yrr.source_system, yxe.event_entry_id
        FROM yacht_scoring_raw_row yrr
        LEFT JOIN yacht_scoring_xref_entry yxe
          ON yxe.source_system = yrr.source_system
         AND yxe.source_event_id = yrr.source_event_id
         AND yxe.source_entry_id = yrr.source_entry_id
        WHERE yrr.asset_type IN ('scraped_entry_listing', 'deduplicated_entry')
        ORDER BY yrr.ingested_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        row_hash = row[1]
        source_system = row[2] or "yacht_scoring_csv"
        event_entry_id = str(row[3]) if row[3] else None

        if not event_entry_id:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        c_row = conn.execute(
            """
            SELECT candidate_entity_id FROM candidate_source_link
            WHERE source_table_name = 'event_entry'
              AND source_row_pk = %s
              AND candidate_entity_type = 'registration'
            LIMIT 1
            """,
            (event_entry_id,),
        ).fetchone()

        if not c_row:
            ctrs.rows_skipped_no_xref_link += 1
            continue

        try:
            inserted = _link_source(
                conn, "registration", str(c_row[0]),
                "yacht_scoring_raw_row", pk, source_system, row_hash,
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"ys_entries_link pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Manual curation ingestion (source_system='manual_curation')
# ---------------------------------------------------------------------------

def _ingest_participants_from_manual_patches(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Apply active manual_participant_patch rows to candidate_participant.

    Unlike operational sources, manual patches OVERWRITE non-null patch fields
    (fill-nulls-only would defeat the corrective intent of a manual edit).

    When patch_display_name is provided, normalized_name is recomputed so the
    scorer reads the corrected identity value (not the stale pre-edit version).
    """
    rows = conn.execute(
        """
        SELECT id, candidate_participant_id,
               patch_display_name, patch_date_of_birth, patch_best_email, patch_best_phone
        FROM manual_participant_patch
        WHERE status = 'active'
        ORDER BY created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        candidate_id = str(row[1])

        patch_fields: dict[str, Any] = {}
        if row[2] is not None:
            patch_fields["display_name"] = row[2]
            # Keep normalized_name in sync so scorer and fingerprint lookups
            # use the corrected identity value.
            patch_fields["normalized_name"] = normalize_person_name_for_identity(row[2])
        if row[3] is not None:
            patch_fields["date_of_birth"] = str(row[3])
        if row[4] is not None:
            patch_fields["best_email"] = row[4]
        if row[5] is not None:
            patch_fields["best_phone"] = row[5]

        if not patch_fields:
            continue

        try:
            set_parts = [f"{col} = %s" for col in patch_fields] + ["updated_at = now()"]
            conn.execute(
                f"UPDATE candidate_participant SET {', '.join(set_parts)} WHERE id = %s",
                list(patch_fields.values()) + [candidate_id],
            )
            inserted = _link_source(
                conn, "participant", candidate_id,
                "manual_participant_patch", pk, "manual_curation",
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"manual/participant pk={pk}: {exc}")


def _ingest_addresses_from_manual_patches(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Ingest active manual_participant_address_patch rows into candidate_participant_address."""
    rows = conn.execute(
        """
        SELECT id, candidate_participant_id, address_raw, line1, city, state,
               postal_code, country_code, is_primary
        FROM manual_participant_address_patch
        WHERE status = 'active'
        ORDER BY created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        candidate_id = str(row[1])
        address_raw = row[2]

        try:
            # Manual patches use ON CONFLICT DO UPDATE (overwrite structured fields)
            # instead of the normal DO NOTHING, so a re-patch fixing city/state
            # on an existing address_raw actually takes effect.
            conn.execute(
                """
                INSERT INTO candidate_participant_address
                    (candidate_participant_id, address_raw, line1, city, state,
                     postal_code, country_code, is_primary, source_table_name, source_row_pk)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (candidate_participant_id, address_raw) DO UPDATE
                    SET line1        = COALESCE(EXCLUDED.line1,        candidate_participant_address.line1),
                        city         = COALESCE(EXCLUDED.city,         candidate_participant_address.city),
                        state        = COALESCE(EXCLUDED.state,        candidate_participant_address.state),
                        postal_code  = COALESCE(EXCLUDED.postal_code,  candidate_participant_address.postal_code),
                        country_code = COALESCE(EXCLUDED.country_code, candidate_participant_address.country_code),
                        is_primary   = EXCLUDED.is_primary,
                        source_table_name = EXCLUDED.source_table_name,
                        source_row_pk     = EXCLUDED.source_row_pk,
                        updated_at        = now()
                """,
                (
                    candidate_id, address_raw,
                    row[3], row[4], row[5], row[6], row[7], bool(row[8]),
                    "manual_participant_address_patch", pk,
                ),
            )
            ctrs.participant_addresses_linked += 1
            inserted = _link_source(
                conn, "participant", candidate_id,
                "manual_participant_address_patch", pk, "manual_curation",
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"manual/address pk={pk}: {exc}")


def _ingest_yachts_from_manual_patches(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Apply active manual_yacht_patch rows to candidate_yacht (overwrite non-null fields).

    When patch_name or patch_sail_number is provided, the corresponding normalized
    fields are recomputed so the scorer and fingerprint lookups use the corrected
    identity values (not the stale pre-edit versions).
    """
    rows = conn.execute(
        """
        SELECT id, candidate_yacht_id,
               patch_name, patch_sail_number, patch_length_feet, patch_yacht_type
        FROM manual_yacht_patch
        WHERE status = 'active'
        ORDER BY created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        candidate_id = str(row[1])

        patch_fields: dict[str, Any] = {}
        if row[2] is not None:
            patch_fields["name"] = row[2]
            patch_fields["normalized_name"] = slug_name(row[2])
        if row[3] is not None:
            patch_fields["sail_number"] = row[3]
            patch_fields["normalized_sail_number"] = slug_name(row[3])
        if row[4] is not None:
            patch_fields["length_feet"] = float(row[4])
        if row[5] is not None:
            patch_fields["yacht_type"] = row[5]

        if not patch_fields:
            continue

        try:
            set_parts = [f"{col} = %s" for col in patch_fields] + ["updated_at = now()"]
            conn.execute(
                f"UPDATE candidate_yacht SET {', '.join(set_parts)} WHERE id = %s",
                list(patch_fields.values()) + [candidate_id],
            )
            inserted = _link_source(
                conn, "yacht", candidate_id,
                "manual_yacht_patch", pk, "manual_curation",
            )
            if inserted:
                ctrs.source_links_inserted += 1
            else:
                ctrs.source_links_skipped_duplicate += 1
            ctrs.yachts_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"manual/yacht pk={pk}: {exc}")


def _ingest_ownerships_from_manual_patches(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Register active manual_yacht_ownership_patch rows as candidate-layer evidence.

    There is no candidate_yacht_ownership table.  Instead each active 'add' patch:
    - Creates a source_link from candidate_participant to the patch row
    - Creates a source_link from candidate_yacht to the patch row
    - Adds a 'yacht_owner' role_assignment to candidate_participant_role_assignment

    'remove' operation patches only register a source link to record the correction
    intent; operational removal is handled during canonical promotion/sync.
    """
    rows = conn.execute(
        """
        SELECT id, candidate_participant_id, candidate_yacht_id,
               role, operation, effective_start
        FROM manual_yacht_ownership_patch
        WHERE status = 'active'
        ORDER BY created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        participant_cid = str(row[1])
        yacht_cid = str(row[2])
        role = row[3]
        operation = row[4]

        try:
            # Source-link both candidate entities to this patch row
            for entity_type, cid in (("participant", participant_cid), ("yacht", yacht_cid)):
                inserted = _link_source(
                    conn, entity_type, cid,
                    "manual_yacht_ownership_patch", pk, "manual_curation",
                )
                if inserted:
                    ctrs.source_links_inserted += 1
                else:
                    ctrs.source_links_skipped_duplicate += 1

            # For 'add' patches, record a role assignment on the participant
            if operation == "add":
                role_label = "yacht_owner" if role == "owner" else "yacht_co_owner"
                source_context = f"manual|yacht:{yacht_cid}"
                conn.execute(
                    """
                    INSERT INTO candidate_participant_role_assignment
                        (candidate_participant_id, role, source_context)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (
                        candidate_participant_id,
                        role,
                        COALESCE(candidate_event_id::text, ''),
                        COALESCE(candidate_registration_id::text, '')
                    ) DO NOTHING
                    """,
                    (participant_cid, role_label, source_context),
                )
                ctrs.participant_roles_linked += 1

            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"manual/ownership pk={pk}: {exc}")


def _ingest_memberships_from_manual_patches(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
) -> None:
    """Register active manual_club_membership_patch rows as candidate-layer evidence.

    There is no candidate_club_membership table.  Instead each active 'add' patch:
    - Creates a source_link from candidate_participant to the patch row
    - Creates a source_link from candidate_club to the patch row
    - Adds a role_assignment (membership_role) to candidate_participant_role_assignment

    'remove' operation patches only register source links.
    """
    rows = conn.execute(
        """
        SELECT id, candidate_participant_id, candidate_club_id,
               membership_role, operation
        FROM manual_club_membership_patch
        WHERE status = 'active'
        ORDER BY created_at
        """
    ).fetchall()

    for row in rows:
        pk = str(row[0])
        participant_cid = str(row[1])
        club_cid = str(row[2])
        membership_role = row[3]
        operation = row[4]

        try:
            for entity_type, cid in (("participant", participant_cid), ("club", club_cid)):
                inserted = _link_source(
                    conn, entity_type, cid,
                    "manual_club_membership_patch", pk, "manual_curation",
                )
                if inserted:
                    ctrs.source_links_inserted += 1
                else:
                    ctrs.source_links_skipped_duplicate += 1

            if operation == "add":
                source_context = f"manual|club:{club_cid}"
                conn.execute(
                    """
                    INSERT INTO candidate_participant_role_assignment
                        (candidate_participant_id, role, source_context)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (
                        candidate_participant_id,
                        role,
                        COALESCE(candidate_event_id::text, ''),
                        COALESCE(candidate_registration_id::text, '')
                    ) DO NOTHING
                    """,
                    (participant_cid, membership_role, source_context),
                )
                ctrs.participant_roles_linked += 1

            ctrs.participants_ingested += 1
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"manual/membership pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def _run_step_with_savepoint(
    conn: psycopg.Connection,
    ctrs: SourceToCandidateCounters,
    step_name: str,
    step_fn,
) -> None:
    """Run one pipeline step under a savepoint.

    Some row-level ingesters catch DB exceptions and increment counters/warnings.
    Without savepoint recovery, those caught exceptions still leave the current
    transaction in INERROR, causing the next SQL statement to fail with
    InFailedSqlTransaction. Wrapping each step with a savepoint lets us recover
    and continue to a final report.
    """
    conn.execute("SAVEPOINT s2c_step")
    try:
        step_fn(conn, ctrs)
        if conn.info.transaction_status == pq.TransactionStatus.INERROR:
            conn.execute("ROLLBACK TO SAVEPOINT s2c_step")
            ctrs.warnings.append(
                f"{step_name}: step rolled back after row-level DB error(s); "
                "see earlier warnings for root cause"
            )
        conn.execute("RELEASE SAVEPOINT s2c_step")
    except Exception:
        # Keep the transaction usable for upstream caller handling.
        try:
            conn.execute("ROLLBACK TO SAVEPOINT s2c_step")
            conn.execute("RELEASE SAVEPOINT s2c_step")
        except Exception:
            conn.rollback()
        raise


def run_source_to_candidate(
    conn: psycopg.Connection,
    entity_type: str = "all",
    dry_run: bool = False,
) -> SourceToCandidateCounters:
    """Run the source-to-candidate pipeline.

    Args:
        conn: Open psycopg connection (transaction already started by caller).
        entity_type: One of 'participant', 'yacht', 'event', 'registration',
                     'club', or 'all'.
        dry_run: If True, the caller should ROLLBACK after this returns.

    Returns:
        SourceToCandidateCounters with run statistics.
    """
    ctrs = SourceToCandidateCounters()

    run_clubs = entity_type in ("club", "all")
    run_events = entity_type in ("event", "all")
    run_yachts = entity_type in ("yacht", "all")
    run_participants = entity_type in ("participant", "all")
    run_registrations = entity_type in ("registration", "all")

    # Clubs — no upstream dependencies
    if run_clubs:
        _run_step_with_savepoint(
            conn, ctrs, "clubs/yacht_club", _ingest_clubs_from_yacht_club
        )
        # Link airtable raw club rows to candidate_club via xref
        _run_step_with_savepoint(
            conn, ctrs, "clubs/airtable_link", _link_airtable_raw_clubs_to_candidates
        )

    # Events — no upstream dependencies
    if run_events:
        _run_step_with_savepoint(
            conn, ctrs, "events/event_instance", _ingest_events_from_event_instance
        )
        # Link raw event rows from airtable + yacht_scoring to candidate_event via xref
        _run_step_with_savepoint(
            conn, ctrs, "events/airtable_link", _link_airtable_raw_events_to_candidates
        )
        _run_step_with_savepoint(
            conn, ctrs, "events/yacht_scoring_link", _link_ys_raw_events_to_candidates
        )

    # Yachts — no upstream dependencies
    if run_yachts:
        _run_step_with_savepoint(
            conn, ctrs, "yachts/yacht", _ingest_yachts_from_yacht
        )
        # Link airtable raw yacht rows and ys unique_yacht rows to candidate_yacht via xref
        _run_step_with_savepoint(
            conn, ctrs, "yachts/airtable_link", _link_airtable_raw_yachts_to_candidates
        )
        _run_step_with_savepoint(
            conn, ctrs, "yachts/yacht_scoring_link", _link_ys_raw_yachts_to_candidates
        )

    # Participants — no upstream dependencies (but benefits from yachts/events existing)
    if run_participants:
        _run_step_with_savepoint(
            conn, ctrs, "participants/participant", _ingest_participants_from_participant_table
        )
        _run_step_with_savepoint(
            conn, ctrs, "participants/jotform", _ingest_participants_from_jotform
        )
        _run_step_with_savepoint(
            conn, ctrs, "participants/mailchimp", _ingest_participants_from_mailchimp
        )
        _run_step_with_savepoint(
            conn, ctrs, "participants/airtable", _ingest_participants_from_airtable
        )
        _run_step_with_savepoint(
            conn, ctrs, "participants/yacht_scoring", _ingest_participants_from_yacht_scoring
        )
        _run_step_with_savepoint(
            conn, ctrs, "participants/related_contacts", _ingest_participants_from_related_contacts
        )

    # Manual curation patches — must run after operational sources so corrections
    # overwrite values ingested from less-authoritative upstream tables.
    if run_participants:
        _run_step_with_savepoint(
            conn, ctrs, "participants/manual_patches", _ingest_participants_from_manual_patches
        )
        _run_step_with_savepoint(
            conn, ctrs, "participants/manual_addresses", _ingest_addresses_from_manual_patches
        )
        _run_step_with_savepoint(
            conn, ctrs, "participants/manual_ownerships", _ingest_ownerships_from_manual_patches
        )
        _run_step_with_savepoint(
            conn, ctrs, "participants/manual_memberships", _ingest_memberships_from_manual_patches
        )
    if run_yachts:
        _run_step_with_savepoint(
            conn, ctrs, "yachts/manual_patches", _ingest_yachts_from_manual_patches
        )

    # Registrations — must run after clubs, events, yachts, participants
    if run_registrations:
        _run_step_with_savepoint(
            conn, ctrs, "registrations/event_entry", _ingest_registrations_from_event_entry
        )
        # Link raw entry rows from airtable + yacht_scoring to candidate_registration via xref
        _run_step_with_savepoint(
            conn, ctrs, "registrations/airtable_link", _link_airtable_raw_entries_to_candidates
        )
        _run_step_with_savepoint(
            conn, ctrs, "registrations/yacht_scoring_link", _link_ys_raw_entries_to_candidates
        )

    return ctrs


def build_pipeline_report(
    ctrs: SourceToCandidateCounters,
    skipped_tables: list[tuple[str, str]] | None = None,
    dry_run: bool = False,
) -> str:
    """Return a human-readable text report of a source-to-candidate run."""
    lines = [
        "=" * 60,
        "Source-to-Candidate Pipeline Report",
        f"  dry_run: {dry_run}",
        "=" * 60,
        "Clubs:",
        f"  rows ingested:       {ctrs.clubs_ingested}",
        f"  candidates created:  {ctrs.clubs_candidate_created}",
        f"  candidates enriched: {ctrs.clubs_candidate_enriched}",
        "Events:",
        f"  rows ingested:       {ctrs.events_ingested}",
        f"  candidates created:  {ctrs.events_candidate_created}",
        f"  candidates enriched: {ctrs.events_candidate_enriched}",
        "Yachts:",
        f"  rows ingested:       {ctrs.yachts_ingested}",
        f"  candidates created:  {ctrs.yachts_candidate_created}",
        f"  candidates enriched: {ctrs.yachts_candidate_enriched}",
        "Participants:",
        f"  rows ingested:       {ctrs.participants_ingested}",
        f"  candidates created:  {ctrs.participants_candidate_created}",
        f"  candidates enriched: {ctrs.participants_candidate_enriched}",
        f"  contacts linked:     {ctrs.participant_contacts_linked}",
        f"  addresses linked:    {ctrs.participant_addresses_linked}",
        f"  roles linked:        {ctrs.participant_roles_linked}",
        "Registrations:",
        f"  rows ingested:       {ctrs.registrations_ingested}",
        f"  candidates created:  {ctrs.registrations_candidate_created}",
        f"  candidates enriched: {ctrs.registrations_candidate_enriched}",
        "Source Links:",
        f"  inserted:            {ctrs.source_links_inserted}",
        f"  skipped (duplicate): {ctrs.source_links_skipped_duplicate}",
        "Skipped Rows (expected):",
        f"  no xref link:        {ctrs.rows_skipped_no_xref_link}",
        f"  no owner name (ys):  {ctrs.rows_skipped_no_owner_name}",
        "Under-Combination Prevention:",
        f"  reused email-bearer: {ctrs.participants_under_combination_reused}",
        f"  ambiguous (skipped): {ctrs.participants_under_combination_ambiguous}",
        f"DB errors:             {ctrs.db_errors}",
    ]

    if skipped_tables:
        lines.append("\nIntentionally Skipped Tables:")
        for tbl, reason in skipped_tables:
            lines.append(f"  {tbl}: {reason}")

    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")

    lines.append("=" * 60)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Under-combination remediation
# ---------------------------------------------------------------------------

@dataclass
class UnderCombinationRemediationCounters:
    groups_examined: int = 0
    groups_merged: int = 0
    loser_rows_deleted: int = 0
    links_transferred: int = 0
    contacts_transferred: int = 0
    addresses_transferred: int = 0
    roles_transferred: int = 0
    conflicts_skipped: int = 0
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if k != "warnings"}
        d["warnings"] = self.warnings
        return d


def _participant_alias_cluster_key(normalized_name: str | None) -> str | None:
    """Return a token-order-insensitive key for participant-name remediation.

    This is intentionally narrow: we only normalize token order so
    "fname lname" and "lname fname" cluster together. Other safety criteria
    (single clear winner, loser eligibility) are still enforced separately.
    """
    norm = normalize_name(normalized_name)
    if not norm:
        return None
    tokens = norm.split()
    if len(tokens) < 2:
        return norm
    return " ".join(sorted(tokens))


def _remediation_rank_key(row: tuple[Any, ...]) -> tuple[int, int, float, float, str]:
    """Deterministic winner ranking key for remediation candidates.

    Row shape:
      (id, normalized_name, best_email, is_promoted, quality_score,
       updated_at, resolution_state, display_name)
    """
    quality = row[4]
    quality_num = float(quality) if quality is not None else -1_000_000_000.0
    updated_at = row[5]
    updated_ts = float(updated_at.timestamp()) if updated_at is not None else 0.0
    return (
        0 if row[2] is not None else 1,  # best_email DESC
        0 if bool(row[3]) else 1,        # is_promoted DESC
        -quality_num,                    # quality_score DESC NULLS LAST
        -updated_ts,                     # updated_at DESC
        str(row[0]),                     # id ASC
    )


def run_under_combination_remediation(
    conn: psycopg.Connection,
    dry_run: bool = False,
) -> "UnderCombinationRemediationCounters":
    """Consolidate existing split candidate_participant pairs.

    A split pair is a normalized_name that has both:
    - at least one candidate with best_email IS NOT NULL (winner candidate)
    - at least one candidate with best_email IS NULL (loser candidate)

    For each such group:
    1. Select winner via deterministic ranking.
    2. Validate each null-email candidate as an eligible loser
       (resolution_state='reject', is_promoted=false, best_email IS NULL).
    3. Transfer source links, contacts, addresses, roles to winner.
    4. Fill winner nulls only (COALESCE).
    5. Write audit log entry.
    6. Delete loser.

    Dry-run: reports planned actions but writes nothing.
    """
    ctrs = UnderCombinationRemediationCounters()

    # Load participant candidates once and cluster by alias-aware key.
    candidate_rows = conn.execute(
        """
        SELECT id, normalized_name, best_email, is_promoted, quality_score, updated_at,
               resolution_state, display_name
        FROM candidate_participant
        WHERE normalized_name IS NOT NULL
        """
    ).fetchall()

    clustered: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for row in candidate_rows:
        alias_key = _participant_alias_cluster_key(row[1])
        if alias_key:
            clustered[alias_key].append(row)

    for alias_key in sorted(clustered):
        cluster_rows = clustered[alias_key]
        has_email_bearer = any(row[2] is not None for row in cluster_rows)
        has_null_email = any(row[2] is None for row in cluster_rows)
        if not (has_email_bearer and has_null_email):
            continue

        ctrs.groups_examined += 1

        # Rank candidates in this alias-aware cluster deterministically.
        candidates = sorted(cluster_rows, key=_remediation_rank_key)
        normalized_names = sorted({str(row[1]) for row in candidates if row[1]})

        # Safety: only auto-consolidate when there's one clear email-bearing winner.
        email_bearers = [row for row in candidates if row[2] is not None]
        if len(email_bearers) != 1:
            ctrs.conflicts_skipped += 1
            ctrs.warnings.append(
                f"remediation_skip: alias_key={alias_key!r} normalized_names={normalized_names!r} "
                f"— ambiguous winner ({len(email_bearers)} email-bearing candidates)"
            )
            continue

        winner = email_bearers[0]
        winner_id = str(winner[0])
        winner_email = winner[2]

        # Eligible losers: null email, reject state, not promoted
        losers = [
            c for c in candidates
            if str(c[0]) != winner_id and c[2] is None and c[6] == "reject" and not c[3]
        ]

        if dry_run and losers:
            planned_loser_ids = [str(c[0]) for c in losers]
            ctrs.warnings.append(
                f"remediation_plan: alias_key={alias_key!r} normalized_names={normalized_names!r} "
                f"winner_id={winner_id} winner_email={winner_email!r} "
                f"loser_ids={planned_loser_ids!r}"
            )

        if not losers:
            # All null-email candidates are ineligible (promoted or wrong state)
            ineligible = [
                c for c in candidates if str(c[0]) != winner_id and c[2] is None
            ]
            if ineligible:
                ctrs.conflicts_skipped += 1
                ctrs.warnings.append(
                    f"remediation_skip: alias_key={alias_key!r} normalized_names={normalized_names!r} — "
                    f"{len(ineligible)} null-email candidate(s) ineligible for merge "
                    "(promoted or not in reject state)"
                )
            continue

        # Process each eligible loser
        group_errors = 0
        for loser in losers:
            loser_id = str(loser[0])
            sp = f"remediation_{ctrs.groups_examined}_{ctrs.loser_rows_deleted}"
            try:
                if not dry_run:
                    conn.execute(f"SAVEPOINT {sp}")

                # 1. Transfer candidate_source_link rows
                links = conn.execute(
                    """
                    SELECT candidate_entity_type, source_table_name, source_row_pk,
                           source_row_hash, source_system, link_score, link_reason
                    FROM candidate_source_link
                    WHERE candidate_entity_id = %s
                    """,
                    (loser_id,),
                ).fetchall()
                for lnk in links:
                    if not dry_run:
                        conn.execute(
                            """
                            UPDATE candidate_source_link loser_link
                            SET candidate_entity_id = %s
                            WHERE loser_link.candidate_entity_type = %s
                              AND loser_link.candidate_entity_id = %s
                              AND loser_link.source_table_name = %s
                              AND loser_link.source_row_pk = %s
                              AND NOT EXISTS (
                                    SELECT 1
                                    FROM candidate_source_link winner_link
                                    WHERE winner_link.candidate_entity_type = loser_link.candidate_entity_type
                                      AND winner_link.candidate_entity_id = %s
                                      AND winner_link.source_table_name = loser_link.source_table_name
                                      AND winner_link.source_row_pk = loser_link.source_row_pk
                              )
                            """,
                            (
                                winner_id,
                                lnk[0],
                                loser_id,
                                lnk[1],
                                lnk[2],
                                winner_id,
                            ),
                        )
                    ctrs.links_transferred += 1

                # 2. Transfer candidate_participant_contact rows
                contacts = conn.execute(
                    """
                    SELECT contact_type, raw_value, normalized_value, is_primary,
                           source_table_name, source_row_pk
                    FROM candidate_participant_contact
                    WHERE candidate_participant_id = %s
                    """,
                    (loser_id,),
                ).fetchall()
                for c in contacts:
                    if not dry_run:
                        if c[2] is not None:
                            conn.execute(
                                """
                                INSERT INTO candidate_participant_contact
                                    (candidate_participant_id, contact_type, raw_value,
                                     normalized_value, is_primary, source_table_name, source_row_pk)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (candidate_participant_id, contact_type, normalized_value)
                                    WHERE normalized_value IS NOT NULL
                                DO NOTHING
                                """,
                                (winner_id, c[0], c[1], c[2], c[3], c[4], c[5]),
                            )
                        else:
                            conn.execute(
                                """
                                INSERT INTO candidate_participant_contact
                                    (candidate_participant_id, contact_type, raw_value,
                                     normalized_value, is_primary, source_table_name, source_row_pk)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (candidate_participant_id, contact_type, raw_value)
                                    WHERE normalized_value IS NULL
                                DO NOTHING
                                """,
                                (winner_id, c[0], c[1], None, c[3], c[4], c[5]),
                            )
                    ctrs.contacts_transferred += 1

                # 3. Transfer candidate_participant_address rows
                addresses = conn.execute(
                    """
                    SELECT address_raw, line1, city, state, postal_code, country_code,
                           is_primary, source_table_name, source_row_pk
                    FROM candidate_participant_address
                    WHERE candidate_participant_id = %s
                    """,
                    (loser_id,),
                ).fetchall()
                for a in addresses:
                    if not dry_run:
                        conn.execute(
                            """
                            INSERT INTO candidate_participant_address
                                (candidate_participant_id, address_raw, line1, city, state,
                                 postal_code, country_code, is_primary,
                                 source_table_name, source_row_pk)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (candidate_participant_id, address_raw) DO NOTHING
                            """,
                            (winner_id, a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8]),
                        )
                    ctrs.addresses_transferred += 1

                # 4. Transfer candidate_participant_role_assignment rows
                roles = conn.execute(
                    """
                    SELECT role, candidate_event_id, candidate_registration_id, source_context
                    FROM candidate_participant_role_assignment
                    WHERE candidate_participant_id = %s
                    """,
                    (loser_id,),
                ).fetchall()
                for r in roles:
                    if not dry_run:
                        conn.execute(
                            """
                            INSERT INTO candidate_participant_role_assignment
                                (candidate_participant_id, role, candidate_event_id,
                                 candidate_registration_id, source_context)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (
                                candidate_participant_id,
                                role,
                                COALESCE(candidate_event_id::text, ''),
                                COALESCE(candidate_registration_id::text, '')
                            ) DO NOTHING
                            """,
                            (winner_id, r[0], r[1], r[2], r[3]),
                        )
                    ctrs.roles_transferred += 1

                # 5. Fill winner nulls (COALESCE enrichment) from loser top-level fields
                if not dry_run:
                    conn.execute(
                        """
                        UPDATE candidate_participant SET
                            display_name    = COALESCE(candidate_participant.display_name,
                                                       src.display_name),
                            date_of_birth   = COALESCE(candidate_participant.date_of_birth,
                                                       src.date_of_birth),
                            best_phone      = COALESCE(candidate_participant.best_phone,
                                                       src.best_phone),
                            updated_at      = now()
                        FROM (SELECT display_name, date_of_birth, best_phone
                              FROM candidate_participant WHERE id = %s) src
                        WHERE candidate_participant.id = %s
                        """,
                        (loser_id, winner_id),
                    )

                # 6. Write audit log
                if not dry_run:
                    conn.execute(
                        """
                        INSERT INTO resolution_manual_action_log
                            (entity_type, candidate_entity_id, canonical_entity_id,
                             action_type, reason_code, actor, source)
                        VALUES ('participant', %s, NULL, 'merge',
                                'under_combination_consolidation',
                                'pipeline_under_combination_fix', 'pipeline')
                        """,
                        (loser_id,),
                    )

                # 7. Repoint candidate_registration FK references from loser -> winner
                if not dry_run:
                    conn.execute(
                        """
                        UPDATE candidate_registration
                           SET candidate_primary_participant_id = %s,
                               updated_at = now()
                         WHERE candidate_primary_participant_id = %s
                        """,
                        (winner_id, loser_id),
                    )

                # 8. Delete loser's original source links (already copied to winner)
                if not dry_run:
                    conn.execute(
                        "DELETE FROM candidate_source_link WHERE candidate_entity_id = %s",
                        (loser_id,),
                    )

                # 9. Delete loser
                if not dry_run:
                    conn.execute(
                        "DELETE FROM candidate_participant WHERE id = %s",
                        (loser_id,),
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")

                ctrs.loser_rows_deleted += 1

            except Exception as exc:
                group_errors += 1
                ctrs.db_errors += 1
                ctrs.warnings.append(
                    f"remediation_error: alias_key={alias_key!r} loser_id={loser_id}: {exc}"
                )
                if not dry_run:
                    try:
                        conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                        conn.execute(f"RELEASE SAVEPOINT {sp}")
                    except Exception:
                        pass
                continue

        if losers and group_errors == 0:
            ctrs.groups_merged += 1

    return ctrs


def build_remediation_report(
    ctrs: UnderCombinationRemediationCounters,
    dry_run: bool = False,
) -> str:
    """Return a human-readable text report of a remediation run."""
    lines = [
        "=" * 60,
        "Under-Combination Remediation Report",
        f"  dry_run: {dry_run}",
        "=" * 60,
        f"  groups examined:      {ctrs.groups_examined}",
        f"  groups merged:        {ctrs.groups_merged}",
        f"  loser rows deleted:   {ctrs.loser_rows_deleted}",
        f"  links transferred:    {ctrs.links_transferred}",
        f"  contacts transferred: {ctrs.contacts_transferred}",
        f"  addresses transferred:{ctrs.addresses_transferred}",
        f"  roles transferred:    {ctrs.roles_transferred}",
        f"  conflicts skipped:    {ctrs.conflicts_skipped}",
        f"  db errors:            {ctrs.db_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")
    lines.append("=" * 60)
    return "\n".join(lines)
