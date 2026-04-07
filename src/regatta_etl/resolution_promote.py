"""regatta_etl.resolution_promote

Candidate → Canonical promotion pipeline (--mode resolution_promote).

Promotes all candidates with resolution_state='auto_promote' and is_promoted=false
to their corresponding canonical_* table.  Records the promotion in:
  - candidate_canonical_link  (unique per candidate; idempotent)
  - resolution_manual_action_log  (source='pipeline')

Updates the candidate row: is_promoted=true, promoted_canonical_id=<canonical_id>.

Processing order: club → event → yacht → participant → registration
(registrations need canonical FKs from earlier-promoted entities).

Idempotency:
  - candidate_canonical_link has UNIQUE (candidate_entity_type, candidate_entity_id)
  - Each candidate is wrapped in a SAVEPOINT; failures roll back only that candidate.
  - If a candidate_canonical_link already exists (partial prior run), the canonical ID
    is reused and the candidate row is repaired rather than inserting a duplicate.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import psycopg

from regatta_etl.normalize import (
    build_person_display_name,
    is_likely_org_name,
    normalize_person_name_for_identity,
    parse_name_parts,
)
from regatta_etl.resolution_lifecycle import _write_provenance

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class PromoteCounters:
    candidates_promoted: int = 0
    candidates_already_promoted: int = 0  # is_promoted=true on entry (skipped)
    candidates_skipped_missing_dep: int = 0  # registration deps not yet promoted
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates_promoted": self.candidates_promoted,
            "candidates_already_promoted": self.candidates_already_promoted,
            "candidates_skipped_missing_dep": self.candidates_skipped_missing_dep,
            "db_errors": self.db_errors,
            "warnings": self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def _lookup_canonical_id(
    conn: psycopg.Connection,
    candidate_entity_type: str,
    candidate_entity_id: str,
) -> str | None:
    """Return the canonical_entity_id for a promoted candidate, or None."""
    row = conn.execute(
        """
        SELECT canonical_entity_id
        FROM candidate_canonical_link
        WHERE candidate_entity_type = %s AND candidate_entity_id = %s
        """,
        (candidate_entity_type, candidate_entity_id),
    ).fetchone()
    return str(row[0]) if row else None


def _log_promotion(
    conn: psycopg.Connection,
    entity_type: str,
    candidate_id: str,
    canonical_id: str,
    score_before: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO resolution_manual_action_log
            (entity_type, candidate_entity_id, canonical_entity_id,
             action_type, score_before, actor, source)
        VALUES (%s, %s, %s, 'promote', %s, 'pipeline', 'pipeline')
        """,
        (entity_type, candidate_id, canonical_id, score_before),
    )


# ---------------------------------------------------------------------------
# Per-entity-type canonical INSERT helpers
# ---------------------------------------------------------------------------

def _insert_canonical_club(conn: psycopg.Connection, pk: str) -> str:
    row = conn.execute(
        """
        INSERT INTO canonical_club
            (name, normalized_name, website, phone, address_raw, state_usa,
             canonical_confidence_score)
        SELECT name, normalized_name, website, phone, address_raw, state_usa,
               quality_score
        FROM candidate_club WHERE id = %s
        RETURNING id
        """,
        (pk,),
    ).fetchone()
    return str(row[0])


def _insert_canonical_event(conn: psycopg.Connection, pk: str) -> str:
    row = conn.execute(
        """
        INSERT INTO canonical_event
            (event_name, normalized_event_name, season_year, event_external_id,
             start_date, end_date, location_raw, canonical_confidence_score)
        SELECT event_name, normalized_event_name, season_year, event_external_id,
               start_date, end_date, location_raw, quality_score
        FROM candidate_event WHERE id = %s
        RETURNING id
        """,
        (pk,),
    ).fetchone()
    return str(row[0])


def _insert_canonical_yacht(conn: psycopg.Connection, pk: str) -> str:
    row = conn.execute(
        """
        INSERT INTO canonical_yacht
            (name, normalized_name, sail_number, normalized_sail_number,
             length_feet, yacht_type, canonical_confidence_score)
        SELECT name, normalized_name, sail_number, normalized_sail_number,
               length_feet, yacht_type, quality_score
        FROM candidate_yacht WHERE id = %s
        RETURNING id
        """,
        (pk,),
    ).fetchone()
    return str(row[0])


def _insert_canonical_participant(conn: psycopg.Connection, pk: str) -> str:
    candidate_row = conn.execute(
        """
        SELECT display_name, date_of_birth, best_email, best_phone, quality_score
        FROM candidate_participant
        WHERE id = %s
        """,
        (pk,),
    ).fetchone()
    first_name, last_name = parse_name_parts(candidate_row[0] if candidate_row else None)
    display_name = build_person_display_name(first_name, last_name)
    normalized_name = normalize_person_name_for_identity(display_name)
    row = conn.execute(
        """
        INSERT INTO canonical_participant
            (display_name, normalized_name, first_name, last_name,
             date_of_birth, best_email, best_phone,
             canonical_confidence_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            display_name,
            normalized_name,
            first_name,
            last_name,
            candidate_row[1] if candidate_row else None,
            candidate_row[2] if candidate_row else None,
            candidate_row[3] if candidate_row else None,
            candidate_row[4] if candidate_row else None,
        ),
    ).fetchone()
    return str(row[0])


def _sync_canonical_participant_fields(
    conn: psycopg.Connection,
    candidate_participant_id: str,
    canonical_participant_id: str,
) -> None:
    candidate_row = conn.execute(
        """
        SELECT display_name, date_of_birth, best_email, best_phone, quality_score
        FROM candidate_participant
        WHERE id = %s
        """,
        (candidate_participant_id,),
    ).fetchone()
    first_name, last_name = parse_name_parts(candidate_row[0] if candidate_row else None)
    display_name = build_person_display_name(first_name, last_name)
    normalized_name = normalize_person_name_for_identity(display_name)
    conn.execute(
        """
        UPDATE canonical_participant
        SET display_name = CASE
                WHEN canonical_participant.display_name IS NULL
                  OR canonical_participant.display_name LIKE '%%@%%'
                THEN %s
                ELSE canonical_participant.display_name
            END,
            normalized_name = CASE
                WHEN canonical_participant.normalized_name IS NULL
                  OR canonical_participant.display_name LIKE '%%@%%'
                THEN %s
                ELSE canonical_participant.normalized_name
            END,
            first_name = CASE
                WHEN canonical_participant.first_name IS NULL
                  OR canonical_participant.first_name ~* '^[^[:space:]@]+@[^[:space:]@]+\\.[^[:space:]@]+$'
                  OR canonical_participant.display_name LIKE '%%@%%'
                THEN %s
                ELSE canonical_participant.first_name
            END,
            last_name = CASE
                WHEN canonical_participant.last_name IS NULL
                  OR canonical_participant.last_name ~* '^[^[:space:]@]+@[^[:space:]@]+\\.[^[:space:]@]+$'
                  OR canonical_participant.display_name LIKE '%%@%%'
                THEN %s
                ELSE canonical_participant.last_name
            END,
            date_of_birth = COALESCE(canonical_participant.date_of_birth, %s),
            best_email = COALESCE(canonical_participant.best_email, %s),
            best_phone = COALESCE(canonical_participant.best_phone, %s),
            canonical_confidence_score = COALESCE(canonical_participant.canonical_confidence_score, %s)
        WHERE id = %s
        """,
        (
            display_name,
            normalized_name,
            first_name,
            last_name,
            candidate_row[1] if candidate_row else None,
            candidate_row[2] if candidate_row else None,
            candidate_row[3] if candidate_row else None,
            candidate_row[4] if candidate_row else None,
            canonical_participant_id,
        ),
    )


def _sync_canonical_participant_children(
    conn: psycopg.Connection,
    candidate_participant_id: str,
    canonical_participant_id: str,
) -> None:
    """Copy participant child evidence into canonical tables without duplication.

    This function is safe to call on first promotion and on subsequent repair
    reruns for already-promoted participants.
    """
    conn.execute(
        """
        UPDATE canonical_participant_contact existing
        SET contact_subtype = COALESCE(existing.contact_subtype, c.contact_subtype),
            is_primary      = existing.is_primary OR c.is_primary
        FROM candidate_participant_contact c
        WHERE c.candidate_participant_id = %s
          AND existing.canonical_participant_id = %s
          AND existing.contact_type = c.contact_type
          AND existing.raw_value = c.raw_value
          AND COALESCE(existing.normalized_value, '') = COALESCE(c.normalized_value, '')
        """,
        (candidate_participant_id, canonical_participant_id),
    )

    conn.execute(
        """
        INSERT INTO canonical_participant_contact
            (canonical_participant_id, contact_type, contact_subtype,
             raw_value, normalized_value, is_primary)
        SELECT DISTINCT
            %s::uuid,
            c.contact_type,
            c.contact_subtype,
            c.raw_value,
            c.normalized_value,
            c.is_primary
        FROM candidate_participant_contact c
        WHERE c.candidate_participant_id = %s
          AND NOT EXISTS (
              SELECT 1
              FROM canonical_participant_contact existing
              WHERE existing.canonical_participant_id = %s
                AND existing.contact_type = c.contact_type
                AND existing.raw_value = c.raw_value
                AND COALESCE(existing.normalized_value, '') = COALESCE(c.normalized_value, '')
          )
        """,
        (canonical_participant_id, candidate_participant_id, canonical_participant_id),
    )

    conn.execute(
        """
        INSERT INTO canonical_participant_address
            (canonical_participant_id, address_raw, line1, city, state,
             postal_code, country_code, is_primary)
        SELECT DISTINCT
            %s::uuid,
            a.address_raw,
            a.line1,
            a.city,
            a.state,
            a.postal_code,
            a.country_code,
            a.is_primary
        FROM candidate_participant_address a
        WHERE a.candidate_participant_id = %s
          AND NOT EXISTS (
              SELECT 1
              FROM canonical_participant_address existing
              WHERE existing.canonical_participant_id = %s
                AND existing.address_raw = a.address_raw
          )
        """,
        (canonical_participant_id, candidate_participant_id, canonical_participant_id),
    )

    conn.execute(
        """
        INSERT INTO canonical_participant_role_assignment
            (canonical_participant_id, role, canonical_event_id,
             canonical_registration_id, source_context)
        SELECT DISTINCT
            %s::uuid,
            r.role,
            event_link.canonical_entity_id,
            reg_link.canonical_entity_id,
            r.source_context
        FROM candidate_participant_role_assignment r
        LEFT JOIN candidate_canonical_link event_link
               ON event_link.candidate_entity_type = 'event'
              AND event_link.candidate_entity_id = r.candidate_event_id
        LEFT JOIN candidate_canonical_link reg_link
               ON reg_link.candidate_entity_type = 'registration'
              AND reg_link.candidate_entity_id = r.candidate_registration_id
        WHERE r.candidate_participant_id = %s
          AND (r.candidate_event_id IS NULL OR event_link.canonical_entity_id IS NOT NULL)
          AND (r.candidate_registration_id IS NULL OR reg_link.canonical_entity_id IS NOT NULL)
          AND NOT EXISTS (
              SELECT 1
              FROM canonical_participant_role_assignment existing
              WHERE existing.canonical_participant_id = %s
                AND existing.role = r.role
                AND COALESCE(existing.canonical_event_id::text, '')
                    = COALESCE(event_link.canonical_entity_id::text, '')
                AND COALESCE(existing.canonical_registration_id::text, '')
                    = COALESCE(reg_link.canonical_entity_id::text, '')
                AND COALESCE(existing.source_context, '') = COALESCE(r.source_context, '')
          )
        """,
        (canonical_participant_id, candidate_participant_id, canonical_participant_id),
    )


def _insert_canonical_registration(
    conn: psycopg.Connection,
    pk: str,
    canonical_event_id: str | None,
    canonical_yacht_id: str | None,
    canonical_participant_id: str | None,
) -> str:
    row = conn.execute(
        """
        INSERT INTO canonical_registration
            (registration_external_id, canonical_event_id, canonical_yacht_id,
             canonical_primary_participant_id, entry_status, registered_at,
             canonical_confidence_score)
        SELECT registration_external_id, %s, %s, %s,
               entry_status, registered_at, quality_score
        FROM candidate_registration WHERE id = %s
        RETURNING id
        """,
        (canonical_event_id, canonical_yacht_id, canonical_participant_id, pk),
    ).fetchone()
    return str(row[0])


_CANONICAL_INSERTERS = {
    "club":         _insert_canonical_club,
    "event":        _insert_canonical_event,
    "yacht":        _insert_canonical_yacht,
    "participant":  _insert_canonical_participant,
}

_CANDIDATE_TABLE = {
    "club":         "candidate_club",
    "event":        "candidate_event",
    "yacht":        "candidate_yacht",
    "participant":  "candidate_participant",
    "registration": "candidate_registration",
}


# ---------------------------------------------------------------------------
# Per-entity-type promotion
# ---------------------------------------------------------------------------

def _promote_entity_type(
    conn: psycopg.Connection,
    entity_type: str,
    ctrs: PromoteCounters,
) -> None:
    """Promote all auto_promote candidates of a single entity type."""
    table = _CANDIDATE_TABLE[entity_type]

    # Fetch candidates that need promotion
    if entity_type == "registration":
        rows = conn.execute(
            """
            SELECT id, quality_score,
                   candidate_event_id, candidate_yacht_id, candidate_primary_participant_id
            FROM candidate_registration
            WHERE resolution_state = 'auto_promote' AND is_promoted = false
            ORDER BY created_at
            """
        ).fetchall()
    elif entity_type == "participant":
        # FOR-224: exclude nameless records from the promotion query entirely.
        # A candidate with normalized_name IS NULL has zero identity signal and
        # must not be auto-promoted.  The scoring pipeline sets hard_block:missing_name
        # on such candidates, but a belt-and-suspenders DB-level guard here ensures
        # re-scoring races don't slip a nameless record through.
        rows = conn.execute(
            """
            SELECT id, quality_score, is_promoted, promoted_canonical_id, display_name, normalized_name
            FROM candidate_participant
            WHERE resolution_state = 'auto_promote'
              AND normalized_name IS NOT NULL
            ORDER BY created_at
            """
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT id, quality_score FROM {table} "
            f"WHERE resolution_state = 'auto_promote' AND is_promoted = false "
            f"ORDER BY created_at"
        ).fetchall()

    for idx, raw_row in enumerate(rows):
        pk = str(raw_row[0])
        score_before = float(raw_row[1]) if raw_row[1] is not None else None
        was_already_promoted = False
        stored_canonical_id = None
        candidate_display_name = None
        candidate_normalized_name = None
        if entity_type == "participant":
            was_already_promoted = bool(raw_row[2]) or raw_row[3] is not None
            stored_canonical_id = str(raw_row[3]) if raw_row[3] else None
            candidate_display_name = raw_row[4]
            candidate_normalized_name = raw_row[5]
        sp = f"promote_{entity_type}_{idx}"

        conn.execute(f"SAVEPOINT {sp}")
        try:
            if entity_type == "participant" and is_likely_org_name(
                candidate_display_name or candidate_normalized_name
            ):
                conn.execute(
                    """
                    UPDATE candidate_participant
                    SET resolution_state = 'reject'
                    WHERE id = %s
                    """,
                    (pk,),
                )
                ctrs.warnings.append(
                    f"participant pk={pk}: org-like candidate blocked from promotion (FOR-222)"
                )
                conn.execute(f"RELEASE SAVEPOINT {sp}")
                continue

            # Check if a canonical_link already exists (partial prior run recovery)
            existing_canonical_id = _lookup_canonical_id(conn, entity_type, pk)

            if existing_canonical_id:
                canonical_id = existing_canonical_id
            elif stored_canonical_id:
                # FOR-220: guard against reusing a canonical that already belongs to a
                # different candidate.  This can happen when promoted_canonical_id is set
                # on a candidate row pointing to a canonical that was claimed by someone
                # else (e.g. after a botched data migration).  If detected, walk this
                # candidate back to 'review' and skip rather than create a collision.
                collision_row = conn.execute(
                    """
                    SELECT candidate_entity_id
                    FROM candidate_canonical_link
                    WHERE candidate_entity_type = %s
                      AND canonical_entity_id   = %s
                      AND candidate_entity_id  != %s
                    LIMIT 1
                    """,
                    (entity_type, stored_canonical_id, pk),
                ).fetchone()
                if collision_row is not None:
                    conn.execute(
                        f"""
                        UPDATE {table}
                        SET is_promoted = false,
                            resolution_state = 'review',
                            promoted_canonical_id = NULL
                        WHERE id = %s
                        """,
                        (pk,),
                    )
                    ctrs.warnings.append(
                        f"{entity_type} pk={pk}: stored_canonical_id {stored_canonical_id} "
                        f"already claimed by {collision_row[0]} — reset to review (FOR-220)"
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue
                canonical_id = stored_canonical_id
            elif entity_type == "registration":
                # Resolve canonical FKs from already-promoted entities
                cand_event_id  = str(raw_row[2]) if raw_row[2] else None
                cand_yacht_id  = str(raw_row[3]) if raw_row[3] else None
                cand_part_id   = str(raw_row[4]) if raw_row[4] else None

                # candidate_event_id is NOT NULL on candidate_registration, so must resolve
                if not cand_event_id:
                    ctrs.candidates_skipped_missing_dep += 1
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue

                can_event_id = _lookup_canonical_id(conn, "event", cand_event_id)
                if not can_event_id:
                    ctrs.candidates_skipped_missing_dep += 1
                    ctrs.warnings.append(
                        f"registration {pk}: event {cand_event_id} not yet promoted"
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue

                can_yacht_id = _lookup_canonical_id(conn, "yacht", cand_yacht_id) if cand_yacht_id else None
                can_part_id  = _lookup_canonical_id(conn, "participant", cand_part_id) if cand_part_id else None

                canonical_id = _insert_canonical_registration(
                    conn, pk, can_event_id, can_yacht_id, can_part_id
                )
            else:
                inserter = _CANONICAL_INSERTERS[entity_type]
                canonical_id = inserter(conn, pk)

            if entity_type == "participant":
                _sync_canonical_participant_fields(conn, pk, canonical_id)
                _sync_canonical_participant_children(conn, pk, canonical_id)

            # Record the promotion link (idempotent via UNIQUE constraint)
            conn.execute(
                """
                INSERT INTO candidate_canonical_link
                    (candidate_entity_type, candidate_entity_id, canonical_entity_id,
                     promotion_score, promotion_mode, promoted_by)
                VALUES (%s, %s, %s, %s, 'auto', 'pipeline')
                ON CONFLICT (candidate_entity_type, candidate_entity_id) DO NOTHING
                """,
                (entity_type, pk, canonical_id, score_before),
            )

            if was_already_promoted:
                conn.execute(
                    f"""
                    UPDATE {table}
                    SET promoted_canonical_id = COALESCE(promoted_canonical_id, %s)
                    WHERE id = %s
                    """,
                    (canonical_id, pk),
                )
                conn.execute(f"RELEASE SAVEPOINT {sp}")
                ctrs.candidates_already_promoted += 1
                continue

            # Update candidate as promoted
            conn.execute(
                f"UPDATE {table} SET is_promoted = true, promoted_canonical_id = %s WHERE id = %s",
                (canonical_id, pk),
            )

            # Audit log
            _log_promotion(conn, entity_type, pk, canonical_id, score_before)

            # Field-level provenance
            _write_provenance(
                conn,
                entity_type=entity_type,
                canonical_id=canonical_id,
                candidate_id=pk,
                candidate_score=score_before,
                rule_version=None,
                decided_by="auto_promote",
            )

            conn.execute(f"RELEASE SAVEPOINT {sp}")
            ctrs.candidates_promoted += 1

        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            ctrs.db_errors += 1
            ctrs.warnings.append(f"{entity_type} pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def run_promote(
    conn: psycopg.Connection,
    entity_type: str = "all",
    dry_run: bool = False,
) -> PromoteCounters:
    """Promote auto_promote candidates to canonical tables.

    Args:
        conn: Open psycopg connection (caller manages transaction).
        entity_type: One of 'participant','yacht','event','registration','club','all'.
        dry_run: If True, caller should ROLLBACK after this returns.

    Returns:
        PromoteCounters with run statistics.
    """
    ctrs = PromoteCounters()
    entity_types = (
        ["club", "event", "yacht", "participant", "registration"]
        if entity_type == "all"
        else [entity_type]
    )
    for et in entity_types:
        _promote_entity_type(conn, et, ctrs)
    return ctrs


def build_promote_report(ctrs: PromoteCounters, dry_run: bool = False) -> str:
    lines = [
        "=" * 60,
        "Candidate → Canonical Promotion Report",
        f"  dry_run: {dry_run}",
        "=" * 60,
        f"  candidates promoted:         {ctrs.candidates_promoted}",
        f"  already promoted (skipped):  {ctrs.candidates_already_promoted}",
        f"  skipped (dep not promoted):  {ctrs.candidates_skipped_missing_dep}",
        f"DB errors:                     {ctrs.db_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")
    lines.append("=" * 60)
    return "\n".join(lines)
