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
    row = conn.execute(
        """
        INSERT INTO canonical_participant
            (display_name, normalized_name, date_of_birth, best_email, best_phone,
             canonical_confidence_score)
        SELECT display_name, normalized_name, date_of_birth, best_email, best_phone,
               quality_score
        FROM candidate_participant WHERE id = %s
        RETURNING id
        """,
        (pk,),
    ).fetchone()
    return str(row[0])


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
    else:
        rows = conn.execute(
            f"SELECT id, quality_score FROM {table} "
            f"WHERE resolution_state = 'auto_promote' AND is_promoted = false "
            f"ORDER BY created_at"
        ).fetchall()

    for idx, raw_row in enumerate(rows):
        pk = str(raw_row[0])
        score_before = float(raw_row[1]) if raw_row[1] is not None else None
        sp = f"promote_{entity_type}_{idx}"

        conn.execute(f"SAVEPOINT {sp}")
        try:
            # Check if a canonical_link already exists (partial prior run recovery)
            existing_canonical_id = _lookup_canonical_id(conn, entity_type, pk)

            if existing_canonical_id:
                canonical_id = existing_canonical_id
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
