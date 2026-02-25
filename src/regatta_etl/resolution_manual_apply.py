"""regatta_etl.resolution_manual_apply

Manual review decision ingestion pipeline (--mode resolution_manual_apply).

Reads a CSV of human review decisions for candidate entities and applies each
decision (promote / reject / hold) with full audit logging.

Input CSV format (comma-delimited, header row required):
    candidate_entity_type,candidate_entity_id,action,reason_code,actor

Required columns: candidate_entity_type, candidate_entity_id, action, actor
Optional:         reason_code (defaults to 'manual_review')

Valid actions:    promote | reject | hold
Valid types:      participant | yacht | event | registration | club

State-guard rules:
  - promote: only from resolution_state IN ('review','hold') and is_promoted=False
  - reject/hold: blocked when is_promoted=True (cannot change state of promoted candidate)

Partial-run recovery:
  If candidate_canonical_link already exists for a promote row, the existing
  canonical_id is reused (after verifying the canonical row exists). If the link
  is stale (canonical row missing), the stale link is deleted and a fresh
  canonical row + link is inserted.

Processing order: caller drives; SAVEPOINT per row for per-row isolation.
"""

from __future__ import annotations

import csv
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

from regatta_etl.resolution_lifecycle import _write_provenance
from regatta_etl.resolution_promote import (
    _insert_canonical_club,
    _insert_canonical_event,
    _insert_canonical_participant,
    _insert_canonical_registration,
    _insert_canonical_yacht,
    _lookup_canonical_id,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VALID_ACTIONS = frozenset({"promote", "reject", "hold"})
_VALID_ENTITY_TYPES = frozenset({"participant", "yacht", "event", "registration", "club"})
_REQUIRED_COLS = frozenset({"candidate_entity_type", "candidate_entity_id", "action", "actor"})

_CANDIDATE_TABLE = {
    "participant":  "candidate_participant",
    "yacht":        "candidate_yacht",
    "club":         "candidate_club",
    "event":        "candidate_event",
    "registration": "candidate_registration",
}

_CANONICAL_TABLE = {
    "participant":  "canonical_participant",
    "yacht":        "canonical_yacht",
    "club":         "canonical_club",
    "event":        "canonical_event",
    "registration": "canonical_registration",
}

_CANONICAL_INSERTERS = {
    "club":         _insert_canonical_club,
    "event":        _insert_canonical_event,
    "yacht":        _insert_canonical_yacht,
    "participant":  _insert_canonical_participant,
}


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class ManualApplyCounters:
    rows_read: int = 0
    rows_applied: int = 0
    rows_skipped_already_promoted: int = 0
    rows_skipped_missing_dep: int = 0
    rows_invalid: int = 0
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "rows_applied": self.rows_applied,
            "rows_skipped_already_promoted": self.rows_skipped_already_promoted,
            "rows_skipped_missing_dep": self.rows_skipped_missing_dep,
            "rows_invalid": self.rows_invalid,
            "db_errors": self.db_errors,
            "warnings": self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_candidate_state(
    conn: psycopg.Connection,
    entity_type: str,
    pk: str,
) -> dict[str, Any] | None:
    """Return is_promoted, quality_score, resolution_state for a candidate, or None."""
    table = _CANDIDATE_TABLE[entity_type]
    row = conn.execute(
        f"SELECT is_promoted, quality_score, resolution_state FROM {table} WHERE id = %s",
        (pk,),
    ).fetchone()
    if row is None:
        return None
    return {"is_promoted": row[0], "quality_score": row[1], "resolution_state": row[2]}


def _canonical_row_exists(
    conn: psycopg.Connection,
    entity_type: str,
    canonical_id: str,
) -> bool:
    """Return True if the canonical row exists in its table."""
    table = _CANONICAL_TABLE[entity_type]
    row = conn.execute(
        f"SELECT id FROM {table} WHERE id = %s",
        (canonical_id,),
    ).fetchone()
    return row is not None


def _log_manual_action(
    conn: psycopg.Connection,
    entity_type: str,
    pk: str,
    canonical_id: str | None,
    action_type: str,
    score_before: float | None,
    reason_code: str,
    actor: str,
) -> None:
    conn.execute(
        """
        INSERT INTO resolution_manual_action_log
            (entity_type, candidate_entity_id, canonical_entity_id,
             action_type, score_before, reason_code, actor, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'sheet_import')
        """,
        (entity_type, pk, canonical_id, action_type, score_before, reason_code, actor),
    )


# ---------------------------------------------------------------------------
# Per-action apply functions
# ---------------------------------------------------------------------------

def _apply_promote(
    conn: psycopg.Connection,
    entity_type: str,
    pk: str,
    reason_code: str,
    actor: str,
    ctrs: ManualApplyCounters,
) -> None:
    state = _fetch_candidate_state(conn, entity_type, pk)
    if state is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"promote {entity_type} {pk}: candidate not found")
        return

    if state["is_promoted"]:
        ctrs.rows_skipped_already_promoted += 1
        ctrs.warnings.append(f"promote {entity_type} {pk}: already promoted, skipped")
        return

    if state["resolution_state"] not in ("review", "hold"):
        ctrs.rows_invalid += 1
        ctrs.warnings.append(
            f"promote {entity_type} {pk}: resolution_state='{state['resolution_state']}'"
            " is not review/hold; blocked"
        )
        return

    score_before = float(state["quality_score"]) if state["quality_score"] is not None else None
    table = _CANDIDATE_TABLE[entity_type]

    # Partial-run recovery: check if a canonical link already exists.
    existing_canonical_id = _lookup_canonical_id(conn, entity_type, pk)
    canonical_id: str | None = None

    if existing_canonical_id:
        if _canonical_row_exists(conn, entity_type, existing_canonical_id):
            canonical_id = existing_canonical_id
        else:
            # Stale link: canonical row missing/corrupt — delete and re-insert.
            ctrs.warnings.append(
                f"promote {entity_type} {pk}: stale canonical link {existing_canonical_id},"
                " deleting and re-promoting"
            )
            conn.execute(
                """
                DELETE FROM candidate_canonical_link
                WHERE candidate_entity_type = %s AND candidate_entity_id = %s
                """,
                (entity_type, pk),
            )

    if canonical_id is None:
        # Insert new canonical row.
        if entity_type == "registration":
            # Resolve canonical FKs for registration.
            reg_row = conn.execute(
                """
                SELECT candidate_event_id, candidate_yacht_id,
                       candidate_primary_participant_id
                FROM candidate_registration WHERE id = %s
                """,
                (pk,),
            ).fetchone()
            if not reg_row or not reg_row[0]:
                ctrs.rows_invalid += 1
                ctrs.warnings.append(f"promote registration {pk}: missing candidate_event_id")
                return

            cand_event_id = str(reg_row[0])
            can_event_id = _lookup_canonical_id(conn, "event", cand_event_id)
            if not can_event_id:
                ctrs.rows_skipped_missing_dep += 1
                ctrs.warnings.append(
                    f"promote registration {pk}: event {cand_event_id} not yet promoted"
                )
                return

            cand_yacht_id = str(reg_row[1]) if reg_row[1] else None
            cand_part_id = str(reg_row[2]) if reg_row[2] else None
            can_yacht_id = _lookup_canonical_id(conn, "yacht", cand_yacht_id) if cand_yacht_id else None
            can_part_id = _lookup_canonical_id(conn, "participant", cand_part_id) if cand_part_id else None

            canonical_id = _insert_canonical_registration(
                conn, pk, can_event_id, can_yacht_id, can_part_id
            )
        else:
            inserter = _CANONICAL_INSERTERS[entity_type]
            canonical_id = inserter(conn, pk)

    # Upsert candidate_canonical_link (DO UPDATE to always reflect current canonical_id).
    conn.execute(
        """
        INSERT INTO candidate_canonical_link
            (candidate_entity_type, candidate_entity_id, canonical_entity_id,
             promotion_score, promotion_mode, promoted_by)
        VALUES (%s, %s, %s, %s, 'manual', %s)
        ON CONFLICT (candidate_entity_type, candidate_entity_id)
        DO UPDATE SET
            canonical_entity_id = EXCLUDED.canonical_entity_id,
            promotion_score     = EXCLUDED.promotion_score,
            promoted_by         = EXCLUDED.promoted_by,
            promoted_at         = now()
        """,
        (entity_type, pk, canonical_id, score_before, actor),
    )

    # Update candidate: mark promoted and lock state.
    conn.execute(
        f"""
        UPDATE {table}
        SET is_promoted          = true,
            promoted_canonical_id = %s,
            resolution_state      = 'auto_promote'
        WHERE id = %s
        """,
        (canonical_id, pk),
    )

    _log_manual_action(conn, entity_type, pk, canonical_id, "promote", score_before, reason_code, actor)

    # Field-level provenance
    _write_provenance(
        conn,
        entity_type=entity_type,
        canonical_id=canonical_id,
        candidate_id=pk,
        candidate_score=score_before,
        rule_version=None,
        decided_by="manual",
    )

    ctrs.rows_applied += 1


def _apply_state_change(
    conn: psycopg.Connection,
    entity_type: str,
    pk: str,
    new_state: str,
    reason_code: str,
    actor: str,
    ctrs: ManualApplyCounters,
) -> None:
    state = _fetch_candidate_state(conn, entity_type, pk)
    if state is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"{new_state} {entity_type} {pk}: candidate not found")
        return

    if state["is_promoted"]:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(
            f"{new_state} {entity_type} {pk}: is_promoted=True; cannot change state of promoted candidate"
        )
        return

    score_before = float(state["quality_score"]) if state["quality_score"] is not None else None
    table = _CANDIDATE_TABLE[entity_type]

    conn.execute(
        f"UPDATE {table} SET resolution_state = %s WHERE id = %s",
        (new_state, pk),
    )
    _log_manual_action(conn, entity_type, pk, None, new_state, score_before, reason_code, actor)
    ctrs.rows_applied += 1


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def run_manual_apply(
    conn: psycopg.Connection,
    decisions_path: Path | str,
    rescore_after_apply: bool = False,
    dry_run: bool = False,
    validate_only: bool = False,
) -> ManualApplyCounters:
    """Apply manual review decisions from a CSV file to candidate tables.

    Args:
        conn: Open psycopg connection (caller manages transaction).
        decisions_path: Path to CSV file with manual decisions.
        rescore_after_apply: If True, re-run scoring for entity types that had
            successful promotes (to update confidence_reasons + last_score_run_id).
            Does NOT re-score entity types that only had reject/hold actions, to
            avoid overriding manually set states.
        dry_run: If True, caller should ROLLBACK after this returns.
        validate_only: If True, parse and validate every row without any DB operations.
            Returns counters only; ignores rescore_after_apply and dry_run.

    Returns:
        ManualApplyCounters with run statistics.
    """
    ctrs = ManualApplyCounters()
    affected_promote_types: set[str] = set()

    path = Path(decisions_path)
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"decisions CSV is empty or has no header: {path}")
        missing = _REQUIRED_COLS - set(reader.fieldnames)
        if missing:
            raise ValueError(
                f"decisions CSV missing required columns: {sorted(missing)}"
            )

        if validate_only:
            for idx, raw_row in enumerate(reader):
                ctrs.rows_read += 1
                entity_type = (raw_row.get("candidate_entity_type") or "").strip().lower()
                pk = (raw_row.get("candidate_entity_id") or "").strip()
                action = (raw_row.get("action") or "").strip().lower()
                actor = (raw_row.get("actor") or "").strip()
                if not entity_type or not pk or not action or not actor:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(
                        f"row {idx}: missing required field(s) "
                        f"(entity_type={entity_type!r}, pk={pk!r}, "
                        f"action={action!r}, actor={actor!r})"
                    )
                    continue
                if entity_type not in _VALID_ENTITY_TYPES:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(
                        f"row {idx}: unknown candidate_entity_type={entity_type!r}"
                    )
                    continue
                if action not in _VALID_ACTIONS:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(f"row {idx}: unknown action={action!r}")
                    continue
            return ctrs

        for idx, raw_row in enumerate(reader):
            ctrs.rows_read += 1
            sp = f"manual_apply_{idx}"
            conn.execute(f"SAVEPOINT {sp}")
            try:
                entity_type = (raw_row.get("candidate_entity_type") or "").strip().lower()
                pk = (raw_row.get("candidate_entity_id") or "").strip()
                action = (raw_row.get("action") or "").strip().lower()
                actor = (raw_row.get("actor") or "").strip()
                reason_code = (raw_row.get("reason_code") or "").strip() or "manual_review"

                # Validate required fields.
                if not entity_type or not pk or not action or not actor:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(
                        f"row {idx}: missing required field(s) "
                        f"(entity_type={entity_type!r}, pk={pk!r}, "
                        f"action={action!r}, actor={actor!r})"
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue

                if entity_type not in _VALID_ENTITY_TYPES:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(
                        f"row {idx}: unknown candidate_entity_type={entity_type!r}"
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue

                if action not in _VALID_ACTIONS:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(
                        f"row {idx}: unknown action={action!r}"
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue

                rows_applied_before = ctrs.rows_applied
                if action == "promote":
                    _apply_promote(conn, entity_type, pk, reason_code, actor, ctrs)
                else:
                    _apply_state_change(conn, entity_type, pk, action, reason_code, actor, ctrs)

                if ctrs.rows_applied > rows_applied_before and action == "promote":
                    affected_promote_types.add(entity_type)

                conn.execute(f"RELEASE SAVEPOINT {sp}")

            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                ctrs.db_errors += 1
                ctrs.warnings.append(f"row {idx}: {exc}")

    # Optional re-score for entity types with successful promotes only.
    if rescore_after_apply and affected_promote_types:
        from regatta_etl.resolution_score import run_score
        for et in affected_promote_types:
            run_score(conn, entity_type=et)

    return ctrs


def build_manual_apply_report(ctrs: ManualApplyCounters, dry_run: bool = False) -> str:
    lines = [
        "=" * 60,
        "Manual Review Decision Apply Report",
        f"  dry_run: {dry_run}",
        "=" * 60,
        f"  rows read:                      {ctrs.rows_read}",
        f"  rows applied:                   {ctrs.rows_applied}",
        f"  skipped (already promoted):     {ctrs.rows_skipped_already_promoted}",
        f"  skipped (missing dep):          {ctrs.rows_skipped_missing_dep}",
        f"  invalid rows:                   {ctrs.rows_invalid}",
        f"DB errors:                        {ctrs.db_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")
    if not dry_run and ctrs.rows_applied > 0:
        lines.append(
            "\nNote: run --mode resolution_score to re-score candidates affected by"
            " reject/hold decisions."
        )
    lines.append("=" * 60)
    return "\n".join(lines)
