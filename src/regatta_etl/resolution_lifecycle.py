"""regatta_etl.resolution_lifecycle

Canonical lifecycle operations pipeline (--mode resolution_lifecycle).
Also provides _write_provenance() helper for use by resolution_promote
and resolution_manual_apply.

Lifecycle operations (--lifecycle-op):
    merge   -- merge two canonical entities; relinks candidates from merge → keep
    demote  -- demote a promoted candidate back to review; deletes canonical if sole link
    unlink  -- like demote but canonical is always preserved; NBAs preserved
    split   -- clone a canonical for a subset of its candidates

Input CSV formats (one row per candidate/canonical, --decisions-path):

    merge:
        canonical_entity_type, keep_canonical_id, merge_canonical_id, reason_code, actor

    demote | unlink:
        candidate_entity_type, candidate_entity_id, reason_code, actor

    split:
        canonical_entity_type, old_canonical_id, candidate_entity_id, reason_code, actor
        (rows with the same old_canonical_id are batched into one split operation)

Depends on: 0015_provenance_nba_lifecycle (canonical_attribute_provenance)
            0012_canonical_tables (canonical_* + candidate_canonical_link)
            0011_candidate_canonical_core (candidate_* + candidate_canonical_link)
"""

from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

# ---------------------------------------------------------------------------
# Entity-type attribute lists for provenance
# ---------------------------------------------------------------------------

_PROVENANCE_ATTRS: dict[str, list[str]] = {
    "participant":  ["display_name", "normalized_name", "date_of_birth", "best_email", "best_phone"],
    "yacht":        ["name", "normalized_name", "sail_number", "normalized_sail_number",
                     "length_feet", "yacht_type"],
    "club":         ["name", "normalized_name", "website", "phone", "address_raw", "state_usa"],
    "event":        ["event_name", "normalized_event_name", "season_year", "event_external_id",
                     "start_date", "end_date", "location_raw"],
    "registration": ["registration_external_id", "entry_status", "registered_at"],
}

# Column lists for cloning a canonical row (excludes id, timestamps)
_CLONE_COLS: dict[str, str] = {
    "participant":  (
        "display_name, normalized_name, first_name, last_name, date_of_birth, "
        "best_email, best_phone, canonical_confidence_score"
    ),
    "yacht":        (
        "name, normalized_name, sail_number, normalized_sail_number, "
        "length_feet, yacht_type, canonical_confidence_score"
    ),
    "club":         (
        "name, normalized_name, website, phone, address_raw, state_usa, "
        "canonical_confidence_score"
    ),
    "event":        (
        "event_name, normalized_event_name, season_year, event_external_id, "
        "start_date, end_date, location_raw, canonical_confidence_score"
    ),
    "registration": (
        "registration_external_id, canonical_event_id, canonical_yacht_id, "
        "canonical_primary_participant_id, entry_status, registered_at, "
        "canonical_confidence_score"
    ),
}

_CANONICAL_TABLE = {
    "participant":  "canonical_participant",
    "yacht":        "canonical_yacht",
    "club":         "canonical_club",
    "event":        "canonical_event",
    "registration": "canonical_registration",
}

_CANDIDATE_TABLE = {
    "participant":  "candidate_participant",
    "yacht":        "candidate_yacht",
    "club":         "candidate_club",
    "event":        "candidate_event",
    "registration": "candidate_registration",
}

_REQUIRED_COLS_BY_OP: dict[str, frozenset[str]] = {
    "merge":  frozenset({"canonical_entity_type", "keep_canonical_id",
                         "merge_canonical_id", "reason_code", "actor"}),
    "demote": frozenset({"candidate_entity_type", "candidate_entity_id",
                         "reason_code", "actor"}),
    "unlink": frozenset({"candidate_entity_type", "candidate_entity_id",
                         "reason_code", "actor"}),
    "split":  frozenset({"canonical_entity_type", "old_canonical_id",
                         "candidate_entity_id", "reason_code", "actor"}),
}

_VALID_ENTITY_TYPES = frozenset({"participant", "yacht", "event", "registration", "club"})


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class LifecycleCounters:
    rows_read: int = 0
    rows_applied: int = 0
    rows_invalid: int = 0
    rows_skipped: int = 0
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "rows_applied": self.rows_applied,
            "rows_invalid": self.rows_invalid,
            "rows_skipped": self.rows_skipped,
            "db_errors": self.db_errors,
            "warnings": self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Provenance helper (shared with resolution_promote + resolution_manual_apply)
# ---------------------------------------------------------------------------

def _write_provenance(
    conn: psycopg.Connection,
    entity_type: str,
    canonical_id: str,
    candidate_id: str,
    candidate_score: float | None,
    rule_version: str | None,
    decided_by: str,
) -> None:
    """Upsert canonical_attribute_provenance for all tracked attributes.

    Reads current attribute values from canonical_<entity_type> and upserts one
    provenance row per attribute.  On conflict (canonical + attribute already has a
    row), the existing row is overwritten with the new source/score/decided_by.

    Args:
        conn: psycopg connection (within a live transaction/savepoint).
        entity_type: One of participant|yacht|event|registration|club.
        canonical_id: UUID of the canonical entity.
        candidate_id: UUID of the candidate that sourced the attributes.
        candidate_score: Quality score of the candidate at decision time (or None).
        rule_version: YAML rule-set version string, or None.
        decided_by: 'auto_promote' | 'manual' | 'merge'.
    """
    attrs = _PROVENANCE_ATTRS.get(entity_type)
    if not attrs:
        return

    canon_table = _CANONICAL_TABLE[entity_type]
    attr_list = ", ".join(attrs)

    row = conn.execute(
        f"SELECT {attr_list} FROM {canon_table} WHERE id = %s",
        (canonical_id,),
    ).fetchone()
    if row is None:
        return

    for attr_name, attr_val in zip(attrs, row):
        str_val = str(attr_val) if attr_val is not None else None
        conn.execute(
            """
            INSERT INTO canonical_attribute_provenance
                (canonical_entity_type, canonical_entity_id, attribute_name,
                 attribute_value, source_candidate_type, source_candidate_id,
                 source_score, rule_version, decided_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (canonical_entity_type, canonical_entity_id, attribute_name)
            DO UPDATE SET
                attribute_value       = EXCLUDED.attribute_value,
                source_candidate_type = EXCLUDED.source_candidate_type,
                source_candidate_id   = EXCLUDED.source_candidate_id,
                source_score          = EXCLUDED.source_score,
                rule_version          = EXCLUDED.rule_version,
                decided_by            = EXCLUDED.decided_by,
                decided_at            = now()
            """,
            (entity_type, canonical_id, attr_name, str_val,
             entity_type, candidate_id, candidate_score, rule_version, decided_by),
        )


# ---------------------------------------------------------------------------
# Shared internal helpers
# ---------------------------------------------------------------------------

def _dismiss_nbas_for_candidate(
    conn: psycopg.Connection,
    entity_type: str,
    candidate_id: str,
) -> None:
    """Dismiss all open NBAs targeting this candidate entity.

    resolution_score.py writes NBAs with target_entity_type = 'candidate_{entity_type}'
    (e.g. 'candidate_participant'), so we must match that convention here.
    """
    target_type = f"candidate_{entity_type}"
    conn.execute(
        """
        UPDATE next_best_action
        SET status = 'dismissed'
        WHERE target_entity_type = %s
          AND target_entity_id   = %s
          AND status = 'open'
        """,
        (target_type, candidate_id),
    )


def _log_lifecycle_action(
    conn: psycopg.Connection,
    entity_type: str,
    candidate_id: str,
    canonical_id: str | None,
    action_type: str,
    reason_code: str,
    actor: str,
) -> None:
    conn.execute(
        """
        INSERT INTO resolution_manual_action_log
            (entity_type, candidate_entity_id, canonical_entity_id,
             action_type, reason_code, actor, source)
        VALUES (%s, %s, %s, %s, %s, %s, 'sheet_import')
        """,
        (entity_type, candidate_id, canonical_id, action_type, reason_code, actor),
    )


def _migrate_canonical_refs(
    conn: psycopg.Connection,
    entity_type: str,
    old_id: str,
    new_id: str | None,
) -> None:
    """Reroute or clear canonical_registration FK references before deleting a canonical.

    For merge (new_id provided): UPDATE FK = new_id WHERE FK = old_id.
    For demote (new_id = None):  UPDATE FK = NULL  WHERE FK = old_id.

    Only applies to entity types that appear as FKs on canonical_registration.
    """
    if entity_type == "participant":
        conn.execute(
            "UPDATE canonical_registration "
            "SET canonical_primary_participant_id = %s "
            "WHERE canonical_primary_participant_id = %s",
            (new_id, old_id),
        )
    elif entity_type == "event":
        conn.execute(
            "UPDATE canonical_registration "
            "SET canonical_event_id = %s "
            "WHERE canonical_event_id = %s",
            (new_id, old_id),
        )
    elif entity_type == "yacht":
        conn.execute(
            "UPDATE canonical_registration "
            "SET canonical_yacht_id = %s "
            "WHERE canonical_yacht_id = %s",
            (new_id, old_id),
        )
    # club and registration: no FK references from canonical_registration — no-op.


def _delete_canonical_with_provenance(
    conn: psycopg.Connection,
    entity_type: str,
    canonical_id: str,
) -> None:
    """Delete provenance rows then the canonical row itself."""
    conn.execute(
        "DELETE FROM canonical_attribute_provenance "
        "WHERE canonical_entity_type = %s AND canonical_entity_id = %s",
        (entity_type, canonical_id),
    )
    conn.execute(
        f"DELETE FROM {_CANONICAL_TABLE[entity_type]} WHERE id = %s",
        (canonical_id,),
    )


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

def _apply_merge(
    conn: psycopg.Connection,
    entity_type: str,
    keep_id: str,
    merge_id: str,
    reason_code: str,
    actor: str,
    ctrs: LifecycleCounters,
) -> None:
    canon_table = _CANONICAL_TABLE[entity_type]
    cand_table = _CANDIDATE_TABLE[entity_type]

    # 1. Verify both canonicals exist.
    if conn.execute(
        f"SELECT id FROM {canon_table} WHERE id = %s", (keep_id,)
    ).fetchone() is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"merge {entity_type}: keep_canonical_id {keep_id} not found")
        return

    if conn.execute(
        f"SELECT id FROM {canon_table} WHERE id = %s", (merge_id,)
    ).fetchone() is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"merge {entity_type}: merge_canonical_id {merge_id} not found")
        return

    if keep_id == merge_id:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"merge {entity_type}: keep_id == merge_id ({keep_id})")
        return

    # 2. Find candidates linked to merge_id (before relinking).
    linked_rows = conn.execute(
        """
        SELECT candidate_entity_id
        FROM candidate_canonical_link
        WHERE candidate_entity_type = %s AND canonical_entity_id = %s
        """,
        (entity_type, merge_id),
    ).fetchall()
    linked_candidate_ids = [str(r[0]) for r in linked_rows]

    # Guard: a canonical with no linked candidates is inconsistent state.
    # resolution_manual_action_log.candidate_entity_id is NOT NULL, so logging
    # requires at least one candidate.  Reject as invalid; the orphan canonical
    # should be investigated and cleaned up separately.
    if not linked_candidate_ids:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(
            f"merge {entity_type}: merge_canonical_id {merge_id} has no linked candidates; "
            "merge rejected (orphan canonical — clean up separately)"
        )
        return

    provenance_candidate_id = linked_candidate_ids[0]

    # 3. Relink all merge candidates to keep (linked_candidate_ids always non-empty here).
    conn.execute(
        """
        UPDATE candidate_canonical_link
        SET canonical_entity_id = %s
        WHERE candidate_entity_type = %s AND canonical_entity_id = %s
        """,
        (keep_id, entity_type, merge_id),
    )
    conn.execute(
        f"""
        UPDATE {cand_table}
        SET promoted_canonical_id = %s
        WHERE promoted_canonical_id = %s
        """,
        (keep_id, merge_id),
    )

    # 4 & 5. Fill-nulls-only survivorship + write provenance for changed attrs.
    attrs = _PROVENANCE_ATTRS.get(entity_type, [])
    if attrs and provenance_candidate_id:
        attr_list = ", ".join(attrs)
        keep_vals = conn.execute(
            f"SELECT {attr_list} FROM {canon_table} WHERE id = %s", (keep_id,)
        ).fetchone()
        merge_vals = conn.execute(
            f"SELECT {attr_list} FROM {canon_table} WHERE id = %s", (merge_id,)
        ).fetchone()

        fill_attrs = [
            (attr, mv)
            for attr, kv, mv in zip(attrs, keep_vals, merge_vals)
            if kv is None and mv is not None
        ]

        if fill_attrs:
            set_clauses = ", ".join(f"{a} = %s" for a, _ in fill_attrs)
            fill_vals = [v for _, v in fill_attrs]
            conn.execute(
                f"UPDATE {canon_table} SET {set_clauses} WHERE id = %s",
                (*fill_vals, keep_id),
            )
            # Write provenance for each newly-filled attribute.
            for attr, _ in fill_attrs:
                row = conn.execute(
                    f"SELECT {attr} FROM {canon_table} WHERE id = %s", (keep_id,)
                ).fetchone()
                str_val = str(row[0]) if row and row[0] is not None else None
                conn.execute(
                    """
                    INSERT INTO canonical_attribute_provenance
                        (canonical_entity_type, canonical_entity_id, attribute_name,
                         attribute_value, source_candidate_type, source_candidate_id,
                         decided_by)
                    VALUES (%s, %s, %s, %s, %s, %s, 'merge')
                    ON CONFLICT (canonical_entity_type, canonical_entity_id, attribute_name)
                    DO UPDATE SET
                        attribute_value       = EXCLUDED.attribute_value,
                        source_candidate_type = EXCLUDED.source_candidate_type,
                        source_candidate_id   = EXCLUDED.source_candidate_id,
                        decided_by            = EXCLUDED.decided_by,
                        decided_at            = now()
                    """,
                    (entity_type, keep_id, attr, str_val,
                     entity_type, provenance_candidate_id),
                )

    # 5b. Reroute canonical_registration FK references from merge_id → keep_id.
    _migrate_canonical_refs(conn, entity_type, merge_id, keep_id)

    # 6. Delete the merge canonical (+ its provenance rows).
    _delete_canonical_with_provenance(conn, entity_type, merge_id)

    # 7. Dismiss open NBAs for all relinked candidates.
    for cid in linked_candidate_ids:
        _dismiss_nbas_for_candidate(conn, entity_type, cid)

    # 8. Log audit (one entry per merge row; provenance_candidate_id is always non-None
    #    here because the orphan-merge guard above returns early when linked_candidate_ids
    #    is empty).
    _log_lifecycle_action(
        conn, entity_type, provenance_candidate_id, keep_id, "merge", reason_code, actor
    )

    ctrs.rows_applied += 1


# ---------------------------------------------------------------------------
# Demote
# ---------------------------------------------------------------------------

def _apply_demote(
    conn: psycopg.Connection,
    entity_type: str,
    candidate_id: str,
    reason_code: str,
    actor: str,
    ctrs: LifecycleCounters,
) -> None:
    cand_table = _CANDIDATE_TABLE[entity_type]

    # 1. Fetch candidate state.
    row = conn.execute(
        f"SELECT is_promoted, promoted_canonical_id FROM {cand_table} WHERE id = %s",
        (candidate_id,),
    ).fetchone()
    if row is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"demote {entity_type} {candidate_id}: candidate not found")
        return

    is_promoted = row[0]
    canonical_id = str(row[1]) if row[1] is not None else None

    # 2. Guard: must be promoted.
    if not is_promoted:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"demote {entity_type} {candidate_id}: not promoted, skipped")
        return

    if canonical_id is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(
            f"demote {entity_type} {candidate_id}: is_promoted=true but promoted_canonical_id is NULL"
        )
        return

    # 4. Count all links to this canonical BEFORE deleting.
    link_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM candidate_canonical_link
        WHERE candidate_entity_type = %s AND canonical_entity_id = %s
        """,
        (entity_type, canonical_id),
    ).fetchone()[0]

    # 5. Delete this candidate's link.
    conn.execute(
        """
        DELETE FROM candidate_canonical_link
        WHERE candidate_entity_type = %s AND candidate_entity_id = %s
        """,
        (entity_type, candidate_id),
    )

    # 6. Reset candidate state (single UPDATE → trigger Rule 1 bypassed via is_promoted=false).
    conn.execute(
        f"""
        UPDATE {cand_table}
        SET is_promoted           = false,
            promoted_canonical_id = NULL,
            resolution_state      = 'review'
        WHERE id = %s
        """,
        (candidate_id,),
    )

    # 7. If this was the sole linked candidate, delete the now-orphaned canonical.
    if link_count == 1:
        _migrate_canonical_refs(conn, entity_type, canonical_id, None)
        _delete_canonical_with_provenance(conn, entity_type, canonical_id)

    # 8. Dismiss open NBAs for this candidate.
    _dismiss_nbas_for_candidate(conn, entity_type, candidate_id)

    # 9. Log.
    logged_canonical_id = None if link_count == 1 else canonical_id
    _log_lifecycle_action(
        conn, entity_type, candidate_id, logged_canonical_id, "demote", reason_code, actor
    )
    ctrs.rows_applied += 1


# ---------------------------------------------------------------------------
# Unlink
# ---------------------------------------------------------------------------

def _apply_unlink(
    conn: psycopg.Connection,
    entity_type: str,
    candidate_id: str,
    reason_code: str,
    actor: str,
    ctrs: LifecycleCounters,
) -> None:
    cand_table = _CANDIDATE_TABLE[entity_type]

    # 1. Fetch candidate state.
    row = conn.execute(
        f"SELECT is_promoted, promoted_canonical_id FROM {cand_table} WHERE id = %s",
        (candidate_id,),
    ).fetchone()
    if row is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"unlink {entity_type} {candidate_id}: candidate not found")
        return

    is_promoted = row[0]
    canonical_id = str(row[1]) if row[1] is not None else None

    if not is_promoted:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(f"unlink {entity_type} {candidate_id}: not promoted, skipped")
        return

    if canonical_id is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(
            f"unlink {entity_type} {candidate_id}: is_promoted=true but promoted_canonical_id is NULL"
        )
        return

    # 5. Delete this candidate's link (canonical always preserved — differs from demote).
    conn.execute(
        """
        DELETE FROM candidate_canonical_link
        WHERE candidate_entity_type = %s AND candidate_entity_id = %s
        """,
        (entity_type, candidate_id),
    )

    # 6. Reset candidate state (single UPDATE → trigger Rule 1 bypassed via is_promoted=false).
    conn.execute(
        f"""
        UPDATE {cand_table}
        SET is_promoted           = false,
            promoted_canonical_id = NULL,
            resolution_state      = 'review'
        WHERE id = %s
        """,
        (candidate_id,),
    )

    # Note: canonical preserved regardless of remaining link count.
    # Note: NBAs preserved; candidate returns to review and may need enrichment.

    # 9. Log.
    _log_lifecycle_action(
        conn, entity_type, candidate_id, canonical_id, "unlink", reason_code, actor
    )
    ctrs.rows_applied += 1


# ---------------------------------------------------------------------------
# Split
# ---------------------------------------------------------------------------

def _apply_split(
    conn: psycopg.Connection,
    entity_type: str,
    old_canonical_id: str,
    candidate_ids: list[str],
    reason_code: str,
    actor: str,
    ctrs: LifecycleCounters,
) -> None:
    canon_table = _CANONICAL_TABLE[entity_type]
    cand_table = _CANDIDATE_TABLE[entity_type]

    # Verify old canonical exists.
    if conn.execute(
        f"SELECT id FROM {canon_table} WHERE id = %s", (old_canonical_id,)
    ).fetchone() is None:
        for cid in candidate_ids:
            ctrs.rows_invalid += 1
            ctrs.warnings.append(
                f"split {entity_type}: old_canonical_id {old_canonical_id} not found "
                f"(candidate {cid})"
            )
        return

    # 1. Validate each candidate is linked to old_canonical_id.
    valid_candidate_ids: list[str] = []
    for cid in candidate_ids:
        link = conn.execute(
            """
            SELECT candidate_entity_id
            FROM candidate_canonical_link
            WHERE candidate_entity_type = %s
              AND candidate_entity_id   = %s
              AND canonical_entity_id   = %s
            """,
            (entity_type, cid, old_canonical_id),
        ).fetchone()
        if link is None:
            ctrs.rows_invalid += 1
            ctrs.warnings.append(
                f"split {entity_type} candidate {cid}: not linked to {old_canonical_id}"
            )
        else:
            valid_candidate_ids.append(cid)

    if not valid_candidate_ids:
        return

    # 2. Clone the old canonical row → new_canonical_id.
    clone_cols = _CLONE_COLS[entity_type]
    new_row = conn.execute(
        f"""
        INSERT INTO {canon_table} ({clone_cols})
        SELECT {clone_cols}
        FROM {canon_table}
        WHERE id = %s
        RETURNING id
        """,
        (old_canonical_id,),
    ).fetchone()
    new_canonical_id = str(new_row[0])

    # 3. Relink valid candidates to new canonical.
    for cid in valid_candidate_ids:
        conn.execute(
            """
            UPDATE candidate_canonical_link
            SET canonical_entity_id = %s
            WHERE candidate_entity_type = %s AND candidate_entity_id = %s
            """,
            (new_canonical_id, entity_type, cid),
        )
        conn.execute(
            f"UPDATE {cand_table} SET promoted_canonical_id = %s WHERE id = %s",
            (new_canonical_id, cid),
        )

    # 4. Write provenance for new canonical (clone semantics → decided_by='merge').
    _write_provenance(
        conn,
        entity_type=entity_type,
        canonical_id=new_canonical_id,
        candidate_id=valid_candidate_ids[0],
        candidate_score=None,
        rule_version=None,
        decided_by="merge",
    )

    # 5. Log audit (one entry per split group, against old canonical).
    _log_lifecycle_action(
        conn, entity_type, valid_candidate_ids[0], old_canonical_id,
        "split", reason_code, actor,
    )

    # 6. Increment rows_applied per valid split candidate.
    ctrs.rows_applied += len(valid_candidate_ids)


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def run_lifecycle(
    conn: psycopg.Connection,
    decisions_path: "Path | str",
    lifecycle_op: str,
    dry_run: bool = False,
) -> LifecycleCounters:
    """Apply lifecycle operations from a CSV file.

    Args:
        conn: Open psycopg connection (caller manages transaction).
        decisions_path: Path to input CSV file.
        lifecycle_op: One of 'merge', 'demote', 'unlink', 'split'.
        dry_run: If True, caller should ROLLBACK after this returns.

    Returns:
        LifecycleCounters with run statistics.
    """
    ctrs = LifecycleCounters()
    path = Path(decisions_path)
    required_cols = _REQUIRED_COLS_BY_OP[lifecycle_op]

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"lifecycle CSV is empty or has no header: {path}")
        missing = required_cols - set(reader.fieldnames)
        if missing:
            raise ValueError(
                f"lifecycle CSV ({lifecycle_op}) missing required columns: {sorted(missing)}"
            )
        rows = list(reader)

    if lifecycle_op == "split":
        return _run_split(conn, rows, ctrs)

    # merge / demote / unlink — one SAVEPOINT per row
    for idx, raw_row in enumerate(rows):
        ctrs.rows_read += 1
        sp = f"lifecycle_{lifecycle_op}_{idx}"
        conn.execute(f"SAVEPOINT {sp}")
        try:
            invalid = _dispatch_row(conn, lifecycle_op, idx, raw_row, ctrs)
            if not invalid:
                conn.execute(f"RELEASE SAVEPOINT {sp}")
            else:
                conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            ctrs.db_errors += 1
            ctrs.warnings.append(f"{lifecycle_op} row {idx}: {exc}")

    return ctrs


def _dispatch_row(
    conn: psycopg.Connection,
    lifecycle_op: str,
    idx: int,
    raw_row: dict,
    ctrs: LifecycleCounters,
) -> bool:
    """Dispatch one CSV row to the appropriate apply function.

    Returns True if the row was rejected as invalid (for caller bookkeeping).
    """
    if lifecycle_op == "merge":
        entity_type = (raw_row.get("canonical_entity_type") or "").strip().lower()
        keep_id = (raw_row.get("keep_canonical_id") or "").strip()
        merge_id = (raw_row.get("merge_canonical_id") or "").strip()
        reason = (raw_row.get("reason_code") or "").strip()
        actor = (raw_row.get("actor") or "").strip()

        if not entity_type or not keep_id or not merge_id or not actor:
            ctrs.rows_invalid += 1
            ctrs.warnings.append(f"merge row {idx}: missing required field(s)")
            return True
        if entity_type not in _VALID_ENTITY_TYPES:
            ctrs.rows_invalid += 1
            ctrs.warnings.append(f"merge row {idx}: unknown entity_type={entity_type!r}")
            return True
        _apply_merge(conn, entity_type, keep_id, merge_id, reason, actor, ctrs)

    else:  # demote | unlink
        entity_type = (raw_row.get("candidate_entity_type") or "").strip().lower()
        cand_id = (raw_row.get("candidate_entity_id") or "").strip()
        reason = (raw_row.get("reason_code") or "").strip()
        actor = (raw_row.get("actor") or "").strip()

        if not entity_type or not cand_id or not actor:
            ctrs.rows_invalid += 1
            ctrs.warnings.append(f"{lifecycle_op} row {idx}: missing required field(s)")
            return True
        if entity_type not in _VALID_ENTITY_TYPES:
            ctrs.rows_invalid += 1
            ctrs.warnings.append(f"{lifecycle_op} row {idx}: unknown entity_type={entity_type!r}")
            return True

        if lifecycle_op == "demote":
            _apply_demote(conn, entity_type, cand_id, reason, actor, ctrs)
        else:
            _apply_unlink(conn, entity_type, cand_id, reason, actor, ctrs)

    return False


def _run_split(
    conn: psycopg.Connection,
    rows: list[dict],
    ctrs: LifecycleCounters,
) -> LifecycleCounters:
    """Handle split: batch rows by (entity_type, old_canonical_id); one SAVEPOINT per group."""
    # Groups keyed by (entity_type, old_canonical_id); value: list of (candidate_id, reason, actor).
    groups: dict[tuple[str, str], list[tuple[str, str, str]]] = defaultdict(list)

    for raw_row in rows:
        ctrs.rows_read += 1
        entity_type = (raw_row.get("canonical_entity_type") or "").strip().lower()
        old_id = (raw_row.get("old_canonical_id") or "").strip()
        cand_id = (raw_row.get("candidate_entity_id") or "").strip()
        reason = (raw_row.get("reason_code") or "").strip()
        actor = (raw_row.get("actor") or "").strip()

        if not entity_type or not old_id or not cand_id or not actor:
            ctrs.rows_invalid += 1
            ctrs.warnings.append("split row: missing required field(s)")
            continue
        if entity_type not in _VALID_ENTITY_TYPES:
            ctrs.rows_invalid += 1
            ctrs.warnings.append(f"split row: unknown entity_type={entity_type!r}")
            continue
        groups[(entity_type, old_id)].append((cand_id, reason, actor))

    for gidx, ((entity_type, old_id), cand_tuples) in enumerate(groups.items()):
        sp = f"split_grp_{gidx}"
        # Use reason/actor from first row of group.
        reason = cand_tuples[0][1]
        actor = cand_tuples[0][2]
        cand_ids = [t[0] for t in cand_tuples]

        conn.execute(f"SAVEPOINT {sp}")
        try:
            _apply_split(conn, entity_type, old_id, cand_ids, reason, actor, ctrs)
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception as exc:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            ctrs.db_errors += 1
            ctrs.warnings.append(f"split {entity_type} {old_id}: {exc}")

    return ctrs


def build_lifecycle_report(ctrs: LifecycleCounters, dry_run: bool = False) -> str:
    lines = [
        "=" * 60,
        "Resolution Lifecycle Operation Report",
        f"  dry_run: {dry_run}",
        "=" * 60,
        f"  rows read:      {ctrs.rows_read}",
        f"  rows applied:   {ctrs.rows_applied}",
        f"  rows invalid:   {ctrs.rows_invalid}",
        f"  rows skipped:   {ctrs.rows_skipped}",
        f"DB errors:        {ctrs.db_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")
    lines.append("=" * 60)
    return "\n".join(lines)
