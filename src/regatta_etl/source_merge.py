"""regatta_etl.source_merge

Source-layer yacht merge pipeline.

Reads a CSV of human-confirmed duplicate yacht source records and merges each
drop_id into keep_id at the source layer, without touching canonical tables.

Scope: yacht only (FOR-203). Participant and club merges are deferred until
the yacht workflow is proven safe.

Input CSV format (comma-delimited, header row required):
    entity_type, keep_id, drop_id, reason, actor

Required columns: entity_type, keep_id, drop_id, actor
Optional:         reason (defaults to 'source_merge')

Valid entity_types: yacht

Per-row merge steps (all within a SAVEPOINT):
  1. Re-point all source-layer FK references from drop_id → keep_id
  2. Re-point candidate_source_link.source_row_pk from drop_id → keep_id
  3. Explicitly clean up the stale drop candidate:
     - If the drop candidate has no remaining source links, set its
       resolution_state = 'reject' so it is excluded from future promotion.
     - If the drop candidate was already promoted, unlink it from its canonical
       record (delete candidate_canonical_link) and log that action separately.
       The canonical record itself is NOT deleted — that requires a separate
       lifecycle operation and human review.
  4. Delete the drop_id source row
  5. Write resolution_manual_action_log (source='pipeline') with action_type='merge'
     and before_payload / after_payload JSON snapshots

Conflict handling:
  - event_entry has UNIQUE(event_instance_id, yacht_id): if keep already has
    a row for the same event_instance, the drop row is deleted (keep covers it).
  - candidate_source_link has UNIQUE(entity_type, entity_id, table, row_pk):
    conflicting drop links are deleted before re-pointing keep.

Idempotency:
  If drop_id no longer exists (already merged), the row is skipped with a warning.

Usage:
    python -m regatta_etl.source_merge \\
        --input artifacts/qa/yacht_merges.csv \\
        --db-dsn "$DB_DSN" \\
        [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
import psycopg.rows


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Participant and club merges are deferred until the yacht workflow is proven.
_VALID_ENTITY_TYPES = frozenset({"yacht"})
_REQUIRED_COLS = frozenset({"entity_type", "keep_id", "drop_id", "actor"})

_SOURCE_TABLE = {
    "yacht":       "yacht",
    "club":        "yacht_club",
    "participant": "participant",
}

# FK tables that reference each source entity (table, column).
# Order matters: child rows must be re-pointed before the parent is deleted.
# Entries with a 3rd element are (table, column, conflict_strategy):
#   'update'  — plain UPDATE; no unique conflict expected
#   'dedup'   — delete conflicting drop rows first, then UPDATE
_YACHT_FK_TABLES: list[tuple[str, str, str]] = [
    ("yacht_ownership",                "yacht_id",  "update"),
    ("yacht_rating",                   "yacht_id",  "update"),
    ("event_entry",                    "yacht_id",  "dedup"),   # UNIQUE(event_instance_id, yacht_id)
    ("airtable_xref_yacht",            "yacht_id",  "update"),
    ("yacht_scoring_xref_yacht",       "yacht_id",  "update"),
    ("regattaman_snapshot_xref_yacht", "yacht_id",  "update"),
]

_CLUB_FK_TABLES: list[tuple[str, str, str]] = [
    ("event_series",        "yacht_club_id", "update"),
    ("club_membership",     "yacht_club_id", "update"),
    ("airtable_xref_club",  "yacht_club_id", "update"),
]

_PARTICIPANT_FK_TABLES: list[tuple[str, str, str]] = [
    ("participant_contact_point",              "participant_id", "update"),
    ("participant_address",                    "participant_id", "update"),
    ("participant_related_contact",            "participant_id", "update"),
    ("club_membership",                        "participant_id", "update"),
    ("yacht_ownership",                        "participant_id", "update"),
    ("event_entry_participant",                "participant_id", "dedup"),  # UNIQUE(event_entry_id, participant_id, role)
    ("mailchimp_contact_state",                "participant_id", "update"),
    ("mailchimp_contact_tag",                  "participant_id", "update"),
    ("participant_mailchimp_identity",         "participant_id", "dedup"),  # UNIQUE(participant_id, email_normalized)
    ("mailchimp_identity_review_queue",        "candidate_participant_id", "update"),
    ("airtable_xref_participant",              "participant_id", "update"),
    ("yacht_scoring_xref_participant",         "participant_id", "update"),
    ("bhyc_member_xref_participant",           "participant_id", "update"),
    ("regattaman_snapshot_xref_participant",   "participant_id", "update"),
]

_FK_MAP: dict[str, list[tuple[str, str, str]]] = {
    "yacht":       _YACHT_FK_TABLES,
    "club":        _CLUB_FK_TABLES,
    "participant": _PARTICIPANT_FK_TABLES,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sp(name: str) -> str:
    """Return a valid unquoted SQL savepoint identifier.

    PostgreSQL savepoint names are parsed as bare identifiers, so they must not
    contain hyphens. UUIDs use hyphens; this strips them to produce safe names.
    """
    return name.replace("-", "")


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class SourceMergeCounters:
    rows_read: int = 0
    rows_merged: int = 0
    rows_skipped_missing: int = 0
    rows_invalid: int = 0
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_read":             self.rows_read,
            "rows_merged":           self.rows_merged,
            "rows_skipped_missing":  self.rows_skipped_missing,
            "rows_invalid":          self.rows_invalid,
            "db_errors":             self.db_errors,
            "warnings":              self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _snapshot_row(
    conn: psycopg.Connection,
    table: str,
    row_id: str,
) -> dict | None:
    """Return a JSON-serialisable snapshot of a source row, or None if missing."""
    row = conn.execute(
        f"SELECT * FROM {table} WHERE id = %s",
        (row_id,),
    ).fetchone()
    if row is None:
        return None
    # psycopg dict_row returns a dict-like; convert to plain dict for JSON.
    return {k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v)
            for k, v in row.items()}


def _log_merge(
    conn: psycopg.Connection,
    entity_type: str,
    keep_id: str,
    drop_id: str,
    before_payload: dict,
    after_payload: dict,
    reason: str,
    actor: str,
) -> None:
    conn.execute(
        """
        INSERT INTO resolution_manual_action_log
            (entity_type, candidate_entity_id, canonical_entity_id,
             action_type, before_payload, after_payload,
             reason_code, actor, source)
        VALUES (%s, %s::uuid, %s::uuid, 'merge', %s, %s, %s, %s, 'pipeline')
        """,
        (
            entity_type,
            drop_id,
            keep_id,
            json.dumps(before_payload),
            json.dumps(after_payload),
            reason,
            actor,
        ),
    )


def _repoint_source_links(
    conn: psycopg.Connection,
    entity_type: str,
    source_table: str,
    keep_id: str,
    drop_id: str,
    survivor_candidate_ids: list[str],
) -> None:
    """Move source lineage for the merged row onto the surviving candidate(s).

    Links for loser candidates must be deleted, not re-pointed, otherwise those
    candidates never become sourceless and the stale-candidate cleanup path can
    never fire. When the keep source row already has candidate(s), those are the
    survivors. If it has none, the caller passes the best former drop candidate
    so one candidate keeps lineage to the surviving source row.
    """
    if not survivor_candidate_ids:
        conn.execute(
            """
            DELETE FROM candidate_source_link
            WHERE candidate_entity_type = %s
              AND source_table_name     = %s
              AND source_row_pk         = %s
            """,
            (entity_type, source_table, drop_id),
        )
        return

    # Delete drop-row links for candidates that should not retain lineage.
    conn.execute(
        """
        DELETE FROM candidate_source_link
        WHERE candidate_entity_type = %s
          AND source_table_name     = %s
          AND source_row_pk         = %s
          AND NOT (candidate_entity_id = ANY(%s::uuid[]))
        """,
        (entity_type, source_table, drop_id, survivor_candidate_ids),
    )

    # Delete survivor drop links that would conflict after re-pointing.
    conn.execute(
        """
        DELETE FROM candidate_source_link drop_link
        USING candidate_source_link keep_link
        WHERE drop_link.candidate_entity_type = %s
          AND drop_link.source_table_name     = %s
          AND drop_link.source_row_pk         = %s
          AND keep_link.candidate_entity_type = drop_link.candidate_entity_type
          AND keep_link.candidate_entity_id   = drop_link.candidate_entity_id
          AND keep_link.source_table_name     = %s
          AND keep_link.source_row_pk         = %s
          AND drop_link.candidate_entity_id   = ANY(%s::uuid[])
        """,
        (entity_type, source_table, drop_id, source_table, keep_id, survivor_candidate_ids),
    )

    # Re-point the survivor drop-row links to the surviving source row.
    conn.execute(
        """
        UPDATE candidate_source_link
        SET source_row_pk = %s
        WHERE candidate_entity_type = %s
          AND source_table_name     = %s
          AND source_row_pk         = %s
          AND candidate_entity_id   = ANY(%s::uuid[])
        """,
        (keep_id, entity_type, source_table, drop_id, survivor_candidate_ids),
    )


def _find_all_candidates_for_source(
    conn: psycopg.Connection,
    entity_type: str,
    source_table: str,
    source_row_pk: str,
) -> list[str]:
    """Return all candidate_entity_ids linked to a source row, best-quality first.

    A single source row can link to more than one candidate (e.g. when the same
    source yacht was ingested via multiple paths and fingerprint-based dedup
    produced distinct candidates). We order by quality_score DESC so callers can
    use [0] as the "best" candidate and treat the rest as secondary.
    """
    rows = conn.execute(
        """
        SELECT csl.candidate_entity_id
        FROM candidate_source_link csl
        JOIN candidate_yacht cy ON cy.id = csl.candidate_entity_id
        WHERE csl.candidate_entity_type = %s
          AND csl.source_table_name     = %s
          AND csl.source_row_pk         = %s
        ORDER BY cy.quality_score DESC NULLS LAST, cy.created_at ASC
        """,
        (entity_type, source_table, source_row_pk),
    ).fetchall()
    return [str(r["candidate_entity_id"]) for r in rows]


def _cleanup_stale_candidate(
    conn: psycopg.Connection,
    entity_type: str,
    drop_candidate_id: str,
    keep_candidate_id: str | None,
    actor: str,
    reason: str,
    warnings: list[str],
) -> None:
    """After re-pointing source links away from the drop source row, clean up
    the specific drop candidate if it is now sourceless.

    Only operates on drop_candidate_id — never touches unrelated candidates.

    Two cases:
      A) Drop candidate has no remaining source links → mark rejected so it
         is excluded from future promotion runs.
      B) Drop candidate was already promoted → unlink it from its canonical
         record (delete candidate_canonical_link). The canonical row is NOT
         deleted here; that requires a separate lifecycle decision. A warning
         is emitted so the operator knows manual follow-up is needed.

    Also re-points candidate_registration.candidate_yacht_id from the drop
    candidate to the keep candidate so registration candidates stay consistent.
    """
    row = conn.execute(
        """
        SELECT cy.id, cy.is_promoted, cy.resolution_state
        FROM candidate_yacht cy
        WHERE cy.id = %s::uuid
          AND NOT EXISTS (
              SELECT 1 FROM candidate_source_link csl
              WHERE csl.candidate_entity_type = 'yacht'
                AND csl.candidate_entity_id   = cy.id
          )
        """,
        (drop_candidate_id,),
    ).fetchone()

    if row is None:
        # Drop candidate still has source links (shared with keep) — not stale.
        return

    cid = str(row["id"])
    is_promoted = row["is_promoted"]
    current_state = row["resolution_state"]

    # Re-point candidate_registration.candidate_yacht_id before rejecting.
    # candidate_registration.stable_fingerprint = sha256(event_id|ext_id|yacht_id),
    # so a plain UPDATE of candidate_yacht_id would leave the fingerprint stale.
    # We must recompute the fingerprint and detect collisions explicitly rather
    # than relying on UniqueViolation (which fires on the fingerprint column,
    # not the logical event+ext_id match).
    if keep_candidate_id:
        from regatta_etl.resolution_source_to_candidate import registration_fingerprint

        reg_rows = conn.execute(
            """
            SELECT id, candidate_event_id, registration_external_id
            FROM candidate_registration
            WHERE candidate_yacht_id = %s::uuid
            """,
            (cid,),
        ).fetchall()
        for reg_row in reg_rows:
            reg_id   = str(reg_row["id"])
            event_id = str(reg_row["candidate_event_id"])
            ext_id   = reg_row["registration_external_id"]

            new_fp = registration_fingerprint(event_id, ext_id, keep_candidate_id)

            # Check whether a registration already exists with the new fingerprint.
            collision = conn.execute(
                "SELECT id FROM candidate_registration WHERE stable_fingerprint = %s",
                (new_fp,),
            ).fetchone()

            if collision:
                # Keep already covers this event+yacht combination.
                # Before deleting the drop registration, re-point any
                # candidate_participant_role_assignment rows that FK to it.
                # candidate_registration_id is nullable with no ON DELETE CASCADE,
                # so a plain DELETE would either fail on FK violation or leave
                # orphaned role context.
                surviving_reg_id = str(collision["id"])
                role_rows = conn.execute(
                    "SELECT id, candidate_participant_id, role, candidate_event_id "
                    "FROM candidate_participant_role_assignment "
                    "WHERE candidate_registration_id = %s::uuid",
                    (reg_id,),
                ).fetchall()
                for role_row in role_rows:
                    role_asgn_id  = str(role_row["id"])
                    part_id       = str(role_row["candidate_participant_id"])
                    role_val      = role_row["role"]
                    evt_id        = str(role_row["candidate_event_id"]) if role_row["candidate_event_id"] else None
                    sp_role       = _sp(f"role_{role_asgn_id}")
                    conn.execute(f"SAVEPOINT {sp_role}")
                    try:
                        conn.execute(
                            "UPDATE candidate_participant_role_assignment "
                            "SET candidate_registration_id = %s::uuid "
                            "WHERE id = %s::uuid",
                            (surviving_reg_id, role_asgn_id),
                        )
                        conn.execute(f"RELEASE SAVEPOINT {sp_role}")
                    except psycopg.errors.UniqueViolation:
                        # Surviving registration already has this participant+role —
                        # drop the duplicate role assignment.
                        conn.execute(f"ROLLBACK TO SAVEPOINT {sp_role}")
                        conn.execute(f"RELEASE SAVEPOINT {sp_role}")
                        conn.execute(
                            "DELETE FROM candidate_participant_role_assignment "
                            "WHERE id = %s::uuid",
                            (role_asgn_id,),
                        )
                # All role rows handled — now safe to delete the drop registration.
                conn.execute(
                    "DELETE FROM candidate_registration WHERE id = %s::uuid",
                    (reg_id,),
                )
            else:
                # Safe to re-point: update both the FK and the fingerprint.
                conn.execute(
                    """
                    UPDATE candidate_registration
                    SET candidate_yacht_id  = %s::uuid,
                        stable_fingerprint  = %s
                    WHERE id = %s::uuid
                    """,
                    (keep_candidate_id, new_fp, reg_id),
                )

    if is_promoted:
        # Unlink from canonical — do NOT delete the canonical row.
        conn.execute(
            "DELETE FROM candidate_canonical_link "
            "WHERE candidate_entity_type = %s AND candidate_entity_id = %s::uuid",
            (entity_type, cid),
        )
        conn.execute(
            "UPDATE candidate_yacht SET is_promoted = false, "
            "promoted_canonical_id = NULL, resolution_state = 'reject' "
            "WHERE id = %s::uuid",
            (cid,),
        )
        conn.execute(
            """
            INSERT INTO resolution_manual_action_log
                (entity_type, candidate_entity_id, canonical_entity_id,
                 action_type, reason_code, actor, source)
            VALUES (%s, %s::uuid, NULL, 'unlink', %s, %s, 'pipeline')
            """,
            (entity_type, cid, reason, actor),
        )
        warnings.append(
            f"source_merge: stale promoted candidate {cid} ({entity_type}) "
            f"was unlinked from its canonical record. "
            f"Review the orphaned canonical_{entity_type} row manually."
        )
    elif current_state != "reject":
        conn.execute(
            "UPDATE candidate_yacht SET resolution_state = 'reject' "
            "WHERE id = %s::uuid",
            (cid,),
        )


def _repoint_fks(
    conn: psycopg.Connection,
    entity_type: str,
    keep_id: str,
    drop_id: str,
) -> None:
    """Re-point all source-layer FK references from drop_id → keep_id."""
    fk_tables = _FK_MAP[entity_type]

    for table, column, strategy in fk_tables:
        if strategy == "dedup":
            # For tables with unique constraints that would conflict:
            # delete the drop row when keep already covers it.
            # We identify conflicts by looking for a keep row whose other
            # indexed columns match the drop row we want to re-point.
            # Rather than hard-coding the index columns per table, we use a
            # generic approach: attempt the UPDATE and catch the unique
            # violation using a nested SAVEPOINT.
            rows = conn.execute(
                f"SELECT id FROM {table} WHERE {column} = %s::uuid",
                (drop_id,),
            ).fetchall()
            for (row_id,) in rows:
                sp = _sp(f"dedup_{table}_{row_id}")
                conn.execute(f"SAVEPOINT {sp}")
                try:
                    conn.execute(
                        f"UPDATE {table} SET {column} = %s::uuid WHERE id = %s::uuid",
                        (keep_id, row_id),
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                except psycopg.errors.UniqueViolation:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    # keep already covers this row; drop it.
                    conn.execute(
                        f"DELETE FROM {table} WHERE id = %s::uuid",
                        (row_id,),
                    )
        else:
            conn.execute(
                f"UPDATE {table} SET {column} = %s::uuid WHERE {column} = %s::uuid",
                (keep_id, drop_id),
            )


# ---------------------------------------------------------------------------
# Per-row merge
# ---------------------------------------------------------------------------

def _apply_merge(
    conn: psycopg.Connection,
    entity_type: str,
    keep_id: str,
    drop_id: str,
    reason: str,
    actor: str,
    ctrs: SourceMergeCounters,
) -> None:
    source_table = _SOURCE_TABLE[entity_type]

    # Snapshot both rows before any changes.
    before_snap = _snapshot_row(conn, source_table, drop_id)
    if before_snap is None:
        ctrs.rows_skipped_missing += 1
        ctrs.warnings.append(
            f"merge {entity_type} drop={drop_id}: source row not found — already merged or invalid"
        )
        return

    after_snap = _snapshot_row(conn, source_table, keep_id)
    if after_snap is None:
        ctrs.rows_invalid += 1
        ctrs.warnings.append(
            f"merge {entity_type} keep={keep_id}: keep row not found — skipping"
        )
        return

    # Look up all candidates for each source row BEFORE re-pointing.
    # A single source row may link to multiple candidates; we collect them all
    # so cleanup is applied to every stale drop candidate, not just the first.
    # Best-quality candidate is first in each list.
    drop_candidate_ids = _find_all_candidates_for_source(conn, entity_type, source_table, drop_id)
    keep_candidate_ids = _find_all_candidates_for_source(conn, entity_type, source_table, keep_id)
    if keep_candidate_ids:
        survivor_candidate_ids = keep_candidate_ids
        best_keep_candidate_id = keep_candidate_ids[0]
    elif drop_candidate_ids:
        survivor_candidate_ids = [drop_candidate_ids[0]]
        best_keep_candidate_id = drop_candidate_ids[0]
    else:
        survivor_candidate_ids = []
        best_keep_candidate_id = None

    # Re-point source-layer FKs.
    _repoint_fks(conn, entity_type, keep_id, drop_id)

    # Move candidate_source_link lineage to the surviving candidate(s) only.
    _repoint_source_links(
        conn, entity_type, source_table, keep_id, drop_id, survivor_candidate_ids
    )

    # Clean up every stale drop candidate that is now sourceless.
    for drop_cid in drop_candidate_ids:
        if drop_cid in survivor_candidate_ids:
            continue
        _cleanup_stale_candidate(
            conn, entity_type, drop_cid, best_keep_candidate_id,
            actor, reason, ctrs.warnings,
        )

    # Delete the drop source row.
    conn.execute(
        f"DELETE FROM {source_table} WHERE id = %s::uuid",
        (drop_id,),
    )

    # Audit log.
    _log_merge(conn, entity_type, keep_id, drop_id, before_snap, after_snap, reason, actor)

    ctrs.rows_merged += 1


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def run_source_merge(
    conn: psycopg.Connection,
    input_path: Path | str,
    dry_run: bool = False,
) -> SourceMergeCounters:
    """Apply source-layer merges from a CSV file.

    Args:
        conn:       Open psycopg connection (caller manages transaction).
        input_path: Path to CSV with merge decisions.
        dry_run:    If True, caller should ROLLBACK after this returns.

    Returns:
        SourceMergeCounters with run statistics.
    """
    ctrs = SourceMergeCounters()
    path = Path(input_path)

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"source_merge CSV is empty or has no header: {path}")

        # Strip whitespace from column names (common CSV export artifact).
        reader.fieldnames = [f.strip() for f in reader.fieldnames]

        missing = _REQUIRED_COLS - set(reader.fieldnames)
        if missing:
            raise ValueError(
                f"source_merge CSV missing required columns: {sorted(missing)}"
            )

        for idx, raw_row in enumerate(reader):
            ctrs.rows_read += 1
            sp = f"source_merge_{idx}"
            conn.execute(f"SAVEPOINT {sp}")
            try:
                entity_type = (raw_row.get("entity_type") or "").strip().lower()
                keep_id     = (raw_row.get("keep_id")     or "").strip()
                drop_id     = (raw_row.get("drop_id")     or "").strip()
                actor       = (raw_row.get("actor")       or "").strip()
                reason      = (raw_row.get("reason")      or "").strip() or "source_merge"
                qa_action   = (raw_row.get("qa_action")   or "").strip().lower()

                # Skip rows the reviewer marked anything other than 'merge'.
                if qa_action and qa_action != "merge":
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    ctrs.rows_read -= 1  # don't count non-merge rows
                    continue

                if not entity_type or not keep_id or not drop_id or not actor:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(
                        f"row {idx}: missing required field(s) "
                        f"(entity_type={entity_type!r}, keep_id={keep_id!r}, "
                        f"drop_id={drop_id!r}, actor={actor!r})"
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue

                if entity_type not in _VALID_ENTITY_TYPES:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(
                        f"row {idx}: unknown entity_type={entity_type!r}"
                    )
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue

                if keep_id == drop_id:
                    ctrs.rows_invalid += 1
                    ctrs.warnings.append(f"row {idx}: keep_id == drop_id ({keep_id})")
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                    continue

                _apply_merge(conn, entity_type, keep_id, drop_id, reason, actor, ctrs)
                conn.execute(f"RELEASE SAVEPOINT {sp}")

            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                ctrs.db_errors += 1
                ctrs.warnings.append(f"row {idx}: {exc}")

    return ctrs


def build_source_merge_report(ctrs: SourceMergeCounters, dry_run: bool = False) -> str:
    lines = [
        "=" * 60,
        "Source Merge Report",
        f"  dry_run: {dry_run}",
        "=" * 60,
        f"  rows read:               {ctrs.rows_read}",
        f"  rows merged:             {ctrs.rows_merged}",
        f"  skipped (already gone):  {ctrs.rows_skipped_missing}",
        f"  invalid rows:            {ctrs.rows_invalid}",
        f"  DB errors:               {ctrs.db_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")
    if not dry_run and ctrs.rows_merged > 0:
        lines.append(
            "\nNext steps:"
            "\n  1. python -m regatta_etl.resolution_source_to_candidate --db-dsn $DB_DSN"
            "\n  2. python -m regatta_etl.resolution_score --db-dsn $DB_DSN"
            "\n  3. python -m regatta_etl.resolution_promote --db-dsn $DB_DSN"
        )
    lines.append("=" * 60)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Merge duplicate source-layer records (yacht / club / participant)."
    )
    parser.add_argument("--input",   required=True, help="Path to merge decisions CSV")
    parser.add_argument("--db-dsn",  required=True, help="PostgreSQL DSN")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and apply changes but ROLLBACK at the end")
    args = parser.parse_args(argv)

    with psycopg.connect(args.db_dsn, row_factory=psycopg.rows.dict_row) as conn:
        conn.autocommit = False
        ctrs = run_source_merge(conn, args.input, dry_run=args.dry_run)
        print(build_source_merge_report(ctrs, dry_run=args.dry_run))
        if args.dry_run:
            conn.rollback()
        elif ctrs.db_errors == 0:
            conn.commit()
        else:
            conn.commit()  # partial success; errors are row-isolated via SAVEPOINTs
            sys.exit(1)


if __name__ == "__main__":
    main()
