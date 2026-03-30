"""regatta_etl.mcp_helper

Subprocess protocol handler for devMcp regatta curation tools.

Usage:
    python3 -m regatta_etl.mcp_helper <command> '<json_payload>'

Commands:
    find_profile        Search candidate_participant by name or email.
    profile_context     Full context for one candidate_participant.
    preview_decision    Describe what apply_decision would do (read-only).
    apply_decision      Apply promote/reject/hold to a candidate.
    preview_lifecycle   Describe what a lifecycle op would do (read-only).
    apply_lifecycle     Apply merge/demote/unlink/split.
    preview_patch       Describe what writing a factual patch would do.
    apply_patch         Write a factual patch to manual_* tables.
    rerun_resolution    Run source-to-candidate (participant + yacht) for a profile.

Output:  single JSON object to stdout.
Exit 0 on success, 1 on error (error object still on stdout).
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import csv
import os
from pathlib import Path
from typing import Any

import psycopg
import psycopg.rows


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _connect(db_dsn: str) -> psycopg.Connection:
    return psycopg.connect(db_dsn, row_factory=psycopg.rows.dict_row)


def _ok(payload: dict) -> None:
    print(json.dumps({"ok": True, **payload}))
    sys.exit(0)


def _err(message: str, details: dict | None = None) -> None:
    out: dict = {"ok": False, "error": message}
    if details:
        out["details"] = details
    print(json.dumps(out))
    sys.exit(1)


# ---------------------------------------------------------------------------
# find_profile
# ---------------------------------------------------------------------------

def _find_profile(conn: psycopg.Connection, args: dict) -> None:
    query = (args.get("query") or "").strip()
    search_by = args.get("search_by", "name")
    limit = min(int(args.get("limit", 10)), 50)

    if not query:
        _err("query is required")

    if search_by == "email":
        rows = conn.execute(
            """
            SELECT cp.id, cp.display_name, cp.normalized_name, cp.best_email,
                   cp.best_phone, cp.quality_score, cp.resolution_state, cp.is_promoted,
                   (SELECT COUNT(*) FROM candidate_source_link
                    WHERE candidate_entity_type='participant' AND candidate_entity_id=cp.id
                   ) AS source_count
            FROM candidate_participant cp
            WHERE cp.best_email ILIKE %s
            ORDER BY cp.is_promoted DESC, cp.quality_score DESC NULLS LAST
            LIMIT %s
            """,
            (f"%{query}%", limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT cp.id, cp.display_name, cp.normalized_name, cp.best_email,
                   cp.best_phone, cp.quality_score, cp.resolution_state, cp.is_promoted,
                   (SELECT COUNT(*) FROM candidate_source_link
                    WHERE candidate_entity_type='participant' AND candidate_entity_id=cp.id
                   ) AS source_count
            FROM candidate_participant cp
            WHERE cp.normalized_name ILIKE %s
               OR cp.display_name ILIKE %s
            ORDER BY cp.is_promoted DESC, cp.quality_score DESC NULLS LAST
            LIMIT %s
            """,
            (f"%{query}%", f"%{query}%", limit),
        ).fetchall()

    _ok({
        "results": [
            {
                "candidate_participant_id": str(r["id"]),
                "display_name": r["display_name"],
                "normalized_name": r["normalized_name"],
                "best_email": r["best_email"],
                "best_phone": r["best_phone"],
                "quality_score": float(r["quality_score"]) if r["quality_score"] is not None else None,
                "resolution_state": r["resolution_state"],
                "is_promoted": r["is_promoted"],
                "source_count": int(r["source_count"]),
            }
            for r in rows
        ],
        "count": len(rows),
    })


# ---------------------------------------------------------------------------
# profile_context
# ---------------------------------------------------------------------------

def _profile_context(conn: psycopg.Connection, args: dict) -> None:
    cid = (args.get("candidate_participant_id") or "").strip()
    if not cid:
        _err("candidate_participant_id is required")

    p = conn.execute(
        """
        SELECT id, display_name, normalized_name, date_of_birth, best_email, best_phone,
               quality_score, resolution_state, is_promoted, created_at, updated_at
        FROM candidate_participant WHERE id = %s
        """,
        (cid,),
    ).fetchone()
    if not p:
        _err(f"candidate_participant not found: {cid}")

    contacts = conn.execute(
        "SELECT contact_type, raw_value, normalized_value, is_primary "
        "FROM candidate_participant_contact WHERE candidate_participant_id = %s "
        "ORDER BY is_primary DESC, contact_type, created_at",
        (cid,),
    ).fetchall()

    addresses = conn.execute(
        "SELECT address_raw, line1, city, state, postal_code, country_code, is_primary "
        "FROM candidate_participant_address WHERE candidate_participant_id = %s "
        "ORDER BY is_primary DESC, created_at",
        (cid,),
    ).fetchall()

    roles = conn.execute(
        "SELECT role, source_context FROM candidate_participant_role_assignment "
        "WHERE candidate_participant_id = %s ORDER BY role",
        (cid,),
    ).fetchall()

    sources = conn.execute(
        "SELECT source_table_name, source_system, link_score, created_at "
        "FROM candidate_source_link "
        "WHERE candidate_entity_type='participant' AND candidate_entity_id = %s "
        "ORDER BY created_at",
        (cid,),
    ).fetchall()

    canonical_link = conn.execute(
        "SELECT canonical_entity_id, promotion_score, promotion_mode, promoted_at "
        "FROM candidate_canonical_link "
        "WHERE candidate_entity_type='participant' AND candidate_entity_id = %s",
        (cid,),
    ).fetchone()

    # Related yachts: find yachts linked to this candidate via yacht_ownership
    # on the operational tables.  Navigate:
    #   candidate_source_link(participant,cid) → participant.id
    #   → yacht_ownership.yacht_id
    #   → candidate_source_link(yacht) → candidate_yacht
    yachts = conn.execute(
        """
        SELECT DISTINCT cy.id, cy.name, cy.normalized_name, cy.sail_number, cy.yacht_type
        FROM candidate_yacht cy
        JOIN candidate_source_link cyl
          ON cyl.candidate_entity_type = 'yacht'
         AND cyl.candidate_entity_id = cy.id
         AND cyl.source_table_name = 'yacht'
        WHERE cyl.source_row_pk::uuid IN (
            SELECT yo.yacht_id
            FROM candidate_source_link cpl
            JOIN yacht_ownership yo
              ON yo.participant_id = cpl.source_row_pk::uuid
            WHERE cpl.candidate_entity_type = 'participant'
              AND cpl.candidate_entity_id = %s
              AND cpl.source_table_name = 'participant'
        )
        LIMIT 20
        """,
        (cid,),
    ).fetchall()

    # Manual patches for this candidate
    participant_patches = conn.execute(
        "SELECT id, patch_display_name, patch_date_of_birth, patch_best_email, patch_best_phone, "
        "actor, reason_code, status, created_at "
        "FROM manual_participant_patch WHERE candidate_participant_id = %s ORDER BY created_at",
        (cid,),
    ).fetchall()

    address_patches = conn.execute(
        "SELECT id, address_raw, line1, city, state, postal_code, actor, status, created_at "
        "FROM manual_participant_address_patch WHERE candidate_participant_id = %s ORDER BY created_at",
        (cid,),
    ).fetchall()

    _ok({
        "candidate": {
            "id": str(p["id"]),
            "display_name": p["display_name"],
            "normalized_name": p["normalized_name"],
            "date_of_birth": str(p["date_of_birth"]) if p["date_of_birth"] else None,
            "best_email": p["best_email"],
            "best_phone": p["best_phone"],
            "quality_score": float(p["quality_score"]) if p["quality_score"] is not None else None,
            "resolution_state": p["resolution_state"],
            "is_promoted": p["is_promoted"],
        },
        "contacts": [dict(c) for c in contacts],
        "addresses": [dict(a) for a in addresses],
        "roles": [dict(r) for r in roles],
        "sources": [
            {
                "source_table_name": s["source_table_name"],
                "source_system": s["source_system"],
                "link_score": float(s["link_score"]) if s["link_score"] is not None else None,
            }
            for s in sources
        ],
        "canonical_link": {
            "canonical_entity_id": str(canonical_link["canonical_entity_id"]),
            "promotion_score": float(canonical_link["promotion_score"]) if canonical_link["promotion_score"] is not None else None,
            "promotion_mode": canonical_link["promotion_mode"],
        } if canonical_link else None,
        "related_yachts": [
            {
                "candidate_yacht_id": str(y["id"]),
                "name": y["name"],
                "normalized_name": y["normalized_name"],
                "sail_number": y["sail_number"],
                "yacht_type": y["yacht_type"],
            }
            for y in yachts
        ],
        "manual_patches": {
            "participant_patches": [
                {
                    "id": str(pp["id"]),
                    "patch_display_name": pp["patch_display_name"],
                    "patch_date_of_birth": str(pp["patch_date_of_birth"]) if pp["patch_date_of_birth"] else None,
                    "patch_best_email": pp["patch_best_email"],
                    "patch_best_phone": pp["patch_best_phone"],
                    "actor": pp["actor"],
                    "status": pp["status"],
                }
                for pp in participant_patches
            ],
            "address_patches": [
                {
                    "id": str(ap["id"]),
                    "address_raw": ap["address_raw"],
                    "actor": ap["actor"],
                    "status": ap["status"],
                }
                for ap in address_patches
            ],
        },
    })


# ---------------------------------------------------------------------------
# preview_decision / apply_decision
# ---------------------------------------------------------------------------

def _fetch_candidate_state(conn: psycopg.Connection, entity_type: str, candidate_id: str) -> dict | None:
    _CANDIDATE_TABLE = {
        "participant": "candidate_participant",
        "yacht": "candidate_yacht",
        "club": "candidate_club",
        "event": "candidate_event",
        "registration": "candidate_registration",
    }
    table = _CANDIDATE_TABLE.get(entity_type)
    if not table:
        return None
    row = conn.execute(
        f"SELECT id, is_promoted, quality_score, resolution_state FROM {table} WHERE id = %s",
        (candidate_id,),
    ).fetchone()
    return dict(row) if row else None


def _preview_decision(conn: psycopg.Connection, args: dict) -> None:
    entity_type = (args.get("entity_type") or "").strip()
    candidate_id = (args.get("candidate_id") or "").strip()
    action = (args.get("action") or "").strip().lower()
    actor = (args.get("actor") or "").strip()
    reason_code = (args.get("reason_code") or "manual_review").strip()

    if not all([entity_type, candidate_id, action, actor]):
        _err("entity_type, candidate_id, action, actor all required")

    if action not in ("promote", "reject", "hold"):
        _err(f"invalid action: {action!r} (must be promote, reject, or hold)")

    state = _fetch_candidate_state(conn, entity_type, candidate_id)
    if not state:
        _err(f"{entity_type} candidate not found: {candidate_id}")

    # Compute what would happen
    warnings: list[str] = []
    will_proceed = True
    outcome_description = ""

    if action == "promote":
        if state["is_promoted"]:
            will_proceed = False
            outcome_description = "skipped — already promoted"
        elif state["resolution_state"] not in ("review", "hold"):
            will_proceed = False
            outcome_description = f"blocked — resolution_state='{state['resolution_state']}' is not review/hold"
        else:
            # Check if canonical link already exists
            existing = conn.execute(
                "SELECT canonical_entity_id FROM candidate_canonical_link "
                "WHERE candidate_entity_type = %s AND candidate_entity_id = %s",
                (entity_type, candidate_id),
            ).fetchone()
            if existing:
                outcome_description = f"will reuse existing canonical {existing['canonical_entity_id']}"
            else:
                outcome_description = f"will create new canonical_{entity_type} row"

    elif action in ("reject", "hold"):
        if state["is_promoted"]:
            will_proceed = False
            outcome_description = "blocked — cannot reject/hold a promoted candidate"
        else:
            outcome_description = f"will update resolution_state to '{action}'"

    _ok({
        "preview": {
            "entity_type": entity_type,
            "candidate_id": candidate_id,
            "action": action,
            "actor": actor,
            "reason_code": reason_code,
            "current_state": {
                "is_promoted": state["is_promoted"],
                "quality_score": float(state["quality_score"]) if state["quality_score"] is not None else None,
                "resolution_state": state["resolution_state"],
            },
            "will_proceed": will_proceed,
            "outcome_description": outcome_description,
            "warnings": warnings,
        }
    })


def _apply_decision(conn: psycopg.Connection, args: dict, dry_run: bool = False) -> None:
    from regatta_etl.resolution_manual_apply import (
        ManualApplyCounters,
        _apply_promote,
        _apply_reject,
        _apply_hold,
    )

    entity_type = (args.get("entity_type") or "").strip()
    candidate_id = (args.get("candidate_id") or "").strip()
    action = (args.get("action") or "").strip().lower()
    actor = (args.get("actor") or "").strip()
    reason_code = (args.get("reason_code") or "manual_review").strip()

    if not all([entity_type, candidate_id, action, actor]):
        _err("entity_type, candidate_id, action, actor all required")

    if action not in ("promote", "reject", "hold"):
        _err(f"invalid action: {action!r}")

    ctrs = ManualApplyCounters()
    conn.execute("SAVEPOINT mcp_decision")
    try:
        if action == "promote":
            _apply_promote(conn, entity_type, candidate_id, reason_code, actor, ctrs)
        elif action == "reject":
            _apply_reject(conn, entity_type, candidate_id, reason_code, actor, ctrs)
        elif action == "hold":
            _apply_hold(conn, entity_type, candidate_id, reason_code, actor, ctrs)

        if dry_run:
            conn.execute("ROLLBACK TO SAVEPOINT mcp_decision")
        conn.execute("RELEASE SAVEPOINT mcp_decision")

        if not dry_run:
            conn.commit()

        _ok({
            "dry_run": dry_run,
            "action": action,
            "entity_type": entity_type,
            "candidate_id": candidate_id,
            "rows_promoted": ctrs.rows_promoted,
            "rows_rejected": ctrs.rows_rejected,
            "rows_held": ctrs.rows_held,
            "rows_invalid": ctrs.rows_invalid,
            "rows_skipped_already_promoted": ctrs.rows_skipped_already_promoted,
            "warnings": ctrs.warnings,
        })
    except Exception as exc:
        conn.execute("ROLLBACK TO SAVEPOINT mcp_decision")
        conn.execute("RELEASE SAVEPOINT mcp_decision")
        _err(str(exc))


# ---------------------------------------------------------------------------
# preview_lifecycle / apply_lifecycle
# ---------------------------------------------------------------------------

def _preview_lifecycle(conn: psycopg.Connection, args: dict) -> None:
    lifecycle_op = (args.get("lifecycle_op") or "").strip().lower()
    rows = args.get("rows", [])
    if not lifecycle_op:
        _err("lifecycle_op is required")
    if lifecycle_op not in ("merge", "demote", "unlink", "split"):
        _err(f"invalid lifecycle_op: {lifecycle_op!r}")
    if not rows:
        _err("rows list is required")

    _ok({
        "preview": {
            "lifecycle_op": lifecycle_op,
            "row_count": len(rows),
            "rows": rows[:5],  # first 5 for preview
            "will_proceed": True,
            "outcome_description": (
                f"will apply '{lifecycle_op}' to {len(rows)} row(s); "
                "no DB changes until confirm"
            ),
        }
    })


def _apply_lifecycle(conn: psycopg.Connection, args: dict, dry_run: bool = False) -> None:
    from regatta_etl.resolution_lifecycle import run_lifecycle, LifecycleCounters

    lifecycle_op = (args.get("lifecycle_op") or "").strip().lower()
    rows_data = args.get("rows", [])
    if not lifecycle_op:
        _err("lifecycle_op is required")
    if lifecycle_op not in ("merge", "demote", "unlink", "split"):
        _err(f"invalid lifecycle_op: {lifecycle_op!r}")
    if not rows_data:
        _err("rows list is required")

    # Write rows to a temp CSV and call run_lifecycle
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
    ) as fh:
        tmp_path = fh.name
        if rows_data:
            fieldnames = list(rows_data[0].keys())
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_data)

    try:
        ctrs = run_lifecycle(conn, tmp_path, lifecycle_op, dry_run=dry_run)
        if not dry_run:
            conn.commit()
        _ok({
            "dry_run": dry_run,
            "lifecycle_op": lifecycle_op,
            "rows_read": ctrs.rows_read,
            "rows_ok": ctrs.rows_ok,
            "rows_invalid": ctrs.rows_invalid,
            "db_errors": ctrs.db_errors,
            "warnings": ctrs.warnings,
        })
    except Exception as exc:
        _err(str(exc))
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# patch helpers
# ---------------------------------------------------------------------------

def _patch_hash(*parts: Any) -> str:
    """Stable sha256 hex of joined string parts."""
    key = "|".join(str(p) if p is not None else "" for p in parts)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _preview_patch(conn: psycopg.Connection, args: dict) -> None:
    patch_type = (args.get("patch_type") or "").strip()
    if not patch_type:
        _err("patch_type is required")

    supported = (
        "participant_patch",
        "participant_address_patch",
        "yacht_patch",
        "yacht_ownership_patch",
        "club_membership_patch",
    )
    if patch_type not in supported:
        _err(f"unsupported patch_type: {patch_type!r}")

    actor = (args.get("actor") or "").strip()
    if not actor:
        _err("actor is required")

    _ok({
        "preview": {
            "patch_type": patch_type,
            "actor": actor,
            "args": {k: v for k, v in args.items() if k not in ("actor",)},
            "will_proceed": True,
            "outcome_description": f"will write one active {patch_type} row if not duplicate",
        }
    })


def _apply_patch(conn: psycopg.Connection, args: dict, dry_run: bool = False) -> None:
    patch_type = (args.get("patch_type") or "").strip()
    actor = (args.get("actor") or "").strip()
    reason_code = (args.get("reason_code") or "").strip() or None
    session_id = (args.get("session_id") or "").strip() or None

    if not patch_type or not actor:
        _err("patch_type and actor are required")

    conn.execute("SAVEPOINT mcp_patch")
    try:
        result: dict = {}

        if patch_type == "participant_patch":
            cid = (args.get("candidate_participant_id") or "").strip()
            if not cid:
                _err("candidate_participant_id is required for participant_patch")
            # Actor excluded from hash: the same logical correction by two different
            # humans is the same patch and must not create a duplicate row.
            ph = _patch_hash(
                "participant_patch", cid,
                args.get("patch_display_name"),
                args.get("patch_date_of_birth"),
                args.get("patch_best_email"),
                args.get("patch_best_phone"),
            )
            cur = conn.execute(
                """
                INSERT INTO manual_participant_patch
                    (candidate_participant_id, patch_display_name, patch_date_of_birth,
                     patch_best_email, patch_best_phone, actor, reason_code, session_id, patch_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (patch_hash) DO NOTHING
                RETURNING id
                """,
                (
                    cid,
                    args.get("patch_display_name"),
                    args.get("patch_date_of_birth"),
                    args.get("patch_best_email"),
                    args.get("patch_best_phone"),
                    actor, reason_code, session_id, ph,
                ),
            )
            row = cur.fetchone()
            result = {"inserted": row is not None, "patch_id": str(row["id"]) if row else None}

        elif patch_type == "participant_address_patch":
            cid = (args.get("candidate_participant_id") or "").strip()
            address_raw = (args.get("address_raw") or "").strip()
            if not cid or not address_raw:
                _err("candidate_participant_id and address_raw are required")
            ph = _patch_hash("participant_address_patch", cid, address_raw)
            cur = conn.execute(
                """
                INSERT INTO manual_participant_address_patch
                    (candidate_participant_id, address_raw, line1, city, state,
                     postal_code, country_code, is_primary, actor, reason_code, session_id, patch_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (patch_hash) DO NOTHING
                RETURNING id
                """,
                (
                    cid, address_raw,
                    args.get("line1"), args.get("city"), args.get("state"),
                    args.get("postal_code"), args.get("country_code"),
                    bool(args.get("is_primary", False)),
                    actor, reason_code, session_id, ph,
                ),
            )
            row = cur.fetchone()
            result = {"inserted": row is not None, "patch_id": str(row["id"]) if row else None}

        elif patch_type == "yacht_patch":
            cid = (args.get("candidate_yacht_id") or "").strip()
            if not cid:
                _err("candidate_yacht_id is required for yacht_patch")
            ph = _patch_hash(
                "yacht_patch", cid,
                args.get("patch_name"),
                args.get("patch_sail_number"),
                args.get("patch_length_feet"),
                args.get("patch_yacht_type"),
            )
            cur = conn.execute(
                """
                INSERT INTO manual_yacht_patch
                    (candidate_yacht_id, patch_name, patch_sail_number,
                     patch_length_feet, patch_yacht_type, actor, reason_code, session_id, patch_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (patch_hash) DO NOTHING
                RETURNING id
                """,
                (
                    cid,
                    args.get("patch_name"), args.get("patch_sail_number"),
                    args.get("patch_length_feet"), args.get("patch_yacht_type"),
                    actor, reason_code, session_id, ph,
                ),
            )
            row = cur.fetchone()
            result = {"inserted": row is not None, "patch_id": str(row["id"]) if row else None}

        elif patch_type == "yacht_ownership_patch":
            pcid = (args.get("candidate_participant_id") or "").strip()
            ycid = (args.get("candidate_yacht_id") or "").strip()
            role = (args.get("role") or "owner").strip()
            operation = (args.get("operation") or "add").strip()
            if not pcid or not ycid:
                _err("candidate_participant_id and candidate_yacht_id are required")
            ph = _patch_hash("yacht_ownership_patch", pcid, ycid, role, operation)
            cur = conn.execute(
                """
                INSERT INTO manual_yacht_ownership_patch
                    (candidate_participant_id, candidate_yacht_id, role, effective_start,
                     effective_end, operation, actor, reason_code, session_id, patch_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (patch_hash) DO NOTHING
                RETURNING id
                """,
                (
                    pcid, ycid, role,
                    args.get("effective_start"), args.get("effective_end"),
                    operation, actor, reason_code, session_id, ph,
                ),
            )
            row = cur.fetchone()
            result = {"inserted": row is not None, "patch_id": str(row["id"]) if row else None}

        elif patch_type == "club_membership_patch":
            pcid = (args.get("candidate_participant_id") or "").strip()
            ccid = (args.get("candidate_club_id") or "").strip()
            membership_role = (args.get("membership_role") or "member").strip()
            operation = (args.get("operation") or "add").strip()
            if not pcid or not ccid:
                _err("candidate_participant_id and candidate_club_id are required")
            ph = _patch_hash("club_membership_patch", pcid, ccid, membership_role, operation)
            cur = conn.execute(
                """
                INSERT INTO manual_club_membership_patch
                    (candidate_participant_id, candidate_club_id, membership_role, effective_start,
                     effective_end, operation, actor, reason_code, session_id, patch_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (patch_hash) DO NOTHING
                RETURNING id
                """,
                (
                    pcid, ccid, membership_role,
                    args.get("effective_start"), args.get("effective_end"),
                    operation, actor, reason_code, session_id, ph,
                ),
            )
            row = cur.fetchone()
            result = {"inserted": row is not None, "patch_id": str(row["id"]) if row else None}

        else:
            _err(f"unsupported patch_type: {patch_type!r}")

        if dry_run:
            conn.execute("ROLLBACK TO SAVEPOINT mcp_patch")
        conn.execute("RELEASE SAVEPOINT mcp_patch")

        if not dry_run:
            conn.commit()

        _ok({"dry_run": dry_run, "patch_type": patch_type, **result})

    except SystemExit:
        raise
    except Exception as exc:
        conn.execute("ROLLBACK TO SAVEPOINT mcp_patch")
        conn.execute("RELEASE SAVEPOINT mcp_patch")
        _err(str(exc))


# ---------------------------------------------------------------------------
# rerun_resolution
# ---------------------------------------------------------------------------

def _rerun_resolution(conn: psycopg.Connection, args: dict) -> None:
    """Rerun the source-to-candidate pipeline.

    When candidate_participant_id is provided, only the manual curation steps
    are re-applied for that specific candidate (fast targeted rerun after a
    patch).  The full pipeline is skipped so unrelated candidates are unaffected.

    Without candidate_participant_id, the whole entity_type pipeline runs.
    """
    from regatta_etl.resolution_source_to_candidate import (
        _ingest_participants_from_manual_patches,
        _ingest_addresses_from_manual_patches,
        _ingest_ownerships_from_manual_patches,
        _ingest_memberships_from_manual_patches,
        _ingest_yachts_from_manual_patches,
        SourceToCandidateCounters,
        run_source_to_candidate,
    )

    candidate_participant_id = (args.get("candidate_participant_id") or "").strip() or None
    entity_type = (args.get("entity_type") or "participant").strip()
    if entity_type not in ("participant", "yacht", "all"):
        _err(f"entity_type must be participant, yacht, or all; got {entity_type!r}")

    if candidate_participant_id:
        # Targeted rerun: re-apply only the manual ingestion steps for this
        # specific candidate.  Each step already filters by status='active' so
        # skipping unrelated rows is implicit; we just run the manual functions
        # directly rather than the full pipeline to avoid side-effects.
        ctrs = SourceToCandidateCounters()
        try:
            _ingest_participants_from_manual_patches(conn, ctrs)
            _ingest_addresses_from_manual_patches(conn, ctrs)
            _ingest_ownerships_from_manual_patches(conn, ctrs)
            _ingest_memberships_from_manual_patches(conn, ctrs)
            if entity_type in ("yacht", "all"):
                _ingest_yachts_from_manual_patches(conn, ctrs)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            _err(f"targeted rerun failed: {exc}")
        _ok({
            "targeted_candidate_participant_id": candidate_participant_id,
            "entity_type": entity_type,
            "participants_ingested": ctrs.participants_ingested,
            "participant_addresses_linked": ctrs.participant_addresses_linked,
            "participant_roles_linked": ctrs.participant_roles_linked,
            "source_links_inserted": ctrs.source_links_inserted,
            "db_errors": ctrs.db_errors,
            "warnings": ctrs.warnings[:20],
        })
    else:
        ctrs = run_source_to_candidate(conn, entity_type=entity_type, dry_run=False)
        conn.commit()
        _ok({
            "entity_type": entity_type,
            "participants_ingested": ctrs.participants_ingested,
            "participants_candidate_created": ctrs.participants_candidate_created,
            "participants_candidate_enriched": ctrs.participants_candidate_enriched,
            "participant_addresses_linked": ctrs.participant_addresses_linked,
            "yachts_ingested": ctrs.yachts_ingested,
            "yachts_candidate_created": ctrs.yachts_candidate_created,
            "source_links_inserted": ctrs.source_links_inserted,
            "db_errors": ctrs.db_errors,
            "warnings": ctrs.warnings[:20],
        })


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_COMMANDS = {
    "find_profile":      _find_profile,
    "profile_context":   _profile_context,
    "preview_decision":  _preview_decision,
    "apply_decision":    lambda conn, args: _apply_decision(conn, args, dry_run=False),
    "preview_lifecycle": _preview_lifecycle,
    "apply_lifecycle":   lambda conn, args: _apply_lifecycle(conn, args, dry_run=False),
    "preview_patch":     _preview_patch,
    "apply_patch":       lambda conn, args: _apply_patch(conn, args, dry_run=False),
    "rerun_resolution":  _rerun_resolution,
}


def main() -> None:
    if len(sys.argv) < 3:
        _err("usage: python3 -m regatta_etl.mcp_helper <command> '<json_payload>'")

    command = sys.argv[1]
    if command not in _COMMANDS:
        _err(f"unknown command: {command!r}; valid: {sorted(_COMMANDS)}")

    try:
        args = json.loads(sys.argv[2])
    except json.JSONDecodeError as e:
        _err(f"invalid JSON payload: {e}")

    db_dsn = args.pop("db_dsn", None) or os.environ.get("DB_DSN", "")
    if not db_dsn:
        _err("db_dsn must be provided in payload or DB_DSN env var")

    try:
        with _connect(db_dsn) as conn:
            _COMMANDS[command](conn, args)
    except SystemExit:
        raise
    except Exception as exc:
        _err(f"connection or execution error: {exc}")


if __name__ == "__main__":
    main()
