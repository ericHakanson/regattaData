#!/usr/bin/env python3
"""Build resolution_lifecycle merge/split plans from reviewed direct-mail decisions."""

from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path

import psycopg

from regatta_etl.direct_mail_lifecycle_plan import build_lifecycle_plans
from regatta_etl.direct_mail_review import normalize_reviewer_csv
from regatta_etl.normalize import (
    normalize_email,
    normalize_person_name_for_identity,
    normalize_phone,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert reviewed direct-mail decisions into lifecycle merge/split plan CSVs."
        )
    )
    parser.add_argument("--input", required=True, help="Input reviewed CSV path")
    parser.add_argument("--merge-plan-out", required=True, help="Output merge plan CSV path")
    parser.add_argument("--split-plan-out", required=True, help="Output split plan CSV path")
    parser.add_argument("--conflicts-out", required=True, help="Output conflicts CSV path")
    parser.add_argument(
        "--summary-json",
        default=None,
        help="Optional summary JSON output path",
    )
    parser.add_argument(
        "--actor",
        default=os.environ.get("USER", "operator"),
        help="Actor value written into lifecycle plan rows",
    )
    return parser


def _load_canonical_map(db_dsn: str, candidate_ids: list[str]) -> dict[str, str]:
    if not candidate_ids:
        return {}

    valid_candidate_ids: list[str] = []
    for cid in candidate_ids:
        try:
            valid_candidate_ids.append(str(uuid.UUID(cid)))
        except (ValueError, AttributeError, TypeError):
            continue
    if not valid_candidate_ids:
        return {}

    with psycopg.connect(db_dsn) as conn:
        try:
            rows = conn.execute(
                """
                SELECT
                    candidate_entity_id::text,
                    canonical_entity_id::text
                FROM candidate_canonical_link
                WHERE candidate_entity_type = 'participant'
                  AND candidate_entity_id = ANY(%s::uuid[])
                ORDER BY candidate_entity_id, canonical_entity_id
                """,
                (valid_candidate_ids,),
            ).fetchall()
        except psycopg.errors.UndefinedTable:
            # Older/partial schemas may not have candidate_canonical_link yet.
            # Fall back to promoted_canonical_id on candidate_participant.
            conn.rollback()
            try:
                rows = conn.execute(
                    """
                    SELECT
                        id::text AS candidate_entity_id,
                        promoted_canonical_id::text AS canonical_entity_id
                    FROM candidate_participant
                    WHERE promoted_canonical_id IS NOT NULL
                      AND id = ANY(%s::uuid[])
                    ORDER BY id, promoted_canonical_id
                    """,
                    (valid_candidate_ids,),
                ).fetchall()
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
                # Some environments don't include resolution tables yet.
                conn.rollback()
                return {}

    # deterministic first canonical per candidate
    out: dict[str, str] = {}
    for cid, canonical_id in rows:
        out.setdefault(cid, canonical_id)
    return out


def _parse_canonical_ids(raw_value: str) -> list[str]:
    raw = (raw_value or "").strip()
    if not raw:
        return []
    if raw.startswith("{") and raw.endswith("}"):
        raw = raw[1:-1]
    parts = [p.strip().strip('"') for p in raw.split(",") if p.strip()]
    out: list[str] = []
    for p in parts:
        try:
            out.append(str(uuid.UUID(p)))
        except (ValueError, AttributeError, TypeError):
            continue
    return out


def _build_canonical_map_from_rows(reviewed_rows: list[dict[str, str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in reviewed_rows:
        cid = (row.get("candidate_id") or "").strip()
        if not cid:
            continue
        try:
            cid = str(uuid.UUID(cid))
        except (ValueError, AttributeError, TypeError):
            continue
        canonical_ids = _parse_canonical_ids(row.get("canonical_ids", ""))
        if not canonical_ids:
            continue
        out.setdefault(cid, canonical_ids[0])
    return out


def _load_existing_canonical_ids(db_dsn: str, canonical_ids: list[str]) -> set[str]:
    if not canonical_ids:
        return set()
    valid_canonical_ids: list[str] = []
    for cid in canonical_ids:
        try:
            valid_canonical_ids.append(str(uuid.UUID(cid)))
        except (ValueError, AttributeError, TypeError):
            continue
    if not valid_canonical_ids:
        return set()

    with psycopg.connect(db_dsn) as conn:
        try:
            rows = conn.execute(
                """
                SELECT id::text
                FROM canonical_participant
                WHERE id = ANY(%s::uuid[])
                """,
                (valid_canonical_ids,),
            ).fetchall()
        except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
            conn.rollback()
            # Older environments may not have canonical tables yet.
            return set(valid_canonical_ids)
    return {row[0] for row in rows}


def _split_pipe(value: str) -> list[str]:
    return [part.strip() for part in (value or "").split("|") if part.strip()]


def _normalized_emails(value: str) -> set[str]:
    out: set[str] = set()
    for raw in _split_pipe(value):
        normalized = normalize_email(raw)
        if normalized:
            out.add(normalized)
    return out


def _normalized_phones(value: str) -> set[str]:
    out: set[str] = set()
    for raw in _split_pipe(value):
        normalized = normalize_phone(raw)
        if normalized:
            out.add(normalized)
    return out


def _candidate_row_index(reviewed_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    indexed: dict[str, dict[str, str]] = {}
    for row in reviewed_rows:
        cid = (row.get("candidate_id") or row.get("candidateId") or "").strip()
        if cid and cid not in indexed:
            indexed[cid] = row
    return indexed


def _load_existing_candidate_ids(db_dsn: str, candidate_ids: list[str]) -> set[str]:
    if not candidate_ids:
        return set()
    valid_ids: list[str] = []
    for cid in candidate_ids:
        try:
            valid_ids.append(str(uuid.UUID(cid)))
        except (ValueError, AttributeError, TypeError):
            continue
    if not valid_ids:
        return set()
    with psycopg.connect(db_dsn) as conn:
        try:
            rows = conn.execute(
                "SELECT id::text FROM candidate_participant WHERE id = ANY(%s::uuid[])",
                (valid_ids,),
            ).fetchall()
        except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
            conn.rollback()
            return set()
    return {row[0] for row in rows}


def _recover_candidate_ids(
    db_dsn: str,
    reviewed_rows: list[dict[str, str]],
    candidate_ids: list[str],
) -> tuple[dict[str, str], list[str]]:
    """Recover stale reviewed candidate IDs to current candidate IDs.

    Matching strategy:
    1) unique intersection(email_hits, phone_hits)
    2) unique email hit
    3) unique phone hit
    4) unique (email|phone) intersect name hits
    5) unique name hit when no strong hit exists
    """
    if not candidate_ids:
        return {}, []

    existing_ids = _load_existing_candidate_ids(db_dsn, candidate_ids)
    stale_ids = sorted([cid for cid in candidate_ids if cid and cid not in existing_ids])
    if not stale_ids:
        return {}, []

    stale_row_by_id = _candidate_row_index(reviewed_rows)
    stale_row_by_id = {cid: stale_row_by_id[cid] for cid in stale_ids if cid in stale_row_by_id}
    if not stale_row_by_id:
        return {}, stale_ids

    with psycopg.connect(db_dsn) as conn:
        try:
            cand_rows = conn.execute(
                """
                SELECT id::text, normalized_name, best_email, best_phone
                FROM candidate_participant
                """
            ).fetchall()
        except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
            conn.rollback()
            return {}, stale_ids

        try:
            contact_rows = conn.execute(
                """
                SELECT candidate_participant_id::text, contact_type,
                       COALESCE(NULLIF(BTRIM(normalized_value), ''), NULLIF(BTRIM(raw_value), '')) AS v
                FROM candidate_participant_contact
                """
            ).fetchall()
        except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
            conn.rollback()
            contact_rows = []

    email_index: dict[str, set[str]] = defaultdict(set)
    phone_index: dict[str, set[str]] = defaultdict(set)
    name_index: dict[str, set[str]] = defaultdict(set)

    for cid, normalized_name, best_email, best_phone in cand_rows:
        if normalized_name:
            name_index[normalized_name].add(cid)
        if best_email:
            email = normalize_email(best_email)
            if email:
                email_index[email].add(cid)
        if best_phone:
            phone = normalize_phone(best_phone)
            if phone:
                phone_index[phone].add(cid)

    for cid, contact_type, value in contact_rows:
        if not value:
            continue
        if contact_type == "email":
            email = normalize_email(value)
            if email:
                email_index[email].add(cid)
        elif contact_type == "phone":
            phone = normalize_phone(value)
            if phone:
                phone_index[phone].add(cid)

    recovered: dict[str, str] = {}
    unresolved: list[str] = []

    for old_id, row in stale_row_by_id.items():
        display_name = (
            (row.get("candidate_display_name") or "").strip()
            or (row.get("display_name") or "").strip()
        )
        normalized_name = normalize_person_name_for_identity(display_name)
        emails = _normalized_emails(row.get("email_address", ""))
        phones = _normalized_phones(row.get("phone", ""))

        email_hits: set[str] = set()
        for email in emails:
            email_hits |= email_index.get(email, set())

        phone_hits: set[str] = set()
        for phone in phones:
            phone_hits |= phone_index.get(phone, set())

        chosen = None
        intersection = email_hits & phone_hits if email_hits and phone_hits else set()
        if len(intersection) == 1:
            chosen = next(iter(intersection))
        elif len(email_hits) == 1:
            chosen = next(iter(email_hits))
        elif len(phone_hits) == 1:
            chosen = next(iter(phone_hits))
        else:
            strong_hits = email_hits | phone_hits
            name_hits = set(name_index.get(normalized_name, set())) if normalized_name else set()
            if strong_hits and len(strong_hits & name_hits) == 1:
                chosen = next(iter(strong_hits & name_hits))
            elif not strong_hits and len(name_hits) == 1:
                chosen = next(iter(name_hits))

        if chosen:
            recovered[old_id] = chosen
        else:
            unresolved.append(old_id)

    return recovered, unresolved


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = _build_parser().parse_args()
    db_dsn = os.environ.get("DB_DSN")
    if not db_dsn:
        raise SystemExit("DB_DSN is not set.")

    with tempfile.TemporaryDirectory() as tmpdir:
        normalized_path = Path(tmpdir) / "normalized.csv"
        normalize_reviewer_csv(
            input_path=Path(args.input),
            output_path=normalized_path,
        )
        with normalized_path.open(newline="", encoding="utf-8") as handle:
            reviewed_rows = list(csv.DictReader(handle))

    candidate_ids = sorted(
        {
            (row.get("candidate_id") or row.get("candidateId") or "").strip()
            for row in reviewed_rows
            if (row.get("candidate_id") or row.get("candidateId") or "").strip()
        }
        | {
            (row.get("referenceCandidateId") or row.get("refCandidateId") or "").strip()
            for row in reviewed_rows
            if (row.get("referenceCandidateId") or row.get("refCandidateId") or "").strip()
        }
    )
    canonical_map_csv = _build_canonical_map_from_rows(reviewed_rows)
    canonical_map_db = _load_canonical_map(db_dsn, candidate_ids)
    recovered_candidate_ids, unresolved_recovery_candidate_ids = _recover_candidate_ids(
        db_dsn=db_dsn,
        reviewed_rows=reviewed_rows,
        candidate_ids=candidate_ids,
    )
    recovered_canonical_map = _load_canonical_map(
        db_dsn,
        sorted(set(recovered_candidate_ids.values())),
    )
    for old_candidate_id, recovered_candidate_id in recovered_candidate_ids.items():
        canonical_id = recovered_canonical_map.get(recovered_candidate_id)
        if canonical_id:
            canonical_map_db[old_candidate_id] = canonical_id

    canonical_map = dict(canonical_map_csv)
    # DB mappings, when available, should win over CSV-export snapshots.
    canonical_map.update(canonical_map_db)
    existing_canonical_ids = _load_existing_canonical_ids(
        db_dsn,
        sorted(set(canonical_map.values())),
    )
    canonical_map = {
        candidate_id: canonical_id
        for candidate_id, canonical_id in canonical_map.items()
        if canonical_id in existing_canonical_ids
    }
    result = build_lifecycle_plans(
        reviewed_rows=reviewed_rows,
        canonical_by_candidate=canonical_map,
        actor=args.actor,
    )

    _write_csv(
        path=Path(args.merge_plan_out),
        rows=result.merge_rows,
        fieldnames=[
            "canonical_entity_type",
            "keep_canonical_id",
            "merge_canonical_id",
            "reason_code",
            "actor",
        ],
    )
    _write_csv(
        path=Path(args.split_plan_out),
        rows=result.split_rows,
        fieldnames=[
            "canonical_entity_type",
            "old_canonical_id",
            "candidate_entity_id",
            "reason_code",
            "actor",
        ],
    )
    _write_csv(
        path=Path(args.conflicts_out),
        rows=result.conflicts,
        fieldnames=[
            "candidate_id",
            "reference_candidate_id",
            "evaluation",
            "reason",
            "details",
        ],
    )

    summary = {
        "rows_read": len(reviewed_rows),
        "candidate_ids_considered": len(candidate_ids),
        "canonical_mapped_from_csv": len(canonical_map_csv),
        "canonical_mapped_from_db": len(canonical_map_db),
        "canonical_mapped": len(canonical_map),
        "canonical_ids_existing_in_db": len(existing_canonical_ids),
        "candidate_ids_recovered_from_stale_ids": len(recovered_candidate_ids),
        "candidate_ids_unresolved_after_recovery": len(unresolved_recovery_candidate_ids),
        "merge_rows": len(result.merge_rows),
        "split_rows": len(result.split_rows),
        "conflicts": len(result.conflicts),
        "merge_plan_out": args.merge_plan_out,
        "split_plan_out": args.split_plan_out,
        "conflicts_out": args.conflicts_out,
    }

    if args.summary_json:
        summary_path = Path(args.summary_json)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
