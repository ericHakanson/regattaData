#!/usr/bin/env python3
"""Backfill structured name components for participant and canonical_participant.

Rules applied:
1) canonical_participant:
   - If canonical first/last are missing, fill from display_name only when
     canonical display_name matches at least one linked candidate display_name.
   - middle_name / name_prefix / name_suffix are filled when null.
2) participant:
   - Fill missing first/middle/last/prefix/suffix from participant.full_name.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

import psycopg

from regatta_etl.normalize import parse_person_name_parts, trim


@dataclass
class Counters:
    canonical_rows_scanned: int = 0
    canonical_rows_updated: int = 0
    participant_rows_scanned: int = 0
    participant_rows_updated: int = 0


def _coalesce(existing: str | None, candidate: str | None) -> str | None:
    return existing if trim(existing) is not None else trim(candidate)


def _backfill_canonical(conn: psycopg.Connection, dry_run: bool, ctrs: Counters) -> None:
    rows = conn.execute(
        """
        SELECT
            cp.id::text,
            cp.display_name,
            cp.first_name,
            cp.middle_name,
            cp.last_name,
            cp.name_prefix,
            cp.name_suffix,
            EXISTS (
                SELECT 1
                FROM candidate_canonical_link ccl
                JOIN candidate_participant cand
                  ON cand.id = ccl.candidate_entity_id
                WHERE ccl.candidate_entity_type = 'participant'
                  AND ccl.canonical_entity_id = cp.id
                  AND NULLIF(BTRIM(cand.display_name), '') = NULLIF(BTRIM(cp.display_name), '')
            ) AS has_display_match
        FROM canonical_participant cp
        ORDER BY cp.created_at, cp.id
        """
    ).fetchall()

    for row in rows:
        ctrs.canonical_rows_scanned += 1
        canonical_id = row[0]
        display_name = trim(row[1])
        first_name = row[2]
        middle_name = row[3]
        last_name = row[4]
        name_prefix = row[5]
        name_suffix = row[6]
        has_display_match = bool(row[7])

        parsed = parse_person_name_parts(display_name)
        should_fill_first_last = has_display_match and display_name is not None
        new_first = first_name
        new_last = last_name
        if should_fill_first_last:
            new_first = _coalesce(first_name, parsed.first_name)
            new_last = _coalesce(last_name, parsed.last_name)

        new_middle = _coalesce(middle_name, parsed.middle_name)
        new_prefix = _coalesce(name_prefix, parsed.name_prefix)
        new_suffix = _coalesce(name_suffix, parsed.name_suffix)

        if (
            trim(new_first) == trim(first_name)
            and trim(new_middle) == trim(middle_name)
            and trim(new_last) == trim(last_name)
            and trim(new_prefix) == trim(name_prefix)
            and trim(new_suffix) == trim(name_suffix)
        ):
            continue

        conn.execute(
            """
            UPDATE canonical_participant
            SET
                first_name = %s,
                middle_name = %s,
                last_name = %s,
                name_prefix = %s,
                name_suffix = %s
            WHERE id = %s
            """,
            (new_first, new_middle, new_last, new_prefix, new_suffix, canonical_id),
        )
        ctrs.canonical_rows_updated += 1

    if dry_run:
        conn.rollback()
    else:
        conn.commit()


def _backfill_participant(conn: psycopg.Connection, dry_run: bool, ctrs: Counters) -> None:
    rows = conn.execute(
        """
        SELECT id::text, full_name, first_name, middle_name, last_name, name_prefix, name_suffix
        FROM participant
        ORDER BY created_at, id
        """
    ).fetchall()

    for row in rows:
        ctrs.participant_rows_scanned += 1
        participant_id = row[0]
        full_name = row[1]
        first_name = row[2]
        middle_name = row[3]
        last_name = row[4]
        name_prefix = row[5]
        name_suffix = row[6]

        parsed = parse_person_name_parts(full_name)
        new_first = _coalesce(first_name, parsed.first_name)
        new_middle = _coalesce(middle_name, parsed.middle_name)
        new_last = _coalesce(last_name, parsed.last_name)
        new_prefix = _coalesce(name_prefix, parsed.name_prefix)
        new_suffix = _coalesce(name_suffix, parsed.name_suffix)

        if (
            trim(new_first) == trim(first_name)
            and trim(new_middle) == trim(middle_name)
            and trim(new_last) == trim(last_name)
            and trim(new_prefix) == trim(name_prefix)
            and trim(new_suffix) == trim(name_suffix)
        ):
            continue

        conn.execute(
            """
            UPDATE participant
            SET
                first_name = %s,
                middle_name = %s,
                last_name = %s,
                name_prefix = %s,
                name_suffix = %s
            WHERE id = %s
            """,
            (new_first, new_middle, new_last, new_prefix, new_suffix, participant_id),
        )
        ctrs.participant_rows_updated += 1

    if dry_run:
        conn.rollback()
    else:
        conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill participant/canonical name components")
    parser.add_argument("--db-dsn", default=os.environ.get("DB_DSN"), help="PostgreSQL DSN")
    parser.add_argument("--dry-run", action="store_true", help="Run without committing changes")
    args = parser.parse_args()

    if not args.db_dsn:
        raise SystemExit("DB_DSN is required (set env var or pass --db-dsn).")

    ctrs = Counters()
    with psycopg.connect(args.db_dsn, autocommit=False) as conn:
        _backfill_canonical(conn, dry_run=args.dry_run, ctrs=ctrs)
        _backfill_participant(conn, dry_run=args.dry_run, ctrs=ctrs)

    print(
        {
            "dry_run": args.dry_run,
            "canonical_rows_scanned": ctrs.canonical_rows_scanned,
            "canonical_rows_updated": ctrs.canonical_rows_updated,
            "participant_rows_scanned": ctrs.participant_rows_scanned,
            "participant_rows_updated": ctrs.participant_rows_updated,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
