#!/usr/bin/env python3
"""Backfill structured address fields from address_raw across address tables."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass

import psycopg

from regatta_etl.normalize import (
    normalize_country_code,
    normalize_postal_code_for_storage,
    parse_mailing_address_components,
    trim,
)


@dataclass
class TableStats:
    scanned: int = 0
    updated: int = 0
    skipped: int = 0


TABLES = (
    "participant_address",
    "candidate_participant_address",
    "canonical_participant_address",
)


def _coalesce_address_raw(
    address_raw: str | None,
    line1: str | None,
    line2: str | None,
    city: str | None,
    state: str | None,
    postal_code: str | None,
    country_code: str | None,
) -> str | None:
    if trim(address_raw):
        return trim(address_raw)
    parts = [
        trim(line1),
        trim(line2),
        trim(city),
        trim(state),
        trim(postal_code),
        trim(country_code),
    ]
    composed = ", ".join(part for part in parts if part)
    return trim(composed)


def _update_table(conn: psycopg.Connection, table_name: str) -> TableStats:
    stats = TableStats()
    rows = conn.execute(
        f"""
        SELECT id::text, address_raw, line1, line2, city, state, postal_code, country_code
        FROM {table_name}
        """
    ).fetchall()
    for row in rows:
        stats.scanned += 1
        row_id, address_raw, line1, line2, city, state, postal_code, country_code = row
        seed = _coalesce_address_raw(address_raw, line1, line2, city, state, postal_code, country_code)
        if not seed:
            stats.skipped += 1
            continue

        parsed = parse_mailing_address_components(seed, fallback_country_code=country_code)
        if not parsed.line1 and not parsed.city and not parsed.state and not parsed.postal_code:
            stats.skipped += 1
            continue

        line1_norm = trim(line1)
        line2_norm = trim(line2)
        city_norm = trim(city)
        state_norm = trim(state)
        country_norm = normalize_country_code(country_code) or normalize_country_code(parsed.country_code)
        postal_norm = normalize_postal_code_for_storage(trim(postal_code), country_norm)

        # Promote parser result when row appears crowded and parser has better structure.
        crowded = bool(line1_norm and not city_norm and not state_norm and parsed.city and parsed.state)
        next_line1 = parsed.line1 if crowded and parsed.line1 else (line1_norm or parsed.line1)
        next_line2 = line2_norm or parsed.line2
        next_city = city_norm or parsed.city
        next_state = state_norm or parsed.state
        next_country = country_norm
        next_postal = postal_norm or normalize_postal_code_for_storage(parsed.postal_code, next_country)

        if (
            next_line1 == line1_norm
            and next_line2 == line2_norm
            and next_city == city_norm
            and next_state == state_norm
            and next_postal == trim(postal_code)
            and next_country == normalize_country_code(country_code)
        ):
            stats.skipped += 1
            continue

        conn.execute(
            f"""
            UPDATE {table_name}
            SET line1 = %s,
                line2 = %s,
                city = %s,
                state = %s,
                postal_code = %s,
                country_code = %s,
                updated_at = NOW()
            WHERE id = %s::uuid
            """,
            (
                next_line1,
                next_line2,
                next_city,
                next_state,
                next_postal,
                next_country,
                row_id,
            ),
        )
        stats.updated += 1
    return stats


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill line1/line2/city/state/postal/country from address_raw."
    )
    parser.add_argument(
        "--db-dsn",
        default=os.environ.get("DB_DSN", ""),
        help="PostgreSQL DSN. Defaults to DB_DSN env var.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run backfill without committing.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if not args.db_dsn:
        raise SystemExit("DB_DSN is not set.")

    out: dict[str, object] = {"dry_run": args.dry_run, "tables": {}}
    with psycopg.connect(args.db_dsn, autocommit=False) as conn:
        for table in TABLES:
            stats = _update_table(conn, table)
            out["tables"][table] = {
                "scanned": stats.scanned,
                "updated": stats.updated,
                "skipped": stats.skipped,
            }
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

