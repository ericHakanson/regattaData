# 10 — Neon Operations (primary database)

As of the 2026-08 platform pivot, the regattaData primary database is **Neon**
(serverless PostgreSQL, us-east-2). This runbook is the current source of truth
for connecting and operating the database. The Google Cloud SQL runbooks
(`01-bootstrap-gcp.md`, `05-cloud-sql-migration-plan.md`, `06-post-migration-validation.md`)
are retained as historical records; Cloud SQL is a read-only fallback during the
soak window and is being decommissioned.

## Connecting

Everything in this repo reads a single DSN from the `DB_DSN` environment variable
(`psycopg`), so pointing at Neon is just setting `DB_DSN`.

```bash
cp .env.example .env      # then paste your Neon connection string into DB_DSN
set -a; . ./.env; set +a  # export DB_DSN
```

Neon DSN shape (from the Neon console → Connection Details):

```
postgresql://<user>:<password>@<endpoint>.<region>.aws.neon.tech/<db>?sslmode=require
```

`sslmode=require` is mandatory for Neon. There is no proxy and no `gcloud` step.

Health check:

```bash
scripts/dev_db_check.sh    # uses $DB_DSN; verifies connectivity + core tables
```

## Running the ETL

Unchanged — the `regatta-import` CLI takes `--db-dsn "$DB_DSN"`. Always dry-run first:

```bash
.venv/bin/regatta-import --mode resolution_score   --db-dsn "$DB_DSN" --entity-type all --dry-run
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type all --dry-run
```

> **Performance note.** The resolution pipeline does per-row round-trips, so a full
> pass from a laptop over WAN to Neon takes minutes (it was near-instant on the old
> local Cloud SQL proxy). Correctness is unaffected. For faster runs, execute the
> ETL in-region near Neon or batch the writes — tracked in Linear (FOR-864).

## Neon MCP

Neon exposes an MCP server so agents can query/operate the DB directly:

```bash
claude mcp add --transport http neon https://mcp.neon.tech/mcp   # then /mcp to OAuth
```

## Migration record — 2026-08-07 (Cloud SQL → Neon)

- **Source:** GCP Cloud SQL `regattadata:us-central1:regatta-data-small`, db `regatta_data` (34 MB, 67 tables).
- **Target:** Neon project (org `noisy-tree-93015473`), db `neondb`, role `neondb_owner`, Free tier (34 MB « 0.5 GB cap).
- **Schema:** all 36 repo migrations (`0001`–`0036`) applied to Neon. GCP is a strict
  subset of the migration schema (9 extra empty tables on Neon from migrations
  0019/0021/0022/0023 that never landed on prod). No divergent drift.
- **Data:** full `pg_dump` retained backup taken; data-only load into Neon.
  Verified **35,859 rows across all 67 tables, byte-identical by content md5**
  (aggregate fingerprint `5357b17…f420bcae`).
- **Cutover smoke:** `resolution_score` + `resolution_promote` dry-runs green on
  Neon, 0 DB errors, rolled back.
- **GCP status:** left running read-only as fallback; decommission is owner-gated
  (Linear FOR-854), not part of this change.

## Fallback to Cloud SQL (during soak only)

If Neon is unavailable during the soak window, the legacy path still works:

```bash
PROJECT_ID=regattadata INSTANCE_ID=regatta-data-small scripts/dev_start_proxy.sh --background
export DB_DSN="postgresql://regatta_app@127.0.0.1:5433/regatta_data"   # password via ~/.pgpass
```

Remove this fallback once Cloud SQL is decommissioned (FOR-854).
