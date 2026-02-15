# Migration Smoke Tests

This suite validates migration apply order, key constraints, and materialized view behavior.

## What It Covers

- Applies migrations `0001` through `0006` in order on a throwaway database.
- Seeds representative event/entry/participant/yacht data.
- Verifies critical constraints:
  - `document_status` XOR subject rule
  - nullable uniqueness behavior via partial indexes
- Verifies materialized view refresh and expected row counts.

## Local Run

```bash
migrations/tests/run_smoke.sh
```

### Requirements

- Local PostgreSQL tools: `psql`, `createdb`, `dropdb`.
- A reachable Postgres server.

## Cloud SQL Run

```bash
migrations/tests/run_smoke_cloudsql.sh \
  --target-env staging \
  --project-id my-gcp-project \
  --instance-connection-name my-gcp-project:us-central1:regatta-sql \
  --db-user regatta_app \
  --db-password-env REGATTA_DB_PASSWORD
```

### Requirements

- Everything needed for local run, plus:
- `gcloud` CLI (authenticated)
- `cloud-sql-proxy` (or `cloud_sql_proxy`)
- `python3` (for port selection)

### Flags

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--target-env` | yes | — | `dev`, `staging`, or `prod` |
| `--project-id` | yes | — | GCP project ID |
| `--instance-connection-name` | yes | — | `project:region:instance` |
| `--db-user` | yes | — | Database user |
| `--db-password-env` | no | — | Env var name holding DB password |
| `--db-maintenance` | no | `postgres` | Maintenance DB for createdb/dropdb |
| `--db-prefix` | no | `regatta_smoke` | Temp DB name prefix |
| `--keep-db` | no | `false` | Retain temp DB after run |
| `--allow-prod` | no | `false` | Allow execution against prod |
| `--logs-dir` | no | `migrations/tests/logs` | Log output directory |

### Safety

- Running against `prod` requires `--allow-prod true` and prints a warning banner.
- Each run creates a uniquely named temp database and drops it on exit (unless `--keep-db true`).
- The Cloud SQL proxy is always terminated on exit regardless of pass/fail.

### Logs

Each run creates a timestamped directory under `--logs-dir` containing:

- `proxy.log` — Cloud SQL proxy output
- `smoke.log` — smoke suite output
- `cleanup.log` — cleanup errors (if any)

### Keep DB for debugging

```bash
migrations/tests/run_smoke_cloudsql.sh \
  --target-env dev \
  --project-id my-gcp-project \
  --instance-connection-name my-gcp-project:us-central1:regatta-sql-dev \
  --db-user regatta_app \
  --db-password-env REGATTA_DB_PASSWORD \
  --keep-db true
```
