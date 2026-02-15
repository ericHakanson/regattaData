# Requirements: Cloud SQL Smoke-Test Execution Wrapper

## Document Purpose
Define implementation requirements for a wrapper that runs `migrations/tests/run_smoke.sh` against a Cloud SQL PostgreSQL validation target safely and repeatably.

## Audience
- Primary implementers: Claude Code or Gemini.
- Primary operators: engineers running migration validation in dev/staging.

## Objective
Provide a single command wrapper to execute migration smoke tests on Cloud SQL with:
- explicit environment targeting,
- strong safety defaults,
- deterministic logging/output,
- clean teardown behavior.

## In Scope
- Wrapper script and related documentation.
- Cloud SQL connectivity orchestration for smoke tests.
- Safety controls to prevent accidental production impact.
- Log capture and run summary output.

## Out of Scope
- Changing migration SQL logic.
- Performance benchmark execution.
- Provisioning Cloud SQL instances.

## Target Artifact
- Script path: `migrations/tests/run_smoke_cloudsql.sh`
- Companion doc update: `migrations/tests/README.md`

## Functional Requirements

### FR-1: Invocation Contract
The wrapper must support:
- CLI usage with explicit flags.
- `--help` output describing all flags and defaults.
- Non-interactive execution suitable for CI.

Minimum flags:
- `--target-env` required; allowed values: `dev`, `staging`, `prod`.
- `--project-id` required.
- `--instance-connection-name` required (`project:region:instance`).
- `--db-user` required.
- `--db-password-env` optional; env var name holding password.
- `--db-maintenance` optional; default `postgres`.
- `--db-prefix` optional; default `regatta_smoke`.
- `--keep-db` optional; default `false`.
- `--allow-prod` optional; default `false`.
- `--logs-dir` optional; default `migrations/tests/logs`.

### FR-2: Safety Gates
1. Wrapper must refuse to run when `--target-env prod` unless `--allow-prod true` is also provided.
2. Wrapper must print a clear warning banner when running on `prod`, even when allowed.
3. Wrapper must create a uniquely named temporary database per run using prefix + timestamp/random suffix.
4. Wrapper must drop the temp database on exit unless `--keep-db true`.
5. Wrapper must fail fast if required tools are unavailable.

Required tools check:
- `psql`
- `createdb`
- `dropdb`
- `gcloud`
- `cloud-sql-proxy` (or equivalent `cloud_sql_proxy` binary)

### FR-3: Cloud SQL Connectivity
The wrapper must support proxy-based connectivity by launching a local Cloud SQL proxy process for the specified instance.

Required behavior:
1. Start proxy bound to localhost on an available port.
2. Export connection variables so `run_smoke.sh` uses proxy endpoint.
3. Stop proxy on exit regardless of success/failure.
4. Fail with actionable error if proxy startup fails.

### FR-4: Smoke Suite Execution
1. Wrapper must execute `migrations/tests/run_smoke.sh`. The smoke script was minimally modified to detect a pre-provisioned database via `PGDATABASE` and skip local `createdb`/`dropdb` in that case; standalone local behavior is preserved.
2. Wrapper must pass DB connection settings via standard Postgres env vars (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` as needed).
3. Wrapper must propagate the smoke script exit code.

### FR-5: Logging and Output
1. Wrapper must create a per-run log directory under `--logs-dir`.
2. Capture:
- wrapper stdout/stderr
- smoke suite stdout/stderr
- proxy stdout/stderr
3. Print concise terminal summary:
- target instance
- target environment
- temp database name
- pass/fail result
- log directory path

### FR-6: Idempotent and Parallel-Safe Operation
1. Each run must use a unique DB name.
2. Simultaneous executions must not collide on DB name or log file names.
3. Cleanup must only drop the DB created by this run.

### FR-7: Error Handling
1. Wrapper must return non-zero on any failure.
2. On failure, summary must include failed phase/tool when detectable.
3. If cleanup fails, wrapper must report cleanup failure explicitly.

## Non-Functional Requirements

### NFR-1: Portability
- Must run on macOS and Linux with Bash 4+ compatible syntax.

### NFR-2: Security
- No secrets echoed to terminal logs.
- If `--db-password-env` is set, wrapper reads secret from environment only.
- No plaintext secrets written to repo files.

### NFR-3: Runtime Expectations
- Wrapper startup checks should complete in under 10 seconds (excluding proxy startup/network latency).

## Acceptance Criteria
1. Running with missing required flags exits non-zero and prints usage.
2. Running with `--target-env prod` and no `--allow-prod true` exits non-zero before proxy start.
3. Running in dev/staging successfully executes smoke tests against Cloud SQL and returns `0` on pass.
4. When smoke tests fail, wrapper exits non-zero and preserves logs.
5. When `--keep-db false`, temporary DB is removed after run.
6. When `--keep-db true`, temporary DB remains and name is printed.
7. Proxy process is always terminated at script exit.

## Recommended CLI Examples
```bash
# Staging validation run (default cleanup)
migrations/tests/run_smoke_cloudsql.sh \
  --target-env staging \
  --project-id my-gcp-project \
  --instance-connection-name my-gcp-project:us-central1:regatta-sql \
  --db-user regatta_app \
  --db-password-env REGATTA_DB_PASSWORD

# Keep database for post-failure inspection
migrations/tests/run_smoke_cloudsql.sh \
  --target-env dev \
  --project-id my-gcp-project \
  --instance-connection-name my-gcp-project:us-central1:regatta-sql-dev \
  --db-user regatta_app \
  --db-password-env REGATTA_DB_PASSWORD \
  --keep-db true
```

## Implementation Notes (Non-Binding)
- Prefer `mktemp -d` for run-local temp files and PID tracking.
- Use `trap` for cleanup of proxy and temp database.
- Use a dedicated local proxy port auto-selection strategy.
- Keep wrapper minimal and delegate assertions to `run_smoke.sh`.
