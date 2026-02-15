# Runbook 06: Post-Migration Validation Gates

## Objective
Define the required go/no-go checks after schema deployment and before production cutover.

## Gate 1: Target Cloud SQL Compatibility
1. Confirm target engine version and flags.
- `SELECT version();`
- `SHOW server_version;`
- `SHOW max_connections;`

2. Apply migration bundle to a fresh validation database on the target Cloud SQL instance.
- Must pass phases `0001` through `0006` with no manual edits.

3. Verify objects exist.
- Core tables from `docs/architecture/cloud-sql-schema-spec.md`
- Materialized views:
- `mv_missing_required_documents`
- `mv_probable_unregistered_returners`
- `mv_entity_resolution_review_queue`

Go/No-Go:
- `GO` only if all migrations and object checks pass on target Cloud SQL.

## Gate 2: Functional Smoke Validation
1. Run smoke suite.
- `migrations/tests/run_smoke.sh`

2. Required assertions from smoke suite:
- `document_status` XOR constraint enforcement (`num_nonnulls(...) = 1`).
- Partial unique index behavior for nullable uniqueness columns.
- MV refresh success for all three materialized views.
- Correct missing-doc row counts for participant/entry/yacht scope test cases.

Go/No-Go:
- `GO` only if smoke suite exits `0`.

## Gate 3: Performance Baseline
1. Seed representative data volume in staging/validation environment.
2. Capture `EXPLAIN (ANALYZE, BUFFERS)` for:
- `REFRESH MATERIALIZED VIEW mv_missing_required_documents`
- `REFRESH MATERIALIZED VIEW mv_probable_unregistered_returners`
- Priority NBA selector queries

3. Record baseline metrics:
- refresh duration
- peak CPU / IOPS
- lock wait or blocked sessions

Go/No-Go:
- `GO` only if refresh/query times meet agreed operational window.

## Gate 4: Concurrency and Operational Safety
1. Run concurrency test window:
- ingestion writes active during MV refresh scheduling window.

2. Validate no unacceptable lock contention:
- no sustained blocked writer sessions caused by refresh
- no job starvation or retry storms

3. Verify failure handling:
- refresh failure is logged and retryable
- fallback path works (degraded mode or deferred export)

Go/No-Go:
- `GO` only if refresh is operationally safe under concurrent load.

## Sign-off Checklist
- Architecture sign-off: complete
- Data QA sign-off: complete
- Operations sign-off: complete
- Cutover timestamp recorded
- Rollback owner assigned
