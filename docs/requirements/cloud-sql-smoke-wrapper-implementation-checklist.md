# Implementation Checklist: Cloud SQL Smoke Wrapper

Use this checklist to implement `migrations/tests/run_smoke_cloudsql.sh` per `docs/requirements/cloud-sql-smoke-wrapper-requirements.md`.

## 1. Setup and Scope
- [ ] Confirm target artifact path: `migrations/tests/run_smoke_cloudsql.sh`.
- [ ] Confirm wrapper executes existing `migrations/tests/run_smoke.sh` unchanged.
- [ ] Confirm no migration SQL changes are required for this task.

## 2. CLI Contract
- [ ] Implement `--help` with usage, flag descriptions, defaults, and examples.
- [ ] Implement required flags:
- [ ] `--target-env` (`dev|staging|prod`)
- [ ] `--project-id`
- [ ] `--instance-connection-name`
- [ ] `--db-user`
- [ ] Implement optional flags with defaults:
- [ ] `--db-password-env` (default unset)
- [ ] `--db-maintenance` (default `postgres`)
- [ ] `--db-prefix` (default `regatta_smoke`)
- [ ] `--keep-db` (default `false`)
- [ ] `--allow-prod` (default `false`)
- [ ] `--logs-dir` (default `migrations/tests/logs`)
- [ ] Validate invalid/missing flag values fail fast with non-zero exit and usage output.

## 3. Safety Gates
- [ ] Block `--target-env prod` unless `--allow-prod true`.
- [ ] Print high-visibility warning banner when running with `--target-env prod`.
- [ ] Generate unique temporary DB name using prefix + timestamp + random suffix.
- [ ] Ensure cleanup drops only the DB created by this run unless `--keep-db true`.

## 4. Prerequisite Tooling Checks
- [ ] Validate availability of `psql`.
- [ ] Validate availability of `createdb`.
- [ ] Validate availability of `dropdb`.
- [ ] Validate availability of `gcloud`.
- [ ] Validate availability of `cloud-sql-proxy` or `cloud_sql_proxy`.
- [ ] Fail fast with actionable error message when any tool is missing.

## 5. Cloud SQL Connectivity
- [ ] Start Cloud SQL proxy for `--instance-connection-name`.
- [ ] Bind proxy to localhost on an available port.
- [ ] Export PG connection vars for downstream commands:
- [ ] `PGHOST`
- [ ] `PGPORT`
- [ ] `PGUSER`
- [ ] `PGPASSWORD` (if configured)
- [ ] `PGDATABASE` (maintenance DB where appropriate)
- [ ] Verify proxy readiness before running smoke tests.
- [ ] Ensure proxy process is terminated in all exit paths.

## 6. Smoke Execution Orchestration
- [ ] Create the temporary DB using wrapper-resolved connection context.
- [ ] Invoke `migrations/tests/run_smoke.sh`.
- [ ] Ensure wrapper propagates smoke script exit code.
- [ ] Respect `--keep-db` behavior:
- [ ] `false`: drop temporary DB in cleanup.
- [ ] `true`: keep DB and print name clearly.

## 7. Logging and Run Summary
- [ ] Create per-run log directory under `--logs-dir`.
- [ ] Capture wrapper stdout/stderr to log files.
- [ ] Capture proxy stdout/stderr to log files.
- [ ] Capture smoke suite stdout/stderr to log files.
- [ ] Print concise terminal summary including:
- [ ] target environment
- [ ] instance connection name
- [ ] temporary DB name
- [ ] pass/fail result
- [ ] log directory path

## 8. Security Controls
- [ ] Do not print plaintext passwords.
- [ ] If `--db-password-env` is provided, read password from env var only.
- [ ] Fail with clear message if password env var name is provided but unset.
- [ ] Ensure no secrets are written to repository files.

## 9. Error Handling and Exit Semantics
- [ ] Return non-zero on any failed step.
- [ ] Include failing phase/tool in error output when detectable.
- [ ] Report cleanup failure distinctly if cleanup cannot complete.
- [ ] Keep logs available on failure for triage.

## 10. Documentation Updates
- [ ] Update `migrations/tests/README.md` with Cloud SQL wrapper usage.
- [ ] Add examples for `dev/staging` and explicit `prod` override flow.
- [ ] Document required environment variables and dependencies.

## 11. Validation Checklist (Must Pass Before Merge)
- [ ] Missing required flags returns non-zero and usage.
- [ ] `--target-env prod` without `--allow-prod true` is blocked.
- [ ] Dev/staging happy path returns zero when smoke passes.
- [ ] Failure path returns non-zero and preserves logs.
- [ ] Proxy always terminates after run.
- [ ] `--keep-db false` drops DB; `--keep-db true` preserves DB.

## 12. PR Exit Criteria
- [ ] Script is executable (`chmod +x`).
- [ ] Shell style/lint checks pass (if configured).
- [ ] Requirements traceability note references:
- [ ] `docs/requirements/cloud-sql-smoke-wrapper-requirements.md`
- [ ] Test evidence (command output snippets) attached in PR description.
