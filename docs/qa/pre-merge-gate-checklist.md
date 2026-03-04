# Pre-Merge Gate Checklist

## Usage
Mark each item pass/fail and attach command output evidence.

## 1) Repo Hygiene
- [ ] `git status --short` reviewed; no accidental files.
- [ ] Secret scan passes: `./scripts/qa_secret_scan.sh`.
- [ ] `.gitignore` protects local secret files.

## 2) Test Gate
- [ ] Targeted unit tests for changed areas pass.
- [ ] Targeted integration tests for changed areas pass.
- [ ] Any known failing test is documented as pre-existing.

## 3) CLI/Runtime Gate
- [ ] New/changed CLI mode appears in `--help`.
- [ ] Dry-run path validated before real-run for DB-mutating modes.
- [ ] Run report JSON generated and archived.

## 4) Data Safety Gate
- [ ] No unexpected transaction-abort behavior.
- [ ] No destructive SQL in migration/pipeline logic.
- [ ] Idempotency rerun behavior checked where applicable.

## 5) Acceptance Gate
- [ ] Acceptance criteria from spec satisfied with evidence.
- [ ] Evidence includes command + output snippet + report path.
- [ ] Reviewer notes include open risks and follow-ups.
