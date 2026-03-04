# Parallel QA Runbook (Claude Sessions)

## Purpose
Provide a repeatable QA process while Claude builds in parallel for:

1. `docs/requirements/club-participant-yacht-promotion-enable-spec.md`
2. `docs/requirements/mailchimp-event-registration-activation-spec.md`

This runbook is intentionally implementation-agnostic and evidence-first.

## Ground Rules
1. Do not block implementation sessions with speculative refactors.
2. Validate against acceptance criteria from each spec, not assumptions.
3. Capture command, output, and run/report IDs for every pass/fail claim.
4. Prefer dry-run first for all DB-mutating modes.

## Pre-QA Setup
```bash
source /Users/erichakanson/projects/regattaData/.venv/bin/activate
```

Optional DB setup:
```bash
export REGATTA_DB_PASSWORD='***'
export PGPASSWORD="$REGATTA_DB_PASSWORD"
export DB_DSN="postgresql://regatta_app@127.0.0.1:5433/regatta_data"
```

## Session A QA: Club/Participant/Yacht Promotion Enablement

### A1) Baseline State Snapshot
```bash
psql "$DB_DSN" -c "
SELECT entity_type, resolution_state, is_promoted, COUNT(*) AS n
FROM (
  SELECT 'club' AS entity_type, resolution_state, is_promoted FROM candidate_club
  UNION ALL
  SELECT 'participant', resolution_state, is_promoted FROM candidate_participant
  UNION ALL
  SELECT 'yacht', resolution_state, is_promoted FROM candidate_yacht
) s
GROUP BY 1,2,3
ORDER BY 1,2,3;
"
```

### A2) Score Dry-Run and Real Run
```bash
.venv/bin/regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type all --dry-run
.venv/bin/regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type all
```

### A3) Promote Dry-Run
```bash
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type all --dry-run
```

### A4) Promote Real Run (only if dry-run passes)
```bash
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type all
```

### A5) Post-Run Verification
```bash
psql "$DB_DSN" -c "
SELECT entity_type, resolution_state, is_promoted, COUNT(*) AS n
FROM (
  SELECT 'club' AS entity_type, resolution_state, is_promoted FROM candidate_club
  UNION ALL
  SELECT 'participant', resolution_state, is_promoted FROM candidate_participant
  UNION ALL
  SELECT 'yacht', resolution_state, is_promoted FROM candidate_yacht
) s
GROUP BY 1,2,3
ORDER BY 1,2,3;
"
```

```bash
psql "$DB_DSN" -c "
SELECT
  (SELECT COUNT(*) FROM canonical_club) AS canonical_club,
  (SELECT COUNT(*) FROM canonical_participant) AS canonical_participant,
  (SELECT COUNT(*) FROM canonical_yacht) AS canonical_yacht;
"
```

### A6) Required Evidence
1. `resolution_score` run report JSON path and counters.
2. `resolution_promote` dry-run report JSON.
3. `resolution_promote` real-run report JSON (if executed).
4. Before/after SQL snapshots.

## Session B QA: Mailchimp Event Registration Activation

### B1) Mode Availability
```bash
.venv/bin/regatta-import --help | grep -n "mailchimp_event_activation"
```

### B2) Dry-Run CSV Generation
```bash
.venv/bin/regatta-import \
  --mode mailchimp_event_activation \
  --db-dsn "$DB_DSN" \
  --event-window-days 45 \
  --segment-type all \
  --delivery-mode csv \
  --output-path "artifacts/exports/mailchimp_event_activation.csv" \
  --dry-run
```

### B3) Real CSV Run (only after dry-run review)
```bash
.venv/bin/regatta-import \
  --mode mailchimp_event_activation \
  --db-dsn "$DB_DSN" \
  --event-window-days 45 \
  --segment-type all \
  --delivery-mode csv \
  --output-path "artifacts/exports/mailchimp_event_activation.csv"
```

### B4) Suppression and Dedupe Checks
Use implementation-provided audit tables if present (`mailchimp_activation_run`, `mailchimp_activation_row`):

```bash
psql "$DB_DSN" -c "
SELECT status, COUNT(*) AS n
FROM mailchimp_activation_run
GROUP BY 1
ORDER BY 1;
"
```

```bash
psql "$DB_DSN" -c "
SELECT
  COUNT(*) AS rows_total,
  COUNT(DISTINCT email_normalized) AS distinct_emails,
  COUNT(*) FILTER (WHERE is_suppressed) AS suppressed_rows
FROM mailchimp_activation_row;
"
```

### B5) Required Evidence
1. Dry-run output + report JSON path.
2. Real-run output + report JSON path (if executed).
3. CSV row count and unique email count.
4. Suppression checks proving unsubscribed/cleaned rows were excluded.

## Test Gate (Both Sessions)
Run these at minimum:
```bash
.venv/bin/pytest -q tests/unit/test_resolution_score.py tests/integration/test_resolution_score_and_promote.py
```

If activation tests exist, add:
```bash
.venv/bin/pytest -q tests/unit/test_mailchimp_activation.py tests/integration/test_mailchimp_activation.py
```

## Exit Criteria
QA signoff requires:
1. No unexpected DB transaction aborts.
2. All acceptance-criteria SQL checks pass.
3. Dry-run before real-run for every mutating mode.
4. Run reports archived and referenced in review notes.
