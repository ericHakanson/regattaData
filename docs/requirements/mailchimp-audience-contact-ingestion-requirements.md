# Requirements: Mailchimp Audience Contact Ingestion

## Objective
Ingest all records and attributes from:
1. `rawEvidence/audience_export_37a135f743/subscribed_email_audience_export_37a135f743.csv`
2. `rawEvidence/audience_export_37a135f743/unsubscribed_email_audience_export_37a135f743.csv`
3. `rawEvidence/audience_export_37a135f743/cleaned_email_audience_export_37a135f743.csv`

Primary goals:
1. Preserve every source attribute for every row (lossless).
2. Upsert participant/contact data with safe entity resolution (merge/create appropriately).
3. Keep reruns idempotent and auditable.

## Baseline Profile (Current Files)
1. `subscribed`: 889 rows, 21 columns.
2. `unsubscribed`: 70 rows, 25 columns.
3. `cleaned`: 63 rows, 23 columns.
4. No duplicate emails within each file.
5. No email overlap across the three files in this snapshot.
6. Timestamp fields are parseable as `%Y-%m-%d %H:%M:%S`.

These are observed baselines, not permanent assumptions.

## Pipeline Direction
Use existing `regatta_etl` package and unified CLI.
1. Add `--mode mailchimp_audience`.
2. Reuse shared components (`RejectWriter`, run report, counters, normalization helpers).
3. Do not create a separate repo or runtime.

## Canonical CLI Invocation
```bash
python -m regatta_etl.import_regattaman_csv \
  --mode mailchimp_audience \
  --db-dsn "$DB_DSN" \
  --subscribed-path "rawEvidence/audience_export_37a135f743/subscribed_email_audience_export_37a135f743.csv" \
  --unsubscribed-path "rawEvidence/audience_export_37a135f743/unsubscribed_email_audience_export_37a135f743.csv" \
  --cleaned-path "rawEvidence/audience_export_37a135f743/cleaned_email_audience_export_37a135f743.csv" \
  --rejects-path "artifacts/rejects/mailchimp_audience_rejects.csv" \
  --dry-run
```

Real run is the same command without `--dry-run`.

## Data Retention Model
### 1) Mandatory Raw Capture (Lossless)
Add a raw table (new migration) for immutable row capture, e.g. `mailchimp_audience_row`:
1. `id` uuid PK.
2. `source_system` text not null default `mailchimp_audience_csv`.
3. `source_file_name` text not null.
4. `audience_status` text not null check in (`subscribed`, `unsubscribed`, `cleaned`).
5. `source_email_raw` text not null.
6. `source_email_normalized` text not null.
7. `row_hash` text not null.
8. `raw_payload` jsonb not null (full row as ingested).
9. `ingested_at` timestamptz not null default now().
10. unique constraint on (`source_system`, `source_file_name`, `row_hash`).

### 2) Curated Contact Projection
Project into existing person-contact model:
1. `participant` (name-based attributes).
2. `participant_contact_point` (email/phone).
3. `participant_address` (address string + parsed components when feasible).

Add Mailchimp-specific status/history table (new migration), e.g. `mailchimp_contact_state`:
1. `participant_id` FK.
2. `email_normalized` text not null.
3. `audience_status` (`subscribed`/`unsubscribed`/`cleaned`) not null.
4. `status_at` timestamptz null (from `LAST_CHANGED`, `UNSUB_TIME`, `CLEAN_TIME` as available).
5. status metadata columns:
   - unsub fields: `unsub_campaign_title`, `unsub_campaign_id`, `unsub_reason`, `unsub_reason_other`
   - clean fields: `clean_campaign_title`, `clean_campaign_id`
   - subscribe fields: `last_changed`
6. `leid`, `euid`, `member_rating`, `optin_*`, `confirm_*`, timezone fields, country/region, notes.
7. unique key on (`participant_id`, `audience_status`, `status_at`, `source_file_name`, `row_hash`) or equivalent conflict-safe identity.

Add tag bridge table (new migration), e.g. `mailchimp_contact_tag`:
1. `participant_id` FK.
2. `email_normalized`.
3. `tag_value` text not null.
4. provenance fields (`source_system`, `source_file_name`, `observed_at`).
5. unique key on (`participant_id`, `email_normalized`, `tag_value`, `source_file_name`).

## Entity Resolution Rules
Apply deterministic resolution in this exact order:
1. Normalize email (`trim+lower`) and resolve by existing `participant_contact_point` email normalized value.
2. Else resolve by `participant.normalized_full_name` only when name is present and non-ambiguous.
3. Else create new `participant`.

Ambiguity handling:
1. If email maps to multiple participants, reject row with `ambiguous_email_match`; do not guess.
2. If name-only maps to multiple participants, reject row with `ambiguous_name_match`; do not guess.

Merge/update policy:
1. Never overwrite higher-confidence non-null values with null/blank.
2. Fill nulls only for optional participant fields.
3. Contact points and addresses: lookup-before-insert; no duplicate logical rows.

## Field Normalization Requirements
1. `Email Address` -> normalized email.
2. `Phone Number` -> normalized via existing phone normalizer.
3. `Address` -> preserve raw string; parse best-effort components without dropping raw.
4. Timestamps (`OPTIN_TIME`, `CONFIRM_TIME`, `LAST_CHANGED`, `UNSUB_TIME`, `CLEAN_TIME`) parse `%Y-%m-%d %H:%M:%S`.
5. `TAGS` parse as comma-delimited tag list with quote-aware splitting; trim each tag and drop empties.
6. Preserve all source IDs (`LEID`, `EUID`) exactly as text.

## Status Semantics
1. Rows from subscribed file -> state `subscribed`.
2. Rows from unsubscribed file -> state `unsubscribed`.
3. Rows from cleaned file -> state `cleaned`.
4. Status rows are append-only history (do not delete previous states).
5. If the same contact appears with multiple states in future files, retain all states with timestamps.

## Validation and Rejects
Fatal run errors:
1. Missing required headers by file type.
2. DB connectivity/transaction failures.
3. Migration/schema mismatch.

Row-level rejects (continue run, record reason):
1. missing/blank `Email Address`.
2. malformed email after normalization.
3. ambiguous email match.
4. ambiguous name match.
5. unparseable required datetime when required for status event.
6. db_constraint_error.

Reject artifact must include:
1. full source row.
2. `_reject_reason`.
3. source file name and audience status.

## Idempotency Requirements
1. Raw table conflict-safe by (`source_system`, `source_file_name`, `row_hash`).
2. Curated tables must use lookup-before-insert or ON CONFLICT with stable keys.
3. Rerunning same files must produce zero duplicate logical contact points, addresses, status records, and tags.

## Performance and Reliability
1. Stream CSV reads; do not require in-memory full-file materialization.
2. Use bounded transactions with per-row savepoints.
3. Dry-run executes full DB path and always rolls back.
4. Dry-run exits non-zero if DB-phase errors or configured reject-rate threshold are exceeded.

## Reporting
Extend run report counters to include:
1. `rows_read`, `rows_rejected`, `db_phase_errors`.
2. `raw_rows_inserted`.
3. `participants_inserted`, `participants_matched_existing`.
4. `contact_points_inserted`, `addresses_inserted`.
5. `mailchimp_status_rows_inserted`.
6. `mailchimp_tags_inserted`.
7. `warnings`.

## Tests
### Unit
1. Tag parser for quoted/comma-separated values.
2. Date parser for Mailchimp datetime fields.
3. Email/phone normalization edge cases.

### Integration
1. End-to-end run with all three files.
2. Idempotency rerun asserts zero new logical inserts.
3. Ambiguous-email resolution reject case.
4. Raw-capture completeness (all source rows present in raw table).
5. Dry-run rollback and non-zero on DB-phase error.

## Acceptance Criteria
1. 100% source rows are preserved in raw capture.
2. 100% source columns remain recoverable from `raw_payload`.
3. Contact projection is deterministic and idempotent.
4. Entity resolution never guesses on ambiguous matches.
5. Rerun of the same inputs creates no duplicate logical contact data.

## Implementation Checklist
### 0) Decisions
- [ ] Confirm final table names for Mailchimp raw/status/tag storage.
- [ ] Confirm reject-rate threshold for production runs.

### 1) Schema
- [ ] Add migration for Mailchimp raw/status/tag tables and indexes.
- [ ] Add appropriate unique constraints/indexes for idempotency and lookup speed.

### 2) Pipeline
- [ ] Add `--mode mailchimp_audience` to unified CLI.
- [ ] Implement file-type adapter for subscribed/unsubscribed/cleaned CSV contracts.
- [ ] Implement row-level resolution + curated upsert logic.

### 3) Quality
- [ ] Implement reject writer with machine-readable reason codes.
- [ ] Implement run-report counters.
- [ ] Add required unit and integration tests.
- [ ] Verify rerun idempotency on all three files.

## Recommended Dev Task List
### Phase 1: Schema + Contracts (Blockers First)
1. Create migration for `mailchimp_audience_row`, `mailchimp_contact_state`, and `mailchimp_contact_tag`.
2. Add all idempotency constraints and lookup indexes.
3. Define exact CSV header contracts per file type (`subscribed`, `unsubscribed`, `cleaned`).
4. Add schema-level enums/check constraints for valid audience statuses.
5. Add rollback migration and verify up/down locally.

### Phase 2: Mode Wiring + Parsing
1. Add `--mode mailchimp_audience` to `import_regattaman_csv.py` dispatch.
2. Implement Mailchimp loader module (single pipeline, multi-file support).
3. Implement file classifier and per-file required-header validation.
4. Implement normalization/parsing helpers for tags, datetimes, and address parsing.
5. Add raw-lossless insert first in row flow before curated projection validation.

### Phase 3: Resolution + Curated Upserts
1. Implement deterministic participant resolution chain (email -> name-non-ambiguous -> insert).
2. Implement ambiguity rejects (`ambiguous_email_match`, `ambiguous_name_match`).
3. Implement contact point/address lookup-before-insert behavior.
4. Implement append-only `mailchimp_contact_state` writes.
5. Implement tag normalization and bridge-table idempotent inserts.

### Phase 4: Reliability + Reporting
1. Add per-row savepoint handling for DB-phase row failures.
2. Ensure run-fatal behavior on DB connectivity/transaction errors.
3. Ensure dry-run does full DB execution path and always rolls back.
4. Add reject-rate threshold gate and non-zero exit behavior.
5. Emit run report JSON with all Mailchimp counters.

### Phase 5: Test + Release
1. Add unit tests for tag/date/email/phone parsing edge cases.
2. Add integration tests for full import, idempotency rerun, ambiguity rejects, and raw capture completeness.
3. Run dry-run on the three production files and review rejects.
4. Run real import once; then rerun to verify no duplicate logical inserts.
5. Record verification queries and final run artifacts in the PR.

## Execution Checklist (Recommended)
### Pre-Dev
- [ ] Confirm business owner acceptance of ambiguity behavior (reject vs. tie-break).
- [ ] Confirm production reject-rate threshold.
- [ ] Confirm migration naming/version ordering with current migration chain.

### Pre-Merge
- [ ] All new migrations apply cleanly to empty DB and current DB.
- [ ] Unit tests pass.
- [ ] Integration tests pass (including idempotency rerun).
- [ ] Dry-run on all three Mailchimp files passes threshold gates.
- [ ] Reject file reviewed and reason distribution understood.

### Pre-Prod Run
- [ ] Cloud SQL connectivity and app-user privileges validated.
- [ ] Backup/snapshot policy confirmed.
- [ ] `--dry-run` executed against production target DB and reviewed.
- [ ] Real run command prepared with explicit file paths and run ID.

### Post-Prod Validation
- [ ] Raw row count equals total input rows across all files.
- [ ] Curated counters and reject counts match expectations.
- [ ] Rerun produces zero duplicate logical inserts.
- [ ] Final report JSON and rejects CSV archived with run ID.
