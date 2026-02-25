# Requirements: Airtable Copy Ingestion (Multi-Asset, Enrichment-First)

## Objective
Ingest all records and attributes from:
1. `rawEvidence/AirtableCopy/clubs-Grid view.csv`
2. `rawEvidence/AirtableCopy/events-Grid view.csv`
3. `rawEvidence/AirtableCopy/entries-Grid view.csv`
4. `rawEvidence/AirtableCopy/yachts-Grid view.csv`
5. `rawEvidence/AirtableCopy/owners-Grid view.csv`
6. `rawEvidence/AirtableCopy/participants-Grid view.csv`

Primary goals:
1. Preserve 100% of source rows and source columns (lossless).
2. Ingest all asset types through one pipeline mode.
3. Handle duplicates safely: avoid false merges, allow enrichment where confidence is high.
4. Keep reruns fully idempotent and auditable.

## Source Profile (Current Snapshot)
1. `entries`: 26,616 rows, 23 columns.
2. `events`: 2,963 rows, 22 columns.
3. `yachts`: 5,680 rows, 16 columns.
4. `owners`: 3,867 rows, 26 columns (many sparse rows).
5. `clubs`: 1,481 rows, 16 columns.
6. `participants`: 298 rows, 24 columns.

Observed source/asset variants:
1. Entries `source`: `regattaman_2021..2025`, `yachtScoring`.
2. Events `source`: `regattaman_2021..2025`, `yachtScoring_2021..2025`.

Observed ID quality:
1. `*_global_id` fields are unique where present.
2. `event_global_id` is blank on many rows (2,131/2,963).
3. Data includes many duplicate candidate links and cross-system references.

These are baseline observations, not assumptions for future files.

## Pipeline Direction
Use one pipeline mode in existing CLI:
1. Add `--mode airtable_copy`.
2. Add one required flag: `--airtable-dir` (directory containing all 6 CSVs).
3. Keep pipeline in current `regatta_etl` package; do not create a separate project.

## Ingestion Strategy
Two-layer model is mandatory:
1. Raw layer: append-only lossless capture of every row from every file, regardless of quality.
2. Curated layer: enrichment-only projection into existing domain tables using conservative matching.

No source row is dropped from raw capture.

## Raw Layer Requirements (Lossless)
Add migration `0009_airtable_copy_tables.sql` with at least:

### `airtable_copy_row`
1. `id` uuid PK.
2. `source_system` text not null default `airtable_copy_csv`.
3. `asset_name` text not null check in (`clubs`, `events`, `entries`, `yachts`, `owners`, `participants`).
4. `source_file_name` text not null.
5. `source_row_ordinal` integer not null (1-based data-row index).
6. `source_primary_id` text null (`club_global_id`, `event_global_id`, `entries_global_id`, etc.).
7. `source_type` text null (from `source` column where available).
8. `row_hash` text not null.
9. `raw_payload` jsonb not null.
10. `ingested_at` timestamptz not null default now().
11. Unique key on (`source_system`, `asset_name`, `source_file_name`, `row_hash`).

### `airtable_copy_reject`
1. Optional but recommended for machine-queryable reject analytics.
2. If not created, continue using CSV reject artifact.

Raw insert must run before curated validation.

## Curated Layer Requirements
Reuse existing tables where possible and upsert conservatively:
1. `participant`
2. `participant_contact_point`
3. `participant_address`
4. `yacht`
5. `yacht_club`
6. `event_series`, `event_instance`, `event_entry`, `event_entry_participant`
7. Link tables already in schema (`yacht_ownership`, `club_membership`) where confidence permits.

### Enrichment Policy
Because this dataset is scraped + manually assembled:
1. Treat as medium-confidence enrichment source.
2. Never overwrite trusted non-null values with lower-confidence values.
3. Default to fill-nulls-only on core identity fields.
4. Record provenance on new/updated rows via `source_system='airtable_copy_csv'`.

## Entity Resolution Rules (Safe Matching)
Apply deterministic, strict ordering. Reject ambiguity; do not guess.

### Participant
1. Email exact normalized match via `participant_contact_point` (preferred).
2. Else phone exact normalized match (only if unique).
3. Else normalized full name exact match (only if unique).
4. Else insert new participant.
5. If multiple matches at any step: reject curated projection for that row (`ambiguous_participant_match`), keep raw row.

### Yacht
1. If `yacht_global_id` has prior mapping in local xref table, use it.
2. Else match by (`normalized_name`, `normalized_sail_number`) when sail number exists.
3. Else match by (`normalized_name`, `length_feet`) when length exists.
4. No broad name-only merge unless exactly one candidate exists and no conflicting sail/length evidence.
5. On ambiguity: reject curated projection (`ambiguous_yacht_match`), keep raw row.

### Club
1. If `club_global_id` mapping exists, use it.
2. Else match by normalized club name exactly.
3. Else insert new club with conservative default status (`unknown`).

### Event
1. Prefer parsed `entries_url`/`Event URL` race_id + year tuple when available.
2. Else use existing global IDs where non-null.
3. Else fallback to (`event_name`, normalized host club, inferred season year) only if unique.
4. Ambiguous event link -> curated reject (`ambiguous_event_match`), raw retained.

## Crosswalk/Xref Requirement
Add xref tables to stabilize reruns and cross-asset joins:
1. `airtable_xref_participant(source_primary_id -> participant_id)`
2. `airtable_xref_yacht(source_primary_id -> yacht_id)`
3. `airtable_xref_club(source_primary_id -> yacht_club_id)`
4. `airtable_xref_event(source_primary_id or parsed race key -> event_instance_id)`

Each xref must include `source_system`, `asset_name`, `created_at`, `last_seen_at`.

## Asset-Type Handling
Capture and preserve source type taxonomy as-is:
1. `regattaman_2021..2025`
2. `yachtScoring`
3. `yachtScoring_2021..2025`
4. future unknown values (must be accepted, not rejected).

Do not hard-fail on unknown `source` value; write warning counter.

## Validation and Rejects
Run-fatal errors:
1. Missing required files.
2. Missing required headers per file contract.
3. DB connectivity/transaction errors that prevent progress.
4. Migration/schema mismatch.

Row-level curated rejects (continue run):
1. Missing minimum identity for target entity (e.g., no email/phone/name for participant projection).
2. Ambiguous participant/yacht/club/event match.
3. Unparseable strict field required for a specific projection path.
4. Constraint violation on curated insert (`db_constraint_error`).

Reject output must include:
1. Full row payload.
2. `asset_name`.
3. `_reject_reason` (stable code).
4. `_reject_context` (short machine-readable context string).

## Idempotency Requirements
1. Raw rows idempotent by unique raw key.
2. Curated insert paths must use ON CONFLICT or lookup-before-insert with stable keys.
3. Xref tables must prevent remapping drift across reruns.
4. Rerunning identical input set must produce zero duplicate logical entities/relationships.

## Processing Order
Use deterministic order for better linkage:
1. Clubs
2. Events
3. Yachts
4. Owners
5. Participants
6. Entries

If dependencies are unresolved, keep raw row and reject curated projection for that row.

## Performance and Reliability
1. Stream each CSV file; avoid loading full files into memory.
2. Use one outer transaction per run with per-row savepoints.
3. `--dry-run` executes full DB path and always rolls back.
4. Dry-run and real run must both exit non-zero if DB-phase errors exceed zero.

## Reporting
Extend `RunCounters` for this mode with:
1. `airtable_rows_raw_inserted`
2. `airtable_rows_curated_processed`
3. `airtable_rows_curated_rejected`
4. `airtable_xref_inserted`
5. `airtable_warnings_unknown_source_type`
6. per-asset counts read/inserted/rejected (clubs/events/entries/yachts/owners/participants)

Run report JSON must include all mode counters plus warning list.

## Canonical CLI Invocation
```bash
python -m regatta_etl.import_regattaman_csv \
  --mode airtable_copy \
  --db-dsn "$DB_DSN" \
  --airtable-dir "rawEvidence/AirtableCopy" \
  --rejects-path "artifacts/rejects/airtable_copy_rejects.csv" \
  --dry-run
```

Real run is the same command without `--dry-run`.

## Tests
### Unit
1. URL/race key parsing from `entries_url` and `Event URL`.
2. Source type normalization and acceptance of unknown types.
3. Identity extractors for each asset (`*_global_id`, fallback keys).
4. Duplicate-safe matchers and ambiguity detection logic.

### Integration
1. End-to-end import on a fixture subset covering all 6 assets.
2. Raw completeness: every source row present in `airtable_copy_row`.
3. Idempotency rerun: zero duplicate logical inserts.
4. Ambiguity paths produce curated reject + preserved raw row.
5. Dry-run rollback: no persisted writes.

## Acceptance Criteria
1. 100% of Airtable rows are persisted in raw layer.
2. 100% of Airtable columns are recoverable from `raw_payload`.
3. Curated enrichment is deterministic and idempotent.
4. Ambiguous matches never auto-merge.
5. Rerun of same inputs yields no duplicate logical entities/links.

## Recommended Dev Task List
### Phase 1: Contracts + Migration
1. Define per-file header contracts and required fields.
2. Add `0009_airtable_copy_tables.sql` (raw + xref + optional reject table).
3. Add indexes/constraints for idempotency and lookup speed.

### Phase 2: CLI + Loader
1. Add `--mode airtable_copy` and `--airtable-dir` validation.
2. Build a single orchestrator that loads all six files in fixed order.
3. Add per-asset adapters (clubs/events/yachts/owners/participants/entries).

### Phase 3: Matching + Projection
1. Implement strict participant/yacht/club/event matchers.
2. Implement xref upsert/read paths.
3. Implement fill-nulls-only enrichment writes.
4. Implement curated reject paths for ambiguities.

### Phase 4: Reliability + Observability
1. Add per-row savepoints and stable reject reason codes.
2. Add counters and run report fields for Airtable mode.
3. Add dry-run parity and non-zero exit gates.

### Phase 5: QA + Release
1. Add unit/integration tests listed above.
2. Dry-run full Airtable directory and review rejects.
3. Real run and immediate rerun to verify idempotency.
4. Archive report/reject artifacts by run ID.

## Execution Checklist
### Pre-Dev
- [ ] Confirm source confidence policy: Airtable is enrichment-only, fill-nulls-only.
- [ ] Confirm xref table naming and migration ordering.
- [ ] Confirm required headers for each CSV.

### Pre-Merge
- [ ] Migration applies cleanly to empty + current schema.
- [ ] Unit tests pass.
- [ ] Integration tests pass.
- [ ] Full dry-run on `rawEvidence/AirtableCopy` is successful or expected rejects are reviewed.

### Pre-Prod
- [ ] Cloud SQL connectivity and privileges validated.
- [ ] Backup/snapshot policy confirmed.
- [ ] Production dry-run report reviewed.

### Post-Prod
- [ ] Raw row totals match source row totals per asset.
- [ ] Curated reject reasons reviewed and accepted.
- [ ] Rerun idempotency verified.
- [ ] Final report and reject artifacts archived.

## Claude Implementation Handoff Prompt
Implement `docs/requirements/airtable-copy-ingestion-requirements.md` exactly.

Constraints:
1. Use one pipeline mode: `--mode airtable_copy` with `--airtable-dir`.
2. Preserve all rows/attributes losslessly in raw storage before curated validation.
3. Treat Airtable as enrichment-only, fill-nulls-only for trusted identity fields.
4. Reject ambiguous matches (participant/yacht/club/event) instead of guessing.
5. Use deterministic matching and xref tables to stabilize reruns.
6. Enforce idempotency across raw, curated, and xref layers.
7. Dry-run must execute full DB path and always roll back.
8. Both dry-run and real run must exit non-zero when `db_phase_errors > 0`.

Deliverables:
1. Migration `0009_airtable_copy_tables.sql`.
2. New ingestion module in `src/regatta_etl/` for Airtable mode.
3. CLI wiring in `src/regatta_etl/import_regattaman_csv.py`.
4. Counter/report updates in `src/regatta_etl/shared.py`.
5. Unit + integration tests covering all acceptance criteria.

Definition of done:
1. Tests pass.
2. Full dry-run on `rawEvidence/AirtableCopy` succeeds or expected rejects are documented.
3. Real run + rerun demonstrate no duplicate logical inserts.
4. Run report and rejects artifacts are produced and summarized.
