# Requirements: Yacht Scoring Data Ingestion (All Folders/Subfolders)

## Objective
Ingest all data under:
`rawEvidence/yacht scoring`

Scope includes nested folders and root-level files:
1. `scrapedEvents/*.csv`
2. `scrapedEntries/*.csv`
3. `deduplicated_entries.csv`
4. `unique_yachts.csv`

Primary goals:
1. Preserve all rows and all attributes (lossless raw capture).
2. Create a sustainable single pipeline mode for Yacht Scoring assets.
3. Safely enrich curated tables without unsafe merges.
4. Ensure idempotent reruns and full auditability.

## Baseline Profile (Current Snapshot)
1. Total files: 402 CSVs.
2. Total rows across all CSVs: 39,242.
3. Directories:
   - `scrapedEntries`: 370 files, 12,420 rows.
   - `scrapedEvents`: 30 files, 4,889 rows.
   - root files: 2 files, 21,933 rows.
4. Distinct schema variants: 27.
5. Large curated-like files:
   - `deduplicated_entries.csv`: 11,541 rows.
   - `unique_yachts.csv`: 10,392 rows.

Observed key parseability:
1. `scrapedEvents` `title-small href` contains parseable `/emenu/{event_id}` for all rows in current snapshot.
2. `scrapedEntries` `title-small href` contains parseable `/boatdetail/{event_id}/{entry_id}` for 12,414 rows.
3. `deduplicated_entries.csv` parseable `entryUrl` IDs align to 11,535 distinct entry IDs (high overlap with scraped entries).

These are baseline observations, not future assumptions.

## Pipeline Direction
Add one mode to unified CLI:
1. `--mode yacht_scoring`
2. Required flag: `--yacht-scoring-dir`
3. Optional flag: `--max-reject-rate` (reuse existing gate behavior)
4. Reuse common reporting/reject infrastructure (`RunCounters`, `RejectWriter`)

Do not create a separate standalone project.

## Data Model: Raw + Curated
Use a two-layer model.

### 1) Mandatory Raw Layer (Lossless)
Add migration `0010_yacht_scoring_tables.sql` with at least:

#### `yacht_scoring_raw_row`
1. `id` uuid PK.
2. `source_system` text not null default `yacht_scoring_csv`.
3. `asset_type` text not null:
   - `scraped_event_listing`
   - `scraped_entry_listing`
   - `deduplicated_entry`
   - `unique_yacht`
   - `unknown`
4. `source_file_name` text not null.
5. `source_file_path` text not null (relative path under mode root).
6. `source_row_ordinal` integer not null.
7. `source_event_id` text null (parsed from URL where possible).
8. `source_entry_id` text null (parsed from URL where possible).
9. `row_hash` text not null.
10. `raw_payload` jsonb not null.
11. `ingested_at` timestamptz not null default now().
12. Unique key: (`source_system`, `source_file_path`, `row_hash`).

Raw insert must happen before curated validation.

### 2) Curated Projection Layer
Project parseable rows into existing domain entities:
1. `yacht_club`
2. `event_series`
3. `event_instance`
4. `event_entry`
5. `event_entry_participant`
6. `yacht`
7. `participant`
8. `yacht_ownership` (only when confidence threshold is met)

Add Yacht Scoring xref tables (or equivalent mapping model):
1. `yacht_scoring_xref_event` (`event_id` -> `event_instance_id`)
2. `yacht_scoring_xref_entry` (`event_id`,`entry_id` -> `event_entry_id`)
3. `yacht_scoring_xref_yacht` (stable source key -> `yacht_id`)
4. `yacht_scoring_xref_participant` (stable source key -> `participant_id`)

Xref tables are required to keep reruns deterministic.

## Asset Classification Rules
Classify each row by source path and/or headers:
1. `deduplicated_entries.csv` -> `deduplicated_entry`.
2. `unique_yachts.csv` -> `unique_yacht`.
3. `scrapedEvents/*.csv` with event headers (`title-small href`, `w-[20%]`) -> `scraped_event_listing`.
4. `scrapedEntries/*.csv` with entry headers (`title-small href`, `title-small 2`, `flex`) -> `scraped_entry_listing`.
5. Any row/file not matching known contract -> `unknown` (raw only, no curated attempt).

## Curated Source Precedence
For duplicate/conflicting records, prefer curated sources in this order:
1. `deduplicated_entry` (highest confidence for entry-level owner/yacht rows).
2. `unique_yacht` (enrichment-only for yacht-level attributes).
3. `scraped_entry_listing` (fallback for missing records).
4. `scraped_event_listing` (event metadata and event dimension).

Precedence affects curated upserts only; raw capture always keeps all rows.

## Parsing & Normalization Requirements
### URL Parsing
1. Parse `event_id` from:
   - `.../emenu/{event_id}`
   - `.../current_event_entries/{event_id}`
2. Parse `(event_id, entry_id)` from:
   - `.../boatdetail/{event_id}/{entry_id}`
3. Rows with unparseable URLs remain in raw; curated reject if URL key is required.

### Event Fields
1. Event name:
   - from `title-small` in scraped events.
   - from `eventUrl` context where available.
2. Event location:
   - from `w-[10%]` when present.
3. Event date range:
   - from `w-[20%]` (store raw string; parse season year best-effort).

### Entry/Yacht/Owner Fields
Canonical mapping (when present):
1. Sail number: `title-small` or `sailNumber`.
2. Entry URL: `title-small href` or `entryUrl`.
3. Yacht name: `title-small 2` or `yachtName`.
4. Owner/skipper name: `flex` or `ownerName`.
5. Affiliation: `tablescraper-selected-row` or `ownerAffiliation`.
6. Location: `tablescraper-selected-row 2` or `ownerLocation`.
7. Yacht type: `tablescraper-selected-row 3` or `yachtType`.

Retain raw values and normalized values where derived.

## Entity Resolution Rules (Conservative)
### Participant
1. No email/phone in most Yacht Scoring rows -> avoid aggressive merge into existing participant graph.
2. Match existing participant only when exact normalized full name is unique.
3. If ambiguous name match, reject curated participant link (`ambiguous_participant_match`) and keep raw row.
4. Otherwise create participant and xref by stable source key:
   - normalized(owner_name + affiliation + location) when available.

### Yacht
1. Prefer xref lookup.
2. Else match by (`normalized_name`, `normalized_sail_number`) if sail exists.
3. Else match by (`normalized_name`, `yacht_type`) only when unique.
4. Ambiguous yacht candidate set -> curated reject (`ambiguous_yacht_match`), raw retained.

### Event
1. Prefer xref via `event_id`.
2. Else match using parsed `event_id` from URL.
3. Else fallback to (`normalized_event_name`, inferred season_year) only when unique.
4. Ambiguous event mapping -> curated reject (`ambiguous_event_match`), raw retained.

### Entry
1. Prefer xref on (`event_id`, `entry_id`).
2. Else upsert by (`event_instance_id`, `yacht_id`) while storing source external identifiers.
3. Never create multiple event entries for the same logical source entry.

## Duplicate Handling Policy
1. All duplicates are retained in raw layer.
2. Curated layer dedupes by stable keys and xref mappings.
3. If same source entry appears in multiple files, it should map to one curated `event_entry`.
4. Conflicting attribute values:
   - keep earliest non-null unless new value is higher confidence by source precedence.
   - never overwrite with null.

## Validation and Rejects
Run-fatal:
1. Missing required root directory.
2. No CSV files discovered.
3. DB connectivity/transaction failures.
4. Migration/schema mismatch.

Row-level curated rejects (continue):
1. Missing required identity for specific projection path.
2. Unparseable required URL key.
3. Ambiguous participant/yacht/event match.
4. DB constraint error on curated insert.

Reject artifact must include:
1. Full row payload.
2. `_asset_type`.
3. `_source_file_path`.
4. `_reject_reason` (stable code).
5. `_reject_context`.

## Idempotency Requirements
1. Raw row idempotency by (`source_system`, `source_file_path`, `row_hash`).
2. Xref uniqueness by source key.
3. Curated tables use ON CONFLICT or lookup-before-insert with stable business keys.
4. Rerun same directory must yield zero duplicate logical entities/links.

## Performance and Reliability
1. Recursively stream files; do not load all files into memory.
2. Use one outer transaction per run with per-row savepoints.
3. `--dry-run` runs full DB path and always rolls back.
4. Dry-run and real run exit non-zero when `db_phase_errors > 0`.
5. Reject-rate gate applies to curated rejects, not raw capture.

## Reporting
Extend counters with Yacht Scoring mode metrics:
1. `yacht_scoring_rows_raw_inserted`
2. `yacht_scoring_rows_curated_processed`
3. `yacht_scoring_rows_curated_rejected`
4. `yacht_scoring_events_upserted`
5. `yacht_scoring_entries_upserted`
6. `yacht_scoring_yachts_upserted`
7. `yacht_scoring_participants_upserted`
8. `yacht_scoring_xref_inserted`
9. `yacht_scoring_unknown_schema_rows`

Include per-asset read counts and warning list.

## Canonical CLI Invocation
```bash
python -m regatta_etl.import_regattaman_csv \
  --mode yacht_scoring \
  --db-dsn "$DB_DSN" \
  --yacht-scoring-dir "rawEvidence/yacht scoring" \
  --rejects-path "artifacts/rejects/yacht_scoring_rejects.csv" \
  --dry-run
```

Real run is the same command without `--dry-run`.

## Tests
### Unit
1. URL parsers for `emenu`, `current_event_entries`, `boatdetail`.
2. Schema classifier by headers/path.
3. Source precedence conflict resolver.
4. Stable source-key builders for xref tables.

### Integration
1. End-to-end run across nested folders with fixture subset.
2. Raw completeness: all input rows captured in raw table.
3. Idempotency rerun: no duplicate logical inserts.
4. Ambiguity rejects preserve raw row while skipping curated link.
5. Dry-run rollback and non-zero on DB-phase errors.

## Acceptance Criteria
1. 100% of rows under `rawEvidence/yacht scoring` captured in raw table.
2. 100% of source columns recoverable from `raw_payload`.
3. Curated projection is deterministic and idempotent.
4. Ambiguous matches do not auto-merge.
5. Real run + rerun produce stable counts with no duplicate logical entries.

## Recommended Dev Task List
### Phase 1: Schema + Contracts
1. Add `0010_yacht_scoring_tables.sql` (raw + xref tables + indexes).
2. Define known header contracts for `scrapedEvents`, `scrapedEntries`, root curated files.
3. Define unknown-schema fallback behavior (raw-only).

### Phase 2: CLI + Discovery
1. Add `--mode yacht_scoring` + `--yacht-scoring-dir`.
2. Implement recursive file discovery and deterministic file ordering.
3. Implement per-row asset classification.

### Phase 3: Parsing + Curated Upserts
1. Implement URL and field extractors with variant fallbacks.
2. Implement conservative participant/yacht/event resolution + xref writes.
3. Implement event/entry upserts with source precedence.

### Phase 4: Reliability + Reporting
1. Add savepoint handling and stable reject reason codes.
2. Add counters and report JSON fields for mode.
3. Ensure dry-run parity and failure gates.

### Phase 5: QA + Runbook
1. Add unit/integration tests above.
2. Execute full dry-run against production-like DB.
3. Run real import, then rerun for idempotency verification.
4. Archive report and reject artifacts by run ID.

## Execution Checklist
### Pre-Dev
- [ ] Confirm mode name and CLI flags.
- [ ] Confirm raw/xref table naming and migration ordering.
- [ ] Confirm source precedence policy.

### Pre-Merge
- [ ] Migration applies cleanly on empty and current DB.
- [ ] Unit + integration tests pass.
- [ ] Dry-run on real directory completes and reject profile reviewed.

### Pre-Prod
- [ ] Cloud SQL connectivity and privileges validated.
- [ ] Backup/snapshot policy confirmed.
- [ ] Production dry-run report approved.

### Post-Prod
- [ ] Raw row count matches source row count.
- [ ] Curated reject distribution reviewed and accepted.
- [ ] Rerun idempotency verified.
- [ ] Report/reject artifacts archived.

## Claude Implementation Handoff Prompt
Implement `docs/requirements/yacht-scoring-ingestion-requirements.md` exactly.

Non-negotiables:
1. Ingest all rows from all nested files into raw storage first.
2. Preserve all attributes in `raw_payload`.
3. Use one mode (`yacht_scoring`) and one directory flag (`--yacht-scoring-dir`).
4. Curated projection must be conservative and ambiguity-safe.
5. Reruns must be idempotent across raw, xref, and curated layers.
6. Dry-run must run full DB logic and rollback.

Deliverables:
1. New migration `0010_yacht_scoring_tables.sql`.
2. New ingestion module in `src/regatta_etl/`.
3. CLI wiring in `src/regatta_etl/import_regattaman_csv.py`.
4. Counter/report updates in `src/regatta_etl/shared.py`.
5. Unit + integration tests for all acceptance criteria.
