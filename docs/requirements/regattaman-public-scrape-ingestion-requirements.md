# Requirements: Regattaman Public-Scrape Ingestion (2021-2025)

## Purpose
Define development requirements to ingest public/unauthenticated Regattaman scrape exports while minimizing pipeline sprawl.

Input files:
1. `rawEvidence/all_regattaman_entries_2021_2025.csv`
2. `rawEvidence/consolidated_regattaman_events_2021_2025_with_event_url.csv`

Primary priority order:
1. Data accuracy
2. Sustainable, flawless performance
3. Minimize number of pipelines

## Decision Direction
Build one unified Regattaman ingestion pipeline with source adapters, not separate codebases.

Implementation direction:
1. Extend current Python ETL package (`regatta_etl`) with a second input adapter for public scrape CSVs.
2. Keep one orchestration entrypoint and one shared normalization + upsert engine.
3. Preserve source provenance so private export and public scrape can coexist without destructive overwrites.

Separate pipeline is allowed only if hard constraints are proven:
1. Incompatible runtime/SLA requirements, or
2. Unresolvable schema/contract divergence after staged adapter design review.

## Observed Data Characteristics (For Requirement Baselines)
From local profiling of the provided files:
1. Entries file: 15,075 rows, 14 columns.
2. Events file: 867 rows, 11 columns.
3. Entries contain 5 yearly sources (`regattaman_2021` ... `regattaman_2025`).
4. Entries map to 730 unique `entries_url`; events include 867 unique `entries_url` (137 events with no scraped entries).
5. `entries_url` in entries is 100% parseable for `race_id` + `yr`.
6. `Hist` present for ~71.2% of entry rows; `sku` extractable from `Hist` when present.
7. Entries have nontrivial nulls (`Yacht Name` blank 382 rows; `Boat Type` blank 2,918 rows; ratings sparsity).
8. Events have sparse `Results URL` especially 2025 (~14.7% present).

These values are requirement input, not hard-coded assumptions.

## Scope
In scope:
1. Ingest public scrape event metadata.
2. Ingest public scrape entry/participant/yacht signals.
3. Link entries to events deterministically.
4. Upsert into existing Cloud SQL schema with idempotency and provenance.
5. Support backfill for 2021-2025 and repeatable incremental reruns.

Out of scope (this phase):
1. HTML re-scraping system changes.
2. New product-facing features.
3. Non-Regattaman source changes.

## Source Contracts
### Public Entries CSV Contract
Required columns:
- `source`, `Event Name`, `entries_url`, `Fleet`, `Name`, `Yacht Name`

Optional columns:
- `City`, `Sail Num`, `Boat Type`, `Split Rating`, `Race Rat`, `Cru Rat`, `R/C Cfg`, `Hist`

### Public Events CSV Contract
Required columns:
- `source`, `Event Name`, `entries_url`, `Host Club`

Optional columns:
- `Date`, `Day`, `When`, `Region`, `Event URL`, `Info URL`, `Results URL`

Header trimming is mandatory for all CSV ingestion paths.

## Canonical Ingestion Model
Use a two-stage model:
1. Stage: parse + normalize + validate raw rows into typed staging records.
2. Apply: deterministic mapping/upsert into core tables.

Required source tag values:
1. `registration_source='regattaman_public_scrape'` for `event_entry`.
2. `source_system='regattaman_public_scrape_csv'` for relationship/status tables.

## Accuracy Requirements
### Event Identity
1. Derive `season_year` from `source` and validate against `entries_url` `yr` query param.
2. Derive stable external event key from `entries_url` parsed `race_id` + `yr`.
3. Do not key event identity by `Event Name` alone.
4. If `race_id`/`yr` parse fails, reject row with explicit reason.

### Entry Identity
1. Entry-level dedupe key must be deterministic and stable across reruns.
2. Preferred external id priority:
   - `sku` parsed from `Hist` (when available)
   - else synthetic hash key from:
     `source_year + entries_url + Fleet + Name + Yacht Name + Sail Num`
3. Collision detection must be explicit; collisions are fatal unless disambiguated.

### Participant/Yacht Resolution
1. Public scrape lacks owner-contact quality from private export; treat participant linkage confidence as lower.
2. Resolve participants using deterministic name normalization only unless contact evidence exists.
3. Resolve yachts using normalized yacht name plus sail number when available.
4. Do not overwrite high-confidence contact data loaded from private exports with lower-confidence scrape data.

### Source Precedence
Define precedence policy by attribute:
1. Registration truth precedence:
   - private authenticated export > public scrape.
2. Supplemental fields missing in private export may be filled from public scrape (non-destructive enrich only).
3. Provenance must be stored for every write path.

### Reject Taxonomy
Must write reject rows with machine-readable reason codes, including:
1. `missing_required_column`
2. `invalid_year_or_race_id`
3. `event_link_not_found`
4. `entry_identity_collision`
5. `invalid_numeric_field`
6. `db_constraint_error`

## Performance And Reliability Requirements
1. Throughput target: full backfill (15,075 entries + 867 events) completes in under 10 minutes on standard dev Cloud SQL sizing.
2. Memory target: streaming/batched read; avoid loading full files unless explicitly needed for reconciliation.
3. Transaction model:
   - event context/batch setup transaction
   - bounded row/batch transactions with retry for transient errors
4. Idempotency:
   - rerunning same files produces zero duplicate logical records.
5. Observability:
   - structured logs with run_id and per-stage counters.
   - JSON run report in `artifacts/reports/`.
6. Failure semantics:
   - connectivity/schema failures are fatal.
   - row-level data issues go to rejects with threshold-based fail guard (configurable).

## Pipeline Architecture Requirements
1. Keep one CLI surface for Regattaman ingestion with explicit `--mode`:
   - `private_export`
   - `public_scrape`
2. Shared reusable components:
   - CSV reader/header normalizer
   - normalization utilities
   - dedupe/upsert utilities
   - run reporting/reject writer
3. Source-specific adapter modules map raw fields to canonical staging model.
4. Do not fork separate repositories or duplicated orchestration logic.

## Schema Mapping Requirements
Minimum target table coverage for public scrape mode:
1. `yacht_club`
2. `event_series`
3. `event_instance`
4. `yacht`
5. `participant`
6. `event_entry`
7. `event_entry_participant`

Optional enrichment tables (phase 2):
1. `yacht_rating`
2. `participant_address` (low confidence, optional)
3. `club_membership` (only when confidence threshold met)

## Reconciliation Requirements (Events ↔ Entries)
1. Join entries to events by exact `entries_url` after canonical URL normalization.
2. If no event row exists for an entry URL:
   - synthesize minimal event from entry metadata when permitted by policy, or
   - reject with `event_link_not_found`.
3. Produce reconciliation metrics:
   - matched entry rows
   - unmatched entry rows
   - events without entries

## Non-Functional Quality Gates
1. Type checking/linting pass for new modules.
2. Unit tests for normalization/parsing and identity-key builders.
3. Integration tests against ephemeral Postgres with migrations `0001`-`0006`.
4. Backfill smoke test using the two provided files.
5. Idempotency test: second run yields no net inserts for logical keys.
6. Regression test ensuring private-export ingestion behavior is unchanged.

## Acceptance Criteria
1. One ingestion command can run both current private export and new public scrape mode.
2. Public scrape backfill (2021-2025 files) runs successfully with:
   - deterministic event-year linking,
   - explicit rejects,
   - zero duplicate logical entries on rerun.
3. Run report includes at minimum:
   - rows_read
   - rows_rejected
   - db_phase_errors
   - events_upserted
   - entries_upserted
   - participants_upserted
   - yachts_upserted
   - unmatched_entry_urls
4. Attribute precedence rules prevent lower-confidence public data from degrading higher-confidence private data.
5. End-to-end runtime and memory remain within performance targets.

## Implementation Phasing
Phase 1: Canonical public scrape ingest (events + entries + core upserts).
Phase 2: Enrichment fields (ratings, optional membership/address with confidence rules).
Phase 3: Ongoing incremental ingestion with reconciliation dashboards/alerts.

## Open Design Questions To Resolve Before Coding
1. Should unmatched `entries_url` rows synthesize event records or always reject?
2. Which columns from public scrape are allowed to update existing participant/yacht attributes?
3. Should `event_entry_participant.role` default to `skipper`, `registrant`, or `other` for public scrape rows with ambiguous role semantics?
4. What reject-rate threshold should fail a production run?

## Implementation Checklist
### 0) Design Decisions (Blockers)
- [ ] Confirm policy for unmatched `entries_url` (`synthesize_event` vs `reject`).
- [ ] Confirm public-scrape update policy for existing participant/yacht attributes.
- [ ] Confirm default `event_entry_participant.role` for public scrape rows.
- [ ] Confirm reject-rate threshold that fails production runs.

### 1) Pipeline Unification
- [ ] Keep one CLI entrypoint and add `--mode` with values `private_export` and `public_scrape`.
- [ ] Refactor shared components (CSV reader, header normalization, reject writer, reporting) into reusable modules.
- [ ] Preserve backward-compatible behavior for `private_export` mode.

### 2) Public Scrape Adapter (Stage Layer)
- [ ] Implement parser for `all_regattaman_entries_2021_2025.csv`.
- [ ] Implement parser for `consolidated_regattaman_events_2021_2025_with_event_url.csv`.
- [ ] Enforce required columns and header trimming.
- [ ] Normalize canonical URL forms for `entries_url`, `Event URL`, `Info URL`, `Results URL`.
- [ ] Parse `source` into `season_year` and validate with `yr` query parameter.
- [ ] Parse `race_id` and `yr` from `entries_url` into canonical event key.
- [ ] Extract `sku` from `Hist` where present.

### 3) Mapping + Apply Layer
- [ ] Upsert `yacht_club`, `event_series`, `event_instance` from public events file.
- [ ] Upsert `participant`, `yacht`, `event_entry`, `event_entry_participant` from entries file.
- [ ] Implement deterministic entry identity: `sku` when present, else synthetic hash key.
- [ ] Implement source precedence rules so public scrape does not degrade higher-confidence private-export attributes.
- [ ] Stamp provenance fields (`registration_source`, `source_system`) for all writes.

### 4) Reconciliation + Rejects
- [ ] Join entries to events by canonical `entries_url`.
- [ ] Emit reconciliation counters: matched entries, unmatched entries, events-without-entries.
- [ ] Implement required reject codes:
  - [ ] `missing_required_column`
  - [ ] `invalid_year_or_race_id`
  - [ ] `event_link_not_found`
  - [ ] `entry_identity_collision`
  - [ ] `invalid_numeric_field`
  - [ ] `db_constraint_error`
- [ ] Write reject artifacts with row payload + machine-readable reason code.

### 5) Idempotency + Reliability
- [ ] Ensure rerun safety: no duplicate logical entries for same source files.
- [ ] Add bounded transaction strategy (batch/row) with transient retry where appropriate.
- [ ] Keep fatal-failure semantics for connectivity/schema errors.
- [ ] Add configurable fail-fast guard when reject rate exceeds threshold.

### 6) Observability + Reporting
- [ ] Emit structured logs keyed by `run_id`.
- [ ] Extend run report JSON to include:
  - [ ] `events_upserted`
  - [ ] `entries_upserted`
  - [ ] `participants_upserted`
  - [ ] `yachts_upserted`
  - [ ] `unmatched_entry_urls`
  - [ ] existing counters (`rows_read`, `rows_rejected`, `db_phase_errors`)

### 7) Testing
- [ ] Unit tests for parser/normalizer functions and identity-key builders.
- [ ] Unit tests for URL canonicalization and `race_id`/`yr` extraction.
- [ ] Integration tests on ephemeral Postgres with migrations `0001`-`0006`.
- [ ] Backfill smoke test using both public scrape files.
- [ ] Idempotency test (second run adds zero duplicate logical records).
- [ ] Regression tests proving `private_export` mode behavior is unchanged.

### 8) Performance Validation
- [ ] Benchmark full backfill on dev Cloud SQL sizing.
- [ ] Verify runtime target (<10 minutes) and memory behavior (streaming/batched).
- [ ] Document query/index tuning needed to maintain target performance.

### 9) Deployment Readiness
- [ ] Add runbook section for public scrape ingestion operation and rollback.
- [ ] Document required env vars/flags and production invocation examples.
- [ ] Define on-call alert triggers for high reject rate and reconciliation drift.
- [ ] Sign off acceptance criteria in this doc before production cutover.
