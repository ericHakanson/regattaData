# Requirements: Mailchimp Event Registration Activation (CDP-Driven)

## 1. Objective
Use this database as the source-of-truth CDP to drive effective email communications for forthcoming event registration.

Primary outcome:
1. Generate high-quality, suppression-safe Mailchimp audiences for upcoming event registration.
2. Keep audience selection deterministic, auditable, and repeatable from DB state.
3. Provide shortest path to execution (CSV export first), with API sync as optional automation.

## 2. Business Outcome (Now)
Focus immediately on event-registration communications:
1. Reach members likely to register for upcoming events.
2. Respect unsubscribe/cleaned states.
3. Avoid duplicate contacts and inconsistent targeting.

Future channels (Twilio, social, etc.) are out of scope for this spec.

## 3. Baseline Dependencies
Assumes existing tables/pipelines are present:
1. Canonical data: `canonical_participant`, `canonical_event`, `canonical_registration`.
2. Mailchimp ingestion data: `mailchimp_audience_row`, `mailchimp_contact_state`, `mailchimp_contact_tag`.
3. Resolution/promotion pipeline already run for events/registrations (and optionally participant when available).

If any dependency is missing, pipeline must fail fast with clear error text.

## 4. Scope
In scope (v1):
1. Audience eligibility SQL based on forthcoming events.
2. Suppression logic from Mailchimp status history.
3. Deterministic dedupe at normalized email level.
4. Export artifact generation (CSV).
5. Optional Mailchimp API upsert mode behind explicit flag.
6. Run reports and persisted audience snapshots for audit.

Out of scope (v1):
1. Campaign creative/content generation.
2. Twilio/social activation.
3. Bi-directional CRM sync.

## 5. Activation Modes
Implement mode:
1. `--mode mailchimp_event_activation`

### 5.1 Delivery Modes
1. `csv` (default, fastest path): write ready-to-import CSV.
2. `api` (optional): upsert contacts and tags via Mailchimp API.

## 6. CLI Contract
Add options:
1. `--mode mailchimp_event_activation`
2. `--db-dsn`
3. `--event-window-days` (default `45`)
4. `--segment-type` (`upcoming_registrants`, `likely_registrants`, `all`; default `all`)
5. `--delivery-mode` (`csv`, `api`; default `csv`)
6. `--output-path` (required for `csv`)
7. `--mailchimp-list-id` (required for `api`)
8. `--mailchimp-api-key-env` (default `MAILCHIMP_API_KEY`)
9. `--dry-run`

Example (CSV first):
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

## 7. Audience Definitions
All audiences must be generated from DB state only.

### 7.1 Segment A: `upcoming_registrants`
People already registered for events starting within window.

Eligibility:
1. `canonical_registration` linked to `canonical_event` with start date between now and now + window.
2. Linked `canonical_primary_participant_id` exists and has non-null email.
3. Email passes suppression filters (Section 8).

### 7.2 Segment B: `likely_registrants`
People not currently registered for an upcoming event but likely to register.

Eligibility heuristic (v1):
1. Participant has historical registrations in past N seasons (default 3).
2. Participant not currently registered for the targeted upcoming event(s).
3. Non-null email and passes suppression filters.

### 7.3 Segment C: `all`
Union of A and B, deduped by normalized email.

## 8. Suppression Rules (Mandatory)
For each normalized email, evaluate latest known Mailchimp state:
1. If latest state is `unsubscribed` => suppress.
2. If latest state is `cleaned` => suppress.
3. If no Mailchimp history => eligible (unless other local suppression flags are added later).

Never auto-resubscribe suppressed contacts.

## 9. Dedupe and Identity Rules
1. Primary key for delivery is normalized email.
2. One output row per normalized email.
3. If multiple participants share email, pick deterministic winner:
   - highest canonical confidence score, then
   - most recently updated participant, then
   - lowest UUID lexical tie-breaker.
4. Record all contributing participant IDs in audit payload for traceability.

## 10. Output Schema (CSV)
Required columns:
1. `email`
2. `first_name`
3. `last_name`
4. `display_name`
5. `segment_types` (comma-delimited: `upcoming_registrants`, `likely_registrants`)
6. `upcoming_event_count`
7. `historical_registration_count`
8. `suppression_status` (`eligible` only in final export; suppressed rows tracked in audit table/report)
9. `source_participant_id`
10. `generated_at`

Optional enrichment columns:
1. `club_name`
2. `yacht_name`
3. `last_registered_event_name`

## 11. Persistence and Audit
Add new tables:

### 11.1 `mailchimp_activation_run`
1. `id` uuid PK
2. `started_at`, `finished_at`
3. `mode` (`csv`|`api`)
4. `segment_type`
5. `event_window_days`
6. `status` (`running`|`ok`|`failed`)
7. `counters` jsonb
8. `created_by` text

### 11.2 `mailchimp_activation_row`
1. `id` uuid PK
2. `run_id` FK
3. `email_normalized`
4. `participant_id`
5. `segment_types` text[]
6. `is_suppressed` bool
7. `suppression_reason` text nullable
8. `payload` jsonb (full export row shape)
9. unique (`run_id`, `email_normalized`)

## 12. API Mode Requirements (Optional in v1 rollout)
If `delivery-mode=api`:
1. Upsert members into configured list/audience.
2. Apply tags:
   - `segment:upcoming_registrants`
   - `segment:likely_registrants`
   - `source:regattadata_cdp`
3. Never call subscribe/reactivate endpoints for suppressed contacts.
4. Handle API throttling/retries with bounded backoff.
5. Store per-row API outcome in run counters and warning list.

## 13. Idempotency
1. Same run inputs should produce identical deduped audience rows.
2. CSV mode: deterministic row ordering (by email).
3. API mode: repeated runs should converge (upsert/tag idempotency), not duplicate members.

## 14. Reporting
Run report must include:
1. `rows_considered`
2. `rows_eligible`
3. `rows_suppressed_unsubscribed`
4. `rows_suppressed_cleaned`
5. `rows_deduped_out`
6. `rows_exported_csv`
7. `rows_api_upserted`
8. `rows_api_failed`
9. `db_errors`
10. `warnings`

## 15. Safety and Compliance
1. Respect suppression states from Mailchimp history.
2. No silent fallback that broadens audience scope.
3. Fail fast if API mode requested without list ID/API key.
4. Dry-run executes full selection/suppression logic and writes no external side effects.
5. Production campaigns must use gated segments/tags, never "All contacts."
6. Activation runs with API failures are treated as failed and must not be used for sends.

## 16. Testing Requirements

### 16.1 Unit
1. Suppression resolver (latest state logic).
2. Email-level dedupe tie-breaker.
3. Segment assignment logic for A/B/all.
4. CSV schema and deterministic ordering.

### 16.2 Integration
1. End-to-end run with seeded canonical + mailchimp status data.
2. Suppressed contacts excluded from export.
3. Duplicate-email participants collapse to one row deterministically.
4. Dry-run rollback semantics.
5. API mode happy-path and partial-failure handling (mocked HTTP).

## 17. Acceptance Criteria
1. CSV export generated from DB with deterministic, suppression-safe rows.
2. No unsubscribed/cleaned contact appears in deliverable audience.
3. Run report and activation snapshot tables fully populated.
4. Rerunning with same inputs yields stable output and no duplication side effects.
5. Optional API mode can sync the same audience without violating suppression rules.

## 18. Rollout Plan (Shortest Path)
1. Implement DB audience builder + suppression + CSV export first.
2. Validate with dry-run and stakeholder review of CSV.
3. Run production CSV export and import into Mailchimp manually.
4. Add API mode only after CSV flow is proven effective.

## 19. Operational Safety References
1. Pre-send runbook checklist: `docs/runbooks/09-mailchimp-pre-send-checklist.md`
2. Production safety requirements: `docs/requirements/mailchimp-production-send-safety-requirements.md`

## 20. Claude Implementation Checklist
- [ ] Add activation tables migration.
- [ ] Add `mailchimp_event_activation` CLI mode and options.
- [ ] Implement segment SQL and suppression logic.
- [ ] Implement deterministic email dedupe.
- [ ] Implement CSV output + run report counters.
- [ ] Add unit/integration tests.
- [ ] (Optional) Add API sync mode with retry + audit.
- [ ] Provide runbook commands for dry-run and production run.
