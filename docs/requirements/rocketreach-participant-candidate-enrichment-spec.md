# Requirements: RocketReach Participant Candidate Enrichment (Pipeline-Compatible)

## 1. Objective
Enrich `participant` candidates with RocketReach API data to improve candidate completeness and scoring quality.

This enrichment must extend the existing workflow:
1. `source -> candidate -> canonical`
2. It must not bypass candidate scoring/promotion.
3. It must not write directly to canonical participant records.

## 2. Non-Negotiable Workflow Constraint
RocketReach is an upstream evidence source only.

Required flow:
1. Ingest RocketReach response as source evidence tied to candidate(s).
2. Apply controlled candidate-field enrichment (with provenance).
3. Re-run `resolution_score` on participant candidates.
4. Promote through existing `resolution_promote` flow.

Prohibited behavior:
1. Direct writes to `canonical_participant`.
2. Direct writes to `canonical_participant_contact` or `canonical_participant_address`.
3. One-step shortcut that marks candidate promoted without scoring rules.

## 3. Scope
In scope:
1. New participant enrichment mode using RocketReach API.
2. Selection policy for which participant candidates are looked up.
3. Raw capture + auditability of provider responses.
4. Controlled update of candidate participant fields.
5. Provenance linkage into existing candidate/source model.
6. Run reporting, counters, tests, and dry-run support.

Out of scope:
1. Canonical survivorship redesign.
2. Replacing existing source ingestion modes.
3. Non-participant entities (`yacht`, `club`, `event`, `registration`) in v1.

## 4. Mode and CLI Contract
Add mode:
1. `--mode participant_enrichment_rocketreach`

Required options:
1. `--db-dsn`
2. `--rocketreach-api-key-env` (default `ROCKETREACH_API_KEY`)
3. `--max-candidates` (default `500`)
4. `--candidate-states` (default `review,hold,reject`)
5. `--require-missing` (enum: `email`, `phone`, `email_or_phone`, `none`; default `email_or_phone`)
6. `--cooldown-days` (default `30`)
7. `--dry-run`

Optional options:
1. `--source-system-label` (default `rocketreach_api`)
2. `--rejects-path` (default `artifacts/rejects/rocketreach_participant_enrichment_rejects.csv`)
3. `--timeout-seconds` (default `20`)
4. `--max-retries` (default `3`)
5. `--qps-limit` (default `2`)
6. `--rocketreach-environment` (enum: `production`, `sandbox`; default `production`)

## 5. Candidate Selection Policy
Candidates eligible for lookup must satisfy all:
1. `candidate_entity_type = participant`.
2. `is_promoted = false`.
3. `resolution_state` in configured `--candidate-states`.
4. Missing at least the required fields based on `--require-missing`.
5. Not enriched from RocketReach within cooldown window.

Ordering (deterministic):
1. Highest `quality_score` first.
2. Most recently updated candidate.
3. Lowest UUID lexical tie-breaker.

## 6. Data Capture and Schema Additions
Add migration for RocketReach enrichment tracking.

Required tables:
1. `rocketreach_enrichment_run`
2. `rocketreach_enrichment_row`

### 6.1 `rocketreach_enrichment_run`
Minimum columns:
1. `id` uuid PK
2. `started_at`, `finished_at`
3. `status` (`running`|`ok`|`failed`)
4. `dry_run` bool
5. `requested_max_candidates` int
6. `counters` jsonb
7. `warnings` jsonb

### 6.2 `rocketreach_enrichment_row`
Minimum columns:
1. `id` uuid PK
2. `run_id` FK
3. `candidate_participant_id` uuid not null
4. `request_payload` jsonb
5. `response_payload` jsonb
6. `provider_person_id` text null
7. `match_confidence` numeric(5,4) null
8. `status` (`matched`|`no_match`|`ambiguous`|`api_error`|`rate_limited`|`skipped`)
9. `error_code` text null
10. `applied_field_mask` text[] null
11. `created_at` timestamptz default now()

## 7. Field Mapping and Update Policy
Map RocketReach fields into candidate participant fields only.

Target field classes:
1. Email(s)
2. Phone(s)
3. Job/company metadata (if candidate schema supports it)
4. Social/LinkedIn URL (if schema supports it)

Update rules:
1. Default behavior: fill-null only.
2. Do not overwrite non-null candidate values unless explicit policy allows and provenance confidence is higher.
3. Do not clear existing values based on null RocketReach responses.
4. Every applied value must record source provenance (`source_system = rocketreach_api` + provider reference id when available).

## 8. Identity Safety Rules (Participant)
RocketReach enrichment must be fail-closed on identity ambiguity.

Required checks before applying enrichment:
1. Candidate identity anchor must exist (normalized full name and at least one existing contact hint, or equivalent policy-defined anchor).
2. If API returns multiple plausible persons and no deterministic winner, mark `ambiguous`; do not apply fields.
3. If returned person data conflicts with strong existing candidate identity signals, skip apply and queue reject reason.

Reason codes (minimum):
1. `rocketreach_no_match`
2. `rocketreach_ambiguous_match`
3. `rocketreach_identity_conflict`
4. `rocketreach_rate_limited`
5. `rocketreach_api_error`

## 8.1 RocketReach API Interaction Requirements
Based on RocketReach API/SDK behavior:
1. Lookups may be asynchronous. Implementation must support:
   - submit lookup request,
   - poll status endpoint,
   - stop only when terminal (`complete` or terminal error), or timeout/retry ceiling reached.
2. Non-blocking lookup flow must be first-class (do not assume immediate result).
3. On HTTP `429`, apply bounded backoff and retry; do not tight-loop status checks.
4. API calls that incur lookup charges must be explicitly counted and reported.
5. Any timed-out or non-terminal lookup at run end must be marked `skipped` or `api_error` with reason and included in rejects/reporting.

## 9. Candidate Source Link Integration
For each successfully applied enrichment:
1. Insert source evidence link to existing candidate-source linkage model.
2. Use `source_system = rocketreach_api` (or configured label).
3. Persist `source_primary_id = provider_person_id` when present.
4. Ensure idempotency: rerun must not duplicate source links for same candidate + provider id.

This enables downstream trust-aware scoring via existing source-trust mechanisms.

## 10. Scoring/Promotion Integration
After enrichment, operators continue with normal pipeline:
1. `resolution_score --entity-type participant`
2. `resolution_promote --entity-type participant --dry-run`
3. `resolution_promote --entity-type participant`

No separate promotion path is allowed for RocketReach-enriched participants.

## 11. Rate Limits, Reliability, and Safety
1. Enforce QPS limit and retry with exponential backoff on transient failures.
2. Respect API quota; stop run safely on sustained quota/rate failures.
3. Use per-row savepoints so one API/DB failure does not abort the entire run.
4. Dry-run must execute selection + API call simulation path (or real-read path if configured) but must roll back all DB writes.
5. Surface non-zero exit on DB-phase errors.
6. Surface warning/failure when API-side failures exceed configured threshold (`--max-api-failure-rate`, default 0.10).

## 12. Idempotency
1. Same input and same API response should not create duplicate logical enrichment rows.
2. Re-running within cooldown should skip recently attempted candidates.
3. Re-running after cooldown may refresh evidence but must keep full audit trail.

## 13. Reporting and Counters
Add counters:
1. `rocketreach_candidates_considered`
2. `rocketreach_candidates_called`
3. `rocketreach_matches_applied`
4. `rocketreach_no_match`
5. `rocketreach_ambiguous`
6. `rocketreach_api_errors`
7. `rocketreach_rate_limited`
8. `rocketreach_fields_enriched`
9. `rocketreach_source_links_inserted`
10. `db_phase_errors`
11. `rocketreach_lookup_credits_used`
12. `rocketreach_status_polls`
13. `rocketreach_non_terminal_timeouts`

Run report must include these counters and top warning/error reasons.

## 14. Testing Requirements
### 14.1 Unit
1. Candidate selection predicate behavior (`--require-missing`, state filter, cooldown).
2. Mapping/normalization from RocketReach payload to candidate fields.
3. Identity ambiguity/conflict gate logic.
4. Idempotent source-link behavior.
5. Counter increments by status class.
6. 429 retry/backoff behavior.
7. Async lookup completion and timeout branches.

### 14.2 Integration
1. End-to-end non-dry run writes run/row audit tables and candidate field enrichments only.
2. Dry-run writes no persistent changes.
3. Ambiguous and conflict responses produce no candidate field update.
4. Post-enrichment `resolution_score` reflects additional source evidence and/or feature completeness.
5. No direct canonical table writes in enrichment mode.
6. API polling path works for both inline-complete and delayed-complete lookup responses.

## 15. Acceptance Criteria
1. Enrichment mode runs successfully with deterministic candidate selection.
2. All RocketReach responses are auditable in enrichment row tables.
3. Candidate fields are enriched safely with provenance and idempotency.
4. Canonical participant data changes only through existing score/promote pipeline.
5. Tests pass for enrichment logic and pipeline boundary constraints.

## 16. Rollout Plan
1. Implement schema + mode + audit logging.
2. Validate with `--dry-run` on a limited sample (`--max-candidates 50`).
3. Run non-dry enrichment on controlled batch.
4. Re-run participant score + promote flow.
5. Review promotion delta and false-positive risk before increasing batch size.

## 17. Example Commands
```bash
# 1) Dry run enrichment
.venv/bin/regatta-import \
  --mode participant_enrichment_rocketreach \
  --db-dsn "$DB_DSN" \
  --max-candidates 100 \
  --candidate-states review,hold,reject \
  --require-missing email_or_phone \
  --dry-run

# 2) Non-dry enrichment
.venv/bin/regatta-import \
  --mode participant_enrichment_rocketreach \
  --db-dsn "$DB_DSN" \
  --max-candidates 100 \
  --candidate-states review,hold,reject \
  --require-missing email_or_phone

# 3) Existing workflow continues unchanged
.venv/bin/regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type participant
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type participant --dry-run
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type participant
```

## 18. External API References
1. RocketReach API docs: `https://rocketreach.co/api`
2. RocketReach Python SDK/readme: `https://github.com/rocketreach/rocketreach_python`
