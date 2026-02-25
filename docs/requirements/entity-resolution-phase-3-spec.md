# Technical Spec: Entity Resolution Phase 3 (Hardening + Lifecycle + Provenance)

## 1. Objective
Phase 3 hardens the candidate/canonical system after Phase 1-2 by adding:
1. Strict state integrity and transition controls.
2. Canonical lifecycle operations beyond promote (`merge`, `split`, `demote`, `unlink`).
3. Field-level survivorship provenance for every canonical attribute.
4. Human-in-the-loop workflow hardening (`next_best_action` lifecycle + import/export contracts).
5. Purge-readiness reporting with explicit lineage and safety gates.

This phase is about correctness, auditability, and operational control, not new source ingestion adapters.

## 2. Baseline And Dependencies
Phase 3 assumes these are already in place and migrated:
1. Candidate/canonical core schema (`0011`, `0012`).
2. Scoring + promote + manual apply foundations.
3. Existing source pipelines (Regattaman, Jotform, Mailchimp, Airtable, Yacht Scoring).

If any dependency is missing, Phase 3 implementation must fail fast with clear migration/feature checks.

## 3. Scope
In scope:
1. Schema constraints and triggers for candidate promotion integrity.
2. New lifecycle pipeline capabilities (`merge`, `split`, `demote`, `unlink`).
3. Provenance tables and writes during promote/merge/split/demote/unlink.
4. NBA lifecycle states and ownership metadata.
5. Purge-readiness report mode and snapshot table.
6. Performance/index hardening for resolution paths.

Out of scope:
1. Physical source-table purge/delete execution.
2. New external scraping collectors.
3. UI implementation.

## 4. Required Outcomes
1. Promoted candidates cannot drift into invalid state combinations.
2. Every canonical attribute value is explainable by provenance rows.
3. Lifecycle operations are idempotent, auditable, and reversible where feasible.
4. Review operators can prioritize work from structured NBA queues.
5. Purge readiness is measurable with deterministic safety criteria.

## 5. Schema Requirements
Implement via new migrations after existing sequence (recommended: `0014`, `0015`, `0016`).

### 5.1 Candidate Integrity Constraints
Apply to each `candidate_*` entity table:
1. `CHECK (resolution_state IN ('auto_promote','review','hold','reject'))`.
2. `CHECK ((is_promoted = true AND promoted_canonical_id IS NOT NULL) OR (is_promoted = false AND promoted_canonical_id IS NULL))`.
3. FK on `promoted_canonical_id` to matching `canonical_*` table where not already present.
4. Trigger-based transition guard enforcing allowed transitions:
   - `review -> hold|reject|auto_promote`
   - `hold -> review|reject|auto_promote`
   - `reject -> review|hold`
   - `auto_promote -> auto_promote` (except explicit lifecycle demote operation)

Demote operation must bypass normal guard through controlled function/procedure only.

### 5.2 Canonical Lifecycle Governance Tables
Add/extend:
1. `resolution_lifecycle_action`
   - `id` uuid PK
   - `entity_type`
   - `action_type` (`merge`,`split`,`demote`,`unlink`,`edit`)
   - `actor`
   - `source` (`pipeline`,`sheet_import`,`db_manual`)
   - `reason_code`
   - `before_payload` jsonb
   - `after_payload` jsonb
   - `created_at`
2. `resolution_lifecycle_action_item`
   - per-row child rows mapping affected candidate/canonical IDs
   - includes `status` (`applied`,`skipped`,`error`) and error text.

`resolution_manual_action_log` remains authoritative for promote/reject/hold and must also record lifecycle events or reference lifecycle action IDs.

### 5.3 Canonical Attribute Provenance
Add `canonical_attribute_provenance`:
1. `id` uuid PK.
2. `canonical_entity_type` (`participant`,`yacht`,`event`,`registration`,`club`).
3. `canonical_entity_id` uuid not null.
4. `attribute_name` text not null.
5. `attribute_value_hash` text not null.
6. `selected_from_candidate_id` uuid null.
7. `selected_from_source_table` text null.
8. `selected_from_source_pk` text null.
9. `selection_method` (`auto_survivorship`,`manual_override`,`merge_rule`,`split_rule`,`demote_rebuild`).
10. `rule_set_id` uuid null.
11. `score_context` numeric(5,4) null.
12. `recorded_at` timestamptz not null default now().
13. Unique key: (`canonical_entity_type`,`canonical_entity_id`,`attribute_name`,`attribute_value_hash`).

Requirement: Every canonical field populated by pipeline logic must have at least one provenance row.

### 5.4 Next Best Action Lifecycle Hardening
Extend `next_best_action` as needed:
1. `status` constrained to `open|in_progress|done|dismissed`.
2. `owner` text null.
3. `closed_at` timestamptz null.
4. `closed_reason` text null.
5. Partial unique index to avoid duplicate open enrich actions per candidate/reason:
   - unique on (`target_entity_type`,`target_entity_id`,`action_type`,`reason_code`) where `status in ('open','in_progress')`.

### 5.5 Purge Readiness Snapshot
Add `resolution_purge_readiness_snapshot`:
1. `id` uuid PK.
2. `run_id` uuid.
3. `captured_at` timestamptz.
4. `lineage_coverage_json` jsonb.
5. `canonical_coverage_json` jsonb.
6. `unresolved_counts_json` jsonb.
7. `blocking_issues_json` jsonb.
8. `is_purge_ready` boolean.

## 6. Pipeline Requirements

### 6.1 New Mode: `resolution_lifecycle_apply`
Add CLI mode for lifecycle actions with CSV input:
1. Required columns: `entity_type`, `action_type`, `primary_id`, `secondary_id`, `actor`.
2. Optional: `reason_code`, `payload_json`.
3. Supported actions:
   - `merge`: combine two canonical records, re-point candidate links to survivor.
   - `split`: create a new canonical record and reassign selected candidates.
   - `demote`: canonical -> candidate-only (clear links, set candidate state to review/hold as configured).
   - `unlink`: remove candidate<->canonical link without deleting candidate.
4. Per-row savepoint isolation.
5. Idempotent reruns by action fingerprint.
6. Mandatory audit log writes for each row.

### 6.2 Manual Apply Integration Requirements
`resolution_manual_apply` must remain consistent with lifecycle operations:
1. Cannot reject/hold already promoted candidates.
2. Promote on stale/broken canonical link must self-heal link or recreate canonical row.
3. If `--rescore-after-apply` is enabled, promoted candidates must stay `auto_promote`.

### 6.3 Provenance Writes
During promote/merge/split/edit/demote-rebuild:
1. Recompute survivorship winners.
2. Upsert corresponding `canonical_attribute_provenance` rows.
3. Record rule version/run context when available.

### 6.4 Purge Readiness Mode
Add CLI mode `resolution_purge_readiness_report`:
1. Computes and prints machine-readable report.
2. Persists snapshot to `resolution_purge_readiness_snapshot`.
3. Reports blocking issues, including:
   - source rows not linked to any candidate.
   - promoted candidates missing canonical links.
   - canonical rows missing attribute provenance.
   - unresolved candidates by entity/state bucket.

## 7. Scoring + NBA Requirements
1. Scoring must clear/rewrite scorer-generated open NBAs only.
2. No NBA creation for candidates already promoted.
3. No NBA creation for hard-block rejects.
4. NBA priority score must be deterministic from rule weights.
5. Add counters in reports:
   - `nbas_written`
   - `nbas_closed`
   - `nbas_dismissed`

## 8. Performance Requirements
1. All lifecycle modes must support savepoint-per-row with bounded memory.
2. Add indexes for all hot-path joins and lookups in:
   - `candidate_source_link`
   - `candidate_canonical_link`
   - `canonical_attribute_provenance`
   - `next_best_action`
3. Add regression test ensuring no full table scans on core promote/lookup queries at expected cardinalities.

## 9. Testing Requirements

### 9.1 Unit
1. State transition guard function/triggers.
2. Lifecycle action validation and idempotency fingerprinting.
3. Provenance row construction for each entity type.
4. NBA state transitions.

### 9.2 Integration
1. Promote -> merge -> split -> demote -> unlink lifecycle sequence with audit assertions.
2. Stale-link recovery correctness.
3. Re-score after manual apply does not downgrade promoted state.
4. Purge readiness report catches seeded coverage gaps.
5. Idempotent rerun for lifecycle CSV.

### 9.3 Migration
1. Fresh DB migration chain passes.
2. Upgrade path from existing pre-phase-3 DB passes.
3. Constraint backfill scripts succeed against current data.

## 10. Acceptance Criteria
1. No candidate row can be promoted without a valid canonical link.
2. No canonical row used operationally without field-level provenance coverage.
3. Lifecycle operations are fully auditable and idempotent.
4. NBA queue supports actionable lifecycle (`open` -> `done/dismissed`) without duplicate active work items.
5. Purge readiness snapshot accurately flags blocking lineage/provenance issues.
6. All new tests pass with no regressions in existing pipeline suites.

## 11. Implementation Sequence (Recommended)
1. Migration set A: constraints + NBA lifecycle + provenance table.
2. Pipeline updates: scoring/manual apply/provenance writes.
3. New lifecycle mode + tests.
4. Purge readiness mode + snapshot table + tests.
5. Performance/index pass and explain-plan checks.

## 12. Developer Task Checklist
- [ ] Create phase-3 migrations and update integration migration list.
- [ ] Implement candidate transition guard trigger/function.
- [ ] Implement provenance writer utilities and wire into promote + lifecycle paths.
- [ ] Implement `resolution_lifecycle_apply` CLI mode and CSV contract.
- [ ] Implement `resolution_purge_readiness_report` CLI mode + snapshot writes.
- [ ] Extend `next_best_action` status lifecycle and duplicate-open prevention.
- [ ] Add/adjust counters in run reports.
- [ ] Add unit tests for guards/provenance/lifecycle validations.
- [ ] Add integration tests for lifecycle chains and purge-readiness blocking.
- [ ] Run full suite and publish a short runbook update.

## 13. Handoff Prompt For Claude
Implement this Phase 3 spec in small, reviewable PR-sized commits:
1. migrations first,
2. then scoring/manual apply compatibility changes,
3. then lifecycle mode,
4. then purge-readiness mode,
5. then indexing/perf + runbook.

Do not skip tests between phases. Keep all operations idempotent and auditable. Preserve existing mode behavior unless explicitly changed by this spec.
