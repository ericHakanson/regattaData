# Technical Spec: Candidate + Canonical Entity Resolution

## 1. Objective
Build entity-resolution pipelines and schema to consolidate all existing data into:
1. `candidate_*` tables (all records represented, scored, traceable)
2. `canonical_*` tables (trusted promoted subset)

Entity scope for v1:
1. Participants
2. Yachts
3. Events
4. Registrations
5. Clubs

Non-negotiable behavior:
1. Every source record remains traceable as `[source] -> [candidate] -> [canonical?]`.
2. All records exist in candidate tables.
3. Only high-confidence candidates are promoted to canonical.
4. Low-confidence candidates stay in candidate and can be re-scored after enrichment.

## 2. Locked Decisions
1. Source tables remain in place for now; eventual purge is future-state only.
2. Create new empty `canonical_*` tables (do not repurpose current operational tables).
3. Candidate model is entity-level with many-to-one source linkage.
4. Scoring thresholds (default):
   - `score >= 0.95` -> `auto_promote`
   - `0.75 <= score < 0.95` -> `review`
   - `0.50 <= score < 0.75` -> `hold`
   - `< 0.50` -> `reject`
5. Rules are source-aware and loaded from YAML.
6. Canonical IDs are UUIDs (surrogate keys), not deterministic hashes.
7. Participant model includes owner/crew/parent roles and all contact types.
8. Age of majority is fixed at 18.
9. Manual actions must be logged with numeric score context.
10. Prioritization is data-quality-based (not calendar-based).

## 3. Naming Convention
Use strict prefixes:
1. `source_*` (existing tables; immutable provenance layer)
2. `candidate_*` (resolution working layer)
3. `canonical_*` (trusted operational layer)

Cross-cutting tables:
1. `candidate_source_link`
2. `candidate_canonical_link`
3. `resolution_score_run`
4. `resolution_manual_action_log`
5. `resolution_rule_set`

## 4. Source Coverage Requirement
All rows from all existing tables must be consumable by the source-to-candidate pipeline. Current table set includes:
1. Raw/source-specific (`airtable_copy_row`, `jotform_waiver_submission`, `mailchimp_audience_row`, `mailchimp_contact_state`, `mailchimp_contact_tag`, `raw_asset`)
2. Existing operational entities/links (`participant`, `participant_contact_point`, `participant_address`, `participant_related_contact`, `yacht`, `yacht_rating`, `yacht_ownership`, `yacht_club`, `club_membership`, `event_series`, `event_instance`, `event_entry`, `event_entry_participant`, `document_*`)
3. Existing resolution/ops (`identity_candidate_match`, `identity_merge_action`, `next_best_action`)
4. Existing xrefs (`airtable_xref_*`)

“Consumable” means each row is either:
1. linked as evidence to a candidate entity, or
2. logged as intentionally skipped with reason code in pipeline report.

## 5. Schema Design
Implement via new migration: `0011_candidate_canonical_core.sql` (or split if needed).

### 5.1 Candidate Core Tables
#### `candidate_participant`
1. `id` uuid PK.
2. `stable_fingerprint` text not null (deterministic, non-PK).
3. `display_name` text.
4. `normalized_name` text.
5. `date_of_birth` date null.
6. `best_email` text null.
7. `best_phone` text null.
8. `quality_score` numeric(5,4) not null default 0.
9. `resolution_state` text check (`auto_promote`,`review`,`hold`,`reject`) not null.
10. `confidence_reasons` jsonb not null default `[]`.
11. `is_promoted` boolean not null default false.
12. `promoted_canonical_id` uuid null.
13. `created_at`, `updated_at`.

#### `candidate_yacht`
1. `id` uuid PK.
2. `stable_fingerprint` text not null.
3. `name`, `normalized_name`.
4. `sail_number`, `normalized_sail_number`.
5. `length_feet`, `yacht_type`.
6. `quality_score`, `resolution_state`, `confidence_reasons`.
7. `is_promoted`, `promoted_canonical_id`.
8. timestamps.

#### `candidate_event`
1. `id` uuid PK.
2. `stable_fingerprint` text not null.
3. `event_name`, `normalized_event_name`.
4. `season_year` int null.
5. `event_external_id` text null (race_id/emenu id/etc).
6. `start_date`, `end_date` null.
7. `location_raw` text null.
8. `quality_score`, `resolution_state`, `confidence_reasons`.
9. `is_promoted`, `promoted_canonical_id`.
10. timestamps.

#### `candidate_registration`
1. `id` uuid PK.
2. `stable_fingerprint` text not null.
3. `registration_external_id` text null.
4. `candidate_event_id` uuid not null FK -> `candidate_event.id`.
5. `candidate_yacht_id` uuid null FK -> `candidate_yacht.id`.
6. `candidate_primary_participant_id` uuid null FK -> `candidate_participant.id`.
7. `entry_status` text null.
8. `registered_at` timestamptz null.
9. `quality_score`, `resolution_state`, `confidence_reasons`.
10. `is_promoted`, `promoted_canonical_id`.
11. timestamps.

#### `candidate_club`
1. `id` uuid PK.
2. `stable_fingerprint` text not null.
3. `name`, `normalized_name`.
4. `website`, `phone`, `address_raw`, `state_usa`.
5. `quality_score`, `resolution_state`, `confidence_reasons`.
6. `is_promoted`, `promoted_canonical_id`.
7. timestamps.

### 5.2 Candidate Child Tables
#### `candidate_participant_contact`
1. `id` uuid PK.
2. `candidate_participant_id` uuid FK.
3. `contact_type` (`email`,`phone`,`social`,`other`).
4. `contact_subtype` text.
5. `raw_value`, `normalized_value`.
6. `is_primary` boolean.
7. provenance + timestamps.

#### `candidate_participant_address`
1. `id` uuid PK.
2. `candidate_participant_id` uuid FK.
3. `address_raw` text not null.
4. parsed columns nullable (`line1`,`city`,`state`,`postal_code`,`country_code`).
5. `is_primary`.
6. provenance + timestamps.

#### `candidate_participant_role_assignment`
1. `id` uuid PK.
2. `candidate_participant_id` uuid FK.
3. `role` text check (`owner`,`co_owner`,`crew`,`skipper`,`parent`,`guardian`,`registrant`,`emergency_contact`,`other`).
4. `candidate_event_id` uuid null FK.
5. `candidate_registration_id` uuid null FK.
6. `source_context` text.
7. timestamps.

### 5.3 Canonical Tables
Create separate canonical equivalents:
1. `canonical_participant`
2. `canonical_participant_contact`
3. `canonical_participant_address`
4. `canonical_participant_role_assignment`
5. `canonical_yacht`
6. `canonical_club`
7. `canonical_event`
8. `canonical_registration`

Each canonical table includes:
1. UUID PK.
2. survivorship fields (selected best values).
3. `canonical_confidence_score` numeric.
4. `last_resolution_run_id` uuid.
5. standard timestamps.

### 5.4 Traceability Tables
#### `candidate_source_link`
1. `id` uuid PK.
2. `candidate_entity_type` text check (`participant`,`yacht`,`event`,`registration`,`club`).
3. `candidate_entity_id` uuid not null.
4. `source_table_name` text not null.
5. `source_row_pk` text not null.
6. `source_row_hash` text null.
7. `source_system` text null.
8. `link_score` numeric(5,4) not null.
9. `link_reason` jsonb not null.
10. unique (`candidate_entity_type`,`candidate_entity_id`,`source_table_name`,`source_row_pk`).

#### `candidate_canonical_link`
1. `id` uuid PK.
2. `candidate_entity_type` text.
3. `candidate_entity_id` uuid.
4. `canonical_entity_id` uuid.
5. `promotion_score` numeric(5,4).
6. `promotion_mode` text check (`auto`,`manual`).
7. `promoted_at`, `promoted_by`.
8. unique (`candidate_entity_type`,`candidate_entity_id`).

### 5.5 Resolution Governance Tables
#### `resolution_rule_set`
1. `id` uuid PK.
2. `entity_type` text.
3. `source_system` text.
4. `version` text not null.
5. `yaml_content` text not null.
6. `yaml_hash` text not null.
7. `is_active` boolean.
8. `created_at`, `activated_at`.

#### `resolution_score_run`
1. `id` uuid PK.
2. `entity_type` text.
3. `source_scope` text.
4. `rule_set_id` uuid FK.
5. `started_at`, `finished_at`.
6. counters jsonb.
7. `status` text (`ok`,`failed`).

#### `resolution_manual_action_log`
1. `id` uuid PK.
2. `entity_type` text.
3. `candidate_entity_id` uuid not null.
4. `canonical_entity_id` uuid null.
5. `action_type` text (`promote`,`merge`,`split`,`demote`,`edit`,`unlink`).
6. `before_payload` jsonb.
7. `after_payload` jsonb.
8. `score_before` numeric(5,4) null.
9. `score_after` numeric(5,4) null.
10. `reason_code` text.
11. `actor` text not null.
12. `source` text (`db_manual`,`sheet_import`,`pipeline`).
13. `created_at` timestamptz.

## 6. Computed Participant Age + Minor Flag
Store `date_of_birth` in candidate and canonical participant tables.
Do not persist static age as table column.
Provide a view for runtime computation:
1. `canonical_participant_enriched`
2. includes:
   - `age_years = EXTRACT(YEAR FROM age(current_date, date_of_birth))::int`
   - `is_minor = (age_years < 18)`

Same optional view for candidate participants.

## 7. Scoring Rules via YAML
Rules must be externalized and versioned.

File location:
1. `config/resolution_rules/*.yml`

Required YAML structure:
1. `entity_type`
2. `source_system`
3. `version`
4. `thresholds` (`auto_promote`,`review`,`hold`)
5. `feature_weights`
6. `hard_blocks`
7. `source_precedence`
8. `survivorship_rules`
9. `missing_attribute_penalties`

Example (participant excerpt):
```yaml
entity_type: participant
source_system: regattaman|mailchimp|jotform|airtable|yacht_scoring
version: "v1.0.0"
thresholds:
  auto_promote: 0.95
  review: 0.75
  hold: 0.50
feature_weights:
  email_exact: 0.55
  phone_exact: 0.20
  dob_exact: 0.15
  normalized_name_exact: 0.10
hard_blocks:
  - conflicting_dob
  - conflicting_high_confidence_email
source_precedence:
  - jotform_waiver_csv
  - regattaman_csv_export
  - regattaman_public_scrape_csv
  - mailchimp_audience_csv
  - airtable_copy_csv
  - yacht_scoring_csv
survivorship_rules:
  date_of_birth: highest_precedence_non_null
  email_primary: highest_score_confirmed
missing_attribute_penalties:
  missing_email: 0.10
  missing_phone: 0.05
```

Every scored candidate row must persist:
1. numeric score
2. reason payload
3. rule version/hash
4. scoring run ID

## 8. Pipeline Architecture
Implement three pipelines (shared module framework):

### 8.1 Source -> Candidate
1. Read all source tables.
2. Build or update entity-level candidates.
3. Attach every source row via `candidate_source_link`.
4. Upsert candidate child records (contacts, addresses, roles).
5. Never drop candidate rows.

### 8.2 Candidate Scoring
1. Load active YAML rule set per entity/source.
2. Compute score + reasons.
3. Set `resolution_state`.
4. Write `identity_candidate_match` entries where pairwise matching is used.
5. Write `next_best_action` records focused on missing high-value attributes.

### 8.3 Candidate Promotion -> Canonical
1. Promote candidates where `resolution_state='auto_promote'`.
2. Always create `candidate_canonical_link`.
3. Log promotion in `identity_merge_action` and `resolution_manual_action_log` (`source='pipeline'`).
4. Candidate rows remain; canonical is promoted subset.

## 9. Manual Review + Sheet Roundtrip
Support export/import workflow:
1. Export review candidates (`score`, reasons, missing attrs, source links).
2. Allow manual decisions in sheet or SQL.
3. Ingest decisions via dedicated command (e.g., `--mode resolution_manual_apply`).
4. All manual changes must write `resolution_manual_action_log`.
5. Re-score impacted candidates after manual apply.

## 10. Data Quality Prioritization (No Calendar SLA)
Do not use time-based review milestones.
Prioritize by quality gap:
1. Highest score below promote threshold first.
2. Highest expected score lift from missing attributes.
3. Highest downstream impact (records linked to many registrations/events).

Use `next_best_action` for this queueing behavior.

## 11. Merge Guardrails (Must Not Auto-Merge)
1. Participant name-only matches with >1 candidate.
2. Yacht name-only matches with conflicting sail numbers.
3. Event name-only matches across multiple years/hosts.
4. Club matches with conflicting normalized names and no corroborating attributes.
5. Any hard block from YAML.

These stay in candidate with `review`/`hold`/`reject`.

## 12. Idempotency
1. Source->candidate upserts must be idempotent.
2. Candidate->canonical promotion must be idempotent.
3. Re-running same inputs/rules cannot create duplicate candidate or canonical records.
4. Link tables enforce unique constraints for deterministic lineage.

## 13. CLI / Modules
Add resolution CLI modes:
1. `--mode resolution_source_to_candidate`
2. `--mode resolution_score`
3. `--mode resolution_promote`
4. `--mode resolution_manual_apply` (sheet/manual decisions import)

Required flags:
1. `--entity-type` (`participant|yacht|event|registration|club|all`)
2. `--rule-file` (YAML path) for scoring runs
3. `--dry-run`

## 14. Testing Requirements
### Unit
1. YAML parser + validation.
2. Scoring math and threshold routing.
3. Hard-block behavior.
4. Survivorship/source precedence logic.
5. age/is_minor view logic.

### Integration
1. Source->candidate lineage for every source table.
2. Candidate->canonical auto-promote path.
3. Review/hold/reject non-promotion path.
4. Manual override apply + audit logging.
5. Idempotency across reruns.
6. Mixed-source enrichment increases score and can trigger later promotion.

## 15. Acceptance Criteria
1. Every source row is linked to at least one candidate entity or recorded as intentionally skipped with reason.
2. Every candidate has score + state + rule version/hash.
3. Every canonical row has back-link(s) to candidate + source lineage.
4. Manual actions are fully auditable with numeric before/after scores.
5. Participant canonical view exposes `age_years` and `is_minor` based on DOB and age-of-majority=18.
6. No unsafe auto-merges occur under guardrails.

## 16. Source Table Purge Readiness (Future Only)
Define purge as out-of-scope for implementation but include readiness report command.
Readiness report must show:
1. 100% lineage coverage from source rows to candidates.
2. canonical coverage percentages by entity type.
3. unresolved candidate counts and reasons.
4. backup snapshot ID and retention confirmation.

No source purge logic should run automatically.

## 17. Deliverables for Claude
1. New migrations for candidate/canonical/resolution governance tables.
2. New resolution pipeline modules and CLI mode wiring.
3. YAML rules engine + validation.
4. Candidate/canonical lineage + audit logging.
5. Unit + integration tests meeting this spec.
6. Runbook markdown for operations (`dry-run`, score, promote, manual apply, reconciliation).
