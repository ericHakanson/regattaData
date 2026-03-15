# Requirements: Participant Hold-Pool Address Normalization and RocketReach Readiness

## 1. Objective
Prepare the `participant` hold pool for budget-aware RocketReach enrichment by:
1. normalizing noisy `candidate_participant_address` location strings,
2. deriving one best geographic hint per hold candidate,
3. classifying each hold candidate for enrichment readiness,
4. feeding only readiness-qualified candidates into the existing `source -> candidate -> score -> promote` workflow.

This is a preparatory layer for external enrichment. It is not a replacement for candidate scoring or canonical promotion.

## 2. Background (Observed Production State)
Current production observations:
1. `candidate_participant` state counts:
   - `auto_promote = 1412`
   - `hold = 3897`
   - `reject = 4572`
2. Hold-pool child evidence:
   - `with_child_email = 0`
   - `with_child_phone = 0`
   - `with_child_address = 3897`
3. Hold-pool source overlap:
   - `yacht_scoring_raw_row = 3897`
   - `participant = 3878`
4. Hold-pool name overlap with `auto_promote` is only `2`.
5. Address quality sample shows noisy but often useful geography hints, for example:
   - `Larchmont NY USA`
   - `Southport Connecticut United States`
   - `Guilford Ct USA`
   - `USA`
   - `Aspen Yacht club`
   - `Laser`
6. Hold-pool address-shape breakdown:
   - likely `city state country` = `3381`
   - country-only = `196`
   - obvious non-address/junk = `67`

Interpretation:
1. The hold pool is not primarily a merge problem.
2. The hold pool is mostly unique, address-only candidates.
3. A large majority of hold candidates already have location strings that are probably usable for external enrichment.
4. Raw address strings are too noisy to use directly and would waste RocketReach quota.

## 3. Non-Negotiable Workflow Constraint
This work must remain inside the current pipeline contract:
1. normalize address/location evidence at the candidate layer,
2. classify candidates for RocketReach eligibility,
3. run RocketReach only on readiness-qualified candidates,
4. write any resulting contact evidence back to candidate/source structures,
5. re-run `resolution_score`,
6. promote only through `resolution_promote`.

Prohibited behavior:
1. direct writes to canonical tables,
2. external enrichment against the entire hold pool without readiness filtering,
3. spending RocketReach credits on country-only or junk-location candidates by default.

## 4. Scope
In scope:
1. participant hold-pool address normalization
2. location-quality classification
3. one-best-location selection per hold candidate
4. RocketReach readiness filtering logic
5. reporting/counters for readiness classes
6. integration with the existing RocketReach participant enrichment mode
7. tests

Out of scope:
1. redesign of participant scoring thresholds
2. canonical survivorship changes
3. broad candidate merge heuristics
4. non-participant entities
5. changing Mailchimp identity policy

## 5. Functional Requirements

### R1. Normalize Candidate Address Hints
For each `candidate_participant_address` row attached to a hold candidate, implementation must derive a normalized location hint.

Minimum normalization rules:
1. trim leading/trailing whitespace,
2. collapse repeated internal whitespace,
3. normalize case consistently,
4. normalize common country tokens:
   - `USA`, `Usa`, `United States` -> `USA`
   - `CAN`, `Canada` -> `CAN`
   - `IND`, `India` -> `IND`
5. normalize common state tokens where straightforward:
   - `Connecticut` -> `CT`
   - similar high-frequency US state expansions may be added conservatively,
6. preserve original raw value for auditability.

This does not need to become a full postal parser in v1.

### R2. Deduplicate Equivalent Address Variants Per Candidate
Equivalent variants for the same candidate must be deduplicated.

Examples that should collapse to one logical location hint:
1. `Larchmont NY USA` and `Larchmont NY Usa`
2. `Southport CT USA` and `Southport Connecticut United States`
3. `Guilford Ct USA` and `Guilford CT USA`

Deduplication must occur per candidate, not globally across all candidates.

### R3. Classify Location Hint Quality
Each normalized candidate location hint must be assigned a quality class.

Minimum classes:
1. `city_state_country`
2. `city_country`
3. `state_country`
4. `country_only`
5. `freeform_partial`
6. `non_address_junk`
7. `empty_or_null`

Minimum junk rules in v1:
1. known boat/class-like values such as `Laser`,
2. club-only strings such as values matching `yacht club`,
3. strings that contain no plausible geography tokens.

### R4. Derive One Best Geographic Hint Per Hold Candidate
Each hold candidate must receive one best enrichment hint derived from its normalized address variants.

Selection policy:
1. prefer `city_state_country`,
2. then `city_country`,
3. then `state_country`,
4. then `freeform_partial`,
5. never choose `country_only` or `non_address_junk` as enrichment-ready hints.

If multiple high-quality hints remain and materially conflict, candidate must not be marked ready automatically.

### R5. Classify Hold Candidates for RocketReach Readiness
Every hold candidate must be assigned one readiness status.

Minimum statuses:
1. `enrichment_ready`
2. `too_vague`
3. `junk_location`
4. `multi_location_conflict`
5. `already_has_contact`
6. `not_hold`

Eligibility rules for `enrichment_ready`:
1. `resolution_state = 'hold'`
2. `is_promoted = false`
3. normalized name present
4. no effective email evidence
5. no effective phone evidence
6. exactly one best geographic hint of acceptable quality

Country-only candidates must default to `too_vague`.

### R6. Integrate with RocketReach Candidate Selection
The existing RocketReach participant enrichment mode must support an option that limits selection to readiness-qualified hold candidates.

Recommended CLI additions:
1. `--selection-profile hold_geo_ready`
2. or equivalent flags such as:
   - `--candidate-states hold`
   - `--require-geo-ready`

The implementation may choose the final CLI shape, but the behavior must be explicit and deterministic.

### R7. Preserve Auditability
All normalization and readiness decisions must remain auditable.

Minimum requirement:
1. preserve raw address rows unchanged,
2. record normalized/best-hint/readiness outputs in a durable table or materialized pipeline artifact,
3. make it possible to trace from a RocketReach-selected candidate back to:
   - raw address strings,
   - normalized hint,
   - readiness reason.

## 6. Data Model Requirements
Implementation may choose one of these patterns:
1. new table(s), or
2. a materialized/intermediate table maintained by pipeline mode, or
3. candidate-level derived columns if justified.

Preferred approach:
1. `candidate_participant_geo_hint`
2. `candidate_participant_enrichment_readiness`

### 6.1 `candidate_participant_geo_hint`
Minimum columns:
1. `id` uuid PK
2. `candidate_participant_id` uuid not null
3. `source_address_row_id` uuid/text nullable
4. `address_raw` text not null
5. `normalized_hint` text nullable
6. `city` text nullable
7. `state_region` text nullable
8. `country_code` text nullable
9. `quality_class` text not null
10. `is_best_hint` bool not null default false
11. `created_at` timestamptz default now()

### 6.2 `candidate_participant_enrichment_readiness`
Minimum columns:
1. `candidate_participant_id` uuid PK
2. `readiness_status` text not null
3. `reason_code` text not null
4. `best_hint_text` text nullable
5. `best_city` text nullable
6. `best_state_region` text nullable
7. `best_country_code` text nullable
8. `evaluated_at` timestamptz not null default now()

If Claude proposes a lighter-weight schema, it must still satisfy auditability and idempotency.

## 7. Mode and CLI Requirements
Add a pipeline mode for readiness derivation.

Recommended mode:
1. `--mode participant_hold_geo_prepare`

Required behavior:
1. read current hold candidates and their address child rows,
2. normalize and classify address hints,
3. compute one best hint per candidate,
4. compute readiness rows,
5. write report counters,
6. support `--dry-run`.

Optional flags:
1. `--max-candidates`
2. `--states hold`
3. `--include-country-only` (default false)
4. `--allow-freeform-partial` (default false)

## 8. RocketReach Integration Requirements
This spec complements, not replaces, `docs/requirements/rocketreach-participant-candidate-enrichment-spec.md`.

Required integration behavior:
1. RocketReach selection must be able to source candidates from readiness outputs.
2. Default production recommendation must prefer `enrichment_ready` candidates only.
3. The best geographic hint must be used as the geographic narrowing input for RocketReach lookup when supported by the provider query shape.
4. Country-only, junk, and conflicting-location candidates must be skipped by default.

## 9. Counters and Reporting
Add counters for the new prep mode.

Minimum counters:
1. `hold_candidates_considered`
2. `hold_candidates_with_address`
3. `geo_hints_normalized`
4. `geo_hints_deduplicated`
5. `geo_candidates_ready`
6. `geo_candidates_too_vague`
7. `geo_candidates_junk_location`
8. `geo_candidates_multi_location_conflict`
9. `geo_candidates_already_has_contact`
10. `db_errors`

If integrated into RocketReach selection reporting, expose at least:
1. candidates eligible after readiness filter,
2. candidates skipped for vague/junk/conflict reasons.

## 10. Implementation Requirements

### I1. Build Candidate-Level Address Aggregation
Aggregate all `candidate_participant_address` rows for hold candidates.

Required joins:
1. `candidate_participant`
2. `candidate_participant_address`
3. optionally `candidate_participant_contact` for readiness exclusion

### I2. Normalize Without Mutating Raw Evidence
Do not overwrite `candidate_participant_address.address_raw`.
All normalization must happen in derived output.

### I3. Filter Junk Conservatively
Initial junk filter may be heuristic but must be deterministic and documented.

Minimum explicit junk patterns:
1. class/boat-only strings like `Laser`
2. club-only strings matching `yacht club`
3. exact country-only tokens unless explicitly allowed

### I4. Handle Conflicts Conservatively
If a candidate has multiple materially different city/state hints and no deterministic winner, mark `multi_location_conflict`.
Do not send such candidates to RocketReach by default.

### I5. Exclude Candidates Already Carrying Contact Evidence
If a hold candidate already has effective child email or phone evidence in future reruns, classify as `already_has_contact` rather than `enrichment_ready`.

## 11. Test Requirements

### 11.1 Unit
1. normalize country/state token variants
2. collapse equivalent location strings
3. classify country-only strings as `too_vague`
4. classify junk values such as `Laser` or `Aspen Yacht club` as `non_address_junk`
5. choose best hint from multiple variants
6. detect conflicting hints

### 11.2 Integration
1. hold candidate with address variants like `Southport CT USA` and `Southport Connecticut United States` produces one best normalized hint
2. hold candidate with only `USA` becomes `too_vague`
3. hold candidate with `Laser` becomes `junk_location`
4. hold candidate with multiple conflicting cities becomes `multi_location_conflict`
5. hold candidate with one clean `city state country` hint becomes `enrichment_ready`
6. rerunning the prep mode is idempotent
7. RocketReach selection filtered by readiness chooses only `enrichment_ready` hold candidates
8. raw address evidence remains unchanged after prep mode

## 12. Acceptance Criteria
1. The prep mode can classify the full hold pool without DB errors.
2. Equivalent location variants for a candidate collapse to one best hint.
3. Country-only and junk-location candidates are excluded from readiness by default.
4. RocketReach candidate selection can consume readiness-qualified hold candidates only.
5. The implementation preserves raw candidate address evidence and provides auditable normalized outputs.

## 13. Rollout Plan
1. implement normalization + readiness derivation mode
2. run `--dry-run` on the current hold pool
3. inspect readiness counts and top excluded reasons
4. run a small RocketReach pilot only on `enrichment_ready` candidates
5. re-run `resolution_score` and `resolution_promote --dry-run`

## 14. Recommended Validation Queries

### 14.1 Hold-Pool Readiness Summary
```sql
SELECT readiness_status, COUNT(*) AS n
FROM candidate_participant_enrichment_readiness
GROUP BY 1
ORDER BY 2 DESC, 1;
```

### 14.2 Best-Hint Coverage
```sql
SELECT quality_class, COUNT(*) AS n
FROM candidate_participant_geo_hint
WHERE is_best_hint = true
GROUP BY 1
ORDER BY 2 DESC, 1;
```

### 14.3 Ready Candidates Sample
```sql
SELECT
  cp.id,
  cp.normalized_name,
  cer.best_hint_text,
  cer.best_city,
  cer.best_state_region,
  cer.best_country_code
FROM candidate_participant_enrichment_readiness cer
JOIN candidate_participant cp
  ON cp.id = cer.candidate_participant_id
WHERE cer.readiness_status = 'enrichment_ready'
ORDER BY cp.normalized_name
LIMIT 100;
```

## 15. Explicit Non-Goals
Do not:
1. auto-merge hold candidates into auto-promote candidates by name alone,
2. broaden participant scoring again in this change,
3. send country-only/junk candidates to RocketReach by default,
4. bypass the existing candidate scoring/promotion pipeline.
