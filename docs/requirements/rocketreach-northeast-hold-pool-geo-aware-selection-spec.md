# Requirements: RocketReach Northeast Hold-Pool Selection and Geo-Aware Lookup

## 1. Objective
Refine participant RocketReach enrichment so paid lookups are limited to a high-confidence subset of the hold pool:
1. `resolution_state = 'hold'`
2. `readiness_status = 'enrichment_ready'`
3. `best_country_code = 'USA'`
4. `best_state_region IN ('NY','CT','RI','MA','ME','NH')`
5. candidate name passes a person-name sanity gate
6. RocketReach request payload includes geographic narrowing derived from the geo-prepare outputs

This work must improve query precision and reduce wasted RocketReach credits.

## 2. Background (Observed Production Evidence)
Current production observations:
1. Hold-pool geo-prepare on the first `500` candidates produced:
   - `enrichment_ready = 435`
   - `too_vague = 25`
   - `junk_location = 12`
   - `multi_location_conflict = 28`
2. Northeast USA `enrichment_ready` subset size:
   - total = `346`
   - `CT = 142`
   - `NY = 76`
   - `MA = 70`
   - `RI = 49`
   - `ME = 5`
   - `NH = 4`
3. Name-quality screening on this Northeast subset:
   - likely org names = `5`
   - likely compound names = `0`
4. Current RocketReach live runs are failing because request payloads are still name-only, for example:
   - `{"name": "Conway, Ryan"}`
   - `{"name": "Skip McGuire"}`
5. Recent RocketReach results:
   - `api_error = 110`
   - `rocketreach_email_required = 35`
6. Geo-ready data already exists but is not yet being used in the outbound RocketReach lookup payload.

Interpretation:
1. Selection quality is now good enough to target a focused Northeast subset.
2. The current RocketReach implementation is still using the wrong lookup shape.
3. Further paid runs should not happen until the lookup request actually uses geography and excludes obvious non-person names.

## 3. Non-Negotiable Workflow Constraint
RocketReach remains upstream evidence only.

Required flow:
1. prepare geo readiness,
2. select a constrained subset,
3. perform RocketReach lookup,
4. write candidate/source evidence,
5. rerun `resolution_score`,
6. promote only through `resolution_promote`.

Prohibited behavior:
1. direct writes to canonical tables,
2. broad live RocketReach runs against all hold candidates,
3. name-only RocketReach lookups for the Northeast hold pool,
4. disabling existing identity-safety gates.

## 4. Scope
In scope:
1. RocketReach candidate selection refinement
2. Northeast-only geography filter
3. person-name quality gate
4. geo-aware request payload construction
5. tests
6. reporting/counters for filtered/skipped candidates

Out of scope:
1. participant scoring rule changes
2. hold-pool geo normalization itself
3. Mailchimp ingestion
4. canonical promotion logic
5. non-USA geographies in v1 of this refinement

## 5. Functional Requirements

### R1. Add Northeast-Only Selection Filter
RocketReach participant enrichment must support a Northeast-only filter based on geo-prepare outputs.

Minimum geography filter:
1. `candidate_participant_enrichment_readiness.readiness_status = 'enrichment_ready'`
2. `best_country_code = 'USA'`
3. `best_state_region IN ('NY','CT','RI','MA','ME','NH')`

Recommended CLI options:
1. `--geo-country USA`
2. `--geo-states NY,CT,RI,MA,ME,NH`

If Claude prefers a single preset flag, acceptable alternative:
1. `--selection-profile northeast_hold_geo_ready`

### R2. Keep Hold-Pool and Geo-Ready Constraints
This refinement must be additive to the existing hold-pool targeting.

Minimum default live targeting for this mode:
1. `candidate_states = ['hold']`
2. `require_geo_ready = true`
3. `require_missing = 'email_or_phone'`

### R3. Add Person-Name Quality Gate
Before a candidate is sent to RocketReach, the implementation must reject likely non-person names.

Minimum skip cases:
1. normalized name contains organization-like tokens:
   - `club`
   - `team`
   - `junior` when used as an organization/program token
   - `yacht club`
2. normalized name is obviously multi-person or list-like
3. normalized name is missing or too short to represent a plausible person

Recommended initial rules:
1. reject names containing `&`
2. reject names containing `/`
3. reject names with comma-separated inverted formatting if the string still appears malformed after normalization
4. reject names with more than a policy-defined number of tokens when they look like concatenated people/program labels
5. reject known org/program patterns by regex

This gate must fail closed: skipped, not called.

### R4. Use Geo Hint in RocketReach Request Payload
The outbound RocketReach lookup request for geo-ready hold candidates must include geographic narrowing derived from readiness outputs.

Minimum request inputs:
1. `name`
2. one or more of:
   - `city`
   - `state`
   - `country`

Claude must verify the actual supported RocketReach request parameters in the current implementation/docs before finalizing exact payload field names.

Acceptable mapping strategy:
1. `name = candidate_participant.display_name or normalized_name`
2. `city = candidate_participant_enrichment_readiness.best_city`
3. `state = candidate_participant_enrichment_readiness.best_state_region`
4. `country = candidate_participant_enrichment_readiness.best_country_code`

If RocketReach requires different parameter names, adapt accordingly, but geography must materially narrow the search.

### R5. Remove Email Requirement for Geo-Ready Hold Candidates
The current `rocketreach_email_required` skip path must not apply to the intended hold-pool lookup shape when:
1. candidate is geo-ready,
2. candidate is in the selected geography filter,
3. a name and geographic hint are present.

Email should remain optional in this path.

### R6. Preserve Existing Identity Safety
Geo-aware lookup must not weaken identity safety.

Required behavior:
1. if API response is ambiguous, do not apply fields
2. if response conflicts with strong existing candidate identity evidence, do not apply fields
3. continue to record audit rows for skipped/ambiguous/error outcomes

### R7. Add Explicit Skip Reason for Name-Quality Gate
Add a distinct skip/error reason for candidates rejected before calling RocketReach because the candidate name is not a plausible person.

Recommended reason code:
1. `rocketreach_non_person_name`

This must be reported separately from API errors.

## 6. Implementation Requirements

### I1. Extend Candidate Selection Query
File: `src/regatta_etl/import_rocketreach_enrichment.py`

Current `_select_candidates(...)` already supports `require_geo_ready`.
Extend it to support:
1. geo-country filter
2. geo-state list filter
3. join/readiness-based access to best city/state/country for request building

Implementation may:
1. include readiness columns directly in the selected row dict, or
2. load them in a second bulk step

Avoid N+1 query patterns if practical.

### I2. Add Person-Name Gate Before API Call
Add a helper such as:
1. `_is_plausible_person_name(...)`
2. or `_preflight_candidate_for_geo_lookup(...)`

This gate should run before making a billable API call.

If a candidate fails this gate:
1. create an enrichment row with `status = 'skipped'`
2. record `error_code = 'rocketreach_non_person_name'`
3. do not call the API

### I3. Build Geo-Aware Request Payload
Update request construction so geo-ready candidates use both name and geography.

Minimum requirement:
1. request payload recorded in `rocketreach_enrichment_row.request_payload` must visibly contain the geographic narrowing values used for the lookup

This is necessary for auditability and debugging.

### I4. Preserve Existing Audit and Counters
Existing audit tables and counters must still work.
Add counters if needed:
1. `rocketreach_non_person_name`
2. `rocketreach_geo_filtered_candidates`
3. `rocketreach_geo_ready_candidates_called`

If Claude chooses different names, keep them consistent with existing counter conventions.

## 7. CLI Requirements
Add CLI support for geography-restricted runs.

Recommended flags:
1. `--geo-country` (default none)
2. `--geo-states` (default none)
3. `--require-person-name-quality/--no-require-person-name-quality` (default true for geo-ready hold profile)

Acceptable alternative:
1. a single preset profile flag, if it is implemented clearly and documented.

## 8. Test Requirements

### 8.1 Unit
1. person-name gate accepts plausible individual names
2. person-name gate rejects org/program names such as `noroton yacht club junior big boat`
3. person-name gate rejects malformed multi-person/list-like names
4. geo filter includes only configured Northeast states
5. request payload builder includes geographic fields from readiness data
6. geo-ready hold candidates no longer hit `rocketreach_email_required`

### 8.2 Integration
1. candidate in `CT` with `enrichment_ready` is selected when Northeast filter is enabled
2. candidate in `FL` with `enrichment_ready` is excluded when Northeast filter is enabled
3. candidate with org-like name is skipped before API call with `rocketreach_non_person_name`
4. selected candidate writes a request payload containing name + geography
5. RocketReach dry-run selection/reporting reflects Northeast filtering
6. existing `require_geo_ready` behavior remains intact

## 9. Acceptance Criteria
1. RocketReach live selection can be constrained to the Northeast USA hold pool.
2. Outbound request payloads for this path include geographic narrowing.
3. Non-person candidate names are skipped before billable API calls.
4. The previous `rocketreach_email_required` skip path no longer blocks intended geo-ready hold candidates.
5. Tests pass for selection, gating, and request-building behavior.

## 10. Recommended Rollout Plan
1. implement the Northeast geo filter and person-name gate
2. run dry-run selection on:
   - `hold`
   - `require_geo_ready`
   - `USA`
   - `NY,CT,RI,MA,ME,NH`
3. inspect selected names and recorded request payloads
4. run a live pilot of `10-25` candidates max
5. inspect `rocketreach_enrichment_row` outcomes before scaling
6. rerun `resolution_score` only after matched/applied outcomes exist

## 11. Recommended Validation Queries

### 11.1 Northeast Ready Distribution
```sql
SELECT
  best_state_region,
  COUNT(*) AS ready_candidates
FROM candidate_participant_enrichment_readiness
WHERE readiness_status = 'enrichment_ready'
  AND best_country_code = 'USA'
  AND best_state_region IN ('NY','CT','RI','MA','ME','NH')
GROUP BY 1
ORDER BY 2 DESC, 1;
```

### 11.2 Name-Quality Summary
```sql
SELECT
  COUNT(*) AS northeast_ready_total,
  COUNT(*) FILTER (WHERE cp.normalized_name ~* 'yacht club|club|team|junior') AS likely_org_names,
  COUNT(*) FILTER (WHERE cp.normalized_name ~ '[,&/]') AS likely_compound_names
FROM candidate_participant_enrichment_readiness cper
JOIN candidate_participant cp
  ON cp.id = cper.candidate_participant_id
WHERE cper.readiness_status = 'enrichment_ready'
  AND cper.best_country_code = 'USA'
  AND cper.best_state_region IN ('NY','CT','RI','MA','ME','NH');
```

### 11.3 Post-Run Outcome Summary
```sql
SELECT status, error_code, COUNT(*) AS n
FROM rocketreach_enrichment_row
GROUP BY 1,2
ORDER BY n DESC, status, error_code;
```

## 12. Explicit Non-Goals
Do not:
1. broaden paid RocketReach runs beyond the Northeast subset in this change,
2. remove existing ambiguity/conflict protections,
3. reroute enrichment outside the existing candidate scoring pipeline,
4. assume raw geo readiness alone is sufficient without person-name sanity checks.
