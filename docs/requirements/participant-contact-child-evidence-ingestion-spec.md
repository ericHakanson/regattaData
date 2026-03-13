# Requirements: Participant Contact and Address Child Evidence Ingestion

## Objective
Ensure participant source ingestion preserves contact and address evidence at the candidate layer even when a participant candidate is not yet promotable.

Specifically:
1. When participant source rows contain email, phone, and/or address data, that evidence must be attached to the resolved `candidate_participant` via:
   - `candidate_participant_contact`
   - `candidate_participant_address`
2. This must work for source rows coming from:
   - `participant`
   - `yacht_scoring_raw_row`
3. Reject candidates must be allowed to carry child contact/address evidence before rescoring.

This is a source-to-candidate fidelity fix. It is not a scoring-rule change and not a canonical-layer change.

## Background (Observed Production Evidence)
Current production diagnostics show:
1. `8469` participant candidates are in `resolution_state='reject'`.
2. Of those rejects:
   - `best_email IS NOT NULL` = `0`
   - `best_phone IS NOT NULL` = `0`
3. Also of those rejects:
   - `candidate_participant_contact` child rows = `0`
   - `candidate_participant_address` child rows = `0`
4. Reject-side source evidence is still present:
   - `yacht_scoring_raw_row` links = `11443`
   - `participant` links = `8449`

Interpretation:
1. Source rows are reaching `candidate_participant`.
2. Source provenance is being preserved in `candidate_source_link`.
3. Contact/address evidence is not being materialized as candidate child rows for the reject pool.

That means the current bottleneck is candidate construction fidelity, not scoring, remediation, or promotion.

## Scope
In scope:
1. `resolution_source_to_candidate` participant ingestion behavior.
2. Candidate child row creation for contact and address evidence.
3. Integration tests for `participant` and `yacht_scoring_raw_row` source paths.
4. Regression coverage for reject candidates retaining child evidence pre-score.

Out of scope:
1. Changes to participant scoring thresholds.
2. Changes to canonical promotion logic.
3. RocketReach or Mailchimp API enrichment behavior.
4. New schema or migrations unless absolutely required.

## Problem Statement
The current source-to-candidate pipeline appears to preserve top-level participant identity and source links but fails to attach available contact/address evidence to `candidate_participant` child tables for many unresolved candidates.

This is materially harmful because:
1. The candidate layer loses useful evidence needed for later scoring and review.
2. Reject candidates become permanently data-poor even when raw source evidence exists.
3. Subsequent enrichment tooling is forced to operate on weaker anchors than necessary.

## Functional Requirements

### R1. Attach Participant Contact Evidence During Source Ingestion
When a participant source row resolves to a `candidate_participant` and contains contact data, ingestion must write child rows to `candidate_participant_contact`.

Required contact types:
1. Email
2. Phone

Rules:
1. Use existing candidate resolution behavior first.
2. Once the target `candidate_participant_id` is known, attach contact child rows using the existing idempotent helper behavior already present in code.
3. Contact child row insertion must not depend on whether the target candidate will later score as `auto_promote`, `review`, `hold`, or `reject`.
4. Duplicate inserts must remain idempotent across reruns.

### R2. Attach Participant Address Evidence During Source Ingestion
When a participant source row resolves to a `candidate_participant` and contains address data, ingestion must write child rows to `candidate_participant_address`.

Rules:
1. Preserve the raw address string whenever available.
2. Populate structured address columns when source data provides them.
3. Address child row insertion must not depend on scoring state.
4. Duplicate inserts must remain idempotent across reruns.

### R3. Applies to `participant` Source Path
For the ingestion path reading from the `participant` table:
1. If the participant row has email contact(s), create `candidate_participant_contact` child rows.
2. If the participant row has phone contact(s), create `candidate_participant_contact` child rows.
3. If the participant row has participant address row(s), create `candidate_participant_address` child rows.
4. This must work whether the target candidate is:
   - newly created, or
   - existing and matched by fingerprint or under-combination reuse logic.

### R4. Applies to `yacht_scoring_raw_row` Source Path
For the participant ingestion path derived from `yacht_scoring_raw_row`:
1. If row-level source data contains email, attach an email child row.
2. If row-level source data contains phone, attach a phone child row.
3. If row-level source data contains address, attach an address child row.
4. If a given yacht scoring schema variant does not provide contact/address fields, behavior may remain no-op for that row.
5. The implementation must be schema-tolerant: missing fields are acceptable, but present fields must not be silently discarded.

### R5. Reject Candidates May Carry Child Evidence
A participant candidate in `resolution_state='reject'` is allowed to have:
1. `candidate_participant_contact` rows
2. `candidate_participant_address` rows

This is expected and correct. Rejection at the current score does not mean evidence should be dropped.

### R6. No Forced Top-Level Mutation Requirement
This change does not require source ingestion to immediately populate top-level `best_email` or `best_phone` on every affected candidate.

It is acceptable for:
1. child evidence to be written first, and
2. top-level participant scoring behavior to continue using existing rules until later scoring/refinement logic consumes that evidence.

If the current code path already sets top-level `best_email` / `best_phone` for some sources, preserve that behavior. Do not remove it.

### R7. Idempotency and Provenance
The fix must preserve:
1. existing `candidate_source_link` behavior
2. existing idempotent upsert semantics for contact/address child rows
3. rerun safety of `resolution_source_to_candidate --entity-type participant`

Re-running ingestion must not create duplicate contact/address child rows for the same candidate and source evidence.

## Implementation Requirements

### I1. Reuse Existing Helpers
File: `src/regatta_etl/resolution_source_to_candidate.py`

The implementation should use the existing helper functions rather than inventing a parallel insert path:
1. `_upsert_contact(...)`
2. `_upsert_address(...)`

This is a wiring/call-site fix, not a schema redesign.

### I2. Participant Table Ingestion
File: `src/regatta_etl/resolution_source_to_candidate.py`

In the participant-table ingestion path:
1. Resolve or create the target `candidate_participant`.
2. Enumerate participant contact points relevant to email/phone.
3. Call `_upsert_contact(...)` for each eligible contact point.
4. Enumerate participant addresses.
5. Call `_upsert_address(...)` for each eligible address row.

Important:
1. This must happen after candidate resolution so the child rows attach to the final candidate id.
2. This must work both for newly created candidates and reused existing candidates.

### I3. Yacht Scoring Participant Ingestion
File: `src/regatta_etl/resolution_source_to_candidate.py`

In the yacht-scoring participant ingestion path:
1. Identify which payload fields may contain participant email, phone, or address information.
2. Normalize them consistently with existing participant ingestion conventions.
3. Call `_upsert_contact(...)` and `_upsert_address(...)` when those fields are present and non-empty.

Implementation note:
1. If there are multiple yacht scoring row shapes, handle the known shapes explicitly.
2. Do not fail the row simply because optional contact/address fields are absent.

### I4. No Scoring Changes in This Patch
Do not change:
1. `config/resolution_rules/participant.yml`
2. participant score thresholds
3. promotion logic

This patch should isolate source-to-candidate evidence fidelity.

## Test Requirements

File: `tests/integration/test_resolution_source_to_candidate.py`

### T1. Participant Source Row with Email Produces Contact Child Row
Add a regression test proving:
1. Seed a `participant` row with email.
2. Run `run_source_to_candidate(..., entity_type='participant')`.
3. Assert a `candidate_participant_contact` row exists with:
   - `contact_type='email'`
   - the expected `candidate_participant_id`

This should go beyond a mere global count assertion. The test must verify the child row is attached to the intended candidate.

### T2. Participant Source Row with Address Produces Address Child Row
Add a test proving:
1. Seed a `participant` row and a `participant_address` row.
2. Run participant source ingestion.
3. Assert a `candidate_participant_address` row exists for the resolved candidate.

Again, verify attachment to the intended candidate, not just a nonzero table count.

### T3. Reject Candidate Can Carry Email/Phone/Address Child Evidence
Add an integration test proving:
1. Seed source data such that the resulting participant candidate remains low-confidence / reject after scoring.
2. Ensure the source row includes at least one contact or address datum.
3. Run:
   - `resolution_source_to_candidate --entity-type participant`
   - `resolution_score --entity-type participant`
4. Assert:
   - candidate remains `resolution_state='reject'`
   - corresponding `candidate_participant_contact` and/or `candidate_participant_address` row still exists

Purpose:
1. prove child evidence survives before promotion
2. prove reject state does not imply child evidence absence

### T4. Yacht Scoring Row with Contact Data Produces Child Evidence
Add an integration test proving:
1. Seed a `yacht_scoring_raw_row` payload that includes participant name plus at least one contact field.
2. Run participant source ingestion.
3. Assert the resolved candidate gets the expected child contact row.

If address fields exist in the supported yacht scoring schema, add the analogous address assertion too.

### T5. Idempotency
Add or extend a test proving:
1. Running `run_source_to_candidate(..., entity_type='participant')` twice
2. does not duplicate the same candidate contact/address child evidence rows.

### T6. Existing Under-Combination Behavior Must Still Pass
The new change must not regress:
1. unique-email name reuse logic
2. ambiguous-name warning behavior
3. under-combination remediation tests

## Acceptance Criteria
The implementation is acceptable when all of the following are true:

1. Participant source ingestion attaches email/phone/address child evidence to candidate participants when the source data provides it.
2. This works for both:
   - `participant`
   - `yacht_scoring_raw_row`
3. Reject candidates are allowed and demonstrated by tests to retain child contact/address evidence.
4. Running `resolution_source_to_candidate --entity-type participant` twice remains idempotent for child evidence rows.
5. Existing under-combination tests still pass.
6. No schema migration is introduced unless there is a demonstrated technical need.

## Operational Verification Queries
After implementation, use these queries in the real environment.

### Q1. Reject Candidates with Child Contact Evidence
```sql
SELECT
  COUNT(DISTINCT cp.id) AS reject_candidates,
  COUNT(DISTINCT cp.id) FILTER (WHERE cpc.candidate_participant_id IS NOT NULL) AS rejects_with_contact_rows,
  COUNT(DISTINCT cp.id) FILTER (WHERE cpa.candidate_participant_id IS NOT NULL) AS rejects_with_address_rows
FROM candidate_participant cp
LEFT JOIN candidate_participant_contact cpc
  ON cpc.candidate_participant_id = cp.id
LEFT JOIN candidate_participant_address cpa
  ON cpa.candidate_participant_id = cp.id
WHERE cp.resolution_state = 'reject'
  AND cp.is_promoted = false;
```

Expected direction:
1. `rejects_with_contact_rows` should rise above `0`
2. `rejects_with_address_rows` should rise above `0`

### Q2. Contact Child Rows by Source Table
```sql
SELECT source_table_name, contact_type, COUNT(*) AS n
FROM candidate_participant_contact
GROUP BY 1,2
ORDER BY 1,2;
```

Expected direction:
1. contact evidence attributable to participant ingestion sources is visible

### Q3. Address Child Rows by Source Table
```sql
SELECT source_table_name, COUNT(*) AS n
FROM candidate_participant_address
GROUP BY 1
ORDER BY 2 DESC;
```

### Q4. Post-Fix Participant Rebuild Sequence
After Claude implements the change, run:
```bash
.venv/bin/regatta-import --mode resolution_source_to_candidate --db-dsn "$DB_DSN" --entity-type participant
.venv/bin/regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type participant
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type participant --dry-run
```

Review whether:
1. reject child-evidence counts increased
2. review/auto-promote counts improved
3. no new DB errors were introduced

## Notes for Claude
This spec is intentionally narrow.

Do not solve this by:
1. weakening identity matching
2. changing scoring thresholds
3. introducing RocketReach as a substitute for missing source fidelity

Solve it at the source-to-candidate layer by ensuring existing source evidence is actually preserved in candidate child tables.
