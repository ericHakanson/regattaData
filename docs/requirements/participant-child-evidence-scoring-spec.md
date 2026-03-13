# Requirements: Participant Scoring Must Consume Child Contact and Address Evidence

## Objective
Update participant scoring so `resolution_score` uses evidence already stored on:
1. `candidate_participant_contact`
2. `candidate_participant_address`

This is required because participant source ingestion now preserves child contact/address evidence, but participant scoring still only reads top-level candidate columns:
1. `best_email`
2. `best_phone`
3. `date_of_birth`
4. `normalized_name`

The result today is that many participant candidates carry valid child evidence but still score as if that evidence does not exist.

## Background (Observed Production Evidence)
Current production state after the child-evidence ingestion fix:
1. `candidate_participant`:
   - `auto_promote = 1412`
   - `reject = 8469`
2. Reject candidate top-level coverage:
   - `best_email IS NOT NULL = 0`
   - `best_phone IS NOT NULL = 0`
3. Reject candidate child evidence coverage:
   - `candidate_participant_contact` rows on rejects = `0`
   - `candidate_participant_address` rows on rejects = `3897`
4. Address source distribution:
   - `yacht_scoring_raw_row = 4850`
   - `participant_address = 622`
5. Sample reject `confidence_reasons` still show only:
   - `feature:normalized_name_exact:0.1000`
   - `penalty:missing_email:0.1000`
   - `penalty:missing_phone:0.0500`
   - final `quality_score = 0.0000`

Interpretation:
1. Child address evidence is now present at scale.
2. Participant scoring ignores that evidence entirely.
3. Future child email/phone evidence would also be ignored unless scoring is changed.

## Scope
In scope:
1. `src/regatta_etl/resolution_score.py`
2. `config/resolution_rules/participant.yml`
3. Participant-specific scoring tests
4. Run-report visibility for child-evidence-driven scoring

Out of scope:
1. Source ingestion behavior
2. Mailchimp or RocketReach ingestion logic
3. Canonical promotion logic
4. Identity matching policy changes

## Non-Negotiable Constraints
1. Child evidence must improve scoring fidelity without bypassing the candidate scoring pipeline.
2. Address-only evidence must not create an unsafe `auto_promote` path.
3. Email/phone child evidence must be treated as functionally equivalent to top-level email/phone presence for scoring.
4. The scoring outcome must remain explainable via `confidence_reasons`.

## Current Technical Limitation
Today `_features_participant(...)` in `resolution_score.py` derives participant features only from the candidate row:
1. `email_exact` from `best_email`
2. `phone_exact` from `best_phone`
3. `dob_exact` from `date_of_birth`
4. `normalized_name_exact` from `normalized_name`

No child-table lookup is performed, so:
1. `candidate_participant_contact(contact_type='email')` is ignored if `best_email` is null.
2. `candidate_participant_contact(contact_type='phone')` is ignored if `best_phone` is null.
3. `candidate_participant_address` is ignored entirely.

## Functional Requirements

### R1. Child Email Evidence Counts for Participant Scoring
Participant scoring must treat a candidate as having email evidence when either of the following is true:
1. `candidate_participant.best_email` is non-null
2. at least one `candidate_participant_contact` row exists with:
   - `contact_type = 'email'`
   - non-null usable value

This must satisfy the participant email feature for scoring and suppress the missing-email penalty.

### R2. Child Phone Evidence Counts for Participant Scoring
Participant scoring must treat a candidate as having phone evidence when either of the following is true:
1. `candidate_participant.best_phone` is non-null
2. at least one `candidate_participant_contact` row exists with:
   - `contact_type = 'phone'`
   - non-null usable value

This must satisfy the participant phone feature for scoring and suppress the missing-phone penalty.

### R3. Child Address Evidence Adds a New Participant Scoring Feature
Participant scoring must introduce a new boolean feature representing address evidence:
1. recommended feature name: `address_present`

This feature is true when at least one `candidate_participant_address` row exists for the candidate.

This feature must:
1. contribute positive score when present
2. appear in `confidence_reasons`
3. not create an unsafe direct path to `auto_promote`

### R4. Conservative Address Policy
Initial policy must be conservative.

Required behavior:
1. Address evidence alone must not be enough for `auto_promote`.
2. Address evidence alone must not be enough for `review`.
3. Recommended initial target: address-only participants should rise from `reject` to `hold`, not higher.

This means the participant YAML policy must be adjusted carefully so that:
1. `normalized_name + address_present - missing_email - missing_phone`
2. lands in `hold`
3. but remains below `review`

Claude should tune `participant.yml` accordingly and document the resulting arithmetic in comments.

### R5. Child Email/Phone Must Behave Like Top-Level Email/Phone
A candidate with:
1. `best_email = null`
2. `best_phone = null`
3. child `candidate_participant_contact` rows for email and phone

must score equivalently to a candidate that had the same email/phone evidence on top-level fields.

This is the key requirement for future participant enrichment sources.

### R6. No Double Counting
If both top-level and child evidence exist for the same logical attribute:
1. email must count once
2. phone must count once
3. address must count once

Child evidence is a fallback or supplement for presence detection, not a separate additive multiplier for the same attribute.

### R7. Explainability in Confidence Reasons
Scoring output must remain explainable.

Minimum requirement:
1. `confidence_reasons` must still show the scored feature reason such as:
   - `feature:email_exact:...`
   - `feature:phone_exact:...`
   - `feature:address_present:...`

Recommended additional transparency:
1. append participant-specific evidence origin reasons when child evidence was used, for example:
   - `evidence:child_email_present`
   - `evidence:child_phone_present`
   - `evidence:child_address_present`

These evidence-origin reasons must not affect score directly unless they are actual weighted features.

## Implementation Requirements

### I1. Extend Participant Feature Extraction
File: `src/regatta_etl/resolution_score.py`

Modify participant scoring so it can evaluate:
1. top-level participant columns
2. child contact existence
3. child address existence

Preferred implementation:
1. fetch candidate rows as today
2. for participant scoring only, precompute child-evidence maps keyed by candidate id:
   - has_email_child
   - has_phone_child
   - has_address_child
3. derive effective participant features from the union of top-level and child evidence

This should avoid N+1 per-candidate queries if practical.

### I2. Add Address Feature to Participant Rule File
File: `config/resolution_rules/participant.yml`

Add a new participant feature weight:
1. `address_present`

Also tune thresholds or weights as needed so the initial conservative policy is satisfied:
1. address-only participants should reach `hold`
2. address-only participants should not reach `review`
3. address-only participants must never auto-promote

Claude must document the new score arithmetic in YAML comments, similar to the existing participant comments.

### I3. Preserve Existing Email/Phone Feature Names
To minimize disruption:
1. keep `email_exact`
2. keep `phone_exact`
3. keep `dob_exact`
4. keep `normalized_name_exact`

Recommended behavior:
1. `email_exact` becomes true if top-level email exists OR child email exists
2. `phone_exact` becomes true if top-level phone exists OR child phone exists

This avoids creating a second set of email/phone weights unless Claude finds a strong reason otherwise.

### I4. Add Child-Evidence Counters to Scoring Report
Add participant scoring counters so operators can see whether child evidence is materially affecting scoring.

Minimum recommended counters:
1. `participant_child_email_used`
2. `participant_child_phone_used`
3. `participant_child_address_used`
4. `participant_address_only_hold`

If Claude prefers a different naming shape, keep it consistent with existing `ScoreCounters` conventions.

Run reports should make it obvious whether the new scoring path is being exercised.

## Test Requirements

File: `tests/integration/test_resolution_score_and_promote.py`

### T1. Child Email Satisfies Email Feature
Add an integration test where:
1. a `candidate_participant` has `best_email = null`
2. a child `candidate_participant_contact(contact_type='email')` exists
3. scoring is run

Assert:
1. missing-email penalty does not appear
2. email feature reason does appear
3. resulting score matches expected arithmetic

### T2. Child Phone Satisfies Phone Feature
Add the analogous test for child phone evidence.

Assert:
1. missing-phone penalty does not appear
2. phone feature reason does appear

### T3. Child Email + Child Phone Behave Like Top-Level Email + Phone
Add a regression-equivalence test:
1. candidate A has top-level email+phone
2. candidate B has null top-level email+phone but equivalent child contact rows
3. all other participant fields are equal

Assert:
1. equal quality scores
2. equal resolution states

### T4. Address-Only Candidate Gets Conservative Lift
Add an integration test where:
1. `normalized_name` is present
2. email and phone are absent at top-level
3. no child email/phone exists
4. a child address row exists

Assert:
1. score is greater than `0.0000`
2. `feature:address_present:...` appears
3. state is `hold`
4. state is not `review`
5. state is not `auto_promote`

### T5. Address-Only Candidates Still Do Not Promote
Add an end-to-end test:
1. seed an address-only participant candidate
2. run score
3. run promote

Assert:
1. candidate is not promoted
2. no canonical participant row is created for it

### T6. Existing Top-Level Scoring Behavior Remains Valid
Existing participant scoring tests must still pass for:
1. top-level email-only participants
2. top-level email+phone participants
3. name-only participants

If thresholds or weights change, update expected scores carefully and document why.

## Acceptance Criteria
This work is complete when all of the following are true:

1. Participant scoring uses child contact/address evidence in addition to top-level fields.
2. Child email suppresses the missing-email penalty.
3. Child phone suppresses the missing-phone penalty.
4. Child address contributes a visible positive feature reason.
5. Address-only candidates move out of `reject` into `hold`, but not into `review` or `auto_promote`.
6. Candidates with child email+phone can score equivalently to top-level email+phone candidates.
7. `resolution_score` reports show nonzero usage of child evidence in real runs.

## Operational Verification Queries
After implementation, validate with these queries.

### Q1. Rejects vs Hold After Re-Score
```sql
SELECT
  resolution_state,
  COUNT(*) AS total
FROM candidate_participant
GROUP BY 1
ORDER BY 1;
```

Expected direction:
1. some current address-only rejects should move into `hold`
2. no unexpected explosion into `auto_promote`

### Q2. Hold Candidates with Address Evidence
```sql
SELECT
  COUNT(DISTINCT cp.id) AS hold_candidates_with_address
FROM candidate_participant cp
JOIN candidate_participant_address cpa
  ON cpa.candidate_participant_id = cp.id
WHERE cp.resolution_state = 'hold'
  AND cp.is_promoted = false;
```

### Q3. Child Evidence Usage in Scoring Reasons
```sql
SELECT reason, COUNT(*) AS n
FROM (
  SELECT jsonb_array_elements_text(confidence_reasons) AS reason
  FROM candidate_participant
) r
WHERE reason LIKE 'feature:address_present:%'
   OR reason LIKE 'evidence:child_%'
GROUP BY 1
ORDER BY 2 DESC, 1;
```

## Recommended Runbook After Implementation
```bash
.venv/bin/regatta-import --mode resolution_source_to_candidate --db-dsn "$DB_DSN" --entity-type participant
.venv/bin/regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type participant
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type participant --dry-run
```

Then inspect:
1. `hold` count increase
2. `reject` count decrease
3. `auto_promote` count stability
4. scoring run counters indicating child-evidence usage

## Notes for Claude
This spec intentionally does not ask you to solve the participant reject pool by lowering thresholds broadly.

Solve it by:
1. using the evidence already present in child tables
2. adding a conservative address feature
3. keeping promotion safety intact

Do not solve it by:
1. weakening identity rules
2. force-filling top-level `best_email` / `best_phone` from child rows as a shortcut
3. bypassing scoring and promoting based on child evidence directly
