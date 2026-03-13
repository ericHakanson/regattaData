# Requirements: Mailchimp Participant Evidence Must Flow Into Candidate Evidence

## Objective
Make Mailchimp-resolved participant evidence materially affect the participant candidate pipeline.

Specifically:
1. Mailchimp evidence already ingested into operational / Mailchimp tables must flow into:
   - `candidate_participant_contact`
   - `candidate_participant_address`
2. That evidence must attach to the same participant candidates used by `resolution_score`.
3. After the fix, Mailchimp-origin email / phone evidence must be visible to participant scoring via the existing child-evidence scoring path.

This is a `source -> candidate -> score -> promote` fidelity fix. It is not a new enrichment system and it must not bypass existing resolution or promotion rules.

## Background (Observed Production Evidence)
Observed on March 12, 2026:
1. `mailchimp_audience` real run succeeded:
   - `rows_read = 1022`
   - `participants_matched_existing = 764`
   - `mailchimp_identity_links_updated = 764`
   - `mailchimp_missing_name_unique_email_accepted = 757`
   - `db_phase_errors = 0`
2. Re-running:
   - `resolution_source_to_candidate --entity-type participant`
   - `resolution_score --entity-type participant`
   - `resolution_promote --entity-type participant --dry-run`
   produced no scoring change attributable to Mailchimp evidence.
3. Participant scoring report remained:
   - `child email used = 0`
   - `child phone used = 0`
   - `child address used = 3897`
   - `address-only -> hold = 3897`
4. That means Mailchimp evidence is landing in Mailchimp/operational tables, but not in the participant candidate evidence path consumed by scoring.

## Problem Statement
There is a disconnect between:
1. `mailchimp_audience` ingestion, which resolves many rows to operational `participant` records and writes curated contact/address evidence there, and
2. `resolution_source_to_candidate`, which should project that evidence into candidate evidence for scoring.

Today, `resolution_source_to_candidate` has a Mailchimp ingestion path, but it is too raw-row-centric:
1. it reads directly from `mailchimp_audience_row`
2. it fingerprints off raw Mailchimp name/email
3. it does not use the participant resolution outcome already captured by Mailchimp ingestion
4. it does not meaningfully improve the unresolved participant candidate pool in production

Operationally, this means Mailchimp has been matched to 764 existing participants, but participant candidate scoring still sees zero Mailchimp-driven child email/phone evidence.

## Scope
In scope:
1. Mailchimp-related participant projection inside `resolution_source_to_candidate`
2. Candidate child evidence creation for Mailchimp-derived email/phone/address
3. Integration tests proving Mailchimp evidence reaches candidate scoring
4. Idempotency and source-link behavior

Out of scope:
1. Mailchimp CSV ingestion contract itself
2. Mailchimp strict identity policy changes
3. RocketReach
4. Canonical promotion logic
5. Scoring rule changes unrelated to Mailchimp evidence flow

## Design Principle
Mailchimp source rows that were already identity-resolved to an operational participant must enrich the candidate graph through that resolved participant anchor, not through an isolated raw Mailchimp candidate branch.

In plain terms:
1. If Mailchimp ingestion already determined “this row belongs to participant X”,
2. then source-to-candidate should enrich the participant candidate derived from participant X,
3. not create or maintain a separate candidate based only on raw Mailchimp row identity.

## Functional Requirements

### R1. Use Resolved Participant Identity as the Primary Anchor
For Mailchimp-derived participant projection in `resolution_source_to_candidate`:
1. Prefer the participant identity already resolved by `mailchimp_audience`.
2. Use `mailchimp_contact_state.participant_id` as the authoritative curated anchor for Mailchimp rows that passed strict identity checks.
3. Do not rely solely on raw Mailchimp `First Name` / `Last Name` / email fingerprint when a resolved `participant_id` is available.

### R2. Mailchimp-Resolved Participants Must Enrich Candidate Evidence
For each Mailchimp-resolved participant:
1. resolve or reuse the target `candidate_participant`
2. attach candidate child contact evidence from Mailchimp-derived data
3. attach candidate child address evidence from Mailchimp-derived data when present
4. create candidate source links that preserve Mailchimp provenance

Minimum candidate child evidence targets:
1. `candidate_participant_contact(contact_type='email')`
2. `candidate_participant_contact(contact_type='phone')`
3. `candidate_participant_address`

### R3. Avoid Isolated Mailchimp Candidate Branches for Resolved Participants
When a Mailchimp row already corresponds to an operational participant:
1. do not create a duplicate candidate branch solely from raw Mailchimp name/email unless there is a demonstrated technical reason
2. enrich the participant candidate that would also be reached from the `participant` table path

This is the central behavior change.

### R4. Quarantined / Unresolved Mailchimp Rows Must Remain Out of Candidate Projection
Rows that were quarantined by Mailchimp identity policy must:
1. remain preserved in raw Mailchimp tables
2. remain visible in review queues
3. not be projected into candidate evidence

No guessing and no fallback candidate creation for quarantined Mailchimp rows.

### R5. Mailchimp Email Must Reach Candidate Evidence and Scoring
For Mailchimp-resolved participants with email:
1. a `candidate_participant_contact(contact_type='email')` row must exist on the target candidate
2. participant scoring must be able to consume that evidence through the existing child-evidence scoring path

Expected operational effect after subsequent scoring:
1. `child email used` should become nonzero

### R6. Mailchimp Phone Must Reach Candidate Evidence and Scoring
For Mailchimp-resolved participants with phone:
1. a `candidate_participant_contact(contact_type='phone')` row must exist on the target candidate
2. participant scoring must be able to consume that evidence through the existing child-evidence scoring path

Expected operational effect after subsequent scoring:
1. `child phone used` should become nonzero

### R7. Mailchimp Address Must Reach Candidate Evidence
For Mailchimp-resolved participants with address:
1. a `candidate_participant_address` row must exist on the target candidate
2. source provenance must indicate Mailchimp origin

If structured address parsing is already available and safe, preserve it.
If only raw address is safe, preserve raw address without inventing structured fields.

### R8. Preserve Existing Participant-Table Projection
This fix must not break the existing participant-table projection path.

The goal is:
1. participant-table evidence continues to flow as today
2. Mailchimp-resolved participant evidence joins that same candidate graph
3. reruns remain idempotent

## Preferred Implementation Direction

### I1. Replace Raw-Row-Only Mailchimp Candidate Projection With Resolved-Participant Projection
File: `src/regatta_etl/resolution_source_to_candidate.py`

Current function:
1. `_ingest_participants_from_mailchimp(...)`

Required behavior change:
1. build Mailchimp participant projection primarily from resolved participant linkage, not just raw Mailchimp rows
2. use `mailchimp_contact_state.participant_id` as the starting point for rows that passed strict identity resolution
3. join back to `mailchimp_audience_row` for raw payload and source provenance as needed

Acceptable implementation shapes:
1. rewrite `_ingest_participants_from_mailchimp(...)` to project from `mailchimp_contact_state`
2. or add a new helper that resolves Mailchimp rows to `participant_id` first, then enriches candidate evidence from that participant anchor

### I2. Reuse Existing Candidate Child Evidence Helpers
File: `src/regatta_etl/resolution_source_to_candidate.py`

Use existing helpers rather than inventing a separate insert path:
1. `_upsert_contact(...)`
2. `_upsert_address(...)`
3. `_link_source(...)`
4. existing participant candidate resolution / under-combination helpers when relevant

### I3. Candidate Resolution Must Follow the Participant Graph
For a Mailchimp-resolved operational participant:
1. derive the target candidate in the same way as participant-table ingestion would
2. if an existing candidate is already the correct projection target, enrich that candidate
3. if no candidate exists yet, create the candidate once and attach evidence there

### I4. Mailchimp Provenance Must Be Preserved
Mailchimp evidence projected into candidate child tables must retain provenance:
1. `source_table_name = 'mailchimp_audience_row'` for direct row evidence
2. candidate source links should make Mailchimp origin traceable

If the implementation uses `mailchimp_contact_state` as the resolution anchor, that is acceptable, but the underlying Mailchimp row provenance must remain recoverable.

### I5. Do Not Weaken Strict Mailchimp Identity Policy
This change must not:
1. auto-link quarantined Mailchimp rows
2. bypass `participant_mailchimp_identity`
3. reintroduce permissive raw-email-only projection for ambiguous rows

The strict identity policy remains authoritative.

## Test Requirements

File: `tests/integration/test_resolution_source_to_candidate.py`

### T1. Mailchimp-Resolved Participant Enriches Existing Candidate, Not Isolated Duplicate
Add an integration test where:
1. seed an operational `participant`
2. seed Mailchimp-derived state/row for that same participant
3. run `resolution_source_to_candidate(..., entity_type='participant')`

Assert:
1. candidate evidence attaches to the intended participant candidate
2. no extra standalone Mailchimp-only duplicate candidate is created for that same person

### T2. Mailchimp Email Produces Candidate Email Child Row on Target Candidate
Add a test proving:
1. Mailchimp-resolved participant with email
2. source-to-candidate run
3. target candidate has `candidate_participant_contact(contact_type='email')`

Verify attachment to the intended candidate id, not just a global count.

### T3. Mailchimp Phone Produces Candidate Phone Child Row on Target Candidate
Add the analogous phone test.

### T4. Mailchimp Address Produces Candidate Address Child Row on Target Candidate
Add the analogous address test.

### T5. Quarantined Mailchimp Row Does Not Project Candidate Evidence
Add a regression test where:
1. Mailchimp row remains quarantined by identity policy
2. source-to-candidate runs

Assert:
1. no candidate child contact/address evidence is created from that row
2. raw Mailchimp capture remains intact

### T6. Idempotency
Add or extend a rerun test proving:
1. running `resolution_source_to_candidate(..., entity_type='participant')` twice
2. does not duplicate Mailchimp-derived candidate child evidence
3. does not duplicate candidate source links

### T7. End-to-End Mailchimp Evidence Reaches Scoring
Add an integration test spanning:
1. Mailchimp-resolved participant projection into candidate evidence
2. `run_score(conn, entity_type='participant')`

Assert:
1. `confidence_reasons` shows Mailchimp-derived child email and/or child phone being used through scoring
2. the candidate score/state improves accordingly

File: `tests/integration/test_resolution_score_and_promote.py`

### T8. Mailchimp-Origin Child Evidence Is Consumable by Participant Scoring
Add a participant scoring test that explicitly seeds Mailchimp-origin child contact evidence and confirms:
1. child email suppresses `missing_email`
2. child phone suppresses `missing_phone`
3. provenance origin does not prevent scoring consumption

## Acceptance Criteria
This work is complete when all of the following are true:

1. Mailchimp rows that were already resolved to a participant enrich that participant’s candidate evidence graph.
2. Mailchimp-resolved participant evidence creates candidate child contact/address rows on the intended candidate.
3. Quarantined Mailchimp rows do not project candidate evidence.
4. Reruns remain idempotent.
5. A real production rerun of:
   - `mailchimp_audience`
   - `resolution_source_to_candidate --entity-type participant`
   - `resolution_score --entity-type participant`
   produces:
   - `child email used > 0` and/or `child phone used > 0`
   - no DB errors
6. Promotion remains conservative; no unreviewed explosion of `auto_promote`.

## Operational Verification Queries
After implementation, run these in production.

### Q1. Mailchimp-Origin Candidate Contact Evidence
```sql
SELECT source_table_name, contact_type, COUNT(*) AS n
FROM candidate_participant_contact
WHERE source_table_name = 'mailchimp_audience_row'
GROUP BY 1,2
ORDER BY 1,2;
```

Expected direction:
1. email and/or phone rows should be nonzero

### Q2. Mailchimp-Origin Candidate Address Evidence
```sql
SELECT source_table_name, COUNT(*) AS n
FROM candidate_participant_address
WHERE source_table_name = 'mailchimp_audience_row'
GROUP BY 1;
```

### Q3. Scoring Child Evidence Usage
Run:
```bash
.venv/bin/regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type participant
```

Then inspect report counters:
1. `child email used`
2. `child phone used`

Expected direction:
1. at least one of these becomes nonzero

### Q4. Promotion Safety Check
```bash
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type participant --dry-run
```

Review:
1. new promotable counts
2. absence of DB errors
3. no surprising jump inconsistent with Mailchimp coverage size

## Notes for Claude
This is not a request to make Mailchimp ingestion more permissive.

Do not solve this by:
1. weakening strict Mailchimp identity rules
2. auto-linking quarantined Mailchimp rows
3. creating more raw-row-based Mailchimp-only candidates

Solve it by making already-resolved Mailchimp participant evidence enrich the same candidate graph used by scoring.
