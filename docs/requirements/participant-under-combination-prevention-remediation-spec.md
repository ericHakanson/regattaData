# Requirements: Participant Under-Combination Prevention and Remediation

## Objective
Eliminate participant "under-combination" in the source-to-candidate pipeline, where the same person is split into:
1. A name+email candidate (promotable), and
2. A name-only candidate (typically rejected).

The fix must:
1. Prevent creation of new split candidate pairs.
2. Provide a safe one-time remediation path for existing split pairs.
3. Preserve idempotency, provenance, and auditability.

## Background (Observed Evidence)
Current production snapshot shows split groups where one `normalized_name` maps to:
1. Candidate A: `best_email` populated, `resolution_state='auto_promote'`.
2. Candidate B: `best_email` null, `resolution_state='reject'`.

Observed count from diagnostic query:
1. `split_name_groups = 29`
2. Export file: `artifacts/reports/participant_under_combination_map.csv` (60 lines including header).

## Scope
In scope:
1. Participant ingestion behavior in `resolution_source_to_candidate`.
2. Existing split-pair remediation for participant candidates.
3. Tests and acceptance queries.

Out of scope:
1. Club/yacht/event candidate behavior.
2. Canonical scoring rule changes.
3. RocketReach API behavior itself.

## Root Cause
`participant_fingerprint(normalized_name, best_email)` creates different fingerprints for:
1. Name-only source rows (`email = null`), and
2. Name+email source rows (`email != null`).

When both row types exist for the same person, two candidate rows are created and maintained independently.

## Functional Requirements

### R1. Prevention Rule (New Behavior)
During participant ingestion, for an incoming participant source row where:
1. `normalized_name` is present, and
2. incoming `best_email` is null,

the pipeline must:
1. Look up existing `candidate_participant` rows by `normalized_name`.
2. If exactly one matching candidate has non-null `best_email`, treat it as the target candidate and enrich/link that existing candidate.
3. If zero matching candidates have non-null `best_email`, keep current behavior.
4. If multiple matching candidates have non-null `best_email`, keep current behavior and append a warning (no guessing).

This rule applies only to participant entity ingestion paths that currently generate name-only records:
1. `participant` table path (if contact email absent).
2. `jotform_waiver_submission` path (if email absent).
3. `yacht_scoring_raw_row` path (owner-name only records).
4. Any other participant source using name-only fallback.

### R2. Determinism and Idempotency
The rule must be deterministic:
1. For the "exactly one candidate with email" case, reuse that candidate every run.
2. Re-running source-to-candidate must not create additional participant candidates for those rows.

### R3. One-Time Remediation for Existing Split Pairs
Add a remediation operation (code path or dedicated utility function) that consolidates existing split pairs:
1. Winner selection (per `normalized_name`):
   - Prefer candidate with non-null `best_email`.
   - Tie-breakers: `is_promoted DESC`, `quality_score DESC`, `updated_at DESC`, `id ASC`.
2. Loser eligibility (strict safety):
   - `resolution_state='reject'`
   - `is_promoted=false`
   - `best_email is null`
3. Transfer from loser to winner:
   - `candidate_source_link` rows
   - `candidate_participant_contact` rows
   - `candidate_participant_address` rows
   - `candidate_participant_role_assignment` rows
   using conflict-safe inserts and duplicate skipping.
4. Fill winner nulls only (COALESCE semantics) for top-level participant fields.
5. Delete loser row only after successful transfer and safety checks.

Remediation must support:
1. Dry-run mode (report only, no writes).
2. Real mode (transactional writes).
3. Counter/report output:
   - groups_examined
   - groups_merged
   - loser_rows_deleted
   - links_transferred
   - contacts_transferred
   - addresses_transferred
   - roles_transferred
   - conflicts_skipped
   - db_errors

### R4. Audit Logging
Each successful consolidation must write an audit entry into `resolution_manual_action_log` with:
1. `entity_type='participant'`
2. `action_type='merge'`
3. `reason_code='under_combination_consolidation'`
4. `actor` = pipeline/system actor string (for example, `pipeline_under_combination_fix`)
5. `source='pipeline'`

`candidate_entity_id` should reference the loser candidate id for traceability.

### R5. Safety Rules
The remediation process must skip and warn (not fail entire run) when:
1. Winner candidate cannot be selected deterministically.
2. Loser candidate is promoted or linked to canonical (unexpected state).
3. Transfer insert fails due to non-recoverable DB constraint issues.

## Implementation Plan

## I1. Source-to-Candidate Prevention (Mandatory)
File: `src/regatta_etl/resolution_source_to_candidate.py`

Implement helper(s):
1. `_find_email_bearing_candidate_by_name(conn, normalized_name) -> str | None | "ambiguous"`
2. `_resolve_participant_target_candidate(...)` used by participant ingestion functions before `_upsert_candidate`.

Integration points:
1. `_ingest_participants_from_participant_table`
2. `_ingest_participants_from_jotform`
3. `_ingest_participants_from_yacht_scoring`
4. Optional: any remaining participant ingestion path where `norm_email` can be null.

Behavior:
1. If incoming email is null and helper returns exactly one candidate id, use that candidate id path (update/link child rows/source links) instead of generating name-only fingerprint candidate.
2. Preserve existing code path when helper returns none or ambiguous.

## I2. Remediation Operation (Mandatory)
Preferred placement:
1. Add a dedicated function in `resolution_source_to_candidate.py` (or a sibling module) callable from CLI.
2. Add CLI wiring in `import_regattaman_csv.py` as either:
   - new mode, or
   - sub-operation under existing resolution lifecycle tooling.

Minimum required execution flow:
1. Build split groups (`normalized_name` with both null-email and non-null-email candidates).
2. Select winner + losers via deterministic ranking.
3. For each eligible loser:
   - transfer child/source rows to winner (conflict-safe),
   - fill winner nulls only,
   - write audit log,
   - delete loser.
4. Emit structured report and counters.

## I3. No Schema Migration Required
No new table is required for core fix.
If implementation adds a dedicated run/audit table, include a migration and tests, but this is optional.

## Test Requirements

### Unit Tests
Add tests for helper selection logic:
1. Name-only row + exactly one email-bearing candidate -> returns that candidate id.
2. Name-only row + zero email-bearing candidates -> returns none.
3. Name-only row + multiple email-bearing candidates -> returns ambiguous.
4. Winner selection sorting correctness for remediation.

### Integration Tests
File: `tests/integration/test_resolution_source_to_candidate.py`

Add new tests:
1. `test_name_only_after_name_email_reuses_existing_candidate`
   - Seed one participant source with email, then one source with same normalized name but no email.
   - Assert only one `candidate_participant` row exists for that name.
2. `test_name_only_ambiguous_email_bearing_candidates_does_not_guess`
   - Seed two existing candidates with same normalized name and non-null email.
   - Ingest name-only source row.
   - Assert no forced merge; warning count increases.
3. `test_under_combination_remediation_merges_existing_pair`
   - Seed winner/loser pair mirroring observed production pattern.
   - Run remediation in real mode.
   - Assert loser deleted, winner retained, links/contacts moved.
4. `test_under_combination_remediation_dry_run_no_writes`
   - Assert counters/report reflect planned merges, DB unchanged.

### Regression Assertions
1. Existing participant ingestion idempotency tests must still pass.
2. Source link uniqueness/idempotency behavior must remain stable.

## Acceptance Criteria
1. Running prevention logic on repeated source ingestion does not create new split pairs.
2. Existing split groups in current environment reduce from 29 to 0, except explicitly skipped ambiguous groups (if any), each with warning.
3. Post-fix query for split groups returns expected reduced count:

```sql
WITH c AS (
  SELECT normalized_name, best_email
  FROM candidate_participant
  WHERE normalized_name IS NOT NULL
)
SELECT COUNT(*) AS split_name_groups
FROM (
  SELECT normalized_name
  FROM c
  GROUP BY normalized_name
  HAVING BOOL_OR(best_email IS NULL) AND BOOL_OR(best_email IS NOT NULL)
) x;
```

4. Participant scoring improves (lower reject count and/or increased promotable candidates), with no DB errors.

## Operational Runbook (After Implementation)
1. Run remediation in dry-run mode and review counters/report.
2. Run remediation in real mode.
3. Run:
   - `resolution_source_to_candidate --entity-type participant`
   - `resolution_score --entity-type participant`
   - `resolution_promote --entity-type participant --dry-run`
4. Confirm split-name query and reject penalty distribution are materially improved.

