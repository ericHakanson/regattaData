# Technical Spec: Enable Canonical Promotion For Club, Participant, Yacht

## 1. Objective
Enable safe, repeatable promotion of `club`, `participant`, and `yacht` candidates to canonical records, in addition to the currently promoted `event` and `registration` entities.

This spec is focused on promotion readiness and operational safety, not on introducing new upstream source adapters.

## 2. Current Baseline (As Observed)
Current candidate state distribution shows no promoted `club`, `participant`, or `yacht`:

1. `club`: all `hold`, `is_promoted=false`.
2. `participant`: only `review/hold/reject`, `is_promoted=false`.
3. `yacht`: only `review/reject`, `is_promoted=false`.
4. `event` and `registration` are already promoted.

Operational impact:
1. Canonical layers for clubs, participants, yachts remain empty.
2. Downstream features relying on those canonical entities cannot progress.

## 3. Problem Statement
Promotion blockage for these three entity types is primarily policy/scoring driven, plus state-machine constraints:

1. Thresholds and penalties route almost all candidates below `auto_promote`.
2. DB trigger `enforce_candidate_state_transition()` disallows direct `reject -> auto_promote`.
3. Manual threshold swings can cause transaction-abort behavior when scoring attempts forbidden transitions.

## 4. Scope
In scope:
1. Scoring logic hardening so forbidden transitions do not abort runs.
2. Rule/YAML policy updates for `club`, `participant`, `yacht`.
3. Promotion-readiness diagnostics to explain what blocks `auto_promote`.
4. Integration tests for staged transition and end-to-end scoring/promotion.

Out of scope:
1. New source ingestion pipelines.
2. Canonical schema redesign.
3. UI changes.

## 5. Required Outcomes
1. Non-zero `auto_promote` candidates for each of `club`, `participant`, `yacht`.
2. Non-zero canonical rows for each of `canonical_club`, `canonical_participant`, `canonical_yacht` after promote run.
3. No run-level abort due to `reject -> auto_promote` transition rule.
4. Dry-run and non-dry-run outputs remain deterministic and auditable.

## 6. Functional Requirements

### 6.1 State-Machine-Safe Scoring Writes
`resolution_score` must never fail a full transaction due to a forbidden direct transition.

Required behavior when target state is `auto_promote` and current state is `reject`:
1. Stage candidate through allowed transition first (`reject -> review`), then `review -> auto_promote`.
2. Both updates must happen in-row savepoint isolation.
3. If staged update fails for a row, capture warning + increment `db_errors` for that row only; continue.

Alternative acceptable behavior:
1. Cap target to `review` with an explicit reason code, then require a subsequent scoring pass to reach `auto_promote`.
2. Must still avoid transaction abort.

### 6.2 Rule Policy Updates (YAML)
Provide updated policy versions for:
1. `config/resolution_rules/club.yml`
2. `config/resolution_rules/participant.yml`
3. `config/resolution_rules/yacht.yml`

Requirements for these updates:
1. Version bump in each file.
2. Comments documenting rationale and risk tradeoffs.
3. Keep thresholds ordered (`hold <= review <= auto_promote`).
4. Preserve hard-block semantics unless explicitly changed with justification.

### 6.3 Promotion Readiness Diagnostics
Add a diagnostics output (CLI mode or helper query doc) that reports for each entity:
1. Current state counts.
2. Score distribution (min, p25, p50, p75, max).
3. Top `confidence_reasons` frequencies.
4. Count of rows that would newly enter `auto_promote` under current rules.

Must be runnable before and after policy changes.

### 6.4 Report Counters
Extend scoring/promotion report counters with:
1. `state_transitions_staged` (reject->review->auto_promote staged rows).
2. `state_transitions_capped` (rows capped to review for safety path, if used).
3. `new_auto_promote_club`, `new_auto_promote_participant`, `new_auto_promote_yacht`.

## 7. Safety Constraints
1. Do not disable or loosen DB state-machine trigger globally.
2. Do not bypass constraints with direct SQL hacks outside controlled pipeline logic.
3. Any policy that increases promotion must remain explainable via `confidence_reasons`.
4. Promote step remains dry-run validated before any non-dry execution.

## 8. Testing Requirements

### 8.1 Unit Tests
1. Scoring update path handles `reject -> auto_promote` without transaction-abort.
2. Transition staging logic behavior per entity table.
3. New report counters are populated correctly.

### 8.2 Integration Tests
1. Seed candidates in `reject` that should become `auto_promote` under new rules.
2. Run `resolution_score` non-dry and verify no fatal abort.
3. Verify expected staged/capped behavior and final states.
4. Run `resolution_promote --dry-run` then non-dry and assert canonical inserts for all three entities.

### 8.3 Regression
1. Existing scoring and promotion tests still pass.
2. Existing event/registration promotion behavior unchanged.

## 9. Acceptance Criteria
A change set is complete only when all are true:

1. `resolution_score --entity-type all` succeeds with `db_errors=0`.
2. Post-score, each of `club`, `participant`, `yacht` has `auto_promote > 0`.
3. `resolution_promote --entity-type all --dry-run` shows promotable rows for those entities and `db_errors=0`.
4. Non-dry promote creates rows in:
   - `canonical_club`
   - `canonical_participant`
   - `canonical_yacht`
5. No trigger-related transaction abort occurs in scoring runs.

## 10. Rollout Plan
1. Implement scoring write hardening first.
2. Add diagnostics and tests.
3. Apply YAML policy updates.
4. Run dry-run score + promote.
5. Run non-dry score + promote.
6. Capture before/after metrics and publish run report IDs.

## 11. Execution Checklist (For Claude)
- [ ] Implement state-machine-safe scoring write logic.
- [ ] Add counters and report output for staged/capped transitions.
- [ ] Update club/participant/yacht YAML policies with version bumps.
- [ ] Add unit + integration tests for transition safety and promotion outcomes.
- [ ] Run full test suite; document any pre-existing failures.
- [ ] Provide exact run commands and expected verification SQL.

## 12. Verification Commands
```bash
.venv/bin/regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type all
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type all --dry-run
.venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type all
```

```sql
SELECT entity_type, resolution_state, is_promoted, COUNT(*) AS n
FROM (
  SELECT 'club' AS entity_type, resolution_state, is_promoted FROM candidate_club
  UNION ALL
  SELECT 'participant', resolution_state, is_promoted FROM candidate_participant
  UNION ALL
  SELECT 'yacht', resolution_state, is_promoted FROM candidate_yacht
) s
GROUP BY 1,2,3
ORDER BY 1,2,3;
```

```sql
SELECT
  (SELECT COUNT(*) FROM canonical_club) AS canonical_club,
  (SELECT COUNT(*) FROM canonical_participant) AS canonical_participant,
  (SELECT COUNT(*) FROM canonical_yacht) AS canonical_yacht;
```
