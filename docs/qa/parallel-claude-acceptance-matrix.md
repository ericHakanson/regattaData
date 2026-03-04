# Parallel QA Acceptance Matrix

Use this as a pass/fail checklist with evidence links (run IDs, report files, SQL output).

## Session A: Club/Participant/Yacht Promotion Enablement

| ID | Requirement | Pass/Fail | Evidence |
|---|---|---|---|
| A-1 | `resolution_score --entity-type all` completes with `db_errors=0` |  |  |
| A-2 | No fatal transaction abort from state transition constraints |  |  |
| A-3 | Post-score: each of `club`, `participant`, `yacht` has `auto_promote > 0` (or documented policy decision if not) |  |  |
| A-4 | `resolution_promote --dry-run` completes with `db_errors=0` |  |  |
| A-5 | Promote dry-run shows expected promotable volume for target entities |  |  |
| A-6 | Real promote completes with `db_errors=0` |  |  |
| A-7 | Canonical row counts increase in `canonical_club`, `canonical_participant`, `canonical_yacht` |  |  |
| A-8 | Tests for transition-safety and promotion behavior pass |  |  |

## Session B: Mailchimp Event Registration Activation

| ID | Requirement | Pass/Fail | Evidence |
|---|---|---|---|
| B-1 | CLI mode `mailchimp_event_activation` exists and validates args |  |  |
| B-2 | Dry-run CSV activation completes without DB errors |  |  |
| B-3 | Real CSV activation run completes and writes output |  |  |
| B-4 | Output is deduped by normalized email |  |  |
| B-5 | Unsubscribed/cleaned contacts are suppressed |  |  |
| B-6 | Run/audit tables are populated (`mailchimp_activation_run`, `mailchimp_activation_row`) |  |  |
| B-7 | Activation report counters are present and coherent |  |  |
| B-8 | Activation tests pass (unit + integration) |  |  |

## Cross-Cutting

| ID | Requirement | Pass/Fail | Evidence |
|---|---|---|---|
| X-1 | No secrets introduced in tracked files |  |  |
| X-2 | `.gitignore` protects local secret files (`/env`, `.env*`) |  |  |
| X-3 | Existing pipelines unaffected (`yacht_scoring`, `resolution_source_to_candidate`) |  |  |
| X-4 | Run reports are archived under `artifacts/reports/` with referenced run IDs |  |  |

## Notes
1. Any failed row must include exact command + output snippet.
2. Any accepted deviation must include explicit approval rationale.
