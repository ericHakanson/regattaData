# QA Code Review Standard (For Claude Code Contributions)

## Purpose
Provide a consistent review standard focused on correctness, data integrity, and operability.

## Review Priorities
1. Data correctness and temporal integrity.
2. Behavioral regressions against requirements.
3. Security and PII handling.
4. Reliability, idempotency, and observability.
5. Test quality and coverage for changed behavior.

## Severity Levels
- `S0 Critical`: data loss/corruption risk, security exposure, or production outage risk.
- `S1 High`: incorrect business outcome likely (wrong targeting, wrong status).
- `S2 Medium`: maintainability/performance issue with moderate operational risk.
- `S3 Low`: minor clarity or style issue.

## Required Checks Per PR
- Requirements traceability: changed code maps to documented requirement(s).
- Schema safety: migrations are forward-safe; constraints/indexes are appropriate.
- Idempotency: ingestion/retry paths do not duplicate records.
- Temporal logic: effective dates and status transitions are valid.
- Outlier support: owner-contact and participant roles can diverge without data loss.
- Access control: secrets and credentials are not hardcoded; IAM assumptions are least privilege.
- Logging: meaningful structured logs for failures and decision outputs.
- Tests: unit/integration tests cover rules, entity resolution thresholds, and edge cases.

## Review Output Format
Use this format for each review:

1. Findings (ordered by severity)
- `[Severity] file_path:line - issue summary`
- `Risk:`
- `Recommendation:`

2. Open questions/assumptions
- Any ambiguity that blocks approval.

3. Decision
- `Approved`, `Approved with conditions`, or `Changes required`.

## Domain-Specific QA Cases
- Co-owner changes mid-season do not rewrite historical ownership.
- Same event name across years resolves to one series with multiple instances.
- Participant merged records preserve historical entry links.
- Missing waiver action only triggers for entities actually subject to requirement.
- Auto-merged records preserve reversible lineage for unmerge.
- Export schema remains stable or versioned when changed.
