# Runbook 04: Entity Resolution Operations

## Objective
Operate, monitor, and tune match/merge quality for participants, clubs, events, and yachts.

## Pipeline Stages
1. Candidate generation
- Build candidate pairs using deterministic keys (normalized email/phone/name/sail number/event keys).

2. Feature scoring
- Compute weighted feature vector and confidence score.

3. Decisioning
- Auto-merge if score is above configured threshold.
- Route to manual review if score is in review band.
- Reject if below review band.

4. Merge recording
- Persist source IDs, target canonical ID, features, score, and decision timestamp.

## Monitoring
- Auto-merge volume and percentage by entity type.
- Manual review queue age and size.
- Post-merge correction/unmerge rate.
- Precision from periodic sampled audits.

## Recovery
1. Bad merge detected
- Execute unmerge procedure.
- Rebuild impacted denormalized views/materialized tables.

2. Threshold issue detected
- Reduce auto-merge threshold usage by entity or disable auto-merge temporarily.
- Re-route uncertain records to manual review.

## Governance
- Threshold changes require change note and effective date.
- Keep immutable history of merge and unmerge actions.
