# Runbook 07: Materialized View Refresh Strategy

## Objective
Define safe, repeatable refresh operations for materialized views during ingestion and recommendation cycles.

## Scope
- `mv_missing_required_documents`
- `mv_probable_unregistered_returners`
- `mv_entity_resolution_review_queue`

## Strategy
1. Prefer `REFRESH MATERIALIZED VIEW CONCURRENTLY` in operational jobs.
2. Use non-concurrent refresh only for initial bootstrap or maintenance windows.
3. Run refresh after ingestion + normalization commits complete for the batch.

## Preconditions for Concurrent Refresh
1. View has at least one valid unique index that covers all rows.
2. Refresh is executed outside an explicit transaction block.
3. Sufficient temp space and maintenance window available.

## Recommended Execution Order
1. `mv_entity_resolution_review_queue`
2. `mv_probable_unregistered_returners`
3. `mv_missing_required_documents`

Rationale: cheaper views first to surface upstream data issues before refreshing the heaviest view.

## Example Commands
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entity_resolution_review_queue;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_probable_unregistered_returners;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_missing_required_documents;
```

## Scheduling Guidance
1. Daily full refresh after ingestion batch completion.
2. Optional intra-day refresh for active event periods.
3. Skip refresh if source ingestion for required feeds failed.

## Locking and Failure Behavior
1. If concurrent refresh fails:
- log failure with batch/run id
- retry with bounded backoff
- alert if retry budget exceeded

2. If non-concurrent refresh is required:
- run in maintenance window
- pause writer-heavy jobs if needed

3. Never run multiple refreshes for the same MV in parallel.

## Observability
Track per refresh job:
- start/end time
- duration
- rows in MV after refresh
- query ID / run ID
- error class and retry count

## Operational Guardrails
1. Add timeout to refresh jobs.
2. Add circuit breaker after repeated failures.
3. Emit health status used by export/NBA jobs.

## Rollback/Recovery
1. If refresh repeatedly fails, continue serving last successful MV snapshot.
2. Disable dependent export if snapshot staleness exceeds threshold.
3. Rebuild with non-concurrent refresh in maintenance window if indexes are suspected invalid.
