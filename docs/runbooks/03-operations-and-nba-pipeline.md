# Runbook 03: Operations and Next-Best-Action Pipeline

## Objective
Operate daily ingestion and recommendation generation with observability and recovery paths.

## Daily Flow
1. Pull source snapshots from `regattaman.com`, `Jotform`, and scraping jobs.
2. Store raw payloads and pages in GCS with provenance metadata.
3. Normalize and upsert into Cloud SQL.
4. Execute entity resolution and persist candidate/merge outputs.
5. Run data quality checks and conflict reports.
6. Run NBA rules and generate prioritized actions.
7. Write action/audience exports as versioned CSV + JSON files to GCS.
8. Publish export manifest for downstream Google Workspace consumers (Sheets/Docs/Slides workflows).

## Operational Checks
- Ingestion success rate and latency by source.
- Freshness of key entities (clubs, events, participants, entries).
- Count of unresolved identity conflicts by entity type.
- Count of missing-document actions by event.
- Drift in auto-merge rate and manual-review queue size.
- Export completeness by schema version and file count.

## Incident Handling
1. Pipeline failure
- Inspect Cloud Run logs and job execution IDs.
- Re-run failed step idempotently using same input snapshot.

2. Bad data import
- Quarantine offending source payload.
- Re-run normalization after source mapping fix.

3. Entity-resolution quality regression
- Compare match score distribution and auto-merge precision to prior runs.
- Roll back threshold config or feature weights.

4. Recommendation quality regression
- Compare action volume and composition to prior runs.
- Roll back rule-set version if false positive rate spikes.

5. Export contract regression
- Validate generated CSV/JSON headers against current schema version.
- Re-run export for affected batch after schema fix.

## Retention Operations
- Apply raw artifact lifecycle for 3-year retention in GCS.
- Run periodic PII policy jobs to enforce 10-year retention lifecycle.

## Change Management
- Version matcher features, thresholds, rule definitions, and export schema.
- Announce breaking changes to downstream consumers before deployment.
