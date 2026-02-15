# Runbook 05: Cloud SQL Migration Plan (Initial Build)

## Objective
Create and deploy the initial production schema safely with deterministic rollback points.

## Migration Phases
1. `0001_extensions.sql`
- Enable required extensions (`pgcrypto`, optional `citext`).
- Create shared utility functions (`set_updated_at`).

2. `0002_core_entities.sql`
- Create `yacht_club`, `participant`, `yacht`, `event_series`, `event_instance`.
- Add foundational indexes and unique constraints.

3. `0003_relationships.sql`
- Create `participant_contact_point`, `participant_address`, `club_membership`, `yacht_ownership`, `yacht_rating`.
- Add temporal check constraints.

4. `0004_event_ops.sql`
- Create `event_entry`, `event_entry_participant`, `document_type`, `document_requirement`, `document_status`.

5. `0005_resolution_and_actions.sql`
- Create `identity_candidate_match`, `identity_merge_action`, `next_best_action`, `raw_asset`.

6. `0006_views.sql`
- Create materialized views for NBA and resolution review queue.

## Backfill Alignment
- Load order should follow phase dependencies.
- Registration truth from `regattaman.com` should populate `event_entry` first.
- Supplemental crew/participant mappings from `yachtscoring.com` enrich `event_entry_participant`.
- Waiver status from `Jotform` populates `document_status`.

## Release Procedure
1. Apply migrations to dev Cloud SQL instance.
2. Run schema validation checks (constraints, indexes, triggers present).
3. Run migration smoke suite (`migrations/tests/run_smoke.sh`) against a fresh validation DB.
4. Execute backfill dry-run and reconciliation report.
5. Promote same migration bundle to staging.
6. Execute load/perf smoke tests.
7. Promote to production during low-activity window.

## Rollback Strategy
- For migration errors before data load: roll forward with corrective migration.
- For data corruption during load: restore via Cloud SQL PITR to pre-load timestamp.
- Keep raw source snapshots in GCS for reproducible re-ingestion.

## Validation Checklist
- All tables in `docs/architecture/cloud-sql-schema-spec.md` exist.
- Unique constraints block duplicate event entries per yacht/event instance.
- Temporal constraints reject invalid date ranges.
- Owner-contact outlier scenarios are insertable and queryable.
- Raw retention marker is set to 3 years for new assets.
- PII policy jobs are scheduled for 10-year lifecycle controls.
