# Runbook 02: Schema Migrations and Historical Backfill

## Objective
Apply schema safely and load historical owners/registrations into the unified model.

## Migration Strategy
1. Baseline schema creation
- Create core tables and constraints.
- Add indexes for lookup paths (identity features, sail number, event/year, ownership date ranges).

2. Controlled rollout
- Apply migrations in non-production first.
- Verify migration checksum/version tracking.

3. Backfill phases
- Phase A: participants and contact points from all available sources.
- Phase B: yachts and ratings.
- Phase C: ownership history (`yacht_ownership`).
- Phase D: events (`event_series`, `event_instance`) and entries (`event_entry`) primarily from `regattaman.com`.
- Phase E: event roles (`event_entry_participant`) from `yachtscoring.com` and any available curated sources.
- Phase F: documentation status from `Jotform` into `document_status`.

4. Source precedence
- Registration and yacht-entry status: `regattaman.com` preferred source.
- Participant involvement enrichment: `yachtscoring.com` supplemental source.
- Waiver/document assertion: `Jotform` preferred source.

5. Reconciliation checks
- Row counts by entity vs source baseline.
- Duplicate detection report for participants, clubs, events, and yachts.
- Ownership overlap conflict report.
- Event entry uniqueness conflict report.
- Waiver status mismatch report.

## Data Quality Gates
- No null foreign keys on required relations.
- No active ownership rows with impossible date windows.
- No duplicate active registration for same yacht + event instance unless explicitly allowed.
- Outlier cases remain representable without forced normalization:
- owner present as non-participating contact,
- owner participating on different yacht entry in same event instance.

## Rollback
- Use point-in-time restore for Cloud SQL if irreversible corruption occurs.
- Keep source snapshots in GCS for deterministic re-runs.
