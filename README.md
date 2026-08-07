# Regatta Data Orchestration Platform

This repository defines the architecture and delivery standards for a sailboat regatta promotion orchestration layer.

## Platform Direction
- Primary database: **Neon** (serverless PostgreSQL). Connect via `DB_DSN` — see `.env.example` and `docs/runbooks/10-neon-operations.md`.
- Storage-heavy artifacts (scraped pages, raw files): `Google Cloud Storage (GCS)`.
- _Historical:_ the platform previously ran on `Cloud SQL for PostgreSQL`; migrated to Neon in the 2026-08 pivot. Cloud SQL docs below are retained as records and describe the read-only fallback during decommissioning.

## Documentation Index
- Requirements: `docs/requirements/product-requirements.md`
- Cloud SQL smoke wrapper requirements: `docs/requirements/cloud-sql-smoke-wrapper-requirements.md`
- Cloud SQL smoke wrapper implementation checklist: `docs/requirements/cloud-sql-smoke-wrapper-implementation-checklist.md`
- Open-question status and future revisit items: `docs/requirements/open-questions.md`
- Decision log: `docs/requirements/decisions-log.md`
- Target architecture and data model: `docs/architecture/target-architecture.md`
- Cloud SQL schema specification: `docs/architecture/cloud-sql-schema-spec.md`
- Implementation contract for Claude Code: `docs/architecture/implementation-contract.md`
- Architecture decision records:
  - `docs/decisions/adr-001-platform-gcp.md`
  - `docs/decisions/adr-002-person-owner-registration-model.md`
  - `docs/decisions/adr-003-source-systems-and-ingestion.md`
  - `docs/decisions/adr-004-entity-resolution-and-auto-merge.md`
- Runbooks:
  - `docs/runbooks/01-bootstrap-gcp.md`
  - `docs/runbooks/02-schema-migrations-and-backfill.md`
  - `docs/runbooks/03-operations-and-nba-pipeline.md`
  - `docs/runbooks/04-entity-resolution-operations.md`
  - `docs/runbooks/05-cloud-sql-migration-plan.md`
  - `docs/runbooks/06-post-migration-validation.md`
  - `docs/runbooks/07-materialized-view-refresh-strategy.md`
  - `docs/runbooks/10-neon-operations.md` (**current** primary-database runbook)
- QA and code review standard: `docs/qa/code-review-standard.md`
- Migration smoke tests: `migrations/tests/README.md`

## Scope Reminder
In scope: data orchestration and next-best-action targeting.

Out of scope: registration UI/forms, payment collection/processing, and direct email/SMS sending.
