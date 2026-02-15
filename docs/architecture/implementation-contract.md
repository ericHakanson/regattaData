# Implementation Contract for Claude Code

## Objective
Translate architecture into code without re-deciding core data-model and platform choices.

## Non-Negotiables
- Use `Cloud SQL for PostgreSQL` as primary relational store.
- Use `GCS` for raw/storage-heavy assets.
- Do not implement payment, registration UI/forms, or direct email/SMS sending.
- Preserve historical state with effective dating for ownership, memberships, and participation.

## Source Integrations (Initial)
- `regattaman.com` for yacht-entry registration state.
- `Jotform` for participant waiver/document status.
- Scraped data from `regattaman.com` and `yachtscoring.com`.

## Policy Baselines (Confirmed)
- Auto-merge thresholds:
- participants `>= 0.95`
- yachts `>= 0.97`
- clubs `>= 0.98`
- events `>= 0.98`
- Retention:
- raw artifacts 3 years
- PII 10 years
- Exports:
- versioned CSV + JSON
- Google Workspace flows (Sheets/Docs/Slides) are initial consumer target.

## Required Initial Deliverables
1. Migration set for core schema in `docs/architecture/target-architecture.md` and `docs/architecture/cloud-sql-schema-spec.md`.
2. Seed/test fixtures for recurring events and ownership changes over time.
3. Ingestion skeleton with idempotent upsert pattern.
4. Entity resolution pipeline with deterministic + scored matching and auto-merge threshold config.
5. NBA rule execution job with at least 3 baseline rules.
6. Export job writing versioned audience/action files to GCS.
7. Automated tests covering temporal and outlier role edge cases.

## Definition of Done (Initial Release)
- Migrations apply cleanly from empty database.
- Backfill dry-run produces reconciliation report.
- Entity resolution output includes score, matched features, and merge decision.
- NBA output includes `reason_code` and `priority_score`.
- Export manifest supports Google Workspace consumption.
- Failures are observable through structured logs.
- QA checks in `docs/qa/code-review-standard.md` pass.
