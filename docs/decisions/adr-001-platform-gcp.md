# ADR-001: Platform Standardization on Google Cloud

## Status
Accepted

## Date
2026-02-15

## Context
The required target platform is Google Cloud services, specifically Cloud SQL and GCS.

## Decision
Use Google Cloud services as primary platform:
- Cloud SQL for PostgreSQL as primary relational data store.
- Google Cloud Storage for storage-heavy and raw assets.
- Cloud Run for service runtime.
- Cloud Scheduler and Pub/Sub for orchestration.

## Consequences
- All migrations, operational tooling, and deployment runbooks target GCP services directly.
