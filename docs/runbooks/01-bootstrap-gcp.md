# Runbook 01: Bootstrap GCP Environment

## Objective
Provision the minimum Google Cloud foundation for the orchestration platform.

## Prerequisites
- `gcloud` CLI authenticated to the target org.
- Billing-enabled GCP project.
- IAM permissions for Cloud SQL, GCS, Secret Manager, Cloud Run, Pub/Sub, Scheduler.

## Steps
1. Enable APIs
- `sqladmin.googleapis.com`
- `run.googleapis.com`
- `storage.googleapis.com`
- `secretmanager.googleapis.com`
- `pubsub.googleapis.com`
- `cloudscheduler.googleapis.com`

2. Create Cloud SQL instance (PostgreSQL)
- Private IP preferred.
- Automated backups enabled.
- Point-in-time recovery enabled.

3. Create Cloud SQL database and service account
- Create database (for example `regatta_data`).
- Create least-privilege service account for app access.

4. Create GCS buckets
- `raw-assets` bucket (scraped HTML, ingestion payloads).
- `exports` bucket (audience and action exports).
- Configure uniform bucket-level access.
- Configure lifecycle for `raw-assets` to retain 3 years (delete at 1095 days).

5. Configure secrets
- DB connection secret(s).
- External system API credentials.

6. Networking and IAM
- Restrict Cloud SQL and bucket access to service accounts only.
- Avoid broad project editor roles.

7. Policy jobs
- Schedule periodic job for PII lifecycle governance with 10-year retention policy.

## Validation
- App service account can connect to Cloud SQL.
- App service account can read/write expected GCS paths.
- Bucket lifecycle policy is active for raw artifacts.
- Audit logs enabled and visible.

## Rollback
- If provisioning is incorrect, delete newly created resources before data onboarding.
