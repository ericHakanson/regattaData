# BHYC Member Directory Scrape + Ingestion Requirements

## Purpose
Build a slow, respectful, authenticated pipeline that:

1. Crawls the BHYC online directory and member profile pages using authorized member credentials.
2. Archives downloaded HTML to GCS.
3. Extracts member data and ingests it into source/candidate/canonical layers with full lineage.

Target start URL:

`https://www.bhyc.net/Default.aspx?p=v35Directory&ssid=100073&vnf=1`

## Top Priority Constraint
Do not disrupt BHYC website operations.

The scraper must favor politeness and stability over speed.

## Scope
### In scope
1. Authenticated directory/profile crawl under the permitted member directory area.
2. Member profile HTML parsing for all visible fields.
3. Raw HTML archival to GCS with metadata.
4. Ingestion into source records, candidate linking, and canonical promotion/merge where confidence supports it.

### Out of scope
1. Bypassing access control or scraping non-member/private areas not visible to the authenticated member.
2. Aggressive parallel scraping.

## Functional Requirements
### FR-1: Authentication
1. Login must use runtime credentials from environment/secrets only.
2. No credentials in code, logs, reports, or committed files.
3. Session expiration must be detected and reported as `auth_failed`.
4. On auth failure, stop safely (no rapid login retry loops).

### FR-2: Crawl Discovery
1. Start from the directory page.
2. Discover member profile links (for example `default.aspx?p=MemProfile&id=...`).
3. De-duplicate discovered profile URLs by stable key (`member_id`).

### FR-3: Respectful Request Policy
1. Concurrency: exactly 1 in-flight request per host.
2. Base delay: minimum 10 seconds between requests.
3. Jitter: random +/- 3 seconds per request.
4. Maximum sustained rate: 6 requests/minute.
5. Backoff behavior:
   - `429`, `403`, `5xx`, transport errors: exponential backoff with cap.
   - Stop after 5 consecutive failures (`failed_safe_stop`).
6. Must support resumable checkpoints to avoid repeated full recrawls.
7. If Bright Data is used, enforce single-session low-rate mode only (no bursts, no multi-thread pools).

### FR-4: Raw HTML Archival (GCS)
1. Store every fetched directory/profile HTML page to GCS.
2. Object naming must be deterministic and run-scoped.
3. Persist metadata for each fetched object:
   - source URL
   - fetch timestamp
   - HTTP status
   - content hash
   - page type (`directory` or `member_profile`)
   - member id when present
   - run id
4. Preserve source references in DB (`raw_asset` + source linkage).

### FR-5: Profile Extraction
Parser must extract all available fields from profile tables, including:

1. Identity fields:
   - display name/full name/title/first/middle/last/suffix
2. Membership fields:
   - membership begins
   - membership type
3. Contact fields:
   - primary email
   - secondary email
   - phone numbers by labeled type
4. Household fields:
   - other members
   - relationship labels (spouse/child/etc.)
5. Address fields by section:
   - summer physical
   - summer mailing
   - winter address
   - winter mailing
   - year round address
6. Boat fields:
   - boat names and boat types

The parser must tolerate missing/blank sections without failing the row.

### FR-6: Ingestion and Resolution
1. Create/Upsert source records for BHYC profile rows with stable source keys.
2. Link each source row to candidate entities through `candidate_source_link`.
3. Promote/merge into canonical entities when confidence thresholds are met.
4. Preserve lineage and provenance for all canonical updates.
5. For ambiguous matches, route to `review`/`hold` instead of forcing canonical merges.

### FR-7: Idempotency
1. Re-running same crawl content must not create duplicates in source/candidate/canonical layers.
2. Fingerprint/source-key logic must be deterministic.
3. Incremental re-runs should only add changes/new rows.

### FR-8: Dry Run
1. `--dry-run` mode must perform crawl + archive + parse + reporting.
2. `--dry-run` must not commit entity-resolution writes.

## Non-Functional Requirements
### NFR-1: Reliability
1. Run must emit a structured report with counters and warning/error categories.
2. On recoverable row-level parse issues, continue run and record warnings.
3. On repeated transport/auth failures, stop safely with clear reason code.

### NFR-2: Security and Privacy
1. Redact PII from standard logs where feasible.
2. Enforce least privilege on GCS and DB credentials.
3. Follow project retention policy for raw HTML and extracted PII.

### NFR-3: Observability
Run report must include at minimum:

1. Pages discovered/fetched/archived/parsed/rejected.
2. Candidate created/enriched counts.
3. Canonical created/updated/merged counts.
4. Source links inserted/skipped.
5. Error buckets: `auth`, `network`, `parse`, `db`, `rate_limit`.
6. Safe stop reason when applicable.

## Acceptance Criteria
1. Full dry-run completes with no unhandled exceptions.
2. Real run completes with `db_errors=0` for clean test data.
3. Every fetched page has GCS archival record + DB source reference.
4. Coverage reconciliation is explainable:
   - discovered rows = linked rows + expected skipped rows + rejected rows.
5. Re-run idempotency validated (no duplicate source/candidate/canonical rows).
6. No high-rate or burst traffic behavior observed during run.

## CLI Expectations (Proposed)
Recommended new mode:

`--mode bhyc_member_directory`

Proposed options:

1. `--start-url`
2. `--member-user-env` / `--member-pass-env`
3. `--gcs-bucket`
4. `--gcs-prefix`
5. `--max-pages`
6. `--dry-run`
7. `--resume-from-checkpoint`
8. `--request-delay-seconds`
9. `--request-jitter-seconds`
10. `--max-consecutive-failures`

## Open Questions for Implementation
1. Canonical auto-promotion threshold for BHYC-sourced profiles (same as current participant rule set, or custom)?
2. Exact mapping of household/relationship fields into existing role/contact models.
3. Whether vCard endpoint (`GetVcard.aspx`) should be fetched or only profile HTML parsed.
4. Final retention period for BHYC raw HTML in GCS.

## Implementation Note for Claude
Treat this as a conservative operations pipeline, not a high-throughput scraper. The main success metric is respectful behavior and data integrity, not speed.

## Credential Handling Note
Credentials will be provided by the operator via local environment variables. Do not commit credential files (including `.env`) to source control.
