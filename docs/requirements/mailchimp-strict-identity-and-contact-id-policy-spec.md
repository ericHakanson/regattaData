# Requirements: Mailchimp Strict Identity Matching and Participant Contact ID Linkage

## 1. Objective

Prevent incorrect person merges between Mailchimp and RegattaData by enforcing strict identity rules, even at the cost of higher manual-review volume.

Secondary objective:
Persist a durable Mailchimp contact identifier on the participant record model so downstream activation is traceable and conflict-safe.

## 2. Policy Decision

1. Email-only auto-linking is not acceptable for curated participant linkage.
2. When identity evidence is insufficient or conflicting, the system must quarantine for manual review instead of guessing.
3. Data quality risk is prioritized over automation throughput.

## 2.1 Export Schema Reality (Current Mailchimp CSVs)

Observed export schema includes `LEID` and `EUID` but does not include canonical Mailchimp contact/member ID.

Policy implications:
1. `LEID` and `EUID` must be captured for every row when present.
2. Missing canonical contact ID is expected and must not trigger fallback guessing.
3. Canonical contact ID must be backfilled through Mailchimp API identity lookup and then persisted.

## 3. Scope

In scope:
1. Mailchimp audience ingestion identity matching policy.
2. Participant-level Mailchimp identity persistence model.
3. Conflict handling and manual-review queue requirements.
4. Testing and acceptance criteria.
5. Malformed address field-shift handling.
6. Contact-ID acquisition when exports omit Mailchimp contact ID.

Out of scope:
1. Campaign copy/content.
2. Mailchimp UX design.
3. Non-Mailchimp external identity systems.

## 4. Strict Match Rules (Policy v2)

For a Mailchimp source row, curated linkage to an existing participant is allowed only when all applicable checks pass:

1. Primary key check:
   - normalized email exact match to exactly one participant contact-point email.

2. Name corroboration check:
   - if source has first/last name and target participant has name, normalized full-name must exactly match.
   - if either side lacks name data, row is not auto-linked and must be queued for manual review.

3. Optional corroboration conflict checks:
   - if source phone exists and target has phone, normalized phone mismatch is a hard conflict.
   - if source address exists and target has address, normalized address mismatch is a hard conflict.

4. Conflict behavior:
   - any hard conflict must not update curated participant/contact tables.
   - row is retained in raw capture and inserted into manual-review queue with machine-readable reason.

5. No-email behavior:
   - rows without valid normalized email cannot be auto-linked and must remain rejected/quarantined.

6. Address field-shift anomaly behavior:
   - detect likely field shifts (example: street+city+state stuffed into `City`, zip value in `State/Region`).
   - if parse confidence is high, normalize into structured parts and mark with a warning flag.
   - if parse confidence is low or conflicting, do not write curated address fields; queue row for manual review.
   - never use malformed/shifted address fields as primary identity evidence.

## 5. Required Data Model Additions

## 5.1 Participant Mailchimp Identity Link Table

Add table `participant_mailchimp_identity`:

1. `id` uuid PK.
2. `participant_id` uuid FK not null.
3. `mailchimp_contact_id` text null.
4. `leid` text null.
5. `euid` text null.
6. `subscriber_hash` text null.
7. `email_normalized` text not null.
8. `first_seen_at` timestamptz not null default now().
9. `last_seen_at` timestamptz not null default now().
10. `source_system` text not null.
11. `source_file_name` text null.
12. `is_active` bool not null default true.

Required constraints/indexes:

1. Unique on (`participant_id`, `email_normalized`).
2. Unique on `mailchimp_contact_id` when non-null.
3. Unique on `leid` when non-null.
4. Unique on `euid` when non-null.
5. Index on (`email_normalized`).

## 5.2 Manual Review Queue

Add table `mailchimp_identity_review_queue`:

1. `id` uuid PK.
2. `source_file_name` text not null.
3. `email_normalized` text null.
4. `candidate_participant_id` uuid null.
5. `reason_code` text not null.
6. `reason_detail` text null.
7. `raw_payload` jsonb not null.
8. `status` text not null default `open` (`open`,`resolved`,`dismissed`).
9. `created_at` timestamptz not null default now().
10. `resolved_at` timestamptz null.
11. `resolved_by` text null.

## 6. Write Policy

1. Raw capture remains mandatory for all rows.
2. Curated participant linkage runs only after strict checks pass.
3. `participant_mailchimp_identity` is upserted whenever:
   - ingestion row provides `LEID`/`EUID`.
   - activation API returns Mailchimp `id` or equivalent member identity.
4. Identity conflicts must not overwrite existing participant linkage.
5. Any identity conflict creates a review-queue row and increments reject/quarantine counters.
6. Curated address writes are blocked when `address_field_shift_suspected` is present and unresolved.

## 7. API Activation Policy Alignment

1. Activation API mode must prefer known `participant_mailchimp_identity` for target correlation when available.
2. If API response returns a Mailchimp contact ID not yet linked, it must be written to `participant_mailchimp_identity`.
3. If returned Mailchimp contact ID is already linked to a different participant, run must fail and queue conflict.
4. If contact ID is unavailable in source exports, activation/ingestion must rely on subscriber hash plus LEID/EUID until contact ID is backfilled.
5. Until strict identity is satisfied, activation may apply safe tags/segment markers only and must not overwrite profile PII fields (name, phone, address).

## 8. Mailchimp Contact ID Acquisition Policy

Exports may not contain Mailchimp contact/member ID even when LEID/EUID are present.

Required behavior:

1. Always persist `LEID`, `EUID`, and `subscriber_hash` when available.
2. Treat `LEID`/`EUID` as auxiliary identifiers, not guaranteed substitutes for Mailchimp contact ID.
3. Add an API-assisted backfill path to populate `mailchimp_contact_id` into `participant_mailchimp_identity` using:
   - list/audience ID
   - `subscriber_hash` derived from normalized email
4. If backfill lookup returns no member, keep `mailchimp_contact_id` null and record warning/retry metadata; do not auto-link to another participant.
5. If backfill returns a contact ID already linked to a different participant, queue conflict and fail closed.

## 9. Reason Codes (Minimum Set)

1. `ambiguous_email_match`
2. `email_name_mismatch`
3. `email_phone_mismatch`
4. `email_address_mismatch`
5. `missing_name_for_email_match`
6. `mailchimp_contact_id_conflict`
7. `leid_conflict`
8. `euid_conflict`
9. `address_field_shift_suspected`
10. `mailchimp_contact_id_missing`
11. `insufficient_identity_for_profile_write`

## 10. Counters and Reporting

Add counters:

1. `mailchimp_identity_rows_quarantined`
2. `mailchimp_identity_conflicts`
3. `mailchimp_identity_links_inserted`
4. `mailchimp_identity_links_updated`
5. `mailchimp_contact_id_conflicts`
6. `mailchimp_address_shift_rows`
7. `mailchimp_contact_id_backfill_attempted`
8. `mailchimp_contact_id_backfill_succeeded`
9. `mailchimp_contact_id_backfill_missing`

Run reports must include these fields for both ingestion and activation modes where relevant.

## 11. Testing Requirements

Unit tests:

1. Email+name exact match accepted.
2. Email match with name mismatch quarantined.
3. Email match with missing name quarantined.
4. Phone/address mismatch conflict behavior.
5. Contact ID upsert and conflict detection.
6. Address field-shift detection and quarantine path.
7. High-confidence address field-shift normalization path.

Integration tests:

1. End-to-end ingestion with strict mode and review queue insertion.
2. No curated writes on conflicted rows.
3. Participant Mailchimp identity link persistence from LEID/EUID.
4. API response contact ID linkage and cross-participant conflict fail path.
5. Contact-ID backfill path from subscriber hash when contact ID is absent in CSV export.

## 12. Acceptance Criteria

1. No curated participant auto-link occurs on email-only evidence.
2. All conflicted rows are recoverable from raw capture and visible in review queue.
3. Participant-level Mailchimp identity linkage exists and is conflict-safe.
4. Activation runs fail closed on identity conflicts.
5. QA proves strict policy through passing unit/integration tests.
6. Malformed address field-shift rows do not silently corrupt curated addresses.
7. Contact-ID backfill policy handles missing IDs deterministically and audibly.

## 13. Claude Implementation Checklist

### 13.1 Schema

- [ ] Add migration for:
  - `participant_mailchimp_identity`
  - `mailchimp_identity_review_queue`
- [ ] Add unique/index constraints exactly as specified in Section 5.
- [ ] Ensure migration is idempotent and reversible per project standards.

### 13.2 Ingestion Logic (`mailchimp_audience`)

- [ ] Add strict identity decision gate:
  - email unique match required
  - name corroboration required when names exist
  - no-name rows do not auto-link; queue for review
- [ ] Add hard conflict checks for phone/address mismatch when both sides have values.
- [ ] Add address field-shift detector and confidence scoring.
- [ ] Block curated address writes when shift/conflict confidence is unsafe.
- [ ] Route conflicts to `mailchimp_identity_review_queue` with reason codes.
- [ ] Keep raw capture and reject behavior intact.
- [ ] Upsert `participant_mailchimp_identity` with `LEID`/`EUID`/email context.

### 13.3 Activation Logic (`mailchimp_event_activation`)

- [ ] On API success, persist Mailchimp contact ID (and subscriber hash) into `participant_mailchimp_identity`.
- [ ] Fail closed on participant-contact-id collisions and queue conflicts.
- [ ] Ensure run status/counters reflect identity conflicts as failures.
- [ ] Add contact-ID backfill step (subscriber-hash lookup) when `mailchimp_contact_id` is missing.

### 13.4 Counters and Reporting

- [ ] Add required counters from Section 10 to shared run counters.
- [ ] Emit counters in run report JSON and console report.

### 13.5 Tests

- [ ] Add unit tests for strict identity decision outcomes and reason codes.
- [ ] Add integration tests for queue insertion and no curated writes on conflict.
- [ ] Add integration tests for participant Mailchimp ID upsert and conflict fail path.
- [ ] Add tests for address field-shift detection + quarantine behavior.
- [ ] Add tests for contact-ID backfill success/missing/conflict paths.
- [ ] Preserve passing state of existing Mailchimp ingestion/activation suites.

### 13.6 Rollout

- [ ] Dry-run ingestion on production-like snapshot and review queued conflicts.
- [ ] Approve conflict handling thresholds and manual-review process.
- [ ] Run production ingestion only after reviewer sign-off.

## 14. Migration Stub

Initial SQL stub for implementation:

- `migrations/0020_mailchimp_identity_policy_tables.sql`

Expected objects in that migration:

1. `participant_mailchimp_identity`
2. `mailchimp_identity_review_queue`
3. Required unique/index constraints from Section 5.
