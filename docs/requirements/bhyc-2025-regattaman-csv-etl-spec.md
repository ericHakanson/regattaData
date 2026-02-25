# ETL Specification: BHYC 2025 Regatta CSV Import

## Status
- Draft for implementation by Claude.
- Repository currently has schema + runbooks, but no executable Python ingestion pipeline.

## Source
- File: `rawEvidence/export2025bhycRegatta - export.csv`
- Format: CSV with quoted values.
- Observed profile:
1. 65 rows, 27 columns.
2. Headers contain trailing spaces in multiple fields (for example `Name `, `Email `, `LOA `).
3. Sentinel timestamps `0000-00-00 00:00:00` appear in `date_paid` and `date_updated`.
4. `oid` is not unique (same owner can register multiple boats).
5. One row has blank `Yacht Name` and cannot produce an `event_entry`.

## Goal
Load this file into Cloud SQL PostgreSQL tables for:
1. Owners (`participant`, contact, address, membership, ownership links).
2. Yachts (`yacht`).
3. One event registration set (`event_series`, `event_instance`, `event_entry`, `event_entry_participant`).

## Out Of Scope
1. Entity-resolution scoring and auto-merge tables.
2. Waiver/document status ingestion.
3. NBA generation.

## Target Tables
- `yacht_club`
- `event_series`
- `event_instance`
- `participant`
- `participant_contact_point`
- `participant_address`
- `club_membership`
- `yacht`
- `yacht_ownership`
- `event_entry`
- `event_entry_participant`
- `raw_asset` (optional but recommended)

## Required CLI Contract
Implement a single command entrypoint, example:

```bash
python -m regatta_etl.import_regattaman_csv \
  --db-dsn "$DB_DSN" \
  --csv-path "rawEvidence/export2025bhycRegatta - export.csv" \
  --host-club-name "Boothbay Harbor Yacht Club" \
  --host-club-normalized "boothbay-harbor-yacht-club" \
  --event-series-name "BHYC Regatta" \
  --event-series-normalized "bhyc-regatta" \
  --event-display-name "BHYC Regatta 2025" \
  --season-year 2025 \
  --registration-source "regattaman" \
  --source-system "regattaman_csv_export" \
  --asset-type "regattaman_csv_export"
```

### Optional CLI Flags
- `--dry-run` (no writes, full validation/report).
- `--gcs-bucket` and `--gcs-object` (if set, write `raw_asset` record).
- `--rejects-path` (default `./artifacts/rejects/bhyc_2025_rejects.csv`).
- `--run-id` (override generated UUID for log correlation).

## Header Handling
The parser must trim whitespace from headers before lookup.

Expected normalized header names:
- `oid`
- `bid`
- `date_entered`
- `cid`
- `sku`
- `parent_sku`
- `date_paid`
- `date_updated`
- `Paid Type`
- `Discounts`
- `ownername`
- `Name`
- `owner_address`
- `City`
- `owner_state`
- `ccode`
- `owner_zip`
- `owner_hphone`
- `owner_cphone`
- `Email`
- `org_name`
- `Club`
- `org_abbrev`
- `Yacht Name`
- `Boat  Type`
- `LOA`
- `Moor Depth`

## Normalization Rules
1. `trim(value)`: strip leading/trailing whitespace; treat empty string as null.
2. `normalize_space(value)`: collapse internal runs of whitespace to single spaces.
3. `normalize_email(value)`: lowercase + trim.
4. `normalize_phone(value)`: keep digits only; if 10 digits, prefix with `+1`; if 11 digits starting with `1`, prefix `+`.
5. `normalize_name(value)` for lookup: lowercase, remove punctuation except spaces, collapse spaces.
6. `slug_name(value)` for `normalized_name` fields in clubs/events/yachts: lowercase alnum with `-` separators.
7. `parse_ts(value)`: parse `%Y-%m-%d %H:%M:%S`; sentinel `0000-00-00 00:00:00` => null.
8. `parse_date_from_ts(value)`: date part from parsed timestamp.
9. `parse_numeric(value)`: decimal or null.

## Name Parsing Rules For Multi-Owner Rows
Use `ownername` as the primary owner source of truth.

For additional co-owners:
1. Parse `Name` by splitting on `&`.
2. Also split on case-insensitive ` and `.
3. Trim each token; remove duplicates by normalized name.
4. Ensure primary `ownername` is included even if absent from `Name`.

Primary owner maps to `role='owner'`, others to `role='co_owner'`.

## Event Context Rules
This file is for one event instance.

1. Upsert host club from CLI:
   - `yacht_club.name = --host-club-name`
   - `yacht_club.normalized_name = --host-club-normalized`
   - `vitality_status = 'active'`
2. Upsert event series:
   - keyed by `(yacht_club_id, event_series.normalized_name)`
3. Upsert event instance:
   - keyed by `(event_series_id, season_year)`
   - `display_name = --event-display-name`
   - `registration_open_at = min(date_entered)` across valid rows if null in DB.

## Source-To-Target Mapping
### Participant
- `participant.full_name` <- cleaned `ownername`
- `participant.normalized_full_name` <- `normalize_name(ownername)`
- `participant.first_name`, `participant.last_name` <- parsed from `ownername`:
  - if comma format `Last, First`, split on first comma
  - else fallback: first token = first_name, remainder = last_name

Resolution priority:
1. Match existing participant by normalized email via `participant_contact_point`.
2. Else match by normalized phone via `participant_contact_point`.
3. Else match by `participant.normalized_full_name`.
4. Else insert new participant.

### Participant Contact Points
- Email:
  - `contact_type='email'`
  - `contact_subtype='primary'`
  - `contact_value_raw` <- `Email`
  - `contact_value_normalized` <- normalized email
  - `is_primary=true`
- Home phone from `owner_hphone`:
  - `contact_type='phone'`
  - `contact_subtype='home'`
- Mobile phone from `owner_cphone`:
  - `contact_type='phone'`
  - `contact_subtype='mobile'`

Insert only when value is non-null. Do not duplicate an existing same `(participant_id, contact_type, contact_subtype, contact_value_normalized)`.

### Participant Address
- Create one `participant_address` with:
  - `address_type='mailing'`
  - `line1` <- `owner_address`
  - `city` <- `City`
  - `state` <- `owner_state`
  - `postal_code` <- `owner_zip`
  - `country_code` <- `ccode`
  - `address_raw` <- concatenated raw components
  - `is_primary=true`
- Insert only if at least one address component is non-null.

### Club + Membership
- Participant affiliation club from `org_name` (not host event club).
- Skip placeholder values: null, `-`, `none/other` (case-insensitive).
- Upsert `yacht_club` by normalized name, `vitality_status='unknown'` unless already set.
- Upsert `club_membership` for participant and club:
  - `membership_role='member'`
  - `effective_start = date(date_entered)` when available
  - `effective_end = null`
  - no duplicate active membership rows.

### Yacht
- `yacht.name` <- `Yacht Name` (required for entry creation)
- `yacht.normalized_name` <- slug of yacht name
- `yacht.model` <- `Boat  Type`
- `yacht.length_feet` <- parsed `LOA`
- `sail_number` and `normalized_sail_number` remain null (not present in file)

Resolve existing yacht by `(normalized_name, length_feet)` when length is present, else by `normalized_name`.

### Yacht Ownership
For each parsed owner from row:
- `participant_id` from owner resolution
- `yacht_id` from yacht resolution
- `role`:
  - primary owner (`ownername`) => `owner`
  - additional owners => `co_owner`
- `is_primary_contact` true for primary owner, else false
- `effective_start = date(date_entered)` if available, else import date
- `effective_end = null`
- `ownership_pct = null`

Do not insert duplicate active ownership rows for same `(participant_id, yacht_id, role)`.

### Event Entry
- `event_instance_id` <- resolved event instance
- `yacht_id` <- resolved yacht
- `registration_source` <- `--registration-source`
- `registration_external_id` <- `sku` (fallback `parent_sku`)
- `registered_at` <- parsed `date_entered`
- `entry_status` derivation:
  1. if valid `date_paid` exists => `confirmed`
  2. else if `Paid Type` equals `free` => `confirmed`
  3. else if valid `date_entered` exists => `submitted`
  4. else => `unknown`

Use `INSERT ... ON CONFLICT (event_instance_id, yacht_id) DO UPDATE` and keep:
1. `registered_at = LEAST(existing.registered_at, excluded.registered_at)` when both non-null.
2. `entry_status` as max precedence: `confirmed > submitted > draft > unknown`.
3. non-null `registration_external_id`.

### Event Entry Participant
For each owner participant on the row:
- insert `event_entry_participant`:
  - `role='owner_contact'`
  - `participation_state='non_participating_contact'`
  - `source_system=--source-system`

Optional extension (not required in v1): also insert primary owner as `registrant`.

### Raw Asset (Optional, Recommended)
If `--gcs-bucket` and `--gcs-object` are provided:
- compute SHA-256 of local CSV as `content_hash`
- insert/upsert into `raw_asset`:
  - `source_system=--source-system`
  - `asset_type=--asset-type`
  - `gcs_bucket`, `gcs_object`
  - `captured_at = now()`
  - `retention_delete_after = current_date + interval '3 years'`

## Row Validation And Reject Rules
Reject row (write to rejects CSV with reason) when:
1. `sku` is blank.
2. `ownername` is blank.
3. `Yacht Name` is blank.
4. `ambiguous_contact_match` — In a real run, if a normalized email or phone
   lookup returns multiple existing participant matches, reject the row for
   manual review rather than binding arbitrarily to one match.  Write reason
   `ambiguous_contact_match` to the rejects file and continue the run.

Do not fail the entire run for row-level rejects.

Fail the run when:
1. Header set is missing required columns after trim.
2. DB connectivity/transaction errors occur.
3. Event context upsert fails.

## Transaction And Idempotency
1. Use one DB transaction per input row for isolation and retry safety.
2. Wrap event context upsert in a separate transaction executed once per run.
3. Safe re-run requirement:
   - re-running same file must not create duplicate `event_entry` rows.
   - duplicate contacts/addresses/ownership/memberships should be prevented by lookup-before-insert logic.
4. Produce deterministic behavior regardless of row order.

## Logging And Run Report
Emit structured logs keyed by `run_id` with counters:
1. rows_read
2. rows_rejected
3. participants_inserted
4. participants_matched_existing
5. yachts_inserted
6. entries_upserted
7. owner_links_inserted
8. memberships_inserted
9. contact_points_inserted
10. addresses_inserted

Print final summary and write JSON report to `./artifacts/reports/<run_id>.json`.

## Acceptance Criteria
1. `--dry-run` reports validation outcomes without writes.
2. Real run on this file completes without fatal errors.
3. Exactly one `event_instance` for season 2025 is used/created.
4. One `event_entry` exists per valid yacht row for that event instance.
5. Re-running same command is idempotent for `event_entry` and does not inflate owner/contact rows.
6. Reject report includes the known blank-yacht row.

## Minimal Test Plan
1. Unit tests for normalization functions:
   - email, phone, name normalization
   - timestamp sentinel handling
2. Unit tests for multi-owner parsing from `Name`.
3. Integration test (transactional test DB):
   - import sample fixture with duplicate owner and duplicate `oid`
   - assert no duplicate `event_entry` on second run
4. Integration test for reject handling:
   - blank `Yacht Name` row goes to reject file
   - run continues
5. Integration test for entry status derivation:
   - paid row => `confirmed`
   - unpaid + free => `confirmed`
   - unpaid non-free => `submitted`

## Open Decisions For User Confirmation
1. Confirm canonical event metadata values:
   - host club normalized name
   - event series name
   - event display name
2. Confirm whether primary owner should also be inserted as `registrant`.
3. Confirm whether placeholder club names should be skipped or preserved as a synthetic club.
