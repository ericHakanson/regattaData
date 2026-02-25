# Specification: BHYC 2025 Jotform Waiver Ingestion

## Objective
Ingest `rawEvidence/2025_BHYC_Regatta_WAIVER_RELEA2026-02-17_23_14_12.csv` into Cloud SQL with:
1. Zero loss of source data (`all records, all attributes`).
2. Deterministic participant/event linkage where possible.
3. Idempotent reruns and clear reconciliation outputs.

Primary priorities:
1. Accuracy and completeness.
2. Reliable, sustainable performance.
3. Reuse existing ingestion architecture (avoid standalone pipeline sprawl).

## Source File Contract
File:
- `rawEvidence/2025_BHYC_Regatta_WAIVER_RELEA2026-02-17_23_14_12.csv`

Observed profile baseline:
1. 304 data rows.
2. 26 columns.
3. `Submission ID` is unique in this extract (304/304).
4. `Last Update Date` is blank for all rows.
5. `Signed Document` is multiline URL text (2 URLs in most rows).
6. All rows are 2025 submissions.

## Implementation Direction
Use one unified ingestion codebase (`regatta_etl`) with a Jotform adapter mode.

Required CLI mode contract:
1. Extend shared CLI with `--mode jotform_waiver` (preferred), or
2. Add a new module within same package (`regatta_etl.import_jotform_waiver`) that reuses shared parsing/upsert/report components.

Do not build a separate independent repository/pipeline runtime.

## Required Data-Capture Guarantees
### Lossless Source Capture (Mandatory)
Persist every row and every raw attribute exactly as received.

Required table (new migration): `jotform_waiver_submission`
1. `id` uuid PK.
2. `source_system` text not null default `jotform_csv_export`.
3. `source_file_name` text not null.
4. `source_submission_id` text not null.
5. `source_submitted_at_raw` text not null.
6. `source_last_update_at_raw` text null.
7. `raw_payload` jsonb not null (all 26 fields as key/value, unmodified values).
8. `row_hash` text not null.
9. `ingested_at` timestamptz not null default now().
10. `is_latest` boolean not null default true.
11. Unique constraint: (`source_system`, `source_submission_id`, `row_hash`).

Rationale:
1. Guarantees no attribute loss.
2. Supports audit/history if same submission is re-exported with changed values.

### Canonical Projection (Curated Layer)
In addition to raw capture, project data into normalized model tables.

Minimum projection targets:
1. `participant`
2. `participant_contact_point`
3. `participant_address`
4. `event_entry_participant` (when event entry can be linked)
5. `document_type`
6. `document_requirement`
7. `document_status`

Recommended new table for contact richness:
1. `participant_related_contact` (new migration) to preserve emergency/guardian entities without flattening:
   - `participant_id` uuid FK.
   - `related_contact_type` text check in (`emergency`, `guardian`).
   - `related_full_name` text.
   - `relationship_label` text.
   - `phone_raw` text.
   - `phone_normalized` text.
   - `email_raw` text.
   - `email_normalized` text.
   - `source_submission_id` text.
   - `source_system` text.
   - timestamps.

If this table is not added in phase 1, related contact fields must still be preserved in `raw_payload` and emitted in export views.

## Source-To-Target Mapping
### Core Submission Fields
1. `Submission ID` -> `jotform_waiver_submission.source_submission_id` (primary idempotency key).
2. `Submission Date` -> parsed timestamptz for document assertion timestamp.
3. `Last Update Date` -> parsed when present, else null.
4. `Submission IP` -> raw payload retained; optional dedicated column if needed for fraud/audit.

### Participant Identity + Contact
1. `Name` -> `participant.full_name` (+ normalized name).
2. `Competitor E mail` -> `participant_contact_point` (`email`, subtype `primary`).
3. `Numbers only, No dashes. Start with area code` -> `participant_contact_point` (`phone`, subtype `primary_mobile`).
4. `Address` + `Postal code` -> `participant_address` with `address_raw` mandatory and parsed components best effort.

### Event/Yacht Context Hints
1. `Boat Name` -> candidate yacht/event linkage key (normalized).
2. `Sail Number` -> candidate yacht/event linkage key (normalized).
3. `I am the skipper (person in charge)` -> role hint for `event_entry_participant`:
   - `Yes` => role `skipper`
   - `No` => role `crew` (or `other`, per finalized policy)

### Waiver + Signatures
1. `Please check box acknowledging above` -> boolean acknowledgment flag.
2. `I have read and understand the media waiver.` -> boolean media waiver assertion.
3. `Signed Document` -> split newline URLs; retain all URLs in raw + normalized list.
4. `Participant Signature` -> evidence URL.
5. `Agreement to the above of Parent or Guardian for participant under 18` -> guardian consent flag.
6. `Parent or Guardian Signature (if participant under 18)` -> guardian signature evidence URL.

### Minor + Guardian Contact
1. `I am age 18 or older` -> boolean.
2. If minor (`No`), map:
   - `Name of Parent Or Guardian`
   - `Parent or Guardian Phone:  Numbers only, No dashes. Start with area code`
   - `Parent or Guardian  E mail`
   into `participant_related_contact` as `guardian`.

### Emergency Contact
Map:
1. `Name of your emergency contact`
2. `Relationship (optional)`
3. `Emergency phone contact`
4. `Emergency email`
into `participant_related_contact` as `emergency`.

## Document Tracking Requirements (Jotform Truth)
Jotform is waiver truth source.

Required behavior:
1. Ensure `document_type` exists for waiver (scope `participant`), e.g. normalized name `bhyc-waiver-release`.
2. Ensure `document_requirement` exists for BHYC 2025 event instance.
3. Insert `document_status` with:
   - `status='received'`
   - `participant_id=<resolved participant>`
   - `status_at=<Submission Date parsed>`
   - `source_system='jotform_csv_export'`
   - `evidence_ref` containing signed doc/signature references (JSON serialized text allowed).

No downgrade behavior:
1. Do not overwrite a later `received` assertion with older timestamp from duplicate submission exports.
2. If multiple submissions exist for same participant/event, keep latest in participant-status view and preserve all raw rows.

## Event Linkage Requirements
Event context for this file:
1. Host club: Boothbay Harbor Yacht Club.
2. Event series: BHYC Regatta.
3. Season year: 2025.

Link participant submission to `event_entry` using deterministic strategy:
1. Exact match by normalized `Sail Number` against yacht on 2025 event entries.
2. Else exact match by normalized `Boat Name` within same event instance.
3. Else unresolved; keep submission + participant + contacts, and emit reconciliation row.

Unresolved rows must not be dropped.

## Idempotency Rules
1. Raw row idempotency key: (`source_system`, `Submission ID`, `row_hash`).
2. Curated upserts must be lookup-before-insert or conflict-safe.
3. Re-running same file must not duplicate:
   - participant primary contact points,
   - document requirement rows,
   - current logical waiver status assertions.
4. Re-running same file may append new raw row versions if row content changed for same `Submission ID`.

## Validation + Reject Policy
Fatal run errors:
1. Missing required headers.
2. Database connectivity/transaction failure.
3. Event context resolution failure.

Row-level rejects (write reject artifact, continue):
1. missing `Submission ID`
2. missing `Name`
3. missing `Competitor E mail`
4. unparseable `Submission Date`
5. malformed email/phone beyond normalization tolerance.

Even rejected curated rows must still be captured in raw submission table unless parsing is impossible.

## Performance/Operations Requirements
1. Must process this file (<1k rows) in under 60 seconds in dev environment.
2. Must support larger future Jotform exports via streaming CSV parse and bounded transactions.
3. Structured run report required:
   - rows_read
   - raw_rows_inserted
   - curated_rows_processed
   - rows_rejected
   - participants_upserted
   - contacts_upserted
   - document_status_upserted
   - unresolved_event_links
   - db_phase_errors

## Testing Requirements
### Unit
1. Date parsing (`Jul 23, 2025` format).
2. Multiline signed-document splitting.
3. Email/phone normalization from Jotform-specific fields.
4. Minor/guardian and emergency contact extraction.

### Integration
1. Full-file dry-run against ephemeral Postgres with migrations.
2. Real-run idempotency (second run no duplicate logical curated records).
3. Raw capture completeness test: each CSV row present in `jotform_waiver_submission`.
4. Event-link reconciliation test: unresolved linkage rows are counted and exported, not dropped.
5. Document status assertions created for linked participants.

## Acceptance Criteria
1. 100% of CSV rows are persisted losslessly in raw submission storage.
2. 100% of columns are retrievable per submission from stored payload.
3. Curated participant/contact/document data is loaded with deterministic idempotent behavior.
4. Unresolved event links are explicitly reported, never silently discarded.
5. Rerun of same file produces no duplicate logical curated records.

## Open Questions For Final Sign-Off
1. For non-skipper submissions, should `event_entry_participant.role` default to `crew` or `other`?
2. Should emergency/guardian contacts be first-class participants now, or kept in `participant_related_contact` only?
3. Should event-link unresolved rows block production runs above a threshold (for example >5%)?
