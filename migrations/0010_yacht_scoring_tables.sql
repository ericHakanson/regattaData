-- Migration: 0010_yacht_scoring_tables.sql
-- Purpose: Add tables for Yacht Scoring multi-file ingestion pipeline.
-- Ref: docs/requirements/yacht-scoring-ingestion-requirements.md
-- Assets: scraped_event_listing, scraped_entry_listing, deduplicated_entry, unique_yacht

BEGIN;

-- ============================================================
-- yacht_scoring_raw_row (Lossless source capture — append-only)
-- ============================================================
CREATE TABLE yacht_scoring_raw_row (
    id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system      text        NOT NULL DEFAULT 'yacht_scoring_csv',
    asset_type         text        NOT NULL CHECK (asset_type IN (
                           'scraped_event_listing',
                           'scraped_entry_listing',
                           'deduplicated_entry',
                           'unique_yacht',
                           'unknown'
                       )),
    source_file_name   text        NOT NULL,
    source_file_path   text        NOT NULL,
    source_row_ordinal integer     NOT NULL,
    source_event_id    text,
    source_entry_id    text,
    row_hash           text        NOT NULL,
    raw_payload        jsonb       NOT NULL,
    ingested_at        timestamptz NOT NULL DEFAULT now()
);

-- Idempotency: same row content in same file path → no duplicate insert
CREATE UNIQUE INDEX idx_ys_raw_row_unique
    ON yacht_scoring_raw_row (source_system, source_file_path, row_hash);

CREATE INDEX idx_ys_raw_row_event_id
    ON yacht_scoring_raw_row (source_event_id)
    WHERE source_event_id IS NOT NULL;

CREATE INDEX idx_ys_raw_row_event_entry
    ON yacht_scoring_raw_row (source_event_id, source_entry_id)
    WHERE source_event_id IS NOT NULL AND source_entry_id IS NOT NULL;

-- ============================================================
-- yacht_scoring_xref_event
-- Maps Yacht Scoring numeric event_id → event_instance.id
-- ============================================================
CREATE TABLE yacht_scoring_xref_event (
    id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system      text        NOT NULL DEFAULT 'yacht_scoring_csv',
    source_event_id    text        NOT NULL,
    event_instance_id  uuid        NOT NULL REFERENCES event_instance (id),
    created_at         timestamptz NOT NULL DEFAULT now(),
    last_seen_at       timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_ys_xref_event_unique
    ON yacht_scoring_xref_event (source_system, source_event_id);

CREATE INDEX idx_ys_xref_event_iid
    ON yacht_scoring_xref_event (event_instance_id);

-- ============================================================
-- yacht_scoring_xref_entry
-- Maps (event_id, entry_id) → event_entry.id
-- ============================================================
CREATE TABLE yacht_scoring_xref_entry (
    id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system      text        NOT NULL DEFAULT 'yacht_scoring_csv',
    source_event_id    text        NOT NULL,
    source_entry_id    text        NOT NULL,
    event_entry_id     uuid        NOT NULL REFERENCES event_entry (id),
    created_at         timestamptz NOT NULL DEFAULT now(),
    last_seen_at       timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_ys_xref_entry_unique
    ON yacht_scoring_xref_entry (source_system, source_event_id, source_entry_id);

CREATE INDEX idx_ys_xref_entry_eid
    ON yacht_scoring_xref_entry (event_entry_id);

-- ============================================================
-- yacht_scoring_xref_yacht
-- Maps stable source yacht key → yacht.id
-- Key format: "n:{name_slug}:s:{sail_slug}" (sail present or absent)
-- ============================================================
CREATE TABLE yacht_scoring_xref_yacht (
    id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system      text        NOT NULL DEFAULT 'yacht_scoring_csv',
    source_yacht_key   text        NOT NULL,
    yacht_id           uuid        NOT NULL REFERENCES yacht (id),
    created_at         timestamptz NOT NULL DEFAULT now(),
    last_seen_at       timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_ys_xref_yacht_unique
    ON yacht_scoring_xref_yacht (source_system, source_yacht_key);

CREATE INDEX idx_ys_xref_yacht_yid
    ON yacht_scoring_xref_yacht (yacht_id);

-- ============================================================
-- yacht_scoring_xref_participant
-- Maps stable source participant key → participant.id
-- Key format: "{name_norm}|{affiliation_slug}|{location_slug}"
-- ============================================================
CREATE TABLE yacht_scoring_xref_participant (
    id                       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system            text        NOT NULL DEFAULT 'yacht_scoring_csv',
    source_participant_key   text        NOT NULL,
    participant_id           uuid        NOT NULL REFERENCES participant (id),
    created_at               timestamptz NOT NULL DEFAULT now(),
    last_seen_at             timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_ys_xref_participant_unique
    ON yacht_scoring_xref_participant (source_system, source_participant_key);

CREATE INDEX idx_ys_xref_participant_pid
    ON yacht_scoring_xref_participant (participant_id);

COMMIT;
