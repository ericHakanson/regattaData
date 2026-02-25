-- Migration: 0009_airtable_copy_tables.sql
-- Purpose: Add tables for Airtable copy multi-asset ingestion.
-- Ref: docs/requirements/airtable-copy-ingestion-requirements.md
-- Assets: clubs, events, entries, yachts, owners, participants

BEGIN;

-- ============================================================
-- airtable_copy_row (Lossless source capture — append-only)
-- ============================================================
CREATE TABLE airtable_copy_row (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     text        NOT NULL DEFAULT 'airtable_copy_csv',
    asset_name        text        NOT NULL CHECK (asset_name IN
                          ('clubs','events','entries','yachts','owners','participants')),
    source_file_name  text        NOT NULL,
    source_row_ordinal integer    NOT NULL,
    source_primary_id text,
    source_type       text,
    row_hash          text        NOT NULL,
    raw_payload       jsonb       NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now()
);

-- Idempotency key: same row in same file produces same hash → no duplicate
CREATE UNIQUE INDEX idx_airtable_copy_row_unique
    ON airtable_copy_row (source_system, asset_name, source_file_name, row_hash);

-- Lookup by source primary ID (e.g. Airtable global_id)
CREATE INDEX idx_airtable_copy_row_primary_id
    ON airtable_copy_row (asset_name, source_primary_id)
    WHERE source_primary_id IS NOT NULL;

-- ============================================================
-- airtable_xref_participant
-- Maps owner_global_id / participant_global_id → participant.id
-- ============================================================
CREATE TABLE airtable_xref_participant (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     text        NOT NULL DEFAULT 'airtable_copy_csv',
    asset_name        text        NOT NULL,
    source_primary_id text        NOT NULL,
    participant_id    uuid        NOT NULL REFERENCES participant (id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    last_seen_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_airtable_xref_participant_unique
    ON airtable_xref_participant (source_system, asset_name, source_primary_id);

CREATE INDEX idx_airtable_xref_participant_pid
    ON airtable_xref_participant (participant_id);

-- ============================================================
-- airtable_xref_yacht
-- Maps yacht_global_id → yacht.id
-- ============================================================
CREATE TABLE airtable_xref_yacht (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     text        NOT NULL DEFAULT 'airtable_copy_csv',
    asset_name        text        NOT NULL,
    source_primary_id text        NOT NULL,
    yacht_id          uuid        NOT NULL REFERENCES yacht (id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    last_seen_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_airtable_xref_yacht_unique
    ON airtable_xref_yacht (source_system, asset_name, source_primary_id);

CREATE INDEX idx_airtable_xref_yacht_yid
    ON airtable_xref_yacht (yacht_id);

-- ============================================================
-- airtable_xref_club
-- Maps club_global_id → yacht_club.id
-- ============================================================
CREATE TABLE airtable_xref_club (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     text        NOT NULL DEFAULT 'airtable_copy_csv',
    asset_name        text        NOT NULL,
    source_primary_id text        NOT NULL,
    yacht_club_id     uuid        NOT NULL REFERENCES yacht_club (id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    last_seen_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_airtable_xref_club_unique
    ON airtable_xref_club (source_system, asset_name, source_primary_id);

CREATE INDEX idx_airtable_xref_club_cid
    ON airtable_xref_club (yacht_club_id);

-- ============================================================
-- airtable_xref_event
-- Maps canonical race key (race:{race_id}:yr:{yr}) → event_instance.id
-- ============================================================
CREATE TABLE airtable_xref_event (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     text        NOT NULL DEFAULT 'airtable_copy_csv',
    asset_name        text        NOT NULL,
    source_primary_id text        NOT NULL,
    event_instance_id uuid        NOT NULL REFERENCES event_instance (id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    last_seen_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_airtable_xref_event_unique
    ON airtable_xref_event (source_system, asset_name, source_primary_id);

CREATE INDEX idx_airtable_xref_event_eid
    ON airtable_xref_event (event_instance_id);

COMMIT;
