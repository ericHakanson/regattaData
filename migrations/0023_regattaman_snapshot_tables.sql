-- Migration: 0023_regattaman_snapshot_tables.sql
-- Purpose: Add tables for Regattaman owner+yacht snapshot ingestion.
-- Ref: FOR-159 (Implement importer mode for local Regattaman owner+yacht CSV snapshot)
-- Source file: finalMergedAllRegattaman - unique.csv
-- Columns: nameLower, yachtNameLower, boatTypeLower, Sail, Rating (PHRF)

BEGIN;

-- ============================================================
-- regattaman_snapshot_row (Lossless source capture — append-only)
-- ============================================================
CREATE TABLE regattaman_snapshot_row (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     text        NOT NULL DEFAULT 'regattaman_snapshot_csv',
    source_file_name  text        NOT NULL,
    source_row_ordinal integer    NOT NULL,
    source_primary_id text        NOT NULL,
    row_hash          text        NOT NULL,
    raw_payload       jsonb       NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now()
);

-- Idempotency key: same row in same file produces same hash → no duplicate
CREATE UNIQUE INDEX idx_regattaman_snapshot_row_unique
    ON regattaman_snapshot_row (source_system, source_file_name, row_hash);

-- Lookup by source primary ID (sha256 of nameLower|yachtNameLower|Sail)
CREATE INDEX idx_regattaman_snapshot_row_primary_id
    ON regattaman_snapshot_row (source_primary_id);

-- ============================================================
-- regattaman_snapshot_xref_participant
-- Maps {row_primary_id}:{owner_idx} → participant.id
-- ============================================================
CREATE TABLE regattaman_snapshot_xref_participant (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     text        NOT NULL DEFAULT 'regattaman_snapshot_csv',
    source_primary_id text        NOT NULL,
    participant_id    uuid        NOT NULL REFERENCES participant (id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    last_seen_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_regattaman_snapshot_xref_participant_unique
    ON regattaman_snapshot_xref_participant (source_system, source_primary_id);

CREATE INDEX idx_regattaman_snapshot_xref_participant_pid
    ON regattaman_snapshot_xref_participant (participant_id);

-- ============================================================
-- regattaman_snapshot_xref_yacht
-- Maps row_primary_id → yacht.id
-- ============================================================
CREATE TABLE regattaman_snapshot_xref_yacht (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     text        NOT NULL DEFAULT 'regattaman_snapshot_csv',
    source_primary_id text        NOT NULL,
    yacht_id          uuid        NOT NULL REFERENCES yacht (id),
    created_at        timestamptz NOT NULL DEFAULT now(),
    last_seen_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_regattaman_snapshot_xref_yacht_unique
    ON regattaman_snapshot_xref_yacht (source_system, source_primary_id);

CREATE INDEX idx_regattaman_snapshot_xref_yacht_yid
    ON regattaman_snapshot_xref_yacht (yacht_id);

COMMIT;
