-- Migration: 0018_bhyc_member_directory_tables.sql
-- Purpose: Source tables for BHYC member directory scrape + ingestion pipeline.
-- Ref: docs/requirements/bhyc-member-directory-scrape-ingestion-requirements.md
-- Depends on: 0001 (set_updated_at), 0002 (participant, yacht, yacht_club)

BEGIN;

-- ============================================================
-- bhyc_member_raw_row
-- Lossless capture of each fetched page (profile HTML + vCard),
-- one row per (source_system, member_id, page_type, run_id).
-- Idempotent: re-running the same crawl does not insert duplicate rows.
-- ============================================================
CREATE TABLE bhyc_member_raw_row (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system    text        NOT NULL DEFAULT 'bhyc_member_directory',
    member_id        text        NOT NULL,
    page_type        text        NOT NULL CHECK (page_type IN (
                         'directory_listing',
                         'member_profile',
                         'vcard'
                     )),
    source_url       text        NOT NULL,
    run_id           text        NOT NULL,
    gcs_bucket       text,
    gcs_object       text,
    http_status      integer,
    content_hash     text,
    fetched_at       timestamptz,
    parsed_json      jsonb,
    parse_warnings   jsonb,
    ingested_at      timestamptz NOT NULL DEFAULT now()
);

-- One row per (member_id, page_type) per run; re-run upserts via DO NOTHING
CREATE UNIQUE INDEX idx_bhyc_raw_row_unique
    ON bhyc_member_raw_row (source_system, member_id, page_type, run_id);

CREATE INDEX idx_bhyc_raw_row_member_id
    ON bhyc_member_raw_row (member_id);

CREATE INDEX idx_bhyc_raw_row_run_id
    ON bhyc_member_raw_row (run_id);

-- ============================================================
-- bhyc_member_xref_participant
-- Maps a BHYC member_id (+ optional household relationship_label)
-- to an operational participant.id.
-- relationship_label = NULL  → primary/direct member
-- relationship_label = 'spouse', 'child', etc. → household member
-- ============================================================
CREATE TABLE bhyc_member_xref_participant (
    id                   uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system        text        NOT NULL DEFAULT 'bhyc_member_directory',
    member_id            text        NOT NULL,
    -- NULL = primary member; else household relationship ('spouse','child',…)
    relationship_label   text,
    participant_id       uuid        NOT NULL REFERENCES participant (id),
    created_at           timestamptz NOT NULL DEFAULT now(),
    last_seen_at         timestamptz NOT NULL DEFAULT now()
);

-- Unique per (member_id, relationship) using COALESCE to handle NULL
CREATE UNIQUE INDEX idx_bhyc_xref_participant_unique
    ON bhyc_member_xref_participant (
        source_system,
        member_id,
        COALESCE(relationship_label, '')
    );

CREATE INDEX idx_bhyc_xref_participant_pid
    ON bhyc_member_xref_participant (participant_id);

COMMIT;
