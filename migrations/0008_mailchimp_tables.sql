-- Migration: 0008_mailchimp_tables.sql
-- Purpose: Add tables for Mailchimp audience contact ingestion.
-- Ref: docs/requirements/mailchimp-audience-contact-ingestion-requirements.md

BEGIN;

-- ============================================================
-- mailchimp_audience_row (Lossless Source Capture)
-- ============================================================
CREATE TABLE mailchimp_audience_row (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system           text        NOT NULL DEFAULT 'mailchimp_audience_csv',
    source_file_name        text        NOT NULL,
    audience_status         text        NOT NULL CHECK (audience_status IN ('subscribed', 'unsubscribed', 'cleaned')),
    source_email_raw        text,
    source_email_normalized text,
    row_hash                text        NOT NULL,
    raw_payload             jsonb       NOT NULL,
    ingested_at             timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_mailchimp_audience_row_unique
    ON mailchimp_audience_row (source_system, source_file_name, row_hash);

CREATE INDEX idx_mailchimp_audience_row_email
    ON mailchimp_audience_row (source_email_normalized);

-- ============================================================
-- mailchimp_contact_state (Append-only status history)
-- ============================================================
CREATE TABLE mailchimp_contact_state (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_id          uuid        NOT NULL REFERENCES participant (id),
    email_normalized        text        NOT NULL,
    audience_status         text        NOT NULL CHECK (audience_status IN ('subscribed', 'unsubscribed', 'cleaned')),
    status_at               timestamptz,
    -- Subscribe-specific
    last_changed            timestamptz,
    optin_time              timestamptz,
    confirm_time            timestamptz,
    optin_ip                text,
    confirm_ip              text,
    -- Unsubscribe-specific
    unsub_campaign_title    text,
    unsub_campaign_id       text,
    unsub_reason            text,
    unsub_reason_other      text,
    -- Clean-specific
    clean_campaign_title    text,
    clean_campaign_id       text,
    -- Shared Mailchimp attributes
    leid                    text,
    euid                    text,
    member_rating           integer,
    gmtoff                  text,
    dstoff                  text,
    timezone                text,
    cc                      text,
    region                  text,
    notes                   text,
    -- Provenance
    source_system           text        NOT NULL DEFAULT 'mailchimp_audience_csv',
    source_file_name        text        NOT NULL,
    row_hash                text        NOT NULL,
    ingested_at             timestamptz NOT NULL DEFAULT now()
);

-- Unique per source row: one state record per (participant, file, row)
CREATE UNIQUE INDEX idx_mailchimp_contact_state_unique
    ON mailchimp_contact_state (participant_id, source_file_name, row_hash);

CREATE INDEX idx_mailchimp_contact_state_participant
    ON mailchimp_contact_state (participant_id);

CREATE INDEX idx_mailchimp_contact_state_email
    ON mailchimp_contact_state (email_normalized);

-- ============================================================
-- mailchimp_contact_tag (Tag bridge table)
-- ============================================================
CREATE TABLE mailchimp_contact_tag (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_id   uuid        NOT NULL REFERENCES participant (id),
    email_normalized text        NOT NULL,
    tag_value        text        NOT NULL,
    source_system    text        NOT NULL DEFAULT 'mailchimp_audience_csv',
    source_file_name text        NOT NULL,
    observed_at      timestamptz,
    ingested_at      timestamptz NOT NULL DEFAULT now()
);

-- One tag row per (participant, email, tag, file) — idempotent rerun safe
CREATE UNIQUE INDEX idx_mailchimp_contact_tag_unique
    ON mailchimp_contact_tag (participant_id, email_normalized, tag_value, source_file_name);

CREATE INDEX idx_mailchimp_contact_tag_participant
    ON mailchimp_contact_tag (participant_id);

COMMIT;
