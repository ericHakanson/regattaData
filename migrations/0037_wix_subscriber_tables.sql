-- Migration: 0037_wix_subscriber_tables.sql
-- Purpose: Lossless raw capture for Wix website-subscriber exports, mirroring the
--          mailchimp_audience_row / jotform_waiver_submission source-capture pattern.
-- Ref: FOR-886 (promote the one-time Wix SQL insert to a first-class importer).

BEGIN;

CREATE TABLE wix_subscriber_row (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system           text        NOT NULL DEFAULT 'wix',
    source_file_name        text        NOT NULL,
    source_email_raw        text,
    source_email_normalized text,
    subscriber_status       text,   -- e.g. 'Subscribed' | 'Never subscribed'
    labels                  text,
    wix_source              text,   -- e.g. 'Form Submission' | 'Site Members' | 'Manual Creation'
    created_at_raw          text,
    language                text,
    raw_payload             jsonb       NOT NULL,
    row_hash                text        NOT NULL,
    ingested_at             timestamptz NOT NULL DEFAULT now(),
    is_latest               boolean     NOT NULL DEFAULT true,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

-- Idempotency: the same email + identical raw content is captured once.
-- COALESCE the (nullable) email so NULL emails collapse to '' — a plain unique
-- index would treat NULLs as distinct and let identical no-email rows duplicate.
CREATE UNIQUE INDEX idx_wix_subscriber_row_unique
    ON wix_subscriber_row (source_system, COALESCE(source_email_normalized, ''), row_hash);

CREATE INDEX idx_wix_subscriber_row_email
    ON wix_subscriber_row (source_email_normalized);

CREATE TRIGGER trg_wix_subscriber_row_updated_at
    BEFORE UPDATE ON wix_subscriber_row
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
