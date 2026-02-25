-- Migration: 0007_jotform_tables.sql
-- Purpose: Add tables to support Jotform waiver ingestion.
-- Ref: docs/requirements/bhyc-2025-jotform-waiver-ingestion-spec.md

BEGIN;

-- ============================================================
-- jotform_waiver_submission (Lossless Source Capture)
-- ============================================================
CREATE TABLE jotform_waiver_submission (
    id                        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system             text        NOT NULL DEFAULT 'jotform_csv_export',
    source_file_name          text        NOT NULL,
    source_submission_id      text        NOT NULL,
    source_submitted_at_raw   text        NOT NULL,
    source_last_update_at_raw text,
    raw_payload               jsonb       NOT NULL,
    row_hash                  text        NOT NULL,
    ingested_at               timestamptz NOT NULL DEFAULT now(),
    is_latest                 boolean     NOT NULL DEFAULT true,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_jotform_waiver_submission_unique 
    ON jotform_waiver_submission (source_system, source_submission_id, row_hash);

CREATE TRIGGER trg_jotform_waiver_submission_updated_at
    BEFORE UPDATE ON jotform_waiver_submission
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- participant_related_contact (Emergency/Guardian)
-- ============================================================
CREATE TABLE participant_related_contact (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_id          uuid        NOT NULL REFERENCES participant (id),
    related_contact_type    text        NOT NULL CHECK (related_contact_type IN ('emergency', 'guardian')),
    related_full_name       text,
    relationship_label      text,
    phone_raw               text,
    phone_normalized        text,
    email_raw               text,
    email_normalized        text,
    source_submission_id    text,
    source_system           text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_part_related_contact_participant_id ON participant_related_contact (participant_id);

CREATE TRIGGER trg_participant_related_contact_updated_at
    BEFORE UPDATE ON participant_related_contact
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
