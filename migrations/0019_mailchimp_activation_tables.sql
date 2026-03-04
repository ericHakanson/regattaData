-- Migration: 0019_mailchimp_activation_tables.sql
-- Purpose: Audit tables for the Mailchimp event-registration activation pipeline.
-- Ref: docs/requirements/mailchimp-event-registration-activation-spec.md
-- Depends on: 0001 (gen_random_uuid)

BEGIN;

-- ============================================================
-- mailchimp_activation_run
-- One row per pipeline execution; tracks mode, segment, counters, status.
-- ============================================================

CREATE TABLE mailchimp_activation_run (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    mode              TEXT        NOT NULL CHECK (mode IN ('csv', 'api')),
    segment_type      TEXT        NOT NULL CHECK (segment_type IN (
                          'upcoming_registrants', 'likely_registrants', 'all'
                      )),
    event_window_days INT         NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'running'
                          CHECK (status IN ('running', 'ok', 'failed')),
    counters          JSONB,
    created_by        TEXT
);

CREATE INDEX idx_mailchimp_activation_run_status
    ON mailchimp_activation_run (status, started_at DESC);

-- ============================================================
-- mailchimp_activation_row
-- One row per (run, normalized email); captures eligible and suppressed rows.
-- ============================================================

CREATE TABLE mailchimp_activation_row (
    id                 UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id             UUID    NOT NULL REFERENCES mailchimp_activation_run (id) ON DELETE CASCADE,
    email_normalized   TEXT    NOT NULL,
    participant_id     UUID,
    segment_types      TEXT[]  NOT NULL DEFAULT '{}',
    is_suppressed      BOOL    NOT NULL DEFAULT FALSE,
    suppression_reason TEXT,
    payload            JSONB,
    UNIQUE (run_id, email_normalized)
);

CREATE INDEX idx_mailchimp_activation_row_run_id
    ON mailchimp_activation_row (run_id);

CREATE INDEX idx_mailchimp_activation_row_email
    ON mailchimp_activation_row (email_normalized);

COMMIT;
