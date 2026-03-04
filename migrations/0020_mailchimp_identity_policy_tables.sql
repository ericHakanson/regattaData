-- Migration: 0020_mailchimp_identity_policy_tables.sql
-- Purpose: Strict Mailchimp identity linkage + manual review queue.
-- Ref: docs/requirements/mailchimp-strict-identity-and-contact-id-policy-spec.md
--
-- NOTE:
-- This is an implementation stub for the strict-identity policy.
-- Validate naming, indexes, and conflict behavior with application code before rollout.

BEGIN;

-- ============================================================
-- participant_mailchimp_identity
-- One active identity mapping per participant/email with Mailchimp IDs.
-- ============================================================

CREATE TABLE IF NOT EXISTS participant_mailchimp_identity (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_id        UUID        NOT NULL REFERENCES participant (id) ON DELETE CASCADE,
    mailchimp_contact_id  TEXT,
    leid                  TEXT,
    euid                  TEXT,
    subscriber_hash       TEXT,
    email_normalized      TEXT        NOT NULL,
    first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_system         TEXT        NOT NULL,
    source_file_name      TEXT,
    is_active             BOOL        NOT NULL DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_participant_mailchimp_identity_participant_email
    ON participant_mailchimp_identity (participant_id, email_normalized);

CREATE UNIQUE INDEX IF NOT EXISTS uq_participant_mailchimp_identity_contact_id
    ON participant_mailchimp_identity (mailchimp_contact_id)
    WHERE mailchimp_contact_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_participant_mailchimp_identity_leid
    ON participant_mailchimp_identity (leid)
    WHERE leid IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_participant_mailchimp_identity_euid
    ON participant_mailchimp_identity (euid)
    WHERE euid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_participant_mailchimp_identity_email
    ON participant_mailchimp_identity (email_normalized);

CREATE INDEX IF NOT EXISTS idx_participant_mailchimp_identity_participant
    ON participant_mailchimp_identity (participant_id);

-- ============================================================
-- mailchimp_identity_review_queue
-- Quarantined identity-conflict rows for manual adjudication.
-- ============================================================

CREATE TABLE IF NOT EXISTS mailchimp_identity_review_queue (
    id                        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_file_name          TEXT        NOT NULL,
    email_normalized          TEXT,
    candidate_participant_id  UUID        REFERENCES participant (id) ON DELETE SET NULL,
    reason_code               TEXT        NOT NULL,
    reason_detail             TEXT,
    raw_payload               JSONB       NOT NULL,
    status                    TEXT        NOT NULL DEFAULT 'open'
                               CHECK (status IN ('open', 'resolved', 'dismissed')),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at               TIMESTAMPTZ,
    resolved_by               TEXT
);

CREATE INDEX IF NOT EXISTS idx_mailchimp_identity_review_queue_status_created
    ON mailchimp_identity_review_queue (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_mailchimp_identity_review_queue_email
    ON mailchimp_identity_review_queue (email_normalized);

CREATE INDEX IF NOT EXISTS idx_mailchimp_identity_review_queue_candidate
    ON mailchimp_identity_review_queue (candidate_participant_id);

COMMIT;
