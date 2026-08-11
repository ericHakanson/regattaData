-- Migration: 0038_participant_suppression.sql
-- Purpose: First-class consent/suppression surface so the send-audience build can
--          exclude addresses we must not email — minors reachable only via a
--          guardian's email, manual opt-outs, third-party (emergency/guardian)
--          contacts with no consent — independent of the Mailchimp mirror.
--          Soft-delete (deleted_at) so a suppression can be lifted without
--          destroying the audit trail (see DATA_PROTECTION.md).
-- Ref: FOR-884 data-quality pass — minor/guardian-email consent gap surfaced
--      2026-08-11 (jeremiahgarland@yahoo.com = guardian email for a minor,
--      Ruby Garland, that was 'subscribed' in the Mailchimp audience mirror).

BEGIN;

CREATE TABLE participant_suppression (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Anchor: a suppression targets a participant, a specific email, or both.
    -- participant_id NULL supports email-only suppressions (an address present in
    -- the Mailchimp mirror that never resolved to a participant).
    participant_id    uuid        REFERENCES participant (id),
    -- email_normalized NULL means the suppression applies to ALL of the
    -- participant's emails (participant-wide); non-NULL targets that one address.
    email_normalized  text,
    -- Free-text but controlled; known values (2026-08): 'minor_guardian_email',
    -- 'manual_optout', 'third_party_no_consent', 'hard_bounce'.
    reason_code       text        NOT NULL,
    -- What the suppression blocks. Kept narrow for now.
    suppression_scope text        NOT NULL DEFAULT 'email_send'
        CHECK (suppression_scope IN ('email_send')),
    notes             text,
    source_system     text        NOT NULL DEFAULT 'manual',
    actor             text        NOT NULL,
    observed_at       timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    -- Soft-delete: lifting a suppression sets deleted_at, preserving the record.
    deleted_at        timestamptz,
    -- Every suppression must anchor to a participant, an email, or both.
    CONSTRAINT participant_suppression_anchor_present
        CHECK (participant_id IS NOT NULL OR email_normalized IS NOT NULL)
);

-- One ACTIVE suppression per (participant, email, reason). NULLS NOT DISTINCT so a
-- participant-wide (email NULL) or email-only (participant NULL) row cannot be
-- duplicated. Soft-deleted rows are excluded from the constraint, so a lifted
-- suppression can be re-added later.
CREATE UNIQUE INDEX idx_participant_suppression_active_unique
    ON participant_suppression (participant_id, email_normalized, reason_code)
    NULLS NOT DISTINCT
    WHERE deleted_at IS NULL;

-- Audience-build join path: look up active suppressions by email.
CREATE INDEX idx_participant_suppression_email_active
    ON participant_suppression (email_normalized)
    WHERE deleted_at IS NULL AND email_normalized IS NOT NULL;

CREATE INDEX idx_participant_suppression_participant
    ON participant_suppression (participant_id)
    WHERE participant_id IS NOT NULL;

CREATE TRIGGER trg_participant_suppression_updated_at
    BEFORE UPDATE ON participant_suppression
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Active suppressed EMAIL addresses, resolved for the send-audience exclusion join:
--   * explicit email_normalized rows, plus
--   * every email of a participant carrying a participant-wide (email NULL)
--     suppression.
-- Emails are lower-cased so the join matches other normalized-email surfaces.
CREATE VIEW active_suppressed_email AS
    SELECT DISTINCT
        lower(ps.email_normalized) AS email_normalized,
        ps.reason_code
    FROM participant_suppression ps
    WHERE ps.deleted_at IS NULL
      AND ps.email_normalized IS NOT NULL
    UNION
    SELECT DISTINCT
        lower(pcp.contact_value_normalized) AS email_normalized,
        ps.reason_code
    FROM participant_suppression ps
    JOIN participant_contact_point pcp
      ON pcp.participant_id = ps.participant_id
     AND pcp.contact_type = 'email'
    WHERE ps.deleted_at IS NULL
      AND ps.email_normalized IS NULL
      AND pcp.contact_value_normalized IS NOT NULL;

-- Data migration: promote the interim DO_NOT_EMAIL_MINOR marker (written into
-- mailchimp_contact_tag during the 2026-08-11 DQ pass) onto this surface. This is a
-- no-op on databases where that tag was never written (e.g. fresh test databases).
-- The interim tag rows are intentionally LEFT in place — removing them is a
-- separate, approval-gated step (DATA_PROTECTION.md).
INSERT INTO participant_suppression
    (participant_id, email_normalized, reason_code, source_system, actor, notes, observed_at)
SELECT DISTINCT
    t.participant_id,
    lower(t.email_normalized),
    'minor_guardian_email',
    t.source_system,
    'dq884_migration',
    'Migrated from mailchimp_contact_tag DO_NOT_EMAIL_MINOR (minor reachable only via a guardian email).',
    t.observed_at
FROM mailchimp_contact_tag t
WHERE t.tag_value = 'DO_NOT_EMAIL_MINOR'
ON CONFLICT DO NOTHING;

COMMIT;
