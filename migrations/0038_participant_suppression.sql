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
        CHECK (participant_id IS NOT NULL OR email_normalized IS NOT NULL),
    -- email_normalized must actually be normalized (lower-cased) so the active-unique
    -- indexes can't be defeated by case variation (e.g. 'Foo@x' vs 'foo@x') and so it
    -- matches the lower-cased addresses the active_suppressed_email view emits.
    CONSTRAINT participant_suppression_email_is_normalized
        CHECK (email_normalized IS NULL OR email_normalized = lower(email_normalized))
);

-- One ACTIVE suppression per (participant, email, reason). Enforced with three
-- portable partial unique indexes — one per anchor shape — instead of a single
-- NULLS NOT DISTINCT index (which requires PostgreSQL 15+). This keeps the migration
-- cross-version and gives the NULL anchors real uniqueness (a default unique index
-- treats NULLs as distinct, so a plain index would NOT dedupe the NULL shapes).
-- Soft-deleted rows are excluded so a lifted suppression can be re-added later.
-- (The all-NULL shape is impossible: participant_suppression_anchor_present forbids it.)
CREATE UNIQUE INDEX idx_participant_suppression_active_both
    ON participant_suppression (participant_id, email_normalized, reason_code)
    WHERE deleted_at IS NULL
      AND participant_id IS NOT NULL
      AND email_normalized IS NOT NULL;

CREATE UNIQUE INDEX idx_participant_suppression_active_participant_wide
    ON participant_suppression (participant_id, reason_code)
    WHERE deleted_at IS NULL
      AND email_normalized IS NULL;

CREATE UNIQUE INDEX idx_participant_suppression_active_email_only
    ON participant_suppression (email_normalized, reason_code)
    WHERE deleted_at IS NULL
      AND participant_id IS NULL;

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
--
-- Robustness (defensive, even though this source can't currently violate either):
--   * DISTINCT ON (participant_id, email) collapses multiple tag rows for the same
--     anchor to one, so the statement never proposes two rows that collide on the
--     active-unique index.
--   * LEFT JOIN participant + guard skips any tag row whose participant_id does not
--     resolve, so a stale/dangling anchor could never abort the migration on the FK.
--     (mailchimp_contact_tag.participant_id already FKs participant(id), so this is
--     belt-and-suspenders for future/mirrored sources.)
INSERT INTO participant_suppression
    (participant_id, email_normalized, reason_code, source_system, actor, notes, observed_at)
SELECT DISTINCT ON (t.participant_id, lower(t.email_normalized))
    t.participant_id,
    lower(t.email_normalized),
    'minor_guardian_email',
    t.source_system,
    'dq884_migration',
    'Migrated from mailchimp_contact_tag DO_NOT_EMAIL_MINOR (minor reachable only via a guardian email).',
    t.observed_at
FROM mailchimp_contact_tag t
LEFT JOIN participant p ON p.id = t.participant_id
WHERE t.tag_value = 'DO_NOT_EMAIL_MINOR'
  AND (t.participant_id IS NULL OR p.id IS NOT NULL)
ORDER BY t.participant_id, lower(t.email_normalized), t.observed_at DESC NULLS LAST
ON CONFLICT DO NOTHING;

COMMIT;
