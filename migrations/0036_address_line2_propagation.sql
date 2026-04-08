-- Migration: 0036_address_line2_propagation.sql
-- Purpose: propagate address line2 support into candidate/canonical/manual layers.
--
-- Rationale:
--   participant_address already supports line2 (0003).
--   candidate_participant_address / canonical_participant_address did not,
--   causing secondary unit data (apt/suite/unit) to be dropped during
--   projection/promotion.

BEGIN;

ALTER TABLE candidate_participant_address
    ADD COLUMN IF NOT EXISTS line2 text;

ALTER TABLE canonical_participant_address
    ADD COLUMN IF NOT EXISTS line2 text;

ALTER TABLE manual_participant_address_patch
    ADD COLUMN IF NOT EXISTS line2 text;

COMMIT;
