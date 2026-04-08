-- Migration: 0035_name_components.sql
-- Purpose:
--   1) Add structured person-name component columns for participant entities.
--   2) Support downstream deterministic parsing/backfill of first/middle/last
--      with optional prefix/suffix.
--
-- Notes:
--   - This migration only introduces schema columns.
--   - Data backfill uses Python business rules in
--     scripts/backfill_name_components.py.

BEGIN;

ALTER TABLE participant
    ADD COLUMN IF NOT EXISTS middle_name text,
    ADD COLUMN IF NOT EXISTS name_prefix text,
    ADD COLUMN IF NOT EXISTS name_suffix text;

ALTER TABLE canonical_participant
    ADD COLUMN IF NOT EXISTS middle_name text,
    ADD COLUMN IF NOT EXISTS name_prefix text,
    ADD COLUMN IF NOT EXISTS name_suffix text;

COMMIT;
