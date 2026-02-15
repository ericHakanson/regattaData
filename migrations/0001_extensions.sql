-- Migration: 0001_extensions.sql
-- Purpose: Enable required extensions and create shared utility functions.
-- Ref: docs/runbooks/05-cloud-sql-migration-plan.md (Phase 1)
--      docs/architecture/cloud-sql-schema-spec.md (Notes for Implementation)

BEGIN;

-- UUID generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Case-insensitive text type for normalized keys
CREATE EXTENSION IF NOT EXISTS citext;

-- Shared trigger function: auto-set updated_at on row modification
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMIT;
