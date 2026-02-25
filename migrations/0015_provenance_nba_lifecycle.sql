-- Migration: 0015_provenance_nba_lifecycle.sql
-- Purpose: Field-level provenance tracking + NBA lifecycle expansion.
--
--   A. canonical_attribute_provenance — one row per (canonical entity, attribute_name);
--      records which candidate won each attribute, at what score, and by which mechanism.
--
--   B. next_best_action lifecycle expansion:
--      - New status values: in_progress, done (in addition to open, dismissed, actioned)
--      - New ownership columns: assigned_to, claimed_at, actioned_at, actioned_by
--
-- Depends on: 0005_resolution_and_actions (next_best_action)
--             0012_canonical_tables (canonical_* tables)

-- ============================================================
-- A. canonical_attribute_provenance
-- ============================================================

CREATE TABLE canonical_attribute_provenance (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_entity_type text        NOT NULL
        CHECK (canonical_entity_type IN
            ('participant','yacht','event','registration','club')),
    canonical_entity_id   uuid        NOT NULL,
    attribute_name        text        NOT NULL,
    attribute_value       text,
    source_candidate_type text        NOT NULL,
    source_candidate_id   uuid        NOT NULL,
    source_score          numeric(5,4),
    rule_version          text,
    decided_by            text        NOT NULL
        CHECK (decided_by IN ('auto_promote','manual','merge')),
    decided_at            timestamptz NOT NULL DEFAULT now(),
    created_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (canonical_entity_type, canonical_entity_id, attribute_name)
);

CREATE INDEX idx_canonical_attr_prov_canonical
    ON canonical_attribute_provenance (canonical_entity_type, canonical_entity_id);

-- ============================================================
-- B. next_best_action lifecycle expansion
-- ============================================================

-- Drop existing status CHECK (any name, by content) then add expanded one.
-- Pattern matches migration 0013 approach.
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'next_best_action'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%status%'
    LOOP
        EXECUTE 'ALTER TABLE next_best_action DROP CONSTRAINT '
                || quote_ident(r.conname);
    END LOOP;
END;
$$;

-- New status values: open → in_progress → done (completion path)
--                   open | in_progress → dismissed (cancel)
--                   'actioned' preserved for backward compatibility (maps to done)
ALTER TABLE next_best_action
    ADD CONSTRAINT next_best_action_status_check
    CHECK (status IN ('open','in_progress','done','dismissed','actioned'));

-- Ownership columns for human-in-loop workflow tracking
ALTER TABLE next_best_action
    ADD COLUMN assigned_to  text,
    ADD COLUMN claimed_at   timestamptz,
    ADD COLUMN actioned_at  timestamptz,
    ADD COLUMN actioned_by  text;
