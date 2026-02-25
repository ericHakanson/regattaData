-- Migration: 0017_index_additions.sql
-- Purpose: Add missing indexes for hot lookups introduced by lifecycle ops,
--          NBA cleanup, and lineage coverage queries.
--
-- New indexes:
--   idx_nba_target                         — NBA cleanup by candidate in demote/merge/unlink
--   idx_candidate_source_link_entity_type  — lineage coverage query by entity type
--
-- Existing indexes confirmed sufficient for new operations:
--   idx_candidate_canonical_link_canonical ON (candidate_entity_type, canonical_entity_id)
--       covers demote/unlink/merge reverse lookup (already exists in 0012)
--   idx_resolution_manual_action_canonical ON (canonical_entity_id)
--       covers audit lookups (already exists in 0011)
--
-- Depends on: 0005_resolution_and_actions (next_best_action)
--             0011_candidate_canonical_core (candidate_source_link)

-- Efficient NBA status lookup by target entity (used in _write_nbas, demote, merge, unlink)
CREATE INDEX idx_nba_target
    ON next_best_action (target_entity_type, target_entity_id, status);

-- Lineage query: candidate_source_link filtered by entity type
CREATE INDEX IF NOT EXISTS idx_candidate_source_link_entity_type
    ON candidate_source_link (candidate_entity_type);
