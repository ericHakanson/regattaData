-- Migration: 0027_sourceless_candidate_repair.sql
-- Purpose: Follow up FOR-229 by repairing candidates stranded without any
--          candidate_source_link provenance after source-link dedupe.
--
-- Why this exists:
--   0025 correctly established exclusive source ownership on
--   (candidate_entity_type, source_table_name, source_row_pk), but candidates
--   that lost their only source link were left in an invalid state.
--   A candidate with zero source links must not remain promoted/reviewable.
--
-- What this migration does:
--   1. Adds candidates_without_source_links to lineage_coverage_snapshot so the
--      profiling/reporting layer can assert on this condition.
--   2. Demotes/rejects any candidate_* row with zero candidate_source_link rows.
--   3. Deletes stale candidate_canonical_link rows for those candidates.
--   4. Writes resolution_manual_action_log entries using existing governance
--      semantics:
--         - promoted candidates -> action_type='unlink'
--         - non-promoted candidates -> action_type='reject'
--
-- Note:
--   This migration repairs invalid candidate/canonical state. It does not
--   attempt to recreate deleted source-link rows; recovering historical lineage
--   from backup/WAL remains an operator task when needed.

BEGIN;

ALTER TABLE lineage_coverage_snapshot
    ADD COLUMN IF NOT EXISTS candidates_without_source_links bigint NOT NULL DEFAULT 0;

CREATE TEMP TABLE for_229_sourceless_candidates (
    entity_type          text NOT NULL,
    candidate_id         uuid NOT NULL,
    canonical_entity_id  uuid,
    is_promoted          boolean NOT NULL,
    resolution_state     text NOT NULL
) ON COMMIT DROP;

DO $$
DECLARE
    etype text;
    tbl   text;
BEGIN
    FOREACH etype IN ARRAY ARRAY['participant','yacht','club','event','registration'] LOOP
        tbl := 'candidate_' || etype;

        TRUNCATE for_229_sourceless_candidates;

        EXECUTE format(
            $q$
            INSERT INTO for_229_sourceless_candidates
                (entity_type, candidate_id, canonical_entity_id, is_promoted, resolution_state)
            SELECT
                %L,
                c.id,
                COALESCE(c.promoted_canonical_id, ccl.canonical_entity_id),
                c.is_promoted,
                c.resolution_state
            FROM %I c
            LEFT JOIN candidate_canonical_link ccl
              ON ccl.candidate_entity_type = %L
             AND ccl.candidate_entity_id = c.id
            WHERE NOT EXISTS (
                SELECT 1
                FROM candidate_source_link csl
                WHERE csl.candidate_entity_type = %L
                  AND csl.candidate_entity_id = c.id
            )
            $q$,
            etype, tbl, etype, etype
        );

        DELETE FROM candidate_canonical_link ccl
        USING for_229_sourceless_candidates t
        WHERE t.entity_type = etype
          AND ccl.candidate_entity_type = t.entity_type
          AND ccl.candidate_entity_id = t.candidate_id;

        INSERT INTO resolution_manual_action_log
            (entity_type, candidate_entity_id, canonical_entity_id,
             action_type, reason_code, actor, source)
        SELECT
            t.entity_type,
            t.candidate_id,
            t.canonical_entity_id,
            'unlink',
            'for_229_sourceless_candidate_repair',
            'pipeline',
            'pipeline'
        FROM for_229_sourceless_candidates t
        WHERE t.is_promoted = true
           OR t.canonical_entity_id IS NOT NULL;

        INSERT INTO resolution_manual_action_log
            (entity_type, candidate_entity_id, canonical_entity_id,
             action_type, reason_code, actor, source)
        SELECT
            t.entity_type,
            t.candidate_id,
            NULL,
            'reject',
            'for_229_sourceless_candidate_repair',
            'pipeline',
            'pipeline'
        FROM for_229_sourceless_candidates t
        WHERE t.is_promoted = false
          AND t.canonical_entity_id IS NULL
          AND t.resolution_state <> 'reject';

        EXECUTE format(
            $q$
            UPDATE %I c
            SET is_promoted = false,
                promoted_canonical_id = NULL,
                resolution_state = 'reject'
            FROM for_229_sourceless_candidates t
            WHERE t.entity_type = %L
              AND t.candidate_id = c.id
              AND (
                    c.is_promoted = true
                 OR c.promoted_canonical_id IS NOT NULL
                 OR c.resolution_state <> 'reject'
              )
            $q$,
            tbl, etype
        );
    END LOOP;
END;
$$;

COMMIT;
