-- Migration: 0025_integrity_constraints.sql
-- Purpose: Fix the FOR-220 / FOR-221 integrity regressions without breaking
--          legitimate lifecycle-merge state.
--
--   FOR-220 — Canonical ID collisions
--     Promotion-time misuse of stored_canonical_id could leave candidates in a
--     promoted state pointing at canonicals already claimed elsewhere.
--     Cleanup here is conservative:
--       1. preserve valid multi-candidate canonicals created by audited merge/split ops
--       2. collapse only unaudited duplicate candidate_canonical_link rows
--       3. reset any candidate row whose promoted_canonical_id no longer has a
--          matching bridge row back to resolution_state='review'
--
--   FOR-221 — Source record shared by multiple candidates
--     candidate_source_link now becomes exclusive ownership provenance on
--     (candidate_entity_type, source_table_name, source_row_pk).
--     Before deduplicating old BHYC rows, household/member mentions are backfilled
--     into bhyc_household_candidate_evidence so that household semantics remain
--     representable without shared source-row ownership.
--
-- Depends on: 0011_candidate_canonical_core, 0012_canonical_tables,
--             0018_bhyc_member_directory_tables

BEGIN;

-- ============================================================
-- BHYC explicit household evidence
-- ============================================================

CREATE TABLE IF NOT EXISTS bhyc_household_candidate_evidence (
    id                       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system            text        NOT NULL DEFAULT 'bhyc_member_directory',
    bhyc_member_raw_row_id   uuid        NOT NULL REFERENCES bhyc_member_raw_row (id) ON DELETE CASCADE,
    member_id                text        NOT NULL,
    relationship_label       text        NOT NULL,
    participant_id           uuid        NOT NULL REFERENCES participant (id),
    candidate_participant_id uuid        NOT NULL REFERENCES candidate_participant (id) ON DELETE CASCADE,
    created_at               timestamptz NOT NULL DEFAULT now(),
    last_seen_at             timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bhyc_household_candidate_evidence_unique
    ON bhyc_household_candidate_evidence (
        source_system,
        bhyc_member_raw_row_id,
        candidate_participant_id,
        relationship_label
    );

CREATE INDEX IF NOT EXISTS idx_bhyc_household_candidate_evidence_candidate
    ON bhyc_household_candidate_evidence (candidate_participant_id);

-- Backfill household evidence before source-link dedupe removes the old
-- relationship-tagged duplicate links.
INSERT INTO bhyc_household_candidate_evidence
    (source_system, bhyc_member_raw_row_id, member_id, relationship_label,
     participant_id, candidate_participant_id)
SELECT DISTINCT
    'bhyc_member_directory',
    csl.source_row_pk::uuid,
    bmr.member_id,
    csl.link_reason->>'relationship',
    bxp.participant_id,
    csl.candidate_entity_id
FROM candidate_source_link csl
JOIN bhyc_member_raw_row bmr
  ON bmr.id::text = csl.source_row_pk
JOIN bhyc_member_xref_participant bxp
  ON bxp.source_system = 'bhyc_member_directory'
 AND bxp.member_id = bmr.member_id
 AND COALESCE(bxp.relationship_label, '') = COALESCE(csl.link_reason->>'relationship', '')
WHERE csl.candidate_entity_type = 'participant'
  AND csl.source_table_name = 'bhyc_member_raw_row'
  AND csl.link_reason ? 'relationship'
ON CONFLICT (source_system, bhyc_member_raw_row_id, candidate_participant_id, relationship_label)
DO UPDATE SET
    participant_id = EXCLUDED.participant_id,
    last_seen_at = now();

-- ============================================================
-- FOR-220: Clean up unaudited canonical collisions only
-- ============================================================

WITH duplicate_groups AS (
    SELECT
        candidate_entity_type,
        canonical_entity_id
    FROM candidate_canonical_link
    GROUP BY candidate_entity_type, canonical_entity_id
    HAVING COUNT(*) > 1
),
unaudited_duplicate_links AS (
    SELECT
        ccl.id,
        ROW_NUMBER() OVER (
            PARTITION BY ccl.candidate_entity_type, ccl.canonical_entity_id
            ORDER BY ccl.promoted_at ASC, ccl.id ASC
        ) AS rn
    FROM candidate_canonical_link ccl
    JOIN duplicate_groups dg
      ON dg.candidate_entity_type = ccl.candidate_entity_type
     AND dg.canonical_entity_id = ccl.canonical_entity_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM resolution_manual_action_log rmal
        WHERE rmal.entity_type = ccl.candidate_entity_type
          AND rmal.canonical_entity_id = ccl.canonical_entity_id
          AND rmal.action_type IN ('merge', 'split')
    )
)
DELETE FROM candidate_canonical_link
WHERE id IN (
    SELECT id
    FROM unaudited_duplicate_links
    WHERE rn > 1
);

-- Walk back any candidate row that still claims promotion but no longer has a
-- matching bridge row. This also repairs stale promoted_canonical_id values.
DO $$
DECLARE
    etype text;
    tbl   text;
BEGIN
    FOREACH etype IN ARRAY ARRAY['participant','yacht','club','event','registration'] LOOP
        tbl := 'candidate_' || etype;
        EXECUTE format(
            $q$
            UPDATE %I AS c
            SET is_promoted = false,
                resolution_state = 'review',
                promoted_canonical_id = NULL
            WHERE c.is_promoted = true
              AND c.promoted_canonical_id IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM candidate_canonical_link ccl
                    WHERE ccl.candidate_entity_type = %L
                      AND ccl.candidate_entity_id = c.id
                      AND ccl.canonical_entity_id = c.promoted_canonical_id
              )
            $q$,
            tbl, etype
        );
    END LOOP;
END;
$$;

-- ============================================================
-- FOR-221: Deduplicate candidate_source_link + add UNIQUE constraint
-- ============================================================

DELETE FROM candidate_source_link
WHERE id IN (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY candidate_entity_type, source_table_name, source_row_pk
                   ORDER BY created_at ASC, id ASC
               ) AS rn
        FROM candidate_source_link
    ) ranked
    WHERE rn > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_source_link_source_unique
    ON candidate_source_link (candidate_entity_type, source_table_name, source_row_pk);

COMMIT;
