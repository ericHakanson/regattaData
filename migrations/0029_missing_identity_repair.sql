-- Migration: 0029_missing_identity_repair.sql
-- Purpose: Repair participant candidates/canonicals that have no usable name
--          signal (FOR-224).
--
-- What this migration does:
--   1. Demotes promoted participant candidates with no identity anchor back to
--      resolution_state='hold'.
--   2. Deletes their candidate_canonical_link row and clears promoted flags.
--   3. Deletes the canonical participant when the demoted candidate was its sole
--      link, mirroring lifecycle demote semantics.
--   4. Moves unpromoted nameless review/auto_promote candidates to 'hold'.
--   5. Appends hard_block:missing_name to confidence_reasons for all repaired rows.

BEGIN;

CREATE TEMP TABLE for_224_promoted_missing_identity (
    candidate_id uuid PRIMARY KEY,
    canonical_id uuid NOT NULL,
    canonical_link_count bigint NOT NULL
) ON COMMIT DROP;

INSERT INTO for_224_promoted_missing_identity
    (candidate_id, canonical_id, canonical_link_count)
SELECT
    cp.id,
    canonical_target.canonical_id,
    canonical_target.link_count
FROM candidate_participant cp
JOIN LATERAL (
    SELECT
        COALESCE(ccl.canonical_entity_id, cp.promoted_canonical_id) AS canonical_id,
        (
            SELECT COUNT(*)
            FROM candidate_canonical_link ccl2
            WHERE ccl2.candidate_entity_type = 'participant'
              AND ccl2.canonical_entity_id = COALESCE(ccl.canonical_entity_id, cp.promoted_canonical_id)
        ) AS link_count
    FROM candidate_canonical_link ccl
    WHERE ccl.candidate_entity_type = 'participant'
      AND ccl.candidate_entity_id = cp.id
    UNION ALL
    SELECT
        cp.promoted_canonical_id,
        0
    WHERE cp.promoted_canonical_id IS NOT NULL
      AND NOT EXISTS (
            SELECT 1
            FROM candidate_canonical_link ccl3
            WHERE ccl3.candidate_entity_type = 'participant'
              AND ccl3.candidate_entity_id = cp.id
      )
    LIMIT 1
) AS canonical_target ON TRUE
LEFT JOIN canonical_participant canon
  ON canon.id = canonical_target.canonical_id
WHERE cp.is_promoted = true
  AND (
        COALESCE(NULLIF(BTRIM(cp.normalized_name), ''), NULL) IS NULL
     OR (
            canon.id IS NOT NULL
        AND COALESCE(NULLIF(BTRIM(canon.first_name), ''), NULL) IS NULL
        AND COALESCE(NULLIF(BTRIM(canon.last_name), ''), NULL) IS NULL
     )
  );

DELETE FROM candidate_canonical_link ccl
USING for_224_promoted_missing_identity t
WHERE ccl.candidate_entity_type = 'participant'
  AND ccl.candidate_entity_id = t.candidate_id;

UPDATE candidate_participant cp
SET is_promoted = false,
    promoted_canonical_id = NULL,
    resolution_state = 'hold',
    confidence_reasons = CASE
        WHEN cp.confidence_reasons IS NULL
          OR cp.confidence_reasons = 'null'::jsonb
            THEN '["hard_block:missing_name"]'::jsonb
        WHEN cp.confidence_reasons @> '["hard_block:missing_name"]'::jsonb
            THEN cp.confidence_reasons
        ELSE cp.confidence_reasons || '["hard_block:missing_name"]'::jsonb
    END
FROM for_224_promoted_missing_identity t
WHERE cp.id = t.candidate_id;

UPDATE canonical_registration cr
SET canonical_primary_participant_id = NULL
FROM for_224_promoted_missing_identity t
WHERE t.canonical_link_count = 1
  AND cr.canonical_primary_participant_id = t.canonical_id;

DELETE FROM canonical_attribute_provenance cap
USING for_224_promoted_missing_identity t
WHERE t.canonical_link_count = 1
  AND cap.canonical_entity_type = 'participant'
  AND cap.canonical_entity_id = t.canonical_id;

DELETE FROM canonical_participant canon
USING for_224_promoted_missing_identity t
WHERE t.canonical_link_count = 1
  AND canon.id = t.canonical_id;

INSERT INTO resolution_manual_action_log
    (entity_type, candidate_entity_id, canonical_entity_id,
     action_type, reason_code, actor, source)
SELECT
    'participant',
    t.candidate_id,
    CASE WHEN t.canonical_link_count = 1 THEN NULL ELSE t.canonical_id END,
    'demote',
    'for_224_missing_identity_repair',
    'pipeline',
    'pipeline'
FROM for_224_promoted_missing_identity t;

CREATE TEMP TABLE for_224_hold_missing_identity (
    candidate_id uuid PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO for_224_hold_missing_identity (candidate_id)
SELECT cp.id
FROM candidate_participant cp
WHERE cp.is_promoted = false
  AND cp.resolution_state IN ('review', 'auto_promote')
  AND COALESCE(NULLIF(BTRIM(cp.normalized_name), ''), NULL) IS NULL;

UPDATE candidate_participant cp
SET resolution_state = 'hold',
    confidence_reasons = CASE
        WHEN cp.confidence_reasons IS NULL
          OR cp.confidence_reasons = 'null'::jsonb
            THEN '["hard_block:missing_name"]'::jsonb
        WHEN cp.confidence_reasons @> '["hard_block:missing_name"]'::jsonb
            THEN cp.confidence_reasons
        ELSE cp.confidence_reasons || '["hard_block:missing_name"]'::jsonb
    END
FROM for_224_hold_missing_identity t
WHERE cp.id = t.candidate_id;

INSERT INTO resolution_manual_action_log
    (entity_type, candidate_entity_id, canonical_entity_id,
     action_type, reason_code, actor, source)
SELECT
    'participant',
    t.candidate_id,
    NULL,
    'hold',
    'for_224_missing_identity_repair',
    'pipeline',
    'pipeline'
FROM for_224_hold_missing_identity t;

COMMIT;
