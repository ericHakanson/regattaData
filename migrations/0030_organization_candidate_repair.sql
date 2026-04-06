-- Migration: 0030_organization_candidate_repair.sql
-- Purpose: Remove organization-like entities from the participant candidate
--          pool and demote any promoted org-like canonicals (FOR-222).

BEGIN;

CREATE TEMP TABLE for_222_promoted_org_candidates (
    candidate_id uuid PRIMARY KEY,
    canonical_id uuid NOT NULL,
    canonical_link_count bigint NOT NULL
) ON COMMIT DROP;

INSERT INTO for_222_promoted_org_candidates
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
WHERE cp.is_promoted = true
  AND (
        COALESCE(cp.display_name, '') ~* '\m(yacht\s+club|sailing\s+club|boat\s+club|cruising\s+club|racing\s+club|coast\s+guard|yacht\s+squad|fleet|squadron|foundation|association|assoc\.?|society|team|committee|regatta|authority|district|university|college|school|academy|department|dept\.?|division|bureau|agency|international|national|state\s+of|town\s+of|city\s+of|charity|nonprofit|non-profit)\M'
     OR COALESCE(cp.normalized_name, '') ~* '\m(yacht\s+club|sailing\s+club|boat\s+club|cruising\s+club|racing\s+club|coast\s+guard|yacht\s+squad|fleet|squadron|foundation|association|assoc\.?|society|team|committee|regatta|authority|district|university|college|school|academy|department|dept\.?|division|bureau|agency|international|national|state\s+of|town\s+of|city\s+of|charity|nonprofit|non-profit)\M'
     OR COALESCE(cp.display_name, '') ~* '(^|[\s,])(l\.?l\.?c\.?|inc\.?|corp\.?|ltd\.?|llp\.?|lp\.?|p\.?c\.?|trust|estate)\.?$'
     OR COALESCE(cp.normalized_name, '') ~* '(^|[\s,])(l\.?l\.?c\.?|inc\.?|corp\.?|ltd\.?|llp\.?|lp\.?|p\.?c\.?|trust|estate)\.?$'
  );

DELETE FROM candidate_canonical_link ccl
USING for_222_promoted_org_candidates t
WHERE ccl.candidate_entity_type = 'participant'
  AND ccl.candidate_entity_id = t.candidate_id;

UPDATE candidate_participant cp
SET is_promoted = false,
    promoted_canonical_id = NULL,
    resolution_state = 'reject',
    confidence_reasons = CASE
        WHEN cp.confidence_reasons IS NULL
          OR cp.confidence_reasons = 'null'::jsonb
            THEN '["hard_block:organization_entity"]'::jsonb
        WHEN cp.confidence_reasons @> '["hard_block:organization_entity"]'::jsonb
            THEN cp.confidence_reasons
        ELSE cp.confidence_reasons || '["hard_block:organization_entity"]'::jsonb
    END
FROM for_222_promoted_org_candidates t
WHERE cp.id = t.candidate_id;

UPDATE canonical_registration cr
SET canonical_primary_participant_id = NULL
FROM for_222_promoted_org_candidates t
WHERE t.canonical_link_count = 1
  AND cr.canonical_primary_participant_id = t.canonical_id;

DELETE FROM canonical_attribute_provenance cap
USING for_222_promoted_org_candidates t
WHERE t.canonical_link_count = 1
  AND cap.canonical_entity_type = 'participant'
  AND cap.canonical_entity_id = t.canonical_id;

DELETE FROM canonical_participant canon
USING for_222_promoted_org_candidates t
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
    'for_222_organization_candidate_repair',
    'pipeline',
    'pipeline'
FROM for_222_promoted_org_candidates t;

CREATE TEMP TABLE for_222_reject_org_candidates (
    candidate_id uuid PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO for_222_reject_org_candidates (candidate_id)
SELECT cp.id
FROM candidate_participant cp
WHERE cp.is_promoted = false
  AND cp.resolution_state <> 'reject'
  AND (
        COALESCE(cp.display_name, '') ~* '\m(yacht\s+club|sailing\s+club|boat\s+club|cruising\s+club|racing\s+club|coast\s+guard|yacht\s+squad|fleet|squadron|foundation|association|assoc\.?|society|team|committee|regatta|authority|district|university|college|school|academy|department|dept\.?|division|bureau|agency|international|national|state\s+of|town\s+of|city\s+of|charity|nonprofit|non-profit)\M'
     OR COALESCE(cp.normalized_name, '') ~* '\m(yacht\s+club|sailing\s+club|boat\s+club|cruising\s+club|racing\s+club|coast\s+guard|yacht\s+squad|fleet|squadron|foundation|association|assoc\.?|society|team|committee|regatta|authority|district|university|college|school|academy|department|dept\.?|division|bureau|agency|international|national|state\s+of|town\s+of|city\s+of|charity|nonprofit|non-profit)\M'
     OR COALESCE(cp.display_name, '') ~* '(^|[\s,])(l\.?l\.?c\.?|inc\.?|corp\.?|ltd\.?|llp\.?|lp\.?|p\.?c\.?|trust|estate)\.?$'
     OR COALESCE(cp.normalized_name, '') ~* '(^|[\s,])(l\.?l\.?c\.?|inc\.?|corp\.?|ltd\.?|llp\.?|lp\.?|p\.?c\.?|trust|estate)\.?$'
  );

UPDATE candidate_participant cp
SET resolution_state = 'reject',
    confidence_reasons = CASE
        WHEN cp.confidence_reasons IS NULL
          OR cp.confidence_reasons = 'null'::jsonb
            THEN '["hard_block:organization_entity"]'::jsonb
        WHEN cp.confidence_reasons @> '["hard_block:organization_entity"]'::jsonb
            THEN cp.confidence_reasons
        ELSE cp.confidence_reasons || '["hard_block:organization_entity"]'::jsonb
    END
FROM for_222_reject_org_candidates t
WHERE cp.id = t.candidate_id;

INSERT INTO resolution_manual_action_log
    (entity_type, candidate_entity_id, canonical_entity_id,
     action_type, reason_code, actor, source)
SELECT
    'participant',
    t.candidate_id,
    NULL,
    'reject',
    'for_222_organization_candidate_repair',
    'pipeline',
    'pipeline'
FROM for_222_reject_org_candidates t;

COMMIT;
