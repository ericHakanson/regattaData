-- Migration: 0032_mobile_contact_subtype_backfill.sql
-- Purpose: Backfill and normalize participant phone contact_subtype values so
--          mobile numbers surface correctly in downstream exports (FOR-219).

BEGIN;

CREATE OR REPLACE FUNCTION _normalize_phone_contact_subtype(subtype text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN subtype IS NULL THEN NULL
        WHEN lower(btrim(subtype)) IN ('mobile', 'cell', 'cellular', 'sms', 'text', 'text_message', 'primary_mobile')
            THEN 'mobile'
        WHEN lower(btrim(subtype)) IN ('home', 'primary_home')
            THEN 'home'
        WHEN lower(btrim(subtype)) IN ('work', 'office', 'business')
            THEN 'work'
        WHEN btrim(subtype) = '' THEN NULL
        ELSE lower(btrim(subtype))
    END
$$;

UPDATE participant_contact_point
SET contact_subtype = _normalize_phone_contact_subtype(contact_subtype)
WHERE contact_type = 'phone';

UPDATE candidate_participant_contact
SET contact_subtype = _normalize_phone_contact_subtype(contact_subtype)
WHERE contact_type = 'phone';

UPDATE canonical_participant_contact
SET contact_subtype = _normalize_phone_contact_subtype(contact_subtype)
WHERE contact_type = 'phone';

-- Backfill candidate-layer phone subtype from source-layer participant contacts.
UPDATE candidate_participant_contact cpc
SET contact_subtype = _normalize_phone_contact_subtype(pcp.contact_subtype)
FROM participant_contact_point pcp
WHERE cpc.contact_type = 'phone'
  AND cpc.source_table_name = 'participant_contact_point'
  AND cpc.source_row_pk = pcp.id::text
  AND _normalize_phone_contact_subtype(pcp.contact_subtype) IS NOT NULL
  AND COALESCE(cpc.contact_subtype, '') <> _normalize_phone_contact_subtype(pcp.contact_subtype);

-- For already-promoted canonicals, infer subtype from linked candidate child contacts
-- when the contact payload matches.
UPDATE canonical_participant_contact canon
SET contact_subtype = _normalize_phone_contact_subtype(cpc.contact_subtype)
FROM candidate_canonical_link ccl
JOIN candidate_participant_contact cpc
  ON cpc.candidate_participant_id = ccl.candidate_entity_id
 AND cpc.contact_type = 'phone'
WHERE ccl.candidate_entity_type = 'participant'
  AND canon.canonical_participant_id = ccl.canonical_entity_id
  AND canon.contact_type = cpc.contact_type
  AND canon.raw_value = cpc.raw_value
  AND COALESCE(canon.normalized_value, '') = COALESCE(cpc.normalized_value, '')
  AND _normalize_phone_contact_subtype(cpc.contact_subtype) IS NOT NULL
  AND COALESCE(canon.contact_subtype, '') <> _normalize_phone_contact_subtype(cpc.contact_subtype);

DROP FUNCTION _normalize_phone_contact_subtype(text);

COMMIT;
