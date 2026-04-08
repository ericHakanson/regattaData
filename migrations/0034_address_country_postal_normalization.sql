-- Migration: 0034_address_country_postal_normalization.sql
-- Purpose: enforce storage-level normalization for address country/postal values.
--
-- Requirements addressed:
--   1) Country code stored as ISO alpha-2 where mappable (US/CA in particular).
--   2) US ZIP-like postal values normalized to 5-digit form, preserving leading 0.
--
-- Scope:
--   - participant_address
--   - candidate_participant_address
--   - canonical_participant_address
--   - manual_participant_address_patch
--
-- Notes:
--   - This migration intentionally does not rewrite address_raw to avoid accidental
--     uniqueness collisions on (candidate_participant_id, address_raw) surfaces.
--   - Canadian postal codes are canonicalized to "A1A 1A1" when parseable.

BEGIN;

WITH normalized AS (
    SELECT
        id,
        CASE
            WHEN NULLIF(BTRIM(country_code), '') IS NULL THEN
                CASE
                    WHEN UPPER(regexp_replace(COALESCE(postal_code, ''), '\s+', '', 'g')) ~ '^[A-Z]\d[A-Z]\d[A-Z]\d$' THEN 'CA'
                    WHEN regexp_replace(COALESCE(postal_code, ''), '\D', '', 'g') ~ '^(\d{4}|\d{5}|\d{9})$' THEN 'US'
                    ELSE NULL
                END
            WHEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA')
                THEN 'US'
            WHEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) IN ('CA', 'CAN', 'CANADA')
                THEN 'CA'
            WHEN LENGTH(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) = 2
                THEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g'))
            ELSE NULLIF(BTRIM(country_code), '')
        END AS country_code_norm
    FROM participant_address
),
normalized_postal AS (
    SELECT
        p.id,
        n.country_code_norm,
        CASE
            WHEN n.country_code_norm = 'CA' THEN
                CASE
                    WHEN UPPER(regexp_replace(COALESCE(p.postal_code, ''), '\s+', '', 'g')) ~ '^[A-Z]\d[A-Z]\d[A-Z]\d$'
                        THEN SUBSTR(UPPER(regexp_replace(p.postal_code, '\s+', '', 'g')), 1, 3)
                             || ' ' ||
                             SUBSTR(UPPER(regexp_replace(p.postal_code, '\s+', '', 'g')), 4, 3)
                    ELSE NULLIF(UPPER(BTRIM(p.postal_code)), '')
                END
            ELSE
                CASE
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{9}$'
                        THEN SUBSTR(regexp_replace(p.postal_code, '\D', '', 'g'), 1, 5)
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{5}$'
                        THEN regexp_replace(p.postal_code, '\D', '', 'g')
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{4}$'
                        THEN '0' || regexp_replace(p.postal_code, '\D', '', 'g')
                    ELSE NULLIF(BTRIM(p.postal_code), '')
                END
        END AS postal_code_norm
    FROM participant_address p
    JOIN normalized n ON n.id = p.id
)
UPDATE participant_address p
SET
    country_code = np.country_code_norm,
    postal_code = np.postal_code_norm
FROM normalized_postal np
WHERE np.id = p.id
  AND (
      p.country_code IS DISTINCT FROM np.country_code_norm
      OR p.postal_code IS DISTINCT FROM np.postal_code_norm
  );

WITH normalized AS (
    SELECT
        id,
        CASE
            WHEN NULLIF(BTRIM(country_code), '') IS NULL THEN
                CASE
                    WHEN UPPER(regexp_replace(COALESCE(postal_code, ''), '\s+', '', 'g')) ~ '^[A-Z]\d[A-Z]\d[A-Z]\d$' THEN 'CA'
                    WHEN regexp_replace(COALESCE(postal_code, ''), '\D', '', 'g') ~ '^(\d{4}|\d{5}|\d{9})$' THEN 'US'
                    ELSE NULL
                END
            WHEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA')
                THEN 'US'
            WHEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) IN ('CA', 'CAN', 'CANADA')
                THEN 'CA'
            WHEN LENGTH(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) = 2
                THEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g'))
            ELSE NULLIF(BTRIM(country_code), '')
        END AS country_code_norm
    FROM candidate_participant_address
),
normalized_postal AS (
    SELECT
        p.id,
        n.country_code_norm,
        CASE
            WHEN n.country_code_norm = 'CA' THEN
                CASE
                    WHEN UPPER(regexp_replace(COALESCE(p.postal_code, ''), '\s+', '', 'g')) ~ '^[A-Z]\d[A-Z]\d[A-Z]\d$'
                        THEN SUBSTR(UPPER(regexp_replace(p.postal_code, '\s+', '', 'g')), 1, 3)
                             || ' ' ||
                             SUBSTR(UPPER(regexp_replace(p.postal_code, '\s+', '', 'g')), 4, 3)
                    ELSE NULLIF(UPPER(BTRIM(p.postal_code)), '')
                END
            ELSE
                CASE
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{9}$'
                        THEN SUBSTR(regexp_replace(p.postal_code, '\D', '', 'g'), 1, 5)
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{5}$'
                        THEN regexp_replace(p.postal_code, '\D', '', 'g')
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{4}$'
                        THEN '0' || regexp_replace(p.postal_code, '\D', '', 'g')
                    ELSE NULLIF(BTRIM(p.postal_code), '')
                END
        END AS postal_code_norm
    FROM candidate_participant_address p
    JOIN normalized n ON n.id = p.id
)
UPDATE candidate_participant_address p
SET
    country_code = np.country_code_norm,
    postal_code = np.postal_code_norm
FROM normalized_postal np
WHERE np.id = p.id
  AND (
      p.country_code IS DISTINCT FROM np.country_code_norm
      OR p.postal_code IS DISTINCT FROM np.postal_code_norm
  );

WITH normalized AS (
    SELECT
        id,
        CASE
            WHEN NULLIF(BTRIM(country_code), '') IS NULL THEN
                CASE
                    WHEN UPPER(regexp_replace(COALESCE(postal_code, ''), '\s+', '', 'g')) ~ '^[A-Z]\d[A-Z]\d[A-Z]\d$' THEN 'CA'
                    WHEN regexp_replace(COALESCE(postal_code, ''), '\D', '', 'g') ~ '^(\d{4}|\d{5}|\d{9})$' THEN 'US'
                    ELSE NULL
                END
            WHEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA')
                THEN 'US'
            WHEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) IN ('CA', 'CAN', 'CANADA')
                THEN 'CA'
            WHEN LENGTH(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) = 2
                THEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g'))
            ELSE NULLIF(BTRIM(country_code), '')
        END AS country_code_norm
    FROM canonical_participant_address
),
normalized_postal AS (
    SELECT
        p.id,
        n.country_code_norm,
        CASE
            WHEN n.country_code_norm = 'CA' THEN
                CASE
                    WHEN UPPER(regexp_replace(COALESCE(p.postal_code, ''), '\s+', '', 'g')) ~ '^[A-Z]\d[A-Z]\d[A-Z]\d$'
                        THEN SUBSTR(UPPER(regexp_replace(p.postal_code, '\s+', '', 'g')), 1, 3)
                             || ' ' ||
                             SUBSTR(UPPER(regexp_replace(p.postal_code, '\s+', '', 'g')), 4, 3)
                    ELSE NULLIF(UPPER(BTRIM(p.postal_code)), '')
                END
            ELSE
                CASE
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{9}$'
                        THEN SUBSTR(regexp_replace(p.postal_code, '\D', '', 'g'), 1, 5)
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{5}$'
                        THEN regexp_replace(p.postal_code, '\D', '', 'g')
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{4}$'
                        THEN '0' || regexp_replace(p.postal_code, '\D', '', 'g')
                    ELSE NULLIF(BTRIM(p.postal_code), '')
                END
        END AS postal_code_norm
    FROM canonical_participant_address p
    JOIN normalized n ON n.id = p.id
)
UPDATE canonical_participant_address p
SET
    country_code = np.country_code_norm,
    postal_code = np.postal_code_norm
FROM normalized_postal np
WHERE np.id = p.id
  AND (
      p.country_code IS DISTINCT FROM np.country_code_norm
      OR p.postal_code IS DISTINCT FROM np.postal_code_norm
  );

WITH normalized AS (
    SELECT
        id,
        CASE
            WHEN NULLIF(BTRIM(country_code), '') IS NULL THEN
                CASE
                    WHEN UPPER(regexp_replace(COALESCE(postal_code, ''), '\s+', '', 'g')) ~ '^[A-Z]\d[A-Z]\d[A-Z]\d$' THEN 'CA'
                    WHEN regexp_replace(COALESCE(postal_code, ''), '\D', '', 'g') ~ '^(\d{4}|\d{5}|\d{9})$' THEN 'US'
                    ELSE NULL
                END
            WHEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA')
                THEN 'US'
            WHEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) IN ('CA', 'CAN', 'CANADA')
                THEN 'CA'
            WHEN LENGTH(regexp_replace(country_code, '[^A-Za-z]', '', 'g')) = 2
                THEN UPPER(regexp_replace(country_code, '[^A-Za-z]', '', 'g'))
            ELSE NULLIF(BTRIM(country_code), '')
        END AS country_code_norm
    FROM manual_participant_address_patch
),
normalized_postal AS (
    SELECT
        p.id,
        n.country_code_norm,
        CASE
            WHEN n.country_code_norm = 'CA' THEN
                CASE
                    WHEN UPPER(regexp_replace(COALESCE(p.postal_code, ''), '\s+', '', 'g')) ~ '^[A-Z]\d[A-Z]\d[A-Z]\d$'
                        THEN SUBSTR(UPPER(regexp_replace(p.postal_code, '\s+', '', 'g')), 1, 3)
                             || ' ' ||
                             SUBSTR(UPPER(regexp_replace(p.postal_code, '\s+', '', 'g')), 4, 3)
                    ELSE NULLIF(UPPER(BTRIM(p.postal_code)), '')
                END
            ELSE
                CASE
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{9}$'
                        THEN SUBSTR(regexp_replace(p.postal_code, '\D', '', 'g'), 1, 5)
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{5}$'
                        THEN regexp_replace(p.postal_code, '\D', '', 'g')
                    WHEN regexp_replace(COALESCE(p.postal_code, ''), '\D', '', 'g') ~ '^\d{4}$'
                        THEN '0' || regexp_replace(p.postal_code, '\D', '', 'g')
                    ELSE NULLIF(BTRIM(p.postal_code), '')
                END
        END AS postal_code_norm
    FROM manual_participant_address_patch p
    JOIN normalized n ON n.id = p.id
)
UPDATE manual_participant_address_patch p
SET
    country_code = np.country_code_norm,
    postal_code = np.postal_code_norm
FROM normalized_postal np
WHERE np.id = p.id
  AND (
      p.country_code IS DISTINCT FROM np.country_code_norm
      OR p.postal_code IS DISTINCT FROM np.postal_code_norm
  );

COMMIT;
