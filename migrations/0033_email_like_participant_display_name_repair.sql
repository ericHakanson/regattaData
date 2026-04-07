-- Migration: 0033_email_like_participant_display_name_repair.sql
-- Purpose: Remove email-like participant display-name artifacts that survived
--          the earlier FOR-230 repair path and rebuild candidate/canonical
--          names from cleaned participant rows.

BEGIN;

WITH participant_clean_base AS (
    SELECT
        id,
        CASE
            WHEN NULLIF(BTRIM(first_name), '') ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN NULL
            ELSE NULLIF(BTRIM(first_name), '')
        END AS clean_first_name,
        CASE
            WHEN NULLIF(BTRIM(last_name), '') ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN NULL
            ELSE NULLIF(BTRIM(last_name), '')
        END AS clean_last_name,
        NULLIF(BTRIM(full_name), '') AS raw_full_name
    FROM participant
),
participant_clean AS (
    SELECT
        id,
        clean_first_name,
        clean_last_name,
        CASE
            WHEN NULLIF(CONCAT_WS(' ', clean_first_name, clean_last_name), '') IS NOT NULL
                THEN NULLIF(CONCAT_WS(' ', clean_first_name, clean_last_name), '')
            WHEN raw_full_name IS NOT NULL
             AND raw_full_name NOT LIKE '%@%'
             AND raw_full_name !~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
                THEN raw_full_name
            ELSE raw_full_name
        END AS clean_full_name
    FROM participant_clean_base
),
participant_prepared AS (
    SELECT
        id,
        clean_first_name,
        clean_last_name,
        clean_full_name,
        CASE
            WHEN clean_full_name IS NULL THEN NULL
            ELSE NULLIF(
                BTRIM(
                    regexp_replace(
                        lower(
                            regexp_replace(
                                clean_full_name,
                                '[^[:alnum:]_[:space:]]',
                                '',
                                'g'
                            )
                        ),
                        '\s+',
                        ' ',
                        'g'
                    )
                ),
                ''
            )
        END AS clean_normalized_full_name
    FROM participant_clean
)
UPDATE participant p
SET first_name = pp.clean_first_name,
    last_name = pp.clean_last_name,
    full_name = pp.clean_full_name,
    normalized_full_name = pp.clean_normalized_full_name
FROM participant_prepared pp
WHERE p.id = pp.id
  AND (
      p.first_name IS DISTINCT FROM pp.clean_first_name
      OR p.last_name IS DISTINCT FROM pp.clean_last_name
      OR p.full_name IS DISTINCT FROM pp.clean_full_name
      OR p.normalized_full_name IS DISTINCT FROM pp.clean_normalized_full_name
  );

UPDATE candidate_participant
SET display_name = NULL,
    normalized_name = NULL
WHERE COALESCE(display_name, '') LIKE '%@%';

WITH candidate_name_rollup AS (
    SELECT
        csl.candidate_entity_id AS candidate_id,
        MIN(NULLIF(BTRIM(p.first_name), '')) FILTER (
            WHERE NULLIF(BTRIM(p.first_name), '') IS NOT NULL
              AND NULLIF(BTRIM(p.first_name), '') !~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
        ) AS first_name,
        MIN(NULLIF(BTRIM(p.last_name), '')) FILTER (
            WHERE NULLIF(BTRIM(p.last_name), '') IS NOT NULL
              AND NULLIF(BTRIM(p.last_name), '') !~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
        ) AS last_name,
        MIN(NULLIF(BTRIM(p.full_name), '')) FILTER (
            WHERE NULLIF(BTRIM(p.full_name), '') IS NOT NULL
              AND NULLIF(BTRIM(p.full_name), '') NOT LIKE '%@%'
              AND NULLIF(BTRIM(p.full_name), '') !~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
        ) AS full_name
    FROM candidate_source_link csl
    JOIN participant p
      ON p.id::text = csl.source_row_pk
    WHERE csl.candidate_entity_type = 'participant'
      AND csl.source_table_name = 'participant'
    GROUP BY csl.candidate_entity_id
),
candidate_prepared AS (
    SELECT
        candidate_id,
        COALESCE(
            NULLIF(CONCAT_WS(' ', first_name, last_name), ''),
            full_name
        ) AS display_name
    FROM candidate_name_rollup
),
candidate_normalized AS (
    SELECT
        candidate_id,
        display_name,
        CASE
            WHEN display_name IS NULL THEN NULL
            ELSE NULLIF(
                BTRIM(
                    regexp_replace(
                        lower(
                            regexp_replace(
                                display_name,
                                '[^[:alnum:]_[:space:]]',
                                '',
                                'g'
                            )
                        ),
                        '\s+',
                        ' ',
                        'g'
                    )
                ),
                ''
            )
        END AS normalized_name
    FROM candidate_prepared
)
UPDATE candidate_participant cp
SET display_name = cn.display_name,
    normalized_name = cn.normalized_name
FROM candidate_normalized cn
WHERE cp.id = cn.candidate_id
  AND (
      cp.display_name IS DISTINCT FROM cn.display_name
      OR cp.normalized_name IS DISTINCT FROM cn.normalized_name
  );

UPDATE canonical_participant
SET display_name = NULL,
    normalized_name = NULL
WHERE COALESCE(display_name, '') LIKE '%@%';

UPDATE canonical_participant
SET first_name = NULL
WHERE first_name IS NOT NULL
  AND first_name ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$';

UPDATE canonical_participant
SET last_name = NULL
WHERE last_name IS NOT NULL
  AND last_name ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$';

WITH canonical_name_rollup AS (
    SELECT
        ccl.canonical_entity_id AS canonical_id,
        MIN(cp.display_name) FILTER (
            WHERE cp.display_name IS NOT NULL
              AND cp.display_name NOT LIKE '%@%'
        ) AS display_name,
        MIN(cp.normalized_name) FILTER (
            WHERE cp.display_name IS NOT NULL
              AND cp.display_name NOT LIKE '%@%'
        ) AS normalized_name
    FROM candidate_canonical_link ccl
    JOIN candidate_participant cp
      ON cp.id = ccl.candidate_entity_id
    WHERE ccl.candidate_entity_type = 'participant'
    GROUP BY ccl.canonical_entity_id
),
canonical_prepared AS (
    SELECT
        canonical_id,
        display_name,
        normalized_name,
        CASE
            WHEN display_name IS NULL THEN NULL
            WHEN display_name LIKE '%,%' THEN NULLIF(BTRIM(split_part(display_name, ',', 2)), '')
            WHEN strpos(display_name, ' ') > 0 THEN NULLIF(BTRIM(regexp_replace(display_name, '\s+\S+\s*$', '')), '')
            ELSE display_name
        END AS first_name_guess,
        CASE
            WHEN display_name IS NULL THEN NULL
            WHEN display_name LIKE '%,%' THEN NULLIF(BTRIM(split_part(display_name, ',', 1)), '')
            WHEN strpos(display_name, ' ') > 0 THEN NULLIF(BTRIM(regexp_replace(display_name, '^.*\s', '')), '')
            ELSE NULL
        END AS last_name_guess
    FROM canonical_name_rollup
)
UPDATE canonical_participant cp
SET display_name = cnp.display_name,
    normalized_name = cnp.normalized_name,
    first_name = cnp.first_name_guess,
    last_name = cnp.last_name_guess
FROM canonical_prepared cnp
WHERE cp.id = cnp.canonical_id
  AND (
      cp.display_name IS DISTINCT FROM cnp.display_name
      OR cp.normalized_name IS DISTINCT FROM cnp.normalized_name
      OR cp.first_name IS DISTINCT FROM cnp.first_name_guess
      OR cp.last_name IS DISTINCT FROM cnp.last_name_guess
  );

COMMIT;
