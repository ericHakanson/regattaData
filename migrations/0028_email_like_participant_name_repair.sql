-- Migration: 0028_email_like_participant_name_repair.sql
-- Purpose: Repair participant/candidate/canonical person-name fields that
--          currently store email-like values (FOR-230).

BEGIN;

WITH cleaned_participant AS (
    SELECT
        id,
        CASE
            WHEN NULLIF(BTRIM(first_name), '') ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN NULL
            ELSE NULLIF(BTRIM(first_name), '')
        END AS clean_first_name,
        CASE
            WHEN NULLIF(BTRIM(last_name), '') ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN NULL
            ELSE NULLIF(BTRIM(last_name), '')
        END AS clean_last_name
    FROM participant
)
UPDATE participant p
SET first_name = c.clean_first_name,
    last_name = c.clean_last_name,
    full_name = COALESCE(
        NULLIF(CONCAT_WS(' ', c.clean_first_name, c.clean_last_name), ''),
        p.full_name
    ),
    normalized_full_name = COALESCE(
        NULLIF(
            BTRIM(
                regexp_replace(
                    lower(
                        regexp_replace(
                            CONCAT_WS(' ', c.clean_first_name, c.clean_last_name),
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
        ),
        p.normalized_full_name
    )
FROM cleaned_participant c
WHERE p.id = c.id
  AND (
      p.first_name IS DISTINCT FROM c.clean_first_name
      OR p.last_name IS DISTINCT FROM c.clean_last_name
      OR (
          NULLIF(CONCAT_WS(' ', c.clean_first_name, c.clean_last_name), '') IS NOT NULL
          AND (
              p.full_name IS DISTINCT FROM NULLIF(CONCAT_WS(' ', c.clean_first_name, c.clean_last_name), '')
              OR p.normalized_full_name IS DISTINCT FROM NULLIF(
                    BTRIM(
                        regexp_replace(
                            lower(
                                regexp_replace(
                                    CONCAT_WS(' ', c.clean_first_name, c.clean_last_name),
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
          )
      )
  );

UPDATE candidate_participant
SET display_name = NULL
WHERE display_name IS NOT NULL
  AND display_name LIKE '%@%';

UPDATE candidate_participant
SET normalized_name = NULL
WHERE normalized_name IS NOT NULL
  AND normalized_name LIKE '%@%';

WITH candidate_names AS (
    SELECT
        csl.candidate_entity_id AS candidate_id,
        MIN(NULLIF(BTRIM(p.first_name), '')) FILTER (
            WHERE NULLIF(BTRIM(p.first_name), '') IS NOT NULL
              AND NULLIF(BTRIM(p.first_name), '') !~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
        ) AS first_name,
        MIN(NULLIF(BTRIM(p.last_name), '')) FILTER (
            WHERE NULLIF(BTRIM(p.last_name), '') IS NOT NULL
              AND NULLIF(BTRIM(p.last_name), '') !~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
        ) AS last_name
    FROM candidate_source_link csl
    JOIN participant p
      ON p.id::text = csl.source_row_pk
    WHERE csl.candidate_entity_type = 'participant'
      AND csl.source_table_name = 'participant'
    GROUP BY csl.candidate_entity_id
),
candidate_names_prepared AS (
    SELECT
        candidate_id,
        NULLIF(CONCAT_WS(' ', first_name, last_name), '') AS display_name,
        NULLIF(
            BTRIM(
                regexp_replace(
                    lower(
                        regexp_replace(
                            CONCAT_WS(' ', first_name, last_name),
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
        ) AS normalized_name
    FROM candidate_names
)
UPDATE candidate_participant cp
SET display_name = COALESCE(cnp.display_name, cp.display_name),
    normalized_name = COALESCE(cnp.normalized_name, cp.normalized_name)
FROM candidate_names_prepared cnp
WHERE cp.id = cnp.candidate_id
  AND (
      cp.display_name IS DISTINCT FROM COALESCE(cnp.display_name, cp.display_name)
      OR cp.normalized_name IS DISTINCT FROM COALESCE(cnp.normalized_name, cp.normalized_name)
  );

UPDATE canonical_participant
SET first_name = NULL
WHERE first_name IS NOT NULL
  AND first_name ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$';

UPDATE canonical_participant
SET last_name = NULL
WHERE last_name IS NOT NULL
  AND last_name ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$';

UPDATE canonical_participant
SET display_name = NULL
WHERE display_name IS NOT NULL
  AND display_name LIKE '%@%';

UPDATE canonical_participant
SET normalized_name = NULL
WHERE normalized_name IS NOT NULL
  AND normalized_name LIKE '%@%';

WITH canonical_names AS (
    SELECT
        ccl.canonical_entity_id AS canonical_id,
        MIN(cp.display_name) FILTER (
            WHERE cp.display_name IS NOT NULL
              AND cp.display_name NOT LIKE '%@%'
        ) AS display_name,
        MIN(cp.normalized_name) FILTER (
            WHERE cp.normalized_name IS NOT NULL
              AND cp.normalized_name NOT LIKE '%@%'
        ) AS normalized_name
    FROM candidate_canonical_link ccl
    JOIN candidate_participant cp
      ON cp.id = ccl.candidate_entity_id
    WHERE ccl.candidate_entity_type = 'participant'
    GROUP BY ccl.canonical_entity_id
),
canonical_names_prepared AS (
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
    FROM canonical_names
)
UPDATE canonical_participant cp
SET display_name = COALESCE(cnp.display_name, cp.display_name),
    normalized_name = COALESCE(cnp.normalized_name, cp.normalized_name),
    first_name = CASE
        WHEN cnp.first_name_guess ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN NULL
        ELSE COALESCE(cnp.first_name_guess, cp.first_name)
    END,
    last_name = CASE
        WHEN cnp.last_name_guess ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN NULL
        ELSE COALESCE(cnp.last_name_guess, cp.last_name)
    END
FROM canonical_names_prepared cnp
WHERE cp.id = cnp.canonical_id
  AND (
      cp.display_name IS DISTINCT FROM COALESCE(cnp.display_name, cp.display_name)
      OR cp.normalized_name IS DISTINCT FROM COALESCE(cnp.normalized_name, cp.normalized_name)
      OR cp.first_name IS DISTINCT FROM CASE
            WHEN cnp.first_name_guess ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN NULL
            ELSE COALESCE(cnp.first_name_guess, cp.first_name)
        END
      OR cp.last_name IS DISTINCT FROM CASE
            WHEN cnp.last_name_guess ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN NULL
            ELSE COALESCE(cnp.last_name_guess, cp.last_name)
        END
  );

COMMIT;
