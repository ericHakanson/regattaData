-- Direct-mail owner/participant mailing-address export.
--
-- Purpose:
--   Build a near-term campaign list from candidate participant evidence, not
--   only promoted canonicals. Most usable non-BHYC mailing evidence currently
--   lives in candidate_participant_address while the candidate is still hold.
--
-- Audience tiers:
--   owner      - explicit owner/co-owner role evidence, including operational
--                RegattaMan/BHYC yacht_ownership bridged through participant
--   skipper    - skipper role evidence, no owner role
--   registrant - registrant role evidence, no owner/skipper role
--
-- Address rule:
--   require structured line1 + postal_code. Location-only yacht-scoring rows
--   such as "Salem MA USA" are excluded because they are not mailable.
--
-- Lineage rule:
--   require at least one candidate_source_link. Rows made sourceless by earlier
--   cleanup are excluded from the default campaign list until lineage is repaired.

WITH participant_bridge AS (
    SELECT DISTINCT
        csl.candidate_entity_id AS candidate_id,
        csl.source_row_pk::uuid AS participant_id
    FROM candidate_source_link csl
    WHERE csl.candidate_entity_type = 'participant'
      AND csl.source_table_name = 'participant'
),
role_evidence AS (
    SELECT
        r.candidate_participant_id AS candidate_id,
        r.role,
        r.role = 'owner' AS is_owner_role,
        r.role = 'skipper' AS is_skipper_role,
        r.role = 'registrant' AS is_registrant_role
    FROM candidate_participant_role_assignment r
    WHERE r.role IN ('owner', 'skipper', 'registrant')

    UNION ALL

    SELECT DISTINCT
        pb.candidate_id,
        yo.role,
        yo.role IN ('owner', 'co_owner') AS is_owner_role,
        FALSE AS is_skipper_role,
        FALSE AS is_registrant_role
    FROM participant_bridge pb
    JOIN yacht_ownership yo
      ON yo.participant_id = pb.participant_id
    WHERE yo.role IN ('owner', 'co_owner')
),
role_rollup AS (
    SELECT
        r.candidate_id,
        BOOL_OR(r.is_owner_role) AS has_owner_role,
        BOOL_OR(r.is_skipper_role) AS has_skipper_role,
        BOOL_OR(r.is_registrant_role) AS has_registrant_role,
        STRING_AGG(DISTINCT r.role, ' | ' ORDER BY r.role) AS roles
    FROM role_evidence r
    GROUP BY r.candidate_id
),
address_options AS (
    SELECT
        a.candidate_participant_id AS candidate_id,
        a.address_raw,
        a.line1,
        a.line2,
        a.city,
        a.state,
        CASE
            WHEN regexp_replace(COALESCE(a.postal_code, ''), '\D', '', 'g') ~ '^\d{9}$'
                THEN SUBSTR(regexp_replace(a.postal_code, '\D', '', 'g'), 1, 5)
            WHEN regexp_replace(COALESCE(a.postal_code, ''), '\D', '', 'g') ~ '^\d{5}$'
                THEN regexp_replace(a.postal_code, '\D', '', 'g')
            WHEN regexp_replace(COALESCE(a.postal_code, ''), '\D', '', 'g') ~ '^\d{4}$'
                THEN '0' || regexp_replace(a.postal_code, '\D', '', 'g')
            ELSE NULLIF(BTRIM(a.postal_code), '')
        END AS postal_code,
        COALESCE(
            CASE
                WHEN NULLIF(BTRIM(a.country_code), '') IS NULL THEN NULL
                WHEN UPPER(regexp_replace(a.country_code, '[^A-Za-z]', '', 'g')) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA')
                    THEN 'US'
                WHEN UPPER(regexp_replace(a.country_code, '[^A-Za-z]', '', 'g')) IN ('CA', 'CAN', 'CANADA')
                    THEN 'CA'
                WHEN LENGTH(regexp_replace(a.country_code, '[^A-Za-z]', '', 'g')) = 2
                    THEN UPPER(regexp_replace(a.country_code, '[^A-Za-z]', '', 'g'))
                ELSE NULL
            END,
            'US'
        ) AS country_code,
        a.is_primary,
        a.source_table_name,
        a.source_row_pk,
        a.updated_at,
        pa.source_system AS participant_address_source_system,
        CASE
            WHEN a.source_table_name = 'bhyc_member_raw_row' THEN 10
            WHEN pa.source_system = 'jotform_csv_export' THEN 20
            WHEN pa.source_system = 'regattaman_csv_export' THEN 30
            WHEN pa.source_system = 'bhyc_member_directory' THEN 40
            WHEN pa.source_system = 'mailchimp_audience_csv' THEN 50
            ELSE 90
        END AS source_priority,
        CASE
            WHEN NULLIF(BTRIM(a.line1), '') IS NOT NULL
             AND NULLIF(BTRIM(a.city), '') IS NOT NULL
             AND NULLIF(BTRIM(a.state), '') IS NOT NULL
             AND NULLIF(BTRIM(a.postal_code), '') IS NOT NULL
                THEN 1
            ELSE 0
        END AS structured_address_score,
        COUNT(*) OVER (PARTITION BY a.candidate_participant_id) AS address_count
    FROM candidate_participant_address a
    LEFT JOIN participant_address pa
      ON a.source_table_name = 'participant_address'
     AND a.source_row_pk = pa.id::text
    WHERE NULLIF(BTRIM(a.line1), '') IS NOT NULL
      AND NULLIF(BTRIM(a.postal_code), '') IS NOT NULL
),
ranked_address AS (
    SELECT
        ao.*,
        ROW_NUMBER() OVER (
            PARTITION BY ao.candidate_id
            ORDER BY
                ao.structured_address_score DESC,
                ao.is_primary DESC,
                ao.source_priority ASC,
                ao.updated_at DESC NULLS LAST,
                ao.address_raw ASC
        ) AS rn
    FROM (
        SELECT
            address_options.*
        FROM address_options
    ) ao
),
contact_rollup AS (
    SELECT
        c.candidate_participant_id AS candidate_id,
        STRING_AGG(
            DISTINCT COALESCE(NULLIF(BTRIM(c.normalized_value), ''), NULLIF(BTRIM(c.raw_value), '')),
            ' | ' ORDER BY COALESCE(NULLIF(BTRIM(c.normalized_value), ''), NULLIF(BTRIM(c.raw_value), ''))
        ) FILTER (WHERE c.contact_type = 'email') AS email_address,
        STRING_AGG(
            DISTINCT COALESCE(NULLIF(BTRIM(c.normalized_value), ''), NULLIF(BTRIM(c.raw_value), '')),
            ' | ' ORDER BY COALESCE(NULLIF(BTRIM(c.normalized_value), ''), NULLIF(BTRIM(c.raw_value), ''))
        ) FILTER (WHERE c.contact_type = 'phone') AS phone
    FROM candidate_participant_contact c
    GROUP BY c.candidate_participant_id
),
canonical_rollup AS (
    SELECT
        ccl.candidate_entity_id AS candidate_id,
        ARRAY_AGG(DISTINCT ccl.canonical_entity_id::text ORDER BY ccl.canonical_entity_id::text) AS canonical_ids,
        MAX(NULLIF(BTRIM(cp.first_name), '')) AS canonical_first_name,
        MAX(NULLIF(BTRIM(cp.last_name), '')) AS canonical_last_name
    FROM candidate_canonical_link ccl
    LEFT JOIN canonical_participant cp
      ON cp.id = ccl.canonical_entity_id
    WHERE ccl.candidate_entity_type = 'participant'
    GROUP BY ccl.candidate_entity_id
),
boat_evidence AS (
    SELECT DISTINCT
        pb.candidate_id,
        y.name AS boat_name,
        y.sail_number,
        y.model AS boat_type,
        'yacht_ownership' AS boat_source
    FROM participant_bridge pb
    JOIN yacht_ownership yo
      ON yo.participant_id = pb.participant_id
    JOIN yacht y
      ON y.id = yo.yacht_id

    UNION ALL

    SELECT DISTINCT
        csl.candidate_entity_id AS candidate_id,
        NULLIF(BTRIM(jws.raw_payload->>'Boat Name'), '') AS boat_name,
        NULLIF(BTRIM(jws.raw_payload->>'Sail Number'), '') AS sail_number,
        NULL::text AS boat_type,
        'jotform_waiver_submission' AS boat_source
    FROM candidate_source_link csl
    JOIN jotform_waiver_submission jws
      ON csl.candidate_entity_type = 'participant'
     AND csl.source_table_name = 'jotform_waiver_submission'
     AND csl.source_row_pk = jws.id::text
    WHERE NULLIF(BTRIM(jws.raw_payload->>'Boat Name'), '') IS NOT NULL
       OR NULLIF(BTRIM(jws.raw_payload->>'Sail Number'), '') IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        csl.candidate_entity_id AS candidate_id,
        COALESCE(
            NULLIF(BTRIM(ys.raw_payload->>'yachtName'), ''),
            NULLIF(BTRIM(ys.raw_payload->>'Name'), ''),
            NULLIF(BTRIM(ys.raw_payload->>'title-small'), '')
        ) AS boat_name,
        COALESCE(
            NULLIF(BTRIM(ys.raw_payload->>'sailNumber'), ''),
            NULLIF(BTRIM(ys.raw_payload->>'Sail Number'), '')
        ) AS sail_number,
        COALESCE(
            NULLIF(BTRIM(ys.raw_payload->>'yachtType'), ''),
            NULLIF(BTRIM(ys.raw_payload->>'Boat Type'), ''),
            NULLIF(BTRIM(ys.raw_payload->>'Type'), '')
        ) AS boat_type,
        'yacht_scoring_raw_row' AS boat_source
    FROM candidate_source_link csl
    JOIN yacht_scoring_raw_row ys
      ON csl.candidate_entity_type = 'participant'
     AND csl.source_table_name = 'yacht_scoring_raw_row'
     AND csl.source_row_pk = ys.id::text
    WHERE COALESCE(
        NULLIF(BTRIM(ys.raw_payload->>'yachtName'), ''),
        NULLIF(BTRIM(ys.raw_payload->>'Name'), ''),
        NULLIF(BTRIM(ys.raw_payload->>'title-small'), ''),
        NULLIF(BTRIM(ys.raw_payload->>'sailNumber'), ''),
        NULLIF(BTRIM(ys.raw_payload->>'Sail Number'), '')
    ) IS NOT NULL
),
boat_rollup AS (
    SELECT
        be.candidate_id,
        STRING_AGG(DISTINCT NULLIF(BTRIM(be.boat_name), ''), ' | '
            ORDER BY NULLIF(BTRIM(be.boat_name), '')) AS boat_name,
        STRING_AGG(DISTINCT NULLIF(BTRIM(be.sail_number), ''), ' | '
            ORDER BY NULLIF(BTRIM(be.sail_number), '')) AS sail_number,
        STRING_AGG(DISTINCT NULLIF(BTRIM(be.boat_type), ''), ' | '
            ORDER BY NULLIF(BTRIM(be.boat_type), '')) AS boat_type,
        STRING_AGG(DISTINCT be.boat_source, ' | ' ORDER BY be.boat_source) AS boat_sources
    FROM boat_evidence be
    GROUP BY be.candidate_id
),
club_evidence AS (
    SELECT DISTINCT
        pb.candidate_id,
        yc.name AS club_affiliation,
        'club_membership' AS club_source
    FROM participant_bridge pb
    JOIN club_membership cm
      ON cm.participant_id = pb.participant_id
    JOIN yacht_club yc
      ON yc.id = cm.yacht_club_id

    UNION ALL

    SELECT DISTINCT
        csl.candidate_entity_id AS candidate_id,
        NULLIF(BTRIM(ys.raw_payload->>'ownerAffiliation'), '') AS club_affiliation,
        'yacht_scoring_raw_row' AS club_source
    FROM candidate_source_link csl
    JOIN yacht_scoring_raw_row ys
      ON csl.candidate_entity_type = 'participant'
     AND csl.source_table_name = 'yacht_scoring_raw_row'
     AND csl.source_row_pk = ys.id::text
    WHERE NULLIF(BTRIM(ys.raw_payload->>'ownerAffiliation'), '') IS NOT NULL
),
club_rollup AS (
    SELECT
        ce.candidate_id,
        STRING_AGG(DISTINCT NULLIF(BTRIM(ce.club_affiliation), ''), ' | '
            ORDER BY NULLIF(BTRIM(ce.club_affiliation), '')) AS club_affiliation,
        STRING_AGG(DISTINCT ce.club_source, ' | ' ORDER BY ce.club_source) AS club_sources
    FROM club_evidence ce
    GROUP BY ce.candidate_id
),
source_rollup AS (
    SELECT
        csl.candidate_entity_id AS candidate_id,
        COUNT(DISTINCT csl.source_table_name || ':' || csl.source_row_pk) AS source_record_count,
        ARRAY_AGG(
            DISTINCT (csl.source_table_name || ':' || csl.source_row_pk)
            ORDER BY (csl.source_table_name || ':' || csl.source_row_pk)
        ) AS source_record_ids
    FROM candidate_source_link csl
    WHERE csl.candidate_entity_type = 'participant'
    GROUP BY csl.candidate_entity_id
)
SELECT
    cp.id::text AS candidate_id,
    COALESCE(cr.canonical_ids, ARRAY[]::text[]) AS canonical_ids,
    CASE
        WHEN cp.is_promoted OR cr.canonical_ids IS NOT NULL THEN 'promote'
        ELSE cp.resolution_state
    END AS candidate_decision,
    cp.quality_score AS candidate_score,
    CASE
        WHEN rr.has_owner_role THEN 'owner'
        WHEN rr.has_skipper_role THEN 'skipper'
        ELSE 'registrant'
    END AS audience_tier,
    rr.roles,
    COALESCE(
        NULLIF(BTRIM(CONCAT_WS(' ', cr.canonical_first_name, cr.canonical_last_name)), ''),
        NULLIF(BTRIM(cp.display_name), '')
    ) AS display_name,
    cr.canonical_first_name AS canonical_first_name,
    cr.canonical_last_name AS canonical_last_name,
    cp.display_name AS candidate_display_name,
    con.email_address,
    con.phone,
    br.boat_name,
    br.sail_number,
    br.boat_type,
    br.boat_sources,
    cl.club_affiliation,
    cl.club_sources,
    ra.line1,
    ra.line2 AS address2,
    ra.city,
    ra.state,
    ra.postal_code,
    ra.country_code AS country_code,
    ra.address_raw,
    ra.source_table_name AS address_source_table,
    ra.participant_address_source_system AS address_source_system,
    ra.source_row_pk AS address_source_row_pk,
    ra.address_count,
    COALESCE(sr.source_record_count, 0) AS source_record_count,
    COALESCE(sr.source_record_ids, ARRAY[]::text[]) AS source_record_ids
FROM candidate_participant cp
JOIN role_rollup rr
  ON rr.candidate_id = cp.id
JOIN ranked_address ra
  ON ra.candidate_id = cp.id
 AND ra.rn = 1
LEFT JOIN contact_rollup con
  ON con.candidate_id = cp.id
LEFT JOIN canonical_rollup cr
  ON cr.candidate_id = cp.id
LEFT JOIN boat_rollup br
  ON br.candidate_id = cp.id
LEFT JOIN club_rollup cl
  ON cl.candidate_id = cp.id
JOIN source_rollup sr
  ON sr.candidate_id = cp.id
ORDER BY
    audience_tier,
    cr.canonical_last_name NULLS LAST,
    cp.display_name NULLS LAST,
    cp.id;
