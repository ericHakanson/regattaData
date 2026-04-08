-- Direct-mail owner/participant mailing-address export, canonical-first
-- event-enriched variant.
--
-- This output is canonical-rooted (one row per canonical participant), while
-- still aggregating evidence from all linked candidate rows and source systems.

WITH participant_bridge AS (
    SELECT DISTINCT
        csl.candidate_entity_id AS candidate_id,
        csl.source_row_pk::uuid AS participant_id
    FROM candidate_source_link csl
    WHERE csl.candidate_entity_type = 'participant'
      AND csl.source_table_name = 'participant'
),
candidate_canonical AS (
    SELECT
        cp.id AS candidate_id,
        COALESCE(cp.promoted_canonical_id, ccl.canonical_entity_id) AS canonical_id
    FROM candidate_participant cp
    LEFT JOIN LATERAL (
        SELECT ccl_inner.canonical_entity_id
        FROM candidate_canonical_link ccl_inner
        WHERE ccl_inner.candidate_entity_type = 'participant'
          AND ccl_inner.candidate_entity_id = cp.id
        ORDER BY ccl_inner.canonical_entity_id
        LIMIT 1
    ) ccl ON TRUE
    WHERE COALESCE(cp.promoted_canonical_id, ccl.canonical_entity_id) IS NOT NULL
),
owner_yacht_bridge AS (
    SELECT DISTINCT
        pb.candidate_id,
        pb.participant_id,
        y.id AS yacht_id,
        y.name AS boat_name,
        y.sail_number,
        y.model AS boat_type,
        yo.role AS ownership_role
    FROM participant_bridge pb
    JOIN yacht_ownership yo
      ON yo.participant_id = pb.participant_id
    JOIN yacht y
      ON y.id = yo.yacht_id
    WHERE yo.role IN ('owner', 'co_owner')
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
        oyb.candidate_id,
        oyb.ownership_role AS role,
        TRUE AS is_owner_role,
        FALSE AS is_skipper_role,
        FALSE AS is_registrant_role
    FROM owner_yacht_bridge oyb
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
        END AS structured_address_score
    FROM candidate_participant_address a
    LEFT JOIN participant_address pa
      ON a.source_table_name = 'participant_address'
     AND a.source_row_pk = pa.id::text
    WHERE NULLIF(BTRIM(a.line1), '') IS NOT NULL
      AND NULLIF(BTRIM(a.postal_code), '') IS NOT NULL
),
boat_evidence AS (
    SELECT DISTINCT
        oyb.candidate_id,
        oyb.boat_name,
        oyb.sail_number,
        oyb.boat_type,
        'yacht_ownership' AS boat_source
    FROM owner_yacht_bridge oyb

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
event_registration_evidence AS (
    SELECT DISTINCT
        oyb.candidate_id,
        ee.id::text AS event_registration_record_id,
        ei.id::text AS event_record_id,
        ei.display_name AS event_name,
        COALESCE(ee.registered_at, ei.start_date::timestamptz, ee.created_at) AS event_registration_date,
        ee.registration_source AS event_registration_source
    FROM owner_yacht_bridge oyb
    JOIN event_entry ee
      ON ee.yacht_id = oyb.yacht_id
    JOIN event_instance ei
      ON ei.id = ee.event_instance_id
),
canonical_name_rollup AS (
    SELECT
        cp.id AS canonical_id,
        NULLIF(BTRIM(cp.first_name), '') AS canonical_first_name,
        NULLIF(BTRIM(cp.last_name), '') AS canonical_last_name,
        NULLIF(BTRIM(cp.display_name), '') AS canonical_display_name
    FROM canonical_participant cp
),
canonical_candidate_rollup AS (
    SELECT
        cc.canonical_id,
        ARRAY_AGG(DISTINCT cc.candidate_id::text ORDER BY cc.candidate_id::text) AS candidate_ids,
        MAX(cp.quality_score) AS candidate_score,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM(cp.display_name), ''),
            ' | ' ORDER BY NULLIF(BTRIM(cp.display_name), '')
        ) AS candidate_display_name,
        BOOL_OR(cp.is_promoted) AS any_promoted,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM(cp.resolution_state), ''),
            ' | ' ORDER BY NULLIF(BTRIM(cp.resolution_state), '')
        ) AS candidate_decision
    FROM candidate_canonical cc
    JOIN candidate_participant cp
      ON cp.id = cc.candidate_id
    GROUP BY cc.canonical_id
),
canonical_role_rollup AS (
    SELECT
        cc.canonical_id,
        BOOL_OR(re.is_owner_role) AS has_owner_role,
        BOOL_OR(re.is_skipper_role) AS has_skipper_role,
        BOOL_OR(re.is_registrant_role) AS has_registrant_role,
        STRING_AGG(DISTINCT re.role, ' | ' ORDER BY re.role) AS roles
    FROM candidate_canonical cc
    JOIN role_evidence re
      ON re.candidate_id = cc.candidate_id
    GROUP BY cc.canonical_id
),
canonical_address_options AS (
    SELECT
        cc.canonical_id,
        ao.address_raw,
        ao.line1,
        ao.line2,
        ao.city,
        ao.state,
        ao.postal_code,
        ao.country_code,
        ao.is_primary,
        ao.source_table_name,
        ao.source_row_pk,
        ao.updated_at,
        ao.participant_address_source_system,
        ao.source_priority,
        ao.structured_address_score
    FROM candidate_canonical cc
    JOIN address_options ao
      ON ao.candidate_id = cc.candidate_id
),
canonical_ranked_address AS (
    SELECT
        cao.*,
        COUNT(*) OVER (PARTITION BY cao.canonical_id) AS address_count,
        ROW_NUMBER() OVER (
            PARTITION BY cao.canonical_id
            ORDER BY
                cao.structured_address_score DESC,
                cao.is_primary DESC,
                cao.source_priority ASC,
                cao.updated_at DESC NULLS LAST,
                cao.address_raw ASC
        ) AS rn
    FROM canonical_address_options cao
),
canonical_contact_rollup AS (
    SELECT
        cc.canonical_id,
        STRING_AGG(
            DISTINCT COALESCE(NULLIF(BTRIM(c.normalized_value), ''), NULLIF(BTRIM(c.raw_value), '')),
            ' | ' ORDER BY COALESCE(NULLIF(BTRIM(c.normalized_value), ''), NULLIF(BTRIM(c.raw_value), ''))
        ) FILTER (WHERE c.contact_type = 'email') AS email_address,
        STRING_AGG(
            DISTINCT COALESCE(NULLIF(BTRIM(c.normalized_value), ''), NULLIF(BTRIM(c.raw_value), '')),
            ' | ' ORDER BY COALESCE(NULLIF(BTRIM(c.normalized_value), ''), NULLIF(BTRIM(c.raw_value), ''))
        ) FILTER (WHERE c.contact_type = 'phone') AS phone
    FROM candidate_canonical cc
    JOIN candidate_participant_contact c
      ON c.candidate_participant_id = cc.candidate_id
    GROUP BY cc.canonical_id
),
canonical_boat_rollup AS (
    SELECT
        cc.canonical_id,
        STRING_AGG(DISTINCT NULLIF(BTRIM(be.boat_name), ''), ' | '
            ORDER BY NULLIF(BTRIM(be.boat_name), '')) AS boat_name,
        STRING_AGG(DISTINCT NULLIF(BTRIM(be.sail_number), ''), ' | '
            ORDER BY NULLIF(BTRIM(be.sail_number), '')) AS sail_number,
        STRING_AGG(DISTINCT NULLIF(BTRIM(be.boat_type), ''), ' | '
            ORDER BY NULLIF(BTRIM(be.boat_type), '')) AS boat_type,
        STRING_AGG(DISTINCT be.boat_source, ' | ' ORDER BY be.boat_source) AS boat_sources
    FROM candidate_canonical cc
    JOIN boat_evidence be
      ON be.candidate_id = cc.candidate_id
    GROUP BY cc.canonical_id
),
canonical_club_rollup AS (
    SELECT
        cc.canonical_id,
        STRING_AGG(DISTINCT NULLIF(BTRIM(ce.club_affiliation), ''), ' | '
            ORDER BY NULLIF(BTRIM(ce.club_affiliation), '')) AS club_affiliation,
        STRING_AGG(DISTINCT ce.club_source, ' | ' ORDER BY ce.club_source) AS club_sources
    FROM candidate_canonical cc
    JOIN club_evidence ce
      ON ce.candidate_id = cc.candidate_id
    GROUP BY cc.canonical_id
),
canonical_event_registration_rollup AS (
    SELECT
        cc.canonical_id,
        COUNT(DISTINCT ere.event_registration_record_id) AS matching_event_registration_count,
        MAX(ere.event_registration_date)::date AS most_recent_matching_event_registration_date,
        ARRAY_AGG(DISTINCT ere.event_record_id ORDER BY ere.event_record_id) AS event_record_ids,
        ARRAY_AGG(
            DISTINCT ere.event_registration_record_id
            ORDER BY ere.event_registration_record_id
        ) AS event_registration_record_ids,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM(ere.event_registration_source), ''),
            ' | ' ORDER BY NULLIF(BTRIM(ere.event_registration_source), '')
        ) AS event_registration_sources,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM(ere.event_name), ''),
            ' | ' ORDER BY NULLIF(BTRIM(ere.event_name), '')
        ) AS matching_event_names
    FROM candidate_canonical cc
    JOIN event_registration_evidence ere
      ON ere.candidate_id = cc.candidate_id
    GROUP BY cc.canonical_id
),
canonical_source_rollup AS (
    SELECT
        cc.canonical_id,
        COUNT(DISTINCT csl.source_table_name || ':' || csl.source_row_pk) AS source_record_count,
        ARRAY_AGG(
            DISTINCT (csl.source_table_name || ':' || csl.source_row_pk)
            ORDER BY (csl.source_table_name || ':' || csl.source_row_pk)
        ) AS source_record_ids
    FROM candidate_canonical cc
    JOIN candidate_source_link csl
      ON csl.candidate_entity_type = 'participant'
     AND csl.candidate_entity_id = cc.candidate_id
    GROUP BY cc.canonical_id
)
SELECT
    ccr.canonical_id::text AS canonical_id,
    ARRAY[ccr.canonical_id::text] AS canonical_ids,
    ccr.candidate_ids,
    CASE
        WHEN ccr.any_promoted THEN 'promote'
        ELSE COALESCE(ccr.candidate_decision, 'promote')
    END AS candidate_decision,
    ccr.candidate_score AS candidate_score,
    CASE
        WHEN rr.has_owner_role THEN 'owner'
        WHEN rr.has_skipper_role THEN 'skipper'
        ELSE 'registrant'
    END AS audience_tier,
    rr.roles,
    COALESCE(
        NULLIF(BTRIM(CONCAT_WS(' ', cnr.canonical_first_name, cnr.canonical_last_name)), ''),
        cnr.canonical_display_name,
        ccr.candidate_display_name
    ) AS display_name,
    cnr.canonical_first_name AS canonical_first_name,
    cnr.canonical_last_name AS canonical_last_name,
    ccr.candidate_display_name AS candidate_display_name,
    con.email_address,
    con.phone,
    br.boat_name,
    br.sail_number,
    br.boat_type,
    br.boat_sources,
    cl.club_affiliation,
    cl.club_sources,
    COALESCE(err.event_record_ids, ARRAY[]::text[]) AS event_record_ids,
    COALESCE(err.event_registration_record_ids, ARRAY[]::text[]) AS event_registration_record_ids,
    COALESCE(err.matching_event_registration_count, 0) AS matching_event_registration_count,
    err.most_recent_matching_event_registration_date,
    err.event_registration_sources,
    err.matching_event_names,
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
FROM canonical_candidate_rollup ccr
JOIN canonical_role_rollup rr
  ON rr.canonical_id = ccr.canonical_id
JOIN canonical_ranked_address ra
  ON ra.canonical_id = ccr.canonical_id
 AND ra.rn = 1
LEFT JOIN canonical_name_rollup cnr
  ON cnr.canonical_id = ccr.canonical_id
LEFT JOIN canonical_contact_rollup con
  ON con.canonical_id = ccr.canonical_id
LEFT JOIN canonical_boat_rollup br
  ON br.canonical_id = ccr.canonical_id
LEFT JOIN canonical_club_rollup cl
  ON cl.canonical_id = ccr.canonical_id
LEFT JOIN canonical_event_registration_rollup err
  ON err.canonical_id = ccr.canonical_id
LEFT JOIN canonical_source_rollup sr
  ON sr.canonical_id = ccr.canonical_id
ORDER BY
    audience_tier,
    err.matching_event_registration_count DESC NULLS LAST,
    err.most_recent_matching_event_registration_date DESC NULLS LAST,
    cnr.canonical_last_name NULLS LAST,
    display_name NULLS LAST,
    ccr.canonical_id;
