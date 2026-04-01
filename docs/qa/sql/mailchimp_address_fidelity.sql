-- QA snapshot for Mailchimp mailing-address fidelity.
-- Focused on export suffix: 973afc1ed4
--
-- Goal:
-- 1. quantify address-bearing Mailchimp rows in the fresh export
-- 2. show where those rows land across participant -> candidate -> canonical
-- 3. surface rows that still need human-assisted matching or review

-- -------------------------------------------------------------------------
-- 1) Address-bearing source rows in the fresh export
-- -------------------------------------------------------------------------
SELECT
    source_file_name,
    audience_status,
    COUNT(*) AS address_rows,
    COUNT(DISTINCT source_email_normalized) AS distinct_emails
FROM mailchimp_audience_row
WHERE source_file_name LIKE '%973afc1ed4.csv'
  AND NULLIF(BTRIM(COALESCE(raw_payload->>'Address', '')), '') IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;


-- -------------------------------------------------------------------------
-- 2) Stage-by-stage address coverage
-- -------------------------------------------------------------------------
WITH mailchimp_address_rows AS (
    SELECT
        mar.id::text AS audience_row_id,
        mar.source_file_name,
        mar.audience_status,
        mar.source_email_normalized,
        BTRIM(COALESCE(mar.raw_payload->>'Address', '')) AS source_address_trim,
        BTRIM(COALESCE(mar.raw_payload->>'First Name', '')) AS source_first_name,
        BTRIM(COALESCE(mar.raw_payload->>'Last Name', '')) AS source_last_name,
        mar.row_hash
    FROM mailchimp_audience_row mar
    WHERE mar.source_file_name LIKE '%973afc1ed4.csv'
      AND NULLIF(BTRIM(COALESCE(mar.raw_payload->>'Address', '')), '') IS NOT NULL
),
resolved_rows AS (
    SELECT
        mr.*,
        mcs.participant_id::text AS participant_id,
        pa.id::text AS participant_address_id,
        csl.candidate_entity_id::text AS candidate_participant_id,
        cp.resolution_state AS candidate_resolution_state,
        cpa.id::text AS candidate_address_id,
        ccl.canonical_entity_id::text AS canonical_participant_id,
        can_addr.id::text AS canonical_address_id
    FROM mailchimp_address_rows mr
    LEFT JOIN mailchimp_contact_state mcs
           ON mcs.source_file_name = mr.source_file_name
          AND mcs.row_hash = mr.row_hash
    LEFT JOIN participant_address pa
           ON pa.participant_id = mcs.participant_id
          AND pa.address_raw = mr.source_address_trim
    LEFT JOIN candidate_source_link csl
           ON csl.candidate_entity_type = 'participant'
          AND csl.source_table_name = 'participant'
          AND csl.source_row_pk = mcs.participant_id::text
    LEFT JOIN candidate_participant cp
           ON cp.id::text = csl.candidate_entity_id::text
    LEFT JOIN candidate_participant_address cpa
           ON cpa.candidate_participant_id::text = csl.candidate_entity_id::text
          AND cpa.address_raw = mr.source_address_trim
    LEFT JOIN candidate_canonical_link ccl
           ON ccl.candidate_entity_type = 'participant'
          AND ccl.candidate_entity_id::text = csl.candidate_entity_id::text
    LEFT JOIN canonical_participant_address can_addr
           ON can_addr.canonical_participant_id::text = ccl.canonical_entity_id::text
          AND can_addr.address_raw = mr.source_address_trim
)
SELECT
    COUNT(*) AS source_rows_with_address,
    COUNT(*) FILTER (WHERE participant_id IS NOT NULL) AS resolved_to_participant,
    COUNT(*) FILTER (WHERE participant_address_id IS NOT NULL) AS reached_participant_address,
    COUNT(*) FILTER (WHERE candidate_participant_id IS NOT NULL) AS reached_candidate_participant,
    COUNT(*) FILTER (WHERE candidate_address_id IS NOT NULL) AS reached_candidate_address,
    COUNT(*) FILTER (WHERE canonical_participant_id IS NOT NULL) AS reached_canonical_participant,
    COUNT(*) FILTER (WHERE canonical_address_id IS NOT NULL) AS reached_canonical_address
FROM resolved_rows;


-- -------------------------------------------------------------------------
-- 3) Missing-canonical reason buckets
-- -------------------------------------------------------------------------
WITH mailchimp_address_rows AS (
    SELECT
        mar.source_file_name,
        mar.source_email_normalized,
        BTRIM(COALESCE(mar.raw_payload->>'Address', '')) AS source_address_trim,
        mar.row_hash
    FROM mailchimp_audience_row mar
    WHERE mar.source_file_name LIKE '%973afc1ed4.csv'
      AND NULLIF(BTRIM(COALESCE(mar.raw_payload->>'Address', '')), '') IS NOT NULL
),
address_state AS (
    SELECT
        mr.*,
        mcs.participant_id::text AS participant_id,
        csl.candidate_entity_id::text AS candidate_participant_id,
        cp.resolution_state AS candidate_resolution_state,
        ccl.canonical_entity_id::text AS canonical_participant_id,
        can_addr.id::text AS canonical_address_id,
        review.reason_code AS review_reason_code
    FROM mailchimp_address_rows mr
    LEFT JOIN mailchimp_contact_state mcs
           ON mcs.source_file_name = mr.source_file_name
          AND mcs.row_hash = mr.row_hash
    LEFT JOIN candidate_source_link csl
           ON csl.candidate_entity_type = 'participant'
          AND csl.source_table_name = 'participant'
          AND csl.source_row_pk = mcs.participant_id::text
    LEFT JOIN candidate_participant cp
           ON cp.id::text = csl.candidate_entity_id::text
    LEFT JOIN candidate_canonical_link ccl
           ON ccl.candidate_entity_type = 'participant'
          AND ccl.candidate_entity_id::text = csl.candidate_entity_id::text
    LEFT JOIN canonical_participant_address can_addr
           ON can_addr.canonical_participant_id::text = ccl.canonical_entity_id::text
          AND can_addr.address_raw = mr.source_address_trim
    LEFT JOIN LATERAL (
        SELECT q.reason_code
        FROM mailchimp_identity_review_queue q
        WHERE q.source_file_name = mr.source_file_name
          AND q.email_normalized = mr.source_email_normalized
          AND BTRIM(COALESCE(q.raw_payload->>'Address', '')) = mr.source_address_trim
        ORDER BY q.created_at DESC
        LIMIT 1
    ) review ON TRUE
)
SELECT
    CASE
        WHEN canonical_address_id IS NOT NULL THEN 'canonical_address_present'
        WHEN review_reason_code IS NOT NULL THEN 'review_queue:' || review_reason_code
        WHEN participant_id IS NULL THEN 'unresolved_without_review_row'
        WHEN candidate_participant_id IS NULL THEN 'resolved_participant_missing_candidate_link'
        WHEN candidate_resolution_state IN ('hold', 'review', 'reject')
            THEN 'candidate_state:' || candidate_resolution_state
        WHEN canonical_participant_id IS NULL THEN 'candidate_not_promoted'
        ELSE 'canonical_missing_address_after_promotion'
    END AS outcome_bucket,
    COUNT(*) AS rows
FROM address_state
GROUP BY 1
ORDER BY 2 DESC, 1;


-- -------------------------------------------------------------------------
-- 4) Human-assisted matching worklist
-- Rows that still have Mailchimp address evidence but are not yet canonical.
-- This is the list to review when a human can help match names safely.
-- -------------------------------------------------------------------------
WITH mailchimp_address_rows AS (
    SELECT
        mar.source_file_name,
        mar.audience_status,
        mar.source_email_normalized,
        BTRIM(COALESCE(mar.raw_payload->>'First Name', '')) AS source_first_name,
        BTRIM(COALESCE(mar.raw_payload->>'Last Name', '')) AS source_last_name,
        BTRIM(COALESCE(mar.raw_payload->>'Phone Number', '')) AS source_phone,
        BTRIM(COALESCE(mar.raw_payload->>'Address', '')) AS source_address_trim,
        mar.row_hash
    FROM mailchimp_audience_row mar
    WHERE mar.source_file_name LIKE '%973afc1ed4.csv'
      AND NULLIF(BTRIM(COALESCE(mar.raw_payload->>'Address', '')), '') IS NOT NULL
),
address_state AS (
    SELECT
        mr.*,
        mcs.participant_id::text AS participant_id,
        p.full_name AS participant_full_name,
        csl.candidate_entity_id::text AS candidate_participant_id,
        cp.resolution_state AS candidate_resolution_state,
        ccl.canonical_entity_id::text AS canonical_participant_id,
        review.reason_code AS review_reason_code,
        review.reason_detail AS review_reason_detail
    FROM mailchimp_address_rows mr
    LEFT JOIN mailchimp_contact_state mcs
           ON mcs.source_file_name = mr.source_file_name
          AND mcs.row_hash = mr.row_hash
    LEFT JOIN participant p
           ON p.id = mcs.participant_id
    LEFT JOIN candidate_source_link csl
           ON csl.candidate_entity_type = 'participant'
          AND csl.source_table_name = 'participant'
          AND csl.source_row_pk = mcs.participant_id::text
    LEFT JOIN candidate_participant cp
           ON cp.id::text = csl.candidate_entity_id::text
    LEFT JOIN candidate_canonical_link ccl
           ON ccl.candidate_entity_type = 'participant'
          AND ccl.candidate_entity_id::text = csl.candidate_entity_id::text
    LEFT JOIN LATERAL (
        SELECT q.reason_code, q.reason_detail
        FROM mailchimp_identity_review_queue q
        WHERE q.source_file_name = mr.source_file_name
          AND q.email_normalized = mr.source_email_normalized
          AND BTRIM(COALESCE(q.raw_payload->>'Address', '')) = mr.source_address_trim
          AND q.status = 'open'
        ORDER BY q.created_at DESC
        LIMIT 1
    ) review ON TRUE
)
SELECT
    source_file_name,
    audience_status,
    source_email_normalized,
    source_first_name,
    source_last_name,
    source_phone,
    source_address_trim AS mailchimp_address,
    participant_id,
    participant_full_name,
    candidate_participant_id,
    candidate_resolution_state,
    review_reason_code,
    review_reason_detail
FROM address_state
WHERE canonical_participant_id IS NULL
ORDER BY
    review_reason_code NULLS FIRST,
    candidate_resolution_state NULLS FIRST,
    source_last_name,
    source_first_name,
    source_email_normalized;
