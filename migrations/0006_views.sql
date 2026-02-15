-- Migration: 0006_views.sql
-- Purpose: Create materialized views for NBA pipeline and resolution review.
-- Ref: docs/runbooks/05-cloud-sql-migration-plan.md (Phase 6)
--      docs/architecture/cloud-sql-schema-spec.md (Required Materialized Views)
-- Views: mv_missing_required_documents, mv_probable_unregistered_returners,
--        mv_entity_resolution_review_queue

BEGIN;

-- ============================================================
-- mv_missing_required_documents
-- Outstanding mandatory document requirements per subject
-- (participant, event_entry, or yacht) whose latest status is
-- not "received".  Keyed by (requirement, subject) so one
-- subject's completion cannot mask another's gap.
--
-- Status CTEs are split by subject type so a participant-scoped
-- status row (event_entry_id IS NULL) cannot bleed into
-- entry-scope lookups and vice versa.
-- ============================================================
CREATE MATERIALIZED VIEW mv_missing_required_documents AS
WITH latest_participant_status AS (
    -- Latest status per (requirement, participant) for participant-scoped docs.
    -- Rows where event_entry_id IS NOT NULL are excluded to prevent cross-scope bleed.
    SELECT DISTINCT ON (document_requirement_id, participant_id)
        document_requirement_id,
        participant_id,
        status
    FROM document_status
    WHERE participant_id IS NOT NULL
      AND event_entry_id IS NULL
    ORDER BY document_requirement_id, participant_id, status_at DESC, id DESC
),
latest_entry_status AS (
    -- Latest status per (requirement, event_entry) for entry-scoped docs.
    -- Rows where participant_id IS NOT NULL are excluded to prevent cross-scope bleed.
    SELECT DISTINCT ON (document_requirement_id, event_entry_id)
        document_requirement_id,
        event_entry_id,
        status
    FROM document_status
    WHERE event_entry_id IS NOT NULL
      AND participant_id IS NULL
    ORDER BY document_requirement_id, event_entry_id, status_at DESC, id DESC
),
latest_yacht_status AS (
    -- Latest status per (requirement, yacht via event_entry) for yacht-scoped docs.
    -- We join through event_entry to resolve yacht_id since document_status
    -- references event_entry_id, not yacht_id directly.
    SELECT DISTINCT ON (ds.document_requirement_id, ee.yacht_id)
        ds.document_requirement_id,
        ee.yacht_id,
        ds.status
    FROM document_status ds
    JOIN event_entry ee ON ee.id = ds.event_entry_id
    WHERE ds.event_entry_id IS NOT NULL
    ORDER BY ds.document_requirement_id, ee.yacht_id, ds.status_at DESC, ds.id DESC
)
SELECT
    dr.id                   AS document_requirement_id,
    dr.event_instance_id,
    dr.document_type_id,
    dt.name                 AS document_type_name,
    dt.scope                AS document_scope,
    dr.required_for_role,
    dr.due_at,
    ei.display_name         AS event_display_name,
    ei.season_year,
    subj.participant_id,
    subj.event_entry_id,
    subj.yacht_id,
    subj.latest_status
FROM document_requirement dr
JOIN document_type dt ON dt.id = dr.document_type_id
JOIN event_instance ei ON ei.id = dr.event_instance_id
-- Expand each requirement to every subject that must satisfy it,
-- then keep only rows where the latest assertion is NOT received.
CROSS JOIN LATERAL (
    -- Participant-scoped: one row per distinct participant in a matching role.
    -- Wrapped in a subselect so DISTINCT ON / ORDER BY does not conflict
    -- with UNION ALL (Postgres requires ORDER BY only on the last branch
    -- of a UNION, but DISTINCT ON needs its own ORDER BY).
    SELECT * FROM (
        SELECT DISTINCT ON (eep.participant_id)
               eep.participant_id,
               NULL::uuid AS event_entry_id,
               NULL::uuid AS yacht_id,
               lps.status AS latest_status
        FROM event_entry ee
        JOIN event_entry_participant eep ON eep.event_entry_id = ee.id
        LEFT JOIN latest_participant_status lps
            ON lps.document_requirement_id = dr.id
           AND lps.participant_id = eep.participant_id
        WHERE ee.event_instance_id = dr.event_instance_id
          AND dt.scope = 'participant'
          AND (dr.required_for_role IS NULL OR eep.role = dr.required_for_role)
          AND (lps.status IS NULL OR lps.status <> 'received')
        ORDER BY eep.participant_id
    ) _participant_subj

    UNION ALL

    -- Entry-scoped: one row per event_entry in this event
    SELECT NULL::uuid AS participant_id,
           ee.id      AS event_entry_id,
           NULL::uuid AS yacht_id,
           les.status AS latest_status
    FROM event_entry ee
    LEFT JOIN latest_entry_status les
        ON les.document_requirement_id = dr.id
       AND les.event_entry_id = ee.id
    WHERE ee.event_instance_id = dr.event_instance_id
      AND dt.scope = 'entry'
      AND (les.status IS NULL OR les.status <> 'received')

    UNION ALL

    -- Yacht-scoped: one row per distinct yacht entered in this event.
    -- Wrapped in a subselect for the same DISTINCT ON / UNION ALL reason.
    SELECT * FROM (
        SELECT DISTINCT ON (ee.yacht_id)
               NULL::uuid AS participant_id,
               NULL::uuid AS event_entry_id,
               ee.yacht_id,
               lys.status AS latest_status
        FROM event_entry ee
        LEFT JOIN latest_yacht_status lys
            ON lys.document_requirement_id = dr.id
           AND lys.yacht_id = ee.yacht_id
        WHERE ee.event_instance_id = dr.event_instance_id
          AND dt.scope = 'yacht'
          AND (lys.status IS NULL OR lys.status <> 'received')
        ORDER BY ee.yacht_id
    ) _yacht_subj
) subj
WHERE dr.is_mandatory = true
WITH NO DATA;

-- Unique index uses COALESCE sentinel for the two NULL subject columns
-- so that each (requirement, exactly-one-subject) row is unique.
CREATE UNIQUE INDEX idx_mv_missing_docs_pk
    ON mv_missing_required_documents (
        document_requirement_id,
        COALESCE(participant_id, '00000000-0000-0000-0000-000000000000'::uuid),
        COALESCE(event_entry_id, '00000000-0000-0000-0000-000000000000'::uuid),
        COALESCE(yacht_id,       '00000000-0000-0000-0000-000000000000'::uuid)
    );

-- ============================================================
-- mv_probable_unregistered_returners
-- Participants who entered at least one event in a prior season
-- for the same event series but have no entry in the target
-- season's instance.  Target season is derived from the latest
-- event_instance per series (registration_open_at IS NOT NULL)
-- so the view works for preseason planning and non-calendar-year
-- schedules without hard-coding wall-clock year.
-- ============================================================
CREATE MATERIALIZED VIEW mv_probable_unregistered_returners AS
WITH target_instances AS (
    -- The latest instance per series that has registration open,
    -- representing the "upcoming / current" season to check against.
    SELECT DISTINCT ON (event_series_id)
        id              AS event_instance_id,
        event_series_id,
        display_name,
        season_year
    FROM event_instance
    WHERE registration_open_at IS NOT NULL
    ORDER BY event_series_id, season_year DESC
)
SELECT DISTINCT
    p.id                    AS participant_id,
    p.full_name,
    es.id                   AS event_series_id,
    es.name                 AS event_series_name,
    ti.event_instance_id    AS current_event_instance_id,
    ti.display_name         AS current_event_display_name,
    ti.season_year          AS current_season_year,
    max(prev_ei.season_year) AS last_participated_year
FROM participant p
-- Previous participation
JOIN event_entry_participant eep ON eep.participant_id = p.id
JOIN event_entry prev_ee         ON prev_ee.id = eep.event_entry_id
JOIN event_instance prev_ei      ON prev_ei.id = prev_ee.event_instance_id
JOIN event_series es             ON es.id = prev_ei.event_series_id
-- Target season instance exists for this series
JOIN target_instances ti         ON ti.event_series_id = es.id
-- No entry in target season
WHERE NOT EXISTS (
    SELECT 1
    FROM event_entry curr_ee
    JOIN event_entry_participant curr_eep ON curr_eep.event_entry_id = curr_ee.id
    WHERE curr_ee.event_instance_id = ti.event_instance_id
      AND curr_eep.participant_id = p.id
)
AND prev_ei.season_year < ti.season_year
GROUP BY p.id, p.full_name, es.id, es.name,
         ti.event_instance_id, ti.display_name, ti.season_year
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_unregistered_returners_pk
    ON mv_probable_unregistered_returners (participant_id, current_event_instance_id);

-- ============================================================
-- mv_entity_resolution_review_queue
-- Candidate matches awaiting manual review, ordered by score.
-- ============================================================
CREATE MATERIALIZED VIEW mv_entity_resolution_review_queue AS
SELECT
    icm.id          AS match_id,
    icm.entity_type,
    icm.left_entity_id,
    icm.right_entity_id,
    icm.score,
    icm.feature_payload,
    icm.decided_at,
    icm.created_at
FROM identity_candidate_match icm
WHERE icm.decision = 'review'
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_resolution_review_pk
    ON mv_entity_resolution_review_queue (match_id);

COMMIT;
