-- Migration: 0021_rocketreach_enrichment_tables.sql
-- Purpose: Audit tables for the RocketReach participant enrichment pipeline.
-- Ref: docs/requirements/rocketreach-participant-candidate-enrichment-spec.md
-- Depends on: 0001 (gen_random_uuid), 0011 (candidate_participant)

BEGIN;

-- ============================================================
-- rocketreach_enrichment_run
-- One row per pipeline execution.
-- ============================================================

CREATE TABLE rocketreach_enrichment_run (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at                 TIMESTAMPTZ,
    status                      TEXT        NOT NULL DEFAULT 'running'
                                    CHECK (status IN ('running', 'ok', 'failed')),
    dry_run                     BOOLEAN     NOT NULL DEFAULT false,
    requested_max_candidates    INT         NOT NULL,
    counters                    JSONB       NOT NULL DEFAULT '{}',
    warnings                    JSONB       NOT NULL DEFAULT '[]',
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rocketreach_run_status
    ON rocketreach_enrichment_run (status, started_at DESC);

-- ============================================================
-- rocketreach_enrichment_row
-- One row per candidate processed in a run.
-- ============================================================

CREATE TABLE rocketreach_enrichment_row (
    id                       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                   UUID        NOT NULL
                                 REFERENCES rocketreach_enrichment_run (id)
                                 ON DELETE CASCADE,
    candidate_participant_id UUID        NOT NULL,
    request_payload          JSONB,
    response_payload         JSONB,
    provider_person_id       TEXT,
    match_confidence         NUMERIC(5,4),
    status                   TEXT        NOT NULL
                                 CHECK (status IN (
                                     'matched', 'no_match', 'ambiguous',
                                     'api_error', 'rate_limited', 'skipped'
                                 )),
    error_code               TEXT,
    applied_field_mask       TEXT[],
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rocketreach_row_run_id
    ON rocketreach_enrichment_row (run_id);

-- Enables cooldown check: find recent enrichment rows for a candidate
CREATE INDEX idx_rocketreach_row_candidate_created
    ON rocketreach_enrichment_row (candidate_participant_id, created_at DESC);

COMMIT;
