-- Migration: 0005_resolution_and_actions.sql
-- Purpose: Create entity resolution, merge tracking, NBA, and raw asset tables.
-- Ref: docs/runbooks/05-cloud-sql-migration-plan.md (Phase 5)
--      docs/architecture/cloud-sql-schema-spec.md
-- Tables: identity_candidate_match, identity_merge_action, next_best_action, raw_asset

BEGIN;

-- ============================================================
-- identity_candidate_match
-- ============================================================
CREATE TABLE identity_candidate_match (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type     text          NOT NULL CHECK (entity_type IN ('participant', 'yacht', 'yacht_club', 'event')),
    left_entity_id  uuid          NOT NULL,
    right_entity_id uuid          NOT NULL,
    score           numeric(5,4)  NOT NULL CHECK (score >= 0 AND score <= 1),
    feature_payload jsonb         NOT NULL,
    decision        text          NOT NULL CHECK (decision IN ('auto_merge', 'review', 'reject')),
    decided_at      timestamptz   NOT NULL,
    created_at      timestamptz   NOT NULL DEFAULT now(),
    updated_at      timestamptz   NOT NULL DEFAULT now(),
    -- Enforce canonical ordering so (A,B) and (B,A) cannot both exist,
    -- and prevent self-matches.
    CHECK (left_entity_id < right_entity_id),
    UNIQUE (entity_type, left_entity_id, right_entity_id)
);

CREATE INDEX idx_candidate_match_type_decision_score
    ON identity_candidate_match (entity_type, decision, score);

CREATE TRIGGER trg_identity_candidate_match_updated_at
    BEFORE UPDATE ON identity_candidate_match
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- identity_merge_action
-- ============================================================
CREATE TABLE identity_merge_action (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type         text        NOT NULL CHECK (entity_type IN ('participant', 'yacht', 'yacht_club', 'event')),
    surviving_entity_id uuid        NOT NULL,
    merged_entity_id    uuid        NOT NULL,
    CHECK (surviving_entity_id <> merged_entity_id),
    match_id            uuid        REFERENCES identity_candidate_match (id),
    merge_method        text        NOT NULL CHECK (merge_method IN ('auto', 'manual')),
    merged_at           timestamptz NOT NULL,
    merged_by           text        NOT NULL,
    is_reverted         boolean     NOT NULL DEFAULT false,
    reverted_at         timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (entity_type, merged_entity_id)
);

CREATE INDEX idx_merge_action_type_surviving
    ON identity_merge_action (entity_type, surviving_entity_id);

CREATE TRIGGER trg_identity_merge_action_updated_at
    BEFORE UPDATE ON identity_merge_action
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- next_best_action
-- ============================================================
CREATE TABLE next_best_action (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    action_type           text          NOT NULL,
    target_entity_type    text          NOT NULL,
    target_entity_id      uuid          NOT NULL,
    event_instance_id     uuid          REFERENCES event_instance (id),
    priority_score        numeric(8,4)  NOT NULL,
    reason_code           text          NOT NULL,
    reason_detail         text          NOT NULL,
    recommended_channel   text          NOT NULL,
    generated_at          timestamptz   NOT NULL,
    rule_version          text          NOT NULL,
    status                text          NOT NULL CHECK (status IN ('open', 'dismissed', 'actioned')),
    created_at            timestamptz   NOT NULL DEFAULT now(),
    updated_at            timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX idx_nba_status_priority ON next_best_action (status, priority_score);
CREATE INDEX idx_nba_event_action    ON next_best_action (event_instance_id, action_type);

CREATE TRIGGER trg_next_best_action_updated_at
    BEFORE UPDATE ON next_best_action
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- raw_asset
-- ============================================================
CREATE TABLE raw_asset (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system           text        NOT NULL,
    asset_type              text        NOT NULL,
    gcs_bucket              text        NOT NULL,
    gcs_object              text        NOT NULL,
    content_hash            text,
    captured_at             timestamptz NOT NULL,
    retention_delete_after  date        NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (gcs_bucket, gcs_object)
);

CREATE INDEX idx_raw_asset_source_captured ON raw_asset (source_system, captured_at);
CREATE INDEX idx_raw_asset_retention       ON raw_asset (retention_delete_after);

CREATE TRIGGER trg_raw_asset_updated_at
    BEFORE UPDATE ON raw_asset
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
