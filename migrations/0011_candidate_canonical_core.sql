-- Migration: 0011_candidate_canonical_core.sql
-- Purpose: Candidate entity tables, child tables, traceability links, and
--          resolution governance tables for entity-resolution pipeline.
-- Ref: docs/requirements/entity-resolution-candidate-canonical-tech-spec.md
-- Depends on: 0001 (set_updated_at), 0005 (resolution ops exist as context)
--
-- Table groups:
--   A. Resolution governance  : resolution_rule_set, resolution_score_run,
--                                resolution_manual_action_log
--   B. Candidate core         : candidate_participant, candidate_yacht,
--                                candidate_club, candidate_event,
--                                candidate_registration
--   C. Candidate child tables : candidate_participant_contact,
--                                candidate_participant_address,
--                                candidate_participant_role_assignment
--   D. Traceability           : candidate_source_link

BEGIN;

-- ============================================================
-- A. Resolution governance
-- ============================================================

-- resolution_rule_set
-- Stores versioned YAML rule files loaded into the DB for audit/replay.
CREATE TABLE resolution_rule_set (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type   text        NOT NULL CHECK (entity_type IN ('participant','yacht','event','registration','club')),
    source_system text,
    version       text        NOT NULL,
    yaml_content  text        NOT NULL,
    yaml_hash     text        NOT NULL,
    is_active     boolean     NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now(),
    activated_at  timestamptz
);

-- Only one active rule set per (entity_type, source_system) pair at a time.
CREATE UNIQUE INDEX idx_resolution_rule_set_active
    ON resolution_rule_set (entity_type, COALESCE(source_system, ''))
    WHERE is_active = true;

CREATE INDEX idx_resolution_rule_set_entity
    ON resolution_rule_set (entity_type, is_active);

-- resolution_score_run
-- Records each scoring pipeline execution with counters and status.
CREATE TABLE resolution_score_run (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type   text        NOT NULL CHECK (entity_type IN ('participant','yacht','event','registration','club','all')),
    source_scope  text,
    rule_set_id   uuid        REFERENCES resolution_rule_set (id),
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    counters      jsonb       NOT NULL DEFAULT '{}',
    status        text        NOT NULL CHECK (status IN ('running','ok','failed')) DEFAULT 'running',
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_resolution_score_run_entity_status
    ON resolution_score_run (entity_type, status, started_at DESC);

CREATE TRIGGER trg_resolution_score_run_updated_at
    BEFORE UPDATE ON resolution_score_run
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- resolution_manual_action_log
-- Full audit trail of every manual or pipeline-driven resolution action.
CREATE TABLE resolution_manual_action_log (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type         text        NOT NULL CHECK (entity_type IN ('participant','yacht','event','registration','club')),
    candidate_entity_id uuid        NOT NULL,
    canonical_entity_id uuid,
    action_type         text        NOT NULL CHECK (action_type IN ('promote','merge','split','demote','edit','unlink')),
    before_payload      jsonb,
    after_payload       jsonb,
    score_before        numeric(5,4),
    score_after         numeric(5,4),
    reason_code         text,
    actor               text        NOT NULL,
    source              text        NOT NULL CHECK (source IN ('db_manual','sheet_import','pipeline')),
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_resolution_manual_action_entity
    ON resolution_manual_action_log (entity_type, candidate_entity_id, created_at DESC);

CREATE INDEX idx_resolution_manual_action_canonical
    ON resolution_manual_action_log (canonical_entity_id)
    WHERE canonical_entity_id IS NOT NULL;

-- ============================================================
-- B. Candidate core tables
-- ============================================================

-- candidate_participant
-- One row per unique real-world person, identified by stable_fingerprint.
-- Fingerprint = sha256(normalized_name | best_email_or_empty).
CREATE TABLE candidate_participant (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    stable_fingerprint    text        NOT NULL,
    display_name          text,
    normalized_name       text,
    date_of_birth         date,
    best_email            text,
    best_phone            text,
    quality_score         numeric(5,4) NOT NULL DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 1),
    resolution_state      text        NOT NULL DEFAULT 'hold'
                              CHECK (resolution_state IN ('auto_promote','review','hold','reject')),
    confidence_reasons    jsonb       NOT NULL DEFAULT '[]',
    is_promoted           boolean     NOT NULL DEFAULT false,
    promoted_canonical_id uuid,
    last_score_run_id     uuid        REFERENCES resolution_score_run (id),
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_candidate_participant_fingerprint
    ON candidate_participant (stable_fingerprint);

CREATE INDEX idx_candidate_participant_state_score
    ON candidate_participant (resolution_state, quality_score DESC);

CREATE INDEX idx_candidate_participant_email
    ON candidate_participant (best_email)
    WHERE best_email IS NOT NULL;

CREATE INDEX idx_candidate_participant_promoted
    ON candidate_participant (promoted_canonical_id)
    WHERE is_promoted = true;

CREATE TRIGGER trg_candidate_participant_updated_at
    BEFORE UPDATE ON candidate_participant
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- candidate_yacht
-- One row per unique vessel identified by stable_fingerprint.
-- Fingerprint = sha256(normalized_name | normalized_sail_or_empty).
CREATE TABLE candidate_yacht (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    stable_fingerprint      text        NOT NULL,
    name                    text,
    normalized_name         text,
    sail_number             text,
    normalized_sail_number  text,
    length_feet             numeric(6,2),
    yacht_type              text,
    quality_score           numeric(5,4) NOT NULL DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 1),
    resolution_state        text        NOT NULL DEFAULT 'hold'
                                CHECK (resolution_state IN ('auto_promote','review','hold','reject')),
    confidence_reasons      jsonb       NOT NULL DEFAULT '[]',
    is_promoted             boolean     NOT NULL DEFAULT false,
    promoted_canonical_id   uuid,
    last_score_run_id       uuid        REFERENCES resolution_score_run (id),
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_candidate_yacht_fingerprint
    ON candidate_yacht (stable_fingerprint);

CREATE INDEX idx_candidate_yacht_state_score
    ON candidate_yacht (resolution_state, quality_score DESC);

CREATE INDEX idx_candidate_yacht_sail
    ON candidate_yacht (normalized_sail_number)
    WHERE normalized_sail_number IS NOT NULL;

CREATE TRIGGER trg_candidate_yacht_updated_at
    BEFORE UPDATE ON candidate_yacht
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- candidate_club
-- One row per unique sailing club.
-- Fingerprint = sha256(normalized_name).
CREATE TABLE candidate_club (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    stable_fingerprint    text        NOT NULL,
    name                  text,
    normalized_name       text,
    website               text,
    phone                 text,
    address_raw           text,
    state_usa             text,
    quality_score         numeric(5,4) NOT NULL DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 1),
    resolution_state      text        NOT NULL DEFAULT 'hold'
                              CHECK (resolution_state IN ('auto_promote','review','hold','reject')),
    confidence_reasons    jsonb       NOT NULL DEFAULT '[]',
    is_promoted           boolean     NOT NULL DEFAULT false,
    promoted_canonical_id uuid,
    last_score_run_id     uuid        REFERENCES resolution_score_run (id),
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_candidate_club_fingerprint
    ON candidate_club (stable_fingerprint);

CREATE INDEX idx_candidate_club_state_score
    ON candidate_club (resolution_state, quality_score DESC);

CREATE TRIGGER trg_candidate_club_updated_at
    BEFORE UPDATE ON candidate_club
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- candidate_event
-- One row per unique regatta event instance.
-- Fingerprint = sha256(normalized_event_name | season_year_or_empty | event_external_id_or_empty).
CREATE TABLE candidate_event (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    stable_fingerprint    text        NOT NULL,
    event_name            text,
    normalized_event_name text,
    season_year           int,
    event_external_id     text,
    start_date            date,
    end_date              date,
    location_raw          text,
    quality_score         numeric(5,4) NOT NULL DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 1),
    resolution_state      text        NOT NULL DEFAULT 'hold'
                              CHECK (resolution_state IN ('auto_promote','review','hold','reject')),
    confidence_reasons    jsonb       NOT NULL DEFAULT '[]',
    is_promoted           boolean     NOT NULL DEFAULT false,
    promoted_canonical_id uuid,
    last_score_run_id     uuid        REFERENCES resolution_score_run (id),
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_candidate_event_fingerprint
    ON candidate_event (stable_fingerprint);

CREATE INDEX idx_candidate_event_state_score
    ON candidate_event (resolution_state, quality_score DESC);

CREATE INDEX idx_candidate_event_season
    ON candidate_event (season_year)
    WHERE season_year IS NOT NULL;

CREATE TRIGGER trg_candidate_event_updated_at
    BEFORE UPDATE ON candidate_event
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- candidate_registration
-- One row per unique event entry.
-- Fingerprint = sha256(candidate_event_id | registration_external_id_or_empty | candidate_yacht_id_or_empty).
CREATE TABLE candidate_registration (
    id                              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    stable_fingerprint              text        NOT NULL,
    registration_external_id        text,
    candidate_event_id              uuid        NOT NULL REFERENCES candidate_event (id),
    candidate_yacht_id              uuid        REFERENCES candidate_yacht (id),
    candidate_primary_participant_id uuid       REFERENCES candidate_participant (id),
    entry_status                    text,
    registered_at                   timestamptz,
    quality_score                   numeric(5,4) NOT NULL DEFAULT 0 CHECK (quality_score >= 0 AND quality_score <= 1),
    resolution_state                text        NOT NULL DEFAULT 'hold'
                                        CHECK (resolution_state IN ('auto_promote','review','hold','reject')),
    confidence_reasons              jsonb       NOT NULL DEFAULT '[]',
    is_promoted                     boolean     NOT NULL DEFAULT false,
    promoted_canonical_id           uuid,
    last_score_run_id               uuid        REFERENCES resolution_score_run (id),
    created_at                      timestamptz NOT NULL DEFAULT now(),
    updated_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_candidate_registration_fingerprint
    ON candidate_registration (stable_fingerprint);

CREATE INDEX idx_candidate_registration_event
    ON candidate_registration (candidate_event_id, resolution_state);

CREATE INDEX idx_candidate_registration_yacht
    ON candidate_registration (candidate_yacht_id)
    WHERE candidate_yacht_id IS NOT NULL;

CREATE INDEX idx_candidate_registration_participant
    ON candidate_registration (candidate_primary_participant_id)
    WHERE candidate_primary_participant_id IS NOT NULL;

CREATE TRIGGER trg_candidate_registration_updated_at
    BEFORE UPDATE ON candidate_registration
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- C. Candidate child tables
-- ============================================================

-- candidate_participant_contact
-- Email, phone, and social contacts for a candidate participant.
CREATE TABLE candidate_participant_contact (
    id                       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_participant_id uuid        NOT NULL REFERENCES candidate_participant (id) ON DELETE CASCADE,
    contact_type             text        NOT NULL CHECK (contact_type IN ('email','phone','social','other')),
    contact_subtype          text,
    raw_value                text        NOT NULL,
    normalized_value         text,
    is_primary               boolean     NOT NULL DEFAULT false,
    source_table_name        text,
    source_row_pk            text,
    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_candidate_participant_contact_pid
    ON candidate_participant_contact (candidate_participant_id);

CREATE INDEX idx_candidate_participant_contact_norm
    ON candidate_participant_contact (normalized_value)
    WHERE normalized_value IS NOT NULL;

-- Idempotency: one contact row per (participant, type, effective value).
-- When normalized_value is present, use it as the deduplication key.
-- When absent, fall back to raw_value.
CREATE UNIQUE INDEX idx_candidate_participant_contact_unique_norm
    ON candidate_participant_contact (candidate_participant_id, contact_type, normalized_value)
    WHERE normalized_value IS NOT NULL;

CREATE UNIQUE INDEX idx_candidate_participant_contact_unique_raw
    ON candidate_participant_contact (candidate_participant_id, contact_type, raw_value)
    WHERE normalized_value IS NULL;

CREATE TRIGGER trg_candidate_participant_contact_updated_at
    BEFORE UPDATE ON candidate_participant_contact
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- candidate_participant_address
-- Mailing/residential addresses for a candidate participant.
CREATE TABLE candidate_participant_address (
    id                       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_participant_id uuid        NOT NULL REFERENCES candidate_participant (id) ON DELETE CASCADE,
    address_raw              text        NOT NULL,
    line1                    text,
    city                     text,
    state                    text,
    postal_code              text,
    country_code             text,
    is_primary               boolean     NOT NULL DEFAULT false,
    source_table_name        text,
    source_row_pk            text,
    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_candidate_participant_address_pid
    ON candidate_participant_address (candidate_participant_id);

-- One address row per (participant, raw text) — prevents duplicates on re-run.
CREATE UNIQUE INDEX idx_candidate_participant_address_unique
    ON candidate_participant_address (candidate_participant_id, address_raw);

CREATE TRIGGER trg_candidate_participant_address_updated_at
    BEFORE UPDATE ON candidate_participant_address
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- candidate_participant_role_assignment
-- Roles held by a candidate participant, optionally scoped to an event or registration.
CREATE TABLE candidate_participant_role_assignment (
    id                        uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_participant_id  uuid        NOT NULL REFERENCES candidate_participant (id) ON DELETE CASCADE,
    role                      text        NOT NULL CHECK (role IN (
                                  'owner','co_owner','crew','skipper',
                                  'parent','guardian','registrant',
                                  'emergency_contact','other'
                              )),
    candidate_event_id        uuid        REFERENCES candidate_event (id),
    candidate_registration_id uuid        REFERENCES candidate_registration (id),
    source_context            text,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now()
);

-- Idempotency: one row per (participant, role, event-or-empty, registration-or-empty).
-- NULLable FKs use COALESCE so the expression is never NULL, making the unique index fire.
CREATE UNIQUE INDEX idx_candidate_role_unique
    ON candidate_participant_role_assignment (
        candidate_participant_id,
        role,
        COALESCE(candidate_event_id::text, ''),
        COALESCE(candidate_registration_id::text, '')
    );

CREATE INDEX idx_candidate_role_event
    ON candidate_participant_role_assignment (candidate_event_id)
    WHERE candidate_event_id IS NOT NULL;

CREATE TRIGGER trg_candidate_participant_role_updated_at
    BEFORE UPDATE ON candidate_participant_role_assignment
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- D. Traceability: candidate_source_link
-- ============================================================

-- candidate_source_link
-- Maps every source table row to the candidate entity it contributed to.
-- Idempotent: re-running same source produces same link (DO NOTHING on conflict).
CREATE TABLE candidate_source_link (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_entity_type text        NOT NULL CHECK (candidate_entity_type IN ('participant','yacht','event','registration','club')),
    candidate_entity_id   uuid        NOT NULL,
    source_table_name     text        NOT NULL,
    source_row_pk         text        NOT NULL,
    source_row_hash       text,
    source_system         text,
    link_score            numeric(5,4) NOT NULL DEFAULT 1.0 CHECK (link_score >= 0 AND link_score <= 1),
    link_reason           jsonb       NOT NULL DEFAULT '{}',
    created_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (candidate_entity_type, candidate_entity_id, source_table_name, source_row_pk)
);

CREATE INDEX idx_candidate_source_link_entity
    ON candidate_source_link (candidate_entity_type, candidate_entity_id);

CREATE INDEX idx_candidate_source_link_source
    ON candidate_source_link (source_table_name, source_row_pk);

COMMIT;
