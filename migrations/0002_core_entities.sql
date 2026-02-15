-- Migration: 0002_core_entities.sql
-- Purpose: Create core entity tables with foundational indexes and constraints.
-- Ref: docs/runbooks/05-cloud-sql-migration-plan.md (Phase 2)
--      docs/architecture/cloud-sql-schema-spec.md
-- Tables: yacht_club, participant, yacht, event_series, event_instance

BEGIN;

-- ============================================================
-- yacht_club
-- ============================================================
CREATE TABLE yacht_club (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text        NOT NULL,
    normalized_name text        NOT NULL,
    vitality_status text        NOT NULL CHECK (vitality_status IN ('active', 'inactive', 'unknown')),
    vitality_last_verified_at timestamptz,
    website_url     text,
    notes           text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_yacht_club_normalized_name ON yacht_club (normalized_name);
CREATE INDEX idx_yacht_club_vitality_status ON yacht_club (vitality_status);

CREATE TRIGGER trg_yacht_club_updated_at
    BEFORE UPDATE ON yacht_club
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- participant
-- ============================================================
CREATE TABLE participant (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name             text    NOT NULL,
    normalized_full_name  text    NOT NULL,
    first_name            text,
    last_name             text,
    date_of_birth         date,
    is_deceased           boolean NOT NULL DEFAULT false,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_participant_normalized_full_name ON participant (normalized_full_name);

CREATE TRIGGER trg_participant_updated_at
    BEFORE UPDATE ON participant
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- yacht
-- ============================================================
CREATE TABLE yacht (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name                    text          NOT NULL,
    normalized_name         text          NOT NULL,
    sail_number             text,
    normalized_sail_number  text,
    builder                 text,
    designer                text,
    model                   text,
    length_feet             numeric(6,2),
    created_at              timestamptz   NOT NULL DEFAULT now(),
    updated_at              timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX idx_yacht_normalized_name ON yacht (normalized_name);
CREATE INDEX idx_yacht_normalized_sail_number ON yacht (normalized_sail_number);

CREATE TRIGGER trg_yacht_updated_at
    BEFORE UPDATE ON yacht
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- event_series
-- ============================================================
CREATE TABLE event_series (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    yacht_club_id   uuid        NOT NULL REFERENCES yacht_club (id),
    name            text        NOT NULL,
    normalized_name text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (yacht_club_id, normalized_name)
);

CREATE TRIGGER trg_event_series_updated_at
    BEFORE UPDATE ON event_series
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- event_instance
-- ============================================================
CREATE TABLE event_instance (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_series_id       uuid        NOT NULL REFERENCES event_series (id),
    display_name          text        NOT NULL,
    season_year           int         NOT NULL,
    start_date            date,
    end_date              date,
    registration_open_at  timestamptz,
    registration_close_at timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_series_id, season_year),
    CHECK (end_date IS NULL OR start_date IS NULL OR end_date >= start_date)
);

CREATE TRIGGER trg_event_instance_updated_at
    BEFORE UPDATE ON event_instance
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
