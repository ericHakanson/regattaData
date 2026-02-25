-- Migration: 0012_canonical_tables.sql
-- Purpose: Canonical entity tables, promotion link, and enriched views.
-- Ref: docs/requirements/entity-resolution-candidate-canonical-tech-spec.md
-- Depends on: 0011_candidate_canonical_core (candidate tables + resolution_score_run)
--
-- Table groups:
--   A. Canonical core    : canonical_participant, canonical_yacht, canonical_club,
--                          canonical_event, canonical_registration
--   B. Canonical children: canonical_participant_contact,
--                          canonical_participant_address,
--                          canonical_participant_role_assignment
--   C. Promotion bridge  : candidate_canonical_link
--   D. Views             : canonical_participant_enriched,
--                          candidate_participant_enriched

BEGIN;

-- ============================================================
-- A. Canonical core tables
-- ============================================================

-- canonical_participant
CREATE TABLE canonical_participant (
    id                         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name               text,
    normalized_name            text,
    first_name                 text,
    last_name                  text,
    date_of_birth              date,
    best_email                 text,
    best_phone                 text,
    canonical_confidence_score numeric(5,4) CHECK (canonical_confidence_score >= 0 AND canonical_confidence_score <= 1),
    last_resolution_run_id     uuid        REFERENCES resolution_score_run (id),
    created_at                 timestamptz NOT NULL DEFAULT now(),
    updated_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_canonical_participant_email
    ON canonical_participant (best_email)
    WHERE best_email IS NOT NULL;

CREATE INDEX idx_canonical_participant_name
    ON canonical_participant (normalized_name)
    WHERE normalized_name IS NOT NULL;

CREATE TRIGGER trg_canonical_participant_updated_at
    BEFORE UPDATE ON canonical_participant
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- canonical_yacht
CREATE TABLE canonical_yacht (
    id                         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name                       text,
    normalized_name            text,
    sail_number                text,
    normalized_sail_number     text,
    length_feet                numeric(6,2),
    yacht_type                 text,
    canonical_confidence_score numeric(5,4) CHECK (canonical_confidence_score >= 0 AND canonical_confidence_score <= 1),
    last_resolution_run_id     uuid        REFERENCES resolution_score_run (id),
    created_at                 timestamptz NOT NULL DEFAULT now(),
    updated_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_canonical_yacht_sail
    ON canonical_yacht (normalized_sail_number)
    WHERE normalized_sail_number IS NOT NULL;

CREATE TRIGGER trg_canonical_yacht_updated_at
    BEFORE UPDATE ON canonical_yacht
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- canonical_club
CREATE TABLE canonical_club (
    id                         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name                       text,
    normalized_name            text,
    website                    text,
    phone                      text,
    address_raw                text,
    state_usa                  text,
    canonical_confidence_score numeric(5,4) CHECK (canonical_confidence_score >= 0 AND canonical_confidence_score <= 1),
    last_resolution_run_id     uuid        REFERENCES resolution_score_run (id),
    created_at                 timestamptz NOT NULL DEFAULT now(),
    updated_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_canonical_club_name
    ON canonical_club (normalized_name)
    WHERE normalized_name IS NOT NULL;

CREATE TRIGGER trg_canonical_club_updated_at
    BEFORE UPDATE ON canonical_club
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- canonical_event
CREATE TABLE canonical_event (
    id                         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_name                 text,
    normalized_event_name      text,
    season_year                int,
    event_external_id          text,
    start_date                 date,
    end_date                   date,
    location_raw               text,
    canonical_confidence_score numeric(5,4) CHECK (canonical_confidence_score >= 0 AND canonical_confidence_score <= 1),
    last_resolution_run_id     uuid        REFERENCES resolution_score_run (id),
    created_at                 timestamptz NOT NULL DEFAULT now(),
    updated_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_canonical_event_season
    ON canonical_event (season_year)
    WHERE season_year IS NOT NULL;

CREATE TRIGGER trg_canonical_event_updated_at
    BEFORE UPDATE ON canonical_event
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- canonical_registration
CREATE TABLE canonical_registration (
    id                               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    registration_external_id         text,
    canonical_event_id               uuid        REFERENCES canonical_event (id),
    canonical_yacht_id               uuid        REFERENCES canonical_yacht (id),
    canonical_primary_participant_id uuid        REFERENCES canonical_participant (id),
    entry_status                     text,
    registered_at                    timestamptz,
    canonical_confidence_score       numeric(5,4) CHECK (canonical_confidence_score >= 0 AND canonical_confidence_score <= 1),
    last_resolution_run_id           uuid        REFERENCES resolution_score_run (id),
    created_at                       timestamptz NOT NULL DEFAULT now(),
    updated_at                       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_canonical_registration_event
    ON canonical_registration (canonical_event_id);

CREATE INDEX idx_canonical_registration_participant
    ON canonical_registration (canonical_primary_participant_id)
    WHERE canonical_primary_participant_id IS NOT NULL;

CREATE TRIGGER trg_canonical_registration_updated_at
    BEFORE UPDATE ON canonical_registration
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- B. Canonical child tables
-- ============================================================

-- canonical_participant_contact
CREATE TABLE canonical_participant_contact (
    id                        uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_participant_id  uuid        NOT NULL REFERENCES canonical_participant (id) ON DELETE CASCADE,
    contact_type              text        NOT NULL CHECK (contact_type IN ('email','phone','social','other')),
    contact_subtype           text,
    raw_value                 text        NOT NULL,
    normalized_value          text,
    is_primary                boolean     NOT NULL DEFAULT false,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_canonical_participant_contact_pid
    ON canonical_participant_contact (canonical_participant_id);

CREATE TRIGGER trg_canonical_participant_contact_updated_at
    BEFORE UPDATE ON canonical_participant_contact
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- canonical_participant_address
CREATE TABLE canonical_participant_address (
    id                        uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_participant_id  uuid        NOT NULL REFERENCES canonical_participant (id) ON DELETE CASCADE,
    address_raw               text        NOT NULL,
    line1                     text,
    city                      text,
    state                     text,
    postal_code               text,
    country_code              text,
    is_primary                boolean     NOT NULL DEFAULT false,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_canonical_participant_address_pid
    ON canonical_participant_address (canonical_participant_id);

CREATE TRIGGER trg_canonical_participant_address_updated_at
    BEFORE UPDATE ON canonical_participant_address
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- canonical_participant_role_assignment
CREATE TABLE canonical_participant_role_assignment (
    id                          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_participant_id    uuid        NOT NULL REFERENCES canonical_participant (id) ON DELETE CASCADE,
    role                        text        NOT NULL CHECK (role IN (
                                    'owner','co_owner','crew','skipper',
                                    'parent','guardian','registrant',
                                    'emergency_contact','other'
                                )),
    canonical_event_id          uuid        REFERENCES canonical_event (id),
    canonical_registration_id   uuid        REFERENCES canonical_registration (id),
    source_context              text,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_canonical_role_participant
    ON canonical_participant_role_assignment (canonical_participant_id, role);

CREATE TRIGGER trg_canonical_participant_role_updated_at
    BEFORE UPDATE ON canonical_participant_role_assignment
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- C. Promotion bridge: candidate_canonical_link
-- ============================================================

-- candidate_canonical_link
-- Records the promotion of a candidate to a canonical record.
-- A candidate can be promoted to at most one canonical record (UNIQUE on candidate).
CREATE TABLE candidate_canonical_link (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_entity_type text        NOT NULL CHECK (candidate_entity_type IN ('participant','yacht','event','registration','club')),
    candidate_entity_id   uuid        NOT NULL,
    canonical_entity_id   uuid        NOT NULL,
    promotion_score       numeric(5,4),
    promotion_mode        text        NOT NULL CHECK (promotion_mode IN ('auto','manual')),
    promoted_at           timestamptz NOT NULL DEFAULT now(),
    promoted_by           text,
    UNIQUE (candidate_entity_type, candidate_entity_id)
);

CREATE INDEX idx_candidate_canonical_link_canonical
    ON candidate_canonical_link (candidate_entity_type, canonical_entity_id);

-- ============================================================
-- D. Enriched views (age computation; no persisted age column)
-- ============================================================

-- canonical_participant_enriched
-- Adds runtime age_years and is_minor computed from date_of_birth.
-- is_minor uses age_of_majority = 18 (fixed per spec).
CREATE VIEW canonical_participant_enriched AS
SELECT
    cp.*,
    CASE
        WHEN cp.date_of_birth IS NOT NULL
        THEN EXTRACT(YEAR FROM age(current_date, cp.date_of_birth))::int
        ELSE NULL
    END AS age_years,
    CASE
        WHEN cp.date_of_birth IS NOT NULL
        THEN EXTRACT(YEAR FROM age(current_date, cp.date_of_birth))::int < 18
        ELSE NULL
    END AS is_minor
FROM canonical_participant cp;

-- candidate_participant_enriched
-- Same age computation for candidate layer (review/hold candidates).
CREATE VIEW candidate_participant_enriched AS
SELECT
    cp.*,
    CASE
        WHEN cp.date_of_birth IS NOT NULL
        THEN EXTRACT(YEAR FROM age(current_date, cp.date_of_birth))::int
        ELSE NULL
    END AS age_years,
    CASE
        WHEN cp.date_of_birth IS NOT NULL
        THEN EXTRACT(YEAR FROM age(current_date, cp.date_of_birth))::int < 18
        ELSE NULL
    END AS is_minor
FROM candidate_participant cp;

COMMIT;
