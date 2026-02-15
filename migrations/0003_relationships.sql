-- Migration: 0003_relationships.sql
-- Purpose: Create relationship tables with temporal check constraints.
-- Ref: docs/runbooks/05-cloud-sql-migration-plan.md (Phase 3)
--      docs/architecture/cloud-sql-schema-spec.md
-- Tables: participant_contact_point, participant_address, club_membership,
--         yacht_ownership, yacht_rating

BEGIN;

-- ============================================================
-- participant_contact_point
-- ============================================================
CREATE TABLE participant_contact_point (
    id                        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_id            uuid        NOT NULL REFERENCES participant (id),
    contact_type              text        NOT NULL CHECK (contact_type IN ('email', 'phone', 'social')),
    contact_subtype           text,
    contact_value_raw         text        NOT NULL,
    contact_value_normalized  text,
    is_primary                boolean     NOT NULL DEFAULT false,
    is_verified               boolean     NOT NULL DEFAULT false,
    source_system             text        NOT NULL,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_contact_point_participant_id ON participant_contact_point (participant_id);
CREATE INDEX idx_contact_point_value_normalized ON participant_contact_point (contact_value_normalized);

CREATE TRIGGER trg_participant_contact_point_updated_at
    BEFORE UPDATE ON participant_contact_point
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- participant_address
-- ============================================================
CREATE TABLE participant_address (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_id  uuid        NOT NULL REFERENCES participant (id),
    address_type    text        NOT NULL CHECK (address_type IN ('mailing', 'residential', 'other')),
    line1           text,
    line2           text,
    city            text,
    state           text,
    postal_code     text,
    country_code    text,
    address_raw     text,
    is_primary      boolean     NOT NULL DEFAULT false,
    source_system   text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_address_participant_id ON participant_address (participant_id);

CREATE TRIGGER trg_participant_address_updated_at
    BEFORE UPDATE ON participant_address
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- club_membership
-- ============================================================
CREATE TABLE club_membership (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_id  uuid        NOT NULL REFERENCES participant (id),
    yacht_club_id   uuid        NOT NULL REFERENCES yacht_club (id),
    membership_role text,
    effective_start date,
    effective_end   date,
    source_system   text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (effective_end IS NULL OR effective_end >= effective_start)
);

CREATE INDEX idx_club_membership_participant_club ON club_membership (participant_id, yacht_club_id);

CREATE TRIGGER trg_club_membership_updated_at
    BEFORE UPDATE ON club_membership
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- yacht_ownership
-- ============================================================
CREATE TABLE yacht_ownership (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_id    uuid          NOT NULL REFERENCES participant (id),
    yacht_id          uuid          NOT NULL REFERENCES yacht (id),
    role              text          NOT NULL CHECK (role IN ('owner', 'co_owner')),
    ownership_pct     numeric(5,2)  CHECK (ownership_pct IS NULL OR (ownership_pct >= 0 AND ownership_pct <= 100)),
    is_primary_contact boolean      NOT NULL DEFAULT false,
    effective_start   date          NOT NULL,
    effective_end     date,
    source_system     text          NOT NULL,
    created_at        timestamptz   NOT NULL DEFAULT now(),
    updated_at        timestamptz   NOT NULL DEFAULT now(),
    CHECK (effective_end IS NULL OR effective_end >= effective_start)
);

CREATE INDEX idx_yacht_ownership_participant_yacht ON yacht_ownership (participant_id, yacht_id);

CREATE TRIGGER trg_yacht_ownership_updated_at
    BEFORE UPDATE ON yacht_ownership
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- yacht_rating
-- ============================================================
CREATE TABLE yacht_rating (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    yacht_id        uuid        NOT NULL REFERENCES yacht (id),
    rating_system   text        NOT NULL,
    rating_category text        NOT NULL,
    rating_value    text        NOT NULL,
    effective_start date,
    effective_end   date,
    source_system   text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (effective_end IS NULL OR effective_end >= effective_start)
);

-- Partial unique indexes to handle nullable effective_start correctly.
-- In Postgres, UNIQUE constraints treat each NULL as distinct, so a
-- single inline UNIQUE would allow duplicate rows where effective_start
-- IS NULL.  These two indexes cover both cases.
CREATE UNIQUE INDEX idx_yacht_rating_unique_with_start
    ON yacht_rating (yacht_id, rating_system, rating_category, effective_start)
    WHERE effective_start IS NOT NULL;

CREATE UNIQUE INDEX idx_yacht_rating_unique_null_start
    ON yacht_rating (yacht_id, rating_system, rating_category)
    WHERE effective_start IS NULL;

CREATE TRIGGER trg_yacht_rating_updated_at
    BEFORE UPDATE ON yacht_rating
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
