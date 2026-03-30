-- ---------------------------------------------------------------------------
-- 0024_manual_curation_tables.sql
--
-- Source-layer schema for conversational manual curation edits.
-- Human factual corrections land here and flow through the normal
-- source → candidate → canonical pipeline as source_system='manual_curation'.
--
-- Design principles:
--   • Never writes directly to canonical tables (those are pipeline outputs).
--   • Every row carries full provenance: actor, reason_code, session_id.
--   • Idempotency: a (patch_hash) UNIQUE index prevents exact-duplicate patches.
--     patch_hash = sha256(table_name || target_id || sorted patch fields).
--   • status IN ('active','superseded','revoked') so patches can be withdrawn.
--   • created_at / updated_at on all tables; updated_at maintained by trigger.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 1. manual_participant_patch
--    Corrections to name, date_of_birth, best_email, best_phone on a
--    candidate_participant.  Target must be a known candidate_participant_id.
-- ---------------------------------------------------------------------------

CREATE TABLE manual_participant_patch (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_participant_id    UUID        NOT NULL
                                            REFERENCES candidate_participant(id)
                                            ON DELETE CASCADE,
    -- Patch fields: only non-null values are applied.
    patch_display_name          TEXT,
    patch_date_of_birth         DATE,
    patch_best_email            TEXT,
    patch_best_phone            TEXT,
    -- Provenance
    actor                       TEXT        NOT NULL,
    reason_code                 TEXT,
    session_id                  TEXT,
    -- Idempotency + lifecycle
    patch_hash                  TEXT        NOT NULL,   -- sha256 of (target + all patch values)
    status                      TEXT        NOT NULL DEFAULT 'active'
                                            CHECK (status IN ('active','superseded','revoked')),
    -- Timestamps
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX ux_manual_participant_patch_hash
    ON manual_participant_patch (patch_hash);

CREATE INDEX idx_manual_participant_patch_candidate
    ON manual_participant_patch (candidate_participant_id)
    WHERE status = 'active';

CREATE TRIGGER trg_manual_participant_patch_updated_at
    BEFORE UPDATE ON manual_participant_patch
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------------
-- 2. manual_participant_address_patch
--    Adds (or supersedes) an address for a candidate_participant.
--    address_raw is the canonical human-readable single string; structured
--    fields are optional enrichment.
--    Idempotency: one active address_raw per candidate.
-- ---------------------------------------------------------------------------

CREATE TABLE manual_participant_address_patch (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_participant_id    UUID        NOT NULL
                                            REFERENCES candidate_participant(id)
                                            ON DELETE CASCADE,
    address_raw                 TEXT        NOT NULL,
    line1                       TEXT,
    city                        TEXT,
    state                       TEXT,
    postal_code                 TEXT,
    country_code                CHAR(2),
    is_primary                  BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Provenance
    actor                       TEXT        NOT NULL,
    reason_code                 TEXT,
    session_id                  TEXT,
    -- Lifecycle
    patch_hash                  TEXT        NOT NULL,
    status                      TEXT        NOT NULL DEFAULT 'active'
                                            CHECK (status IN ('active','superseded','revoked')),
    -- Timestamps
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX ux_manual_participant_address_patch_hash
    ON manual_participant_address_patch (patch_hash);

-- One active row per (candidate_participant_id, address_raw)
CREATE UNIQUE INDEX ux_manual_participant_address_active
    ON manual_participant_address_patch (candidate_participant_id, address_raw)
    WHERE status = 'active';

CREATE INDEX idx_manual_participant_address_candidate
    ON manual_participant_address_patch (candidate_participant_id)
    WHERE status = 'active';

CREATE TRIGGER trg_manual_participant_address_patch_updated_at
    BEFORE UPDATE ON manual_participant_address_patch
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------------
-- 3. manual_yacht_patch
--    Corrections to name, sail_number, length_feet, yacht_type on a
--    candidate_yacht.
-- ---------------------------------------------------------------------------

CREATE TABLE manual_yacht_patch (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_yacht_id          UUID        NOT NULL
                                            REFERENCES candidate_yacht(id)
                                            ON DELETE CASCADE,
    -- Patch fields
    patch_name                  TEXT,
    patch_sail_number           TEXT,
    patch_length_feet           NUMERIC(6,2),
    patch_yacht_type            TEXT,
    -- Provenance
    actor                       TEXT        NOT NULL,
    reason_code                 TEXT,
    session_id                  TEXT,
    -- Idempotency + lifecycle
    patch_hash                  TEXT        NOT NULL,
    status                      TEXT        NOT NULL DEFAULT 'active'
                                            CHECK (status IN ('active','superseded','revoked')),
    -- Timestamps
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX ux_manual_yacht_patch_hash
    ON manual_yacht_patch (patch_hash);

CREATE INDEX idx_manual_yacht_patch_candidate
    ON manual_yacht_patch (candidate_yacht_id)
    WHERE status = 'active';

CREATE TRIGGER trg_manual_yacht_patch_updated_at
    BEFORE UPDATE ON manual_yacht_patch
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------------
-- 4. manual_yacht_ownership_patch
--    Records that a participant owns (or owned) a yacht with a specific role.
--    Used by the conversational tool to add/remove ownership without direct
--    canonical edits.  end_operation='remove' marks an ended ownership.
-- ---------------------------------------------------------------------------

CREATE TABLE manual_yacht_ownership_patch (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_participant_id    UUID        NOT NULL
                                            REFERENCES candidate_participant(id)
                                            ON DELETE CASCADE,
    candidate_yacht_id          UUID        NOT NULL
                                            REFERENCES candidate_yacht(id)
                                            ON DELETE CASCADE,
    role                        TEXT        NOT NULL DEFAULT 'owner'
                                            CHECK (role IN ('owner','co_owner')),
    effective_start             DATE,
    effective_end               DATE,
    -- 'add' = assert this ownership; 'remove' = end/revoke this ownership
    operation                   TEXT        NOT NULL DEFAULT 'add'
                                            CHECK (operation IN ('add','remove')),
    -- Provenance
    actor                       TEXT        NOT NULL,
    reason_code                 TEXT,
    session_id                  TEXT,
    -- Idempotency + lifecycle
    patch_hash                  TEXT        NOT NULL,
    status                      TEXT        NOT NULL DEFAULT 'active'
                                            CHECK (status IN ('active','superseded','revoked')),
    -- Timestamps
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX ux_manual_yacht_ownership_patch_hash
    ON manual_yacht_ownership_patch (patch_hash);

-- One active 'add' row per (participant, yacht, role)
CREATE UNIQUE INDEX ux_manual_yacht_ownership_active_add
    ON manual_yacht_ownership_patch (candidate_participant_id, candidate_yacht_id, role)
    WHERE status = 'active' AND operation = 'add';

CREATE INDEX idx_manual_yacht_ownership_participant
    ON manual_yacht_ownership_patch (candidate_participant_id)
    WHERE status = 'active';

CREATE TRIGGER trg_manual_yacht_ownership_patch_updated_at
    BEFORE UPDATE ON manual_yacht_ownership_patch
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---------------------------------------------------------------------------
-- 5. manual_club_membership_patch
--    Records that a participant is (or was) a member of a club, or ends an
--    existing membership.  Mirrors yacht_ownership semantics.
-- ---------------------------------------------------------------------------

CREATE TABLE manual_club_membership_patch (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_participant_id    UUID        NOT NULL
                                            REFERENCES candidate_participant(id)
                                            ON DELETE CASCADE,
    candidate_club_id           UUID        NOT NULL
                                            REFERENCES candidate_club(id)
                                            ON DELETE CASCADE,
    membership_role             TEXT        NOT NULL DEFAULT 'member'
                                            CHECK (membership_role IN ('member','officer','commodore','fleet_captain','junior')),
    effective_start             DATE,
    effective_end               DATE,
    -- 'add' = assert membership; 'remove' = end/revoke membership
    operation                   TEXT        NOT NULL DEFAULT 'add'
                                            CHECK (operation IN ('add','remove')),
    -- Provenance
    actor                       TEXT        NOT NULL,
    reason_code                 TEXT,
    session_id                  TEXT,
    -- Idempotency + lifecycle
    patch_hash                  TEXT        NOT NULL,
    status                      TEXT        NOT NULL DEFAULT 'active'
                                            CHECK (status IN ('active','superseded','revoked')),
    -- Timestamps
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX ux_manual_club_membership_patch_hash
    ON manual_club_membership_patch (patch_hash);

-- One active 'add' row per (participant, club, role)
CREATE UNIQUE INDEX ux_manual_club_membership_active_add
    ON manual_club_membership_patch (candidate_participant_id, candidate_club_id, membership_role)
    WHERE status = 'active' AND operation = 'add';

CREATE INDEX idx_manual_club_membership_participant
    ON manual_club_membership_patch (candidate_participant_id)
    WHERE status = 'active';

CREATE TRIGGER trg_manual_club_membership_patch_updated_at
    BEFORE UPDATE ON manual_club_membership_patch
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
