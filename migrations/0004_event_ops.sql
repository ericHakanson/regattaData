-- Migration: 0004_event_ops.sql
-- Purpose: Create event operations and document tracking tables.
-- Ref: docs/runbooks/05-cloud-sql-migration-plan.md (Phase 4)
--      docs/architecture/cloud-sql-schema-spec.md
-- Tables: event_entry, event_entry_participant, document_type,
--         document_requirement, document_status

BEGIN;

-- ============================================================
-- event_entry
-- ============================================================
CREATE TABLE event_entry (
    id                        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_instance_id         uuid        NOT NULL REFERENCES event_instance (id),
    yacht_id                  uuid        NOT NULL REFERENCES yacht (id),
    entry_status              text        NOT NULL CHECK (entry_status IN ('draft', 'submitted', 'confirmed', 'withdrawn', 'unknown')),
    registration_source       text        NOT NULL,
    registration_external_id  text,
    registered_at             timestamptz,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_instance_id, yacht_id)
);

CREATE INDEX idx_event_entry_registration ON event_entry (registration_source, registration_external_id);

CREATE TRIGGER trg_event_entry_updated_at
    BEFORE UPDATE ON event_entry
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- event_entry_participant
-- ============================================================
CREATE TABLE event_entry_participant (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_entry_id      uuid        NOT NULL REFERENCES event_entry (id),
    participant_id      uuid        NOT NULL REFERENCES participant (id),
    role                text        NOT NULL CHECK (role IN ('skipper', 'crew', 'owner_contact', 'registrant', 'other')),
    participation_state text        NOT NULL CHECK (participation_state IN ('participating', 'non_participating_contact', 'unknown')),
    source_system       text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_entry_id, participant_id, role)
);

CREATE INDEX idx_event_entry_participant_participant_id ON event_entry_participant (participant_id);

CREATE TRIGGER trg_event_entry_participant_updated_at
    BEFORE UPDATE ON event_entry_participant
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- document_type
-- ============================================================
CREATE TABLE document_type (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text        NOT NULL,
    normalized_name text        NOT NULL,
    scope           text        NOT NULL CHECK (scope IN ('participant', 'entry', 'yacht')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (normalized_name, scope)
);

CREATE TRIGGER trg_document_type_updated_at
    BEFORE UPDATE ON document_type
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- document_requirement
-- ============================================================
CREATE TABLE document_requirement (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_instance_id uuid        NOT NULL REFERENCES event_instance (id),
    document_type_id  uuid        NOT NULL REFERENCES document_type (id),
    required_for_role text,
    due_at            timestamptz,
    is_mandatory      boolean     NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- Partial unique indexes to handle nullable required_for_role correctly.
-- A single inline UNIQUE allows multiple rows with required_for_role IS NULL
-- for the same (event_instance_id, document_type_id) in Postgres.
CREATE UNIQUE INDEX idx_doc_requirement_unique_with_role
    ON document_requirement (event_instance_id, document_type_id, required_for_role)
    WHERE required_for_role IS NOT NULL;

CREATE UNIQUE INDEX idx_doc_requirement_unique_null_role
    ON document_requirement (event_instance_id, document_type_id)
    WHERE required_for_role IS NULL;

CREATE TRIGGER trg_document_requirement_updated_at
    BEFORE UPDATE ON document_requirement
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- document_status
-- ============================================================
CREATE TABLE document_status (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    document_requirement_id uuid        NOT NULL REFERENCES document_requirement (id),
    participant_id          uuid        REFERENCES participant (id),
    event_entry_id          uuid        REFERENCES event_entry (id),
    status                  text        NOT NULL CHECK (status IN ('missing', 'received', 'expired', 'unknown')),
    status_at               timestamptz NOT NULL,
    evidence_ref            text,
    source_system           text        NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    -- Exactly one subject must be set: participant XOR event_entry.
    CHECK (num_nonnulls(participant_id, event_entry_id) = 1)
);

CREATE INDEX idx_document_status_status_at ON document_status (status, status_at);

CREATE TRIGGER trg_document_status_updated_at
    BEFORE UPDATE ON document_status
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
