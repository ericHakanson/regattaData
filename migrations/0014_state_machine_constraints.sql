-- Migration: 0014_state_machine_constraints.sql
-- Purpose: Lock the resolution state machine at the DB level.
--
--   1. CHECK constraint ck_promoted_has_canonical on all 5 candidate tables:
--      if is_promoted=true then resolution_state must be 'auto_promote'
--      AND promoted_canonical_id must be non-null.
--
--   2. BEFORE UPDATE trigger enforce_candidate_state_transition() on all 5 tables:
--      Rule 1 — if both OLD and NEW are is_promoted=true, state cannot leave 'auto_promote'.
--      Rule 2 — cannot jump directly from 'reject' to 'auto_promote' (must re-score first).
--
--   Lifecycle op bypass: demote/unlink set is_promoted=false + resolution_state='review'
--   in one UPDATE; Rule 1 fires only when OLD.is_promoted=true AND NEW.is_promoted=true,
--   so lifecycle ops bypass Rule 1 without any session variable.
--
-- Depends on: 0011_candidate_canonical_core

-- ============================================================
-- 1. CHECK constraint on all 5 candidate tables
-- ============================================================

ALTER TABLE candidate_participant
    ADD CONSTRAINT ck_promoted_has_canonical
    CHECK (NOT is_promoted OR (
        resolution_state = 'auto_promote' AND promoted_canonical_id IS NOT NULL));

ALTER TABLE candidate_yacht
    ADD CONSTRAINT ck_promoted_has_canonical
    CHECK (NOT is_promoted OR (
        resolution_state = 'auto_promote' AND promoted_canonical_id IS NOT NULL));

ALTER TABLE candidate_club
    ADD CONSTRAINT ck_promoted_has_canonical
    CHECK (NOT is_promoted OR (
        resolution_state = 'auto_promote' AND promoted_canonical_id IS NOT NULL));

ALTER TABLE candidate_event
    ADD CONSTRAINT ck_promoted_has_canonical
    CHECK (NOT is_promoted OR (
        resolution_state = 'auto_promote' AND promoted_canonical_id IS NOT NULL));

ALTER TABLE candidate_registration
    ADD CONSTRAINT ck_promoted_has_canonical
    CHECK (NOT is_promoted OR (
        resolution_state = 'auto_promote' AND promoted_canonical_id IS NOT NULL));

-- ============================================================
-- 2. Trigger function for state-transition validation
-- ============================================================

CREATE OR REPLACE FUNCTION enforce_candidate_state_transition()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    -- Rule 1: Once promoted, resolution_state is locked to 'auto_promote'.
    -- Lifecycle ops (demote/unlink) set is_promoted=false first, so they bypass this.
    IF OLD.is_promoted = true AND NEW.is_promoted = true
       AND NEW.resolution_state <> 'auto_promote' THEN
        RAISE EXCEPTION
            'candidate % (id=%): promoted candidate cannot transition from auto_promote to %',
            TG_TABLE_NAME, NEW.id, NEW.resolution_state;
    END IF;

    -- Rule 2: Cannot jump directly from 'reject' to 'auto_promote'.
    -- Candidate must be re-scored through 'review' or 'hold' first.
    IF OLD.resolution_state = 'reject'
       AND NEW.resolution_state = 'auto_promote'
       AND OLD.is_promoted = false THEN
        RAISE EXCEPTION
            'candidate % (id=%): cannot transition reject -> auto_promote; re-score first',
            TG_TABLE_NAME, NEW.id;
    END IF;

    RETURN NEW;
END;
$$;

-- ============================================================
-- 3. Attach trigger to all 5 candidate tables
-- ============================================================

CREATE TRIGGER trg_candidate_participant_state
    BEFORE UPDATE ON candidate_participant
    FOR EACH ROW EXECUTE FUNCTION enforce_candidate_state_transition();

CREATE TRIGGER trg_candidate_yacht_state
    BEFORE UPDATE ON candidate_yacht
    FOR EACH ROW EXECUTE FUNCTION enforce_candidate_state_transition();

CREATE TRIGGER trg_candidate_club_state
    BEFORE UPDATE ON candidate_club
    FOR EACH ROW EXECUTE FUNCTION enforce_candidate_state_transition();

CREATE TRIGGER trg_candidate_event_state
    BEFORE UPDATE ON candidate_event
    FOR EACH ROW EXECUTE FUNCTION enforce_candidate_state_transition();

CREATE TRIGGER trg_candidate_registration_state
    BEFORE UPDATE ON candidate_registration
    FOR EACH ROW EXECUTE FUNCTION enforce_candidate_state_transition();
