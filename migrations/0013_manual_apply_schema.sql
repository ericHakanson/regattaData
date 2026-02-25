-- Migration 0013: Expand resolution_manual_action_log.action_type CHECK
--
-- Adds 'reject' and 'hold' as valid action_type values so that
-- resolution_manual_apply can log state-change decisions distinctly.
--
-- Uses a DO loop to drop ALL CHECK constraints referencing action_type by
-- content rather than by name — safe when constraint names differ across
-- environments or multiple partial checks exist.

DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'resolution_manual_action_log'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%action_type%'
    LOOP
        EXECUTE 'ALTER TABLE resolution_manual_action_log DROP CONSTRAINT '
                || quote_ident(r.conname);
    END LOOP;
END;
$$;

ALTER TABLE resolution_manual_action_log
    ADD CONSTRAINT resolution_manual_action_log_action_type_check
    CHECK (action_type IN
        ('promote', 'merge', 'split', 'demote', 'edit', 'unlink', 'reject', 'hold'));
