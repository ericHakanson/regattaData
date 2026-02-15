-- Validate core integrity constraints added during review hardening.

DO $$
DECLARE
  v_dr_entry uuid;
  v_participant uuid;
  v_entry uuid;
  v_yacht uuid;
  saw_expected_failure boolean;
BEGIN
  SELECT dr.id
  INTO v_dr_entry
  FROM document_requirement dr
  JOIN document_type dt ON dt.id = dr.document_type_id
  WHERE dt.normalized_name = 'smoke-dock-fee'
    AND dt.scope = 'entry'
  LIMIT 1;

  SELECT id INTO v_participant FROM participant WHERE normalized_full_name = 'smoke participant one' LIMIT 1;
  SELECT id INTO v_entry FROM event_entry ORDER BY created_at LIMIT 1;
  SELECT id INTO v_yacht FROM yacht WHERE normalized_name = 'smoke-boat-a' LIMIT 1;

  -- XOR rule: both subject columns NULL must fail.
  saw_expected_failure := false;
  BEGIN
    INSERT INTO document_status (document_requirement_id, status, status_at, source_system)
    VALUES (v_dr_entry, 'missing', now(), 'smoke');
  EXCEPTION WHEN check_violation THEN
    saw_expected_failure := true;
  END;
  IF NOT saw_expected_failure THEN
    RAISE EXCEPTION 'Expected check_violation for NULL participant_id + NULL event_entry_id';
  END IF;

  -- XOR rule: both subject columns populated must fail.
  saw_expected_failure := false;
  BEGIN
    INSERT INTO document_status (document_requirement_id, participant_id, event_entry_id, status, status_at, source_system)
    VALUES (v_dr_entry, v_participant, v_entry, 'missing', now(), 'smoke');
  EXCEPTION WHEN check_violation THEN
    saw_expected_failure := true;
  END;
  IF NOT saw_expected_failure THEN
    RAISE EXCEPTION 'Expected check_violation for populated participant_id + event_entry_id';
  END IF;

  -- document_requirement partial uniqueness: NULL role duplicate must fail.
  saw_expected_failure := false;
  BEGIN
    INSERT INTO document_requirement (event_instance_id, document_type_id, required_for_role, is_mandatory)
    SELECT event_instance_id, document_type_id, NULL, true
    FROM document_requirement dr
    JOIN document_type dt ON dt.id = dr.document_type_id
    WHERE dt.normalized_name = 'smoke-dock-fee'
      AND dt.scope = 'entry'
    LIMIT 1;
  EXCEPTION WHEN unique_violation THEN
    saw_expected_failure := true;
  END;
  IF NOT saw_expected_failure THEN
    RAISE EXCEPTION 'Expected unique_violation for duplicate NULL required_for_role';
  END IF;

  -- yacht_rating partial uniqueness: NULL effective_start duplicate must fail.
  INSERT INTO yacht_rating (yacht_id, rating_system, rating_category, rating_value, source_system)
  VALUES (v_yacht, 'PHRF', 'Spinnaker', '100', 'smoke');

  saw_expected_failure := false;
  BEGIN
    INSERT INTO yacht_rating (yacht_id, rating_system, rating_category, rating_value, source_system)
    VALUES (v_yacht, 'PHRF', 'Spinnaker', '101', 'smoke');
  EXCEPTION WHEN unique_violation THEN
    saw_expected_failure := true;
  END;
  IF NOT saw_expected_failure THEN
    RAISE EXCEPTION 'Expected unique_violation for duplicate yacht_rating NULL effective_start';
  END IF;
END $$;
