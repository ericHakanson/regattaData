-- Refresh materialized views and validate expected missing-document output.

REFRESH MATERIALIZED VIEW mv_missing_required_documents;
REFRESH MATERIALIZED VIEW mv_probable_unregistered_returners;
REFRESH MATERIALIZED VIEW mv_entity_resolution_review_queue;

DO $$
DECLARE
  c_participant_received int;
  c_participant_missing int;
  c_entry_missing int;
  c_yacht_missing int;
BEGIN
  SELECT count(*) INTO c_participant_received
  FROM mv_missing_required_documents mv
  JOIN document_requirement dr ON dr.id = mv.document_requirement_id
  JOIN document_type dt ON dt.id = dr.document_type_id
  WHERE dt.normalized_name = 'smoke-waiver'
    AND dt.scope = 'participant';

  IF c_participant_received <> 0 THEN
    RAISE EXCEPTION 'Expected 0 missing rows for satisfied participant requirement, got %', c_participant_received;
  END IF;

  SELECT count(*) INTO c_participant_missing
  FROM mv_missing_required_documents mv
  JOIN document_requirement dr ON dr.id = mv.document_requirement_id
  JOIN document_type dt ON dt.id = dr.document_type_id
  WHERE dt.normalized_name = 'smoke-crew-brief'
    AND dt.scope = 'participant';

  -- Participant appears on two entries but should appear once for the requirement.
  IF c_participant_missing <> 1 THEN
    RAISE EXCEPTION 'Expected 1 missing row for participant requirement dedupe case, got %', c_participant_missing;
  END IF;

  SELECT count(*) INTO c_entry_missing
  FROM mv_missing_required_documents mv
  JOIN document_requirement dr ON dr.id = mv.document_requirement_id
  JOIN document_type dt ON dt.id = dr.document_type_id
  WHERE dt.normalized_name = 'smoke-dock-fee'
    AND dt.scope = 'entry';

  IF c_entry_missing <> 1 THEN
    RAISE EXCEPTION 'Expected 1 missing row for entry requirement, got %', c_entry_missing;
  END IF;

  SELECT count(*) INTO c_yacht_missing
  FROM mv_missing_required_documents mv
  JOIN document_requirement dr ON dr.id = mv.document_requirement_id
  JOIN document_type dt ON dt.id = dr.document_type_id
  WHERE dt.normalized_name = 'smoke-insurance'
    AND dt.scope = 'yacht';

  IF c_yacht_missing <> 1 THEN
    RAISE EXCEPTION 'Expected 1 missing row for yacht requirement, got %', c_yacht_missing;
  END IF;
END $$;
