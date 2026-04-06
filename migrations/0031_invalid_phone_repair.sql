-- Migration: 0031_invalid_phone_repair.sql
-- Purpose: Remove malformed phone values from participant candidate/canonical
--          layers and clear invalid best_phone fields (FOR-226).

BEGIN;

-- Source layer: retain raw values for provenance, but normalized phone must be
-- valid E.164-style (+ followed by 10-15 digits) or NULL.
UPDATE participant_contact_point
SET contact_value_normalized = NULL
WHERE contact_type = 'phone'
  AND contact_value_normalized IS NOT NULL
  AND contact_value_normalized !~ '^\+\d{10,15}$';

-- Candidate/canonical child rows should not retain malformed phone contacts.
DELETE FROM candidate_participant_contact
WHERE contact_type = 'phone'
  AND NOT (
        COALESCE(normalized_value, raw_value, '') ~ '^\+\d{10,15}$'
  );

DELETE FROM canonical_participant_contact
WHERE contact_type = 'phone'
  AND NOT (
        COALESCE(normalized_value, raw_value, '') ~ '^\+\d{10,15}$'
  );

-- Clear invalid best_phone fields that can still leak into scoring/exports.
UPDATE candidate_participant
SET best_phone = NULL
WHERE best_phone IS NOT NULL
  AND best_phone !~ '^\+\d{10,15}$';

UPDATE canonical_participant
SET best_phone = NULL
WHERE best_phone IS NOT NULL
  AND best_phone !~ '^\+\d{10,15}$';

COMMIT;
