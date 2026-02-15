-- Seed deterministic test data for migration smoke validation.

INSERT INTO yacht_club (name, normalized_name, vitality_status)
VALUES ('Smoke Club', 'smoke-club', 'active')
RETURNING id AS club_id \gset

INSERT INTO event_series (yacht_club_id, name, normalized_name)
VALUES (:'club_id', 'Smoke Regatta', 'smoke-regatta')
RETURNING id AS series_id \gset

INSERT INTO event_instance (event_series_id, display_name, season_year, registration_open_at)
VALUES (:'series_id', 'Smoke Regatta 2026', 2026, now())
RETURNING id AS instance_id \gset

INSERT INTO yacht (name, normalized_name)
VALUES ('Smoke Boat A', 'smoke-boat-a')
RETURNING id AS yacht_a \gset

INSERT INTO yacht (name, normalized_name)
VALUES ('Smoke Boat B', 'smoke-boat-b')
RETURNING id AS yacht_b \gset

INSERT INTO participant (full_name, normalized_full_name)
VALUES ('Smoke Participant One', 'smoke participant one')
RETURNING id AS p1 \gset

INSERT INTO event_entry (event_instance_id, yacht_id, entry_status, registration_source)
VALUES (:'instance_id', :'yacht_a', 'submitted', 'regattaman')
RETURNING id AS entry_a \gset

INSERT INTO event_entry (event_instance_id, yacht_id, entry_status, registration_source)
VALUES (:'instance_id', :'yacht_b', 'submitted', 'regattaman')
RETURNING id AS entry_b \gset

-- Same participant on two entries to validate participant dedupe behavior in MV.
INSERT INTO event_entry_participant (event_entry_id, participant_id, role, participation_state, source_system)
VALUES
  (:'entry_a', :'p1', 'crew', 'participating', 'smoke'),
  (:'entry_b', :'p1', 'crew', 'participating', 'smoke');

INSERT INTO document_type (name, normalized_name, scope)
VALUES ('Smoke Waiver', 'smoke-waiver', 'participant')
RETURNING id AS dt_participant_received \gset

INSERT INTO document_type (name, normalized_name, scope)
VALUES ('Smoke Crew Brief', 'smoke-crew-brief', 'participant')
RETURNING id AS dt_participant_missing \gset

INSERT INTO document_type (name, normalized_name, scope)
VALUES ('Smoke Dock Fee', 'smoke-dock-fee', 'entry')
RETURNING id AS dt_entry \gset

INSERT INTO document_type (name, normalized_name, scope)
VALUES ('Smoke Insurance', 'smoke-insurance', 'yacht')
RETURNING id AS dt_yacht \gset

INSERT INTO document_requirement (event_instance_id, document_type_id, required_for_role, is_mandatory)
VALUES (:'instance_id', :'dt_participant_received', 'crew', true)
RETURNING id AS dr_participant_received \gset

INSERT INTO document_requirement (event_instance_id, document_type_id, required_for_role, is_mandatory)
VALUES (:'instance_id', :'dt_participant_missing', 'crew', true)
RETURNING id AS dr_participant_missing \gset

INSERT INTO document_requirement (event_instance_id, document_type_id, is_mandatory)
VALUES (:'instance_id', :'dt_entry', true)
RETURNING id AS dr_entry \gset

INSERT INTO document_requirement (event_instance_id, document_type_id, is_mandatory)
VALUES (:'instance_id', :'dt_yacht', true)
RETURNING id AS dr_yacht \gset

-- Participant-scoped requirement satisfied.
INSERT INTO document_status (document_requirement_id, participant_id, status, status_at, source_system)
VALUES (:'dr_participant_received', :'p1', 'received', now(), 'smoke');

-- Entry-scoped requirement: entry_a received, entry_b missing.
INSERT INTO document_status (document_requirement_id, event_entry_id, status, status_at, source_system)
VALUES
  (:'dr_entry', :'entry_a', 'received', now(), 'smoke'),
  (:'dr_entry', :'entry_b', 'missing', now(), 'smoke');

-- Yacht-scoped requirement: yacht_a received through entry_a; yacht_b missing.
INSERT INTO document_status (document_requirement_id, event_entry_id, status, status_at, source_system)
VALUES (:'dr_yacht', :'entry_a', 'received', now(), 'smoke');
