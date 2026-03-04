SELECT
    (SELECT COUNT(*) FROM canonical_club) AS canonical_club,
    (SELECT COUNT(*) FROM canonical_participant) AS canonical_participant,
    (SELECT COUNT(*) FROM canonical_yacht) AS canonical_yacht,
    (SELECT COUNT(*) FROM canonical_event) AS canonical_event,
    (SELECT COUNT(*) FROM canonical_registration) AS canonical_registration;
