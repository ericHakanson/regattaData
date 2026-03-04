SELECT
    entity_type,
    resolution_state,
    is_promoted,
    COUNT(*) AS n
FROM (
    SELECT 'club' AS entity_type, resolution_state, is_promoted FROM candidate_club
    UNION ALL
    SELECT 'participant', resolution_state, is_promoted FROM candidate_participant
    UNION ALL
    SELECT 'yacht', resolution_state, is_promoted FROM candidate_yacht
    UNION ALL
    SELECT 'event', resolution_state, is_promoted FROM candidate_event
    UNION ALL
    SELECT 'registration', resolution_state, is_promoted FROM candidate_registration
) s
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
