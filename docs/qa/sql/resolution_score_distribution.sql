SELECT
    entity_type,
    MIN(quality_score) AS score_min,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY quality_score) AS score_p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY quality_score) AS score_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY quality_score) AS score_p75,
    MAX(quality_score) AS score_max,
    COUNT(*) AS n
FROM (
    SELECT 'club' AS entity_type, quality_score FROM candidate_club
    UNION ALL
    SELECT 'participant', quality_score FROM candidate_participant
    UNION ALL
    SELECT 'yacht', quality_score FROM candidate_yacht
    UNION ALL
    SELECT 'event', quality_score FROM candidate_event
    UNION ALL
    SELECT 'registration', quality_score FROM candidate_registration
) s
GROUP BY 1
ORDER BY 1;
