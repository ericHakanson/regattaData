# Resolution Diagnostics Queries

Run these queries before and after applying YAML policy changes to understand
candidate state distributions, score ranges, and promotion readiness.

---

## 1. State Counts per Entity Type

```sql
SELECT
    entity_type,
    resolution_state,
    is_promoted,
    COUNT(*) AS n
FROM (
    SELECT 'club'         AS entity_type, resolution_state, is_promoted FROM candidate_club
    UNION ALL
    SELECT 'participant',                  resolution_state, is_promoted FROM candidate_participant
    UNION ALL
    SELECT 'yacht',                        resolution_state, is_promoted FROM candidate_yacht
    UNION ALL
    SELECT 'event',                        resolution_state, is_promoted FROM candidate_event
    UNION ALL
    SELECT 'registration',                 resolution_state, is_promoted FROM candidate_registration
) s
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

---

## 2. Score Distribution per Entity Type (min / p25 / p50 / p75 / max)

```sql
SELECT
    entity_type,
    MIN(quality_score)                                    AS score_min,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY quality_score) AS score_p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY quality_score) AS score_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY quality_score) AS score_p75,
    MAX(quality_score)                                    AS score_max,
    COUNT(*)                                              AS n
FROM (
    SELECT 'club'         AS entity_type, quality_score FROM candidate_club
    UNION ALL
    SELECT 'participant',                  quality_score FROM candidate_participant
    UNION ALL
    SELECT 'yacht',                        quality_score FROM candidate_yacht
    UNION ALL
    SELECT 'event',                        quality_score FROM candidate_event
    UNION ALL
    SELECT 'registration',                 quality_score FROM candidate_registration
) s
GROUP BY 1
ORDER BY 1;
```

---

## 3. Top confidence_reasons Frequencies per Entity Type

Returns the top 20 most common individual reason strings across all candidates
(expand `LIMIT` and filter on `entity_type` as needed).

```sql
WITH reason_rows AS (
    SELECT 'club' AS entity_type, jsonb_array_elements_text(confidence_reasons) AS reason
    FROM candidate_club
    UNION ALL
    SELECT 'participant', jsonb_array_elements_text(confidence_reasons)
    FROM candidate_participant
    UNION ALL
    SELECT 'yacht', jsonb_array_elements_text(confidence_reasons)
    FROM candidate_yacht
    UNION ALL
    SELECT 'event', jsonb_array_elements_text(confidence_reasons)
    FROM candidate_event
    UNION ALL
    SELECT 'registration', jsonb_array_elements_text(confidence_reasons)
    FROM candidate_registration
)
SELECT entity_type, reason, COUNT(*) AS freq
FROM reason_rows
GROUP BY 1, 2
ORDER BY 1, 3 DESC
LIMIT 20;
```

---

## 4. Rows That Would Newly Enter auto_promote Under Current v1.1.0 Rules

These queries show candidates whose current `quality_score` already meets the
new YAML thresholds but are not yet in `auto_promote` state.  Running
`resolution_score --entity-type all` with the v1.1.0 YAML files will promote
them.

### Clubs (auto_promote threshold: 0.45)
```sql
SELECT COUNT(*) AS would_enter_auto_promote
FROM candidate_club
WHERE quality_score >= 0.45
  AND resolution_state <> 'auto_promote'
  AND is_promoted = false;
```

### Participants (auto_promote threshold: 0.75)
```sql
SELECT COUNT(*) AS would_enter_auto_promote
FROM candidate_participant
WHERE quality_score >= 0.75
  AND resolution_state <> 'auto_promote'
  AND is_promoted = false;
```

### Yachts (auto_promote threshold: 0.75)
```sql
SELECT COUNT(*) AS would_enter_auto_promote
FROM candidate_yacht
WHERE quality_score >= 0.75
  AND resolution_state <> 'auto_promote'
  AND is_promoted = false;
```

### Combined (all three entity types)
```sql
SELECT
    (SELECT COUNT(*) FROM candidate_club        WHERE quality_score >= 0.45 AND resolution_state <> 'auto_promote' AND is_promoted = false) AS club_promotable,
    (SELECT COUNT(*) FROM candidate_participant WHERE quality_score >= 0.75 AND resolution_state <> 'auto_promote' AND is_promoted = false) AS participant_promotable,
    (SELECT COUNT(*) FROM candidate_yacht       WHERE quality_score >= 0.75 AND resolution_state <> 'auto_promote' AND is_promoted = false) AS yacht_promotable;
```

---

## 5. Post-Promotion Canonical Row Counts

Run after `resolution_promote --entity-type all` to confirm canonical rows exist.

```sql
SELECT
    (SELECT COUNT(*) FROM canonical_club)         AS canonical_club,
    (SELECT COUNT(*) FROM canonical_participant)   AS canonical_participant,
    (SELECT COUNT(*) FROM canonical_yacht)         AS canonical_yacht,
    (SELECT COUNT(*) FROM canonical_event)         AS canonical_event,
    (SELECT COUNT(*) FROM canonical_registration)  AS canonical_registration;
```

---

## 6. Candidates Still Blocked from auto_promote

Candidates with a hard block reason in confidence_reasons.

```sql
WITH hard_blocked AS (
    SELECT 'club' AS entity_type, id, quality_score, resolution_state,
           jsonb_array_elements_text(confidence_reasons) AS reason
    FROM candidate_club
    UNION ALL
    SELECT 'participant', id, quality_score, resolution_state,
           jsonb_array_elements_text(confidence_reasons)
    FROM candidate_participant
    UNION ALL
    SELECT 'yacht', id, quality_score, resolution_state,
           jsonb_array_elements_text(confidence_reasons)
    FROM candidate_yacht
)
SELECT entity_type, reason, COUNT(DISTINCT id) AS blocked_candidates
FROM hard_blocked
WHERE reason LIKE 'hard_block:%'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

---

## Usage

Run **before** applying new YAML and scoring to establish baseline:

```bash
psql "$DB_DSN" -f docs/runbooks/resolution-diagnostics-queries.md
```

Run **after** `resolution_score --entity-type all` to confirm expected transitions.

Run **after** `resolution_promote --entity-type all` to confirm canonical rows.
