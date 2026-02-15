# ADR-004: Entity Resolution and Confidence-Scored Auto-Merge

## Status
Accepted

## Date
2026-02-15

## Context
The system must reconcile duplicate and near-duplicate records across people, clubs, events, and yachts.

## Decision
Implement entity resolution with:
1. deterministic matching stage,
2. weighted scoring stage,
3. auto-merge for high-confidence candidates,
4. manual review for medium-confidence candidates.

Coverage includes participants, clubs, events, and yachts.

Accepted baseline thresholds:
- participants: `>= 0.95`
- yachts: `>= 0.97`
- clubs: `>= 0.98`
- events: `>= 0.98`

## Consequences
- Match features, scores, and merge lineage must be stored for audit.
- Thresholds are configurable and should be tuned against measured merge quality.
- Unmerge/correction capability is required.
