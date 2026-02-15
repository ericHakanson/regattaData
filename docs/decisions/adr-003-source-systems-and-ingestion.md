# ADR-003: Initial Source Systems and Ingestion Boundaries

## Status
Accepted

## Date
2026-02-15

## Context
Data availability is fragmented and differs by source.

## Decision
Initial source system boundary is:
- `regattaman.com`: registration and yacht-entry signals.
- `Jotform`: participant waiver status.
- `yachtscoring.com`: supplemental participant/event signals via scraping.

No direct integration with other clubs' private systems is assumed in phase 1.

## Consequences
- Ingestion must preserve source provenance and confidence by attribute.
- Data model must tolerate partial and conflicting source coverage.
- Recommendation rules should account for source completeness gaps.
