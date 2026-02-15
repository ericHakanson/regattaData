# ADR-002: Unified Participant Model for Owners and Event Participants

## Status
Accepted

## Date
2026-02-15

## Context
Historical data includes separate concepts of owners and participants. The domain requires time-varying yacht ownership and event-specific crew/registrant roles.

## Decision
Use a unified `participant` entity for all people.

Represent semantics through link tables:
- Ownership: `yacht_ownership(participant_id, yacht_id, role, effective_start, effective_end, ownership_pct, is_primary_contact)`
- Event involvement: `event_entry_participant(event_entry_id, participant_id, role, participation_state)`

Registration state remains on `event_entry` and related status fields, not on participant directly.

## Rationale
- Avoids duplicate person records and cross-table synchronization issues.
- Preserves historical ownership and event role changes independently.
- Supports outlier scenarios where owner/contact and participant roles diverge.

## Consequences
- Identity resolution quality is critical for participant deduplication.
- Role enums and temporal constraints must be enforced with application and DB constraints.
