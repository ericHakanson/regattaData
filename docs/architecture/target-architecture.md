# Target Architecture (Google Cloud)

## Architectural Summary
- System of record for relational operations: `Cloud SQL for PostgreSQL`.
- Raw and heavy assets: `GCS`.
- Orchestration and APIs: `Cloud Run` services.
- Scheduling and async work: `Cloud Scheduler` + `Pub/Sub`.
- Secrets and credentials: `Secret Manager`.
- Observability: `Cloud Logging` and `Cloud Monitoring`.

## Source System Model
1. Registration source (yacht entry): `regattaman.com`.
2. Waiver source (participant document status): `Jotform`.
3. Supplemental participant/event signal via scraping: `yachtscoring.com`.
4. No assumption of direct system integration for other clubs.

## Logical Components
1. Ingestion Services
- Pull/scrape source records and snapshots.
- Write raw payloads/artifacts to GCS.
- Write ingestion metadata and normalized records to Cloud SQL.

2. Entity Resolution Services
- Resolve and merge participants, clubs, events, and yachts with scored matches.
- Persist scoring evidence and merge lineage.

3. Core Data Services
- Maintain normalized transactional schema with temporal history.
- Enforce role and date-window constraints.

4. Recommendation Engine (Next Best Action)
- Rule-based scorer over curated Cloud SQL views/materialized tables.
- Produces prioritized actions and rationale.

5. Export Services
- Publish audience/action exports for downstream campaign tools via versioned files in GCS.

## Core Relational Model
Recommended core tables (minimum set):
- `yacht_club`
- `participant`
- `participant_contact_point` (phone/email/social)
- `participant_address`
- `yacht`
- `yacht_rating`
- `club_membership`
- `yacht_ownership` (participant-to-yacht with effective dates)
- `event_series` (recurring named event owned by club)
- `event_instance` (year/date-specific occurrence)
- `event_entry` (a yacht's registration/entry in an event instance)
- `event_entry_participant` (participants and their role on an entry)
- `document_type`
- `document_requirement`
- `document_status`
- `identity_candidate_match`
- `identity_merge_action`
- `next_best_action`
- `raw_asset` (metadata pointer to GCS object)

## Key Relationship Decisions
1. Recurring events
- Use `event_series` for stable identity and `event_instance` per year/edition.

2. Owners and participants
- Use one person table: `participant`.
- Distinguish ownership through `yacht_ownership` rows with effective dating.
- Distinguish event involvement through `event_entry_participant` roles.

3. Registration handling
- Model registration at `event_entry` (yacht entered in an event instance).
- Map people to the entry via `event_entry_participant`.
- Auto-propagate active owners as `owner_contact` on entry when no explicit owner-contact exists.
- Allow owner-contact to exist without crew/skipper participation status.

4. Outlier support
- Do not enforce rule that owner must crew on owned yacht.
- Do not enforce rule that participant can only appear on one yacht per event unless explicitly defined by race rules.

5. Documentation tracking
- Track required documents independently from submission mechanism.
- `document_status` stores assertion of state (`missing`, `received`, `expired`, `unknown`) and source evidence.

## Example Role Model
Recommended role enums:
- `event_entry_participant.role`: `skipper`, `crew`, `owner_contact`, `registrant`, `other`
- `event_entry_participant.participation_state`: `participating`, `non_participating_contact`, `unknown`
- `yacht_ownership.role`: `owner`, `co_owner`

## Entity Resolution Baseline
1. Deterministic stage
- exact email/phone normalization matches,
- exact or near-exact sail number + yacht name,
- club/event name normalization with date context.

2. Scored stage
- weighted features produce confidence score per entity type.
- high-confidence candidates auto-merge.
- medium confidence routed for manual review.

3. Auditability
- persist input features, scores, and merge decisions.
- support unmerge/correction workflow.

## Next Best Action (NBA) Baseline
Start with deterministic rules, for example:
- `missing_required_document`: entry has unmet required document before event cutoff.
- `high_likelihood_not_registered`: participant/yacht has repeated historical attendance but no current entry.
- `reactivation_target`: previously active participant inactive for N seasons.
- `club_growth_candidate`: member of affiliated club with matching yacht profile and no recent attendance.

Each action should include:
- `action_type`
- `priority_score`
- `reason_code`
- `reason_detail`
- `recommended_channel` (email/social/manual follow-up)
- `generated_at`

## Data Governance
- Use UUID primary keys.
- Use `created_at`, `updated_at`, and source provenance fields on mutable entities.
- Soft-delete only when required; prefer explicit status fields.
- Add unique constraints for natural keys where stable (for example, normalized sail number + class context).
