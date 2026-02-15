# Cloud SQL Schema Specification (Initial)

## Scope
This specification defines the initial PostgreSQL schema for the regatta orchestration data platform.

## Conventions
- Primary keys: `uuid` with DB-generated default.
- Timestamps: `timestamptz` in UTC.
- Mutable tables include `created_at` and `updated_at`.
- Source provenance fields included where source disagreement is possible.

## Core Tables

### `yacht_club`
- `id` uuid pk
- `name` text not null
- `normalized_name` text not null
- `vitality_status` text not null check in (`active`, `inactive`, `unknown`)
- `vitality_last_verified_at` timestamptz null
- `website_url` text null
- `notes` text null
- `created_at`, `updated_at`

Constraints/indexes:
- unique index on `normalized_name`
- index on `vitality_status`

### `participant`
- `id` uuid pk
- `full_name` text not null
- `normalized_full_name` text not null
- `first_name` text null
- `last_name` text null
- `date_of_birth` date null
- `is_deceased` boolean not null default false
- `created_at`, `updated_at`

Constraints/indexes:
- index on `normalized_full_name`

### `participant_contact_point`
- `id` uuid pk
- `participant_id` uuid fk -> `participant.id`
- `contact_type` text not null check in (`email`, `phone`, `social`)
- `contact_subtype` text null (for example `mobile`, `home`, `instagram`)
- `contact_value_raw` text not null
- `contact_value_normalized` text null
- `is_primary` boolean not null default false
- `is_verified` boolean not null default false
- `source_system` text not null
- `created_at`, `updated_at`

Constraints/indexes:
- index on `participant_id`
- index on `contact_value_normalized`

### `participant_address`
- `id` uuid pk
- `participant_id` uuid fk -> `participant.id`
- `address_type` text not null check in (`mailing`, `residential`, `other`)
- `line1`, `line2`, `city`, `state`, `postal_code`, `country_code`
- `address_raw` text null
- `is_primary` boolean not null default false
- `source_system` text not null
- `created_at`, `updated_at`

Constraints/indexes:
- index on `participant_id`

### `yacht`
- `id` uuid pk
- `name` text not null
- `normalized_name` text not null
- `sail_number` text null
- `normalized_sail_number` text null
- `builder` text null
- `designer` text null
- `model` text null
- `length_feet` numeric(6,2) null
- `created_at`, `updated_at`

Constraints/indexes:
- index on `normalized_name`
- index on `normalized_sail_number`

### `yacht_rating`
- `id` uuid pk
- `yacht_id` uuid fk -> `yacht.id`
- `rating_system` text not null
- `rating_category` text not null
- `rating_value` text not null
- `effective_start` date null
- `effective_end` date null
- `source_system` text not null
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`yacht_id`, `rating_system`, `rating_category`, `effective_start`)
- check (`effective_end` is null or `effective_end` >= `effective_start`)

### `club_membership`
- `id` uuid pk
- `participant_id` uuid fk -> `participant.id`
- `yacht_club_id` uuid fk -> `yacht_club.id`
- `membership_role` text null
- `effective_start` date null
- `effective_end` date null
- `source_system` text not null
- `created_at`, `updated_at`

Constraints/indexes:
- index on (`participant_id`, `yacht_club_id`)
- check (`effective_end` is null or `effective_end` >= `effective_start`)

### `yacht_ownership`
- `id` uuid pk
- `participant_id` uuid fk -> `participant.id`
- `yacht_id` uuid fk -> `yacht.id`
- `role` text not null check in (`owner`, `co_owner`)
- `ownership_pct` numeric(5,2) null
- `is_primary_contact` boolean not null default false
- `effective_start` date not null
- `effective_end` date null
- `source_system` text not null
- `created_at`, `updated_at`

Constraints/indexes:
- index on (`participant_id`, `yacht_id`)
- check (`ownership_pct` is null or (`ownership_pct` >= 0 and `ownership_pct` <= 100))
- check (`effective_end` is null or `effective_end` >= `effective_start`)

### `event_series`
- `id` uuid pk
- `yacht_club_id` uuid fk -> `yacht_club.id`
- `name` text not null
- `normalized_name` text not null
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`yacht_club_id`, `normalized_name`)

### `event_instance`
- `id` uuid pk
- `event_series_id` uuid fk -> `event_series.id`
- `display_name` text not null
- `season_year` int not null
- `start_date` date null
- `end_date` date null
- `registration_open_at` timestamptz null
- `registration_close_at` timestamptz null
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`event_series_id`, `season_year`)
- check (`end_date` is null or `start_date` is null or `end_date` >= `start_date`)

### `event_entry`
- `id` uuid pk
- `event_instance_id` uuid fk -> `event_instance.id`
- `yacht_id` uuid fk -> `yacht.id`
- `entry_status` text not null check in (`draft`, `submitted`, `confirmed`, `withdrawn`, `unknown`)
- `registration_source` text not null
- `registration_external_id` text null
- `registered_at` timestamptz null
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`event_instance_id`, `yacht_id`)
- index on (`registration_source`, `registration_external_id`)

### `event_entry_participant`
- `id` uuid pk
- `event_entry_id` uuid fk -> `event_entry.id`
- `participant_id` uuid fk -> `participant.id`
- `role` text not null check in (`skipper`, `crew`, `owner_contact`, `registrant`, `other`)
- `participation_state` text not null check in (`participating`, `non_participating_contact`, `unknown`)
- `source_system` text not null
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`event_entry_id`, `participant_id`, `role`)
- index on `participant_id`

### `document_type`
- `id` uuid pk
- `name` text not null
- `normalized_name` text not null
- `scope` text not null check in (`participant`, `entry`, `yacht`)
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`normalized_name`, `scope`)

### `document_requirement`
- `id` uuid pk
- `event_instance_id` uuid fk -> `event_instance.id`
- `document_type_id` uuid fk -> `document_type.id`
- `required_for_role` text null
- `due_at` timestamptz null
- `is_mandatory` boolean not null default true
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`event_instance_id`, `document_type_id`, `required_for_role`)

### `document_status`
- `id` uuid pk
- `document_requirement_id` uuid fk -> `document_requirement.id`
- `participant_id` uuid fk -> `participant.id` null
- `event_entry_id` uuid fk -> `event_entry.id` null
- `status` text not null check in (`missing`, `received`, `expired`, `unknown`)
- `status_at` timestamptz not null
- `evidence_ref` text null
- `source_system` text not null
- `created_at`, `updated_at`

Constraints/indexes:
- check (not (`participant_id` is null and `event_entry_id` is null))
- index on (`status`, `status_at`)

### `identity_candidate_match`
- `id` uuid pk
- `entity_type` text not null check in (`participant`, `yacht`, `yacht_club`, `event`)
- `left_entity_id` uuid not null
- `right_entity_id` uuid not null
- `score` numeric(5,4) not null
- `feature_payload` jsonb not null
- `decision` text not null check in (`auto_merge`, `review`, `reject`)
- `decided_at` timestamptz not null
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`entity_type`, `left_entity_id`, `right_entity_id`)
- index on (`entity_type`, `decision`, `score`)

### `identity_merge_action`
- `id` uuid pk
- `entity_type` text not null
- `surviving_entity_id` uuid not null
- `merged_entity_id` uuid not null
- `match_id` uuid fk -> `identity_candidate_match.id`
- `merge_method` text not null check in (`auto`, `manual`)
- `merged_at` timestamptz not null
- `merged_by` text not null
- `is_reverted` boolean not null default false
- `reverted_at` timestamptz null
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`entity_type`, `merged_entity_id`)
- index on (`entity_type`, `surviving_entity_id`)

### `next_best_action`
- `id` uuid pk
- `action_type` text not null
- `target_entity_type` text not null
- `target_entity_id` uuid not null
- `event_instance_id` uuid fk -> `event_instance.id` null
- `priority_score` numeric(8,4) not null
- `reason_code` text not null
- `reason_detail` text not null
- `recommended_channel` text not null
- `generated_at` timestamptz not null
- `rule_version` text not null
- `status` text not null check in (`open`, `dismissed`, `actioned`)
- `created_at`, `updated_at`

Constraints/indexes:
- index on (`status`, `priority_score`)
- index on (`event_instance_id`, `action_type`)

### `raw_asset`
- `id` uuid pk
- `source_system` text not null
- `asset_type` text not null
- `gcs_bucket` text not null
- `gcs_object` text not null
- `content_hash` text null
- `captured_at` timestamptz not null
- `retention_delete_after` date not null
- `created_at`, `updated_at`

Constraints/indexes:
- unique (`gcs_bucket`, `gcs_object`)
- index on (`source_system`, `captured_at`)
- index on `retention_delete_after`

## Retention/Policy Mapping
- Raw assets in `raw_asset.retention_delete_after`: `captured_at + interval '3 years'`.
- PII lifecycle policy (10 years) should be enforced with periodic policy jobs over participant-linked tables.

## Required Materialized Views (Initial)
1. `mv_missing_required_documents`
2. `mv_probable_unregistered_returners`
3. `mv_entity_resolution_review_queue`

## Notes for Implementation
- Use extensions: `pgcrypto` (or `uuid-ossp`) and `citext` if case-insensitive keys are needed.
- Implement update triggers for `updated_at`.
- Keep large source payloads out of Cloud SQL; store in GCS and reference from `raw_asset`.
