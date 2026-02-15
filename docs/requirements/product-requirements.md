# Product Requirements

## Problem Statement
Regatta organizers need a single orchestration layer to identify who to engage and why, across yachts, participants, yacht clubs, and recurring events.

## Product Goal
Provide reliable, queryable intelligence to drive event participation and operational follow-up, including:
- participation growth targeting,
- missing documentation follow-up,
- campaign audience targeting (for use by external messaging systems).

## Scope
### In Scope
- Consolidated data layer for clubs, yachts, events, participants, ownership, membership, registrations, and documentation status.
- Historical tracking across years.
- Recommendation outputs for "next best action".
- Storage of large raw artifacts (scraped pages and similar) in GCS with metadata in Cloud SQL.

### Out of Scope
- Registration form hosting/submission.
- Payment collection and processing.
- Direct outbound messaging (email/SMS execution).

## Operating Reality (Confirmed)
1. Registration source (yacht-level): `regattaman.com`.
2. Waiver source (participant-level): `Jotform`.
3. Scraped sources include `regattaman.com` and `yachtscoring.com`.
4. `regattaman.com` does not provide participant-level coverage; `yachtscoring.com` does.
5. Access to other clubs' internal systems is not assumed.

## Core Domain Requirements
1. Yacht clubs run recurring events with the same name year over year.
2. Yachts can participate in zero or more events across years.
3. Participants can participate in zero or more events across years.
4. Yacht ownership is time-varying, supports multiple owners, and is historically tracked.
5. Participants can be associated with zero or more clubs, yachts, and events.
6. Outlier participation scenarios must be supported, including:
- an owner not participating in their yacht's event entry,
- an owner participating on a different yacht in the same event.

## Data Requirements
### Yacht Clubs
- Name
- Location and address
- Notes/details
- Website and social URLs
- Vitality status

### Yachts
- Name
- Type (builder and/or designer)
- Sail number
- Multiple ratings by system/category (for example: PHRF spinnaker/non-spinnaker, IRC TCC)
- Owner history (current and former)

### Participants
- Full name
- Phones (multiple with type)
- Addresses (stored as provided; no paid third-party validation in initial phase)
- Emails
- Social profiles
- Yacht ownership links
- Club membership links
- Event participation links
- Waiver/document completion status

## Functional Requirements
1. Import and reconcile data from multiple sources (manual imports, scraped content, external registration systems).
2. Preserve history (ownership, membership, participation, documentation status) with effective dates.
3. Produce action-ready segments, such as:
- likely participants not yet registered,
- entries missing waiver/required docs,
- high-value targets for campaign exports.
4. Support annual event recurrence without data duplication errors.
5. Provide auditable recommendation rationale.
6. Run entity resolution for people, clubs, events, and yachts with confidence scoring and auto-merge for high-confidence matches.

## Non-Functional Requirements
- Clear data lineage from raw source to curated records.
- Idempotent ingestion jobs.
- Role-based access and least privilege on GCP resources.
- PII-aware controls (encryption at rest, restricted access, audit logging).
- Operational observability for pipelines and recommendation jobs.

## Success Metrics (Initial)
- Data freshness SLA (target): daily for primary entities.
- Match confidence threshold for identity resolution.
- Recommendation precision proxy (manual spot-check acceptance rate).
- Reduction in missing-document cases before race day.
