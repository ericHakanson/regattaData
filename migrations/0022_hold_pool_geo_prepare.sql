-- Migration: 0022_hold_pool_geo_prepare.sql
-- Purpose: Add tables for hold-pool address normalization and RocketReach
--          enrichment-readiness classification.
-- Ref: docs/requirements/participant-hold-pool-address-normalization-rocketreach-readiness-spec.md

BEGIN;

-- ============================================================
-- candidate_participant_geo_hint
-- One row per unique raw address per candidate.
-- Stores the normalized geographic hint derived from each
-- candidate_participant_address row; is_best_hint=true marks
-- the single hint selected for enrichment purposes.
-- ============================================================
CREATE TABLE candidate_participant_geo_hint (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_participant_id uuid        NOT NULL REFERENCES candidate_participant (id),
    source_address_row_id   text,                           -- soft ref to candidate_participant_address.id
    address_raw             text        NOT NULL,
    normalized_hint         text,                           -- canonical form for dedup / display
    city                    text,
    state_region            text,
    country_code            text,
    quality_class           text        NOT NULL CHECK (quality_class IN (
                                            'city_state_country',
                                            'city_country',
                                            'state_country',
                                            'country_only',
                                            'freeform_partial',
                                            'non_address_junk',
                                            'empty_or_null'
                                        )),
    is_best_hint            bool        NOT NULL DEFAULT false,
    created_at              timestamptz NOT NULL DEFAULT now()
);

-- One row per unique raw address per candidate
CREATE UNIQUE INDEX idx_geo_hint_candidate_address
    ON candidate_participant_geo_hint (candidate_participant_id, address_raw);

CREATE INDEX idx_geo_hint_candidate_id
    ON candidate_participant_geo_hint (candidate_participant_id);

-- Partial index for fast "get best hint for candidate" lookup
CREATE INDEX idx_geo_hint_best
    ON candidate_participant_geo_hint (candidate_participant_id)
    WHERE is_best_hint = true;

-- ============================================================
-- candidate_participant_enrichment_readiness
-- One row per candidate processed by participant_hold_geo_prepare.
-- Replaced in-place on each rerun (ON CONFLICT DO UPDATE).
-- ============================================================
CREATE TABLE candidate_participant_enrichment_readiness (
    candidate_participant_id uuid        PRIMARY KEY REFERENCES candidate_participant (id),
    readiness_status         text        NOT NULL CHECK (readiness_status IN (
                                            'enrichment_ready',
                                            'too_vague',
                                            'junk_location',
                                            'multi_location_conflict',
                                            'already_has_contact',
                                            'not_hold'
                                        )),
    reason_code             text        NOT NULL,
    best_hint_text          text,
    best_city               text,
    best_state_region       text,
    best_country_code       text,
    evaluated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_enrichment_readiness_status
    ON candidate_participant_enrichment_readiness (readiness_status);

COMMIT;
