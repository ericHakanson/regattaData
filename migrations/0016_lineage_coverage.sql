-- Migration: 0016_lineage_coverage.sql
-- Purpose: Lineage coverage snapshot table for purge-readiness controls.
--
--   lineage_coverage_snapshot stores one row per (entity_type, snapshot_at) recording
--   candidate→canonical promotion coverage and optional source→candidate coverage.
--   Used by run_lineage_report() and run_purge_check() to determine whether the
--   entity graph is sufficiently resolved to allow raw-source data purging.
--
-- Depends on: 0011_candidate_canonical_core

CREATE TABLE lineage_coverage_snapshot (
    id                             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type                    text        NOT NULL
        CHECK (entity_type IN ('participant','yacht','event','registration','club')),
    snapshot_at                    timestamptz NOT NULL DEFAULT now(),
    candidates_total               bigint      NOT NULL DEFAULT 0,
    candidates_linked_to_canonical bigint      NOT NULL DEFAULT 0,
    pct_candidate_to_canonical     numeric(6,2),
    source_rows_in_link_table      bigint,
    source_rows_with_candidate     bigint,
    pct_source_to_candidate        numeric(6,2),
    threshold_canonical_pct        numeric(6,2) NOT NULL DEFAULT 90.0,
    threshold_source_pct           numeric(6,2) NOT NULL DEFAULT 90.0,
    unresolved_critical_deps       bigint      NOT NULL DEFAULT 0,
    thresholds_passed              boolean     NOT NULL DEFAULT false,
    notes                          text,
    created_at                     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_lineage_snapshot_entity_time
    ON lineage_coverage_snapshot (entity_type, snapshot_at DESC);
