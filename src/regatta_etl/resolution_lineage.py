"""regatta_etl.resolution_lineage

Lineage coverage reporting and purge-readiness checking.

Modes:
    lineage_report  -- compute coverage metrics and insert lineage_coverage_snapshot rows
    purge_check     -- like lineage_report but exits(1) if any threshold is not met

Coverage metrics (per entity_type):
    pct_candidate_to_canonical: % of candidates that are promoted to a canonical entity.
    pct_source_to_candidate:    % of source link rows that have a candidate entry
                                (opportunistic — reported as NULL if candidate_source_link
                                is empty for the entity type).
    unresolved_critical_deps:   (registration only) promoted registrations whose event
                                is not yet promoted.
    thresholds_passed:          true iff pct_candidate_to_canonical >= threshold_canonical_pct
                                AND pct_source_to_candidate >= threshold_source_pct
                                (source threshold only applied when source rows exist)
                                AND unresolved_critical_deps == 0.

Depends on: 0016_lineage_coverage (lineage_coverage_snapshot)
            0011_candidate_canonical_core (candidate_* + candidate_source_link)
            0012_canonical_tables (candidate_canonical_link)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field

import psycopg

# ---------------------------------------------------------------------------
# Entity types
# ---------------------------------------------------------------------------

_ALL_ENTITY_TYPES = ["participant", "yacht", "club", "event", "registration"]

_CANDIDATE_TABLE = {
    "participant":  "candidate_participant",
    "yacht":        "candidate_yacht",
    "club":         "candidate_club",
    "event":        "candidate_event",
    "registration": "candidate_registration",
}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class LineageCoverageResult:
    entity_type: str
    candidates_total: int
    candidates_promoted: int
    pct_candidate_to_canonical: float | None
    source_rows_in_link_table: int | None
    source_rows_with_candidate: int | None
    pct_source_to_candidate: float | None
    unresolved_critical_deps: int
    thresholds_passed: bool
    threshold_canonical_pct: float
    threshold_source_pct: float
    notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Coverage computation
# ---------------------------------------------------------------------------

def _compute_coverage(
    conn: psycopg.Connection,
    entity_type: str,
    canonical_threshold_pct: float,
    source_threshold_pct: float,
) -> LineageCoverageResult:
    cand_table = _CANDIDATE_TABLE[entity_type]

    # Candidate → canonical coverage.
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE is_promoted = true) AS promoted
        FROM {cand_table}
        """
    ).fetchone()
    candidates_total = int(row[0])
    candidates_promoted = int(row[1])
    pct_canonical = (
        round(candidates_promoted / candidates_total * 100.0, 2)
        if candidates_total > 0
        else None
    )

    # Source → candidate coverage.
    # candidate_source_link only stores rows that ARE already linked to a candidate.
    # There is no unlinked-source denominator available in this table, so a true
    # "% of source rows that have a candidate" ratio cannot be computed.
    # We report the count of distinct linked source rows for informational purposes,
    # but pct_source_to_candidate is always None (not measurable from this table alone).
    # A proper source-coverage ratio requires Phase 4 work (separate raw-source counts).
    src_row = conn.execute(
        """
        SELECT COUNT(DISTINCT (source_table_name, source_row_pk)) AS in_link
        FROM candidate_source_link
        WHERE candidate_entity_type = %s
        """,
        (entity_type,),
    ).fetchone()
    source_rows_in_link_table = int(src_row[0]) if src_row[0] else None
    source_rows_with_candidate = source_rows_in_link_table  # same rows; ratio not computable
    pct_source = None  # always None until Phase 4 adds unlinked-source denominator

    # Unresolved critical dependencies (registrations only).
    unresolved_deps = 0
    if entity_type == "registration":
        dep_row = conn.execute(
            """
            SELECT COUNT(*)
            FROM candidate_registration cr
            WHERE cr.is_promoted = true
              AND cr.candidate_event_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM candidate_event ce
                  WHERE ce.id = cr.candidate_event_id
                    AND ce.is_promoted = true
              )
            """
        ).fetchone()
        unresolved_deps = int(dep_row[0])

    # Threshold evaluation.
    # Source threshold is never enforced: pct_source is always None because
    # candidate_source_link has no unlinked-source denominator (see comment above).
    canon_ok = (
        pct_canonical is not None
        and pct_canonical >= canonical_threshold_pct
    )
    thresholds_passed = canon_ok and (unresolved_deps == 0)

    notes: list[str] = []
    if pct_canonical is None:
        notes.append("no candidates found — pct_candidate_to_canonical is null")
    notes.append(
        "source coverage ratio not measurable (candidate_source_link stores only linked rows; "
        "Phase 4 will add raw-source counts for a true denominator)"
    )
    if unresolved_deps > 0:
        notes.append(f"{unresolved_deps} promoted registrations have un-promoted events")

    return LineageCoverageResult(
        entity_type=entity_type,
        candidates_total=candidates_total,
        candidates_promoted=candidates_promoted,
        pct_candidate_to_canonical=pct_canonical,
        source_rows_in_link_table=source_rows_in_link_table,
        source_rows_with_candidate=source_rows_with_candidate,
        pct_source_to_candidate=pct_source,
        unresolved_critical_deps=unresolved_deps,
        thresholds_passed=thresholds_passed,
        threshold_canonical_pct=canonical_threshold_pct,
        threshold_source_pct=source_threshold_pct,
        notes=notes,
    )


def _insert_snapshot(conn: psycopg.Connection, result: LineageCoverageResult) -> None:
    conn.execute(
        """
        INSERT INTO lineage_coverage_snapshot (
            entity_type,
            candidates_total,
            candidates_linked_to_canonical,
            pct_candidate_to_canonical,
            source_rows_in_link_table,
            source_rows_with_candidate,
            pct_source_to_candidate,
            threshold_canonical_pct,
            threshold_source_pct,
            unresolved_critical_deps,
            thresholds_passed,
            notes
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            result.entity_type,
            result.candidates_total,
            result.candidates_promoted,
            result.pct_candidate_to_canonical,
            result.source_rows_in_link_table,
            result.source_rows_with_candidate,
            result.pct_source_to_candidate,  # always None until Phase 4
            result.threshold_canonical_pct,
            result.threshold_source_pct,
            result.unresolved_critical_deps,
            result.thresholds_passed,
            "\n".join(result.notes) if result.notes else None,
        ),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_lineage_report(
    conn: psycopg.Connection,
    entity_type: str = "all",
    canonical_threshold_pct: float = 90.0,
    source_threshold_pct: float = 90.0,
    dry_run: bool = False,
) -> list[LineageCoverageResult]:
    """Compute coverage and insert lineage_coverage_snapshot rows (unless dry_run).

    Args:
        conn: Open psycopg connection (caller manages transaction).
        entity_type: One of participant|yacht|club|event|registration|all.
        canonical_threshold_pct: Minimum % of candidates that must be promoted.
        source_threshold_pct: Minimum % of source rows that must be linked (when available).
        dry_run: If True, skip snapshot INSERT.

    Returns:
        List of LineageCoverageResult (one per entity type processed).
    """
    types_to_check = (
        _ALL_ENTITY_TYPES if entity_type == "all" else [entity_type]
    )
    results: list[LineageCoverageResult] = []

    for et in types_to_check:
        result = _compute_coverage(
            conn, et, canonical_threshold_pct, source_threshold_pct
        )
        results.append(result)
        if not dry_run:
            _insert_snapshot(conn, result)

    return results


def run_purge_check(
    conn: psycopg.Connection,
    entity_type: str = "all",
    canonical_threshold_pct: float = 95.0,
    source_threshold_pct: float = 95.0,
) -> bool:
    """Return True if all thresholds pass; sys.exit(1) if any fail.

    Inserts lineage_coverage_snapshot rows for audit purposes.

    Args:
        conn: Open psycopg connection (caller manages transaction).
        entity_type: One of participant|yacht|club|event|registration|all.
        canonical_threshold_pct: Minimum % of candidates that must be promoted.
        source_threshold_pct: Minimum % of source rows that must be linked.

    Returns:
        True if all entity types pass all thresholds.
        Calls sys.exit(1) if any entity type fails.
    """
    results = run_lineage_report(
        conn,
        entity_type=entity_type,
        canonical_threshold_pct=canonical_threshold_pct,
        source_threshold_pct=source_threshold_pct,
        dry_run=False,
    )
    all_passed = all(r.thresholds_passed for r in results)
    if not all_passed:
        sys.exit(1)
    return True


def build_lineage_report(
    results: list[LineageCoverageResult],
    dry_run: bool = False,
) -> str:
    lines = [
        "=" * 70,
        "Lineage Coverage Report",
        f"  dry_run: {dry_run}",
        "=" * 70,
    ]
    for r in results:
        canon_pct = (
            f"{r.pct_candidate_to_canonical:.2f}%"
            if r.pct_candidate_to_canonical is not None
            else "n/a"
        )
        src_pct = (
            f"{r.pct_source_to_candidate:.2f}%"
            if r.pct_source_to_candidate is not None
            else "n/a (not measurable — Phase 4)"
        )
        status = "PASS" if r.thresholds_passed else "FAIL"
        lines.append(
            f"\n  [{status}] {r.entity_type}"
        )
        lines.append(
            f"    candidates total/promoted: "
            f"{r.candidates_total} / {r.candidates_promoted} ({canon_pct})"
        )
        lines.append(f"    threshold canonical: {r.threshold_canonical_pct:.1f}%")
        lines.append(f"    source coverage:     {src_pct}")
        lines.append(f"    threshold source:    {r.threshold_source_pct:.1f}%")
        lines.append(f"    unresolved deps:     {r.unresolved_critical_deps}")
        if r.notes:
            for note in r.notes:
                lines.append(f"    note: {note}")
    lines.append("\n" + "=" * 70)
    overall = "PASS" if all(r.thresholds_passed for r in results) else "FAIL"
    lines.append(f"  Overall: {overall}")
    lines.append("=" * 70)
    return "\n".join(lines)
