"""regatta_etl.resolution_score

Candidate scoring pipeline (--mode resolution_score).

For each candidate entity of the requested type, extracts features from the
candidate row, applies the active YAML rule set (weights, penalties, hard blocks),
and writes quality_score + resolution_state + confidence_reasons back to the
candidate table.  Also updates last_score_run_id for traceability.

Feature extraction maps YAML feature names → boolean presence checks on candidate
table columns.  See _FEATURE_EXTRACTORS for the per-entity-type mapping.

Processing order (run 'all'): club → event → yacht → participant → registration
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

from regatta_etl.resolution_rules import (
    RuleSet,
    close_score_run,
    compute_score,
    load_rule_set,
    open_score_run,
    register_rule_set,
)

# ---------------------------------------------------------------------------
# Default rule file location
# ---------------------------------------------------------------------------

_DEFAULT_RULES_DIR = Path(__file__).parent.parent.parent / "config" / "resolution_rules"


def _default_rule_path(entity_type: str) -> Path:
    return _DEFAULT_RULES_DIR / f"{entity_type}.yml"


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class ScoreCounters:
    candidates_scored: int = 0
    candidates_auto_promote: int = 0
    candidates_review: int = 0
    candidates_hold: int = 0
    candidates_rejected: int = 0
    nbas_written: int = 0
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates_scored": self.candidates_scored,
            "candidates_auto_promote": self.candidates_auto_promote,
            "candidates_review": self.candidates_review,
            "candidates_hold": self.candidates_hold,
            "candidates_rejected": self.candidates_rejected,
            "nbas_written": self.nbas_written,
            "db_errors": self.db_errors,
            "warnings": self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Feature extractors
# Map YAML feature_weight keys → boolean columns on each candidate table.
# ---------------------------------------------------------------------------

def _features_participant(row: dict[str, Any]) -> dict[str, bool]:
    return {
        "email_exact":           bool(row["best_email"]),
        "phone_exact":           bool(row["best_phone"]),
        "dob_exact":             bool(row["date_of_birth"]),
        "normalized_name_exact": bool(row["normalized_name"]),
    }


def _features_yacht(row: dict[str, Any]) -> dict[str, bool]:
    return {
        "sail_number_exact":  bool(row["normalized_sail_number"]),
        "name_normalized":    bool(row["normalized_name"]),
        "yacht_type_present": bool(row["yacht_type"]),
        "length_feet_present": bool(row["length_feet"]),
    }


def _features_club(row: dict[str, Any]) -> dict[str, bool]:
    return {
        "name_normalized":   bool(row["normalized_name"]),
        "website_present":   bool(row["website"]),
        "state_usa_present": bool(row["state_usa"]),
        "phone_present":     bool(row["phone"]),
    }


def _features_event(row: dict[str, Any]) -> dict[str, bool]:
    return {
        "external_id_present": bool(row["event_external_id"]),
        "season_year_present": row["season_year"] is not None,
        "name_normalized":     bool(row["normalized_event_name"]),
        "dates_present":       bool(row["start_date"] or row["end_date"]),
    }


def _features_registration(row: dict[str, Any]) -> dict[str, bool]:
    return {
        "external_id_present": bool(row["registration_external_id"]),
        "event_resolved":      bool(row["candidate_event_id"]),
        "yacht_resolved":      bool(row["candidate_yacht_id"]),
        "participant_resolved": bool(row["candidate_primary_participant_id"]),
    }


# ---------------------------------------------------------------------------
# Table/column metadata
# ---------------------------------------------------------------------------

_FEATURE_EXTRACTORS: dict[str, Any] = {
    "participant":  _features_participant,
    "yacht":        _features_yacht,
    "club":         _features_club,
    "event":        _features_event,
    "registration": _features_registration,
}

_CANDIDATE_TABLE = {
    "participant":  "candidate_participant",
    "yacht":        "candidate_yacht",
    "club":         "candidate_club",
    "event":        "candidate_event",
    "registration": "candidate_registration",
}

# Columns fetched per entity type for feature extraction.
# is_promoted is included so the scorer can preserve resolution_state for
# already-promoted candidates and skip NBA generation.
_SELECT_COLS: dict[str, str] = {
    "participant":  "id, normalized_name, best_email, best_phone, date_of_birth, quality_score, is_promoted",
    "yacht":        "id, normalized_name, normalized_sail_number, yacht_type, length_feet, quality_score, is_promoted",
    "club":         "id, normalized_name, website, state_usa, phone, quality_score, is_promoted",
    "event":        "id, normalized_event_name, event_external_id, season_year, start_date, end_date, quality_score, is_promoted",
    "registration": "id, registration_external_id, candidate_event_id, candidate_yacht_id, candidate_primary_participant_id, quality_score, is_promoted",
}


# ---------------------------------------------------------------------------
# NBA (next_best_action) writer
# ---------------------------------------------------------------------------

def _write_nbas(
    conn: psycopg.Connection,
    entity_type: str,
    pk: str,
    features: dict[str, bool],
    rule_set: RuleSet,
    state: str,
    reasons: list[Any],
    is_promoted: bool,
) -> int:
    """Delete stale scorer-generated open NBAs and insert fresh ones.

    Only writes NBAs for candidates that are not yet promoted, not in
    auto_promote state, and not blocked by a hard block (where enrichment
    would be misleading — the candidate is blocked regardless of completeness).

    Returns count of NBAs inserted.
    """
    target_type = f"candidate_{entity_type}"

    # Narrow delete: only scorer-generated rows; preserves unrelated operational NBAs.
    conn.execute(
        """
        DELETE FROM next_best_action
        WHERE target_entity_type = %s
          AND target_entity_id = %s
          AND status = 'open'
          AND action_type = 'enrich_candidate'
          AND recommended_channel = 'manual_enrichment'
        """,
        (target_type, pk),
    )

    if state == "auto_promote":
        return 0  # candidate is ready; no enrichment NBAs needed

    if is_promoted:
        return 0  # already linked to canonical; computed state doesn't matter

    # Suppress NBA creation when a hard block caused the non-auto_promote state
    if any(str(r).startswith("hard_block:") for r in reasons):
        return 0

    inserted = 0
    for feature_name, present in features.items():
        if present:
            continue
        weight = rule_set.feature_weights.get(feature_name, 0.0)
        if weight <= 0:
            continue
        conn.execute(
            """
            INSERT INTO next_best_action
                (action_type, target_entity_type, target_entity_id,
                 priority_score, reason_code, reason_detail,
                 recommended_channel, generated_at, rule_version, status)
            VALUES ('enrich_candidate', %s, %s, %s, %s, %s,
                    'manual_enrichment', now(), %s, 'open')
            """,
            (
                target_type, pk, weight,
                f"missing_{feature_name}",
                f"{feature_name} missing; worth +{weight:.2f} toward auto_promote",
                rule_set.version,
            ),
        )
        inserted += 1
    return inserted


# ---------------------------------------------------------------------------
# Per-entity-type scoring
# ---------------------------------------------------------------------------

def _score_entity_type(
    conn: psycopg.Connection,
    entity_type: str,
    rule_set: RuleSet,
    score_run_id: str,
    ctrs: ScoreCounters,
) -> None:
    """Score all candidates of a single entity type and write scores back."""
    table = _CANDIDATE_TABLE[entity_type]
    cols = _SELECT_COLS[entity_type]
    extractor = _FEATURE_EXTRACTORS[entity_type]

    rows = conn.execute(
        f"SELECT {cols} FROM {table} ORDER BY created_at"
    ).fetchall()
    col_names = [c.strip() for c in cols.split(",")]

    for raw_row in rows:
        row = dict(zip(col_names, raw_row))
        pk = str(row["id"])
        is_promoted: bool = bool(row["is_promoted"])
        try:
            features = extractor(row)
            score, state, reasons = compute_score(rule_set, features)
            # Preserve resolution_state='auto_promote' for already-promoted candidates
            # so a re-score cannot downgrade their state back to review/hold.
            conn.execute(
                f"""
                UPDATE {table}
                SET quality_score      = %s,
                    resolution_state   = CASE WHEN is_promoted THEN 'auto_promote' ELSE %s END,
                    confidence_reasons = %s::jsonb,
                    last_score_run_id  = %s
                WHERE id = %s
                """,
                (score, state, json.dumps(reasons), score_run_id, pk),
            )
            ctrs.candidates_scored += 1
            effective_state = "auto_promote" if is_promoted else state
            if effective_state == "auto_promote":
                ctrs.candidates_auto_promote += 1
            elif effective_state == "review":
                ctrs.candidates_review += 1
            elif effective_state == "hold":
                ctrs.candidates_hold += 1
            else:
                ctrs.candidates_rejected += 1
            nba_count = _write_nbas(
                conn, entity_type, pk, features, rule_set, state, reasons, is_promoted
            )
            ctrs.nbas_written += nba_count
        except Exception as exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(f"{entity_type} pk={pk}: {exc}")


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def run_score(
    conn: psycopg.Connection,
    entity_type: str = "all",
    rule_file: Path | None = None,
    dry_run: bool = False,
) -> ScoreCounters:
    """Score candidate entities using the YAML rule set.

    Args:
        conn: Open psycopg connection (caller manages transaction).
        entity_type: One of 'participant','yacht','event','registration','club','all'.
        rule_file: Path to a YAML rule file.  When None, the default path
                   config/resolution_rules/{entity_type}.yml is used.
                   Ignored for entity_type='all' (each type uses its own file).
        dry_run: If True, caller should ROLLBACK after this returns.

    Returns:
        ScoreCounters with run statistics.
    """
    ctrs = ScoreCounters()
    entity_types = (
        ["club", "event", "yacht", "participant", "registration"]
        if entity_type == "all"
        else [entity_type]
    )

    for et in entity_types:
        path = rule_file if (rule_file and entity_type != "all") else _default_rule_path(et)
        rule_set = load_rule_set(path)
        rule_set_id = register_rule_set(conn, rule_set)
        score_run_id = open_score_run(conn, et, rule_set.source_system, rule_set_id)
        step_failed = False
        try:
            _score_entity_type(conn, et, rule_set, score_run_id, ctrs)
        except Exception as exc:
            step_failed = True
            ctrs.db_errors += 1
            ctrs.warnings.append(f"score run for {et} failed: {exc}")

        # Always attempt to close the run record, but avoid masking the primary
        # failure path with a second exception.
        close_status = "failed" if step_failed else "ok"
        try:
            close_score_run(conn, score_run_id, close_status, ctrs.to_dict())
        except Exception as close_exc:
            ctrs.db_errors += 1
            ctrs.warnings.append(
                f"close_score_run failed for {et} ({close_status}): {close_exc}"
            )
            # If the connection is already gone, continuing further entity loops
            # only creates repeated noise.
            if conn.closed:
                break

    return ctrs


def build_score_report(ctrs: ScoreCounters, dry_run: bool = False) -> str:
    lines = [
        "=" * 60,
        "Candidate Scoring Pipeline Report",
        f"  dry_run: {dry_run}",
        "=" * 60,
        f"  candidates scored:   {ctrs.candidates_scored}",
        f"    → auto_promote:    {ctrs.candidates_auto_promote}",
        f"    → review:          {ctrs.candidates_review}",
        f"    → hold:            {ctrs.candidates_hold}",
        f"    → rejected:        {ctrs.candidates_rejected}",
        f"  NBAs written:        {ctrs.nbas_written}",
        f"DB errors:             {ctrs.db_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")
    lines.append("=" * 60)
    return "\n".join(lines)
