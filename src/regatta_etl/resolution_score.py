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
import yaml

from regatta_etl.normalize import is_likely_org_name
from regatta_etl.resolution_rules import (
    RuleSet,
    close_score_run,
    compute_score,
    load_rule_set,
    open_score_run,
    register_rule_set,
    resolution_state_from_score,
)

# ---------------------------------------------------------------------------
# Default rule file location
# ---------------------------------------------------------------------------

_DEFAULT_RULES_DIR = Path(__file__).parent.parent.parent / "config" / "resolution_rules"
_DEFAULT_SOURCE_TRUST_PATH = _DEFAULT_RULES_DIR / "source_trust.yml"


def _default_rule_path(entity_type: str) -> Path:
    return _DEFAULT_RULES_DIR / f"{entity_type}.yml"


# ---------------------------------------------------------------------------
# Source trust policy
# ---------------------------------------------------------------------------

_SOURCE_TRUST_DEFAULT_KEYS = frozenset({
    "unknown_source_weight",
    "high_trust_threshold",
    "min_distinct_sources_for_auto_promote",
    "require_high_trust_for_auto_promote",
    "single_source_penalty",
    "no_high_trust_penalty",
    "multi_source_bonus",
    "max_total_adjustment_abs",
})
_SOURCE_TRUST_TIER_KEYS = frozenset({"high", "medium", "low"})
_UNKNOWN_SOURCE_KEY = "__unknown__"


class SourceTrustValidationError(ValueError):
    """Raised when source_trust.yml fails validation."""


@dataclass(frozen=True)
class SourceTrustSource:
    weight: float
    tier: str
    notes: str | None = None


@dataclass(frozen=True)
class SourceTrustPolicy:
    version: str
    defaults: dict[str, Any]
    source_weights: dict[str, SourceTrustSource]
    entity_overrides: dict[str, dict[str, Any]]

    def settings_for(self, entity_type: str) -> dict[str, Any]:
        settings = dict(self.defaults)
        settings.update(self.entity_overrides.get(entity_type, {}))
        return settings

    def source_rule_for(self, source_system: str) -> SourceTrustSource | None:
        return self.source_weights.get(source_system)


def _coerce_bool(value: Any, key: str) -> bool:
    if isinstance(value, bool):
        return value
    raise SourceTrustValidationError(f"'{key}' must be a boolean.")


def _coerce_float(
    value: Any,
    key: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise SourceTrustValidationError(f"'{key}' must be numeric.")
    if minimum is not None and result < minimum:
        raise SourceTrustValidationError(f"'{key}' must be >= {minimum}.")
    if maximum is not None and result > maximum:
        raise SourceTrustValidationError(f"'{key}' must be <= {maximum}.")
    return result


def _coerce_int(value: Any, key: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise SourceTrustValidationError(f"'{key}' must be an integer.")
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise SourceTrustValidationError(f"'{key}' must be an integer.")
    if result < minimum:
        raise SourceTrustValidationError(f"'{key}' must be >= {minimum}.")
    return result


def load_source_trust_policy(path: Path) -> SourceTrustPolicy:
    raw = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise SourceTrustValidationError("source_trust.yml root must be a mapping.")

    defaults = data.get("defaults")
    if not isinstance(defaults, dict):
        raise SourceTrustValidationError("'defaults' must be a mapping.")
    missing_defaults = _SOURCE_TRUST_DEFAULT_KEYS - set(defaults.keys())
    if missing_defaults:
        raise SourceTrustValidationError(
            f"Missing source trust default keys: {sorted(missing_defaults)}"
        )

    normalized_defaults = {
        "unknown_source_weight": _coerce_float(
            defaults["unknown_source_weight"], "defaults.unknown_source_weight", minimum=0.0, maximum=1.0
        ),
        "high_trust_threshold": _coerce_float(
            defaults["high_trust_threshold"], "defaults.high_trust_threshold", minimum=0.0, maximum=1.0
        ),
        "min_distinct_sources_for_auto_promote": _coerce_int(
            defaults["min_distinct_sources_for_auto_promote"],
            "defaults.min_distinct_sources_for_auto_promote",
            minimum=1,
        ),
        "require_high_trust_for_auto_promote": _coerce_bool(
            defaults["require_high_trust_for_auto_promote"],
            "defaults.require_high_trust_for_auto_promote",
        ),
        "single_source_penalty": _coerce_float(
            defaults["single_source_penalty"], "defaults.single_source_penalty", minimum=0.0, maximum=1.0
        ),
        "no_high_trust_penalty": _coerce_float(
            defaults["no_high_trust_penalty"], "defaults.no_high_trust_penalty", minimum=0.0, maximum=1.0
        ),
        "multi_source_bonus": _coerce_float(
            defaults["multi_source_bonus"], "defaults.multi_source_bonus", minimum=0.0, maximum=1.0
        ),
        "max_total_adjustment_abs": _coerce_float(
            defaults["max_total_adjustment_abs"], "defaults.max_total_adjustment_abs", minimum=0.0, maximum=1.0
        ),
    }

    source_weights = data.get("source_weights")
    if not isinstance(source_weights, dict) or not source_weights:
        raise SourceTrustValidationError("'source_weights' must be a non-empty mapping.")

    normalized_source_weights: dict[str, SourceTrustSource] = {}
    for source_name, source_cfg in source_weights.items():
        if not isinstance(source_cfg, dict):
            raise SourceTrustValidationError(f"source_weights.{source_name} must be a mapping.")
        tier = str(source_cfg.get("tier", "")).strip().lower()
        if tier not in _SOURCE_TRUST_TIER_KEYS:
            raise SourceTrustValidationError(
                f"source_weights.{source_name}.tier must be one of {sorted(_SOURCE_TRUST_TIER_KEYS)}."
            )
        normalized_source_weights[str(source_name)] = SourceTrustSource(
            weight=_coerce_float(
                source_cfg.get("weight"),
                f"source_weights.{source_name}.weight",
                minimum=0.0,
                maximum=1.0,
            ),
            tier=tier,
            notes=str(source_cfg.get("notes")) if source_cfg.get("notes") is not None else None,
        )

    entity_overrides_raw = data.get("entity_overrides") or {}
    if not isinstance(entity_overrides_raw, dict):
        raise SourceTrustValidationError("'entity_overrides' must be a mapping when present.")

    normalized_entity_overrides: dict[str, dict[str, Any]] = {}
    for entity_type, override_cfg in entity_overrides_raw.items():
        if entity_type not in _CANDIDATE_TABLE:
            raise SourceTrustValidationError(f"Unknown entity override '{entity_type}'.")
        if not isinstance(override_cfg, dict):
            raise SourceTrustValidationError(f"entity_overrides.{entity_type} must be a mapping.")
        unknown_override_keys = set(override_cfg.keys()) - _SOURCE_TRUST_DEFAULT_KEYS
        if unknown_override_keys:
            raise SourceTrustValidationError(
                f"Unknown entity override keys for {entity_type}: {sorted(unknown_override_keys)}"
            )
        normalized_override: dict[str, Any] = {}
        for key, value in override_cfg.items():
            if key == "require_high_trust_for_auto_promote":
                normalized_override[key] = _coerce_bool(value, f"entity_overrides.{entity_type}.{key}")
            elif key == "min_distinct_sources_for_auto_promote":
                normalized_override[key] = _coerce_int(
                    value, f"entity_overrides.{entity_type}.{key}", minimum=1
                )
            else:
                normalized_override[key] = _coerce_float(
                    value, f"entity_overrides.{entity_type}.{key}", minimum=0.0, maximum=1.0
                )
        normalized_entity_overrides[entity_type] = normalized_override

    return SourceTrustPolicy(
        version=str(data.get("version") or ""),
        defaults=normalized_defaults,
        source_weights=normalized_source_weights,
        entity_overrides=normalized_entity_overrides,
    )


@dataclass(frozen=True)
class CandidateSourceProfile:
    source_systems: tuple[str, ...]

    @property
    def distinct_source_count(self) -> int:
        return len(self.source_systems)


@dataclass(frozen=True)
class TrustAdjustmentResult:
    adjusted_score: float
    resolution_state: str
    reasons: list[str]
    was_adjusted: bool
    was_capped: bool


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
    # State-machine-safe transition counters (§6.1 / §6.4)
    state_transitions_staged: int = 0   # reject→review→auto_promote two-step
    state_transitions_capped: int = 0   # rows capped to review (safety path)
    # Per-entity new-entry-to-auto_promote counters (§6.4)
    new_auto_promote_club: int = 0
    new_auto_promote_participant: int = 0
    new_auto_promote_yacht: int = 0
    # Child-evidence usage counters (participant only)
    participant_child_email_used: int = 0
    participant_child_phone_used: int = 0
    participant_child_address_used: int = 0
    participant_address_only_hold: int = 0
    trust_adjusted_candidates: int = 0
    trust_capped_candidates: int = 0
    participant_score_mean: float | None = None
    participant_score_stddev: float | None = None
    participant_score_unique_values: int = 0
    participant_hold_score_unique_values: int = 0
    participant_auto_promote_score_unique_values: int = 0
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
            "state_transitions_staged": self.state_transitions_staged,
            "state_transitions_capped": self.state_transitions_capped,
            "new_auto_promote_club": self.new_auto_promote_club,
            "new_auto_promote_participant": self.new_auto_promote_participant,
            "new_auto_promote_yacht": self.new_auto_promote_yacht,
            "participant_child_email_used": self.participant_child_email_used,
            "participant_child_phone_used": self.participant_child_phone_used,
            "participant_child_address_used": self.participant_child_address_used,
            "participant_address_only_hold": self.participant_address_only_hold,
            "trust_adjusted_candidates": self.trust_adjusted_candidates,
            "trust_capped_candidates": self.trust_capped_candidates,
            "participant_score_mean": self.participant_score_mean,
            "participant_score_stddev": self.participant_score_stddev,
            "participant_score_unique_values": self.participant_score_unique_values,
            "participant_hold_score_unique_values": self.participant_hold_score_unique_values,
            "participant_auto_promote_score_unique_values": self.participant_auto_promote_score_unique_values,
            "db_errors": self.db_errors,
            "warnings": self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Feature extractors
# Map YAML feature_weight keys → boolean columns on each candidate table.
# ---------------------------------------------------------------------------

def _fetch_participant_source_counts(
    conn: psycopg.Connection,
) -> dict[str, int]:
    """Return a map of candidate_participant_id → source link count.

    Used to populate the source_count_score continuous feature (FOR-225).
    """
    rows = conn.execute(
        """
        SELECT candidate_entity_id::text, COUNT(*) AS cnt
        FROM candidate_source_link
        WHERE candidate_entity_type = 'participant'
        GROUP BY candidate_entity_id
        """
    ).fetchall()
    return {cid: int(cnt) for cid, cnt in rows}


def _fetch_participant_child_evidence(
    conn: psycopg.Connection,
) -> dict[str, dict[str, bool]]:
    """Return a map of candidate_participant_id → {has_email, has_phone, has_address}.

    Uses two bulk queries (contacts + addresses) to avoid N+1 per-candidate lookups.
    Only includes candidates that have at least one relevant child row.
    """
    result: dict[str, dict[str, bool]] = {}

    # Bulk fetch: any non-null normalized contact value by type
    contact_rows = conn.execute(
        """
        SELECT candidate_participant_id::text, contact_type
        FROM candidate_participant_contact
        WHERE normalized_value IS NOT NULL
          AND contact_type IN ('email', 'phone')
        """
    ).fetchall()
    for cid, ctype in contact_rows:
        entry = result.setdefault(cid, {"has_email": False, "has_phone": False, "has_address": False})
        if ctype == "email":
            entry["has_email"] = True
        elif ctype == "phone":
            entry["has_phone"] = True

    # Bulk fetch: address existence
    address_rows = conn.execute(
        """
        SELECT DISTINCT candidate_participant_id::text
        FROM candidate_participant_address
        """
    ).fetchall()
    for (cid,) in address_rows:
        entry = result.setdefault(cid, {"has_email": False, "has_phone": False, "has_address": False})
        entry["has_address"] = True

    return result


def _fetch_candidate_source_profiles(
    conn: psycopg.Connection,
    entity_type: str,
) -> dict[str, CandidateSourceProfile]:
    """Return a map of candidate id -> distinct source systems."""
    rows = conn.execute(
        """
        SELECT candidate_entity_id::text,
               COALESCE(NULLIF(source_system, ''), %s) AS source_system
        FROM candidate_source_link
        WHERE candidate_entity_type = %s
        """,
        (_UNKNOWN_SOURCE_KEY, entity_type),
    ).fetchall()
    grouped: dict[str, set[str]] = {}
    for candidate_id, source_system in rows:
        grouped.setdefault(str(candidate_id), set()).add(str(source_system))
    return {
        candidate_id: CandidateSourceProfile(tuple(sorted(source_systems)))
        for candidate_id, source_systems in grouped.items()
    }


def _update_participant_score_distribution_stats(
    conn: psycopg.Connection,
    ctrs: ScoreCounters,
) -> None:
    """Populate participant score distribution stats used by FOR-225 reporting."""
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT quality_score) AS unique_scores,
            COUNT(DISTINCT quality_score) FILTER (WHERE resolution_state = 'hold') AS hold_unique_scores,
            COUNT(DISTINCT quality_score) FILTER (WHERE resolution_state = 'auto_promote') AS auto_promote_unique_scores,
            AVG(quality_score)::float8 AS mean_score,
            COALESCE(STDDEV_POP(quality_score)::float8, 0.0) AS stddev_score
        FROM candidate_participant
        """
    ).fetchone()
    if row is None or int(row[0]) == 0:
        return

    ctrs.participant_score_unique_values = int(row[1])
    ctrs.participant_hold_score_unique_values = int(row[2])
    ctrs.participant_auto_promote_score_unique_values = int(row[3])
    ctrs.participant_score_mean = float(row[4]) if row[4] is not None else None
    ctrs.participant_score_stddev = float(row[5]) if row[5] is not None else None

    if ctrs.participant_score_unique_values < 10:
        ctrs.warnings.append(
            "participant score distribution has fewer than 10 distinct values"
        )
    if ctrs.participant_hold_score_unique_values < 2:
        ctrs.warnings.append(
            "participant hold-band score distribution is collapsed"
        )


def _apply_source_trust(
    rule_set: RuleSet,
    entity_type: str,
    base_score: float,
    profile: CandidateSourceProfile | None,
    policy: SourceTrustPolicy,
) -> TrustAdjustmentResult:
    """Adjust score/state using source trust policy for one candidate."""
    settings = policy.settings_for(entity_type)
    source_systems = list(profile.source_systems) if profile is not None else []
    if not source_systems:
        return TrustAdjustmentResult(
            adjusted_score=base_score,
            resolution_state=resolution_state_from_score(rule_set, base_score),
            reasons=[],
            was_adjusted=False,
            was_capped=False,
        )
    weights: list[float] = []
    for source_system in source_systems:
        source_rule = policy.source_rule_for(source_system)
        if source_rule is None:
            weights.append(float(settings["unknown_source_weight"]))
        else:
            weights.append(source_rule.weight)
    has_high_trust = any(weight >= float(settings["high_trust_threshold"]) for weight in weights)

    adjustment = 0.0
    reasons: list[str] = [f"trust:distinct_sources:{len(source_systems)}"]
    if len(source_systems) == 1:
        adjustment -= float(settings["single_source_penalty"])
        reasons.append(f"trust:single_source_penalty:{float(settings['single_source_penalty']):.4f}")
    elif len(source_systems) >= int(settings["min_distinct_sources_for_auto_promote"]):
        adjustment += float(settings["multi_source_bonus"])
        reasons.append(f"trust:multi_source_bonus:{float(settings['multi_source_bonus']):.4f}")

    if has_high_trust:
        reasons.append("trust:high_trust_source_present")
    else:
        adjustment -= float(settings["no_high_trust_penalty"])
        reasons.append(f"trust:no_high_trust_penalty:{float(settings['no_high_trust_penalty']):.4f}")

    max_abs = float(settings["max_total_adjustment_abs"])
    clamped_adjustment = max(-max_abs, min(max_abs, adjustment))
    if clamped_adjustment != adjustment:
        reasons.append(f"trust:adjustment_clamped:{clamped_adjustment:.4f}")
    adjusted_score = round(min(1.0, max(0.0, base_score + clamped_adjustment)), 4)
    adjusted_state = resolution_state_from_score(rule_set, adjusted_score)

    cap_reasons: list[str] = []
    if adjusted_state == "auto_promote":
        if len(source_systems) < int(settings["min_distinct_sources_for_auto_promote"]):
            adjusted_state = "review"
            cap_reasons.append("trust:auto_promote_cap:insufficient_distinct_sources")
        if bool(settings["require_high_trust_for_auto_promote"]) and not has_high_trust:
            adjusted_state = "review"
            cap_reasons.append("trust:auto_promote_cap:no_high_trust_source")

    return TrustAdjustmentResult(
        adjusted_score=adjusted_score,
        resolution_state=adjusted_state,
        reasons=reasons + cap_reasons,
        was_adjusted=clamped_adjustment != 0.0,
        was_capped=bool(cap_reasons),
    )


def _features_participant(row: dict[str, Any]) -> dict[str, bool | float]:
    # email/phone: top-level field OR child contact evidence (precomputed in row)
    has_email = bool(row["best_email"]) or bool(row.get("_child_email"))
    has_phone = bool(row["best_phone"]) or bool(row.get("_child_phone"))
    # address_present only activates when no email or phone evidence exists.
    # This keeps address as a conservative lift for contact-poor candidates only —
    # it must not amplify the score of candidates already bearing email/phone signals.
    has_address = bool(row.get("_child_address")) and not has_email and not has_phone
    # source_count_score: continuous [0.0..1.0], normalised at 10 source links.
    # Provides intra-band differentiation so address-only hold candidates are not
    # all scored identically at 0.30 (FOR-225).
    source_count_score: float = min(row.get("_source_count", 0) / 10.0, 1.0)
    return {
        "email_exact":           has_email,
        "phone_exact":           has_phone,
        "dob_exact":             bool(row["date_of_birth"]),
        "normalized_name_exact": bool(row["normalized_name"]),
        "address_present":       has_address,
        "source_count_score":    source_count_score,
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
# resolution_state is included so staged-transition logic can detect reject→auto_promote.
_SELECT_COLS: dict[str, str] = {
    "participant":  "id, normalized_name, best_email, best_phone, date_of_birth, quality_score, is_promoted, resolution_state",
    "yacht":        "id, normalized_name, normalized_sail_number, yacht_type, length_feet, quality_score, is_promoted, resolution_state",
    "club":         "id, normalized_name, website, state_usa, phone, quality_score, is_promoted, resolution_state",
    "event":        "id, normalized_event_name, event_external_id, season_year, start_date, end_date, quality_score, is_promoted, resolution_state",
    "registration": "id, registration_external_id, candidate_event_id, candidate_yacht_id, candidate_primary_participant_id, quality_score, is_promoted, resolution_state",
}


# Features excluded from NBA generation.
# These are child-table-derived signals that operators cannot directly enrich
# via manual action, so generating "missing_X" enrichment NBAs for them is
# misleading and creates queue noise.
_NON_NBA_FEATURES: frozenset[str] = frozenset({"address_present", "source_count_score"})


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
        if feature_name in _NON_NBA_FEATURES:
            continue  # child-evidence features are not operator-enrichable
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
    source_trust_policy: SourceTrustPolicy,
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

    # Precompute child evidence map once for participant scoring (avoids N+1).
    child_evidence: dict[str, dict[str, bool]] = {}
    source_counts: dict[str, int] = {}
    source_profiles = _fetch_candidate_source_profiles(conn, entity_type)
    if entity_type == "participant":
        child_evidence = _fetch_participant_child_evidence(conn)
        source_counts = _fetch_participant_source_counts(conn)

    for idx, raw_row in enumerate(rows):
        row = dict(zip(col_names, raw_row))
        pk = str(row["id"])
        is_promoted: bool = bool(row["is_promoted"])
        current_state: str = str(row["resolution_state"])

        # Augment participant rows with child evidence flags and source count.
        if entity_type == "participant":
            ev = child_evidence.get(pk, {})
            row["_child_email"]   = ev.get("has_email", False)
            row["_child_phone"]   = ev.get("has_phone", False)
            row["_child_address"] = ev.get("has_address", False)
            # source_count_score: continuous [0.0..1.0], max at 10+ source links
            row["_source_count"]  = source_counts.get(pk, 0)

        sp = f"score_{entity_type}_{idx}"
        conn.execute(f"SAVEPOINT {sp}")
        try:
            features = extractor(row)
            score, state, reasons = compute_score(rule_set, features)
            trust_result = _apply_source_trust(
            rule_set,
            entity_type,
            score,
            source_profiles.get(pk),
            source_trust_policy,
        )
            score = trust_result.adjusted_score
            state = trust_result.resolution_state
            reasons.extend(trust_result.reasons)
            if trust_result.was_adjusted:
                ctrs.trust_adjusted_candidates += 1
            if trust_result.was_capped and not is_promoted:
                ctrs.trust_capped_candidates += 1
                ctrs.state_transitions_capped += 1

            # FOR-224: a participant with zero name signal has no identity anchor.
            # Cap at 'hold' regardless of score so nameless email-only records
            # never auto-promote. Always record the hard block so downstream
            # profiling and migrations can identify the condition reliably.
            if entity_type == "participant" and not row["normalized_name"]:
                if "hard_block:missing_name" not in reasons:
                    reasons.append("hard_block:missing_name")
                if state in ("auto_promote", "review"):
                    state = "hold"

            # FOR-222: org-like participant names are not person identities.
            # Reject them from the participant resolution path so they cannot be
            # promoted into canonical_participant.
            if entity_type == "participant" and is_likely_org_name(
                row.get("display_name") or row.get("normalized_name")
            ):
                state = "reject"
                if "hard_block:organization_entity" not in reasons:
                    reasons.append("hard_block:organization_entity")

            # Append child-evidence origin annotations to confidence_reasons.
            if entity_type == "participant":
                if row.get("_child_email") and not bool(row["best_email"]):
                    reasons.append("evidence:child_email_present")
                    ctrs.participant_child_email_used += 1
                if row.get("_child_phone") and not bool(row["best_phone"]):
                    reasons.append("evidence:child_phone_present")
                    ctrs.participant_child_phone_used += 1
                # address_present only contributed to the score when both email and phone
                # are absent (see _features_participant conditional logic).
                if features.get("address_present"):
                    reasons.append("evidence:child_address_present")
                    ctrs.participant_child_address_used += 1
                # Track address-only candidates that land in hold
                if state == "hold" and features.get("address_present"):
                    ctrs.participant_address_only_hold += 1

            # Determine the state we intend to write.
            # Promoted candidates are locked to 'auto_promote' regardless of score.
            write_state = "auto_promote" if is_promoted else state

            # State-machine-safe write:
            # The DB trigger enforce_candidate_state_transition() forbids a direct
            # reject → auto_promote UPDATE (Rule 2 in migration 0014).  When the
            # computed write_state is 'auto_promote' but the current DB state is
            # 'reject', we stage through 'review' first so both UPDATEs are
            # trigger-safe.  Both steps are inside the same SAVEPOINT so a failure
            # rolls back only this candidate, not the whole transaction.
            if write_state == "auto_promote" and current_state == "reject":
                conn.execute(
                    f"UPDATE {table} SET resolution_state = 'review' WHERE id = %s",
                    (pk,),
                )
                ctrs.state_transitions_staged += 1

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
            conn.execute(f"RELEASE SAVEPOINT {sp}")

            ctrs.candidates_scored += 1
            effective_state = "auto_promote" if is_promoted else state
            if effective_state == "auto_promote":
                ctrs.candidates_auto_promote += 1
                # Track entity-specific new entries to auto_promote (§6.4)
                if current_state != "auto_promote":
                    if entity_type == "club":
                        ctrs.new_auto_promote_club += 1
                    elif entity_type == "participant":
                        ctrs.new_auto_promote_participant += 1
                    elif entity_type == "yacht":
                        ctrs.new_auto_promote_yacht += 1
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
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
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
    source_trust_policy = load_source_trust_policy(_DEFAULT_SOURCE_TRUST_PATH)
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
            _score_entity_type(conn, et, rule_set, source_trust_policy, score_run_id, ctrs)
            if et == "participant":
                _update_participant_score_distribution_stats(conn, ctrs)
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
        f"  candidates scored:         {ctrs.candidates_scored}",
        f"    → auto_promote:          {ctrs.candidates_auto_promote}",
        f"      new club→auto_promote: {ctrs.new_auto_promote_club}",
        f"      new part→auto_promote: {ctrs.new_auto_promote_participant}",
        f"      new yacht→auto_promote:{ctrs.new_auto_promote_yacht}",
        f"    → review:                {ctrs.candidates_review}",
        f"    → hold:                  {ctrs.candidates_hold}",
        f"    → rejected:              {ctrs.candidates_rejected}",
        f"  NBAs written:              {ctrs.nbas_written}",
        f"  staged transitions:        {ctrs.state_transitions_staged}",
        f"  capped transitions:        {ctrs.state_transitions_capped}",
        f"  trust-adjusted:            {ctrs.trust_adjusted_candidates}",
        f"  trust-capped:              {ctrs.trust_capped_candidates}",
        f"Child evidence (participant):",
        f"  child email used:          {ctrs.participant_child_email_used}",
        f"  child phone used:          {ctrs.participant_child_phone_used}",
        f"  child address used:        {ctrs.participant_child_address_used}",
        f"  address-only → hold:       {ctrs.participant_address_only_hold}",
        f"Participant score distribution:",
        f"  score mean:                {ctrs.participant_score_mean:.4f}" if ctrs.participant_score_mean is not None else "  score mean:                n/a",
        f"  score stddev:              {ctrs.participant_score_stddev:.4f}" if ctrs.participant_score_stddev is not None else "  score stddev:              n/a",
        f"  score unique values:       {ctrs.participant_score_unique_values}",
        f"  hold-band unique values:   {ctrs.participant_hold_score_unique_values}",
        f"  auto-promote unique vals:  {ctrs.participant_auto_promote_score_unique_values}",
        f"DB errors:                   {ctrs.db_errors}",
    ]
    if ctrs.warnings:
        lines.append(f"\nWarnings ({len(ctrs.warnings)}):")
        for w in ctrs.warnings[:20]:
            lines.append(f"  {w}")
        if len(ctrs.warnings) > 20:
            lines.append(f"  ... and {len(ctrs.warnings) - 20} more")
    lines.append("=" * 60)
    return "\n".join(lines)
