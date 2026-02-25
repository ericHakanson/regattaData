"""regatta_etl.resolution_rules

YAML-based scoring rules engine for entity-resolution pipelines.

Responsibilities:
  - Load and validate YAML rule files from config/resolution_rules/*.yml
  - Compute quality scores and resolve resolution_state for candidates
  - Apply feature weights, missing-attribute penalties, and hard blocks
  - Register rule sets into the DB (resolution_rule_set table)
  - Hash YAML content for traceability

Usage:
    from pathlib import Path
    from regatta_etl.resolution_rules import load_rule_set, compute_score

    rule_set = load_rule_set(Path("config/resolution_rules/participant.yml"))
    score, state, reasons = compute_score(
        rule_set,
        features={"email_exact": True, "normalized_name_exact": True},
    )
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_ENTITY_TYPES = frozenset({"participant", "yacht", "event", "registration", "club"})

REQUIRED_YAML_KEYS = frozenset({
    "entity_type",
    "source_system",
    "version",
    "thresholds",
    "feature_weights",
    "hard_blocks",
    "source_precedence",
    "survivorship_rules",
    "missing_attribute_penalties",
})

REQUIRED_THRESHOLD_KEYS = frozenset({"auto_promote", "review", "hold"})

VALID_RESOLUTION_STATES = ("auto_promote", "review", "hold", "reject")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RuleSetValidationError(ValueError):
    """Raised when a YAML rule file fails schema validation."""


# ---------------------------------------------------------------------------
# RuleSet dataclass
# ---------------------------------------------------------------------------

@dataclass
class RuleSet:
    """Parsed, validated scoring rule set loaded from a YAML file."""

    entity_type: str
    source_system: str
    version: str
    yaml_hash: str
    thresholds: dict[str, float]
    feature_weights: dict[str, float]
    hard_blocks: list[str]
    source_precedence: list[str]
    survivorship_rules: dict[str, str]
    missing_attribute_penalties: dict[str, float]
    raw_yaml: str = field(repr=False, default="")

    @property
    def threshold_auto_promote(self) -> float:
        return float(self.thresholds["auto_promote"])

    @property
    def threshold_review(self) -> float:
        return float(self.thresholds["review"])

    @property
    def threshold_hold(self) -> float:
        return float(self.thresholds["hold"])


# ---------------------------------------------------------------------------
# Loader + validator
# ---------------------------------------------------------------------------

def load_rule_set(yaml_path: Path) -> RuleSet:
    """Load, validate, and return a RuleSet from a YAML file.

    Args:
        yaml_path: Absolute or relative path to the YAML rule file.

    Returns:
        A validated RuleSet instance.

    Raises:
        RuleSetValidationError: If any required field is missing or invalid.
        FileNotFoundError: If the YAML file does not exist.
    """
    raw = yaml_path.read_text(encoding="utf-8")
    data: dict[str, Any] = yaml.safe_load(raw)
    validate_rule_set(data)
    yaml_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return RuleSet(
        entity_type=data["entity_type"],
        source_system=str(data["source_system"]),
        version=str(data["version"]),
        yaml_hash=yaml_hash,
        thresholds={k: float(v) for k, v in data["thresholds"].items()},
        feature_weights={k: float(v) for k, v in data["feature_weights"].items()},
        hard_blocks=list(data.get("hard_blocks") or []),
        source_precedence=list(data.get("source_precedence") or []),
        survivorship_rules=dict(data.get("survivorship_rules") or {}),
        missing_attribute_penalties={
            k: float(v) for k, v in (data.get("missing_attribute_penalties") or {}).items()
        },
        raw_yaml=raw,
    )


def validate_rule_set(data: dict[str, Any]) -> None:
    """Raise RuleSetValidationError if data does not match required schema.

    Validates:
      - Required top-level keys present
      - entity_type is one of the allowed values
      - thresholds keys and value ranges (hold <= review <= auto_promote)
      - feature_weights non-empty, all values >= 0
    """
    if not isinstance(data, dict):
        raise RuleSetValidationError("YAML root must be a mapping.")

    missing_keys = REQUIRED_YAML_KEYS - set(data.keys())
    if missing_keys:
        raise RuleSetValidationError(f"Missing required YAML keys: {sorted(missing_keys)}")

    entity_type = data.get("entity_type")
    if entity_type not in VALID_ENTITY_TYPES:
        raise RuleSetValidationError(
            f"Invalid entity_type '{entity_type}'. Must be one of {sorted(VALID_ENTITY_TYPES)}."
        )

    thresholds = data.get("thresholds") or {}
    missing_thresh = REQUIRED_THRESHOLD_KEYS - set(thresholds.keys())
    if missing_thresh:
        raise RuleSetValidationError(f"Missing threshold keys: {sorted(missing_thresh)}")

    for key, val in thresholds.items():
        try:
            fval = float(val)
        except (TypeError, ValueError):
            raise RuleSetValidationError(f"Threshold '{key}' value '{val}' is not numeric.")
        if not (0.0 <= fval <= 1.0):
            raise RuleSetValidationError(
                f"Threshold '{key}' value {fval} must be in [0.0, 1.0]."
            )

    hold = float(thresholds.get("hold", 0))
    review = float(thresholds.get("review", 0))
    auto_promote = float(thresholds.get("auto_promote", 0))
    if hold > review:
        raise RuleSetValidationError(f"'hold' threshold ({hold}) must be <= 'review' ({review}).")
    if review > auto_promote:
        raise RuleSetValidationError(
            f"'review' threshold ({review}) must be <= 'auto_promote' ({auto_promote})."
        )

    feature_weights = data.get("feature_weights") or {}
    if not feature_weights:
        raise RuleSetValidationError("'feature_weights' must not be empty.")
    for feat, weight in feature_weights.items():
        try:
            fw = float(weight)
        except (TypeError, ValueError):
            raise RuleSetValidationError(
                f"feature_weight '{feat}' value '{weight}' is not numeric."
            )
        if fw < 0:
            raise RuleSetValidationError(
                f"feature_weight '{feat}' value {fw} must be >= 0."
            )


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------

def compute_score(
    rule_set: RuleSet,
    features: dict[str, bool],
    hard_block_flags: list[str] | None = None,
) -> tuple[float, str, list[str]]:
    """Compute quality score and resolution_state for a candidate.

    The score is the sum of feature weights for all present features, then
    penalties for missing high-value attributes are subtracted, clamped to
    [0.0, 1.0].  Hard blocks short-circuit to score=0.0, state='reject'.

    Args:
        rule_set: Active RuleSet with weights, thresholds, and penalties.
        features: Mapping from feature name to boolean (True = feature present).
        hard_block_flags: List of hard-block condition names detected for this
            candidate.  If any match a rule in rule_set.hard_blocks, the
            candidate is immediately rejected.

    Returns:
        (score, resolution_state, confidence_reasons)
          score             — float in [0.0, 1.0]
          resolution_state  — one of 'auto_promote', 'review', 'hold', 'reject'
          confidence_reasons — list of human-readable reason strings
    """
    reasons: list[str] = []

    # Hard block check — any matching flag forces reject
    if hard_block_flags:
        for flag in hard_block_flags:
            if flag in rule_set.hard_blocks:
                reasons.append(f"hard_block:{flag}")
                return 0.0, "reject", reasons

    # Weighted feature sum
    score = 0.0
    for feat, weight in rule_set.feature_weights.items():
        if features.get(feat):
            score += weight
            reasons.append(f"feature:{feat}:{weight:.4f}")

    # Missing-attribute penalties
    for penalty_key, penalty_val in rule_set.missing_attribute_penalties.items():
        # e.g. "missing_email" → look for any feature starting with "email"
        attr_name = penalty_key.removeprefix("missing_")
        has_attr = any(
            k.startswith(attr_name) and bool(features.get(k))
            for k in rule_set.feature_weights
        )
        if not has_attr:
            score = max(0.0, score - penalty_val)
            reasons.append(f"penalty:{penalty_key}:{penalty_val:.4f}")

    # Clamp to [0.0, 1.0] and round to 4 decimal places
    score = round(min(1.0, max(0.0, score)), 4)

    # Threshold routing
    if score >= rule_set.threshold_auto_promote:
        state = "auto_promote"
    elif score >= rule_set.threshold_review:
        state = "review"
    elif score >= rule_set.threshold_hold:
        state = "hold"
    else:
        state = "reject"

    return score, state, reasons


def resolution_state_from_score(rule_set: RuleSet, score: float) -> str:
    """Return resolution_state string for a pre-computed numeric score."""
    if score >= rule_set.threshold_auto_promote:
        return "auto_promote"
    if score >= rule_set.threshold_review:
        return "review"
    if score >= rule_set.threshold_hold:
        return "hold"
    return "reject"


# ---------------------------------------------------------------------------
# DB persistence helpers
# ---------------------------------------------------------------------------

def register_rule_set(conn: Any, rule_set: RuleSet) -> str:
    """Upsert a RuleSet into resolution_rule_set and return its UUID.

    If an active rule set with the same (entity_type, source_system, yaml_hash)
    already exists, returns its existing ID without modification.

    A new version deactivates any prior active rule set for the same
    (entity_type, source_system) before inserting the new one.

    Args:
        conn: An open psycopg connection.
        rule_set: The RuleSet to register.

    Returns:
        The UUID string of the active resolution_rule_set row.
    """
    # Check if this exact hash is already registered and active
    existing = conn.execute(
        """
        SELECT id FROM resolution_rule_set
        WHERE entity_type = %s
          AND COALESCE(source_system, '') = %s
          AND yaml_hash = %s
          AND is_active = true
        LIMIT 1
        """,
        (rule_set.entity_type, rule_set.source_system or "", rule_set.yaml_hash),
    ).fetchone()
    if existing:
        return str(existing[0])

    # Deactivate existing active rule set for this entity/source combination
    conn.execute(
        """
        UPDATE resolution_rule_set
        SET is_active = false
        WHERE entity_type = %s
          AND COALESCE(source_system, '') = %s
          AND is_active = true
        """,
        (rule_set.entity_type, rule_set.source_system or ""),
    )

    # Insert the new active rule set
    row = conn.execute(
        """
        INSERT INTO resolution_rule_set
            (entity_type, source_system, version, yaml_content, yaml_hash,
             is_active, activated_at)
        VALUES (%s, %s, %s, %s, %s, true, now())
        RETURNING id
        """,
        (
            rule_set.entity_type,
            rule_set.source_system or None,
            rule_set.version,
            rule_set.raw_yaml,
            rule_set.yaml_hash,
        ),
    ).fetchone()
    return str(row[0])


def open_score_run(conn: Any, entity_type: str, source_scope: str | None, rule_set_id: str | None) -> str:
    """Insert a new resolution_score_run with status='running' and return its UUID."""
    row = conn.execute(
        """
        INSERT INTO resolution_score_run (entity_type, source_scope, rule_set_id, status)
        VALUES (%s, %s, %s, 'running')
        RETURNING id
        """,
        (entity_type, source_scope, rule_set_id),
    ).fetchone()
    return str(row[0])


def close_score_run(conn: Any, run_id: str, status: str, counters: dict[str, Any]) -> None:
    """Mark a resolution_score_run as finished with final counters."""
    import json
    conn.execute(
        """
        UPDATE resolution_score_run
        SET status = %s, finished_at = now(), counters = %s
        WHERE id = %s
        """,
        (status, json.dumps(counters), run_id),
    )
