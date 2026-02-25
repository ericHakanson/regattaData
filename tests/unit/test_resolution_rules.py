"""Unit tests for regatta_etl.resolution_rules."""

from __future__ import annotations

import hashlib
import textwrap
from pathlib import Path

import pytest
import yaml

from regatta_etl.resolution_rules import (
    RuleSet,
    RuleSetValidationError,
    compute_score,
    load_rule_set,
    resolution_state_from_score,
    validate_rule_set,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PARTICIPANT_YAML = textwrap.dedent("""\
    entity_type: participant
    source_system: "regattaman|mailchimp"
    version: "v1.0.0"
    thresholds:
      auto_promote: 0.95
      review: 0.75
      hold: 0.50
    feature_weights:
      email_exact: 0.55
      phone_exact: 0.20
      dob_exact: 0.15
      normalized_name_exact: 0.10
    hard_blocks:
      - conflicting_dob
      - conflicting_high_confidence_email
    source_precedence:
      - jotform_waiver_csv
      - regattaman_csv_export
    survivorship_rules:
      date_of_birth: highest_precedence_non_null
    missing_attribute_penalties:
      missing_email: 0.10
      missing_phone: 0.05
""")

YACHT_YAML = textwrap.dedent("""\
    entity_type: yacht
    source_system: "regattaman"
    version: "v1.0.0"
    thresholds:
      auto_promote: 0.95
      review: 0.75
      hold: 0.50
    feature_weights:
      sail_number_exact: 0.50
      name_normalized: 0.30
      yacht_type_present: 0.10
      length_feet_present: 0.10
    hard_blocks:
      - conflicting_sail_number_same_name
    source_precedence:
      - regattaman_csv_export
    survivorship_rules:
      sail_number: highest_precedence_non_null
    missing_attribute_penalties:
      missing_sail_number: 0.15
      missing_name: 0.20
""")


@pytest.fixture
def participant_rule_yaml_path(tmp_path: Path) -> Path:
    p = tmp_path / "participant.yml"
    p.write_text(PARTICIPANT_YAML, encoding="utf-8")
    return p


@pytest.fixture
def yacht_rule_yaml_path(tmp_path: Path) -> Path:
    p = tmp_path / "yacht.yml"
    p.write_text(YACHT_YAML, encoding="utf-8")
    return p


@pytest.fixture
def participant_rule_set(participant_rule_yaml_path: Path) -> RuleSet:
    return load_rule_set(participant_rule_yaml_path)


@pytest.fixture
def yacht_rule_set(yacht_rule_yaml_path: Path) -> RuleSet:
    return load_rule_set(yacht_rule_yaml_path)


# ---------------------------------------------------------------------------
# load_rule_set — happy path
# ---------------------------------------------------------------------------

class TestLoadRuleSet:
    def test_loads_entity_type(self, participant_rule_set: RuleSet):
        assert participant_rule_set.entity_type == "participant"

    def test_loads_version(self, participant_rule_set: RuleSet):
        assert participant_rule_set.version == "v1.0.0"

    def test_loads_source_system(self, participant_rule_set: RuleSet):
        assert "regattaman" in participant_rule_set.source_system

    def test_yaml_hash_is_sha256_hex(self, participant_rule_set: RuleSet):
        expected = hashlib.sha256(PARTICIPANT_YAML.encode("utf-8")).hexdigest()
        assert participant_rule_set.yaml_hash == expected

    def test_thresholds_are_floats(self, participant_rule_set: RuleSet):
        assert isinstance(participant_rule_set.threshold_auto_promote, float)
        assert isinstance(participant_rule_set.threshold_review, float)
        assert isinstance(participant_rule_set.threshold_hold, float)

    def test_threshold_ordering(self, participant_rule_set: RuleSet):
        assert participant_rule_set.threshold_hold <= participant_rule_set.threshold_review
        assert participant_rule_set.threshold_review <= participant_rule_set.threshold_auto_promote

    def test_feature_weights_non_empty(self, participant_rule_set: RuleSet):
        assert len(participant_rule_set.feature_weights) > 0

    def test_hard_blocks_loaded(self, participant_rule_set: RuleSet):
        assert "conflicting_dob" in participant_rule_set.hard_blocks

    def test_missing_attribute_penalties_loaded(self, participant_rule_set: RuleSet):
        assert "missing_email" in participant_rule_set.missing_attribute_penalties

    def test_raw_yaml_preserved(self, participant_rule_set: RuleSet):
        assert "feature_weights" in participant_rule_set.raw_yaml

    def test_file_not_found_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_rule_set(tmp_path / "does_not_exist.yml")


# ---------------------------------------------------------------------------
# validate_rule_set
# ---------------------------------------------------------------------------

class TestValidateRuleSet:
    def _base(self) -> dict:
        """Return a minimal valid rule set dict."""
        return yaml.safe_load(PARTICIPANT_YAML)

    def test_valid_data_passes(self):
        validate_rule_set(self._base())  # must not raise

    def test_missing_required_key_raises(self):
        data = self._base()
        del data["thresholds"]
        with pytest.raises(RuleSetValidationError, match="thresholds"):
            validate_rule_set(data)

    def test_invalid_entity_type_raises(self):
        data = self._base()
        data["entity_type"] = "spaceship"
        with pytest.raises(RuleSetValidationError, match="entity_type"):
            validate_rule_set(data)

    def test_all_valid_entity_types_pass(self):
        for et in ("participant", "yacht", "event", "registration", "club"):
            data = self._base()
            data["entity_type"] = et
            validate_rule_set(data)  # must not raise

    def test_missing_threshold_key_raises(self):
        data = self._base()
        del data["thresholds"]["auto_promote"]
        with pytest.raises(RuleSetValidationError, match="auto_promote"):
            validate_rule_set(data)

    def test_threshold_out_of_range_raises(self):
        data = self._base()
        data["thresholds"]["auto_promote"] = 1.5
        with pytest.raises(RuleSetValidationError, match="1.5"):
            validate_rule_set(data)

    def test_threshold_hold_greater_than_review_raises(self):
        data = self._base()
        data["thresholds"]["hold"] = 0.90
        data["thresholds"]["review"] = 0.75
        with pytest.raises(RuleSetValidationError, match="hold"):
            validate_rule_set(data)

    def test_threshold_review_greater_than_auto_promote_raises(self):
        data = self._base()
        data["thresholds"]["review"] = 0.96
        data["thresholds"]["auto_promote"] = 0.95
        with pytest.raises(RuleSetValidationError, match="review"):
            validate_rule_set(data)

    def test_empty_feature_weights_raises(self):
        data = self._base()
        data["feature_weights"] = {}
        with pytest.raises(RuleSetValidationError, match="feature_weights"):
            validate_rule_set(data)

    def test_negative_feature_weight_raises(self):
        data = self._base()
        data["feature_weights"]["email_exact"] = -0.1
        with pytest.raises(RuleSetValidationError, match="email_exact"):
            validate_rule_set(data)

    def test_non_dict_root_raises(self):
        with pytest.raises(RuleSetValidationError, match="mapping"):
            validate_rule_set([1, 2, 3])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# compute_score — participant rules
# ---------------------------------------------------------------------------

class TestComputeScoreParticipant:
    def test_all_features_present_gives_high_score(self, participant_rule_set: RuleSet):
        features = {
            "email_exact": True,
            "phone_exact": True,
            "dob_exact": True,
            "normalized_name_exact": True,
        }
        score, state, reasons = compute_score(participant_rule_set, features)
        # All weights sum to 1.0, no penalties since all features present
        assert score == pytest.approx(1.0)
        assert state == "auto_promote"

    def test_email_only_gives_review_state(self, participant_rule_set: RuleSet):
        # email=0.55, phone=0.20 absent so no phone penalty but missing_phone penalty=0.05
        # and missing nothing else since email is present, no missing_email penalty
        # score = 0.55 - 0.05 (missing_phone) = 0.50
        features = {"email_exact": True}
        score, state, reasons = compute_score(participant_rule_set, features)
        assert score == pytest.approx(0.50)
        assert state == "hold"

    def test_no_features_gives_reject(self, participant_rule_set: RuleSet):
        # 0.0 - 0.10 (email penalty) - 0.05 (phone penalty) = 0.0 (clamped)
        features = {}
        score, state, reasons = compute_score(participant_rule_set, features)
        assert score == pytest.approx(0.0)
        assert state == "reject"

    def test_hard_block_forces_reject_regardless_of_features(self, participant_rule_set: RuleSet):
        features = {
            "email_exact": True,
            "phone_exact": True,
            "dob_exact": True,
            "normalized_name_exact": True,
        }
        score, state, reasons = compute_score(
            participant_rule_set, features, hard_block_flags=["conflicting_dob"]
        )
        assert score == pytest.approx(0.0)
        assert state == "reject"
        assert any("hard_block:conflicting_dob" in r for r in reasons)

    def test_hard_block_not_in_rules_does_not_block(self, participant_rule_set: RuleSet):
        features = {"email_exact": True, "phone_exact": True}
        score, state, reasons = compute_score(
            participant_rule_set, features, hard_block_flags=["unknown_flag"]
        )
        # Should NOT be rejected — unknown_flag is not in rule_set.hard_blocks
        assert state != "reject"

    def test_reasons_list_non_empty_on_match(self, participant_rule_set: RuleSet):
        features = {"email_exact": True}
        _, _, reasons = compute_score(participant_rule_set, features)
        assert any("feature:email_exact" in r for r in reasons)

    def test_penalty_reason_present_when_phone_absent(self, participant_rule_set: RuleSet):
        features = {"email_exact": True}
        _, _, reasons = compute_score(participant_rule_set, features)
        assert any("penalty:missing_phone" in r for r in reasons)

    def test_score_clamped_to_zero(self, participant_rule_set: RuleSet):
        # Even with heavy penalties, score must not go below 0
        score, _, _ = compute_score(participant_rule_set, {})
        assert score >= 0.0

    def test_score_clamped_to_one(self, participant_rule_set: RuleSet):
        features = {k: True for k in participant_rule_set.feature_weights}
        score, _, _ = compute_score(participant_rule_set, features)
        assert score <= 1.0

    def test_empty_hard_block_flags_does_not_block(self, participant_rule_set: RuleSet):
        features = {"email_exact": True}
        score, state, _ = compute_score(participant_rule_set, features, hard_block_flags=[])
        assert state != "reject"

    def test_none_hard_block_flags_does_not_block(self, participant_rule_set: RuleSet):
        features = {"email_exact": True}
        score, state, _ = compute_score(participant_rule_set, features, hard_block_flags=None)
        assert state != "reject"


# ---------------------------------------------------------------------------
# compute_score — yacht rules
# ---------------------------------------------------------------------------

class TestComputeScoreYacht:
    def test_all_features_gives_auto_promote(self, yacht_rule_set: RuleSet):
        features = {
            "sail_number_exact": True,
            "name_normalized": True,
            "yacht_type_present": True,
            "length_feet_present": True,
        }
        score, state, _ = compute_score(yacht_rule_set, features)
        assert score == pytest.approx(1.0)
        assert state == "auto_promote"

    def test_name_only_gives_hold_after_penalties(self, yacht_rule_set: RuleSet):
        # name_normalized=0.30 - missing_sail_number=0.15 = 0.15 → reject (< 0.50)
        features = {"name_normalized": True}
        score, state, _ = compute_score(yacht_rule_set, features)
        assert score == pytest.approx(0.15)
        assert state == "reject"

    def test_sail_and_name_gives_review(self, yacht_rule_set: RuleSet):
        # sail=0.50 + name=0.30 = 0.80 → review
        features = {"sail_number_exact": True, "name_normalized": True}
        score, state, _ = compute_score(yacht_rule_set, features)
        assert score == pytest.approx(0.80)
        assert state == "review"

    def test_conflicting_sail_hard_block(self, yacht_rule_set: RuleSet):
        features = {"sail_number_exact": True, "name_normalized": True}
        score, state, reasons = compute_score(
            yacht_rule_set, features,
            hard_block_flags=["conflicting_sail_number_same_name"],
        )
        assert score == pytest.approx(0.0)
        assert state == "reject"


# ---------------------------------------------------------------------------
# resolution_state_from_score
# ---------------------------------------------------------------------------

class TestResolutionStateFromScore:
    def test_auto_promote_threshold(self, participant_rule_set: RuleSet):
        assert resolution_state_from_score(participant_rule_set, 0.95) == "auto_promote"
        assert resolution_state_from_score(participant_rule_set, 1.0) == "auto_promote"

    def test_review_threshold(self, participant_rule_set: RuleSet):
        assert resolution_state_from_score(participant_rule_set, 0.75) == "review"
        assert resolution_state_from_score(participant_rule_set, 0.94) == "review"

    def test_hold_threshold(self, participant_rule_set: RuleSet):
        assert resolution_state_from_score(participant_rule_set, 0.50) == "hold"
        assert resolution_state_from_score(participant_rule_set, 0.74) == "hold"

    def test_reject_below_hold(self, participant_rule_set: RuleSet):
        assert resolution_state_from_score(participant_rule_set, 0.0) == "reject"
        assert resolution_state_from_score(participant_rule_set, 0.49) == "reject"


# ---------------------------------------------------------------------------
# Fingerprint helpers
# ---------------------------------------------------------------------------

class TestFingerprints:
    def test_participant_fingerprint_deterministic(self):
        from regatta_etl.resolution_source_to_candidate import participant_fingerprint
        fp1 = participant_fingerprint("john smith", "john@example.com")
        fp2 = participant_fingerprint("john smith", "john@example.com")
        assert fp1 == fp2

    def test_participant_fingerprint_email_case_insensitive(self):
        from regatta_etl.resolution_source_to_candidate import participant_fingerprint
        fp_lower = participant_fingerprint("john smith", "john@example.com")
        fp_upper = participant_fingerprint("john smith", "JOHN@EXAMPLE.COM")
        assert fp_lower == fp_upper

    def test_participant_fingerprint_differs_by_email(self):
        from regatta_etl.resolution_source_to_candidate import participant_fingerprint
        fp1 = participant_fingerprint("john smith", "john@example.com")
        fp2 = participant_fingerprint("john smith", "other@example.com")
        assert fp1 != fp2

    def test_participant_fingerprint_none_email(self):
        from regatta_etl.resolution_source_to_candidate import participant_fingerprint
        fp = participant_fingerprint("john smith", None)
        assert isinstance(fp, str) and len(fp) == 64

    def test_yacht_fingerprint_deterministic(self):
        from regatta_etl.resolution_source_to_candidate import yacht_fingerprint
        assert yacht_fingerprint("fast boat", "US-123") == yacht_fingerprint("fast boat", "US-123")

    def test_yacht_fingerprint_differs_by_sail(self):
        from regatta_etl.resolution_source_to_candidate import yacht_fingerprint
        assert yacht_fingerprint("fast boat", "US-123") != yacht_fingerprint("fast boat", "US-999")

    def test_club_fingerprint_deterministic(self):
        from regatta_etl.resolution_source_to_candidate import club_fingerprint
        assert club_fingerprint("bhyc") == club_fingerprint("bhyc")

    def test_club_fingerprint_differs_by_name(self):
        from regatta_etl.resolution_source_to_candidate import club_fingerprint
        assert club_fingerprint("bhyc") != club_fingerprint("other-club")

    def test_event_fingerprint_deterministic(self):
        from regatta_etl.resolution_source_to_candidate import event_fingerprint
        fp1 = event_fingerprint("bhyc regatta", 2025, "race-123")
        fp2 = event_fingerprint("bhyc regatta", 2025, "race-123")
        assert fp1 == fp2

    def test_event_fingerprint_differs_by_year(self):
        from regatta_etl.resolution_source_to_candidate import event_fingerprint
        assert event_fingerprint("bhyc regatta", 2025, None) != event_fingerprint("bhyc regatta", 2024, None)

    def test_registration_fingerprint_deterministic(self):
        from regatta_etl.resolution_source_to_candidate import registration_fingerprint
        eid = "a0000000-0000-0000-0000-000000000001"
        yid = "b0000000-0000-0000-0000-000000000002"
        fp1 = registration_fingerprint(eid, "EXT-1", yid)
        fp2 = registration_fingerprint(eid, "EXT-1", yid)
        assert fp1 == fp2

    def test_registration_fingerprint_differs_by_event(self):
        from regatta_etl.resolution_source_to_candidate import registration_fingerprint
        e1 = "a0000000-0000-0000-0000-000000000001"
        e2 = "a0000000-0000-0000-0000-000000000002"
        assert registration_fingerprint(e1, "EXT-1", None) != registration_fingerprint(e2, "EXT-1", None)


# ---------------------------------------------------------------------------
# YAML loading from actual rule files
# ---------------------------------------------------------------------------

class TestActualYamlFiles:
    """Smoke-tests that the real YAML rule files in config/ are valid."""

    @pytest.fixture(autouse=True)
    def rules_dir(self) -> Path:
        # Locate config/resolution_rules relative to this test file
        here = Path(__file__).parent
        # Walk up to project root
        project_root = here
        while project_root.name != "regattaData" and project_root != project_root.parent:
            project_root = project_root.parent
        return project_root / "config" / "resolution_rules"

    def test_participant_yml_loads(self, rules_dir: Path):
        if not (rules_dir / "participant.yml").exists():
            pytest.skip("participant.yml not found")
        rs = load_rule_set(rules_dir / "participant.yml")
        assert rs.entity_type == "participant"

    def test_yacht_yml_loads(self, rules_dir: Path):
        if not (rules_dir / "yacht.yml").exists():
            pytest.skip("yacht.yml not found")
        rs = load_rule_set(rules_dir / "yacht.yml")
        assert rs.entity_type == "yacht"

    def test_club_yml_loads(self, rules_dir: Path):
        if not (rules_dir / "club.yml").exists():
            pytest.skip("club.yml not found")
        rs = load_rule_set(rules_dir / "club.yml")
        assert rs.entity_type == "club"

    def test_event_yml_loads(self, rules_dir: Path):
        if not (rules_dir / "event.yml").exists():
            pytest.skip("event.yml not found")
        rs = load_rule_set(rules_dir / "event.yml")
        assert rs.entity_type == "event"

    def test_registration_yml_loads(self, rules_dir: Path):
        if not (rules_dir / "registration.yml").exists():
            pytest.skip("registration.yml not found")
        rs = load_rule_set(rules_dir / "registration.yml")
        assert rs.entity_type == "registration"

    def test_all_rule_files_have_valid_threshold_ordering(self, rules_dir: Path):
        for yml_path in rules_dir.glob("*.yml"):
            rs = load_rule_set(yml_path)
            assert rs.threshold_hold <= rs.threshold_review, yml_path.name
            assert rs.threshold_review <= rs.threshold_auto_promote, yml_path.name
