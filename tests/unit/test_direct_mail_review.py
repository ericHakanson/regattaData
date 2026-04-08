"""Unit tests for direct-mail reviewer CSV contract normalization."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from regatta_etl.direct_mail_review import (
    apply_send_safety_gate,
    normalize_reviewer_csv,
    repair_overloaded_line1_addresses,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_normalize_reviewer_csv_handles_true_header_alias_and_value_normalization(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    summary_path = tmp_path / "summary.json"

    fieldnames = [
        "TRUE",
        "candidateRecordEvaluation",
        "containsErrata",
        "humanComment",
        "referenceCandidateId",
        "candidate_id",
        "countDistinctDisplayName",
    ]
    rows = [
        {
            "TRUE": "TRUE",
            "candidateRecordEvaluation": "UNDER COMBINED",
            "containsErrata": "TRUE",
            "humanComment": "needs review",
            "referenceCandidateId": "69ad1e5d-89a9-47c7-9139-b4789e18075b",
            "candidate_id": "3a84a433-44a6-4fdd-8123-2b6a65c6c7cc",
            "countDistinctDisplayName": "2",
        },
        {
            "TRUE": "FALSE",
            "candidateRecordEvaluation": "OVER COMBINATION",
            "containsErrata": "FALSE",
            "humanComment": "",
            "referenceCandidateId": "d057b177-0715-4027-8bb8-2a5361d16e07",
            "candidate_id": "da909559-0896-4736-b9c4-3b69861582bc",
            "countDistinctDisplayName": "3",
        },
    ]
    _write_csv(input_path, fieldnames, rows)

    summary = normalize_reviewer_csv(input_path, output_path, summary_path)

    with output_path.open(newline="", encoding="utf-8") as handle:
        out_rows = list(csv.DictReader(handle))

    assert out_rows[0]["sendCard"] == "true"
    assert out_rows[0]["candidateRecordEvaluation"] == "undercombined"
    assert out_rows[0]["containsErrata"] == "true"

    assert out_rows[1]["sendCard"] == "false"
    assert out_rows[1]["candidateRecordEvaluation"] == "overcombined"
    assert out_rows[1]["containsErrata"] == "false"

    assert summary.rows_read == 2
    assert summary.send_card_counts == {"true": 1, "false": 1}
    assert summary.evaluation_counts == {"undercombined": 1, "overcombined": 1}
    assert summary.contains_errata_counts == {"true": 1, "false": 1}

    summary_json = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_json["rows_read"] == 2


def test_normalize_reviewer_csv_accepts_studio_aliases_and_autofills_distinct_count(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input_studio_aliases.csv"
    output_path = tmp_path / "output_studio_aliases.csv"

    fieldnames = [
        "sendCard",
        "entityResolution",
        "containsErrata",
        "humanComment",
        "refCandidateId",
        "candidate_id",
        "display_name",
        "candidate_display_name",
    ]
    rows = [
        {
            "sendCard": "TRUE",
            "entityResolution": "UNDER COMBINED",
            "containsErrata": "FALSE",
            "humanComment": "",
            "refCandidateId": "69ad1e5d-89a9-47c7-9139-b4789e18075b",
            "candidate_id": "3a84a433-44a6-4fdd-8123-2b6a65c6c7cc",
            "display_name": "Ken Colburn",
            "candidate_display_name": "Ken Colburn",
        },
        {
            "sendCard": "",
            "entityResolution": "",
            "containsErrata": "",
            "humanComment": "",
            "refCandidateId": "",
            "candidate_id": "da909559-0896-4736-b9c4-3b69861582bc",
            "display_name": "Ken Colburn",
            "candidate_display_name": "Ken Colburn",
        },
    ]
    _write_csv(input_path, fieldnames, rows)

    summary = normalize_reviewer_csv(input_path, output_path)

    with output_path.open(newline="", encoding="utf-8") as handle:
        out_rows = list(csv.DictReader(handle))

    assert out_rows[0]["candidateRecordEvaluation"] == "undercombined"
    assert out_rows[0]["referenceCandidateId"] == "69ad1e5d-89a9-47c7-9139-b4789e18075b"
    assert out_rows[0]["countDistinctDisplayName"] == "2"
    assert out_rows[1]["countDistinctDisplayName"] == "2"
    assert summary.invalid_count_distinct_display_name == 0


def test_normalize_reviewer_csv_raises_on_missing_required_columns(tmp_path: Path) -> None:
    input_path = tmp_path / "input_missing.csv"
    output_path = tmp_path / "output_missing.csv"
    fieldnames = [
        "TRUE",
        "candidateRecordEvaluation",
        # missing containsErrata
        "humanComment",
        "referenceCandidateId",
        "countDistinctDisplayName",
    ]
    rows = [
        {
            "TRUE": "TRUE",
            "candidateRecordEvaluation": "",
            "humanComment": "",
            "referenceCandidateId": "",
            "countDistinctDisplayName": "1",
        }
    ]
    _write_csv(input_path, fieldnames, rows)

    with pytest.raises(ValueError, match="Missing required reviewer columns"):
        normalize_reviewer_csv(input_path, output_path)


def test_normalize_reviewer_csv_reports_invalid_uuid_and_invalid_distinct_count(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input_invalids.csv"
    output_path = tmp_path / "output_invalids.csv"

    fieldnames = [
        "sendCard",
        "candidateRecordEvaluation",
        "containsErrata",
        "humanComment",
        "referenceCandidateId",
        "countDistinctDisplayName",
    ]
    rows = [
        {
            "sendCard": "yes",
            "candidateRecordEvaluation": "UNDER COMBINED",
            "containsErrata": "1",
            "humanComment": "",
            "referenceCandidateId": "not-a-uuid",
            "countDistinctDisplayName": "many",
        }
    ]
    _write_csv(input_path, fieldnames, rows)

    summary = normalize_reviewer_csv(input_path, output_path)

    assert summary.invalid_reference_candidate_ids == 1
    assert summary.invalid_count_distinct_display_name == 1
    assert len(summary.warnings) == 2


def test_apply_send_safety_gate_blocks_undercombined_and_errata_by_default(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input_gate.csv"
    output_path = tmp_path / "output_gate.csv"

    fieldnames = [
        "TRUE",  # alias for sendCard
        "candidateRecordEvaluation",
        "containsErrata",
        "humanComment",
        "referenceCandidateId",
        "countDistinctDisplayName",
    ]
    rows = [
        {
            "TRUE": "TRUE",
            "candidateRecordEvaluation": "UNDER COMBINED",
            "containsErrata": "",
            "humanComment": "",
            "referenceCandidateId": "69ad1e5d-89a9-47c7-9139-b4789e18075b",
            "countDistinctDisplayName": "2",
        },
        {
            "TRUE": "TRUE",
            "candidateRecordEvaluation": "",
            "containsErrata": "TRUE",
            "humanComment": "",
            "referenceCandidateId": "",
            "countDistinctDisplayName": "1",
        },
        {
            "TRUE": "TRUE",
            "candidateRecordEvaluation": "",
            "containsErrata": "",
            "humanComment": "",
            "referenceCandidateId": "",
            "countDistinctDisplayName": "1",
        },
        {
            "TRUE": "",
            "candidateRecordEvaluation": "",
            "containsErrata": "",
            "humanComment": "",
            "referenceCandidateId": "",
            "countDistinctDisplayName": "1",
        },
    ]
    _write_csv(input_path, fieldnames, rows)

    summary = apply_send_safety_gate(input_path, output_path)

    with output_path.open(newline="", encoding="utf-8") as handle:
        out_rows = list(csv.DictReader(handle))

    assert out_rows[0]["send_ready"] == "false"
    assert out_rows[0]["send_block_reason"] == "undercombined"
    assert out_rows[1]["send_ready"] == "false"
    assert out_rows[1]["send_block_reason"] == "errata"
    assert out_rows[2]["send_ready"] == "true"
    assert out_rows[2]["send_block_reason"] == "ready"
    assert out_rows[3]["send_ready"] == "false"
    assert out_rows[3]["send_block_reason"] == "not_requested"

    assert summary.send_requested == 3
    assert summary.send_ready == 1
    assert summary.send_status_counts["blocked"] == 2
    assert summary.block_reason_counts["undercombined"] == 1
    assert summary.block_reason_counts["errata"] == 1


def test_apply_send_safety_gate_allows_override_with_reason(tmp_path: Path) -> None:
    input_path = tmp_path / "input_override.csv"
    output_path = tmp_path / "output_override.csv"

    fieldnames = [
        "sendCard",
        "candidateRecordEvaluation",
        "containsErrata",
        "humanComment",
        "referenceCandidateId",
        "countDistinctDisplayName",
        "sendOverride",
        "sendOverrideReason",
    ]
    rows = [
        {
            "sendCard": "TRUE",
            "candidateRecordEvaluation": "UNDER COMBINED",
            "containsErrata": "TRUE",
            "humanComment": "",
            "referenceCandidateId": "69ad1e5d-89a9-47c7-9139-b4789e18075b",
            "countDistinctDisplayName": "2",
            "sendOverride": "TRUE",
            "sendOverrideReason": "verified manually for this drop",
        }
    ]
    _write_csv(input_path, fieldnames, rows)

    summary = apply_send_safety_gate(input_path, output_path)

    with output_path.open(newline="", encoding="utf-8") as handle:
        out_rows = list(csv.DictReader(handle))

    assert out_rows[0]["send_ready"] == "true"
    assert out_rows[0]["send_status"] == "ready_override"
    assert out_rows[0]["send_block_reason"] == "override"
    assert summary.overrides_applied == 1


def test_repair_overloaded_line1_addresses_parses_common_us_pattern(tmp_path: Path) -> None:
    input_path = tmp_path / "input_address.csv"
    output_path = tmp_path / "output_address.csv"

    fieldnames = [
        "line1",
        "city",
        "state",
        "postal_code",
        "country_code",
        "containsErrata",
        "humanComment",
    ]
    rows = [
        {
            "line1": "33 Grimes Ave., East Boothbay, ME 4544 US",
            "city": "",
            "state": "",
            "postal_code": "",
            "country_code": "",
            "containsErrata": "TRUE",
            "humanComment": "attribute overloaded; all address data in Line1",
        }
    ]
    _write_csv(input_path, fieldnames, rows)

    summary = repair_overloaded_line1_addresses(input_path, output_path)

    with output_path.open(newline="", encoding="utf-8") as handle:
        out_rows = list(csv.DictReader(handle))

    assert summary.rows_attempted == 1
    assert summary.rows_repaired == 1
    assert out_rows[0]["address_repair_status"] == "repaired"
    assert out_rows[0]["city_repaired"] == "East Boothbay"
    assert out_rows[0]["state_repaired"] == "ME"
    assert out_rows[0]["postal_code_repaired"] == "04544"


def test_repair_overloaded_line1_addresses_marks_not_needed_when_structured_present(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input_address_clean.csv"
    output_path = tmp_path / "output_address_clean.csv"

    fieldnames = [
        "line1",
        "city",
        "state",
        "postal_code",
        "country_code",
        "containsErrata",
        "humanComment",
    ]
    rows = [
        {
            "line1": "10 Rogers Rd Apt 308",
            "city": "Freeport",
            "state": "ME",
            "postal_code": "04032",
            "country_code": "US",
            "containsErrata": "",
            "humanComment": "",
        }
    ]
    _write_csv(input_path, fieldnames, rows)

    summary = repair_overloaded_line1_addresses(input_path, output_path)

    with output_path.open(newline="", encoding="utf-8") as handle:
        out_rows = list(csv.DictReader(handle))

    assert summary.rows_attempted == 0
    assert summary.rows_repaired == 0
    assert out_rows[0]["address_repair_status"] == "not_needed"
