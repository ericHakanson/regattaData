"""Helpers for normalizing direct-mail reviewer CSV artifacts."""

from __future__ import annotations

import csv
import json
import re
import tempfile
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from regatta_etl.normalize import (
    normalize_country_code,
    normalize_postal_code_for_storage,
    trim,
)


_REQUIRED_REVIEW_COLUMNS = [
    "sendCard",
    "candidateRecordEvaluation",
    "containsErrata",
    "humanComment",
    "referenceCandidateId",
]

_OPTIONAL_REVIEW_COLUMNS = [
    "countDistinctDisplayName",
]

CANONICAL_REVIEW_COLUMNS = _REQUIRED_REVIEW_COLUMNS + _OPTIONAL_REVIEW_COLUMNS

_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "sendCard": ("sendCard", "send card", "send_card", "TRUE", "true"),
    "candidateRecordEvaluation": (
        "candidateRecordEvaluation",
        "candidate_record_evaluation",
        "candidate evaluation",
        "entityResolution",
        "entity_resolution",
        "entity resolution",
    ),
    "containsErrata": ("containsErrata", "contains_errata", "contains errata"),
    "humanComment": ("humanComment", "human_comment", "human comment"),
    "referenceCandidateId": (
        "referenceCandidateId",
        "reference_candidate_id",
        "reference candidate id",
        "refCandidateId",
        "ref_candidate_id",
        "ref candidate id",
    ),
    "countDistinctDisplayName": (
        "countDistinctDisplayName",
        "count_distinct_display_name",
        "count distinct display name",
    ),
}


def _norm_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _norm_eval(value: str) -> str:
    raw = re.sub(r"[^a-z]+", "", (value or "").strip().lower())
    if raw in {"undercombined", "undercombination"}:
        return "undercombined"
    if raw in {"overcombined", "overcombination"}:
        return "overcombined"
    return ""


def _norm_bool(value: str) -> str:
    lower = (value or "").strip().lower()
    if lower in {"true", "1", "yes", "y"}:
        return "true"
    if lower in {"false", "0", "no", "n"}:
        return "false"
    return ""


def _boolish(value: str) -> bool:
    return _norm_bool(value) == "true"


@dataclass
class ReviewContractSummary:
    rows_read: int
    send_card_counts: dict[str, int]
    evaluation_counts: dict[str, int]
    contains_errata_counts: dict[str, int]
    invalid_reference_candidate_ids: int
    invalid_count_distinct_display_name: int
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "send_card_counts": self.send_card_counts,
            "evaluation_counts": self.evaluation_counts,
            "contains_errata_counts": self.contains_errata_counts,
            "invalid_reference_candidate_ids": self.invalid_reference_candidate_ids,
            "invalid_count_distinct_display_name": self.invalid_count_distinct_display_name,
            "warnings": self.warnings,
        }


@dataclass
class SendSafetySummary:
    rows_read: int
    send_requested: int
    send_ready: int
    overrides_applied: int
    send_status_counts: dict[str, int]
    block_reason_counts: dict[str, int]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "send_requested": self.send_requested,
            "send_ready": self.send_ready,
            "overrides_applied": self.overrides_applied,
            "send_status_counts": self.send_status_counts,
            "block_reason_counts": self.block_reason_counts,
            "warnings": self.warnings,
        }


@dataclass
class AddressRepairSummary:
    rows_read: int
    rows_attempted: int
    rows_repaired: int
    repair_status_counts: dict[str, int]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "rows_attempted": self.rows_attempted,
            "rows_repaired": self.rows_repaired,
            "repair_status_counts": self.repair_status_counts,
            "warnings": self.warnings,
        }


def _resolve_column_map(headers: list[str]) -> dict[str, str]:
    header_map = {_norm_header(h): h for h in headers}
    resolved: dict[str, str] = {}
    missing: list[str] = []

    for canonical in _REQUIRED_REVIEW_COLUMNS:
        aliases = _COLUMN_ALIASES[canonical]
        actual = None
        for alias in aliases:
            mapped = header_map.get(_norm_header(alias))
            if mapped:
                actual = mapped
                break
        if not actual:
            missing.append(canonical)
            continue
        resolved[canonical] = actual

    if missing:
        raise ValueError(f"Missing required reviewer columns: {sorted(missing)}")

    for canonical in _OPTIONAL_REVIEW_COLUMNS:
        aliases = _COLUMN_ALIASES[canonical]
        for alias in aliases:
            mapped = header_map.get(_norm_header(alias))
            if mapped:
                resolved[canonical] = mapped
                break
    return resolved


def normalize_reviewer_csv(
    input_path: Path,
    output_path: Path,
    summary_path: Path | None = None,
) -> ReviewContractSummary:
    with input_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no header row: {input_path}")
        fieldnames = list(reader.fieldnames)
        column_map = _resolve_column_map(fieldnames)
        rows = list(reader)

    rewritten_fieldnames = []
    for field in fieldnames:
        canonical_name = None
        for canonical, actual in column_map.items():
            if field == actual:
                canonical_name = canonical
                break
        rewritten_fieldnames.append(canonical_name or field)

    if "countDistinctDisplayName" not in rewritten_fieldnames:
        rewritten_fieldnames.append("countDistinctDisplayName")

    send_counter: Counter[str] = Counter()
    eval_counter: Counter[str] = Counter()
    errata_counter: Counter[str] = Counter()
    invalid_reference = 0
    invalid_distinct_name_count = 0
    warnings: list[str] = []

    normalized_rows: list[dict[str, str]] = []
    for idx, row in enumerate(rows, start=2):
        out_row: dict[str, str] = {}
        for src_field, dst_field in zip(fieldnames, rewritten_fieldnames):
            out_row[dst_field] = row.get(src_field, "")

        send_value = _norm_bool(out_row.get("sendCard", ""))
        eval_value = _norm_eval(out_row.get("candidateRecordEvaluation", ""))
        errata_value = _norm_bool(out_row.get("containsErrata", ""))

        out_row["sendCard"] = send_value
        out_row["candidateRecordEvaluation"] = eval_value
        out_row["containsErrata"] = errata_value

        send_counter[send_value or "<blank>"] += 1
        eval_counter[eval_value or "<blank>"] += 1
        errata_counter[errata_value or "<blank>"] += 1

        ref_id = (out_row.get("referenceCandidateId") or "").strip()
        if ref_id:
            try:
                uuid.UUID(ref_id)
            except ValueError:
                invalid_reference += 1
                warnings.append(
                    f"row {idx}: invalid referenceCandidateId (not UUID): {ref_id}"
                )

        distinct_name_count = (out_row.get("countDistinctDisplayName") or "").strip()
        if distinct_name_count and not distinct_name_count.isdigit():
            invalid_distinct_name_count += 1
            warnings.append(
                "row "
                f"{idx}: invalid countDistinctDisplayName (expected integer): "
                f"{distinct_name_count}"
            )

        normalized_rows.append(out_row)

    display_counts: Counter[str] = Counter()
    for row in normalized_rows:
        display_name = (
            trim(row.get("candidate_display_name"))
            or trim(row.get("display_name"))
            or ""
        )
        key = re.sub(r"\s+", " ", display_name.lower()).strip()
        if key:
            display_counts[key] += 1

    for row in normalized_rows:
        distinct_name_count = (row.get("countDistinctDisplayName") or "").strip()
        if distinct_name_count:
            continue
        display_name = (
            trim(row.get("candidate_display_name"))
            or trim(row.get("display_name"))
            or ""
        )
        key = re.sub(r"\s+", " ", display_name.lower()).strip()
        if key:
            row["countDistinctDisplayName"] = str(display_counts[key])
        else:
            row["countDistinctDisplayName"] = "1"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rewritten_fieldnames)
        writer.writeheader()
        writer.writerows(normalized_rows)

    summary = ReviewContractSummary(
        rows_read=len(normalized_rows),
        send_card_counts=dict(send_counter),
        evaluation_counts=dict(eval_counter),
        contains_errata_counts=dict(errata_counter),
        invalid_reference_candidate_ids=invalid_reference,
        invalid_count_distinct_display_name=invalid_distinct_name_count,
        warnings=warnings,
    )

    if summary_path:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary.to_dict(), indent=2), encoding="utf-8")

    return summary


def apply_send_safety_gate(
    input_path: Path,
    output_path: Path,
    summary_path: Path | None = None,
) -> SendSafetySummary:
    """Apply send gating from reviewer annotations.

    Gate rule:
      send_ready is true only when sendCard=true AND no quality blockers,
      unless explicit override is provided.
    """
    warnings: list[str] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        normalized_path = Path(tmpdir) / "normalized.csv"
        normalize_reviewer_csv(input_path=input_path, output_path=normalized_path)
        with normalized_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            fieldnames = list(reader.fieldnames or [])

    if "sendOverride" not in fieldnames:
        fieldnames.append("sendOverride")
    if "sendOverrideReason" not in fieldnames:
        fieldnames.append("sendOverrideReason")

    output_fields = fieldnames + ["send_requested", "send_ready", "send_status", "send_block_reason"]

    send_status_counts: Counter[str] = Counter()
    block_reason_counts: Counter[str] = Counter()
    send_requested_count = 0
    send_ready_count = 0
    overrides_applied = 0

    gated_rows: list[dict[str, str]] = []
    for idx, row in enumerate(rows, start=2):
        send_requested = _boolish(row.get("sendCard", ""))
        contains_errata = _boolish(row.get("containsErrata", ""))
        evaluation = _norm_eval(row.get("candidateRecordEvaluation", ""))
        override = _boolish(row.get("sendOverride", ""))
        override_reason = (row.get("sendOverrideReason") or "").strip()

        blockers: list[str] = []
        if evaluation == "undercombined":
            blockers.append("undercombined")
        if evaluation == "overcombined":
            blockers.append("overcombined")
        if contains_errata:
            blockers.append("errata")

        if send_requested:
            send_requested_count += 1

        if override and not override_reason:
            warnings.append(
                f"row {idx}: sendOverride=true but sendOverrideReason is blank; "
                "override ignored"
            )
            override = False

        if send_requested and override:
            send_ready = True
            send_status = "ready_override"
            send_block_reason = "override"
            overrides_applied += 1
        elif not send_requested:
            send_ready = False
            send_status = "not_requested"
            send_block_reason = "not_requested"
        elif blockers:
            send_ready = False
            send_status = "blocked"
            send_block_reason = "|".join(sorted(blockers))
        else:
            send_ready = True
            send_status = "ready"
            send_block_reason = "ready"

        if send_ready:
            send_ready_count += 1

        send_status_counts[send_status] += 1
        block_reason_counts[send_block_reason] += 1

        out = dict(row)
        out["send_requested"] = "true" if send_requested else "false"
        out["send_ready"] = "true" if send_ready else "false"
        out["send_status"] = send_status
        out["send_block_reason"] = send_block_reason
        gated_rows.append(out)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(gated_rows)

    summary = SendSafetySummary(
        rows_read=len(gated_rows),
        send_requested=send_requested_count,
        send_ready=send_ready_count,
        overrides_applied=overrides_applied,
        send_status_counts=dict(send_status_counts),
        block_reason_counts=dict(block_reason_counts),
        warnings=warnings,
    )

    if summary_path:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary.to_dict(), indent=2), encoding="utf-8")

    return summary


def _parse_overloaded_line1(raw_line1: str) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Best-effort parser for common US line1-overloaded address strings."""
    state_name_map = {
        "maine": "ME",
        "massachusetts": "MA",
        "newhampshire": "NH",
        "newyork": "NY",
        "rhodeisland": "RI",
        "connecticut": "CT",
        "florida": "FL",
        "maryland": "MD",
        "md": "MD",
        "me": "ME",
        "ma": "MA",
        "nh": "NH",
        "ny": "NY",
        "ri": "RI",
        "ct": "CT",
        "fl": "FL",
    }

    def _norm_state(value: str | None) -> str | None:
        if not value:
            return None
        key = re.sub(r"[^a-z]", "", value.lower())
        return state_name_map.get(key)

    def _split_line1_city(pre: str) -> tuple[str | None, str | None]:
        tokens = [t for t in pre.strip().split() if t]
        if len(tokens) < 2:
            return (None, None)

        marker_tokens = {
            "st",
            "street",
            "ave",
            "avenue",
            "rd",
            "road",
            "ln",
            "lane",
            "dr",
            "drive",
            "way",
            "point",
            "blvd",
            "circle",
            "cir",
            "ct",
            "court",
            "ter",
            "terrace",
            "highway",
            "hwy",
            "box",
        }
        cut = None
        for idx, token in enumerate(tokens):
            normalized = re.sub(r"[^a-z0-9]", "", token.lower())
            if normalized in marker_tokens:
                cut = idx + 1
            elif normalized in {"apt", "unit"} and idx + 1 < len(tokens):
                cut = idx + 2

        if cut and 0 < cut < len(tokens):
            line1_part = " ".join(tokens[:cut]).strip(", ")
            city_part = " ".join(tokens[cut:]).strip(", ")
            if line1_part and city_part:
                return (line1_part, city_part)

        if len(tokens) >= 3:
            city_part = " ".join(tokens[-2:])
            line1_part = " ".join(tokens[:-2]).strip(", ")
            if line1_part:
                return (line1_part, city_part)
        return (" ".join(tokens[:-1]).strip(", "), tokens[-1].strip(", "))

    text = trim(raw_line1) or ""
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) < 3:
        # Pattern: "<line1 and city>, <state>" with no ZIP.
        trailing_state = re.match(r"^(.*?),\s*([A-Za-z][A-Za-z\s]{1,})\s*$", text)
        if not trailing_state:
            return (None, None, None, None, None)
        pre = trailing_state.group(1).strip()
        state = _norm_state(trailing_state.group(2))
        if not state:
            return (None, None, None, None, None)
        parsed_line1, parsed_city = _split_line1_city(pre)
        if not parsed_line1 or not parsed_city:
            return (None, None, None, None, None)

        country = "US" if re.search(r"\b(US|USA|UNITED STATES)\b", text.upper()) else None
        postal_match = re.search(r"\b(\d{4,5}(?:-\d{4})?)\b", text)
        postal = (
            normalize_postal_code_for_storage(postal_match.group(1), country)
            if postal_match
            else None
        )
        country = normalize_country_code(country)
        return (parsed_line1, parsed_city, state, postal, country)

    parsed_line1 = parts[0]
    parsed_city = parts[1] if len(parts) > 1 else None
    tail = " ".join(parts[2:])
    tail_up = tail.upper()

    state_match = re.search(r"\b([A-Z]{2}|[A-Z]+(?:\s+[A-Z]+)?)\b", tail_up)
    state = _norm_state(state_match.group(1) if state_match else None)

    country = None
    if re.search(r"\b(US|USA|UNITED STATES)\b", tail_up):
        country = "US"
    country = normalize_country_code(country)

    postal_match = re.search(r"\b(\d{4,5}(?:-\d{4})?)\b", tail_up)
    postal = (
        normalize_postal_code_for_storage(postal_match.group(1), country)
        if postal_match
        else None
    )

    if not parsed_city or not state:
        return (None, None, None, None, None)

    return (parsed_line1, parsed_city, state, postal, country)


def repair_overloaded_line1_addresses(
    input_path: Path,
    output_path: Path,
    summary_path: Path | None = None,
) -> AddressRepairSummary:
    """Build address-repair suggestions for rows with overloaded line1 values."""
    with input_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            raise ValueError(f"CSV has no header row: {input_path}")
        rows = list(reader)

    output_fields = fieldnames + [
        "line1_repaired",
        "city_repaired",
        "state_repaired",
        "postal_code_repaired",
        "country_code_repaired",
        "address_repair_status",
    ]

    rows_attempted = 0
    rows_repaired = 0
    warnings: list[str] = []
    status_counts: Counter[str] = Counter()

    out_rows: list[dict[str, str]] = []
    for idx, row in enumerate(rows, start=2):
        line1 = trim(row.get("line1")) or ""
        city = trim(row.get("city")) or ""
        state = trim(row.get("state")) or ""
        country = normalize_country_code(trim(row.get("country_code"))) or ""
        postal = normalize_postal_code_for_storage(trim(row.get("postal_code")), country)

        comment = (row.get("humanComment") or "").strip().lower()
        contains_errata = _boolish(row.get("containsErrata", ""))
        has_overload_hint = "overloaded" in comment or line1.count(",") >= 2
        missing_structured = not city or not state or not postal

        status = "not_attempted"
        repaired_line1 = line1
        repaired_city = city
        repaired_state = state
        repaired_postal = postal or ""
        repaired_country = country

        if not line1:
            status = "not_attempted"
        elif not missing_structured:
            status = "not_needed"
        elif contains_errata or has_overload_hint:
            rows_attempted += 1
            parsed = _parse_overloaded_line1(line1)
            if parsed[0]:
                parsed_line1, parsed_city, parsed_state, parsed_postal, parsed_country = parsed
                repaired_line1 = parsed_line1 or repaired_line1
                repaired_city = repaired_city or (parsed_city or "")
                repaired_state = repaired_state or (parsed_state or "")
                repaired_postal = repaired_postal or (parsed_postal or "")
                repaired_country = repaired_country or (parsed_country or "")
                status = "repaired"
                rows_repaired += 1
            else:
                status = "parse_failed"
                warnings.append(f"row {idx}: unable to parse overloaded line1: {line1}")
        else:
            status = "not_attempted"

        status_counts[status] += 1

        out = dict(row)
        out["line1_repaired"] = repaired_line1
        out["city_repaired"] = repaired_city
        out["state_repaired"] = repaired_state
        out["postal_code_repaired"] = repaired_postal
        out["country_code_repaired"] = repaired_country
        out["address_repair_status"] = status
        out_rows.append(out)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(out_rows)

    summary = AddressRepairSummary(
        rows_read=len(out_rows),
        rows_attempted=rows_attempted,
        rows_repaired=rows_repaired,
        repair_status_counts=dict(status_counts),
        warnings=warnings,
    )

    if summary_path:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary.to_dict(), indent=2), encoding="utf-8")

    return summary
