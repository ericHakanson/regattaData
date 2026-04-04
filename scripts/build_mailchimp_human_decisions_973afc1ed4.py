#!/usr/bin/env python3
"""Build email-level decision artifacts from the human-reviewed Mailchimp sheet."""

from __future__ import annotations

import csv
import os
from collections import defaultdict
from pathlib import Path

import psycopg


REPO_ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV = REPO_ROOT / (
    "artifacts/qa/Human_reviewmailchimp_address_review_973afc1ed4 - "
    "mailchimp_address_review_973afc1ed4.csv"
)
DECISIONS_CSV = REPO_ROOT / "artifacts/qa/mailchimp_human_decisions_973afc1ed4.csv"
CONFLICTS_CSV = REPO_ROOT / "artifacts/qa/mailchimp_human_decisions_conflicts_973afc1ed4.csv"


def _norm_conclusion(value: str | None) -> str:
    v = (value or "").strip().lower()
    if v == "overcombination":
        return "over combination"
    if v == "delete test user":
        return "delete test account"
    return v


def _pipeline_action(conclusion: str, reject_reason: str, is_conflict: bool) -> str:
    if is_conflict:
        return "manual_resolution_required"
    if conclusion == "looks valid":
        return "no_action"
    if conclusion == "under combination":
        return "queue_under_combination"
    if conclusion == "under combination (keep both addresses)":
        return "queue_under_combination_keep_both_addresses"
    if conclusion == "over combination":
        return "queue_over_combination_split"
    if conclusion == "distinct person":
        return "keep_separate"
    if conclusion == "delete test account":
        return "delete_test_record"
    if conclusion == "possible email typo":
        return "manual_email_correction"
    if not conclusion and reject_reason == "looks valid":
        return "no_action"
    return "manual_resolution_required"


def _load_rows() -> list[dict[str, str]]:
    with INPUT_CSV.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _load_canonical_ids(participant_ids: list[str]) -> dict[str, list[str]]:
    dsn = os.environ.get("DB_DSN")
    if not dsn or not participant_ids:
        return {}

    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT
                csl.source_row_pk::text AS participant_id,
                ccl.canonical_entity_id::text AS canonical_participant_id
            FROM candidate_source_link csl
            JOIN candidate_canonical_link ccl
              ON ccl.candidate_entity_type = 'participant'
             AND ccl.candidate_entity_id = csl.candidate_entity_id
            WHERE csl.candidate_entity_type = 'participant'
              AND csl.source_table_name = 'participant'
              AND csl.source_row_pk = ANY(%s::text[])
            ORDER BY csl.source_row_pk, ccl.canonical_entity_id
            """,
            (participant_ids,),
        ).fetchall()

    grouped: dict[str, list[str]] = defaultdict(list)
    for participant_id, canonical_id in rows:
        if canonical_id not in grouped[participant_id]:
            grouped[participant_id].append(canonical_id)
    return dict(grouped)


def main() -> int:
    if not INPUT_CSV.exists():
        raise SystemExit(f"Missing input CSV: {INPUT_CSV}")

    rows = _load_rows()
    by_email: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_email[(row.get("email") or "").strip().lower()].append(row)

    participant_ids = sorted(
        {
            (row.get("target_participant_id") or "").strip()
            for row in rows
            if (row.get("target_participant_id") or "").strip()
        }
    )
    canonical_by_participant = _load_canonical_ids(participant_ids)

    decisions_fields = [
        "email",
        "review_row_count",
        "reject_reasons",
        "reviewer_conclusions_raw",
        "reviewer_conclusion_normalized",
        "reviewer_notes_merged",
        "reference_participant_ids",
        "target_participant_ids",
        "canonical_participant_ids",
        "source_name",
        "source_phone",
        "source_address",
        "has_conflict",
        "conflict_reason",
        "pipeline_action",
    ]
    conflict_fields = [
        "email",
        "conflict_reason",
        "reviewer_conclusions_raw",
        "reviewer_notes_merged",
        "reference_participant_ids",
        "target_participant_ids",
    ]

    decision_rows: list[dict[str, str]] = []
    conflict_rows: list[dict[str, str]] = []

    for email in sorted(by_email):
        group = by_email[email]
        first = group[0]

        reject_reasons = sorted(
            {(row.get("reject_reason") or "").strip() for row in group if (row.get("reject_reason") or "").strip()}
        )
        conclusions_raw = sorted(
            {
                (row.get("reviewerConclusion") or "").strip()
                for row in group
                if (row.get("reviewerConclusion") or "").strip()
            }
        )
        conclusions_norm = sorted(
            {_norm_conclusion(row.get("reviewerConclusion")) for row in group if _norm_conclusion(row.get("reviewerConclusion"))}
        )
        notes = sorted(
            {
                (row.get("reviewerNotes") or "").strip()
                for row in group
                if (row.get("reviewerNotes") or "").strip()
            }
        )
        ref_ids = sorted(
            {
                (row.get("referenceParticipantId") or "").strip()
                for row in group
                if (row.get("referenceParticipantId") or "").strip()
            }
        )
        target_ids = sorted(
            {
                (row.get("target_participant_id") or "").strip()
                for row in group
                if (row.get("target_participant_id") or "").strip()
            }
        )
        canonical_ids: list[str] = []
        for target_id in target_ids:
            for canonical_id in canonical_by_participant.get(target_id, []):
                if canonical_id not in canonical_ids:
                    canonical_ids.append(canonical_id)

        source_name_parts = [first.get("source_first_name") or "", first.get("source_last_name") or ""]
        source_name = " ".join(part for part in source_name_parts if part).strip()

        conflict_reason = ""
        if len(conclusions_norm) > 1:
            conflict_reason = "mixed_conclusions"

        has_conflict = conflict_reason != ""
        normalized_conclusion = conclusions_norm[0] if len(conclusions_norm) == 1 else ""
        action = _pipeline_action(
            normalized_conclusion,
            reject_reasons[0] if len(reject_reasons) == 1 else "",
            has_conflict,
        )

        row_out = {
            "email": email,
            "review_row_count": str(len(group)),
            "reject_reasons": " | ".join(reject_reasons),
            "reviewer_conclusions_raw": " | ".join(conclusions_raw),
            "reviewer_conclusion_normalized": normalized_conclusion,
            "reviewer_notes_merged": " | ".join(notes),
            "reference_participant_ids": " | ".join(ref_ids),
            "target_participant_ids": " | ".join(target_ids),
            "canonical_participant_ids": " | ".join(canonical_ids),
            "source_name": source_name,
            "source_phone": (first.get("source_phone") or "").strip(),
            "source_address": (first.get("source_address") or "").strip(),
            "has_conflict": "true" if has_conflict else "false",
            "conflict_reason": conflict_reason,
            "pipeline_action": action,
        }
        decision_rows.append(row_out)

        if has_conflict:
            conflict_rows.append(
                {
                    "email": email,
                    "conflict_reason": conflict_reason,
                    "reviewer_conclusions_raw": row_out["reviewer_conclusions_raw"],
                    "reviewer_notes_merged": row_out["reviewer_notes_merged"],
                    "reference_participant_ids": row_out["reference_participant_ids"],
                    "target_participant_ids": row_out["target_participant_ids"],
                }
            )

    DECISIONS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with DECISIONS_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=decisions_fields)
        writer.writeheader()
        writer.writerows(decision_rows)

    with CONFLICTS_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=conflict_fields)
        writer.writeheader()
        writer.writerows(conflict_rows)

    print(DECISIONS_CSV)
    print(f"emails {len(decision_rows)}")
    print(f"conflicts {len(conflict_rows)}")
    print(CONFLICTS_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
