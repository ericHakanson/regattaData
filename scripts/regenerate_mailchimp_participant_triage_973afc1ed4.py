#!/usr/bin/env python3
"""Regenerate the Mailchimp participant triage CSV with canonical IDs."""

from __future__ import annotations

import csv
import os
import sys
from collections import defaultdict
from pathlib import Path
import re

import psycopg


REPO_ROOT = Path(__file__).resolve().parent.parent
REVIEW_CSV = REPO_ROOT / "artifacts/qa/mailchimp_address_review_973afc1ed4.csv"
TRIAGE_CSV = REPO_ROOT / "artifacts/qa/mailchimp_participant_triage_973afc1ed4.csv"
FIDELITY_TXT = REPO_ROOT / "artifacts/qa/mailchimp_address_fidelity_973afc1ed4.txt"


def norm_name(value: str) -> str:
    return " ".join((value or "").lower().replace(",", " ").split())


def last_name(value: str) -> str:
    parts = norm_name(value).split()
    return parts[-1] if parts else ""


def load_fidelity_outcomes(path: Path) -> dict[str, int]:
    """Parse outcome_bucket counts from the fidelity report text output."""
    if not path.exists():
        return {}

    outcomes: dict[str, int] = {}
    # Example lines:
    # canonical_address_present           |  259
    # review_queue:ambiguous_email_match |    1
    line_re = re.compile(r"^\s*([a-z_:\-]+)\s+\|\s+(\d+)\s*$")
    with path.open(encoding="utf-8") as handle:
        for raw in handle:
            m = line_re.match(raw.rstrip("\n"))
            if not m:
                continue
            key = m.group(1)
            if key == "outcome_bucket":
                continue
            outcomes[key] = int(m.group(2))
    return outcomes


def main() -> int:
    db_dsn = os.environ.get("DB_DSN")

    if not REVIEW_CSV.exists():
        print(f"Missing review CSV: {REVIEW_CSV}", file=sys.stderr)
        return 1

    rows = list(csv.DictReader(REVIEW_CSV.open(newline="", encoding="utf-8")))
    by_email: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_email[row["email"]].append(row)

    participant_ids = sorted(
        {row["target_participant_id"] for row in rows if row.get("target_participant_id")}
    )
    canonical_by_participant: dict[str, list[str]] = {}

    if participant_ids and db_dsn:
        with psycopg.connect(db_dsn) as conn:
            matched = conn.execute(
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
        for participant_id, canonical_id in matched:
            if canonical_id not in grouped[participant_id]:
                grouped[participant_id].append(canonical_id)
        canonical_by_participant = dict(grouped)
    elif participant_ids and not db_dsn:
        print("DB_DSN is not set; skipping canonical ID enrichment.", file=sys.stderr)

    fieldnames = [
        "email",
        "review_reason",
        "source_name",
        "source_phone",
        "source_address",
        "match_count",
        "target_participant_ids",
        "canonical_participant_ids",
        "candidate_names",
        "candidate_phones",
        "candidate_addresses",
        "recommended_action",
        "recommended_notes",
        "reviewer_decision",
        "reviewer_notes",
    ]

    TRIAGE_CSV.parent.mkdir(parents=True, exist_ok=True)
    with TRIAGE_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()

        for email in sorted(by_email):
            group = by_email[email]
            first = group[0]
            reason = first["reject_reason"]
            source_name = " ".join(
                part for part in [first["source_first_name"], first["source_last_name"]] if part
            ).strip()
            target_participant_ids = [
                row["target_participant_id"] for row in group if row["target_participant_id"]
            ]
            canonical_ids: list[str] = []
            for participant_id in target_participant_ids:
                for canonical_id in canonical_by_participant.get(participant_id, []):
                    if canonical_id not in canonical_ids:
                        canonical_ids.append(canonical_id)

            candidate_names = [row["target_full_name"] for row in group if row["target_full_name"]]
            candidate_phones = sorted({row["target_phone"] for row in group if row["target_phone"]})
            candidate_addresses = sorted(
                {row["target_address"] for row in group if row["target_address"]}
            )
            last_names = sorted({last_name(name) for name in candidate_names if name})

            action = "needs_human_review"
            notes = ""
            if reason == "email_phone_mismatch":
                action = "leave_unresolved"
                notes = (
                    "Existing participant has same email but conflicting phone; "
                    "likely shared/family email or two real people."
                )
            elif reason == "email_address_mismatch":
                action = "manual_address_patch"
                notes = (
                    "Looks like same person with malformed stored address/postal; "
                    "patch target address data before rerun."
                )
            elif reason == "email_name_mismatch":
                if email.endswith("@wix.com"):
                    action = "ignore_demo_or_leave_unresolved"
                    notes = "Demo/test-style row; low priority unless demo data matters."
                else:
                    notes = "Email is unique but names differ; likely bad source or bad target identity."
            elif reason == "ambiguous_email_match":
                if len(candidate_phones) == 1 and len(last_names) == 1:
                    action = "likely_same_person_duplicate_participants"
                    notes = (
                        "All candidate rows share one phone and one surname; likely duplicate "
                        "source participants (e.g. First Last vs Last, First)."
                    )
                elif len(candidate_phones) == 1:
                    action = "likely_same_household_or_bad_duplicate"
                    notes = (
                        "Candidates share one phone but names differ; review carefully before merge."
                    )
                else:
                    notes = (
                        "Multiple participants share this email with conflicting corroboration; "
                        "do not auto-merge."
                    )

            writer.writerow(
                {
                    "email": email,
                    "review_reason": reason,
                    "source_name": source_name,
                    "source_phone": first["source_phone"],
                    "source_address": first["source_address"],
                    "match_count": first["match_count"] or len(group),
                    "target_participant_ids": " | ".join(target_participant_ids),
                    "canonical_participant_ids": " | ".join(canonical_ids),
                    "candidate_names": " | ".join(candidate_names),
                    "candidate_phones": " | ".join(candidate_phones),
                    "candidate_addresses": " | ".join(candidate_addresses),
                    "recommended_action": action,
                    "recommended_notes": notes,
                    "reviewer_decision": "",
                    "reviewer_notes": "",
                }
            )

    print(TRIAGE_CSV)
    print(f"triage_emails_from_review_csv {len(by_email)}")
    if not REVIEW_CSV.samefile(TRIAGE_CSV):
        print(f"review_snapshot {REVIEW_CSV}")

    fidelity_outcomes = load_fidelity_outcomes(FIDELITY_TXT)
    if fidelity_outcomes:
        print(f"fidelity_snapshot {FIDELITY_TXT}")
        for key in sorted(fidelity_outcomes):
            if key == "canonical_address_present" or key.startswith("review_queue:"):
                print(f"{key} {fidelity_outcomes[key]}")
        review_queue_total = sum(
            value for key, value in fidelity_outcomes.items() if key.startswith("review_queue:")
        )
        print(f"fidelity_review_queue_rows {review_queue_total}")
    else:
        print("fidelity_snapshot_missing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
