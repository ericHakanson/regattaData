#!/bin/zsh

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

cd "$REPO_ROOT"

if [[ -z "${DB_DSN:-}" ]]; then
  echo "DB_DSN is not set."
  exit 1
fi

if [[ ! -f ".venv/bin/activate" ]]; then
  echo "Missing virtualenv at .venv/bin/activate"
  exit 1
fi

source .venv/bin/activate

mkdir -p artifacts/qa artifacts/rejects artifacts/logs

present_count=$(psql "$DB_DSN" -Atqc "
SELECT
  (to_regclass('public.manual_participant_patch') IS NOT NULL)::int +
  (to_regclass('public.manual_participant_address_patch') IS NOT NULL)::int +
  (to_regclass('public.manual_yacht_patch') IS NOT NULL)::int +
  (to_regclass('public.manual_yacht_ownership_patch') IS NOT NULL)::int +
  (to_regclass('public.manual_club_membership_patch') IS NOT NULL)::int;
")

if [[ "$present_count" == "0" ]]; then
  echo "Applying migrations/0024_manual_curation_tables.sql"
  psql "$DB_DSN" -v ON_ERROR_STOP=1 -f migrations/0024_manual_curation_tables.sql
elif [[ "$present_count" == "5" ]]; then
  echo "0024 manual-curation tables already exist; skipping migration."
else
  echo "Partial 0024 schema state detected (${present_count}/5 tables present). Aborting."
  exit 1
fi

echo "Running participant source-to-candidate"
regatta-import --mode resolution_source_to_candidate --db-dsn "$DB_DSN" --entity-type participant

echo "Running participant scoring"
regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type participant

echo "Running participant promotion"
regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type participant

echo "Writing fidelity report"
psql "$DB_DSN" -f docs/qa/sql/mailchimp_address_fidelity.sql \
  | tee artifacts/qa/mailchimp_address_fidelity_973afc1ed4.txt

echo "Regenerating review CSV"
python - <<'PY'
import csv
import os

import psycopg

infile = "artifacts/rejects/mailchimp_audience_973afc1ed4_rejects.csv"
outfile = "artifacts/qa/mailchimp_address_review_973afc1ed4.csv"
interesting = {
    "email_address_mismatch",
    "ambiguous_email_match",
    "email_phone_mismatch",
    "email_name_mismatch",
}

with open(infile, newline="", encoding="utf-8-sig") as f:
    rows = [row for row in csv.DictReader(f) if row["_reject_reason"] in interesting]

fieldnames = [
    "reject_reason",
    "email",
    "source_first_name",
    "source_last_name",
    "source_phone",
    "source_address",
    "match_count",
    "target_participant_id",
    "target_full_name",
    "target_phone",
    "target_address",
]

with psycopg.connect(os.environ["DB_DSN"]) as conn, open(outfile, "w", newline="", encoding="utf-8") as out:
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()

    for row in rows:
        email = (row.get("Email Address") or "").strip().lower()
        matches = conn.execute(
            """
            SELECT
                p.id::text,
                p.full_name,
                (
                    SELECT pa.address_raw
                    FROM participant_address pa
                    WHERE pa.participant_id = p.id
                    ORDER BY pa.is_primary DESC, pa.created_at ASC
                    LIMIT 1
                ) AS target_address,
                (
                    SELECT pc.contact_value_normalized
                    FROM participant_contact_point pc
                    WHERE pc.participant_id = p.id
                      AND pc.contact_type = 'phone'
                    ORDER BY pc.is_primary DESC, pc.created_at ASC
                    LIMIT 1
                ) AS target_phone
            FROM participant_contact_point ep
            JOIN participant p ON p.id = ep.participant_id
            WHERE ep.contact_type = 'email'
              AND ep.contact_value_normalized = %s
            ORDER BY p.created_at, p.id
            """,
            (email,),
        ).fetchall()

        if not matches:
            writer.writerow(
                {
                    "reject_reason": row["_reject_reason"],
                    "email": email,
                    "source_first_name": (row.get("First Name") or "").strip(),
                    "source_last_name": (row.get("Last Name") or "").strip(),
                    "source_phone": (row.get("Phone Number") or "").strip(),
                    "source_address": (row.get("Address") or "").strip(),
                    "match_count": 0,
                    "target_participant_id": "",
                    "target_full_name": "",
                    "target_phone": "",
                    "target_address": "",
                }
            )
            continue

        for pid, full_name, target_address, target_phone in matches:
            writer.writerow(
                {
                    "reject_reason": row["_reject_reason"],
                    "email": email,
                    "source_first_name": (row.get("First Name") or "").strip(),
                    "source_last_name": (row.get("Last Name") or "").strip(),
                    "source_phone": (row.get("Phone Number") or "").strip(),
                    "source_address": (row.get("Address") or "").strip(),
                    "match_count": len(matches),
                    "target_participant_id": pid,
                    "target_full_name": full_name or "",
                    "target_phone": target_phone or "",
                    "target_address": target_address or "",
                }
            )

print(outfile)
PY

echo "Regenerating participant triage CSV"
python scripts/regenerate_mailchimp_participant_triage_973afc1ed4.py

echo "Done"
echo "Fidelity report: artifacts/qa/mailchimp_address_fidelity_973afc1ed4.txt"
echo "Review CSV: artifacts/qa/mailchimp_address_review_973afc1ed4.csv"
