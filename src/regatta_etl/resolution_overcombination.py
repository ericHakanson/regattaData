"""regatta_etl.resolution_overcombination

FOR-228 — Overcombination (false-merge) suspect detection.

Builds a report of candidate_participant records that look like multiple real
people merged into one.  Heuristics are intentionally conservative — high
source-record count alone is common in active sailors; the flags trigger only
when multiple independent signals converge.

Heuristic flags (each adds to the candidate's overcombination score):
  - multi_email_domain : 2+ distinct email domains on candidate contacts
  - high_boat_count    : 4+ distinct canonical_yacht records linked via
                         registrations or operational yacht ownership
  - high_source_count  : 235+ source links (chosen as the 95th-percentile cutoff
                         described in FOR-228)
  - space_in_fname     : normalized_name first token contains a space (concatenated
                         first names — e.g. "John Jane Smith")
  - household_merge    : candidate appears in BHYC household evidence,
                         indicating an intentional household grouping
                         (this is a carve-out, NOT a flag; sets score to 0)

Candidates with flag_count >= FLAG_THRESHOLD are written to the output CSV.

CLI: --mode overcombination_suspect_report --output-path <csv>
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

# ---------------------------------------------------------------------------
# Thresholds (tunable)
# ---------------------------------------------------------------------------

HIGH_BOAT_COUNT: int = 4
HIGH_SOURCE_COUNT: int = 235
FLAG_THRESHOLD: int = 2   # candidate must trigger at least 2 flags to be a suspect


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class OvercombinationCounters:
    candidates_scanned: int = 0
    suspects_found: int = 0
    household_carve_outs: int = 0
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates_scanned":   self.candidates_scanned,
            "suspects_found":       self.suspects_found,
            "household_carve_outs": self.household_carve_outs,
            "db_errors":            self.db_errors,
            "warnings":             self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

def run_overcombination_report(
    conn: psycopg.Connection,
    output_path: Path,
    dry_run: bool = False,
) -> OvercombinationCounters:
    """Emit a CSV of overcombination suspect candidates.

    Each output row contains:
        candidate_id, name, source_record_count, distinct_boat_count,
        distinct_email_domain_count, fname_has_space,
        is_household_merge, flags, flag_count, recommended_action

    The output is for human review only — no DB writes are made.

    Args:
        conn:        Open psycopg connection.
        output_path: Path to write the suspect CSV.
        dry_run:     Ignored (no DB writes).
    """
    ctrs = OvercombinationCounters()

    # ------------------------------------------------------------------ #
    # 1. Source record counts per candidate                               #
    # ------------------------------------------------------------------ #

    source_counts: dict[str, int] = {}
    source_rows = conn.execute(
        """
        SELECT candidate_entity_id::text, COUNT(*) AS cnt
        FROM candidate_source_link
        WHERE candidate_entity_type = 'participant'
        GROUP BY candidate_entity_id
        """
    ).fetchall()
    for cid, cnt in source_rows:
        source_counts[cid] = int(cnt)

    # ------------------------------------------------------------------ #
    # 2. Household-merge candidates (intentional grouping — carve out)    #
    # ------------------------------------------------------------------ #

    household_cids: set[str] = set()
    hh_rows = conn.execute(
        """
        SELECT DISTINCT candidate_participant_id::text
        FROM bhyc_household_candidate_evidence
        """
    ).fetchall()
    for (cid,) in hh_rows:
        household_cids.add(cid)

    # ------------------------------------------------------------------ #
    # 3. Distinct email domains per candidate                             #
    # ------------------------------------------------------------------ #

    domain_counts: dict[str, int] = {}
    email_rows = conn.execute(
        """
        SELECT candidate_participant_id::text,
               COUNT(DISTINCT split_part(normalized_value, '@', 2)) AS domain_cnt
        FROM candidate_participant_contact
        WHERE contact_type = 'email'
          AND normalized_value IS NOT NULL
          AND normalized_value LIKE '%@%'
        GROUP BY candidate_participant_id
        """
    ).fetchall()
    for cid, dcnt in email_rows:
        domain_counts[cid] = int(dcnt)

    # ------------------------------------------------------------------ #
    # 4. Distinct canonical_yacht count per candidate                     #
    # ------------------------------------------------------------------ #

    boat_counts: dict[str, int] = {}
    boat_rows = conn.execute(
        """
        WITH participant_yacht_links AS (
            SELECT DISTINCT
                cp.id::text AS candidate_id,
                ccl_y.canonical_entity_id::text AS canonical_yacht_id
            FROM candidate_participant cp
            JOIN candidate_participant_role_assignment r
                 ON r.candidate_participant_id = cp.id
                AND r.role IN ('owner', 'co_owner', 'skipper')
            JOIN candidate_registration cr
                 ON cr.id = r.candidate_registration_id
                AND cr.candidate_yacht_id IS NOT NULL
            JOIN candidate_canonical_link ccl_y
                 ON ccl_y.candidate_entity_type = 'yacht'
                AND ccl_y.candidate_entity_id = cr.candidate_yacht_id

            UNION

            SELECT DISTINCT
                csl_p.candidate_entity_id::text AS candidate_id,
                ccl_y.canonical_entity_id::text AS canonical_yacht_id
            FROM candidate_source_link csl_p
            JOIN yacht_ownership yo
                 ON yo.participant_id::text = csl_p.source_row_pk
            JOIN candidate_source_link csl_y
                 ON csl_y.candidate_entity_type = 'yacht'
                AND csl_y.source_table_name = 'yacht'
                AND csl_y.source_row_pk = yo.yacht_id::text
            JOIN candidate_canonical_link ccl_y
                 ON ccl_y.candidate_entity_type = 'yacht'
                AND ccl_y.candidate_entity_id = csl_y.candidate_entity_id
            WHERE csl_p.candidate_entity_type = 'participant'
              AND csl_p.source_table_name = 'participant'
        )
        SELECT candidate_id, COUNT(DISTINCT canonical_yacht_id) AS boat_cnt
        FROM participant_yacht_links
        GROUP BY candidate_id
        """
    ).fetchall()
    for cid, bcnt in boat_rows:
        boat_counts[cid] = int(bcnt)

    # ------------------------------------------------------------------ #
    # 5. Fetch all candidates and evaluate flags                          #
    # ------------------------------------------------------------------ #

    rows = conn.execute(
        """
        SELECT id::text, normalized_name, best_email, resolution_state
        FROM candidate_participant
        WHERE resolution_state != 'reject'
        ORDER BY id
        """
    ).fetchall()
    ctrs.candidates_scanned = len(rows)

    suspects: list[dict[str, Any]] = []

    for row in rows:
        cid      = str(row[0])
        norm_name = row[1] or ""

        src_count   = source_counts.get(cid, 0)
        boat_count  = boat_counts.get(cid, 0)
        dom_count   = domain_counts.get(cid, 0)
        is_hh_merge = cid in household_cids

        # fname space check: first token of normalized_name contains a space
        # after splitting on space — implies concatenated first names.
        # normalized_name is already collapsed, so "John Jane Smith" would
        # have fname="john" with no space; we look for display names with
        # 3+ tokens where position 0 has no obvious single-name meaning.
        # Simpler heuristic: normalized_name has 3+ space-separated tokens
        # AND the middle token looks like a first name (not an initial).
        parts = norm_name.strip().split()
        fname_has_space = (
            len(parts) >= 3
            and len(parts[0]) > 2   # not an initial like "j"
            and len(parts[1]) > 2   # middle token also a word, not initial
        )

        # ---- Flag evaluation ----
        flags: list[str] = []

        if dom_count >= 2:
            flags.append("multi_email_domain")
        if boat_count >= HIGH_BOAT_COUNT:
            flags.append("high_boat_count")
        if src_count >= HIGH_SOURCE_COUNT:
            flags.append("high_source_count")
        if fname_has_space:
            flags.append("space_in_fname")

        # Household merge: intentional grouping — carve out regardless of other flags
        if is_hh_merge:
            ctrs.household_carve_outs += 1
            continue

        flag_count = len(flags)
        if flag_count < FLAG_THRESHOLD:
            continue

        suspects.append({
            "candidate_id":               cid,
            "name":                        norm_name,
            "source_record_count":         src_count,
            "distinct_boat_count":         boat_count,
            "distinct_email_domain_count": dom_count,
            "fname_has_space":             fname_has_space,
            "is_household_merge":          False,
            "flags":                       "|".join(flags),
            "flag_count":                  flag_count,
            "recommended_action":          "investigate" if flag_count == 2 else "split_candidate",
        })

    ctrs.suspects_found = len(suspects)

    # ------------------------------------------------------------------ #
    # 6. Write output CSV                                                 #
    # ------------------------------------------------------------------ #

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "candidate_id", "name",
                "source_record_count", "distinct_boat_count",
                "distinct_email_domain_count", "fname_has_space",
                "is_household_merge", "flags", "flag_count",
                "recommended_action",
            ],
        )
        writer.writeheader()
        # Sort by flag_count DESC, then source_record_count DESC
        for s in sorted(suspects, key=lambda x: (-x["flag_count"], -x["source_record_count"])):
            writer.writerow(s)

    return ctrs
