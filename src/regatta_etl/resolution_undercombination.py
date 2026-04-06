"""regatta_etl.resolution_undercombination

FOR-227 — Undercombination suspect pair detection.

Builds a report of (candidate_A, candidate_B) pairs that look like the same
real-world person split across two candidate_participant records.

Signal sources (any two candidates sharing one of these signals are suspect):
  - Shared last name + first-name nickname expansion match
  - Shared normalized email (exact)
  - Shared normalized phone number
  - Shared postal_code + city address
  - Shared boat (candidate linked to the same canonical_yacht through
    registrations or operational yacht ownership)

Pairs are scored 0.0–1.0 by the sum of signal weights defined in SIGNAL_WEIGHTS.
Pairs with score >= SUSPECT_THRESHOLD are written to the output CSV.

Nickname expansion:
  NICKNAME_MAP maps each canonical form to a set of known short forms (and vice
  versa via inversion).  Both directions are checked so Jeff↔Jeffrey and
  Jeffrey↔Jeff both match.

CLI: --mode undercombination_suspect_report --output-path <csv>
"""

from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

# ---------------------------------------------------------------------------
# Nickname expansion table
# ---------------------------------------------------------------------------

# Maps canonical (long) form → set of short forms.
# The inversion (short → long) is computed at module load.
_CANONICAL_TO_NICKNAMES: dict[str, frozenset[str]] = {
    "robert":      frozenset({"bob", "rob", "bobby", "robbie"}),
    "william":     frozenset({"bill", "will", "billy", "willy", "liam"}),
    "james":       frozenset({"jim", "jimmy", "jamie"}),
    "john":        frozenset({"jack", "johnny", "jon"}),
    "richard":     frozenset({"rick", "rich", "dick", "richie"}),
    "charles":     frozenset({"charlie", "chuck", "chas"}),
    "michael":     frozenset({"mike", "mick", "mikey"}),
    "thomas":      frozenset({"tom", "tommy"}),
    "christopher": frozenset({"chris", "kris", "topher"}),
    "david":       frozenset({"dave", "davie"}),
    "matthew":     frozenset({"matt", "matty"}),
    "joseph":      frozenset({"joe", "joey"}),
    "daniel":      frozenset({"dan", "danny"}),
    "andrew":      frozenset({"andy", "drew"}),
    "edward":      frozenset({"ed", "eddie", "ned", "ted", "teddy"}),
    "george":      frozenset({"georgie"}),
    "henry":       frozenset({"harry", "hank"}),
    "peter":       frozenset({"pete"}),
    "alexander":   frozenset({"alex", "al", "lex", "xander"}),
    "nathaniel":   frozenset({"nate", "nat", "nathan"}),
    "nathan":      frozenset({"nate", "nat"}),
    "jeffrey":     frozenset({"jeff", "geoff", "geoffrey"}),
    "geoffrey":    frozenset({"geoff", "jeff", "jeffrey"}),
    "samuel":      frozenset({"sam", "sammy"}),
    "benjamin":    frozenset({"ben", "benny", "benji"}),
    "anthony":     frozenset({"tony", "ant"}),
    "stephen":     frozenset({"steve", "stevie"}),
    "steven":      frozenset({"steve", "stevie", "stephen"}),
    "timothy":     frozenset({"tim", "timmy"}),
    "donald":      frozenset({"don", "donnie"}),
    "kenneth":     frozenset({"ken", "kenny"}),
    "lawrence":    frozenset({"larry", "laurence"}),
    "laurence":    frozenset({"larry", "lawrence"}),
    "raymond":     frozenset({"ray"}),
    "gregory":     frozenset({"greg"}),
    "gerald":      frozenset({"gerry", "jerry"}),
    "jerome":      frozenset({"jerry"}),
    "frederick":   frozenset({"fred", "freddy"}),
    "francis":     frozenset({"frank", "fran"}),
    "jonathan":    frozenset({"jon", "jonny"}),
    "nicholas":    frozenset({"nick", "nicky"}),
    "patrick":     frozenset({"pat", "paddy"}),
    "walter":      frozenset({"walt", "wally"}),
    "albert":      frozenset({"al", "bert"}),
    "arthur":      frozenset({"art"}),
    "margaret":    frozenset({"peggy", "maggie", "meg", "marge", "margie"}),
    "elizabeth":   frozenset({"liz", "beth", "betty", "eliza", "ellie", "lisa"}),
    "katherine":   frozenset({"kate", "kathy", "katy", "kat", "katie"}),
    "catherine":   frozenset({"cathy", "cat", "kate", "katie"}),
    "patricia":    frozenset({"pat", "patty", "trish", "tricia"}),
    "barbara":     frozenset({"barb", "barbie"}),
    "dorothy":     frozenset({"dot", "dottie"}),
    "virginia":    frozenset({"ginny", "ginger"}),
    "jennifer":    frozenset({"jen", "jenny", "jenn"}),
    "jessica":     frozenset({"jess", "jessie"}),
    "stephanie":   frozenset({"steph"}),
    "deborah":     frozenset({"deb", "debbie"}),
    "victoria":    frozenset({"vicki", "vicky", "tori"}),
    "samantha":    frozenset({"sam", "sammy"}),
    "jacqueline":  frozenset({"jackie", "jacque"}),
    "suzanne":     frozenset({"sue", "suzy"}),
    "susan":       frozenset({"sue", "suzy", "susie"}),
    "rebecca":     frozenset({"becca", "becky"}),
    "carolyn":     frozenset({"carol", "carrie"}),
    "joanne":      frozenset({"jo", "joann"}),
}

# Build full expansion: any form → set of all equivalent forms (including itself)
_NICKNAME_GROUPS: dict[str, frozenset[str]] = {}
for _canonical, _nicks in _CANONICAL_TO_NICKNAMES.items():
    _all = frozenset({_canonical}) | _nicks
    for _form in _all:
        existing = _NICKNAME_GROUPS.get(_form, frozenset())
        _NICKNAME_GROUPS[_form] = existing | _all


def _fname_equivalent(a: str, b: str) -> bool:
    """Return True if first names a and b are the same or known nickname equivalents."""
    if not a or not b:
        return False
    a_lo = a.strip().lower()
    b_lo = b.strip().lower()
    if a_lo == b_lo:
        return True
    group_a = _NICKNAME_GROUPS.get(a_lo)
    group_b = _NICKNAME_GROUPS.get(b_lo)
    if group_a and b_lo in group_a:
        return True
    if group_b and a_lo in group_b:
        return True
    return False


# ---------------------------------------------------------------------------
# Signal weights
# ---------------------------------------------------------------------------

SIGNAL_WEIGHTS: dict[str, float] = {
    "shared_email":         0.60,
    "shared_phone":         0.50,
    "shared_address":       0.25,
    "shared_boat":          0.20,
    "lname_fname_nickname": 0.35,
}

SUSPECT_THRESHOLD: float = 0.50


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class UndercombinationCounters:
    candidates_scanned: int = 0
    pairs_evaluated: int = 0
    suspects_found: int = 0
    db_errors: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates_scanned":  self.candidates_scanned,
            "pairs_evaluated":     self.pairs_evaluated,
            "suspects_found":      self.suspects_found,
            "db_errors":           self.db_errors,
            "warnings":            self.warnings[:50],
        }


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

def _split_fname_lname(normalized_name: str | None) -> tuple[str, str]:
    """Best-effort split of 'firstname lastname' → (fname, lname)."""
    if not normalized_name:
        return ("", "")
    parts = normalized_name.strip().split()
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], parts[-1])


def run_undercombination_report(
    conn: psycopg.Connection,
    output_path: Path,
    dry_run: bool = False,
) -> UndercombinationCounters:
    """Emit a CSV of undercombination suspect pairs.

    Each output row contains:
        candidate_id_a, candidate_id_b,
        name_a, name_b,
        signals (pipe-separated list),
        score,
        recommended_action

    The output is intended for human review only — no DB writes are made.

    Args:
        conn:        Open psycopg connection.
        output_path: Path to write the suspect CSV.
        dry_run:     Ignored (no DB writes; parameter kept for CLI consistency).
    """
    ctrs = UndercombinationCounters()

    # ------------------------------------------------------------------ #
    # 1. Collect candidates with their identity signals                   #
    # ------------------------------------------------------------------ #

    candidates = conn.execute(
        """
        SELECT id::text, normalized_name, best_email, best_phone
        FROM candidate_participant
        WHERE resolution_state != 'reject'
        ORDER BY normalized_name NULLS LAST, id
        """
    ).fetchall()
    ctrs.candidates_scanned = len(candidates)

    # Index by signal for O(1) lookup
    email_index: dict[str, list[str]]   = {}  # normalized_email → [candidate_ids]
    phone_index: dict[str, list[str]]   = {}  # normalized_phone → [candidate_ids]
    lname_index: dict[str, list[tuple[str, str, str]]] = {}  # lname → [(id, fname, norm_name)]

    for row in candidates:
        cid, norm_name, email, phone = str(row[0]), row[1], row[2], row[3]
        if email:
            email_index.setdefault(email.lower(), []).append(cid)
        if phone:
            phone_index.setdefault(phone, []).append(cid)
        _, lname = _split_fname_lname(norm_name)
        if lname:
            fname, _ = _split_fname_lname(norm_name)
            lname_index.setdefault(lname.lower(), []).append((cid, fname.lower(), norm_name or ""))

    # ------------------------------------------------------------------ #
    # 2. Shared address index: (city_slug, postal_code) → [candidate_ids] #
    # ------------------------------------------------------------------ #

    addr_rows = conn.execute(
        """
        SELECT candidate_participant_id::text,
               lower(trim(city)),
               postal_code
        FROM candidate_participant_address
        WHERE city IS NOT NULL OR postal_code IS NOT NULL
        """
    ).fetchall()

    addr_index: dict[tuple[str, str], list[str]] = {}
    for cid, city, postal in addr_rows:
        key = (city or "", postal or "")
        if key != ("", ""):
            addr_index.setdefault(key, []).append(cid)

    # ------------------------------------------------------------------ #
    # 3. Shared boat index: canonical_yacht_id → [candidate_ids]          #
    # ------------------------------------------------------------------ #

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
            WHERE cp.resolution_state != 'reject'

            UNION

            SELECT DISTINCT
                csl_p.candidate_entity_id::text AS candidate_id,
                ccl_y.canonical_entity_id::text AS canonical_yacht_id
            FROM candidate_source_link csl_p
            JOIN candidate_participant cp
                 ON cp.id = csl_p.candidate_entity_id
                AND cp.resolution_state != 'reject'
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
        SELECT candidate_id, canonical_yacht_id
        FROM participant_yacht_links
        """
    ).fetchall()

    boat_index: dict[str, list[str]] = {}
    for cid, yacht_can_id in boat_rows:
        boat_index.setdefault(yacht_can_id, []).append(cid)

    # ------------------------------------------------------------------ #
    # 4. Enumerate suspect pairs                                          #
    # ------------------------------------------------------------------ #

    suspect_pairs: dict[tuple[str, str], dict[str, Any]] = {}

    def _record(a: str, b: str, signal: str) -> None:
        key = (min(a, b), max(a, b))
        entry = suspect_pairs.setdefault(key, {"signals": [], "score": 0.0})
        if signal not in entry["signals"]:
            entry["signals"].append(signal)
            entry["score"] += SIGNAL_WEIGHTS.get(signal, 0.0)

    # Email pairs
    for email, ids in email_index.items():
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                _record(ids[i], ids[j], "shared_email")

    # Phone pairs
    for phone, ids in phone_index.items():
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                _record(ids[i], ids[j], "shared_phone")

    # Address pairs
    for (city, postal), ids in addr_index.items():
        deduped = list(dict.fromkeys(ids))  # preserve first occurrence, drop dups
        for i in range(len(deduped)):
            for j in range(i + 1, len(deduped)):
                _record(deduped[i], deduped[j], "shared_address")

    # Boat pairs
    for yacht_id, ids in boat_index.items():
        deduped = list(dict.fromkeys(ids))
        for i in range(len(deduped)):
            for j in range(i + 1, len(deduped)):
                _record(deduped[i], deduped[j], "shared_boat")

    # Nickname/same-lname pairs
    for lname, entries in lname_index.items():
        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                id_i, fname_i, _ = entries[i]
                id_j, fname_j, _ = entries[j]
                if _fname_equivalent(fname_i, fname_j):
                    _record(id_i, id_j, "lname_fname_nickname")

    ctrs.pairs_evaluated = len(suspect_pairs)

    # ------------------------------------------------------------------ #
    # 5. Filter to threshold + enrich with names                         #
    # ------------------------------------------------------------------ #

    above_threshold = {
        k: v for k, v in suspect_pairs.items() if v["score"] >= SUSPECT_THRESHOLD
    }
    ctrs.suspects_found = len(above_threshold)

    # Bulk-fetch names for all candidate IDs in suspect pairs
    all_ids = {cid for pair in above_threshold for cid in pair}
    name_map: dict[str, str] = {}
    if all_ids:
        id_list = list(all_ids)
        placeholders = ", ".join(["%s"] * len(id_list))
        name_rows = conn.execute(
            f"""
            SELECT id::text, COALESCE(normalized_name, best_email, 'UNKNOWN')
            FROM candidate_participant
            WHERE id IN ({placeholders})
            """,
            id_list,
        ).fetchall()
        name_map = {str(r[0]): r[1] for r in name_rows}

    # ------------------------------------------------------------------ #
    # 6. Write output CSV                                                 #
    # ------------------------------------------------------------------ #

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "candidate_id_a", "candidate_id_b",
                "name_a", "name_b",
                "signals", "score",
                "recommended_action",
            ],
        )
        writer.writeheader()

        for (cid_a, cid_b), info in sorted(
            above_threshold.items(), key=lambda x: -x[1]["score"]
        ):
            writer.writerow({
                "candidate_id_a":      cid_a,
                "candidate_id_b":      cid_b,
                "name_a":              name_map.get(cid_a, ""),
                "name_b":              name_map.get(cid_b, ""),
                "signals":             "|".join(info["signals"]),
                "score":               f"{info['score']:.2f}",
                "recommended_action":  "merge" if info["score"] >= 0.80 else "review",
            })

    return ctrs
