"""Build lifecycle merge/split plans from reviewed direct-mail decisions."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import re
from typing import Iterable


@dataclass
class LifecyclePlanResult:
    merge_rows: list[dict[str, str]]
    split_rows: list[dict[str, str]]
    conflicts: list[dict[str, str]]


def _find_components(edges: Iterable[tuple[str, str]]) -> list[set[str]]:
    graph: dict[str, set[str]] = defaultdict(set)
    for a, b in edges:
        if not a or not b:
            continue
        graph[a].add(b)
        graph[b].add(a)

    visited: set[str] = set()
    components: list[set[str]] = []
    for node in graph:
        if node in visited:
            continue
        stack = [node]
        comp: set[str] = set()
        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            comp.add(cur)
            stack.extend(graph[cur] - visited)
        if comp:
            components.append(comp)
    return components


def _norm_eval(value: str) -> str:
    raw = re.sub(r"[^a-z]+", "", (value or "").strip().lower())
    if raw in {"undercombined", "undercombination"}:
        return "undercombined"
    if raw in {"overcombined", "overcombination"}:
        return "overcombined"
    return ""


def build_lifecycle_plans(
    reviewed_rows: Iterable[dict[str, str]],
    canonical_by_candidate: dict[str, str],
    actor: str,
) -> LifecyclePlanResult:
    merge_rows: list[dict[str, str]] = []
    split_rows: list[dict[str, str]] = []
    conflicts: list[dict[str, str]] = []

    merge_seen: set[tuple[str, str]] = set()

    # Step 1: undercombined -> merge rows
    over_edges_same_canonical: list[tuple[str, str]] = []
    for row in reviewed_rows:
        evaluation = _norm_eval(
            (row.get("candidateRecordEvaluation") or row.get("entityResolution") or "")
        )
        cid = (row.get("candidate_id") or row.get("candidateId") or "").strip()
        ref = (row.get("referenceCandidateId") or row.get("refCandidateId") or "").strip()
        if not cid or not ref:
            continue

        c_canonical = canonical_by_candidate.get(cid)
        r_canonical = canonical_by_candidate.get(ref)
        if not c_canonical or not r_canonical:
            conflicts.append(
                {
                    "candidate_id": cid,
                    "reference_candidate_id": ref,
                    "evaluation": evaluation,
                    "reason": "missing_canonical_mapping",
                    "details": "candidate and/or reference candidate missing canonical mapping",
                }
            )
            continue

        if evaluation == "undercombined":
            if c_canonical == r_canonical:
                conflicts.append(
                    {
                        "candidate_id": cid,
                        "reference_candidate_id": ref,
                        "evaluation": evaluation,
                        "reason": "already_same_canonical",
                        "details": "no merge required; both candidates already map to same canonical",
                    }
                )
                continue

            key = tuple(sorted((r_canonical, c_canonical)))
            if key in merge_seen:
                continue
            merge_seen.add(key)
            merge_rows.append(
                {
                    "canonical_entity_type": "participant",
                    "keep_canonical_id": r_canonical,
                    "merge_canonical_id": c_canonical,
                    "reason_code": "under_combination_reviewed",
                    "actor": actor,
                }
            )
            continue

        if evaluation == "overcombined":
            if c_canonical != r_canonical:
                conflicts.append(
                    {
                        "candidate_id": cid,
                        "reference_candidate_id": ref,
                        "evaluation": evaluation,
                        "reason": "canonical_already_distinct",
                        "details": "candidate and reference already map to different canonicals",
                    }
                )
                continue
            over_edges_same_canonical.append((cid, ref))

    # Step 2: overcombined -> split rows.
    # For each connected component, keep one deterministic anchor candidate and split the rest.
    split_seen: set[str] = set()
    for comp in _find_components(over_edges_same_canonical):
        if len(comp) < 2:
            continue
        comp_sorted = sorted(comp)
        anchor = comp_sorted[0]
        canonical_id = canonical_by_candidate.get(anchor)
        if not canonical_id:
            for cid in comp_sorted:
                conflicts.append(
                    {
                        "candidate_id": cid,
                        "reference_candidate_id": anchor,
                        "evaluation": "overcombined",
                        "reason": "missing_anchor_canonical",
                        "details": "anchor candidate missing canonical mapping",
                    }
                )
            continue
        for cid in comp_sorted[1:]:
            if cid in split_seen:
                continue
            split_seen.add(cid)
            split_rows.append(
                {
                    "canonical_entity_type": "participant",
                    "old_canonical_id": canonical_id,
                    "candidate_entity_id": cid,
                    "reason_code": "over_combination_reviewed",
                    "actor": actor,
                }
            )

    return LifecyclePlanResult(
        merge_rows=merge_rows,
        split_rows=split_rows,
        conflicts=conflicts,
    )
