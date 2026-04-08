"""Unit tests for direct-mail lifecycle plan builder."""

from __future__ import annotations

from regatta_etl.direct_mail_lifecycle_plan import build_lifecycle_plans


def test_build_lifecycle_plans_builds_deduped_merge_rows() -> None:
    reviewed_rows = [
        {
            "candidate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "referenceCandidateId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "candidateRecordEvaluation": "undercombined",
        },
        {
            # duplicate edge; should dedupe
            "candidate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "referenceCandidateId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "candidateRecordEvaluation": "undercombined",
        },
    ]
    canonical_map = {
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa": "11111111-1111-1111-1111-111111111111",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": "22222222-2222-2222-2222-222222222222",
    }

    result = build_lifecycle_plans(reviewed_rows, canonical_map, actor="tester")

    assert result.merge_rows == [
        {
            "canonical_entity_type": "participant",
            "keep_canonical_id": "22222222-2222-2222-2222-222222222222",
            "merge_canonical_id": "11111111-1111-1111-1111-111111111111",
            "reason_code": "under_combination_reviewed",
            "actor": "tester",
        }
    ]
    assert result.split_rows == []
    assert result.conflicts == []


def test_build_lifecycle_plans_dedupes_mirrored_undercombined_edges() -> None:
    reviewed_rows = [
        {
            "candidate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "referenceCandidateId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "candidateRecordEvaluation": "undercombined",
        },
        {
            "candidate_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "referenceCandidateId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "candidateRecordEvaluation": "undercombined",
        },
    ]
    canonical_map = {
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa": "11111111-1111-1111-1111-111111111111",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": "22222222-2222-2222-2222-222222222222",
    }

    result = build_lifecycle_plans(reviewed_rows, canonical_map, actor="tester")

    assert result.merge_rows == [
        {
            "canonical_entity_type": "participant",
            "keep_canonical_id": "22222222-2222-2222-2222-222222222222",
            "merge_canonical_id": "11111111-1111-1111-1111-111111111111",
            "reason_code": "under_combination_reviewed",
            "actor": "tester",
        }
    ]
    assert result.split_rows == []
    assert result.conflicts == []


def test_build_lifecycle_plans_builds_split_rows_from_overcombined_component() -> None:
    reviewed_rows = [
        {
            "candidate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "referenceCandidateId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "candidateRecordEvaluation": "overcombined",
        },
        {
            "candidate_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "referenceCandidateId": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "candidateRecordEvaluation": "overcombined",
        },
        {
            "candidate_id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "referenceCandidateId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "candidateRecordEvaluation": "overcombined",
        },
    ]
    canonical_map = {
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa": "11111111-1111-1111-1111-111111111111",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": "11111111-1111-1111-1111-111111111111",
        "cccccccc-cccc-cccc-cccc-cccccccccccc": "11111111-1111-1111-1111-111111111111",
    }

    result = build_lifecycle_plans(reviewed_rows, canonical_map, actor="tester")

    assert result.merge_rows == []
    # anchor is lexicographically smallest candidate_id (aaaa...), so split b and c
    assert result.split_rows == [
        {
            "canonical_entity_type": "participant",
            "old_canonical_id": "11111111-1111-1111-1111-111111111111",
            "candidate_entity_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "reason_code": "over_combination_reviewed",
            "actor": "tester",
        },
        {
            "canonical_entity_type": "participant",
            "old_canonical_id": "11111111-1111-1111-1111-111111111111",
            "candidate_entity_id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "reason_code": "over_combination_reviewed",
            "actor": "tester",
        },
    ]
    assert result.conflicts == []


def test_build_lifecycle_plans_accepts_entity_resolution_and_ref_aliases() -> None:
    reviewed_rows = [
        {
            "candidate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "refCandidateId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "entityResolution": "UNDER COMBINED",
        },
    ]
    canonical_map = {
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa": "11111111-1111-1111-1111-111111111111",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": "22222222-2222-2222-2222-222222222222",
    }

    result = build_lifecycle_plans(reviewed_rows, canonical_map, actor="tester")

    assert result.merge_rows == [
        {
            "canonical_entity_type": "participant",
            "keep_canonical_id": "22222222-2222-2222-2222-222222222222",
            "merge_canonical_id": "11111111-1111-1111-1111-111111111111",
            "reason_code": "under_combination_reviewed",
            "actor": "tester",
        }
    ]
    assert result.split_rows == []
    assert result.conflicts == []
