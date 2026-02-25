# Technical Spec: Source-Trust Weighted Candidate Scoring

## 1. Objective
Introduce a dedicated YAML policy file to account for source trust in candidate scoring, so candidates supported by higher-trust sources score and route differently than candidates supported only by lower-trust sources.

This must integrate with existing `resolution_score` behavior, not replace it.

## 2. Problem Statement
Current scoring is feature-presence based (email/phone/name/etc.) and does not materially distinguish source credibility at score time. The same feature set can produce the same score regardless of whether evidence came from high-trust or low-trust sources.

Operational consequence:
1. Over-promotion risk when low-trust-only candidates look complete.
2. Under-explainable outcomes because source trust is not represented in scoring reasons.
3. Harder policy evolution when source confidence changes over time.

## 3. Scope
In scope:
1. New source-trust YAML file and validator.
2. Score-time source evidence extraction from `candidate_source_link`.
3. Deterministic trust adjustment + routing gates layered onto base score.
4. CLI support for explicit source-trust file path.
5. Reporting and reason codes for trust-driven outcomes.
6. Unit/integration tests.

Out of scope:
1. Rebuilding source ingestion pipelines.
2. Changing candidate schema fields used for base feature extraction.
3. Canonical survivorship redesign.

## 4. Design Principles
1. Backward-compatible by default: existing per-entity rule files remain authoritative for base scoring.
2. Explicit and auditable: trust effects must appear in `confidence_reasons`.
3. Conservative promotion: lack of high-trust evidence should cap state.
4. Deterministic and reproducible: same candidate + same YAML => same output.

## 5. New YAML Policy File
Create:
1. `config/resolution_rules/source_trust.yml`

### 5.1 Required Top-Level Keys
1. `version`
2. `defaults`
3. `source_weights`
4. `entity_overrides`

### 5.2 Required Defaults Keys
1. `unknown_source_weight` (float, 0.0-1.0)
2. `high_trust_threshold` (float, 0.0-1.0)
3. `min_distinct_sources_for_auto_promote` (int >= 1)
4. `require_high_trust_for_auto_promote` (bool)
5. `single_source_penalty` (float, 0.0-1.0)
6. `no_high_trust_penalty` (float, 0.0-1.0)
7. `multi_source_bonus` (float, 0.0-1.0)
8. `max_total_adjustment_abs` (float, 0.0-1.0)

### 5.3 `source_weights` Shape
`source_weights` is a map keyed by `source_system` values found in `candidate_source_link.source_system`.

Each source entry requires:
1. `weight` (float, 0.0-1.0)
2. `tier` (enum: `high`, `medium`, `low`)
3. `notes` (string, optional)

### 5.4 `entity_overrides` Shape
Optional per entity type override (`participant`, `yacht`, `club`, `event`, `registration`) for any defaults key above.

### 5.5 Baseline YAML (initial proposal)
```yaml
version: "v1.0.0"

defaults:
  unknown_source_weight: 0.40
  high_trust_threshold: 0.80
  min_distinct_sources_for_auto_promote: 2
  require_high_trust_for_auto_promote: true
  single_source_penalty: 0.08
  no_high_trust_penalty: 0.12
  multi_source_bonus: 0.05
  max_total_adjustment_abs: 0.20

source_weights:
  operational_db:
    weight: 0.95
    tier: high
    notes: "Operationally curated entity tables."
  jotform_waiver_csv:
    weight: 0.90
    tier: high
  regattaman_csv_export:
    weight: 0.85
    tier: high
  regattaman_public_scrape_csv:
    weight: 0.75
    tier: medium
  airtable_copy_csv:
    weight: 0.70
    tier: medium
  yacht_scoring_csv:
    weight: 0.65
    tier: medium
  mailchimp_audience_csv:
    weight: 0.60
    tier: medium

entity_overrides:
  registration:
    min_distinct_sources_for_auto_promote: 1
    require_high_trust_for_auto_promote: false
  participant:
    no_high_trust_penalty: 0.15
```

## 6. Scoring Semantics
Base score remains from current entity rule YAML (`feature_weights`, `penalties`, `hard_blocks`).

Add source-trust layer:
1. Gather candidate source systems from `candidate_source_link` for candidate `(entity_type, id)`.
2. Compute:
   - `distinct_source_count`
   - `has_high_trust_source` (any source weight >= `high_trust_threshold`)
   - `max_source_weight`
3. Compute additive adjustment:
   - Start `adj = 0`
   - If `distinct_source_count == 1`, `adj -= single_source_penalty`
   - If `distinct_source_count >= 2`, `adj += multi_source_bonus`
   - If no high-trust source, `adj -= no_high_trust_penalty`
   - Clamp `adj` to `[-max_total_adjustment_abs, +max_total_adjustment_abs]`
4. `final_score = clamp(base_score + adj, 0.0, 1.0)`
5. Routing gates for `auto_promote`:
   - If `require_high_trust_for_auto_promote` and no high-trust source, cap state at `review`.
   - If `distinct_source_count < min_distinct_sources_for_auto_promote`, cap state at `review`.
6. Preserve existing promoted-candidate protection (`is_promoted => resolution_state stays auto_promote`).

## 7. Required Code Changes
## 7.1 New module
Add `src/regatta_etl/source_trust_rules.py`:
1. Dataclass(es) for parsed trust config.
2. YAML loader + strict validator.
3. Helpers:
   - `get_effective_policy(entity_type)`
   - `compute_source_trust_adjustment(entity_type, source_systems)`
   - `apply_auto_promote_gates(...)`

## 7.2 Update scoring pipeline
Modify `src/regatta_etl/resolution_score.py`:
1. Load trust YAML once per run.
2. For each candidate, fetch source systems from `candidate_source_link`.
3. Apply adjustment + gates after existing `compute_score(...)`.
4. Include trust reasons in `confidence_reasons`, for example:
   - `source_trust:distinct_sources=2`
   - `source_trust:has_high_trust_source=true`
   - `source_trust:adjustment=-0.0800`
   - `source_trust:auto_promote_capped=no_high_trust_source`
5. Add counters:
   - `source_trust_adjusted`
   - `source_trust_auto_promote_capped`
   - `source_trust_no_high_trust`
   - `source_trust_single_source`

## 7.3 CLI changes
Modify `src/regatta_etl/import_regattaman_csv.py`:
1. New option `--source-trust-file` (path, optional).
2. Default: `config/resolution_rules/source_trust.yml`.
3. Pass parsed policy into `run_score(...)`.

## 8. Error Handling Requirements
1. If trust YAML is missing/invalid in non-dry-run mode, fail fast before scoring any candidate.
2. Unknown `source_system` values must not fail the run; use `unknown_source_weight` and emit warning reason.
3. If a candidate has zero source links, treat as unknown/low trust, add reason, and cap auto-promote.

## 9. Reporting Requirements
`build_score_report(...)` must include:
1. trust-adjusted candidate count
2. count capped from auto-promote by trust gates
3. count with unknown source systems encountered

Run report JSON must include the same counters.

## 10. Test Requirements
## 10.1 Unit tests
1. YAML validation (required keys, ranges, invalid tiers).
2. Entity override resolution.
3. Adjustment math and clamping.
4. Auto-promote cap behavior.
5. Unknown-source fallback behavior.

## 10.2 Integration tests
1. Candidate with identical base features but different source systems yields different final states.
2. Candidate meeting base auto-promote threshold gets capped to review when trust gates fail.
3. Candidate with multi-source + high-trust evidence promotes as expected.
4. Dry-run executes full path and rolls back with zero DB errors.

## 11. Rollout Plan
1. Add loader + validation + YAML + tests.
2. Wire into `resolution_score` behind default-on policy.
3. Run `resolution_score --dry-run --entity-type all` and compare:
   - base auto_promote vs trust-adjusted auto_promote
   - capped counts
4. Review with domain owner; tune weights.
5. Run non-dry-run after approval.

## 12. Acceptance Criteria
1. Source trust is externally configurable in one YAML file.
2. Scoring output differs when evidence source trust differs (with same base features).
3. Auto-promote can be policy-capped by trust gates.
4. All trust effects are visible in `confidence_reasons` and run reports.
5. Full scoring test suite passes.

## 13. Open Questions (must resolve before implementation)
1. Confirm authoritative list of `source_system` values currently present in production `candidate_source_link`.
2. Confirm whether `operational_db` should always be treated as high trust.
3. Confirm whether any entity type should permit auto-promote with single-source evidence.

Suggested discovery query:
```sql
SELECT candidate_entity_type, source_system, count(*) AS links
FROM candidate_source_link
GROUP BY candidate_entity_type, source_system
ORDER BY candidate_entity_type, links DESC;
```
