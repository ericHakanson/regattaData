# Runbook 08: Entity Resolution Pipeline

## Overview

The entity-resolution system consolidates all ingested data into trusted canonical records.

It runs in three phases:

| Phase | CLI mode | What it does |
|-------|----------|--------------|
| 1. Source → Candidate | `resolution_source_to_candidate` | Reads every source table, builds `candidate_*` records with stable fingerprints, links every row to a candidate via `candidate_source_link`. |
| 2. Candidate Scoring _(Phase 2, future)_ | `resolution_score` | Loads YAML rules, computes quality scores + reasons, sets `resolution_state`. |
| 3. Candidate → Canonical _(Phase 2, future)_ | `resolution_promote` | Promotes `auto_promote` candidates to `canonical_*` via `candidate_canonical_link`. |

---

## Prerequisites

- Migrations `0001`–`0012` applied.
- `PyYAML>=6.0` installed (`pip install -e ".[dev]"`).
- Database DSN available as `$DB_DSN`.
- All ingestion pipelines (private_export, public_scrape, jotform, mailchimp, airtable_copy, yacht_scoring) have been run at least once.

---

## Phase 1: Source → Candidate

### Run (all entity types)

```bash
regatta-import \
  --mode resolution_source_to_candidate \
  --db-dsn "$DB_DSN" \
  --entity-type all
```

### Dry-run (inspect without committing)

```bash
regatta-import \
  --mode resolution_source_to_candidate \
  --db-dsn "$DB_DSN" \
  --entity-type all \
  --dry-run
```

### Run a single entity type

```bash
regatta-import \
  --mode resolution_source_to_candidate \
  --db-dsn "$DB_DSN" \
  --entity-type participant
```

Valid `--entity-type` values: `participant`, `yacht`, `event`, `registration`, `club`, `all`.

**Processing order matters.** `registration` depends on `event` + `yacht` + `participant` candidates existing. Always run `all` or run in order: `club` → `event` → `yacht` → `participant` → `registration`.

### What it produces

- One `candidate_*` row per unique real-world entity (identified by `stable_fingerprint`).
- One `candidate_source_link` row per source table row (idempotent — safe to re-run).
- Child rows in `candidate_participant_contact`, `candidate_participant_address`, `candidate_participant_role_assignment`.

### Fingerprint strategy

| Entity | Fingerprint key |
|--------|----------------|
| participant | `sha256(normalized_name \| normalized_email_or_empty)` |
| yacht | `sha256(normalized_name \| normalized_sail_or_empty)` |
| club | `sha256(normalized_name)` |
| event | `sha256(normalized_name \| season_year \| external_id_or_empty)` |
| registration | `sha256(candidate_event_id \| external_id_or_empty \| candidate_yacht_id_or_empty)` |

### Source tables ingested

| Entity | Source tables |
|--------|---------------|
| club | `yacht_club` |
| event | `event_instance` (joined with `event_series`) |
| yacht | `yacht` |
| participant | `participant`, `jotform_waiver_submission`, `mailchimp_audience_row`, `airtable_copy_row`[participants, owners], `yacht_scoring_raw_row`[deduplicated_entry, scraped_entry_listing], `participant_related_contact` |
| registration | `event_entry` |

### Intentionally skipped tables

Tables that are supplemental, relational, or are the candidate/canonical layer itself are not ingested as standalone entities. They are logged in the pipeline report with a reason code. See `_SKIPPED_TABLES` in `resolution_source_to_candidate.py` for the complete list.

---

## Idempotency

Phase 1 is fully idempotent. Re-running the same inputs:
- Does not create duplicate `candidate_*` rows (fingerprint conflict → fill-nulls-only update).
- Does not create duplicate `candidate_source_link` rows (unique constraint → `DO NOTHING`).

---

## Interpreting the Pipeline Report

The pipeline prints a summary report:

```
============================================================
Source-to-Candidate Pipeline Report
  dry_run: False
============================================================
Clubs:
  rows ingested:       1
  candidates created:  1
  candidates enriched: 0
Events:
  rows ingested:       3
  candidates created:  3
  candidates enriched: 0
...
DB errors:             0
Intentionally Skipped Tables:
  event_series: captured_via_event_instance_join
  ...
============================================================
```

- **candidates created**: new `candidate_*` rows inserted.
- **candidates enriched**: existing rows whose null columns were filled.
- **DB errors**: rows that produced an exception (investigate `warnings` in report).
- Exit code `1` if `db_errors > 0`.

---

## YAML Scoring Rules

Rule files live in `config/resolution_rules/*.yml`. One file per entity type.

```
config/resolution_rules/
  participant.yml
  yacht.yml
  club.yml
  event.yml
  registration.yml
```

### Rule file structure

```yaml
entity_type: participant
source_system: "regattaman|mailchimp|..."
version: "v1.0.0"

thresholds:
  auto_promote: 0.95   # → resolution_state = auto_promote
  review: 0.75         # → resolution_state = review
  hold: 0.50           # → resolution_state = hold
                       # score < hold → reject

feature_weights:
  email_exact: 0.55
  phone_exact: 0.20
  # ...

hard_blocks:
  - conflicting_dob   # Forces score=0, state=reject

source_precedence:
  - jotform_waiver_csv
  - regattaman_csv_export
  # ...

survivorship_rules:
  date_of_birth: highest_precedence_non_null

missing_attribute_penalties:
  missing_email: 0.10  # Subtracted when email absent
```

### Updating a rule file

1. Edit the YAML file.
2. Bump `version` (e.g. `v1.1.0`).
3. Re-run the scoring pipeline — it will deactivate the old `resolution_rule_set` row and insert the new one.

---

## Resolution Governance Tables

| Table | Purpose |
|-------|---------|
| `resolution_rule_set` | Versioned YAML content with hash. One active row per entity type. |
| `resolution_score_run` | Log of every scoring run with counters and status. |
| `resolution_manual_action_log` | Audit trail for all manual and pipeline-driven actions. |

---

## Manual Review Workflow (Phase 2)

_Not yet implemented. Planned for Phase 2._

1. Export `review`/`hold` candidates to a spreadsheet.
2. Make decisions (promote/reject/merge) in the sheet.
3. Ingest decisions via `--mode resolution_manual_apply`.
4. All changes write to `resolution_manual_action_log`.

---

## Canonical Enriched Views

```sql
-- Runtime age computation (no persisted age column)
SELECT display_name, age_years, is_minor
FROM canonical_participant_enriched
WHERE is_minor = true;

-- Same for candidate layer
SELECT display_name, age_years, is_minor
FROM candidate_participant_enriched
WHERE resolution_state = 'review';
```

`is_minor` uses age-of-majority = 18 (fixed per spec).

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `db_errors > 0` in report | Source row caused an exception | Check `warnings` in report output for the specific row PK and error. |
| Registration candidates not created | Events/yachts not yet ingested | Run `--entity-type all` or run entity types in dependency order. |
| Duplicate candidates after re-run | Fingerprint mismatch (normalization changed) | Verify normalization functions haven't changed for existing records. |
| YAML validation error | Missing required key or invalid threshold | Check error message; validate ordering: `hold <= review <= auto_promote`. |
| `PyYAML` import error | Package not installed | Run `pip install PyYAML>=6.0` or `pip install -e ".[dev]"`. |

---

## Source Purge Readiness (Future)

Not implemented. Before any source table can be purged:

1. 100% lineage coverage: every source row has a `candidate_source_link`.
2. Canonical coverage: all desired candidates promoted to canonical.
3. Unresolved candidates reviewed and dispositioned.
4. Backup snapshot confirmed.

Run the readiness report command _(not yet implemented)_ to check coverage before any purge decision.
