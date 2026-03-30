# Runbook 08 — Conversational Manual Curation: QA & Handoff

This document covers QA verification and handoff guidance for the Conversational Manual Curation feature set (FOR-187 through FOR-193).

---

## Architecture Overview

Manual curation follows the standard `source → candidate → canonical` pipeline:

```
devMcp regatta tools
        │
        ▼
mcp_helper.py subprocess
        │
        ├─── READ:  candidate_* tables  (find_profile, profile_context)
        │
        ├─── WRITE: manual_* patch tables  (apply_patch)
        │
        └─── EXEC:  resolution pipeline  (apply_decision, apply_lifecycle, rerun_resolution)
                        │
                        ▼
               candidate_* / canonical_* tables
```

**Critical constraint:** No tool writes directly to `canonical_*` tables. All writes go to either `manual_*` source tables or use the existing resolution/lifecycle pipeline.

---

## Files Delivered

### regattaData repo
| File | Role |
|------|------|
| `migrations/0024_manual_curation_tables.sql` | FOR-187: 5 `manual_*` patch tables |
| `src/regatta_etl/mcp_helper.py` | FOR-189/190/191: Python subprocess handler |
| `src/regatta_etl/resolution_source_to_candidate.py` | FOR-188: +3 ingestion functions + pipeline steps |
| `tests/integration/conftest.py` | +migration 0024 |
| `tests/integration/test_manual_curation_pipeline.py` | FOR-187/188: 15 integration tests |

### devMcp repo
| File | Role |
|------|------|
| `src/regatta/tools.ts` | FOR-189/190/191/192: 9 MCP tools (regatta.curation.*) |
| `src/regatta/client.ts` | Python subprocess wrapper |
| `src/regatta/preview-store.ts` | In-memory preview token store |
| `src/config/schema.ts` | +optional `regatta` config section |
| `src/types/config.ts` | +`RegattaConfig` interface |
| `src/config/load-config.ts` | +regatta section in mergeConfig |
| `src/tools/index.ts` | +regatta tool catalog entries |
| `src/server.ts` | +`registerRegattaTools(...)` call |

---

## devmcp.json Configuration

Add a `regatta` section to `/Users/erichakanson/projects/devMcp/config/devmcp.json`:

```json
{
  "regatta": {
    "enabled": true,
    "dbDsn": "$DB_DSN",
    "pythonBin": "python3",
    "repoPath": "/Users/erichakanson/projects/regattaData"
  }
}
```

`dbDsn` may reference an environment variable with `$VAR_NAME` syntax; the TypeScript client resolves it at call time.

---

## QA Checklist

### Schema (FOR-187)

- [ ] `migrations/0024_manual_curation_tables.sql` applies cleanly after 0023 with no errors.
- [ ] All 5 tables exist: `manual_participant_patch`, `manual_participant_address_patch`, `manual_yacht_patch`, `manual_yacht_ownership_patch`, `manual_club_membership_patch`.
- [ ] `patch_hash UNIQUE` index prevents exact-duplicate rows on each table.
- [ ] `status CHECK (status IN ('active','superseded','revoked'))` rejects invalid values.
- [ ] `ux_manual_participant_address_active` partial unique index rejects duplicate active `(candidate_participant_id, address_raw)`.
- [ ] `ux_manual_yacht_ownership_active_add` partial unique index rejects duplicate active `(participant, yacht, role)` add rows.
- [ ] Revoked/superseded rows do not conflict with new active rows (partial indexes skip non-active status).
- [ ] All FK references to `candidate_participant`, `candidate_yacht`, `candidate_club` are correct.
- [ ] `updated_at` trigger fires correctly on UPDATE.
- [ ] All 15 integration tests in `test_manual_curation_pipeline.py` pass.

### Pipeline (FOR-188)

- [ ] `run_source_to_candidate(conn, entity_type='participant')` applies active `manual_participant_patch` rows (overwrite semantics).
- [ ] `run_source_to_candidate` creates `candidate_participant_address` from active `manual_participant_address_patch` rows.
- [ ] `run_source_to_candidate(conn, entity_type='yacht')` applies active `manual_yacht_patch` rows.
- [ ] All three ingestion steps produce `source_system='manual_curation'` on `candidate_source_link`.
- [ ] Revoked patches are skipped (status filter `WHERE status = 'active'`).
- [ ] Running the pipeline twice produces no duplicate address or source-link rows.
- [ ] Manual curation steps run AFTER operational source steps (correct ordering in `run_source_to_candidate`).
- [ ] `db_errors == 0` for all pipeline tests.

### MCP Tools (FOR-189 / FOR-190 / FOR-191 / FOR-192)

**Negative path: no-config guard**
- [ ] All `regatta.curation.*` tools return `"Regatta integration is not configured"` when `config.regatta?.enabled` is false or absent.

**Find / context (read-only)**
- [ ] `regatta.curation.find_profile` returns candidate list matching name or email query.
- [ ] `regatta.curation.profile_context` returns contacts, addresses, roles, sources, canonical link, related yachts, and manual patches.

**Preview/confirm pattern**
- [ ] `preview_decision` returns `previewToken` (UUID) alongside human-readable preview.
- [ ] `confirm_decision` with a valid token executes the write and invalidates the token.
- [ ] `confirm_decision` with an expired or consumed token returns an error without writing.
- [ ] `confirm_decision` with a token from `preview_lifecycle` returns an error (wrong command check).
- [ ] Same pattern holds for `preview_lifecycle` / `confirm_lifecycle` and `preview_patch` / `confirm_patch`.
- [ ] Tokens expire after ≤ 5 minutes if not consumed.

**Factual patches (FOR-190)**
- [ ] `preview_patch` → `confirm_patch` for `participant_address_patch` writes to `manual_participant_address_patch`, not to `canonical_participant`.
- [ ] `preview_patch` → `confirm_patch` for `participant_patch` writes to `manual_participant_patch`.
- [ ] `preview_patch` → `confirm_patch` for `yacht_patch` writes to `manual_yacht_patch`.
- [ ] `preview_patch` → `confirm_patch` for `yacht_ownership_patch` writes to `manual_yacht_ownership_patch`.
- [ ] `preview_patch` → `confirm_patch` for `club_membership_patch` writes to `manual_club_membership_patch`.
- [ ] Idempotent re-patch (same hash) returns `inserted: false`, not an error.

**Audit trail**
- [ ] Every tool call generates a record in `auditLogger` (verify in audit log dir).
- [ ] `confirm_*` calls log the actor and previewToken in metadata.

**No direct canonical writes**
- [ ] Code-review: `mcp_helper.py` contains no `INSERT INTO canonical_*` or `UPDATE canonical_*` statements.
- [ ] Code-review: `tools.ts` contains no raw SQL.
- [ ] Code-review: `client.ts` connects only via the Python subprocess; no TypeScript DB client.

**Rerun (FOR-192)**
- [ ] `rerun_resolution` triggers source-to-candidate pipeline and returns counters.
- [ ] Running `confirm_patch` + `rerun_resolution` propagates the patch to `candidate_participant_address`.

### Build verification (devMcp)
- [ ] `npm run build` in `/Users/erichakanson/projects/devMcp` produces zero TypeScript errors.
- [ ] Regatta tools appear in `devmcp.project.list` response when `regatta.enabled: true`.

---

## End-to-End Conversational Flow

A complete conversational curation session looks like:

```
1. find_profile          → identify candidate_participant_id
2. profile_context       → review current state
3. preview_patch         → preview adding address "123 Main St, Seattle WA"
                           ← returns previewToken
4. confirm_patch         → commit with token
                           ← returns {ok: true, inserted: true}
5. rerun_resolution      → propagate to candidate_participant_address
6. profile_context       → verify address appears
```

For a resolution decision:

```
1. find_profile          → identify candidate
2. profile_context       → verify resolution_state='review', is_promoted=false
3. preview_decision      → preview promote
                           ← returns previewToken + current_state + outcome_description
4. confirm_decision      → commit
                           ← returns {ok: true, rows_promoted: 1}
```

---

## Evidence Artifacts

- Integration test run: `python -m pytest tests/integration/test_manual_curation_pipeline.py -v`
- TypeScript build: `cd /Users/erichakanson/projects/devMcp && npm run build`
- Migration smoke: apply 0024 against test DB and verify tables + indexes exist
