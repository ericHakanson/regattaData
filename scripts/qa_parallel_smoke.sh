#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="artifacts/qa"
LOG_FILE="$OUT_DIR/parallel_smoke_${TS}.log"
mkdir -p "$OUT_DIR"

log() {
  echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG_FILE"
}

run() {
  log "RUN: $*"
  "$@" 2>&1 | tee -a "$LOG_FILE"
}

run_optional() {
  log "RUN (optional): $*"
  if "$@" 2>&1 | tee -a "$LOG_FILE"; then
    return 0
  fi
  log "OPTIONAL FAILED: $*"
}

log "QA parallel smoke start"
log "repo: $ROOT_DIR"

run git rev-parse --short HEAD
run git status --short

log "Checking spec files exist"
test -f docs/requirements/club-participant-yacht-promotion-enable-spec.md
test -f docs/requirements/mailchimp-event-registration-activation-spec.md

log "Checking QA artifacts exist"
test -f docs/qa/parallel-claude-qa-runbook.md
test -f docs/qa/parallel-claude-acceptance-matrix.md

log "Checking CLI mode visibility"
run .venv/bin/regatta-import --help
run_optional bash -lc ".venv/bin/regatta-import --help | grep -n 'resolution_score'"
run_optional bash -lc ".venv/bin/regatta-import --help | grep -n 'resolution_promote'"
run_optional bash -lc ".venv/bin/regatta-import --help | grep -n 'mailchimp_event_activation'"

log "Running targeted tests if present"
if [[ -f tests/unit/test_resolution_score.py ]]; then
  run .venv/bin/pytest -q tests/unit/test_resolution_score.py
fi
if [[ -f tests/integration/test_resolution_score_and_promote.py ]]; then
  run .venv/bin/pytest -q tests/integration/test_resolution_score_and_promote.py
fi
if [[ -f tests/unit/test_mailchimp_activation.py ]]; then
  run .venv/bin/pytest -q tests/unit/test_mailchimp_activation.py
fi
if [[ -f tests/integration/test_mailchimp_activation.py ]]; then
  run .venv/bin/pytest -q tests/integration/test_mailchimp_activation.py
fi

if [[ "${RUN_DB_SMOKE:-0}" == "1" ]]; then
  log "RUN_DB_SMOKE=1 enabled"
  : "${DB_DSN:?DB_DSN must be set when RUN_DB_SMOKE=1}"

  run psql "$DB_DSN" -c "select current_database(), current_user, now();"

  run psql "$DB_DSN" -c "
  SELECT entity_type, resolution_state, is_promoted, COUNT(*) AS n
  FROM (
    SELECT 'club' AS entity_type, resolution_state, is_promoted FROM candidate_club
    UNION ALL
    SELECT 'participant', resolution_state, is_promoted FROM candidate_participant
    UNION ALL
    SELECT 'yacht', resolution_state, is_promoted FROM candidate_yacht
    UNION ALL
    SELECT 'event', resolution_state, is_promoted FROM candidate_event
    UNION ALL
    SELECT 'registration', resolution_state, is_promoted FROM candidate_registration
  ) s
  GROUP BY 1,2,3
  ORDER BY 1,2,3;"

  run_optional .venv/bin/regatta-import --mode resolution_score --db-dsn "$DB_DSN" --entity-type all --dry-run
  run_optional .venv/bin/regatta-import --mode resolution_promote --db-dsn "$DB_DSN" --entity-type all --dry-run
fi

log "QA parallel smoke complete"
log "log file: $LOG_FILE"
