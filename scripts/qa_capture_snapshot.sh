#!/usr/bin/env bash
set -euo pipefail

: "${DB_DSN:?DB_DSN is required}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SNAP_LABEL="${1:-snapshot}"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="artifacts/qa/${TS}_${SNAP_LABEL}"
mkdir -p "$OUT_DIR"

echo "Writing QA snapshot to $OUT_DIR"

git rev-parse --short HEAD > "$OUT_DIR/git_head.txt"
git status --short > "$OUT_DIR/git_status.txt"

psql "$DB_DSN" -v ON_ERROR_STOP=1 -f docs/qa/sql/resolution_state_snapshot.sql \
  > "$OUT_DIR/resolution_state_snapshot.txt"
psql "$DB_DSN" -v ON_ERROR_STOP=1 -f docs/qa/sql/resolution_score_distribution.sql \
  > "$OUT_DIR/resolution_score_distribution.txt"
psql "$DB_DSN" -v ON_ERROR_STOP=1 -f docs/qa/sql/canonical_counts.sql \
  > "$OUT_DIR/canonical_counts.txt"

# Optional activation snapshot (ignore failures if tables are not present yet)
psql "$DB_DSN" -v ON_ERROR_STOP=1 -f docs/qa/sql/mailchimp_activation_snapshot.sql \
  > "$OUT_DIR/mailchimp_activation_snapshot.txt" 2>/dev/null || true

# Capture latest report metadata
python3 - <<'PY' > "$OUT_DIR/latest_reports_summary.txt"
import glob, json, os
files = sorted(glob.glob("artifacts/reports/*.json"), key=os.path.getmtime, reverse=True)[:10]
for f in files:
    try:
        d = json.load(open(f, encoding="utf-8"))
    except Exception:
        continue
    run_id = d.get("run_id", os.path.basename(f))
    mode = d.get("mode", "unknown")
    dry = d.get("dry_run")
    started = d.get("started_at")
    counters = d.get("counters", {}) if isinstance(d.get("counters"), dict) else {}
    db_errors = counters.get("db_errors", counters.get("db_phase_errors"))
    print(f"{os.path.basename(f)}\trun_id={run_id}\tmode={mode}\tdry_run={dry}\tstarted_at={started}\tdb_errors={db_errors}")
PY

echo "Snapshot complete: $OUT_DIR"
