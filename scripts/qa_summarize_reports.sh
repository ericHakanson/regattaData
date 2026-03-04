#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

N="${1:-20}"

python3 - "$N" <<'PY'
import glob
import json
import os
import sys
from datetime import datetime

try:
    n = int(sys.argv[1])
except Exception:
    n = 20

files = sorted(glob.glob("artifacts/reports/*.json"), key=os.path.getmtime, reverse=True)[:n]
if not files:
    print("No reports found in artifacts/reports")
    sys.exit(0)

print("file\trun_id\tmode\tdry_run\tstarted_at\tdb_errors\trows_read\trows_rejected")
for f in files:
    try:
        d = json.load(open(f, encoding="utf-8"))
    except Exception:
        continue
    run_id = d.get("run_id", os.path.basename(f))
    mode = d.get("mode", "unknown")
    dry = d.get("dry_run")
    started = d.get("started_at", "")
    counters = d.get("counters", {}) if isinstance(d.get("counters"), dict) else {}
    db_errors = counters.get("db_errors", counters.get("db_phase_errors", ""))
    rows_read = counters.get("rows_read", counters.get("yacht_scoring_rows_raw_inserted", ""))
    rows_rejected = counters.get("rows_rejected", counters.get("yacht_scoring_rows_curated_rejected", ""))
    print(f"{os.path.basename(f)}\t{run_id}\t{mode}\t{dry}\t{started}\t{db_errors}\t{rows_read}\t{rows_rejected}")
PY
