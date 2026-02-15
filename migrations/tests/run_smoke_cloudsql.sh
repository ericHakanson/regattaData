#!/usr/bin/env bash
# run_smoke_cloudsql.sh — Cloud SQL wrapper for migration smoke tests.
# Launches a Cloud SQL proxy, creates a temp database, delegates to
# run_smoke.sh, then tears down proxy and temp DB.
#
# Ref: docs/requirements/cloud-sql-smoke-wrapper-requirements.md

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_ENV=""
PROJECT_ID=""
INSTANCE_CONN=""
DB_USER=""
DB_PASSWORD_ENV=""
DB_MAINTENANCE="postgres"
DB_PREFIX="regatta_smoke"
KEEP_DB="false"
ALLOW_PROD="false"
LOGS_DIR="${SCRIPT_DIR}/logs"

PROXY_PID=""
PROXY_PORT=""
TEMP_DB=""
RUN_LOG_DIR=""

# ── Usage ─────────────────────────────────────────────────────────────
usage() {
  cat <<'EOF'
Usage: run_smoke_cloudsql.sh [flags]

Required flags:
  --target-env NAME                dev | staging | prod
  --project-id ID                  GCP project ID
  --instance-connection-name CONN  project:region:instance
  --db-user USER                   Database user

Optional flags:
  --db-password-env VAR            Env var name holding DB password (default: unset)
  --db-maintenance DB              Maintenance database for createdb/dropdb (default: postgres)
  --db-prefix PREFIX               Temp DB name prefix (default: regatta_smoke)
  --keep-db true|false             Retain temp DB after run (default: false)
  --allow-prod true|false          Allow execution against prod (default: false)
  --logs-dir DIR                   Log output directory (default: migrations/tests/logs)
  --help                           Show this message
EOF
}

# ── Argument parsing ──────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-env)               TARGET_ENV="$2";        shift 2 ;;
    --project-id)               PROJECT_ID="$2";        shift 2 ;;
    --instance-connection-name) INSTANCE_CONN="$2";     shift 2 ;;
    --db-user)                  DB_USER="$2";           shift 2 ;;
    --db-password-env)          DB_PASSWORD_ENV="$2";   shift 2 ;;
    --db-maintenance)           DB_MAINTENANCE="$2";    shift 2 ;;
    --db-prefix)                DB_PREFIX="$2";         shift 2 ;;
    --keep-db)                  KEEP_DB="$2";           shift 2 ;;
    --allow-prod)               ALLOW_PROD="$2";        shift 2 ;;
    --logs-dir)                 LOGS_DIR="$2";          shift 2 ;;
    --help)                     usage; exit 0 ;;
    *)
      echo "error: unknown flag: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

# ── Validate required flags ──────────────────────────────────────────
missing=()
[[ -z "$TARGET_ENV" ]]    && missing+=("--target-env")
[[ -z "$PROJECT_ID" ]]    && missing+=("--project-id")
[[ -z "$INSTANCE_CONN" ]] && missing+=("--instance-connection-name")
[[ -z "$DB_USER" ]]       && missing+=("--db-user")

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "error: missing required flags: ${missing[*]}" >&2
  usage >&2
  exit 1
fi

if [[ "$TARGET_ENV" != "dev" && "$TARGET_ENV" != "staging" && "$TARGET_ENV" != "prod" ]]; then
  echo "error: --target-env must be dev, staging, or prod (got: $TARGET_ENV)" >&2
  exit 1
fi

# ── Safety gate: prod protection ─────────────────────────────────────
if [[ "$TARGET_ENV" == "prod" ]]; then
  if [[ "$ALLOW_PROD" != "true" ]]; then
    echo "error: --target-env prod requires --allow-prod true" >&2
    exit 1
  fi
  echo "============================================================"
  echo "  WARNING: Running smoke tests against PRODUCTION instance"
  echo "  Instance: $INSTANCE_CONN"
  echo "  Project:  $PROJECT_ID"
  echo "============================================================"
fi

# ── Tool availability check ──────────────────────────────────────────
REQUIRED_TOOLS=(psql createdb dropdb gcloud python3)
# Accept either cloud-sql-proxy or cloud_sql_proxy
PROXY_BIN=""
if command -v cloud-sql-proxy &>/dev/null; then
  PROXY_BIN="cloud-sql-proxy"
elif command -v cloud_sql_proxy &>/dev/null; then
  PROXY_BIN="cloud_sql_proxy"
fi

missing_tools=()
for tool in "${REQUIRED_TOOLS[@]}"; do
  command -v "$tool" &>/dev/null || missing_tools+=("$tool")
done
[[ -z "$PROXY_BIN" ]] && missing_tools+=("cloud-sql-proxy")

if [[ ${#missing_tools[@]} -gt 0 ]]; then
  echo "error: required tools not found: ${missing_tools[*]}" >&2
  exit 1
fi

# ── Resolve password ─────────────────────────────────────────────────
DB_PASSWORD=""
if [[ -n "$DB_PASSWORD_ENV" ]]; then
  DB_PASSWORD="${!DB_PASSWORD_ENV:-}"
  if [[ -z "$DB_PASSWORD" ]]; then
    echo "error: env var $DB_PASSWORD_ENV is empty or unset" >&2
    exit 1
  fi
fi

# ── Generate unique names ────────────────────────────────────────────
RUN_ID="$(date +%Y%m%d_%H%M%S)_$$_${RANDOM}"
TEMP_DB="${DB_PREFIX}_${RUN_ID}"
RUN_LOG_DIR="${LOGS_DIR}/${RUN_ID}"
mkdir -p "$RUN_LOG_DIR"

# Capture all wrapper stdout/stderr to wrapper.log while still printing to terminal.
exec > >(tee "${RUN_LOG_DIR}/wrapper.log") 2>&1

# ── Cleanup trap ─────────────────────────────────────────────────────
cleanup() {
  local exit_code=$?

  # Stop proxy
  if [[ -n "$PROXY_PID" ]] && kill -0 "$PROXY_PID" 2>/dev/null; then
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
    echo "[wrapper] proxy stopped (pid $PROXY_PID)"
  fi

  # Drop temp DB unless --keep-db true
  if [[ "$KEEP_DB" == "true" ]]; then
    echo "[wrapper] keeping database: $TEMP_DB"
  elif [[ -n "$TEMP_DB" && -n "$PROXY_PORT" ]]; then
    echo "[wrapper] dropping database: $TEMP_DB"
    if ! PGHOST=127.0.0.1 PGPORT="$PROXY_PORT" PGUSER="$DB_USER" \
         PGPASSWORD="$DB_PASSWORD" \
         dropdb --maintenance-db="$DB_MAINTENANCE" "$TEMP_DB" 2>>"${RUN_LOG_DIR}/cleanup.log"; then
      echo "[wrapper] ERROR: failed to drop $TEMP_DB — see ${RUN_LOG_DIR}/cleanup.log" >&2
      exit_code=1
    fi
  fi

  exit "$exit_code"
}
trap cleanup EXIT

# ── Find an available port ───────────────────────────────────────────
find_available_port() {
  # Use Python to bind to port 0 and read the assigned port.
  python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
"
}

PROXY_PORT="$(find_available_port)"

# ── Start Cloud SQL proxy ────────────────────────────────────────────
echo "[wrapper] starting proxy on 127.0.0.1:${PROXY_PORT}"

"$PROXY_BIN" \
  --port "$PROXY_PORT" \
  "$INSTANCE_CONN" \
  > "${RUN_LOG_DIR}/proxy.log" 2>&1 &
PROXY_PID=$!

# Wait for proxy to become ready (up to 30 seconds)
PROXY_READY=false
for i in $(seq 1 30); do
  if ! kill -0 "$PROXY_PID" 2>/dev/null; then
    echo "error: proxy exited prematurely — see ${RUN_LOG_DIR}/proxy.log" >&2
    PROXY_PID=""  # prevent cleanup from trying to kill it
    exit 1
  fi
  if PGHOST=127.0.0.1 PGPORT="$PROXY_PORT" PGUSER="$DB_USER" \
     PGPASSWORD="$DB_PASSWORD" \
     psql -d "$DB_MAINTENANCE" -c "SELECT 1" &>/dev/null; then
    PROXY_READY=true
    break
  fi
  sleep 1
done

if [[ "$PROXY_READY" != "true" ]]; then
  echo "error: proxy did not become ready within 30s — see ${RUN_LOG_DIR}/proxy.log" >&2
  exit 1
fi

echo "[wrapper] proxy ready (pid $PROXY_PID, port $PROXY_PORT)"

# ── Create temp database ─────────────────────────────────────────────
echo "[wrapper] creating database: $TEMP_DB"
PGHOST=127.0.0.1 PGPORT="$PROXY_PORT" PGUSER="$DB_USER" \
  PGPASSWORD="$DB_PASSWORD" \
  createdb --maintenance-db="$DB_MAINTENANCE" "$TEMP_DB"

# ── Run smoke suite ──────────────────────────────────────────────────
echo "[wrapper] delegating to run_smoke.sh"
echo "────────────────────────────────────────"

SMOKE_EXIT=0
PGHOST=127.0.0.1 PGPORT="$PROXY_PORT" PGUSER="$DB_USER" \
  PGPASSWORD="$DB_PASSWORD" PGDATABASE="$TEMP_DB" \
  "$SCRIPT_DIR/run_smoke.sh" 2>&1 | tee "${RUN_LOG_DIR}/smoke.log" || SMOKE_EXIT=$?

echo "────────────────────────────────────────"

# ── Summary ──────────────────────────────────────────────────────────
echo ""
echo "┌─────────────────────────────────────────"
echo "│ Cloud SQL Smoke Test Summary"
echo "├─────────────────────────────────────────"
echo "│ Environment:  $TARGET_ENV"
echo "│ Instance:     $INSTANCE_CONN"
echo "│ Project:      $PROJECT_ID"
echo "│ Temp DB:      $TEMP_DB"
if [[ "$SMOKE_EXIT" -eq 0 ]]; then
  echo "│ Result:       PASS"
else
  echo "│ Result:       FAIL (exit code $SMOKE_EXIT)"
fi
echo "│ Logs:         $RUN_LOG_DIR"
echo "└─────────────────────────────────────────"

exit "$SMOKE_EXIT"
