#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-regattadata}"
INSTANCE_ID="${INSTANCE_ID:-regatta-data}"
PORT="${PORT:-5433}"
REPLACE=0
BACKGROUND=0

for arg in "$@"; do
  case "$arg" in
    --replace) REPLACE=1 ;;
    --background) BACKGROUND=1 ;;
    *)
      echo "Unknown arg: $arg" >&2
      echo "Usage: $0 [--replace] [--background]" >&2
      exit 2
      ;;
  esac
done

command -v gcloud >/dev/null || { echo "gcloud not found" >&2; exit 1; }
command -v cloud-sql-proxy >/dev/null || { echo "cloud-sql-proxy not found" >&2; exit 1; }

echo "Verifying gcloud auth..."
gcloud auth print-access-token >/dev/null
gcloud auth application-default print-access-token >/dev/null

INSTANCE_CONN="$(gcloud sql instances describe "$INSTANCE_ID" --project "$PROJECT_ID" --format='value(connectionName)')"
if [[ -z "$INSTANCE_CONN" ]]; then
  echo "Failed to resolve connection name for ${PROJECT_ID}/${INSTANCE_ID}" >&2
  exit 1
fi

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  if [[ "$REPLACE" -eq 1 ]]; then
    echo "Port $PORT is in use; replacing existing proxy listeners..."
    pkill -f "cloud-sql-proxy.*${PORT}" || true
    sleep 1
  else
    echo "Port $PORT already in use. Re-run with --replace to kill old proxy." >&2
    lsof -nP -iTCP:"$PORT" -sTCP:LISTEN || true
    exit 1
  fi
fi

echo "Starting proxy for $INSTANCE_CONN on 127.0.0.1:$PORT"
if [[ "$BACKGROUND" -eq 1 ]]; then
  LOG="/tmp/cloud-sql-proxy-${PORT}.log"
  nohup cloud-sql-proxy --port "$PORT" "$INSTANCE_CONN" >"$LOG" 2>&1 &
  echo "Proxy started in background. PID=$! LOG=$LOG"
else
  exec cloud-sql-proxy --port "$PORT" "$INSTANCE_CONN"
fi
