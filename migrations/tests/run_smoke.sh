#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

MIGRATIONS=(
  "0001_extensions.sql"
  "0002_core_entities.sql"
  "0003_relationships.sql"
  "0004_event_ops.sql"
  "0005_resolution_and_actions.sql"
  "0006_views.sql"
)
TESTS=(
  "01_seed.sql"
  "02_constraints.sql"
  "03_views.sql"
)

# When PGDATABASE is set (e.g. by run_smoke_cloudsql.sh), use the
# pre-provisioned database and skip local create/drop.  Otherwise
# create a throwaway local database as before.
if [[ -n "${PGDATABASE:-}" ]]; then
  DB_NAME="$PGDATABASE"
  OWNS_DB=false
else
  DB_NAME="regatta_smoke_$(date +%s)_$RANDOM"
  OWNS_DB=true
fi

cleanup() {
  if [[ "$OWNS_DB" == "true" ]]; then
    dropdb "$DB_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "$OWNS_DB" == "true" ]]; then
  createdb "$DB_NAME"
fi

echo "[smoke] using temporary database: $DB_NAME"

for migration in "${MIGRATIONS[@]}"; do
  echo "[smoke] apply migrations/$migration"
  psql -v ON_ERROR_STOP=1 -d "$DB_NAME" -f "$ROOT_DIR/migrations/$migration" >/dev/null
  echo "[smoke] ok migrations/$migration"
done

for test_sql in "${TESTS[@]}"; do
  echo "[smoke] run migrations/tests/$test_sql"
  psql -v ON_ERROR_STOP=1 -d "$DB_NAME" -f "$ROOT_DIR/migrations/tests/$test_sql" >/dev/null
  echo "[smoke] ok migrations/tests/$test_sql"
done

echo "[smoke] PASS"
