#!/usr/bin/env bash
set -euo pipefail

: "${DB_DSN:?DB_DSN is required}"

echo "Checking postgres connectivity..."
psql "$DB_DSN" -v ON_ERROR_STOP=1 -c \
"select current_database() as db, current_user as usr, inet_server_addr() as host, inet_server_port() as port, now();"

echo
echo "Checking candidate/canonical table presence..."
psql "$DB_DSN" -v ON_ERROR_STOP=1 -c "
SELECT
  to_regclass('public.candidate_participant') AS candidate_participant,
  to_regclass('public.candidate_yacht') AS candidate_yacht,
  to_regclass('public.candidate_club') AS candidate_club,
  to_regclass('public.candidate_event') AS candidate_event,
  to_regclass('public.candidate_registration') AS candidate_registration,
  to_regclass('public.canonical_participant') AS canonical_participant,
  to_regclass('public.canonical_yacht') AS canonical_yacht,
  to_regclass('public.canonical_club') AS canonical_club,
  to_regclass('public.canonical_event') AS canonical_event,
  to_regclass('public.canonical_registration') AS canonical_registration;"

echo
echo "DB check complete."
