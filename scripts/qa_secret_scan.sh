#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT

echo "Running lightweight secret scan on tracked files..."

# 1) Known high-risk signatures
git ls-files -z | xargs -0 rg -n --pcre2 \
  -e '-----BEGIN (RSA|EC|OPENSSH|PGP) PRIVATE KEY-----' \
  -e '\b[A-Fa-f0-9]{32}-us[0-9]{1,2}\b' \
  -e 'AKIA[0-9A-Z]{16}' \
  -e 'AIza[0-9A-Za-z\-_]{35}' \
  -e 'xox[baprs]-[0-9A-Za-z\-]{10,}' \
  -e 'ghp_[0-9A-Za-z]{36}' \
  > "$TMP_FILE" || true

# 2) Dangerous assignment patterns (high false-positive risk, shown separately)
git ls-files -z | xargs -0 rg -n --pcre2 \
  -e '(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*["'\'']?[A-Za-z0-9_\-\/+=]{16,}' \
  > "${TMP_FILE}.soft" || true

HARD_COUNT="$(wc -l < "$TMP_FILE" | tr -d ' ')"
SOFT_COUNT="$(wc -l < "${TMP_FILE}.soft" | tr -d ' ')"

if [[ "$HARD_COUNT" -gt 0 ]]; then
  echo
  echo "HIGH-RISK MATCHES:"
  cat "$TMP_FILE"
  echo
  echo "Secret scan FAILED."
  exit 1
fi

echo "No high-risk signatures found."
if [[ "$SOFT_COUNT" -gt 0 ]]; then
  echo
  echo "Review these potential secret assignments (may be false positives):"
  cat "${TMP_FILE}.soft"
fi

echo "Secret scan complete."
