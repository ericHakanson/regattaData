# Secrets Hygiene Runbook

## Goal
Prevent accidental commit of credentials (API keys, DSNs, private keys) while developing and reviewing.

## 1) Local File Hygiene
Keep secrets in local-only files:
1. `/env`
2. `.env.local`

Never store live secrets in:
1. `docs/`
2. `config/`
3. `tests/`
4. committed shell scripts

## 2) Pre-Commit Secret Scan
Run:
```bash
./scripts/qa_secret_scan.sh
```

This scans tracked files for common secret signatures and fails non-zero on high-risk matches.

## 3) Verify Ignore Rules
```bash
git check-ignore -v env .env .env.local || true
git status --short
```

## 4) If a Secret Was Committed
1. Rotate/revoke the credential immediately at provider.
2. Remove from git history (BFG/filter-repo) if required by policy.
3. Force new credential rollout and update local env files only.
4. Add a regression test/check command to prevent recurrence.

## 5) CI/Review Gate
Before merge:
1. `./scripts/qa_secret_scan.sh`
2. Ensure no tracked file contains credentials by spot-checking PR diff.
