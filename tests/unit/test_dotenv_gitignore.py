"""Regression test: real `.env` secrets are git-ignored at every depth, while
`.env.example` templates stay tracked.

Locks the secret-safety property behind the Neon `cp .env.example .env` workflow
(docs/runbooks/10-neon-operations.md). If someone weakens the `.gitignore`
dotenv rules, this fails instead of silently exposing credentials.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Real env files that MUST be ignored — root and nested (secrets live in both).
IGNORED = [
    ".env",
    ".env.local",
    ".env.production",
    "service/.env",
    "apps/api/.env.local",
    "deep/nested/dir/.env",
]

# Templates that MUST stay tracked — root and nested.
TRACKED = [
    ".env.example",
    "services/x/.env.example",
]


def _is_ignored(rel_path: str) -> bool:
    """True if git would ignore `rel_path` (evaluated against the repo's .gitignore)."""
    result = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", rel_path],
        cwd=REPO_ROOT,
    )
    # 0 = ignored, 1 = not ignored, other = error
    assert result.returncode in (0, 1), f"git check-ignore errored for {rel_path}"
    return result.returncode == 0


pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or not (REPO_ROOT / ".git").exists(),
    reason="requires a git work tree",
)


@pytest.mark.parametrize("rel_path", IGNORED)
def test_env_secret_files_are_ignored(rel_path: str) -> None:
    assert _is_ignored(rel_path), f"{rel_path} must be git-ignored to prevent secret leaks"


@pytest.mark.parametrize("rel_path", TRACKED)
def test_env_example_templates_are_tracked(rel_path: str) -> None:
    assert not _is_ignored(rel_path), f"{rel_path} must stay tracked (it is a template)"
