"""Regression test: the repo's `.gitignore` ignores real `.env` secrets at every
depth, while `.env.example` templates stay tracked.

Locks the secret-safety property behind the Neon `cp .env.example .env` workflow
(docs/runbooks/10-neon-operations.md). If someone weakens the `.gitignore`
dotenv rules, this fails instead of silently exposing credentials.

Design notes:
- Self-contained: needs only the `git` binary and the repo's `.gitignore` file.
  It does NOT depend on this checkout having a `.git` directory, so it still runs
  (rather than silently skipping) in source-export / packaged / sandboxed contexts.
- Deterministic + parallel-safe: every assertion runs inside an isolated throwaway
  git repo under pytest's per-test `tmp_path`, seeded with the repo's real
  `.gitignore` and REAL files at root and nested depth.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GITIGNORE = REPO_ROOT / ".gitignore"

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or not GITIGNORE.exists(),
    reason="requires the git binary and the repo .gitignore",
)

# Secret files that MUST be ignored — at root and nested depth.
IGNORED = [
    ".env",
    ".env.local",
    ".env.production",
    "services/api/.env",
    "services/api/.env.local",
    "deep/nested/dir/.env.production",
]

# Template files that MUST stay tracked — at root and nested depth.
TRACKED = [
    ".env.example",
    "services/api/.env.example",
]


def _is_ignored(rel_path: str, *, cwd: Path) -> bool:
    """0 = ignored, 1 = not ignored; anything else is an error."""
    result = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", rel_path],
        cwd=cwd,
    )
    assert result.returncode in (0, 1), f"git check-ignore errored for {rel_path}"
    return result.returncode == 0


@pytest.fixture
def seeded_repo(tmp_path: Path) -> Path:
    """An isolated git repo seeded with the repo's real .gitignore and real
    dotenv files at root + depth. Auto-cleaned by pytest's tmp_path."""
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    shutil.copyfile(GITIGNORE, tmp_path / ".gitignore")
    for rel in IGNORED + TRACKED:
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("SECRET=x\n")
    return tmp_path


@pytest.mark.parametrize("rel_path", IGNORED)
def test_secret_files_ignored_at_any_depth(seeded_repo: Path, rel_path: str) -> None:
    assert _is_ignored(rel_path, cwd=seeded_repo), (
        f"{rel_path} must be git-ignored to prevent secret leaks"
    )


@pytest.mark.parametrize("rel_path", TRACKED)
def test_example_templates_tracked_at_any_depth(seeded_repo: Path, rel_path: str) -> None:
    assert not _is_ignored(rel_path, cwd=seeded_repo), (
        f"{rel_path} template must stay tracked (never ignored)"
    )


def test_repo_ships_a_real_env_example_template() -> None:
    """Sanity: the committed template actually exists in the repo."""
    assert (REPO_ROOT / ".env.example").exists(), "repo must ship a .env.example template"
