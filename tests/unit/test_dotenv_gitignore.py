"""Regression test: real `.env` secrets are git-ignored at every depth, while
`.env.example` templates stay tracked.

Locks the secret-safety property behind the Neon `cp .env.example .env` workflow
(docs/runbooks/10-neon-operations.md). If someone weakens the `.gitignore`
dotenv rules, this fails instead of silently exposing credentials.

The nested-depth cases use REAL temporary files created inside the work tree
(not hypothetical path strings), so `git check-ignore` validates actual behavior
rather than a literal that happens to be unmatched.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or not (REPO_ROOT / ".git").exists(),
    reason="requires a git work tree",
)


def _is_ignored(rel_path: str) -> bool:
    """True if git would ignore `rel_path` (evaluated against the repo's .gitignore).

    A return of 0 (ignored) definitively proves a `.gitignore` pattern matches the
    path, regardless of whether the file exists — so this is a sound check for the
    IGNORED direction. For the TRACKED direction we back it with real files (below),
    since "not ignored" alone does not prove a file is actually committed.
    """
    result = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", rel_path],
        cwd=REPO_ROOT,
    )
    assert result.returncode in (0, 1), f"git check-ignore errored for {rel_path}"
    return result.returncode == 0


# Root-level secret files that MUST be ignored. A positive check-ignore match is
# meaningful independent of file existence.
ROOT_IGNORED = [".env", ".env.local", ".env.production"]


@pytest.mark.parametrize("rel_path", ROOT_IGNORED)
def test_root_env_secret_files_are_ignored(rel_path: str) -> None:
    assert _is_ignored(rel_path), f"{rel_path} must be git-ignored to prevent secret leaks"


def test_committed_env_example_is_present_and_tracked() -> None:
    """The real template must exist and not be ignored (so it can be committed)."""
    template = REPO_ROOT / ".env.example"
    assert template.exists(), ".env.example template is missing"
    assert not _is_ignored(".env.example"), ".env.example must stay tracked"


def test_nested_dotenv_behavior_with_real_files() -> None:
    """Create real files at depth inside the work tree and assert git's actual
    ignore decision: nested secrets ignored, nested template tracked."""
    nested = REPO_ROOT / "_dotenv_gitignore_test_tmp" / "svc"
    try:
        nested.mkdir(parents=True, exist_ok=False)
        (nested / ".env").write_text("SECRET=x\n")
        (nested / ".env.production").write_text("SECRET=x\n")
        (nested / ".env.example").write_text("SECRET=\n")

        base = nested.relative_to(REPO_ROOT).as_posix()
        assert _is_ignored(f"{base}/.env"), "nested .env secret must be ignored"
        assert _is_ignored(f"{base}/.env.production"), "nested .env.* secret must be ignored"
        assert not _is_ignored(f"{base}/.env.example"), "nested .env.example must stay tracked"
    finally:
        shutil.rmtree(REPO_ROOT / "_dotenv_gitignore_test_tmp", ignore_errors=True)
