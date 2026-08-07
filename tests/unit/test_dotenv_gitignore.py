"""Regression test: real `.env` secrets are git-ignored at every depth, while
`.env.example` templates stay tracked.

Locks the secret-safety property behind the Neon `cp .env.example .env` workflow
(docs/runbooks/10-neon-operations.md). If someone weakens the `.gitignore`
dotenv rules, this fails instead of silently exposing credentials.

Depth behavior is validated against the repo's ACTUAL `.gitignore` content, copied
into an isolated throwaway git repo under pytest's per-test `tmp_path` (unique and
auto-cleaned), with REAL files — so it is deterministic, parallel-safe, and never
touches or pollutes the real work tree.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GITIGNORE = REPO_ROOT / ".gitignore"

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or not (REPO_ROOT / ".git").exists(),
    reason="requires git",
)


def _is_ignored(path: str, *, cwd: Path) -> bool:
    """True if git would ignore `path` under `cwd`'s ignore rules.

    Return 0 = ignored, 1 = not ignored; anything else is an error.
    """
    result = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", path],
        cwd=cwd,
    )
    assert result.returncode in (0, 1), f"git check-ignore errored for {path}"
    return result.returncode == 0


# Root-level secret files that MUST be ignored. A positive check-ignore match is
# meaningful independent of file existence.
ROOT_IGNORED = [".env", ".env.local", ".env.production"]


@pytest.mark.parametrize("rel_path", ROOT_IGNORED)
def test_root_env_secret_files_are_ignored(rel_path: str) -> None:
    assert _is_ignored(rel_path, cwd=REPO_ROOT), (
        f"{rel_path} must be git-ignored to prevent secret leaks"
    )


def test_committed_env_example_is_present_and_tracked() -> None:
    """The real template must exist and not be ignored (so it can be committed)."""
    assert (REPO_ROOT / ".env.example").exists(), ".env.example template is missing"
    assert not _is_ignored(".env.example", cwd=REPO_ROOT), ".env.example must stay tracked"


def test_nested_dotenv_behavior_against_real_gitignore(tmp_path: Path) -> None:
    """Copy the repo's real .gitignore into an isolated throwaway repo and assert
    git's actual ignore decision on REAL files created at depth."""
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    shutil.copyfile(GITIGNORE, tmp_path / ".gitignore")

    svc = tmp_path / "services" / "api"
    svc.mkdir(parents=True)
    (svc / ".env").write_text("SECRET=x\n")
    (svc / ".env.production").write_text("SECRET=x\n")
    (svc / ".env.example").write_text("SECRET=\n")

    assert _is_ignored("services/api/.env", cwd=tmp_path), "nested .env secret must be ignored"
    assert _is_ignored("services/api/.env.production", cwd=tmp_path), (
        "nested .env.* secret must be ignored"
    )
    assert not _is_ignored("services/api/.env.example", cwd=tmp_path), (
        "nested .env.example template must stay tracked"
    )
