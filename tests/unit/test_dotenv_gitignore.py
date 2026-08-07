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

import re
import shutil
import subprocess
from pathlib import Path

import pytest

# A dotenv secret is `.env` or `.env.<x>` as the final path component, at any depth —
# but never a `.env.example` template. Evaluated against the full relative path.
# `[^/]*` (zero-or-more) mirrors the gitignore glob `**/.env.*`, which also matches a
# trailing-dot name like `.env.` (empty suffix).
_SECRET_DOTENV_RE = re.compile(r"(?:^|/)\.env(?:\.[^/]*)?$")
_ENV_EXAMPLE_RE = re.compile(r"(?:^|/)\.env\.example$")

def _find_repo_root() -> Path | None:
    """Locate the repo root robustly, without assuming a fixed parent depth.

    Prefer git's own answer; else walk upward for repo markers
    (`.gitignore` + `pyproject.toml`). Return None if neither works, so the
    caller skips the module cleanly rather than guessing a directory depth.
    """
    here = Path(__file__).resolve()
    try:
        top = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=here.parent, capture_output=True, text=True, check=True,
        ).stdout.strip()
        if top and (Path(top) / ".gitignore").exists():
            return Path(top)
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        pass
    for parent in here.parents:
        if (parent / ".gitignore").exists() and (parent / "pyproject.toml").exists():
            return parent
    return None


REPO_ROOT = _find_repo_root()
if REPO_ROOT is None:
    pytest.skip(
        "cannot locate repo root (.gitignore + pyproject.toml)",
        allow_module_level=True,
    )

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

# Non-secret files that merely share an `env` prefix and MUST NOT be ignored. Guards
# against an over-broad `env.*`-style rule silently hiding unrelated code/config.
NON_DOTENV_TRACKED = [
    "env.schema.json",
    "config/env.yaml",
    "env.prod/config.yml",
    "README.env",
]


def _is_ignored(rel_path: str, *, cwd: Path) -> bool:
    """True if git ignores `rel_path` under `cwd`.

    Output-based and portable across git versions: `git check-ignore` prints the
    matched path and exits 0 when ignored, prints nothing and exits 1 when not
    ignored. Any other exit code is a genuine invocation error (raised, not silently
    treated as a pass/fail of the policy).
    """
    result = subprocess.run(
        ["git", "check-ignore", "--", rel_path],
        cwd=cwd, capture_output=True, text=True,
    )
    if result.returncode == 0:
        return bool(result.stdout.strip())
    if result.returncode == 1:
        return False
    raise RuntimeError(
        f"git check-ignore failed for {rel_path!r} "
        f"(exit {result.returncode}): {result.stderr.strip()}"
    )


@pytest.fixture
def seeded_repo(tmp_path: Path) -> Path:
    """An isolated git repo seeded with the repo's real .gitignore and real
    dotenv files at root + depth. Auto-cleaned by pytest's tmp_path."""
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    shutil.copyfile(GITIGNORE, tmp_path / ".gitignore")
    for rel in IGNORED + TRACKED + NON_DOTENV_TRACKED:
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


@pytest.mark.parametrize("rel_path", NON_DOTENV_TRACKED)
def test_non_secret_env_prefixed_files_are_not_ignored(seeded_repo: Path, rel_path: str) -> None:
    assert not _is_ignored(rel_path, cwd=seeded_repo), (
        f"{rel_path} is not a dotenv secret and must not be ignored"
    )


def test_repo_ships_a_real_env_example_template() -> None:
    """Sanity: the committed template actually exists in the repo."""
    assert (REPO_ROOT / ".env.example").exists(), "repo must ship a .env.example template"


def _is_secret_dotenv_path(rel_path: str) -> bool:
    """True if the full relative path names a dotenv secret (`.env`/`.env.*` at any
    depth), excluding `.env.example` templates."""
    return bool(_SECRET_DOTENV_RE.search(rel_path)) and not _ENV_EXAMPLE_RE.search(rel_path)


@pytest.mark.parametrize(
    "path,is_secret",
    [
        (".env", True),
        (".env.local", True),
        (".env.production", True),
        (".env.", True),                 # trailing-dot: gitignore `.env.*` matches it
        ("services/api/.env.", True),
        ("services/api/.env", True),
        ("deep/nested/dir/.env.production", True),
        (".env.example", False),
        ("services/api/.env.example", False),
        ("README.env", False),      # not a dotenv secret (no leading-dot .env segment)
        ("config/env.sample", False),  # `env`, not `.env`
        # Boundary cases: `.env` must be the FINAL path component.
        (".envx", False),               # `.envx` is not `.env`
        (".env.backup/file", False),    # `.env.backup` is a dir, not the final leaf
        ("dir/.env.local", True),
        # `.env.example.bak` is NOT the pristine template — gitignore `**/.env.*`
        # ignores it, so it must be treated as a secret if ever tracked.
        ("foo/.env.example.bak", True),
    ],
)
def test_secret_dotenv_path_predicate(path: str, is_secret: bool) -> None:
    assert _is_secret_dotenv_path(path) is is_secret


def test_no_secret_dotenv_files_are_tracked() -> None:
    """`.gitignore` cannot protect files already committed to the index. Guard against
    that gap directly: no real `.env` / `.env.*` secret file may be tracked at any depth
    (`.env.example` templates are allowed). If this fails, purge with `git rm --cached`."""
    inside = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        pytest.skip("not inside a git work tree (cannot inspect the tracked set)")

    tracked = subprocess.run(
        ["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout.splitlines()
    offenders = [p for p in tracked if _is_secret_dotenv_path(p)]
    assert not offenders, (
        "secret dotenv files are tracked (gitignore won't protect them); "
        f"remove with `git rm --cached`: {offenders}"
    )
