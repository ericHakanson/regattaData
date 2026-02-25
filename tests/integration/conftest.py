"""Integration test fixtures.

Applies migrations 0001–0006 against an ephemeral PostgreSQL database
provided by pytest-postgresql before any integration test runs.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest
from pytest_postgresql import factories

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent.parent
MIGRATIONS = [
    PROJECT_ROOT / "migrations" / "0001_extensions.sql",
    PROJECT_ROOT / "migrations" / "0002_core_entities.sql",
    PROJECT_ROOT / "migrations" / "0003_relationships.sql",
    PROJECT_ROOT / "migrations" / "0004_event_ops.sql",
    PROJECT_ROOT / "migrations" / "0005_resolution_and_actions.sql",
    PROJECT_ROOT / "migrations" / "0006_views.sql",
    PROJECT_ROOT / "migrations" / "0007_jotform_tables.sql",
    PROJECT_ROOT / "migrations" / "0008_mailchimp_tables.sql",
    PROJECT_ROOT / "migrations" / "0009_airtable_copy_tables.sql",
    PROJECT_ROOT / "migrations" / "0010_yacht_scoring_tables.sql",
    PROJECT_ROOT / "migrations" / "0011_candidate_canonical_core.sql",
    PROJECT_ROOT / "migrations" / "0012_canonical_tables.sql",
    PROJECT_ROOT / "migrations" / "0013_manual_apply_schema.sql",
    PROJECT_ROOT / "migrations" / "0014_state_machine_constraints.sql",
    PROJECT_ROOT / "migrations" / "0015_provenance_nba_lifecycle.sql",
    PROJECT_ROOT / "migrations" / "0016_lineage_coverage.sql",
    PROJECT_ROOT / "migrations" / "0017_index_additions.sql",
    PROJECT_ROOT / "migrations" / "0018_bhyc_member_directory_tables.sql",
]

# ---------------------------------------------------------------------------
# pytest-postgresql process fixture
# ---------------------------------------------------------------------------

postgresql_proc = factories.postgresql_proc()
postgresql = factories.postgresql("postgresql_proc")


# ---------------------------------------------------------------------------
# Schema fixture — applies all migrations once per test session
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def db_conn(postgresql):
    """Return a psycopg connection with schema applied.

    Each test gets a fresh schema via function scope so tests are isolated.
    """
    dsn = (
        f"host={postgresql.info.host} "
        f"port={postgresql.info.port} "
        f"dbname={postgresql.info.dbname} "
        f"user={postgresql.info.user} "
        f"password={postgresql.info.password or ''}"
    )
    conn = psycopg.connect(dsn, autocommit=True)
    try:
        for migration in MIGRATIONS:
            sql = migration.read_text(encoding="utf-8")
            conn.execute(sql)
        conn.autocommit = False
        yield conn, dsn
    finally:
        conn.close()
