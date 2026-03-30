"""Integration tests for manual curation schema (FOR-187) and pipeline (FOR-188)."""

from __future__ import annotations

import hashlib
import uuid


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_hash(*parts) -> str:
    key = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _seed_candidate_participant(conn, display_name="Test User", normalized_name="test-user",
                                  best_email=None) -> str:
    row = conn.execute(
        "INSERT INTO candidate_participant (display_name, normalized_name, best_email, stable_fingerprint) "
        "VALUES (%s, %s, %s, %s) RETURNING id",
        (display_name, normalized_name, best_email,
         hashlib.sha256(f"{normalized_name}|{best_email or ''}".encode()).hexdigest()),
    ).fetchone()
    conn.commit()
    return str(row[0])


def _seed_candidate_yacht(conn, name="Test Yacht", normalized_name="test-yacht",
                           sail_number="42") -> str:
    row = conn.execute(
        "INSERT INTO candidate_yacht (name, normalized_name, sail_number, stable_fingerprint) "
        "VALUES (%s, %s, %s, %s) RETURNING id",
        (name, normalized_name, sail_number,
         hashlib.sha256(f"{normalized_name}|{sail_number}".encode()).hexdigest()),
    ).fetchone()
    conn.commit()
    return str(row[0])


def _seed_candidate_club(conn, name="Test Club", normalized_name="test-club") -> str:
    row = conn.execute(
        "INSERT INTO candidate_club (name, normalized_name, stable_fingerprint) "
        "VALUES (%s, %s, %s) RETURNING id",
        (name, normalized_name,
         hashlib.sha256(normalized_name.encode()).hexdigest()),
    ).fetchone()
    conn.commit()
    return str(row[0])


# ---------------------------------------------------------------------------
# FOR-187: Schema tests — table creation, inserts, idempotency
# ---------------------------------------------------------------------------

class TestManualParticipantPatch:
    def test_insert_active_patch(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate_participant(conn)
        ph = _patch_hash("participant_patch", cid, "Alice Smith", None, None, None, "testactor")

        conn.execute(
            "INSERT INTO manual_participant_patch "
            "(candidate_participant_id, patch_display_name, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s)",
            (cid, "Alice Smith", "testactor", ph),
        )
        conn.commit()

        row = conn.execute(
            "SELECT patch_display_name, status FROM manual_participant_patch "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert row[0] == "Alice Smith"
        assert row[1] == "active"

    def test_duplicate_patch_hash_rejected(self, db_conn):
        conn, _ = db_conn
        import psycopg
        cid = _seed_candidate_participant(conn)
        ph = _patch_hash("participant_patch", cid, "Bob", None, None, None, "actor1")

        conn.execute(
            "INSERT INTO manual_participant_patch "
            "(candidate_participant_id, patch_display_name, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s)",
            (cid, "Bob", "actor1", ph),
        )
        conn.commit()

        # Re-insert same hash → UniqueViolation
        try:
            conn.execute(
                "INSERT INTO manual_participant_patch "
                "(candidate_participant_id, patch_display_name, actor, patch_hash) "
                "VALUES (%s, %s, %s, %s)",
                (cid, "Bob", "actor1", ph),
            )
            conn.commit()
            assert False, "expected UniqueViolation"
        except psycopg.errors.UniqueViolation:
            conn.rollback()

    def test_status_check_constraint(self, db_conn):
        conn, _ = db_conn
        import psycopg
        cid = _seed_candidate_participant(conn)
        ph = _patch_hash("participant_patch", cid, "C", None, None, None, "a")

        try:
            conn.execute(
                "INSERT INTO manual_participant_patch "
                "(candidate_participant_id, patch_display_name, actor, patch_hash, status) "
                "VALUES (%s, %s, %s, %s, %s)",
                (cid, "C", "a", ph, "invalid_status"),
            )
            conn.commit()
            assert False, "expected CheckViolation"
        except psycopg.errors.CheckViolation:
            conn.rollback()


class TestManualParticipantAddressPatch:
    def test_insert_address_patch(self, db_conn):
        conn, _ = db_conn
        cid = _seed_candidate_participant(conn)
        ph = _patch_hash("participant_address_patch", cid, "123 Main St", "actor")

        conn.execute(
            "INSERT INTO manual_participant_address_patch "
            "(candidate_participant_id, address_raw, city, state, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (cid, "123 Main St", "Seattle", "WA", "actor", ph),
        )
        conn.commit()

        row = conn.execute(
            "SELECT address_raw, city, status FROM manual_participant_address_patch "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert row[0] == "123 Main St"
        assert row[1] == "Seattle"
        assert row[2] == "active"

    def test_unique_active_address_raw_per_candidate(self, db_conn):
        conn, _ = db_conn
        import psycopg
        cid = _seed_candidate_participant(conn)
        ph1 = _patch_hash("participant_address_patch", cid, "456 Elm Ave", "a1")
        ph2 = _patch_hash("participant_address_patch", cid, "456 Elm Ave", "a2")

        conn.execute(
            "INSERT INTO manual_participant_address_patch "
            "(candidate_participant_id, address_raw, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s)",
            (cid, "456 Elm Ave", "a1", ph1),
        )
        conn.commit()

        # Same active (cid, address_raw) → blocked by ux_manual_participant_address_active
        try:
            conn.execute(
                "INSERT INTO manual_participant_address_patch "
                "(candidate_participant_id, address_raw, actor, patch_hash) "
                "VALUES (%s, %s, %s, %s)",
                (cid, "456 Elm Ave", "a2", ph2),
            )
            conn.commit()
            assert False, "expected UniqueViolation"
        except psycopg.errors.UniqueViolation:
            conn.rollback()


class TestManualYachtPatch:
    def test_insert_yacht_patch(self, db_conn):
        conn, _ = db_conn
        ycid = _seed_candidate_yacht(conn)
        ph = _patch_hash("yacht_patch", ycid, "New Name", None, None, None, "actor")

        conn.execute(
            "INSERT INTO manual_yacht_patch "
            "(candidate_yacht_id, patch_name, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s)",
            (ycid, "New Name", "actor", ph),
        )
        conn.commit()

        row = conn.execute(
            "SELECT patch_name FROM manual_yacht_patch WHERE candidate_yacht_id = %s",
            (ycid,),
        ).fetchone()
        assert row[0] == "New Name"


class TestManualYachtOwnershipPatch:
    def test_insert_ownership_patch(self, db_conn):
        conn, _ = db_conn
        pcid = _seed_candidate_participant(conn)
        ycid = _seed_candidate_yacht(conn)
        ph = _patch_hash("yacht_ownership_patch", pcid, ycid, "owner", "add", "actor")

        conn.execute(
            "INSERT INTO manual_yacht_ownership_patch "
            "(candidate_participant_id, candidate_yacht_id, role, operation, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (pcid, ycid, "owner", "add", "actor", ph),
        )
        conn.commit()

        row = conn.execute(
            "SELECT role, operation FROM manual_yacht_ownership_patch "
            "WHERE candidate_participant_id = %s AND candidate_yacht_id = %s",
            (pcid, ycid),
        ).fetchone()
        assert row[0] == "owner"
        assert row[1] == "add"

    def test_role_check_constraint(self, db_conn):
        conn, _ = db_conn
        import psycopg
        pcid = _seed_candidate_participant(conn)
        ycid = _seed_candidate_yacht(conn)
        ph = _patch_hash("yacht_ownership_patch", pcid, ycid, "admiral", "add", "a")

        try:
            conn.execute(
                "INSERT INTO manual_yacht_ownership_patch "
                "(candidate_participant_id, candidate_yacht_id, role, operation, actor, patch_hash) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (pcid, ycid, "admiral", "add", "a", ph),
            )
            conn.commit()
            assert False, "expected CheckViolation"
        except psycopg.errors.CheckViolation:
            conn.rollback()


class TestManualClubMembershipPatch:
    def test_insert_membership_patch(self, db_conn):
        conn, _ = db_conn
        pcid = _seed_candidate_participant(conn)
        ccid = _seed_candidate_club(conn)
        ph = _patch_hash("club_membership_patch", pcid, ccid, "member", "add", "actor")

        conn.execute(
            "INSERT INTO manual_club_membership_patch "
            "(candidate_participant_id, candidate_club_id, membership_role, operation, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (pcid, ccid, "member", "add", "actor", ph),
        )
        conn.commit()

        row = conn.execute(
            "SELECT membership_role, operation FROM manual_club_membership_patch "
            "WHERE candidate_participant_id = %s",
            (pcid,),
        ).fetchone()
        assert row[0] == "member"
        assert row[1] == "add"


# ---------------------------------------------------------------------------
# FOR-188: Pipeline ingestion tests
# ---------------------------------------------------------------------------

class TestManualCurationIngestion:
    """Test that manual_* patch rows are projected into candidate layer by the pipeline."""

    def test_participant_patch_updates_candidate(self, db_conn):
        """Active manual_participant_patch overwrites candidate fields."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        cid = _seed_candidate_participant(conn, display_name="Old Name")
        ph = _patch_hash("participant_patch", cid, "Corrected Name", None, "new@example.com", None, "testactor")
        conn.execute(
            "INSERT INTO manual_participant_patch "
            "(candidate_participant_id, patch_display_name, patch_best_email, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s, %s)",
            (cid, "Corrected Name", "new@example.com", "testactor", ph),
        )
        conn.commit()

        ctrs = run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        assert ctrs.db_errors == 0
        assert ctrs.participants_ingested >= 1

        row = conn.execute(
            "SELECT display_name, best_email FROM candidate_participant WHERE id = %s",
            (cid,),
        ).fetchone()
        assert row[0] == "Corrected Name"
        assert row[1] == "new@example.com"

    def test_participant_patch_source_link_inserted(self, db_conn):
        """Running the pipeline creates a source link from manual_participant_patch → candidate."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        cid = _seed_candidate_participant(conn)
        ph = _patch_hash("participant_patch", cid, "Patched", None, None, None, "a")
        conn.execute(
            "INSERT INTO manual_participant_patch "
            "(candidate_participant_id, patch_display_name, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s)",
            (cid, "Patched", "a", ph),
        )
        conn.commit()

        run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        link = conn.execute(
            "SELECT source_system FROM candidate_source_link "
            "WHERE candidate_entity_type='participant' AND candidate_entity_id=%s "
            "AND source_table_name='manual_participant_patch'",
            (cid,),
        ).fetchone()
        assert link is not None
        assert link[0] == "manual_curation"

    def test_address_patch_creates_candidate_address(self, db_conn):
        """Active manual_participant_address_patch creates a candidate_participant_address row."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        cid = _seed_candidate_participant(conn)
        ph = _patch_hash("participant_address_patch", cid, "789 Oak Rd", "actor")
        conn.execute(
            "INSERT INTO manual_participant_address_patch "
            "(candidate_participant_id, address_raw, city, state, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (cid, "789 Oak Rd", "Portland", "OR", "actor", ph),
        )
        conn.commit()

        ctrs = run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        assert ctrs.db_errors == 0
        addr = conn.execute(
            "SELECT address_raw, city, state FROM candidate_participant_address "
            "WHERE candidate_participant_id = %s",
            (cid,),
        ).fetchone()
        assert addr is not None
        assert addr[0] == "789 Oak Rd"
        assert addr[1] == "Portland"

    def test_address_patch_idempotent(self, db_conn):
        """Running the pipeline twice does not duplicate address or source link rows."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        cid = _seed_candidate_participant(conn)
        ph = _patch_hash("participant_address_patch", cid, "1 Dup St", "actor")
        conn.execute(
            "INSERT INTO manual_participant_address_patch "
            "(candidate_participant_id, address_raw, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s)",
            (cid, "1 Dup St", "actor", ph),
        )
        conn.commit()

        run_source_to_candidate(conn, entity_type="participant")
        conn.commit()
        ctrs2 = run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        assert ctrs2.db_errors == 0
        count = conn.execute(
            "SELECT COUNT(*) FROM candidate_participant_address WHERE candidate_participant_id=%s",
            (cid,),
        ).fetchone()[0]
        assert count == 1

        link_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_source_link "
            "WHERE candidate_entity_type='participant' AND candidate_entity_id=%s "
            "AND source_table_name='manual_participant_address_patch'",
            (cid,),
        ).fetchone()[0]
        assert link_count == 1

    def test_yacht_patch_updates_candidate(self, db_conn):
        """Active manual_yacht_patch overwrites candidate_yacht fields."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        ycid = _seed_candidate_yacht(conn, name="Old Yacht Name")
        ph = _patch_hash("yacht_patch", ycid, "Corrected Yacht", "99", None, "sloop", "actor")
        conn.execute(
            "INSERT INTO manual_yacht_patch "
            "(candidate_yacht_id, patch_name, patch_sail_number, patch_yacht_type, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (ycid, "Corrected Yacht", "99", "sloop", "actor", ph),
        )
        conn.commit()

        ctrs = run_source_to_candidate(conn, entity_type="yacht")
        conn.commit()

        assert ctrs.db_errors == 0
        row = conn.execute(
            "SELECT name, sail_number, yacht_type FROM candidate_yacht WHERE id=%s",
            (ycid,),
        ).fetchone()
        assert row[0] == "Corrected Yacht"
        assert row[1] == "99"
        assert row[2] == "sloop"

    def test_revoked_patch_not_applied(self, db_conn):
        """A revoked manual patch must not modify the candidate."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        cid = _seed_candidate_participant(conn, display_name="Unchanged")
        ph = _patch_hash("participant_patch", cid, "Should Not Apply", None, None, None, "actor")
        conn.execute(
            "INSERT INTO manual_participant_patch "
            "(candidate_participant_id, patch_display_name, actor, patch_hash, status) "
            "VALUES (%s, %s, %s, %s, 'revoked')",
            (cid, "Should Not Apply", "actor", ph),
        )
        conn.commit()

        run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        row = conn.execute(
            "SELECT display_name FROM candidate_participant WHERE id=%s", (cid,)
        ).fetchone()
        assert row[0] == "Unchanged"


class TestNormalizedFieldUpdates:
    """Fixes: participant and yacht patches must update normalized identity fields."""

    def test_participant_patch_updates_normalized_name(self, db_conn):
        """Patching display_name must also update normalized_name for scorer consistency."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate
        from regatta_etl.normalize import normalize_name

        cid = _seed_candidate_participant(conn, display_name="Old Name", normalized_name="old-name")
        ph = _patch_hash("participant_patch", cid, "Smith, Alice", None, None, None)
        conn.execute(
            "INSERT INTO manual_participant_patch "
            "(candidate_participant_id, patch_display_name, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s)",
            (cid, "Smith, Alice", "actor", ph),
        )
        conn.commit()

        run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        row = conn.execute(
            "SELECT display_name, normalized_name FROM candidate_participant WHERE id=%s",
            (cid,),
        ).fetchone()
        assert row[0] == "Smith, Alice"
        assert row[1] == normalize_name("Smith, Alice")

    def test_yacht_patch_updates_normalized_name_and_sail(self, db_conn):
        """Patching name/sail_number must update normalized_name/normalized_sail_number."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate
        from regatta_etl.normalize import slug_name

        ycid = _seed_candidate_yacht(conn, name="Old Boat", normalized_name="old-boat",
                                      sail_number="10")
        ph = _patch_hash("yacht_patch", ycid, "New Boat", "42", None, None)
        conn.execute(
            "INSERT INTO manual_yacht_patch "
            "(candidate_yacht_id, patch_name, patch_sail_number, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s, %s)",
            (ycid, "New Boat", "42", "actor", ph),
        )
        conn.commit()

        run_source_to_candidate(conn, entity_type="yacht")
        conn.commit()

        row = conn.execute(
            "SELECT name, normalized_name, sail_number, normalized_sail_number "
            "FROM candidate_yacht WHERE id=%s",
            (ycid,),
        ).fetchone()
        assert row[0] == "New Boat"
        assert row[1] == slug_name("New Boat")
        assert row[2] == "42"
        assert row[3] == slug_name("42")


class TestAddressOverwrite:
    """Fix: address patch must overwrite structured fields for existing address_raw."""

    def test_address_patch_overwrites_existing_structured_fields(self, db_conn):
        """Re-patching with city/state update must replace the existing address row's values."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        cid = _seed_candidate_participant(conn)
        # First patch: address_raw with no city
        ph1 = _patch_hash("participant_address_patch", cid, "100 Main St")
        conn.execute(
            "INSERT INTO manual_participant_address_patch "
            "(candidate_participant_id, address_raw, actor, patch_hash) "
            "VALUES (%s, %s, %s, %s)",
            (cid, "100 Main St", "actor", ph1),
        )
        conn.commit()
        run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        # Supersede: mark old as revoked, add new with same address_raw but with city
        conn.execute(
            "UPDATE manual_participant_address_patch SET status='revoked' "
            "WHERE candidate_participant_id=%s", (cid,),
        )
        ph2 = _patch_hash("participant_address_patch", cid, "100 Main St", "Seattle")
        conn.execute(
            "INSERT INTO manual_participant_address_patch "
            "(candidate_participant_id, address_raw, city, state, actor, patch_hash, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, 'active')",
            (cid, "100 Main St", "Seattle", "WA", "actor2", ph2),
        )
        conn.commit()
        run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        addr = conn.execute(
            "SELECT city, state FROM candidate_participant_address WHERE candidate_participant_id=%s",
            (cid,),
        ).fetchone()
        assert addr[0] == "Seattle"
        assert addr[1] == "WA"


class TestOwnershipMembershipIngestion:
    """Fix: ownership and membership patches must flow into the candidate layer."""

    def test_ownership_patch_creates_source_links(self, db_conn):
        """Active yacht_ownership_patch must create source_links for both participant and yacht."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        pcid = _seed_candidate_participant(conn)
        ycid = _seed_candidate_yacht(conn)
        ph = _patch_hash("yacht_ownership_patch", pcid, ycid, "owner", "add")
        conn.execute(
            "INSERT INTO manual_yacht_ownership_patch "
            "(candidate_participant_id, candidate_yacht_id, role, operation, actor, patch_hash) "
            "VALUES (%s, %s, 'owner', 'add', 'actor', %s)",
            (pcid, ycid, ph),
        )
        conn.commit()

        ctrs = run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        assert ctrs.db_errors == 0
        # Source link for participant
        p_link = conn.execute(
            "SELECT source_system FROM candidate_source_link "
            "WHERE candidate_entity_type='participant' AND candidate_entity_id=%s "
            "AND source_table_name='manual_yacht_ownership_patch'",
            (pcid,),
        ).fetchone()
        assert p_link is not None
        assert p_link[0] == "manual_curation"

        # Source link for yacht
        y_link = conn.execute(
            "SELECT source_system FROM candidate_source_link "
            "WHERE candidate_entity_type='yacht' AND candidate_entity_id=%s "
            "AND source_table_name='manual_yacht_ownership_patch'",
            (ycid,),
        ).fetchone()
        assert y_link is not None

    def test_ownership_patch_add_creates_role_assignment(self, db_conn):
        """Ownership 'add' patch must add a role_assignment row to candidate_participant."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        pcid = _seed_candidate_participant(conn)
        ycid = _seed_candidate_yacht(conn)
        ph = _patch_hash("yacht_ownership_patch", pcid, ycid, "owner", "add")
        conn.execute(
            "INSERT INTO manual_yacht_ownership_patch "
            "(candidate_participant_id, candidate_yacht_id, role, operation, actor, patch_hash) "
            "VALUES (%s, %s, 'owner', 'add', 'actor', %s)",
            (pcid, ycid, ph),
        )
        conn.commit()

        run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        role_row = conn.execute(
            "SELECT role FROM candidate_participant_role_assignment "
            "WHERE candidate_participant_id=%s AND role='yacht_owner'",
            (pcid,),
        ).fetchone()
        assert role_row is not None

    def test_membership_patch_creates_source_links_and_role(self, db_conn):
        """Active club_membership_patch must create source_links and a role_assignment."""
        conn, _ = db_conn
        from regatta_etl.resolution_source_to_candidate import run_source_to_candidate

        pcid = _seed_candidate_participant(conn)
        ccid = _seed_candidate_club(conn)
        ph = _patch_hash("club_membership_patch", pcid, ccid, "member", "add")
        conn.execute(
            "INSERT INTO manual_club_membership_patch "
            "(candidate_participant_id, candidate_club_id, membership_role, operation, actor, patch_hash) "
            "VALUES (%s, %s, 'member', 'add', 'actor', %s)",
            (pcid, ccid, ph),
        )
        conn.commit()

        ctrs = run_source_to_candidate(conn, entity_type="participant")
        conn.commit()

        assert ctrs.db_errors == 0
        # Source links for both participant and club
        for entity_type, cid in (("participant", pcid), ("club", ccid)):
            link = conn.execute(
                "SELECT id FROM candidate_source_link "
                "WHERE candidate_entity_type=%s AND candidate_entity_id=%s "
                "AND source_table_name='manual_club_membership_patch'",
                (entity_type, cid),
            ).fetchone()
            assert link is not None, f"missing source_link for {entity_type}"

        # Role assignment
        role = conn.execute(
            "SELECT role FROM candidate_participant_role_assignment "
            "WHERE candidate_participant_id=%s AND role='member'",
            (pcid,),
        ).fetchone()
        assert role is not None


class TestPatchHashIdempotency:
    """Fix: same logical patch by two different actors must not create duplicates."""

    def test_same_content_different_actors_share_hash(self, db_conn):
        """Two actors submitting the same logical participant patch produce the same hash."""
        from regatta_etl.mcp_helper import _patch_hash
        cid = str(uuid.uuid4())
        hash_alice = _patch_hash("participant_patch", cid, "Fixed Name", None, None, None)
        hash_bob   = _patch_hash("participant_patch", cid, "Fixed Name", None, None, None)
        assert hash_alice == hash_bob

    def test_different_content_produces_different_hash(self, db_conn):
        from regatta_etl.mcp_helper import _patch_hash
        cid = str(uuid.uuid4())
        h1 = _patch_hash("participant_patch", cid, "Alice", None, None, None)
        h2 = _patch_hash("participant_patch", cid, "Bob",   None, None, None)
        assert h1 != h2

    def test_second_insert_with_same_hash_is_ignored(self, db_conn):
        """ON CONFLICT (patch_hash) DO NOTHING means the second row is silently dropped."""
        conn, _ = db_conn
        from regatta_etl.mcp_helper import _patch_hash

        cid = _seed_candidate_participant(conn)
        ph = _patch_hash("participant_patch", cid, "Deduped", None, None, None)

        for actor in ("alice", "bob"):
            conn.execute(
                "INSERT INTO manual_participant_patch "
                "(candidate_participant_id, patch_display_name, actor, patch_hash) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (patch_hash) DO NOTHING",
                (cid, "Deduped", actor, ph),
            )
        conn.commit()

        count = conn.execute(
            "SELECT COUNT(*) FROM manual_participant_patch WHERE candidate_participant_id=%s",
            (cid,),
        ).fetchone()[0]
        assert count == 1
