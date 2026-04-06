from __future__ import annotations

import csv
import hashlib
import uuid
from pathlib import Path

from regatta_etl.normalize import normalize_person_name_for_identity, slug_name
from regatta_etl.resolution_overcombination import run_overcombination_report
from regatta_etl.resolution_undercombination import run_undercombination_report


PROJECT_ROOT = Path(__file__).parent.parent.parent


def _fingerprint(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _insert_candidate_participant(conn, name: str) -> str:
    row = conn.execute(
        """
        INSERT INTO candidate_participant
            (stable_fingerprint, display_name, normalized_name, resolution_state)
        VALUES (%s, %s, %s, 'review')
        RETURNING id::text
        """,
        (_fingerprint("candidate_participant", name, str(uuid.uuid4())), name, normalize_person_name_for_identity(name)),
    ).fetchone()
    return str(row[0])


def _insert_candidate_yacht(conn, name: str) -> str:
    normalized_name = slug_name(name)
    row = conn.execute(
        """
        INSERT INTO candidate_yacht
            (stable_fingerprint, name, normalized_name, resolution_state)
        VALUES (%s, %s, %s, 'review')
        RETURNING id::text
        """,
        (_fingerprint("candidate_yacht", name, str(uuid.uuid4())), name, normalized_name),
    ).fetchone()
    return str(row[0])


def _insert_participant(conn, name: str) -> str:
    row = conn.execute(
        """
        INSERT INTO participant (full_name, normalized_full_name, first_name, last_name)
        VALUES (%s, %s, %s, %s)
        RETURNING id::text
        """,
        (name, normalize_person_name_for_identity(name), name.split()[0], name.split()[-1]),
    ).fetchone()
    return str(row[0])


def _insert_yacht(conn, name: str) -> str:
    row = conn.execute(
        """
        INSERT INTO yacht (name, normalized_name)
        VALUES (%s, %s)
        RETURNING id::text
        """,
        (name, slug_name(name)),
    ).fetchone()
    return str(row[0])


def _insert_canonical_yacht(conn, name: str) -> str:
    row = conn.execute(
        """
        INSERT INTO canonical_yacht (name, normalized_name)
        VALUES (%s, %s)
        RETURNING id::text
        """,
        (name, slug_name(name)),
    ).fetchone()
    return str(row[0])


def _link_source(conn, entity_type: str, candidate_id: str, source_table: str, source_row_pk: str) -> None:
    conn.execute(
        """
        INSERT INTO candidate_source_link
            (candidate_entity_type, candidate_entity_id, source_table_name, source_row_pk)
        VALUES (%s, %s, %s, %s)
        """,
        (entity_type, candidate_id, source_table, source_row_pk),
    )


def _link_candidate_to_canonical(conn, entity_type: str, candidate_id: str, canonical_id: str) -> None:
    conn.execute(
        """
        INSERT INTO candidate_canonical_link
            (candidate_entity_type, candidate_entity_id, canonical_entity_id, promotion_mode)
        VALUES (%s, %s, %s, 'auto')
        """,
        (entity_type, candidate_id, canonical_id),
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_undercombination_shared_boat_includes_ownership_path(db_conn, tmp_path):
    conn, _ = db_conn

    participant_a = _insert_participant(conn, "John Abbott")
    participant_b = _insert_participant(conn, "Jack Abbott")
    candidate_a = _insert_candidate_participant(conn, "John Abbott")
    candidate_b = _insert_candidate_participant(conn, "Jack Abbott")
    yacht_id = _insert_yacht(conn, "First Sight")
    candidate_yacht_id = _insert_candidate_yacht(conn, "First Sight")
    canonical_yacht_id = _insert_canonical_yacht(conn, "First Sight")

    _link_source(conn, "participant", candidate_a, "participant", participant_a)
    _link_source(conn, "participant", candidate_b, "participant", participant_b)
    _link_source(conn, "yacht", candidate_yacht_id, "yacht", yacht_id)
    _link_candidate_to_canonical(conn, "yacht", candidate_yacht_id, canonical_yacht_id)

    conn.execute(
        """
        INSERT INTO yacht_ownership
            (participant_id, yacht_id, role, is_primary_contact, effective_start, source_system)
        VALUES
            (%s, %s, 'owner', true, current_date, 'test'),
            (%s, %s, 'owner', false, current_date, 'test')
        """,
        (participant_a, yacht_id, participant_b, yacht_id),
    )
    conn.execute(
        """
        INSERT INTO candidate_participant_contact
            (candidate_participant_id, contact_type, raw_value, normalized_value, is_primary)
        VALUES
            (%s, 'email', 'shared@example.com', 'shared@example.com', true),
            (%s, 'email', 'shared@example.com', 'shared@example.com', true)
        """,
        (candidate_a, candidate_b),
    )

    out_path = tmp_path / "undercombination.csv"
    ctrs = run_undercombination_report(conn, out_path)
    rows = _read_csv_rows(out_path)

    assert ctrs.suspects_found == 1
    assert len(rows) == 1
    assert {rows[0]["candidate_id_a"], rows[0]["candidate_id_b"]} == {candidate_a, candidate_b}
    assert "shared_boat" in rows[0]["signals"].split("|")


def test_overcombination_high_boat_count_includes_ownership_path(db_conn, tmp_path):
    conn, _ = db_conn

    participant_id = _insert_participant(conn, "Pat Owner")
    candidate_id = _insert_candidate_participant(conn, "Pat Owner")
    _link_source(conn, "participant", candidate_id, "participant", participant_id)

    conn.execute(
        """
        INSERT INTO candidate_participant_contact
            (candidate_participant_id, contact_type, raw_value, normalized_value, is_primary)
        VALUES
            (%s, 'email', 'pat@example.com', 'pat@example.com', true),
            (%s, 'email', 'pat@other.net', 'pat@other.net', false)
        """,
        (candidate_id, candidate_id),
    )

    for idx in range(4):
        yacht_name = f"Boat {idx}"
        yacht_id = _insert_yacht(conn, yacht_name)
        candidate_yacht_id = _insert_candidate_yacht(conn, yacht_name)
        canonical_yacht_id = _insert_canonical_yacht(conn, yacht_name)
        _link_source(conn, "yacht", candidate_yacht_id, "yacht", yacht_id)
        _link_candidate_to_canonical(conn, "yacht", candidate_yacht_id, canonical_yacht_id)
        conn.execute(
            """
            INSERT INTO yacht_ownership
                (participant_id, yacht_id, role, is_primary_contact, effective_start, source_system)
            VALUES (%s, %s, 'owner', %s, current_date, 'test')
            """,
            (participant_id, yacht_id, idx == 0),
        )

    out_path = tmp_path / "overcombination.csv"
    ctrs = run_overcombination_report(conn, out_path)
    rows = _read_csv_rows(out_path)

    assert ctrs.suspects_found == 1
    assert len(rows) == 1
    assert rows[0]["candidate_id"] == candidate_id
    assert rows[0]["distinct_boat_count"] == "4"
    assert "high_boat_count" in rows[0]["flags"].split("|")
    assert "multi_email_domain" in rows[0]["flags"].split("|")


def test_overcombination_household_carve_out_uses_explicit_bhyc_evidence(db_conn, tmp_path):
    conn, _ = db_conn

    participant_id = _insert_participant(conn, "Household Holder")
    candidate_id = _insert_candidate_participant(conn, "Household Holder")
    _link_source(conn, "participant", candidate_id, "participant", participant_id)

    conn.execute(
        """
        INSERT INTO candidate_participant_contact
            (candidate_participant_id, contact_type, raw_value, normalized_value, is_primary)
        VALUES
            (%s, 'email', 'holder@example.com', 'holder@example.com', true),
            (%s, 'email', 'holder@other.net', 'holder@other.net', false)
        """,
        (candidate_id, candidate_id),
    )

    for idx in range(4):
        yacht_name = f"Household Boat {idx}"
        yacht_id = _insert_yacht(conn, yacht_name)
        candidate_yacht_id = _insert_candidate_yacht(conn, yacht_name)
        canonical_yacht_id = _insert_canonical_yacht(conn, yacht_name)
        _link_source(conn, "yacht", candidate_yacht_id, "yacht", yacht_id)
        _link_candidate_to_canonical(conn, "yacht", candidate_yacht_id, canonical_yacht_id)
        conn.execute(
            """
            INSERT INTO yacht_ownership
                (participant_id, yacht_id, role, is_primary_contact, effective_start, source_system)
            VALUES (%s, %s, 'owner', %s, current_date, 'test')
            """,
            (participant_id, yacht_id, idx == 0),
        )

    raw_row_id = str(
        conn.execute(
            """
            INSERT INTO bhyc_member_raw_row
                (member_id, page_type, source_url, run_id)
            VALUES ('hh-1', 'member_profile', 'https://example.test/profile/hh-1', 'run-hh')
            RETURNING id::text
            """
        ).fetchone()[0]
    )
    conn.execute(
        """
        INSERT INTO bhyc_household_candidate_evidence
            (bhyc_member_raw_row_id, member_id, relationship_label, participant_id, candidate_participant_id)
        VALUES (%s, 'hh-1', 'spouse', %s, %s)
        """,
        (raw_row_id, participant_id, candidate_id),
    )

    out_path = tmp_path / "overcombination_household.csv"
    ctrs = run_overcombination_report(conn, out_path)
    rows = _read_csv_rows(out_path)

    assert ctrs.household_carve_outs == 1
    assert rows == []


def test_zip_code_repair_updates_bhyc_raw_and_exported_addresses(db_conn):
    conn, _ = db_conn

    participant_id = _insert_participant(conn, "Zip Fixer")
    candidate_id = _insert_candidate_participant(conn, "Zip Fixer")
    canonical_id = str(
        conn.execute("INSERT INTO canonical_participant DEFAULT VALUES RETURNING id::text").fetchone()[0]
    )
    raw_row_id = str(
        conn.execute(
            """
            INSERT INTO bhyc_member_raw_row
                (member_id, page_type, source_url, run_id, parsed_json)
            VALUES (
                'zip-1',
                'member_profile',
                'https://example.test/profile/zip-1',
                'run-zip',
                %s::jsonb
            )
            RETURNING id::text
            """,
            (
                """
                {
                  "addresses": [
                    {
                      "address_type": "summer_mailing",
                      "raw": "1 Dock St, Boothbay, ME, 4538",
                      "line1": "1 Dock St",
                      "city": "Boothbay",
                      "state": "ME",
                      "postal_code": "4538",
                      "country_code": null
                    }
                  ]
                }
                """,
            ),
        ).fetchone()[0]
    )

    conn.execute(
        """
        INSERT INTO participant_address
            (participant_id, address_type, line1, city, state, postal_code, address_raw, source_system)
        VALUES (%s, 'mailing', '1 Dock St', 'Boothbay', 'ME', '4538', '1 Dock St, Boothbay, ME, 4538', 'bhyc_member_directory')
        """,
        (participant_id,),
    )
    conn.execute(
        """
        INSERT INTO candidate_participant_address
            (candidate_participant_id, address_raw, line1, city, state, postal_code, source_table_name, source_row_pk)
        VALUES (%s, '1 Dock St, Boothbay, ME, 4538', '1 Dock St', 'Boothbay', 'ME', '4538', 'bhyc_member_raw_row', %s)
        """,
        (candidate_id, raw_row_id),
    )
    conn.execute(
        """
        INSERT INTO canonical_participant_address
            (canonical_participant_id, address_raw, line1, city, state, postal_code)
        VALUES (%s, '1 Dock St, Boothbay, ME, 4538', '1 Dock St', 'Boothbay', 'ME', '4538')
        """,
        (canonical_id,),
    )

    migration_sql = (PROJECT_ROOT / "migrations" / "0026_zip_code_repair.sql").read_text(encoding="utf-8")
    conn.commit()
    conn.autocommit = True
    try:
        conn.execute(migration_sql)
    finally:
        conn.autocommit = False

    participant_addr = conn.execute(
        "SELECT postal_code, address_raw FROM participant_address WHERE participant_id = %s",
        (participant_id,),
    ).fetchone()
    candidate_addr = conn.execute(
        "SELECT postal_code, address_raw FROM candidate_participant_address WHERE candidate_participant_id = %s",
        (candidate_id,),
    ).fetchone()
    canonical_addr = conn.execute(
        "SELECT postal_code, address_raw FROM canonical_participant_address WHERE canonical_participant_id = %s",
        (canonical_id,),
    ).fetchone()
    parsed_json = conn.execute(
        "SELECT parsed_json FROM bhyc_member_raw_row WHERE id = %s",
        (raw_row_id,),
    ).fetchone()[0]

    assert participant_addr == ("04538", "1 Dock St, Boothbay, ME, 04538")
    assert candidate_addr == ("04538", "1 Dock St, Boothbay, ME, 04538")
    assert canonical_addr == ("04538", "1 Dock St, Boothbay, ME, 04538")
    assert parsed_json["addresses"][0]["postal_code"] == "04538"
    assert parsed_json["addresses"][0]["raw"] == "1 Dock St, Boothbay, ME, 04538"
