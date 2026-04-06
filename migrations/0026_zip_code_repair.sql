-- Migration: 0026_zip_code_repair.sql
-- Purpose: FOR-218 — repair BHYC 4-digit ZIP codes in both structured columns
--          and studio-facing raw/export payloads.
--
-- Root cause: BHYC membership CSV / profile exports could drop the leading zero
-- on ZIP codes before ingestion (e.g. "04538" -> "4538"). The code fix now
-- normalizes ZIPs at ingest time; this migration repairs previously stored data.
--
-- Surfaces repaired here:
--   - participant_address.postal_code + address_raw
--   - candidate_participant_address.postal_code + address_raw
--   - canonical_participant_address.postal_code + address_raw
--   - bhyc_member_raw_row.parsed_json.addresses[*].postal_code + raw
--
-- Depends on: 0018_bhyc_member_directory_tables, 0025_integrity_constraints

BEGIN;

-- participant_address (operational layer)
UPDATE participant_address
SET postal_code = '0' || postal_code,
    address_raw = COALESCE(
        NULLIF(
            concat_ws(', ',
                NULLIF(trim(line1), ''),
                NULLIF(trim(city), ''),
                NULLIF(trim(state), ''),
                NULLIF(trim('0' || postal_code), ''),
                NULLIF(trim(country_code), '')
            ),
            ''
        ),
        address_raw
    )
WHERE postal_code ~ '^\d{4}$'
  AND source_system = 'bhyc_member_directory';

-- candidate_participant_address (candidate layer)
UPDATE candidate_participant_address
SET postal_code = '0' || postal_code,
    address_raw = COALESCE(
        NULLIF(
            concat_ws(', ',
                NULLIF(trim(line1), ''),
                NULLIF(trim(city), ''),
                NULLIF(trim(state), ''),
                NULLIF(trim('0' || postal_code), ''),
                NULLIF(trim(country_code), '')
            ),
            ''
        ),
        address_raw
    )
WHERE postal_code ~ '^\d{4}$'
  AND source_table_name = 'bhyc_member_raw_row';

-- canonical_participant_address (promoted rows copied from candidate)
UPDATE canonical_participant_address
SET postal_code = '0' || postal_code,
    address_raw = COALESCE(
        NULLIF(
            concat_ws(', ',
                NULLIF(trim(line1), ''),
                NULLIF(trim(city), ''),
                NULLIF(trim(state), ''),
                NULLIF(trim('0' || postal_code), ''),
                NULLIF(trim(country_code), '')
            ),
            ''
        ),
        address_raw
    )
WHERE postal_code ~ '^\d{4}$';

-- BHYC raw/export payloads
WITH rewritten AS (
    SELECT
        bmr.id,
        jsonb_set(
            COALESCE(bmr.parsed_json, '{}'::jsonb),
            '{addresses}',
            COALESCE(
                (
                    SELECT jsonb_agg(
                        CASE
                            WHEN COALESCE(addr.elem->>'postal_code', '') ~ '^\d{4}$' THEN
                                jsonb_set(
                                    jsonb_set(
                                        addr.elem,
                                        '{postal_code}',
                                        to_jsonb('0' || (addr.elem->>'postal_code')),
                                        true
                                    ),
                                    '{raw}',
                                    to_jsonb(
                                        COALESCE(
                                            NULLIF(
                                                concat_ws(', ',
                                                    NULLIF(trim(addr.elem->>'line1'), ''),
                                                    NULLIF(trim(addr.elem->>'city'), ''),
                                                    NULLIF(trim(addr.elem->>'state'), ''),
                                                    NULLIF(trim('0' || (addr.elem->>'postal_code')), ''),
                                                    NULLIF(trim(addr.elem->>'country_code'), '')
                                                ),
                                                ''
                                            ),
                                            addr.elem->>'raw'
                                        )
                                    ),
                                    true
                                )
                            ELSE addr.elem
                        END
                    )
                    FROM jsonb_array_elements(COALESCE(bmr.parsed_json->'addresses', '[]'::jsonb)) AS addr(elem)
                ),
                '[]'::jsonb
            ),
            true
        ) AS parsed_json_new
    FROM bhyc_member_raw_row bmr
    WHERE bmr.parsed_json ? 'addresses'
)
UPDATE bhyc_member_raw_row bmr
SET parsed_json = rewritten.parsed_json_new
FROM rewritten
WHERE rewritten.id = bmr.id
  AND bmr.parsed_json IS DISTINCT FROM rewritten.parsed_json_new;

COMMIT;
