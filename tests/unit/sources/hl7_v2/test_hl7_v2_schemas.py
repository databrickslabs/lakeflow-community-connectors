"""Schema validation tests for HL7 v2 segment schemas.

Ensures that every schema has the correct structure, metadata fields,
field types, and that extractor output keys align with schema field names.
"""
from __future__ import annotations

import pytest
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _EXTRACTORS
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_schemas import (
    GENERIC_SEGMENT_SCHEMA,
    SEGMENT_SCHEMAS,
    SEGMENT_TABLES,
    get_schema,
)

METADATA_FIELD_NAMES = {
    "message_id",
    "message_timestamp",
    "hl7_version",
    "source_file",
    "send_time",
    "create_time",
    "raw_segment",
}


# ── Registry completeness ──────────────────────────────────────────────────

class TestSchemaRegistry:
    def test_every_table_has_a_schema(self):
        for table in SEGMENT_TABLES:
            schema = get_schema(table)
            assert isinstance(schema, StructType), f"No schema for table '{table}'"

    def test_segment_tables_matches_schema_keys(self):
        assert set(SEGMENT_TABLES) == set(SEGMENT_SCHEMAS.keys())

    def test_unknown_segment_falls_back_to_generic(self):
        assert get_schema("ZXX") == GENERIC_SEGMENT_SCHEMA

    def test_case_insensitive_lookup(self):
        assert get_schema("MSH") == get_schema("msh")
        assert get_schema("ObX") == get_schema("obx")


# ── Metadata fields present in every schema ─────────────────────────────────

class TestMetadataFields:
    @pytest.mark.parametrize("table", SEGMENT_TABLES)
    def test_metadata_fields_present(self, table):
        schema = get_schema(table)
        field_names = {f.name for f in schema.fields}
        missing = METADATA_FIELD_NAMES - field_names
        assert not missing, f"Schema '{table}' missing metadata fields: {missing}"


# ── Field types ─────────────────────────────────────────────────────────────

class TestFieldTypes:
    @pytest.mark.parametrize("table", SEGMENT_TABLES)
    def test_all_fields_are_string_long_or_array(self, table):
        # Most fields are StringType (HL7 strings) or LongType (set_id, counts).
        # Single-occurrence composite types (CWE codes, HD designators, ...) are
        # stored as a nested StructType column. Repeating composite types (XPN
        # names, CWE codes, EI ids) are stored as ArrayType(StructType(...)) or
        # ArrayType(StringType()) so all repetitions are preserved.
        schema = get_schema(table)
        for field in schema.fields:
            dt = field.dataType
            ok = isinstance(dt, (StringType, LongType, StructType))
            if not ok and isinstance(dt, ArrayType):
                element_type = dt.elementType
                ok = isinstance(element_type, (StringType, StructType))
            assert ok, (
                f"Schema '{table}' field '{field.name}' has unexpected type "
                f"{dt}. Expected StringType, LongType, StructType, or "
                f"ArrayType of StringType/StructType."
            )

    @pytest.mark.parametrize("table", SEGMENT_TABLES)
    def test_set_id_is_long_when_present(self, table):
        schema = get_schema(table)
        set_id_field = next((f for f in schema.fields if f.name == "set_id"), None)
        if set_id_field is not None:
            assert isinstance(set_id_field.dataType, LongType), (
                f"Schema '{table}' field 'set_id' should be LongType"
            )


# ── Schema / extractor key alignment ───────────────────────────────────────

class TestSchemaExtractorAlignment:
    """Verify that extractor output keys match the schema field names.

    The extractor outputs a dict whose keys (plus metadata keys) should be
    exactly the set of field names in the schema.

    Some extractors (like ORC) don't produce ``set_id`` because the connector's
    ``_parse_api_messages`` injects it dynamically for multi-segment tables.
    """

    CONNECTOR_INJECTED_KEYS = {"set_id"}

    @pytest.mark.parametrize("seg_key", list(_EXTRACTORS.keys()))
    def test_extractor_keys_match_schema(self, seg_key):
        from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
            HL7EncodingChars,
            HL7Segment,
        )

        enc = HL7EncodingChars()
        dummy_fields = ["DUMMY"] + [""] * 60
        dummy_seg = HL7Segment("DUMMY", dummy_fields, enc, "DUMMY|" + "|".join([""] * 60))
        extractor = _EXTRACTORS[seg_key]
        row = extractor(dummy_seg)
        extractor_keys = set(row.keys())

        schema = get_schema(seg_key)
        schema_keys = {f.name for f in schema.fields}

        expected_data_keys = schema_keys - METADATA_FIELD_NAMES
        missing_from_extractor = expected_data_keys - extractor_keys
        extra_in_extractor = extractor_keys - expected_data_keys

        if missing_from_extractor:
            assert missing_from_extractor <= self.CONNECTOR_INJECTED_KEYS, (
                f"Extractor '{seg_key}' key mismatch:\n"
                f"  In schema but not extractor (and not connector-injected): "
                f"{missing_from_extractor - self.CONNECTOR_INJECTED_KEYS}"
            )
        assert not extra_in_extractor, (
            f"Extractor '{seg_key}' has keys not in schema: {extra_in_extractor}"
        )


# ── Generic schema ──────────────────────────────────────────────────────────

class TestGenericSchema:
    def test_generic_schema_is_struct_type(self):
        assert isinstance(GENERIC_SEGMENT_SCHEMA, StructType)

    def test_generic_has_segment_type_field(self):
        names = {f.name for f in GENERIC_SEGMENT_SCHEMA.fields}
        assert "segment_type" in names

    def test_generic_has_field_columns(self):
        names = {f.name for f in GENERIC_SEGMENT_SCHEMA.fields}
        for i in range(1, 26):
            assert f"field_{i}" in names, f"Generic schema missing field_{i}"

    def test_generic_has_metadata(self):
        names = {f.name for f in GENERIC_SEGMENT_SCHEMA.fields}
        assert METADATA_FIELD_NAMES <= names
