"""Tests for the profile registry in profiles/__init__.py."""

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from databricks.labs.community_connector.sources.fhir.profiles import (
    FALLBACK_SCHEMA, PROFILE_CHAIN, _COMMON_FIELDS,
    extract, get_schema, register,
)


def _make_schema(extra_name: str) -> StructType:
    return StructType(list(_COMMON_FIELDS) + [StructField(extra_name, StringType())])


def test_register_and_get_schema():
    schema = _make_schema("test_field")

    @register("TestResource", "test_profile", schema)
    def _extractor(r):
        return {"test_field": r.get("x")}

    assert get_schema("TestResource", "test_profile") == schema


def test_fallback_chain_uk_core_to_base_r4():
    base_schema = _make_schema("base_field")

    @register("FallbackResource", "base_r4", base_schema)
    def _base_extractor(r):
        return {"base_field": r.get("b")}

    # uk_core has no entry — should fall back to base_r4
    assert get_schema("FallbackResource", "uk_core") == base_schema


def test_uk_core_overrides_base_r4():
    base_schema = _make_schema("base_field")
    uk_schema = _make_schema("uk_field")

    @register("OverrideResource", "base_r4", base_schema)
    def _base(r):
        return {"base_field": r.get("b")}

    @register("OverrideResource", "uk_core", uk_schema)
    def _uk(r):
        return {"uk_field": r.get("u")}

    assert get_schema("OverrideResource", "uk_core") == uk_schema
    assert get_schema("OverrideResource", "base_r4") == base_schema


def test_unknown_resource_returns_fallback_schema():
    result = get_schema("DoesNotExist", "uk_core")
    assert result == FALLBACK_SCHEMA
    assert {f.name for f in result.fields} == {"id", "resourceType", "lastUpdated", "raw_json"}


def test_extract_calls_correct_extractor():
    schema = _make_schema("extracted")

    @register("ExtractResource", "base_r4", schema)
    def _ex(r):
        return {"extracted": r.get("val")}

    result = extract({"val": "hello"}, "ExtractResource", "base_r4")
    assert result == {"extracted": "hello"}


def test_extract_unknown_resource_returns_empty_dict():
    result = extract({"id": "x"}, "NonExistentResource", "uk_core")
    assert result == {}


def test_profile_chain_order():
    assert PROFILE_CHAIN[0] == "uk_core"
    assert PROFILE_CHAIN[1] == "base_r4"


def test_common_fields_present():
    names = {f.name for f in _COMMON_FIELDS}
    assert names == {"id", "resourceType", "lastUpdated", "raw_json"}


def test_fallback_schema_is_struct_type():
    assert isinstance(FALLBACK_SCHEMA, StructType)
    lu = next(f for f in FALLBACK_SCHEMA.fields if f.name == "lastUpdated")
    assert isinstance(lu.dataType, TimestampType)
    assert lu.nullable is True
