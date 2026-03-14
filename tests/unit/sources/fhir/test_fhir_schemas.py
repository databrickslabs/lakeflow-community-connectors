"""Tests for fhir_schemas public API.
Validates get_schema() and FALLBACK_SCHEMA work correctly after registry wiring.
"""
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructType, TimestampType

from databricks.labs.community_connector.sources.fhir.fhir_constants import DEFAULT_RESOURCES
from databricks.labs.community_connector.sources.fhir.fhir_schemas import FALLBACK_SCHEMA, get_schema


def test_all_schemas_are_struct_types():
    for resource in DEFAULT_RESOURCES:
        assert isinstance(get_schema(resource), StructType), f"{resource} not StructType"


def test_all_schemas_have_common_fields():
    required = {"id", "resourceType", "lastUpdated", "raw_json"}
    for resource in DEFAULT_RESOURCES:
        names = {f.name for f in get_schema(resource).fields}
        assert required <= names, f"{resource} missing: {required - names}"


def test_last_updated_is_nullable_timestamp():
    schema = get_schema("Patient")
    lu = next(f for f in schema.fields if f.name == "lastUpdated")
    assert isinstance(lu.dataType, TimestampType)
    assert lu.nullable is True


def test_unknown_resource_returns_fallback():
    schema = get_schema("UnknownXYZ")
    assert schema == FALLBACK_SCHEMA
    assert {f.name for f in schema.fields} == {"id", "resourceType", "lastUpdated", "raw_json"}


def test_patient_schema_has_name_as_array():
    schema = get_schema("Patient")
    f = next(f for f in schema.fields if f.name == "name")
    assert isinstance(f.dataType, ArrayType)


def test_patient_schema_has_identifier_as_array():
    schema = get_schema("Patient")
    f = next(f for f in schema.fields if f.name == "identifier")
    assert isinstance(f.dataType, ArrayType)


def test_patient_schema_has_active_boolean():
    schema = get_schema("Patient")
    f = next(f for f in schema.fields if f.name == "active")
    assert isinstance(f.dataType, BooleanType)


def test_observation_schema_has_code_struct():
    schema = get_schema("Observation")
    f = next(f for f in schema.fields if f.name == "code")
    inner = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner


def test_observation_schema_has_value_polymorphic_fields():
    names = {f.name for f in get_schema("Observation").fields}
    assert {"value_quantity", "value_string", "value_boolean"} <= names


def test_uk_core_patient_has_nhs_number():
    # uk_core is the default profile
    names = {f.name for f in get_schema("Patient").fields}
    assert "nhs_number" in names


def test_base_r4_patient_does_not_have_nhs_number():
    names = {f.name for f in get_schema("Patient", "base_r4").fields}
    assert "nhs_number" not in names


def test_get_schema_with_explicit_profile():
    for resource in DEFAULT_RESOURCES:
        assert isinstance(get_schema(resource, "uk_core"), StructType)
        assert isinstance(get_schema(resource, "base_r4"), StructType)
