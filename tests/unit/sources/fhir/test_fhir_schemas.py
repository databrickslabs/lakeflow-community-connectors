from pyspark.sql.types import StringType, TimestampType, BooleanType, StructType

from databricks.labs.community_connector.sources.fhir.fhir_constants import DEFAULT_RESOURCES
from databricks.labs.community_connector.sources.fhir.fhir_schemas import get_schema, FALLBACK_SCHEMA


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
    assert lu.nullable is True  # FHIR spec: meta.lastUpdated is 0..1

def test_patient_has_typed_fields():
    names = {f.name for f in get_schema("Patient").fields}
    assert {"gender", "birthDate", "active"} <= names

def test_observation_has_typed_fields():
    names = {f.name for f in get_schema("Observation").fields}
    assert {"status", "subject_reference", "code_text"} <= names

def test_unknown_resource_returns_fallback():
    schema = get_schema("UnknownXYZ")
    assert schema == FALLBACK_SCHEMA
    assert {f.name for f in schema.fields} == {"id", "resourceType", "lastUpdated", "raw_json"}
