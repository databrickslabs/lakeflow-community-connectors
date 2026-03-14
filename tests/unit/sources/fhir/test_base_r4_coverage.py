"""Tests for base_r4 Coverage schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/coverage.html
  No UK Core profile — base R4 is authoritative.

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  policyHolder     → policy_holder
  subscriberId     → subscriber_id
  beneficiary      → beneficiary
  relationship     → relationship
  payor            → payor  (1..* required)
  class            → class_coverage  (FHIR JSON key is "class" — reserved word workaround)
    class.type     → class_coverage[].type
    class.value    → class_coverage[].value
    class.name     → class_coverage[].name
  subrogation      → subrogation
"""
from pyspark.sql.types import ArrayType, BooleanType, IntegerType, StringType, StructType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_coverage_schema_is_struct_type():
    assert isinstance(get_schema("Coverage", "base_r4"), StructType)


def test_coverage_has_common_fields():
    names = {f.name for f in get_schema("Coverage", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_coverage_status_is_string():
    schema = get_schema("Coverage", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_coverage_beneficiary_is_reference():
    schema = get_schema("Coverage", "base_r4")
    f = next(f for f in schema.fields if f.name == "beneficiary")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert "reference" in inner_names


def test_coverage_payor_is_array_of_reference():
    schema = get_schema("Coverage", "base_r4")
    f = next(f for f in schema.fields if f.name == "payor")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert "reference" in inner_names


def test_coverage_class_coverage_is_array_with_type_value_name():
    schema = get_schema("Coverage", "base_r4")
    f = next(f for f in schema.fields if f.name == "class_coverage")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"type", "value", "name"} <= inner_names


def test_coverage_subscriber_id_is_string():
    schema = get_schema("Coverage", "base_r4")
    f = next(f for f in schema.fields if f.name == "subscriber_id")
    assert isinstance(f.dataType, StringType)


def test_coverage_subrogation_is_boolean():
    schema = get_schema("Coverage", "base_r4")
    f = next(f for f in schema.fields if f.name == "subrogation")
    assert isinstance(f.dataType, BooleanType)


def test_coverage_order_is_integer():
    schema = get_schema("Coverage", "base_r4")
    f = next(f for f in schema.fields if f.name == "order")
    assert isinstance(f.dataType, IntegerType)


def test_coverage_policy_holder_exists():
    names = {f.name for f in get_schema("Coverage", "base_r4").fields}
    assert "policy_holder" in names


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_coverage_extract_full():
    resource = {
        "resourceType": "Coverage",
        "id": "cov1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/coverage-id", "value": "COV001"}],
        "status": "active",
        "type": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                         "code": "EHCPOL", "display": "extended healthcare"}]
        },
        "policyHolder": {"reference": "Patient/p1"},
        "subscriber": {"reference": "Patient/p1"},
        "subscriberId": "SUB-12345",
        "beneficiary": {"reference": "Patient/p1", "display": "Jane Smith"},
        "dependent": "01",
        "relationship": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/subscriber-relationship",
                         "code": "self", "display": "Self"}]
        },
        "period": {"start": "2021-01-01", "end": "2021-12-31"},
        "payor": [{"reference": "Organization/nhsbsa", "display": "NHS Business Services Authority"}],
        "class": [
            {
                "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/coverage-class",
                                      "code": "plan", "display": "Plan"}]},
                "value": "PLAN-NHS-001",
                "name": "NHS Standard Plan",
            },
            {
                "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/coverage-class",
                                      "code": "group", "display": "Group"}]},
                "value": "GRP-001",
                "name": "Primary Group",
            },
        ],
        "order": 1,
        "network": "NHS-Network-1",
        "subrogation": False,
    }
    result = extract(resource, "Coverage", "base_r4")
    assert result["identifier"][0]["value"] == "COV001"
    assert result["status"] == "active"
    assert result["type"]["coding"][0]["code"] == "EHCPOL"
    assert result["policy_holder"]["reference"] == "Patient/p1"
    assert result["subscriber"]["reference"] == "Patient/p1"
    assert result["subscriber_id"] == "SUB-12345"
    assert result["beneficiary"]["reference"] == "Patient/p1"
    assert result["dependent"] == "01"
    assert result["relationship"]["coding"][0]["code"] == "self"
    assert result["period"]["start"] == "2021-01-01"
    assert result["period"]["end"] == "2021-12-31"
    assert len(result["payor"]) == 1
    assert result["payor"][0]["reference"] == "Organization/nhsbsa"
    # class field (mapped to class_coverage)
    assert len(result["class_coverage"]) == 2
    assert result["class_coverage"][0]["type"]["coding"][0]["code"] == "plan"
    assert result["class_coverage"][0]["value"] == "PLAN-NHS-001"
    assert result["class_coverage"][0]["name"] == "NHS Standard Plan"
    assert result["class_coverage"][1]["type"]["coding"][0]["code"] == "group"
    assert result["order"] == 1
    assert result["network"] == "NHS-Network-1"
    assert result["subrogation"] is False


def test_coverage_extract_class_field_reads_json_key_class():
    """Verifies extractor correctly reads the FHIR JSON key 'class' (a Python reserved word)."""
    resource = {
        "resourceType": "Coverage", "id": "cov2",
        "status": "active",
        "beneficiary": {"reference": "Patient/p2"},
        "payor": [{"reference": "Organization/ins1"}],
        "class": [
            {"type": {"coding": [{"code": "plan"}]}, "value": "GOLD", "name": "Gold Plan"}
        ],
    }
    result = extract(resource, "Coverage", "base_r4")
    assert len(result["class_coverage"]) == 1
    assert result["class_coverage"][0]["value"] == "GOLD"


def test_coverage_extract_multiple_payors():
    resource = {
        "resourceType": "Coverage", "id": "cov3",
        "status": "active",
        "beneficiary": {"reference": "Patient/p3"},
        "payor": [
            {"reference": "Organization/ins1"},
            {"reference": "Organization/ins2"},
        ],
    }
    result = extract(resource, "Coverage", "base_r4")
    assert len(result["payor"]) == 2
    assert result["payor"][0]["reference"] == "Organization/ins1"
    assert result["payor"][1]["reference"] == "Organization/ins2"


def test_coverage_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Coverage", "id": "cov-empty"}, "Coverage", "base_r4")
    assert result["identifier"] == []
    assert result["status"] is None
    assert result["type"] is None
    assert result["policy_holder"] is None
    assert result["subscriber"] is None
    assert result["subscriber_id"] is None
    assert result["beneficiary"] is None
    assert result["dependent"] is None
    assert result["relationship"] is None
    assert result["period"] is None
    assert result["payor"] == []
    assert result["class_coverage"] == []
    assert result["order"] is None
    assert result["network"] is None
    assert result["subrogation"] is None
