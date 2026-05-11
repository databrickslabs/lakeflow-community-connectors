"""Tests for base_r4 Observation schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/observation.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Observation v2.5.0

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  effectiveDateTime → effective_datetime
  effectivePeriod   → effective_period
  valueQuantity     → value_quantity
  valueCodeableConcept → value_codeable_concept
  valueString       → value_string
  valueBoolean      → value_boolean
  valueInteger      → value_integer
  dataAbsentReason  → data_absent_reason
  bodySite          → body_site
  referenceRange    → reference_range
  hasMember         → has_member
  derivedFrom       → derived_from
"""
from pyspark.sql.types import (
    ArrayType, BooleanType, LongType, StringType, StructType, TimestampType,
)

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_observation_schema_is_struct_type():
    assert isinstance(get_schema("Observation", "base_r4"), StructType)


def test_observation_has_common_fields():
    names = {f.name for f in get_schema("Observation", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_observation_status_is_string():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_observation_category_is_array_of_codeable_concept():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "category")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"coding", "text"} <= inner_names


def test_observation_code_is_codeable_concept():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "code")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner_names


def test_observation_effective_datetime_is_timestamp():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "effective_datetime")
    assert isinstance(f.dataType, TimestampType)


def test_observation_effective_period_exists():
    names = {f.name for f in get_schema("Observation", "base_r4").fields}
    assert "effective_period" in names


def test_observation_value_quantity_exists():
    names = {f.name for f in get_schema("Observation", "base_r4").fields}
    assert "value_quantity" in names


def test_observation_value_string_is_string():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "value_string")
    assert isinstance(f.dataType, StringType)


def test_observation_value_boolean_is_boolean():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "value_boolean")
    assert isinstance(f.dataType, BooleanType)


def test_observation_value_integer_is_integer():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "value_integer")
    assert isinstance(f.dataType, LongType)


def test_observation_value_codeable_concept_is_struct():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "value_codeable_concept")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner_names


def test_observation_data_absent_reason_exists():
    names = {f.name for f in get_schema("Observation", "base_r4").fields}
    assert "data_absent_reason" in names


def test_observation_reference_range_is_array():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "reference_range")
    assert isinstance(f.dataType, ArrayType)


def test_observation_has_member_is_array():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "has_member")
    assert isinstance(f.dataType, ArrayType)


def test_observation_derived_from_is_array():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "derived_from")
    assert isinstance(f.dataType, ArrayType)


def test_observation_component_is_array_with_inner_fields():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "component")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"code", "value_quantity", "value_string"} <= inner_names


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_observation_extract_effective_datetime():
    resource = {
        "resourceType": "Observation", "id": "obs1",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "8302-2", "display": "Body height"}]},
        "subject": {"reference": "Patient/p1"},
        "effectiveDateTime": "2024-01-15T10:30:00+00:00",
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["status"] == "final"
    assert result["effective_datetime"] == "2024-01-15T10:30:00+00:00"
    assert result["effective_period"] is None
    assert result["code"]["coding"][0]["code"] == "8302-2"
    assert result["subject"]["reference"] == "Patient/p1"


def test_observation_extract_effective_period():
    resource = {
        "resourceType": "Observation", "id": "obs2",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "55284-4"}]},
        "subject": {"reference": "Patient/p1"},
        "effectivePeriod": {"start": "2024-01-01T00:00:00+00:00", "end": "2024-01-15T00:00:00+00:00"},
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["effective_datetime"] is None
    assert result["effective_period"]["start"] == "2024-01-01T00:00:00+00:00"
    assert result["effective_period"]["end"] == "2024-01-15T00:00:00+00:00"


def test_observation_extract_value_quantity():
    resource = {
        "resourceType": "Observation", "id": "obs3",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "8302-2"}]},
        "valueQuantity": {"value": 170.5, "unit": "cm", "system": "http://unitsofmeasure.org", "code": "cm"},
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["value_quantity"]["value"] == 170.5
    assert result["value_quantity"]["unit"] == "cm"
    assert result["value_quantity"]["system"] == "http://unitsofmeasure.org"
    assert result["value_string"] is None
    assert result["value_boolean"] is None
    assert result["value_integer"] is None
    assert result["value_codeable_concept"] is None


def test_observation_extract_value_string():
    resource = {
        "resourceType": "Observation", "id": "obs4",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "11459-5"}]},
        "valueString": "Positive",
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["value_string"] == "Positive"
    assert result["value_quantity"] is None


def test_observation_extract_value_boolean():
    resource = {
        "resourceType": "Observation", "id": "obs5",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "96881-6"}]},
        "valueBoolean": True,
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["value_boolean"] is True


def test_observation_extract_value_integer():
    resource = {
        "resourceType": "Observation", "id": "obs6",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "72514-3"}]},
        "valueInteger": 7,
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["value_integer"] == 7


def test_observation_extract_value_codeable_concept():
    resource = {
        "resourceType": "Observation", "id": "obs7",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "2085-9"}]},
        "valueCodeableConcept": {
            "coding": [{"system": "http://snomed.info/sct", "code": "260385009", "display": "Negative"}],
            "text": "Negative",
        },
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["value_codeable_concept"]["text"] == "Negative"
    assert result["value_codeable_concept"]["coding"][0]["code"] == "260385009"


def test_observation_extract_category():
    resource = {
        "resourceType": "Observation", "id": "obs8",
        "status": "final",
        "category": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category",
                         "code": "vital-signs", "display": "Vital Signs"}]}
        ],
        "code": {"coding": [{"system": "http://loinc.org", "code": "8310-5"}]},
    }
    result = extract(resource, "Observation", "base_r4")
    assert len(result["category"]) == 1
    assert result["category"][0]["coding"][0]["code"] == "vital-signs"


def test_observation_extract_component():
    resource = {
        "resourceType": "Observation", "id": "obs9",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "55284-4"}]},
        "component": [
            {
                "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6", "display": "Systolic BP"}]},
                "valueQuantity": {"value": 120.0, "unit": "mmHg", "system": "http://unitsofmeasure.org", "code": "mm[Hg]"},
            },
            {
                "code": {"coding": [{"system": "http://loinc.org", "code": "8462-4", "display": "Diastolic BP"}]},
                "valueQuantity": {"value": 80.0, "unit": "mmHg", "system": "http://unitsofmeasure.org", "code": "mm[Hg]"},
            },
        ],
    }
    result = extract(resource, "Observation", "base_r4")
    assert len(result["component"]) == 2
    assert result["component"][0]["code"]["coding"][0]["code"] == "8480-6"
    assert result["component"][0]["value_quantity"]["value"] == 120.0
    assert result["component"][1]["value_quantity"]["value"] == 80.0


def test_observation_extract_reference_range():
    resource = {
        "resourceType": "Observation", "id": "obs10",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "2823-3"}]},
        "referenceRange": [
            {"low": {"value": 3.5, "unit": "mmol/L"}, "high": {"value": 5.0, "unit": "mmol/L"}, "text": "3.5-5.0 mmol/L"}
        ],
    }
    result = extract(resource, "Observation", "base_r4")
    assert len(result["reference_range"]) == 1
    assert result["reference_range"][0]["low_value"] == 3.5
    assert result["reference_range"][0]["high_value"] == 5.0
    assert result["reference_range"][0]["text"] == "3.5-5.0 mmol/L"


def test_observation_extract_has_member_and_derived_from():
    resource = {
        "resourceType": "Observation", "id": "obs11",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "55284-4"}]},
        "hasMember": [{"reference": "Observation/obs-systolic"}],
        "derivedFrom": [{"reference": "Observation/obs-raw"}],
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["has_member"][0]["reference"] == "Observation/obs-systolic"
    assert result["derived_from"][0]["reference"] == "Observation/obs-raw"


def test_observation_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Observation", "id": "obs-empty"}, "Observation", "base_r4")
    assert result["status"] is None
    assert result["category"] == []
    assert result["code"] is None
    assert result["effective_datetime"] is None
    assert result["effective_period"] is None
    assert result["value_quantity"] is None
    assert result["value_string"] is None
    assert result["value_boolean"] is None
    assert result["value_integer"] is None
    assert result["value_codeable_concept"] is None
    assert result["component"] == []
    assert result["reference_range"] == []
    assert result["has_member"] == []
    assert result["derived_from"] == []


def test_observation_component_has_value_integer():
    schema = get_schema("Observation", "base_r4")
    f = next(f for f in schema.fields if f.name == "component")
    inner = {sf.name for sf in f.dataType.elementType.fields}
    assert "value_integer" in inner


def test_observation_extract_component_value_integer():
    resource = {
        "resourceType": "Observation", "id": "obs_int",
        "status": "final",
        "code": {"text": "Score"},
        "component": [{"code": {"text": "Pain score"}, "valueInteger": 7}],
    }
    result = extract(resource, "Observation", "base_r4")
    assert result["component"][0]["value_integer"] == 7
