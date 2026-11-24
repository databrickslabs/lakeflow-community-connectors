"""Tests for base_r4 Condition schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/condition.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Condition v2.6.0

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  clinicalStatus       → clinical_status
  verificationStatus   → verification_status
  onsetDateTime        → onset_datetime
  onsetPeriod          → onset_period
  onsetString          → onset_string
  abatementDateTime    → abatement_datetime
  abatementString      → abatement_string
  recordedDate         → recorded_date
  bodySite             → body_site
"""
from pyspark.sql.types import ArrayType, StringType, StructType, TimestampType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_condition_schema_is_struct_type():
    assert isinstance(get_schema("Condition", "base_r4"), StructType)


def test_condition_has_common_fields():
    names = {f.name for f in get_schema("Condition", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_condition_clinical_status_is_codeable_concept():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "clinical_status")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner_names


def test_condition_verification_status_is_codeable_concept():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "verification_status")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner_names


def test_condition_severity_is_codeable_concept():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "severity")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner_names


def test_condition_onset_datetime_is_timestamp():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "onset_datetime")
    assert isinstance(f.dataType, TimestampType)


def test_condition_onset_period_exists():
    names = {f.name for f in get_schema("Condition", "base_r4").fields}
    assert "onset_period" in names


def test_condition_onset_string_is_string():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "onset_string")
    assert isinstance(f.dataType, StringType)


def test_condition_abatement_datetime_is_timestamp():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "abatement_datetime")
    assert isinstance(f.dataType, TimestampType)


def test_condition_abatement_string_is_string():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "abatement_string")
    assert isinstance(f.dataType, StringType)


def test_condition_body_site_is_array():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "body_site")
    assert isinstance(f.dataType, ArrayType)


def test_condition_note_is_array():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "note")
    assert isinstance(f.dataType, ArrayType)


def test_condition_identifier_is_array():
    schema = get_schema("Condition", "base_r4")
    f = next(f for f in schema.fields if f.name == "identifier")
    assert isinstance(f.dataType, ArrayType)


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_condition_extract_full():
    resource = {
        "resourceType": "Condition",
        "id": "cond1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/condition-id", "value": "C001"}],
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                        "code": "active", "display": "Active"}]
        },
        "verificationStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                        "code": "confirmed", "display": "Confirmed"}]
        },
        "severity": {
            "coding": [{"system": "http://snomed.info/sct", "code": "24484000", "display": "Severe"}]
        },
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "44054006", "display": "Diabetes mellitus type 2"}],
            "text": "Type 2 diabetes",
        },
        "bodySite": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "113257007", "display": "Structure of cardiovascular system"}]}
        ],
        "subject": {"reference": "Patient/p1", "display": "John Smith"},
        "encounter": {"reference": "Encounter/enc1"},
        "onsetDateTime": "2010-03-01T00:00:00+00:00",
        "recordedDate": "2010-03-15",
        "recorder": {"reference": "Practitioner/prac1"},
        "note": [{"text": "Well controlled with medication", "time": "2024-01-15T09:00:00+00:00"}],
    }
    result = extract(resource, "Condition", "base_r4")
    assert result["identifier"][0]["value"] == "C001"
    assert result["clinical_status"]["coding"][0]["code"] == "active"
    assert result["verification_status"]["coding"][0]["code"] == "confirmed"
    assert result["severity"]["coding"][0]["code"] == "24484000"
    assert result["code"]["text"] == "Type 2 diabetes"
    assert result["body_site"][0]["coding"][0]["code"] == "113257007"
    assert result["subject"]["reference"] == "Patient/p1"
    assert result["encounter"]["reference"] == "Encounter/enc1"
    assert result["onset_datetime"] == "2010-03-01T00:00:00+00:00"
    assert result["onset_period"] is None
    assert result["onset_string"] is None
    assert result["recorded_date"] == "2010-03-15"
    assert result["recorder"]["reference"] == "Practitioner/prac1"
    assert result["note"][0]["text"] == "Well controlled with medication"


def test_condition_extract_onset_period():
    resource = {
        "resourceType": "Condition", "id": "cond2",
        "code": {"coding": [{"system": "http://snomed.info/sct", "code": "73211009"}]},
        "subject": {"reference": "Patient/p1"},
        "onsetPeriod": {"start": "2020-01-01T00:00:00+00:00", "end": "2020-06-01T00:00:00+00:00"},
    }
    result = extract(resource, "Condition", "base_r4")
    assert result["onset_datetime"] is None
    assert result["onset_period"]["start"] == "2020-01-01T00:00:00+00:00"
    assert result["onset_period"]["end"] == "2020-06-01T00:00:00+00:00"
    assert result["onset_string"] is None


def test_condition_extract_onset_string():
    resource = {
        "resourceType": "Condition", "id": "cond3",
        "code": {"coding": [{"system": "http://snomed.info/sct", "code": "73211009"}]},
        "subject": {"reference": "Patient/p1"},
        "onsetString": "During childhood",
    }
    result = extract(resource, "Condition", "base_r4")
    assert result["onset_datetime"] is None
    assert result["onset_period"] is None
    assert result["onset_string"] == "During childhood"


def test_condition_extract_abatement_variants():
    resource = {
        "resourceType": "Condition", "id": "cond4",
        "code": {"coding": [{"system": "http://snomed.info/sct", "code": "195967001"}]},
        "subject": {"reference": "Patient/p1"},
        "clinicalStatus": {"coding": [{"code": "resolved"}]},
        "abatementDateTime": "2023-06-15T00:00:00+00:00",
    }
    result = extract(resource, "Condition", "base_r4")
    assert result["abatement_datetime"] == "2023-06-15T00:00:00+00:00"
    assert result["abatement_string"] is None


def test_condition_extract_abatement_string():
    resource = {
        "resourceType": "Condition", "id": "cond5",
        "code": {"coding": [{"system": "http://snomed.info/sct", "code": "195967001"}]},
        "subject": {"reference": "Patient/p1"},
        "abatementString": "Approximately 2 years ago",
    }
    result = extract(resource, "Condition", "base_r4")
    assert result["abatement_datetime"] is None
    assert result["abatement_string"] == "Approximately 2 years ago"


def test_condition_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Condition", "id": "cond-empty"}, "Condition", "base_r4")
    assert result["identifier"] == []
    assert result["clinical_status"] is None
    assert result["verification_status"] is None
    assert result["severity"] is None
    assert result["code"] is None
    assert result["body_site"] == []
    assert result["subject"] is None
    assert result["onset_datetime"] is None
    assert result["onset_period"] is None
    assert result["onset_string"] is None
    assert result["abatement_datetime"] is None
    assert result["abatement_string"] is None
    assert result["recorded_date"] is None
    assert result["note"] == []
