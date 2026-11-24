"""Tests for base_r4 Procedure schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/procedure.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Procedure v2.5.0

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  statusReason      → status_reason
  performedDateTime → performed_datetime
  performedPeriod   → performed_period
  reasonCode        → reason_code
  reasonReference   → reason_reference
  bodySite          → body_site
  followUp          → follow_up
  onBehalfOf        → on_behalf_of   (on performer)
"""
from pyspark.sql.types import ArrayType, StringType, StructType, TimestampType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_procedure_schema_is_struct_type():
    assert isinstance(get_schema("Procedure", "base_r4"), StructType)


def test_procedure_has_common_fields():
    names = {f.name for f in get_schema("Procedure", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_procedure_status_is_string():
    schema = get_schema("Procedure", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_procedure_performed_datetime_is_timestamp():
    schema = get_schema("Procedure", "base_r4")
    f = next(f for f in schema.fields if f.name == "performed_datetime")
    assert isinstance(f.dataType, TimestampType)


def test_procedure_performed_period_exists():
    names = {f.name for f in get_schema("Procedure", "base_r4").fields}
    assert "performed_period" in names


def test_procedure_reason_code_is_array():
    schema = get_schema("Procedure", "base_r4")
    f = next(f for f in schema.fields if f.name == "reason_code")
    assert isinstance(f.dataType, ArrayType)


def test_procedure_reason_reference_is_array():
    schema = get_schema("Procedure", "base_r4")
    f = next(f for f in schema.fields if f.name == "reason_reference")
    assert isinstance(f.dataType, ArrayType)


def test_procedure_body_site_is_array():
    schema = get_schema("Procedure", "base_r4")
    f = next(f for f in schema.fields if f.name == "body_site")
    assert isinstance(f.dataType, ArrayType)


def test_procedure_follow_up_is_array():
    schema = get_schema("Procedure", "base_r4")
    f = next(f for f in schema.fields if f.name == "follow_up")
    assert isinstance(f.dataType, ArrayType)


def test_procedure_performer_is_array_with_inner_fields():
    schema = get_schema("Procedure", "base_r4")
    f = next(f for f in schema.fields if f.name == "performer")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"function", "actor", "on_behalf_of"} <= inner_names


def test_procedure_note_is_array():
    schema = get_schema("Procedure", "base_r4")
    f = next(f for f in schema.fields if f.name == "note")
    assert isinstance(f.dataType, ArrayType)


def test_procedure_status_reason_exists():
    names = {f.name for f in get_schema("Procedure", "base_r4").fields}
    assert "status_reason" in names


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_procedure_extract_full():
    resource = {
        "resourceType": "Procedure",
        "id": "proc1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/procedure-id", "value": "PROC001"}],
        "status": "completed",
        "statusReason": {
            "coding": [{"system": "http://snomed.info/sct", "code": "182992009", "display": "Treatment completed"}]
        },
        "category": {
            "coding": [{"system": "http://snomed.info/sct", "code": "387713003", "display": "Surgical procedure"}]
        },
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "80146002", "display": "Appendectomy"}],
            "text": "Appendectomy",
        },
        "subject": {"reference": "Patient/p1", "display": "John Smith"},
        "encounter": {"reference": "Encounter/enc1"},
        "performedDateTime": "2024-01-15T10:00:00+00:00",
        "recorder": {"reference": "Practitioner/prac1"},
        "asserter": {"reference": "Practitioner/prac1"},
        "performer": [
            {
                "function": {"coding": [{"code": "SPRF", "display": "Secondary performer"}]},
                "actor": {"reference": "Practitioner/prac2", "display": "Dr. Jones"},
                "onBehalfOf": {"reference": "Organization/org1"},
            }
        ],
        "location": {"reference": "Location/loc1"},
        "reasonCode": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "74400008", "display": "Appendicitis"}]}
        ],
        "reasonReference": [{"reference": "Condition/cond1"}],
        "bodySite": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "66754008", "display": "Appendix structure"}]}
        ],
        "outcome": {
            "coding": [{"system": "http://snomed.info/sct", "code": "385669000", "display": "Successful"}]
        },
        "report": [{"reference": "DiagnosticReport/dr1"}],
        "complication": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "131148009", "display": "Bleeding"}]}
        ],
        "followUp": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "225368008", "display": "Contact follow up"}]}
        ],
        "note": [{"text": "Procedure completed without complications"}],
    }
    result = extract(resource, "Procedure", "base_r4")
    assert result["identifier"][0]["value"] == "PROC001"
    assert result["status"] == "completed"
    assert result["status_reason"]["coding"][0]["code"] == "182992009"
    assert result["category"]["coding"][0]["code"] == "387713003"
    assert result["code"]["text"] == "Appendectomy"
    assert result["subject"]["reference"] == "Patient/p1"
    assert result["encounter"]["reference"] == "Encounter/enc1"
    assert result["performed_datetime"] == "2024-01-15T10:00:00+00:00"
    assert result["performed_period"] is None
    assert result["recorder"]["reference"] == "Practitioner/prac1"
    assert result["asserter"]["reference"] == "Practitioner/prac1"
    # performer
    assert len(result["performer"]) == 1
    perf = result["performer"][0]
    assert perf["function"]["coding"][0]["code"] == "SPRF"
    assert perf["actor"]["reference"] == "Practitioner/prac2"
    assert perf["on_behalf_of"]["reference"] == "Organization/org1"
    # reason_code, reason_reference, body_site
    assert result["reason_code"][0]["coding"][0]["code"] == "74400008"
    assert result["reason_reference"][0]["reference"] == "Condition/cond1"
    assert result["body_site"][0]["coding"][0]["code"] == "66754008"
    # outcome, report, complication, follow_up
    assert result["outcome"]["coding"][0]["code"] == "385669000"
    assert result["report"][0]["reference"] == "DiagnosticReport/dr1"
    assert result["complication"][0]["coding"][0]["code"] == "131148009"
    assert result["follow_up"][0]["coding"][0]["code"] == "225368008"
    assert result["note"][0]["text"] == "Procedure completed without complications"


def test_procedure_extract_performed_period():
    resource = {
        "resourceType": "Procedure", "id": "proc2",
        "status": "completed",
        "code": {"coding": [{"system": "http://snomed.info/sct", "code": "80146002"}]},
        "subject": {"reference": "Patient/p1"},
        "performedPeriod": {"start": "2024-01-15T09:00:00+00:00", "end": "2024-01-15T11:00:00+00:00"},
    }
    result = extract(resource, "Procedure", "base_r4")
    assert result["performed_datetime"] is None
    assert result["performed_period"]["start"] == "2024-01-15T09:00:00+00:00"
    assert result["performed_period"]["end"] == "2024-01-15T11:00:00+00:00"


def test_procedure_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Procedure", "id": "proc-empty"}, "Procedure", "base_r4")
    assert result["identifier"] == []
    assert result["status"] is None
    assert result["status_reason"] is None
    assert result["category"] is None
    assert result["code"] is None
    assert result["subject"] is None
    assert result["encounter"] is None
    assert result["performed_datetime"] is None
    assert result["performed_period"] is None
    assert result["recorder"] is None
    assert result["asserter"] is None
    assert result["performer"] == []
    assert result["location"] is None
    assert result["reason_code"] == []
    assert result["reason_reference"] == []
    assert result["body_site"] == []
    assert result["outcome"] is None
    assert result["report"] == []
    assert result["complication"] == []
    assert result["follow_up"] == []
    assert result["note"] == []
