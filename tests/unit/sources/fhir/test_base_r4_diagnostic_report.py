"""Tests for base_r4 DiagnosticReport schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/diagnosticreport.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-DiagnosticReport v2.5.0

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  basedOn              → based_on
  effectiveDateTime    → effective_datetime
  effectivePeriod      → effective_period
  resultsInterpreter   → results_interpreter
  conclusionCode       → conclusion_code
  category             → category  (0..* CodeableConcept — array)
  result               → result    (array of Reference to Observation)
"""
from pyspark.sql.types import ArrayType, StringType, StructType, TimestampType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_diagnostic_report_schema_is_struct_type():
    assert isinstance(get_schema("DiagnosticReport", "base_r4"), StructType)


def test_diagnostic_report_has_common_fields():
    names = {f.name for f in get_schema("DiagnosticReport", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_diagnostic_report_status_is_string():
    schema = get_schema("DiagnosticReport", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_diagnostic_report_category_is_array():
    """category is 0..* CodeableConcept — must be ArrayType."""
    schema = get_schema("DiagnosticReport", "base_r4")
    f = next(f for f in schema.fields if f.name == "category")
    assert isinstance(f.dataType, ArrayType)


def test_diagnostic_report_effective_datetime_is_timestamp():
    schema = get_schema("DiagnosticReport", "base_r4")
    f = next(f for f in schema.fields if f.name == "effective_datetime")
    assert isinstance(f.dataType, TimestampType)


def test_diagnostic_report_effective_period_exists():
    names = {f.name for f in get_schema("DiagnosticReport", "base_r4").fields}
    assert "effective_period" in names


def test_diagnostic_report_result_is_array():
    """result is 0..* Reference(Observation) — must be ArrayType."""
    schema = get_schema("DiagnosticReport", "base_r4")
    f = next(f for f in schema.fields if f.name == "result")
    assert isinstance(f.dataType, ArrayType)
    # elements are Reference structs with 'reference' field
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert "reference" in inner_names


def test_diagnostic_report_results_interpreter_is_array():
    schema = get_schema("DiagnosticReport", "base_r4")
    f = next(f for f in schema.fields if f.name == "results_interpreter")
    assert isinstance(f.dataType, ArrayType)


def test_diagnostic_report_conclusion_code_is_array():
    schema = get_schema("DiagnosticReport", "base_r4")
    f = next(f for f in schema.fields if f.name == "conclusion_code")
    assert isinstance(f.dataType, ArrayType)


def test_diagnostic_report_based_on_is_array():
    schema = get_schema("DiagnosticReport", "base_r4")
    f = next(f for f in schema.fields if f.name == "based_on")
    assert isinstance(f.dataType, ArrayType)


def test_diagnostic_report_conclusion_is_string():
    schema = get_schema("DiagnosticReport", "base_r4")
    f = next(f for f in schema.fields if f.name == "conclusion")
    assert isinstance(f.dataType, StringType)


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_diagnostic_report_extract_full():
    resource = {
        "resourceType": "DiagnosticReport",
        "id": "dr1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/report-id", "value": "DR001"}],
        "basedOn": [{"reference": "ServiceRequest/sr1"}],
        "status": "final",
        "category": [
            {
                "coding": [
                    {"system": "http://terminology.hl7.org/CodeSystem/v2-0074",
                     "code": "LAB", "display": "Laboratory"}
                ]
            }
        ],
        "code": {
            "coding": [{"system": "http://loinc.org", "code": "58410-2", "display": "CBC panel"}],
            "text": "Complete Blood Count",
        },
        "subject": {"reference": "Patient/p1", "display": "John Smith"},
        "encounter": {"reference": "Encounter/enc1"},
        "effectiveDateTime": "2024-01-15T09:00:00+00:00",
        "issued": "2024-01-15T12:00:00+00:00",
        "performer": [{"reference": "Organization/org1", "display": "NHS Lab"}],
        "resultsInterpreter": [{"reference": "Practitioner/prac1", "display": "Dr. Smith"}],
        "specimen": [{"reference": "Specimen/spec1"}],
        "result": [
            {"reference": "Observation/obs1"},
            {"reference": "Observation/obs2"},
        ],
        "conclusion": "Normal results — no abnormalities detected.",
        "conclusionCode": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "281900007", "display": "No abnormality detected"}]}
        ],
    }
    result = extract(resource, "DiagnosticReport", "base_r4")
    assert result["identifier"][0]["value"] == "DR001"
    assert result["based_on"][0]["reference"] == "ServiceRequest/sr1"
    assert result["status"] == "final"
    assert result["category"][0]["coding"][0]["code"] == "LAB"
    assert result["code"]["text"] == "Complete Blood Count"
    assert result["subject"]["reference"] == "Patient/p1"
    assert result["encounter"]["reference"] == "Encounter/enc1"
    assert result["effective_datetime"] == "2024-01-15T09:00:00+00:00"
    assert result["effective_period"] is None
    assert result["issued"] == "2024-01-15T12:00:00+00:00"
    assert result["performer"][0]["reference"] == "Organization/org1"
    assert result["results_interpreter"][0]["reference"] == "Practitioner/prac1"
    assert result["specimen"][0]["reference"] == "Specimen/spec1"
    assert len(result["result"]) == 2
    assert result["result"][0]["reference"] == "Observation/obs1"
    assert result["result"][1]["reference"] == "Observation/obs2"
    assert result["conclusion"] == "Normal results — no abnormalities detected."
    assert result["conclusion_code"][0]["coding"][0]["code"] == "281900007"


def test_diagnostic_report_extract_effective_period():
    resource = {
        "resourceType": "DiagnosticReport", "id": "dr2",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "58410-2"}]},
        "subject": {"reference": "Patient/p1"},
        "effectivePeriod": {"start": "2024-01-14T00:00:00+00:00", "end": "2024-01-15T00:00:00+00:00"},
    }
    result = extract(resource, "DiagnosticReport", "base_r4")
    assert result["effective_datetime"] is None
    assert result["effective_period"]["start"] == "2024-01-14T00:00:00+00:00"
    assert result["effective_period"]["end"] == "2024-01-15T00:00:00+00:00"


def test_diagnostic_report_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "DiagnosticReport", "id": "dr-empty"}, "DiagnosticReport", "base_r4")
    assert result["identifier"] == []
    assert result["based_on"] == []
    assert result["status"] is None
    assert result["category"] == []
    assert result["code"] is None
    assert result["subject"] is None
    assert result["encounter"] is None
    assert result["effective_datetime"] is None
    assert result["effective_period"] is None
    assert result["issued"] is None
    assert result["performer"] == []
    assert result["results_interpreter"] == []
    assert result["specimen"] == []
    assert result["result"] == []
    assert result["conclusion"] is None
    assert result["conclusion_code"] == []
