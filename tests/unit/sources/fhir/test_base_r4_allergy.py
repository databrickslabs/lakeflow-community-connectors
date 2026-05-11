"""Tests for base_r4 AllergyIntolerance schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/allergyintolerance.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-AllergyIntolerance v2.5.0

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  clinicalStatus       → clinical_status
  verificationStatus   → verification_status
  category             → category  (CRITICAL: 0..* code — array of strings, NOT CodeableConcept)
  onsetDateTime        → onset_datetime
  onsetString          → onset_string
  recordedDate         → recorded_date
  lastOccurrence       → last_occurrence
  exposureRoute        → exposure_route   (on reaction)
"""
from pyspark.sql.types import ArrayType, StringType, StructType, TimestampType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_allergy_schema_is_struct_type():
    assert isinstance(get_schema("AllergyIntolerance", "base_r4"), StructType)


def test_allergy_has_common_fields():
    names = {f.name for f in get_schema("AllergyIntolerance", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_allergy_category_is_array_of_strings_not_codeable_concept():
    """CRITICAL: category is 0..* code (array of strings), per FHIR R4 spec table."""
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "category")
    assert isinstance(f.dataType, ArrayType)
    # The element type must be StringType — NOT a StructType (CodeableConcept)
    assert isinstance(f.dataType.elementType, StringType), (
        f"category element type should be StringType, got {f.dataType.elementType}"
    )


def test_allergy_clinical_status_is_codeable_concept():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "clinical_status")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner_names


def test_allergy_verification_status_is_codeable_concept():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "verification_status")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner_names


def test_allergy_type_is_string():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "type")
    assert isinstance(f.dataType, StringType)


def test_allergy_criticality_is_string():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "criticality")
    assert isinstance(f.dataType, StringType)


def test_allergy_onset_datetime_is_timestamp():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "onset_datetime")
    assert isinstance(f.dataType, TimestampType)


def test_allergy_onset_string_is_string():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "onset_string")
    assert isinstance(f.dataType, StringType)


def test_allergy_last_occurrence_is_timestamp():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "last_occurrence")
    assert isinstance(f.dataType, TimestampType)


def test_allergy_reaction_is_array_with_inner_fields():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "reaction")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"substance", "manifestation", "severity", "exposure_route"} <= inner_names


def test_allergy_reaction_manifestation_is_array():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "reaction")
    manifestation_field = next(
        sf for sf in f.dataType.elementType.fields if sf.name == "manifestation"
    )
    assert isinstance(manifestation_field.dataType, ArrayType)


def test_allergy_note_is_array():
    schema = get_schema("AllergyIntolerance", "base_r4")
    f = next(f for f in schema.fields if f.name == "note")
    assert isinstance(f.dataType, ArrayType)


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_allergy_extract_full():
    resource = {
        "resourceType": "AllergyIntolerance",
        "id": "ai1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/allergy-id", "value": "AI001"}],
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical",
                        "code": "active", "display": "Active"}]
        },
        "verificationStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification",
                        "code": "confirmed", "display": "Confirmed"}]
        },
        "type": "allergy",
        "category": ["medication", "food"],
        "criticality": "high",
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "372687004", "display": "Amoxicillin"}],
            "text": "Amoxicillin",
        },
        "patient": {"reference": "Patient/p1", "display": "John Smith"},
        "encounter": {"reference": "Encounter/enc1"},
        "onsetDateTime": "2015-06-01T00:00:00+00:00",
        "recordedDate": "2015-06-15",
        "recorder": {"reference": "Practitioner/prac1"},
        "lastOccurrence": "2023-11-01T00:00:00+00:00",
        "note": [{"text": "Patient reports severe reaction"}],
        "reaction": [
            {
                "substance": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "372687004", "display": "Amoxicillin"}]
                },
                "manifestation": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "247472004", "display": "Urticaria"}]}
                ],
                "description": "Widespread hives",
                "onset": "2015-06-01T14:00:00+00:00",
                "severity": "severe",
                "exposureRoute": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "26643006", "display": "Oral route"}]
                },
            }
        ],
    }
    result = extract(resource, "AllergyIntolerance", "base_r4")
    assert result["identifier"][0]["value"] == "AI001"
    assert result["clinical_status"]["coding"][0]["code"] == "active"
    assert result["verification_status"]["coding"][0]["code"] == "confirmed"
    assert result["type"] == "allergy"
    # CRITICAL: category is a plain list of string codes
    assert result["category"] == ["medication", "food"]
    assert isinstance(result["category"][0], str)
    assert result["criticality"] == "high"
    assert result["code"]["text"] == "Amoxicillin"
    assert result["patient"]["reference"] == "Patient/p1"
    assert result["encounter"]["reference"] == "Encounter/enc1"
    assert result["onset_datetime"] == "2015-06-01T00:00:00+00:00"
    assert result["onset_string"] is None
    assert result["recorded_date"] == "2015-06-15"
    assert result["last_occurrence"] == "2023-11-01T00:00:00+00:00"
    assert result["note"][0]["text"] == "Patient reports severe reaction"
    # Reaction
    assert len(result["reaction"]) == 1
    rxn = result["reaction"][0]
    assert rxn["substance"]["coding"][0]["code"] == "372687004"
    assert rxn["manifestation"][0]["coding"][0]["code"] == "247472004"
    assert rxn["description"] == "Widespread hives"
    assert rxn["severity"] == "severe"
    assert rxn["exposure_route"]["coding"][0]["code"] == "26643006"


def test_allergy_extract_onset_string():
    resource = {
        "resourceType": "AllergyIntolerance", "id": "ai2",
        "code": {"coding": [{"system": "http://snomed.info/sct", "code": "372687004"}]},
        "patient": {"reference": "Patient/p1"},
        "onsetString": "Approximately 5 years ago",
    }
    result = extract(resource, "AllergyIntolerance", "base_r4")
    assert result["onset_datetime"] is None
    assert result["onset_string"] == "Approximately 5 years ago"


def test_allergy_extract_reaction_with_no_exposure_route():
    resource = {
        "resourceType": "AllergyIntolerance", "id": "ai3",
        "code": {"coding": [{"system": "http://snomed.info/sct", "code": "387070000"}]},
        "patient": {"reference": "Patient/p1"},
        "reaction": [
            {
                "manifestation": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "39579001", "display": "Anaphylaxis"}]}
                ],
                "severity": "life-threatening",
            }
        ],
    }
    result = extract(resource, "AllergyIntolerance", "base_r4")
    rxn = result["reaction"][0]
    assert rxn["manifestation"][0]["coding"][0]["code"] == "39579001"
    assert rxn["severity"] == "life-threatening"
    assert rxn["exposure_route"] is None
    assert rxn["substance"] is None


def test_allergy_extract_category_empty():
    resource = {
        "resourceType": "AllergyIntolerance", "id": "ai4",
        "code": {"coding": [{"system": "http://snomed.info/sct", "code": "372687004"}]},
        "patient": {"reference": "Patient/p1"},
    }
    result = extract(resource, "AllergyIntolerance", "base_r4")
    assert result["category"] == []


def test_allergy_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "AllergyIntolerance", "id": "ai-empty"}, "AllergyIntolerance", "base_r4")
    assert result["identifier"] == []
    assert result["clinical_status"] is None
    assert result["verification_status"] is None
    assert result["type"] is None
    assert result["category"] == []
    assert result["criticality"] is None
    assert result["code"] is None
    assert result["patient"] is None
    assert result["onset_datetime"] is None
    assert result["onset_string"] is None
    assert result["recorded_date"] is None
    assert result["last_occurrence"] is None
    assert result["note"] == []
    assert result["reaction"] == []
