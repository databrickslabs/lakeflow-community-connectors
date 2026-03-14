"""Tests for base_r4 Encounter schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/encounter.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Encounter v2.5.0

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  class             → class_coding  (CRITICAL: FHIR type is Coding 1..1, NOT CodeableConcept)
  serviceType       → service_type
  episodeOfCare     → episode_of_care
  reasonCode        → reason_code
  reasonReference   → reason_reference
  serviceProvider   → service_provider
  partOf            → part_of
  admitSource       → admit_source       (on hospitalization)
  reAdmission       → re_admission       (on hospitalization)
  dischargeDisposition → discharge_disposition  (on hospitalization)
  physicalType      → physical_type      (on location)
"""
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_encounter_schema_is_struct_type():
    assert isinstance(get_schema("Encounter", "base_r4"), StructType)


def test_encounter_has_common_fields():
    names = {f.name for f in get_schema("Encounter", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_encounter_status_is_string():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_encounter_class_coding_is_coding_not_codeable_concept():
    """CRITICAL: Encounter.class is FHIR type Coding (not CodeableConcept).
    Schema field is class_coding with fields: system, code, display.
    It must NOT have a 'coding' (array) sub-field.
    """
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "class_coding")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    # Coding has system, code, display — no nested 'coding' array
    assert {"system", "code", "display"} <= inner_names
    assert "coding" not in inner_names, (
        "class_coding must be FHIR Coding (not CodeableConcept) — should NOT have a 'coding' sub-array"
    )


def test_encounter_participant_is_array_with_inner_fields():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "participant")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"type", "period", "individual"} <= inner_names


def test_encounter_diagnosis_is_array_with_inner_fields():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "diagnosis")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"condition", "use", "rank"} <= inner_names


def test_encounter_diagnosis_rank_is_integer():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "diagnosis")
    rank_field = next(sf for sf in f.dataType.elementType.fields if sf.name == "rank")
    assert isinstance(rank_field.dataType, IntegerType)


def test_encounter_hospitalization_is_struct_with_fields():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "hospitalization")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"admit_source", "discharge_disposition"} <= inner_names


def test_encounter_location_is_array():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "location")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"location", "status", "physical_type", "period"} <= inner_names


def test_encounter_episode_of_care_is_array():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "episode_of_care")
    assert isinstance(f.dataType, ArrayType)


def test_encounter_reason_code_is_array():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "reason_code")
    assert isinstance(f.dataType, ArrayType)


def test_encounter_reason_reference_is_array():
    schema = get_schema("Encounter", "base_r4")
    f = next(f for f in schema.fields if f.name == "reason_reference")
    assert isinstance(f.dataType, ArrayType)


def test_encounter_service_provider_exists():
    names = {f.name for f in get_schema("Encounter", "base_r4").fields}
    assert "service_provider" in names


def test_encounter_part_of_exists():
    names = {f.name for f in get_schema("Encounter", "base_r4").fields}
    assert "part_of" in names


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_encounter_extract_full():
    resource = {
        "resourceType": "Encounter",
        "id": "enc1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/encounter-id", "value": "ENC001"}],
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "AMB",
            "display": "ambulatory",
        },
        "type": [
            {
                "coding": [{"system": "http://snomed.info/sct", "code": "11429006", "display": "Consultation"}],
                "text": "Consultation",
            }
        ],
        "serviceType": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/service-type",
                        "code": "355", "display": "General Practice"}]
        },
        "priority": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                        "code": "R", "display": "routine"}]
        },
        "subject": {"reference": "Patient/p1", "display": "John Smith"},
        "episodeOfCare": [{"reference": "EpisodeOfCare/eoc1"}],
        "participant": [
            {
                "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                      "code": "PART", "display": "Participant"}]}],
                "period": {"start": "2024-01-15T09:00:00+00:00", "end": "2024-01-15T10:00:00+00:00"},
                "individual": {"reference": "Practitioner/prac1", "display": "Dr. Jones"},
            }
        ],
        "period": {"start": "2024-01-15T09:00:00+00:00", "end": "2024-01-15T10:00:00+00:00"},
        "reasonCode": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "410429000", "display": "Cardiac arrest"}]}
        ],
        "reasonReference": [{"reference": "Condition/cond1"}],
        "diagnosis": [
            {
                "condition": {"reference": "Condition/cond1"},
                "use": {"coding": [{"code": "AD", "display": "Admission diagnosis"}]},
                "rank": 1,
            }
        ],
        "hospitalization": {
            "admitSource": {
                "coding": [{"system": "http://terminology.hl7.org/CodeSystem/admit-source",
                            "code": "gp", "display": "General Practitioner referral"}]
            },
            "dischargeDisposition": {
                "coding": [{"system": "http://terminology.hl7.org/CodeSystem/discharge-disposition",
                            "code": "home", "display": "Home"}]
            },
        },
        "location": [
            {
                "location": {"reference": "Location/loc1", "display": "Cardiology Ward"},
                "status": "active",
                "physicalType": {"coding": [{"code": "wa", "display": "Ward"}]},
                "period": {"start": "2024-01-15T09:00:00+00:00"},
            }
        ],
        "serviceProvider": {"reference": "Organization/org1", "display": "NHS Trust"},
        "partOf": {"reference": "Encounter/enc-parent"},
    }
    result = extract(resource, "Encounter", "base_r4")
    assert result["identifier"][0]["value"] == "ENC001"
    assert result["status"] == "finished"
    # class_coding must be a flat Coding struct — system, code, display
    assert result["class_coding"]["system"] == "http://terminology.hl7.org/CodeSystem/v3-ActCode"
    assert result["class_coding"]["code"] == "AMB"
    assert result["class_coding"]["display"] == "ambulatory"
    assert result["type"][0]["text"] == "Consultation"
    assert result["service_type"]["coding"][0]["code"] == "355"
    assert result["priority"]["coding"][0]["code"] == "R"
    assert result["subject"]["reference"] == "Patient/p1"
    assert result["episode_of_care"][0]["reference"] == "EpisodeOfCare/eoc1"
    # participant
    assert len(result["participant"]) == 1
    p = result["participant"][0]
    assert p["type"][0]["coding"][0]["code"] == "PART"
    assert p["period"]["start"] == "2024-01-15T09:00:00+00:00"
    assert p["individual"]["reference"] == "Practitioner/prac1"
    # period
    assert result["period"]["start"] == "2024-01-15T09:00:00+00:00"
    # reason_code and reason_reference
    assert result["reason_code"][0]["coding"][0]["code"] == "410429000"
    assert result["reason_reference"][0]["reference"] == "Condition/cond1"
    # diagnosis
    assert len(result["diagnosis"]) == 1
    d = result["diagnosis"][0]
    assert d["condition"]["reference"] == "Condition/cond1"
    assert d["use"]["coding"][0]["code"] == "AD"
    assert d["rank"] == 1
    # hospitalization
    assert result["hospitalization"]["admit_source"]["coding"][0]["code"] == "gp"
    assert result["hospitalization"]["discharge_disposition"]["coding"][0]["code"] == "home"
    # location
    assert len(result["location"]) == 1
    loc = result["location"][0]
    assert loc["location"]["reference"] == "Location/loc1"
    assert loc["status"] == "active"
    assert loc["physical_type"]["coding"][0]["code"] == "wa"
    # service_provider and part_of
    assert result["service_provider"]["reference"] == "Organization/org1"
    assert result["part_of"]["reference"] == "Encounter/enc-parent"


def test_encounter_extract_class_coding_from_class_key():
    """Verifies that class_coding is populated from r['class'], not r['class_coding']."""
    resource = {
        "resourceType": "Encounter", "id": "enc2",
        "status": "in-progress",
        "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP", "display": "inpatient encounter"},
    }
    result = extract(resource, "Encounter", "base_r4")
    assert result["class_coding"] is not None
    assert result["class_coding"]["code"] == "IMP"
    assert result["class_coding"]["display"] == "inpatient encounter"


def test_encounter_extract_missing_class_returns_none():
    resource = {
        "resourceType": "Encounter", "id": "enc3",
        "status": "planned",
    }
    result = extract(resource, "Encounter", "base_r4")
    assert result["class_coding"] is None


def test_encounter_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Encounter", "id": "enc-empty"}, "Encounter", "base_r4")
    assert result["identifier"] == []
    assert result["status"] is None
    assert result["class_coding"] is None
    assert result["type"] == []
    assert result["service_type"] is None
    assert result["subject"] is None
    assert result["episode_of_care"] == []
    assert result["participant"] == []
    assert result["period"] is None
    assert result["reason_code"] == []
    assert result["reason_reference"] == []
    assert result["diagnosis"] == []
    assert result["hospitalization"] is None
    assert result["location"] == []
    assert result["service_provider"] is None
    assert result["part_of"] is None
