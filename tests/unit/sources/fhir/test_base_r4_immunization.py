"""Tests for base_r4 Immunization schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/immunization.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Immunization v2.4.0

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  vaccineCode              → vaccine_code
  occurrenceDateTime       → occurrence_datetime
  occurrenceString         → occurrence_string
  primarySource            → primary_source
  lotNumber                → lot_number
  expirationDate           → expiration_date
  doseQuantity             → dose_quantity
  isSubpotent              → is_subpotent
  programEligibility       → program_eligibility
  fundingSource            → funding_source
  protocolApplied          → protocol_applied
    doseNumberPositiveInt  → dose_number_positive_int
    seriesDosesPositiveInt → series_doses_positive_int
    targetDisease          → target_disease
"""
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructType, TimestampType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_immunization_schema_is_struct_type():
    assert isinstance(get_schema("Immunization", "base_r4"), StructType)


def test_immunization_has_common_fields():
    names = {f.name for f in get_schema("Immunization", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_immunization_status_is_string():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_immunization_vaccine_code_is_codeable_concept():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "vaccine_code")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"coding", "text"} <= inner_names


def test_immunization_occurrence_datetime_is_timestamp():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "occurrence_datetime")
    assert isinstance(f.dataType, TimestampType)


def test_immunization_occurrence_string_is_string():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "occurrence_string")
    assert isinstance(f.dataType, StringType)


def test_immunization_primary_source_is_boolean():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "primary_source")
    assert isinstance(f.dataType, BooleanType)


def test_immunization_is_subpotent_is_boolean():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "is_subpotent")
    assert isinstance(f.dataType, BooleanType)


def test_immunization_protocol_applied_is_array():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "protocol_applied")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {
        "series", "target_disease", "dose_number_positive_int",
        "series_doses_positive_int"
    } <= inner_names


def test_immunization_program_eligibility_is_array():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "program_eligibility")
    assert isinstance(f.dataType, ArrayType)


def test_immunization_performer_is_array_with_function_and_actor():
    schema = get_schema("Immunization", "base_r4")
    f = next(f for f in schema.fields if f.name == "performer")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"function", "actor"} <= inner_names


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_immunization_extract_full():
    resource = {
        "resourceType": "Immunization",
        "id": "imm1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/immunization-id", "value": "IMM001"}],
        "status": "completed",
        "vaccineCode": {
            "coding": [{"system": "http://snomed.info/sct", "code": "39114911000001105",
                         "display": "COVID-19 mRNA Vaccine Moderna 0.1mg/0.5mL"}],
            "text": "COVID-19 Vaccine Moderna",
        },
        "patient": {"reference": "Patient/p1", "display": "Jane Smith"},
        "encounter": {"reference": "Encounter/enc1"},
        "occurrenceDateTime": "2021-03-15T10:30:00+00:00",
        "recorded": "2021-03-15",
        "primarySource": True,
        "manufacturer": {"reference": "Organization/moderna"},
        "lotNumber": "MOD-BATCH-001",
        "expirationDate": "2022-06-01",
        "site": {
            "coding": [{"system": "http://snomed.info/sct", "code": "368209003", "display": "Right arm"}]
        },
        "route": {
            "coding": [{"system": "http://snomed.info/sct", "code": "78421000", "display": "Intramuscular route"}]
        },
        "doseQuantity": {"value": 0.5, "unit": "mL", "system": "http://unitsofmeasure.org", "code": "mL"},
        "performer": [
            {
                "function": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0443",
                                          "code": "AP", "display": "Administering Provider"}]},
                "actor": {"reference": "Practitioner/prac1", "display": "Dr. Jones"},
            }
        ],
        "reasonCode": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "429060002",
                          "display": "Procedure to meet occupational requirement"}]}
        ],
        "isSubpotent": False,
        "programEligibility": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/immunization-program-eligibility",
                          "code": "ineligible", "display": "Not Eligible"}]}
        ],
        "fundingSource": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/immunization-funding-source",
                         "code": "public", "display": "Public"}]
        },
        "protocolApplied": [
            {
                "series": "COVID-19 2-dose series",
                "targetDisease": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "840539006",
                                  "display": "COVID-19"}]}
                ],
                "doseNumberPositiveInt": 1,
                "seriesDosesPositiveInt": 2,
            }
        ],
    }
    result = extract(resource, "Immunization", "base_r4")
    assert result["identifier"][0]["value"] == "IMM001"
    assert result["status"] == "completed"
    assert result["vaccine_code"]["coding"][0]["code"] == "39114911000001105"
    assert result["patient"]["reference"] == "Patient/p1"
    assert result["encounter"]["reference"] == "Encounter/enc1"
    assert result["occurrence_datetime"] == "2021-03-15T10:30:00+00:00"
    assert result["occurrence_string"] is None
    assert result["recorded"] == "2021-03-15"
    assert result["primary_source"] is True
    assert result["manufacturer"]["reference"] == "Organization/moderna"
    assert result["lot_number"] == "MOD-BATCH-001"
    assert result["expiration_date"] == "2022-06-01"
    assert result["site"]["coding"][0]["code"] == "368209003"
    assert result["route"]["coding"][0]["code"] == "78421000"
    assert result["dose_quantity"]["value"] == 0.5
    assert result["dose_quantity"]["unit"] == "mL"
    assert len(result["performer"]) == 1
    assert result["performer"][0]["function"]["coding"][0]["code"] == "AP"
    assert result["performer"][0]["actor"]["reference"] == "Practitioner/prac1"
    assert result["reason_code"][0]["coding"][0]["code"] == "429060002"
    assert result["is_subpotent"] is False
    assert result["program_eligibility"][0]["coding"][0]["code"] == "ineligible"
    assert result["funding_source"]["coding"][0]["code"] == "public"
    assert len(result["protocol_applied"]) == 1
    pa = result["protocol_applied"][0]
    assert pa["series"] == "COVID-19 2-dose series"
    assert pa["target_disease"][0]["coding"][0]["code"] == "840539006"
    assert pa["dose_number_positive_int"] == 1
    assert pa["series_doses_positive_int"] == 2


def test_immunization_extract_occurrence_string():
    resource = {
        "resourceType": "Immunization", "id": "imm2",
        "status": "completed",
        "vaccineCode": {"coding": [{"system": "http://snomed.info/sct", "code": "39114911000001105"}]},
        "patient": {"reference": "Patient/p1"},
        "occurrenceString": "Approx. spring 2021",
    }
    result = extract(resource, "Immunization", "base_r4")
    assert result["occurrence_datetime"] is None
    assert result["occurrence_string"] == "Approx. spring 2021"


def test_immunization_extract_protocol_applied_string_variants():
    resource = {
        "resourceType": "Immunization", "id": "imm3",
        "status": "completed",
        "vaccineCode": {"coding": [{"system": "http://snomed.info/sct", "code": "39114911000001105"}]},
        "patient": {"reference": "Patient/p1"},
        "occurrenceDateTime": "2021-06-01T00:00:00+00:00",
        "protocolApplied": [
            {
                "series": "3-dose series",
                "doseNumberString": "first",
                "seriesDosesString": "three",
            }
        ],
    }
    result = extract(resource, "Immunization", "base_r4")
    pa = result["protocol_applied"][0]
    assert pa["dose_number_positive_int"] is None
    assert pa["dose_number_string"] == "first"
    assert pa["series_doses_positive_int"] is None
    assert pa["series_doses_string"] == "three"


def test_immunization_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Immunization", "id": "imm-empty"}, "Immunization", "base_r4")
    assert result["identifier"] == []
    assert result["status"] is None
    assert result["status_reason"] is None
    assert result["vaccine_code"] is None
    assert result["patient"] is None
    assert result["occurrence_datetime"] is None
    assert result["occurrence_string"] is None
    assert result["recorded"] is None
    assert result["primary_source"] is None
    assert result["lot_number"] is None
    assert result["expiration_date"] is None
    assert result["dose_quantity"] is None
    assert result["performer"] == []
    assert result["reason_code"] == []
    assert result["reason_reference"] == []
    assert result["is_subpotent"] is None
    assert result["program_eligibility"] == []
    assert result["funding_source"] is None
    assert result["protocol_applied"] == []
