"""Tests for base_r4 MedicationRequest schema and extractor.

Validated against:
  FHIR R4: https://hl7.org/fhir/R4/medicationrequest.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-MedicationRequest v2.5.0

Field name verification (camelCase in FHIR JSON → snake_case in schema):
  statusReason              → status_reason
  medicationCodeableConcept → medication_codeable_concept
  medicationReference       → medication_reference
  authoredOn                → authored_on
  dosageInstruction         → dosage_instruction
  dispenseRequest           → dispense_request
    numberOfRepeatsAllowed  → number_of_repeats_allowed
    quantity.value          → quantity_value
    quantity.unit           → quantity_unit
    validityPeriod          → validity_period
    expectedSupplyDuration  → expected_supply_duration_value / _unit
  substitution.allowedBoolean → substitution_allowed_boolean
"""
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, StringType, StructType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ── Schema structural tests ───────────────────────────────────────────────────

def test_medication_request_schema_is_struct_type():
    assert isinstance(get_schema("MedicationRequest", "base_r4"), StructType)


def test_medication_request_has_common_fields():
    names = {f.name for f in get_schema("MedicationRequest", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_medication_request_status_is_string():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_medication_request_intent_is_string():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "intent")
    assert isinstance(f.dataType, StringType)


def test_medication_request_medication_codeable_concept_exists():
    """medication[x] polymorphic — both variants must exist in schema."""
    names = {f.name for f in get_schema("MedicationRequest", "base_r4").fields}
    assert "medication_codeable_concept" in names


def test_medication_request_medication_reference_exists():
    names = {f.name for f in get_schema("MedicationRequest", "base_r4").fields}
    assert "medication_reference" in names


def test_medication_request_dosage_instruction_is_array_of_dosage():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "dosage_instruction")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    # DOSAGE struct has at minimum: text, dose_value, dose_unit
    assert {"text", "dose_value", "dose_unit"} <= inner_names


def test_medication_request_dispense_request_is_struct():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "dispense_request")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"validity_period", "number_of_repeats_allowed", "quantity_value",
            "quantity_unit", "expected_supply_duration_value",
            "expected_supply_duration_unit"} <= inner_names


def test_medication_request_dispense_request_number_of_repeats_is_integer():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "dispense_request")
    rpt = next(sf for sf in f.dataType.fields if sf.name == "number_of_repeats_allowed")
    assert isinstance(rpt.dataType, IntegerType)


def test_medication_request_dispense_request_quantity_value_is_double():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "dispense_request")
    qty = next(sf for sf in f.dataType.fields if sf.name == "quantity_value")
    assert isinstance(qty.dataType, DoubleType)


def test_medication_request_substitution_allowed_boolean_is_bool():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "substitution_allowed_boolean")
    assert isinstance(f.dataType, BooleanType)


def test_medication_request_category_is_array():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "category")
    assert isinstance(f.dataType, ArrayType)


def test_medication_request_note_is_array():
    schema = get_schema("MedicationRequest", "base_r4")
    f = next(f for f in schema.fields if f.name == "note")
    assert isinstance(f.dataType, ArrayType)


# ── Extractor tests ───────────────────────────────────────────────────────────

def test_medication_request_extract_codeable_concept_medication():
    resource = {
        "resourceType": "MedicationRequest",
        "id": "mr1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/medication-request-id", "value": "MR001"}],
        "status": "active",
        "intent": "order",
        "category": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/medicationrequest-category",
                         "code": "outpatient", "display": "Outpatient"}]}
        ],
        "medicationCodeableConcept": {
            "coding": [{"system": "http://snomed.info/sct", "code": "372687004", "display": "Amoxicillin"}],
            "text": "Amoxicillin 500mg capsules",
        },
        "subject": {"reference": "Patient/p1", "display": "John Smith"},
        "encounter": {"reference": "Encounter/enc1"},
        "authoredOn": "2024-01-15",
        "requester": {"reference": "Practitioner/prac1", "display": "Dr. Smith"},
        "reasonCode": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "53840001", "display": "Bacterial infection"}]}
        ],
        "reasonReference": [{"reference": "Condition/cond1"}],
        "note": [{"text": "Take with food"}],
        "dosageInstruction": [
            {
                "text": "One capsule three times daily",
                "timing": {"repeat": {"frequency": 3, "period": 1, "periodUnit": "d"}},
                "doseAndRate": [{"doseQuantity": {"value": 500, "unit": "mg"}}],
            }
        ],
        "dispenseRequest": {
            "validityPeriod": {"start": "2024-01-15", "end": "2024-04-15"},
            "numberOfRepeatsAllowed": 2,
            "quantity": {"value": 21, "unit": "capsule"},
            "expectedSupplyDuration": {"value": 7, "unit": "d"},
        },
        "substitution": {"allowedBoolean": False},
    }
    result = extract(resource, "MedicationRequest", "base_r4")
    assert result["identifier"][0]["value"] == "MR001"
    assert result["status"] == "active"
    assert result["intent"] == "order"
    assert result["category"][0]["coding"][0]["code"] == "outpatient"
    # medication as CodeableConcept
    assert result["medication_codeable_concept"]["text"] == "Amoxicillin 500mg capsules"
    assert result["medication_codeable_concept"]["coding"][0]["code"] == "372687004"
    assert result["medication_reference"] is None
    assert result["subject"]["reference"] == "Patient/p1"
    assert result["encounter"]["reference"] == "Encounter/enc1"
    assert result["authored_on"] == "2024-01-15"
    assert result["requester"]["reference"] == "Practitioner/prac1"
    assert result["reason_code"][0]["coding"][0]["code"] == "53840001"
    assert result["reason_reference"][0]["reference"] == "Condition/cond1"
    assert result["note"][0]["text"] == "Take with food"
    # dosage_instruction
    assert len(result["dosage_instruction"]) == 1
    di = result["dosage_instruction"][0]
    assert di["text"] == "One capsule three times daily"
    assert di["dose_value"] == 500
    assert di["dose_unit"] == "mg"
    # dispense_request
    dr = result["dispense_request"]
    assert dr is not None
    assert dr["validity_period"]["start"] == "2024-01-15"
    assert dr["number_of_repeats_allowed"] == 2
    assert dr["quantity_value"] == 21
    assert dr["quantity_unit"] == "capsule"
    assert dr["expected_supply_duration_value"] == 7
    assert dr["expected_supply_duration_unit"] == "d"
    # substitution
    assert result["substitution_allowed_boolean"] is False


def test_medication_request_extract_reference_medication():
    resource = {
        "resourceType": "MedicationRequest", "id": "mr2",
        "status": "active",
        "intent": "plan",
        "medicationReference": {"reference": "Medication/med1", "display": "Amoxicillin"},
        "subject": {"reference": "Patient/p1"},
        "authoredOn": "2024-01-15",
    }
    result = extract(resource, "MedicationRequest", "base_r4")
    assert result["medication_codeable_concept"] is None
    assert result["medication_reference"]["reference"] == "Medication/med1"
    assert result["authored_on"] == "2024-01-15"


def test_medication_request_extract_dispense_request_absent():
    resource = {
        "resourceType": "MedicationRequest", "id": "mr3",
        "status": "completed",
        "intent": "order",
        "medicationCodeableConcept": {
            "coding": [{"system": "http://snomed.info/sct", "code": "372687004"}]
        },
        "subject": {"reference": "Patient/p1"},
    }
    result = extract(resource, "MedicationRequest", "base_r4")
    assert result["dispense_request"] is None
    assert result["substitution_allowed_boolean"] is None


def test_medication_request_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "MedicationRequest", "id": "mr-empty"}, "MedicationRequest", "base_r4")
    assert result["identifier"] == []
    assert result["status"] is None
    assert result["status_reason"] is None
    assert result["intent"] is None
    assert result["category"] == []
    assert result["priority"] is None
    assert result["medication_codeable_concept"] is None
    assert result["medication_reference"] is None
    assert result["subject"] is None
    assert result["encounter"] is None
    assert result["authored_on"] is None
    assert result["requester"] is None
    assert result["reason_code"] == []
    assert result["reason_reference"] == []
    assert result["note"] == []
    assert result["dosage_instruction"] == []
    assert result["dispense_request"] is None
    assert result["substitution_allowed_boolean"] is None
