"""Tests for base_r4 CarePlan, Goal, Device, and DocumentReference schemas and extractors.

Validated against:
  CarePlan:
    FHIR R4: https://hl7.org/fhir/R4/careplan.html
    UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-CarePlan v2.2.0
  Goal:
    FHIR R4: https://hl7.org/fhir/R4/goal.html
    No UK Core profile — base R4 is authoritative.
  Device:
    FHIR R4: https://hl7.org/fhir/R4/device.html
    UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Device v1.2.0
  DocumentReference:
    FHIR R4: https://hl7.org/fhir/R4/documentreference.html
    UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-DocumentReference v2.2.0

Field name verification summary:
  CarePlan:
    careTeam       → care_team
    addresses      → addresses
    goal           → goal
    activity.detail.status      → detail_status
    activity.detail.code        → detail_code
    activity.detail.description → detail_description
    activity.reference          → reference
  Goal:
    lifecycleStatus     → lifecycle_status
    achievementStatus   → achievement_status
    startDate           → start_date
    startCodeableConcept→ start_codeable_concept
    target.measure      → measure
    target.detailQuantity → detail_quantity
    target.detailString   → detail_string
    target.detailBoolean  → detail_boolean
    target.dueDate        → due_date
    statusDate          → status_date
    statusReason        → status_reason
    expressedBy         → expressed_by
  Device:
    deviceName.name     → device_name[].name
    deviceName.type     → device_name[].type
    statusReason        → status_reason (array of CodeableConcept)
    distinctIdentifier  → (not captured — not in MS fields)
    manufactureDate     → manufacture_date
    expirationDate      → expiration_date
    lotNumber           → lot_number
    serialNumber        → serial_number
    modelNumber         → model_number
  DocumentReference:
    docStatus           → doc_status
    relatesTo.code      → code
    relatesTo.target    → target
    content.attachment.url          → attachment_url
    content.attachment.contentType  → attachment_content_type
    content.attachment.title        → attachment_title
    content.format.code             → format_code
    content.format.system           → format_system
    context.facilityType            → facility_type
    context.practiceSetting         → practice_setting
    context.period                  → period
    context.encounter               → encounter
"""
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructType, TimestampType

import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


# ════════════════════════════════════════════════════════════════════════════════
# CarePlan
# ════════════════════════════════════════════════════════════════════════════════

def test_careplan_schema_is_struct_type():
    assert isinstance(get_schema("CarePlan", "base_r4"), StructType)


def test_careplan_has_common_fields():
    names = {f.name for f in get_schema("CarePlan", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_careplan_status_is_string():
    schema = get_schema("CarePlan", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_careplan_intent_is_string():
    schema = get_schema("CarePlan", "base_r4")
    f = next(f for f in schema.fields if f.name == "intent")
    assert isinstance(f.dataType, StringType)


def test_careplan_care_team_is_array():
    schema = get_schema("CarePlan", "base_r4")
    f = next(f for f in schema.fields if f.name == "care_team")
    assert isinstance(f.dataType, ArrayType)


def test_careplan_addresses_is_array():
    schema = get_schema("CarePlan", "base_r4")
    f = next(f for f in schema.fields if f.name == "addresses")
    assert isinstance(f.dataType, ArrayType)


def test_careplan_goal_is_array():
    schema = get_schema("CarePlan", "base_r4")
    f = next(f for f in schema.fields if f.name == "goal")
    assert isinstance(f.dataType, ArrayType)


def test_careplan_activity_has_expected_fields():
    schema = get_schema("CarePlan", "base_r4")
    f = next(f for f in schema.fields if f.name == "activity")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"reference", "detail_status", "detail_code", "detail_description"} <= inner_names


def test_careplan_extract_full():
    resource = {
        "resourceType": "CarePlan",
        "id": "cp1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/care-plan-id", "value": "CP001"}],
        "status": "active",
        "intent": "plan",
        "category": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "734163000",
                          "display": "Care plan"}]}
        ],
        "title": "Diabetes Management Plan",
        "description": "Ongoing management of Type 2 diabetes",
        "subject": {"reference": "Patient/p1", "display": "Jane Smith"},
        "encounter": {"reference": "Encounter/enc1"},
        "period": {"start": "2024-01-01", "end": "2024-12-31"},
        "created": "2024-01-01",
        "author": {"reference": "Practitioner/prac1"},
        "careTeam": [{"reference": "CareTeam/ct1"}],
        "addresses": [{"reference": "Condition/cond1"}],
        "goal": [{"reference": "Goal/goal1"}],
        "activity": [
            {
                "reference": {"reference": "ServiceRequest/sr1"},
            },
            {
                "detail": {
                    "status": "in-progress",
                    "code": {"coding": [{"system": "http://snomed.info/sct", "code": "229070002",
                                          "display": "Exercise therapy"}]},
                    "description": "30 minutes walking daily",
                }
            },
        ],
        "note": [{"text": "Patient is compliant"}],
    }
    result = extract(resource, "CarePlan", "base_r4")
    assert result["identifier"][0]["value"] == "CP001"
    assert result["status"] == "active"
    assert result["intent"] == "plan"
    assert result["category"][0]["coding"][0]["code"] == "734163000"
    assert result["title"] == "Diabetes Management Plan"
    assert result["description"] == "Ongoing management of Type 2 diabetes"
    assert result["subject"]["reference"] == "Patient/p1"
    assert result["encounter"]["reference"] == "Encounter/enc1"
    assert result["period"]["start"] == "2024-01-01"
    assert result["created"] == "2024-01-01"
    assert result["author"]["reference"] == "Practitioner/prac1"
    assert result["care_team"][0]["reference"] == "CareTeam/ct1"
    assert result["addresses"][0]["reference"] == "Condition/cond1"
    assert result["goal"][0]["reference"] == "Goal/goal1"
    assert len(result["activity"]) == 2
    act0 = result["activity"][0]
    assert act0["reference"]["reference"] == "ServiceRequest/sr1"
    assert act0["detail_status"] is None
    act1 = result["activity"][1]
    assert act1["reference"] is None
    assert act1["detail_status"] == "in-progress"
    assert act1["detail_code"]["coding"][0]["code"] == "229070002"
    assert act1["detail_description"] == "30 minutes walking daily"
    assert result["note"][0]["text"] == "Patient is compliant"


def test_careplan_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "CarePlan", "id": "cp-empty"}, "CarePlan", "base_r4")
    assert result["identifier"] == []
    assert result["status"] is None
    assert result["intent"] is None
    assert result["category"] == []
    assert result["title"] is None
    assert result["description"] is None
    assert result["subject"] is None
    assert result["care_team"] == []
    assert result["addresses"] == []
    assert result["goal"] == []
    assert result["activity"] == []
    assert result["note"] == []


# ════════════════════════════════════════════════════════════════════════════════
# Goal
# ════════════════════════════════════════════════════════════════════════════════

def test_goal_schema_is_struct_type():
    assert isinstance(get_schema("Goal", "base_r4"), StructType)


def test_goal_has_common_fields():
    names = {f.name for f in get_schema("Goal", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_goal_lifecycle_status_is_string():
    schema = get_schema("Goal", "base_r4")
    f = next(f for f in schema.fields if f.name == "lifecycle_status")
    assert isinstance(f.dataType, StringType)


def test_goal_start_date_is_string():
    schema = get_schema("Goal", "base_r4")
    f = next(f for f in schema.fields if f.name == "start_date")
    assert isinstance(f.dataType, StringType)


def test_goal_start_codeable_concept_exists():
    names = {f.name for f in get_schema("Goal", "base_r4").fields}
    assert "start_codeable_concept" in names


def test_goal_target_has_expected_fields():
    schema = get_schema("Goal", "base_r4")
    f = next(f for f in schema.fields if f.name == "target")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"measure", "detail_quantity", "detail_string", "detail_boolean", "due_date"} <= inner_names


def test_goal_target_detail_boolean_is_boolean():
    schema = get_schema("Goal", "base_r4")
    target_f = next(f for f in schema.fields if f.name == "target")
    detail_bool = next(sf for sf in target_f.dataType.elementType.fields if sf.name == "detail_boolean")
    assert isinstance(detail_bool.dataType, BooleanType)


def test_goal_status_date_is_string():
    schema = get_schema("Goal", "base_r4")
    f = next(f for f in schema.fields if f.name == "status_date")
    assert isinstance(f.dataType, StringType)


def test_goal_expressed_by_exists():
    names = {f.name for f in get_schema("Goal", "base_r4").fields}
    assert "expressed_by" in names


def test_goal_extract_full():
    resource = {
        "resourceType": "Goal",
        "id": "goal1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/goal-id", "value": "GOAL001"}],
        "lifecycleStatus": "active",
        "achievementStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/goal-achievement",
                         "code": "in-progress", "display": "In Progress"}]
        },
        "category": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/goal-category",
                          "code": "dietary", "display": "Dietary"}]}
        ],
        "priority": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/goal-priority",
                         "code": "medium-priority", "display": "Medium Priority"}]
        },
        "description": {
            "coding": [{"system": "http://snomed.info/sct", "code": "281291005",
                          "display": "Reduction in HbA1c level"}],
            "text": "Reduce HbA1c to below 53 mmol/mol",
        },
        "subject": {"reference": "Patient/p1"},
        "startDate": "2024-01-01",
        "target": [
            {
                "measure": {
                    "coding": [{"system": "http://loinc.org", "code": "4548-4", "display": "HbA1c"}]
                },
                "detailQuantity": {"value": 53, "unit": "mmol/mol",
                                    "system": "http://unitsofmeasure.org", "code": "mmol/mol"},
                "dueDate": "2024-06-01",
            }
        ],
        "statusDate": "2024-01-15",
        "statusReason": "Patient starting new medication regimen",
        "expressedBy": {"reference": "Practitioner/prac1"},
        "addresses": [{"reference": "Condition/cond1"}],
        "note": [{"text": "Monitor monthly"}],
    }
    result = extract(resource, "Goal", "base_r4")
    assert result["identifier"][0]["value"] == "GOAL001"
    assert result["lifecycle_status"] == "active"
    assert result["achievement_status"]["coding"][0]["code"] == "in-progress"
    assert result["category"][0]["coding"][0]["code"] == "dietary"
    assert result["priority"]["coding"][0]["code"] == "medium-priority"
    assert result["description"]["text"] == "Reduce HbA1c to below 53 mmol/mol"
    assert result["subject"]["reference"] == "Patient/p1"
    assert result["start_date"] == "2024-01-01"
    assert result["start_codeable_concept"] is None
    assert len(result["target"]) == 1
    tgt = result["target"][0]
    assert tgt["measure"]["coding"][0]["code"] == "4548-4"
    assert tgt["detail_quantity"]["value"] == 53
    assert tgt["detail_quantity"]["unit"] == "mmol/mol"
    assert tgt["due_date"] == "2024-06-01"
    assert tgt["detail_string"] is None
    assert tgt["detail_boolean"] is None
    assert result["status_date"] == "2024-01-15"
    assert result["status_reason"] == "Patient starting new medication regimen"
    assert result["expressed_by"]["reference"] == "Practitioner/prac1"
    assert result["addresses"][0]["reference"] == "Condition/cond1"
    assert result["note"][0]["text"] == "Monitor monthly"


def test_goal_extract_start_codeable_concept():
    resource = {
        "resourceType": "Goal", "id": "goal2",
        "lifecycleStatus": "active",
        "description": {"coding": [{"system": "http://snomed.info/sct", "code": "281291005"}]},
        "subject": {"reference": "Patient/p1"},
        "startCodeableConcept": {
            "coding": [{"system": "http://snomed.info/sct", "code": "308283009",
                          "display": "Discharge from hospital"}]
        },
    }
    result = extract(resource, "Goal", "base_r4")
    assert result["start_date"] is None
    assert result["start_codeable_concept"]["coding"][0]["code"] == "308283009"


def test_goal_extract_target_detail_variants():
    resource = {
        "resourceType": "Goal", "id": "goal3",
        "lifecycleStatus": "active",
        "description": {"text": "Quit smoking"},
        "subject": {"reference": "Patient/p1"},
        "target": [
            {"detailString": "Complete cessation"},
        ],
        "startDate": "2024-01-01",
    }
    result = extract(resource, "Goal", "base_r4")
    tgt = result["target"][0]
    assert tgt["detail_string"] == "Complete cessation"
    assert tgt["detail_quantity"] is None
    assert tgt["detail_boolean"] is None


def test_goal_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Goal", "id": "goal-empty"}, "Goal", "base_r4")
    assert result["identifier"] == []
    assert result["lifecycle_status"] is None
    assert result["achievement_status"] is None
    assert result["category"] == []
    assert result["description"] is None
    assert result["subject"] is None
    assert result["start_date"] is None
    assert result["start_codeable_concept"] is None
    assert result["target"] == []
    assert result["status_date"] is None
    assert result["status_reason"] is None
    assert result["expressed_by"] is None
    assert result["addresses"] == []
    assert result["note"] == []


# ════════════════════════════════════════════════════════════════════════════════
# Device
# ════════════════════════════════════════════════════════════════════════════════

def test_device_schema_is_struct_type():
    assert isinstance(get_schema("Device", "base_r4"), StructType)


def test_device_has_common_fields():
    names = {f.name for f in get_schema("Device", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_device_status_is_string():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_device_status_reason_is_array_of_codeable_concept():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "status_reason")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"coding", "text"} <= inner_names


def test_device_device_name_is_array_with_name_and_type():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "device_name")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"name", "type"} <= inner_names


def test_device_manufacturer_is_string():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "manufacturer")
    assert isinstance(f.dataType, StringType)


def test_device_lot_number_is_string():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "lot_number")
    assert isinstance(f.dataType, StringType)


def test_device_serial_number_is_string():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "serial_number")
    assert isinstance(f.dataType, StringType)


def test_device_model_number_is_string():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "model_number")
    assert isinstance(f.dataType, StringType)


def test_device_manufacture_date_is_string():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "manufacture_date")
    assert isinstance(f.dataType, StringType)


def test_device_expiration_date_is_string():
    schema = get_schema("Device", "base_r4")
    f = next(f for f in schema.fields if f.name == "expiration_date")
    assert isinstance(f.dataType, StringType)


def test_device_extract_full():
    resource = {
        "resourceType": "Device",
        "id": "dev1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/device-id", "value": "DEV001"}],
        "status": "active",
        "statusReason": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/device-status-reason",
                          "code": "online", "display": "Online"}]}
        ],
        "manufacturer": "Acme Medical Devices",
        "manufactureDate": "2020-01-15",
        "expirationDate": "2025-01-15",
        "lotNumber": "LOT-2020-001",
        "serialNumber": "SN-ABC123",
        "deviceName": [
            {"name": "Pulse Oximeter Model X", "type": "user-friendly-name"},
            {"name": "PulseOx-X", "type": "manufacturer-name"},
        ],
        "modelNumber": "MODEL-X-PRO",
        "type": {
            "coding": [{"system": "http://snomed.info/sct", "code": "706767009",
                          "display": "Application software"}]
        },
        "patient": {"reference": "Patient/p1"},
        "owner": {"reference": "Organization/org1"},
        "contact": [
            {"system": "phone", "value": "+441234567890", "use": "work"}
        ],
        "note": [{"text": "Calibrated 2024-01-01"}],
        "safety": [
            {"coding": [{"system": "urn:oid:2.16.840.1.113883.3.26.1.1",
                          "code": "C106046", "display": "Labeling does not Contain MRI Safety Information"}]}
        ],
    }
    result = extract(resource, "Device", "base_r4")
    assert result["identifier"][0]["value"] == "DEV001"
    assert result["status"] == "active"
    assert result["status_reason"][0]["coding"][0]["code"] == "online"
    assert result["manufacturer"] == "Acme Medical Devices"
    assert result["manufacture_date"] == "2020-01-15"
    assert result["expiration_date"] == "2025-01-15"
    assert result["lot_number"] == "LOT-2020-001"
    assert result["serial_number"] == "SN-ABC123"
    assert len(result["device_name"]) == 2
    assert result["device_name"][0]["name"] == "Pulse Oximeter Model X"
    assert result["device_name"][0]["type"] == "user-friendly-name"
    assert result["device_name"][1]["name"] == "PulseOx-X"
    assert result["model_number"] == "MODEL-X-PRO"
    assert result["type"]["coding"][0]["code"] == "706767009"
    assert result["patient"]["reference"] == "Patient/p1"
    assert result["owner"]["reference"] == "Organization/org1"
    assert result["contact"][0]["system"] == "phone"
    assert result["note"][0]["text"] == "Calibrated 2024-01-01"
    assert result["safety"][0]["coding"][0]["code"] == "C106046"


def test_device_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Device", "id": "dev-empty"}, "Device", "base_r4")
    assert result["identifier"] == []
    assert result["status"] is None
    assert result["status_reason"] == []
    assert result["manufacturer"] is None
    assert result["manufacture_date"] is None
    assert result["expiration_date"] is None
    assert result["lot_number"] is None
    assert result["serial_number"] is None
    assert result["device_name"] == []
    assert result["model_number"] is None
    assert result["type"] is None
    assert result["patient"] is None
    assert result["owner"] is None
    assert result["contact"] == []
    assert result["note"] == []
    assert result["safety"] == []


# ════════════════════════════════════════════════════════════════════════════════
# DocumentReference
# ════════════════════════════════════════════════════════════════════════════════

def test_document_reference_schema_is_struct_type():
    assert isinstance(get_schema("DocumentReference", "base_r4"), StructType)


def test_document_reference_has_common_fields():
    names = {f.name for f in get_schema("DocumentReference", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_document_reference_status_is_string():
    schema = get_schema("DocumentReference", "base_r4")
    f = next(f for f in schema.fields if f.name == "status")
    assert isinstance(f.dataType, StringType)


def test_document_reference_doc_status_is_string():
    schema = get_schema("DocumentReference", "base_r4")
    f = next(f for f in schema.fields if f.name == "doc_status")
    assert isinstance(f.dataType, StringType)


def test_document_reference_date_is_timestamp():
    schema = get_schema("DocumentReference", "base_r4")
    f = next(f for f in schema.fields if f.name == "date")
    assert isinstance(f.dataType, TimestampType)


def test_document_reference_relates_to_has_code_and_target():
    schema = get_schema("DocumentReference", "base_r4")
    f = next(f for f in schema.fields if f.name == "relates_to")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"code", "target"} <= inner_names


def test_document_reference_content_has_expected_fields():
    schema = get_schema("DocumentReference", "base_r4")
    f = next(f for f in schema.fields if f.name == "content")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"attachment_url", "attachment_title", "attachment_content_type",
            "format_code", "format_system"} <= inner_names


def test_document_reference_context_has_expected_fields():
    schema = get_schema("DocumentReference", "base_r4")
    f = next(f for f in schema.fields if f.name == "context")
    assert isinstance(f.dataType, StructType)
    inner_names = {sf.name for sf in f.dataType.fields}
    assert {"encounter", "period", "facility_type", "practice_setting"} <= inner_names


def test_document_reference_extract_full():
    resource = {
        "resourceType": "DocumentReference",
        "id": "docref1",
        "identifier": [{"system": "https://fhir.example.nhs.uk/Id/document-id", "value": "DOC001"}],
        "status": "current",
        "docStatus": "final",
        "type": {
            "coding": [{"system": "http://snomed.info/sct", "code": "734163000",
                          "display": "Care plan (record artifact)"}]
        },
        "category": [
            {"coding": [{"system": "http://hl7.org/fhir/us/core/CodeSystem/us-core-documentreference-category",
                          "code": "clinical-note", "display": "Clinical Note"}]}
        ],
        "subject": {"reference": "Patient/p1"},
        "date": "2024-01-15T10:30:00+00:00",
        "author": [{"reference": "Practitioner/prac1"}],
        "authenticator": {"reference": "Practitioner/prac2"},
        "custodian": {"reference": "Organization/org1"},
        "relatesTo": [
            {
                "code": "replaces",
                "target": {"reference": "DocumentReference/old-docref1"},
            }
        ],
        "description": "Outpatient clinic letter",
        "securityLabel": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-Confidentiality",
                          "code": "N", "display": "Normal"}]}
        ],
        "content": [
            {
                "attachment": {
                    "url": "https://fhir.example.nhs.uk/Binary/binary1",
                    "contentType": "application/pdf",
                    "title": "Clinic Letter 2024-01-15",
                },
                "format": {
                    "system": "urn:oid:1.3.6.1.4.1.19376.1.2.3",
                    "code": "urn:ihe:pcc:handp:2008",
                },
            }
        ],
        "context": {
            "encounter": [{"reference": "Encounter/enc1"}],
            "event": [
                {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                              "code": "GENRL", "display": "General"}]}
            ],
            "period": {"start": "2024-01-15T09:00:00+00:00", "end": "2024-01-15T11:00:00+00:00"},
            "facilityType": {
                "coding": [{"system": "http://snomed.info/sct", "code": "225728007",
                              "display": "Accident and Emergency department"}]
            },
            "practiceSetting": {
                "coding": [{"system": "http://snomed.info/sct", "code": "394576009",
                              "display": "Accident & Emergency"}]
            },
        },
    }
    result = extract(resource, "DocumentReference", "base_r4")
    assert result["identifier"][0]["value"] == "DOC001"
    assert result["status"] == "current"
    assert result["doc_status"] == "final"
    assert result["type"]["coding"][0]["code"] == "734163000"
    assert result["category"][0]["coding"][0]["code"] == "clinical-note"
    assert result["subject"]["reference"] == "Patient/p1"
    assert result["date"] == "2024-01-15T10:30:00+00:00"
    assert result["author"][0]["reference"] == "Practitioner/prac1"
    assert result["authenticator"]["reference"] == "Practitioner/prac2"
    assert result["custodian"]["reference"] == "Organization/org1"
    assert len(result["relates_to"]) == 1
    assert result["relates_to"][0]["code"] == "replaces"
    assert result["relates_to"][0]["target"]["reference"] == "DocumentReference/old-docref1"
    assert result["description"] == "Outpatient clinic letter"
    assert result["security_label"][0]["coding"][0]["code"] == "N"
    assert len(result["content"]) == 1
    c = result["content"][0]
    assert c["attachment_url"] == "https://fhir.example.nhs.uk/Binary/binary1"
    assert c["attachment_content_type"] == "application/pdf"
    assert c["attachment_title"] == "Clinic Letter 2024-01-15"
    assert c["format_system"] == "urn:oid:1.3.6.1.4.1.19376.1.2.3"
    assert c["format_code"] == "urn:ihe:pcc:handp:2008"
    ctx = result["context"]
    assert ctx is not None
    assert ctx["encounter"][0]["reference"] == "Encounter/enc1"
    assert ctx["event"][0]["coding"][0]["code"] == "GENRL"
    assert ctx["period"]["start"] == "2024-01-15T09:00:00+00:00"
    assert ctx["facility_type"]["coding"][0]["code"] == "225728007"
    assert ctx["practice_setting"]["coding"][0]["code"] == "394576009"


def test_document_reference_extract_no_context_returns_none():
    resource = {
        "resourceType": "DocumentReference", "id": "docref2",
        "status": "current",
        "content": [{"attachment": {"url": "https://fhir.example.nhs.uk/Binary/b2",
                                     "contentType": "text/plain"}}],
    }
    result = extract(resource, "DocumentReference", "base_r4")
    assert result["context"] is None


def test_document_reference_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "DocumentReference", "id": "docref-empty"}, "DocumentReference", "base_r4")
    assert result["identifier"] == []
    assert result["status"] is None
    assert result["doc_status"] is None
    assert result["type"] is None
    assert result["category"] == []
    assert result["subject"] is None
    assert result["date"] is None
    assert result["author"] == []
    assert result["authenticator"] is None
    assert result["custodian"] is None
    assert result["relates_to"] == []
    assert result["description"] is None
    assert result["security_label"] == []
    assert result["content"] == []
    assert result["context"] is None
