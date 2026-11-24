"""Tests for uk_core Patient schema and extractor.
Validated against UK Core Patient v2.6.1:
  https://fhir.hl7.org.uk/StructureDefinition/UKCore-Patient
Extension URLs verified from:
  https://github.com/NHSDigital/FHIR-R4-UKCORE-STAGING-MAIN (UKCore-Patient.xml)
  https://hl7.org/fhir/R4/extension-patient-interpreterrequired.html

Key finding: PatientInterpreterRequired is NOT a UK Core extension.
The UK Core Patient profile references the standard HL7 extension:
  http://hl7.org/fhir/StructureDefinition/patient-interpreterRequired (valueBoolean)
"""
import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401
import databricks.labs.community_connector.sources.fhir.profiles.uk_core  # noqa: F401
from pyspark.sql.types import BooleanType, StructType
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


def test_uk_core_patient_schema_includes_base_fields():
    schema = get_schema("Patient", "uk_core")
    names = {f.name for f in schema.fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json",
            "identifier", "active", "name", "telecom", "gender", "birthDate",
            "address", "managingOrganization"} <= names


def test_uk_core_patient_has_nhs_number():
    names = {f.name for f in get_schema("Patient", "uk_core").fields}
    assert "nhs_number" in names


def test_uk_core_patient_has_ethnic_category():
    names = {f.name for f in get_schema("Patient", "uk_core").fields}
    assert "ethnic_category" in names


def test_uk_core_patient_has_birth_sex():
    names = {f.name for f in get_schema("Patient", "uk_core").fields}
    assert "birth_sex" in names


def test_uk_core_patient_interpreter_required_is_boolean():
    schema = get_schema("Patient", "uk_core")
    f = next(f for f in schema.fields if f.name == "interpreter_required")
    assert isinstance(f.dataType, BooleanType)


def test_uk_core_patient_extract_nhs_number():
    resource = {
        "resourceType": "Patient", "id": "p1",
        "identifier": [
            {"system": "https://fhir.nhs.uk/Id/nhs-number", "value": "9000000009", "use": "official"},
            {"system": "https://fhir.example.com/local-id", "value": "LOCAL-123"},
        ],
        "active": True, "gender": "female", "name": [{"family": "Smith", "given": ["Jane"]}],
    }
    r = extract(resource, "Patient", "uk_core")
    assert r["nhs_number"] == "9000000009"
    assert r["gender"] == "female"
    assert r["name"][0]["family"] == "Smith"
    assert len(r["identifier"]) == 2


def test_uk_core_patient_extract_ethnic_category_extension():
    # Use the actual verified extension URL
    from databricks.labs.community_connector.sources.fhir.profiles.uk_core import _EXT_ETHNIC_CATEGORY
    resource = {
        "resourceType": "Patient", "id": "p2",
        "name": [{"family": "Jones"}],
        "extension": [
            {
                "url": _EXT_ETHNIC_CATEGORY,
                "valueCodeableConcept": {
                    "coding": [{"system": "https://fhir.hl7.org.uk/CodeSystem/UKCore-EthnicCategory", "code": "A", "display": "British, Mixed British"}],
                    "text": "British, Mixed British",
                },
            }
        ],
    }
    r = extract(resource, "Patient", "uk_core")
    assert r["ethnic_category"]["text"] == "British, Mixed British"
    assert r["ethnic_category"]["coding"][0]["code"] == "A"


def test_uk_core_patient_extract_birth_sex_extension():
    from databricks.labs.community_connector.sources.fhir.profiles.uk_core import _EXT_BIRTH_SEX
    resource = {
        "resourceType": "Patient", "id": "p3",
        "extension": [
            {
                "url": _EXT_BIRTH_SEX,
                "valueCodeableConcept": {
                    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-AdministrativeGender", "code": "F"}],
                },
            }
        ],
    }
    r = extract(resource, "Patient", "uk_core")
    assert r["birth_sex"]["coding"][0]["code"] == "F"


def test_uk_core_patient_extract_interpreter_required():
    from databricks.labs.community_connector.sources.fhir.profiles.uk_core import _EXT_INTERPRETER_REQUIRED
    resource = {
        "resourceType": "Patient", "id": "p4",
        "extension": [
            {
                "url": _EXT_INTERPRETER_REQUIRED,
                "valueBoolean": True,
            }
        ],
    }
    r = extract(resource, "Patient", "uk_core")
    assert r["interpreter_required"] is True


def test_uk_core_patient_no_uk_extensions_returns_none():
    resource = {"resourceType": "Patient", "id": "p5", "gender": "male"}
    r = extract(resource, "Patient", "uk_core")
    assert r["nhs_number"] is None
    assert r["ethnic_category"] is None
    assert r["birth_sex"] is None
    assert r["interpreter_required"] is None


def test_non_patient_resources_fall_back_to_base_r4():
    # Observation has no uk_core entry — must fall back to base_r4
    base_schema = get_schema("Observation", "base_r4")
    uk_schema = get_schema("Observation", "uk_core")
    assert base_schema == uk_schema
