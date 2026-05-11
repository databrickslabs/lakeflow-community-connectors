"""Tests for base_r4 Patient schema and extractor.
Validated against:
  FHIR R4: https://hl7.org/fhir/R4/patient.html
  UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Patient v2.6.1
"""
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructType, TimestampType
import databricks.labs.community_connector.sources.fhir.profiles.base_r4  # noqa: F401 — triggers @register decorators
from databricks.labs.community_connector.sources.fhir.profiles import get_schema, extract


def test_patient_schema_is_struct_type():
    assert isinstance(get_schema("Patient", "base_r4"), StructType)


def test_patient_has_common_fields():
    names = {f.name for f in get_schema("Patient", "base_r4").fields}
    assert {"id", "resourceType", "lastUpdated", "raw_json"} <= names


def test_patient_identifier_is_array():
    schema = get_schema("Patient", "base_r4")
    f = next(f for f in schema.fields if f.name == "identifier")
    assert isinstance(f.dataType, ArrayType)


def test_patient_name_is_array_of_struct():
    schema = get_schema("Patient", "base_r4")
    f = next(f for f in schema.fields if f.name == "name")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"family", "given", "text", "use"} <= inner_names


def test_patient_telecom_is_array():
    schema = get_schema("Patient", "base_r4")
    f = next(f for f in schema.fields if f.name == "telecom")
    assert isinstance(f.dataType, ArrayType)


def test_patient_address_is_array():
    schema = get_schema("Patient", "base_r4")
    f = next(f for f in schema.fields if f.name == "address")
    assert isinstance(f.dataType, ArrayType)
    inner_names = {sf.name for sf in f.dataType.elementType.fields}
    assert {"city", "postalCode", "country"} <= inner_names


def test_patient_active_is_boolean():
    schema = get_schema("Patient", "base_r4")
    f = next(f for f in schema.fields if f.name == "active")
    assert isinstance(f.dataType, BooleanType)


def test_patient_deceased_fields():
    names = {f.name for f in get_schema("Patient", "base_r4").fields}
    assert "deceased_boolean" in names
    assert "deceased_datetime" in names


def test_patient_extract_full():
    resource = {
        "resourceType": "Patient", "id": "p1",
        "meta": {"lastUpdated": "2024-01-15T10:00:00+00:00"},
        "identifier": [{"system": "https://fhir.nhs.uk/Id/nhs-number", "value": "9000000009", "use": "official"}],
        "active": True,
        "name": [{"family": "Smith", "given": ["John"], "text": "John Smith", "use": "official"}],
        "telecom": [{"system": "phone", "value": "020-1234-5678", "use": "home"}],
        "gender": "male",
        "birthDate": "1980-06-15",
        "address": [{"use": "home", "line": ["10 Victoria St"], "city": "London", "postalCode": "SW1A 1AA", "country": "GB"}],
        "managingOrganization": {"reference": "Organization/nhs-gp", "display": "NHS GP Practice"},
    }
    result = extract(resource, "Patient", "base_r4")
    assert result["identifier"][0]["value"] == "9000000009"
    assert result["identifier"][0]["system"] == "https://fhir.nhs.uk/Id/nhs-number"
    assert result["active"] is True
    assert result["name"][0]["family"] == "Smith"
    assert result["name"][0]["given"] == ["John"]
    assert result["telecom"][0]["system"] == "phone"
    assert result["gender"] == "male"
    assert result["birthDate"] == "1980-06-15"
    assert result["address"][0]["city"] == "London"
    assert result["address"][0]["postalCode"] == "SW1A 1AA"
    assert result["managingOrganization"]["reference"] == "Organization/nhs-gp"


def test_patient_extract_deceased_boolean():
    resource = {"resourceType": "Patient", "id": "p2", "deceasedBoolean": True}
    result = extract(resource, "Patient", "base_r4")
    assert result["deceased_boolean"] is True
    assert result["deceased_datetime"] is None


def test_patient_extract_deceased_datetime():
    resource = {"resourceType": "Patient", "id": "p3", "deceasedDateTime": "2023-05-10T00:00:00+00:00"}
    result = extract(resource, "Patient", "base_r4")
    assert result["deceased_boolean"] is None
    assert result["deceased_datetime"] == "2023-05-10T00:00:00+00:00"


def test_patient_extract_missing_fields_return_none_or_empty():
    result = extract({"resourceType": "Patient", "id": "p4"}, "Patient", "base_r4")
    assert result["identifier"] == []
    assert result["name"] == []
    assert result["gender"] is None
    assert result["managingOrganization"] is None
