import json
from pathlib import Path

from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    SmartAuthClient, FhirHttpClient, iter_bundle_pages, extract_record,
)

CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def make_client():
    config = load_config()
    auth = SmartAuthClient(
        token_url=config.get("token_url", ""),
        client_id=config.get("client_id", ""),
        auth_type=config.get("auth_type", "none"),
        private_key_pem=config.get("private_key_pem", ""),
        client_secret=config.get("client_secret", ""),
        scope=config.get("scope", ""),
    )
    return FhirHttpClient(base_url=config["base_url"], auth_client=auth)


# --- Pure unit tests (no network) ---

def test_auth_none_returns_empty_token():
    auth = SmartAuthClient("", "", "none", "", "", "")
    assert auth.get_token() == ""

def test_extract_record_common_fields():
    resource = {
        "resourceType": "Patient", "id": "p1",
        "meta": {"lastUpdated": "2024-01-15T10:30:00+00:00"},
    }
    record = extract_record(resource, "Patient")
    assert record["id"] == "p1"
    assert record["resourceType"] == "Patient"
    assert record["lastUpdated"] == "2024-01-15T10:30:00+00:00"
    assert json.loads(record["raw_json"]) == resource

def test_extract_record_missing_meta_returns_none_for_last_updated():
    record = extract_record({"resourceType": "Patient", "id": "p2"}, "Patient")
    assert record["lastUpdated"] is None

def test_extract_record_patient_typed_fields():
    resource = {
        "resourceType": "Patient", "id": "p3",
        "meta": {"lastUpdated": "2024-01-01T00:00:00+00:00"},
        "gender": "female", "birthDate": "1985-03-12", "active": True,
        "name": [{"text": "Jane Doe", "family": "Doe"}],
    }
    record = extract_record(resource, "Patient")
    assert record["gender"] == "female"
    assert record["birthDate"] == "1985-03-12"
    assert record["active"] is True
    assert record["name_text"] == "Jane Doe"
    assert record["name_family"] == "Doe"

def test_extract_record_observation_typed_fields():
    resource = {
        "resourceType": "Observation", "id": "obs1",
        "meta": {"lastUpdated": "2024-01-01T00:00:00+00:00"},
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "29463-7"}], "text": "Body weight"},
        "subject": {"reference": "Patient/p1"},
        "valueQuantity": {"value": 70.5, "unit": "kg"},
    }
    record = extract_record(resource, "Observation")
    assert record["status"] == "final"
    assert record["code_text"] == "Body weight"
    assert record["subject_reference"] == "Patient/p1"
    assert record["value_quantity_value"] == 70.5
    assert record["value_quantity_unit"] == "kg"

def test_extract_record_unknown_resource_returns_common_only():
    resource = {"resourceType": "UnknownXYZ", "id": "u1", "meta": {"lastUpdated": "2024-01-01T00:00:00Z"}}
    record = extract_record(resource, "UnknownXYZ")
    assert set(record.keys()) == {"id", "resourceType", "lastUpdated", "raw_json"}


# --- Integration tests (hit live HAPI FHIR server) ---

def test_fhir_client_get_patient_bundle():
    client = make_client()
    resp = client.get("Patient", params={"_count": "3"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["resourceType"] == "Bundle"
    assert body["type"] == "searchset"

def test_iter_bundle_pages_yields_patient_resources():
    client = make_client()
    resources = list(iter_bundle_pages(client, "Patient", {"_count": "5"}, max_records=5))
    assert len(resources) > 0
    for r in resources:
        assert r["resourceType"] == "Patient"
        assert "id" in r

def test_iter_bundle_pages_respects_max_records():
    client = make_client()
    resources = list(iter_bundle_pages(client, "Patient", {"_count": "10"}, max_records=3))
    assert len(resources) <= 3


def test_jwt_assertion_includes_kid_header():
    """kid header is required by SMART on FHIR Backend Services spec."""
    from unittest.mock import patch
    auth = SmartAuthClient(
        token_url="https://auth.example.com/token",
        client_id="my-client",
        auth_type="jwt_assertion",
        kid="key-2024-01",
    )
    with patch("databricks.labs.community_connector.sources.fhir.fhir_utils.jwt") as mock_jwt:
        mock_jwt.encode.return_value = "header.payload.sig"
        data = auth._jwt_assertion_data()

    call_kwargs = mock_jwt.encode.call_args
    headers_passed = call_kwargs.kwargs.get("headers") or {}
    assert headers_passed.get("kid") == "key-2024-01", \
        "kid header must be present in JWT assertion per SMART spec"
    assert data["client_assertion"] == "header.payload.sig"


def test_jwt_assertion_raises_if_kid_missing():
    """kid is required for jwt_assertion per SMART spec — must raise if omitted."""
    from unittest.mock import patch
    auth = SmartAuthClient(
        token_url="https://auth.example.com/token",
        client_id="my-client",
        auth_type="jwt_assertion",
        # kid intentionally omitted
    )
    try:
        auth._jwt_assertion_data()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "kid" in str(e).lower()
