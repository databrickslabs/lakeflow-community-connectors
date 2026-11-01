import json
from pathlib import Path

import pytest

from databricks.labs.community_connector import source_simulator as _sim_pkg
from databricks.labs.community_connector.source_simulator import (
    MODE_SIMULATE,
    Simulator,
)
from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    SmartAuthClient,
    FhirHttpClient,
    iter_bundle_pages,
    extract_record,
)

CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
_SIMULATOR_BASE_URL = "https://simulator-fhir.example.com/fhir"
_FHIR_SPEC_DIR = Path(_sim_pkg.__file__).parent / "specs" / "fhir"



def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def make_client(base_url: str | None = None) -> FhirHttpClient:
    """Build a FhirHttpClient. Default reads dev_config.json (live testing);
    pass ``base_url`` directly to bypass config (e.g. simulator runs)."""
    if base_url is not None:
        auth = SmartAuthClient("", "", "none", "", "", "")
        return FhirHttpClient(base_url=base_url, auth_client=auth)
    config = load_config()
    auth = SmartAuthClient(
        token_url=config.get("token_url", ""),
        client_id=config.get("client_id", ""),
        auth_type=config.get("auth_type", "none"),
        private_key_pem=config.get("private_key_pem", ""),
        client_secret=config.get("client_secret", ""),
        scope=config.get("scope", ""),
        kid=config.get("kid", ""),
        private_key_algorithm=config.get("private_key_algorithm", "RS384"),
    )
    return FhirHttpClient(base_url=config["base_url"], auth_client=auth)


@pytest.fixture
def simulated_fhir_client():
    """A FhirHttpClient pointing at the simulator. Spec/corpus live under
    ``source_simulator/specs/fhir/``."""
    with Simulator(
        mode=MODE_SIMULATE,
        spec_path=_FHIR_SPEC_DIR / "endpoints.yaml",
        corpus_dir=_FHIR_SPEC_DIR / "corpus",
    ):
        yield make_client(base_url=_SIMULATOR_BASE_URL)


# --- Pure unit tests (no network) ---


def test_auth_none_returns_empty_token():
    auth = SmartAuthClient("", "", "none", "", "", "")
    assert auth.get_token() == ""


def test_extract_record_common_fields():
    resource = {
        "resourceType": "Patient",
        "id": "p1",
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
        "resourceType": "Patient",
        "id": "p3",
        "meta": {"lastUpdated": "2024-01-01T00:00:00+00:00"},
        "gender": "female",
        "birthDate": "1985-03-12",
        "active": True,
        "name": [{"text": "Jane Doe", "family": "Doe", "given": ["Jane"]}],
    }
    record = extract_record(resource, "Patient")
    assert record["gender"] == "female"
    assert record["birthDate"] == "1985-03-12"
    assert record["active"] is True
    # name is now an array of HumanName structs
    assert record["name"][0]["family"] == "Doe"
    assert record["name"][0]["text"] == "Jane Doe"
    assert record["name"][0]["given"] == ["Jane"]


def test_extract_record_observation_typed_fields():
    resource = {
        "resourceType": "Observation",
        "id": "obs1",
        "meta": {"lastUpdated": "2024-01-01T00:00:00+00:00"},
        "status": "final",
        "code": {
            "coding": [{"system": "http://loinc.org", "code": "29463-7"}],
            "text": "Body weight",
        },
        "subject": {"reference": "Patient/p1"},
        "valueQuantity": {"value": 70.5, "unit": "kg"},
    }
    record = extract_record(resource, "Observation")
    assert record["status"] == "final"
    # code is now a CodeableConcept struct
    assert record["code"]["text"] == "Body weight"
    assert record["code"]["coding"][0]["code"] == "29463-7"
    # subject is now a Reference struct
    assert record["subject"]["reference"] == "Patient/p1"
    # value_quantity is now a Quantity struct
    assert record["value_quantity"]["value"] == 70.5
    assert record["value_quantity"]["unit"] == "kg"


def test_extract_record_unknown_resource_returns_common_only():
    resource = {
        "resourceType": "UnknownXYZ",
        "id": "u1",
        "meta": {"lastUpdated": "2024-01-01T00:00:00Z"},
    }
    record = extract_record(resource, "UnknownXYZ")
    assert set(record.keys()) == {"id", "resourceType", "lastUpdated", "raw_json", "extension"}


def test_extract_record_absent_extension_is_none_not_json_null():
    """Absent extension must be SQL NULL, not the JSON string "null".

    json.dumps(None) yields the string "null", which parse_json() turns into a
    VARIANT null *value* rather than a NULL column — silently unqueryable.
    """
    record = extract_record({"resourceType": "Patient", "id": "p1"}, "Patient")
    assert record["extension"] is None


def test_extract_record_present_extension_is_json_string():
    resource = {
        "resourceType": "Patient",
        "id": "p1",
        "extension": [{"url": "http://example.org/ext", "valueString": "x"}],
    }
    record = extract_record(resource, "Patient")
    assert json.loads(record["extension"])[0]["url"] == "http://example.org/ext"


# --- Integration tests (driven via the simulator) ---
#
# These exercise the real FhirHttpClient + iter_bundle_pages helper end-to-end:
# the request actually serializes through ``requests``, the simulator
# intercepts it, serves the Bundle from the corpus, and the client + helper
# parse it. No live FHIR server needed.

def test_fhir_client_get_patient_bundle(simulated_fhir_client):
    resp = simulated_fhir_client.get("Patient", params={"_count": "3"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["resourceType"] == "Bundle"
    assert body["type"] == "searchset"


def test_iter_bundle_pages_yields_patient_resources(simulated_fhir_client):
    resources = list(
        iter_bundle_pages(simulated_fhir_client, "Patient", {"_count": "5"})
    )
    assert len(resources) > 0
    for r in resources:
        assert r["resourceType"] == "Patient"
        assert "id" in r


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
    assert headers_passed.get("kid") == "key-2024-01", (
        "kid header must be present in JWT assertion per SMART spec"
    )
    assert data["client_assertion"] == "header.payload.sig"


def test_jwt_assertion_includes_nbf_claim():
    """Epic backend OAuth requires nbf claim (not in SMART v2 base spec but mandatory for Epic).
    nbf must equal iat and cannot be in the future.
    Ref: https://fhir.epic.com/Documentation?docId=oauth2 — JWT claims table.
    """
    from unittest.mock import patch

    auth = SmartAuthClient(
        token_url="https://auth.example.com/token",
        client_id="my-client",
        auth_type="jwt_assertion",
        kid="k1",
    )
    with patch("databricks.labs.community_connector.sources.fhir.fhir_utils.jwt") as mock_jwt:
        mock_jwt.encode.return_value = "h.p.s"
        auth._jwt_assertion_data()
    payload = mock_jwt.encode.call_args.args[0]
    assert "nbf" in payload, "Epic requires nbf claim in JWT assertion"
    assert payload["nbf"] == payload["iat"], "nbf must equal iat (cannot be in the future)"


def test_jwt_assertion_raises_if_kid_missing():
    """kid is required for jwt_assertion per SMART spec — must raise if omitted."""
    auth = SmartAuthClient(
        token_url="https://auth.example.com/token",
        client_id="my-client",
        auth_type="jwt_assertion",
        # kid intentionally omitted
    )
    with pytest.raises(ValueError, match="(?i)kid"):
        auth._jwt_assertion_data()


@pytest.mark.parametrize(
    "configured, expected",
    [(None, "RS384"), ("ES384", "ES384")],  # SMART: SHOULD default RS384, SHALL support ES384
)
def test_jwt_assertion_algorithm(configured, expected):
    from unittest.mock import patch

    kwargs = {"private_key_algorithm": configured} if configured else {}
    auth = SmartAuthClient(
        token_url="https://auth.example.com/token",
        client_id="my-client",
        auth_type="jwt_assertion",
        kid="k1",
        **kwargs,
    )
    with patch("databricks.labs.community_connector.sources.fhir.fhir_utils.jwt") as mock_jwt:
        mock_jwt.encode.return_value = "h.p.s"
        auth._jwt_assertion_data()
    assert mock_jwt.encode.call_args.kwargs["algorithm"] == expected


def test_jwt_assertion_rejects_unsupported_algorithm():
    """Unsupported algorithm must raise ValueError at construction time."""
    with pytest.raises(ValueError, match="RS256.*RS384"):
        SmartAuthClient(
            token_url="https://auth.example.com/token",
            client_id="my-client",
            auth_type="jwt_assertion",
            kid="k1",
            private_key_algorithm="RS256",
        )


def test_unknown_auth_type_rejected_at_construction():
    with pytest.raises(ValueError, match="Unsupported auth_type"):
        SmartAuthClient(token_url="", client_id="", auth_type="basic")


def test_client_secret_uses_http_basic_auth():
    """
    SMART client-confidential-symmetric spec requires HTTP Basic auth.
    client_id and client_secret must NOT appear in the POST body.
    """
    from unittest.mock import patch, MagicMock

    auth = SmartAuthClient(
        token_url="https://auth.example.com/token",
        client_id="my-app",
        auth_type="client_secret",
        client_secret="my-secret",
        scope="system/*.rs",
    )
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "tok", "expires_in": 300}

    with patch(
        "databricks.labs.community_connector.sources.fhir.fhir_utils.requests.post",
        return_value=mock_response,
    ) as mock_post:
        auth._refresh_token()

    call_kwargs = mock_post.call_args.kwargs
    assert "auth" in call_kwargs, "requests.post must use auth= kwarg for HTTP Basic"
    assert call_kwargs["auth"] == ("my-app", "my-secret"), (
        "auth tuple must be (client_id, client_secret)"
    )
    post_data = call_kwargs.get("data", {})
    assert "client_id" not in post_data, "client_id must not be in POST body"
    assert "client_secret" not in post_data, "client_secret must not be in POST body"


def test_jwt_assertion_does_not_use_http_basic_auth():
    """jwt_assertion authenticates via client_assertion body param, not HTTP Basic."""
    from unittest.mock import patch, MagicMock

    auth = SmartAuthClient(
        token_url="https://auth.example.com/token",
        client_id="my-client",
        auth_type="jwt_assertion",
        kid="k1",
        private_key_pem="",
        client_secret="should-not-appear",
    )
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "tok", "expires_in": 300}

    with (
        patch("databricks.labs.community_connector.sources.fhir.fhir_utils.jwt") as mock_jwt,
        patch(
            "databricks.labs.community_connector.sources.fhir.fhir_utils.requests.post",
            return_value=mock_response,
        ) as mock_post,
    ):
        mock_jwt.encode.return_value = "h.p.s"
        auth._refresh_token()

    call_kwargs = mock_post.call_args.kwargs
    assert "auth" not in call_kwargs, (
        "jwt_assertion must not use HTTP Basic auth — credentials are in client_assertion body"
    )


def test_discover_token_url_returns_token_endpoint():
    """discover_token_url must fetch .well-known/smart-configuration and return token_endpoint."""
    from unittest.mock import patch, MagicMock
    from databricks.labs.community_connector.sources.fhir.fhir_utils import discover_token_url
    from databricks.labs.community_connector.sources.fhir.fhir_constants import TOKEN_TIMEOUT

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "token_endpoint": "https://auth.example.com/oauth/token",
        "authorization_endpoint": "https://auth.example.com/oauth/authorize",
    }
    with patch(
        "databricks.labs.community_connector.sources.fhir.fhir_utils.requests.get",
        return_value=mock_resp,
    ) as mock_get:
        result = discover_token_url("https://fhir.example.com/R4")

    mock_get.assert_called_once_with(
        "https://fhir.example.com/R4/.well-known/smart-configuration",
        headers={"Accept": "application/json"},
        timeout=TOKEN_TIMEOUT,
    )
    assert result == "https://auth.example.com/oauth/token"


def test_discover_token_url_raises_on_http_error():
    from unittest.mock import patch, MagicMock
    from databricks.labs.community_connector.sources.fhir.fhir_utils import discover_token_url

    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.text = "Not Found"
    with patch(
        "databricks.labs.community_connector.sources.fhir.fhir_utils.requests.get",
        return_value=mock_resp,
    ):
        with pytest.raises(RuntimeError, match="404"):
            discover_token_url("https://fhir.example.com/R4")


def test_discover_token_url_raises_if_token_endpoint_missing():
    from unittest.mock import patch, MagicMock
    from databricks.labs.community_connector.sources.fhir.fhir_utils import discover_token_url

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"authorization_endpoint": "https://auth.example.com/authorize"}
    with patch(
        "databricks.labs.community_connector.sources.fhir.fhir_utils.requests.get",
        return_value=mock_resp,
    ):
        with pytest.raises(RuntimeError, match="token_endpoint"):
            discover_token_url("https://fhir.example.com/R4")


def test_fhir_connector_auto_discovers_token_url_when_omitted():
    """When token_url is absent and auth_type != none, connector must auto-discover it."""
    from unittest.mock import patch
    from databricks.labs.community_connector.sources.fhir.fhir import FhirLakeflowConnect

    with patch(
        "databricks.labs.community_connector.sources.fhir.fhir_utils.discover_token_url",
        return_value="https://auth.example.com/token",
    ) as mock_discover:
        connector = FhirLakeflowConnect(
            {
                "base_url": "https://fhir.example.com/R4",
                "auth_type": "client_secret",
                "client_id": "cid",
                "client_secret": "sec",
                # token_url intentionally omitted
            }
        )

    mock_discover.assert_called_once_with("https://fhir.example.com/R4")
    assert connector._client._auth_client._token_url == "https://auth.example.com/token"


def test_extract_record_explicit_null_extension_is_none():
    """An explicit JSON null must become SQL NULL, not the string "null"."""
    record = extract_record(
        {"resourceType": "Patient", "id": "p1", "extension": None}, "Patient"
    )
    assert record["extension"] is None
