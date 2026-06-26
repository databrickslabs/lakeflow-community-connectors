import pytest
import requests

from databricks.labs.community_connector.sources.azure_devops.azure_devops import (
    AzureDevopsLakeflowConnect,
)
from databricks.labs.community_connector.sources.azure_devops.azure_devops_utils import (
    EntraClientSecretAuth,
)
from tests.unit.sources.test_suite import LakeflowConnectTests

_POST_PATH = (
    "databricks.labs.community_connector.sources.azure_devops."
    "azure_devops_utils.requests.post"
)


class TestAzureDevopsConnector(LakeflowConnectTests):
    connector_class = AzureDevopsLakeflowConnect
    simulator_source = "azure_devops"
    replay_config = {
        "organization": "simulator-org",
        "project": "simulator-project",
        "personal_access_token": "simulator-fake-pat",
    }


class TestAzureDevopsAuthModes:
    """auth_mode branching in __init__ (offline — no simulator/network)."""

    _BASE = {"organization": "myorg"}

    def test_default_is_pat_basic_auth(self):
        c = AzureDevopsLakeflowConnect(
            {**self._BASE, "personal_access_token": "pat123"}
        )
        assert c._session.headers["Authorization"].startswith("Basic ")
        assert c._session.auth is None

    def test_pat_mode_requires_token(self):
        with pytest.raises(ValueError, match="personal_access_token"):
            AzureDevopsLakeflowConnect({**self._BASE, "auth_mode": "pat"})

    def test_oauth_mode_sets_entra_auth(self):
        c = AzureDevopsLakeflowConnect(
            {
                **self._BASE,
                "auth_mode": "oauth",
                "tenant_id": "t",
                "client_id": "c",
                "client_secret": "s",
            }
        )
        assert isinstance(c._session.auth, EntraClientSecretAuth)
        assert "Authorization" not in c._session.headers

    def test_oauth_mode_requires_sp_creds(self):
        with pytest.raises(ValueError, match="tenant_id"):
            AzureDevopsLakeflowConnect(
                {**self._BASE, "auth_mode": "oauth", "client_id": "c"}
            )

    def test_unknown_auth_mode_raises(self):
        with pytest.raises(ValueError, match="unknown auth_mode"):
            AzureDevopsLakeflowConnect(
                {
                    **self._BASE,
                    "auth_mode": "bogus",
                    "personal_access_token": "x",
                }
            )


class _FakeTokenResponse:
    def __init__(self, status_code=200, token="tok", expires_in=3600):
        self.status_code = status_code
        self.text = "error-body"
        self._token = token
        self._expires_in = expires_in

    def json(self):
        return {"access_token": self._token, "expires_in": self._expires_in}


class TestEntraClientSecretAuth:
    """Client-credentials token acquisition + refresh, POST mocked."""

    @staticmethod
    def _prepared():
        return requests.Request("GET", "https://dev.azure.com/x").prepare()

    def test_fetches_and_sets_bearer(self, monkeypatch):
        calls = []

        def fake_post(url, data, timeout):
            calls.append((url, data))
            return _FakeTokenResponse(token="tok-1")

        monkeypatch.setattr(_POST_PATH, fake_post)
        req = EntraClientSecretAuth("tenant", "client", "secret")(self._prepared())
        assert req.headers["Authorization"] == "Bearer tok-1"
        assert "login.microsoftonline.com/tenant/oauth2/v2.0/token" in calls[0][0]
        assert calls[0][1]["grant_type"] == "client_credentials"
        assert calls[0][1]["scope"].endswith("/.default")

    def test_reuses_token_until_expiry(self, monkeypatch):
        n = {"count": 0}

        def fake_post(url, data, timeout):
            n["count"] += 1
            return _FakeTokenResponse()

        monkeypatch.setattr(_POST_PATH, fake_post)
        auth = EntraClientSecretAuth("tenant", "client", "secret")
        auth(self._prepared())
        auth(self._prepared())
        assert n["count"] == 1  # second request reuses the cached token

    def test_token_request_failure_raises(self, monkeypatch):
        monkeypatch.setattr(
            _POST_PATH, lambda url, data, timeout: _FakeTokenResponse(401)
        )
        with pytest.raises(RuntimeError, match="OAuth token request failed"):
            EntraClientSecretAuth("t", "c", "s")(self._prepared())
