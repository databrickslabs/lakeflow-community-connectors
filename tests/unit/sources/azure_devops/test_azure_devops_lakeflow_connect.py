import pytest

from databricks.labs.community_connector.sources.azure_devops.azure_devops import (
    AzureDevopsLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAzureDevopsConnector(LakeflowConnectTests):
    connector_class = AzureDevopsLakeflowConnect
    simulator_source = "azure_devops"
    replay_config = {
        "organization": "simulator-org",
        "project": "simulator-project",
        "personal_access_token": "simulator-fake-pat",
    }


class TestAzureDevopsRuntimeAuth:
    """Runtime auth selection in __init__ (offline — no network).

    The connector consumes whatever the connection provides at query time:
    a PAT (pat method → Basic) or a UC-injected bearer ``access_token``
    (service_principal / OAuth m2m method → Bearer). It never runs the OAuth
    flow itself — UC mints and refreshes ``access_token`` server-side.
    """

    _BASE = {"organization": "myorg"}

    def test_pat_sets_basic_auth(self):
        c = AzureDevopsLakeflowConnect(
            {**self._BASE, "personal_access_token": "pat123"}
        )
        assert c._session.headers["Authorization"].startswith("Basic ")

    def test_access_token_sets_bearer_auth(self):
        c = AzureDevopsLakeflowConnect(
            {**self._BASE, "access_token": "uc-injected-token"}
        )
        assert c._session.headers["Authorization"] == "Bearer uc-injected-token"

    def test_access_token_takes_precedence_over_pat(self):
        c = AzureDevopsLakeflowConnect(
            {
                **self._BASE,
                "access_token": "bearer-tok",
                "personal_access_token": "pat123",
            }
        )
        assert c._session.headers["Authorization"] == "Bearer bearer-tok"

    def test_no_credential_raises(self):
        with pytest.raises(ValueError, match="access_token.*personal_access_token"):
            AzureDevopsLakeflowConnect({**self._BASE})

    def test_organization_required(self):
        with pytest.raises(ValueError, match="organization"):
            AzureDevopsLakeflowConnect({"personal_access_token": "pat123"})
