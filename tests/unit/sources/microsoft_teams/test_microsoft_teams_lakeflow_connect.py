from databricks.labs.community_connector.sources.microsoft_teams.microsoft_teams import MicrosoftTeamsLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMicrosoftTeamsConnector(LakeflowConnectTests):
    connector_class = MicrosoftTeamsLakeflowConnect
    simulator_source = "microsoft_teams"
    replay_config = {
        "tenant_id": "00000000-0000-0000-0000-000000000000",
        "client_id": "00000000-0000-0000-0000-000000000000",
        "client_secret": "simulator-fake-secret",
    }
