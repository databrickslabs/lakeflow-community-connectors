from databricks.labs.community_connector.sources.microsoft_teams.microsoft_teams import MicrosoftTeamsLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMicrosoftTeamsConnector(LakeflowConnectTests):
    connector_class = MicrosoftTeamsLakeflowConnect
