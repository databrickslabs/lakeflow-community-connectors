from databricks.labs.community_connector.sources.mixpanel.mixpanel import MixpanelLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMixpanelConnector(LakeflowConnectTests):
    connector_class = MixpanelLakeflowConnect
    simulator_source = "mixpanel"
    replay_config = {"api_secret": "simulator-fake-secret"}
    # Simulator corpus dates don't overlap the connector's date-window
    # iteration — fixture limitation, not a bug.
    allow_empty_first_read = frozenset({"events", "engage"})
