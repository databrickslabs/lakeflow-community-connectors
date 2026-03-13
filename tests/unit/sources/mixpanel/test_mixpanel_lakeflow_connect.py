from databricks.labs.community_connector.sources.mixpanel.mixpanel import MixpanelLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMixpanelConnector(LakeflowConnectTests):
    connector_class = MixpanelLakeflowConnect
