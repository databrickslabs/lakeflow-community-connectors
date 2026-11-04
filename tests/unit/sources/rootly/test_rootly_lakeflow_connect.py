"""Run the shared connector contract against synthetic Rootly responses."""

from databricks.labs.community_connector.sources.rootly.rootly import RootlyLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestRootlyConnector(LakeflowConnectTests):
    connector_class = RootlyLakeflowConnect
    simulator_source = "rootly"
    replay_config = {"api_token": "simulator-fake-token"}
    sample_records = 5
    table_configs = {
        table: {"page_size": "2"}
        for table in (
            "incidents",
            "services",
            "users",
            "teams",
            "severities",
            "escalation_policies",
            "schedules",
        )
    }
