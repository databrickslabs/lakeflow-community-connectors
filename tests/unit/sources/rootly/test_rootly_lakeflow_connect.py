"""Run the shared connector contract against synthetic Rootly responses."""

from databricks.labs.community_connector.source_simulator import MODE_LIVE
from databricks.labs.community_connector.sources.rootly.rootly import RootlyLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests, _resolve_env_mode_for_simulator

# Tiny simulator pages exercise pagination. Live accounts can hold thousands of
# incidents, which must drain one page per batch within the termination check's
# iteration cap, so live runs use Rootly's maximum page size.
PAGE_SIZE = "1000" if _resolve_env_mode_for_simulator("rootly") == MODE_LIVE else "2"


class TestRootlyConnector(LakeflowConnectTests):
    connector_class = RootlyLakeflowConnect
    simulator_source = "rootly"
    replay_config = {"api_token": "simulator-fake-token"}
    sample_records = 5
    table_configs = {
        table: {"page_size": PAGE_SIZE}
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
