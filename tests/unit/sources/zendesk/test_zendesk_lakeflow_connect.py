from databricks.labs.community_connector.sources.zendesk.zendesk import ZendeskLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestZendeskConnector(LakeflowConnectTests):
    connector_class = ZendeskLakeflowConnect
    simulator_source = "zendesk"
    replay_config = {
        "subdomain": "simulator",
        "email": "simulator@example.com",
        "api_token": "simulator-fake-token",
    }
    # `ticket_comments` is fetched per-ticket; the simulator's ticket
    # corpus may not seed comments for the first listed ticket. Fixture
    # limitation, not a connector bug.
    allow_empty_first_read = frozenset({"ticket_comments"})
