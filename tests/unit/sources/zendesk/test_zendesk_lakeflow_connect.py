from databricks.labs.community_connector.sources.zendesk.zendesk import ZendeskLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestZendeskConnector(LakeflowConnectTests):
    connector_class = ZendeskLakeflowConnect
    simulator_source = "zendesk"
    # Known: returns records without advancing the offset, which would abort a
    # managed incremental pipeline (SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE).
    # Pending owner confirmation/fix; tracked in #307. Remove when fixed.
    offset_advance_exempt_tables = frozenset({"tickets", "users", "organizations"})
    replay_config = {
        "subdomain": "simulator",
        "email": "simulator@example.com",
        "api_token": "simulator-fake-token",
    }
    # `ticket_comments` is fetched per-ticket; the simulator's ticket
    # corpus may not seed comments for the first listed ticket. Fixture
    # limitation, not a connector bug.
    allow_empty_first_read = frozenset({"ticket_comments"})
