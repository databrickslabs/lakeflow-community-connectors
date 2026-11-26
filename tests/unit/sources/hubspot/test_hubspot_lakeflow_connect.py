from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect
from tests.unit.sources.hubspot.hubspot_test_utils import LakeflowConnectWriteTestUtils
from tests.unit.sources.test_suite import LakeflowConnectTests
from tests.unit.sources.test_write_back_suite import LakeflowConnectWriteBackTests


class TestHubspotConnector(LakeflowConnectWriteBackTests, LakeflowConnectTests):
    connector_class = HubspotLakeflowConnect
    simulator_source = "hubspot"
    # Known: returns records without advancing the offset, which would abort a
    # managed incremental pipeline (SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE).
    # Pending owner confirmation/fix; tracked in #307. Remove when fixed.
    offset_advance_exempt_tables = frozenset(
        {
            "calls",
            "companies",
            "contacts",
            "deals",
            "emails",
            "meetings",
            "notes",
            "tasks",
            "tickets",
        }
    )
    test_utils_class = LakeflowConnectWriteTestUtils
    replay_config = {"access_token": "simulator-fake-access-token"}
