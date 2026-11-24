from databricks.labs.community_connector.sources.microsoft_teams.microsoft_teams import MicrosoftTeamsLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMicrosoftTeamsConnector(LakeflowConnectTests):
    connector_class = MicrosoftTeamsLakeflowConnect
    simulator_source = "microsoft_teams"
    # Known: returns records without advancing the offset, which would abort a
    # managed incremental pipeline (SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE).
    # Pending owner confirmation/fix; tracked in #307. Remove when fixed.
    offset_advance_exempt_tables = frozenset({"messages", "message_replies"})
    replay_config = {
        "tenant_id": "00000000-0000-0000-0000-000000000000",
        "client_id": "00000000-0000-0000-0000-000000000000",
        "client_secret": "simulator-fake-secret",
    }
