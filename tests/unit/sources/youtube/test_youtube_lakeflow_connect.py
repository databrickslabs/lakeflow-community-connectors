"""Lakeflow Connect test suite for the YouTube connector."""

from databricks.labs.community_connector.sources.youtube.youtube import YouTubeLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestYouTubeConnector(LakeflowConnectTests):
    connector_class = YouTubeLakeflowConnect
    simulator_source = "youtube"
    replay_config = {"api_key": "simulator-fake-api-key"}
    record_replay_ignore_query_params = frozenset({"key"})
    sample_records = 10
