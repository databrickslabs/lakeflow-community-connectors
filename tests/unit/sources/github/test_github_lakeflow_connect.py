from databricks.labs.community_connector.sources.github.github import GithubLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGithubConnector(LakeflowConnectTests):
    connector_class = GithubLakeflowConnect
    # Use simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/github/``. CONNECTOR_TEST_MODE=replay picks
    # up simulate behavior; CONNECTOR_TEST_MODE=live falls back to the
    # cassette-based proxy posture (used to refresh corpus periodically).
    simulator_source = "github"
    # The github connector caps its cursor at ``_init_time`` (admission
    # control). With future-records injection, ``test_read_terminates``
    # explicitly verifies that cap by feeding the connector commits
    # past ``now()`` and asserting it still converges.
    inject_future_records = True
