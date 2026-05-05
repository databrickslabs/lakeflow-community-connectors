from databricks.labs.community_connector.sources.github.github import GithubLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGithubConnector(LakeflowConnectTests):
    connector_class = GithubLakeflowConnect
    # Use simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/github/``. CONNECTOR_TEST_MODE=replay picks
    # up simulate behavior; CONNECTOR_TEST_MODE=live falls back to the
    # cassette-based proxy posture (used to refresh corpus periodically).
    # Note: the github commits spec declares ``synthesize_future_records``,
    # which makes ``test_read_terminates`` an explicit cap check — future
    # commits leak through if ``until=<init_time>`` is missing or wrong.
    simulator_source = "github"
    replay_config = {"token": "simulator-fake-token"}
