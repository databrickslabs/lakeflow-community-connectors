from databricks.labs.community_connector.sources.github.github import GithubLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGithubConnector(LakeflowConnectTests):
    connector_class = GithubLakeflowConnect
    # Use simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/github/``. CONNECTOR_TEST_MODE=replay picks
    # up simulate behavior; CONNECTOR_TEST_MODE=live falls back to the
    # cassette-based proxy posture (used to refresh corpus periodically).
    simulator_source = "github"
