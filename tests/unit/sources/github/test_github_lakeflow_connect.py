from databricks.labs.community_connector.sources.github.github import GithubLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGithubConnector(LakeflowConnectTests):
    connector_class = GithubLakeflowConnect
