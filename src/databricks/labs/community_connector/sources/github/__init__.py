"""GitHub source connector."""

from databricks.labs.community_connector.sources.github.github import GithubLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class GithubDataSource(LakeflowSource):
    _lakeflow_connect_cls = GithubLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "github"


__all__ = ["GithubLakeflowConnect",
    "GithubDataSource",
]
