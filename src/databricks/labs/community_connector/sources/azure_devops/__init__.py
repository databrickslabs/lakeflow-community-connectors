"""Azure DevOps source connector."""

from databricks.labs.community_connector.sources.azure_devops.azure_devops import (
    AzureDevopsLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class AzureDevopsDataSource(LakeflowSource):
    _lakeflow_connect_cls = AzureDevopsLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "azure_devops"


__all__ = ["AzureDevopsLakeflowConnect",
    "AzureDevopsDataSource",
]
