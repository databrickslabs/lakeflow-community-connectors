"""Microsoft Purview Unified Catalog source connector."""

from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview import (  # pylint: disable=line-too-long
    MicrosoftPurviewLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class MicrosoftPurviewDataSource(LakeflowSource):
    _lakeflow_connect_cls = MicrosoftPurviewLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "microsoft_purview"


__all__ = [
    "MicrosoftPurviewLakeflowConnect",
    "MicrosoftPurviewDataSource",
]
