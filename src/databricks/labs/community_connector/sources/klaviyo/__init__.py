"""Klaviyo source connector."""

from databricks.labs.community_connector.sources.klaviyo.klaviyo import (
    KlaviyoLakeflowConnect,
)

from databricks.labs.community_connector.sparkpds import LakeflowSource


class KlaviyoDataSource(LakeflowSource):
    _lakeflow_connect_cls = KlaviyoLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "klaviyo"


__all__ = [
    "KlaviyoLakeflowConnect",
    "KlaviyoDataSource",
]
