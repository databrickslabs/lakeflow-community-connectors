"""Mixpanel source connector."""

from databricks.labs.community_connector.sources.mixpanel.mixpanel import MixpanelLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class MixpanelDataSource(LakeflowSource):
    _lakeflow_connect_cls = MixpanelLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "mixpanel"


__all__ = ["MixpanelLakeflowConnect",
    "MixpanelDataSource",
]
