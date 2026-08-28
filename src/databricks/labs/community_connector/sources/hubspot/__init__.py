"""HubSpot source connector."""

from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class HubspotDataSource(LakeflowSource):
    _lakeflow_connect_cls = HubspotLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "hubspot"


__all__ = ["HubspotLakeflowConnect",
    "HubspotDataSource",
]
