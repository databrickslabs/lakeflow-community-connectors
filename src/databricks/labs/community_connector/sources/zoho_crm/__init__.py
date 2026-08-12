"""Zoho CRM source connector."""

from databricks.labs.community_connector.sources.zoho_crm.zoho_crm import ZohoCRMLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class ZohoCRMDataSource(LakeflowSource):
    _lakeflow_connect_cls = ZohoCRMLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "zoho_crm"


__all__ = ["ZohoCRMLakeflowConnect",
    "ZohoCRMDataSource",
]
