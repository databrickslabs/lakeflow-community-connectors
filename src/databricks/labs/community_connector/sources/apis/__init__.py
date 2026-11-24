"""Apis REST Service (Hive historian) source connector."""

from databricks.labs.community_connector.sources.apis.apis import ApisLakeflowConnect

from databricks.labs.community_connector.sparkpds import LakeflowSource


class ApisDataSource(LakeflowSource):
    _lakeflow_connect_cls = ApisLakeflowConnect
    # Keep the default "lakeflow_connect" format name so Unity Catalog
    # connection-option injection keeps working.
    # _format_name = "apis"


__all__ = [
    "ApisLakeflowConnect",
    "ApisDataSource",
]
