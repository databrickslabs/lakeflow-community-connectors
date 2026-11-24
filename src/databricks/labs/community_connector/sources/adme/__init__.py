"""Azure Data Manager for Energy (ADME) source connector."""

from databricks.labs.community_connector.sources.adme.adme import (
    ADMELakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class ADMEDataSource(LakeflowSource):
    _lakeflow_connect_cls = ADMELakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "adme"


__all__ = ["ADMELakeflowConnect",
    "ADMEDataSource",
]
