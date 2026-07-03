"""SAP SuccessFactors connector for Lakeflow Connect."""

from databricks.labs.community_connector.sources.sap_successfactors.sap_successfactors import (
    SapSuccessFactorsLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class SapSuccessFactorsDataSource(LakeflowSource):
    _lakeflow_connect_cls = SapSuccessFactorsLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "sap_successfactors"


__all__ = ["SapSuccessFactorsLakeflowConnect",
    "SapSuccessFactorsDataSource",
]
