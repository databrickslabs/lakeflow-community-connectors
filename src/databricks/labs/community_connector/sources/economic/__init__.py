"""Visma e-conomic source connector."""

from databricks.labs.community_connector.sources.economic.economic import EconomicLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class EconomicDataSource(LakeflowSource):
    _lakeflow_connect_cls = EconomicLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "economic"


__all__ = ["EconomicLakeflowConnect",
    "EconomicDataSource",
]
