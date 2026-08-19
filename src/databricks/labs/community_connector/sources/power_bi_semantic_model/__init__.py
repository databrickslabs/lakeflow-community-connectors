"""Power BI semantic model source connector."""

from databricks.labs.community_connector.sources.power_bi_semantic_model.power_bi_semantic_model import (  # noqa: E501  pylint: disable=line-too-long
    PowerBiSemanticModelLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class PowerBiSemanticModelDataSource(LakeflowSource):
    _lakeflow_connect_cls = PowerBiSemanticModelLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "power_bi_semantic_model"


__all__ = [
    "PowerBiSemanticModelLakeflowConnect",
    "PowerBiSemanticModelDataSource",
]
