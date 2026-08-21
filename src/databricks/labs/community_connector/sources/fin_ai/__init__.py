"""Fin.ai (Intercom) source connector."""

from databricks.labs.community_connector.sources.fin_ai.fin_ai import (
    FinAiLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class FinAiDataSource(LakeflowSource):
    _lakeflow_connect_cls = FinAiLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "fin_ai"


__all__ = [
    "FinAiLakeflowConnect",
    "FinAiDataSource",
]
