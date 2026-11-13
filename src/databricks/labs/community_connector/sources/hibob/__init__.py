"""HiBob (Bob) source connector."""

from databricks.labs.community_connector.sources.hibob.hibob import HibobLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class HibobDataSource(LakeflowSource):
    _lakeflow_connect_cls = HibobLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "hibob"


__all__ = ["HibobLakeflowConnect",
    "HibobDataSource",
]
