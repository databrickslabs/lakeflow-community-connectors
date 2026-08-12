"""Example source connector."""

from databricks.labs.community_connector.sources.example.example import ExampleLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class ExampleDataSource(LakeflowSource):
    _lakeflow_connect_cls = ExampleLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "example"


__all__ = ["ExampleLakeflowConnect",
    "ExampleDataSource",
]
