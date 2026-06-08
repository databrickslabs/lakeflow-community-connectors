"""Example source connector — reference template for new connectors."""

from databricks.labs.community_connector.sources.example.example import ExampleLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class ExampleDataSource(LakeflowSource):
    _lakeflow_connect_cls = ExampleLakeflowConnect


__all__ = ["ExampleLakeflowConnect", "ExampleDataSource"]
