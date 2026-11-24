"""MongoDB source connector."""

from databricks.labs.community_connector.sources.mongodb.mongodb import MongodbLakeflowConnect


from databricks.labs.community_connector.libs.utils import parse_value
from databricks.labs.community_connector.sparkpds import LakeflowSource
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    IS_DELETE_FLOW,
    TABLE_NAME,
    LakeflowStreamReader,
)


class MongodbStreamReader(LakeflowStreamReader):
    """Stream reader that replays a batch exactly between its two offsets.

    Spark replays a planned but uncommitted batch through
    ``readBetweenOffsets(start, end)`` after a restart. The generic reader
    re-reads from ``start`` without a bound; a change stream keeps moving, so
    that can return events of the next batch too (duplicates in ``append``
    tables). This reader stops at ``end``.
    """

    # pylint: disable=invalid-name
    def readBetweenOffsets(self, start: dict, end: dict):
        table_options = {k: v for k, v in self.options.items() if k != IS_DELETE_FLOW}
        records = self.lakeflow_connect.read_table_range(
            self.options[TABLE_NAME], start, end, table_options,
            deletes=self.options.get(IS_DELETE_FLOW) == "true",
        )
        return map(lambda x: parse_value(x, self.schema), records)


class MongodbDataSource(LakeflowSource):
    _lakeflow_connect_cls = MongodbLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "mongodb"

    def simpleStreamReader(self, schema):
        return MongodbStreamReader(self.options, schema, self.lakeflow_connect)


__all__ = ["MongodbLakeflowConnect",
    "MongodbDataSource",
    "MongodbStreamReader",
]
