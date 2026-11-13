"""Optional end-aware replay preserves legacy and delete-flow behavior."""

from types import SimpleNamespace
from unittest.mock import Mock

from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    IS_DELETE_FLOW, LakeflowStreamReader,
)

SCHEMA = StructType([StructField("id", StringType())])


def test_replay_hook_receives_exact_offsets_and_converts_rows():
    replay = Mock(return_value=iter([{"id": "bounded"}]))
    options = {"tableName": "records"}
    reader = LakeflowStreamReader(options, SCHEMA, SimpleNamespace(read_table_between_offsets=replay))
    start, end = {"position": "a"}, {"position": "b"}
    assert list(reader.readBetweenOffsets(start, end)) == [("bounded",)]
    replay.assert_called_once_with("records", start, end, options)


def test_connector_without_hook_keeps_legacy_read():
    read = Mock(return_value=(iter([{"id": "legacy"}]), {"position": "end"}))
    reader = LakeflowStreamReader({"tableName": "records"}, SCHEMA, SimpleNamespace(read_table=read))
    assert list(reader.readBetweenOffsets({}, {"position": "end"})) == [("legacy",)]
    read.assert_called_once_with("records", {}, {"tableName": "records"})


def test_delete_flow_does_not_use_nondelete_replay_hook():
    replay = Mock(side_effect=AssertionError("nondelete hook must not run"))
    deletes = Mock(return_value=(iter([{"id": "deleted"}]), {}))
    reader = LakeflowStreamReader({"tableName": "records", IS_DELETE_FLOW: "true"}, SCHEMA,
        SimpleNamespace(read_table_between_offsets=replay, read_table_deletes=deletes))
    assert list(reader.readBetweenOffsets({}, {"position": "end"})) == [("deleted",)]
    replay.assert_not_called()
    deletes.assert_called_once_with("records", {}, {"tableName": "records"})
