"""Backward-compatible AvailableNow lifecycle hook for source connectors."""

import pytest
from pyspark.sql.streaming.datasource import ReadAllAvailable
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    LakeflowPartitionedStreamReader,
    LakeflowStreamReader,
)


class _DefaultConnector(LakeflowConnect):
    def list_tables(self):
        return []

    def get_table_schema(self, table_name, table_options):
        return StructType([])

    def read_table_metadata(self, table_name, table_options):
        return {}

    def read_table(self, table_name, start_offset, table_options):
        return iter(()), {}

    def latest_offset(self, table_name, table_options, start_offset):
        return {"seq": 0}


class _AvailableNowConnector(_DefaultConnector):
    def __init__(self):
        super().__init__({})
        self.offset_calls = 0
        self.read_calls = 0

    def requires_trigger_available_now(self):
        return True

    def latest_offset(self, table_name, table_options, start_offset):
        self.offset_calls += 1
        return {"seq": 1}

    def get_partitions(self, table_name, table_options, start_offset, end_offset):
        return []

    def read_partition(self, table_name, partition, table_options):
        self.read_calls += 1
        return iter(())


def test_default_connectors_keep_existing_stream_behavior():
    connector = _DefaultConnector({})
    assert connector.requires_trigger_available_now() is False

    reader = LakeflowPartitionedStreamReader({"tableName": "events"}, StructType([]), connector)
    assert reader.latestOffset({}, ReadAllAvailable()) == {"seq": 0}


def test_opt_in_partitioned_connector_fails_before_io_until_available_now_callback():
    connector = _AvailableNowConnector()
    reader = LakeflowPartitionedStreamReader({"tableName": "events"}, StructType([]), connector)

    with pytest.raises(ValueError, match="requires Spark Trigger.AvailableNow"):
        reader.latestOffset({}, ReadAllAvailable())
    assert connector.offset_calls == 0

    reader.prepareForTriggerAvailableNow()
    assert reader.latestOffset({}, ReadAllAvailable()) == {"seq": 1}
    assert connector.offset_calls == 1


def test_opt_in_simple_reader_fails_before_io_until_available_now_callback():
    connector = _AvailableNowConnector()
    reader = LakeflowStreamReader({"tableName": "events"}, StructType([]), connector)

    with pytest.raises(ValueError, match="requires Spark Trigger.AvailableNow"):
        reader.read({})
    assert connector.read_calls == 0

    reader.prepareForTriggerAvailableNow()
    assert reader.read({})[1] == {}
