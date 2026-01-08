# pylint: disable=no-member
import json
from dataclasses import dataclass
from typing import List
from pyspark import pipelines as sdp
from pyspark.sql.functions import col, expr
from libs.spec_parser import SpecParser


@dataclass
class SdpTableConfig:  # pylint: disable=too-many-instance-attributes
    """SDP configuration to ingest a table."""

    source_table: str
    destination_table: str
    view_name: str
    table_config: dict[str, str]
    primary_keys: List[str]
    sequence_by: str
    scd_type: str
    with_deletes: bool = False


def _create_cdc_table(
    spark, connection_name: str, config: SdpTableConfig
) -> None:
    """Create CDC table using streaming and apply_changes

    """

    @sdp.view(name=config.view_name)
    def v():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )

    sdp.create_streaming_table(name=config.destination_table)
    sdp.apply_changes(
        target=config.destination_table,
        source=config.view_name,
        keys=config.primary_keys,
        sequence_by=col(config.sequence_by),
        stored_as_scd_type=config.scd_type,
    )

    # Delete flow - only enabled for cdc_with_deletes ingestion type
    if config.with_deletes:
        delete_view_name = config.source_table + "_delete_staging"

        @sdp.view(name=delete_view_name)
        def delete_view():
            return (
                spark.readStream.format("lakeflow_connect")
                .option("databricks.connection", connection_name)
                .option("tableName", config.source_table)
                .option("isDeleteFlow", "true")
                .options(**config.table_config)
                .load()
            )

        sdp.apply_changes(
            target=config.destination_table,
            source=delete_view_name,
            keys=config.primary_keys,
            sequence_by=col(config.sequence_by),
            stored_as_scd_type=config.scd_type,
            apply_as_deletes=expr("true"),
            name=delete_view_name + "_delete_flow",
        )


def _create_snapshot_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    """Create snapshot table using batch read and apply_changes_from_snapshot"""

    @sdp.view(name=config.view_name)
    def snapshot_view():
        return (
            spark.read.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )

    sdp.create_streaming_table(name=config.destination_table)
    sdp.apply_changes_from_snapshot(
        target=config.destination_table,
        source=config.view_name,
        keys=config.primary_keys,
        stored_as_scd_type=config.scd_type,
    )


def _create_append_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    """Create append table using streaming without apply_changes"""

    sdp.create_streaming_table(name=config.destination_table)

    @sdp.append_flow(name=config.view_name, target=config.destination_table)
    def af():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )


def _get_table_metadata(
    spark, connection_name: str, table_list: list[str], table_configs: dict[str, str]
) -> dict:
    """Get table metadata (primary_keys, cursor_field, ingestion_type etc.)"""
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", ",".join(table_list))
        .option("tableConfigs", json.dumps(table_configs))
        .load()
    )
    metadata = {}
    for row in df.collect():
        table_metadata = {}
        if row["primary_keys"] is not None:
            table_metadata["primary_keys"] = row["primary_keys"]
        if row["cursor_field"] is not None:
            table_metadata["cursor_field"] = row["cursor_field"]
        if row["ingestion_type"] is not None:
            table_metadata["ingestion_type"] = row["ingestion_type"]
        metadata[row["tableName"]] = table_metadata
    return metadata


def ingest(spark, pipeline_spec: dict) -> None:
    """Ingest a list of tables"""

    # parse the pipeline spec
    spec = SpecParser(pipeline_spec)
    connection_name = spec.connection_name()
    table_list = spec.get_table_list()

    # Get table_configurations for all tables. These are merged into one dict
    # keyed by table name.
    table_configs = spec.get_table_configurations()
    metadata = _get_table_metadata(spark, connection_name, table_list, table_configs)

    def _ingest_table(table: str) -> None:
        """Helper function to ingest a single table"""
        primary_keys = metadata[table].get("primary_keys")
        cursor_field = metadata[table].get("cursor_field")
        ingestion_type = metadata[table].get("ingestion_type", "cdc")
        view_name = table + "_staging"
        table_config = spec.get_table_configuration(table)
        destination_table = spec.get_full_destination_table_name(table)

        # Override parameters with spec values if available
        primary_keys = spec.get_primary_keys(table) or primary_keys
        sequence_by = spec.get_sequence_by(table) or cursor_field
        scd_type_raw = spec.get_scd_type(table)
        if scd_type_raw == "APPEND_ONLY":
            ingestion_type = "append"
        scd_type = "2" if scd_type_raw == "SCD_TYPE_2" else "1"

        config = SdpTableConfig(
            source_table=table,
            destination_table=destination_table,
            view_name=view_name,
            table_config=table_config,
            primary_keys=primary_keys,
            sequence_by=sequence_by,
            scd_type=scd_type,
            with_deletes=(ingestion_type == "cdc_with_deletes"),
        )

        if ingestion_type in ("cdc", "cdc_with_deletes"):
            _create_cdc_table(
                spark,
                connection_name,
                config
            )
        elif ingestion_type == "snapshot":
            _create_snapshot_table(spark, connection_name, config)
        elif ingestion_type == "append":
            _create_append_table(spark, connection_name, config)

    for table_name in table_list:
        _ingest_table(table_name)
