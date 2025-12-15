from typing import List
from pyspark import pipelines as sdp
from pyspark.sql.functions import col, expr
from libs.spec_parser import SpecParser


def _create_cdc_table(
    spark,
    connection_name: str,
    source_table: str,
    destination_table: str,
    primary_keys: List[str],
    sequence_by: str,
    scd_type: str,
    view_name: str,
    table_config: dict[str, str],
    has_deletion_tracking: bool = False,
) -> None:
    """Create CDC table using streaming and apply_changes"""

    @sdp.view(name=view_name)
    def v():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", source_table)
            .options(**table_config)
            .load()
        )

    sdp.create_streaming_table(name=destination_table)
    
    # Build apply_changes arguments
    apply_changes_kwargs = {
        "target": destination_table,
        "source": view_name,
        "keys": primary_keys,
        "sequence_by": col(sequence_by),
        "stored_as_scd_type": scd_type,
    }
    
    # Add apply_as_deletes if deletion tracking is enabled
    if has_deletion_tracking:
        apply_changes_kwargs["apply_as_deletes"] = expr("_is_deleted = true")
    
    sdp.apply_changes(**apply_changes_kwargs)


def _create_snapshot_table(
    spark,
    connection_name: str,
    source_table: str,
    destination_table: str,
    primary_keys: List[str],
    scd_type: str,
    view_name: str,
    table_config: dict[str, str],
) -> None:
    """Create snapshot table using batch read and apply_changes_from_snapshot"""

    @sdp.view(name=view_name)
    def snapshot_view():
        return (
            spark.read.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", source_table)
            .options(**table_config)
            .load()
        )

    sdp.create_streaming_table(name=destination_table)
    sdp.apply_changes_from_snapshot(
        target=destination_table,
        source=view_name,
        keys=primary_keys,
        stored_as_scd_type=scd_type,
    )


def _create_append_table(
    spark,
    connection_name: str,
    source_table: str,
    destination_table: str,
    view_name: str,
    table_config: dict[str, str],
) -> None:
    """Create append table using streaming without apply_changes"""

    sdp.create_streaming_table(name=destination_table)

    @sdp.append_flow(name=view_name, target=destination_table)
    def af():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", source_table)
            .options(**table_config)
            .load()
        )


def _get_table_metadata(spark, connection_name: str, table_list: list[str]) -> dict:
    """Get table metadata (primary_keys, cursor_field, ingestion_type etc.)"""
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", ",".join(table_list))
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
        if row["has_deletion_tracking"] is not None:
            table_metadata["has_deletion_tracking"] = row["has_deletion_tracking"]
        else:
            table_metadata["has_deletion_tracking"] = False
        metadata[row["tableName"]] = table_metadata
    return metadata


def ingest(spark, pipeline_spec: dict) -> None:
    """Ingest a list of tables"""

    # parse the pipeline spec
    spec = SpecParser(pipeline_spec)
    connection_name = spec.connection_name()
    table_list = spec.get_table_list()

    metadata = _get_table_metadata(spark, connection_name, table_list)

    def _ingest_table(table: str) -> None:
        """Helper function to ingest a single table"""
        primary_keys = metadata[table].get("primary_keys")
        cursor_field = metadata[table].get("cursor_field")
        ingestion_type = metadata[table].get("ingestion_type", "cdc")
        has_deletion_tracking = metadata[table].get("has_deletion_tracking", False)
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

        if ingestion_type == "cdc":
            _create_cdc_table(
                spark,
                connection_name,
                table,
                destination_table,
                primary_keys,
                sequence_by,
                scd_type,
                view_name,
                table_config,
                has_deletion_tracking,
            )
        elif ingestion_type == "snapshot":
            _create_snapshot_table(
                spark,
                connection_name,
                table,
                destination_table,
                primary_keys,
                scd_type,
                view_name,
                table_config,
            )
        elif ingestion_type == "append":
            _create_append_table(
                spark,
                connection_name,
                table,
                destination_table,
                view_name,
                table_config,
            )

    for table_name in table_list:
        _ingest_table(table_name)
