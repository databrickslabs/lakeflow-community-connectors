from pyspark import pipelines as sdp
from pyspark.sql.functions import col, expr
from libs.spec_parser import SpecParser


def _create_cdc_table(
    spark,
    connection_name: str,
    table: str,
    primary_key: str,
    cursor_field: str,
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
            .option("tableName", table)
            .options(**table_config)
            .load()
        )

    sdp.create_streaming_table(name=table)
    
    # Build apply_changes arguments
    apply_changes_kwargs = {
        "target": table,
        "source": view_name,
        "keys": [primary_key] if isinstance(primary_key, str) else primary_key,
        "sequence_by": col(cursor_field),
        "stored_as_scd_type": "1",
    }
    
    # Add apply_as_deletes if deletion tracking is enabled
    if has_deletion_tracking:
        apply_changes_kwargs["apply_as_deletes"] = expr("_is_deleted = true")
    
    sdp.apply_changes(**apply_changes_kwargs)


def _create_snapshot_table(
    spark,
    connection_name: str,
    table: str,
    primary_key: str,
    view_name: str,
    table_config: dict[str, str],
) -> None:
    """Create snapshot table using batch read and apply_changes_from_snapshot"""

    @sdp.view(name=view_name)
    def snapshot_view():
        return (
            spark.read.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", table)
            .options(**table_config)
            .load()
        )

    sdp.create_streaming_table(name=table)
    sdp.apply_changes_from_snapshot(
        target=table,
        source=view_name,
        keys=[primary_key] if isinstance(primary_key, str) else primary_key,
        stored_as_scd_type="1",
    )


def _create_append_table(
    spark,
    connection_name: str,
    table: str,
    view_name: str,
    table_config: dict[str, str],
) -> None:
    """Create append table using streaming without apply_changes"""

    sdp.create_streaming_table(name=table)

    @sdp.append_flow(name=view_name, target=table)
    def af():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", table)
            .options(**table_config)
            .load()
        )


def _get_table_metadata(spark, connection_name: str, table_list: list[str]) -> dict:
    """Get table metadata (primary_key, cursor_field, ingestion_type etc.)"""
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", ",".join(table_list))
        .load()
    )
    metadata = {}
    for row in df.collect():
        metadata[row["tableName"]] = {
            "primary_key": row["primary_key"] or [],
            "cursor_field": row["cursor_field"] or [],
            "ingestion_type": row["ingestion_type"] or "cdc",
            "has_deletion_tracking": row["has_deletion_tracking"] or False,
        }
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
        primary_key = metadata[table]["primary_key"]
        cursor_field = metadata[table]["cursor_field"]
        ingestion_type = metadata[table].get("ingestion_type", "cdc")
        has_deletion_tracking = metadata[table].get("has_deletion_tracking", False)
        view_name = table + "_staging"
        table_config = spec.get_table_configuration(table)

        if ingestion_type == "cdc":
            _create_cdc_table(
                spark,
                connection_name,
                table,
                primary_key,
                cursor_field,
                view_name,
                table_config,
                has_deletion_tracking,
            )
        elif ingestion_type == "snapshot":
            _create_snapshot_table(
                spark, connection_name, table, primary_key, view_name, table_config
            )
        elif ingestion_type == "append":
            _create_append_table(spark, connection_name, table, view_name, table_config)

    for table_name in table_list:
        _ingest_table(table_name)
