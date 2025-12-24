from typing import List
from pyspark import pipelines as sdp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from libs.spec_parser import SpecParser


def _active_spark():
    """
    Get an active SparkSession without capturing SparkSession in serialized closures.

    In Lakeflow/SDP pipelines, @sdp.view / @sdp.append_flow functions may be serialized.
    Capturing `spark` from an outer scope can trigger:
      [CONTEXT_ONLY_VALID_ON_DRIVER]
    """
    s = SparkSession.getActiveSession()
    if s is not None:
        return s
    return SparkSession.builder.getOrCreate()


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
) -> None:
    """Create CDC table using streaming and apply_changes"""

    @sdp.view(name=view_name)
    def v():
        spark = _active_spark()
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", source_table)
            .options(**table_config)
            .load()
        )

    sdp.create_streaming_table(name=destination_table)
    sdp.apply_changes(
        target=destination_table,
        source=view_name,
        keys=primary_keys,
        sequence_by=col(sequence_by),
        stored_as_scd_type=scd_type,
    )


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
        spark = _active_spark()
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
        spark = _active_spark()
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", source_table)
            .options(**table_config)
            .load()
        )


def _get_table_metadata(spark, connection_name: str, table_list: list[str], base_options: dict[str, str] | None = None) -> dict:
    """Get table metadata (primary_keys, cursor_field, ingestion_type etc.)"""
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", ",".join(table_list))
        .options(**(base_options or {}))
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

    # Base options needed for the metadata read.
    # Some environments do not inject arbitrary UC connection options into the worker options,
    # so we forward pi_base_url/pi_web_api_url from the table_configuration (if provided).
    base_options: dict[str, str] = {}
    for t in table_list:
        cfg = spec.get_table_configuration(t)
        for k in ("pi_base_url", "pi_web_api_url"):
            v = cfg.get(k) if isinstance(cfg, dict) else None
            if v and k not in base_options:
                base_options[k] = str(v)

    metadata = _get_table_metadata(spark, connection_name, table_list, base_options=base_options)

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
