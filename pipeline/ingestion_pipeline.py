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
        metadata[row["tableName"]] = table_metadata
    return metadata


def ingest(spark, pipeline_spec: dict) -> None:
    """
    Ingest tables from a pipeline specification.

    Supports multiple configurations for the same source table (e.g., channels for different teams).
    Each configuration creates a separate staging view, then all are merged into the destination table.
    """
    # Parse the pipeline spec
    spec = SpecParser(pipeline_spec)
    connection_name = spec.connection_name()

    # Get all objects with their full configurations
    objects = spec._model.objects

    # Group objects by (source_table, destination_table) to handle multiple configs
    table_groups = {}
    for idx, obj in enumerate(objects):
        source_table = obj.table.source_table
        dest_catalog = obj.table.destination_catalog
        dest_schema = obj.table.destination_schema
        dest_table = obj.table.destination_table or source_table

        # Create full destination table name
        if dest_catalog and dest_schema:
            full_dest_table = f"`{dest_catalog}`.`{dest_schema}`.`{dest_table}`"
        else:
            full_dest_table = dest_table

        # Use (source_table, full_dest_table) as key
        key = (source_table, full_dest_table)

        if key not in table_groups:
            table_groups[key] = []

        table_groups[key].append((idx, obj))

    # Get unique source tables for metadata fetch
    unique_source_tables = list(dict.fromkeys([obj.table.source_table for obj in objects]))
    metadata = _get_table_metadata(spark, connection_name, unique_source_tables)

    # Process each table group
    for (source_table, destination_table), object_list in table_groups.items():
        # Get metadata for this source table
        primary_keys = metadata[source_table].get("primary_keys")
        cursor_field = metadata[source_table].get("cursor_field")
        ingestion_type = metadata[source_table].get("ingestion_type", "cdc")

        # If only one configuration, use the simple approach
        if len(object_list) == 1:
            idx, obj = object_list[0]
            table_config = obj.table.table_configuration or {}

            # Remove special keys
            special_keys = {"scd_type", "primary_keys", "sequence_by"}
            table_config = {k: v for k, v in table_config.items() if k not in special_keys}

            # Override parameters with spec values if available
            primary_keys = (
                obj.table.table_configuration.get("primary_keys")
                if obj.table.table_configuration
                else None
            ) or primary_keys
            sequence_by = (
                obj.table.table_configuration.get("sequence_by")
                if obj.table.table_configuration
                else None
            ) or cursor_field
            scd_type_raw = (
                obj.table.table_configuration.get("scd_type")
                if obj.table.table_configuration
                else None
            )

            if scd_type_raw == "APPEND_ONLY":
                ingestion_type = "append"
            scd_type = "2" if scd_type_raw == "SCD_TYPE_2" else "1"

            view_name = source_table + "_staging"

            if ingestion_type == "cdc":
                _create_cdc_table(
                    spark,
                    connection_name,
                    source_table,
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
                    source_table,
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
                    source_table,
                    destination_table,
                    view_name,
                    table_config,
                )

        # Multiple configurations - create separate views and merge
        else:
            # Get configuration from first object for defaults
            first_obj = object_list[0][1]
            scd_type_raw = (
                first_obj.table.table_configuration.get("scd_type")
                if first_obj.table.table_configuration
                else None
            )
            if scd_type_raw == "APPEND_ONLY":
                ingestion_type = "append"
            scd_type = "2" if scd_type_raw == "SCD_TYPE_2" else "1"
            sequence_by = (
                first_obj.table.table_configuration.get("sequence_by")
                if first_obj.table.table_configuration
                else None
            ) or cursor_field

            # Create separate staging views for each configuration
            staging_views = []
            for idx, obj in object_list:
                table_config = obj.table.table_configuration or {}

                # Remove special keys
                special_keys = {"scd_type", "primary_keys", "sequence_by"}
                table_config = {k: v for k, v in table_config.items() if k not in special_keys}

                # Create unique view name for this configuration
                view_name = f"{source_table}_staging_{idx}"
                staging_views.append(view_name)

                # Create staging view - use readStream for CDC, read for snapshot
                # IMPORTANT: We use a factory function to avoid Python closure issues
                # where all views would share the same config reference
                def make_view_function(config_copy):
                    if ingestion_type == "cdc":
                        def view_func():
                            return (
                                spark.readStream.format("lakeflow_connect")
                                .option("databricks.connection", connection_name)
                                .option("tableName", source_table)
                                .options(**config_copy)
                                .load()
                            )
                    else:
                        def view_func():
                            return (
                                spark.read.format("lakeflow_connect")
                                .option("databricks.connection", connection_name)
                                .option("tableName", source_table)
                                .options(**config_copy)
                                .load()
                            )
                    return view_func

                sdp.view(name=view_name)(make_view_function(table_config))

            # For CDC mode with multiple configs, create streaming union view
            if ingestion_type == "cdc":
                unified_view_name = f"{source_table}_staging"

                @sdp.view(name=unified_view_name)
                def create_unified_view(views=staging_views):
                    # For streaming views, we need to use readStream
                    dfs = [spark.readStream.table(view) for view in views]
                    from functools import reduce
                    return reduce(lambda df1, df2: df1.union(df2), dfs)

                sdp.create_streaming_table(name=destination_table)
                sdp.apply_changes(
                    target=destination_table,
                    source=unified_view_name,
                    keys=primary_keys,
                    sequence_by=col(sequence_by),
                    stored_as_scd_type=scd_type,
                )
            elif ingestion_type == "snapshot":
                # For snapshot, use regular batch union
                unified_view_name = f"{source_table}_staging"

                @sdp.view(name=unified_view_name)
                def create_unified_view(views=staging_views):
                    dfs = [spark.table(view) for view in views]
                    from functools import reduce
                    return reduce(lambda df1, df2: df1.union(df2), dfs)

                sdp.create_streaming_table(name=destination_table)
                sdp.apply_changes_from_snapshot(
                    target=destination_table,
                    source=unified_view_name,
                    keys=primary_keys,
                    stored_as_scd_type=scd_type,
                )
            elif ingestion_type == "append":
                # For append mode, use streaming union
                sdp.create_streaming_table(name=destination_table)

                @sdp.append_flow(name=f"{source_table}_append_flow", target=destination_table)
                def create_append_flow(views=staging_views):
                    dfs = [spark.readStream.table(view) for view in views]
                    from functools import reduce
                    return reduce(lambda df1, df2: df1.union(df2), dfs)
