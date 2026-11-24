from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# Enable the injection of connection options from Unity Catalog connections into connectors
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

# Connector source name.
source_name = "{SOURCE_NAME}"  # e.g., "github", "salesforce"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
# Update the spec below to configure your ingestion pipeline.
#
# pipeline_spec
# ├── connection_name (required): The Unity Catalog connection name
# └── objects[]: List of tables to ingest
#     └── table
#         ├── source_table (required): The table name in the source system
#         ├── destination_catalog (optional): Target catalog (defaults to pipeline's default)
#         ├── destination_schema (optional): Target schema (defaults to pipeline's default)
#         ├── destination_table (optional): Target table name (defaults to source_table)
#         ├── connector_options (optional): Source-specific options
#         │   └── community_connector_options
#         │       └── options: Per-connector options from the source's README
#         │                     (e.g. "owner"/"repo" for GitHub). These go HERE,
#         │                     NOT under table_configuration.
#         └── table_configuration (optional): Ingestion controls only
#             ├── scd_type (optional): "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
#             └── primary_keys (optional): List of columns to override connector's default keys
# =============================================================================

pipeline_spec = {
    "connection_name": "{CONNECTION_NAME}",
    "objects": [
        # Minimal config: just specify the source table
        {
            "table": {
                "source_table": "<YOUR_TABLE_NAME>",
            }
        },
        # Full config: customize destination and behavior
        {
            "table": {
                "source_table": "<YOUR_TABLE_NAME>",
                "destination_catalog": "<YOUR_CATALOG>",
                "destination_schema": "<YOUR_SCHEMA>",
                "destination_table": "<YOUR_TABLE>",
                # Source-specific options (see the source connector's README) go
                # under connector_options.community_connector_options.options —
                # NOT under table_configuration, where the managed flow ignores
                # them.
                "connector_options": {
                    "community_connector_options": {
                        "options": {
                            # e.g., for some GitHub tables, "owner" and "repo"
                            # are required.
                            "<OTHER_OPTION_NAME>": "<VALUE>",
                        },
                    },
                },
                # table_configuration carries only ingestion controls.
                "table_configuration": {
                    "scd_type": "<SCD_TYPE_1 | SCD_TYPE_2 | APPEND_ONLY>",
                    "primary_keys": ["<PK_COL1>", ...],
                },
            }
        },
        # ... more tables to ingest...
    ],
}


# Dynamically import and register the LakeFlow source
register(spark, source_name)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
