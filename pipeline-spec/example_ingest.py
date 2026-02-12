from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# Enable the injection of connection options from Unity Catalog connections into connectors
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

# Connector source name. Update this to the name of the connector source you want to use.
source_name = "<YOUR_SOURCE_NAME>"  # e.g., "github", "salesforce"

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
#         └── table_configuration (optional)
#             ├── scd_type (optional): "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
#             ├── primary_keys (optional): List of columns to override connector's default keys
#             └── (other options): See source connector's README
# =============================================================================

pipeline_spec = {
    "connection_name": "<YOUR_CONNECTION_NAME>",
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
                "table_configuration": {
                    "scd_type": "<SCD_TYPE_1 | SCD_TYPE_2 | APPEND_ONLY>",
                    "primary_keys": ["<PK_COL1>", ...],
                    "<OTHER_OPTION_NAME>": "<VALUE>",  # e.g., for some GitHub tables, "owner" and "repo" need to be provided.
                },
            }
        },
        # ... more tables to ingest...
    ],
}


# Dynamically import and register the LakeFlow source
# Register through source name. Run the pipeline with the source code.
register(spark, source_name)

# register the LakeflowConnect
from databricks.labs.community_connector.sources.github import GithubLakeflowConnect

register(spark, GithubLakeflowConnect)

# register the Python DataSource implementation
from my_source import MyDataSource

register(spark, MyDataSource)


# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
