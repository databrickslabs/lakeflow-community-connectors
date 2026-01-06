from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

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
#         └── table_configuration (optional)
#             ├── scd_type (optional): "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
#             ├── primary_keys (optional): List of columns to override connector's default keys
#             └── (other options): See source connector's README
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
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
