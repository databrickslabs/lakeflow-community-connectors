
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "freshservice"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
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
#             ├── scd_type: "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
#             ├── primary_keys: List of columns to override connector's default keys
#             └── (other options): See source connector's README
# =============================================================================

# Please update the spec below to configure your ingestion pipeline.

pipeline_spec = {
    "connection_name": "freshservice-new",
    "objects": [
        # Minimal config: just specify the source table
        {
            "table": {
                "source_table": "conversations",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "freshservice",
                "destination_table": "conversations",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                    "primary_keys": ["id","ticket_id"]
                },
            }
        },
        # Full config: customize destination and behavior
        {
            "table": {
                "source_table": "tickets",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "freshservice",
                "destination_table": "tickets",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["id"],
                    "include": "stats,requester,requested_for",  # e.g., for some connectors, additional options may be required (see connector's README).
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
