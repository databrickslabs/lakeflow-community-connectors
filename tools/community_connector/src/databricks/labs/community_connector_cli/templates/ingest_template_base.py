from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# Connector source name
source_name = "{SOURCE_NAME}"

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
pipeline_spec = {PIPELINE_SPEC}

# Dynamically import and register the LakeFlow source
register(spark, source_name)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)

