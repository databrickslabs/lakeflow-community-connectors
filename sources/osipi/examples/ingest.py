# Databricks notebook source
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# Connector source name
source_name = "osipi"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
# OSIPI Ingestion - Core tables to osipi.bronze

pipeline_spec = {
    "connection_name": "osipi_connection",
    "objects": [
        # Data Servers table
        {
            "table": {
                "source_table": "pi_dataservers",
                "destination_catalog": "osipi",
                "destination_schema": "bronze",
            }
        },
        # Points (tags) table
        {
            "table": {
                "source_table": "pi_points",
                "destination_catalog": "osipi",
                "destination_schema": "bronze",
            }
        },
        # Time series data table
        {
            "table": {
                "source_table": "pi_timeseries",
                "destination_catalog": "osipi",
                "destination_schema": "bronze",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                },
            }
        },
    ],
}


# Dynamically import and register the LakeFlow source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
