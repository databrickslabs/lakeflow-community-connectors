# Databricks notebook source
from databricks.labs.community_connector import register
from databricks.labs.community_connector.pipeline import ingest

# COMMAND ----------

register(spark, "palantir")

# COMMAND ----------

pipeline_spec = {
    "connection_name": "palantir",
    "objects": [
        {
            "table": {
                "source_table": "FlightsFinal",
                "destination_catalog": "users",
                "destination_schema": "zuhaib_mohd",
                "destination_table": "flights_final",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_2",
                    "cursor_field": "arrivalTimestamp",
                    "page_size": "10000",
                },
            }
        },
    ],
}

# COMMAND ----------

ingest(spark, pipeline_spec)
