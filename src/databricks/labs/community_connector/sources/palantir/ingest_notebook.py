# Databricks notebook source
# MAGIC %md
# MAGIC # Palantir Foundry Ingestion Pipeline
# MAGIC
# MAGIC This notebook ingests data from Palantir Foundry ontology objects into Databricks
# MAGIC Delta tables using the Lakeflow Community Connector framework.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Upload `_generated_palantir_python_source.py` to your workspace
# MAGIC 2. Create a Unity Catalog connection with your Palantir credentials
# MAGIC 3. Create the destination catalog/schema in Unity Catalog
# MAGIC
# MAGIC **How to run:**
# MAGIC - Create an SDP pipeline in Databricks and point it to this notebook
# MAGIC - Or run as a Databricks Job with a pipeline task

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Register the Palantir connector with Spark

# COMMAND ----------

from _generated_palantir_python_source import register_lakeflow_source

register_lakeflow_source(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define the pipeline specification
# MAGIC
# MAGIC Edit the values below to match your environment:
# MAGIC - `connection_name`: Your Unity Catalog connection name
# MAGIC - `source_table`: Palantir object type API name (e.g., "ExampleFlight")
# MAGIC - `destination_catalog/schema/table`: Where to land the data in Databricks
# MAGIC - `scd_type`: SCD_TYPE_1 (upsert), SCD_TYPE_2 (history), or APPEND_ONLY
# MAGIC - `cursor_field`: Set for incremental sync, omit for snapshot (full refresh)
# MAGIC - `page_size`: Records per API call (default 100, max 1000)

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

# MAGIC %md
# MAGIC ## Step 3: Run ingestion

# COMMAND ----------

from databricks.labs.community_connector.pipeline import ingest

ingest(spark, pipeline_spec)
