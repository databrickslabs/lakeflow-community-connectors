# Databricks notebook source
# pylint: skip-file
# ruff: noqa
"""MongoDB change-stream ingestion pipeline.

Use this SDP notebook for collections that need SCD2 versions of updates
and deletes (``_id`` does not change). The first trigger dumps the collection
and captures a resume token; scheduled runs only watch the oplog.

Documents land as VARIANT in the ``document`` column.

Prerequisites:
1. Unity Catalog connection with ``connection_uri`` and ``database``.
2. Upload ``_generated_mongodb_python_source.py`` next to this notebook, or
   register the connector from the installed package.

Fill in ``CONNECTION_NAME``, catalog, and schema before running.
"""

from databricks.labs.community_connector import register
from databricks.labs.community_connector.pipeline import ingest

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled",
    "true",
)

source_name = "mongodb"
connection_name = "CONNECTION_NAME"

DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "mongodb_bronze"

register(spark, source_name)

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": "nested_only",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "nested_only_scd2",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_2",
                },
            }
        }
    ],
}

ingest(spark, pipeline_spec)
