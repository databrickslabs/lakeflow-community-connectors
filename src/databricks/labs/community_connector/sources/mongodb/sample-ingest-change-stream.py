# Databricks notebook source
# pylint: skip-file
# ruff: noqa
"""MongoDB change-stream ingestion pipeline.

Use this SDP notebook for collections ingested through change streams.
The first trigger dumps the collection and captures a resume token;
scheduled runs only watch the oplog.

Documents land as VARIANT in ``document``. ``document_hash`` is the
comparable stand-in Spark AUTO CDC Type 2 uses (VARIANT cannot be
compared with ``<=>``). Type 1 keeps current state; Type 2 versions a
row only when that hash changes, not on every ``event_time``.

Call ``ingest()`` once per destination: the spec parser keys configuration
by ``source_table``, so Type 1 and Type 2 cannot share one spec object list.

Prerequisites:
1. Unity Catalog connection with ``connection_uri`` and ``database``.
2. Pipeline environment libraries: ``pymongo==4.18.1`` and
   ``dnspython==2.8.0``.
3. Upload ``_generated_mongodb_python_source.py`` next to this notebook,
   or register the connector from the installed package.
4. After adding ``document_hash`` or switching SCD type on an existing
   destination, run a **full refresh**.

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
source_table = "nested_only"

DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "mongodb_bronze"

register(spark, source_name)

ingest(
    spark,
    {
        "connection_name": connection_name,
        "objects": [
            {
                "table": {
                    "source_table": source_table,
                    "destination_catalog": DESTINATION_CATALOG,
                    "destination_schema": DESTINATION_SCHEMA,
                    "destination_table": "nested_only_current",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_1",
                    },
                }
            }
        ],
    },
)

ingest(
    spark,
    {
        "connection_name": connection_name,
        "objects": [
            {
                "table": {
                    "source_table": source_table,
                    "destination_catalog": DESTINATION_CATALOG,
                    "destination_schema": DESTINATION_SCHEMA,
                    "destination_table": "nested_only_history",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_2",
                        "track_history_column_list": ["document_hash"],
                    },
                }
            }
        ],
    },
)
