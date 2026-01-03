"""OSI PI (OSIPI) ingestion example as a *Python file* (not a notebook UI).

Use this file when you want to define an SDP/Lakeflow pipeline in source control as a .py file.

Notes
-----
- This uses the shared `pipeline.ingestion_pipeline.ingest(...)` helper, which is built on
  Spark Declarative Pipelines (`pyspark.pipelines`). It is intended to run in a Lakeflow/SDP
  context (e.g., a DLT pipeline configured to use a Python file).
- Credentials/options are expected to come from the Unity Catalog connection referenced by
  `connection_name` and/or from `table_configuration` options as supported by the connector.
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession

from libs.source_loader import get_register_function
from pipeline.ingestion_pipeline import ingest


def _spark() -> SparkSession:
    s = SparkSession.getActiveSession()
    if s is not None:
        return s
    return SparkSession.builder.getOrCreate()


def main() -> None:
    spark = _spark()

    # Connector id (directory under sources/)
    source_name = "osipi"

    # Unity Catalog connection name (must exist in the workspace)
    connection_name = os.environ.get("OSIPI_CONNECTION_NAME", "osipi_connection")

    # Destination (override via env if desired)
    dest_catalog = os.environ.get("OSIPI_DEST_CATALOG", "osipi")
    dest_schema = os.environ.get("OSIPI_DEST_SCHEMA", "bronzeosipi")

    # Define what to ingest. Keep this small for a runnable example.
    pipeline_spec = {
        "connection_name": connection_name,
        "objects": [
            {
                "table": {
                    "source_table": "pi_dataservers",
                    "destination_catalog": dest_catalog,
                    "destination_schema": dest_schema,
                }
            },
            {
                "table": {
                    "source_table": "pi_points",
                    "destination_catalog": dest_catalog,
                    "destination_schema": dest_schema,
                    # Example connector-specific options (optional)
                    "table_configuration": {
                        "maxCount": "10000",
                    },
                }
            },
        ],
    }

    # Register the connector (DataSource name is still "lakeflow_connect")
    get_register_function(source_name)(spark)

    # Build SDP assets for the tables in the spec
    ingest(spark, pipeline_spec)


if __name__ == "__main__":
    main()
