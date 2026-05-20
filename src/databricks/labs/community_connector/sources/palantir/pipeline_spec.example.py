"""
Palantir Foundry Connector - Pipeline Specification Examples

This file demonstrates how to configure and use the Palantir connector
in Databricks Lakeflow pipelines.
"""

from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# ============================================================================
# Example 1: Basic Snapshot Ingestion (Full Refresh)
# ============================================================================

def example_snapshot_ingestion():
    """
    Ingest ExampleFlight data in snapshot mode (full refresh).
    Suitable for tables without frequent updates.
    """

    source_name = "palantir"
    pipeline_spec = {
        "connection_name": "my_palantir_connection",  # Unity Catalog connection name
        "objects": [
            {
                "table": {
                    "source_table": "ExampleFlight",  # Palantir object type API name
                    "destination_catalog": "main",
                    "destination_schema": "palantir_data",
                    "destination_table": "flights",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_1",  # Overwrite on each sync
                        "page_size": "100"         # Records per API request
                    }
                }
            }
        ]
    }

    # Register and run
    register(spark, source_name)
    ingest(spark, pipeline_spec)


# ============================================================================
# Example 2: Incremental (CDC) Ingestion
# ============================================================================

def example_incremental_ingestion():
    """
    Ingest ExampleFlight data in incremental mode using a cursor field.
    Only syncs new/updated records since last run.
    """

    source_name = "palantir"
    pipeline_spec = {
        "connection_name": "my_palantir_connection",
        "objects": [
            {
                "table": {
                    "source_table": "ExampleFlight",
                    "destination_catalog": "main",
                    "destination_schema": "palantir_data",
                    "destination_table": "flights_incremental",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_2",           # Track history
                        "cursor_field": "departureTimestamp", # Field for tracking changes
                        "page_size": "100"
                    }
                }
            }
        ]
    }

    register(spark, source_name)
    ingest(spark, pipeline_spec)


# ============================================================================
# Example 3: Multiple Object Types (Batch Ingestion)
# ============================================================================

def example_multiple_objects():
    """
    Ingest multiple object types from the same Palantir ontology.
    Efficiently syncs related data in a single pipeline.
    """

    source_name = "palantir"
    pipeline_spec = {
        "connection_name": "my_palantir_connection",
        "objects": [
            # Flights (incremental)
            {
                "table": {
                    "source_table": "ExampleFlight",
                    "destination_catalog": "main",
                    "destination_schema": "palantir_data",
                    "destination_table": "flights",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_2",
                        "cursor_field": "departureTimestamp",
                        "page_size": "100"
                    }
                }
            },
            # Aircraft (snapshot)
            {
                "table": {
                    "source_table": "ExampleAircraft",
                    "destination_catalog": "main",
                    "destination_schema": "palantir_data",
                    "destination_table": "aircraft",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_1",  # Master data, overwrite
                        "page_size": "100"
                    }
                }
            },
            # Airports (snapshot)
            {
                "table": {
                    "source_table": "ExampleAirport",
                    "destination_catalog": "main",
                    "destination_schema": "palantir_data",
                    "destination_table": "airports",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_1",
                        "page_size": "100"
                    }
                }
            },
            # Route Alerts (append only)
            {
                "table": {
                    "source_table": "ExampleRouteAlert",
                    "destination_catalog": "main",
                    "destination_schema": "palantir_data",
                    "destination_table": "route_alerts",
                    "table_configuration": {
                        "scd_type": "APPEND_ONLY",  # Never update, only insert
                        "page_size": "50"
                    }
                }
            }
        ]
    }

    register(spark, source_name)
    ingest(spark, pipeline_spec)


# ============================================================================
# Example 4: Large Dataset with Custom Page Size
# ============================================================================

def example_large_dataset():
    """
    Optimize ingestion for large datasets by increasing page size.
    Larger page sizes = fewer API calls but longer individual requests.
    """

    source_name = "palantir"
    pipeline_spec = {
        "connection_name": "my_palantir_connection",
        "objects": [
            {
                "table": {
                    "source_table": "ExampleFlight",
                    "destination_catalog": "main",
                    "destination_schema": "palantir_data",
                    "destination_table": "flights_bulk",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_1",
                        "page_size": "1000",  # Max page size for faster ingestion
                        "cursor_field": "departureTimestamp"
                    }
                }
            }
        ]
    }

    register(spark, source_name)
    ingest(spark, pipeline_spec)


# ============================================================================
# Example 5: Running in Databricks Notebook
# ============================================================================

def example_notebook_usage():
    """
    Complete example for running in a Databricks notebook.
    """

    # This would be in a Databricks notebook
    # Databricks notebook source
    from pyspark.sql import SparkSession
    from databricks.labs.community_connector.pipeline import ingest
    from databricks.labs.community_connector import register

    # Get Spark session
    spark = SparkSession.builder.getOrCreate()

    # Pipeline configuration
    source_name = "palantir"
    pipeline_spec = {
        "connection_name": "my_palantir_connection",
        "objects": [
            {
                "table": {
                    "source_table": "ExampleFlight",
                    "destination_catalog": "main",
                    "destination_schema": "palantir_data",
                    "destination_table": "flights",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_2",
                        "cursor_field": "departureTimestamp",
                        "page_size": "100"
                    }
                }
            }
        ]
    }

    # Register connector
    register(spark, source_name)

    # Run ingestion
    print("Starting Palantir data ingestion...")
    ingest(spark, pipeline_spec)
    print("✓ Ingestion complete!")

    # Verify data
    df = spark.table("main.palantir_data.flights")
    print(f"Total records: {df.count()}")
    df.show(5)


# ============================================================================
# Example 6: Scheduled Job Configuration
# ============================================================================

def example_scheduled_job():
    """
    Example for setting up a scheduled Databricks job.

    Create a job using Databricks SDK:
    """

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    job = w.jobs.create(
        name="Palantir Daily Ingestion",
        tasks=[
            {
                "task_key": "ingest_palantir_flights",
                "notebook_task": {
                    "notebook_path": "/path/to/your/ingestion_notebook",
                },
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2
                }
            }
        ],
        schedule={
            "quartz_cron_expression": "0 0 * * * ?",  # Daily at midnight
            "timezone_id": "UTC"
        }
    )

    print(f"Created job: {job.job_id}")


# ============================================================================
# Configuration Reference
# ============================================================================

"""
TABLE CONFIGURATION OPTIONS:

scd_type (required):
    - SCD_TYPE_1: Overwrite (upsert based on primary key)
    - SCD_TYPE_2: Track history (new row for each change)
    - APPEND_ONLY: Insert only (never update)

cursor_field (optional):
    - Field name for incremental sync
    - Typically a timestamp field (e.g., "updatedAt", "departureTimestamp")
    - If not specified, uses snapshot mode (full refresh)

page_size (optional):
    - Number of records per API request
    - Default: 100
    - Maximum: 1000
    - Larger values = fewer API calls but longer requests

PRIMARY KEYS:
    - Automatically discovered from Palantir object type definition
    - Used for SCD_TYPE_1 upserts and SCD_TYPE_2 tracking


UNITY CATALOG CONNECTION:

Create connection via Databricks UI:
    1. Settings → Connections → Create Connection
    2. Select "Palantir Foundry"
    3. Enter:
       - Connection name: my_palantir_connection
       - Token: Your Palantir bearer token
       - Hostname: yourcompany.palantirfoundry.com
       - Ontology API Name: ontology-282f1207-...

Or via SDK:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    w.connections.create(
        name="my_palantir_connection",
        connection_type="palantir",
        options={
            "token": "your_bearer_token",
            "hostname": "yourcompany.palantirfoundry.com",
            "ontology_api_name": "ontology-282f1207-..."
        }
    )


BEST PRACTICES:

1. Start small - test with one object type first
2. Use incremental sync for large, frequently updated tables
3. Use snapshot mode for small reference tables
4. Monitor API rate limits (connector includes retry logic)
5. Schedule jobs during off-peak hours for large datasets
6. Use SCD_TYPE_2 when historical tracking is needed
7. Use APPEND_ONLY for immutable event data
"""
