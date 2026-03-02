# Lakeflow Example Community Connector

This documentation provides setup instructions and reference information for the Example source connector. The Example connector ingests data from a simulated REST API that exposes product catalogs, event logs, user directories, order ledgers, and time-series metrics.

## Prerequisites

- A username and password for the Example source system. Any non-empty string values are accepted.

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `username` | String | Yes | Username for authentication with the Example source | `example_user` |
| `password` | String | Yes | Password for authentication with the Example source | `example_password` |
| `externalOptionsAllowList` | String | Yes | Comma-separated list of table-specific options to pass through. Must be set to: `num_rows` | `num_rows` |

### Obtaining Credentials

The Example source accepts any non-empty username and password combination. No account provisioning or API key generation is required.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `num_rows` to enable per-table configuration of this option.

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The connector supports the following five objects:

| Object | Primary Key | Ingestion Mode | Cursor Field | Delete Sync |
|---|---|---|---|---|
| `products` | `product_id` | Snapshot | — | No |
| `events` | — | Append | `created_at` | No |
| `users` | `user_id` | CDC | `updated_at` | No |
| `orders` | `order_id` | CDC with deletes | `updated_at` | Yes |
| `metrics` | `metric_id` | CDC | `updated_at` | No |

### products

A product catalog. Each sync fetches the full set of records (snapshot mode). Supports an optional `category` filter to limit results to a specific product category. No primary-key–based deduplication is needed since every sync replaces the full dataset.

### events

An append-only event log. New records are timestamped with `created_at`. Uses limit-based incremental reads — each API call receives a configurable `limit` parameter so the server controls batch boundaries. Records are never updated or deleted. No primary keys are defined; each record is identified by its `event_id` but the ingestion mode is append-only.

### users

A mutable user directory. Records carry an `updated_at` date that advances on every change. Uses cursor-based incremental reads to fetch only records modified since the last sync. No deletes are supported.

### orders

A mutable order ledger with full lifecycle support. Records carry an `updated_at` timestamp. Uses cursor-based incremental reads with delete synchronization — deleted records are fetched from a dedicated endpoint and tombstoned with primary-key and cursor values populated. Supports optional `user_id` and `status` filters.

### metrics

A hidden time-series metrics table that is not discoverable via the API's table listing endpoint. Its schema is hard-coded in the connector. Uses sliding time-window incremental reads — data is queried in fixed-size windows using `since`/`until` parameters, with the window advancing after each batch. The `value` column is a nested struct containing `count`, `label`, and `measure` subfields.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Special `table_configuration` options

These are source-specific options set inside the `table_configuration` map:

| Option | Applicable Objects | Required | Description | Default |
|---|---|---|---|---|
| `max_records_per_batch` | All incremental objects (`events`, `users`, `orders`, `metrics`) | No | Maximum number of records to fetch per batch | `200` |
| `limit` | `events` | No | Number of records the server returns per API call; controls pagination granularity | `50` |
| `window_seconds` | `metrics` | No | Size of the sliding time window (in seconds) for each incremental read | `3600` |
| `category` | `products` | No | Filter products by category (e.g., `electronics`, `books`, `clothing`) | — |
| `user_id` | `orders` | No | Filter orders by a specific user ID | — |
| `status` | `orders` | No | Filter orders by status (e.g., `pending`, `shipped`, `delivered`) | — |


## Data Type Mapping

| Source Type | Spark SQL Type |
|---|---|
| `string` | `StringType` |
| `integer` | `LongType` |
| `double` | `DoubleType` |
| `timestamp` | `TimestampType` |
| `date` | `DateType` |
| `struct` | `StructType` (nested fields preserved) |


## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure each object with the appropriate table-specific options:

```json
{
  "pipeline_spec": {
      "connection_name": "my_example_connection",
      "object": [
        {
            "table": {
                "source_table": "orders",
                "table_configuration": {
                    "max_records_per_batch": "500",
                    "user_id": "user_0001",
                    "status": "shipped"
                }
            }
        },
        {
            "table": {
                "source_table": "products",
                "table_configuration": {
                    "category": "electronics"
                }
            }
        },
        {
            "table": {
                "source_table": "metrics",
                "table_configuration": {
                    "max_records_per_batch": "500",
                    "window_seconds": "7200"
                }
            }
        },
        {
            "table": {
                "source_table": "events",
                "table_configuration": {
                    "max_records_per_batch": "500",
                    "limit": "50"
                }
            }
        },
        {
            "table": {
                "source_table": "users",
                "table_configuration": {
                    "max_records_per_batch": "500"
                }
            }
        }
      ]
  }
}
```
3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline
- **Use Incremental Sync**: Reduces API calls and improves performance for `events`, `users`, `orders`, and `metrics`
- **Set Appropriate Schedules**: Balance data freshness requirements with API usage limits
- **Tune Batch Sizes**: Adjust `max_records_per_batch` and `window_seconds` based on your data volume. Larger batches reduce the number of pipeline runs but increase per-run processing time.
- **Retry Handling**: The connector automatically retries on HTTP 429, 500, and 503 errors with exponential backoff (up to 5 attempts). Transient errors are expected and handled transparently.

#### Troubleshooting

**Common Issues:**

- **Authentication errors**: Verify that both `username` and `password` are non-empty strings in your Unity Catalog connection.
- **Table not found**: Ensure `source_table` uses one of the exact supported object names: `products`, `events`, `users`, `orders`, or `metrics`.
- **No new data on incremental sync**: The connector caps its cursor at initialization time. If no records were modified since the last sync, the connector correctly returns no data. This is expected behavior.
- **Rate limiting (HTTP 429)**: The connector handles rate limiting automatically with exponential backoff. If errors persist after 5 retries, check the source system's health.


## References

- [Lakeflow Community Connectors Repository](https://github.com/databrickslabs/lakeflow-community-connectors)
