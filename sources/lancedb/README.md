# Lakeflow LanceDB Community Connector

This documentation provides setup instructions and reference information for the LanceDB source connector. This connector enables reading data from LanceDB Cloud instances into Databricks using the Lakeflow framework.

## Prerequisites

To use this connector, you need:

1. **LanceDB Cloud Account**: An active LanceDB Cloud account with a project/database instance
2. **API Key**: A valid API key with read permissions for your LanceDB project
3. **Project Information**: Your project instance name and cloud region
4. **Network Access**: Connectivity to LanceDB Cloud API endpoints (HTTPS)

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_key` | string | Yes | LanceDB Cloud API key for authentication | `"sk-abc123..."` |
| `project_name` | string | Yes | Your LanceDB project instance name | `"my-project-xyz"` |
| `region` | string | Yes | Cloud region where your project is hosted | `"us-east-1"` |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of allowed table-specific options | `"primary_keys,cursor_field,batch_size,filter_expression,columns"` |

**Table-Specific Options** (configured per table via `externalOptionsAllowList`):

The `externalOptionsAllowList` connection option is **required** and must include the following complete list of supported table-specific options:

- `primary_keys` - Comma-separated list of primary key column names (required for CDC mode)
- `cursor_field` - Column name for incremental reads (required for append/CDC modes)
- `batch_size` - Number of rows to fetch per API request (1-10000, default: 1000)
- `filter_expression` - SQL-like filter expression for querying data
- `columns` - Comma-separated list of specific columns to retrieve

### Obtaining LanceDB Credentials

1. **Sign up for LanceDB Cloud**:
   - Visit [LanceDB Cloud](https://accounts.lancedb.com) and complete the onboarding process

2. **Create a Project**:
   - Create a new project in the LanceDB Cloud dashboard
   - Note the project instance name (e.g., `embedding-yhs6bg`)
   - Note the region (e.g., `us-east-1`)

3. **Generate API Key**:
   - In your project dashboard, navigate to API Settings
   - Generate a new API key with read permissions
   - **Important**: Save the API key securely - it won't be shown again

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways:

#### Via UI:
1. Navigate to the "Add Data" page in Databricks
2. Follow the Lakeflow Community Connector UI flow
3. Select "LanceDB" as the source
4. Provide your `api_key`, `project_name`, and `region`
5. **Required**: Set `externalOptionsAllowList` to: `primary_keys,cursor_field,batch_size,filter_expression,columns`

#### Via API:
The connection can also be created using the standard Unity Catalog API with the above parameters.

## Supported Objects

### Dynamic Table Discovery

The LanceDB connector supports **all tables** present in your LanceDB Cloud project. Tables are discovered dynamically via the LanceDB API - you do not need to predefine which tables exist.

### Table Configuration

Each table can be configured with the following options:

#### Primary Keys
- **Purpose**: Define unique identifiers for records (required for CDC ingestion)
- **Configuration**: Specify via the `primary_keys` table option
- **Example**: `"primary_keys": ["id"]` or `"primary_keys": ["user_id", "timestamp"]`

#### Ingestion Types

The connector supports three ingestion strategies:

1. **Snapshot** (default):
   - Full table refresh on each sync
   - No incremental tracking required
   - Use when: Tables are small or full refresh is acceptable

2. **Append**:
   - Incremental append of new records only
   - Requires: `cursor_field` (e.g., `created_at`, `timestamp`)
   - Use when: Tables only grow (no updates/deletes)

3. **CDC (Change Data Capture)**:
   - Incremental upserts based on cursor and primary keys
   - Requires: Both `primary_keys` and `cursor_field` (e.g., `updated_at`)
   - Use when: Tables have updates that need to be captured
   - **Note**: Deletes are not automatically tracked unless a soft-delete field exists

#### Required Configuration by Ingestion Type

| Ingestion Type | Required Options | Optional Options |
|----------------|------------------|------------------|
| Snapshot | None | `batch_size`, `filter_expression`, `columns` |
| Append | `cursor_field` | `batch_size`, `filter_expression`, `columns` |
| CDC | `primary_keys`, `cursor_field` | `batch_size`, `filter_expression`, `columns` |

### Special Columns

- **Vector Embeddings**: LanceDB commonly stores vectors as `fixed_size_list<float32>[N]`. These are mapped to Spark `ArrayType(FloatType())`
- **Nested Structures**: Struct and list types are preserved without flattening
- **Timestamps**: Timestamp fields are automatically converted to Spark `TimestampType`

## Data Type Mapping

The connector maps LanceDB (Apache Arrow) types to Spark types as follows:

| LanceDB/Arrow Type | Spark Type | Notes |
|-------------------|------------|-------|
| `int8`, `int16`, `int32` | `IntegerType` | Small integers |
| `int64` | `LongType` | 64-bit integers (recommended for IDs) |
| `uint8`, `uint16`, `uint32`, `uint64` | `LongType` | Unsigned integers mapped to signed 64-bit |
| `float32` | `FloatType` | 32-bit floating point |
| `float64` | `DoubleType` | 64-bit floating point |
| `utf8`, `large_utf8` | `StringType` | Variable-length strings |
| `binary`, `large_binary` | `BinaryType` | Binary data |
| `bool` | `BooleanType` | Boolean values |
| `date32`, `date64` | `DateType` | Date values |
| `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]` | `TimestampType` | Timestamps with various precisions |
| `struct<...>` | `StructType` | Nested structures (not flattened) |
| `list<T>` | `ArrayType(T)` | Variable-length arrays |
| `fixed_size_list<T>[N]` | `ArrayType(T)` | Fixed-length arrays (vectors/embeddings) |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the LanceDB connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).

2. **Basic Configuration Example** (Snapshot mode):

```python
{
  "pipeline_spec": {
      "connection_name": "my_lancedb_connection",
      "object": [
        {
            "table": {
                "source_table": "documents",
                "batch_size": "1000"
            }
        }
      ]
  }
}
```

3. **Append Mode Configuration** (Incremental reads):

```python
{
  "pipeline_spec": {
      "connection_name": "my_lancedb_connection",
      "object": [
        {
            "table": {
                "source_table": "events",
                "cursor_field": "created_at",
                "batch_size": "1000"
            }
        }
      ]
  }
}
```

4. **CDC Mode Configuration** (Incremental with upserts):

```python
{
  "pipeline_spec": {
      "connection_name": "my_lancedb_connection",
      "object": [
        {
            "table": {
                "source_table": "users",
                "primary_keys": "id",
                "cursor_field": "updated_at",
                "batch_size": "1000"
            }
        }
      ]
  }
}
```

5. **Advanced Configuration** (With filtering and column selection):

```python
{
  "pipeline_spec": {
      "connection_name": "my_lancedb_connection",
      "object": [
        {
            "table": {
                "source_table": "logs",
                "cursor_field": "timestamp",
                "filter_expression": "severity = 'ERROR' AND timestamp > '2024-01-01'",
                "columns": "timestamp,message,severity,user_id",
                "batch_size": "500"
            }
        }
      ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of tables or use filtering to test your pipeline with limited data
- **Use Incremental Sync**: Configure `cursor_field` for append or CDC mode to reduce API calls and improve performance
- **Set Appropriate Batch Sizes**: 
  - Default (1000) works well for most cases
  - Increase to 5000-10000 for tables with small rows
  - Decrease to 100-500 for tables with large nested structures or vectors
- **Monitor API Usage**: LanceDB Cloud has rate limits based on your plan - implement appropriate scheduling
- **Handle Large Embeddings**: Tables with large vector embeddings may require smaller batch sizes to avoid memory issues
- **Use Column Selection**: For tables with many columns, use the `columns` option to retrieve only needed fields
- **Filter Data**: Use `filter_expression` to reduce data transfer and processing time

#### Troubleshooting

**Common Issues:**

1. **Authentication Errors**:
   - Error: `401 Unauthorized` or `403 Forbidden`
   - Solution: Verify your API key is correct and has read permissions
   - Check: Ensure `project_name` and `region` match your LanceDB Cloud project

2. **Rate Limiting**:
   - Error: `429 Too Many Requests`
   - Solution: The connector implements automatic retry with exponential backoff
   - Prevention: Reduce sync frequency or decrease batch sizes
   - Check: Review your LanceDB Cloud plan limits

3. **Table Not Found**:
   - Error: `Table 'X' not found`
   - Solution: Verify the table exists in your LanceDB project
   - Use: `list_tables()` method to see available tables

4. **Schema Mismatch**:
   - Error: Type conversion errors
   - Solution: Check that your table schema is compatible with Spark types
   - Note: Unsupported Arrow types will default to `StringType`

5. **Connection Timeouts**:
   - Error: Connection timeout or request timeout
   - Solution: Check network connectivity to LanceDB Cloud
   - Firewall: Ensure HTTPS access to `*.api.lancedb.com` is allowed

6. **Memory Issues**:
   - Error: Out of memory during data fetch
   - Solution: Reduce `batch_size` parameter
   - Note: Large vector embeddings consume significant memory

7. **SQL Injection Validation Errors**:
   - Error: `Invalid column name` or `Invalid identifier`
   - Solution: The connector validates all identifiers to prevent injection attacks
   - Check: Ensure table names, column names, and identifiers contain only alphanumeric, underscore, and dot characters

## Security Features

The LanceDB connector implements enterprise-grade security:

- **SQL Injection Prevention**: All user-provided identifiers and filter expressions are validated
- **Secure Credential Handling**: API keys are never logged or exposed in error messages
- **Input Validation**: All parameters are validated using Pydantic models with strict type checking
- **Thread Safety**: Connector is safe for concurrent use with thread-safe HTTP session management
- **Rate Limit Handling**: Automatic exponential backoff prevents account suspension
- **Resource Cleanup**: Proper cleanup of connections and resources

## References

- [LanceDB Official Documentation](https://docs.lancedb.com/)
- [LanceDB Cloud REST API Reference](https://docs.lancedb.com/api-reference/rest/index)
- [LanceDB Python SDK](https://lancedb.github.io/lancedb/python/python/)
- [Apache Arrow Type System](https://arrow.apache.org/docs/python/api/datatypes.html)
- [Lakeflow Community Connectors](https://github.com/dey-abhishek/lakeflow-community-connectors)

