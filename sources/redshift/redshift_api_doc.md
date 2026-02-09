# **Amazon Redshift Data API Documentation**

## **Authorization**

- **Chosen method**: AWS IAM credentials with AWS Signature Version 4 authentication for the Redshift Data API.
- **Base URL**: `https://redshift-data.{region}.amazonaws.com` (regional endpoints)
- **Auth placement**:
  - HTTP header: Uses AWS Signature V4 signing process with `access_key_id` and `secret_access_key`
  - Alternatively: IAM role-based authentication when running on AWS infrastructure
- **Required IAM permissions**:
  - `redshift-data:ExecuteStatement` - Execute SQL statements
  - `redshift-data:DescribeStatement` - Check statement status
  - `redshift-data:GetStatementResult` - Retrieve query results
  - `redshift-data:ListTables` - List tables (optional)
  - `redshift-data:DescribeTable` - Describe table schema (optional)
  - `redshift:GetClusterCredentials` - For temporary database credentials (provisioned clusters)
  - `redshift-serverless:GetCredentials` - For serverless workgroups
- **Other supported methods (not used by this connector)**:
  - AWS STS temporary credentials
  - AWS SSO credentials

Example authenticated request using boto3:

```python
import boto3

client = boto3.client(
    'redshift-data',
    region_name='us-east-1',
    aws_access_key_id='<ACCESS_KEY_ID>',
    aws_secret_access_key='<SECRET_ACCESS_KEY>'
)

response = client.execute_statement(
    ClusterIdentifier='my-cluster',
    Database='mydb',
    DbUser='admin',
    Sql='SELECT * FROM mytable LIMIT 10'
)
```

Notes:
- Rate limiting: 200 requests per second per Redshift cluster/workgroup
- Statement execution is asynchronous; requires polling for completion
- Results are paginated with maximum 1000 rows per page

## **Object List**

For connector purposes, Redshift **objects/tables** are dynamically discovered from the database.
The object list is **dynamic** (retrieved via API or SQL queries), not static.

### Table Discovery Approaches

1. **Using Redshift Data API**:
   - `ListTables` - Lists tables in specified database and schema
   - `ListSchemas` - Lists available schemas in database

2. **Using SQL Queries**:
   - Query `information_schema.tables` for comprehensive table metadata
   - Supports filtering by schema, table type (TABLE vs VIEW)

### Table Types Supported

| Object Type | Description | Ingestion Support |
|------------|-------------|-------------------|
| `TABLE` | Regular Redshift tables | Full support |
| `VIEW` | Database views | Full support (read-only) |
| `MATERIALIZED VIEW` | Materialized views | Full support (read-only) |
| `EXTERNAL TABLE` | Redshift Spectrum external tables | Full support (read-only) |

### Schema Filtering

The connector supports filtering tables by schema name(s). Common schemas:
- `public` - Default schema for user tables
- `pg_catalog` - System catalog (typically excluded)
- `information_schema` - Metadata schema (typically excluded)
- Custom user-defined schemas

Example SQL query for table listing:

```sql
SELECT 
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
    AND table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_schema, table_name;
```

## **Object Schema**

### Schema Discovery Methods

1. **Using Redshift Data API**:
   - `DescribeTable` - Returns column metadata for a specific table

2. **Using SQL Queries**:
   - Query `information_schema.columns` for detailed column information
   - Query `pg_table_def` for Redshift-specific metadata

### Column Metadata Retrieved

For each column, the connector retrieves:

| Metadata Field | Description | Source |
|---------------|-------------|--------|
| `column_name` | Column name | `information_schema.columns.column_name` |
| `data_type` | Redshift data type | `information_schema.columns.data_type` |
| `is_nullable` | Whether column allows NULL | `information_schema.columns.is_nullable` |
| `column_default` | Default value expression | `information_schema.columns.column_default` |
| `character_maximum_length` | Max length for string types | `information_schema.columns.character_maximum_length` |
| `numeric_precision` | Precision for numeric types | `information_schema.columns.numeric_precision` |
| `numeric_scale` | Scale for numeric types | `information_schema.columns.numeric_scale` |
| `ordinal_position` | Column order in table | `information_schema.columns.ordinal_position` |

Example SQL query for schema retrieval:

```sql
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    ordinal_position
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name = 'mytable'
ORDER BY ordinal_position;
```

Example response:

```
column_name    | data_type | is_nullable | numeric_precision | numeric_scale
---------------|-----------|-------------|-------------------|---------------
id             | integer   | NO          | 32                | 0
name           | varchar   | YES         | NULL              | NULL
created_at     | timestamp | NO          | NULL              | NULL
amount         | numeric   | YES         | 18                | 2
```

## **Get Object Primary Keys**

### Primary Key Discovery Methods

Redshift stores primary key constraints in system catalogs. The connector retrieves them via SQL queries.

**SQL Query for Primary Keys**:

```sql
SELECT 
    kcu.column_name,
    kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
    AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = 'public'
    AND tc.table_name = 'mytable'
ORDER BY kcu.ordinal_position;
```

**Alternative using pg_catalog**:

```sql
SELECT 
    a.attname AS column_name
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
WHERE c.contype = 'p'
    AND n.nspname = 'public'
    AND t.relname = 'mytable'
ORDER BY array_position(c.conkey, a.attnum);
```

**Handling Tables Without Primary Keys**:
- If no primary key is defined, the connector can:
  - Use all columns as a composite key (for snapshot ingestion)
  - Or mark the table as having no primary key
  - User can override via `table_configuration.primary_keys` in pipeline spec

## **Object's ingestion type**

All Redshift tables are ingested with type `snapshot`:
- **`snapshot`**: Full table snapshot on each sync; no incremental read capability via Data API

**Rationale**:
- Redshift Data API does not support CDC (change data capture)
- No built-in change tracking or update timestamps
- Full table reads are the standard pattern for data warehouse ingestion
- Users can implement incremental patterns by:
  - Adding `updated_at` or similar timestamp columns to their tables
  - Using table_options to specify `WHERE` clause filters (future enhancement)
  - Leveraging Databricks Delta Lake's merge capabilities for efficient upserts

**Future Enhancements** (out of scope for initial implementation):
- Support for incremental reads using user-specified cursor columns
- Support for append-only ingestion with timestamp filters
- Integration with Redshift change tracking features if available

## **Read API for Data Retrieval**

### Primary Read Pattern: Async SQL Execution

The Redshift Data API uses an asynchronous execution model:

1. **Submit query** → `ExecuteStatement` → returns `statement_id`
2. **Poll for completion** → `DescribeStatement` → check `Status`
3. **Retrieve results** → `GetStatementResult` → paginate with `NextToken`

### ExecuteStatement API

**HTTP method**: POST
**Endpoint**: `/`
**Action**: `RedshiftData.ExecuteStatement`

**Required parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `Database` | string | yes | Database name |
| `Sql` | string | yes | SQL statement to execute |
| `ClusterIdentifier` | string | conditional | Cluster ID (required for provisioned) |
| `WorkgroupName` | string | conditional | Workgroup name (required for serverless) |
| `DbUser` | string | conditional | Database user (required for provisioned without IAM) |
| `SecretArn` | string | no | ARN of secret for credentials |
| `StatementName` | string | no | Optional statement name for tracking |
| `WithEvent` | boolean | no | Enable EventBridge notification |

**Response**:

```json
{
    "Id": "d7c78c14-3ef4-4891-8e3f-example",
    "ClusterIdentifier": "my-cluster",
    "Database": "mydb",
    "CreatedAt": 1234567890.123
}
```

### DescribeStatement API

**HTTP method**: POST
**Endpoint**: `/`
**Action**: `RedshiftData.DescribeStatement`

**Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `Id` | string | yes | Statement ID from ExecuteStatement |

**Response**:

```json
{
    "Id": "d7c78c14-3ef4-4891-8e3f-example",
    "Status": "FINISHED",
    "Duration": 123456789,
    "ResultRows": 1000,
    "ResultSize": 50000,
    "QueryString": "SELECT * FROM mytable",
    "CreatedAt": 1234567890.123,
    "UpdatedAt": 1234567890.456
}
```

**Status values**:
- `SUBMITTED` - Query submitted but not yet running
- `PICKED` - Query picked up for execution
- `STARTED` - Query execution started
- `FINISHED` - Query completed successfully
- `FAILED` - Query failed with error
- `ABORTED` - Query was cancelled

### GetStatementResult API

**HTTP method**: POST
**Endpoint**: `/`
**Action**: `RedshiftData.GetStatementResult`

**Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `Id` | string | yes | Statement ID |
| `NextToken` | string | no | Pagination token for next page |

**Response structure**:

```json
{
    "Records": [
        [
            {"longValue": 1},
            {"stringValue": "John"},
            {"stringValue": "2024-01-01 10:00:00"}
        ],
        [
            {"longValue": 2},
            {"stringValue": "Jane"},
            {"stringValue": "2024-01-01 11:00:00"}
        ]
    ],
    "ColumnMetadata": [
        {
            "name": "id",
            "typeName": "int4",
            "nullable": 0,
            "precision": 10,
            "scale": 0
        },
        {
            "name": "name",
            "typeName": "varchar",
            "nullable": 1,
            "length": 255
        },
        {
            "name": "created_at",
            "typeName": "timestamp",
            "nullable": 0
        }
    ],
    "TotalNumRows": 2,
    "NextToken": "next-page-token-if-more-results"
}
```

**Field value types**:
- `blobValue` - Binary data (base64 encoded)
- `booleanValue` - Boolean
- `doubleValue` - Double precision float
- `isNull` - Null indicator
- `longValue` - Long integer
- `stringValue` - String

**Pagination**:
- Maximum 1000 rows per page
- Use `NextToken` to retrieve subsequent pages
- Continue until `NextToken` is absent in response

### Example: Complete Read Flow

```python
import boto3
import time

client = boto3.client('redshift-data', region_name='us-east-1')

# Step 1: Submit query
response = client.execute_statement(
    ClusterIdentifier='my-cluster',
    Database='mydb',
    DbUser='admin',
    Sql='SELECT * FROM public.users'
)
statement_id = response['Id']

# Step 2: Poll for completion
while True:
    status = client.describe_statement(Id=statement_id)
    if status['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
        break
    time.sleep(1)

if status['Status'] == 'FAILED':
    raise Exception(f"Query failed: {status.get('Error', 'Unknown error')}")

# Step 3: Retrieve results with pagination
records = []
next_token = None

while True:
    params = {'Id': statement_id}
    if next_token:
        params['NextToken'] = next_token
    
    result = client.get_statement_result(**params)
    records.extend(result['Records'])
    
    next_token = result.get('NextToken')
    if not next_token:
        break
```

### Rate Limits and Considerations

- **Query execution limit**: 200 concurrent queries per cluster/workgroup
- **API rate limit**: 200 requests per second per account per region
- **Result retention**: Query results retained for 24 hours after completion
- **Query timeout**: Maximum 24 hours execution time
- **Result size limit**: Maximum 100 MB per result set (consider pagination)

### Optimization Strategies

1. **Batch queries when possible**: Minimize API calls by fetching larger result sets
2. **Use appropriate polling intervals**: Start with 1-2 seconds, increase for long queries
3. **Leverage connection pooling**: Reuse Redshift Data API client instances
4. **Implement exponential backoff**: Handle rate limiting gracefully
5. **Use column projection**: SELECT only needed columns to reduce result size

## **Field Type Mapping**

### Redshift Data Types to Connector Logical Types

| Redshift Type | TypeName in API | Connector Logical Type | Spark Type | Notes |
|--------------|----------------|----------------------|-----------|-------|
| SMALLINT | int2 | integer | IntegerType | 16-bit integer |
| INTEGER, INT | int4 | integer | IntegerType | 32-bit integer |
| BIGINT | int8 | long | LongType | 64-bit integer |
| DECIMAL, NUMERIC | numeric | decimal | DecimalType(precision, scale) | Configurable precision/scale |
| REAL, FLOAT4 | float4 | float | FloatType | 32-bit floating point |
| DOUBLE PRECISION, FLOAT8, FLOAT | float8 | double | DoubleType | 64-bit floating point |
| BOOLEAN, BOOL | bool | boolean | BooleanType | True/false |
| CHAR, CHARACTER | bpchar | string | StringType | Fixed-length, blank-padded |
| VARCHAR, CHARACTER VARYING | varchar | string | StringType | Variable-length |
| TEXT | text | string | StringType | Variable unlimited length |
| DATE | date | date | DateType | Date without time |
| TIMESTAMP, TIMESTAMP WITHOUT TIME ZONE | timestamp | timestamp | TimestampType | Timestamp without timezone |
| TIMESTAMPTZ, TIMESTAMP WITH TIME ZONE | timestamptz | timestamp | TimestampType | Timestamp with timezone, stored as UTC |
| TIME, TIME WITHOUT TIME ZONE | time | string | StringType | Time without timezone |
| TIMETZ, TIME WITH TIME ZONE | timetz | string | StringType | Time with timezone |
| INTERVAL | interval | string | StringType | Time interval |
| GEOMETRY | geometry | string | StringType | Spatial geometry type |
| GEOGRAPHY | geography | string | StringType | Spatial geography type |
| HLLSKETCH | hllsketch | string | StringType | HyperLogLog sketch |
| SUPER | super | string | StringType | Semi-structured JSON-like data |
| VARBYTE | varbyte | binary | BinaryType | Variable-length binary |

### Special Type Handling

**NUMERIC/DECIMAL precision and scale**:
- Default precision: 18
- Default scale: 0
- Extract from `information_schema.columns`: `numeric_precision`, `numeric_scale`
- Create `DecimalType(precision, scale)` accordingly

**TIMESTAMP types**:
- Both `timestamp` and `timestamptz` map to `TimestampType`
- Redshift stores `timestamptz` in UTC internally
- Connector preserves timezone information when available

**TIME types**:
- Mapped to `StringType` since Spark's `TimestampType` includes date
- Format: `HH:mm:ss` or `HH:mm:ss+TZ`

**SUPER type**:
- Redshift's semi-structured JSON-like type
- Connector treats as `StringType` (JSON string)
- Downstream parsing can be done in Spark using JSON functions

**NULL handling**:
- API returns `{"isNull": true}` for NULL values
- Connector should properly handle and propagate NULLs

### Value Extraction from API Response

The `GetStatementResult` API returns values in typed fields:

```python
def extract_value(field_data):
    """Extract typed value from API field response"""
    if field_data.get('isNull'):
        return None
    elif 'booleanValue' in field_data:
        return field_data['booleanValue']
    elif 'longValue' in field_data:
        return field_data['longValue']
    elif 'doubleValue' in field_data:
        return field_data['doubleValue']
    elif 'stringValue' in field_data:
        return field_data['stringValue']
    elif 'blobValue' in field_data:
        return field_data['blobValue']  # base64 encoded
    else:
        return None
```

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://docs.aws.amazon.com/redshift-data/latest/APIReference/Welcome.html | 2025-01-08 | High | Redshift Data API overview and capabilities |
| Official Docs | https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_ExecuteStatement.html | 2025-01-08 | High | ExecuteStatement API parameters and response |
| Official Docs | https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_DescribeStatement.html | 2025-01-08 | High | Statement status polling and completion detection |
| Official Docs | https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_GetStatementResult.html | 2025-01-08 | High | Result retrieval and pagination with NextToken |
| Official Docs | https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html | 2025-01-08 | High | Complete list of Redshift data types |
| Official Docs | https://docs.aws.amazon.com/redshift/latest/dg/r_Data_types.html | 2025-01-08 | High | Data type details, precision, and scale |
| Official Docs | https://docs.aws.amazon.com/redshift/latest/dg/r_pg_table_def.html | 2025-01-08 | High | System catalog for table definitions |
| Official Docs | https://docs.aws.amazon.com/redshift/latest/dg/c_sysviews_for_information_schema.html | 2025-01-08 | High | information_schema views for metadata queries |
| OSS Implementation | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-redshift | 2025-01-08 | High | Airbyte Redshift connector patterns for JDBC-based access |
| OSS Implementation | https://github.com/singer-io/tap-redshift | 2025-01-08 | Medium | Singer tap implementation patterns |
| AWS Best Practices | https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html | 2025-01-08 | High | Data API best practices, rate limits, and optimization |

## **Sources and References**

- **Official AWS Redshift Data API documentation** (highest confidence)
  - `https://docs.aws.amazon.com/redshift-data/latest/APIReference/Welcome.html`
  - `https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_ExecuteStatement.html`
  - `https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_DescribeStatement.html`
  - `https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_GetStatementResult.html`
  - `https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_ListTables.html`
  - `https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_DescribeTable.html`
- **Official AWS Redshift documentation**
  - `https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html`
  - `https://docs.aws.amazon.com/redshift/latest/dg/r_Data_types.html`
  - `https://docs.aws.amazon.com/redshift/latest/dg/c_sysviews_for_information_schema.html`
  - `https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html`
- **OSS connector implementations** (high confidence for patterns)
  - `https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-redshift`
  - `https://github.com/singer-io/tap-redshift`

When conflicts arise, **official AWS documentation** is treated as the source of truth.
