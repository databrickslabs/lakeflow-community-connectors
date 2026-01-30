# Amazon Redshift Community Connector

## Overview

The Amazon Redshift community connector enables ingestion of data from Amazon Redshift data warehouse clusters and serverless workgroups into Databricks using the AWS Redshift Data API.

This connector uses the Redshift Data API for asynchronous query execution, which provides:
- Serverless execution without managing connections
- Automatic handling of query results up to 100 MB
- IAM-based authentication and authorization
- Support for both provisioned clusters and serverless workgroups

## Features

- **Dynamic Table Discovery**: Automatically discovers all tables and views in your Redshift database
- **Schema Filtering**: Filter tables by schema name(s) to include only relevant datasets
- **Comprehensive Type Support**: Full support for all Redshift data types including SUPER (JSON)
- **Snapshot Ingestion**: Full table ingestion with automatic pagination for large datasets
- **Flexible Authentication**: Supports AWS IAM credentials, IAM roles, or AWS Secrets Manager

## Supported Redshift Versions

- Amazon Redshift provisioned clusters (all versions)
- Amazon Redshift Serverless

## Prerequisites

### AWS Permissions

The IAM user or role used by the connector requires the following permissions:

**For Redshift Data API**:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "redshift-data:ExecuteStatement",
                "redshift-data:DescribeStatement",
                "redshift-data:GetStatementResult",
                "redshift-data:ListTables",
                "redshift-data:DescribeTable"
            ],
            "Resource": "*"
        }
    ]
}
```

**For Provisioned Clusters** (if using IAM authentication):
```json
{
    "Effect": "Allow",
    "Action": [
        "redshift:GetClusterCredentials"
    ],
    "Resource": [
        "arn:aws:redshift:REGION:ACCOUNT_ID:cluster:CLUSTER_NAME",
        "arn:aws:redshift:REGION:ACCOUNT_ID:dbuser:CLUSTER_NAME/*"
    ]
}
```

**For Serverless Workgroups** (if using IAM authentication):
```json
{
    "Effect": "Allow",
    "Action": [
        "redshift-serverless:GetCredentials"
    ],
    "Resource": "arn:aws:redshift-serverless:REGION:ACCOUNT_ID:workgroup/WORKGROUP_ID"
}
```

### Network Access

- The connector uses the AWS Redshift Data API (HTTPS) and does not require direct network connectivity to your Redshift cluster
- Ensure the Databricks workspace can make HTTPS requests to AWS APIs in your region

## Authentication Best Practices

> ⚠️ **SECURITY NOTICE**
> 
> **For Production Use:**
> - ✅ **Use IAM Roles/Instance Profiles** - Attach IAM roles to your Databricks clusters via instance profiles. This eliminates the need for hardcoded credentials.
> - ✅ **Use AWS Secrets Manager** - Store database credentials in AWS Secrets Manager and reference them via `secret_arn`.
> - ❌ **DO NOT hardcode credentials** - Never commit AWS access keys or database passwords to version control.
> 
> **For Local Development/Testing Only:**
> - Use temporary AWS credentials (session tokens) that expire within hours
> - Use environment variables instead of config files: `export AWS_ACCESS_KEY_ID="..."` 
> - Always clean up config files containing credentials after testing
> 
> **Production Authentication Hierarchy (Most to Least Secure):**
> 1. **IAM Instance Profiles** (Databricks clusters) - No credentials needed ✅
> 2. **AWS Secrets Manager** - Credentials managed by AWS ✅
> 3. **IAM User Credentials** - Long-lived access keys ⚠️
> 4. **Temporary Credentials** - Short-lived session tokens ⚠️
> 
> See the [DATABRICKS_SETUP.md](./DATABRICKS_SETUP.md) guide for detailed instructions on setting up instance profiles.

## Connection Configuration

### Unity Catalog Connection Parameters

When creating a Unity Catalog connection for this connector, provide the following parameters:

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `region` | Yes | AWS region where your Redshift cluster/workgroup is located | `us-east-1` |
| `database` | Yes | Database name to connect to | `dev`, `prod` |
| `cluster_identifier` | Conditional* | Redshift cluster identifier (for provisioned clusters) | `my-redshift-cluster` |
| `workgroup_name` | Conditional* | Serverless workgroup name (for serverless) | `my-workgroup` |
| `access_key_id` | Conditional** | AWS access key ID | `AKIAIOSFODNN7EXAMPLE` | <!-- gitleaks:allow -->
| `secret_access_key` | Conditional** | AWS secret access key | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `session_token` | No | AWS session token (required for temporary credentials) | `IQoJb3JpZ2luX2VjE...` |
| `db_user` | No*** | Database user (for cluster authentication) | `admin` |
| `secret_arn` | No | ARN of AWS Secrets Manager secret containing credentials | `arn:aws:secretsmanager:...` |
| `schema_filter` | No | Comma-separated list of schemas to include | `public,analytics` |
| `poll_interval` | No | Seconds between query status polls (default: 2) | `2` |
| `max_poll_attempts` | No | Maximum polling attempts (default: 300) | `300` |

*Either `cluster_identifier` or `workgroup_name` is required (not both)

**Required if not using IAM role-based authentication

***Required for provisioned clusters if not using IAM authentication; not used for serverless

### Authentication Methods

#### Method 1: AWS Access Keys (Explicit Credentials)

Provide `access_key_id` and `secret_access_key` in the connection configuration.

**Provisioned Cluster Example**:
```json
{
    "region": "us-east-1",
    "database": "dev",
    "cluster_identifier": "my-cluster",
    "db_user": "admin",
    "access_key_id": "AKIAIOSFODNN7EXAMPLE", // gitleaks:allow
    "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" // gitleaks:allow
}
```

**Serverless Workgroup Example**:
```json
{
    "region": "us-east-1",
    "database": "dev",
    "workgroup_name": "my-workgroup",
    "access_key_id": "AKIAIOSFODNN7EXAMPLE", // gitleaks:allow
    "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" // gitleaks:allow
}
```

#### Method 2: IAM Role (Recommended for Databricks on AWS)

Attach an IAM role with the required permissions to your Databricks workspace. The connector will automatically use the role credentials.

```json
{
    "region": "us-east-1",
    "database": "dev",
    "cluster_identifier": "my-cluster",
    "db_user": "admin"
}
```

#### Method 3: AWS Secrets Manager

Store credentials in AWS Secrets Manager and reference the secret ARN:

```json
{
    "region": "us-east-1",
    "database": "dev",
    "cluster_identifier": "my-cluster",
    "secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-redshift-secret"
}
```

## Table Configuration

### Discovering Tables

The connector automatically discovers all tables in your database. By default, it excludes system schemas (`pg_catalog`, `information_schema`, `pg_internal`).

**Filter by Schema**:
```json
{
    "schema_filter": "public,analytics,reporting"
}
```

### Table Options

Configure individual tables in the pipeline spec with these options:

| Option | Description | Example |
|--------|-------------|---------|
| `limit` | Limit number of rows (useful for testing) | `"limit": "1000"` |
| `where_clause` | SQL WHERE clause to filter rows (future) | `"where_clause": "created_at > '2024-01-01'"` |

## Supported Data Types

The connector maps Redshift data types to Spark/Delta Lake types:

| Redshift Type | Spark Type | Notes |
|--------------|------------|-------|
| SMALLINT, INTEGER, BIGINT | LongType | All integer types use LongType to avoid overflow |
| DECIMAL, NUMERIC | DecimalType(p, s) | Preserves precision and scale |
| REAL, FLOAT4 | FloatType | 32-bit floating point |
| DOUBLE PRECISION, FLOAT8 | DoubleType | 64-bit floating point |
| BOOLEAN | BooleanType | True/false |
| CHAR, VARCHAR, TEXT | StringType | Character data |
| DATE | DateType | Date without time |
| TIMESTAMP | TimestampType | Timestamp without timezone |
| TIMESTAMPTZ | TimestampType | Timestamp with timezone (UTC) |
| TIME, TIMETZ | StringType | Time values |
| SUPER | StringType | JSON-like semi-structured data |
| GEOMETRY, GEOGRAPHY | StringType | Spatial types |
| VARBYTE | BinaryType | Binary data |

## Ingestion Patterns

### Snapshot Ingestion (Default)

The connector performs full table snapshots on each sync. This is the standard pattern for data warehouse ingestion.

```python
pipeline_spec = {
    "connection_name": "my_redshift_connection",
    "objects": [
        {"table": {"source_table": "public.users"}},
        {"table": {"source_table": "public.orders"}},
    ],
}
```

### Incremental Ingestion (User-Implemented)

While the connector reads full snapshots, you can implement incremental patterns by:

1. **Adding timestamp columns** to your Redshift tables (`updated_at`, `loaded_at`)
2. **Using table options** to filter by timestamp (future enhancement)
3. **Leveraging Delta Lake MERGE** for efficient upserts in Databricks

Example with MERGE in Databricks:

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Example Pipeline Specifications

### Basic Example

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "redshift"
pipeline_spec = {
    "connection_name": "my_redshift_connection",
    "default_destination_catalog": "main",
    "default_destination_schema": "analytics",
    "objects": [
        {
            "table": {
                "source_table": "public.users",
                "destination_table": "redshift_users"
            }
        },
        {
            "table": {
                "source_table": "public.orders",
                "destination_table": "redshift_orders"
            }
        },
    ],
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### Multiple Schemas Example

```python
pipeline_spec = {
    "connection_name": "my_redshift_connection",
    "default_destination_catalog": "main",
    "default_destination_schema": "warehouse",
    "objects": [
        {"table": {"source_table": "public.dim_customers"}},
        {"table": {"source_table": "public.dim_products"}},
        {"table": {"source_table": "sales.fact_orders"}},
        {"table": {"source_table": "sales.fact_order_items"}},
        {"table": {"source_table": "analytics.monthly_revenue"}},
    ],
}
```

### With SCD Type 2 Example

```python
pipeline_spec = {
    "connection_name": "my_redshift_connection",
    "default_destination_catalog": "main",
    "default_destination_schema": "warehouse",
    "objects": [
        {
            "table": {
                "source_table": "public.customers",
                "destination_table": "customers",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_2",
                    "primary_keys": ["customer_id"]
                }
            }
        },
    ],
}
```

## Performance Considerations

### Query Execution Time
- Redshift Data API supports queries up to 24 hours execution time
- The connector polls every 2 seconds by default (configurable with `poll_interval`)
- For very long queries, increase `max_poll_attempts` (default: 300 = 10 minutes)

### Result Set Size
- Maximum result set size: 100 MB per query
- The connector automatically handles pagination (1000 rows per page)
- For very large tables, consider adding `LIMIT` or filtering options

### Rate Limits
- 200 requests per second per account per region for Redshift Data API
- 200 concurrent queries per cluster/workgroup
- The connector respects these limits automatically

### Optimization Tips

1. **Filter at source**: Use schema filtering to reduce discovery overhead
2. **Partition large tables**: Break large tables into chunks using WHERE clauses (when available)
3. **Run during off-peak hours**: Schedule ingestion when Redshift is less busy
4. **Use appropriate instance sizes**: Ensure Databricks cluster can handle result processing
5. **Monitor WLM queues**: Check Redshift workload management for query queueing

## Troubleshooting

### Common Issues

**Error: "Either cluster_identifier or workgroup_name required"**
- Solution: Specify exactly one of these parameters in your connection configuration

**Error: "Failed to execute SQL statement: AccessDenied"**
- Solution: Verify IAM permissions include all required Redshift Data API actions

**Error: "Statement timed out after X polling attempts"**
- Solution: Increase `max_poll_attempts` parameter for long-running queries

**Error: "Table 'X' not found"**
- Solution: Verify table name format is "schema.table" and table exists in database

**Error: "Query failed: disk full"**
- Solution: Redshift cluster disk is full; clean up temporary tables or increase storage

### Debugging Tips

1. **Test connection**: Start with a simple table with few rows
2. **Check AWS CloudTrail**: Review Redshift Data API calls and errors
3. **Use Redshift Query Editor**: Verify SQL queries work directly in Redshift
4. **Monitor Redshift Console**: Check for active queries and cluster health
5. **Enable detailed logging**: Check Databricks cluster logs for detailed error messages

## Limitations

1. **Snapshot only**: The connector does not support incremental CDC (change data capture)
2. **No delete detection**: Deleted rows in Redshift are not detected
3. **Result set size**: Maximum 100 MB per query (use filtering for larger tables)
4. **No parallel execution**: Queries execute sequentially (Redshift Data API limitation)
5. **24-hour timeout**: Queries must complete within 24 hours

## Security Best Practices

1. **Use IAM roles** instead of access keys when possible
2. **Rotate credentials** regularly if using access keys
3. **Use Secrets Manager** for sensitive credentials
4. **Apply least privilege**: Grant only required permissions
5. **Enable encryption**: Ensure Redshift cluster has encryption enabled
6. **Use VPC endpoints**: Keep traffic within AWS network when possible
7. **Monitor access**: Use CloudTrail to audit Redshift Data API calls

## Support and Feedback

For issues, questions, or contributions:
- GitHub Issues: [lakeflow-community-connectors](https://github.com/databrickslabs/lakeflow-community-connectors/issues)
- Documentation: [Lakeflow Community Connectors](https://github.com/databrickslabs/lakeflow-community-connectors)

## License

This connector is licensed under the same license as the parent repository.
