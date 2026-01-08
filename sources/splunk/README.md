# Lakeflow Splunk Community Connector

This documentation provides setup instructions and reference information for the Splunk Metrics Catalog source connector.

The Lakeflow Splunk Connector allows you to extract metrics catalog data from your Splunk Enterprise or Splunk Cloud instance and load it into your data lake or warehouse. This connector retrieves metric names, dimensions, and rollup policies from the Splunk Metrics Catalog REST API.

## Prerequisites

- Access to a Splunk Enterprise or Splunk Cloud instance with the REST API enabled
- Splunk user account with appropriate permissions
- Required Splunk capabilities:
  - `list_metrics_catalog` - Required for reading metrics, dimensions, and rollup policies
- Network access to the Splunk management port (default: 8089)

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `host` | string | Yes | Splunk server hostname or IP address | `splunk.example.com` |
| `port` | integer | No | Splunk management port (default: 8089) | `8089` |
| `username` | string | Yes | Splunk username with API access | `admin` |
| `password` | string | Yes | Password for the Splunk user | `your-password` |
| `verify_ssl` | boolean | No | Whether to verify SSL certificates (default: false) | `false` |

### Getting Your Splunk Credentials

1. **Identify Your Splunk Server**
   - For Splunk Enterprise: Use your Splunk server's hostname or IP address
   - For Splunk Cloud: Use your cloud instance URL (e.g., `your-instance.splunkcloud.com`)

2. **Obtain User Credentials**
   - Log in to your Splunk instance as an admin
   - Navigate to **Settings** → **Users and Authentication** → **Users**
   - Create a new user or use an existing user with the required capabilities
   - Ensure the user has the `list_metrics_catalog` capability assigned through a role

3. **Verify API Access**
   - The Splunk REST API is typically available on port 8089
   - Test connectivity: `curl -k -u username:password https://your-splunk-host:8089/services/server/info`

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Navigate to the Unity Catalog UI and create a "Lakeflow Community Connector" connection

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The Splunk Metrics Catalog connector supports the following objects:

### metrics
- **Primary Key**: `metric_name`
- **Ingestion Strategy**: Snapshot (full refresh)
- **Description**: List of all metric names available in the Splunk metric store
- **Schema**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `metric_name` | STRING | No | Unique name of the metric |
| `index` | STRING | Yes | Index containing the metric |
| `dimensions` | ARRAY<STRUCT<name: STRING, value: STRING>> | Yes | Associated dimension name-value pairs |

### dimensions
- **Primary Key**: `dimension_name`
- **Ingestion Strategy**: Snapshot (full refresh)
- **Description**: List of all dimension names associated with metrics
- **Schema**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `dimension_name` | STRING | No | Unique name of the dimension |

### rollup_policies
- **Primary Key**: `index`
- **Ingestion Strategy**: Snapshot (full refresh)
- **Description**: Metric rollup policies and their configurations
- **Schema**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `index` | STRING | No | Source metric index name |
| `defaultAggregation` | STRING | Yes | Default aggregation functions (e.g., `avg#max`) |
| `metricList` | STRING | Yes | Comma-separated list of metric names |
| `metricListType` | STRING | Yes | `included` or `excluded` |
| `dimensionList` | STRING | Yes | Comma-separated list of dimensions |
| `dimensionListType` | STRING | Yes | `included` or `excluded` |
| `summaries` | ARRAY<STRUCT<rollupIndex: STRING, span: STRING>> | Yes | Rollup summary configurations |

**Note**: All objects use snapshot ingestion because the Splunk Metrics Catalog API does not provide change tracking (no `updated_at` or similar cursor fields). Each sync performs a full refresh of the catalog data.


## Data Type Mapping

The Splunk connector maps source data types to Databricks data types as follows:

| Splunk Type | Databricks Type | Notes |
|-------------|-----------------|-------|
| String | STRING | Text fields (metric names, dimension names, values) |
| Integer | BIGINT | Numeric identifiers, counts |
| Float/Double | DOUBLE | Metric values with decimal precision |
| Boolean | BOOLEAN | True/false flags |
| XML Dict/Object | STRUCT | Nested objects with known structure |
| Array/List | ARRAY | Collections (e.g., dimensions, summaries) |

**Special Field Behaviors:**
- **Aggregation functions**: Stored as `#`-separated strings (e.g., `avg#max#min`)
- **List fields**: Stored as comma-separated strings (e.g., `app,region`)
- **Nested summaries**: Array of structs with `span` and `rollupIndex` properties


## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a single object (e.g., `dimensions`) to test connectivity
- **Full Refresh Only**: This connector uses snapshot ingestion, so each run retrieves all data
- **Set Appropriate Schedules**: Since the Metrics Catalog is typically metadata that doesn't change frequently, daily or weekly syncs are usually sufficient
- **SSL Certificates**: For production, configure proper SSL certificate validation by setting `verify_ssl: true`
- **Monitor Performance**: Large metric catalogs may take longer to sync; consider scheduling during off-peak hours

#### Troubleshooting

**Common Issues:**

- **Authentication Errors (401)**
  - Verify username and password are correct
  - Ensure the user account is active and not locked
  - Check that the user has the required `list_metrics_catalog` capability

- **Access Forbidden (403)**
  - The user lacks required capabilities
  - Request your Splunk admin to assign the `list_metrics_catalog` capability to the user's role

- **Connection Errors**
  - Verify the Splunk host and port are correct
  - Ensure network connectivity to the Splunk management port (8089)
  - Check firewall rules allow access from your Databricks environment
  - For self-signed SSL certificates, set `verify_ssl: false`

- **Empty Results**
  - Verify that metrics data has been ingested into your Splunk instance
  - Check that the user has permissions to view the metric indexes

**Error Handling:**
The connector includes built-in error handling for common scenarios including authentication failures, network issues, and API errors. Check the pipeline logs for detailed error information and recommended actions.


## References

- [Splunk REST API Reference Manual](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF)
- [Metrics Catalog Endpoint Descriptions](https://help.splunk.com/en/splunk-enterprise/rest-api-reference/10.0/metrics-catalog-endpoints/metrics-catalog-endpoint-descriptions)
- [Splunk REST API Endpoints Reference List](https://docs.splunk.com/Documentation/Splunk/9.4.2/RESTREF/RESTlist)
- [Splunk REST API Metrics Reference](https://docs.splunk.com/Documentation/Splunk/9.4.2/RESTREF/RESTmetrics)
- [Metric Rollup Policies Documentation](https://docs.splunk.com/Documentation/Splunk/9.4.2/Metrics/MrollupAppContext)
