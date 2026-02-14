# Lakeflow Mixpanel Community Connector

This documentation provides setup instructions and reference information for the Mixpanel source connector.

The Lakeflow Mixpanel Connector allows you to extract analytics data from your Mixpanel project and load it into your data lake or warehouse. This connector supports both incremental and full refresh synchronization patterns for Mixpanel events, cohorts, and other analytics objects.

## Prerequisites

- Access to a Mixpanel project with API permissions
- Mixpanel service account credentials or API secret
- Appropriate read permissions for the data you want to access
- Network access to reach Mixpanel API endpoints (US: `data.mixpanel.com` or EU: `data-eu.mixpanel.com`)

## Setup

### Required Connection Parameters

Provide the following connection parameters when configuring the connector:

**Option 1: Service Account Authentication (Recommended)**

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `username` | string | Yes | Mixpanel service account username | `your-service-account` |
| `secret` | string | Yes | Mixpanel service account secret | `your-service-secret` |
| `project_id` | integer | No | Mixpanel project ID (optional for service accounts) | `12345` |
| `region` | string | No | Mixpanel data region (US or EU), defaults to `US` | `EU` |
| `project_timezone` | string | No | Project timezone for date calculations, defaults to `US/Pacific` | `UTC` |
| `historical_days` | integer | No | Days of historical data to fetch in initial snapshot, defaults to `10` | `30` |

**Option 2: API Secret Authentication**

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_secret` | string | Yes | Mixpanel project API secret | `your-api-secret-token` |
| `project_id` | integer | No | Mixpanel project ID (optional) | `12345` |
| `region` | string | No | Mixpanel data region (US or EU), defaults to `US` | `EU` |
| `project_timezone` | string | No | Project timezone for date calculations, defaults to `US/Pacific` | `UTC` |
| `historical_days` | integer | No | Days of historical data to fetch in initial snapshot, defaults to `10` | `30` |

> **Note**: All configuration for this connector is done via connection parameters. This connector does not support table-specific options (like `owner` or `repo` in GitHub). Therefore, `externalOptionsAllowList` does **not** need to be included as a connection parameter.

### Obtaining the Required Parameters

**Service Account Method (Recommended)**

1. Log in to your Mixpanel account
2. Navigate to **Settings → Organization Settings → Service Accounts**
3. Click **"Add Service Account"**
4. Configure permissions and generate credentials
5. Save the username and secret securely
6. (Optional) Note your project ID from **Settings → Project Settings**

**API Secret Method**

1. Log in to your Mixpanel project
2. Navigate to **Settings → Project Settings**
3. Find the **"API Secret"** section
4. Copy your API secret (create one if it doesn't exist)
5. Store the secret securely
6. (Optional) Note your project ID from the same settings page

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one

> **Important**: This connector does not require table-specific options (like `owner` or `repo` in GitHub). All configuration is done at the connection level. Do **not** set `externalOptionsAllowList` when creating this connection.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Mixpanel connector supports the following objects with their respective schemas, primary keys, and incremental strategies:

### Events
- **Primary Keys**: `["$insert_id"]`
- **Ingestion Type**: `cdc` (Change Data Capture)
- **Incremental Cursor**: `properties.time`
- **Schema**: Event data with nested `properties` structure. The `$insert_id` field is at the top level for primary key purposes. Standard Mixpanel properties are defined as typed fields within the properties struct, and all custom/application-specific event properties are captured in `properties.custom_properties` map
- **Key Fields**: `event`, `$insert_id`, `properties.distinct_id`, `properties.time`, `properties.$browser`, `properties.$city`, `properties.$os`, `properties.custom_properties`
- **Description**: All events tracked in your Mixpanel project with their associated properties. The `$insert_id` is extracted to the top level to serve as the primary key. Standard Mixpanel SDK properties are available as typed fields, while custom properties are preserved in the `properties.custom_properties` map to ensure no data loss

### Cohorts
- **Primary Keys**: `["id"]`
- **Ingestion Type**: `snapshot` (Full Refresh)
- **Incremental Cursor**: None
- **Schema**: User cohorts defined in Mixpanel including metadata and configuration
- **Key Fields**: `id`, `name`, `description`, `count`, `is_visible`, `is_dynamic`, `created`, `project_id`
- **Description**: User segments and cohorts created in Mixpanel for behavioral analysis (full refresh each sync)

### Cohort Members
- **Primary Keys**: `["cohort_id", "distinct_id"]`
- **Ingestion Type**: `snapshot`
- **Incremental Cursor**: None (full refresh)
- **Schema**: Relationship table showing which users belong to which cohorts
- **Key Fields**: `cohort_id`, `distinct_id`
- **Description**: Many-to-many relationship between cohorts and users, enabling cohort membership analysis

### Engage (User Profiles)
- **Primary Keys**: `["$distinct_id"]`
- **Ingestion Type**: `cdc` (Change Data Capture)
- **Incremental Cursor**: `$properties.$last_seen`
- **Schema**: User profiles with nested `$properties` structure. Standard Mixpanel people properties are defined as typed fields within the $properties struct, and all custom profile properties are captured in `$properties.custom_properties` map
- **Key Fields**: `$distinct_id`, `$properties.$first_name`, `$properties.$last_name`, `$properties.$email`, `$properties.$last_seen`, `$properties.custom_properties`
- **Description**: Individual user profiles with demographic and behavioral data. Standard Mixpanel people properties are available as typed fields, while custom profile properties are preserved in the `custom_properties` map to ensure no data loss

## Data Type Mapping

The Mixpanel connector maps source data types to Databricks data types as follows:

| Mixpanel Type | Databricks Type | Notes |
|---------------|-----------------|-------|
| String | STRING | Event names, user IDs, text properties |
| Integer | BIGINT | Timestamps (Unix epoch milliseconds), counts, IDs |
| Float | DOUBLE | Numeric values with decimals (e.g., revenue amounts) |
| Boolean | BOOLEAN | True/false flags (e.g., `is_visible`, `is_dynamic`) |
| Unix Timestamp | BIGINT | Event times stored as Unix epoch milliseconds |
| ISO 8601 DateTime | STRING | Cohort creation dates and other timestamps |
| Object/JSON | MAP<STRING, STRING> | Complex nested objects (e.g., user properties) |
| Array | ARRAY | Lists of values preserved as arrays |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the **Lakeflow Community Connector** UI flow, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Specify which Mixpanel objects to ingest. Example configuration:

```json
{
  "pipeline_spec": {
      "connection_name": "my_mixpanel_connection",
      "object": [
        {
            "table": {
                "source_table": "events"
            }
        },
        {
            "table": {
                "source_table": "cohorts"
            }
        },
        {
            "table": {
                "source_table": "engage"
            }
        }
      ]
  }
}
```

> **Note**: This connector does not support table-specific options. All tables use the connection-level configuration (e.g., `region`, `historical_days`) that was set when the connection was created.

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow). For incremental tables:

- On the **first run**, the connector will fetch historical data based on the `historical_days` parameter (default: 10 days)
- On **subsequent runs**, the connector uses stored cursors to incrementally sync only new or updated records

#### Best Practices

- **Start Small**: Begin by syncing the `events` table with a limited `historical_days` value to test your pipeline
- **Monitor API Limits**: Mixpanel enforces rate limits (3 requests per second for the export API). The connector includes automatic rate limiting.
- **Use Incremental Sync**: Reduces API calls and improves performance for subsequent runs
- **Set Appropriate Schedules**: Balance data freshness requirements with API usage limits
- **Adjust Historical Window**: For initial loads, consider reducing `historical_days` to avoid long-running API calls
- **Test Thoroughly**: Validate data accuracy and completeness after initial setup
- **Region Configuration**: Ensure the `region` parameter matches your Mixpanel project location (US or EU)

#### Troubleshooting

**Common Issues:**

- **Authentication Errors (`401 Unauthorized`)**:
  - Verify API credentials are correct and not expired
  - Check that service account or API secret has appropriate read permissions
  - For service accounts, ensure the username and secret are both provided
  - For API secrets, verify the token format is correct
- **Rate Limiting (`429 Too Many Requests`)**:
  - The connector includes automatic rate limiting (waits between requests to stay under 3 req/sec)
  - If rate limiting occurs, reduce sync frequency or schedule syncs during off-peak hours
  - The connector fetches events in 7-day batches to manage API load
- **Missing Data**:
  - Check that events exist in the specified time period
  - Verify user permissions and API access levels
  - Ensure the `historical_days` parameter covers your desired data range
- **Region Issues (`403 Forbidden` or authentication failures)**:
  - Verify the `region` parameter is set correctly (US or EU) for your Mixpanel project
  - US projects: `https://data.mixpanel.com` (default)
  - EU projects: `https://data-eu.mixpanel.com` (set `region: "EU"`)
- **Project ID Issues**:
  - For API secret authentication, do NOT include `project_id` (it's automatically derived from the secret)
  - For service account authentication, `project_id` is optional but recommended

**Error Handling:**

The connector includes built-in error handling for common scenarios:
- **Rate limiting**: Automatically waits between requests and returns partial data if rate limited
- **Network issues**: Includes retry logic and timeout handling
- **API errors**: Logs detailed error information for debugging
- **Date parsing**: Supports multiple datetime formats for flexibility

Check the pipeline logs for detailed error information and recommended actions.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/mixpanel/mixpanel.py`
- Official Mixpanel API documentation:
  - [Data Export API](https://developer.mixpanel.com/reference/raw-event-export)
  - [Cohorts API](https://developer.mixpanel.com/reference/cohorts)
  - [Authentication](https://developer.mixpanel.com/reference/authentication)
  - [Rate Limits](https://developer.mixpanel.com/reference/rate-limits)
- Regional endpoints:
  - US: `https://data.mixpanel.com`
  - EU: `https://data-eu.mixpanel.com`