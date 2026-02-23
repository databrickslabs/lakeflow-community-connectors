# Lakeflow AppsFlyer Community Connector

## Authors

- lina.ryoo@databricks.com

This documentation provides setup instructions and reference information for the AppsFlyer source connector.

## Prerequisites

- An AppsFlyer account with admin permissions
- API token for authentication (available in the AppsFlyer dashboard)
- App ID(s) for the applications you want to sync data from
- IP address added to AppsFlyer's IP allowlist (Settings → API Access → IP Allowlist)

## Local Development Setup

For local testing and development:

1. **Copy example configuration files**:
   ```bash
   cd tests/unit/sources/appsflyer/configs
   cp dev_config.json local_config.json
   cp dev_table_config.json local_table_config.json
   ```

2. **Edit `local_config.json`** with your actual credentials:
   ```json
   {
     "api_token": "YOUR_ACTUAL_API_TOKEN",
     "base_url": "https://hq1.appsflyer.com"
   }
   ```
   
   **Note**: The connector automatically waits 60 seconds between API requests to prevent WAF (Web Application Firewall) challenges.

3. **Edit `local_table_config.json`** with your app settings:
   ```json
   {
     "app_id": "com.your.app",
     "start_date": "2026-01-01",
     "lookback_hours": "6",
     "max_days_per_batch": "7"
   }
   ```

**Security Note**: `local_*.json` files are git-ignored to protect your credentials. Never commit files containing actual API tokens or app IDs.

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_token` | string | Yes | API token for AppsFlyer authentication. Obtain from AppsFlyer Dashboard → (User menu) → API Tokens | `abc123-def456-ghi789` |
| `base_url` | string | No | Base URL for the AppsFlyer API. Defaults to `https://hq1.appsflyer.com` (US region). Use `https://eu-west1.appsflyer.com` for EU region. | `https://hq1.appsflyer.com` |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of table-specific options. Must include: `app_id,start_date,lookback_hours,max_days_per_batch` | `app_id,start_date,lookback_hours,max_days_per_batch` |

**Note**: The connector automatically applies a 60-second delay between API requests to prevent rate limiting and WAF (Web Application Firewall) challenges.

**Note**: The `externalOptionsAllowList` is a required connection parameter that must include the following options: `app_id,start_date,lookback_hours,max_days_per_batch`. These options are used to configure data retrieval for individual tables.

### Obtaining Your API Token

1. Log in to your AppsFlyer dashboard
2. Click on your user profile menu (top right)
3. Select **"API Tokens"**
4. Copy the API token (it will be in UUID format)
5. Keep this token secure and do not share it publicly

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for AppsFlyer or create a new one
3. Ensure the `externalOptionsAllowList` parameter includes: `app_id,start_date,lookback_hours,max_days_per_batch`

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The AppsFlyer connector supports the following objects (tables):

### Event-Based Reports

These tables contain raw event data and support incremental synchronization using the `event_time` field as a cursor.

#### `installs_report`
- **Description**: Installation events with full attribution data
- **Primary Keys**: `appsflyer_id`, `event_time`
- **Cursor Field**: `event_time`
- **Ingestion Type**: CDC (Change Data Capture)
- **Required Options**: `app_id`
- **Optional Options**: `start_date`, `lookback_hours`, `max_days_per_batch`

Contains comprehensive attribution data including:
- Attribution details (media source, campaign, adset, ad)
- Device identifiers (AppsFlyer ID, IDFA, GAID, customer user ID)
- Geographic information (country, city, region, postal code)
- Device information (platform, model, OS version)
- Network details (carrier, WiFi status, IP address)
- Multi-touch attribution contributors

#### `in_app_events_report`
- **Description**: In-app events (purchases, registrations, custom events)
- **Primary Keys**: `appsflyer_id`, `event_time`, `event_name`
- **Cursor Field**: `event_time`
- **Ingestion Type**: CDC
- **Required Options**: `app_id`

Contains event-specific data including:
- Event name, value, and revenue
- Validated revenue for purchase events
- Content identifiers and types
- Attribution inherited from install event
- Device and geographic information

#### `uninstall_events_report`
- **Description**: App uninstall events
- **Primary Keys**: `appsflyer_id`, `event_time`
- **Cursor Field**: `event_time`
- **Ingestion Type**: CDC
- **Required Options**: `app_id`

#### `organic_installs_report`
- **Description**: Organic (non-attributed) installation events
- **Primary Keys**: `appsflyer_id`, `event_time`
- **Cursor Field**: `event_time`
- **Ingestion Type**: CDC
- **Required Options**: `app_id`

#### `organic_in_app_events_report`
- **Description**: In-app events from organic users
- **Primary Keys**: `appsflyer_id`, `event_time`, `event_name`
- **Cursor Field**: `event_time`
- **Ingestion Type**: CDC
- **Required Options**: `app_id`

#### `organic_uninstall_events_report`
- **Description**: Uninstall events from organic users
- **Primary Keys**: `appsflyer_id`, `event_time`
- **Cursor Field**: `event_time`
- **Ingestion Type**: CDC
- **Required Options**: `app_id`

### Metadata Tables

#### `apps`
- **Description**: List of apps associated with your AppsFlyer account
- **Primary Keys**: `app_id`
- **Ingestion Type**: Snapshot (full refresh)
- **Required Options**: None

Contains app metadata:
- App ID, name, platform
- Bundle ID / package name
- Time zone and currency settings
- App status (active, pending, archived)

## Data Type Mapping

| AppsFlyer Type | Databricks Type | Notes |
|----------------|-----------------|-------|
| string | STRING | UTF-8 text fields |
| ISO 8601 datetime | STRING | Timestamps in UTC (e.g., `2025-01-09T12:34:56`) |
| integer | LONG | 64-bit integers for all numeric IDs |
| decimal / float | DOUBLE | Revenue, cost, and metric values |
| boolean | BOOLEAN | True/false flags |
| null / empty | NULL | Missing values represented as NULL |

**Important Notes**:
- All timestamps are in UTC format
- Device identifiers (AppsFlyer ID, IDFA, GAID) are stored as strings
- Revenue and cost fields use DOUBLE type to preserve precision
- Empty strings from AppsFlyer are normalized to NULL values

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure table-specific options for each object you want to sync:

```json
{
  "pipeline_spec": {
    "connection_name": "my_appsflyer_connection",
    "objects": [
      {
        "table": {
          "source_table": "installs_report",
          "table_configuration": {
            "app_id": "id123456789",
            "start_date": "2025-01-01",
            "lookback_hours": "6",
            "max_days_per_batch": "7"
          }
        }
      },
      {
        "table": {
          "source_table": "in_app_events_report",
          "table_configuration": {
            "app_id": "id123456789",
            "start_date": "2025-01-01"
          }
        }
      },
      {
        "table": {
          "source_table": "apps"
        }
      }
    ]
  }
}
```

**Table-Specific Options**:

- **`app_id`** (required for event reports): AppsFlyer application identifier (e.g., `id123456789`)
- **`start_date`** (optional): Initial date to start syncing data from (format: `YYYY-MM-DD`). Defaults to 7 days ago if not specified.
- **`lookback_hours`** (optional): Number of hours to look back when syncing incrementally to capture late-arriving events. Default: `6` hours. Recommended range: 3-24 hours.
- **`max_days_per_batch`** (optional): Maximum number of days to fetch in a single batch. Default: `7` days. Adjust based on data volume.

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a single app and a subset of tables (e.g., `installs_report` and `apps`) to test your pipeline
- **Use Incremental Sync**: Event reports support incremental sync using the `event_time` cursor, which reduces API calls and improves performance
- **Configure Lookback Window**: AppsFlyer data may arrive with latency (10-30 minutes typical, up to 6 hours possible). Use the `lookback_hours` option to ensure you capture late-arriving events. Recommended: 6 hours.
- **Batch Size**: For apps with high event volumes, use smaller `max_days_per_batch` values (e.g., 1-3 days) to avoid timeouts
- **Set Appropriate Schedules**: Balance data freshness requirements with API usage limits. Recommended: hourly or daily sync schedules
- **Monitor Rate Limits**: AppsFlyer imposes rate limits based on your account tier. Typical limit: ~1000 requests per day for raw data export endpoints. The connector implements automatic retry with exponential backoff for rate limit errors.
- **Time Zones**: All timestamps are in UTC. Ensure your downstream analysis accounts for time zone differences if needed.

#### Troubleshooting

**Common Issues:**

1. **Authentication Errors (HTTP 401)**
   - Verify your API token is correct and has not expired
   - Ensure the token is copied exactly from the AppsFlyer dashboard
   - Check that your account has admin permissions

2. **Missing `app_id` Error**
   - Event reports require the `app_id` parameter in table configuration
   - Use the `apps` table to list all available app IDs
   - Verify the app ID format (e.g., `id123456789`)

3. **Rate Limiting (HTTP 429)**
   - The connector automatically retries with exponential backoff
   - Reduce `max_days_per_batch` to make smaller requests
   - Increase the sync schedule interval (e.g., from hourly to every 6 hours)
   - Contact AppsFlyer support to increase your rate limit

4. **Late-Arriving Data**
   - Increase the `lookback_hours` parameter (e.g., from 6 to 12 hours)
   - AppsFlyer data typically arrives within 10-30 minutes but can take up to 6 hours
   - The connector automatically handles overlapping records via upsert

5. **Empty Results**
   - Verify the date range (`start_date`) includes periods with actual data
   - Check that the app has the AppsFlyer SDK integrated and is sending events
   - Ensure your API token has permissions to access the specified app

6. **Region-Specific Issues**
   - If your AppsFlyer account is in the EU region, set `base_url` to `https://eu-west1.appsflyer.com`
   - Default is US region (`https://hq1.appsflyer.com`)

## References

- [AppsFlyer Master API Documentation](https://support.appsflyer.com/hc/en-us/articles/213223166-Master-API-user-acquisition-metrics-via-API)
- [AppsFlyer App List API Documentation](https://support.appsflyer.com/hc/en-us/articles/360011999877-App-list-API-for-app-owners)
- [AppsFlyer Raw Data Export Documentation](https://support.appsflyer.com/hc/en-us/categories/201114756-Data-Export-APIs)
- [AppsFlyer Help Center](https://support.appsflyer.com/hc/en-us)
