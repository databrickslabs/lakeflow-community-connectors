# Lakeflow Google Analytics Aggregated Data Community Connector

This documentation describes how to configure and use the **Google Analytics Aggregated Data** Lakeflow community connector to ingest aggregated reporting data from Google Analytics 4 (GA4) into Databricks.

> **Note**: This connector retrieves **aggregated data** through the Google Analytics Data API `runReport` method. It provides dimensional analytics data (e.g., users by country, sessions by date) rather than raw event-level data.

## Prerequisites

- **Google Analytics 4 (GA4) property**: You need access to a GA4 property from which you want to retrieve aggregated data.
- **Google Cloud Project**: A Google Cloud project with the Google Analytics Data API enabled.
- **Service Account credentials**: A service account with access to your GA4 property, plus the JSON key file.
- **Network access**: The environment must be able to reach `https://analyticsdata.googleapis.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.
- **Runtime dependencies**: `requests` (HTTP API calls) and `google-auth` (service account authentication).

## Setup

### Step 1: Create a Google Cloud Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/) and create or select a project.
2. Navigate to **APIs & Services > Library**, search for "Google Analytics Data API", and click **Enable**.
3. Navigate to **IAM & Admin > Service Accounts** and click **Create Service Account**.
4. Enter a name (e.g., "ga4-data-reader"), click **Create and Continue**, skip optional roles, click **Done**.
5. Click the service account, go to the **Keys** tab, click **Add Key > Create new key**, select **JSON**, and click **Create**. Save the downloaded file securely.

### Step 2: Grant Service Account Access to GA4 Property

1. In [Google Analytics](https://analytics.google.com/), select your GA4 property.
2. Click **Admin** (gear icon) > **Property Access Management** > **+** > **Add users**.
3. Paste the service account email (e.g., `ga4-data-reader@project-id.iam.gserviceaccount.com`).
4. Select **Viewer** role, uncheck "Notify new users by email", click **Add**.
5. Repeat for each GA4 property you want to include.

### Step 3: Find Your GA4 Property ID(s)

1. In Google Analytics, click **Admin** > **Property Settings**.
2. Copy the numeric **Property ID** (e.g., `123456789`).

### Step 4: Create a Unity Catalog Connection

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select or create a connection for this source.
3. Set `externalOptionsAllowList` to:
   ```
   dimensions,metrics,start_date,lookback_days,dimension_filter,metric_filter,page_size,primary_keys,prebuilt_report
   ```

### Connection Parameters

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `property_ids` | string | yes | JSON array of GA4 property IDs (numeric strings). | `["123456789"]` or `["123456789", "987654321"]` |
| `credentials_json` | string | yes | Complete service account JSON key (paste the entire downloaded file). Must have access to all listed properties. | `{"type": "service_account", ...}` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of allowed table-specific options (see Step 4). | `dimensions,metrics,...` |

**Example `credentials_json` format:**
```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "abc123...",
  "private_key": "<YOUR_PRIVATE_KEY_STRING>",
  "client_email": "your-sa@your-project.iam.gserviceaccount.com",
  "client_id": "123456789...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

### Multi-Property Support

The connector supports ingesting from **multiple GA4 properties** in a single connection:

- Fetches data from all specified properties in each sync
- Adds a `property_id` field (StringType) to every record as the **first column**
- Prepends `property_id` to primary keys for uniqueness across properties
- Tracks incremental cursors globally across all properties
- Each property has independent rate limits and quotas

The `property_id` field is **always included** even for single-property configurations, ensuring forward compatibility if you add properties later.

## Supported Objects

### 1. Prebuilt Reports (Recommended for Common Use Cases)

Use the report name directly as `source_table` — no `table_configuration` needed. Dimensions, metrics, and primary keys are configured automatically.

| Report Name | Description | Dimensions | Metrics |
|-------------|-------------|------------|---------|
| `traffic_by_country` | Daily users, sessions, page views by country | `date`, `country` | `activeUsers`, `sessions`, `screenPageViews` |
| `user_acquisition` | Daily traffic sources and campaigns | `date`, `sessionSource`, `sessionMedium` | `sessions`, `activeUsers`, `newUsers`, `engagementRate` |
| `events_summary` | Daily event breakdown | `date`, `eventName` | `eventCount`, `activeUsers` |
| `page_performance` | Daily page views by path and title | `date`, `pagePath`, `pageTitle` | `screenPageViews`, `averageSessionDuration`, `bounceRate` |
| `device_breakdown` | Daily users by device and browser | `date`, `deviceCategory`, `browser` | `activeUsers`, `sessions`, `engagementRate` |
| `landing_page_performance` | Landing page performance | `date`, `landingPage` | `sessions`, `activeUsers`, `newUsers`, `keyEvents`, `totalRevenue` |
| `ecommerce_purchases` | Item-level ecommerce performance | `date`, `itemName`, `itemId` | `itemsViewed`, `itemsAddedToCart`, `itemsPurchased`, `itemRevenue` |
| `purchase_journey` | User activity across shopping stages | `date`, `eventName` | `activeUsers`, `eventCount` |
| `checkout_journey` | Users at checkout steps | `date`, `eventName` | `activeUsers`, `eventCount` |
| `promotions_performance` | Internal promotion clicks and views | `date`, `itemPromotionName` | `itemsViewedInPromotion`, `itemsClickedInPromotion`, `itemRevenue` |
| `user_retention` | New vs returning users | `date`, `newVsReturning` | `activeUsers`, `sessions`, `engagementRate` |
| `demographic_details` | Users by language and location | `date`, `language`, `country` | `activeUsers`, `newUsers`, `engagedSessions` |
| `audience_performance` | Audience-level performance | `date`, `audienceName` | `activeUsers`, `sessions`, `keyEvents` |
| `tech_details` | Browsers and operating systems | `date`, `browser`, `operatingSystem` | `activeUsers`, `sessions`, `engagementRate` |
| `advertising_channels` | Channel group, ad cost, and ROAS | `date`, `defaultChannelGroup` | `keyEvents`, `advertiserAdCost`, `totalRevenue` |

> **Reserved Names**: Prebuilt report names are reserved for automatic configuration. To use a custom report with a prebuilt name, explicitly provide `dimensions` in `table_configuration` (though a different name is recommended).

### 2. Custom Reports

For reports not covered by prebuilt options, configure dimensions and metrics via `table_configuration`. Use any unique name as `source_table`.

### Ingestion Mode and Primary Keys

The connector determines ingestion mode and primary keys dynamically:

- **`cdc`**: When the `date` dimension is included. Enables MERGE behavior so the lookback window can re-fetch and update settling data (GA4 data typically finalizes within 48-72 hours).
- **`snapshot`**: When no `date` dimension is present. The entire report is refreshed each sync.
- **Primary keys**: Automatically inferred as `["property_id"] + dimensions`. Can be overridden via `primary_keys` in table options.

## Table Options

Table-specific options are passed via `table_configuration` in the pipeline spec.

**For prebuilt reports**, all options are optional overrides. **For custom reports**, `dimensions` and `metrics` are required.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `dimensions` | string (JSON array) | custom only | N/A | Dimension names (e.g., `'["date", "country"]'`). Max 9 per report. |
| `metrics` | string (JSON array) | custom only | N/A | Metric names (e.g., `'["activeUsers", "sessions"]'`). Min 1, max 10 per report. |
| `prebuilt_report` | string | no | N/A | Load a prebuilt report config by name (alternative to using the name as `source_table`). |
| `primary_keys` | array | no | Auto-inferred | Override primary keys. Default: `["property_id"] + dimensions`. |
| `start_date` | string | no | `"30daysAgo"` | Start date for first sync. Accepts `YYYY-MM-DD` or relative values like `"30daysAgo"`, `"7daysAgo"`, `"yesterday"`. |
| `lookback_days` | string | no | `"3"` | Days to look back for incremental syncs (accounts for data processing delays). |
| `dimension_filter` | string (JSON object) | no | null | Filter expression for dimensions (see [GA4 filter syntax](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/FilterExpression)). |
| `metric_filter` | string (JSON object) | no | null | Filter expression for metrics. |
| `page_size` | string | no | `"10000"` | Rows per API request (max 100,000). |

> **Note**: `dimensions` and `metrics` are JSON strings (e.g., `'["date", "country"]'`). `primary_keys` is a native array. All other options are plain strings.

### Common Dimensions and Metrics

**Dimensions**: `date`, `country`, `city`, `deviceCategory`, `browser`, `operatingSystem`, `sessionSource`, `sessionMedium`, `sessionCampaignName`, `pagePath`, `pageTitle`, `eventName`, `language`, `newVsReturning`

**Metrics**: `activeUsers`, `newUsers`, `sessions`, `screenPageViews`, `eventCount`, `conversions`, `totalRevenue`, `engagementRate`, `averageSessionDuration`, `bounceRate`, `sessionsPerUser`

For the complete list, see the [GA4 Dimensions & Metrics Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema) or query your property's metadata API:
```
GET https://analyticsdata.googleapis.com/v1beta/properties/{PROPERTY_ID}/metadata
```

## Data Type Mapping

The connector infers types by querying the GA4 metadata API once (cached for the session):

| GA4 API Type / Field | Examples | Connector Type | Notes |
|----------------------|----------|----------------|-------|
| `property_id` (added by connector) | "123456789" | StringType | Always first field |
| Dimension (general) | country, deviceCategory | StringType | |
| Date dimension | date, firstSessionDate | DateType | Parsed from YYYYMMDD |
| DateTime dimension | dateHour, dateHourMinute | StringType | Contains time components |
| TYPE_INTEGER | activeUsers, sessions | LongType | |
| TYPE_FLOAT | engagementRate, bounceRate | DoubleType | |
| TYPE_CURRENCY | totalRevenue | DoubleType | |
| TYPE_SECONDS, TYPE_MILLISECONDS | averageSessionDuration | LongType or DoubleType | |

## How to Run

### Configure Your Pipeline

Define a `pipeline_spec` referencing your Unity Catalog connection and one or more reports:

```json
{
  "connection_name": "google_analytics_connection",
  "objects": [
    {
      "table": {
        "source_table": "traffic_by_country"
      }
    },
    {
      "table": {
        "source_table": "engagement_by_device",
        "table_configuration": {
          "dimensions": "[\"date\", \"deviceCategory\"]",
          "metrics": "[\"activeUsers\", \"engagementRate\"]",
          "start_date": "30daysAgo",
          "lookback_days": "3"
        }
      }
    }
  ]
}
```

Each report must have a **unique** `source_table` name.

### Run and Schedule

- **Incremental tables** (with `date` dimension): First run fetches from `start_date` to today. Subsequent runs use the stored cursor with the lookback window.
- **Snapshot tables** (without `date` dimension): Entire report is refreshed each sync.

### Example Configurations

**Prebuilt report (zero config):**
```json
{ "table": { "source_table": "traffic_by_country" } }
```

**Prebuilt report with overrides:**
```json
{
  "table": {
    "source_table": "traffic_by_country",
    "table_configuration": {
      "start_date": "7daysAgo",
      "lookback_days": "1"
    }
  }
}
```

**Custom report with filters:**
```json
{
  "table": {
    "source_table": "campaign_performance",
    "table_configuration": {
      "dimensions": "[\"date\", \"sessionSource\", \"sessionMedium\", \"sessionCampaignName\"]",
      "metrics": "[\"sessions\", \"conversions\", \"totalRevenue\"]",
      "start_date": "90daysAgo",
      "lookback_days": "7"
    }
  }
}
```

### Best Practices

- **Start with prebuilt reports** for common use cases to reduce configuration.
- **Always include `date`** for time-series data to enable incremental sync.
- **Set lookback 3-7 days** to account for GA4 data processing delays (typically 24-48 hours).
- **Test dimension/metric combinations** with a small date range first — not all are compatible.
- **Monitor quotas**: GA4 enforces 25,000 tokens/day and 5,000 tokens/hour per property.

## Troubleshooting

- **Authentication failures (`403 Forbidden`)**:
  - Verify the service account has Viewer/Analyst role on all GA4 properties in `property_ids`.
  - Check that `property_ids` and `credentials_json` are correct and complete.

- **Invalid dimension or metric names**:
  - The connector validates field names before making data requests.
  - Check spelling and verify the field exists via the metadata API.

- **Incompatible dimension/metric combinations (`400 Bad Request`)**:
  - Not all dimensions and metrics can be combined. See the [compatibility matrix](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema).
  - Test with a smaller set first; the API error message indicates which fields conflict.

- **Rate limiting (`429 Too Many Requests`)**:
  - The connector retries automatically with exponential backoff.
  - Reduce sync frequency or split reports if quotas are consistently exceeded.

- **Empty results**:
  - Check that your property has data for the requested date range.
  - Verify dimensions/metrics exist and filters aren't excluding all data.

- **Data freshness issues**:
  - GA4 data processes within 24-48 hours. Increase `lookback_days` for late-arriving data.

- **Shadowing prebuilt report warning**:
  - Occurs when using a prebuilt report name with custom `dimensions`. Choose a different name for custom reports.

## Rate Limits and Quotas

| Quota Type | Standard Limit | Description |
|------------|----------------|-------------|
| Tokens per day | 25,000 | Per property per day |
| Tokens per hour | 5,000 | Per property per hour |
| Concurrent requests | 10 | Maximum simultaneous requests |

Each property has independent quotas. The connector handles rate limiting with exponential backoff.

## Known Limitations

- **API limits per request**: Max 9 dimensions, 10 metrics, 4 date ranges. The connector validates dimensions/metrics limits before API calls. Split reports to work around limits.
- **Data freshness**: GA4 data processes within 24-48 hours; recent data may change.
- **Sampling**: Large queries may be sampled. Smaller date ranges help avoid this.
- **Thresholding**: Google applies privacy thresholding for low-volume data.
- **Cardinality**: High-cardinality dimensions may produce "(other)" aggregation rows.
- **Read-only**: No write functionality.

## Technical Details

The connector uses two GA4 Data API endpoints:

1. **`getMetadata`** (called once): Retrieves available dimensions/metrics and their types for validation and schema generation.
2. **`runReport`** (called per sync): Fetches aggregated data with pagination and incremental cursor tracking.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/google_analytics_aggregated/google_analytics_aggregated.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/google_analytics_aggregated/google_analytics_aggregated_api_doc.md`
- [Google Analytics Data API Overview](https://developers.google.com/analytics/devguides/reporting/data/v1)
- [Dimensions & Metrics Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
- [runReport Method](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport)
- [Quotas and Limits](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)
