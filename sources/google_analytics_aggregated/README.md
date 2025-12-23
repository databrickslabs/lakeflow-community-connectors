# Lakeflow Google Analytics Aggregated Data Community Connector

This documentation describes how to configure and use the **Google Analytics Aggregated Data** Lakeflow community connector to ingest aggregated reporting data from Google Analytics 4 (GA4) into Databricks.

> **Note**: This connector retrieves **aggregated data** through the Google Analytics Data API `runReport` method. It provides dimensional analytics data (e.g., users by country, sessions by date) rather than raw event-level data.

## Prerequisites

- **Google Analytics 4 (GA4) property**: You need access to a GA4 property from which you want to retrieve aggregated data.
- **Google Cloud Project**: A Google Cloud project with the Google Analytics Data API enabled.
- **Service Account credentials**:
  - A service account with access to your GA4 property.
  - The service account JSON key file.
- **Network access**: The environment running the connector must be able to reach `https://analyticsdata.googleapis.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Step 1: Create a Google Cloud Service Account

1. **Create or select a Google Cloud Project**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one

2. **Enable the Google Analytics Data API**:
   - In the Google Cloud Console, navigate to **APIs & Services → Library**
   - Search for "Google Analytics Data API"
   - Click **Enable**

3. **Create a Service Account**:
   - Navigate to **IAM & Admin → Service Accounts**
   - Click **Create Service Account**
   - Enter a name (e.g., "ga4-data-reader")
   - Click **Create and Continue**
   - Skip the optional role assignment steps
   - Click **Done**

4. **Create and download the JSON key**:
   - Click on the newly created service account
   - Go to the **Keys** tab
   - Click **Add Key → Create new key**
   - Select **JSON** format
   - Click **Create**
   - The JSON key file will be downloaded to your computer
   - **Save this file securely** - you'll need it for the connector configuration

### Step 2: Grant Service Account Access to GA4 Property

1. **Open Google Analytics**:
   - Go to [Google Analytics](https://analytics.google.com/)
   - Select your GA4 property

2. **Add the service account as a user**:
   - Click **Admin** (gear icon in the bottom left)
   - In the **Property** column, click **Property Access Management**
   - Click the **+** button in the top right
   - Select **Add users**

3. **Configure access**:
   - In the email field, paste the service account email from the JSON key file
     - Format: `service-account-name@project-id.iam.gserviceaccount.com`
     - Example: `ga4-data-reader@ga4-project-481509.iam.gserviceaccount.com`
   - Select the role: **Viewer** or **Analyst** (Viewer is sufficient for read-only access)
   - Uncheck "Notify new users by email" (service accounts don't receive emails)
   - Click **Add**

### Step 3: Find Your GA4 Property ID

Your Property ID is a numeric value (e.g., `123456789`) that identifies your GA4 property:

1. In Google Analytics, click **Admin** (gear icon)
2. In the **Property** column, click **Property Settings**
3. Your **Property ID** is displayed at the top of the page (numeric value)
4. Copy this value - you'll need it for the connector configuration

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name                       | Type   | Required | Description                                                                                                                     | Example                            |
|----------------------------|--------|----------|---------------------------------------------------------------------------------------------------------------------------------|------------------------------------|
| `property_id`              | string | yes      | Google Analytics 4 property ID (numeric).                                                                                       | `"123456789"`                      |
| `credentials_json`         | object | yes      | Complete service account JSON key as a JSON object (paste the entire content of the downloaded JSON file).                      | `{"type": "service_account", ...}` |
| `externalOptionsAllowList` | string | yes      | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector requires table-specific options, so this parameter must be set. | `dimensions,metrics,start_date,lookback_days,dimension_filter,metric_filter,page_size` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`dimensions,metrics,start_date,lookback_days,dimension_filter,metric_filter,page_size`

> **Note**: Table-specific options such as `dimensions`, `metrics`, or `start_date` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **Property ID**:
  - Follow Step 3 above to find your numeric GA4 property ID.
  
- **Service Account JSON**:
  - Follow Steps 1-2 above to create and download the service account JSON key file.
  - Open the downloaded JSON file in a text editor.
  - Copy the **entire JSON content** (the whole object with all fields).
  - Paste this as the value of `credentials_json` when creating the connection.

**Example of `credentials_json` format**:
```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "abc123...",
  "private_key": "<YOUR_PRIVATE_KEY_STRING>",
  "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
  "client_id": "123456789...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/...",
  "universe_domain": "googleapis.com"
}
```

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `dimensions,metrics,start_date,lookback_days,dimension_filter,metric_filter,page_size` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Google Analytics Aggregated Data connector exposes **user-defined custom reports** based on dimensions and metrics:

- `custom_report` - Aggregated data combining any available dimensions and metrics

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key dynamically based on the requested dimensions:

| Table           | Description                                           | Ingestion Type | Primary Key                                           | Incremental Cursor (if any) |
|-----------------|-------------------------------------------------------|----------------|-------------------------------------------------------|------------------------------|
| `custom_report` | User-defined aggregated data report                   | `append` or `snapshot` | Composite of all dimensions                           | `date` dimension (if present) |

**Ingestion Type Logic**:
- **`append`**: Used when the `date` dimension is included. The connector tracks the maximum date and incrementally fetches new data with a lookback window.
- **`snapshot`**: Used when no `date` dimension is present. The entire report is refreshed on each sync.

**Primary Key Logic**:
- The primary key is the **composite of all dimensions** requested in the report.
- Example: If dimensions are `["date", "country"]`, the primary key is the combination of both fields.

### Required and optional table options

Table-specific options are passed via the pipeline spec under `table` in `objects`. All options for the `custom_report` table:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `dimensions` | JSON array string | no | `"[]"` | List of dimension names to include (e.g., `"[\"date\", \"country\"]"`). Up to 9 dimensions. |
| `metrics` | JSON array string | yes | N/A | List of metric names to include (e.g., `"[\"activeUsers\", \"sessions\"]"`). At least 1 metric required, up to 10 metrics. |
| `start_date` | string | no | `"30daysAgo"` | Initial start date for first sync. Can be YYYY-MM-DD format or relative like `"30daysAgo"`, `"7daysAgo"`, `"yesterday"`. |
| `lookback_days` | string | no | `"3"` | Number of days to look back for incremental syncs (accounts for data processing delays). |
| `dimension_filter` | JSON object string | no | null | Filter expression for dimensions (see Google Analytics Data API documentation for filter syntax). |
| `metric_filter` | JSON object string | no | null | Filter expression for metrics (see Google Analytics Data API documentation for filter syntax). |
| `page_size` | string | no | `"10000"` | Number of rows per API request (max 100,000). |

> **Important**: All table options must be provided as **strings**, including JSON arrays and objects. For example: `"dimensions": "[\"date\", \"country\"]"` (not `"dimensions": ["date", "country"]`).

### Common Dimensions and Metrics

**Popular Dimensions**:
- `date` - Date in YYYYMMDD format (recommended for incremental sync)
- `country` - User's country
- `city` - User's city
- `deviceCategory` - Device category (desktop, mobile, tablet)
- `browser` - Browser used
- `operatingSystem` - Operating system
- `sessionSource` - Source of the session
- `sessionMedium` - Medium of the session
- `sessionCampaignName` - Campaign name
- `pagePath` - Page path
- `pageTitle` - Page title
- `eventName` - Event name
- `language` - User's language
- `newVsReturning` - New vs returning user classification

**Popular Metrics**:
- `activeUsers` - Number of distinct active users
- `newUsers` - Number of new users
- `sessions` - Number of sessions
- `screenPageViews` - Number of screen/page views
- `eventCount` - Number of events
- `conversions` - Number of conversions
- `totalRevenue` - Total revenue
- `engagementRate` - Engagement rate
- `averageSessionDuration` - Average session duration
- `bounceRate` - Bounce rate
- `sessionsPerUser` - Average sessions per user

For a complete list of available dimensions and metrics, refer to the [Google Analytics Dimensions & Metrics Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema).

### Schema highlights

- **Dynamic Schema**: The schema is generated dynamically based on the requested dimensions and metrics.
- **Dimension Fields**: All dimensions are returned as `StringType` (as the API returns them).
- **Metric Fields**: All metrics are returned as `StringType` (the API returns values as strings; parsing happens downstream).
- **Date Format**: The `date` dimension is automatically converted from YYYYMMDD format to YYYY-MM-DD format for easier handling.

## Data Type Mapping

Google Analytics Data API returns all values as strings. The connector preserves this format in the schema:

| GA4 API Type     | Example Fields                  | Connector Type | Notes |
|------------------|---------------------------------|----------------|-------|
| Dimension (any)  | country, city, deviceCategory   | StringType     | All dimension values are strings |
| Date dimension   | date (YYYYMMDD format)          | StringType     | Converted to YYYY-MM-DD format in records |
| TYPE_INTEGER     | activeUsers, sessions           | StringType     | Returned as string (e.g., "1234") |
| TYPE_FLOAT       | engagementRate, bounceRate      | StringType     | Returned as string (e.g., "56.78") |
| TYPE_CURRENCY    | totalRevenue                    | StringType     | Returned as string (e.g., "1234.56") |

> **Note**: All metric values are returned as strings from the API. You can parse them to numeric types in your downstream transformations based on the metric type.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Google Analytics Aggregated connector source in your workspace.

### Step 2: Configure Your Pipeline

In your pipeline code, configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Google Analytics Aggregated connector.
- One or more **tables** to ingest, each with table options specifying dimensions and metrics.

Example `pipeline_spec` snippet for a basic report:

```json
{
  "pipeline_spec": {
    "connection_name": "google_analytics_connection",
    "object": [
      {
        "table": {
          "source_table": "custom_report",
          "dimensions": "[\"date\", \"country\", \"deviceCategory\"]",
          "metrics": "[\"activeUsers\", \"sessions\", \"screenPageViews\"]",
          "start_date": "30daysAgo",
          "lookback_days": "3"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your GA4 `property_id` and `credentials_json`.
- For each `table`:
  - `source_table` must be `"custom_report"`.
  - `dimensions` and `metrics` define what data to retrieve (as JSON array strings).
  - `start_date` sets the initial backfill date (optional, defaults to "30daysAgo").
  - `lookback_days` sets the incremental sync lookback window (optional, defaults to 3).

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

**For incremental tables (with `date` dimension)**:
- On the **first run**: The connector fetches data from `start_date` to today.
- On **subsequent runs**: The connector uses the stored cursor (maximum date from previous sync) and applies the lookback window to capture late-arriving data.

**For snapshot tables (without `date` dimension)**:
- The entire report is refreshed on each sync.

### Best Practices

- **Start with common dimensions**: Begin with `date` and one or two other dimensions (e.g., `country`, `deviceCategory`) to validate the setup.
- **Use incremental sync**: Always include the `date` dimension for time-series data to enable efficient incremental syncs.
- **Set appropriate lookback**: Use `lookback_days` of 3-7 to account for Google Analytics data processing delays (data is typically finalized within 24-48 hours).
- **Monitor quotas**: Google Analytics enforces API quotas (25,000 tokens/day, 5,000 tokens/hour per property). Plan your sync frequency accordingly.
- **Test dimension/metric combinations**: Not all dimension/metric combinations are compatible. Test your configuration with a small date range first.

### Example Configurations

**Example 1: Traffic by date and country**
```json
{
  "source_table": "custom_report",
  "dimensions": "[\"date\", \"country\"]",
  "metrics": "[\"activeUsers\", \"sessions\", \"screenPageViews\"]",
  "start_date": "30daysAgo",
  "lookback_days": "3"
}
```

**Example 2: Device and browser breakdown**
```json
{
  "source_table": "custom_report",
  "dimensions": "[\"date\", \"deviceCategory\", \"browser\"]",
  "metrics": "[\"activeUsers\", \"sessions\", \"bounceRate\"]",
  "start_date": "7daysAgo",
  "lookback_days": "2"
}
```

**Example 3: Campaign performance**
```json
{
  "source_table": "custom_report",
  "dimensions": "[\"date\", \"sessionSource\", \"sessionMedium\", \"sessionCampaignName\"]",
  "metrics": "[\"sessions\", \"conversions\", \"totalRevenue\"]",
  "start_date": "90daysAgo",
  "lookback_days": "7"
}
```

**Example 4: Page performance (snapshot)**
```json
{
  "source_table": "custom_report",
  "dimensions": "[\"pagePath\", \"pageTitle\"]",
  "metrics": "[\"screenPageViews\", \"averageSessionDuration\"]",
  "start_date": "7daysAgo"
}
```

### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`403 Forbidden`)**:
  - Verify that the service account email has been added to the GA4 property with Viewer or Analyst role.
  - Check that the `property_id` is correct and matches the property where access was granted.
  - Ensure the `credentials_json` is valid and complete.

- **Invalid dimension/metric combinations (`400 Bad Request`)**:
  - Not all dimensions and metrics can be combined. Refer to the [Google Analytics compatibility matrix](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema).
  - Test with a smaller set of dimensions/metrics first.

- **Rate limiting (`429 Too Many Requests`)**:
  - You've exceeded the API quota (25,000 tokens/day or 5,000 tokens/hour).
  - Reduce sync frequency or split reports across multiple properties if available.
  - The connector automatically retries with exponential backoff.

- **Empty results**:
  - Check that your GA4 property has data for the requested date range.
  - Verify that the dimensions and metrics exist for your property (custom dimensions/metrics may not be available).
  - Check for filters that might be excluding all data.

- **Data freshness issues**:
  - Google Analytics data is typically processed within 24-48 hours.
  - Increase `lookback_days` to 7 if you need to capture late-arriving data.
  - Recent data may be incomplete or change as processing completes.

## Rate Limits and Quotas

Google Analytics Data API enforces the following quotas per property:

| Quota Type              | Standard Limit | Description                                            |
|-------------------------|----------------|--------------------------------------------------------|
| Tokens per day          | 25,000         | Total API tokens consumed per day per property         |
| Tokens per hour         | 5,000          | Total API tokens consumed per hour per property        |
| Concurrent requests     | 10             | Maximum simultaneous requests                          |

**Token Consumption**:
- Basic request: 1 token
- Additional tokens based on complexity (dimensions beyond 4, metrics beyond 4)

The connector automatically handles rate limiting with exponential backoff and retry logic.

## Known Limitations

- **Data Freshness**: Google Analytics data is typically processed within 24-48 hours. Recent data may be incomplete or updated as processing completes.
- **Sampling**: Very large queries may be sampled by Google Analytics. Breaking queries into smaller date ranges can help avoid sampling.
- **Thresholding**: Google applies data thresholding for privacy when volumes are low. Some small metric values may be withheld.
- **Cardinality**: High-cardinality dimensions may result in data aggregation into "(other)" rows.
- **Read-Only**: This connector only supports reading aggregated data. There is no write functionality.

## References

- Connector implementation: `sources/google_analytics_aggregated/google_analytics_aggregated.py`
- Connector API documentation: `sources/google_analytics_aggregated/google_analytics_aggregated_api_doc.md`
- Official Google Analytics Data API documentation:
  - [Google Analytics Data API Overview](https://developers.google.com/analytics/devguides/reporting/data/v1)
  - [runReport Method Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport)
  - [Creating a Report Guide](https://developers.google.com/analytics/devguides/reporting/data/v1/basics)
  - [Dimensions & Metrics Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
  - [Quotas and Limits](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)

