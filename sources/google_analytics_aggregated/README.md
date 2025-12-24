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
- **Runtime dependencies**: The connector requires the following Python packages (automatically installed in Databricks/Lakeflow environments):
  - `requests` - For HTTP API calls
  - `google-auth` - For service account authentication

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

The Google Analytics Aggregated Data connector supports **two ways to define reports**:

### 1. Prebuilt Reports (Recommended for Common Use Cases)

The connector includes predefined report configurations for common analytics needs. **Simply use the report name as your `source_table`** - no additional configuration required.

**Available Prebuilt Reports:**

| Report Name | Description | Dimensions | Metrics | Primary Keys |
|-------------|-------------|------------|---------|--------------|
| `traffic_by_country` | Daily active users, sessions, and page views by country | `date`, `country` | `activeUsers`, `sessions`, `screenPageViews` | `["date", "country"]` |

**Benefits:**
- ✅ **Zero configuration** - just use the report name
- ✅ Dimensions, metrics, and primary keys configured automatically
- ✅ Quick setup with consistent definitions
- ✅ Can optionally override any setting (date ranges, filters, etc.)

**Example using prebuilt report (zero config):**
```json
{
  "table": {
    "source_table": "traffic_by_country"
  }
}
```

No `table_configuration` needed. The connector automatically knows:
- Dimensions: `["date", "country"]`
- Metrics: `["activeUsers", "sessions", "screenPageViews"]`
- Primary Keys: `["date", "country"]`
- Ingestion Type: `append` (with `date` as cursor)

**Example with optional overrides:**
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

> **Reserved Names**: Prebuilt report names are reserved for automatic configuration. To use a custom report with a prebuilt name, explicitly provide `dimensions` in `table_configuration` (though a different name is recommended to avoid confusion).

> **Note**: More prebuilt reports can be added to `prebuilt_reports.json` as needed. You can also request additional common reports to be included.

### 2. Custom Reports (For Specific Needs)

For reports not covered by prebuilt options, you can manually configure dimensions, metrics, and other settings:

- **Any table name** - You define custom report names (e.g., `engagement_by_device`, `conversion_funnel`)
- Each report is configured with specific dimensions and metrics via `table_configuration`
- Multiple reports can be ingested in a single pipeline as long as each has a unique name

**Example custom report:**
```json
{
  "table": {
    "source_table": "engagement_by_device",
    "table_configuration": {
      "dimensions": "[\"date\", \"deviceCategory\"]",
      "metrics": "[\"activeUsers\", \"engagementRate\"]",
      "primary_keys": ["date", "deviceCategory"],
      "start_date": "30daysAgo",
      "lookback_days": "3"
    }
  }
}
```

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key dynamically based on the requested dimensions:

| Configuration   | Description                                           | Ingestion Type | Primary Key                                           | Incremental Cursor (if any) |
|-----------------|-------------------------------------------------------|----------------|-------------------------------------------------------|------------------------------|
| Any report name | User-defined aggregated data report with custom dimensions/metrics | `append` or `snapshot` | Composite of all dimensions                           | `date` dimension (if present) |

**Ingestion Type Logic**:
- **`append`**: Used when the `date` dimension is included. The connector tracks the maximum date and incrementally fetches new data with a lookback window.
- **`snapshot`**: Used when no `date` dimension is present. The entire report is refreshed on each sync.

**Primary Key Logic**:

**For Prebuilt Reports** (using report name as `source_table`):
- ✅ Primary keys are **automatically configured** - no need to specify them
- The connector retrieves primary keys from the prebuilt report definition

**For Custom Reports**:
<!-- TODO: UX IMPROVEMENT - This requirement should be removed in future versions -->
<!-- ARCHITECTURAL ISSUE: The ingestion pipeline calls _get_table_metadata() before -->
<!-- table configurations are available (ingestion_pipeline.py line 124). Since GA4's -->
<!-- primary_keys are derived FROM dimensions (in table_options), the connector receives -->
<!-- empty table_options and returns empty primary_keys, causing "APPLY CHANGES query -->
<!-- requires at least one join key" errors. -->
<!-- -->
<!-- FIX: Modify ingestion_pipeline.py to retrieve table configs before metadata and -->
<!-- pass them to _get_table_metadata() with .options(**table_config). -->
<!-- Until fixed, primary_keys MUST be explicitly specified for custom reports. -->
- The primary key **must be explicitly defined** in `table_configuration` as `primary_keys`
- It should be set to the **list of all dimensions** in your report (in the same order)
- Example: If dimensions are `["date", "country"]`, set `"primary_keys": ["date", "country"]`

### Required and optional table options

Table-specific options are passed via the pipeline spec under `table_configuration` in `objects`. 

**For Prebuilt Reports (when using report name as `source_table`):**

When using a prebuilt report name directly as the `source_table`, **no table configuration is required**. All settings (dimensions, metrics, primary keys) are configured automatically.

Optional overrides:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `start_date` | string | no | `"30daysAgo"` | Override the default start date for first sync. |
| `lookback_days` | string | no | `"3"` | Override the default lookback window. |
| `dimension_filter` | string (JSON object) | no | null | Add filter expression for dimensions. |
| `metric_filter` | string (JSON object) | no | null | Add filter expression for metrics. |
| `page_size` | string | no | `"10000"` | Override the default page size. |

> **Important architectural limitation**: Due to how the ingestion pipeline retrieves metadata, `primary_keys` must be explicitly specified even for prebuilt reports, matching the prebuilt report's dimensions. This is redundant but currently required. See the TODO comments in the code for the architectural fix needed.

**For Custom Reports:**

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `dimensions` | string (JSON array) | yes | N/A | List of dimension names as a JSON string (e.g., `"[\"date\", \"country\"]"`). Up to 9 dimensions. |
| `metrics` | string (JSON array) | yes | N/A | List of metric names as a JSON string (e.g., `"[\"activeUsers\", \"sessions\"]"`). At least 1 metric required, up to 10 metrics. |
| `primary_keys` | array | yes | N/A | List of dimension names forming the composite key (e.g., `["date", "country"]`). **Must exactly match all dimensions** in your report. <!-- TODO: Remove this redundant requirement --> |
| `start_date` | string | no | `"30daysAgo"` | Initial start date for first sync. Can be YYYY-MM-DD format or relative like `"30daysAgo"`, `"7daysAgo"`, `"yesterday"`. |
| `lookback_days` | string | no | `"3"` | Number of days to look back for incremental syncs (accounts for data processing delays). |
| `dimension_filter` | string (JSON object) | no | null | Filter expression for dimensions as a JSON string (see Google Analytics Data API documentation for filter syntax). |
| `metric_filter` | string (JSON object) | no | null | Filter expression for metrics as a JSON string (see Google Analytics Data API documentation for filter syntax). |
| `page_size` | string | no | `"10000"` | Number of rows per API request (max 100,000). |

> **Important**: 
> - For **prebuilt reports**: Only specify `prebuilt_report` name, other settings are optional overrides
> - For **custom reports**: `dimensions`, `metrics`, and filter options must be provided as **JSON strings** (e.g., `"[\"date\", \"country\"]"`)
> - `primary_keys` is a **native array** (e.g., `["date", "country"]`) and only required for custom reports
> - All other options are regular strings (e.g., `"30daysAgo"`, `"3"`)

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

For a complete list of available dimensions and metrics, refer to the [Google Analytics Dimensions & Metrics Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema) or query your property's metadata API.

### Discovering Available Dimensions and Metrics

To see all dimensions and metrics available for your specific property (including custom ones):

```bash
GET https://analyticsdata.googleapis.com/v1beta/properties/{YOUR_PROPERTY_ID}/metadata
```

This is useful for discovering custom dimensions/metrics defined in your GA4 property.

### Schema highlights

- **Dynamic Schema**: The schema is generated dynamically based on the requested dimensions and metrics.
- **Type Inference**: The connector automatically determines proper data types by querying the Google Analytics metadata API:
  - **Date Dimensions** (`date`, `firstSessionDate`): `DateType` - Automatically parsed from YYYYMMDD format
  - **String Dimensions** (all others): `StringType`
  - **Integer Metrics** (`activeUsers`, `sessions`, etc.): `LongType` (64-bit integer)
  - **Float Metrics** (`engagementRate`, `bounceRate`, etc.): `DoubleType` (64-bit float)
- **Validation**: The connector validates that requested dimensions and metrics exist in your property, catching typos and non-existent fields before making data requests.

## Data Type Mapping

The connector automatically infers proper data types using the Google Analytics metadata API:

| GA4 API Type     | Example Fields                  | Connector Type | Example Values | Notes |
|------------------|---------------------------------|----------------|----------------|-------|
| Dimension (any)  | country, city, deviceCategory   | StringType     | "United States", "desktop" | All non-date dimensions are strings |
| Date dimension   | date, firstSessionDate          | DateType       | 2025-12-24 | Parsed from YYYYMMDD format (e.g., "20251224" → date(2025, 12, 24)) |
| TYPE_INTEGER     | activeUsers, sessions, newUsers | LongType       | 1234 | 64-bit integers, parsed from API string responses |
| TYPE_FLOAT       | engagementRate, bounceRate      | DoubleType     | 56.78 | 64-bit floats, parsed from API string responses |
| TYPE_CURRENCY    | totalRevenue                    | DoubleType     | 1234.56 | Currency values as floats |
| TYPE_SECONDS, TYPE_MILLISECONDS | averageSessionDuration | LongType or DoubleType | Varies | Time durations parsed to numeric types |

### Type Inference Process

1. **Initialization**: Connector calls the Google Analytics `getMetadata` API once to retrieve type information for all dimensions and metrics in your property
2. **Caching**: Metadata is cached for the duration of the connector session (no repeated API calls)
3. **Schema Generation**: When generating a table schema, the connector looks up each metric's type and maps it to the appropriate PySpark type
4. **Data Parsing**: When reading data, string values from the API are parsed to their target types (integers, floats, dates)

This ensures that your data arrives in Databricks with proper types, ready for analytics without additional transformation.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Google Analytics Aggregated connector source in your workspace.

### Step 2: Configure Your Pipeline

In your pipeline code, configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Google Analytics Aggregated connector.
- One or more **reports** to ingest (prebuilt or custom), each with a unique name and `table_configuration`.

Example `pipeline_spec` mixing prebuilt and custom reports:

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
          "primary_keys": ["date", "deviceCategory"],
          "start_date": "30daysAgo",
          "lookback_days": "3"
        }
      }
    }
  ]
}
```

- `connection_name` must point to the UC connection configured with your GA4 `property_id` and `credentials_json`.
- For each report:
  - `source_table` - Give each report a **unique, descriptive name**
    - **For prebuilt reports**: Use the prebuilt report name (e.g., `"traffic_by_country"`)
    - **For custom reports**: Use any unique name you choose
  - `table_configuration` (optional for prebuilt, required for custom):
    - **Prebuilt**: Omit entirely for zero-config, or include to override defaults (start_date, lookback_days, filters)
    - **Custom**: `dimensions`, `metrics`, `primary_keys`, and other settings
  
> **Note**: Each report must have a unique `source_table` name to avoid conflicts in the ingestion pipeline.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

**For incremental tables (with `date` dimension)**:
- On the **first run**: The connector fetches data from `start_date` to today.
- On **subsequent runs**: The connector uses the stored cursor (maximum date from previous sync) and applies the lookback window to capture late-arriving data.

**For snapshot tables (without `date` dimension)**:
- The entire report is refreshed on each sync.

### Best Practices

- **Start with prebuilt reports**: Use prebuilt reports for common use cases to reduce configuration complexity.
- **Use descriptive report names**: Give each report a unique, descriptive `source_table` name (e.g., `traffic_by_country`, `campaign_performance`) that clearly indicates what data it contains.
- **Multiple reports in one pipeline**: You can configure multiple reports in a single pipeline spec - just ensure each has a unique `source_table` name.
- **Start with common dimensions**: For custom reports, begin with `date` and one or two other dimensions (e.g., `country`, `deviceCategory`) to validate the setup.
- **Use incremental sync**: Always include the `date` dimension for time-series data to enable efficient incremental syncs.
- **Set appropriate lookback**: Use `lookback_days` of 3-7 to account for Google Analytics data processing delays (data is typically finalized within 24-48 hours).
- **Monitor quotas**: Google Analytics enforces API quotas (25,000 tokens/day, 5,000 tokens/hour per property). Plan your sync frequency accordingly.
- **Test dimension/metric combinations**: Not all dimension/metric combinations are compatible. Test your configuration with a small date range first.

### Example Configurations

**Example 1: Prebuilt report (simplest)**
```json
{
  "table": {
    "source_table": "traffic_by_country"
  }
}
```

No configuration needed! The connector automatically knows:
- Dimensions: `["date", "country"]`
- Metrics: `["activeUsers", "sessions", "screenPageViews"]`
- Primary Keys: `["date", "country"]`

**Example 2: Prebuilt report with overrides**
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

**Example 3: Custom report - Device and browser breakdown**
```json
{
  "table": {
    "source_table": "traffic_by_device_browser",
    "table_configuration": {
      "dimensions": "[\"date\", \"deviceCategory\", \"browser\"]",
      "metrics": "[\"activeUsers\", \"sessions\", \"bounceRate\"]",
      "primary_keys": ["date", "deviceCategory", "browser"],
      "start_date": "7daysAgo",
      "lookback_days": "2"
    }
  }
}
```

**Example 4: Custom report - Campaign performance**
```json
{
  "table": {
    "source_table": "campaign_performance",
    "table_configuration": {
      "dimensions": "[\"date\", \"sessionSource\", \"sessionMedium\", \"sessionCampaignName\"]",
      "metrics": "[\"sessions\", \"conversions\", \"totalRevenue\"]",
      "primary_keys": ["date", "sessionSource", "sessionMedium", "sessionCampaignName"],
      "start_date": "90daysAgo",
      "lookback_days": "7"
    }
  }
}
```

**Example 5: Custom report - Page performance (snapshot)**
```json
{
  "table": {
    "source_table": "page_performance_snapshot",
    "table_configuration": {
      "dimensions": "[\"pagePath\", \"pageTitle\"]",
      "metrics": "[\"screenPageViews\", \"averageSessionDuration\"]",
      "primary_keys": ["pagePath", "pageTitle"],
      "start_date": "7daysAgo",
      "scd_type": "SCD_TYPE_1"
    }
  }
}
```

### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`403 Forbidden`)**:
  - Verify that the service account email has been added to the GA4 property with Viewer or Analyst role.
  - Check that the `property_id` is correct and matches the property where access was granted.
  - Ensure the `credentials_json` is valid and complete.

- **Invalid dimension or metric names**:
  - **Error message**: `Invalid report configuration: Unknown dimensions: ['contry']`
  - **Cause**: Typo in dimension or metric name, or the field doesn't exist in your property
  - **Solution**: 
    - Check spelling (e.g., `"contry"` should be `"country"`)
    - Verify the field exists in your property by calling the metadata API:
      ```
      GET https://analyticsdata.googleapis.com/v1beta/properties/{YOUR_PROPERTY_ID}/metadata
      ```
    - Custom dimensions/metrics must exist in your GA4 property configuration
  - The connector validates field names before making data requests, catching typos early

- **Invalid dimension/metric combinations (`400 Bad Request`)**:
  - Not all dimensions and metrics can be combined due to Google Analytics compatibility rules.
  - **Example**: Some metrics are session-scoped while others are user-scoped and cannot be mixed
  - **Solution**:
    - Refer to the [Google Analytics compatibility matrix](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
    - Test with a smaller set of dimensions/metrics first
    - The API error message will indicate which fields are incompatible

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

- **Warning about shadowing prebuilt report names**:
  - **Warning message**: `⚠️  WARNING: Using custom configuration for 'traffic_by_country' (shadowing prebuilt report)`
  - **Cause**: You're using a prebuilt report name as `source_table` but providing custom `dimensions` in `table_configuration`
  - **Effect**: The connector uses your custom dimensions (not the prebuilt ones)
  - **Solution**: Choose a different `source_table` name for custom reports to avoid confusion (e.g., `my_traffic_by_country`)

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

## Technical Details

### API Usage

The connector uses the following Google Analytics Data API endpoints:

1. **`getMetadata`** (called once during initialization):
   - Retrieves all available dimensions and metrics for your property
   - Provides type information for proper schema generation
   - Enables validation of dimension/metric names

2. **`runReport`** (called for each data sync):
   - Fetches aggregated report data with specified dimensions and metrics
   - Handles pagination for large result sets
   - Supports incremental sync with date-based cursor tracking

### Performance Characteristics

- **Initialization**: ~0.3-0.5 seconds (includes metadata fetch and caching)
- **Schema generation**: < 0.1 seconds (uses cached metadata)
- **Data fetching**: Varies by report size (10,000 rows/page, automatic pagination)
- **Validation**: Zero additional API calls (uses cached metadata)

### Design Decisions

- **Type Inference via `getMetadata`**: See `VALIDATION_RATIONALE.md` for why we use lightweight validation instead of `checkCompatibility` API
- **Individual Reports vs. Batch**: See `BATCH_RATIONALE.md` for why we use individual `runReport` calls instead of `batchRunReports`

## References

- Connector implementation: `sources/google_analytics_aggregated/google_analytics_aggregated.py`
- Connector API documentation: `sources/google_analytics_aggregated/google_analytics_aggregated_api_doc.md`
- Design decisions:
  - `VALIDATION_RATIONALE.md` - Why lightweight validation is used
  - `BATCH_RATIONALE.md` - Why individual reports are used
  - `TEST_SUMMARY.md` - Comprehensive test results
- Official Google Analytics Data API documentation:
  - [Google Analytics Data API Overview](https://developers.google.com/analytics/devguides/reporting/data/v1)
  - [getMetadata Method Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/getMetadata)
  - [runReport Method Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport)
  - [Creating a Report Guide](https://developers.google.com/analytics/devguides/reporting/data/v1/basics)
  - [Dimensions & Metrics Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
  - [Quotas and Limits](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)
