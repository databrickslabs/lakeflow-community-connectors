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

### Step 3: Find Your GA4 Property ID(s)

Your Property ID is a numeric value (e.g., `123456789`) that identifies your GA4 property:

1. In Google Analytics, click **Admin** (gear icon)
2. In the **Property** column, click **Property Settings**
3. Your **Property ID** is displayed at the top of the page (numeric value)
4. Copy this value - you'll need it for the connector configuration

**For Multiple Properties:**
- The connector supports ingesting data from multiple GA4 properties in a single connection
- Repeat the above steps for each property you want to include
- Ensure the service account has access to all properties (repeat Step 2 for each property)

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name                       | Type   | Required | Description                                                                                                                     | Example                            |
|----------------------------|--------|----------|---------------------------------------------------------------------------------------------------------------------------------|------------------------------------|
| `property_ids`             | array  | yes      | List of Google Analytics 4 property IDs (numeric strings). For a single property, use a list with one element.                 | `["123456789"]` or `["123456789", "987654321"]` |
| `credentials_json`         | object | yes      | Complete service account JSON key as a JSON object (paste the entire content of the downloaded JSON file). The service account must have access to all properties listed in `property_ids`. | `{"type": "service_account", ...}` |
| `externalOptionsAllowList` | string | yes      | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector requires table-specific options, so this parameter must be set. | `dimensions,metrics,start_date,lookback_days,dimension_filter,metric_filter,page_size,tableConfigs` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`dimensions,metrics,start_date,lookback_days,dimension_filter,metric_filter,page_size,tableConfigs`

> **Note**: Table-specific options such as `dimensions`, `metrics`, or `start_date` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **Property IDs**:
  - Follow Step 3 above to find your numeric GA4 property ID(s).
  - For a single property, provide a list with one element: `["123456789"]`
  - For multiple properties, provide all property IDs in the list: `["123456789", "987654321"]`
  
- **Service Account JSON**:
  - Follow Steps 1-2 above to create and download the service account JSON key file.
  - Ensure the service account has access to **all** properties listed in `property_ids`.
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

### Multi-Property Support

The connector supports ingesting data from **one or multiple GA4 properties** in a single connection.

**Single Property Configuration:**
```json
{
  "property_ids": ["123456789"],
  "credentials_json": { ... }
}
```

**Multiple Properties Configuration:**
```json
{
  "property_ids": ["123456789", "987654321", "555666777"],
  "credentials_json": { ... }
}
```

**How Multi-Property Works:**
- ✅ Fetches data from all specified properties in each sync
- ✅ Automatically adds a `property_id` field to every record containing the numeric property ID
- ✅ Prepends `property_id` to the primary keys to ensure uniqueness across properties
- ✅ Tracks incremental sync cursors globally across all properties

**Schema Stability Design:**

To prevent breaking schema changes, the connector **always includes the `property_id` field** in the output schema and primary keys, regardless of whether you configure a single property or multiple properties. This design decision ensures:

- ✅ **Forward compatibility**: You can add more properties later without breaking downstream pipelines
- ✅ **Consistent schema**: The schema remains stable across configuration changes
- ✅ **No data loss**: Existing data continues to work when properties are added

| Field Name | Type | Description | Added When |
|------------|------|-------------|------------|
| `property_id` | string | The numeric GA4 property ID (e.g., "123456789") | **Always** (single or multiple properties) |

This field is added as the **first field** in the schema and the **first element** in the primary key list.

**Example Primary Keys (Always Include property_id):**

| Report Configuration | Primary Keys |
|----------------------|--------------|
| Prebuilt: `traffic_by_country` | `["property_id", "date", "country"]` |
| Custom with `date`, `deviceCategory` | `["property_id", "date", "deviceCategory"]` |
| Custom snapshot (no date) | `["property_id", "country", "city"]` |

> **Note**: Even with a single property, `property_id` is included to allow seamless addition of properties in the future.

**Rate Limits & Quotas:**
- Each property has **independent** rate limits and quotas
- Total API load is the sum across all properties
- One property reaching quota limits does not affect others

**Use Cases for Multi-Property:**
- Consolidate data from multiple brands or websites into a single table
- Compare metrics across different GA4 properties
- Simplify ETL pipelines when analyzing multiple properties together

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `dimensions,metrics,start_date,lookback_days,dimension_filter,metric_filter,page_size,tableConfigs` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Google Analytics Aggregated Data connector supports **two ways to define reports**:

### 1. Prebuilt Reports (Recommended for Common Use Cases)

The connector includes predefined report configurations for common analytics needs. **Simply use the report name as your `source_table`** - no additional configuration required.

**Available Prebuilt Reports:**

| Report Name | Description | Dimensions | Metrics |
|-------------|-------------|------------|---------|
| `traffic_by_country` | Daily active users, sessions, and page views by country | `date`, `country` | `activeUsers`, `sessions`, `screenPageViews` |
| `user_acquisition` | Daily traffic sources and campaign performance | `date`, `sessionSource`, `sessionMedium` | `sessions`, `activeUsers`, `newUsers`, `engagementRate` |
| `events_summary` | Daily event breakdown by event name | `date`, `eventName` | `eventCount`, `activeUsers` |
| `page_performance` | Daily page views by page path and title | `date`, `pagePath`, `pageTitle` | `screenPageViews`, `averageSessionDuration`, `bounceRate` |
| `device_breakdown` | Daily users by device category and browser | `date`, `deviceCategory`, `browser` | `activeUsers`, `sessions`, `engagementRate` |
| `landing_page_performance` | Performance of the first page users land on | `date`, `landingPage` | `sessions`, `activeUsers`, `newUsers`, `keyEvents`, `totalRevenue` |
| `ecommerce_purchases` | Item-level performance for ecommerce sales | `date`, `itemName`, `itemId` | `itemsViewed`, `itemsAddedToCart`, `itemsPurchased`, `itemRevenue` |
| `purchase_journey` | User activity across shopping stages (View Item, Add to Cart, etc.) | `date`, `eventName` | `activeUsers`, `eventCount` |
| `checkout_journey` | Daily active users at specific checkout steps | `date`, `eventName` | `activeUsers`, `eventCount` |
| `promotions_performance` | Internal promotion clicks and views | `date`, `itemPromotionName` | `itemsViewedInPromotion`, `itemsClickedInPromotion`, `itemRevenue` |
| `user_retention` | Comparison of new vs returning users | `date`, `newVsReturning` | `activeUsers`, `sessions`, `engagementRate` |
| `demographic_details` | User breakdown by language and geographic location | `date`, `language`, `country` | `activeUsers`, `newUsers`, `engagedSessions` |
| `audience_performance` | Performance metrics for defined user audiences | `date`, `audienceName` | `activeUsers`, `sessions`, `keyEvents` |
| `tech_details` | Detailed breakdown of browsers and operating systems | `date`, `browser`, `operatingSystem` | `activeUsers`, `sessions`, `engagementRate` |
| `advertising_channels` | Performance by channel group, ad cost, and ROAS | `date`, `defaultChannelGroup` | `keyEvents`, `advertiserAdCost`, `totalRevenue` |

> **Note**: The `property_id` field is **always automatically included** as the first element in primary keys for schema stability.

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
- Primary Keys: `["property_id", "date", "country"]` (property_id always included)
- Ingestion Type: `cdc` (with `date` as cursor, enables settlement-aware sync)

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

> **Note**: The connector ships with 15 prebuilt reports covering common analytics use cases. More reports can be added to `prebuilt_reports.json` as needed.

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
      "start_date": "30daysAgo",
      "lookback_days": "3"
    }
  }
}
```

> **Note**: Primary keys are automatically inferred as `["property_id", "date", "deviceCategory"]` from the dimensions.

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key dynamically based on the requested dimensions:

| Configuration   | Description                                           | Ingestion Type | Primary Key                                           | Incremental Cursor (if any) |
|-----------------|-------------------------------------------------------|----------------|-------------------------------------------------------|------------------------------|
| Any report name | User-defined aggregated data report with custom dimensions/metrics | `cdc` or `snapshot` | Composite of all dimensions                           | `date` dimension (if present) |

**Ingestion Type Logic**:
- **`cdc`**: Used when the `date` dimension is included. This enables MERGE behavior (via `apply_changes`) so the lookback window can re-fetch and update settled data. GA4 data typically settles within 48-72 hours as attribution models finalize.
- **`snapshot`**: Used when no `date` dimension is present. The entire report is refreshed on each sync.

**Primary Key Logic**:

**For All Reports** (both prebuilt and custom):
- Primary keys are **automatically inferred** from dimensions
- The connector automatically constructs primary keys as: `["property_id"] + dimensions`
- `property_id` is **always automatically prepended** for schema stability
- Example: If dimensions are `["date", "country"]`, primary keys will be `["property_id", "date", "country"]`
- **Optional override**: You can still explicitly define `primary_keys` if you need a custom order or subset

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

**For Custom Reports:**

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `dimensions` | string (JSON array) | yes | N/A | List of dimension names as a JSON string (e.g., `"[\"date\", \"country\"]"`). **API Limit: Maximum 9 dimensions per report.** The connector validates this before making API calls. |
| `metrics` | string (JSON array) | yes | N/A | List of metric names as a JSON string (e.g., `"[\"activeUsers\", \"sessions\"]"`). At least 1 metric required. **API Limit: Maximum 10 metrics per report.** The connector validates this before making API calls. |
| `primary_keys` | array | no | Auto-inferred | Optional override for primary keys. By default, automatically inferred as `["property_id"] + dimensions`. If specified, **must always start with `"property_id"`** for schema stability. |
| `start_date` | string | no | `"30daysAgo"` | Initial start date for first sync. Can be YYYY-MM-DD format or relative like `"30daysAgo"`, `"7daysAgo"`, `"yesterday"`. |
| `lookback_days` | string | no | `"3"` | Number of days to look back for incremental syncs (accounts for data processing delays). |
| `dimension_filter` | string (JSON object) | no | null | Filter expression for dimensions as a JSON string (see Google Analytics Data API documentation for filter syntax). |
| `metric_filter` | string (JSON object) | no | null | Filter expression for metrics as a JSON string (see Google Analytics Data API documentation for filter syntax). |
| `page_size` | string | no | `"10000"` | Number of rows per API request (max 100,000). |

> **Important**: 
> - For **prebuilt reports**: Only specify `prebuilt_report` name, other settings are optional overrides
> - For **custom reports**: `dimensions` and `metrics` must be provided as **JSON strings** (e.g., `"[\"date\", \"country\"]"`)
> - `primary_keys` (if specified) is a **native array** (e.g., `["property_id", "date", "country"]`) and **must always start with `"property_id"`** for schema stability
> - All other options are regular strings (e.g., `"30daysAgo"`, `"3"`)
> - **Automatic Inference**: Primary keys and ingestion type are automatically inferred from your report configuration - no manual specification needed
> - **API Limits Validation**: The connector automatically validates that your report configuration doesn't exceed API limits (9 dimensions, 10 metrics) before making requests. If limits are exceeded, you'll receive a clear error message with suggestions for splitting your report.

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

- **Schema Stability**: The connector **always includes a `property_id` field** as the first column in every table, regardless of single or multi-property configuration. This ensures forward compatibility if you add more properties later.
- **Dynamic Schema**: The schema is generated dynamically based on the requested dimensions and metrics, plus the `property_id` field.
- **Type Inference**: The connector automatically determines proper data types by querying the Google Analytics metadata API:
  - **Property ID**: `StringType` - Always the first field in the schema
  - **Date Dimensions** (`date`, `firstSessionDate`): `DateType` - Automatically parsed from YYYYMMDD format
  - **DateTime Dimensions** (`dateHour`, `dateHourMinute`): `StringType` - Kept as strings since they contain time components
  - **String Dimensions** (all others): `StringType`
  - **Integer Metrics** (`activeUsers`, `sessions`, etc.): `LongType` (64-bit integer)
  - **Float Metrics** (`engagementRate`, `bounceRate`, etc.): `DoubleType` (64-bit float)
- **Validation**: The connector validates that requested dimensions and metrics exist in your property and don't exceed API limits, catching typos, non-existent fields, and limit violations before making data requests.

## Data Type Mapping

The connector automatically infers proper data types using the Google Analytics metadata API:

| GA4 API Type / Field | Example Fields                  | Connector Type | Example Values | Notes |
|----------------------|---------------------------------|----------------|----------------|-------|
| **Property ID** (added by connector) | property_id | StringType | "123456789" | **Always included** as the first field for schema stability |
| Dimension (any)  | country, city, deviceCategory   | StringType     | "United States", "desktop" | All non-date dimensions are strings |
| Date dimension   | date, firstSessionDate          | DateType       | 2025-12-24 | Parsed from YYYYMMDD format (e.g., "20251224" → date(2025, 12, 24)) |
| DateTime dimension | dateHour, dateHourMinute       | StringType     | "2024011514", "202401151430" | Kept as strings (contain time components that DateType cannot represent) |
| TYPE_INTEGER     | activeUsers, sessions, newUsers | LongType       | 1234 | 64-bit integers, parsed from API string responses |
| TYPE_FLOAT       | engagementRate, bounceRate      | DoubleType     | 56.78 | 64-bit floats, parsed from API string responses |
| TYPE_CURRENCY    | totalRevenue                    | DoubleType     | 1234.56 | Currency values as floats |
| TYPE_SECONDS, TYPE_MILLISECONDS | averageSessionDuration | LongType or DoubleType | Varies | Time durations parsed to numeric types |

### Type Inference Process

1. **Initialization**: Connector calls the Google Analytics `getMetadata` API once to retrieve type information for all dimensions and metrics in your property
2. **Caching**: Metadata is cached for the duration of the connector session (no repeated API calls)
3. **Schema Generation**: When generating a table schema:
   - The `property_id` field is always added first as StringType
   - The connector looks up each dimension and metric's type and maps it to the appropriate PySpark type
4. **Data Parsing**: When reading data:
   - The `property_id` is added to each record from the configuration
   - String values from the API are parsed to their target types (integers, floats, dates)

This ensures that your data arrives in Databricks with proper types and schema stability, ready for analytics without additional transformation.

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
          "start_date": "30daysAgo",
          "lookback_days": "3"
        }
      }
    }
  ]
}
```

- `connection_name` must point to the UC connection configured with your GA4 `property_ids` and `credentials_json`.
- For each report:
  - `source_table` - Give each report a **unique, descriptive name**
    - **For prebuilt reports**: Use the prebuilt report name (e.g., `"traffic_by_country"`)
    - **For custom reports**: Use any unique name you choose
  - `table_configuration` (optional for prebuilt, required for custom):
    - **Prebuilt**: Omit entirely for zero-config, or include to override defaults (start_date, lookback_days, filters)
    - **Custom**: `dimensions`, `metrics`, and optional settings (primary_keys are auto-inferred)
  
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
- Primary Keys: `["property_id", "date", "country"]`

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
      "start_date": "7daysAgo",
      "scd_type": "SCD_TYPE_1"
    }
  }
}
```

### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`403 Forbidden`)**:
  - Verify that the service account email has been added to all GA4 properties (listed in `property_ids`) with Viewer or Analyst role.
  - Check that the `property_ids` are correct and match the properties where access was granted.
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

**Multi-Property Considerations**:
- Each property has **independent quotas** - one property exhausting its quota does not affect others
- Total API load is the **sum** of requests across all properties
- Plan sync frequency and report complexity accordingly when using multiple properties

## Known Limitations

- **API Request Limits**: The Google Analytics Data API enforces the following limits per request:
  - **Maximum 9 dimensions** per report
  - **Maximum 10 metrics** per report
  - **Maximum 4 date ranges** per request (not user-configurable; connector uses 1 date range)
  - The connector validates dimensions and metrics limits before making API calls and provides clear error messages if exceeded
  - To work with more dimensions or metrics, split your report into multiple separate reports
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
- **Automatic Inference**: Zero additional API calls or configuration overhead
  - Primary keys automatically inferred from dimensions
  - Ingestion type automatically determined (cdc vs snapshot)
  - No redundant configuration needed
- **Validation**: Zero additional API calls (uses cached metadata)
  - Validates dimension/metric existence (catches typos)
  - Validates API limits (9 dimensions max, 10 metrics max)
  - Catches errors before making expensive data requests

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/google_analytics_aggregated/google_analytics_aggregated.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/google_analytics_aggregated/google_analytics_aggregated_api_doc.md`
- Official Google Analytics Data API documentation:
  - [Google Analytics Data API Overview](https://developers.google.com/analytics/devguides/reporting/data/v1)
  - [getMetadata Method Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/getMetadata)
  - [runReport Method Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport)
  - [Creating a Report Guide](https://developers.google.com/analytics/devguides/reporting/data/v1/basics)
  - [Dimensions & Metrics Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
  - [Quotas and Limits](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)
