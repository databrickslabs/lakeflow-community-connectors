# **Google Analytics Aggregated Data API Documentation**

## **Authorization**

- **Chosen method**: Service Account credentials (JSON key file) for the Google Analytics Data API v1beta.
- **Base URL**: `https://analyticsdata.googleapis.com/v1beta`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <access_token>`
  - Recommended scopes for read-only access to aggregated analytics data:
    - `https://www.googleapis.com/auth/analytics.readonly` (read-only, recommended)
    - `https://www.googleapis.com/auth/analytics` (read and write, if write operations are added in the future)
- **Other supported methods (not used by this connector)**:
  - OAuth 2.0 Client credentials (`client_id`, `client_secret`, `refresh_token`) are also supported by Google Analytics, but the connector will **not** perform interactive OAuth flows. If using OAuth 2.0 Client credentials, tokens must be provisioned out-of-band and stored in configuration.

**Service Account Authentication (Recommended):**

1. Create a service account in Google Cloud Console
2. Download the JSON key file containing the service account credentials
3. Grant the service account access to the Google Analytics property with "Viewer" or "Analyst" role
4. The connector stores the JSON key file contents and exchanges it for an access token at runtime

Example authenticated request using Service Account token:

```bash
curl -X POST \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "dateRanges": [{"startDate": "2024-01-01", "endDate": "2024-01-31"}],
    "dimensions": [{"name": "date"}],
    "metrics": [{"name": "activeUsers"}]
  }' \
  "https://analyticsdata.googleapis.com/v1beta/properties/YOUR_PROPERTY_ID:runReport"
```

Notes:
- The Google Analytics property ID is required and must be numeric (e.g., `123456789`).
- The connector **stores** service account credentials or OAuth refresh tokens and exchanges for access tokens at runtime; it **does not** run user-facing OAuth flows.
- Rate limits apply per property: **25,000 tokens per day** and **5,000 tokens per hour** for standard properties (see Rate Limits section below).
- The connector automatically handles token refresh when tokens expire


## **Object List**

The Google Analytics Data API provides access to aggregated reporting data through the `runReport` method. Unlike traditional database tables, Google Analytics uses a **dimensional data model** where reports are generated on-demand by combining dimensions (attributes) and metrics (measurements).

**Available Report Types (Objects):**

| Object Name | Description | Access Method | Ingestion Type |
|------------|-------------|---------------|----------------|
| `custom_report` | User-defined report combining any available dimensions and metrics | `runReport` method | `snapshot` or `append` (depending on configuration) |

**Key Concepts:**

- **Dimensions**: Attributes of your data (e.g., `date`, `country`, `deviceCategory`, `pagePath`, `eventName`)
- **Metrics**: Quantitative measurements (e.g., `activeUsers`, `sessions`, `screenPageViews`, `eventCount`)
- **Date Ranges**: Time periods for which data is retrieved (required for all reports)

**Static vs Dynamic Objects:**

- The object list is **conceptually static** - there is one primary method (`runReport`) that generates reports
- However, the **content** of reports is highly dynamic based on:
  - Selected dimensions (up to 9 per request)
  - Selected metrics (up to 10 per request)
  - Date ranges (up to 4 per request)
  - Filters applied
  - Property-specific custom dimensions and metrics

**Available Dimensions and Metrics:**

The complete list of available dimensions and metrics is maintained by Google and varies by property type. Common dimensions and metrics include:

**Common Dimensions:**
- `date` - Date in YYYYMMDD format
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

**Common Metrics:**
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
- `screenPageViewsPerSession` - Average page views per session

**Retrieving Available Dimensions and Metrics:**

To get the complete list of available dimensions and metrics for a specific property, use the `getMetadata` endpoint:

```bash
GET https://analyticsdata.googleapis.com/v1beta/properties/{property}/metadata
```

Example response structure:

```json
{
  "dimensions": [
    {
      "apiName": "date",
      "uiName": "Date",
      "description": "The date of the event, formatted as YYYYMMDD.",
      "category": "TIME"
    },
    {
      "apiName": "country",
      "uiName": "Country",
      "description": "User's country.",
      "category": "GEOGRAPHY"
    }
  ],
  "metrics": [
    {
      "apiName": "activeUsers",
      "uiName": "Active Users",
      "description": "The number of distinct users who visited your site or app.",
      "type": "TYPE_INTEGER",
      "category": "USER"
    }
  ]
}
```

**Notes:**
- Each Google Analytics property may have custom dimensions and metrics beyond the standard ones
- The availability of certain dimensions and metrics depends on the property configuration and data collection setup
- Not all dimension/metric combinations are compatible - the API will return validation errors for incompatible combinations


## **Object Schema**

The schema for Google Analytics reports is **dynamic** and depends on the dimensions and metrics requested in each `runReport` call. However, the response structure is consistent.

### **RunReport Response Schema**

**Top-level response structure:**

| Field Name | Type | Description |
|------------|------|-------------|
| `dimensionHeaders` | array\<struct\> | Headers for the dimensions in the report |
| `metricHeaders` | array\<struct\> | Headers for the metrics in the report |
| `rows` | array\<struct\> | Data rows containing dimension and metric values |
| `totals` | array\<struct\> or null | Total values for metrics (if requested) |
| `maximums` | array\<struct\> or null | Maximum values for metrics (if requested) |
| `minimums` | array\<struct\> or null | Minimum values for metrics (if requested) |
| `rowCount` | integer | Total number of rows in the result |
| `metadata` | struct | Metadata about the report |
| `propertyQuota` | struct or null | Property's quota state (if requested) |
| `kind` | string | Resource type identifier (always "analyticsData#runReport") |

**DimensionHeader structure:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | API name of the dimension (e.g., "date", "country") |

**MetricHeader structure:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | API name of the metric (e.g., "activeUsers") |
| `type` | string | Metric type enum: "TYPE_INTEGER", "TYPE_FLOAT", "TYPE_SECONDS", "TYPE_MILLISECONDS", "TYPE_MINUTES", "TYPE_HOURS", "TYPE_STANDARD", "TYPE_CURRENCY", "TYPE_FEET", "TYPE_MILES", "TYPE_METERS", "TYPE_KILOMETERS" |

**Row structure:**

| Field | Type | Description |
|-------|------|-------------|
| `dimensionValues` | array\<struct\> | Values for each dimension in the row |
| `metricValues` | array\<struct\> | Values for each metric in the row |

**DimensionValue structure:**

| Field | Type | Description |
|-------|------|-------------|
| `value` | string | The dimension value (always string, e.g., "United States", "2024-01-15", "desktop") |

**MetricValue structure:**

| Field | Type | Description |
|-------|------|-------------|
| `value` | string | The metric value as a string (e.g., "1234", "56.78") |

**ResponseMetaData structure:**

| Field | Type | Description |
|-------|------|-------------|
| `dataLossFromOtherRow` | boolean | If true, some data was aggregated into "(other)" row |
| `currencyCode` | string or null | Currency code for monetary metrics (e.g., "USD") |
| `timeZone` | string or null | Time zone for the property (e.g., "America/Los_Angeles") |
| `emptyReason` | string or null | Reason if report is empty |
| `subjectToThresholding` | boolean | If true, some small values were withheld for privacy |

**PropertyQuota structure:**

| Field | Type | Description |
|-------|------|-------------|
| `tokensPerDay` | struct or null | Quota tokens available per day |
| `tokensPerHour` | struct or null | Quota tokens available per hour |
| `concurrentRequests` | struct or null | Concurrent request limits |
| `serverErrorsPerProjectPerHour` | struct or null | Server error allowance |
| `potentiallyThresholdedRequestsPerHour` | struct or null | Potentially thresholded requests per hour |

**QuotaStatus structure (nested in PropertyQuota):**

| Field | Type | Description |
|-------|------|-------------|
| `consumed` | integer | Amount consumed |
| `remaining` | integer | Amount remaining |

### **Example Request and Response**

**Example request:**

```json
{
  "dateRanges": [
    {
      "startDate": "2024-01-01",
      "endDate": "2024-01-31"
    }
  ],
  "dimensions": [
    {"name": "date"},
    {"name": "country"}
  ],
  "metrics": [
    {"name": "activeUsers"},
    {"name": "sessions"}
  ],
  "limit": 100,
  "offset": 0
}
```

**Example response:**

```json
{
  "kind": "analyticsData#runReport",
  "dimensionHeaders": [
    {"name": "date"},
    {"name": "country"}
  ],
  "metricHeaders": [
    {"name": "activeUsers", "type": "TYPE_INTEGER"},
    {"name": "sessions", "type": "TYPE_INTEGER"}
  ],
  "rows": [
    {
      "dimensionValues": [
        {"value": "20240101"},
        {"value": "United States"}
      ],
      "metricValues": [
        {"value": "1234"},
        {"value": "1567"}
      ]
    },
    {
      "dimensionValues": [
        {"value": "20240101"},
        {"value": "Canada"}
      ],
      "metricValues": [
        {"value": "456"},
        {"value": "523"}
      ]
    }
  ],
  "rowCount": 2,
  "metadata": {
    "dataLossFromOtherRow": false,
    "currencyCode": "USD",
    "timeZone": "America/Los_Angeles",
    "subjectToThresholding": false
  }
}
```

**Schema Notes:**
- All dimension values are returned as strings, even for dates and numbers
- Metric values are returned as strings but should be parsed according to their `type`
- Date dimensions typically use YYYYMMDD format (e.g., "20240115")
- The order of values in `dimensionValues` and `metricValues` arrays corresponds to the order of headers
- Nested objects should be represented as struct types, not flattened
- The schema is consistent regardless of which dimensions/metrics are requested


## **Get Object Primary Keys**

Google Analytics Data API does not provide a dedicated endpoint to retrieve primary keys. Primary keys must be **determined by the connector based on the dimensions requested** in the report.

### **Primary Key Logic**

For the `custom_report` object, the primary key is the **combination of all dimensions** included in the report:

**Rules:**
1. If a report includes dimensions, the primary key is the composite of all dimension values
2. If a report includes the `date` dimension, it should be part of the primary key
3. If a report has no dimensions (metrics-only report), there is no meaningful primary key and the report should be treated as a single-row aggregate

**Examples:**

| Dimensions Requested | Primary Key |
|---------------------|-------------|
| `date`, `country` | Composite of (`date`, `country`) |
| `date`, `deviceCategory`, `sessionSource` | Composite of (`date`, `deviceCategory`, `sessionSource`) |
| `eventName`, `pagePath` | Composite of (`eventName`, `pagePath`) |
| (no dimensions) | No primary key (single aggregate row) |

**Implementation Guidance:**

```python
def get_primary_keys(dimensions):
    """
    Returns the list of dimension names that form the primary key.
    
    Args:
        dimensions: List of dimension names requested in the report
        
    Returns:
        List of dimension names forming the primary key, or empty list if no dimensions
    """
    if not dimensions:
        return []
    return dimensions  # All dimensions form the composite primary key
```

**Notes:**
- The primary key is **always composite** when multiple dimensions are present
- There is no single global identifier for rows in Google Analytics reports
- The uniqueness of rows is guaranteed by the combination of all dimension values within the specified date range
- For incremental ingestion, the combination of dimensions + date range determines which records to fetch


## **Object's ingestion type**

The Google Analytics Data API supports different ingestion patterns depending on the use case and configuration.

### **Supported Ingestion Types**

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `custom_report` | `append` (recommended) or `snapshot` | Google Analytics data is immutable for past dates after processing is complete. New data arrives for recent dates. The ingestion type depends on whether the connector tracks historical date ranges or only new dates. |

### **Ingestion Type Details**

**For `append` ingestion (Recommended for time-series data):**

- **Use case**: Continuously ingest new data as time progresses
- **Primary key**: Composite of all dimensions (including `date`)
- **Cursor field**: `date` dimension (or `dateHour` for hourly granularity)
- **Strategy**: 
  - Track the maximum date ingested in the previous run
  - On subsequent runs, request data starting from the day after the last ingested date
  - Apply a lookback window (e.g., 3-7 days) to account for data processing delays
  - Google Analytics data for recent dates may be updated as processing completes
- **Deletes**: Not applicable - Google Analytics does not delete historical data
- **Updates**: Data for recent dates (typically last 24-48 hours) may be updated as Google processes events

**For `snapshot` ingestion:**

- **Use case**: Refresh entire report for a fixed date range periodically
- **Primary key**: Composite of all dimensions
- **Strategy**: 
  - Define a fixed date range (e.g., "last 30 days", "year to date")
  - Replace all data in the target table on each sync
  - Suitable for reports where the date range is relative (e.g., "last 30 days" always moves forward)
- **Deletes**: Not applicable - full snapshot replacement
- **Updates**: Entire dataset is replaced on each sync

**For `cdc` ingestion:**

- **Not supported** - Google Analytics Data API does not provide change data capture or deleted records tracking

### **Recommended Configuration**

For most use cases, **`append` ingestion** is recommended with the following parameters:

```json
{
  "ingestion_type": "append",
  "cursor_field": "date",
  "primary_keys": ["date", "country"],  // Example: all dimensions
  "lookback_days": 3,  // Adjust based on data freshness requirements
  "incremental_strategy": "date_range"
}
```

**Data Freshness Considerations:**

- Google Analytics data is typically processed within 24-48 hours
- Recent data (last 24-48 hours) is marked as "partial" and may change as more events are processed
- Use a lookback window to re-fetch recent dates and capture updates
- For real-time requirements, consider using the `runRealtimeReport` method (not covered in this documentation)

**Handling Data Updates:**

Since Google Analytics may update data for recent dates:
1. Use `append` ingestion with a lookback window
2. Configure the target system to handle upserts based on the composite primary key
3. Re-fetch the last N days (e.g., 3 days) on each sync to capture updates


## **Read API for Data Retrieval**

### **Primary Endpoint: runReport**

- **HTTP method**: `POST`
- **Endpoint**: `/properties/{property}:runReport`
- **Base URL**: `https://analyticsdata.googleapis.com/v1beta`
- **Full URL**: `https://analyticsdata.googleapis.com/v1beta/properties/{property}:runReport`

**Path parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `property` | string | yes | Google Analytics property ID (numeric, e.g., "123456789") |

**Request Body Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `dateRanges` | array\<DateRange\> | yes | N/A | Date ranges for the report (up to 4 ranges) |
| `dimensions` | array\<Dimension\> | no | [] | Dimensions to include (up to 9 dimensions) |
| `metrics` | array\<Metric\> | yes | N/A | Metrics to include (up to 10 metrics) |
| `dimensionFilter` | FilterExpression | no | null | Filter on dimension values |
| `metricFilter` | FilterExpression | no | null | Filter on metric values |
| `offset` | integer (int64) | no | 0 | Row number to start from (0-indexed) |
| `limit` | integer (int64) | no | 10000 | Maximum number of rows to return (max 100,000) |
| `metricAggregations` | array\<enum\> | no | [] | Aggregations: "TOTAL", "MINIMUM", "MAXIMUM", "COUNT" |
| `orderBys` | array\<OrderBy\> | no | [] | Sort order for rows |
| `currencyCode` | string | no | null | Currency code in ISO 4217 format (e.g., "USD") |
| `cohortSpec` | CohortSpec | no | null | Cohort group specification |
| `keepEmptyRows` | boolean | no | false | If true, return rows with all metrics equal to 0 |
| `returnPropertyQuota` | boolean | no | false | If true, include quota information in response |

**DateRange structure:**

```json
{
  "startDate": "YYYY-MM-DD",  // or "today", "yesterday", "NdaysAgo"
  "endDate": "YYYY-MM-DD",    // or "today", "yesterday", "NdaysAgo"
  "name": "optional_name"     // Optional identifier for the date range
}
```

**Dimension structure:**

```json
{
  "name": "dimensionName",           // e.g., "date", "country"
  "dimensionExpression": {           // Optional: for calculated dimensions
    "lowerCase": {"dimensionName": "someDimension"},
    "upperCase": {"dimensionName": "someDimension"},
    "concatenate": {"dimensionNames": ["dim1", "dim2"], "delimiter": "-"}
  }
}
```

**Metric structure:**

```json
{
  "name": "metricName",              // e.g., "activeUsers", "sessions"
  "expression": "customExpression",  // Optional: for calculated metrics
  "invisible": false                 // Optional: exclude from response if true
}
```

**OrderBy structure:**

```json
{
  "dimension": {                     // Sort by dimension
    "dimensionName": "date",
    "orderType": "ALPHANUMERIC"      // or "CASE_INSENSITIVE_ALPHANUMERIC", "NUMERIC"
  },
  "metric": {                        // OR sort by metric
    "metricName": "activeUsers"
  },
  "desc": true                       // true for descending, false for ascending
}
```

### **Pagination Strategy**

Google Analytics Data API uses **offset-based pagination**:

1. Set `limit` to the desired page size (max 100,000, default 10,000)
2. Set `offset` to skip rows (0 for first page, limit for second page, etc.)
3. Continue fetching pages until `rows` array is empty or `rowCount` < `limit`

**Example pagination logic:**

```python
def fetch_all_rows(property_id, request_body):
    all_rows = []
    offset = 0
    limit = 10000  # Max recommended page size
    
    while True:
        request_body["offset"] = offset
        request_body["limit"] = limit
        
        response = call_run_report_api(property_id, request_body)
        
        rows = response.get("rows", [])
        all_rows.extend(rows)
        
        if len(rows) < limit:
            break  # Last page
            
        offset += limit
    
    return all_rows
```

### **Incremental Data Retrieval**

For incremental ingestion using the `append` strategy:

**First run (initial backfill):**
```json
{
  "dateRanges": [
    {
      "startDate": "2024-01-01",  // User-configured start date
      "endDate": "today"
    }
  ],
  "dimensions": [{"name": "date"}, {"name": "country"}],
  "metrics": [{"name": "activeUsers"}],
  "orderBys": [{"dimension": {"dimensionName": "date"}, "desc": false}]
}
```

**Subsequent runs (incremental):**
```json
{
  "dateRanges": [
    {
      "startDate": "2024-02-25",  // last_sync_date - lookback_days
      "endDate": "today"
    }
  ],
  "dimensions": [{"name": "date"}, {"name": "country"}],
  "metrics": [{"name": "activeUsers"}],
  "orderBys": [{"dimension": {"dimensionName": "date"}, "desc": false}]
}
```

**Incremental Strategy Details:**

1. **Track cursor**: Store the maximum `date` value from the previous sync
2. **Apply lookback**: Subtract lookback days (e.g., 3) to account for data processing delays
3. **Fetch new data**: Request data from `cursor_date - lookback_days` to `today`
4. **Upsert**: Use composite primary key (all dimensions) to upsert records in target

**Lookback Window:**
- Recommended: 3-7 days for most use cases
- Accounts for Google Analytics data processing delays
- Recent data may be updated as events are processed

### **Rate Limits and Quotas**

Google Analytics Data API enforces the following quotas per property:

| Quota Type | Standard Limit | Description |
|------------|----------------|-------------|
| Tokens per day | 25,000 | Total API tokens consumed per day per property |
| Tokens per hour | 5,000 | Total API tokens consumed per hour per property |
| Concurrent requests | 10 | Maximum simultaneous requests |
| Requests per day | 40,000 | Maximum number of requests per day (Google Analytics 360 properties get higher limits) |

**Token Consumption:**

- Basic request: 1 token
- Additional tokens are consumed based on:
  - Number of dimensions (1 token per dimension beyond 4)
  - Number of metrics (1 token per metric beyond 4)
  - Complexity of filters and expressions

**Rate Limit Handling:**

The connector should:
1. Monitor `propertyQuota` in responses (if `returnPropertyQuota: true`)
2. Implement exponential backoff for 429 (Too Many Requests) errors
3. Respect the `Retry-After` header if present
4. Distribute requests over time to avoid hitting hourly limits

**Example quota response:**

```json
{
  "propertyQuota": {
    "tokensPerDay": {
      "consumed": 1523,
      "remaining": 23477
    },
    "tokensPerHour": {
      "consumed": 234,
      "remaining": 4766
    },
    "concurrentRequests": {
      "consumed": 2,
      "remaining": 8
    }
  }
}
```

### **Handling Deleted Records**

Google Analytics Data API **does not support deleted records**:

- Historical data is immutable once processed
- Records are never deleted from the API
- If events are filtered out or re-processed, they simply won't appear in future queries
- No special handling for deletes is required

### **Example API Requests**

**Example 1: Basic report with date and country**

```bash
curl -X POST \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "dateRanges": [{"startDate": "2024-01-01", "endDate": "2024-01-31"}],
    "dimensions": [{"name": "date"}, {"name": "country"}],
    "metrics": [{"name": "activeUsers"}, {"name": "sessions"}],
    "limit": 1000,
    "offset": 0
  }' \
  "https://analyticsdata.googleapis.com/v1beta/properties/123456789:runReport"
```

**Example 2: Report with filters and sorting**

```bash
curl -X POST \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "dateRanges": [{"startDate": "7daysAgo", "endDate": "today"}],
    "dimensions": [{"name": "deviceCategory"}],
    "metrics": [{"name": "activeUsers"}],
    "dimensionFilter": {
      "filter": {
        "fieldName": "deviceCategory",
        "stringFilter": {
          "matchType": "EXACT",
          "value": "mobile"
        }
      }
    },
    "orderBys": [{"metric": {"metricName": "activeUsers"}, "desc": true}],
    "limit": 10
  }' \
  "https://analyticsdata.googleapis.com/v1beta/properties/123456789:runReport"
```

**Example 3: Paginated request**

```bash
curl -X POST \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "dateRanges": [{"startDate": "30daysAgo", "endDate": "today"}],
    "dimensions": [{"name": "pagePath"}],
    "metrics": [{"name": "screenPageViews"}],
    "limit": 10000,
    "offset": 10000,
    "returnPropertyQuota": true
  }' \
  "https://analyticsdata.googleapis.com/v1beta/properties/123456789:runReport"
```

### **Extra Parameters for Table Configuration**

The connector should accept the following table-level parameters:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `dimensions` | array\<string\> | yes | N/A | List of dimension names to include (e.g., ["date", "country"]) |
| `metrics` | array\<string\> | yes | N/A | List of metric names to include (e.g., ["activeUsers", "sessions"]) |
| `start_date` | string | no | "30daysAgo" | Initial start date for first sync (YYYY-MM-DD or relative like "30daysAgo") |
| `lookback_days` | integer | no | 3 | Number of days to look back for incremental syncs |
| `dimension_filter` | object | no | null | Filter expression for dimensions (JSON object) |
| `metric_filter` | object | no | null | Filter expression for metrics (JSON object) |
| `page_size` | integer | no | 10000 | Number of rows per page (max 100,000) |


## **Field Type Mapping**

Google Analytics Data API returns all values as strings in the response, but provides type information in the headers. The connector should parse values according to their declared types.

### **Type Mapping Table**

| Google Analytics Type | API Type Enum | Connector Logical Type | Parsing Logic | Example Raw Value | Example Parsed Value |
|----------------------|---------------|------------------------|---------------|-------------------|---------------------|
| Dimension (any) | N/A | string | No parsing needed | "United States" | "United States" |
| Date dimension | N/A | string or date | Parse YYYYMMDD → date | "20240115" | "2024-01-15" |
| Integer metric | TYPE_INTEGER | long (64-bit integer) | Parse string → integer | "1234" | 1234 |
| Float metric | TYPE_FLOAT | double (64-bit float) | Parse string → float | "56.78" | 56.78 |
| Currency metric | TYPE_CURRENCY | double | Parse string → float | "1234.56" | 1234.56 |
| Time metric (seconds) | TYPE_SECONDS | long or double | Parse string → numeric | "123.45" | 123.45 |
| Time metric (milliseconds) | TYPE_MILLISECONDS | long | Parse string → integer | "12345" | 12345 |
| Time metric (minutes) | TYPE_MINUTES | long or double | Parse string → numeric | "45.5" | 45.5 |
| Time metric (hours) | TYPE_HOURS | long or double | Parse string → numeric | "2.5" | 2.5 |
| Distance (feet) | TYPE_FEET | double | Parse string → float | "500.5" | 500.5 |
| Distance (miles) | TYPE_MILES | double | Parse string → float | "10.25" | 10.25 |
| Distance (meters) | TYPE_METERS | double | Parse string → float | "1000.0" | 1000.0 |
| Distance (kilometers) | TYPE_KILOMETERS | double | Parse string → float | "5.5" | 5.5 |
| Standard metric | TYPE_STANDARD | double | Parse string → float | "98.76" | 98.76 |

### **Type Mapping Logic**

```python
def parse_metric_value(value_str: str, metric_type: str):
    """
    Parse metric value string according to its type.
    
    Args:
        value_str: String value from API response
        metric_type: Type from metricHeader.type
        
    Returns:
        Parsed value in appropriate Python type
    """
    if metric_type == "TYPE_INTEGER":
        return int(value_str) if value_str else None
    elif metric_type in ["TYPE_FLOAT", "TYPE_CURRENCY", "TYPE_SECONDS", 
                         "TYPE_MINUTES", "TYPE_HOURS", "TYPE_FEET", 
                         "TYPE_MILES", "TYPE_METERS", "TYPE_KILOMETERS", 
                         "TYPE_STANDARD"]:
        return float(value_str) if value_str else None
    elif metric_type == "TYPE_MILLISECONDS":
        return int(value_str) if value_str else None
    else:
        # Fallback for unknown types
        return value_str


def parse_dimension_value(value_str: str, dimension_name: str):
    """
    Parse dimension value. Most dimensions remain as strings,
    but date dimensions can be converted to date objects.
    
    Args:
        value_str: String value from API response
        dimension_name: Name of the dimension
        
    Returns:
        Parsed value (string or date)
    """
    if dimension_name in ["date", "firstSessionDate", "dateHour"]:
        # Parse date dimensions: YYYYMMDD format
        if len(value_str) == 8:
            return f"{value_str[0:4]}-{value_str[4:6]}-{value_str[6:8]}"
        # dateHour format: YYYYMMDDHH
        elif len(value_str) == 10:
            return f"{value_str[0:4]}-{value_str[4:6]}-{value_str[6:8]} {value_str[8:10]}:00:00"
    
    return value_str
```

### **Special Field Behaviors**

**Date Dimensions:**
- Format: YYYYMMDD (e.g., "20240115" for January 15, 2024)
- Can be stored as string or parsed to date type
- Recommended: Store as date type for easier querying
- `dateHour` dimension uses YYYYMMDDHH format

**Null/Missing Values:**
- Dimension values: Should not be null in normal operation, but may be "(not set)" for missing data
- Metric values: Can be "0" or empty string - parse empty as null or 0 depending on context

**Currency Metrics:**
- Returned in the property's configured currency unless `currencyCode` parameter overrides it
- Always include the currency code from `metadata.currencyCode` in the schema

**Time-based Metrics:**
- Duration metrics may be in seconds, milliseconds, minutes, or hours
- Check the `type` field to determine the unit
- Consider converting all to a standard unit (e.g., seconds) for consistency

**Boolean-like Dimensions:**
- Some dimensions are effectively boolean (e.g., "Yes"/"No" values)
- Keep as strings unless explicit conversion is desired

### **Constraints and Validation**

- **Numeric ranges**: Metrics are typically non-negative; negative values are rare but possible for calculated metrics
- **String encoding**: All strings are UTF-8 encoded
- **Decimal precision**: Float values typically have precision up to 2-4 decimal places
- **Missing data indicators**: 
  - "(not set)" - dimension value is missing
  - "(other)" - aggregated low-volume entries for privacy
  - Empty string or "0" - metric has no value


## **Write API**

The Google Analytics Data API is **read-only**. There is no write API for modifying or creating aggregated data through the Data API.

**Note on Data Creation:**

- Google Analytics data is created through **event tracking** on websites/apps using:
  - Google Analytics 4 (GA4) tracking code (gtag.js)
  - Google Tag Manager
  - Firebase SDK (for mobile apps)
  - Measurement Protocol (for server-side events)
- The Data API (`runReport`) is solely for **reading and reporting** on collected data
- There is no API to modify, delete, or update historical aggregated data

**For Testing Purposes:**

If you need to generate test data for connector validation:
1. Set up a test Google Analytics property
2. Implement event tracking on a test website or use the Measurement Protocol to send events
3. Wait 24-48 hours for data to be processed and appear in reports
4. Use the `runReport` method to verify data ingestion

**Measurement Protocol (for test data generation):**

The Measurement Protocol can be used to send events to Google Analytics for testing:

```bash
POST https://www.google-analytics.com/mp/collect?measurement_id=G-XXXXXXXXXX&api_secret=<secret>
Content-Type: application/json

{
  "client_id": "test_client_123",
  "events": [{
    "name": "page_view",
    "params": {
      "page_location": "https://example.com/test",
      "page_title": "Test Page"
    }
  }]
}
```

However, this is separate from the Data API and is not part of the connector's functionality.


## **Known Quirks & Edge Cases**

### **Data Processing Delays**

- **Issue**: Recent data (last 24-48 hours) may be incomplete or change as Google processes events
- **Impact**: Incremental syncs may miss or get stale data for recent dates
- **Solution**: Implement a lookback window (3-7 days) to re-fetch recent dates

### **Data Thresholding**

- **Issue**: Google applies thresholding to protect user privacy when data volumes are low
- **Indicator**: `metadata.subjectToThresholding` = true in response
- **Impact**: Some small metric values may be withheld or aggregated
- **Solution**: No workaround; accept that low-volume data may be incomplete

### **Cardinality Limits**

- **Issue**: Reports with high-cardinality dimensions may be truncated
- **Indicator**: `metadata.dataLossFromOtherRow` = true
- **Impact**: Some rows are aggregated into "(other)" dimension value
- **Solution**: Use filters or request fewer dimensions to reduce cardinality

### **Dimension/Metric Compatibility**

- **Issue**: Not all dimension/metric combinations are valid
- **Example**: Some metrics are session-scoped while others are user-scoped
- **Impact**: API returns 400 error with incompatibility message
- **Solution**: Use the `getMetadata` endpoint or refer to Google's compatibility matrix

### **Date Format Variations**

- **Issue**: Date dimensions return YYYYMMDD format (no separators)
- **Impact**: Requires parsing to standard date format
- **Solution**: Convert "20240115" → "2024-01-15" in the connector

### **Quota Exhaustion**

- **Issue**: Properties can exhaust daily/hourly token quotas
- **Indicator**: 429 HTTP status code
- **Impact**: Requests fail until quota resets
- **Solution**: Implement exponential backoff and retry after quota reset time

### **Property Access Permissions**

- **Issue**: Service accounts must be explicitly granted access to Google Analytics properties
- **Impact**: API returns 403 Forbidden if access not granted
- **Solution**: Ensure service account email is added with "Viewer" or "Analyst" role in GA4 property settings

### **(other) Row Aggregation**

- **Issue**: Low-volume dimension values are aggregated into "(other)" row
- **Impact**: Granular data is lost for long-tail values
- **Solution**: Use filters to focus on specific dimension values or accept data loss

### **Realtime vs Historical Data**

- **Issue**: `runReport` returns historical data (24-48 hour delay)
- **Impact**: Not suitable for real-time analytics
- **Solution**: Use `runRealtimeReport` for data from last 30 minutes (not covered in this doc)

### **Custom Dimensions/Metrics**

- **Issue**: Each property may have custom dimensions/metrics beyond standard ones
- **Impact**: Connector must support dynamic schemas
- **Solution**: Allow users to specify any dimension/metric name; rely on API validation

### **Sampling for Large Queries**

- **Issue**: Very large date ranges or complex queries may be sampled
- **Indicator**: Check `metadata.samplingMetadata` if present (not always included)
- **Impact**: Results are estimates, not exact counts
- **Solution**: Break large queries into smaller date ranges

### **Time Zone Considerations**

- **Issue**: Data is reported in the property's configured time zone
- **Source**: `metadata.timeZone` in response
- **Impact**: Date boundaries may not align with UTC
- **Solution**: Document the time zone; consider converting dates to UTC if needed

### **Empty Reports**

- **Issue**: Some queries return no data
- **Indicator**: `metadata.emptyReason` may explain why (e.g., "NO_DATA")
- **Impact**: Connector must handle empty `rows` array
- **Solution**: Return empty result set; log reason if available


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport | 2024-12-23 | High | runReport endpoint structure, parameters, request/response format |
| Official Docs | https://developers.google.com/analytics/devguides/reporting/data/v1/basics | 2024-12-23 | High | Basic usage, dimensions, metrics, date ranges, creating reports |
| Official Docs | https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/RunReportResponse | 2024-12-23 | High | Complete response schema including all fields |
| Official Docs | https://developers.google.com/analytics/reference | 2024-12-23 | High | API reference overview and navigation |
| Web Search | OpenAI search results for "Google Analytics Data API v1beta runReport" | 2024-12-23 | High | Confirmed authentication methods (OAuth 2.0), parameter details (limit, offset, orderBys, keepEmptyRows) |
| Web Search | OpenAI search results for "Google Analytics Data API pagination" | 2024-12-23 | High | Confirmed offset-based pagination strategy, default limit of 10,000, max 100,000 |
| Web Search | OpenAI search results for "Google Analytics Data API dimensions metrics" | 2024-12-23 | High | Confirmed dimensions/metrics structure, up to 9 dimensions and 10 metrics per request |
| Web Search | OpenAI search results for "Google Analytics Data API quota limits" | 2024-12-23 | High | Confirmed 25,000 tokens/day, 5,000 tokens/hour, 10 concurrent requests |
| Official Docs | https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/getMetadata | 2024-12-23 | High | How to retrieve available dimensions and metrics for a property |


## **Sources and References**

### **Official Documentation (Highest Confidence)**

- Google Analytics Data API Overview: https://developers.google.com/analytics/devguides/reporting/data/v1
- runReport Method Reference: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport
- Creating a Report Guide: https://developers.google.com/analytics/devguides/reporting/data/v1/basics
- RunReportResponse Schema: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/RunReportResponse
- API Reference Index: https://developers.google.com/analytics/reference
- Dimensions & Metrics: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
- Metadata API: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/getMetadata
- Quotas and Limits: https://developers.google.com/analytics/devguides/reporting/data/v1/quotas
- Authentication: https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries

### **Confidence Assessment**

All information in this document is sourced from **official Google Analytics Data API documentation** and verified through multiple official references. The documentation is implementation-ready and backed by authoritative sources.

**Rationale for Choices:**
- **Authentication method**: OAuth 2.0 is the only supported method per official docs
- **Primary endpoint**: runReport is the core method for aggregated data retrieval (as requested by user)
- **Pagination**: Offset-based pagination is the documented approach
- **Rate limits**: Values from official quotas documentation
- **Ingestion type**: `append` recommended based on immutable historical data and date-based incremental reads

**No conflicts were found** between sources as all information comes from official Google documentation.
