# Lakeflow Klaviyo Connector

The Lakeflow Klaviyo Connector allows you to extract marketing data from your Klaviyo account and load it into your data lake or warehouse. This connector supports incremental synchronization for profiles and events, enabling efficient data ingestion for marketing analytics.

## Set up

### Prerequisites
- A Klaviyo account with API access
- A Private API Key with appropriate read scopes

### Required Parameters

To configure the Klaviyo connector, you'll need to provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_key` | string | Yes | Klaviyo Private API Key | `pk_abc123...` |

### Getting Your Klaviyo API Key

1. Log in to your Klaviyo account
2. Navigate to **Settings** → **API Keys** (or **Account** → **Settings** → **API Keys**)
3. Click **Create Private API Key**
4. Select the required scopes for your use case:
   - `profiles:read` - Read customer profiles
   - `events:read` - Read customer events
   - `lists:read` - Read marketing lists
   - `campaigns:read` - Read campaign data
   - `metrics:read` - Read event metrics
   - `flows:read` - Read automated flows
   - `segments:read` - Read audience segments
   - `templates:read` - Read email templates
5. Copy your API key (starts with `pk_`)
6. **Important**: Keep your private key secure - never share it publicly

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.

The connection can also be created using the standard Unity Catalog API.

## Objects Supported

The Klaviyo connector supports the following tables/objects:

| Table Name | Primary Key | Ingestion Type | Cursor Field | Description |
|------------|-------------|----------------|--------------|-------------|
| `profiles` | `id` | CDC | `updated` | Customer/subscriber profiles |
| `events` | `id` | Append | `datetime` | Customer actions and activities |
| `lists` | `id` | Snapshot | N/A | Marketing list definitions |
| `campaigns` | `id` | CDC | `updated_at` | Email/SMS campaign metadata |
| `metrics` | `id` | Snapshot | N/A | Event type definitions |
| `flows` | `id` | CDC | `updated` | Automated flow configurations |
| `segments` | `id` | Snapshot | N/A | Dynamic audience segments |
| `templates` | `id` | CDC | `updated` | Email template definitions |

> **Note**: Table names are case-sensitive. Use the exact names shown above (lowercase).

### Object Details

#### `profiles`
- **Primary Key**: `id`
- **Ingestion Type**: CDC (Change Data Capture)
- **Cursor Field**: `updated`
- **Description**: Customer/subscriber profiles containing contact information, subscription status, custom properties, and predictive analytics.
- **Key Fields**:
  - `id`: Unique Klaviyo profile identifier
  - `email`: Email address
  - `phone_number`: Phone number with country code
  - `external_id`: Your system's customer ID
  - `first_name`, `last_name`: Name fields
  - `location`: Nested address and geographic data
  - `properties`: Custom properties (stored as MapType)
  - `subscriptions`: Email and SMS subscription status
  - `predictive_analytics`: Customer lifetime value predictions
  - `created`, `updated`: Timestamps

#### `events`
- **Primary Key**: `id`
- **Ingestion Type**: Append (events are immutable)
- **Cursor Field**: `datetime`
- **Description**: Customer actions including purchases, email opens, clicks, page views, and custom events.
- **Key Fields**:
  - `id`: Unique event identifier
  - `metric_id`: Reference to the metric/event type
  - `profile_id`: Reference to the customer profile
  - `timestamp`, `datetime`: When the event occurred
  - `event_properties`: Event-specific data (varies by event type, stored as MapType)
  - `uuid`: Event UUID

#### `lists`
- **Primary Key**: `id`
- **Ingestion Type**: Snapshot
- **Description**: Marketing list definitions for organizing subscribers.
- **Key Fields**:
  - `id`: Unique list identifier
  - `name`: List name
  - `opt_in_process`: Single or double opt-in
  - `created`, `updated`: Timestamps

#### `campaigns`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `updated_at`
- **Description**: Email and SMS campaign metadata and configuration.
- **Key Fields**:
  - `id`: Unique campaign identifier
  - `name`: Campaign name
  - `status`: Draft, scheduled, sent, or cancelled
  - `audiences`: Target audience configuration
  - `send_options`: Smart sending and transactional settings
  - `tracking_options`: Click and open tracking settings
  - `send_strategy`: Immediate, scheduled, or smart send time
  - `created_at`, `updated_at`, `scheduled_at`, `send_time`: Timestamps

#### `metrics`
- **Primary Key**: `id`
- **Ingestion Type**: Snapshot
- **Description**: Definitions of all tracked event types in your account (e.g., "Placed Order", "Opened Email").
- **Key Fields**:
  - `id`: Unique metric identifier
  - `name`: Metric name
  - `integration`: Source integration information
  - `created`, `updated`: Timestamps

#### `flows`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `updated`
- **Description**: Automated marketing flow definitions and configurations.
- **Key Fields**:
  - `id`: Unique flow identifier
  - `name`: Flow name
  - `status`: Draft, manual, or live
  - `archived`: Whether the flow is archived
  - `trigger_type`: What triggers the flow (list, segment, metric, etc.)
  - `created`, `updated`: Timestamps

#### `segments`
- **Primary Key**: `id`
- **Ingestion Type**: Snapshot
- **Description**: Dynamic audience segment definitions.
- **Key Fields**:
  - `id`: Unique segment identifier
  - `name`: Segment name
  - `definition`: Segment conditions (stored as MapType)
  - `is_active`, `is_processing`, `is_starred`: Status flags
  - `created`, `updated`: Timestamps

#### `templates`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `updated`
- **Description**: Email template definitions with HTML and text content.
- **Key Fields**:
  - `id`: Unique template identifier
  - `name`: Template name
  - `editor_type`: CODE, DRAG_AND_DROP, or HYBRID
  - `html`: HTML content
  - `text`: Plain text version
  - `created`, `updated`: Timestamps

## Incremental Sync Strategy

### Change Data Capture (CDC)
For `profiles`, `campaigns`, `flows`, and `templates`, the connector uses timestamp-based incremental sync:

1. **Initial Sync**: Fetches all records from Klaviyo
2. **Subsequent Syncs**: Fetches only records updated after the last checkpoint using the cursor field
3. **Cursor Tracking**: Automatically tracks the latest timestamp for efficient updates

### Append-Only (Events)
For `events`, the connector uses append-only sync:

1. **Initial Sync**: Fetches all events from Klaviyo
2. **Subsequent Syncs**: Fetches only events created after the last checkpoint
3. Events are immutable once created

### Snapshot (Lists, Metrics, Segments)
For `lists`, `metrics`, and `segments`, the connector performs a full refresh on each sync. These are typically small metadata tables that don't change frequently.

## Data Type Mapping

| Klaviyo Type | Spark Type | Notes |
|--------------|------------|-------|
| String ID | StringType | Klaviyo IDs are 26-character base62 strings |
| String | StringType | Text fields |
| Boolean | BooleanType | Status flags |
| ISO 8601 DateTime | StringType | Timestamps stored as strings |
| Number (Integer) | LongType | Numeric values |
| Number (Decimal) | DoubleType | Floating-point values |
| Object | StructType or MapType | Nested structures or flexible dictionaries |
| Array | ArrayType | Lists of values |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Example configuration:

```json
{
  "pipeline_spec": {
    "connection_name": "your_klaviyo_connection",
    "object": [
      {
        "table": {
          "source_table": "profiles"
        }
      },
      {
        "table": {
          "source_table": "events"
        }
      },
      {
        "table": {
          "source_table": "lists"
        }
      }
    ]
  }
}
```

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline
- **Use Incremental Sync**: Profiles and events support incremental sync for efficiency
- **Set Appropriate Schedules**: Balance data freshness requirements with API usage limits
- **Monitor Rate Limits**: Klaviyo has per-account rate limits with burst and steady windows

## Implementation Details

### API Endpoints Used
The connector uses the Klaviyo REST API v2024-10-15:
- **Base URL**: `https://a.klaviyo.com/api`
- **Pagination**: Cursor-based pagination via `page[cursor]` parameter
- **Filtering**: JSON:API filter syntax for incremental reads
- **Page Size**: Up to 100 records per page

Examples:
- Profiles: `GET /api/profiles/`
- Events: `GET /api/events/`
- Lists: `GET /api/lists/`
- Campaigns: `GET /api/campaigns/`

### Rate Limiting
- Klaviyo uses fixed-window rate limiting with burst and steady windows
- The connector implements 0.15-second delays between requests
- On HTTP 429 responses, the connector waits and retries with exponential backoff
- Rate limit headers are returned in responses:
  - `X-RateLimit-Limit-Burst`
  - `X-RateLimit-Remaining-Burst`
  - `X-RateLimit-Limit-Steady`
  - `X-RateLimit-Remaining-Steady`

### JSON:API Format
Klaviyo uses JSON:API format. The connector automatically:
- Extracts `attributes` from each record
- Adds the `id` field to the top level
- Extracts relationship IDs when present

## Troubleshooting

### Common Issues

**Authentication Errors**:
- Verify API key is correct and starts with `pk_`
- Ensure the API key has the required read scopes
- Check that the secret scope and key name match your configuration

**Missing Data**:
- Verify the data exists in Klaviyo
- Confirm incremental cursor is advancing properly
- Check if data was created/updated within the sync window

**Rate Limiting**:
- Reduce sync frequency
- The connector automatically handles rate limit responses
- Contact Klaviyo support to understand your account's rate limits

**Schema Mismatches**:
- Ensure connector version matches your Klaviyo API version
- Custom properties are stored as MapType - parse them in downstream transformations
- Check for null values in expected fields

### Error Handling
The connector includes built-in error handling for:
- API authentication errors
- Rate limiting (automatic retry with backoff)
- Network connectivity issues
- Invalid table names

Check pipeline logs for detailed error information.

## API Documentation

For complete Klaviyo API documentation, see:
- [Klaviyo API Reference](https://developers.klaviyo.com/en/reference/api_overview)
- [Authentication Guide](https://developers.klaviyo.com/en/reference/api_overview#authentication)
- [Pagination Guide](https://developers.klaviyo.com/en/docs/pagination_)

### Object-Specific Documentation:
- [Profiles API](https://developers.klaviyo.com/en/reference/get_profiles)
- [Events API](https://developers.klaviyo.com/en/reference/get_events)
- [Lists API](https://developers.klaviyo.com/en/reference/get_lists)
- [Campaigns API](https://developers.klaviyo.com/en/reference/get_campaigns)
- [Metrics API](https://developers.klaviyo.com/en/reference/get_metrics)
- [Flows API](https://developers.klaviyo.com/en/reference/get_flows)
- [Segments API](https://developers.klaviyo.com/en/reference/get_segments)
- [Templates API](https://developers.klaviyo.com/en/reference/get_templates)

## Support

For issues or questions:
- Check Klaviyo API documentation at https://developers.klaviyo.com
- Review connector logs in Databricks
- Verify API key permissions in Klaviyo account settings

