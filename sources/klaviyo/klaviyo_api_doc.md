# **Klaviyo API Documentation**

## **Authorization**

- **Chosen method**: Private API Key authentication for server-side RESTful API requests.
- **Base URL**: `https://a.klaviyo.com/api`
- **API Version**: `2024-10-15` (latest stable version as of documentation date)
- **Auth placement**:
  - HTTP header: `Authorization: Klaviyo-API-Key <private_api_key>`
  - API version header: `revision: 2024-10-15`
  - Accept header: `Accept: application/json`
- **Required scopes**: API keys require specific scopes for each endpoint:
  - `profiles:read` - Read profiles data
  - `events:read` - Read events data
  - `lists:read` - Read lists data
  - `campaigns:read` - Read campaigns data
  - `metrics:read` - Read metrics data
  - `flows:read` - Read flows data
  - `segments:read` - Read segments data
  - `templates:read` - Read templates data

**Other supported methods (not used by this connector)**:
- OAuth: Recommended for tech partners, but connector will use Private API Key for simplicity.
- Public Key: For client-side APIs only, not applicable for server-side data ingestion.

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Klaviyo-API-Key pk_your_private_key" \
  -H "revision: 2024-10-15" \
  -H "Accept: application/json" \
  "https://a.klaviyo.com/api/profiles/"
```

Notes:
- Rate limiting is per-account with burst (short) and steady (long) windows.
- If rate limit is exceeded, HTTP 429 is returned.
- All endpoints use JSON:API format for requests and responses.


## **Object List**

For connector purposes, we treat specific Klaviyo REST resources as **objects/tables**.
The object list is **static** (defined by the connector), not discovered dynamically from an API.

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `profiles` | Customer/subscriber profiles with contact info and custom properties | `GET /api/profiles/` | `cdc` (upserts based on `updated`) |
| `events` | Customer actions (purchases, email opens, clicks, page views, custom events) | `GET /api/events/` | `append` (new events, ordered by `datetime`) |
| `lists` | Marketing lists and their definitions | `GET /api/lists/` | `snapshot` |
| `campaigns` | Email/SMS campaign definitions and metadata | `GET /api/campaigns/` | `cdc` (based on `updated_at`) |
| `metrics` | All tracked event types/metrics in the account | `GET /api/metrics/` | `snapshot` |
| `flows` | Automated flow definitions and configurations | `GET /api/flows/` | `cdc` (based on `updated`) |
| `segments` | Dynamic audience segments | `GET /api/segments/` | `snapshot` |
| `templates` | Email template definitions | `GET /api/templates/` | `cdc` (based on `updated`) |

**Connector scope for initial implementation**:
- Focuses on core marketing analytics objects: `profiles`, `events`, `lists`, `campaigns`, `metrics`, `flows`.
- `segments` and `templates` included for completeness.


## **Object Schema**

### General notes

- Klaviyo uses JSON:API format, where the response structure is:
  ```json
  {
    "data": [...],
    "links": {...}
  }
  ```
- Each resource object has `type`, `id`, and `attributes` fields.
- The connector extracts `attributes` and adds `id` as a top-level field.
- Nested JSON objects are preserved as nested structures.


### `profiles` object (primary table)

**Source endpoint**:
`GET /api/profiles/`

**Key behavior**:
- Returns customer/subscriber profiles with contact information and custom properties.
- Supports filtering by `updated` for incremental reads.
- Uses cursor-based pagination.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique Klaviyo profile ID (e.g., `01GDDKASAP8TKDDA2GRZDSVP4H`). |
| `email` | string or null | Email address of the profile. |
| `phone_number` | string or null | Phone number with country code. |
| `external_id` | string or null | External identifier from your system. |
| `first_name` | string or null | First name. |
| `last_name` | string or null | Last name. |
| `organization` | string or null | Organization/company name. |
| `locale` | string or null | Locale setting (e.g., `en-US`). |
| `title` | string or null | Job title. |
| `image` | string or null | URL to profile image. |
| `created` | string (ISO 8601 datetime) | Profile creation timestamp. |
| `updated` | string (ISO 8601 datetime) | Last update timestamp. Used as incremental cursor. |
| `last_event_date` | string (ISO 8601 datetime) or null | Timestamp of last event. |
| `location` | struct | Location information (see nested schema). |
| `properties` | struct | Custom properties dictionary. |
| `subscriptions` | struct | Subscription status for email and SMS. |
| `predictive_analytics` | struct or null | Predicted customer value and churn risk. |

**Nested `location` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `address1` | string or null | Street address line 1. |
| `address2` | string or null | Street address line 2. |
| `city` | string or null | City name. |
| `region` | string or null | State/region. |
| `zip` | string or null | Postal/zip code. |
| `country` | string or null | Country code (ISO 3166-1 alpha-2). |
| `latitude` | double or null | Latitude coordinate. |
| `longitude` | double or null | Longitude coordinate. |
| `timezone` | string or null | Timezone (e.g., `America/New_York`). |
| `ip` | string or null | IP address. |

**Nested `subscriptions` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `email` | struct | Email subscription status with `marketing` sub-object. |
| `sms` | struct | SMS subscription status with `marketing` sub-object. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Klaviyo-API-Key pk_your_private_key" \
  -H "revision: 2024-10-15" \
  -H "Accept: application/json" \
  "https://a.klaviyo.com/api/profiles/?page[size]=100"
```

**Example response (truncated)**:

```json
{
  "data": [
    {
      "type": "profile",
      "id": "01GDDKASAP8TKDDA2GRZDSVP4H",
      "attributes": {
        "email": "sarah.mason@example.com",
        "phone_number": "+15551234567",
        "external_id": "cust_12345",
        "first_name": "Sarah",
        "last_name": "Mason",
        "organization": null,
        "locale": "en-US",
        "title": null,
        "image": null,
        "created": "2023-01-15T10:30:00+00:00",
        "updated": "2024-06-20T14:22:00+00:00",
        "last_event_date": "2024-06-20T14:22:00+00:00",
        "location": {
          "address1": "123 Main St",
          "address2": null,
          "city": "Boston",
          "region": "MA",
          "zip": "02101",
          "country": "US",
          "latitude": null,
          "longitude": null,
          "timezone": "America/New_York",
          "ip": null
        },
        "properties": {
          "customer_tier": "gold",
          "total_orders": 15
        },
        "subscriptions": {
          "email": {
            "marketing": {
              "can_receive_email_marketing": true,
              "consent": "SUBSCRIBED",
              "consent_timestamp": "2023-01-15T10:30:00+00:00"
            }
          },
          "sms": {
            "marketing": {
              "can_receive_sms_marketing": false,
              "consent": "UNSUBSCRIBED"
            }
          }
        },
        "predictive_analytics": {
          "historic_clv": 450.00,
          "predicted_clv": 150.00,
          "total_clv": 600.00,
          "historic_number_of_orders": 15,
          "predicted_number_of_orders": 3,
          "average_days_between_orders": 30.0,
          "average_order_value": 30.0,
          "churn_probability": 0.15,
          "expected_date_of_next_order": "2024-07-20T00:00:00+00:00"
        }
      },
      "links": {
        "self": "https://a.klaviyo.com/api/profiles/01GDDKASAP8TKDDA2GRZDSVP4H/"
      }
    }
  ],
  "links": {
    "self": "https://a.klaviyo.com/api/profiles/",
    "next": "https://a.klaviyo.com/api/profiles/?page[cursor]=abc123"
  }
}
```


### `events` object

**Source endpoint**:
`GET /api/events/`

**Key behavior**:
- Returns customer events (actions like purchases, email opens, clicks).
- Supports filtering by `datetime` for incremental reads.
- Events are immutable once created (append-only).

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique event ID. |
| `metric_id` | string | ID of the metric (event type) this event belongs to. |
| `profile_id` | string | ID of the profile that triggered the event. |
| `timestamp` | string (ISO 8601 datetime) | When the event occurred. |
| `datetime` | string (ISO 8601 datetime) | Same as timestamp, used for cursor. |
| `event_properties` | struct | Event-specific data (varies by event type). |
| `uuid` | string | UUID of the event. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Klaviyo-API-Key pk_your_private_key" \
  -H "revision: 2024-10-15" \
  -H "Accept: application/json" \
  "https://a.klaviyo.com/api/events/?page[size]=100&sort=-datetime"
```

**Example response (truncated)**:

```json
{
  "data": [
    {
      "type": "event",
      "id": "4xzVj5",
      "attributes": {
        "metric_id": "UMTmhY",
        "profile_id": "01GDDKASAP8TKDDA2GRZDSVP4H",
        "timestamp": "2024-06-20T14:22:00+00:00",
        "datetime": "2024-06-20T14:22:00+00:00",
        "event_properties": {
          "$event_id": "ord_12345",
          "$value": 49.99,
          "items": [
            {"product_id": "SKU001", "quantity": 2}
          ]
        },
        "uuid": "550e8400-e29b-41d4-a716-446655440000"
      }
    }
  ],
  "links": {
    "self": "https://a.klaviyo.com/api/events/",
    "next": "https://a.klaviyo.com/api/events/?page[cursor]=xyz789"
  }
}
```


### `lists` object

**Source endpoint**:
`GET /api/lists/`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique list ID. |
| `name` | string | List name. |
| `created` | string (ISO 8601 datetime) | List creation timestamp. |
| `updated` | string (ISO 8601 datetime) | Last update timestamp. |
| `opt_in_process` | string | Opt-in type (`single_opt_in` or `double_opt_in`). |


### `campaigns` object

**Source endpoint**:
`GET /api/campaigns/`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique campaign ID. |
| `name` | string | Campaign name. |
| `status` | string | Campaign status (`draft`, `scheduled`, `sent`, `cancelled`). |
| `archived` | boolean | Whether the campaign is archived. |
| `audiences` | struct | Target audience configuration. |
| `send_options` | struct | Send configuration (use_smart_sending, etc.). |
| `tracking_options` | struct | Tracking settings. |
| `send_strategy` | struct | Sending strategy (immediate, scheduled, smart send time). |
| `created_at` | string (ISO 8601 datetime) | Campaign creation timestamp. |
| `updated_at` | string (ISO 8601 datetime) | Last update timestamp. |
| `scheduled_at` | string (ISO 8601 datetime) or null | Scheduled send time. |
| `send_time` | string (ISO 8601 datetime) or null | Actual send time. |


### `metrics` object

**Source endpoint**:
`GET /api/metrics/`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique metric ID. |
| `name` | string | Metric name (e.g., `Placed Order`, `Opened Email`). |
| `created` | string (ISO 8601 datetime) | Metric creation timestamp. |
| `updated` | string (ISO 8601 datetime) | Last update timestamp. |
| `integration` | struct | Integration that created this metric. |


### `flows` object

**Source endpoint**:
`GET /api/flows/`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique flow ID. |
| `name` | string | Flow name. |
| `status` | string | Flow status (`draft`, `manual`, `live`). |
| `archived` | boolean | Whether the flow is archived. |
| `created` | string (ISO 8601 datetime) | Flow creation timestamp. |
| `updated` | string (ISO 8601 datetime) | Last update timestamp. |
| `trigger_type` | string | Type of trigger (e.g., `list`, `segment`, `metric`). |


### `segments` object

**Source endpoint**:
`GET /api/segments/`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique segment ID. |
| `name` | string | Segment name. |
| `definition` | struct | Segment definition/conditions. |
| `created` | string (ISO 8601 datetime) | Segment creation timestamp. |
| `updated` | string (ISO 8601 datetime) | Last update timestamp. |
| `is_active` | boolean | Whether the segment is active. |
| `is_processing` | boolean | Whether the segment is currently being processed. |
| `is_starred` | boolean | Whether the segment is starred/favorited. |


### `templates` object

**Source endpoint**:
`GET /api/templates/`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique template ID. |
| `name` | string | Template name. |
| `editor_type` | string | Editor type (`CODE`, `DRAG_AND_DROP`, `HYBRID`). |
| `html` | string | Template HTML content. |
| `text` | string or null | Plain text version. |
| `created` | string (ISO 8601 datetime) | Template creation timestamp. |
| `updated` | string (ISO 8601 datetime) | Last update timestamp. |


## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get primary keys. Primary keys are defined **statically** based on the resource schema.

| Object | Primary Key | Type | Notes |
|--------|-------------|------|-------|
| `profiles` | `id` | string | Unique Klaviyo profile ID (26 characters). |
| `events` | `id` | string | Unique event ID. |
| `lists` | `id` | string | Unique list ID. |
| `campaigns` | `id` | string | Unique campaign ID. |
| `metrics` | `id` | string | Unique metric ID. |
| `flows` | `id` | string | Unique flow ID. |
| `segments` | `id` | string | Unique segment ID. |
| `templates` | `id` | string | Unique template ID. |


## **Object's Ingestion Type**

| Object | Ingestion Type | Cursor Field | Rationale |
|--------|----------------|--------------|-----------|
| `profiles` | `cdc` | `updated` | Profiles have a stable `id` and `updated` timestamp for incremental syncs. Profiles can be updated. |
| `events` | `append` | `datetime` | Events are immutable once created. New events are appended, ordered by datetime. |
| `lists` | `snapshot` | N/A | Lists are relatively small metadata. Full refresh is sufficient. |
| `campaigns` | `cdc` | `updated_at` | Campaigns can be updated and have `updated_at` for incremental syncs. |
| `metrics` | `snapshot` | N/A | Metrics are a small set of event types. Full refresh is appropriate. |
| `flows` | `cdc` | `updated` | Flows can be updated and have `updated` timestamp. |
| `segments` | `snapshot` | N/A | Segments are metadata. Full refresh is sufficient. |
| `templates` | `cdc` | `updated` | Templates can be updated and have `updated` timestamp. |


## **Read API for Data Retrieval**

### General API Patterns

**Base URL**: `https://a.klaviyo.com/api`

**Common Headers**:
```
Authorization: Klaviyo-API-Key <private_api_key>
revision: 2024-10-15
Accept: application/json
```

**Pagination**:
- Klaviyo uses cursor-based pagination.
- Response includes `links.next` with the next page cursor.
- Query parameter: `page[cursor]=<cursor_value>`
- Page size: `page[size]=100` (max 100 for most endpoints).

**Filtering**:
- JSON:API filter syntax: `filter=<filter_expression>`
- Greater than: `filter=greater-than(updated,2024-01-01T00:00:00Z)`
- Less than: `filter=less-than(datetime,2024-12-31T23:59:59Z)`

**Sorting**:
- Sort ascending: `sort=<field>`
- Sort descending: `sort=-<field>`


### Primary read endpoint for `profiles`

- **HTTP method**: `GET`
- **Endpoint**: `/api/profiles/`

**Key query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `page[cursor]` | string | no | none | Cursor for pagination. |
| `page[size]` | integer | no | 20 | Page size (max 100). |
| `filter` | string | no | none | JSON:API filter expression. |
| `sort` | string | no | none | Sort field (e.g., `updated`, `-updated`). |
| `fields[profile]` | string | no | all | Sparse fieldsets - comma-separated list of fields to return. |

**Incremental strategy**:
- Use `filter=greater-than(updated,<last_updated_timestamp>)` to fetch updated profiles.
- Sort by `updated` ascending to process in order.
- Track the maximum `updated` value as the next cursor.

**Example incremental read**:

```bash
curl -X GET \
  -H "Authorization: Klaviyo-API-Key pk_your_private_key" \
  -H "revision: 2024-10-15" \
  -H "Accept: application/json" \
  "https://a.klaviyo.com/api/profiles/?page[size]=100&filter=greater-than(updated,2024-01-01T00:00:00Z)&sort=updated"
```


### Primary read endpoint for `events`

- **HTTP method**: `GET`
- **Endpoint**: `/api/events/`

**Key query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `page[cursor]` | string | no | none | Cursor for pagination. |
| `page[size]` | integer | no | 20 | Page size (max 100). |
| `filter` | string | no | none | JSON:API filter expression. |
| `sort` | string | no | `-datetime` | Sort field. |

**Incremental strategy**:
- Use `filter=greater-than(datetime,<last_datetime>)` to fetch new events.
- Sort by `datetime` ascending to process in order.
- Events are append-only; no need to track updates.

**Example incremental read**:

```bash
curl -X GET \
  -H "Authorization: Klaviyo-API-Key pk_your_private_key" \
  -H "revision: 2024-10-15" \
  -H "Accept: application/json" \
  "https://a.klaviyo.com/api/events/?page[size]=100&filter=greater-than(datetime,2024-01-01T00:00:00Z)&sort=datetime"
```


### Snapshot endpoints (lists, metrics, segments)

For `lists`, `metrics`, and `segments`, the connector performs a full scan on each sync.

- **Lists**: `GET /api/lists/`
- **Metrics**: `GET /api/metrics/`
- **Segments**: `GET /api/segments/`

All support pagination via `page[cursor]` and `page[size]`.


### Rate Limits

Klaviyo uses a fixed-window rate limiting algorithm with two windows:

| Window Type | Description |
|-------------|-------------|
| Burst | Short-term limit (per second/minute) |
| Steady | Long-term limit (per minute/hour) |

**Typical rate limits** (varies by endpoint):
- Most GET endpoints: 75 burst / 700 steady per minute
- Some endpoints may have lower limits

**Rate limit headers in response**:
- `X-RateLimit-Limit-Burst`: Burst limit
- `X-RateLimit-Remaining-Burst`: Remaining burst requests
- `X-RateLimit-Limit-Steady`: Steady limit
- `X-RateLimit-Remaining-Steady`: Remaining steady requests

**Handling rate limits**:
- On HTTP 429, wait and retry with exponential backoff.
- Add delays between requests to stay under limits (e.g., 0.1-0.5 seconds).


## **Field Type Mapping**

### General mapping (Klaviyo JSON → connector logical types)

| Klaviyo JSON Type | Example Fields | Connector Logical Type | Notes |
|-------------------|----------------|------------------------|-------|
| string | `id`, `email`, `name`, `status` | string | UTF-8 text. |
| boolean | `archived`, `is_active` | boolean | Standard true/false. |
| string (ISO 8601 datetime) | `created`, `updated`, `datetime` | timestamp | Stored as UTC timestamps. |
| object | `location`, `properties`, `subscriptions` | struct | Nested records. |
| array | `items` (in event_properties) | array | Arrays of values or structs. |
| number | Numeric custom properties | double or long | Depends on context. |
| null | Any nullable field | corresponding type + null | When fields are absent or null, return `null`. |

### Special behaviors

- All IDs in Klaviyo are strings (26-character base62 encoding).
- Timestamps use ISO 8601 format with timezone (e.g., `2024-06-20T14:22:00+00:00`).
- The `properties` field in profiles is a flexible struct that can contain any custom properties.
- The `event_properties` field in events varies by event type.


## **Known Quirks & Edge Cases**

- **JSON:API format**: Responses use JSON:API format with `data`, `links`, and optionally `included` arrays. The connector must extract `attributes` from each data item.
- **Custom properties**: Profile `properties` and event `event_properties` are flexible dictionaries. Schema should use MapType or generic struct.
- **Rate limiting**: Different endpoints may have different rate limits. The connector should respect `Retry-After` headers on 429 responses.
- **API versioning**: Klaviyo uses date-based API versions. Always include the `revision` header.
- **Eventual consistency**: There may be slight delays between when data is created and when it appears in API responses.
- **Subscription consent**: Subscription status is complex with nested structures for email and SMS marketing consent.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.klaviyo.com/en/reference/api_overview | 2024-12-27 | High | Authentication methods, rate limits, JSON:API format, pagination. |
| Official Docs | https://developers.klaviyo.com/en/reference/get_profiles | 2024-12-27 | High | Profiles endpoint behavior, parameters, response structure. |
| Official Docs | https://developers.klaviyo.com/en/reference/get_events | 2024-12-27 | High | Events endpoint behavior, filtering by datetime. |
| Official Docs | https://developers.klaviyo.com/en/reference/get_lists | 2024-12-27 | High | Lists endpoint structure. |
| Official Docs | https://developers.klaviyo.com/en/reference/get_campaigns | 2024-12-27 | High | Campaigns endpoint and fields. |
| Official Docs | https://developers.klaviyo.com/en/reference/get_metrics | 2024-12-27 | High | Metrics endpoint structure. |
| Official Docs | https://developers.klaviyo.com/en/reference/get_flows | 2024-12-27 | High | Flows endpoint structure. |
| User-provided | prompts/klaviyo_api.md | 2024-12-27 | High | Overview of available data, authentication, key considerations. |


## **Sources and References**

- **Official Klaviyo API documentation** (highest confidence)
  - https://developers.klaviyo.com/en/reference/api_overview
  - https://developers.klaviyo.com/en/reference/get_profiles
  - https://developers.klaviyo.com/en/reference/get_events
  - https://developers.klaviyo.com/en/reference/get_lists
  - https://developers.klaviyo.com/en/reference/get_campaigns
  - https://developers.klaviyo.com/en/reference/get_metrics
  - https://developers.klaviyo.com/en/reference/get_flows
  - https://developers.klaviyo.com/en/reference/get_segments
  - https://developers.klaviyo.com/en/reference/get_templates
- **Klaviyo Help Center**
  - https://help.klaviyo.com
- **User-provided documentation**
  - `prompts/klaviyo_api.md` in project repository

When conflicts arise, **official Klaviyo documentation** is treated as the source of truth.

