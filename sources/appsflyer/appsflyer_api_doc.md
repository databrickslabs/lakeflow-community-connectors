# **AppsFlyer API Documentation**

## **Authorization**

- **Chosen method**: API Token (Bearer Token) for AppsFlyer APIs.
- **Base URL**: `https://hq1.appsflyer.com` (may vary by region - US: hq1, EU: eu-west1)
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <API_TOKEN>`
  - Alternative headers used:
    - `authentication: <API_TOKEN>` (for some endpoints)
    - `accept: application/json` or `accept: text/csv` for response format
- **Obtaining API Token**:
  - Admin users can retrieve the API token from the AppsFlyer dashboard
  - Navigate to: AppsFlyer Dashboard → (User menu) → API Tokens
  - Token has the format of a v4 UUID
- **Other supported methods (not used by this connector)**:
  - OAuth is not supported; tokens are long-lived and provisioned out-of-band

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Bearer <API_TOKEN>" \
  -H "accept: application/json" \
  "https://hq1.appsflyer.com/api/mng/apps"
```

Notes:
- Rate limiting varies by endpoint and account tier
- Recommended to respect rate limits and implement exponential backoff for retries
- Authentication token should be kept secure and rotated periodically

## **Object List**

For connector purposes, we treat specific AppsFlyer resources as **objects/tables**.  
The object list is **static** (defined by the connector), representing different types of reports and data available from AppsFlyer.

| Object Name | Description | Primary Endpoint | Ingestion Type (planned) |
|------------|-------------|------------------|--------------------------|
| `apps` | List of apps associated with the account | `GET /api/mng/apps` | `snapshot` |
| `installs_report` | Installation events for an app | `GET /export/<app_id>/installs_report/v5` | `cdc` (based on event_time) |
| `in_app_events_report` | In-app event data for an app | `GET /export/<app_id>/in_app_events_report/v5` | `cdc` (based on event_time) |
| `uninstall_events_report` | Uninstall events for an app | `GET /export/<app_id>/uninstall_events_report/v5` | `cdc` (based on event_time) |
| `organic_installs_report` | Organic (non-attributed) installs | `GET /export/<app_id>/organic_installs_report/v5` | `cdc` (based on event_time) |
| `organic_in_app_events_report` | Organic in-app events | `GET /export/<app_id>/organic_in_app_events_report/v5` | `cdc` (based on event_time) |
| `daily_report` | Daily aggregated metrics (partners) | `GET /export/<app_id>/partners_report/v5` | `cdc` (based on date) |
| `cohort_report` | Cohort-based retention metrics | `GET /export/<app_id>/cohort_report/v5` | `snapshot` (configurable) |
| `retargeting_installs_report` | Re-engagement installs | `GET /export/<app_id>/retargeting_installs_report/v5` | `cdc` (based on event_time) |
| `retargeting_in_app_events_report` | Re-engagement in-app events | `GET /export/<app_id>/retargeting_in_app_events_report/v5` | `cdc` (based on event_time) |

**Connector scope for initial implementation**:
- Step 1 focuses on the `apps` object and `installs_report` to document in detail
- Other objects are listed for future extension

High-level notes on objects:
- **apps**: Metadata about applications; treated as snapshot
- **installs_report**: Core table for installation events with attribution data
- **in_app_events_report**: Post-install events (purchases, registrations, etc.)
- **uninstall_events_report**: Records when users uninstall the app
- **daily_report**: Aggregated metrics by day, media source, and campaign
- **cohort_report**: Retention and LTV metrics grouped by cohorts
- All event-based reports support date range filtering for incremental reads

## **Object Schema**

### General notes

- AppsFlyer provides CSV and JSON response formats via the `accept` header
- For the connector, we define **tabular schemas** per object, derived from the JSON/CSV representation
- Nested JSON objects are modeled as **nested structures** rather than being fully flattened
- Field availability may vary based on the app platform (iOS, Android) and report type

### `apps` object (snapshot metadata)

**Source endpoint**:  
`GET /api/mng/apps`

**Key behavior**:
- Returns list of all apps associated with the account
- Includes app ID, name, platform, and status
- No pagination required (typically small result set)

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `app_id` | string | Unique identifier for the app (format: `id123456789`) |
| `app_name` | string | Display name of the app |
| `platform` | string | Platform type: `ios`, `android`, `windows_phone`, `amazon`, etc. |
| `bundle_id` | string | iOS bundle ID or Android package name |
| `time_zone` | string | Time zone configured for the app |
| `currency` | string | Default currency code (e.g., `USD`, `EUR`) |
| `status` | string | App status: `active`, `pending`, `archived` |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <API_TOKEN>" \
  -H "accept: application/json" \
  "https://hq1.appsflyer.com/api/mng/apps"
```

**Example response (truncated)**:

```json
[
  {
    "app_id": "id123456789",
    "app_name": "My Awesome App",
    "platform": "ios",
    "bundle_id": "com.example.myapp",
    "time_zone": "America/New_York",
    "currency": "USD",
    "status": "active"
  }
]
```

### `installs_report` object (primary table)

**Source endpoint**:  
`GET /export/<app_id>/installs_report/v5`

**Key behavior**:
- Returns installation events with full attribution data
- Supports date range filtering via `from` and `to` parameters (YYYY-MM-DD)
- Data is available with a latency of ~10-30 minutes
- Maximum date range per request: 90 days
- Results can be paginated or returned as a single file

**High-level schema (connector view)**:

Top-level fields (from the AppsFlyer Raw Data Export schema):

| Column Name | Type | Description |
|------------|------|-------------|
| `attributed_touch_type` | string | Attribution method: `click`, `impression`, `tv` |
| `attributed_touch_time` | string (ISO 8601 datetime) | Time of the attributed touch event |
| `install_time` | string (ISO 8601 datetime) | Time when the app was installed |
| `event_time` | string (ISO 8601 datetime) | Event timestamp (primary cursor field) |
| `event_name` | string | Always `install` for this report |
| `event_value` | string | Empty for install events |
| `event_revenue` | decimal | Empty for install events |
| `event_revenue_currency` | string | Currency code (e.g., `USD`) |
| `event_revenue_usd` | decimal | Revenue in USD |
| `event_source` | string | Source of the event: `SDK`, `S2S` |
| `is_receipt_validated` | boolean | Whether in-app purchase receipt was validated |
| `af_prt` | string | Media source (partner) name |
| `media_source` | string | Media source name (partner) |
| `channel` | string | Sub-publisher or site ID |
| `keywords` | string | Campaign keywords |
| `campaign` | string | Campaign name |
| `campaign_id` | string | Campaign identifier |
| `adset` | string | Ad set name |
| `adset_id` | string | Ad set identifier |
| `ad` | string | Ad name |
| `ad_id` | string | Ad identifier |
| `ad_type` | string | Type of ad |
| `site_id` | string | Site identifier |
| `sub_site_id` | string | Sub-site identifier |
| `sub_param_1` | string | Custom sub parameter 1 |
| `sub_param_2` | string | Custom sub parameter 2 |
| `sub_param_3` | string | Custom sub parameter 3 |
| `sub_param_4` | string | Custom sub parameter 4 |
| `sub_param_5` | string | Custom sub parameter 5 |
| `cost_model` | string | Cost model: `CPI`, `CPA`, `CPC`, `CPM` |
| `cost_value` | decimal | Cost per action |
| `cost_currency` | string | Currency of cost |
| `contributor_1_af_prt` | string | First contributor partner |
| `contributor_1_media_source` | string | First contributor media source |
| `contributor_1_campaign` | string | First contributor campaign |
| `contributor_1_touch_type` | string | First contributor touch type |
| `contributor_1_touch_time` | string (ISO 8601 datetime) | First contributor touch time |
| `contributor_2_af_prt` | string | Second contributor partner |
| `contributor_2_media_source` | string | Second contributor media source |
| `contributor_2_campaign` | string | Second contributor campaign |
| `contributor_2_touch_type` | string | Second contributor touch type |
| `contributor_2_touch_time` | string (ISO 8601 datetime) | Second contributor touch time |
| `contributor_3_af_prt` | string | Third contributor partner |
| `contributor_3_media_source` | string | Third contributor media source |
| `contributor_3_campaign` | string | Third contributor campaign |
| `contributor_3_touch_type` | string | Third contributor touch type |
| `contributor_3_touch_time` | string (ISO 8601 datetime) | Third contributor touch time |
| `region` | string | Geographic region |
| `country_code` | string | Two-letter country code (ISO 3166-1 alpha-2) |
| `state` | string | State or province |
| `city` | string | City name |
| `postal_code` | string | Postal/ZIP code |
| `dma` | string | Designated Market Area (US only) |
| `ip` | string | IP address of the user |
| `wifi` | boolean | Whether connection was Wi-Fi |
| `operator` | string | Mobile carrier name |
| `carrier` | string | Mobile carrier |
| `language` | string | Device language |
| `appsflyer_id` | string | Unique AppsFlyer device identifier |
| `advertising_id` | string | Platform advertising ID (IDFA/GAID) |
| `idfa` | string | iOS Identifier for Advertisers (iOS only) |
| `android_id` | string | Android ID (Android only) |
| `customer_user_id` | string | Custom user identifier set by the app |
| `imei` | string | International Mobile Equipment Identity (if available) |
| `idfv` | string | iOS Identifier for Vendors (iOS only) |
| `platform` | string | Platform: `ios`, `android` |
| `device_type` | string | Device type: `phone`, `tablet`, `desktop`, etc. |
| `device_model` | string | Device model name |
| `device_category` | string | Device category |
| `os_version` | string | Operating system version |
| `app_version` | string | App version |
| `sdk_version` | string | AppsFlyer SDK version |
| `app_id` | string | App identifier (connector-derived) |
| `app_name` | string | App name |
| `bundle_id` | string | Bundle ID / package name |
| `is_retargeting` | boolean | Whether this is a retargeting conversion |
| `retargeting_conversion_type` | string | Type of retargeting conversion |
| `af_siteid` | string | AppsFlyer site ID parameter |
| `match_type` | string | Attribution matching method |
| `attribution_lookback` | string | Attribution lookback window |
| `af_keywords` | string | AppsFlyer keywords parameter |
| `http_referrer` | string | HTTP referrer URL |
| `original_url` | string | Original attribution URL |
| `user_agent` | string | User agent string |
| `is_primary_attribution` | boolean | Whether this is the primary attribution |

> The columns listed above define the **complete connector schema** for the `installs_report` table.  
> If additional AppsFlyer fields are needed in the future, they must be added as new columns here.

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <API_TOKEN>" \
  -H "accept: application/json" \
  "https://hq1.appsflyer.com/export/id123456789/installs_report/v5?from=2025-01-01&to=2025-01-07"
```

### `in_app_events_report` object

**Source endpoint**:  
`GET /export/<app_id>/in_app_events_report/v5`

**High-level schema (connector view)**:

Similar to `installs_report`, but includes event-specific fields:

| Column Name | Type | Description |
|------------|------|-------------|
| `event_time` | string (ISO 8601 datetime) | Event timestamp (cursor field) |
| `event_name` | string | Name of the in-app event |
| `event_value` | string | Event value (stringified JSON or simple value) |
| `event_revenue` | decimal | Revenue amount for the event |
| `event_revenue_currency` | string | Currency code for revenue |
| `event_revenue_usd` | decimal | Revenue converted to USD |
| `af_revenue` | decimal | Validated revenue (for purchase events) |
| `af_currency` | string | Currency for af_revenue |
| `af_quantity` | integer | Quantity for purchase events |
| `af_content_id` | string | Content identifier |
| `af_content_type` | string | Content type |
| `af_price` | decimal | Price of item |
| `appsflyer_id` | string | Unique AppsFlyer device identifier (primary key) |
| `customer_user_id` | string | Custom user identifier |
| `install_time` | string (ISO 8601 datetime) | Time of app installation |
| `media_source` | string | Attributed media source |
| `campaign` | string | Campaign name |
| `platform` | string | Platform: `ios`, `android` |
| `device_model` | string | Device model |
| `os_version` | string | OS version |
| `app_id` | string | App identifier |
| `app_version` | string | App version |
| `sdk_version` | string | SDK version |

> In-app events inherit most attribution fields from the install event plus event-specific data.

## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get primary keys for objects.  
Instead, primary keys are defined **statically** based on the resource schema.

- **Primary key for `apps`**: `app_id`  
  - Type: string  
  - Property: Unique identifier for each app in the account

- **Primary key for `installs_report`**: `appsflyer_id` + `event_time`  
  - Type: string (appsflyer_id) + timestamp (event_time)  
  - Property: Combination uniquely identifies each install event

- **Primary key for `in_app_events_report`**: `appsflyer_id` + `event_time` + `event_name`  
  - Type: string + timestamp + string  
  - Property: Combination uniquely identifies each in-app event

Primary keys for additional objects (defined statically):

- **`uninstall_events_report`**: `appsflyer_id` + `event_time`  
- **`daily_report`**: `date` + `media_source` + `campaign` + `geo` (composite)  
- **`cohort_report`**: `cohort_date` + `cohort_day` + `geo` (composite)

## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports incremental reads via cursor.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Planned ingestion types for AppsFlyer objects:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `apps` | `snapshot` | App list is relatively small and changes infrequently; full refresh is acceptable. |
| `installs_report` | `cdc` | Install events are immutable after creation. Use `event_time` as cursor for incremental sync. Events may arrive late, so a lookback window is recommended. |
| `in_app_events_report` | `cdc` | In-app events are immutable. Use `event_time` as cursor with lookback window for late-arriving events. |
| `uninstall_events_report` | `cdc` | Uninstall events are immutable. Use `event_time` as cursor. |
| `organic_installs_report` | `cdc` | Organic installs follow same pattern as attributed installs. |
| `organic_in_app_events_report` | `cdc` | Organic events follow same pattern as attributed events. |
| `daily_report` | `cdc` | Daily aggregated metrics can be incrementally synced by date. |
| `cohort_report` | `snapshot` | Cohort data changes as users progress through days; typically refreshed fully. |

For `installs_report` and event reports:
- **Primary key**: `appsflyer_id` + `event_time` (composite)
- **Cursor field**: `event_time`
- **Sort order**: Ascending by `event_time`
- **Deletes**: AppsFlyer events are immutable; no delete handling required
- **Lookback window**: Recommended 3-6 hours to capture late-arriving events

## **Read API for Data Retrieval**

### Primary read endpoint for `installs_report`

- **HTTP method**: `GET`
- **Endpoint**: `/export/<app_id>/installs_report/v5`
- **Base URL**: `https://hq1.appsflyer.com`

**Path parameters**:
- `app_id` (string, required): Application identifier from the apps list

**Key query parameters** (relevant for ingestion):

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `from` | string (YYYY-MM-DD) | yes | none | Start date for the report (inclusive) |
| `to` | string (YYYY-MM-DD) | yes | none | End date for the report (inclusive) |
| `timezone` | string | no | UTC | Timezone for date interpretation (e.g., `America/New_York`) |
| `maximum_rows` | integer | no | 1000000 | Maximum number of rows to return (up to 1M per request) |
| `additional_fields` | string (comma-separated) | no | none | Additional optional fields to include |

**Response format**:
- Content-Type controlled by `Accept` header
- `application/json`: Returns JSON array of records
- `text/csv`: Returns CSV file with header row

**Pagination strategy**:
- AppsFlyer does not use traditional cursor/page-based pagination
- Instead, use date range windowing:
  - Split large date ranges into smaller windows (e.g., 1-7 day windows)
  - Use `maximum_rows` to limit response size
  - If a single day exceeds maximum_rows, consider using hourly bucketing (if supported) or multiple requests with additional filters

**Incremental strategy**:
- On the first run:
  - Start from a configurable `start_date` (e.g., 30 days ago)
  - Fetch data in daily or weekly windows
- On subsequent runs:
  - Use the maximum `event_time` from the previous sync as the new `from` date
  - Apply a lookback window (subtract 3-6 hours) to capture late-arriving events
  - Fetch data from `from_date` to current date/time (or up to data availability)
- Sort by `event_time` ascending (implicit in AppsFlyer response)
- Reprocess overlapping records based on primary key (upsert)

**Handling deletes / data corrections**:
- AppsFlyer events are immutable once reported
- Data corrections (e.g., fraud reversals) may result in changed attribution
- No explicit delete handling required; treat all events as append or upsert

**Example incremental read request**:

```bash
FROM_DATE="2025-01-01"
TO_DATE="2025-01-02"
curl -X GET \
  -H "Authorization: Bearer <API_TOKEN>" \
  -H "accept: application/json" \
  "https://hq1.appsflyer.com/export/id123456789/installs_report/v5?from=${FROM_DATE}&to=${TO_DATE}&timezone=UTC"
```

### Read endpoints for other report types

For objects treated as events (`in_app_events_report`, `uninstall_events_report`, etc.), the connector uses similar patterns:

- **In-app events**:  
  - Endpoint: `GET /export/<app_id>/in_app_events_report/v5`  
  - Key query params: `from`, `to`, `timezone`, `event_name` (optional filter)
  - Incremental strategy: same as installs, using `event_time` as cursor

- **Uninstall events**:  
  - Endpoint: `GET /export/<app_id>/uninstall_events_report/v5`  
  - Key query params: `from`, `to`, `timezone`
  - Incremental strategy: same as installs

- **Daily report** (aggregated):  
  - Endpoint: `GET /export/<app_id>/partners_report/v5`  
  - Key query params: `from`, `to`, `groupings` (e.g., `date,geo,media_source`)
  - Incremental strategy: fetch by date range; upsert based on composite key

### Read endpoint for snapshot-style metadata objects

For `apps` object (snapshot):
- Endpoint: `GET /api/mng/apps`
- No pagination or filtering required
- Returns full list of apps for the account
- Treated as snapshot: replace entire table on each sync

Example request:

```bash
curl -X GET \
  -H "Authorization: Bearer <API_TOKEN>" \
  -H "accept: application/json" \
  "https://hq1.appsflyer.com/api/mng/apps"
```

## **Field Type Mapping**

### General mapping (AppsFlyer JSON → connector logical types)

| AppsFlyer JSON Type | Example Fields | Connector Logical Type | Notes |
|---------------------|----------------|------------------------|-------|
| string | `app_id`, `event_name`, `media_source`, `country_code` | string | UTF-8 text |
| string (ISO 8601 datetime) | `event_time`, `install_time`, `attributed_touch_time` | timestamp with timezone | Parse as UTC timestamps |
| number (integer) | `af_quantity` | long / integer | Prefer 64-bit integer (`LongType`) |
| number (decimal) | `event_revenue`, `cost_value` | decimal or double | Use DoubleType for revenue fields |
| boolean | `wifi`, `is_retargeting`, `is_receipt_validated` | boolean | Standard true/false |
| object | (rare in raw data export) | struct | Represented as nested records |
| null / empty string | Missing or null fields | corresponding type + null | When fields are absent, surface `null` |

### Special behaviors and constraints

- **appsflyer_id** and other identifiers should be stored as **strings** (not integers)
- **event_time** and timestamp fields use ISO 8601 format in UTC (e.g., `"2025-01-09T12:34:56"`); parsing must handle this format
- **event_revenue** and cost fields are decimals; use DoubleType or DecimalType to avoid precision loss
- **country_code** is a two-letter ISO 3166-1 alpha-2 code
- **Nested structures**: Contributor data (contributor_1, contributor_2, contributor_3) should be modeled as repeated fields or structs
- **Empty strings vs nulls**: AppsFlyer may return empty strings for missing values; connector should normalize to `null`

## **Rate Limits and Best Practices**

AppsFlyer imposes rate limits based on account tier and endpoint:

- **General guidance**: 
  - Typical limit: ~1000 requests per day for raw data export endpoints (varies by plan)
  - App List API: 20 requests per minute, 100 requests per day
- **Best practices**:
  - Use date windowing to minimize number of API calls
  - Implement exponential backoff for rate limit (HTTP 429) responses
  - Cache app list locally; refresh periodically (e.g., daily)
  - Request data in larger windows (e.g., 7 days) when possible
  - Monitor response headers for rate limit information (if provided)
- **Error handling**:
  - HTTP 401: Invalid or expired API token
  - HTTP 403: Insufficient permissions
  - HTTP 429: Rate limit exceeded; back off and retry
  - HTTP 500/502/503: Server error; retry with exponential backoff

## **Known Quirks & Edge Cases**

- **Late-arriving data**:
  - AppsFlyer data may arrive with latency (10-30 minutes typical; up to 6 hours possible)
  - Implement a lookback window (3-6 hours) when syncing incrementally
- **Time zones**:
  - All timestamps in raw data export are in UTC by default
  - Account time zone can affect date boundaries; use `timezone` parameter for consistency
- **Missing fields**:
  - Not all fields are present for all events (e.g., IDFA missing if user opted out)
  - Connector should treat missing fields as `null`, not empty strings
- **Attribution model**:
  - AppsFlyer uses last-touch attribution by default
  - Multi-touch attribution data available via contributor fields
- **Fraud detection**:
  - Events may be marked as fraud retroactively
  - No explicit API to query fraud changes; rely on full refresh or long lookback
- **Platform differences**:
  - iOS and Android have different identifier fields (IDFA vs GAID, IDFV vs Android ID)
  - Some fields are platform-specific
- **API versioning**:
  - Current version is v5 for raw data export APIs
  - Older versions (v4) may still work but are deprecated

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://support.appsflyer.com/hc/en-us/articles/213223166-Master-API-user-acquisition-metrics-via-API | 2025-01-09 | High | Master API endpoint behavior, authentication method, parameters |
| Official Docs | https://support.appsflyer.com/hc/en-us/articles/360011999877-App-list-API-for-app-owners | 2025-01-09 | High | App List API endpoint, rate limits (20/min, 100/day) |
| Official Docs | https://support.appsflyer.com/hc/en-us | 2025-01-09 | High | General AppsFlyer API structure and authentication |
| OSS Connector | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-appsflyer | 2025-01-09 | Medium | Reference implementation for field names, incremental sync patterns |

## **Sources and References**

- **Official AppsFlyer API documentation** (highest confidence)
  - `https://support.appsflyer.com/hc/en-us/articles/213223166-Master-API-user-acquisition-metrics-via-API`
  - `https://support.appsflyer.com/hc/en-us/articles/360011999877-App-list-API-for-app-owners`
  - `https://support.appsflyer.com/hc/en-us/articles/207034486-Server-to-server-events-API-for-mobile-S2S-mobile`
  - AppsFlyer Data Export APIs documentation
- **Airbyte AppsFlyer source connector** (medium confidence)
  - `https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-appsflyer`
  - Used for validation of field names, pagination patterns, and incremental sync strategies

When conflicts arise, **official AppsFlyer documentation** is treated as the source of truth, with the Airbyte connector used to validate practical implementation details.
