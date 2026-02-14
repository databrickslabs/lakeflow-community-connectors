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

### **Connection Parameters** (dev_config.json)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api_token` | string | ✅ Yes | - | AppsFlyer API authentication token. Format: UUID v4. Obtain from AppsFlyer Dashboard → API Tokens. Must have permissions for all apps you want to sync. |
| `base_url` | string | ❌ Optional | `https://hq1.appsflyer.com` | Regional API endpoint. US region: `https://hq1.appsflyer.com`, EU region: `https://eu-west1.appsflyer.com`. Use the region where your AppsFlyer account is hosted. |

**Example dev_config.json**:
```json
{
  "api_token": "YOUR_APPSFLYER_API_TOKEN_HERE",
  "base_url": "https://hq1.appsflyer.com"
}
```

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

**Total Supported Tables**: 7 (1 snapshot + 6 CDC event reports)

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `apps` | List of apps associated with the account | `GET /api/mng/apps` | `snapshot` |
| `installs_report` | Installation events for an app | `GET /api/raw-data/export/app/<app_id>/installs_report/v5` | `cdc` (based on event_time) |
| `in_app_events_report` | In-app event data for an app | `GET /api/raw-data/export/app/<app_id>/in_app_events_report/v5` | `cdc` (based on event_time) |
| `uninstall_events_report` | Uninstall events for an app | `GET /api/raw-data/export/app/<app_id>/uninstall_events_report/v5` | `cdc` (based on event_time) |
| `organic_installs_report` | Organic (non-attributed) installs | `GET /api/raw-data/export/app/<app_id>/organic_installs_report/v5` | `cdc` (based on event_time) |
| `organic_in_app_events_report` | Organic in-app events | `GET /api/raw-data/export/app/<app_id>/organic_in_app_events_report/v5` | `cdc` (based on event_time) |
| `organic_uninstall_events_report` | Organic uninstall events | `GET /api/raw-data/export/app/<app_id>/organic_uninstall_events_report/v5` | `cdc` (based on event_time) |

**Implementation Status**:
- All 7 tables are fully implemented and tested
- Complete CDC support with event_time-based incremental sync
- Production-ready with rate limiting and error handling

**Response Format**:
- **Management API** (`/api/mng/*`): Returns JSON with JSON:API format
- **Raw Data Export API** (`/api/raw-data/export/*`): Returns CSV format with UTF-8 BOM

High-level notes on objects:
- **apps**: Metadata about applications; treated as snapshot (JSON format)
- **installs_report**: Core table for installation events with attribution data (CSV format)
- **in_app_events_report**: Post-install events (purchases, registrations, etc.) (CSV format)
- **uninstall_events_report**: Records when users uninstall the app (CSV format)
- **organic_installs_report**: Non-attributed installation events (CSV format)
- **organic_in_app_events_report**: Non-attributed in-app events (CSV format)
- **organic_uninstall_events_report**: Non-attributed uninstall events (CSV format)
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

### `installs_report` object (primary event table)

**Source endpoint**:  
`GET /api/raw-data/export/app/<app_id>/installs_report/v5`

**Key behavior**:
- Returns installation events with full attribution data (85 fields)
- Supports date range filtering via `from` and `to` parameters (YYYY-MM-DD)
- Data is available with a latency of ~10-30 minutes (up to 6 hours for late arrivals)
- Maximum date range per request: 90 days
- Response format: CSV with UTF-8 BOM encoding
- Connector automatically handles lookback window for late-arriving events

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
  "https://hq1.appsflyer.com/api/raw-data/export/app/id123456789/installs_report/v5?from=2025-01-01&to=2025-01-07"
```

### `in_app_events_report` object

**Source endpoint**:  
`GET /api/raw-data/export/app/<app_id>/in_app_events_report/v5`

**Key behavior**:
- Returns in-app event data (purchases, registrations, custom events)
- 30 fields focused on event-specific data and attribution
- Response format: CSV with UTF-8 BOM encoding

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

### `uninstall_events_report` object

**Source endpoint**:  
`GET /api/raw-data/export/app/<app_id>/uninstall_events_report/v5`

**Key behavior**:
- Returns uninstall event data
- 10 fields with essential information
- Response format: CSV with UTF-8 BOM encoding

### `organic_installs_report` object

**Source endpoint**:  
`GET /api/raw-data/export/app/<app_id>/organic_installs_report/v5`

**Key behavior**:
- Returns organic (non-attributed) installation events
- Same schema as `installs_report` (85 fields)
- Captures users who installed without paid attribution

### `organic_in_app_events_report` object

**Source endpoint**:  
`GET /api/raw-data/export/app/<app_id>/organic_in_app_events_report/v5`

**Key behavior**:
- Returns in-app events from organic users
- Same schema as `in_app_events_report` (30 fields)

### `organic_uninstall_events_report` object

**Source endpoint**:  
`GET /api/raw-data/export/app/<app_id>/organic_uninstall_events_report/v5`

**Key behavior**:
- Returns uninstall events from organic users
- Same schema as `uninstall_events_report` (10 fields)

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
- **`organic_installs_report`**: `appsflyer_id` + `event_time`  
- **`organic_in_app_events_report`**: `appsflyer_id` + `event_time` + `event_name`  
- **`organic_uninstall_events_report`**: `appsflyer_id` + `event_time`

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
| `organic_uninstall_events_report` | `cdc` | Organic uninstalls follow same pattern as attributed uninstalls. |

For `installs_report` and event reports:
- **Primary key**: `appsflyer_id` + `event_time` (composite)
- **Cursor field**: `event_time`
- **Sort order**: Ascending by `event_time`
- **Deletes**: AppsFlyer events are immutable; no delete handling required
- **Lookback window**: Recommended 3-6 hours to capture late-arriving events

## **Read API for Data Retrieval**

### Primary read endpoint for `installs_report`

- **HTTP method**: `GET`
- **Endpoint**: `/api/raw-data/export/app/<app_id>/installs_report/v5`
- **Base URL**: `https://hq1.appsflyer.com`

**Path parameters**:
- `app_id` (string, required): Application identifier from the apps list

**API Query Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `from` | string (YYYY-MM-DD) | yes | none | Start date for the report (inclusive) |
| `to` | string (YYYY-MM-DD) | yes | none | End date for the report (inclusive) |
| `timezone` | string | no | UTC | Timezone for date interpretation (e.g., `America/New_York`) |
| `maximum_rows` | integer | no | 1000000 | Maximum number of rows to return (up to 1M per request) |
| `additional_fields` | string (comma-separated) | no | none | Additional optional fields to include |

### **Table Configuration Parameters** (dev_table_config.json)

These parameters are passed as `table_options` when reading event tables. Each table can have its own configuration.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `app_id` | string | ✅ Yes (for event tables) | - | AppsFlyer application identifier. Format: package name (e.g., `com.myapp`) or AppsFlyer ID (e.g., `id123456789`). Required for all event tables. Not needed for `apps` table. Obtain from AppsFlyer dashboard or apps API. |
| `start_date` | string (YYYY-MM-DD) | ❌ Optional | 7 days ago | Initial date to start syncing data from on first run. Used when no previous offset exists. Format: `YYYY-MM-DD`. Recommended to start with recent date (e.g., last 7-14 days) to avoid large initial sync. |
| `lookback_hours` | integer | ❌ Optional | 6 | Number of hours to look back when syncing incrementally. Handles late-arriving events. AppsFlyer data latency is typically 10-30 minutes but can be up to 6 hours. Recommended range: 3-24 hours. Higher values ensure completeness but may increase processing time. |
| `max_days_per_batch` | integer | ❌ Optional | 7 | Maximum number of days to fetch in a single API request. Used to control batch size and avoid timeouts. Smaller values (1-3 days) recommended for high-volume apps. Larger values (7-30 days) work well for low-volume apps. Maximum supported by API: 90 days. |

**Example dev_table_config.json**:
```json
{
  "installs_report": {
    "app_id": "com.myapp",
    "start_date": "2026-01-08",
    "lookback_hours": "6",
    "max_days_per_batch": "7"
  },
  "in_app_events_report": {
    "app_id": "com.myapp",
    "start_date": "2026-01-08",
    "lookback_hours": "6",
    "max_days_per_batch": "7"
  },
  "apps": {}
}
```

**Parameter Usage by Table**:

| Table | app_id | start_date | lookback_hours | max_days_per_batch |
|-------|--------|------------|----------------|-------------------|
| `apps` | ❌ | ❌ | ❌ | ❌ |
| `installs_report` | ✅ | ⭕ | ⭕ | ⭕ |
| `in_app_events_report` | ✅ | ⭕ | ⭕ | ⭕ |
| `uninstall_events_report` | ✅ | ⭕ | ⭕ | ⭕ |
| `organic_installs_report` | ✅ | ⭕ | ⭕ | ⭕ |
| `organic_in_app_events_report` | ✅ | ⭕ | ⭕ | ⭕ |
| `organic_uninstall_events_report` | ✅ | ⭕ | ⭕ | ⭕ |

✅ = Required, ⭕ = Optional, ❌ = Not used

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
  - Start from a configurable `start_date` (default: 7 days ago)
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
  "https://hq1.appsflyer.com/api/raw-data/export/app/id123456789/installs_report/v5?from=${FROM_DATE}&to=${TO_DATE}&timezone=UTC"
```

### Read endpoints for other report types

For objects treated as events (`in_app_events_report`, `uninstall_events_report`, etc.), the connector uses similar patterns:

- **In-app events**:  
  - Endpoint: `GET /api/raw-data/export/app/<app_id>/in_app_events_report/v5`  
  - Key query params: `from`, `to`, `timezone`, `event_name` (optional filter)
  - Incremental strategy: same as installs, using `event_time` as cursor

- **Uninstall events**:  
  - Endpoint: `GET /api/raw-data/export/app/<app_id>/uninstall_events_report/v5`  
  - Key query params: `from`, `to`, `timezone`
  - Incremental strategy: same as installs

- **Organic installs**:  
  - Endpoint: `GET /api/raw-data/export/app/<app_id>/organic_installs_report/v5`  
  - Key query params: `from`, `to`, `timezone`
  - Incremental strategy: same as installs

- **Organic in-app events**:  
  - Endpoint: `GET /api/raw-data/export/app/<app_id>/organic_in_app_events_report/v5`  
  - Key query params: `from`, `to`, `timezone`, `event_name` (optional filter)
  - Incremental strategy: same as in_app_events

- **Organic uninstall events**:  
  - Endpoint: `GET /api/raw-data/export/app/<app_id>/organic_uninstall_events_report/v5`  
  - Key query params: `from`, `to`, `timezone`
  - Incremental strategy: same as uninstalls

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

## **Rate Limits and Error Handling**

### **Rate Limit Overview**

AppsFlyer imposes **per-app, per-day, per-report-type** rate limits that vary by account tier:

| Endpoint Type | Typical Limit | Scope | Notes |
|---------------|---------------|-------|-------|
| **Install Reports** | Varies by plan | Per app, per day | Includes: `installs_report`, `organic_installs_report`, `reinstalls` |
| **In-App Event Reports** | Varies by plan | Per app, per day | Includes: `in_app_events_report`, `organic_in_app_events_report` |
| **Uninstall Reports** | Varies by plan | Per app, per day | Includes: `uninstall_events_report`, `organic_uninstall_events_report` |
| **App List API** | 20 req/min, 100 req/day | Per account | Management API endpoint |
| **Total Raw Data Exports** | ~1000 requests/day | Per account | Aggregate across all report types |

**Important**: Each report type (installs, in-app events, uninstalls) has its own separate daily quota per app.

### **Rate Limit Errors**

#### **HTTP 400 - Daily Quota Exceeded**

AppsFlyer returns **HTTP 400** (not 429) when daily quota is exhausted. The error message indicates the specific report type limit reached.

**Example Error for Install Reports**:
```
RuntimeError: AppsFlyer API error for organic_installs_report: 400 You've reached your maximum number of install reports that can be downloaded today for this app. <a href="https://support.appsflyer.com/hc/en-us/articles/207034366-Rate-limitations-and-access-windows-for-reports-and-reporting-APIs#rawdata-rate-limitation-policy?utm_source=hq1&utm_medium=ui&utm_campaign=reports">Learn more</a>. For details about increasing your limit, contact your CSM or hello@appsflyer.com
```

**Example Error for In-App Event Reports**:
```
RuntimeError: AppsFlyer API error for organic_in_app_events_report: 400 You've reached your maximum number of in-app event reports that can be downloaded today for this app. <a href="...">Learn more</a>. For details about increasing your limit, contact your CSM or hello@appsflyer.com
```

**Key Characteristics**:
- Status code: `400 Bad Request` (not `429 Too Many Requests`)
- Error message includes: "You've reached your maximum number of [report_type] reports"
- Quota resets daily (typically at midnight UTC)
- Each report type has independent quota

### **Error Handling in Connector**

The connector implements the following error handling strategy:

| Status Code | Meaning | Connector Behavior | User Action Required |
|-------------|---------|-------------------|---------------------|
| **400** with rate limit message | Daily quota exceeded for specific report type | Raises `RuntimeError` immediately, does not retry | Wait until next day (quota reset) OR reduce sync frequency OR contact AppsFlyer to increase limit |
| **401** | Invalid or expired API token | Raises authentication error | Update `api_token` in connection config |
| **403** | Insufficient permissions | Raises permission error | Verify API token has access to the app |
| **429** | Rate limit (requests per minute) | Automatic retry with exponential backoff | None - connector handles automatically |
| **500/502/503** | Server error | Automatic retry with exponential backoff | If persistent, contact AppsFlyer support |

**Note**: The connector does **not** automatically retry HTTP 400 rate limit errors because:
1. The quota is per-day, so immediate retries will fail
2. Retrying wastes API calls and may affect other report types
3. User should be aware of quota limits for capacity planning

### **Best Practices to Avoid Rate Limits**

1. **Optimize Sync Frequency**:
   - For low-volume apps: Sync daily or less frequently
   - For high-volume apps: Sync hourly but monitor quota usage
   - Avoid running full-refresh unnecessarily

2. **Use Incremental Sync Efficiently**:
   - Set appropriate `start_date` (default: 7 days ago)
   - Use `max_days_per_batch` to control request size (default: 7 days)
   - Longer batch windows = fewer API calls
   - Example: 7-day batch = 1 API call vs 1-day batch = 7 API calls

3. **Sync Only Needed Tables**:
   - Don't sync all 7 tables if you only need 2-3
   - Organic reports use same quota as non-organic (e.g., `organic_installs_report` + `installs_report` = 2 install report calls)
   - Prioritize critical tables (e.g., `installs_report`, `in_app_events_report`)

4. **Monitor Quota Usage**:
   - Track how many API calls each pipeline run makes
   - Calculate daily quota needed: `(days_per_sync / max_days_per_batch) * sync_frequency * number_of_tables`
   - Example: Syncing 7 days of data for 6 tables daily = 6 API calls per day

5. **Coordinate Multiple Pipelines**:
   - If multiple pipelines access the same app, they share the quota
   - Consider consolidating into one pipeline with multiple tables
   - Schedule pipelines at different times to spread out API calls

6. **Cache App List**:
   - `apps` table rarely changes
   - Sync once daily or on-demand rather than with every pipeline run

### **What to Do When Rate Limited**

#### **Immediate Actions**:
1. **Wait for Quota Reset**: Quotas typically reset at midnight UTC. Schedule next run after reset.
2. **Check Current Usage**: Review how many API calls were made today for this app.
3. **Identify the Report Type**: Error message indicates which report type hit the limit (installs, in-app events, etc.)

#### **Short-Term Solutions**:
1. **Increase `max_days_per_batch`**: Fetch more days per request
   ```json
   {
     "app_id": "com.myapp",
     "max_days_per_batch": "14"  // Changed from 7 to 14 days
   }
   ```
2. **Reduce Sync Frequency**: Change from hourly to every 6 hours or daily
3. **Disable Non-Critical Tables**: Temporarily pause syncing less important tables

#### **Long-Term Solutions**:
1. **Contact AppsFlyer**: Request quota increase
   - Email: hello@appsflyer.com
   - Contact your Customer Success Manager (CSM)
   - Reference: https://support.appsflyer.com/hc/en-us/articles/207034366
2. **Upgrade Account Tier**: Higher tiers typically include higher quotas
3. **Optimize Data Strategy**: Only sync necessary date ranges and tables

### **Connector Configuration for Rate Limit Management**

```json
{
  "connection_name": "my_appsflyer_conn",
  "objects": [
    {
      "table": {
        "source_table": "installs_report",
        "app_id": "com.myapp",
        "start_date": "2026-01-08",
        "max_days_per_batch": "14",  // Larger batches = fewer API calls
        "lookback_hours": "6"
      }
    },
    {
      "table": {
        "source_table": "in_app_events_report",
        "app_id": "com.myapp",
        "start_date": "2026-01-08",
        "max_days_per_batch": "14"
      }
    }
  ]
}
```

### **Quota Calculation Example**

**Scenario**: Syncing 6 tables daily for an app with 30 days of historical data

**Initial Load** (First Run):
- Date range: 30 days (from `start_date`)
- Batch size: 7 days (`max_days_per_batch`)
- API calls per table: 30 / 7 = 5 calls (rounded up)
- Total calls: 5 calls × 6 tables = **30 API calls** (one-time)

**Daily Incremental Sync**:
- Date range: 1 day (yesterday + lookback window)
- Batch size: 7 days (covers 1 day easily)
- API calls per table: 1 call
- Total calls: 1 call × 6 tables = **6 API calls per day**

**Optimization**: Increase `max_days_per_batch` to 30:
- Initial load: 30 / 30 = 1 call per table = **6 API calls** (saves 24 calls)
- Daily sync: Still 1 call per table = **6 API calls per day**

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

## **Implementation Notes**

### **Connector Features**
- **7 Tables Supported**: Complete coverage of core attribution and event data
- **Smart Incremental Sync**: Event_time-based CDC with configurable lookback window
- **85 Attribution Fields**: Comprehensive data including multi-touch attribution
- **Production-Ready**: Error handling, rate limiting, retry logic all implemented
- **CSV Parsing**: Automatic UTF-8 BOM handling and field normalization
- **Type Safety**: Full PySpark StructType schemas defined

### **Configuration Summary**

**Connection-level** (set once in dev_config.json):
- `api_token` ✅ Required - API authentication token
- `base_url` ⭕ Optional - Regional endpoint (default: US)

**Table-level** (per-table in dev_table_config.json):
- `app_id` ✅ Required for event tables - Application identifier
- `start_date` ⭕ Optional - Initial sync date (default: 7 days ago)
- `lookback_hours` ⭕ Optional - Late arrival window (default: 6 hours)
- `max_days_per_batch` ⭕ Optional - Batch size (default: 7 days)

See **Authorization** and **Read API for Data Retrieval** sections above for detailed parameter descriptions.

### **Tested Scenarios**
- ✅ Connection initialization and authentication
- ✅ List all 7 tables
- ✅ Get schema for each table (85, 30, and 10 field variants)
- ✅ Read table metadata (primary keys, cursor fields, ingestion types)
- ✅ Read data from all tables with proper configuration
- ✅ Handle rate limit errors gracefully
- ✅ Process empty responses and normalize CSV data

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://support.appsflyer.com/hc/en-us/articles/213223166-Master-API-user-acquisition-metrics-via-API | 2026-01-09 | High | Master API endpoint behavior, authentication method, parameters |
| Official Docs | https://support.appsflyer.com/hc/en-us/articles/360011999877-App-list-API-for-app-owners | 2026-01-09 | High | App List API endpoint, rate limits (20/min, 100/day) |
| Official Docs | https://support.appsflyer.com/hc/en-us | 2026-01-09 | High | General AppsFlyer API structure and authentication |
| OSS Connector | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-appsflyer | 2026-01-09 | Medium | Reference implementation for field names, incremental sync patterns |
| Implementation | Local testing with real AppsFlyer account | 2026-01-10 | High | Verified all 7 tables, rate limits, data formats, error handling |
| Production Testing | Databricks pipeline execution with real data | 2026-01-10 | High | Confirmed HTTP 400 rate limit errors, per-report-type quotas, error messages |

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
