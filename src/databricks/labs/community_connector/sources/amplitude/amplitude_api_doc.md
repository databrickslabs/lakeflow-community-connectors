# Amplitude API Documentation

## Authorization

Amplitude uses **HTTP Basic Authentication** for all Analytics API calls. The credentials are the project's **API Key** and **Secret Key**.

### Preferred Method: HTTP Basic Auth

Pass credentials as a Basic Auth header (or via cURL's `-u` flag):

```
Authorization: Basic base64(api_key:secret_key)
```

**Where to find credentials:**
1. In the Amplitude Analytics web app, go to **Settings → Projects**.
2. Select the target project.
3. On the General tab, click **Manage** next to **API Key** or **Secret Key** to view/copy.

> Only project Managers and Admins can generate or revoke API keys and secret keys. Secret keys are shown only at generation time.

**Example request (cURL):**
```bash
curl --location --request GET 'https://amplitude.com/api/2/events/list' \
-u '{api_key}:{secret_key}'
```

**Alternative — inline Basic header:**
```bash
curl --location --request GET 'https://amplitude.com/api/2/events/list' \
--header 'Authorization: Basic {base64_encoded_api_key_and_secret_key}'
```

### EU Data Residency

If the Amplitude project uses EU data residency, replace the base URL:

| Data residency | Base URL |
|---|---|
| Standard (default) | `https://amplitude.com` |
| EU | `https://analytics.eu.amplitude.com` |

This applies to **all** API endpoints documented here. The Export API and Dashboard REST API both support EU endpoints.

### Connector Config Parameters

The connector needs two credentials:
- `api_key` (string, required)
- `secret_key` (string, required)

Optionally:
- `data_region` (enum: `"standard"` or `"eu"`, default `"standard"`)
- `start_date` (ISO 8601 datetime, e.g. `2024-01-01T00:00:00Z`) — controls how far back Events backfill goes
- `request_time_range` (integer, hours, default 24) — width of each Export API window; reduce if large event volumes cause timeouts

---

## Object List

Amplitude exposes the following objects through its REST APIs that are relevant for a Lakeflow connector. The object list is **static** (not discoverable via API):

| Object (table) | API Source | Sync Type | Primary Key |
|---|---|---|---|
| `events` | Export API | Incremental (append) | `uuid` |
| `events_list` | Dashboard REST API | Full Refresh (snapshot) | `value` |
| `active_users_counts` | Dashboard REST API | Incremental | `date` |
| `average_session_length` | Dashboard REST API | Incremental | `date` |
| `cohorts` | Behavioral Cohorts API | Full Refresh (snapshot) | `id` |
| `annotations` | Chart Annotations API | Full Refresh (snapshot) | `id` |

**Coverage note:** These six objects are supported by both Airbyte (connector v0.7.31) and Fivetran for Amplitude. The `events` table is the most important — it contains all raw product analytics event data.

---

## Object Schema

### 1. `events` (Export API)

Raw event records exported via the Amplitude Export API. Each event is one JSON object per line in a gzipped NDJSON archive.

**Schema** (all fields present in every event record):

| Field | Type | Description |
|---|---|---|
| `uuid` | string (UUID) | Unique event identifier. Use as primary key. |
| `event_id` | integer | Sequential event ID within the project. |
| `event_type` | string | Name of the event (e.g., `"Add Content to Cart"`). |
| `event_time` | string (UTC ISO-8601) | Time the event occurred on the client. |
| `server_received_time` | string (UTC ISO-8601) | Time Amplitude's server received the event. |
| `server_upload_time` | string (UTC ISO-8601) | Time Amplitude uploaded/processed the event. **Used as the incremental cursor.** |
| `client_event_time` | string (UTC ISO-8601) | Client-reported event time. |
| `client_upload_time` | string (UTC ISO-8601) | Time the client uploaded the event. |
| `processed_time` | string (UTC ISO-8601) | Internal Amplitude processing time. |
| `user_id` | string | Application-assigned user identifier. |
| `device_id` | string | Device identifier (auto-assigned by Amplitude SDK). |
| `amplitude_id` | long (integer) | Amplitude's internal user identifier. |
| `$insert_id` | string | Optional deduplication ID sent by the client. |
| `app` | integer | Amplitude project (app) ID. |
| `platform` | string | Platform (e.g., `"iOS"`, `"Android"`, `"Web"`). |
| `os_name` | string | Operating system name. |
| `os_version` | string | Operating system version. |
| `device_family` | string | Device family (e.g., `"iPhone"`, `"Samsung Galaxy"`). |
| `device_type` | string | Device type. |
| `device_carrier` | string | Mobile carrier name. |
| `country` | string | Country of the user. |
| `region` | string | Region/state. |
| `city` | string | City. |
| `dma` | string | Designated Market Area (US only). |
| `language` | string | Language setting on the device. |
| `ip_address` | string | IP address at the time of the event. |
| `location_lat` | float | GPS latitude (if collected). |
| `location_lng` | float | GPS longitude (if collected). |
| `version_name` | string | Application version. |
| `start_version` | string | App version when the user was first seen. |
| `session_id` | long (integer) | Session identifier. |
| `paying` | boolean | Whether the user is a paying customer. |
| `library` | string | SDK library that sent the event. |
| `event_properties` | object (dict) | Custom event properties (varies per event type). |
| `user_properties` | object (dict) | User properties at time of event. |
| `group_properties` | object (dict) | Group-level properties. |
| `groups` | object (dict) | Group membership information. |
| `data` | object (dict) | Extra data payload (rarely used). |
| `amplitude_attribution_ids` | string | Attribution identifiers. |
| `sample_rate` | null | Always null in exported data. |

**Example record:**
```json
{
  "server_received_time": "2024-03-15T10:05:00.000Z",
  "app": 123456,
  "device_carrier": "Verizon",
  "city": "San Francisco",
  "user_id": "user_abc123",
  "uuid": "550e8400-e29b-41d4-a716-446655440000",
  "event_time": "2024-03-15T10:04:58.000Z",
  "platform": "iOS",
  "os_version": "17.3",
  "amplitude_id": 987654321,
  "processed_time": "2024-03-15T10:05:01.000Z",
  "version_name": "4.2.1",
  "ip_address": "192.0.2.1",
  "paying": true,
  "event_type": "Purchase Completed",
  "library": "amplitude-ios/8.12.0",
  "server_upload_time": "2024-03-15T10:05:00.000Z",
  "event_id": 1234567,
  "os_name": "ios",
  "event_properties": {"item_id": "sku_999", "price": 29.99},
  "user_properties": {"plan": "pro"},
  "device_id": "device_xyz",
  "country": "United States",
  "region": "California",
  "session_id": 1710496800000,
  "device_family": "iPhone"
}
```

---

### 2. `events_list` (Dashboard REST API)

Catalog of event types visible in the project, with this week's metrics.

| Field | Type | Description |
|---|---|---|
| `value` | string | Raw event name in the data (primary key). |
| `display` | string | Display name shown in the Amplitude UI. |
| `non_active` | boolean | Whether the event is marked inactive. |
| `deleted` | boolean | Whether the event has been deleted. |
| `hidden` | boolean | Whether the event is hidden. |
| `flow_hidden` | boolean | Whether hidden from Pathfinder/Pathfinder Users. |
| `totals` | integer | Total event occurrences this week. |

**Example response:**
```json
{
  "data": [
    {
      "non_active": false,
      "value": "Purchase Completed",
      "totals": 1505645,
      "deleted": false,
      "flow_hidden": false,
      "hidden": false,
      "display": "Purchase Completed"
    }
  ]
}
```

> Note: Hidden events are NOT returned by this API. Only visible events appear.

---

### 3. `active_users_counts` (Dashboard REST API)

Daily counts of active or new users. The API returns a time series; the connector flattens it to one row per date.

| Field | Type | Description |
|---|---|---|
| `date` | string (YYYY-MM-DD) | The date (primary key). |
| `count` | integer | Number of active (or new) users on that date. |
| `segment` | string | Segment label (e.g., country name if grouped). Default: `"Totals"`. |

**Raw API response structure:**
```json
{
  "data": {
    "series": [[46109, 47542]],
    "seriesMeta": ["United States"],
    "xValues": ["2024-03-14", "2024-03-15"]
  }
}
```

---

### 4. `average_session_length` (Dashboard REST API)

Daily average session length in seconds.

| Field | Type | Description |
|---|---|---|
| `date` | string (YYYY-MM-DD) | The date (primary key). |
| `length` | float | Average session length in seconds. |

**Raw API response structure:**
```json
{
  "data": {
    "series": [[1204.02, 1197.41]],
    "seriesMeta": [{"segmentIndex": 0}],
    "xValues": ["2024-03-14", "2024-03-15"]
  }
}
```

---

### 5. `cohorts` (Behavioral Cohorts API)

Behavioral cohort definitions in the Amplitude project. Does not include individual user memberships (those require an async 3-step download).

| Field | Type | Description |
|---|---|---|
| `id` | string | Cohort ID (primary key). |
| `name` | string | Cohort name. |
| `appId` | integer | Amplitude project ID. |
| `archived` | boolean | Whether the cohort is archived. |
| `description` | string | Description text. |
| `finished` | boolean | Whether ML training is complete (internal). |
| `published` | boolean | Whether the cohort is discoverable by other users. |
| `hidden` | boolean | Whether the cohort is hidden. |
| `size` | integer | Number of users in the cohort. |
| `type` | string | Cohort type (internal Amplitude representation). |
| `owners` | array[string] | Email addresses of cohort owners. |
| `viewers` | array[string] | Email addresses of cohort viewers. |
| `lastMod` | integer (Unix timestamp) | Last modified time. |
| `createdAt` | integer (Unix timestamp) | Creation time. |
| `lastComputed` | integer (Unix timestamp) | Last computed time. |
| `metadata` | array[string] | Optional metadata (from funnel/microscope). |
| `view_count` | integer | View count. |
| `popularity` | integer | Popularity score. |
| `last_viewed` | integer (Unix timestamp) | Last viewed time. |
| `chart_id` | string | Chart ID if cohort was created from a chart. |
| `edit_id` | string | Edit ID if cohort was created from a chart. |
| `is_predictive` | boolean | Whether cohort uses predictive ML. |
| `is_official_content` | boolean | Whether it is official Amplitude content. |
| `location_id` | string | Location ID (from chart-based cohorts). |
| `shortcut_ids` | array[string] | Shortcut IDs. |
| `definition` | object | Amplitude internal cohort definition (complex nested object). |

---

### 6. `annotations` (Chart Annotations API)

Chart annotations marking events in Amplitude charts (e.g., releases, incidents).

| Field | Type | Description |
|---|---|---|
| `id` | integer | Annotation ID (primary key). |
| `label` | string | Annotation title. |
| `start` | string (ISO 8601) | Start timestamp of the annotation. |
| `end` | string (ISO 8601) | End timestamp (null if point annotation). |
| `details` | string | Detailed description text. |
| `chart_id` | string | Chart ID if annotation is chart-specific; null for global. |
| `category` | object | Category object with `id` (integer) and `name` (string). |

---

## Get Object Primary Keys

| Object | Primary Key | Notes |
|---|---|---|
| `events` | `uuid` | Globally unique UUID per event. Fivetran alternatively uses composite `(event_id, device_id, server_upload_time)`. |
| `events_list` | `value` | Raw event name is unique per project. |
| `active_users_counts` | `date` | One row per day after flattening. |
| `average_session_length` | `date` | One row per day after flattening. |
| `cohorts` | `id` | String cohort ID. |
| `annotations` | `id` | Integer annotation ID. |

---

## Object's Ingestion Type

| Object | Ingestion Type | Reason |
|---|---|---|
| `events` | `append` | Export API returns immutable historical events by upload-time window; new events arrive over time. Deleted events are not surfaced via Export API. |
| `events_list` | `snapshot` | Full refresh; no cursor field. Totals reflect the current week only. |
| `active_users_counts` | `cdc` | Time series; can be read incrementally by advancing the date cursor. New data is non-destructive. |
| `average_session_length` | `cdc` | Time series; can be read incrementally by advancing the date cursor. |
| `cohorts` | `snapshot` | No reliable cursor field (lastMod is a Unix int and filtering is not supported by the list endpoint). Full refresh is safest. |
| `annotations` | `snapshot` | No offset/cursor pagination; filtered by `start`/`end` time but full list is small. |

---

## Read API for Data Retrieval

### A. `events` — Export API

**Endpoint:**
```
GET https://amplitude.com/api/2/export
```

**Authentication:** HTTP Basic (`api_key:secret_key`)

**Query Parameters:**

| Name | Required | Format | Description |
|---|---|---|---|
| `start` | Yes | `YYYYMMDDTHH` | First hour of the export window (inclusive). |
| `end` | Yes | `YYYYMMDDTHH` | Last hour of the export window (inclusive). |

**Example:**
```bash
curl --location --request GET \
  'https://amplitude.com/api/2/export?start=20240315T00&end=20240315T23' \
  -u '{api_key}:{secret_key}'
```

**Response format:**
- Returns a **ZIP archive** (Content-Type: application/zip).
- The ZIP contains multiple gzipped NDJSON files (one or more per hour).
- Each line in each file is a single JSON event object.
- **Unzip** → **gunzip each file** → **parse NDJSON line-by-line**.

**Python example:**
```python
import io, zipfile, gzip, json, requests
from requests.auth import HTTPBasicAuth

resp = requests.get(
    "https://amplitude.com/api/2/export",
    params={"start": "20240315T00", "end": "20240315T23"},
    auth=HTTPBasicAuth(api_key, secret_key),
    stream=True,
)
resp.raise_for_status()

with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
    for name in zf.namelist():
        with zf.open(name) as gz_file:
            with gzip.open(gz_file, "rt", encoding="utf-8") as f:
                for line in f:
                    event = json.loads(line)
                    yield event
```

**Incremental strategy:**
- Cursor field: `server_upload_time` (UTC ISO-8601)
- Query parameter format: `YYYYMMDDTHH` (hourly resolution)
- On each incremental sync, advance the start/end window to cover `[last_cursor, now]`
- Recommended window size: 1–24 hours per request (default 24h; reduce to 1–6h for high-volume projects)
- Data is available ~2 hours after the hour ends (e.g., data for 8–9 PM is available at 11 PM)
- **Lookback:** Apply a 2-hour lookback buffer to handle late-arriving events
- **No deletes:** The Export API does not surface deleted events. Events are append-only.

**Status codes:**

| Code | Meaning |
|---|---|
| 200 | Success — returns ZIP archive |
| 400 | File size exceeds 4GB limit — reduce time range |
| 404 | No data for the requested time range |
| 504 | Timeout — reduce time range or use S3 export |

**Constraints:**
- Maximum time range per request: 365 days
- Maximum response size: 4GB (use smaller windows for high-volume projects)
- If data for a single hour exceeds 4GB, use Amplitude's Amazon S3 Export instead
- Rate limits: No documented rate limit, but max 5 concurrent requests across all API endpoints

---

### B. `events_list` — Dashboard REST API

**Endpoint:**
```
GET https://amplitude.com/api/2/events/list
```

**Authentication:** HTTP Basic

**No query parameters required.**

**Example:**
```bash
curl --location --request GET 'https://amplitude.com/api/2/events/list' \
-u '{api_key}:{secret_key}'
```

**Response:**
```json
{
  "data": [
    {
      "non_active": false,
      "value": "Add Content to Cart",
      "totals": 1505645,
      "deleted": false,
      "flow_hidden": false,
      "hidden": false,
      "display": "Add Content to Cart"
    }
  ]
}
```

**Pagination:** None. Returns all visible event types in a single response.

**Sync:** Full refresh only. Hidden events are excluded.

---

### C. `active_users_counts` — Dashboard REST API

**Endpoint:**
```
GET https://amplitude.com/api/2/users
```

**Authentication:** HTTP Basic

**Query Parameters:**

| Name | Required | Description |
|---|---|---|
| `start` | Yes | Start date `YYYYMMDD` |
| `end` | Yes | End date `YYYYMMDD` |
| `m` | No | `active` (default) or `new` — which user type to count |
| `i` | No | `1` (daily, default), `7` (weekly), or `30` (monthly) |
| `s` | No | Segment filter JSON |
| `g` | No | Group-by property name (e.g., `country`) |

**Example (daily active users for a date range):**
```bash
curl --location --request GET \
  'https://amplitude.com/api/2/users?start=20240301&end=20240315&m=active&i=1' \
  -u '{api_key}:{secret_key}'
```

**Response:**
```json
{
  "data": {
    "series": [[46109, 47542], [42845, 42626]],
    "seriesMeta": ["United States", "Canada"],
    "xValues": ["2024-03-14", "2024-03-15"]
  }
}
```

**Flatten logic for connector:**
Zip `xValues` with the `series` values. For the default (no group-by) case, `series` has one inner array and `seriesMeta` is `["Totals"]`.

**Incremental strategy:**
- Cursor: `date` (last synced date in `YYYY-MM-DD` format)
- On incremental sync, set `start` to `last_synced_date` and `end` to today
- Request format: Convert cursor `YYYY-MM-DD` → `YYYYMMDD` for the API

**Rate limits (Dashboard REST API — cost model):**
- Active Users endpoint cost: ~30–120 cost units per request (depending on group-by)
- Shared budget: 108,000 cost/hour, 1,000 cost/5-minute burst window
- Concurrent limit: 5 requests across all REST API endpoints

---

### D. `average_session_length` — Dashboard REST API

**Endpoint:**
```
GET https://amplitude.com/api/2/sessions/average
```

**Authentication:** HTTP Basic

**Query Parameters:**

| Name | Required | Description |
|---|---|---|
| `start` | Yes | Start date `YYYYMMDD` |
| `end` | Yes | End date `YYYYMMDD` |

**Example:**
```bash
curl --location --request GET \
  'https://amplitude.com/api/2/sessions/average?start=20240301&end=20240315' \
  -u '{api_key}:{secret_key}'
```

**Response:**
```json
{
  "data": {
    "series": [[1204.0238276716443, 1197.4160169086904]],
    "seriesMeta": [{"segmentIndex": 0}],
    "xValues": ["2024-03-14", "2024-03-15"]
  }
}
```

**Flatten logic:** Zip `xValues` with `series[0]` values → one row per date.

**Incremental strategy:**
- Same as `active_users_counts`: cursor on `date`, request from `last_synced_date` to today.

---

### E. `cohorts` — Behavioral Cohorts API

**Endpoint:**
```
GET https://amplitude.com/api/3/cohorts
```

**Authentication:** HTTP Basic

**Optional query parameter:**

| Name | Description |
|---|---|
| `includeSyncInfo` | Boolean. Set to `true` to include sync metadata in the response. |

**Example:**
```bash
curl --location --request GET 'https://amplitude.com/api/3/cohorts' \
-u '{api_key}:{secret_key}'
```

**Response:**
```json
{
  "cohorts": [
    {
      "appId": 123456,
      "id": "id_12345",
      "name": "Active Last 30 Days",
      "size": 111,
      "published": true,
      "archived": false,
      "lastMod": 1679437294,
      "createdAt": 1679437288,
      "lastComputed": 1679440233
    }
  ]
}
```

**Pagination:** None. Returns all cohorts in a single response.

**Sync:** Full refresh. Max cohort size supported: 2,000,000 users (in the cohort metadata, not individual users).

**Note:** This endpoint returns cohort **definitions** only. Downloading individual user memberships requires a separate 3-step async flow (request → poll → download) which is more complex and subject to monthly usage quotas. User membership download is deferred; see Deferred Tables section.

---

### F. `annotations` — Chart Annotations API

**Endpoint:**
```
GET https://amplitude.com/api/3/annotations
```

**Authentication:** HTTP Basic

**Optional query parameters:**

| Name | Description |
|---|---|
| `category` | Filter by category name. |
| `chart_id` | Filter by chart ID. |
| `start` | ISO 8601 timestamp — only annotations after this time. |
| `end` | ISO 8601 timestamp — only annotations before this time. |

**Example (all annotations):**
```bash
curl --location --request GET 'https://amplitude.com/api/3/annotations' \
-u '{api_key}:{secret_key}'
```

**Response:**
```json
{
  "data": [
    {
      "id": 12345,
      "start": "2024-03-01T07:00:00+00:00",
      "details": "Feature X released",
      "category": {"id": 45678, "name": "Releases"},
      "end": null,
      "label": "Feature X Release",
      "chart_id": null
    }
  ]
}
```

**Pagination:** None. Returns all annotations in a single response.

**Sync:** Full refresh. Volume is typically very low (tens to hundreds of annotations per project).

---

## Field Type Mapping

### Amplitude → Spark/Delta Lake Types

| Amplitude API Type | Spark Type | Notes |
|---|---|---|
| `string` | `StringType` | Most text fields |
| `integer` (int) | `IntegerType` | Small integer values |
| `long` | `LongType` | `amplitude_id`, `session_id` |
| `float` | `DoubleType` | `location_lat`, `location_lng`, session length |
| `boolean` | `BooleanType` | `paying`, `archived`, `hidden`, etc. |
| `UTC ISO-8601 timestamp` (string in Export API) | `TimestampType` | Parse from string: `server_upload_time`, `event_time`, etc. |
| `ISO 8601 timestamp` (string in Annotations) | `TimestampType` | Parse annotation `start`/`end` fields |
| `Unix timestamp` (integer, in Cohorts API) | `TimestampType` | `lastMod`, `createdAt`, `lastComputed` — convert from epoch seconds |
| `dict` / `object` | `StringType` (JSON) or `MapType` | `event_properties`, `user_properties`, `group_properties`, `groups`, `data` |
| `array[string]` | `ArrayType(StringType)` | `owners`, `viewers`, `shortcut_ids` |
| `null` | `NullType` or nullable field | `sample_rate` is always null |
| `UUID` string | `StringType` | `uuid` field in events |

### Special field behaviors

- **`event_properties` / `user_properties` / `group_properties`:** Highly variable nested JSON objects. Contents depend on the event type and user properties defined in the Amplitude project. Represent as `StringType` (JSON string) or use `MapType(StringType, StringType)` for basic key-value pairs. Schema varies per event type.
- **`session_id`:** A long integer representing Unix epoch milliseconds at session start. Can be used to correlate events in the same session.
- **`amplitude_id`:** Amplitude's internal user identifier, stable across devices. A user can have multiple `device_id` values but a single `amplitude_id`.
- **`paying`:** A boolean sent by the client SDK. Can be null/missing for events where it was not set.
- **`data_region` effect:** EU projects use `analytics.eu.amplitude.com`; data stays in EU and is not replicated to US servers.

---

## Known Quirks and Implementation Notes

1. **Export API uses `server_upload_time` as the cursor, not `event_time`:** The Export API time range filters by when Amplitude received and processed the event (`server_upload_time`), not when the event occurred on the client (`event_time`). This means late-arriving events (e.g., offline events synced later) will appear in the window corresponding to their upload time, not their occurrence time. Apply a 2-hour lookback buffer since data is available ~2 hours after the hour ends.

2. **ZIP → gzip → NDJSON:** The Export API response is a ZIP file containing one or more `.gz` files per hour. Each `.gz` is a gzipped newline-delimited JSON file. Implementation must: (1) download ZIP bytes, (2) unzip in memory, (3) gunzip each entry, (4) parse each line as JSON.

3. **404 = No data (not an error):** The Export API returns HTTP 404 when there are no events in the requested time range. The connector must treat 404 as an empty result (0 rows), not an error.

4. **Time zone:** The Export API and most REST API endpoints return data in UTC. The Dashboard REST API time zone matches the Amplitude project's configured time zone for chart data — for consistency, use UTC-based requests.

5. **Dashboard REST API cost model:** Rate limiting uses a cost formula: `cost = (# days) × (# conditions) × (cost per query type)`. Active Users cost is ~30 (no group-by) or ~120 (with group-by). Budget: 108,000/hr, 1,000/5-min burst, 5 concurrent requests max.

6. **Events List excludes hidden events:** `GET /api/2/events/list` only returns visible events. If events were hidden after the last sync, they will disappear from future syncs but won't be marked as deleted in the output.

7. **Cohort `lastMod` is epoch seconds (integer):** Unlike ISO 8601 strings used elsewhere, the Cohorts API returns `lastMod`, `createdAt`, and `lastComputed` as Unix epoch seconds integers. Multiply by 1000 to convert to milliseconds if needed.

8. **EU data residency connector config:** Connectors must support switching the base URL from `https://amplitude.com` to `https://analytics.eu.amplitude.com` based on the `data_region` config parameter.

9. **Backfill scope for events:** Fivetran notes that Amplitude's primary key for events changed on May 1, 2014. For integrity, start backfills from 2014-05-01 at the earliest. In practice, connectors should let users specify `start_date` and default to a reasonable lookback (e.g., 1 year).

10. **Annotations API uses a different base path (`/api/3/`):** Unlike most Analytics endpoints which use `/api/2/`, Annotations and Cohorts use `/api/3/`. The Behavioral Cohorts download flow uses `/api/5/`.

---

## Deferred Tables

The following Amplitude objects are not included in the initial batch due to complexity, low coverage in reference implementations, or async/non-standard API patterns:

| Object | API | Reason for Deferral |
|---|---|---|
| **Cohort User Memberships** | `GET /api/5/cohorts/request/{id}` then poll then download | 3-step async flow; subject to monthly download quotas (500/month default); result is a list of user IDs or Amplitude IDs — not structured tabular data suitable for simple append |
| **User Activity** | `GET /api/2/useractivity` | Per-user endpoint requiring a `user` query param; not a bulk data export; intended for individual user lookup |
| **User Search** | `GET /api/2/usersearch` | Per-query search, not bulk export; rate limited to 360/hr |
| **Event Segmentation** | `GET /api/2/events/segmentation` | Analytics query API — requires specifying event and time range; not raw data; more suited for dashboarding than data ingestion |
| **Funnel Analysis** | `GET /api/2/funnels` | Analytics aggregate endpoint, not raw data |
| **Retention Analysis** | `GET /api/2/retention` | Analytics aggregate endpoint, not raw data |
| **Revenue LTV** | `GET /api/2/revenue/ltv` | Analytics aggregate endpoint; requires revenue tracking setup |
| **Real-time Active Users** | `GET /api/2/realtime` | Real-time only, up to 2 days of data; not suitable for batch ingestion |

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://amplitude.com/docs/apis/analytics/export | 2026-06-17 | High | Export API endpoint, query params, response schema (all event fields), status codes, size limits, considerations |
| Official Docs | https://amplitude.com/docs/apis/authentication | 2026-06-17 | High | API Key/Secret Key auth, where to find credentials |
| Official Docs | https://amplitude.com/docs/apis/analytics/dashboard-rest | 2026-06-17 | High | Dashboard REST API endpoints, rate limits, cost model, active users, average session length, events list schemas |
| Official Docs | https://amplitude.com/docs/apis/analytics/behavioral-cohorts | 2026-06-17 | High | Cohorts API endpoint, full cohort object schema, async download flow, usage limits |
| Official Docs | https://amplitude.com/docs/apis/analytics/chart-annotations | 2026-06-17 | High | Annotations API endpoint, full annotation schema, query parameters |
| Airbyte OSS | https://docs.airbyte.com/integrations/sources/amplitude | 2026-06-17 | High | Supported streams (Events, Events List, Active Users, Avg Session Length, Cohorts, Annotations), incremental vs full refresh classification, rate limit handling approach, EU data region config |
| Airbyte GitHub PR | https://github.com/airbytehq/airbyte/pull/75406 | 2026-06-17 | High | Confirmed cost-based rate limiting (108k/hr, 1k/5min burst), 5 concurrent request limit, Export API has no documented rate limit |
| Fivetran Docs | https://fivetran.com/docs/connectors/applications/amplitude | 2026-06-17 | Medium | Primary key for EVENT table (`id`, `device_id`, `server_upload_time`), sync history from 2014-05-01, supported tables (EVENT, EVENT_TYPE, EVENT_AMPLITUDE_ATTRIBUTION_ID) |
| Fivetran Docs | https://beta.fivetran.com/docs/connectors/applications/amplitude | 2026-06-17 | Medium | Confirmed delete capture on EVENT_TYPE table, custom data in EVENT table |
| Webeyez Technical Blog | https://webeyez.com/insights/guides/https-amplitude-com-api-2-export-guide | 2026-06-17 | Low | Confirmed chunking by 1-2 hour windows best practice, idempotency for retries, state store pattern |
| Nexla Docs | https://docs.nexla.com/user-guides/connectors/amplitude_api/amplitude_api_data_source | 2026-06-17 | Low | Confirmed Export API returns ZIP archives; Nexla processes automatically |
