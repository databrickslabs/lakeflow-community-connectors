# Lakeflow Amplitude Community Connector

Ingest product analytics data from **Amplitude** into Databricks Delta tables via Lakeflow Connect — no coding required.

This connector pulls from Amplitude's Export API, Dashboard REST API, Behavioral Cohorts API, Chart Annotations API, and Taxonomy API, giving you a complete picture of your product usage in your lakehouse.

---

## Prerequisites

- **Amplitude account and project**: Access to the Amplitude Analytics project whose data you want to ingest.
- **API Key and Secret Key**: The connector authenticates with HTTP Basic auth using your project's **API Key** and **Secret Key**.
  - Only project **Managers** and **Admins** can generate or view these credentials.
  - The Secret Key is shown **only at generation time** — copy it somewhere safe before closing the dialog.
- **Data region**: Know whether your Amplitude project uses **Standard** (US) or **EU** data residency.
- **Network access**: The Databricks environment running the connector must be able to reach:
  - Standard (US): `https://amplitude.com`
  - EU: `https://analytics.eu.amplitude.com`

---

## Step 1: Get Your Amplitude Credentials

**API Key and Secret Key:**
1. In Amplitude, go to **Settings → Projects** and select your project.
2. On the **General** tab, click **Manage** next to **API Key** or **Secret Key** to view and copy both values.
3. Store them securely — you will need them when creating the connection.

**Data region:**
- If your project was created in the EU, you'll need `data_region = eu`.
- Otherwise, leave it as the default (`standard`).

---

## Step 2: Create a Connection in Databricks

1. In your Databricks workspace, go to **Add Data → Lakeflow Community Connectors**.
2. Search for **Amplitude** and click **Configure**.
3. Fill in the connection form:

| Field | Required | What to enter |
|-------|----------|---------------|
| **API Key** | Yes | Your Amplitude project API Key |
| **Secret Key** | Yes | Your Amplitude project Secret Key |
| **Data Region** | No | `standard` (default) or `eu` |
| **Start Date** | No | ISO-8601 date for the earliest events to backfill, e.g. `2024-01-01`. Only affects the `events` table and other incremental tables on the first run. Leave blank to sync only the most recent data. |
| **External Options Allow List** | Yes | Copy and paste this value exactly: `window_hours,window_days,m,i,g` |

> **Why do I need External Options Allow List?**
> This connector supports optional per-table settings (such as date window widths and grouping). The `externalOptionsAllowList` field tells Databricks which option names are allowed to pass through from your pipeline to the connector. Simply paste `window_hours,window_days,m,i,g` into the field — you don't need to understand each option unless you want to customize behavior later.

4. Click **Test connection** to verify, then **Save**.

---

## Step 3: Configure Your Pipeline

Once the connection is saved, Databricks will guide you through creating an ingestion pipeline. You will choose which tables to ingest. The pipeline configuration (stored in `ingest.py` in your workspace) looks like this — **you only need to edit the table list and any optional settings**:

```json
{
  "connection_name": "my_amplitude_connection",
  "objects": [
    { "table": { "source_table": "events" } },
    { "table": { "source_table": "events_list" } },
    { "table": { "source_table": "active_users_counts" } },
    { "table": { "source_table": "average_session_length" } },
    { "table": { "source_table": "sessions_per_user" } },
    { "table": { "source_table": "session_length_distribution" } },
    { "table": { "source_table": "cohorts" } },
    { "table": { "source_table": "annotations" } },
    { "table": { "source_table": "taxonomy_events" } },
    { "table": { "source_table": "taxonomy_user_properties" } }
  ]
}
```

> **Tip — Start small:** Add only 2–3 lightweight tables (e.g. `events_list`, `taxonomy_events`) on the first run to verify credentials and data shape. Add the high-volume `events` table once you've confirmed the connection is working.

---

## Step 4: Run and Schedule the Pipeline

Run the pipeline from the Databricks UI or schedule it as a recurring job.

- **First run**: the connector performs a full backfill. For the `events` table, backfill starts from the `start_date` you set in the connection (or the most recent 24-hour window if `start_date` was left blank).
- **Subsequent runs**: the connector picks up from where it left off using stored watermarks — only new or changed data is fetched.

---

## Supported Tables

The connector exposes the following 10 tables. Use the exact names from the **Table** column as `source_table` in your pipeline:

| Table | API source | How it syncs | Primary key(s) | Notes |
|-------|-----------|--------------|----------------|-------|
| `events` | Export API | Append-only (incremental) | `uuid` | Raw event stream. High volume — tune with `window_hours`. |
| `events_list` | Dashboard REST API | Snapshot (full refresh) | `value` | Catalog of all event types defined in your project. |
| `active_users_counts` | Dashboard REST API | CDC (incremental by date) | `date`, `segment` | Daily/weekly/monthly active or new user counts. |
| `average_session_length` | Dashboard REST API | CDC (incremental by date) | `date` | Average session duration per day. |
| `session_length_distribution` | Dashboard REST API | Snapshot | `bucket` | Session length histogram over a rolling window. |
| `sessions_per_user` | Dashboard REST API | CDC (incremental by date) | `date` | Average number of sessions per user per day. |
| `cohorts` | Behavioral Cohorts API | Snapshot (full refresh) | `id` | User segments defined in Amplitude. |
| `annotations` | Chart Annotations API | Snapshot (full refresh) | `id` | Chart annotations for marking events on dashboards. |
| `taxonomy_events` | Taxonomy API | Snapshot (full refresh) | `event_type` | Event type definitions from your Tracking Plan. |
| `taxonomy_user_properties` | Taxonomy API | Snapshot (full refresh) | `user_property` | User property definitions from your Tracking Plan. |

### How incremental tables work

- **`events`** (append-only): reads the Export API in hourly windows. The cursor is `server_upload_time` — the time Amplitude processed the event, not the client-side event time. Late-arriving events (e.g. from offline devices) appear in the window matching their upload time.
- **`active_users_counts`, `average_session_length`, `sessions_per_user`** (CDC by date): read Dashboard endpoints in day-width windows. The cursor is a `YYYY-MM-DD` date string. The API returns a time series which the connector flattens to one row per date.
- **`session_length_distribution`** (snapshot): fetches the session length histogram over a rolling lookback window. Returns one row per bucket (e.g. `"60s-300s"`). There is no per-day dimension — the API aggregates the entire requested range.
- **`events_list`, `cohorts`, `annotations`, `taxonomy_events`, `taxonomy_user_properties`** (snapshot): full refresh every run.

> **No delete capture:** None of the tables track deletions. Hidden/deleted events simply stop appearing in snapshot tables. The Export API does not surface deleted events.

---

## Optional Per-Table Settings

These settings are **optional** — all tables work without them. To use them, add them inside the `table_configuration` block for the relevant table in your pipeline configuration:

```json
{ "table": {
    "source_table": "events",
    "table_configuration": { "window_hours": "6" }
  }
}
```

| Option | Applies to | Default | Description |
|--------|-----------|---------|-------------|
| `window_hours` | `events` | `24` | Width (in hours) of each Export API request. Reduce to `1`–`6` for high-volume projects that hit Export API timeouts or the 4 GB per-window limit. |
| `lookback_hours` | `events` | `2` | How far back from "now" to stop reading, so the connector never queries hours whose data has not been published yet. Amplitude's Export API makes an hour's data available ~2 hours after the server receives it, so the default `2` avoids silently skipping not-yet-available events. Increase if you still see gaps; set `0` to read right up to the current hour. |
| `window_days` | `active_users_counts`, `average_session_length`, `sessions_per_user`, `session_length_distribution` | `30` | Width (in days) of each date window. |
| `m` | `active_users_counts` | `active` | User metric — `active` or `new`. |
| `i` | `active_users_counts` | `1` | Interval — `1` (daily), `7` (weekly), or `30` (monthly). |
| `g` | `active_users_counts` | *(none)* | Group-by property (e.g. `country`). When set, results are split by group value and the primary key becomes `(date, segment)`. |

**Full example with options:**

```json
{
  "connection_name": "my_amplitude_connection",
  "objects": [
    {
      "table": {
        "source_table": "events",
        "table_configuration": { "window_hours": "6" }
      }
    },
    {
      "table": {
        "source_table": "active_users_counts",
        "table_configuration": { "window_days": "30", "m": "active", "i": "1", "g": "country" }
      }
    },
    {
      "table": {
        "source_table": "average_session_length",
        "table_configuration": { "window_days": "30" }
      }
    },
    {
      "table": {
        "source_table": "sessions_per_user",
        "table_configuration": { "window_days": "30" }
      }
    },
    { "table": { "source_table": "session_length_distribution" } },
    { "table": { "source_table": "events_list" } },
    { "table": { "source_table": "cohorts" } },
    { "table": { "source_table": "annotations" } },
    { "table": { "source_table": "taxonomy_events" } },
    { "table": { "source_table": "taxonomy_user_properties" } }
  ]
}
```

---

## Schema Notes

### Fields that need special handling

- **`events`**: `event_properties`, `user_properties`, `group_properties`, `groups`, and `data` are free-form objects stored as `MapType(StringType, StringType)` — their keys vary by event type and project.
- **`cohorts`**: `definition` is a complex Amplitude-internal blob serialized to a **JSON string**. Timestamp fields (`lastMod`, `createdAt`, `lastComputed`, `last_viewed`) are Unix epoch **seconds** stored as `LongType`.
- **`annotations`**: `category` is a struct `{id, name}`.
- **`taxonomy_events`**: `category_name` is flattened from the API's nested `category.name`. `tags` is an array of strings.
- **`taxonomy_user_properties`**: Custom user properties have a `gp:` prefix in `user_property` (e.g. `gp:plan_type`). `enum_values` and `classifications` are arrays of strings. `regex` stores the validation regex pattern if one is defined.

### Data type mapping

| Amplitude API type | Spark / Delta type | Notes |
|--------------------|--------------------|-------|
| string | `StringType` | Most text fields. |
| integer / count | `LongType` | All numeric IDs and counts — `LongType` prevents overflow. |
| float | `DoubleType` | Floating-point values (`location_lat`, `avg_sessions`, etc.). |
| boolean | `BooleanType` | `true`/`false` flags. |
| ISO-8601 timestamp (string) | `TimestampType` | `event_time`, `server_upload_time`, annotation `start`/`end`. |
| Unix epoch seconds (integer) | `LongType` | Cohort `lastMod`, `createdAt`, `lastComputed`. Multiply by 1000 for milliseconds. |
| object / free-form dict | `MapType(StringType, StringType)` | `event_properties`, `user_properties`, etc. |
| nested struct | `StructType` | Annotation `category` → `{id, name}`. |
| complex nested object | `StringType` (JSON) | Cohort `definition` — serialized to a JSON string. |
| array of strings | `ArrayType(StringType)` | Cohort `owners`/`viewers`; taxonomy `tags`, `classifications`, `enum_values`. |

---

## Troubleshooting

### Authentication failures (`401` / `403`)
- Verify `api_key` and `secret_key` are correct and have not been revoked.
- Only project **Managers** and **Admins** can issue keys. The Secret Key is visible only at creation time — if it was lost, regenerate it in Amplitude (**Settings → Projects → API Key & Secret Key**).

### Wrong data region
- If your project uses EU data residency, the connection must have `data_region = eu`. A Standard-region key will not work against the EU host and vice versa.

### `events` table returns no rows
- The Export API returns HTTP 404 when there is no data in the requested window — the connector treats this as 0 rows (not an error).
- Check that `start_date` is not in the future and that you've allowed for the ~2-hour data-availability lag (Export data is available approximately 2 hours after the end of each hour).

### Export API timeouts (`504`) or size errors (`400`)
- Reduce `window_hours` so each request covers a smaller time range.
- For extremely high-volume projects, consider Amplitude's native S3 Export as an alternative.

### Rate limiting (`429`)
- The connector retries `429`, `500`, `502`, `503`, and `504` responses with exponential backoff.
- Persistent throttling means you should widen the schedule interval or reduce pipeline concurrency.
- The Dashboard REST API has a cost model (~108,000 cost units/hour, 1,000 per 5-minute burst). Avoid unnecessary group-by queries, which cost more.

### Missing or redacted events
- `events_list` only returns **visible** events. Hidden or deleted event types will not appear.
- The Export API does not surface deletions — events disappearing from Amplitude dashboards will simply stop appearing in new export windows.

---

## Reference

| Resource | Link |
|----------|------|
| Amplitude Authentication docs | https://amplitude.com/docs/apis/authentication |
| Export API | https://amplitude.com/docs/apis/analytics/export |
| Dashboard REST API | https://amplitude.com/docs/apis/analytics/dashboard-rest |
| Behavioral Cohorts API | https://amplitude.com/docs/apis/analytics/behavioral-cohorts |
| Chart Annotations API | https://amplitude.com/docs/apis/analytics/chart-annotations |
| Taxonomy API | https://amplitude.com/docs/apis/analytics/taxonomy |
| Connector implementation | `src/databricks/labs/community_connector/sources/amplitude/amplitude.py` |
| Connector schemas | `src/databricks/labs/community_connector/sources/amplitude/amplitude_schemas.py` |
