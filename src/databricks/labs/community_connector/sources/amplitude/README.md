# Lakeflow Amplitude Community Connector

This documentation describes how to configure and use the **Amplitude** Lakeflow community connector to ingest product analytics data from Amplitude's Analytics APIs (Export API, Dashboard REST API, Behavioral Cohorts API, Chart Annotations API, and Taxonomy API) into Databricks.

## Prerequisites

- **Amplitude account and project**: You need access to the Amplitude Analytics project whose data you want to ingest.
- **API Key and Secret Key**: The connector authenticates with HTTP Basic auth using your project's **API Key** and **Secret Key**.
  - Only project **Managers** and **Admins** can generate or view API keys and secret keys.
  - The Secret Key is shown **only at generation time**, so store it securely.
- **Data region awareness**: Know whether your Amplitude project uses **Standard** (US) or **EU** data residency, since they use different API hosts.
- **Network access**: The environment running the connector must be able to reach either `https://amplitude.com` (standard) or `https://analytics.eu.amplitude.com` (EU).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector.

| Name        | Type   | Required | Description                                                                                          | Example                      |
|-------------|--------|----------|------------------------------------------------------------------------------------------------------|------------------------------|
| `api_key`   | string | Yes      | Amplitude project API Key used for HTTP Basic authentication.                                         | `1234567890abcdef...`        |
| `secret_key`| string | Yes      | Amplitude project Secret Key used for HTTP Basic authentication.                                      | `abcdef1234567890...`        |
| `data_region` | string | No     | Data-residency region. One of `standard` (default, US host) or `eu` (EU host). Case-insensitive.      | `eu`                         |
| `start_date`  | string | No     | ISO-8601 datetime lower bound for the initial backfill of incremental tables (`events`, `active_users_counts`, `average_session_length`, `sessions_per_user`). If omitted, the connector defaults to a single recent window. | `2024-01-01T00:00:00Z` |

> **Note:** If both `api_key` and `secret_key` are not provided, the connector raises an error at startup. An unsupported `data_region` value also raises an error (only `standard` and `eu` are accepted).

#### `externalOptionsAllowList` (required)

This connector supports **table-specific options** that are passed per table via the pipeline spec. For the connection to allow these options to pass through, you **must** set the `externalOptionsAllowList` connection option to the full list below:

```
window_hours,window_days,m,i,g
```

| Option         | Applies to                                                                                    | Description |
|----------------|-----------------------------------------------------------------------------------------------|-------------|
| `window_hours` | `events`                                                                                      | Width (in hours) of each Export API window per read call. Default `24`. Reduce for high-volume projects that hit timeouts or the 4 GB response limit. |
| `window_days`  | `active_users_counts`, `average_session_length`, `sessions_per_user`, `session_length_distribution` | Width (in days) of each Dashboard date-series window per read call. Default `30` (distribution uses `30`, others use `30`). |
| `m`            | `active_users_counts`                                                                         | User metric: `active` (default) or `new`. |
| `i`            | `active_users_counts`                                                                         | Interval: `1` (daily, default), `7` (weekly), or `30` (monthly). |
| `g`            | `active_users_counts`                                                                         | Group-by property name (e.g. `country`). When set, results are split into one segment per group value. |

### Obtaining the Required Parameters

**API Key and Secret Key:**
1. In the Amplitude Analytics web app, go to **Settings → Projects**.
2. Select the target project.
3. On the **General** tab, click **Manage** next to **API Key** or **Secret Key** to view/copy the values.
4. Use these as the `api_key` and `secret_key` connection options.

**Data region:**
- If your Amplitude project uses **EU data residency**, set `data_region` to `eu`. Otherwise leave it unset (or `standard`).

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `window_hours,window_days,m,i,g` (required for this connector to pass table-specific options through to the source).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Amplitude connector exposes a **static list** of tables (Amplitude does not provide object discovery). Use the exact lowercase names below as `source_table`:

| Table                          | API source                | Ingestion type | Primary key(s)      | Incremental cursor    |
|--------------------------------|---------------------------|----------------|---------------------|-----------------------|
| `events`                       | Export API                | `append`       | `uuid`              | `server_upload_time`  |
| `events_list`                  | Dashboard REST API        | `snapshot`     | `value`             | n/a                   |
| `active_users_counts`          | Dashboard REST API        | `cdc`          | `date`, `segment`   | `date`                |
| `average_session_length`       | Dashboard REST API        | `cdc`          | `date`              | `date`                |
| `session_length_distribution`  | Dashboard REST API        | `snapshot`     | `bucket`            | n/a                   |
| `sessions_per_user`            | Dashboard REST API        | `cdc`          | `date`              | `date`                |
| `cohorts`                      | Behavioral Cohorts API    | `snapshot`     | `id`                | n/a                   |
| `annotations`                  | Chart Annotations API     | `snapshot`     | `id`                | n/a                   |
| `taxonomy_events`              | Taxonomy API              | `snapshot`     | `event_type`        | n/a                   |
| `taxonomy_user_properties`     | Taxonomy API              | `snapshot`     | `user_property`     | n/a                   |

### Incremental behavior

- **`events`** (append-only): Reads the Export API in sliding hourly windows. The cursor is `server_upload_time` (ISO-8601), i.e. the time Amplitude processed the event — **not** the client `event_time`. Each read call covers one `window_hours`-wide window and advances the cursor up to an init-time snapshot, so a `Trigger.AvailableNow` run terminates deterministically. Late-arriving (e.g. offline) events appear in the window matching their upload time. Append-only tables never truncate a server response, so `window_hours` (not a record cap) bounds per-call work.
- **`active_users_counts`, `average_session_length`, `sessions_per_user`** (CDC by date): Read the Dashboard endpoints in sliding day windows. The cursor is a `YYYY-MM-DD` date string that advances up to the init-time date. The API returns a `{series, xValues}` time series which the connector flattens to one row per date. For `active_users_counts` the primary key is `(date, segment)` — `segment` is always populated (`"Totals"` when `g=` is not set, or the group-by value when it is).
- **`session_length_distribution`** (snapshot): Fetches the session length histogram over a sliding `window_days` lookback. Returns one row per pre-defined bucket (e.g. `"60s-300s"`). There is no per-day dimension — the API aggregates over the entire requested range.
- **`events_list`, `cohorts`, `annotations`, `taxonomy_events`, `taxonomy_user_properties`** (snapshot): Full refresh on every run; these endpoints have no reliable cursor and return the full list in a single response.

> **No delete capture:** None of the tables support delete synchronization. The Export API does not surface deleted events; hidden/deleted events simply stop appearing in snapshot tables.

### Columns that require attention

- **`events`**: `event_properties`, `user_properties`, `group_properties`, `groups`, and `data` are free-form objects modeled as `MapType(StringType, StringType)` since their keys vary per event type and project.
- **`cohorts`**: `definition` is an opaque, deeply-nested blob serialized to a JSON **string**. Timestamp fields (`lastMod`, `createdAt`, `lastComputed`, `last_viewed`) are Unix epoch **seconds** stored as `LongType`.
- **`annotations`**: `category` is a nested struct with `id` and `name`.
- **`taxonomy_events`**: `category_name` is flattened from the nested `category.name` field in the API response. `tags` is an array of strings.
- **`taxonomy_user_properties`**: Custom user properties have a `gp:` prefix in `user_property` (e.g. `gp:plan_type`). `enum_values` and `classifications` are arrays of strings.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (one of the supported tables above). |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default). |
| `destination_schema` | No | Target schema (defaults to pipeline's default). |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables (`events`) do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys. |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

All source-specific options are optional and have sensible defaults. They must be included in the connection's `externalOptionsAllowList` (see Setup).

- **`events`**:
  - `window_hours` (integer, default `24`): Width of each Export API time window. Reduce (e.g. `1`–`6`) for high-volume projects that hit timeouts or the 4 GB per-window response limit.
  - The initial backfill lower bound comes from the **connection-level** `start_date` option. If unset, only the most recent `window_hours` window is read.
- **`active_users_counts`**:
  - `window_days` (integer, default `30`): Width of each date window.
  - `m` (string, default `active`): `active` or `new` users.
  - `i` (string, default `1`): Interval — `1` (daily), `7` (weekly), or `30` (monthly).
  - `g` (string, optional): Group-by property name (e.g. `country`) to split counts by segment.
- **`average_session_length`**:
  - `window_days` (integer, default `30`): Width of each date window.
- **`session_length_distribution`**:
  - `window_days` (integer, default `30`): Lookback window for the session length histogram. The API aggregates all sessions in this range into a single set of bucket counts.
- **`sessions_per_user`**:
  - `window_days` (integer, default `30`): Width of each date window.
- **`events_list`, `cohorts`, `annotations`, `taxonomy_events`, `taxonomy_user_properties`**: No source-specific options required.

## Data Type Mapping

Amplitude API types are mapped to Spark/Delta types as follows:

| Amplitude API type | Example fields | Spark type | Notes |
|---|---|---|---|
| string | `event_type`, `user_id`, `value`, `name`, `user_property`, `bucket` | `StringType` | Most text fields. |
| integer | `event_id`, `app`, `totals`, `size`, `count` | `LongType` | All numeric IDs/counts stored as `LongType` to avoid overflow. |
| long | `amplitude_id`, `session_id` | `LongType` | Large integer identifiers. |
| float | `location_lat`, `location_lng`, `length`, `avg_sessions` | `DoubleType` | Floating-point values. |
| boolean | `paying`, `archived`, `hidden`, `deleted`, `is_active`, `is_array_type` | `BooleanType` | Standard `true`/`false`. |
| UTC ISO-8601 timestamp (string) | `event_time`, `server_upload_time`, annotation `start`/`end` | `TimestampType` | Parsed from string. |
| Unix epoch seconds (integer) | cohort `lastMod`, `createdAt`, `lastComputed` | `LongType` | Stored as raw epoch seconds; multiply by 1000 for milliseconds. |
| object / dict | `event_properties`, `user_properties`, `group_properties`, `groups`, `data` | `MapType(StringType, StringType)` | Free-form keys vary per event type/project. |
| nested object | annotation `category` | `StructType` | `{id, name}`. |
| complex nested object | cohort `definition` | `StringType` (JSON) | Serialized to a JSON string. |
| array of strings | cohort `owners`, `viewers`; taxonomy `tags`, `classifications`, `enum_values` | `ArrayType(StringType)` | Preserved as arrays. |
| UUID string | `events.uuid` | `StringType` | Globally unique event identifier (primary key). |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Amplitude connector source (`amplitude.py`) into your workspace so Lakeflow can load it.

### Step 2: Configure Your Pipeline

In your pipeline file (e.g. `ingest.py`), configure a `pipeline_spec` that references the Unity Catalog connection for this connector and one or more tables to ingest. Example:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

register(spark, "amplitude")

pipeline_spec = {
    "connection_name": "amplitude_connection",
    "objects": [
        # ---- raw events (append-only, incremental by server_upload_time) ----
        {"table": {"source_table": "events",
                   "table_configuration": {"window_hours": "6"}}},

        # ---- dashboard time-series (CDC by date) ----
        {"table": {"source_table": "active_users_counts",
                   "table_configuration": {"window_days": "30", "m": "active",
                                           "i": "1", "g": "country"}}},
        {"table": {"source_table": "average_session_length",
                   "table_configuration": {"window_days": "30"}}},
        {"table": {"source_table": "sessions_per_user",
                   "table_configuration": {"window_days": "30"}}},

        # ---- session length histogram (snapshot over window) ----
        {"table": {"source_table": "session_length_distribution",
                   "table_configuration": {"window_days": "30"}}},

        # ---- snapshots (full-refresh each run) ----
        {"table": {"source_table": "events_list"}},
        {"table": {"source_table": "cohorts"}},
        {"table": {"source_table": "annotations"}},
        {"table": {"source_table": "taxonomy_events"}},
        {"table": {"source_table": "taxonomy_user_properties"}},
    ]
}

ingest(spark, pipeline_spec)
```

- `connection_name` must point to the UC connection configured with your Amplitude `api_key` and `secret_key` (and optional `data_region` / `start_date`), with `externalOptionsAllowList` set to `window_hours,window_days,m,i,g`.
- For each `table`, `source_table` must be one of the supported table names, and source-specific options go under `table_configuration`.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g. a scheduled job or workflow). For the `events` table:

- On the **first run**, set the connection-level `start_date` to control how far back the backfill goes. If omitted, only the most recent window is read.
- On **subsequent runs**, the connector resumes from the stored `server_upload_time` cursor. Because Export data is available ~2 hours after the hour ends, allow for a short lag before recent events appear.

#### Best Practices

- **Start small**: Begin with lightweight snapshot tables (e.g. `events_list`, `taxonomy_events`, `taxonomy_user_properties`) to validate configuration and data shape before enabling high-volume incremental tables like `events`.
- **Use incremental sync**: Rely on the `append`/`cdc` cursors for `events`, `active_users_counts`, and `average_session_length` to minimize API calls.
- **Tune the Export window**: Lower `window_hours` for high-volume projects to avoid Export API timeouts (HTTP 504) and the 4 GB per-window size limit (HTTP 400). If a single hour exceeds 4 GB, use Amplitude's Amazon S3 Export instead.
- **Respect rate limits**: The Dashboard REST API uses a cost model (~108,000 cost units/hour, 1,000 per 5-minute burst, max 5 concurrent requests across all endpoints). Space out syncs and avoid unnecessary group-by queries, which cost more.
- **Set appropriate schedules**: Balance data freshness against API usage limits.

#### Troubleshooting

**Common Issues:**

- **Authentication failures (`401` / `403`)**: Verify `api_key` and `secret_key` are correct and not revoked. Only project Managers/Admins can issue keys; the Secret Key is visible only at creation time.
- **Wrong data region / connection errors**: If your project uses EU data residency, set `data_region` to `eu`. A standard-region key will not work against the EU host and vice versa.
- **`events` returns no rows**: The Export API returns HTTP 404 when there is no data in the requested window — the connector treats this as 0 rows, not an error. Also confirm `start_date` is not in the future and that you have allowed for the ~2-hour data-availability lag.
- **Export API timeouts (`504`) or size errors (`400`)**: Reduce `window_hours` so each request covers a smaller time range. For extremely high volume, use Amplitude's S3 Export.
- **Rate limiting (`429`)**: The connector retries transient errors (`429`, `500`, `502`, `503`, `504`) with exponential backoff. Persistent throttling means you should widen schedule intervals or reduce concurrency.
- **Missing hidden/deleted events**: `events_list` only returns visible events, and the Export API does not surface deletes. Disappearing events are expected behavior, not data loss in the connector.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/amplitude/amplitude.py`
- Connector schemas and constants: `src/databricks/labs/community_connector/sources/amplitude/amplitude_schemas.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/amplitude/amplitude_api_doc.md`
- Official Amplitude API documentation:
  - Authentication: `https://amplitude.com/docs/apis/authentication`
  - Export API: `https://amplitude.com/docs/apis/analytics/export`
  - Dashboard REST API: `https://amplitude.com/docs/apis/analytics/dashboard-rest`
  - Behavioral Cohorts API: `https://amplitude.com/docs/apis/analytics/behavioral-cohorts`
  - Chart Annotations API: `https://amplitude.com/docs/apis/analytics/chart-annotations`
  - Taxonomy API: `https://amplitude.com/docs/apis/analytics/taxonomy`
