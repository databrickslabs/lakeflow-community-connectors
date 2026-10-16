# Lakeflow Klaviyo Community Connector

Ingest data from the **Klaviyo JSON:API** into Databricks Delta tables via Lakeflow Connect. The connector reads eight core Klaviyo objects — profiles, events, lists, campaigns, metrics, flows, segments, and templates — using opaque cursor pagination and incremental `updated`/`updated_at` filters. The runtime flattens Klaviyo's JSON:API envelope (`{type, id, attributes, relationships}`) so primary keys and cursor fields land as flat top-level columns. Two high-volume tables (`events`, `campaigns`) can be read in parallel across executors; the rest run on a single driver.

## Prerequisites

- **Klaviyo account**: any tier. The eight supported endpoints are available on all plans.
- **Private API key**: a server-side key beginning with `pk_…`. Generate at [klaviyo.com/account#api-keys](https://www.klaviyo.com/account#api-keys). Public keys (the 6-character company ID) are for client-side use only and will not work.
- **Read scopes** on the API key: `profiles:read`, `events:read`, `lists:read`, `campaigns:read`, `metrics:read`, `flows:read`, `segments:read`, `templates:read`.
- **Network access**: the environment running the connector must reach `https://a.klaviyo.com/api/`.
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_key` | string (secret) | yes | Klaviyo private API key. Must begin with `pk_`. Sent on every request as `Authorization: Klaviyo-API-Key <key>`. | `pk_abc123…` |
| `api_revision` | string | no | Klaviyo API revision date, sent as the `revision` request header. Pins response shape so an upstream default change cannot break the connector. Defaults to `2024-10-15`. See [Klaviyo's revision strategy](https://developers.klaviyo.com/en/docs/api_versioning_and_deprecation_policy). | `2024-10-15` |
| `externalOptionsAllowList` | string | no | Comma-separated allow-list of per-table options that the framework will forward to the connector. Required only if you want to override defaults per table. See **Table options** below. | `max_records_per_batch,lookback_seconds,page_size,window_seconds,start_timestamp` |

### Obtaining the API key

1. Sign in to your Klaviyo account.
2. Open **Settings → API keys** (or navigate directly to [klaviyo.com/account#api-keys](https://www.klaviyo.com/account#api-keys)).
3. Click **Create Private API Key**, give it a name (e.g. `Lakeflow ingestion`), and grant at least the read scopes listed under **Prerequisites**.
4. Copy the key (it begins with `pk_…`). The key is shown once — store it in a secret manager.

> **Authentication detail.** Klaviyo expects the literal prefix `Klaviyo-API-Key` in front of the key in the `Authorization` header (not `Bearer`). The connector adds this prefix for you; supply only the raw `pk_…` value in `api_key`.

### Pinning the API revision

Klaviyo follows a 2-year revision lifecycle (Stable → Deprecated → Retired). Each revision is pinned by sending the `revision` header on every request. **Always set `api_revision`** rather than letting Klaviyo default to "latest" — a new default can change response shapes without warning. The connector defaults to `2024-10-15` (the revision the schemas in this repo were derived from). Subscribe to the [Klaviyo developer changelog](https://developers.klaviyo.com/en/docs/changelog) and plan to update the pin every 12–18 months.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide the required connection parameters (`api_key`, optionally `api_revision`, and an `externalOptionsAllowList` if you intend to set per-table options).

The connection can also be created using the standard Unity Catalog API. Example SQL:

```sql
CREATE CONNECTION my_klaviyo_connection
TYPE lakeflow_community_connector
OPTIONS (
  source = 'klaviyo',
  api_key = SECRET('my_scope', 'klaviyo_api_key'),
  api_revision = '2024-10-15',
  externalOptionsAllowList = 'max_records_per_batch,lookback_seconds,page_size,window_seconds,start_timestamp'
);
```

## Supported Objects

The Klaviyo connector exposes **8 tables**:

| Table | Description | Primary key | Cursor field | Ingestion type | Notes |
|-------|-------------|-------------|--------------|----------------|-------|
| `profiles` | Customer contact records — the central identity object in Klaviyo (one per identifiable person). | `id` | `updated` | `cdc` | Open-ended `properties` map for custom attributes. |
| `events` | Immutable event log — every action a profile took (Placed Order, Opened Email, …). High-volume; append-only. | `id` | `datetime` | `append` | 1-hour default lookback for late-arriving events. Partitioned by time window across executors. `profile_id` and `metric_id` are hoisted from the JSON:API relationships block as flat columns. |
| `lists` | Static subscriber list definitions. | `id` | `updated` | `cdc` | Server filter is `greater-than`-only (no `greater-or-equal`); the connector applies a 1-second lookback to avoid missing equal-timestamp records. Max page size is 10. |
| `campaigns` | Email / SMS marketing campaign objects, with audience, scheduling, and tracking settings. | `id` | `updated_at` (per channel) | `cdc` | **Channel fan-out**: Klaviyo *requires* a `messages.channel` filter on every request, so the connector issues one paginated walk per channel (`email`, `sms`) and unions results. The cursor is a **per-channel bag** (not a single timestamp). A `channel` column is stamped onto each record. Partitioned by channel across executors. Klaviyo's general docs list `mobile_push` as a campaign channel, but on revision `2024-10-15` the `messages.channel` filter rejects it (HTTP 400) — see `CAMPAIGN_CHANNELS` in `klaviyo_schemas.py` for the active set, and expand the tuple + re-record cassettes if a future revision adds support. |
| `metrics` | Metric definitions — categories of events (e.g. "Placed Order"). Reference / lookup table. | `id` | — | `snapshot` | No timestamp filter exists on this endpoint, so every sync re-fetches the full set. Small table (typically < 200 records). |
| `flows` | Marketing automation flows (multi-step sequences). | `id` | `updated` | `cdc` | **Strictest rate limit of all eight tables: 3/s burst, 60/m steady.** The connector paces requests accordingly. Max page size is 50. |
| `segments` | Dynamic audience segments (condition-based, not static). | `id` | `updated` | `cdc` | Server filter is `greater-than`-only; same 1-second lookback as `lists`. `definition` is stored as an open-ended map (the condition tree shape is arbitrary). Max page size is 10. |
| `templates` | Email / SMS message templates including HTML, plain-text, and AMP bodies. | `id` | `updated` | `cdc` | `html` can be large. Max page size is 10. |

### Record shape

Klaviyo wraps each record as `{type, id, attributes: {...}, relationships: {...}}`. The connector **flattens** this so:

- `id` and `type` are top-level columns.
- Every field under `attributes` is hoisted to the top level (the framework does not allow dotted primary keys, so this flattening is required).
- For `events`, the `profile` and `metric` relationship IDs are hoisted to `profile_id` and `metric_id`.
- For `campaigns`, the queried channel is stamped onto every row as a `channel` column so downstream queries don't have to re-parse the nested `messages` block.

Free-form Klaviyo objects (`profiles.properties`, `events.event_properties`, `segments.definition`) are typed as `MapType(StringType, StringType)` because their schemas are user-defined and cannot be pinned. Datetimes are stored as ISO 8601 strings (cast downstream as needed). See `klaviyo_schemas.py` for the full Spark schemas.

### Table options

These are forwarded per-call when the option name appears in `externalOptionsAllowList`. Connection-level fields (`api_key`, `api_revision`) cannot appear in this list — Databricks rejects that at deploy time.

| Option | Applies to | Default | Description |
|--------|------------|---------|-------------|
| `max_records_per_batch` | all incremental tables (`profiles`, `events`, `lists`, `campaigns`, `flows`, `segments`, `templates`) | unbounded | Admission-control cap. The connector returns at most this many records per microbatch and emits a partial cursor when the cap is reached. Useful for keeping `Trigger.AvailableNow` runs converging on very busy accounts. |
| `page_size` | `profiles`, `events`, `lists`, `flows`, `segments`, `templates` | per-endpoint max (`profiles` 100, `events` 1000, `lists` 10, `flows` 50, `segments` 10, `templates` 10) | Value sent as `page[size]` and clamped to the endpoint's documented maximum. Ignored for `campaigns` and `metrics` — revision `2024-10-15` rejects `page[size]` on those endpoints with HTTP 400, so the connector omits the parameter and lets the server choose its default page size (cursor pagination still works). |
| `lookback_seconds` | `lists`, `segments` (default 1), `events` (default 3600) | see description | Subtracted from the saved cursor when issuing the next call. Required for `lists` / `segments` because their server filter is `greater-than`-only — without lookback, records whose `updated` equals the previous cursor would be missed. For `events`, the lookback catches Klaviyo events whose `datetime` is back-dated by the producer. |
| `window_seconds` | `events` (partitioned reads) | 86400 (one day) | Time-window size when splitting `events` across executors. One day is a good balance — small accounts get one cheap partition per window; large accounts get parallelism on multi-day backfills. |
| `start_timestamp` | `events` (partitioned reads) | one window before the current cap | First-microbatch backfill anchor for `events`. Supply an ISO 8601 RFC 3339 timestamp (e.g. `2024-01-01T00:00:00Z`) to load history older than one window. |

## How to Run

### Step 1: Reference the connector in your workspace

Use the Lakeflow Community Connector UI to register or reference the Klaviyo connector source in your workspace.

### Step 2: Configure your pipeline

In your `ingest.py` (or equivalent), point at the Unity Catalog connection and list the tables to ingest:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
register(spark, "klaviyo")

pipeline_spec = {
    "connection_name": "my_klaviyo_connection",
    "objects": [
        {"table": {"source_table": "profiles"}},
        {"table": {"source_table": "events"}},
        {"table": {"source_table": "lists"}},
        {"table": {"source_table": "campaigns"}},
        {"table": {"source_table": "metrics"}},
        {"table": {"source_table": "flows"}},
        {"table": {"source_table": "segments"}},
        {"table": {"source_table": "templates"}},
    ],
}

ingest(spark, pipeline_spec)
```

To override per-table options (for example, to backfill events from a specific date), pass them under the table entry — they must also be listed in `externalOptionsAllowList` on the UC connection:

```python
{
    "table": {
        "source_table": "events",
        "table_options": {
            "start_timestamp": "2024-01-01T00:00:00Z",
            "window_seconds": "43200",  # 12-hour windows for finer parallelism
        },
    },
},
```

### Step 3: Run the pipeline

The first run does a full backfill across all tables. `metrics` is a snapshot table and reloads fully on every run. CDC and append tables resume from the last committed cursor on subsequent runs. For `campaigns`, the saved cursor is a per-channel bag — each channel resumes independently.

## Authentication notes

- **Header shape.** The connector sends `Authorization: Klaviyo-API-Key <pk_…>` on every request. The literal `Klaviyo-API-Key` prefix is mandatory — `Bearer` will return 401.
- **Revision pin.** Every request also carries `revision: <api_revision>` (default `2024-10-15`). Without an explicit pin, Klaviyo would return the latest revision, and any future shape change would surface as a schema mismatch in the destination tables.
- **Scopes.** The eight read scopes listed under **Prerequisites** are the minimum — narrower scopes will produce 403 on the affected endpoint.
- **OAuth.** Klaviyo also supports OAuth for tech partners. The connector accepts only private API keys (`pk_…`) — no interactive OAuth flow.

## Rate limits

Klaviyo uses a fixed-window algorithm with **burst (per-second)** and **steady (per-minute)** windows. Limits are per-account, per-endpoint:

| Table | Tier | Burst | Steady |
|-------|------|-------|--------|
| `events` | XL | 350/s | 3,500/m |
| `profiles` | L | 75/s | 750/m |
| `lists` | L | 75/s | 750/m |
| `segments` | L | 75/s | 750/m |
| `templates` | L | 75/s | 750/m |
| `campaigns` | M | 10/s | 150/m |
| `metrics` | M | 10/s | 150/m |
| **`flows`** | **S** | **3/s** | **60/m** |

`flows` is **the strictest endpoint by a wide margin** — the connector paces requests on this table aggressively and runs it single-driver. If you sync `flows` alongside other tables, expect it to be the long pole. Klaviyo's stricter limits also apply when using `include=` or `additional-fields[profile]=predictive_analytics` — the connector avoids both.

When Klaviyo returns 429, the connector backs off using the `Retry-After` header value with exponential growth and jitter.

## Partitioned reads

Two tables opt in to partitioned streaming so the Spark engine can spread reads across executors:

- **`events`** — split by time window. The connector uses `greater-or-equal(datetime,start) AND less-than(datetime,end)` to read each window independently. Window size is controlled by `window_seconds` (default 1 day).
- **`campaigns`** — split by channel. Because Klaviyo *requires* a `messages.channel` filter on every request, the two accepted channel values on revision `2024-10-15` (`email`, `sms`) form a natural two-way partition; each executor walks one channel's cursor pages. `mobile_push` is documented by Klaviyo as a campaign channel but rejected by the filter on this revision — see `CAMPAIGN_CHANNELS` in `klaviyo_schemas.py`.

Every other table falls back to a single-driver read. Their per-second budgets are too low to benefit from parallelism (`lists` / `segments` cap at 10 records per page; `metrics` is a snapshot), or the endpoint is the strict-tier `flows`.

## Troubleshooting

### `401 Unauthorized`

**Cause:** invalid or missing key, wrong header format, or the key was deleted in the Klaviyo UI.

**Fix:** verify the key still exists in **Settings → API keys**. Confirm it begins with `pk_` (public keys are not accepted). Test with curl:
```
curl -H "Authorization: Klaviyo-API-Key pk_…" \
     -H "revision: 2024-10-15" \
     https://a.klaviyo.com/api/metrics/
```

### `403 Forbidden` on a specific table

**Cause:** the API key is missing the read scope for that resource.

**Fix:** edit the key in **Settings → API keys** and grant the missing scope (e.g. `flows:read`). Klaviyo applies scope changes immediately — no key reissue needed.

### `429 Too Many Requests` (especially on `flows`)

**Cause:** you've hit the burst (per-second) or steady (per-minute) window for that endpoint. `flows` is the most likely culprit at 3/s, 60/m.

**Fix:** the connector retries with `Retry-After` automatically. If retries are exhausted, run `flows` on its own schedule (less frequent than the high-volume `events` / `profiles` cadence), or shrink the set of tables ingested in a single pipeline run.

### `campaigns` table has fewer records than expected

**Cause:** the channel cursor bag tracks each channel independently. If one channel hasn't drained yet (e.g. due to `max_records_per_batch`), the others may also pause to keep the per-channel watermarks correct.

**Fix:** rerun the pipeline — subsequent microbatches will resume each channel from its own saved cursor. The bag converges across batches.

### `events` looks like it's missing very recent records

**Cause:** the connector caps reads at the connector init timestamp so `Trigger.AvailableNow` runs terminate predictably. Events that arrived after the run started will be picked up on the next run.

**Fix:** expected behavior. If your account has aggressively back-dated events, increase `lookback_seconds` (default 3600) on the `events` table.

### Schema validation errors after a long time without updates

**Cause:** Klaviyo released a new API revision and the default response shape changed.

**Fix:** confirm `api_revision` is set on the UC connection (it should be — `2024-10-15` is the default). If you want to adopt a newer revision, update it intentionally and verify the schemas in `klaviyo_schemas.py` still match the response.

## Limitations

- **`campaigns` cursor is a per-channel bag, not a single timestamp.** On revision `2024-10-15` the saved offset has the shape `{cap, updated_at__email, updated_at__sms}` — one slot per channel accepted by the `messages.channel` filter. Each channel advances independently. Downstream consumers shouldn't try to interpret the cursor as a wall-clock checkpoint. If `CAMPAIGN_CHANNELS` is widened in a future revision (e.g. to add `mobile_push`), an extra `updated_at__<channel>` slot will appear in the bag.
- **`metrics` is snapshot-only.** Klaviyo's metrics endpoint exposes no timestamp filter, so every sync re-fetches all metric definitions. The table is small (typically < 200 records) so this is cheap, but it means you cannot do incremental ingestion of metrics.
- **`lists` and `segments` server filter is `greater-than`-only.** The connector applies a 1-second lookback on the cursor to avoid missing records whose `updated` equals the previous watermark. With heavy concurrent writes you may see the same record returned twice across microbatches; the framework's CDC merge dedupes by primary key.
- **No write-back.** Connector is read-only.
- **No `include` / compound documents.** Klaviyo's `include=…` parameter would embed related records (e.g. `profile` on `events`) but triggers stricter rate limits and complicates schema. The connector fetches related tables separately and exposes foreign keys (`profile_id`, `metric_id` on `events`) so you can join downstream.
- **`predictive_analytics` not requested.** `additional-fields[profile]=predictive_analytics` would expose Klaviyo's per-profile predictive metrics but drops `profiles` to 10/s burst, 150/m steady. Not worth the cost for general ingestion.
- **Connector init timestamp caps every read.** Records whose cursor value is past `_init_ts` are deferred to the next microbatch. This is the mechanism that lets `Trigger.AvailableNow` converge on a busy account.
- **Reporting endpoints not included.** `campaign_values_reports`, `flow_series_reports`, `coupons`, `catalogs`, and `forms` use different API paradigms (POST-based query bodies) and are deferred to a future batch.

## References

- [Klaviyo API Overview](https://developers.klaviyo.com/en/reference/api_overview)
- [Filter DSL](https://developers.klaviyo.com/en/docs/filtering)
- [Rate Limits & Error Handling](https://developers.klaviyo.com/en/docs/rate_limits_and_error_handling)
- [API Versioning & Deprecation Policy](https://developers.klaviyo.com/en/docs/api_versioning_and_deprecation_policy)
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Klaviyo JSON:API (`https://a.klaviyo.com/api/`)
- **Supported Objects**: 8 tables (`profiles`, `events`, `lists`, `campaigns`, `metrics`, `flows`, `segments`, `templates`)
- **API Revision**: `2024-10-15` (default, configurable via `api_revision`)
- **Authentication**: Private API key (`pk_…`) sent as `Authorization: Klaviyo-API-Key <key>`
- **Supported Ingestion Types**: `cdc`, `append`, `snapshot`
- **Partitioned Reads**: `events` (time window), `campaigns` (channel fan-out)
