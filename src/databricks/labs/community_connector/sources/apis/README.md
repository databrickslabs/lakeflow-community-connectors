# Lakeflow Apis Community Connector

This documentation provides setup instructions and reference information for the **Apis** source connector, which ingests industrial/IoT historian data from the **Apis REST Service** into Databricks.

The Apis REST Service is the HTTP front end of a "Hive" — an industrial data historian / SCADA-style runtime (the API in this connector matches Prediktor's *Apis Foundation* Hive platform). A Hive **instance** hosts pluggable **modules**, each module exposes **items** (points/tags), and each item has a **current value** and — when the item is configured for logging — **recorded history**.

The connector reads three tables from a Hive instance: the item catalog (`items`), current values (`values`), and historical/aggregated values (`timeseries`).

> **Validation status**: this connector has not yet been through a full `/validate-connector` record-mode pass. A manual spot-check against a live Apis deployment (2026-09-22) confirmed the `items`/`values`/`timeseries` field names, the `module`/`item_type` derivations, and the `Authorization` header form — but the full record-mode test suite has not run. See [Known Limitations](#known-limitations) before using it in production.

## Prerequisites

- **A reachable Apis REST Service endpoint**: the root URL of the service in front of your Hive, for example `http://hive-host:8080` or `https://hive-host:443`. Apis is a self-hosted / on-premises product, so this is a customer-specific host and port — there is no vendor-hosted cloud endpoint.
- **A bearer token for the service**: the API requires an `Authorization` header on *every* request. The token is issued by your Apis administrator out of band; the API itself defines no login, token, or refresh endpoint, so the token is a static, pre-issued credential.
- **A running Hive instance**: the instance must be running for module/item/value endpoints to return data. If you do not know your instance name, the connector can discover it, but you must at least be authorized to list instances.
- **Items configured for logging** (only for the `timeseries` table): historical values exist only for items that the Hive has been configured to log into its time-series store. An item appearing in `items` does not guarantee it has history.
- **Network access**: the Databricks compute running the pipeline must be able to reach the Apis host and port. Because Apis is typically deployed on a private/plant network, this usually requires network connectivity (for example, a serverless network policy, VPC peering, or a private link) to the historian's network.
- **Lakeflow / Databricks environment**: a workspace where you can create a Unity Catalog connection for a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when creating the Unity Catalog connection.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `base_url` | string | yes | Root URL of the Apis REST Service (scheme, host, and port; no trailing path). Accepted aliases: `host`, `url`. | `http://hive-host:8080` |
| `token` | string | yes | Pre-issued bearer token sent in the `Authorization` header. Accepted aliases: `api_token`, `access_token`, `bearer_token`. | `eyJhbGciOi...` |
| `auth_scheme` | string | no | Prefix used for the `Authorization` header value. Defaults to `Bearer`, which sends `Authorization: Bearer <token>`. Set it to an empty string (or `none` / `raw`) to send the bare token with no prefix. | `Bearer` |
| `instance` | string | no | Comma-separated Hive instance name(s) to read from. When omitted, the connector discovers the available instance names from the service. Accepted alias: `instances`. Can be overridden per table. | `ApisHive` |
| `request_timeout` | string | no | Per-request timeout in seconds. Defaults to `30`. | `60` |
| `max_retries` | string | no | Attempts per request before failing. Defaults to `4`, with exponential backoff on transient failures (`429`, `500`, `502`, `503`, `504`). | `6` |
| `verify_ssl` | string | no | Set to `false` to skip TLS certificate verification. Defaults to `true`. On-premises Hive deployments often present self-signed certificates; prefer installing the certificate over disabling verification. | `false` |
| `externalOptionsAllowList` | string | **yes** | Comma-separated list of table-specific option names allowed to pass through to the connector. This connector relies on table-specific options, so this parameter is required. | see the definitive list below |

The full, definitive value for `externalOptionsAllowList` is:

```
instance,instances,item,items,item_patterns,attrib,attribs,quality,aggregate,interval,start_timestamp,backfill_days,window_seconds,max_partitions,max_records_per_batch,max_windows_per_read,items_per_request
```

> **Note**: the options above (`item`, `quality`, `aggregate`, `window_seconds`, and so on) are **not** connection parameters. They are supplied per table under `table_configuration` in the pipeline spec, and their names must appear in `externalOptionsAllowList` for the connection to forward them.

### Obtaining the Required Parameters

This connector uses a simple bearer token — there is no OAuth app to register, no redirect URI, no scopes, and no browser sign-in.

1. **Find the service root URL (`base_url`)**
   - Ask your Apis / plant IT administrator for the host and port where the Apis REST Service is published, for example `http://hive-host:8080`.
   - Do **not** use `http://localhost:8080`; that value appears in the API specification only as a development placeholder.
   - Confirm the URL resolves and responds from a machine on the same network before configuring the pipeline.

2. **Obtain the bearer token (`token`)**
   - Request a token from your Apis administrator. The Apis REST Service specification defines no token-issuing endpoint, so the token must be provisioned for you by the platform.
   - Store it as the `token` connection option; it is treated as a secret and never logged.
   - You can verify the token and the expected header form with a direct call:

     ```bash
     curl -i -X GET "http://hive-host:8080/hive?format=json" \
       -H "Authorization: Bearer <token>"
     ```

   - A live spot-check (2026-09-22) confirmed a Prediktor Apis Hive instance accepts **both** `Authorization: Bearer <token>` and the bare token with no prefix — the default `auth_scheme` of `Bearer` should work out of the box. If your deployment differs and this call returns `401`/`403`, retry without the `Bearer ` prefix and set the `auth_scheme` connection option to an empty string if that succeeds instead.

3. **Identify the Hive instance name (`instance`, optional but recommended)**
   - The response to `GET /hive` (as in the `curl` above) lists the instance names, for example `ApisHive`.
   - Pinning `instance` explicitly avoids the discovery call on every pipeline run and makes the ingestion deterministic. You may list several instances, comma-separated.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** flow from the **Add Data** page.
2. Select an existing Lakeflow Community Connector connection for this source, or create a new one and supply `base_url` and `token` (plus any of the optional parameters above).
3. Set `externalOptionsAllowList` to:
   `instance,instances,item,items,item_patterns,attrib,attribs,quality,aggregate,interval,start_timestamp,backfill_days,window_seconds,max_partitions,max_records_per_batch,max_windows_per_read,items_per_request`

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes a **static list** of three tables (exact casing as shown):

- `items`
- `values`
- `timeseries`

Other endpoints of the Apis REST Service are intentionally not exposed as tables: the instance list is an internal lookup used to resolve the Hive instance name, and the instance runstate/module listings are health and configuration calls rather than row-producing datasets. Writing values back to the historian is also out of scope — this connector is read-only.

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|
| `items` | Catalog of items (points/tags) in a Hive instance, optionally enriched with requested attributes | `snapshot` | `instance`, `item_name` | n/a (fully re-listed each run) |
| `values` | Current (real-time) value of each matching item | `cdc` (upsert-only) | `instance`, `item_name` | `timestamp` |
| `timeseries` | Historical — raw or server-aggregated — values per item over a time window | `append` | n/a (append-only) | `timestamp` |

**Incremental behavior**

- `items` is a full re-list on every run. The source offers no change cursor and no created/modified field for the catalog, so the table is treated as a slowly-changing dimension refreshed in full.
- `values` is an upsert stream keyed on `(instance, item_name)`. The connector passes the committed cursor to the source as an "updated since" filter, so each run only requests values that changed since the previous run. Because the source returns one row per item, re-reading a boundary row is harmless — it merges onto the same key.
- `timeseries` is append-only over contiguous, non-overlapping half-open `[start, end)` windows. The cursor advances to the window boundary, not to the last observed record, so empty windows still make progress.
- **No table supports delete synchronization.** The Apis REST Service provides no delete feed and no tombstone field for any object, so no table can be configured for CDC with deletes. A retired item simply stops appearing in subsequent snapshots; detecting removals requires diffing successive `items` snapshots downstream.

### Columns that require attention

- **`instance`** — added by the connector from the Hive instance being read; it is not part of any response body. It is part of the primary key of `items` and `values` because item names are unique only *within* an instance.
- **`item_name`** — the fully-qualified item name, conventionally `<Module>.<Item>` (for example `Worker.Signal1`). Confirmed live: the source's own field is actually named `name` (`values`/`timeseries` use `item` instead) rather than the OpenAPI spec's declared `item_name` — the connector tolerates all of these on read, so this is transparent to you.
- **`module`** (`items` only) — derived by the connector by splitting `item_name` at the first `.`. Confirmed live: this reproduces the real `modules` endpoint's module names exactly, including for deeply nested item paths.
- **`item_type`** (`items` only) — confirmed live: the source's own `type` field (observed values: `Signal`, `Function item`, `Status`). Renamed from the wire field `type` to `item_type` to avoid ambiguity with SQL/Python's own use of that word.
- **`value` / `quality` / `timestamp`** (`values`, `timeseries`) — the source's declared `value: {v, q, t}` triple, flattened onto three top-level columns instead of kept as a nested struct, so downstream SQL can reference each field directly:
  - `value` — the reading itself, stored as a **string**. The source places no type constraint on it; the underlying point may be numeric, boolean, or textual, and only the historian knows which. A numeric point arrives as, for example, `"42.7"`. Cast it downstream where the point's real type is known.
  - `quality` — quality indicator. Confirmed live as an open string, not an enum: a real reading came back `"Good"` (capitalized), while the *filter* parameter only accepts lowercase `good`/`uncertain`/`bad`. Don't assume only those three values appear.
  - `timestamp` — timestamp of the reading, kept as a string. **Confirmed live it is NOT ISO 8601**: an observed value was `"2026-09-22 20:38:02.545"` — space-separated (no `T`), millisecond precision, no timezone. It also doubles as the incremental cursor for both tables; that's safe because the connector's own offsets are always self-generated ISO-8601 strings, never parsed from this field.
- **`attributes`** (`items` only) — an array of `{attrib_id, value}` pairs, populated only when you request attribute IDs via the `attrib` table option. Both sides are open strings: the source documents no registry mapping attribute IDs to meanings, so you must obtain the ID meanings from your Apis administrator. Not yet confirmed live (the spot-check didn't request any `attrib` IDs).
- **`timeseries` has no declared primary key** because append-only tables are inserted rather than merged. Its *logical* uniqueness key is `(instance, item_name, timestamp)`; use that if you deduplicate downstream.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. Applies to `items` and `values`, not `timeseries`. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

#### Applies to all three tables

| Option | Required | Default | Description |
|---|---|---|---|
| `instance` / `instances` | No | connection value, else discovered | Comma-separated Hive instance name(s) for this table, overriding the connection-level setting. |
| `item` / `items` / `item_patterns` | No | `*` | Comma-separated item name patterns, with `*` as the wildcard — for example `Work*.Sig*,Logger.*`. Each pattern is read independently and in parallel. Narrowing patterns is the primary way to keep responses from being truncated (see [Known Limitations](#known-limitations)). **Confirmed live**: the source only supports wildcards on `items`; for `values`/`timeseries`, the connector transparently resolves any wildcard pattern to real item names via `items` first (a literal, non-wildcard name is used as-is, at no extra request cost). |
| `max_partitions` | No | `64` | Upper bound on parallel reads per micro-batch. When a request needs more time windows than this budget allows, windows are widened rather than producing thousands of tiny tasks. |
| `max_records_per_batch` | No | `1000` | Caps records per batch on the sequential (non-parallel) read path. Ignored for `items`, which is always a full snapshot. |

#### `items` only

| Option | Required | Default | Description |
|---|---|---|---|
| `attrib` / `attribs` | No | none | Comma-separated numeric attribute IDs to request for each item. Returned in the `attributes` array column. When omitted, no attributes are requested. |

#### `values` and `timeseries`

| Option | Required | Default | Description |
|---|---|---|---|
| `quality` | No | none (no filter) | Minimum quality filter. One of `good`, `uncertain`, `bad`. An invalid value fails fast with a clear error instead of a bare HTTP 400. |
| `items_per_request` | No | `50` | How many exact item names are batched into one request via repeated `item=` params. Confirmed live that batching multiple exact names into one request works; the real upper limit (URL length, server-side cap) is unconfirmed, so this default is a conservative placeholder — raise it once tested at scale on your deployment. |

#### `timeseries` only

| Option | Required | Default | Description |
|---|---|---|---|
| `start_timestamp` | No | derived from `backfill_days` | Absolute ISO 8601 lower bound for the first run, for example `2024-01-01T00:00:00Z`. **Strongly recommended** — it is the only way to bound the initial backfill precisely. Relative "OPC time" expressions such as `DAY-1D` are not accepted here: the connector always sends absolute instants so incremental reads stay deterministic and replayable. |
| `backfill_days` | No | `1095` (3 years) | How far back the first run reaches when neither a committed cursor nor `start_timestamp` is available. The generous default assumes a historian backfill is the point of the first run; lower it for a quick first run. |
| `window_seconds` | No | `86400` (1 day) | Width of one time window. Windows are contiguous and half-open, so a reading cannot land in two windows. Start small (for example `3600`) when validating a new configuration. |
| `aggregate` | No | none (raw recorded values) | Server-side aggregation function applied per window bucket. Validated locally against the source's full function list, which includes `average`, `timeaverage`, `interpolative`, `minimum`, `maximum`, `total`, `sum`, `count`, `median`, `delta`, `range`, `variance`, `start`, `end`, `duration_good`, `duration_bad`, `percent_good`, `percent_bad`, `worst_quality`, `movingaveragebytime`, `movingaveragebycount`, `lowpassfilter`, and the regression/vector/UA variants. |
| `interval` | No | none (source default `3600`) | Resample bucket size in seconds, between `1` and `86400`. Only meaningful together with `aggregate`; the source does not state whether it is ignored when `aggregate` is absent. |
| `max_windows_per_read` | No | `64` | Maximum number of windows walked per batch on the sequential read path, so it advances in strides comparable to the parallel path. |

## Data Type Mapping

| Apis concept | Source type | Databricks type | Notes |
|---|---|---|---|
| `item_name` | string (source field observed as `name` on `items`, `item` on `values`/`timeseries` — not the OpenAPI spec's declared `item_name`) | `string` | Fully-qualified item name, conventionally `<Module>.<Item>`. |
| `item_type` (`items` only) | string, confirmed live (observed: `Signal`, `Function item`, `Status`) | `string` | The source's own field is named `type`; renamed to `item_type` to avoid ambiguity. |
| `value` (the reading) | untyped — no constraint declared by the source | `string` | Deliberately stringified so a numeric, boolean, or textual point all ingest losslessly instead of failing the batch. Cast downstream (for example `CAST(value AS DOUBLE)`) once the point's real type is known. |
| `quality` (quality) | string, confirmed live as mixed-case (e.g. `"Good"`) | `string` | Open string, not an enum. Raw quality codes beyond `good` / `uncertain` / `bad` (and beyond lowercase) are preserved. |
| `timestamp` (timestamp) | string, confirmed live as `yyyy-MM-dd HH:mm:ss.SSS` (e.g. `"2026-09-22 20:38:02.545"`) — **not** ISO 8601 | `string` | Kept as a string since the source declares no fixed format. Cast with `TO_TIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS')` downstream, not the ISO-8601 form. |
| Attribute ID / value (`items`) | number (request) / undeclared (response) | `array<struct<attrib_id: string, value: string>>` | Both sides kept as open strings because the source publishes no attribute-ID registry. |
| Hive instance name | path segment (not a response field) | `string` | Stamped onto every row by the connector for scoping. |
| Request timestamps (`updatedSince`, `starttime`, `endtime`) | space-separated, no timezone suffix (e.g. `2026-09-23 00:05:00`) — confirmed live; **not** ISO 8601 despite the spec's own example | n/a (request-only) | You (via `start_timestamp`) still supply ISO 8601; the connector converts internally before placing it on the wire. See [Known Limitations](#known-limitations). |
| Response `format` | `json` or `csv` | n/a (request-only) | The connector always requests `json`; the source defines no CSV schema. |

In short: the declared `value` triple is flattened onto `value`/`quality`/`timestamp` columns, item attributes are preserved as an array rather than flattened, every scalar reading is carried as a string to survive an untyped source, and an absent point surfaces as `null` columns rather than a missing row.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which guides you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (for example `ingest.py`).
2. Reference the Unity Catalog connection created above, and add one `table` entry per table you want to ingest, with its options under `table_configuration`.

```json
{
  "pipeline_spec": {
    "connection_name": "apis_connection",
    "object": [
      {
        "table": {
          "source_table": "items",
          "table_configuration": {
            "instance": "ApisHive",
            "item": "Worker.*,Logger.*",
            "attrib": "1,2"
          }
        }
      },
      {
        "table": {
          "source_table": "values",
          "table_configuration": {
            "instance": "ApisHive",
            "item": "Worker.*",
            "quality": "good"
          }
        }
      },
      {
        "table": {
          "source_table": "timeseries",
          "table_configuration": {
            "instance": "ApisHive",
            "item": "Worker.Signal1,Worker.Signal2",
            "start_timestamp": "2024-01-01T00:00:00Z",
            "window_seconds": "3600",
            "aggregate": "average",
            "interval": "600",
            "quality": "good"
          }
        }
      }
    ]
  }
}
```

Notes on the example:

- `connection_name` must point to the Unity Catalog connection holding your `base_url` and `token`.
- `source_table` must be one of `items`, `values`, `timeseries`.
- Omit `aggregate` and `interval` from the `timeseries` entry to ingest raw recorded values instead of resampled aggregates.
- You can ingest the same source table more than once with different destination tables and different item patterns — for example one destination table per plant area.

3. (Optional) Customize the source connector code if you need behavior beyond these options.

### Step 3: Run and Schedule the Pipeline

On the **first run** of `timeseries`, either set `start_timestamp` to the earliest point you actually need or lower `backfill_days`; otherwise the run reaches back three years. Subsequent runs resume from the committed window boundary. For `values`, the first run reads all matching items and later runs request only values that changed since the previous run.

#### Best Practices

- **Start small.** Validate with a single instance, an explicit short item list, and a small `window_seconds` (for example `3600`) before widening to wildcards and full history.
- **Always pin `instance`.** Discovery works, but pinning the instance name removes a request per run and makes failures easier to diagnose.
- **Keep item patterns narrow.** Because the source has no pagination, a narrow `item` pattern is the main defense against truncated responses — and it also increases read parallelism, since each pattern is read independently.
- **Set `start_timestamp` for `timeseries`.** It is the difference between a bounded first run and a three-year backfill.
- **Use `aggregate` + `interval` to reduce volume** when you do not need every recorded point — resampling at the source is far cheaper than ingesting raw history and aggregating afterward.
- **Schedule to match the data's real cadence.** `values` only reports current values, so polling far faster than the historian updates adds load without adding rows. `items` changes rarely — a daily or weekly refresh is usually plenty.
- **Treat rate limits as unknown.** The source documents none, so the connector retries conservatively with exponential backoff. If you see gateway-level throttling, lower `max_partitions` to reduce concurrent requests.
- **Cast types downstream.** Build a view that casts `value` to its real type and `timestamp` with `TO_TIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS')` (confirmed live format, not ISO 8601) once, and consume that view instead of the raw table.

#### Troubleshooting

**Common Issues:**

- **`401` / `403` on every request** — verify the token is correct and not expired. A live spot-check confirmed a Prediktor Apis Hive instance accepts both the `Bearer <token>` and bare-token forms, so this is more likely a bad/expired token than a header-format mismatch; if a direct `curl` does succeed without the `Bearer ` prefix on your deployment, set the `auth_scheme` connection option to an empty string.
- **`404` on an instance-scoped call** — the Hive instance name is wrong or the instance is not running. List instances with `GET /hive` and set `instance` explicitly.
- **TLS / certificate errors** — on-premises Hive deployments frequently use self-signed certificates. Prefer installing the certificate in the runtime's trust store; as a last resort set `verify_ssl` to `false` (not recommended for production).
- **Connection timeouts** — confirm the compute running the pipeline has network reachability to the historian's host and port, then raise `request_timeout` if the historian is simply slow to answer wide queries.
- **A warning about truncated (partial) results** — the source truncated the response and offers no way to resume it. Narrow the `item` pattern (for example per-module wildcards) or shorten `window_seconds`, then re-run. See [Known Limitations](#known-limitations).
- **`timeseries` returns no rows for an item that exists in `items`** — history exists only for items the Hive has been configured to log. Confirm with your Apis administrator that the item is logged, and that the requested window overlaps its recorded range.
- **An error about an invalid ISO 8601 timestamp** — `start_timestamp` must be an absolute instant such as `2024-01-01T00:00:00Z`. Relative "OPC time" expressions like `DAY-1D` are accepted by the source but rejected by the connector, which requires deterministic, replayable bounds.
- **An error about an unsupported `aggregate` or `quality` value** — these are validated before the request is sent; the error message lists the accepted values.
- **Numeric comparisons behave like string comparisons** — `value` is a string by design. Cast it (`CAST(value AS DOUBLE)`) in your downstream transformations.
- **`timeseries` rows look duplicated at window boundaries** — the connector treats windows as half-open `[start, end)`. If your deployment treats the end of a window as inclusive, boundary readings can appear twice; deduplicate on `(instance, item_name, timestamp)` and report the behavior so the windowing can be adjusted.

## Known Limitations

These are limitations of the Apis REST Service itself or of the current, partially-live-validated state of this connector — not configuration mistakes. A manual spot-check against a live Prediktor Apis Hive instance (2026-09-22) confirmed several items below that were previously pure inference; a full `/validate-connector` record-mode pass has not yet run.

1. **No pagination exists in the source API — confirmed live, not just theoretical.** No endpoint defines a `limit`, `offset`, `cursor`, page-token parameter, or `Link` header — and a live, unfiltered `item=*` call against an instance with roughly 100 items already came back `206 Partial Content`. The server **will** truncate with no documented way to detect where it stopped or to resume. The connector therefore issues exactly one request per (instance, item pattern, time window) combination and never invents a continuation parameter. Truncation is mitigated the only way the source allows: **narrow your `item` patterns and shorten `window_seconds`.** A truncated response is logged as a warning rather than silently accepted, so watch pipeline logs for it — if it appears, your table may be incomplete for that run.
2. **The `items` and `timeseries` response schemas are now largely confirmed.** The source's specification declares a response body for the current-values endpoint only, but a live spot-check filled in the rest:
   - `items` — **confirmed**: the identifying field, `module` derivation, and the `item_type` column (source field `type`; observed values `Signal`, `Function item`, `Status`) all matched real responses. The `attributes` column remains unconfirmed (the spot-check didn't request any `attrib` IDs).
   - `values` — **confirmed**: the `{v, q, t}` triple, flattened onto `value`/`quality`/`timestamp`, matches a real reading exactly. Quality is genuinely open and mixed-case (`"Good"`, not `"good"`).
   - `timeseries` — **confirmed**: both the outer per-item bundle shape (`{item, values: [...]}`) and the per-point `{v, q, t}` triple inside it match a real, populated response. Whether `endtime` is inclusive or exclusive is still unconfirmed (the confirmed sample used a 10-second window with data throughout, so the boundary case wasn't exercised).
   - The connector's parsers still deliberately accept several plausible response shapes for these tables rather than hard-failing on an unexpected one, since not every Apis deployment need behave identically to the one spot-checked.
3. **The request timestamp format was wrong — found and fixed via live testing.** The connector originally sent `updatedSince`/`starttime`/`endtime` as ISO-8601 (`2026-09-23T00:05:00Z`), matching the spec's own documented example. Live testing showed the server rejects this for any 2026-dated value with a raw `500 Internal Server Error` / `"Invalid time string: ..."` — regardless of whether the requested time was hours in the past or seconds in the future, ruling out a past/future validation rule. The actual required format, confirmed empirically, is space-separated with **no** timezone suffix (e.g. `2026-09-23 00:05:00`) — matching the exact wire shape the source uses for its own response `value.t` field. **This has been fixed in the connector**: you still supply `start_timestamp` as ISO-8601 in your table configuration; the connector converts it internally before it ever reaches the wire. The response `timestamp` column format is a separate, related fact: also space-separated with no timezone, but with millisecond precision (e.g. `"2026-09-22 20:38:02.545"`) — cast it with `TO_TIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS')`, not the ISO-8601 form.
4. **`values`/`timeseries` reject wildcard item patterns — found and fixed via live testing.** The spec documents wildcard support (`Work*.Sig*`) for `item` on every endpoint, but live testing showed `values` and `timeseries` reject wildcards outright: a bare `*` fails with `"module * not found"`, and even a real module prefix like `ApisOT.*` fails with `"item ApisOT.* not found"`. Only `items` genuinely supports wildcards. **This has been fixed in the connector**: any wildcard `item` pattern given to `values`/`timeseries` is now transparently resolved to real, exact item names via one `items` lookup first, then batched into groups of `items_per_request` exact names per request (confirmed live that batching multiple exact names into a single request works, via the spec's own repeated-`item=`-params style — see `items_per_request` above). The real upper limit on batch size is unconfirmed; the default of `50` is a conservative placeholder pending further live testing at scale.
5. **The `attributes` column and the instance-discovery response shape (`hive_instances`) beyond the top-level `name` field remain unconfirmed.** The `Authorization` header form, by contrast, **is confirmed**: a live instance accepted both `Bearer <token>` and the bare token.
6. **No delete tracking for any object.** The source exposes no delete endpoint and no tombstone field, so no table supports CDC with deletes. Removals can only be inferred by diffing successive `items` snapshots.
7. **No documented rate limits.** The source declares no throttling responses, headers, or guidance. The connector retries conservatively with exponential backoff on throttling and gateway errors, but safe request rates must be established empirically for your deployment.
8. **Attribute IDs have no published meaning.** The `attrib` option takes numeric IDs and the `attributes` column returns them verbatim. There is no ID-to-name registry in the API; obtain the mapping from your Apis administrator.
9. **`interval`'s interaction with `aggregate` is unspecified.** The source does not state whether `interval` is ignored when `aggregate` is omitted. Set them together.
10. **`items` cannot be read incrementally.** The catalog has no change cursor and no modified timestamp, so it is fully re-listed each run. The item pattern narrows scope but is not a cursor.
11. **`values` cursors on wall-clock progress, not on observed record timestamps.** The endpoint returns one current value per item with no ordering guarantee and no upper time bound, so no reliable record-derived cursor exists. Re-reading a boundary row is harmless because the table upserts on `(instance, item_name)`.
12. **No point-in-time historical lookup.** The source has no "value at this instant" endpoint. Retrieving one historical reading requires a `timeseries` request over a narrow window around the desired time.
13. **JSON only.** The source also offers CSV, but defines no CSV schema, so the connector always requests JSON.
14. **Read-only.** The source's write endpoint for pushing values into the historian is intentionally not used by this connector.
15. **Instance runstate and module listings are not ingested.** They are health and configuration lookups rather than row-producing datasets.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/apis/apis.py`
- Table schemas, defaults, and schema provenance notes: `src/databricks/labs/community_connector/sources/apis/apis_schemas.py`
- Source API research and known quirks: `src/databricks/labs/community_connector/sources/apis/apis_api_doc.md`
- Connection parameters and allowed table options: `src/databricks/labs/community_connector/sources/apis/connector_spec.yaml`
- Source OpenAPI specification (Apis REST Service 0.4.0): `src/databricks/labs/community_connector/sources/apis/apis_openapi.json`
- Vendor product documentation (Apis Foundation / Hive, for background on instances, modules, items, and logging): `https://docs.prediktor.com/`
