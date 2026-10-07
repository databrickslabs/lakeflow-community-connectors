# Lakeflow Energy Quantified Community Connector

Ingest European power, gas, coal, carbon, and weather market data from the **Energy Quantified REST API** into Databricks Delta tables via Lakeflow Connect. Energy Quantified is an Oslo-based data provider whose model is **curve-centric**: every data point belongs to a named curve such as `DE Wind Power Production GWh H Actual` or `Futures Coal API-2 USD/t ICE OHLC`. The connector exposes six tables that map directly onto Energy Quantified's curve types — a catalog table plus five curve-scoped tables (timeseries, forecast instances, period intervals, OHLC market data, and SRMC). All five curve-scoped tables require a `curve_name` table option; four of them support partitioned reads so Spark can split a date range across executors.

## Prerequisites

- **Energy Quantified account**: free / freemium or paying. Trial accounts are capped to the last 30 days of history; full historical depth requires a paid subscription.
- **API key**: obtained from your Energy Quantified profile page (see "Setup" below).
- **Network access**: the environment running the connector must be able to reach `https://app.energyquantified.com/api`.
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_key` | string (secret) | yes | Energy Quantified API key, sent as the `X-API-Key` header on every request. | `aaaa-bbbb-cccc-dddd-…` |

### Obtaining the API key

1. Sign in to [https://app.energyquantified.com/](https://app.energyquantified.com/).
2. Open your profile at [https://app.energyquantified.com/profile](https://app.energyquantified.com/profile).
3. Locate the **API key** section and copy the key (format: `aaaa-bbbb-cccc-dddd-…`, longer than the example).
4. Store the key securely — the connector treats it as a secret and injects it into the `X-API-Key` request header.

No OAuth flow is involved; the connector authenticates every request by attaching the raw key as a custom header.

> **Realto subscribers:** Realto distributes Energy Quantified data through a different endpoint (`https://api.realto.io/energyquantified-{country}`) and a different header (`Ocp-Apim-Subscription-Key`). This connector targets the standard `app.energyquantified.com` endpoint only.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide the required `api_key` connection parameter.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Energy Quantified connector exposes **6 tables** that map onto Energy Quantified's curve-type taxonomy:

| Table | Description | Primary key | Cursor | Ingestion type | Required per-table options |
|-------|-------------|-------------|--------|----------------|----------------------------|
| `curves` | Catalog of all available curves across power, gas, coal, weather, carbon, and other commodities. Use this to discover what data your subscription covers. | `name` | — | `snapshot` | none |
| `timeseries` | Continuous point-in-time values for `TIMESERIES` and `SCENARIO_TIMESERIES` curves — production, consumption, prices, weather, etc. One row per (curve, timestamp). | `(curve_name, datetime)` | `datetime` | `append` | `curve_name` |
| `instances` | Forecast instances for `INSTANCE` curves. Each row is one (issue, delivery) tuple — useful for forecast accuracy analysis where multiple issues coexist for the same delivery. | `(curve_name, issued, tag, datetime)` | `issued` | `append` | `curve_name` |
| `periods` | Open-interval data for `PERIOD` curves — installed capacity, plant outages (REMIT), reservoir levels, maintenance windows. One row per interval. | `(curve_name, begin)` | `begin` | `cdc` | `curve_name` |
| `ohlc` | Open/High/Low/Close market data for tradable energy contracts (power, gas, coal futures) on `OHLC` curves. One row per trading day per contract. | `(curve_name, traded_at, period, front, delivery)` | `traded_at` | `append` | `curve_name` |
| `srmc` | Short-Run Marginal Cost derived from coal / gas OHLC curves. Daily series in EUR/MWh, parameterised by efficiency, carbon factors, and a specific contract. | `(curve_name, date, period, front)` | `date` | `append` | `curve_name`, `period`, and either `front` or `delivery` |

### Partitioned reads

`timeseries`, `periods`, `ohlc`, and `srmc` are range-query tables and run through the partitioned-stream path: the connector splits the requested date range into `partition_days`-sized windows (default 30 days) and reads them in parallel across Spark executors. `curves` (paginated catalog walk) and `instances` (cursor walks backward through issue dates) stay on the single-driver path.

### Schema highlights

- **Curve scope on every row** — every non-`curves` table carries a flat `curve_name` column plus an embedded `curve` struct with the curve's type, area, frequency, timezone, unit, denominator, source, and commodity. The flat `curve_name` exists because the framework cannot use dotted-path primary keys.
- **Datetimes as ISO 8601 strings** — the connector requests UTC at the wire (`timezone=UTC`) and stores datetimes as strings. Cast to `TIMESTAMP` downstream per your timezone policy. The original curve timezone is preserved in the `timezone` column.
- **Scenarios preserved** — `timeseries` rows for `SCENARIO_TIMESERIES` curves keep the 40 weather-year ensemble values in a nullable `scenarios` array column.
- **Nullable `value`** — Energy Quantified represents missing data points as `{"d": "...", "v": null}` rather than omitting the row. The connector preserves these gaps; filter `WHERE value IS NOT NULL` downstream if needed.
- **OHLC product fields hoisted** — `traded_at`, `period`, `front`, `delivery` are top-level columns (not nested under a `product` struct) so they can participate in the composite primary key.
- **SRMC audit trail** — `srmc` rows carry both flat `efficiency` / `carbon_emissions` columns and a full `options` struct (efficiency, carbon emissions, conversion factors, carbon tax area) reflecting the exact factors Energy Quantified used for the calculation.

## Table Options

All curve-scoped tables (`timeseries`, `instances`, `periods`, `ohlc`, `srmc`) require `curve_name`. The other options below are forwarded verbatim to the Energy Quantified API for the table(s) they apply to. Set them under `table_configuration` in your `pipeline_spec` per table.

### Curve scope (all non-`curves` tables)

| Option | Applies to | Description |
|--------|------------|-------------|
| `curve_name` | `timeseries`, `instances`, `periods`, `ohlc`, `srmc` | Exact curve identifier from the `curves` catalog (e.g. `DE Wind Power Production GWh H Actual`). URL-encoded into the request path by the connector. |

### Time range and timezone (range-query tables)

| Option | Applies to | Default | Description |
|--------|------------|---------|-------------|
| `start_date` | `timeseries`, `periods`, `ohlc`, `srmc` | `now() - 30d` | Inclusive start date (ISO 8601 `YYYY-MM-DD` or datetime). |
| `end_date` | `timeseries`, `periods`, `ohlc`, `srmc` | `now()` | Exclusive end date. |
| `timezone` | `timeseries`, `instances`, `periods` | `UTC` | Timezone for returned datetimes (`UTC`, `CET`, `Europe/Gas_Day`, …). |
| `partition_days` | `timeseries`, `periods`, `ohlc`, `srmc` | `30` | Per-partition window size for parallel reads. Tune to balance executor parallelism against request count. |
| `lookback_days` | `periods` | `7` | CDC re-scan window. The connector widens the read backward by this many days each microbatch to catch revisions to recently opened intervals (notably outage revisions). |
| `max_records_per_batch` | all curve-scoped tables | unlimited | Admission-control cap on records per microbatch. The connector clamps and resumes at the last completed window. |

### Catalog filters (`curves` only)

These are forwarded as `GET /metadata/curves/` query parameters. Repeatable filters (e.g. `category`) accept the hyphenated form Energy Quantified expects.

| Option | Description |
|--------|-------------|
| `q` | Free-text search (e.g. `wind power germany actual`). |
| `area` | Area code (e.g. `DE`, `FR`, `NO1`, `GB`). |
| `curve_type` | `TIMESERIES`, `SCENARIO_TIMESERIES`, `INSTANCE`, `INSTANCE_PERIOD`, `PERIOD`, `OHLC`. |
| `data_type` | `ACTUAL`, `FORECAST`, `NORMAL`, `SYNTHETIC`. |
| `category` | Category tag (e.g. `Wind`, `Power`, `Production`). |
| `exact_category` | Exact category string match. |
| `commodity` | `Power`, `Gas`, `Coal`, `Oil`, `Carbon`, etc. |
| `source` | Data provider (`ENTSO-E`, `EEX`, `EPEX`, TSO names, …). |
| `frequency` | Resolution (`H`, `D`, `M`, `15min`, …). |
| `only_subscribed` | `true` to return only curves in your subscription. |
| `has_place` | `true` to return only curves linked to a physical place / plant. |
| `page_size` | Page size for catalog walk; default `500`. |

### Timeseries shaping (`timeseries`)

| Option | Description |
|--------|-------------|
| `frequency` | Aggregate to a lower resolution: `H`, `P1D`, `P1M`, `P1Y`, …. |
| `aggregation` | Aggregation method when `frequency` is set: `AVERAGE` (default), `MAX`, `MIN`. |
| `hour_filter` | Hour-bucket filter (`BASE`, `PEAK`) — requires `frequency`. |
| `threshold` | Max missing values allowed per aggregation frame. |
| `threshold_pct` | Max missing percentage per aggregation frame (mutually exclusive with `threshold`). |
| `unit` | Convert output to this unit (`GWh`, `MWh`, `MW`, …). |

### Instance filters (`instances`)

| Option | Description |
|--------|-------------|
| `tags` | Filter by instance tag (e.g. `ec` for ECMWF). |
| `exclude_tags` | Exclude specific tags. |
| `frequency` | Aggregation frequency override. |
| `limit` | Instances per request; capped at `25` (Energy Quantified's hard limit). |

### Contract specifiers (`ohlc`, `srmc`)

| Option | Applies to | Description |
|--------|------------|-------------|
| `period` | `ohlc` (optional), `srmc` (required) | Contract period: `year`, `quarter`, `month`, `week`, `day`. |
| `front` | `ohlc` (optional), `srmc` (required unless `delivery`) | Front contract number (1 = front, 2 = next, …). |
| `delivery` | `ohlc` (optional), `srmc` (required unless `front`) | Specific delivery date. Mutually exclusive with `front` on `srmc`. |
| `unit` | `ohlc`, `srmc`, `periods` | Output unit conversion. |

### SRMC calculation factors (`srmc` only)

| Option | Description |
|--------|-------------|
| `fill` | Gap-fill mode: `fill-holes` or `forward-fill`. |
| `efficiency` | Plant efficiency factor (defaults to the curve's typical value if omitted). |
| `carbon_emissions` | kg CO₂ per kWh fuel (default: `0.34056` coal, `0.202` gas). |
| `api2_tonne_to_mwh` | Coal energy conversion factor. |
| `gas_therm_to_mwh` | Gas energy conversion factor. |
| `carbon_tax_area` | Area for regional carbon tax (e.g. `DE`). |

## How to Run

### Step 1: Reference the connector in your workspace

Use the Lakeflow Community Connector UI to copy or reference the Energy Quantified connector source in your workspace.

### Step 2: Configure your pipeline

In your `ingest.py` (or equivalent), point at the Unity Catalog connection and list the tables to ingest. The example below shows the two patterns side by side: a parameterless `curves` catalog walk and a parameterised `timeseries` fetch for a specific curve.

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
register(spark, "energy_quantified")

pipeline_spec = {
    "connection_name": "my_eq_connection",
    "objects": [
        # Parameterless: full curve catalog, filtered to your subscription.
        {
            "table": {
                "source_table": "curves",
                "table_configuration": {
                    "only_subscribed": "true",
                    "commodity": "Power",
                },
            }
        },
        # Parameterised: one curve, partitioned reads across a date range.
        {
            "table": {
                "source_table": "timeseries",
                "table_configuration": {
                    "curve_name": "DE Wind Power Production GWh H Actual",
                    "start_date": "2024-01-01",
                    "end_date": "2024-04-01",
                    "timezone": "UTC",
                    "partition_days": "30",
                },
            }
        },
    ],
}

ingest(spark, pipeline_spec)
```

To ingest multiple curves into the same destination table, register one `objects` entry per curve and either route them to distinct destination tables or let the framework merge them on the composite primary key (`curve_name` is always part of the PK).

### Step 3: Run the pipeline

The first run does a full backfill within the configured `start_date` / `end_date` window. Subsequent runs advance each table's cursor and pick up only newly-published data — except `periods`, which re-scans the trailing `lookback_days` window to catch revisions.

## Curve Naming Convention

Curve names are human-readable strings such as `DE Wind Power Production GWh H Actual` or `DE Power Base M+1 EUR/MWh EEX OHLC`. The convention encodes area, commodity, attribute, unit, frequency, and data type into one identifier; see the **Data Model Overview** and **Object List** sections of the [API reference](./energy_quantified_api_doc.md) for the full taxonomy and per-dimension value lists.

Energy Quantified embeds the curve name directly in the request path (not as a query parameter), so the connector URL-encodes it before issuing a request. One quirk worth flagging: the `+` character in contract names like `M+1` must be encoded as `%2B` (not `+`, which would decode to a space in a URL path). The connector handles this automatically — just pass the literal curve name (`DE Power Base M+1 EUR/MWh EEX OHLC`) as `curve_name`.

## Rate Limits and Fair Use

Energy Quantified enforces a per-account "fair use" quota whose precise threshold is not publicly documented. The official Python SDK paces requests at a minimum interval of `0.0667 s` between calls (roughly 15 req/s); this connector replicates the same client-side pacing via a `RateLimiter`, so a single Spark driver cannot out-shoot the per-account quota.

Per-executor pacing is independent — each partition constructs its own client, so highly parallel partitioned reads can collectively exceed 15 req/s. If you observe `429 Too Many Requests` errors, reduce `partition_days` (which reduces the number of windows) or limit Spark parallelism.

The connector retries on connection errors, timeouts, and HTTP 429 / 5xx responses with exponential backoff (up to 5 attempts).

## Limitations

- **`curve_name` required on every non-catalog table** — Energy Quantified's API is curve-scoped (one curve per request), so every read of `timeseries`, `instances`, `periods`, `ohlc`, or `srmc` requires `curve_name` in `table_configuration`. To ingest many curves, register multiple `objects` entries.
- **`instances` walks backward, max 25 per request** — Energy Quantified's instances list is reverse-chronological and capped at 25 items per call (10 with ensembles). The connector walks backward via `issued-at-latest` and stays on the single-driver path; very deep historical backfills of forecast instances can be slow.
- **Subscription gating** — many curves require a paid Energy Quantified subscription. Use the `only_subscribed=true` catalog filter to see exactly what your account can access, and expect `403` errors on attempted reads of unsubscribed curves.
- **Trial accounts capped at 30 days** — historical depth on trial accounts is limited; configure `start_date` accordingly.
- **`Europe/Gas_Day` is synthetic** — Energy Quantified's gas-day timezone is not in the standard tz database. The connector preserves it as a string; do not feed it into `pytz` / `zoneinfo`. Request `timezone=UTC` for downstream-friendly timestamps.
- **No write-back** — connector is read-only.
- **No support for `INSTANCE_PERIOD` curves** — `period_instances` (REMIT outage messages) and ensemble forecasts are deferred to a future release.
- **No Realto endpoint support** — only the standard `app.energyquantified.com` endpoint is wired.

## References

- [Energy Quantified](https://www.energyquantified.com/) — provider home page
- [Energy Quantified profile page](https://app.energyquantified.com/profile) — where to obtain your API key
- [Energy Quantified Python SDK docs](https://energyquantified-python.readthedocs.io/en/latest/) — endpoint reference and parameter semantics
- [Connector API reference](./energy_quantified_api_doc.md) — full endpoint, schema, and quirk notes used to build this connector
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Energy Quantified REST API (`https://app.energyquantified.com/api`)
- **Supported Objects**: 6 tables (`curves`, `timeseries`, `instances`, `periods`, `ohlc`, `srmc`)
- **Authentication**: API key via `X-API-Key` header
- **Supported Ingestion Types**: `snapshot`, `append`, `cdc`
- **Partitioned reads**: yes, for `timeseries`, `periods`, `ohlc`, `srmc` (date-range windows)
