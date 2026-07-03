# Energy Quantified API Documentation

**Source:** Energy Quantified (https://www.energyquantified.com/)
**API Base URL:** `https://app.energyquantified.com/api`
**Auth method:** API key via `X-API-Key` request header
**Default rate limit:** ~15 req/s (enforced client-side; server-side quota per account)

---

## Authorization

**Method:** Bearer-style API key, sent as a custom HTTP header on every request.

```
X-API-Key: <your-api-key>
```

**How to obtain a key:**
1. Log into https://app.energyquantified.com/
2. Navigate to Settings → API key
3. Copy the key (format: `aaaa-bbbb-cccc-dddd-...`, longer than the example)

**No OAuth flow is involved.** The connector stores the raw key and injects it as a header at request time.

**Realto subscribers:** Use `Ocp-Apim-Subscription-Key` header and regional base URLs (`https://api.realto.io/energyquantified-{country}`). This connector targets the standard `app.energyquantified.com` endpoint only.

**Trial accounts:** Limited to 30 days of historical data.

**Example request:**
```
GET https://app.energyquantified.com/api/metadata/curves/?q=wind+power+germany&page=1&page-size=50
X-API-Key: aaaa-bbbb-cccc-dddd
```

---

## Rate Limits and Retry Semantics

**Default delay:** The official Python SDK enforces a minimum 0.0667 s between requests (≈15 req/s). The connector should replicate this behaviour.

**Server-side:** Energy Quantified uses per-account quotas. The precise quota is not publicly documented; it is described as "fair use." The SDK does **not** handle HTTP 429 specially — it retries only on connection/timeout/5xx errors (max 3 attempts, 7.5 s initial delay, 2× exponential backoff). Connector implementers should treat a 429 as a terminal error per run and surface it as a human-readable message to reconfigure the crawl cadence.

**Retry behaviour from official SDK:**
- Retry conditions: `ConnectionError`, `Timeout`, HTTP 5xx
- Max tries: 3
- Initial delay: 7.5 s
- Backoff multiplier: 2× (7.5 → 15 → 30 s)
- HTTP 429: not explicitly retried by SDK; treat as a hard rate-limit signal

---

## Data Model Overview

Energy Quantified's model is **curve-centric**. Every data point belongs to a named **curve** such as `DE Wind Power Production GWh H Actual` or `FR Spot Power EUR/MWh H Actual`. Curves are typed:

| `curve_type` | Description | Typical connector table |
|---|---|---|
| `TIMESERIES` | Continuous point-in-time values | `timeseries` |
| `SCENARIO_TIMESERIES` | Timeseries with 40 weather-year scenarios | `timeseries` (same endpoint) |
| `INSTANCE` | Forecasts re-issued on a schedule (many per curve) | `instances` |
| `INSTANCE_PERIOD` | Forecast instances expressed as open intervals | Deferred (see below) |
| `PERIOD` | Open intervals (e.g. plant capacity, outage durations) | `periods` |
| `OHLC` | Market data open/high/low/close per contract | `ohlc` |

SRMC is a **derived product** computed from OHLC coal/gas curves; it does not have its own `curve_type`.

---

## Object List

The curve catalog is the authoritative object list. It is retrievable via API and contains thousands of entries across power, gas, weather, coal, and other commodity categories.

**Catalog endpoint:** `GET /metadata/curves/` — paginated, ~50 results per page by default.

Curves are **not static** — Energy Quantified publishes new curves as new data sources are added. A connector should re-sync the catalog periodically (e.g., daily snapshot).

**Summary of key curve dimensions:**

| Dimension | Examples |
|---|---|
| Area | DE, FR, NO1, NO2, NO3, NO4, NO5, GB, DK1, DK2, SE1–SE4, FI, NL, BE, AT, CH, PL, … |
| Category | Power, Gas, Coal, Weather, Oil, Carbon, Hydro, Wind, Solar, Nuclear, Transmission, … |
| Resolution (frequency) | 15min, H (hourly), D (daily), W (weekly), M (monthly), Y (yearly) |
| Data type | Actual, Forecast, Normal, Synthetic |
| Source | ENTSO-E, EEX, EPEX, TSOs, meteorological agencies, … |

---

## Tables Documented in This File

| Table | curve_type filter | Ingestion type | Notes |
|---|---|---|---|
| `curves` | (all) | `snapshot` | Catalog of available curves |
| `timeseries` | `TIMESERIES`, `SCENARIO_TIMESERIES` | `append` | Main time-series fact data |
| `instances` | `INSTANCE` | `append` | Forecast instances (latest-first list) |
| `periods` | `PERIOD` | `append` | Open-interval data |
| `ohlc` | `OHLC` | `append` | Market OHLC records |
| `srmc` | `OHLC` (derived) | `append` | Short-run marginal cost (derived from OHLC) |

---

## Table: `curves`

### Purpose
Catalog of all available named curves. Used to discover what data is available, which curves a subscription covers, and to bootstrap the other tables (each of which requires a `curve_name`).

### Endpoint
```
GET https://app.energyquantified.com/api/metadata/curves/
```

### Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `q` | string | No | — | Free-text search (e.g. `"wind power germany actual"`) |
| `area` | string | No | — | Area code filter (e.g. `"DE"`, `"FR"`) |
| `curve-type` | string | No | — | Filter by curve type (`"TIMESERIES"`, `"INSTANCE"`, `"PERIOD"`, `"OHLC"`, etc.) |
| `data-type` | string | No | — | Filter by data type (`"ACTUAL"`, `"FORECAST"`, `"NORMAL"`) |
| `category` | string (repeatable) | No | — | Category keyword(s); repeat param for multiple |
| `exact-category` | string | No | — | Exact category string match |
| `commodity` | string | No | — | Commodity filter (`"Power"`, `"Gas"`, `"Coal"`, …) |
| `source` | string | No | — | Data source filter (`"EEX"`, `"EPEX"`, …) |
| `frequency` | string | No | — | Resolution filter (`"H"`, `"D"`, `"M"`, …) |
| `only-subscribed` | boolean | No | `false` | Return only curves in your subscription |
| `has-place` | boolean | No | — | Filter curves associated with a place (plant) |
| `page` | integer | No | `1` | Page number (1-indexed) |
| `page-size` | integer | No | `50` | Results per page |

**Note on HTTP query parameter names:** The SDK translates Python underscore params (`page_size`) to hyphen form (`page-size`) in the actual HTTP request. The REST API expects hyphens.

### Pagination
Page-based. The response envelope contains:

```json
{
  "total_items": 607,
  "total_pages": 61,
  "page": 1,
  "page_size": 10,
  "data": [ ... ]
}
```

Iterate pages until `page >= total_pages`. Use `page-size=500` to reduce round-trips (max page size is not explicitly documented; 500 has been used in practice).

### Response Fields (per curve object)

| Field | Type | Nullable | Description |
|---|---|---|---|
| `name` | string | No | Unique curve identifier (human-readable, e.g. `"DE Wind Power Production GWh H Actual"`) |
| `curve_type` | string | No | One of `TIMESERIES`, `SCENARIO_TIMESERIES`, `INSTANCE`, `INSTANCE_PERIOD`, `PERIOD`, `OHLC` |
| `data_type` | string | No | `ACTUAL`, `FORECAST`, `NORMAL`, `SYNTHETIC` |
| `area` | string | Yes | Primary geographic area code (`"DE"`, `"FR"`, …) |
| `area_sink` | string | Yes | Importing area for cross-border exchange curves |
| `area_source` | string | Yes | Exporting area for cross-border exchange curves |
| `place` | object | Yes | Associated physical plant/place (see Place schema below) |
| `frequency` | string | Yes | Native resolution (`"H"`, `"D"`, `"M"`, `"15min"`, …) |
| `timezone` | string | Yes | Native timezone of datetimes (`"CET"`, `"UTC"`, …) |
| `categories` | array[string] | No | Category tags (e.g. `["Wind", "Power", "Production"]`) |
| `unit` | string | Yes | Value unit (`"GWh"`, `"MW"`, `"EUR"`, …) |
| `denominator` | string | Yes | Denominator for ratio units (e.g. `"MWh"` for `"EUR/MWh"`) |
| `source` | string | Yes | Data provider (`"ENTSO-E"`, `"EEX"`, …) |
| `commodity` | string | Yes | High-level commodity (`"Power"`, `"Gas"`, `"Coal"`) |
| `subscription.access` | string | No | `"PAYING"`, `"FREEMIUM"`, `"NONE"` |
| `subscription.type` | string | Yes | Subscription plan type |

**Place sub-object fields (when present):**

| Field | Type | Description |
|---|---|---|
| `type` | string | Place kind (`"POWER_PLANT"`, `"OFFSHORE_WIND_FARM"`, …) |
| `key` | string | Place identifier |
| `name` | string | Human-readable name |
| `unit` | string | Capacity unit |
| `fuels` | array[string] | Fuel types |
| `areas` | array[string] | Areas the place belongs to |

### Primary Key
`name` — globally unique string identifier.

### Ingestion Type
`snapshot` — the catalog is relatively stable but new curves are added over time. Full re-sync recommended daily or weekly. There is no incremental cursor (no `updated_at` field on curves).

### Required `table_options`
None — the entire catalog is returned with no required parameters.

### Example Request
```
GET https://app.energyquantified.com/api/metadata/curves/?only-subscribed=true&page=1&page-size=100
X-API-Key: <key>
```

---

## Table: `timeseries`

### Purpose
Point-in-time values for `TIMESERIES` and `SCENARIO_TIMESERIES` curves. This is the primary fact table — energy production, consumption, prices, weather, etc. Every row represents one timestamp for one curve.

**Design note:** The API is curve-scoped — each request returns data for **one curve**. In Lakeflow the table must be parameterized by `curve_name` via `table_options`. Alternatively, a wider implementation can loop over all subscribed timeseries curves and union-append results into a single table with `curve_name` as a partition/filter column.

### Endpoint
```
GET https://app.energyquantified.com/api/timeseries/{curve_name}/
```
`{curve_name}` is URL-encoded (spaces → `%20` or `+`).

### Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `begin` | string (ISO 8601 date) | Yes | — | Start of range, inclusive, e.g. `"2024-01-01"` |
| `end` | string (ISO 8601 date) | Yes | — | End of range, exclusive |
| `timezone` | string | No | Curve native | Timezone for returned datetimes (`"UTC"`, `"CET"`, `"Europe/Gas_Day"`, …) |
| `frequency` | string | No | Curve native | Aggregate to lower resolution (`"P1D"`, `"P1M"`, …) |
| `aggregation` | string | No | `"AVERAGE"` | Aggregation method when `frequency` set: `"AVERAGE"`, `"MAX"`, `"MIN"` |
| `hour-filter` | string | No | — | Hour bucket filter: `"BASE"`, `"PEAK"` (requires `frequency`) |
| `threshold` | integer | No | `0` | Max missing values allowed per aggregation frame |
| `threshold-pct` | float | No | — | Max missing % per aggregation frame (mutually exclusive with `threshold`) |
| `unit` | string | No | Curve native | Convert output to this unit (`"GWh"`, `"MWh"`, `"MW"`, …) |

**Date semantics:** `begin` is inclusive; `end` is exclusive. Both are ISO 8601 dates (`YYYY-MM-DD`) or datetimes (`YYYY-MM-DDTHH:MM:SS`). The API is European power market-centric and defaults to CET/CEST for local-time curves. Pass `timezone=UTC` to normalise to UTC.

### Pagination
**Not paginated.** The full result for the requested date range is returned in a single response. For very large ranges the response may be several MB; limit range to manageable chunks (e.g. monthly for high-frequency curves).

### Response JSON Structure

```json
{
  "resolution": {
    "frequency": "H",
    "timezone": "CET"
  },
  "curve": { /* curve object — same schema as /metadata/curves/ */ },
  "instance": null,
  "contract": null,
  "scenario_names": null,
  "unit": "GWh",
  "denominator": null,
  "data": [
    { "d": "2024-01-01T00:00:00+01:00", "v": 12.34 },
    { "d": "2024-01-01T01:00:00+01:00", "v": 11.87 }
  ]
}
```

For `SCENARIO_TIMESERIES` curves (40 weather-year ensembles), `data` items use the `s` or `v`+`s` form:
```json
{ "d": "2024-01-01T00:00:00+01:00", "v": 12.34, "s": [10.1, 11.2, ..., 14.9] }
```

### Response Fields

| Field | Type | Description |
|---|---|---|
| `resolution.frequency` | string | Native or requested frequency |
| `resolution.timezone` | string | Timezone of `d` values |
| `curve.name` | string | Curve identifier |
| `unit` | string | Value unit |
| `denominator` | string / null | Denominator unit |
| `data[].d` | string (ISO 8601 datetime) | Timestamp |
| `data[].v` | number / null | Value at timestamp |
| `data[].s` | array[number] / absent | 40 scenario values (scenario curves only) |

### Flattened Row Schema (for connector output table)

| Column | Type | Notes |
|---|---|---|
| `curve_name` | string | From `curve.name` |
| `datetime` | timestamp (UTC) | Normalised from `data[].d` |
| `value` | double | `data[].v` |
| `unit` | string | From top-level `unit` |
| `frequency` | string | From `resolution.frequency` |
| `timezone` | string | Original timezone from response |

Scenario columns (`s[0]`…`s[39]`) should be deferred or handled as a separate sub-table; they bloat schema significantly.

### Primary Key
`(curve_name, datetime)`

### Cursor Field
`datetime` — append incrementally by advancing `begin` to the last successfully ingested timestamp + 1 period.

### Ingestion Type
`append` — values for a given timestamp do not change after publication (actuals). For forecast curves (`data_type=FORECAST`), values can be revised, so treat as `cdc` if revision tracking matters. Most production use cases treat timeseries as append-only.

### Required `table_options`
`curve_name` (string) — the exact curve name, e.g. `"DE Wind Power Production GWh H Actual"`.

### Example Request
```
GET https://app.energyquantified.com/api/timeseries/DE%20Wind%20Power%20Production%20GWh%20H%20Actual/?begin=2024-01-01&end=2024-02-01&timezone=UTC
X-API-Key: <key>
```

---

## Table: `instances`

### Purpose
Forecast instances — each row represents one time-stamped value from one forecast *issue* of a curve. For example, a day-ahead wind forecast re-issued at 10:00 every day produces one instance per day; each instance covers the next 24–48 hours. Multiple instances coexist for the same delivery period (one per issue time), enabling forecast accuracy analysis.

**Design note:** The API returns up to 25 instances per request (10 with ensembles), not a flat time-ordered sequence. For connector purposes, `instances/list/` is used first to enumerate available instances (metadata only), then individual instances are fetched or the bulk `load` endpoint is used. For incremental sync, filter by `issued_at_latest` advancing a cursor over issue date.

### Endpoints

| Purpose | URL | Method |
|---|---|---|
| List instance metadata | `GET /instances/{curve_name}/list/` | GET |
| Load instances (with data) | `GET /instances/{curve_name}/` | GET |
| Latest instance | `GET /instances/{curve_name}/get/latest/` | GET |
| Specific instance by issue date | `GET /instances/{curve_name}/get/{issued}/` | GET |
| Specific instance by issue date + tag | `GET /instances/{curve_name}/get/{issued}/{tag}/` | GET |
| Relative (stitched day-ahead series) | `GET /instances/{curve_name}/get/relative/` | GET |
| Tags available for curve | `GET /instances/{curve_name}/tags/` | GET |

### Parameters (for `/list/` and load)

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `issued_at_latest` | string (ISO 8601) | No | now | Upper bound on issue date (for pagination/cursor) |
| `issued_at_earliest` | string (ISO 8601) | No | — | Lower bound on issue date |
| `issued_time_of_day` | string (HH:MM) | No | — | Filter by time-of-day of issue |
| `tags` | string (repeatable) | No | — | Filter by instance tags |
| `exclude_tags` | string (repeatable) | No | — | Exclude specific tags |
| `limit` | integer | No | `5` (list: `20`) | Number of instances to return (max 25; 10 with ensembles) |

For `/list/` the response contains metadata only (no timeseries data). For `load`, the response contains full timeseries data per instance.

### Pagination
**Cursor-based, not offset.** Walk backward through time: set `issued_at_latest` to the oldest `issued` datetime seen in the last response. Continue until fewer than `limit` instances are returned or `issued_at_earliest` is reached.

### Response JSON Structure (`/list/`)

```json
[
  {
    "issued": "2024-01-15T10:00:00+01:00",
    "tag": "ec"
  },
  {
    "issued": "2024-01-14T10:00:00+01:00",
    "tag": "ec"
  }
]
```

### Response JSON Structure (full load — `/{curve_name}/`)

Returns an array of timeseries objects. Each is the same structure as the `/timeseries/` response, with an `instance` field populated:

```json
[
  {
    "resolution": { "frequency": "H", "timezone": "CET" },
    "curve": { "name": "DE Wind Power Production GWh H Forecast", ... },
    "instance": {
      "issued": "2024-01-15T10:00:00+01:00",
      "tag": "ec"
    },
    "unit": "GWh",
    "denominator": null,
    "data": [
      { "d": "2024-01-16T00:00:00+01:00", "v": 8.91 }
    ]
  }
]
```

### Flattened Row Schema (for connector output table)

| Column | Type | Notes |
|---|---|---|
| `curve_name` | string | From `curve.name` |
| `issued` | timestamp (UTC) | `instance.issued` — when the forecast was issued |
| `tag` | string | `instance.tag` (e.g. `"ec"` for ECMWF) |
| `datetime` | timestamp (UTC) | `data[].d` — delivery datetime |
| `value` | double | `data[].v` |
| `unit` | string | From top-level `unit` |
| `frequency` | string | From `resolution.frequency` |

### Primary Key
`(curve_name, issued, tag, datetime)`

### Cursor Field
`issued` — walk backward with `issued_at_latest` cursor. For incremental sync from a Lakeflow perspective, record the latest `issued` seen and use `issued_at_earliest=<last_seen>` on the next run.

### Ingestion Type
`append` — once a forecast is issued it does not change. New instances are added over time.

### Required `table_options`
`curve_name` (string) — must be a curve with `curve_type=INSTANCE`.

### Example Request
```
GET https://app.energyquantified.com/api/instances/DE%20Wind%20Power%20Production%20GWh%20H%20Forecast/list/?limit=20&issued_at_latest=2024-02-01T00:00:00
X-API-Key: <key>
```

---

## Table: `periods`

### Purpose
Open-interval (period-based) data where values span a variable-length range with explicit start and end datetimes. Typical use cases: installed capacity over time, plant outages (REMIT), reservoir levels, planned maintenance windows. Each row is one interval for one curve.

### Endpoint
```
GET https://app.energyquantified.com/api/periods/{curve_name}/
```

### Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `begin` | string (ISO 8601) | Yes | — | Start of query range (inclusive) |
| `end` | string (ISO 8601) | Yes | — | End of query range (exclusive) |
| `timezone` | string | No | Curve native | Timezone for returned datetimes |
| `unit` | string | No | Curve native | Unit conversion |

### Pagination
**Not paginated.** The full result for the requested date range is returned in one response.

### Response JSON Structure

```json
{
  "resolution": { "frequency": null, "timezone": "CET" },
  "curve": { "name": "DE Wind Power Installed MW Capacity", ... },
  "instance": null,
  "unit": "MW",
  "denominator": null,
  "data": [
    { "begin": "2020-01-01T00:00:00+01:00", "end": "2020-01-06T00:00:00+01:00", "v": 60645.29 },
    { "begin": "2020-01-06T00:00:00+01:00", "end": "2020-03-01T00:00:00+01:00", "v": 61200.00 }
  ]
}
```

Some curves include a `capacity` field per period (for outage data):
```json
{ "begin": "...", "end": "...", "v": 800.0, "capacity": 1200.0 }
```

### Flattened Row Schema

| Column | Type | Notes |
|---|---|---|
| `curve_name` | string | From `curve.name` |
| `begin` | timestamp (UTC) | Start of period (normalised) |
| `end` | timestamp (UTC) | End of period (normalised); null if open-ended |
| `value` | double | `data[].v` |
| `capacity` | double / null | Installed capacity (outage curves only) |
| `unit` | string | From top-level `unit` |

### Primary Key
`(curve_name, begin)` — `end` is nullable for open-ended periods (e.g. ongoing outages) so it's deliberately not part of the PK; the connector's `TABLE_METADATA["periods"]["primary_keys"]` reflects this.

### Cursor Field
`begin` — filter by `begin >= <last_seen_begin>` on next run. Because periods can be updated (e.g. outage revised), use a lookback window of at least 7 days to catch revisions to recently opened intervals.

### Ingestion Type
`cdc` — period boundaries can be revised (especially outage data). Re-fetch overlapping windows on each sync.

### Required `table_options`
`curve_name` (string) — must be a curve with `curve_type=PERIOD`.

### Example Request
```
GET https://app.energyquantified.com/api/periods/DE%20Wind%20Power%20Installed%20MW%20Capacity/?begin=2020-01-01&end=2020-06-01&timezone=UTC
X-API-Key: <key>
```

---

## Table: `ohlc`

### Purpose
Open/High/Low/Close market data for tradable energy contracts (power, gas, coal futures). Each row represents one trading day's summary for one contract (e.g. German power base-load month+1 traded on EEX). Useful for traders, quants, and anyone tracking forward curve evolution.

### Endpoints

| Purpose | URL | Method |
|---|---|---|
| Historical OHLC by date range | `GET /ohlc/{curve_name}/` | GET |
| Latest trading day | `GET /ohlc/{curve_name}/latest/` | GET |
| As timeseries (single field) | `GET /ohlc/{curve_name}/timeseries/{field}/` | GET |
| Latest as period-forward curve | `GET /ohlc/{curve_name}/latest/periods/{field}/` | GET |

For connector purposes, `GET /ohlc/{curve_name}/` is the primary endpoint.

### Parameters (historical endpoint)

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `begin` | string (ISO 8601 date) | Yes | — | First trading date (inclusive) |
| `end` | string (ISO 8601 date) | Yes | — | Last trading date (exclusive) |
| `period` | string | No | — | Contract period: `"year"`, `"quarter"`, `"month"`, `"week"`, `"day"` |
| `delivery` | string (ISO 8601 date) | No | — | Filter to specific delivery date (requires `period`) |
| `front` | integer (≥ 0) | No | — | Front contract number (requires `period`; 1=front, 2=next, …) |
| `unit` | string | No | Curve native | Unit conversion |

### Pagination
**Not paginated.** Full result for requested date range returned in one response.

### Response JSON Structure

```json
{
  "curve": { "name": "DE Power Base M+1 EUR/MWh EEX OHLC", ... },
  "unit": "EUR",
  "denominator": "MWh",
  "data": [
    {
      "product": {
        "traded_at": "2024-01-15",
        "period": "month",
        "front": 1,
        "delivery": "2024-02-01"
      },
      "open": 72.10,
      "high": 74.50,
      "low": 71.80,
      "close": 73.90,
      "settlement": 73.85,
      "volume": 1250.0,
      "open_interest": 5430.0
    }
  ]
}
```

Fields `open`, `high`, `low`, `close`, `volume`, `open_interest` may be `null` for illiquid contracts; `settlement` is always populated when available.

### Flattened Row Schema

| Column | Type | Notes |
|---|---|---|
| `curve_name` | string | From `curve.name` |
| `traded_at` | date | `product.traded_at` — the trading date |
| `period` | string | `product.period` — contract period type |
| `front` | integer | `product.front` — front number |
| `delivery` | date | `product.delivery` — contract delivery date |
| `open` | double / null | Opening price |
| `high` | double / null | High price |
| `low` | double / null | Low price |
| `close` | double / null | Closing price |
| `settlement` | double / null | Settlement price |
| `volume` | double / null | Volume traded |
| `open_interest` | double / null | Open interest |
| `unit` | string | Price unit (e.g. `"EUR"`) |
| `denominator` | string | Price denominator (e.g. `"MWh"`) |

### Primary Key
`(curve_name, traded_at, period, front, delivery)` — uniquely identifies one contract's trading day record. `front` is part of the PK because a single trading day can carry rows for several front-contract positions on the same `(period, delivery)`.

### Cursor Field
`traded_at` — append by advancing `begin` to last seen `traded_at` + 1 day.

### Ingestion Type
`append` — once a trading day closes, its OHLC record is final. Settlement prices are not revised.

### Required `table_options`
`curve_name` (string) — must be a curve with `curve_type=OHLC`.

### Example Request
```
GET https://app.energyquantified.com/api/ohlc/DE%20Power%20Base%20M%2B1%20EUR%2FMWh%20EEX%20OHLC/?begin=2024-01-01&end=2024-02-01&period=month
X-API-Key: <key>
```

---

## Table: `srmc`

### Purpose
Short-Run Marginal Cost (SRMC) of thermal power plants, calculated from coal and gas forward curves. SRMC represents the variable cost of generating one additional MWh of electricity. This data is used for merit-order and spread analysis.

**Design note:** SRMC is a **derived product** — it is calculated by the Energy Quantified API from underlying coal/gas OHLC curves (not stored as raw time-series). The inputs are the coal or gas forward curve plus fixed factors for efficiency, carbon emissions, and carbon price. The underlying curve must be an `OHLC` curve of commodity type `Gas` or `Coal`.

### Endpoints

| Purpose | URL | Method |
|---|---|---|
| Historical SRMC (OHLC format) | `GET /srmc/{curve_name}/` | GET |
| SRMC as timeseries | `GET /srmc/{curve_name}/timeseries/` | GET |
| Latest SRMC | `GET /srmc/{curve_name}/latest/` | GET |
| Latest SRMC as period-series | `GET /srmc/{curve_name}/latest/periods/` | GET |

For connector purposes, `GET /srmc/{curve_name}/timeseries/` is most convenient as it returns a flat daily series.

### Parameters (timeseries endpoint)

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `begin` | string (ISO 8601 date) | Yes | — | Start date (inclusive) |
| `end` | string (ISO 8601 date) | Yes | — | End date (exclusive) |
| `period` | string | Yes | — | Contract period: `"month"`, `"quarter"`, `"year"` |
| `front` | integer (≥ 1) | No* | — | Front contract number (*required unless `delivery` provided) |
| `delivery` | string (ISO 8601 date) | No* | — | Specific delivery date (*mutually exclusive with `front`) |
| `fill` | string | No | — | Gap fill: `"fill-holes"`, `"forward-fill"` |
| `unit` | string | No | `"EUR/MWh"` | Output unit |
| `efficiency` | float | No | Default per curve | Fuel efficiency factor |
| `carbon_emissions` | float | No | `0.34056` (coal) / `0.202` (gas) | kg CO₂ per kWh fuel |
| `api2_tonne_to_mwh` | float | No | Default per curve | Coal energy conversion factor |
| `gas_therm_to_mwh` | float | No | Default per curve | Gas energy conversion factor |
| `carbon_tax_area` | string | No | — | Area for regional carbon tax (e.g. `"DE"`) |

### Pagination
**Not paginated.** Full result in one response.

### Response JSON Structure (timeseries endpoint)

```json
{
  "resolution": { "frequency": "P1D", "timezone": "CET" },
  "curve": { "name": "Futures Coal API-2 USD/t ICE OHLC", ... },
  "contract": {
    "period": "month",
    "front": 1
  },
  "options": {
    "efficiency": 0.36,
    "carbon_emissions": 0.34056,
    "api2_tonne_to_mwh": 6.978,
    "gas_therm_to_mwh": null,
    "carbon_tax_area": null
  },
  "unit": "EUR",
  "denominator": "MWh",
  "data": [
    { "d": "2024-01-15", "v": 45.23 },
    { "d": "2024-01-16", "v": 44.87 }
  ]
}
```

The `options` block reflects the factors used in the SRMC calculation (useful for auditability).

### Flattened Row Schema

| Column | Type | Notes |
|---|---|---|
| `curve_name` | string | From `curve.name` (underlying coal/gas curve) |
| `date` | date | `data[].d` |
| `value` | double | SRMC in EUR/MWh |
| `unit` | string | `"EUR"` |
| `denominator` | string | `"MWh"` |
| `period` | string | Contract period type |
| `front` | integer / null | Front number |
| `delivery` | date / null | Specific delivery date |
| `efficiency` | double | From `options.efficiency` |
| `carbon_emissions` | double | From `options.carbon_emissions` |

### Primary Key
`(curve_name, date, period, front)` — `delivery` is deliberately excluded. The SRMC API's `contract` block only includes whichever of `front` or `delivery` the caller queried with; the other is always null. Including a perpetually-null column in the PK would cause DLT's `append_flow` to drop every row as null-PK invalid (see commit `537b4ff` and the explanatory comment in `energy_quantified_schemas.py:251-260`). The `(curve_name, date, period, front)` tuple is already unique per request. Callers who only have `delivery` should set `table_configuration.delivery` so it flows into the record's `delivery` column (still useful as a non-PK column for filtering downstream).

### Cursor Field
`date` — append by advancing `begin`.

### Ingestion Type
`append` — SRMC values are computed from settled OHLC data; once a trading day closes, the value is stable.

### Required `table_options`
- `curve_name` (string) — underlying coal/gas OHLC curve name
- `period` (string) — contract period (`"month"`, `"quarter"`, `"year"`)
- `front` or `delivery` — at least one required to specify the contract

### Example Request
```
GET https://app.energyquantified.com/api/srmc/Futures%20Coal%20API-2%20USD%2Ft%20ICE%20OHLC/timeseries/?begin=2024-01-01&end=2024-02-01&period=month&front=1
X-API-Key: <key>
```

---

## Time Zone Conventions

Energy Quantified is an Oslo-based provider serving the European power market. Time zone handling is a first-class concern:

| Timezone identifier | Meaning | Used for |
|---|---|---|
| `CET` / `CEST` | Central European (Summer) Time | Most continental European power curves |
| `UTC` | Universal Coordinated Time | Neutral normalisation, recommended for connectors |
| `WET` | Western European Time (Portugal/UK) | GB, IE, PT curves |
| `EET` | Eastern European Time | Eastern EU curves |
| `Europe/Istanbul` | Turkey | TR curves |
| `Europe/Moscow` | Russia | RU curves |
| `Europe/Gas_Day` | Synthetic gas-day timezone | Gas curves (day starts 06:00 CET) |

**Recommendation for connectors:** Always pass `timezone=UTC` to normalise all datetimes to UTC before storing. Store the original curve timezone as metadata.

**`begin`/`end` semantics:** `begin` is **inclusive**, `end` is **exclusive** across all time-based endpoints. Both accept date strings (`YYYY-MM-DD`) or datetimes with optional timezone offset.

---

## Field Type Mapping

| API type | Python type | Connector / Spark type | Notes |
|---|---|---|---|
| ISO 8601 datetime string | `datetime` | `TimestampType` | Normalise to UTC |
| ISO 8601 date string | `date` | `DateType` | |
| `"H"`, `"P1D"`, `"P1M"` (frequency) | `str` | `StringType` | ISO 8601 duration notation |
| Numeric value | `float` (nullable) | `DoubleType` | May be null for missing data points |
| `curve_type`, `data_type` | `str` (enum) | `StringType` | See enums below |
| `categories` | `list[str]` | `ArrayType(StringType)` | |
| `subscription.access` | `str` | `StringType` | `"PAYING"`, `"FREEMIUM"`, `"NONE"` |
| `scenario values` | `list[float]` | `ArrayType(DoubleType)` | 40 elements for scenario curves |
| `period.begin` / `period.end` | `datetime` | `TimestampType` | `end` may be null for open intervals |

**CurveType enum values:**
`TIMESERIES`, `SCENARIO_TIMESERIES`, `INSTANCE`, `INSTANCE_PERIOD`, `PERIOD`, `OHLC`

**DataType enum values:**
`ACTUAL`, `FORECAST`, `NORMAL`, `SYNTHETIC`

**ContractPeriod tag values:**
`year`, `mdec`, `season`, `quarter`, `month`, `week`, `weekend`, `day`

---

## Deferred Tables

The following objects were identified but are not documented in detail in this batch due to complexity or narrow usefulness as standalone connector tables. They are recommended for a future batch if needed.

| Object | Reason deferred |
|---|---|
| `period_instances` (`INSTANCE_PERIOD` curves) | Combines the complexity of both instances (cursor over issue dates) and periods (open intervals). REMIT outage messages are the primary use case; these require understanding REMIT message IDs as tags. Recommend separate research pass targeting REMIT specifically. |
| `places` (`/metadata/places/`) | Catalog of physical plants with hierarchical parent/child relationships. Useful as a dimension table for plant-level analysis. Low urgency — most connectors join on curve name, not place key. Pagination identical to `/metadata/curves/`. |
| `categories` (`/metadata/categories/`) | Static reference list of category strings. Non-paginated, single response. Trivially simple but of minimal standalone value. |
| `instance_ensembles` (`/ensembles/{curve}/`) | Ensemble forecasts (40 weather-year scenarios per instance). Same endpoint pattern as instances but with `ensembles=True`; max 10 per request. Schema explosion risk — 40 scenario columns per row. Recommend a separate wide-format table if needed. |
| `absolute` (`/instances/{curve}/get/absolute/`) | Cross-sectional view of forecast accuracy for a single delivery point across multiple issue dates. Returns `AbsoluteResult` / `AbsoluteItem` — not a natural time-ordered series. Best suited for ad-hoc analytics, not periodic ingestion. |

---

## Known Quirks

1. **Curve name as URL path segment:** The curve name (e.g. `"DE Power Base M+1 EUR/MWh EEX OHLC"`) is URL-encoded and placed directly in the path, not as a query parameter. Special characters like `+`, `/`, spaces all require encoding. The `+` in contract names (`M+1`) must be encoded as `%2B`, not `+`, since `+` decodes to space in URL paths.

2. **HTTP query parameter hyphenation:** The Python SDK uses underscores (`page_size`, `hour_filter`) but the REST API uses hyphens (`page-size`, `hour-filter`). Always use hyphens in raw HTTP requests.

3. **No explicit 429 handling in SDK:** The official Python SDK retries on 5xx and connection errors but does not specifically handle HTTP 429. A connector implementation should add explicit 429 handling with `Retry-After` header respect.

4. **Trial accounts:** Limited to 30 days of historical data. Full historical depth requires a paying subscription.

5. **Instance list direction:** `/instances/{curve}/list/` returns instances in reverse chronological order (newest first). Pagination by cursor (`issued_at_latest`) walks backward in time.

6. **SRMC requires input factors:** When calling SRMC endpoints without overriding factors, the API applies curve-specific defaults. The `options` block in the response documents the exact factors used, which is important for auditability.

7. **Timezone `Europe/Gas_Day`:** This is a synthetic timezone not in the standard tz database. It represents a gas-market day starting at 06:00 CET. Do not attempt to look it up in `pytz` or `zoneinfo`; handle it as a special case or request UTC output instead.

8. **Null values in data arrays:** A missing data point is represented as `{ "d": "...", "v": null }` rather than omission of the record. Connectors should expect and handle null values.

9. **`denominator` field:** For price-per-unit curves (EUR/MWh), `unit="EUR"` and `denominator="MWh"`. This field is null for plain-unit curves (GWh, MW). Connectors should store both fields for completeness.

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official SDK README (GitHub) | https://github.com/energyquantified/eq-python-client | 2026-05-11 | High | Overview of data types, authentication pattern, Python client design |
| Official SDK docs (ReadTheDocs) | https://energyquantified-python.readthedocs.io/en/latest/ | 2026-05-11 | High | All API methods, parameters, return types |
| Auth docs | https://energyquantified-python.readthedocs.io/en/latest/userguide/auth.html | 2026-05-11 | High | API key location, key format, file-based auth alternative |
| Metadata userguide | https://energyquantified-python.readthedocs.io/en/latest/userguide/metadata.html | 2026-05-11 | High | `/metadata/curves/` params, pagination shape (total_items, total_pages, page, page_size), Curve object fields |
| Timeseries userguide | https://energyquantified-python.readthedocs.io/en/latest/userguide/timeseries.html | 2026-05-11 | High | Timeseries endpoint params, response structure, field names, timezone list |
| Instances userguide | https://energyquantified-python.readthedocs.io/en/latest/userguide/instances.html | 2026-05-11 | High | All instance endpoints, instance object fields, list/load semantics |
| OHLC userguide | https://energyquantified-python.readthedocs.io/en/latest/userguide/ohlc.html | 2026-05-11 | High | OHLC endpoints, Product/OHLC object fields, fill parameter |
| Periods userguide | https://energyquantified-python.readthedocs.io/en/latest/userguide/periods.html | 2026-05-11 | High | Periods endpoint, Periodseries/Period object fields, capacity variant |
| SRMC userguide | https://energyquantified-python.readthedocs.io/en/latest/userguide/srmc.html | 2026-05-11 | High | SRMC methods, required factors, EUR/MWh output |
| Period instances userguide | https://energyquantified-python.readthedocs.io/en/latest/userguide/period-instances.html | 2026-05-11 | High | REMIT context, `INSTANCE_PERIOD` curve type, relative/latest/list methods |
| Full reference (ReadTheDocs) | https://energyquantified-python.readthedocs.io/en/latest/reference/reference.html | 2026-05-11 | High | Base URL (`https://app.energyquantified.com/api`), rate limit defaults (0.0667 s delay = 15 req/s), all API class method signatures |
| SDK source: base.py | https://raw.githubusercontent.com/energyquantified/eq-python-client/master/energyquantified/base.py | 2026-05-11 | Highest | `X-API-Key` header name confirmed; base URL `https://app.energyquantified.com/api` confirmed; timeout 20 s; delay 0.0667 s |
| SDK source: timeseries.py | https://raw.githubusercontent.com/energyquantified/eq-python-client/master/energyquantified/api/timeseries.py | 2026-05-11 | Highest | Exact URL pattern `/timeseries/{safe_curve}/`, HTTP param names (hyphen form), supported curve types |
| SDK source: metadata.py | https://github.com/energyquantified/eq-python-client/blob/master/energyquantified/api/metadata.py | 2026-05-11 | Highest | All `/metadata/*` endpoints, exact query param names (`page-size`, `curve-type`, `data-type`) |
| SDK source: instances.py | https://github.com/energyquantified/eq-python-client/blob/master/energyquantified/api/instances.py | 2026-05-11 | Highest | All `/instances/*` URL patterns, list endpoint, tags endpoint |
| SDK source: ohlc.py | https://github.com/energyquantified/eq-python-client/blob/master/energyquantified/api/ohlc.py | 2026-05-11 | Highest | `/ohlc/{curve}/` endpoint, all sub-endpoints, OHLCField enum |
| SDK source: periods.py | https://github.com/energyquantified/eq-python-client/blob/master/energyquantified/api/periods.py | 2026-05-11 | Highest | `/periods/{curve}/` endpoint URL, period-only curve type restriction |
| SDK source: srmc.py | https://github.com/energyquantified/eq-python-client/blob/master/energyquantified/api/srmc.py | 2026-05-11 | Highest | `/srmc/{curve}/` endpoints, calculation factor params |
| SDK source: retry.py | https://raw.githubusercontent.com/energyquantified/eq-python-client/master/energyquantified/http/retry.py | 2026-05-11 | Highest | Retry config: max_tries=3, initial_delay=7.5 s, backoff=2×; no 429 handling |
| SDK source: rate_limiter.py | https://raw.githubusercontent.com/energyquantified/eq-python-client/master/energyquantified/http/rate_limiter.py | 2026-05-11 | Highest | RateLimiter: proactive delay, no reactive 429 handling |
| SDK source: metadata/curve.py | https://github.com/energyquantified/eq-python-client/blob/master/energyquantified/metadata/curve.py | 2026-05-11 | Highest | Complete Curve class field list with types and docstrings |
| SDK source: metadata/ohlc.py | https://github.com/energyquantified/eq-python-client/blob/master/energyquantified/metadata/ohlc.py | 2026-05-11 | Highest | OHLCField enum, ContractPeriod enum, ContinuousContract, SpecificContract dataclasses |
| SDK source: data/ohlc.py | https://github.com/energyquantified/eq-python-client/blob/master/energyquantified/data/ohlc.py | 2026-05-11 | Highest | OHLC dataclass fields, Product dataclass fields (`traded_at`, `period`, `front`, `delivery`) |
| SDK source: parser/timeseries.py | https://raw.githubusercontent.com/energyquantified/eq-python-client/master/energyquantified/parser/timeseries.py | 2026-05-11 | Highest | Raw JSON field names: `d` (datetime), `v` (value), `s` (scenarios), top-level envelope fields |
| SDK source: parser/periodseries.py | https://raw.githubusercontent.com/energyquantified/eq-python-client/master/energyquantified/parser/periodseries.py | 2026-05-11 | Highest | Period JSON fields: `begin`, `end`, `v`, optional `capacity` |
| SDK source: parser/ohlc.py | https://raw.githubusercontent.com/energyquantified/eq-python-client/master/energyquantified/parser/ohlc.py | 2026-05-11 | Highest | OHLC JSON structure: `product.traded_at`, `product.period`, `product.front`, `product.delivery`, price fields |
