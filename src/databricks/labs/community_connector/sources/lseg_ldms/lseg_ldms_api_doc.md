# **LSEG LDMS (RDMS) REST API Documentation**

LSEG Data Management System (LDMS), branded **RDMS** in its Swagger UI. A single
generic, curve/time-series REST API that serves oil, gas, power, **freight**,
refinery, OPIS rack, options and tabular datasets. There is **no separate "oil"
vs "freight" API** — they are the same API surface exposed on different hosts
with different permissioning:

| Deployment | Host (`base_url`) |
|---|---|
| Oil | `https://oilprod1.rdms.refinitiv.com` |
| Freight | `https://freightprod1.rdms.refinitiv.com` |

One connector implementation therefore serves both; it is deployed as **two
Unity Catalog connections** (one per host), each with its own `api_key`. This
mirrors the GitHub connector's `base_url` override for GitHub Enterprise.

Source: `LDMS REST API Interface Guide v25.0.0.pdf` (Issue 25.0.0, 31 Mar 2025).

## **Authorization**

- **Method:** static API key sent on every request. No OAuth, no session/token
  exchange for REST. (A separate WebSockets push interface has a ticket step;
  not used by this connector.)
- **Header:** `Authorization: <API Key>`, where `<API Key>` is the **entire
  string including the `apikey-v1 ` prefix**, e.g.
  `Authorization: apikey-v1 66kCwNWtbJvdBbcSJvjTzVm4wiKELIiyx58axrC6M71`.
- **Optional header:** `Accept: application/json` (default) or `text/csv` on
  most endpoints.
- **Base URL:** `https://<host>/api/v1/<endpoint>/...` — version segment is `v1`.
- **Health / key check:** `GET /api/v1/KeyStatus` returns (as TEXT) the number
  of hours until the key expires. Use as a lightweight auth/health probe.
- **Permissioning:** all data permissions are tied to the key (DACS). A curve
  the key cannot see returns an `Error` status rather than data.
- **HTTP codes:** 200 ok; 204 no data for dates; 400 bad request (often bad
  date format); **401 not authorised**; 404 unknown curve; 500 server error;
  **403 rate-limit exceeded**.

Example:
```
GET https://oilprod1.rdms.refinitiv.com/api/v1/KeyStatus
Authorization: apikey-v1 <key>
```

## **Object List**

LDMS is **curve-keyed, not table-keyed**: data lives in hundreds of thousands of
individual curves, discovered by metadata search, not a fixed table list. We
therefore expose a small set of **logical tables**; *which* curves/datasets a
table returns is driven by per-table options (`table_options`). The list is
**static** (`list_tables()` returns it directly).

| Logical table | What it is | Selected via (table_options) |
|---|---|---|
| `curve_values` | Time-series / forecast values, one row per observation | `curve_ids` (comma list) **or** `metadata_query`; `scenario_id` (default 0); `start_date`; `result_timezone` (default UTC) |
| `curve_metadata` | Curve catalog (metadata tags) matching a query | `metadata_query`; `max_results` |
| `tabular_data` | Provider tabular datasets (JODI, OPIS, IIR flows/fixtures, corrections) | `data_type` (required, e.g. `JODI`); `fields`; `filter`; `order_by` |

Discovery endpoints backing these:
- Curves: `GET /api/v1/Metadata/Search?query=<q>&MaxResults=&SkipRows=`
  (supports `+`/`-` weighting and `Tag.is=Value`, e.g. `Geography.is=Belgium`).
  Resolve alias→id via `GET /api/v1/Metadata/Alias/CurveID/{alias}`.
- Tabular data types the key can see: `GET /api/v1/TabularData/DataTypes`.

## **Object Schema**

Schemas are effectively **static per logical table** (the value endpoints return
a fixed row shape); tabular-data columns are discoverable per data type.

`curve_values` row (from `CurveValues*` responses; keys added by the connector
marked †):
| Field | Type | Notes |
|---|---|---|
| `curve_id` † | string | route/echoed CurveID |
| `scenario_id` † | integer | 0 for standard PointConnect data |
| `forecast_date` | timestamp | `2000-01-01T00:00:00` for actuals/time-series |
| `value_date` | timestamp | the observation date (UTC) |
| `value` | double | the value |
| `last_update_time` | timestamp | from forecast metadata (TLL); used for `cdc` |

`curve_metadata` row: `curve_id` (string) + one column per metadata tag
(`GET /api/v1/Metadata/TagTypes` enumerates tag columns; values via
`GET /api/v1/Metadata/{CurveID}`).

`tabular_data`: fields discoverable via
`GET /api/v1/TabularData/DataFields/{DataType}` and valid values via
`GET /api/v1/TabularData/DataFieldValues/{DataType}/{FieldName}`. **Always pin an
explicit `fields` list** — columns can be added between releases and default
column order is not guaranteed.

## **Get Object Primary Keys**

Static:
| Table | Primary keys |
|---|---|
| `curve_values` | `curve_id, scenario_id, forecast_date, value_date` |
| `curve_metadata` | `curve_id` |
| `tabular_data` | data-type dependent (declared per `data_type`; e.g. JODI: country + product + flow + period) |

Every LDMS value is uniquely keyed by **CurveID + ScenarioID + ForecastDate +
ValueDate**.

## **Object's ingestion type**

| Table | Ingestion type | Cursor field | Rationale |
|---|---|---|---|
| `curve_values` | `append` (POC) / `cdc` (production) | `value_date` (append) / `last_update_time` (cdc) | Actuals stream forward by `value_date`. Curves get **revised** (corrections, new forecasts); production should use `cdc` keyed on `last_update_time` so restatements upsert on the PK. |
| `curve_metadata` | `snapshot` | — | Catalog; full refresh. |
| `tabular_data` | `append` if the dataset has a period/date field, else `snapshot` | dataset date field | Depends on data type. |

`cdc` driver: `GET /api/v1/CurveDataModified/{CutoffDate}` returns all curves
updated since a cut-off (inclusive; accepts date and/or time) — the incremental
"what changed" query. Per-curve `last_update_time` comes from
`GET /api/v1/CurveForecastList/{CurveID}` or `POST /api/v1/CurveSummaryBatch/Values`.

## **Read API for Data Retrieval**

**Flow:** discover curves (`Metadata/Search`) → optionally discover
scenarios/forecasts/date ranges → fetch values. Prefer the **POST batch**
endpoints for volume.

Single-curve GETs:
- `GET /api/v1/CurveValues/{CurveID}` — raw values, all scenarios. Query:
  `ScenarioID`, `MinForecastDate`, `MaxForecastDate`, `MinValueDate`,
  `MaxValueDate`, `ResultTimezone`.
- `GET /api/v1/CurveValues/Forecast/{CurveID}/{ScenarioID}/{ForecastDate}` —
  a specific forecast.
- `GET /api/v1/CurveValues/LatestForecast/{CurveID}/{ScenarioID}` — most recent
  forecast (`AsAtDate` for point-in-time reproducibility).
- `GET /api/v1/CurveValues/Flatten/{CurveID}/{ScenarioID}` — latest value per
  value-date across forecasts.

Batch (preferred for ingestion) — `POST /api/v1/CurveValuesBatch`:
```json
{
  "scenarioID": 0,
  "minValueDate": "2021-11-23", "maxValueDate": "2021-11-30",
  "resultTimezone": "UTC",
  "totalMaxValues": 100000, "maxValues": 5000,
  "sortValuesDateDescending": false,
  "curveRequests": [
    {"curveID": "700350000"},
    {"curveID": "700350002", "minValueDate": "2021-01-01"}
  ]
}
```
Top-level fields set defaults for every curve; `curveRequests[]` overrides per
curve (only `curveID` required). Also `POST /api/v1/CurveValuesBatch/LatestForecast`.

Per-result **Status**: `Success` / `Truncated: Maximum limit reached` /
`Error` (sub-reasons: unknown curve, DACS permissioning, invalid timezone,
no data / null LastUpdateTime).

**Incremental cursor:** store the max `value_date` seen (append) or max
`last_update_time` (cdc) in the offset dict `{"cursor": "<iso8601>"}`. Next run
sets `MinValueDate`/`MinForecastDate` = cursor. On the first run, fall back to
`table_options["start_date"]`.

**Deletes:** no delete feed. Corrections arrive as revised values (new
`forecast_date` / updated `last_update_time`) — handled by `cdc` upsert, not by
`cdc_with_deletes`.

**Pagination:**
- Offset paging: `PageSize` + `SkipSize` (TabularData/Data, AnalysisGroup,
  Options). `MaxResults` + `SkipRows` on Metadata/Alias search.
- Batch caps: `totalMaxValues`, `totalMaxForecasts`, `maxValues`, `maxForecasts`
  — exceeding a cap yields status `Truncated` (not an error), so re-request the
  remainder with a narrower window.
- **No cursor/continuation tokens.**

**Rate limits (§19):** per-**user** (aggregated across all that user's keys),
e.g. "1500 in 30s". Breach → **HTTP 403** with a body like
`Request rate limit exceeded. Maximum allowed is 1500 in 30s`. Strategy: retry
with exponential backoff on 403 (and 5xx) until the aggregate rate drops.
Numeric limits are environment-specific — confirm in the host's Swagger.

**TabularData read** — `GET /api/v1/TabularData/Data/{DataType}` with `Fields`
(pin explicitly), `Filter`, `OrderBy` (`+`/`-` prefixes), `MaxRows`, `PageSize`,
`SkipSize`.

## **Field Type Mapping**

| LDMS / JSON | Spark (`StructType`) |
|---|---|
| number (value) | `DoubleType` |
| date / datetime (ValueDate, ForecastDate, LastUpdateTime; ISO 8601, UTC) | `TimestampType` |
| CurveID, alias, tag values, status | `StringType` |
| ScenarioID | `IntegerType` |
| tabular fields | per `DataFields` type; default `StringType`, numeric→`DoubleType`, dates→`TimestampType` |

Notes: dates are UTC; actuals use `ForecastDate = 2000-01-01T00:00:00`;
`ScenarioID = 0` for standard data (non-zero returns no data for PointConnect).
Do **not** coerce values in the connector — the framework casts records to the
declared schema.

## **Sources and References**

- **`LDMS REST API Interface Guide v25.0.0.pdf`** (LSEG, Issue 25.0.0,
  31 Mar 2025) — official vendor documentation. *Highest confidence.*
- Per-host **Swagger UI** at `https://<host>/api/swagger` ("RDMS API v1") — the
  authoritative source for field-level request/response models, exact parameter
  casing, and the numeric rate-limit/batch caps the PDF omits. *Highest
  confidence; confirm live before production.*
- Internal `oil-trading/crawler/ENDPOINTS.md` — vetted endpoint catalog distilled
  from the guide. *High confidence.*
- No existing Airbyte/Singer/Fivetran connector for LDMS/RDMS was found; this is
  a net-new source. The curve/forecast model has no direct analog in common
  connectors, so schema and ingestion-type decisions are derived from the vendor
  guide.
