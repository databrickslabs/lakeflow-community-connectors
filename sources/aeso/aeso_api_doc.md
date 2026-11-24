# **AESO API Documentation**

## **Authorization**

- **Chosen method**: API Key authentication for the AESO REST API.
- **Base URL**: `https://api.aeso.ca` (inferred from AESO API Portal)
- **Auth placement**:
  - Typically passed as a query parameter or HTTP header (implementation-specific).
  - The [aeso-python-api](https://github.com/guanjieshen/aeso-python-api) wrapper handles authentication via API key initialization.
- **Required access**: 
  - Register for a free API key at the [AESO API Portal](https://api.aeso.ca).
  - No specific scopes or tiers documented; API key provides access to public market data.

Example authenticated request (via Python wrapper):

```python
from aeso import AESOAPI
import os

api_key = os.environ.get('AESO_API_KEY')
aeso = AESOAPI(api_key)

prices = aeso.get_pool_price_report(
    start_date="2023-06-12",
    end_date="2023-06-13"
)
```

Notes:
- Rate limiting details are not publicly documented; implement reasonable delays between requests.
- API is designed for public electricity market data access.
- Authentication failures return HTTP 401 errors.


## **Object List**

For connector purposes, we treat specific AESO API endpoints as **objects/tables**.  
The object list is **static** (defined by the connector), not discovered dynamically from an API.

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `pool_price` | Hourly electricity pool prices, forecasts, and 30-day rolling averages | Pool Price Report endpoint | `cdc` (upserts based on `begin_datetime_utc` with lookback) |

**Connector scope for initial implementation**:
- Step 1 focuses on the `pool_price` object.
- AESO provides additional market data endpoints (e.g., supply/demand, system alerts) that could be added in future extensions.

High-level notes:
- **Pool Price**: The primary electricity market data endpoint, providing hourly settlement prices in Alberta's electricity market.
- **Hourly granularity**: Data is published hourly with one record per settlement hour.
- **Late-arriving data**: Pool prices can be updated after initial publication due to settlement adjustments, requiring CDC with lookback for accurate ingestion.
- **Future hours**: The API includes forecast prices for future hours where actual pool prices are not yet available (null values).


## **Object Schema**

### General notes

- AESO provides structured data via its REST API, accessible through the [aeso-python-api wrapper](https://github.com/guanjieshen/aeso-python-api).
- For the connector, we define **tabular schemas** per object, derived from the API response structure.
- Timestamps are provided in both UTC and Mountain Time (MPT) formats.

### `pool_price` object (primary table)

**Source endpoint**:  
Pool Price Report (accessed via `aeso.get_pool_price_report()` in the Python wrapper)

**Key behavior**:
- Returns hourly electricity pool prices for Alberta's energy market.
- Includes actual prices (for completed hours), forecast prices (for future hours), and rolling averages.
- Supports date range queries via `start_date` and `end_date` parameters.
- Data can be updated after initial publication due to settlement processes.

**High-level schema (connector view)**:

Top-level fields (all from the AESO API):

| Column Name | Type | Description |
|------------|------|-------------|
| `begin_datetime_utc` | timestamp (UTC) | Settlement hour start time in UTC. Primary key and natural sort field. |
| `begin_datetime_mpt` | timestamp (Mountain Time) | Settlement hour start time in Mountain Time (includes DST transitions). |
| `pool_price` | float (nullable) | Actual electricity pool price in $/MWh. Null for future hours with only forecasts available. Finalized during settlement (typically within 24-72 hours). |
| `forecast_pool_price` | float (nullable) | Forecasted pool price in $/MWh. Updated frequently by AESO as the settlement hour approaches. |
| `rolling_30day_avg` | float (nullable) | 30-day rolling average pool price in $/MWh. |

**Additional connector-derived fields**:

| Column Name | Type | Description |
|------------|------|-------------|
| `ingestion_time` | timestamp (UTC) | Connector-generated timestamp indicating when the row was last ingested/updated. Used for SCD Type 1 sequencing. |

**Data characteristics**:
- **Hourly granularity**: One record per hour (on the hour).
- **Timestamps**: `begin_datetime_utc` represents the start of the settlement hour.
- **Price units**: All prices are in Canadian dollars per megawatt-hour ($/MWh).
- **Nullable fields**: `pool_price` is null for future hours; other fields may be null during data gaps.
- **Price ranges**: Pool prices can be negative during oversupply conditions (common in renewable-heavy grids).
- **Frequent forecast updates**: `forecast_pool_price` is revised continuously by AESO as settlement hours approach. The connector's lookback window captures these updates.
- **Settlement updates**: `pool_price` (actual) is finalized during settlement, typically within 24-72 hours of the hour. The connector's lookback captures these revisions.

**Example API response** (from Python wrapper):

```python
@dataclass
class PoolPrice:
    begin_datetime_utc: datetime  # e.g., 2023-06-12 19:00:00 UTC
    begin_datetime_mpt: datetime  # e.g., 2023-06-12 12:00:00 MST/MDT
    pool_price: float            # e.g., 45.23 ($/MWh) or None
    forecast_pool_price: float   # e.g., 48.50 ($/MWh)
    rolling_30day_avg: float     # e.g., 52.15 ($/MWh)
```

**Example record** (connector representation):

```json
{
  "begin_datetime_utc": "2023-06-12T19:00:00Z",
  "begin_datetime_mpt": "2023-06-12T12:00:00-07:00",
  "pool_price": 45.23,
  "forecast_pool_price": 48.50,
  "rolling_30day_avg": 52.15,
  "ingestion_time": "2024-01-15T10:30:00Z"
}
```

**Null value example** (future hour with no actual price):

```json
{
  "begin_datetime_utc": "2024-01-15T20:00:00Z",
  "begin_datetime_mpt": "2024-01-15T13:00:00-07:00",
  "pool_price": null,
  "forecast_pool_price": 52.10,
  "rolling_30day_avg": 50.25,
  "ingestion_time": "2024-01-15T10:30:00Z"
}
```

> The columns listed above define the **complete connector schema** for the `pool_price` table.  
> If additional AESO API fields become available, they must be added as new columns here.


## **Get Object Primary Key**

There is no dedicated metadata endpoint to get the primary key for the `pool_price` object.  
Instead, the primary key is defined **statically** based on the resource schema and domain knowledge.

- **Primary key for `pool_price`**: `begin_datetime_utc`  
  - Type: timestamp (UTC)  
  - Property: Unique per settlement hour. Each hour has at most one record.
  - Rationale: Settlement hours are the natural granularity of Alberta's electricity market.

The connector will:
- Read the `begin_datetime_utc` field from each pool price record.
- Use it as the immutable primary key for upserts when ingestion type is `cdc`.
- Note that `begin_datetime_mpt` should NOT be used as a primary key due to DST ambiguity (one hour occurs twice during fall DST transitions).

Example showing primary key in response:

```python
price = PoolPrice(
    begin_datetime_utc=datetime(2023, 6, 12, 19, 0, 0),  # Primary key
    begin_datetime_mpt=datetime(2023, 6, 12, 12, 0, 0),
    pool_price=45.23,
    forecast_pool_price=48.50,
    rolling_30day_avg=52.15
)
```


## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Ingestion type for AESO pool_price:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `pool_price` | `cdc` | Pool prices have a stable primary key (`begin_datetime_utc`) and can be updated after initial publication due to settlement adjustments. The connector uses a lookback window to recapture recently updated records, making CDC with upserts the appropriate pattern. |

For `pool_price`:
- **Primary key**: `begin_datetime_utc`
- **Cursor field**: `begin_datetime_utc` (used for watermark tracking)
- **Sequence field**: `ingestion_time` (connector-generated; determines which version is newest for SCD Type 1)
- **Lookback window**: Configurable (default 24 hours) to recapture updated records
- **Sort order**: Ascending by `begin_datetime_utc`
- **Deletes**: AESO does not delete historical records; closed hours remain in the dataset with final settled values.
- **Updates**: Pool prices can be updated for 24-72 hours after initial publication as settlement calculations are finalized.


## **Read API for Data Retrieval**

### Primary read endpoint for `pool_price`

- **Access method**: Via [aeso-python-api wrapper](https://github.com/guanjieshen/aeso-python-api)
- **Function**: `AESOAPI.get_pool_price_report(start_date, end_date)`
- **Underlying endpoint**: Pool Price Report API (exact REST endpoint not publicly documented)

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string | yes | - | Start date in format `YYYY-MM-DD`. Data is fetched from the beginning of this date. |
| `end_date` | string | no | `start_date` | End date in format `YYYY-MM-DD`. If omitted, returns all completed hours for `start_date`. |

**Return type**:
- List of `PoolPrice` objects (Python dataclass)
- Ordered by `begin_datetime_utc` ascending

**Pagination**:
- Not explicitly documented; the wrapper handles any underlying pagination automatically.
- Large date ranges are handled by the wrapper without client-side pagination logic.

Example request:

```python
from aeso import AESOAPI
import os

aeso = AESOAPI(os.environ.get('AESO_API_KEY'))

# Fetch range
prices = aeso.get_pool_price_report(
    start_date="2023-06-12",
    end_date="2023-06-13"
)

# Fetch single day (all completed hours)
prices = aeso.get_pool_price_report(
    start_date="2023-06-12"
)
```

**Incremental strategy**:
- On the first run, the connector:
  - Uses a configurable `start_date` for historical backfill, or
  - Defaults to 30 days ago if no `start_date` is provided.
- On subsequent runs:
  - Uses the maximum `begin_datetime_utc` from the previous sync minus a configurable lookback window (default 24 hours).
  - Fetches from `(high_watermark - lookback_hours)` to today.
  - Applies upserts based on `begin_datetime_utc`, with `ingestion_time` determining the newest record for SCD Type 1.

**Handling updates**:
- AESO updates pool prices as settlement calculations are finalized, typically within 24-72 hours.
- The connector captures updates by:
  - Fetching overlapping data via the lookback window on each run.
  - Using `ingestion_time` (connector-generated) to determine which version is newest.
  - Upserting records with the latest `ingestion_time` for each `begin_datetime_utc`.

**Handling future hours**:
- The API returns forecast prices for future hours where actual pool prices are not yet available.
- These records have:
  - `pool_price`: `null` (or `None` in Python)
  - `forecast_pool_price`: populated with forecasted value
  - These records are updated with actual prices as hours complete and settlement occurs.

**Date range behavior**:
- `start_date` and `end_date` are inclusive.
- Requesting future dates returns forecasted data.
- Requesting dates beyond available history returns empty results (no error).
- The API automatically handles timezone conversions and DST transitions.


### Batch fetching strategy

For large historical backfills, the connector implements batch fetching:

```python
# Example: Fetch 90 days in 30-day batches
from datetime import datetime, timedelta

start = datetime(2023, 1, 1)
end = datetime(2023, 3, 31)
batch_size = timedelta(days=30)

current = start
all_prices = []

while current <= end:
    batch_end = min(current + batch_size, end)
    
    prices = aeso.get_pool_price_report(
        start_date=current.strftime("%Y-%m-%d"),
        end_date=batch_end.strftime("%Y-%m-%d")
    )
    
    all_prices.extend(prices)
    current = batch_end + timedelta(days=1)
    
    # Rate limiting
    time.sleep(0.1)
```


## **Field Type Mapping**

### General mapping (AESO API → connector logical types)

| AESO API Type | Example Fields | Connector Logical Type | Notes |
|---------------|----------------|------------------------|-------|
| datetime (UTC) | `begin_datetime_utc` | timestamp (UTC) | Stored as UTC timezone-aware timestamp. The Python wrapper returns timezone-aware datetime objects. The connector strips timezone info for PySpark compatibility. |
| datetime (MPT) | `begin_datetime_mpt` | timestamp (Mountain Time) | Mountain Time with DST transitions. Ambiguous during fall DST (one hour repeats). Not recommended as primary key. |
| float | `pool_price`, `forecast_pool_price`, `rolling_30day_avg` | double (nullable) | Prices in $/MWh. Can be null (future hours) or negative (oversupply). |

### Special behaviors and constraints

- **Timestamps**:
  - `begin_datetime_utc` is the authoritative time field; always use this for primary key and time-series analysis.
  - `begin_datetime_mpt` is provided for user convenience but should not be used for joins or keys due to DST ambiguity.
  - The Python wrapper returns timezone-aware datetime objects; the connector strips timezone info to convert to naive UTC for PySpark.
  
- **Prices**:
  - All price fields are in Canadian dollars per megawatt-hour ($/MWh).
  - Prices can be **negative** during oversupply conditions (renewable energy production exceeds demand).
  - `pool_price` is `null` for future hours where actual settled prices are not yet available.
  - Price precision is typically 2 decimal places but should be stored as `double` to preserve accuracy.

- **Null handling**:
  - `pool_price` is frequently null for future hours (only forecasts available).
  - Other fields (`forecast_pool_price`, `rolling_30day_avg`) can be null during data gaps or system issues.
  - The connector must preserve null values (not convert to 0 or skip records).

- **Data quality**:
  - Prices are typically positive but can be negative (oversupply).
  - Extreme prices (>$1000/MWh or <-$100/MWh) are possible during grid emergencies but rare.
  - Missing hours in the time series may indicate API or system issues.


## **Write API**

The AESO API is **read-only** for public market data access. There is no write API for creating or updating pool price data.

- Pool prices are generated by AESO's market settlement processes.
- Data is published via the API for public consumption.
- Users cannot modify or delete historical data via the API.


## **Known Quirks & Edge Cases**

- **Timezone complexity**:
  - `begin_datetime_mpt` includes DST transitions, making it ambiguous during fall time change (one hour occurs twice).
  - **Always use `begin_datetime_utc` as the primary key and time dimension** to avoid DST issues.
  - The connector removes `begin_datetime_mpt` from the schema to prevent downstream confusion.

- **Negative prices**:
  - Pool prices can be **negative** when renewable generation exceeds demand and there's insufficient transmission capacity to export.
  - This is a normal market condition, not a data error.
  - Downstream analytics should handle negative values appropriately.

- **Null vs zero**:
  - `null` (or `None`) indicates data is not available (e.g., future hours with no settled price).
  - Zero (`0.0`) is a valid price indicating supply and demand are balanced with minimal scarcity.
  - The connector must distinguish between null and zero.

- **Settlement adjustments**:
  - Pool prices can be updated for 24-72 hours after initial publication as settlement calculations are refined.
  - The connector uses a configurable lookback window (default 24 hours) to recapture updated values.
  - Records with the same `begin_datetime_utc` but different `ingestion_time` represent successive updates.

- **Forecast accuracy**:
  - `forecast_pool_price` is provided for future hours but accuracy varies based on grid conditions.
  - Forecasts are updated as the settlement hour approaches.
  - Actual prices often differ from forecasts due to unexpected supply/demand changes.

- **Rolling average gaps**:
  - `rolling_30day_avg` may be null for the first 29 days of available data.
  - Gaps in historical data can cause temporary nulls in the rolling average.

- **API availability**:
  - The API may experience brief outages during AESO system maintenance.
  - Historical data is generally stable; recent data (last 24-72 hours) may see updates.

- **Rate limiting**:
  - Explicit rate limits are not documented.
  - Recommended practice: implement 0.1-0.5 second delays between batch requests.
  - API returns HTTP 429 if rate limited.

- **Date range limits**:
  - No documented maximum date range per request.
  - The connector implements batching (default 30 days) to avoid potential timeouts on large historical fetches.

- **Hourly granularity only**:
  - Data is published hourly (on the hour).
  - No sub-hourly or daily aggregated data is available via this endpoint.
  - Running the connector every 15 minutes will often find no new completed hours (expected behavior).


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| GitHub Repository | https://github.com/guanjieshen/aeso-python-api | 2026-01-06 | High | Python wrapper API, PoolPrice dataclass fields, parameter names, and example usage. |
| AESO API Portal | https://api.aeso.ca | 2026-01-06 | High | API key registration process and authentication requirements. |
| AESO Website | https://www.aeso.ca | 2026-01-06 | Medium | Alberta electricity market overview and pool price concepts. |
| Python Package | PyPI - aeso-python-api | 2026-01-06 | High | Package availability, version history, and dependencies. |


## **Sources and References**

- **aeso-python-api GitHub Repository** (highest confidence)
  - https://github.com/guanjieshen/aeso-python-api
  - MIT License
  - Provides Python wrapper for AESO API

- **AESO API Portal**
  - https://api.aeso.ca
  - API key registration and documentation

- **AESO Website**
  - https://www.aeso.ca
  - Alberta Electric System Operator main website

- **PyPI Package**
  - https://pypi.org/project/aeso-python-api/
  - Installation and version information

**Connector implementation reference**:
- `sources/aeso/aeso.py` - Databricks connector implementation
- `sources/aeso/README.md` - Connector user guide

When conflicts arise, **the aeso-python-api GitHub repository** is treated as the source of truth for API behavior, field names, and data types.
