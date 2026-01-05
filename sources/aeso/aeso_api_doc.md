# AESO API Documentation

## Authorization

- **Method**: API Key authentication
- **Placement**: Query parameter or HTTP header (implementation uses header)
- **Registration**: AESO API Portal (https://api.aeso.ca)
- **Rate Limits**: TBD (monitor during implementation)

Example:
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  "https://api.aeso.ca/report/v1/poolPrice?startDate=2023-06-12&endDate=2023-06-13"
```

## Object List

| Object | Description | Endpoint | Ingestion Type |
|--------|-------------|----------|----------------|
| `pool_price` | Hourly pool price data (actual, forecast, rolling avg) | Pool Price Report API | `cdc` |

**Note**: Pool price is the first data set implemented. Additional AESO data streams (generation, demand, interchange) to be added in future iterations.

## Object Schema

### pool_price

| Field | Type | Description |
|-------|------|-------------|
| `begin_datetime_utc` | timestamp (ISO 8601) | Settlement hour start in UTC - natively UTC from source |
| `pool_price` | decimal (nullable) | Actual pool price ($/MWh) - null for future hours with forecast only |
| `forecast_pool_price` | decimal (nullable) | Forecasted price ($/MWh) |
| `rolling_30day_avg` | decimal (nullable) | 30-day rolling average ($/MWh) |
| `ingestion_time` | timestamp (ISO 8601) | UTC timestamp when the row was last ingested/updated |

**Primary Key**: `begin_datetime_utc`

## Ingestion Type

| Object | Type | Primary Key | Cursor | Rationale |
|--------|------|-------------|--------|-----------|
| `pool_price` | `cdc` | `begin_datetime_utc` | `begin_datetime_utc` | Records can be updated due to late-arriving data and settlement adjustments. CDC with lookback window ensures updates are captured. |

**CDC Strategy**:
- **Initial load**: Fetch from `start_date` (configured) to current date
- **Incremental**: Fetch from `(high_watermark - lookback_hours)` to current date
- **Merge**: Upsert by `begin_datetime_utc` primary key

## Read API

### Endpoint
- **Method**: GET
- **Path**: Pool Price Report API (exact path TBD)
- **Base URL**: https://api.aeso.ca/

### Parameters

| Parameter | Type | Required | Format | Description |
|-----------|------|----------|--------|-------------|
| `start_date` | string | Yes | YYYY-MM-DD | Start date (inclusive) |
| `end_date` | string | Yes | YYYY-MM-DD | End date (inclusive) |

**Notes**:
- Both start_date and end_date are required
- Date range is inclusive on both ends
- For single-day queries, set start_date = end_date

### Pagination

- Data fetched in 7-day batches for efficiency
- Large date ranges automatically split into multiple API calls

### Example Request

```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  "https://api.aeso.ca/report/v1/poolPrice?startDate=2024-12-01&endDate=2024-12-07"
```

### Example Response (Conceptual)

```json
{
  "return": {
    "Pool Price Report": [
      {
        "begin_datetime_utc": "2024-12-01T07:00:00",
        "pool_price": 45.67,
        "forecast_pool_price": 44.50,
        "rolling_30day_avg": 52.33,
        "ingestion_time": "2024-12-01T10:30:00"
      }
    ]
  }
}
```

## Configuration Parameters

### api_key (Required)
- **Purpose**: AESO API authentication
- **Obtain**: Register at AESO API Portal

### start_date (Optional)
- **Purpose**: Historical extraction starting point
- **Used**: Initial load only
- **Default**: 30 days ago
- **Format**: YYYY-MM-DD
- **Examples**: `"2024-01-01"`, `"2023-01-01"`

### lookback_hours (Optional)
- **Purpose**: CDC lookback window for capturing late-arriving data
- **Used**: Every incremental sync
- **Default**: 24 hours
- **Range**: 6-168 hours typically
- **Examples**: `24` (hourly schedule), `6` (15-min schedule), `72` (daily schedule)

### Parameter Interaction

- **Initial load**: Uses `start_date`, ignores `lookback_hours`
- **Incremental**: Uses `lookback_hours`, ignores `start_date`
- **End date**: Always automatic (current date), never user-configured

## Field Type Mapping

| AESO Type | Connector Type | Notes |
|-----------|----------------|-------|
| ISO 8601 datetime | TimestampType | UTC and Mountain Time (with DST) |
| Decimal/Float | DoubleType | Prices in $/MWh, 2-4 decimal places |

## Known Quirks

- **DST transitions**: Use `begin_datetime_utc` as primary key to avoid ambiguity during fall-back
- **Negative prices**: Normal during oversupply, not errors
- **Late-arriving data**: Records can be updated hours/days after initial publication
- **Hourly data**: New records published every hour, not more frequently

## Research Log

| Source | URL | Date | Confidence | Confirmed |
|--------|-----|------|------------|-----------|
| GitHub | https://github.com/guanjieshen/aeso-python-api | 2025-12-23 | High | API wrapper structure, field names, authentication, date parameters |

## Sources

- **aeso-python-api** (high confidence): https://github.com/guanjieshen/aeso-python-api
- **AESO API Portal**: https://api.aeso.ca (for API key registration)

**Note**: Implementation uses the aeso-python-api wrapper library, which handles low-level HTTP details.
