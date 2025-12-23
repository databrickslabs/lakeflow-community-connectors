# Lakeflow AESO Community Connector

Extract Alberta electricity market data from AESO API into your data lake. Supports incremental CDC synchronization for hourly pool price data.

## Prerequisites

- AESO API key from [AESO API Portal](https://api.aeso.ca)
- Python environment with `aeso-python-api` package

## Quick Start

### 1. Configure Connection

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `api_key` | Yes | - | AESO API key |
| `start_date` | No | 30 days ago | Historical extraction start (YYYY-MM-DD) |
| `lookback_hours` | No | 24 | CDC lookback window in hours |

**Simple config:**
```json
{
  "api_key": "your-aeso-api-key"
}
```

**With customization:**
```json
{
  "api_key": "your-aeso-api-key",
  "start_date": "2024-01-01",
  "lookback_hours": 24
}
```

### 2. Configure Pipeline

```json
{
  "pipeline_spec": {
    "connection_name": "my_aeso_connection",
    "object": [
      {
        "table": {
          "source_table": "pool_price"
        }
      }
    ]
  }
}
```

### 3. Schedule & Run

The connector automatically handles end dates and fetches up to "now" on every run.

## Configuration Guide

### Two Independent Parameters

**`start_date`** - Historical extraction starting point
- Used: Initial load only
- Purpose: Set where historical backfill begins
- Examples: `"2024-01-01"`, `"2023-01-01"`, or omit for default (30 days ago)

**`lookback_hours`** - CDC lookback window
- Used: Every incremental sync
- Purpose: Recapture recent data to catch late-arriving updates
- Recommendation: Match to your schedule frequency (see table below)

### Scheduling Recommendations

| Schedule | Runs/Day | Recommended `lookback_hours` | Use Case |
|----------|----------|------------------------------|----------|
| Every 15 min | 96 | 6 | Ultra-low latency dashboards |
| Every 30 min | 48 | 12 | Near real-time |
| **Hourly** | 24 | **24** | **Standard (recommended)** |
| Every 4 hours | 6 | 48 | Regular updates |
| Daily | 1 | 72 | Cost-optimized |

**Cron examples:**
```bash
*/15 * * * *  # Every 15 minutes
0 * * * *     # Hourly
0 2 * * *     # Daily at 2 AM
```

**Databricks workflow:**
```json
{
  "quartz_cron_expression": "0 * * * ?",
  "timezone_id": "America/Edmonton"
}
```

## Data Schema

### pool_price Table

| Field | Type | Description |
|-------|------|-------------|
| `begin_datetime_utc` | timestamp | Settlement hour start (UTC) - Primary Key |
| `begin_datetime_mpt` | timestamp | Settlement hour start (Mountain Time) |
| `pool_price` | double | Actual pool price ($/MWh) |
| `forecast_pool_price` | double | Forecasted price ($/MWh) |
| `rolling_30day_avg` | double | 30-day rolling average ($/MWh) |

**Ingestion type:** CDC with upsert by `begin_datetime_utc`

## How It Works

### Initial Load
```
Fetches: start_date (or 30 days ago) → today
Result: High watermark = latest record timestamp
```

### Incremental Sync (CDC with Lookback)
```
Fetches: (high_watermark - lookback_hours) → today
Result: Updated watermark + recaptured late updates
```

**Example with hourly schedule:**
```
Day 1, 2pm: Fetches Jan 1 → Dec 23 (initial load)
Day 2, 2pm: Fetches Dec 22 2pm → Dec 24 2pm (24h lookback)
Day 3, 2pm: Fetches Dec 23 2pm → Dec 25 2pm (24h lookback)
```

Late-arriving data within the lookback window is automatically captured and merged.

## Troubleshooting

**Authentication Errors (401)**
- Verify API key is correct and active
- Check key registration at AESO API Portal

**Rate Limiting (429)**
- Reduce `lookback_hours` for frequent schedules
- Add delays between runs
- Reduce schedule frequency

**Missing Updates**
- Increase `lookback_hours` if late data arrives beyond current window
- Verify pipeline merge/upsert is configured correctly

**Frequent Runs (15-min) Showing "No New Records"**
- Expected! AESO publishes hourly, so 75% of 15-min runs find no new complete hours
- Purpose: Minimize latency and capture late updates quickly

## Data Characteristics

- **Hourly granularity**: One record per settlement hour
- **CDC updates**: Records can be updated after initial publication
- **No deletes**: Records are never deleted, only updated
- **Prices**: Can be negative during oversupply conditions
- **Timestamps**: Always use `begin_datetime_utc` as primary key (DST-safe)

## References

- Connector: `sources/aeso/aeso.py`
- AESO Python API: https://github.com/guanjieshen/aeso-python-api
- AESO Website: https://www.aeso.ca
- API Portal: https://api.aeso.ca
