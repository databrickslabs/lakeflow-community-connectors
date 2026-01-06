# Lakeflow AESO Community Connector

Extract Alberta electricity market data from AESO API into Databricks Delta lake. Supports incremental CDC synchronization for hourly pool price data.

## SCD Type 1 Automatic Configuration

The connector automatically configures `sequence_by: "ingestion_time"` for proper merge behavior. You can optionally override this in your pipeline specification if needed. See [PIPELINE_CONFIG.md](PIPELINE_CONFIG.md) for advanced configuration options.

## Prerequisites

- AESO API key from [AESO API Portal](https://api.aeso.ca)
- Python environment with `aeso-python-api` package

## Quick Start

### 1. Configure Connection

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `api_key` | Yes | - | AESO API key |

**Connection config:**
```json
{
  "api_key": "your-aeso-api-key"
}
```

### 2. Configure Pipeline

**Basic configuration:**
```json
{
  "pipeline_spec": {
    "connection_name": "my_aeso_connection",
    "object": [
      {
        "table": {
          "source_table": "pool_price",
          "destination_catalog": "your_catalog",
          "destination_schema": "your_schema",
          "destination_table": "pool_price",
          "scd_type": "SCD_TYPE_1"
        }
      }
    ]
  }
}
```

**With table configuration:**
```json
{
  "pipeline_spec": {
    "connection_name": "my_aeso_connection",
    "object": [
      {
        "table": {
          "source_table": "pool_price",
          "destination_catalog": "your_catalog",
          "destination_schema": "your_schema",
          "destination_table": "pool_price",
          "table_configuration": {
            "scd_type": "SCD_TYPE_1",
            "start_date": "2024-01-01",
            "lookback_hours": "24",
            "batch_size_days": "7",
            "rate_limit_delay": "0.5"
          }
        }
      }
    ]
  }
}
```

**Table Configuration Options:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `start_date` | 30 days ago | Historical extraction start date (YYYY-MM-DD). Used only for initial load. |
| `lookback_hours` | 24 | CDC lookback window in hours. Used for every incremental sync to recapture updates. |
| `batch_size_days` | 30 | Number of days to fetch per API batch. Smaller batches reduce memory usage but increase API calls. |
| `rate_limit_delay` | 0.1 | Seconds to wait between API calls. Increase if hitting rate limits, decrease for faster fetching. |

**Note**: The connector automatically configures `sequence_by: "ingestion_time"` for proper SCD Type 1 merges. You don't need to specify it unless you want to override the default behavior.

### 3. Schedule & Run

The connector automatically handles end dates and fetches up to "now" on every run.

## Configuration Guide

### Two Independent Table Parameters

**`start_date`** - Historical extraction starting point (table configuration)
- Used: Initial load only
- Purpose: Set where historical backfill begins
- Examples: `"2024-01-01"`, `"2023-01-01"`, or omit for default (30 days ago)
- Configured in: `table_configuration` section of pipeline spec

**`lookback_hours`** - CDC lookback window (table configuration)
- Used: Every incremental sync
- Purpose: Recapture recent data to catch late-arriving updates
- Recommendation: Match to your schedule frequency (see table below)
- Configured in: `table_configuration` section of pipeline spec

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
| `begin_datetime_utc` | timestamp | Settlement hour start (UTC) - Primary Key, natively UTC from source |
| `pool_price` | double (nullable) | Actual pool price ($/MWh) - null for future hours |
| `forecast_pool_price` | double (nullable) | Forecasted price ($/MWh) |
| `rolling_30day_avg` | double (nullable) | 30-day rolling average ($/MWh) |
| `ingestion_time` | timestamp | UTC timestamp when the row was last ingested/updated |

**Ingestion type:** CDC (SCD Type 1)
- **Primary Key**: `begin_datetime_utc` (unique hour identifier)
- **Cursor**: `begin_datetime_utc` (incremental progress tracking)
- **Sequence By**: `ingestion_time` (determines latest record for merges)

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
