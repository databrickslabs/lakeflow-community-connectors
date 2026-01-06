# AESO Connector for Databricks

Ingest Alberta electricity market data (hourly pool prices) from the AESO API into your Databricks lakehouse with automatic incremental updates.

## What You Get

- ⚡ **Hourly pool price data** - Actual prices, forecasts, and 30-day averages
- 🔄 **Automatic forecast updates** - Captures frequently updated forecast prices with configurable lookback
- 📊 **SCD Type 1** - Latest values always win (historical tracking not supported)
- 🚀 **Sub-minute latency** - Optional continuous mode for real-time use cases

## Quick Start

### 1. Get Your API Key

Sign up at [AESO API Portal](https://api.aeso.ca) to get your API key.

### 2. Create Connection in Databricks

Create a Unity Catalog connection with your API key:

```json
{
  "api_key": "your-aeso-api-key"
}
```

### 3. Configure Your Pipeline

**Minimal setup (uses defaults):**

```json
{
  "pipeline_spec": {
    "connection_name": "my_aeso_connection",
    "objects": [{
      "table": {
        "source_table": "pool_price",
        "destination_catalog": "main",
        "destination_schema": "energy",
        "table_configuration": {
          "scd_type": "SCD_TYPE_1"
        }
      }
    }]
  }
}
```

**With all table configuration options:**

```json
{
  "pipeline_spec": {
    "connection_name": "my_aeso_connection",
    "objects": [{
      "table": {
        "source_table": "pool_price",
        "destination_catalog": "main",
        "destination_schema": "energy",
        "destination_table": "pool_price",
        "table_configuration": {
          "scd_type": "SCD_TYPE_1",
          "start_date": "2024-01-01",
          "lookback_hours": "24",
          "batch_size_days": "30",
          "rate_limit_delay": "0.1"
        }
      }
    }]
  }
}
```

### 4. Schedule Your Pipeline

**Standard (hourly):**
```json
{
  "quartz_cron_expression": "0 * * * ?",
  "timezone_id": "America/Edmonton"
}
```

**Real-time (continuous):**
```json
{
  "continuous": {
    "pause_status": "UNPAUSED"
  }
}
```

## Configuration Options

### Table Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `scd_type` | - | **Required.** Must be `"SCD_TYPE_1"` (only supported type) |
| `start_date` | 30 days ago | Historical start date for initial load (YYYY-MM-DD) |
| `lookback_hours` | 24 | Hours to look back on each sync. **Minimum: 24 hours** (required for settlement adjustments). Used for: (1) backfill when pipeline has been stopped, (2) capturing frequent forecast price updates |
| `batch_size_days` | 30 | Days per API batch (reduce for memory constraints) |
| `rate_limit_delay` | 0.1 | Seconds between API calls (increase if rate limited) |

### Choosing Your Schedule

| Schedule | `lookback_hours` | Best For |
|----------|------------------|----------|
| **Continuous** | 24 | Real-time use cases, sub-minute latency |
| Every 15 min | 24 | Ultra-low latency monitoring |
| **Hourly (recommended)** | **24** | **Standard production use** |
| Daily | 72 | Cost-optimized, non-time-sensitive |

**Key insight:** `lookback_hours` serves two purposes:
1. **Backfill after downtime** - If your pipeline stops for hours/days, the lookback ensures you recapture data from that gap
2. **Capture forecast updates** - AESO continuously updates `forecast_pool_price` as settlement hours approach

Actual pool prices are also revised during final settlement. **A minimum 24-hour lookback is enforced** to safely capture settlement adjustments and late-arriving data.

## Output Schema

### pool_price Table

| Column | Type | Description |
|--------|------|-------------|
| `begin_datetime_utc` | timestamp | Hour start in UTC (Primary Key) |
| `pool_price` | double | Actual price $/MWh (null for future hours; finalized during settlement) |
| `forecast_pool_price` | double | Forecasted price $/MWh (updated frequently as hour approaches) |
| `rolling_30day_avg` | double | 30-day rolling average $/MWh |
| `ingestion_time` | timestamp | When this record was last updated |

**Note:** The connector uses `ingestion_time` to determine which version of a record is latest during merges.

## How It Works

### Initial Load
Fetches from `start_date` (or last 30 days) to today, creating your baseline dataset.

### Incremental Updates
Each run fetches from `(last_watermark - lookback_hours)` to today, ensuring:
- **Pipeline downtime backfill** - If the pipeline stops for any period, the lookback recaptures missed data
- **New hourly records** are captured as hours complete
- **Updated forecast prices** are merged in (AESO updates forecasts frequently as hours approach)
- **Revised actual prices** are captured during final settlement (typically within 24-72 hours)

**Example (hourly schedule, 24h lookback):**
- **Run 1:** Fetch Jan 1 → Jan 15 (initial load)
- **Run 2:** Fetch Jan 14 → Jan 15 (1 day overlap to catch updates)
- **Run 3:** Fetch Jan 15 → Jan 16 (1 day overlap to catch updates)

## Common Scenarios

### Starting Fresh with Historical Data

```json
"table_configuration": {
  "scd_type": "SCD_TYPE_1",
  "start_date": "2023-01-01"
}
```

### High-Frequency Updates (15-minute refresh)

```json
"table_configuration": {
  "scd_type": "SCD_TYPE_1",
  "lookback_hours": "6"
}
```

### Real-Time Use Cases (continuous)

```json
"table_configuration": {
  "scd_type": "SCD_TYPE_1",
  "lookback_hours": "2"
}
```

**Pipeline schedule:**
```json
{
  "continuous": {
    "pause_status": "UNPAUSED"
  }
}
```

### Recovery from Pipeline Downtime

If your pipeline stops for hours or days, the `lookback_hours` setting provides automatic backfill:

```json
"table_configuration": {
  "scd_type": "SCD_TYPE_1",
  "lookback_hours": "72"
}
```

**How it works:**
- Pipeline stops at Jan 5, 10:00 AM
- Pipeline restarts at Jan 8, 2:00 PM
- Next run fetches from Jan 5 (watermark - 72h lookback) → today
- All missed data is automatically recaptured

**Recommendation:** Set `lookback_hours` ≥ your maximum expected downtime.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **401 Authentication Error** | Verify API key at [AESO API Portal](https://api.aeso.ca) |
| **Missing recent updates** | Increase `lookback_hours` to capture longer settlement windows |
| **Rate limiting (429)** | Increase `rate_limit_delay` or reduce schedule frequency |
| **High memory usage** | Reduce `batch_size_days` to fetch smaller batches |

## Important Notes

- ⚠️ **Only SCD Type 1** is supported. SCD Type 2 (historical tracking) is not available.
- **Forecast prices update frequently** - AESO revises `forecast_pool_price` as settlement hours approach. Use appropriate `lookback_hours` to capture updates.
- **Actual prices finalize later** - `pool_price` is updated during settlement (typically 24-72 hours after the hour).
- Prices can be **negative** during oversupply conditions (this is normal).
- AESO publishes **hourly**, so frequent schedules (e.g., every 15 min) will often find no new completed hours.
- Always use `begin_datetime_utc` as your time dimension (DST-safe).

## Resources

- **AESO Python API:** https://github.com/guanjieshen/aeso-python-api
- **AESO Website:** https://www.aeso.ca
- **API Portal:** https://api.aeso.ca
- **Connector Code:** `sources/aeso/aeso.py`
