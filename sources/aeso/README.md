# AESO Connector for Databricks

Ingest Alberta electricity market data (hourly pool prices) from the AESO API into your Databricks lakehouse with automatic incremental updates.

## What You Get

- ⚡ **Hourly pool price data** - Actual prices, forecasts, and 30-day averages
- 🔄 **Automatic updates** - Captures late-arriving data with configurable lookback
- 📊 **SCD Type 1** - Latest values always win (historical tracking not supported)
- 🚀 **Sub-minute latency** - Optional continuous mode for real-time dashboards

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

**With custom options:**

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
          "scd_type": "SCD_TYPE_1",
          "start_date": "2024-01-01",
          "lookback_hours": "24"
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
| `lookback_hours` | 24 | Hours to look back on each sync to capture late updates |
| `batch_size_days` | 30 | Days per API batch (reduce for memory constraints) |
| `rate_limit_delay` | 0.1 | Seconds between API calls (increase if rate limited) |

### Choosing Your Schedule

| Schedule | `lookback_hours` | Best For |
|----------|------------------|----------|
| **Continuous** | 1-6 | Real-time dashboards, sub-minute latency |
| Every 15 min | 6 | Ultra-low latency monitoring |
| **Hourly (recommended)** | **24** | **Standard production use** |
| Daily | 72 | Cost-optimized, non-time-sensitive |

**Key insight:** `lookback_hours` should cover the window where AESO may update past records. Hourly data can be revised for settlement, so 24 hours captures most updates.

## Output Schema

### pool_price Table

| Column | Type | Description |
|--------|------|-------------|
| `begin_datetime_utc` | timestamp | Hour start in UTC (Primary Key) |
| `pool_price` | double | Actual price $/MWh (null for future hours) |
| `forecast_pool_price` | double | Forecasted price $/MWh |
| `rolling_30day_avg` | double | 30-day rolling average $/MWh |
| `ingestion_time` | timestamp | When this record was last updated |

**Note:** The connector uses `ingestion_time` to determine which version of a record is latest during merges.

## How It Works

### Initial Load
Fetches from `start_date` (or last 30 days) to today, creating your baseline dataset.

### Incremental Updates
Each run fetches from `(last_watermark - lookback_hours)` to today, ensuring:
- New hourly records are captured
- Updated prices (settlements) are merged in
- No data is missed even with late-arriving updates

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

### Real-Time Dashboard (continuous)

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

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **401 Authentication Error** | Verify API key at [AESO API Portal](https://api.aeso.ca) |
| **Missing recent updates** | Increase `lookback_hours` to capture longer settlement windows |
| **Rate limiting (429)** | Increase `rate_limit_delay` or reduce schedule frequency |
| **High memory usage** | Reduce `batch_size_days` to fetch smaller batches |

## Important Notes

- ⚠️ **Only SCD Type 1** is supported. SCD Type 2 (historical tracking) is not available.
- Prices can be **negative** during oversupply conditions (this is normal).
- AESO publishes **hourly**, so frequent schedules (e.g., every 15 min) will often find no new data.
- Always use `begin_datetime_utc` as your time dimension (DST-safe).

## Resources

- **AESO Python API:** https://github.com/guanjieshen/aeso-python-api
- **AESO Website:** https://www.aeso.ca
- **API Portal:** https://api.aeso.ca
- **Connector Code:** `sources/aeso/aeso.py`
