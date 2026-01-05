# AESO Pipeline Configuration Guide

## Automatic SCD Type 1 Configuration

The AESO connector automatically configures `sequence_by: "ingestion_time"` in its metadata, so you **don't need to specify it** in your pipeline specification. The framework will use the connector's metadata automatically.

## How It Works

The AESO connector uses SCD Type 1 to handle updates:
- Same hour can be ingested multiple times (e.g., updated forecasts)
- The `ingestion_time` field tracks when each record was fetched
- The connector's metadata specifies `sequence_by: "ingestion_time"`
- The pipeline framework reads this from metadata automatically
- **Result**: Proper merges with latest values winning

## Pipeline Configuration

### Minimal Configuration (Recommended)

```json
{
  "pipeline_spec": {
    "connection_name": "my_aeso_connection",
    "object": [
      {
        "table": {
          "source_table": "pool_price",
          "scd_type": "SCD_TYPE_1"
        }
      }
    ]
  }
}
```

**Note**: `sequence_by` is automatically configured from connector metadata. You don't need to specify it.

### Complete Configuration

```json
{
  "pipeline_spec": {
    "connection_name": "my_aeso_connection",
    "object": [
      {
        "table": {
          "source_table": "pool_price",
          "destination_catalog": "main",
          "destination_schema": "energy",
          "destination_table": "aeso_pool_price",
          "scd_type": "SCD_TYPE_1",
          "sequence_by": "ingestion_time"
        }
      }
    ]
  }
}
```

## Field Explanations

| Field | Required | Description |
|-------|----------|-------------|
| `source_table` | Yes | Must be `"pool_price"` |
| `scd_type` | Yes | Use `"SCD_TYPE_1"` (latest value wins) |
| `sequence_by` | No | **Auto-configured** to `"ingestion_time"` from connector metadata |
| `destination_catalog` | No | Target catalog (default: workspace default) |
| `destination_schema` | No | Target schema/database |
| `destination_table` | No | Target table name (default: source_table) |

## How It Works

### Without `sequence_by` (BROKEN) ❌

```
First Run (10:00):
  begin_datetime_utc: 2026-01-05 20:00
  forecast_pool_price: 792.33
  
Second Run (10:15):
  begin_datetime_utc: 2026-01-05 20:00  ← Same key
  forecast_pool_price: 800.00           ← Updated

Result: BOTH records kept (duplicate) or random one wins
```

### With `sequence_by: "ingestion_time"` (CORRECT) ✅

```
First Run (10:00):
  begin_datetime_utc: 2026-01-05 20:00
  forecast_pool_price: 792.33
  ingestion_time: 2026-01-06 10:00:00
  
Second Run (10:15):
  begin_datetime_utc: 2026-01-05 20:00  ← Same key
  forecast_pool_price: 800.00           ← Updated
  ingestion_time: 2026-01-06 10:15:00   ← Later (WINS!)

Result: Second record OVERWRITES first (correct SCD Type 1)
```

## Example Configurations by Use Case

### 15-Minute Schedule (Real-time Monitoring)

```json
{
  "connection_config": {
    "api_key": "YOUR_API_KEY",
    "start_date": "2024-01-01",
    "lookback_hours": 6
  },
  "pipeline_spec": {
    "connection_name": "aeso_realtime",
    "object": [
      {
        "table": {
          "source_table": "pool_price",
          "destination_schema": "energy_realtime",
          "scd_type": "SCD_TYPE_1",
          "sequence_by": "ingestion_time"
        }
      }
    ]
  }
}
```

### Hourly Schedule (Standard)

```json
{
  "connection_config": {
    "api_key": "YOUR_API_KEY",
    "start_date": "2023-01-01",
    "lookback_hours": 24
  },
  "pipeline_spec": {
    "connection_name": "aeso_hourly",
    "object": [
      {
        "table": {
          "source_table": "pool_price",
          "destination_schema": "energy",
          "scd_type": "SCD_TYPE_1",
          "sequence_by": "ingestion_time"
        }
      }
    ]
  }
}
```

### Daily Schedule (Historical Analysis)

```json
{
  "connection_config": {
    "api_key": "YOUR_API_KEY",
    "start_date": "2020-01-01",
    "lookback_hours": 72
  },
  "pipeline_spec": {
    "connection_name": "aeso_daily",
    "object": [
      {
        "table": {
          "source_table": "pool_price",
          "destination_schema": "energy_historical",
          "scd_type": "SCD_TYPE_1",
          "sequence_by": "ingestion_time"
        }
      }
    ]
  }
}
```

## Troubleshooting

### Problem: Records not merging, seeing duplicates

**Symptoms:**
- Multiple records for the same `begin_datetime_utc`
- Forecast values not updating

**Solution:**
Add `"sequence_by": "ingestion_time"` to your pipeline spec table configuration.

### Problem: Getting errors about unknown field

**Symptoms:**
- Pipeline errors mentioning `sequence_by` not recognized

**Solution:**
Check that your pipeline framework version supports the `sequence_by` parameter in the table configuration.

### Problem: Old values not being overwritten

**Symptoms:**
- New ingestions create new records instead of updating
- Table growing with duplicates

**Solution:**
1. Verify `scd_type` is set to `"SCD_TYPE_1"` (not `"APPEND_ONLY"`)
2. Verify `sequence_by` is set to `"ingestion_time"`
3. Check pipeline logs to confirm `apply_changes` is being used (not direct write)

## Verification

After configuring your pipeline, verify it's working correctly:

1. **Run initial load** - should see records ingested
2. **Run second time** - should see records re-fetched due to lookback
3. **Check target table** - should have NO duplicates for same `begin_datetime_utc`
4. **Compare forecast values** - should show latest values, not old ones

If you see duplicates or old values persisting, your `sequence_by` is not configured correctly.

