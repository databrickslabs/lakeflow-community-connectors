# AESO SCD Type 1 Setup - Quick Reference

## Automatic Configuration

The AESO connector is now fully configured for SCD Type 1 merges **automatically**. The pipeline framework reads `sequence_by: "ingestion_time"` from the connector's metadata, so you don't need to specify it in your pipeline configuration.

## Simple Pipeline Configuration

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

That's it! The `sequence_by` field is automatically configured.

## What Changed in the Connector

### 1. Schema (aeso.py)
- ❌ Removed: `begin_datetime_mpt` (Mountain Time - caused timezone confusion)
- ✅ Added: `ingestion_time` (UTC timestamp of when record was fetched)

### 2. Metadata (aeso.py)
- ✅ Added: `"sequence_by": "ingestion_time"` to metadata
- Note: This is for documentation only - pipeline doesn't read it automatically

### 3. Data Collection (aeso.py)
- ✅ Captures `ingestion_time = datetime.utcnow()` once per batch
- ✅ Adds `ingestion_time` to every record

## How SCD Type 1 Works Now

```
First Run (10:00 AM):
  begin_datetime_utc: "2026-01-05 20:00"  ← Primary Key
  forecast_pool_price: 792.33
  ingestion_time: "2026-01-06 10:00:00"   ← Sequence field

Second Run (10:15 AM):
  begin_datetime_utc: "2026-01-05 20:00"  ← Same Primary Key
  forecast_pool_price: 800.00             ← Updated value
  ingestion_time: "2026-01-06 10:15:00"   ← Later timestamp

Result: Second record OVERWRITES first (SCD Type 1)
```

## Verification Checklist

After updating your pipeline configuration:

- [ ] Pipeline spec includes `"sequence_by": "ingestion_time"`
- [ ] Pipeline spec includes `"scd_type": "SCD_TYPE_1"`
- [ ] Run pipeline twice with lookback enabled
- [ ] Check target table - should have NO duplicates for same `begin_datetime_utc`
- [ ] Verify forecast values show latest data, not old values

## Files Modified

### Connector Files (sources/aeso/)
- ✅ `aeso.py` - Schema, metadata, data collection
- ✅ `_generated_aeso_python_source.py` - Auto-regenerated
- ✅ `README.md` - Added warning about pipeline config
- ✅ `PIPELINE_CONFIG.md` - NEW: Complete configuration guide
- ✅ `configs/pipeline_spec_example.json` - NEW: Example specs
- ✅ `tests/test_scd_type1_merge.py` - NEW: Unit tests

### Pipeline Files
- ✅ `pipeline/ingestion_pipeline.py` - ENHANCED
  - Now reads `sequence_by` from connector metadata automatically
  - Benefits all connectors, not just AESO
  - Spec can still override if needed

## Quick Reference: Pipeline Fields

| Field | Value | Required | Notes |
|-------|-------|----------|-------|
| `source_table` | `"pool_price"` | Yes | The AESO table name |
| `scd_type` | `"SCD_TYPE_1"` | Yes | Latest value wins |
| `sequence_by` | - | **No** | Auto-configured from connector metadata |

## Need Help?

See [PIPELINE_CONFIG.md](PIPELINE_CONFIG.md) for:
- Complete configuration examples
- Troubleshooting guide
- Use case specific configs (15-min, hourly, daily)

