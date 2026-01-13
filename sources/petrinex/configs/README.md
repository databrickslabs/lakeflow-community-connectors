# Petrinex Connector Configuration

This directory contains configuration examples for the Petrinex LakeflowConnect connector.

## File Structure

```
configs/
├── README.md                      # This file
├── dev_config.json                # Development connection configuration
├── dev_table_config.json          # Development table configuration
└── pipeline_spec_example.json     # Pipeline specification examples
```

## Configuration Levels

The Petrinex connector uses a two-level configuration approach:

### 1. Connection Configuration (`dev_config.json`)

Connection-level settings that apply to all tables. For Petrinex, no authentication is required:

```json
{
  "_comment": "Petrinex requires no authentication - this file is kept for consistency"
}
```

### 2. Table Configuration (`dev_table_config.json` or `pipeline_spec`)

Table-specific settings that control data loading behavior:

```json
{
  "volumetrics": {
    "from_date": "2024-01",
    "updated_after": "2025-01-01",
    "request_timeout_s": "60"
  }
}
```

## Configuration Parameters

### Connection Level

**No parameters required** - Petrinex PublicData is freely accessible without authentication.

### Table Level

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `from_date` | string | No | 6 months ago | Production month to start from (YYYY-MM format). Use for initial historical loads. |
| `updated_after` | string | No | Calculated | Load only files updated after this date (YYYY-MM-DD format). Use for incremental updates. |
| `request_timeout_s` | integer | No | 60 | HTTP request timeout in seconds. Increase for slow networks. |

### Pipeline Specification

The `table_configuration` section within the pipeline spec supports:

- `scd_type`: "SCD_TYPE_1" (default) or "SCD_TYPE_2"
- All table-level parameters above

Example:

```json
{
  "connection_name": "petrinex_connector",
  "objects": [
    {
      "table": {
        "source_table": "volumetrics",
        "destination_catalog": "main",
        "destination_schema": "energy",
        "destination_table": "petrinex_volumetrics",
        "table_configuration": {
          "scd_type": "SCD_TYPE_1",
          "from_date": "2023-01",
          "request_timeout_s": "120"
        }
      }
    }
  ]
}
```

## Usage Examples

### Minimal Configuration (Defaults)

```json
{
  "connection_name": "petrinex_connector",
  "objects": [
    {
      "table": {
        "source_table": "volumetrics"
      }
    }
  ]
}
```

- Uses last 6 months as default historical range
- SCD Type 1 (latest overwrites)
- Standard timeout (60s)

### Historical Backfill

```json
{
  "table_configuration": {
    "from_date": "2020-01",
    "request_timeout_s": "120"
  }
}
```

- Loads all production months from January 2020 onwards
- Extended timeout for large downloads

### Incremental Updates Only

```json
{
  "table_configuration": {
    "updated_after": "2025-01-01"
  }
}
```

- Loads only files Petrinex updated after Jan 1, 2025
- Skips historical backfill

## Configuration Tips

### Memory Configuration

Petrinex files are large (GB-scale). For historical loads:

```python
spark.conf.set("spark.driver.memory", "32g")
```

### Schedule Recommendations

- **Daily**: Recommended for production workloads
- **Weekly**: Cost-optimized for non-urgent needs
- **Continuous**: Supported but rarely needed (Petrinex updates monthly)

### Date Filtering Strategy

**Choose ONE approach:**

1. **Initial load + incremental** (recommended):
   - First run: Use `from_date` for historical data
   - Subsequent runs: Connector automatically uses `file_updated_ts` watermark

2. **Incremental only** (for existing datasets):
   - Use `updated_after` to skip historical backfill
   - Only processes files updated since the specified date

## Automatic Configuration

The following are automatically configured by the connector:

- `primary_keys`: Composite key (BA_ID, ProductionMonth, Product, Activity, Source)
- `cursor_field`: `file_updated_ts` (tracks which files have been processed)
- `sequence_by`: `ingestion_time` (determines newest record for SCD Type 1)
- `ingestion_type`: `cdc` (change data capture with upserts)

**You do not need to specify these in your configuration.**

## Security Notes

- **No credentials required**: Petrinex PublicData is freely accessible
- **Unity Catalog connections**: Create connections without authentication
- **Data privacy**: Petrinex data is public regulatory data

## Testing Configuration

For local development and testing:

1. Edit `dev_config.json` (if needed for custom jurisdictions)
2. Edit `dev_table_config.json` with your test parameters
3. Run structural tests: `python tests/test_petrinex_structure.py`
4. Run integration tests: `pytest tests/test_petrinex_lakeflow_connect.py`

## Example Pipeline Specs

See `pipeline_spec_example.json` for complete examples:

- **minimal_example**: Bare minimum configuration
- **standard_example**: Typical production setup
- **incremental_only**: Skip historical data
- **historical_backfill**: Load years of history
- **scd_type2_example**: Track historical changes

## Troubleshooting

| Issue | Configuration Fix |
|-------|-------------------|
| Timeout errors | Increase `request_timeout_s` to 120 or higher |
| Memory errors | Increase driver memory in cluster configuration |
| Missing data | Check `from_date` and Petrinex website availability |
| Slow performance | Reduce historical range or schedule less frequently |

## Resources

- **Connector README**: `../README.md`
- **API Documentation**: `../petrinex_api_doc.md`
- **Connector Code**: `../petrinex.py`
- **Tests**: `../tests/`
- **Petrinex Website**: https://www.petrinex.ca
- **Python API**: https://github.com/guanjieshen/petrinex-python-api

