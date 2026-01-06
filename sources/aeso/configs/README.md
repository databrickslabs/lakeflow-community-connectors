# AESO Connector Configuration Files

This directory contains configuration files for the AESO LakeFlow connector.

## File Structure

### `dev_config.json`
**Purpose:** Connection-level configuration for development/testing.

**What it contains:**
- `api_key`: AESO API authentication key

**Usage:**
- Used by test suite (`test_aeso_lakeflow_connect.py`)
- For local development and testing
- Should NOT be committed with actual API keys

**Example:**
```json
{
  "api_key": "your-aeso-api-key-here"
}
```

### `dev_table_config.json`
**Purpose:** Table-specific configuration for development/testing.

**What it contains:**
- Table-level parameters for `pool_price` table
- `start_date`: Historical start date
- `lookback_hours`: CDC lookback window
- `batch_size_days`: API batch size
- `rate_limit_delay`: Delay between API calls

**Usage:**
- Used by test suite for table-specific tests
- Defines default table configuration for local testing
- Can be customized per developer/environment

**Example:**
```json
{
  "pool_price": {
    "start_date": "2024-01-01",
    "lookback_hours": "24",
    "batch_size_days": "30",
    "rate_limit_delay": "0.1"
  }
}
```

### `pipeline_spec_example.json`
**Purpose:** Complete pipeline specification examples for different use cases.

**What it contains:**
- Multiple example pipeline configurations
- Minimal, standard, continuous, and historical backfill scenarios
- Configuration notes and best practices

**Usage:**
- Reference for users setting up pipelines
- Copy and adapt for your specific use case
- Documents all available configuration options

## Configuration Levels

The AESO connector uses a two-level configuration approach:

### 1. Connection Level (Unity Catalog Connection)
Set in Databricks Unity Catalog connection:
```json
{
  "api_key": "your-aeso-api-key"
}
```

### 2. Table Level (Pipeline Specification)
Set in `table_configuration` section of pipeline spec:
```json
{
  "table": {
    "source_table": "pool_price",
    "table_configuration": {
      "scd_type": "SCD_TYPE_1",
      "start_date": "2024-01-01",
      "lookback_hours": "24",
      "batch_size_days": "30",
      "rate_limit_delay": "0.1"
    }
  }
}
```

## Configuration Parameters

### Connection Parameters
| Parameter | Required | Description |
|-----------|----------|-------------|
| `api_key` | Yes | AESO API authentication key from https://api.aeso.ca |

### Table Parameters
| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `scd_type` | Yes | - | Must be `"SCD_TYPE_1"` (only supported type) |
| `start_date` | No | 30 days ago | Historical start date (YYYY-MM-DD) for initial load |
| `lookback_hours` | No | 24 | Hours to look back for CDC updates |
| `batch_size_days` | No | 30 | Days per API batch |
| `rate_limit_delay` | No | 0.1 | Seconds between API calls |

## Security Notes

⚠️ **Never commit API keys to version control**

- Use environment variables for production
- Keep `dev_config.json` with empty or placeholder values in the repository
- Add actual API keys only in your local copy
- The `.gitignore` should exclude files with actual credentials

## Examples by Use Case

### Standard Hourly Sync
```json
{
  "table_configuration": {
    "scd_type": "SCD_TYPE_1",
    "lookback_hours": "24"
  }
}
```

### Real-Time Continuous
```json
{
  "table_configuration": {
    "scd_type": "SCD_TYPE_1",
    "lookback_hours": "2",
    "batch_size_days": "7"
  }
}
```

### Historical Backfill
```json
{
  "table_configuration": {
    "scd_type": "SCD_TYPE_1",
    "start_date": "2020-01-01",
    "lookback_hours": "72",
    "batch_size_days": "30"
  }
}
```

## See Also

- Main connector README: `../README.md`
- API documentation: `../aeso_api_doc.md`
- Test suite: `../tests/`

