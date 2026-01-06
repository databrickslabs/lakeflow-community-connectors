# AESO Connector Tests

Tests for the AESO LakeFlow connector.

## Quick Start

### Local Testing (without PySpark)
```bash
pytest sources/aeso/tests/test_aeso_structure.py -v
```
Validates connector structure, configuration, and code syntax. No API key required.

### Full Integration Testing (requires Databricks)
```bash
pytest sources/aeso/tests/test_aeso_lakeflow_connect.py -v
```
Requires valid API key in `configs/dev_config.json`. Run in Databricks environment with PySpark.

## Configuration

Tests use two config files:
- `configs/dev_config.json` - API credentials (`api_key`)
- `configs/dev_table_config.json` - Table options (`start_date`, `lookback_hours`, etc.)
