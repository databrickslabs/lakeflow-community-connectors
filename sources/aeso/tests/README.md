# AESO Connector Tests

This directory contains tests for the AESO LakeFlow connector.

## Known Issues

⚠️ **PySpark Segfault on macOS**: The full test suite may encounter segmentation faults on certain macOS environments due to a numpy/PySpark compatibility issue. If you encounter this:

1. Run the basic structure test instead: `python sources/aeso/tests/test_basic_structure.py`
2. Test the connector directly in a Databricks notebook or cluster where PySpark is properly configured
3. The issue is environment-specific and does not affect production deployment

## Test Files

### `test_basic_structure.py`
**Purpose:** Lightweight validation of connector structure without PySpark dependencies.

**What it tests:**
- Module imports (without PySpark)
- Configuration files exist
- Requirements.txt has necessary dependencies
- README structure and key sections
- API documentation completeness

**How to run:**
```bash
cd /Users/guanjie.shen/Repos/lakeflow-community-connectors
python sources/aeso/tests/test_basic_structure.py
```

**Requirements:**
- No API key needed
- No PySpark needed (works in any Python environment)

**Use this test when:**
- pytest encounters segmentation faults
- You want to verify structure without full dependencies
- You're doing quick validation of file organization

### `test_aeso_lakeflow_connect.py`
**Purpose:** Comprehensive connector validation using the LakeFlow test suite framework.

**What it tests:**
- Connector initialization with valid/invalid configs
- `list_tables()` method
- `get_table_schema()` for all supported tables
- `read_table_metadata()` for primary keys, cursor fields, and ingestion type
- Basic data reading and schema compliance

**How to run:**
```bash
cd /Users/guanjie.shen/Repos/lakeflow-community-connectors
pytest sources/aeso/tests/test_aeso_lakeflow_connect.py -v
```

**Requirements:**
- Valid API key in `sources/aeso/configs/dev_config.json`
- Access to the LakeFlow test suite (`tests/test_suite.py`)

### `test_scd_type1_merge.py`
**Purpose:** Verify SCD Type 1 merge configuration for late-arriving data.

**What it tests:**
- Metadata includes `sequence_by: "ingestion_time"`
- Schema includes both `begin_datetime_utc` (primary key) and `ingestion_time` (sequence field)
- `ingestion_time` field is non-nullable
- Correct ingestion type (`cdc`)

**How to run:**
```bash
cd /Users/guanjie.shen/Repos/lakeflow-community-connectors
pytest sources/aeso/tests/test_scd_type1_merge.py -v

# Or run directly as a script
python sources/aeso/tests/test_scd_type1_merge.py
```

**Requirements:**
- No API key needed (tests only metadata and schema)

## Running All Tests

**Preferred method** (if pytest works):
```bash
cd /Users/guanjie.shen/Repos/lakeflow-community-connectors
pytest sources/aeso/tests/ -v
```

**Fallback method** (if pytest seg faults):
```bash
cd /Users/guanjie.shen/Repos/lakeflow-community-connectors
python sources/aeso/tests/test_basic_structure.py
```

**Production validation** (most reliable):
- Deploy connector to Databricks
- Test in a Databricks notebook with proper PySpark environment
- Run actual data ingestion with a valid API key

## Test Organization

Tests are organized following Python conventions:
- Test files start with `test_`
- Test functions start with `test_`
- Tests directory is `tests/` (plural)
- Package includes `__init__.py`

