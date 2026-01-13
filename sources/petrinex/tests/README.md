# Petrinex Connector Tests

## Test Files

### `test_petrinex_structure.py`
Lightweight structural tests that don't require PySpark or external dependencies.
- Validates file existence
- Checks JSON config validity
- Verifies Python syntax
- Ensures connector class structure

## Running Tests

### Quick Structural Tests (No PySpark Required)
```bash
cd /path/to/lakeflow-community-connectors
python sources/petrinex/tests/test_petrinex_structure.py
```

### Full Integration Tests (Requires PySpark)
```bash
cd /path/to/lakeflow-community-connectors
pytest sources/petrinex/tests/test_petrinex_lakeflow_connect.py -v
```

## Test Requirements

**Structural tests:**
- Python 3.7+
- No external dependencies

**Integration tests:**
- Python 3.7+
- PySpark 3.0+
- petrinex package (`pip install petrinex`)

## Note on PySpark Compatibility

PySpark may have compatibility issues on macOS with Apple Silicon. The structural tests are designed to run without PySpark to provide basic validation in all environments.

For full testing with PySpark:
1. Use x86_64 Python on Apple Silicon: `arch -x86_64 python`
2. Or use a Linux environment (Docker, Databricks, etc.)

