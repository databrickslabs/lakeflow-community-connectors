# Palantir Connector Tests

This directory contains tests for the Palantir Foundry connector using the shared LakeflowConnect test suite.

## Setup

### 1. Create Local Configuration

Copy the example config and add your real credentials:

```bash
cd tests/unit/sources/palantir/configs
cp dev_config.json.example dev_config.json
```

### 2. Edit `dev_config.json`

Add your Palantir credentials:

```json
{
  "token": "YOUR_PALANTIR_BEARER_TOKEN",
  "hostname": "yourcompany.palantirfoundry.com",
  "ontology_api_name": "ontology-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

**How to get these values:**

- **token**: Generate from Palantir Foundry → Account Settings → Tokens
- **hostname**: Your Palantir instance domain (visible in browser URL)
- **ontology_api_name**: Get from `/api/v2/ontologies` endpoint

### 3. (Optional) Configure Table Options

Edit `dev_table_config.json` to customize per-table options:

```json
{
  "ExampleFlight": {
    "cursor_field": "departureTimestamp",
    "page_size": "10"
  },
  "ExampleAircraft": {
    "page_size": "10"
  }
}
```

## Running Tests

### Run All Tests

```bash
# From repository root
pytest tests/unit/sources/palantir/test_palantir_lakeflow_connect.py -v
```

### Run Directly (for debugging)

```bash
cd tests/unit/sources/palantir
python test_palantir_lakeflow_connect.py
```

## What Gets Tested

The test suite validates:

1. ✅ **Initialization** - Connector instantiates with config
2. ✅ **List Tables** - Returns list of object types
3. ✅ **Get Table Schema** - Schema discovery for all tables
4. ✅ **Read Table Metadata** - Primary keys, cursor fields, ingestion types
5. ✅ **Read Table** - Data reading with pagination
6. ✅ **Read Table Deletes** - Delete tracking (if implemented)

## Expected Output

```
==================================================
LAKEFLOW CONNECT TEST REPORT
==================================================
Connector Class: PalantirLakeflowConnect
Timestamp: 2026-03-09T00:50:12.123456

SUMMARY:
  Total Tests: 6
  Passed: 6
  Failed: 0
  Errors: 0
  Success Rate: 100.0%

TEST RESULTS:
--------------------------------------------------
✅ test_initialization
  Status: PASSED
  Message: Connector initialized successfully

✅ test_list_tables
  Status: PASSED
  Message: Successfully retrieved 4 tables
  Details: {
    "tables": [
      "ExampleFlight",
      "ExampleAircraft",
      "ExampleAirport",
      "ExampleRouteAlert"
    ]
  }

✅ test_get_table_schema
  Status: PASSED
  Message: Successfully retrieved table schema for all 4 tables

✅ test_read_table_metadata
  Status: PASSED
  Message: Successfully retrieved table metadata for all 4 tables

✅ test_read_table
  Status: PASSED
  Message: Successfully read all 4 tables

✅ test_read_table_deletes
  Status: PASSED
  Message: Skipped: connector does not implement read_table_deletes
```

## Troubleshooting

### 403 Forbidden Error

If you see `403 Forbidden`, your token may have IP restrictions:
1. Generate a new token in Palantir **without IP restrictions**
2. Update `dev_config.json` with the new token

### Import Errors

If you see import errors, ensure you're running from the repository root:
```bash
cd /path/to/lakeflow-community-connectors
pytest tests/unit/sources/palantir/test_palantir_lakeflow_connect.py -v
```

### No Tables Found

If `list_tables` returns empty:
1. Verify `ontology_api_name` is correct
2. Check that your token has read permissions on the ontology
3. Test the API directly:
   ```bash
   curl -H "Authorization: Bearer $TOKEN" \
        "https://$HOSTNAME/api/v2/ontologies/$ONTOLOGY/objectTypes"
   ```

## Security Notes

- ⚠️ **Never commit `dev_config.json`** - it contains sensitive credentials
- ✅ The `.gitignore` file prevents accidental commits
- ✅ Use `dev_config.json.example` as a template for documentation
