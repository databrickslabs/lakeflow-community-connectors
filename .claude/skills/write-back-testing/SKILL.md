---
name: write-back-testing
description: Implement test utilities that write test data to the source system and validate end-to-end read cycles.
disable-model-invocation: true
---

# Implement Write-Back Testing

## Prerequisites
**⚠️ You must complete "document-write-back-api" first!** This step requires the write-back API documentation.

If you skipped "document-write-back-api", then this step can also be skipped.

## Goal
Implement test utilities that write test data to the source system, then validate your connector correctly reads and ingests that data. This creates a comprehensive end-to-end validation cycle.

**⚠️ IMPORTANT: Only test against non-production environments. Write operations create real data in the source system.**

## What This Step Validates

This step creates a complete validation cycle:
1. **Write**: Test utils generate and insert test data into the source system
2. **Read**: Connector reads back the data using normal ingestion flow
3. **Verify**: Test suite confirms the written data was correctly ingested

This ensures:
- ✅ Incremental sync picks up newly created records
- ✅ Schema correctly captures all written fields
- ✅ Field mappings and transformations work correctly (use the mapping from the write-back API documentation)
- ✅ Cursor field updates and ordering work as expected
- ✅ End-to-end data integrity from write → read → parse
- ✅ Delete synchronization works for tables with `cdc_with_deletes` ingestion type

---

## Implementation Steps

### Step 1: Create Test Utils File

Create `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_test_utils.py` implementing the interface defined in [lakeflow_connect_test_utils.py](../tests/unit/sources/lakeflow_connect_test_utils.py).

**Use the write-back API documentation as your implementation guide:**
- Write endpoints and payload structure from the "Write-Back APIs" section
- Field name transformations from the mapping table
- Required delays from the "Write-Specific Constraints" section
- Required fields from the endpoint documentation

**Key Methods to Implement:**
- `get_source_name()`: Return the connector name
- `list_insertable_tables()`: List tables that support write operations (only those documented in the write-back API section)
- `generate_rows_and_write()`: Generate test data and write to source system using documented endpoints
- `list_deletable_tables()`: List tables that support delete testing — only for tables with `cdc_with_deletes` ingestion type
- `delete_rows()`: Delete records and return deleted row info for verification via `read_table_deletes`

**Reference Implementation:** See `src/databricks/labs/community_connector/sources/hubspot/hubspot_test_utils.py` for a complete working example.

**Implementation Tips:**
- Initialize API client for write operations in `__init__`
- Use the write endpoints and payload structure from the write-back API documentation
- Apply field name mappings when comparing written vs. read data
- Generate unique test data with timestamps/UUIDs to avoid collisions
- Use identifiable prefixes (e.g., `test_`, `generated_`) in test data
- Add delays after writes based on "Required Delays" (e.g., `time.sleep(5-10)`)

### Step 2: Update Test File

Modify `tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py` to import test utils:

```python
# Add this import
from databricks.labs.community_connector.sources.{source_name}.{source_name}_test_utils import LakeflowConnectTestUtils

def test_{source_name}_connector():
    test_suite.LakeflowConnect = LakeflowConnect
    test_suite.LakeflowConnectTestUtils = LakeflowConnectTestUtils  # Add this line
    
    # Rest remains the same...
```
**Reference Implementation:** See `tests/unit/sources/hubspot/test_hubspot_lakeflow_connect.py` for an example implementation.

### Step 3: Run Tests with Write Validation

Use the project virtual environment (Python 3.10+ required):

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py -v
```

**Additional Tests Now Executed:**
- ✅ `test_list_insertable_tables`: Validates insertable tables list
- ✅ `test_write_to_source`: Writes test data and validates success
- ✅ `test_incremental_after_write`: Validates incremental sync picks up new data

**Expected Output:**
```
test_list_insertable_tables PASSED - Found 2 insertable tables
test_write_to_source PASSED - Successfully wrote to 2 tables
test_incremental_after_write PASSED - Incremental sync captured new records
```

### Step 4: Implement Delete Testing (Optional)

For connectors that support `cdc_with_deletes` ingestion type, you can also test delete synchronization.

**When to Implement:**
- The connector has tables with `ingestion_type: "cdc_with_deletes"` in their metadata
- The source API supports deleting records.

**Methods to Implement:**

1. `list_deletable_tables()`: Return tables that support delete testing
   ```python
   def list_deletable_tables(self) -> List[str]:
       # Only return tables with cdc_with_deletes ingestion type
       return ["contacts"]  # Example for HubSpot
   ```

2. `delete_rows()`: Delete records and return info for verification
   ```python
   def delete_rows(self, table_name: str, number_of_rows: int) -> Tuple[bool, List[Dict], Dict[str, str]]:
       # 1. Optionally insert rows first to maintain data balance
       # 2. Fetch existing records to delete
       # 3. Delete records via source API
       # 4. Return (success, deleted_rows, column_mapping)
       #    - deleted_rows: List of dicts with primary key values
       #    - column_mapping: Maps keys in deleted_rows to paths in read_table_deletes results
       #      Use dot notation for nested paths, e.g., {"hs_object_id": "properties.hs_object_id"}
       return True, [{"id": "123"}], {"id": "id"}
   ```

**Additional Tests Now Executed:**
- ✅ `test_deletable_tables`: Validates deletable tables list
- ✅ `test_delete_and_read_deletes`: Deletes records and verifies they appear in `read_table_deletes` results

**Expected Output:**
```
test_deletable_tables PASSED - Found 1 deletable tables
test_delete_and_read_deletes PASSED - Successfully verified delete flow on table 'contacts'
```

## Common Issues & Debugging

**Issue 1: Write Operation Fails (400/403)**
- **Cause**: Insufficient permissions or missing required fields
- **Fix**: 
  - Verify API credentials have write permissions
  - Check source API docs for required fields
  - Validate generated data matches schema requirements

**Issue 2: Incremental Sync Doesn't Pick Up New Data**
- **Cause**: Cursor timestamp mismatch or eventual consistency delay
- **Fix**:
  - Add `time.sleep(5-60)` after write to allow commit
  - Verify cursor field in new records is newer than existing data
  - Check that cursor field is correctly set in generated data

**Issue 3: Column Mapping Errors**
- **Cause**: Source API transforms field names during write/read, or nests fields under a parent object
- **Fix**:
  - Compare written field names vs. read field names in the returned records
  - Update `column_mapping` return value to reflect transformations
  - Example: `{"email": "properties_email"}` if source adds prefix

**Issue 4: Test Data Conflicts**
- **Cause**: Duplicate IDs or unique constraint violations
- **Fix**:
  - Use timestamps or UUIDs in generated IDs
  - Add random suffixes: `f"test_{time.time()}_{random.randint(1000,9999)}"`
  - Prefix all test data fields with identifiable markers

**Issue 5: Deleted Row Not Found in read_table_deletes Results**
- **Cause**: Column mapping doesn't match the structure of records returned by `read_table_deletes`
- **Fix**:
  - Check how your connector transforms records in `read_table_deletes` (e.g., nesting fields under `properties`)
  - Update `column_mapping` in `delete_rows()` to use the correct path
  - Use dot notation for nested paths: `{"hs_object_id": "properties.hs_object_id"}`
  - Add sufficient delay after delete for eventual consistency (e.g., `time.sleep(60)`)

## Best Practices

1. **Use Test/Sandbox Environment**: Never run write tests against production
2. **Unique Test Data**: Include timestamps/UUIDs to avoid collisions
3. **Identifiable Prefixes**: Use `test_`, `generated_`, etc. in data for easy identification
4. **Minimal Data**: Generate only required fields, keep test data simple
5. **Cleanup Consideration**: Some sources may require manual cleanup of test data
6. **Rate Limiting**: Add delays between writes if source API has rate limits
