---
name: write-back-testing
description: Implement test utilities that write test data to the source system and validate end-to-end read cycles.
disable-model-invocation: true
---

# Implement Write-Back Testing

## Prerequisites
This step requires the write-back API documentation for the source system (typically found at `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md`). If no write-back API doc is available, this step can be skipped.

## Goal
Implement test utilities that write test data to the source system, then validate your connector correctly reads and ingests that data. This creates a complete write → read → verify cycle.

**Only test against non-production environments. Write operations create real data in the source system.**

---

## Implementation Steps

### Step 1: Create Test Utils File

Create `tests/unit/sources/{source_name}/{source_name}_test_utils.py` implementing the interface defined in `tests/unit/sources/lakeflow_connect_test_utils.py`.

The base class `LakeflowConnectWriteTestUtils` provides default no-op implementations for every method (returning empty lists and `(False, [], {})`). You only need to override the methods your source supports.

**Use the write-back API documentation as your implementation guide:**
- Write endpoints and payload structure from the "Write-Back APIs" section
- Field name transformations from the mapping table
- Required delays from the "Write-Specific Constraints" section
- Required fields from the endpoint documentation

**Key Methods to Implement:**
- `list_insertable_tables()`: Return table names that support write operations (only those documented in the write-back API section)
- `generate_rows_and_write(table_name, number_of_rows)`: Generate test data and write to the source system using documented endpoints. Returns `(success, written_rows, column_mapping)`
- `list_deletable_tables()`: Return table names that support delete testing — only for tables with `cdc_with_deletes` ingestion type
- `delete_rows(table_name, number_of_rows)`: Delete records and return deleted row info for verification via `read_table_deletes`. Returns `(success, deleted_rows, column_mapping)`

**Reference Implementation:** See `tests/unit/sources/example/example_test_utils.py` for a complete working example.

**The `column_mapping` Return Value:**

The third element of the tuple returned by `generate_rows_and_write` and `delete_rows` maps field names in `written_rows`/`deleted_rows` to field paths in records returned by the connector's `read_table` / `read_table_deletes`. The test suite uses this to verify written values appear correctly when read back.

Common patterns:
- **Names match**: `{"order_id": "order_id"}`
- **Nested read fields**: `{"email": "properties.email"}` — source nests fields under a parent object (e.g., HubSpot)
- **Field renaming**: `{"language": "user_language"}` — connector normalizes the field name (e.g., Qualtrics `userLanguage` → `user_language`)

Use dot notation for nested paths. The test suite resolves them by traversing nested dicts.

**Implementation Tips:**
- Initialize your API client in `__init__` using the `options` dict (same credentials passed to the connector)
- Generate unique test data with timestamps/UUIDs to avoid collisions; use identifiable prefixes (e.g., `test_`, `generated_`)
- Add delays after writes for eventual consistency (e.g., `time.sleep(15)` for Qualtrics, `time.sleep(60)` for HubSpot)
- Include retry logic for transient errors (429, 500, 503)

### Step 2: Update Test File

Modify `tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py` to register your test utils class by setting the `test_utils_class` attribute:

```python
from databricks.labs.community_connector.sources.{source_name}.{source_name} import {SourceName}LakeflowConnect
from tests.unit.sources.{source_name}.{source_name}_test_utils import LakeflowConnectWriteTestUtils
from tests.unit.sources.test_suite import LakeflowConnectTests


class Test{SourceName}Connector(LakeflowConnectTests):
    connector_class = {SourceName}LakeflowConnect
    test_utils_class = LakeflowConnectWriteTestUtils
```

**Reference:** See `tests/unit/sources/example/test_example_lakeflow_connect.py`.

### Step 3: Run Tests

```bash
source .venv/bin/activate   # use existing venv, or create with: python3.10 -m venv .venv
pip install -e ".[dev]"
pytest tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py -v
```

When `test_utils_class` is set, the test suite automatically runs these additional tests (skipped otherwise):

| Test | What it does |
|------|-------------|
| `test_list_insertable_tables` | Validates that every insertable table also appears in `list_tables()` |
| `test_write_to_source` | Calls `generate_rows_and_write` for each insertable table, verifies the 3-tuple return shape, `success=True`, non-empty rows, and non-empty `column_mapping` |
| `test_incremental_after_write` | Does an initial read to capture the offset, writes 1 row, creates a **fresh connector instance**, reads from the captured offset, and verifies the written row appears using `column_mapping` |

### Step 4: Implement Delete Testing (Optional)

For connectors with `cdc_with_deletes` tables whose source API supports deleting records.

**Methods to Override:**

1. `list_deletable_tables()`: Return tables that support delete testing. Every table returned must have `ingestion_type: "cdc_with_deletes"` — the test suite validates this.

2. `delete_rows(table_name, number_of_rows)`: Recommended approach:
   - Insert rows first (via `generate_rows_and_write`) to maintain data balance
   - Fetch existing records and delete them via the source API
   - Wait for eventual consistency
   - Return `(success, deleted_rows, column_mapping)` where `deleted_rows` contains primary key values

   ```python
   def delete_rows(self, table_name: str, number_of_rows: int) -> Tuple[bool, List[Dict], Dict[str, str]]:
       self.generate_rows_and_write(table_name, number_of_rows)
       # Fetch and delete existing records via source API
       time.sleep(60)
       return True, [{"id": "123"}], {"id": "properties.id"}
   ```

**Tests added:**

| Test | What it does |
|------|-------------|
| `test_list_deletable_tables` | Validates that every deletable table appears in `list_tables()` and has `ingestion_type: "cdc_with_deletes"` |
| `test_delete_and_read_deletes` | Deletes 1 row from the **first** deletable table, then verifies it appears in `read_table_deletes` results |

## Common Issues & Debugging

**Write Operation Fails (400/403)**
- Verify API credentials have write permissions
- Check source API docs for required fields
- Validate generated data matches schema requirements

**Incremental Sync Doesn't Pick Up New Data**
- Add `time.sleep()` after write to allow the source to commit (5–60s depending on the source)
- The test suite creates a fresh connector instance after writing, so connectors that cap cursors at init time will observe the new data
- Verify cursor field in new records is newer than existing data

**Column Mapping Errors (written row not found in read/delete results)**
- Compare written field names vs. read field names in the returned records
- Update `column_mapping` to reflect transformations (nesting, renaming)
- Use dot notation for nested paths: `{"email": "properties.email"}`
- If the connector normalizes names (e.g., camelCase to snake_case), map accordingly: `{"language": "user_language"}`
- For delete testing, add sufficient delay after delete for eventual consistency

**Test Data Conflicts**
- Use `uuid.uuid4().hex[:8]` in generated IDs to avoid collisions
- Prefix test data fields with identifiable markers (e.g., `test_`, `generated_`)
