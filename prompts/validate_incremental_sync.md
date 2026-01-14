# Validate Incremental Sync Behavior

## Goal

Manually validate that a connector's CDC/incremental sync implementation is correct by:
1. Understanding how the offset structure works from the code
2. Verifying the offset matches the cursor field of the last record
3. Testing that passing an offset filters records correctly

## Scope

This guide validates cursor-based incremental sync where:
- Offset contains one or more **cursor fields** (timestamps, IDs)
- Filtering can be tested by setting a midpoint cursor value

**Supported:**
- Single cursor: `{"updated_since": "2024-01-15T00:00:00Z"}`
- Multi-field with cursor: `{"team_id": "...", "updated_since": "2024-01-15T00:00:00Z"}`

**Not supported:**
- Delta tokens (API-provided opaque URLs)
- Opaque pagination tokens (non-cursor-based)

## Prerequisites

- Connector must have CDC tables (tables with `ingestion_type: cdc` or `cdc_with_deletes`)
- Access to connector code and test configuration
- **Run the test suite first** to get table metadata and ingestion types

---

## Step 1: Understand the Offset Structure

### 1.1 Read the Connector Code

First, examine the connector's `read_table` method to understand:

```python
# Look for patterns like:
def read_table(self, table_name: str, start_offset: dict, table_options: dict):
    # How is start_offset used?
    updated_since = start_offset.get("updated_since")  # <-- offset key
    
    # How is the next offset calculated?
    max_updated_at = self._find_max_updated_at(records)
    next_offset = {"updated_since": max_updated_at}  # <-- offset structure
    
    return iter(records), next_offset
```

### 1.2 Document the Offset Structure per table

| Question | Answer |
|----------|--------|
| What are the offset keys? | e.g., `updated_since`, `last_id`, `team_id` |
| Which key is the cursor? | e.g., `updated_since` (the one used for filtering) |
| Single or multi-field? | e.g., single `{"updated_since": "..."}` vs multi `{"team_id": "...", "updated_since": "..."}` |
| How is cursor calculated? | e.g., max of `updated_at` field from records |

---

## Step 2: Run the Test Suite First

Instead of manually reading tables, **run the test suite** which already provides:
- Table metadata (including `ingestion_type` and `cursor_field`)
- Sample records from each table
- Offset information

### 2.1 Run the Test Suite

```bash
cd {repo_root}
python -m pytest sources/{source_name}/test/test_{source_name}_lakeflow_connect.py -v -s
```

### 2.2 Extract Information from Test Output

The test suite output shows:

```
✅ test_read_table_metadata
  Details: {
    "passed_tables": [
      {"table": "ideas", "metadata_keys": ["primary_keys", "cursor_field", "ingestion_type"]},
      ...
    ]
  }

✅ test_read_table
  Details: {
    "passed_tables": [
      {"table": "ideas", "records_sampled": 3, "offset_keys": ["updated_since"], "sample_records": [...]},
      ...
    ]
  }
```

### 2.3 Identify CDC Tables for Validation

**Only validate tables with `ingestion_type: cdc` or `cdc_with_deletes`.**

Skip tables with:
- `ingestion_type: snapshot` - Full refresh, no offset tracking
- `ingestion_type: append` - Append-only, no cursor filtering

From the test output, note:
- Which tables are CDC
- Their `cursor_field` (e.g., `updated_at`)
- The `offset_keys` from `test_read_table` (e.g., `["updated_since"]`)

### 2.4 Verify Offset Structure Matches Code

Compare the test output with what you found in Step 1:

| From Test Suite | From Code Analysis | Match? |
|-----------------|-------------------|--------|
| `offset_keys: ["updated_since"]` | Offset key: `updated_since` | ✅/❌ |
| `cursor_field: updated_at` | Used in `_find_max_updated_at()` | ✅/❌ |

### 2.5 Validate from Test Suite Output

The test_suite output shows the offset returned for each table:

```
✅ test_read_table
  Details: {
    "passed_tables": [
      {
        "table": "ideas",
        "records_sampled": 3,
        "offset_keys": ["updated_since"],     <-- offset key(s) returned
        "sample_records": [...]
      }
    ]
  }
```

**Check:**
1. `offset_keys` is non-empty → offset is being returned ✅
2. For CDC tables, offset should be present

If the test_suite passes and shows `offset_keys`, proceed to Step 3 to verify **filtering actually works**.

### Expected Result

| Check | Expected |
|-------|----------|
| Offset key matches code | ✅ e.g., `updated_since` |
| Offset value equals max cursor from records | ✅ e.g., `2024-01-15T10:30:00Z` |
| Only CDC tables are validated | ✅ Snapshot/Append skipped |

> ⚠️ **IMPORTANT:** If no offset is returned, or passing the offset doesn't change results, 
> there may be an issue with the connector's incremental sync implementation.

---

## Step 3: Test Incremental Filtering

Verify that passing a midpoint cursor value filters records correctly.

### 3.1 Setup

```python
import json
from sources.{source}.{source} import LakeflowConnect

config = json.load(open('sources/{source}/configs/dev_config.json'))
connector = LakeflowConnect(config)
```

### 3.2 Read Without Offset

```python
table = '{table}'
table_options = {}  # Add options if needed

records, offset = connector.read_table(table, {}, table_options)
all_records = list(records)

print(f"Record count: {len(all_records)}")
print(f"Offset: {offset}")

# Get cursor values
cursor_field = 'updated_at'  # From metadata
cursors = sorted([r.get(cursor_field) for r in all_records if r.get(cursor_field)])
print(f"Cursor range: {cursors[0]} to {cursors[-1]}")
```

### 3.3 Pick Midpoint and Test Filtering

```python
# Pick midpoint
midpoint = cursors[len(cursors) // 2]
print(f"Midpoint: {midpoint}")

# Read with midpoint offset
offset_key = list(offset.keys())[0]  # e.g., 'updated_since'
filtered_records, _ = connector.read_table(table, {offset_key: midpoint}, table_options)
filtered = list(filtered_records)

print(f"Filtered count: {len(filtered)} (was {len(all_records)})")

# Validate
violations = [r for r in filtered if r.get(cursor_field) < midpoint]
if len(filtered) < len(all_records) and not violations:
    print("✅ Filtering works correctly")
else:
    print(f"❌ Issues: {len(violations)} records before midpoint")
```

### 3.4 Validate

| Check | Expected |
|-------|----------|
| Filtered count < total count | ✅ e.g., 45 < 100 |
| All returned records have cursor >= midpoint | ✅ |

If both pass, incremental filtering works correctly.

### Expected Result

| Check | Expected |
|-------|----------|
| Filtered count is ~30-70% of total | ✅ |
| All returned records have cursor >= midpoint | ✅ |
| New offset reflects max cursor of filtered records | ✅ |

---

## Step 4: Document Findings

### Validation Report Template

```markdown
## Incremental Sync Validation: {connector_name}

### Offset Structure
- **Offset key:** `{offset_key}`
- **Value type:** {timestamp/id/token}
- **Calculation:** max({cursor_field}) from returned records

### Validation Results

| Table | Ingestion Type | Cursor Field | Offset Key | Offset Matches Max? | Filtering Works? |
|-------|----------------|--------------|------------|---------------------|------------------|
| {table} | CDC | {cursor_field} | {offset_key} | ✅/❌ | ✅/❌/⏭️ |

> **Note:** Filtering test is skipped (⏭️) if offset doesn't match max cursor.

### Code References
- Offset extraction: `{source}.py:L{line}`
- Next offset calculation: `{source}.py:L{line}`
- API filtering: `{source}.py:L{line}`

### Issues Found
- [ ] **BLOCKING:** Offset doesn't match max cursor value (cannot test filtering)
- [ ] Filtering doesn't work (same records returned with offset)
- [ ] Records before offset value are included
```

---

## Common Issues to Check

### 1. Offset Not Updated Correctly

```python
# BAD: Returning empty offset
return iter(records), {}

# GOOD: Returning max cursor as offset
max_cursor = max(r.get(cursor_field) for r in records)
return iter(records), {offset_key: max_cursor}
```

### 2. Offset Not Used for Filtering

```python
# BAD: Ignoring start_offset
def read_table(self, table_name, start_offset, table_options):
    records = self._fetch_all()  # Ignores start_offset!
    return iter(records), {...}

# GOOD: Using start_offset for API filtering
def read_table(self, table_name, start_offset, table_options):
    updated_since = start_offset.get("updated_since")
    records = self._fetch_with_filter(updated_since=updated_since)
    return iter(records), {...}
```

### 3. Offset Key Mismatch

```python
# BAD: Using different keys for read vs write
start_offset.get("since")  # Reading with "since"
return {..., "updated_since": max_val}  # Writing with "updated_since"

# GOOD: Consistent key usage
OFFSET_KEY = "updated_since"
start_offset.get(OFFSET_KEY)
return {..., OFFSET_KEY: max_val}
```

---

## When to Use This Prompt

1. **After running test_suite.py** - Use test output to identify CDC tables
2. **During connector quality review** - Validate offset behavior is correct
3. **When debugging incremental sync issues** - Identify offset mismatches
4. **Before marking a connector as production-ready** - Ensure CDC works correctly

### Workflow

```
1. Run test_suite.py
   └── Get: ingestion_type, cursor_field, offset_keys for each table

2. Identify CDC tables (ingestion_type: cdc or cdc_with_deletes)
   └── Skip: snapshot, append tables

3. Read connector code (Step 1)
   └── Understand: offset key, how it's calculated, server-side filtering

4. Validate from test output (Step 2)
   └── Check offset_keys are returned for CDC tables

5. Test incremental filtering (Step 3)
   └── Use simple Python: connector.read_table() with midpoint offset
```

### Quick Python Reference

```python
import json
from sources.{source}.{source} import LakeflowConnect

# Load config and create connector
config = json.load(open('sources/{source}/configs/dev_config.json'))
connector = LakeflowConnect(config)

# List tables
connector.list_tables()

# Get metadata
connector.read_table_metadata('table_name', {})

# Read table
records, offset = connector.read_table('table_name', {}, {})

# Read with offset (for filtering test)
records, offset = connector.read_table('table_name', {'updated_since': '2024-06-01'}, {})

# Read with table options
records, offset = connector.read_table('table_name', {}, {'max_items': '50'})
```

