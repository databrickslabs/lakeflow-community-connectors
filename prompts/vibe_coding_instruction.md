# Connector Vibe-Coding Prompts

## Workflow Overview

### Core Connector Development (Required)
These steps build a working read-only connector:
1. **Document Source API** - Research and document READ operations only
2. **Set Up Credentials** - Configure authentication for development
3. **Generate Connector Code** - Implement the LakeflowConnect interface
4. **Run Tests and Fix** - Validate read operations work correctly

At this point, you have a **production-ready connector** that can ingest data.

### Write-Back Testing (Optional)
If you need comprehensive end-to-end validation:
1. **Step 5: Document Write-Back APIs** - Research and document WRITE operations (separate from Step 1)
2. **Step 6: Implement Write-Back Testing** - Create test utilities that write data and validate ingestion

**Skip Steps 5-6 if:**
- You want to ship quickly (read-only testing is sufficient)
- Source is read-only
- Only production access available
- Write operations are expensive/risky

### Final Step (Required)
7. **Create Public Documentation** - Generate user-facing README

---

## Step 1: Understanding & Document the Source API

### Goal
Produce a single Markdown document that accurately summarizes a source API for connector implementation. The document must be **complete, source-cited, and implementation-ready** (endpoints, auth, schemas, Python read paths, incremental strategy).

### Output Contract (Strict) 
Create one file named `<source_name>_api_doc.md` under `sources/<source_name>/` directory following the `template/source_api_doc_template.md`. 

**General rules**
- No invented fields. No placeholders unless clearly marked `TBD:` with a short reason.
- If multiple auth methods exist, **choose one** preferred method and **remove others** from the final doc (note rationale in `Known Quirks & Edge Cases`).
- All claims must be supported by the **Research Log** and **Sources**.

### Required Research Steps (do these before writing)
1. **Check user-provided docs** (highest confidence).
2. **Find official API docs** (WebSearch/WebFetch).
3. **Locate reference implementations** (Airbyte OSS—highest non-official confidence; also Singer, dltHub, etc.).
4. **Verify endpoints & schemas** by cross-referencing **at least two** sources (official > reference impl > reputable blogs).
5. **Prefer current/stable APIs**: Always prefer the latest stable API version. Avoid deprecated endpoints even if they're still documented. Check for migration guides and API versioning.
6. **Record everything** in `## Research Log` and list full URLs in `## Sources`.

**Conflict resolution**
- Precedence: **Official docs > Actively maintained OSS (Airbyte) > Other**.
- If unresolved: keep the higher-safety interpretation, mark `TBD:` and add a note in `Known Quirks & Edge Cases`.

**Documentation Requirements:**
- Fill out every section of the documentation template. If any section cannot be completed, add a note to explain.
- Focus on READ operations: endpoints, authentication parameters, object schemas, Python API for reading data, and incremental read strategy.
- Please include all fields in the schema. DO NOT hide any fields using links.
- All information must be backed by official sources or verified or reliable implementations
- **Do NOT document write/create APIs in this step** - those are optional and covered in Step 5 if needed

### Research Log 
Add a table:

| Source Type | URL | Accessed (UTC) | Confidence (High/Med/Low) | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://… | 2025-11-10 | High | Auth params, rate limits |
| Airbyte | https://… | 2025-11-10 | High | Cursor field, pagination |
| Other | https://… | 2025-11-10 | Med | Field descriptions |

### Recommendations
- **NEVER generate API documentation from memory alone** - always research first
- Do **not** include SDK-specific code beyond minimal Python HTTP examples.
- Analyze existing implementations (e.g., Airbyte OSS) to fill in the source API details
- Focus on a single table / object to begin with in the source API documentation if there are many different objects with different APIs. 
- Once the single table ingestion is successful, repeat the steps to include more tables.

### Acceptance Checklist (Reviewer must tick all)
- [ ] All required headings present and in order.
- [ ] Every field in each schema is listed (no omissions).
- [ ] Exactly one authentication method is documented and actionable.
- [ ] Endpoints include params, examples, and pagination details.
- [ ] Incremental strategy defines cursor, order, lookback, and delete handling.
- [ ] Research Log completed; Sources include full URLs.
- [ ] No unverifiable claims; any gaps marked `TBD:` with rationale.

---

## Step 2: Set Up Credentials & Tokens for Source

### Development
Create a file `dev_config.json` under `sources/{source_name}/configs` with required fields based on the source API documentation.
Example:
```json
{
  "user": "YOUR_USER_NAME",
  "password": "YOUR_PASSWORD",
  "token": "YOUR_TOKEN"
}
```

### End-to-end Run: Create UC Connection via UI or API
**Ignore** Fill prompts when building the real agent.

---

## Step 3: Generate the Connector Code in Python

### Goal
Implement the Python connector for **{{source_name}}** that conforms exactly to the interface defined in  
`sources/interface/lakeflow_connect.py`. The implementation should enable reading data from the source API (as documented in Step 1). 

### Implementation Requirements 
- Implement all methods declared in the interface.
- At the beginning of each function, check if the provided `table_name` exists in the list of supported tables. If it does not, raise an explicit exception to inform the user that the table is not supported.
- When returning the schema in the `get_table_schema` function, prefer using StructType over MapType to enforce explicit typing of sub-columns.
- Avoid flattening nested fields when parsing JSON data.
- Prefer using `LongType` over `IntegerType`
- If `ingestion_type` returned from `read_table_metadata` is `cdc`, then `primary_keys` and `cursor_field` are both required.
- In logic of processing records, if a StructType field is absent in the response, assign None as the default value instead of an empty dictionary {}.
- Avoid creating mock objects in the implementation.
- Do not add an extra main function - only implement the defined functions within the LakeflowConnect class. 
- The functions `get_table_schema`, `read_table_metadata`, and `read_table` accept a dictionary argument that may contain additional parameters for customizing how a particular table is read. Using these extra parameters is optional.
- Do not include parameters and options required by individual tables in the connection settings; instead, assume these will be provided through the table options.
- Do not convert the JSON into dictionary based on the `get_table_schema` in `read_table`.
- If a data source provides both a list API and a get API for the same object, always use the list API as the connector is expected to produce a table of objects. Only expand entries by calling the get API when the user explicitly requests this behavior and schema needs to match the read behavior.
- Some objects exist under a parent object, treat the parent object’s identifier(s) as required parameters when listing the child objects. If the user wants these parent parameters to be optional, the correct pattern is: 
  - list the parent objects
  - for each parent object, list the child objects
  - combine the results into a single output table with the parent object identifier as the extra field.
- Refer to `example/example.py` or other connectors under `connector_sources` as examples

---

## Step 4: Run Test and Fix

### Goal
Validate the generated connector for **{{source_name}}** by executing the provided test suite or notebook, diagnosing failures, and applying minimal, targeted fixes until all tests pass. 

**If using IDE like cursor**
- Create a `test_{source_name}_lakeflow_connect.py` under `sources/{source_name}/test/` directory. 
- Use `test/test_suite.py` to run test and follow `sources/example/test/test_example_lakeflow_connect.py` or other sources as an example.
- Please use this option from `sources/{source_name}/configs/dev_configs.json` to initialize for testing.
- Run test: `pytest sources/{source_name}/test/test_{source_name}_lakeflow_connect.py -v`
- (Optional) Generate code to write to the source system based on the source API documentation.
- Run more tests.

**Notes**
- This step is more interactive. Based on testing 
results, we need to make various adjustments
- For external users, please remove the `dev_config.json` after this step.
- Avoid mocking data in tests. Config files will be supplied to enable connections to an actual instance.

---

## Step 5: Document Write-Back APIs (Optional)

### Goal
Research and document the write/create APIs for the source system to enable write-back testing. This step is **completely separate** from the core connector implementation and should only be done if comprehensive end-to-end validation is needed.

### When to Do This Step

**Skip this step if:**
- ❌ Source API is read-only (no create/insert endpoints)
- ❌ Only production environment available (too risky)
- ❌ Write permissions unavailable or expensive to obtain
- ❌ You want to ship the connector quickly (read-only validation is sufficient)
- ❌ Write operations have side effects (notifications, triggers, etc.)

**Do this step if:**
- ✅ Source API supports write operations
- ✅ You have write permissions and test/sandbox environment
- ✅ You want end-to-end validation of write → read → incremental sync cycle
- ✅ You have time for comprehensive testing

### Input
- The `<source_name>_api_doc.md` created in Step 1 (for reference)
- Access to source API documentation

### Output
Add a new section `## Write-Back APIs (For Testing Only)` to your existing `sources/<source_name>/<source_name>_api_doc.md` file.

### Documentation Template for Write-Back APIs

Add this section to your API doc:

````markdown
## Write-Back APIs (For Testing Only)

**⚠️ WARNING: These APIs are documented solely for test data generation. They are NOT part of the connector's read functionality.**

### Purpose
These write endpoints enable automated testing by:
1. Creating test data in the source system
2. Validating that incremental sync picks up newly created records
3. Verifying field mappings and schema correctness end-to-end

### Write Endpoints

#### Create [Object Type]
- **Method**: POST/PUT
- **Endpoint**: `https://api.example.com/v1/objects`
- **Authentication**: Same as read operations / Additional scopes needed
- **Required Fields**: List all required fields for creating a minimal valid record
- **Example Payload**:
```json
{
  "field1": "value1",
  "field2": "value2"
}
```
- **Response**: Document what the API returns (ID, created timestamp, etc.)

### Field Name Transformations

Document any differences between write and read field names:

| Write Field Name | Read Field Name | Notes |
|------------------|-----------------|-------|
| `email` | `properties_email` | API adds `properties_` prefix on read |
| `createdAt` | `created_at` | Different casing convention |

If no transformations exist, state: "Field names are consistent between write and read operations."

### Write-Specific Constraints

- **Rate Limits**: Document write-specific rate limits (if different from read)
- **Eventual Consistency**: Note any delays between write and read visibility
- **Required Delays**: Recommend wait time after writes (e.g., "Wait 5-10 seconds after write before reading")
- **Unique Constraints**: Document fields that must be unique (to guide test data generation)
- **Test Environment**: Confirm sandbox/test environment availability and how to access it

### Research Log for Write APIs

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | ... | YYYY-MM-DD | High | Write endpoints and payload structure |
| Reference Impl | ... | YYYY-MM-DD | Med | Field transformations |
```

### Research Requirements

Follow the same rigorous research process as Step 1:
1. **Check official API documentation** for write/create endpoints
2. **Find reference implementations** (Airbyte test utilities, Singer tap tests, etc.)
3. **Cross-reference at least two sources** for payload structure and required fields
4. **Test if possible**: If you have sandbox access, verify one write operation works
5. **Document everything** in Research Log with full URLs

### Validation Checklist

- [ ] At least one write endpoint documented with complete payload structure
- [ ] All required fields for write operations identified
- [ ] Field name transformations (if any) documented in mapping table
- [ ] Rate limits and constraints noted
- [ ] Eventual consistency delays documented (if applicable)
- [ ] Research log completed with sources

````
---

## Step 6: Implement Write-Back Testing (Optional)

### Prerequisites
**⚠️ You must complete Step 5 first!** This step requires the write-back API documentation created in Step 5.

If you skipped Step 5, then this step can also be skipped.

### Goal
Implement test utilities that write test data to the source system, then validate your connector correctly reads and ingests that data. This creates a comprehensive end-to-end validation cycle.

**⚠️ IMPORTANT: Only test against non-production environments. Write operations create real data in the source system.**

### What This Step Validates

This step creates a complete validation cycle:
1. **Write**: Test utils generate and insert test data into the source system
2. **Read**: Connector reads back the data using normal ingestion flow
3. **Verify**: Test suite confirms the written data was correctly ingested

This ensures:
- ✅ Incremental sync picks up newly created records
- ✅ Schema correctly captures all written fields
- ✅ Field mappings and transformations work correctly (use the mapping from Step 5)
- ✅ Cursor field updates and ordering work as expected
- ✅ End-to-end data integrity from write → read → parse

---

### Implementation Steps

**Step 1: Create Test Utils File**

Create `sources/{source_name}/{source_name}_test_utils.py` implementing the interface defined in `tests/lakeflow_connect_test_utils.py`.

**Use Step 5 documentation as your implementation guide:**
- Write endpoints and payload structure from the "Write-Back APIs" section
- Field name transformations from the mapping table
- Required delays from the "Write-Specific Constraints" section
- Required fields from the endpoint documentation

**Key Methods to Implement:**
- `get_source_name()`: Return the connector name
- `list_insertable_tables()`: List tables that support write operations (only those documented in Step 5)
- `generate_rows_and_write()`: Generate test data and write to source system using documented endpoints

**Reference Implementation:** See `sources/hubspot/hubspot_test_utils.py` for a complete working example.

**Implementation Tips:**
- Initialize API client for write operations in `__init__`
- Use the write endpoints and payload structure from Step 5 documentation
- Apply field name mappings from Step 5 when comparing written vs. read data
- Generate unique test data with timestamps/UUIDs to avoid collisions
- Use identifiable prefixes (e.g., `test_`, `generated_`) in test data
- Add delays after writes based on Step 5 "Required Delays" (e.g., `time.sleep(5-10)`)

---

**Step 2: Update Test File**

Modify `sources/{source_name}/test/test_{source_name}_lakeflow_connect.py` to import test utils:

```python
# Add this import
from sources.{source_name}.{source_name}_test_utils import LakeflowConnectTestUtils

def test_{source_name}_connector():
    test_suite.LakeflowConnect = LakeflowConnect
    test_suite.LakeflowConnectTestUtils = LakeflowConnectTestUtils  # Add this line
    
    # Rest remains the same...
```

**Reference Implementation:** See `sources/hubspot/test/test_hubspot_lakeflow_connect.py` for a example implementation.

---

**Step 3: Run Tests with Write Validation**

```bash
pytest sources/{source_name}/test/test_{source_name}_lakeflow_connect.py -v
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

---

### Common Issues & Debugging

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
- **Cause**: Source API transforms field names during write/read
- **Fix**:
  - Compare written field names vs. read field names
  - Update `column_mapping` return value to reflect transformations
  - Example: `{"email": "properties_email"}` if source adds prefix

**Issue 4: Test Data Conflicts**
- **Cause**: Duplicate IDs or unique constraint violations
- **Fix**:
  - Use timestamps or UUIDs in generated IDs
  - Add random suffixes: `f"test_{time.time()}_{random.randint(1000,9999)}"`
  - Prefix all test data fields with identifiable markers

---

### Best Practices

1. **Use Test/Sandbox Environment**: Never run write tests against production
2. **Unique Test Data**: Include timestamps/UUIDs to avoid collisions
3. **Identifiable Prefixes**: Use `test_`, `generated_`, etc. in data for easy identification
4. **Minimal Data**: Generate only required fields, keep test data simple
5. **Cleanup Consideration**: Some sources may require manual cleanup of test data
6. **Rate Limiting**: Add delays between writes if source API has rate limits

---

## Step 7: Create a Public Connector Documentation

### Goal
Generate the **public-facing documentation** for the **{{source_name}}** connector, targeted at end users.

### Output Contract 
Produce a Markdown file based on the standard template `template/community_connector_doc_template.md` at `sources/{{source_name}}/README.md`.

### Documentation Requirements
- Please use the code implementation as the source of truth.
- Use the source API documentation to cover anything missing.
- Always include a section about how to configure the parameters needed to connect to the source system.
- AVOID mentioning internal implementation terms such as function or argument names from the `LakeflowConnect`.

## Step 7.x (Temporary)
As a temporary workaround for current compatibility issues with Python Data Source and SDP, please run `scripts/merge_python_source.py` on your newly developed source. This will combine the source implementation into a single file.

