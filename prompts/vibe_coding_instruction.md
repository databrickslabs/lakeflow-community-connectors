# Connector Vibe-Coding Prompts

## Step 1: Understanding & Document the Source API

### Goal
Produce a single Markdown document that accurately summarizes a source API for connector implementation. The document must be **complete, source-cited, and implementation-ready** (endpoints, auth, schemas, Python read paths, incremental strategy).

### Output Contract (Strict) 
Create one file named `<source_name>_api_doc.md` under `sources/<source_name>/` directory following the `source_api_doc_template.md`. 

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
- Focus on endpoints, authentication parameters, object schemas, Python API for reading data, and incremental read strategy.
- Please include all fields in the schema. DO NOT hide any fields using links.
- All information must be backed by official sources or verified or reliable implementations

### Optional Research Step: Write-Back Functionality

**Ask the user:** Do you want to implement write-back to source test functionality for end-to-end validation?

**Note to mention to user:** Write-back testing validates the complete write → read cycle and ensures incremental sync captures newly created records. However, it requires write permissions, a test/sandbox environment, and creates real data in the source system.gti Skip if the source is read-only, only production access is available, or write operations are too risky/expensive.

If **YES**, research and document the following:
1. **Write/Create APIs**: Document POST/PUT endpoints for creating records in the source system
   - Required fields and payload structure for creating test data
   - Authentication requirements for write operations
   - Any required permissions or scopes beyond read access
2. **Field Transformation**: Identify if field names differ between write and read operations
   - Example: Writing `email` but reading back as `properties_email`
   - Document any transformations in the API doc
3. **Rate Limits & Constraints**: Note any write-specific rate limits, quotas, or restrictions
4. **Test Environment**: Confirm availability of sandbox/test environment for safe write testing

**Document in a new section** `## Write-Back APIs (Optional)` in your `<source_name>_api_doc.md` if implementing write testing.

If **NO** or if write APIs are not available, skip this optional step.

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

**If using chatbot and need to run notebook**
TODO: UPDATE THIS.
- ~~Import `test/run_test_notebook.py` and update the second cell with the `connection_name` you created in step 2.~~
- ~~Run the notebook.~~
- ~~If you encounter any errors, you can provide them to the chatbot to help debug and fix the generated code.~~


**Notes**
- This step is more interactive. Based on testing 
results, we need to make various adjustments
- For external users, please remove the `dev_config.json` after this step.
- Avoid mocking data in tests. Config files will be supplied to enable connections to an actual instance.

---

## Step 5: Write-Back Testing (Optional)

### Goal
Validate end-to-end connector functionality by testing write operations followed by incremental reads. This ensures your connector can correctly ingest data that was just created in the source system.

**⚠️ IMPORTANT: Only test against non-production environments. Write operations create real data in the source system.**

### Should You Implement Write Testing?

Ask yourself:
- Does the source API support creating/inserting data (POST/PUT endpoints)?
- Do you have write permissions and a test/sandbox environment?
- Would validating the full write → read cycle add confidence to your connector?

If **NO** to any of the above, **skip this step**. Write testing is completely optional.

If **YES** and you want comprehensive end-to-end validation, proceed below.

---

### What Write Testing Validates

Write testing creates a complete validation cycle:
1. **Write**: Test utils generate and insert test data into the source system
2. **Read**: Connector reads back the data using normal ingestion flow
3. **Verify**: Test suite confirms the written data was correctly ingested

This validates:
- ✅ Incremental sync picks up newly created records
- ✅ Schema correctly captures all written fields
- ✅ Field mappings and transformations work correctly
- ✅ End-to-end data integrity

---

### Implementation Steps

**Step 1: Create Test Utils File**

Create `sources/{source_name}/{source_name}_test_utils.py` implementing the interface defined in `tests/lakeflow_connect_test_utils.py`.

**Key Methods to Implement:**
- `get_source_name()`: Return the connector name
- `list_insertable_tables()`: List tables that support write operations
- `generate_rows_and_write()`: Generate test data and write to source system

**Reference Implementation:** See `sources/hubspot/hubspot_test_utils.py` for a complete working example.

**Implementation Tips:**
- Initialize API client for write operations in `__init__`
- Only include tables in `list_insertable_tables()` where you've implemented write logic
- Generate unique test data with timestamps/UUIDs to avoid collisions
- Use identifiable prefixes (e.g., `test_`, `generated_`) in test data
- Return proper `column_mapping` if source transforms field names during write/read
- Add delays after writes if source has eventual consistency (e.g., `time.sleep(5)`)

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

### When to Skip Write Testing

**Skip write testing if:**
- ❌ Source API is read-only (no create/insert endpoints)
- ❌ Only production environment available (too risky)
- ❌ Write permissions unavailable or expensive to obtain
- ❌ Source API charges for write operations
- ❌ Write operations have side effects (notifications, triggers, etc.)

**Write testing is completely optional.** The standard read-only tests in Step 4 are sufficient for most connectors.

---

## Step 6: Create a Public Connector Documentation

### Goal
Generate the **public-facing documentation** for the **{{source_name}}** connector, targeted at end users.

### Output Contract 
Produce a Markdown file based on the standard template `community_connector_doc_template.md` at `sources/{{source_name}}/README.md`.

### Documentation Requirements
- Please use the code implementation as the source of truth.
- Use the source API documentation to cover anything missing.
- Always include a section about how to configure the parameters needed to connect to the source system.
- AVOID mentioning internal implementation terms such as function or argument names from the `LakeflowConnect`.
