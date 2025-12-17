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

## Step 5: Create a Public Connector Documentation

### Goal
Generate the **public-facing documentation** for the **{{source_name}}** connector, targeted at end users.

### Output Contract 
Produce a Markdown file based on the standard template `community_connector_doc_template.md` at `sources/{{source_name}}/README.md`.

### Documentation Requirements
- Please use the code implementation as the source of truth.
- Use the source API documentation to cover anything missing.
- Always include a section about how to configure the parameters needed to connect to the source system.
- AVOID mentioning internal implementation terms such as function or argument names from the `LakeflowConnect`.
