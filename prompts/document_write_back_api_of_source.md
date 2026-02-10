# Document Write-Back APIs 

## Goal
Research and document the write/create APIs for the source system to enable write-back testing. This step is **completely separate** from the core connector implementation and should only be done if comprehensive end-to-end validation is needed.

## When to Do This Step

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

## Input
- The `{source_name}_api_doc.md` created by "understand-source" (for reference)
- Access to source API documentation

## Output
Add a new section `## Write-Back APIs (For Testing Only)` to your existing `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md` file.

## Documentation Template for Write-Back APIs Section

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

## Research Requirements

Follow the same rigorous research process as "understand-source":
1. **Check official API documentation** for write/create endpoints
2. **Find reference implementations** (Airbyte test utilities, Singer tap tests, etc.)
3. **Cross-reference at least two sources** for payload structure and required fields
4. **Test if possible**: If you have sandbox access, verify one write operation works
5. **Document everything** in Research Log with full URLs

## Validation Checklist

- [ ] At least one write endpoint documented with complete payload structure
- [ ] All required fields for write operations identified
- [ ] Field name transformations (if any) documented in mapping table
- [ ] Rate limits and constraints noted
- [ ] Eventual consistency delays documented (if applicable)
- [ ] Research log completed with sources
````

