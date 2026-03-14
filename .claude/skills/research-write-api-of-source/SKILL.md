---
name: research-write-api-of-source
description: Research and document write/create APIs of a source system to enable write-back testing functionality.
disable-model-invocation: true
---

# Research Write APIs of Source System

## Goal

Research and document write/create API endpoints for each table supported by the connector. These write APIs are used **only** for generating test data — they are not part of the connector's read functionality.

## Input

- The existing `{source_name}_api_doc.md` (for auth details, base URLs, and the list of supported tables)
- The connector implementation under `src/databricks/labs/community_connector/sources/{source_name}/` (for table names and field mappings)

## Output

Append a `## Write-Back APIs (For Testing Only)` section to the existing `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md`.

## Research Process

1. **Identify tables** — Read the API doc and connector implementation to get the full list of supported tables.
2. **Search official docs** — For each table, search for POST/PUT/create endpoints in the official API documentation.
3. **Cross-reference** — Verify payload structure and required fields against at least two sources (official docs, Airbyte test utilities, Singer taps, SDK source code, etc.).
4. **Document** — For each table with a write endpoint, fill in the template below. Skip tables that are read-only.
5. **Log sources** — Record every URL consulted in the Research Log.

## Output Template

````markdown
## Write-Back APIs (For Testing Only)

**These APIs are documented solely for test data generation. They are NOT part of the connector's read functionality.**

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
- **Response**: What the API returns (ID, created timestamp, etc.)

### Field Name Transformations

Document any differences between write and read field names:

| Write Field Name | Read Field Name | Notes |
|------------------|-----------------|-------|
| `email` | `properties_email` | API adds `properties_` prefix on read |
| `createdAt` | `created_at` | Different casing convention |

If no transformations exist, state: "Field names are consistent between write and read operations."

### Write-Specific Constraints

- **Rate Limits**: Write-specific rate limits (if different from read)
- **Eventual Consistency**: Delays between write and read visibility
- **Unique Constraints**: Fields that must be unique (to guide test data generation)

### Research Log for Write APIs

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | ... | YYYY-MM-DD | High | Write endpoints and payload structure |
| Reference Impl | ... | YYYY-MM-DD | Med | Field transformations |
````

## Acceptance Checklist

- [ ] Every supported table checked for write endpoint availability
- [ ] Each writable table has a complete endpoint doc with example payload
- [ ] All required fields for write operations identified
- [ ] Field name transformations documented (or explicitly noted as consistent)
- [ ] Write-specific constraints (rate limits, consistency delays) noted
- [ ] Research log completed with full URLs
