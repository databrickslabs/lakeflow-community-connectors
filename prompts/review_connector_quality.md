# Community Connector Quality Review

## Goal

Evaluate the completeness, correctness, and production-readiness of a community connector by comparing it against official documentation and industry-standard implementations (Airbyte, Fivetran).

## Output

Generate a quality review report that identifies gaps, validates coverage, and provides actionable recommendations for improvement. Take your time, do your research well and be thorough.

## Core Principles

1. **Evidence-based** — Every assessment must reference official docs, Airbyte, Fivetran, or connector code
2. **Quantifiable** — Use coverage percentages and counts, not subjective judgments
3. **Actionable** — Identify specific gaps with clear remediation paths
4. **Prioritized research** — Official docs > Fivetran/Airbyte > Community sources

---

## Step 1: Run the Test Suite

Before reviewing schema/column coverage, **run the test suite** to get actual schema and metadata information from the connector. This can be used to fetch the table schema or metadata when the connector makes API calls to fetch `get_table_schema` or `read_table_metadata`.

### Running the Test Suite

```bash
# From the repository root
cd {repo_root}

# Run the connector's test file
python -m pytest sources/{source_name}/test/test_{source_name}_lakeflow_connect.py -v
```

### What the Test Suite Validates

The [test_suite.py](../tests/test_suite.py) automatically validates:

1. **`test_initialization`** - Connector initializes without errors
2. **`test_list_tables`** - Returns a valid list of table names
3. **`test_get_table_schema`** - For each table:
   - Returns `StructType` (not other types)
   - Uses `LongType` instead of `IntegerType`
   - ⚠️ Does NOT validate absence of `MapType` (must check manually)
4. **`test_read_table_metadata`** - For each table:
   - Returns required keys (`primary_keys`, `cursor_field` where applicable)
   - Primary keys exist in schema
   - Cursor field exists in schema (for CDC tables)
   - `read_table_deletes()` implemented if `ingestion_type` is `cdc_with_deletes`
5. **`test_read_table`** - For each table:
   - Returns `(Iterator, dict)` tuple
   - Records are valid dicts
   - Records can be parsed with the declared schema
6. **`test_read_table_deletes`** - For `cdc_with_deletes` tables only

### Using Test Results for Review

After running the test suite, extract the following for your review:

| Information | How to Get It |
|-------------|---------------|
| Supported tables | From `test_list_tables` details |
| Schema per table | From `test_get_table_schema` details (field count) |
| Metadata per table | From `test_read_table_metadata` details |
| Sample records | From `test_read_table` details (shows 2-3 sample records) |
| Data quality | Check if records parse correctly against schema |

### Example Test Output

```
✅ test_initialization
  Status: PASSED
  
✅ test_list_tables
  Status: PASSED
  Details: {"table_count": 5, "tables": ["teams", "channels", "messages", "members", "message_replies"]}

✅ test_get_table_schema
  Status: PASSED
  Details: {"passed_tables": [{"table": "teams", "schema_fields": 10}, ...]}

✅ test_read_table_metadata  
  Status: PASSED
  Details: {"passed_tables": [{"table": "messages", "metadata_keys": ["primary_keys", "cursor_field", "ingestion_type"]}, ...]}
```

---

## Step 2: Review Dimensions

### 1. Table Coverage Analysis

#### Research Steps:
1. **Document Official API Tables**
   - List all tables/endpoints available in the official API documentation
   - Note which tables support incremental sync (cursor fields, change tracking)
   - Note which tables support delete detection

2. **Benchmark Against Industry Standards**

| Source | Total Tables | Incremental Tables | Delete Support |
|--------|--------------|-------------------|----------------|
| Official API Docs | ? | ? | ? |
| Airbyte Connector | ? | ? | ? |
| Fivetran Connector | ? | ? | ? |
| **This Connector** | ? | ? | ? |

3. **Calculate Coverage**
   ```
   Table Coverage = (Tables Implemented / Tables in Official API) × 100%
   Incremental Coverage = (Incremental Tables / Total Incremental-Capable Tables) × 100%
   ```

---

### 2. Schema/Column Coverage

For each implemented table, compare against official API schema:

| Table | Official Columns | Connector Columns | Missing Columns | Coverage % |
|-------|-----------------|-------------------|-----------------|------------|
| table_1 | 25 | 23 | `field_x`, `field_y` | 92% |
| table_2 | 15 | 15 | None | 100% |

#### Research Priority:
| Priority | Source | Confidence |
|----------|--------|------------|
| 1 | Official API Documentation | Highest |
| 2 | Official SDK/Client Libraries | High |
| 3 | Fivetran Schema Documentation | Medium-High |
| 4 | Airbyte Catalog/Schema | Medium |
| 5 | Community Implementations | Low |

**Note:** If official documentation is unavailable, fall back to Fivetran or Airbyte schema definitions as reference.

---

### 3. Delete Handling Analysis

| Question | Answer | Source |
|----------|--------|--------|
| Does the official API support soft deletes (e.g., `deleted_at` field)? | Yes/No | [URL] |
| Does the official API support hard delete detection (e.g., Delta API, webhooks)? | Yes/No | [URL] |
| Does Airbyte implement delete sync? How? | Yes/No - Method | [URL] |
| Does Fivetran implement delete sync? How? | Yes/No - Method | [URL] |
| **Does this connector implement delete sync?** | Yes/No - Method | Code reference |

#### Delete Sync Methods:
| Method | Description |
|--------|-------------|
| `soft_delete` | Records have a `deleted_at` or `is_deleted` field |
| `delta_api` | API returns deleted record IDs (e.g., Microsoft Graph Delta) |
| `tombstone` | Deleted records returned with special marker |
| `full_sync_compare` | Compare snapshots to detect deletions |
| `not_supported` | No delete detection available in API |

---

### 4. Rate Limiting & Error Handling

| Aspect | Official Docs | Fivetran | Airbyte | This Connector |
|--------|---------------|----------|---------|----------------|
| Rate limit (requests/min) | ? | ? | ? | ? |
| Retry strategy documented | Yes/No | Yes/No | Yes/No | Yes/No |
| Exponential backoff | Yes/No | Yes/No | Yes/No | Yes/No |
| Retry-After header support | Yes/No | Yes/No | Yes/No | Yes/No |
| 429 handling (rate limit) | Yes/No | Yes/No | Yes/No | Yes/No |
| 401 handling (auth expired) | Yes/No | Yes/No | Yes/No | Yes/No |
| 403 handling (permission denied) | Yes/No | Yes/No | Yes/No | Yes/No |
| 404 handling (not found) | Yes/No | Yes/No | Yes/No | Yes/No |
| 500/502/503 handling (server errors) | Yes/No | Yes/No | Yes/No | Yes/No |
| Timeout handling | Yes/No | Yes/No | Yes/No | Yes/No |

#### Questions to Answer:
- Has the official documentation mentioned API rate limits?
- Has Fivetran or Airbyte documented rate limit handling?
- Does the connector implement rate limiting as per official recommendations?

---

### 5. Incremental Sync Strategy

For each CDC/incremental table:

| Table | Cursor Field | Official Recommendation | Airbyte Approach | Fivetran Approach | This Connector |
|-------|--------------|------------------------|------------------|-------------------|----------------|
| table_1 | `updated_at` | Timestamp filter | Timestamp | Timestamp | ✅ Timestamp |
| table_2 | `modified` | Delta API | Cursor | Full sync | ✅ Delta API |

#### Key Questions:
- What cursor fields does the official API recommend?
- Does the API support server-side filtering by date/cursor?
- Is there a Delta/Change tracking API available?
- What lookback window is recommended for late-arriving data?

---

### 6. Write-Back Testing (Optional)

> ℹ️ **This section is optional.** If not implemented, note it as a recommendation but do not deduct points.

If the connector has `LakeflowConnectTestUtils` implemented, it enables end-to-end validation:

| Test | What It Validates |
|------|-------------------|
| `test_write_to_source` | Can write test data to source |
| `test_incremental_after_write` | Incremental sync picks up new records |
| `test_delete_and_read_deletes` | Delete sync works for `cdc_with_deletes` tables |

**Check if implemented:**
- [ ] `{source_name}_test_utils.py` exists
- [ ] `list_insertable_tables()` returns tables
- [ ] `generate_rows_and_write()` implemented
- [ ] `list_deletable_tables()` for `cdc_with_deletes` tables

**Reference:** See [implement_write_back_testing.md](./implement_write_back_testing.md) and `sources/hubspot/hubspot_test_utils.py` for example.

---

### 7. Configuration Architecture

Verify that configuration options are correctly categorized:

| Option Type | Belongs In | Examples |
|-------------|------------|----------|
| **Authentication** | Connection options | `api_key`, `token`, `subdomain`, `client_id` |
| **Data filtering/limits** | Table options | `max_records`, `date_range`, `workflow_status`, `filters` |

**Check:**
- [ ] Authentication parameters are in connection options only
- [ ] Data filtering parameters are in table options (not connection)
- [ ] If filtering params are in both (as defaults), this is documented

**Anti-pattern:** Putting `max_records` or `date_filter` in connection options instead of table options.

---

### 8. API Documentation Quality (Optional)

> ℹ️ **This section is optional.** Recommend if missing, but do not deduct points.

| Check | Status | Notes |
|-------|--------|-------|
| `{source_name}_api_doc.md` exists | ✅/❌ | |
| Follows [source_api_doc_template](./templates/source_api_doc_template.md) | ✅/❌ | |
| All read endpoints documented | ✅/❌ | |
| Authentication clearly explained | ✅/❌ | |
| Pagination strategy documented | ✅/❌ | |
| Rate limits documented | ✅/❌ | |
| Research log with source URLs | ✅/❌ | |
| Write-back APIs documented (if applicable) | ✅/❌ | |

---

### 9. Connector Spec Validation (Optional)

> ℹ️ **This section is optional.** Recommend if missing, but do not deduct points.

| Check | Status | Notes |
|-------|--------|-------|
| `connector_spec.yaml` exists | ✅/❌ | |
| All required connection parameters listed | ✅/❌ | |
| Parameter types and descriptions accurate | ✅/❌ | |
| `external_options_allowlist` includes all table options | ✅/❌ | |
| Optional parameters marked as `required: false` | ✅/❌ | |

---

## Review Checklist

### Completeness
- [ ] All major tables from official API are implemented
- [ ] Table coverage ≥ 80% of official API surface
- [ ] Schema coverage ≥ 95% for each implemented table
- [ ] Missing columns documented with rationale

### Incremental Sync
- [ ] All incremental-capable tables use CDC or append mode
- [ ] Cursor fields match official API recommendations
- [ ] Lookback window implemented for late-arriving data
- [ ] Delta/change tracking APIs used where available

### Delete Handling
- [ ] Delete detection implemented where API supports it
- [ ] Soft delete fields captured in schema
- [ ] Delete sync strategy documented in README

### Error Handling
- [ ] Rate limiting implemented per official docs
- [ ] Exponential backoff on retryable errors (429, 503, 500)
- [ ] Retry-After header respected
- [ ] Timeout handling implemented

### Documentation
- [ ] README includes all supported tables
- [ ] Required permissions/scopes documented
- [ ] Configuration options documented
- [ ] Known limitations documented
- [ ] API version clearly stated

---

## Research Log Template

Document all sources consulted during the review:

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official API Docs | https://... | YYYY-MM-DD | Highest | Tables, schemas, rate limits |
| Fivetran Docs | https://... | YYYY-MM-DD | High | Table list, sync modes |
| Airbyte Catalog | https://... | YYYY-MM-DD | Medium | Schema fields, cursor fields |
| Connector Code | `source.py:L123` | YYYY-MM-DD | Highest | Implementation details |

---

## Summary Scorecard

| Criterion | Score | Weight | Weighted Score |
|-----------|-------|--------|----------------|
| Completeness & Functionality | ?/100 | 50% | ? |
| Methodology & Reusability | ?/100 | 30% | ? |
| Code Quality & Efficiency | ?/100 | 20% | ? |
| **Final Score** | | 100% | **?/100** |

---

## Scoring Rubrics

### 1. Completeness & Functionality (50%)

**Question:** Is the connector technically sound, fully implemented, and covers the full connector API surface area?

| Score Range | Rating | Description |
|-------------|--------|-------------|
| 1-33 | Needs Work | Connector is missing functionality, does not fully work, or only covers a small % of the API surface area |
| 34-66 | Acceptable | Connector is fully working but missing some functionality or only has partial API surface area coverage |
| 67-100 | Excellent | Connector is fully working, has full functionality, and complete or nearly complete API coverage |

> ⚠️ **IMPORTANT:** If the test suite fails, the maximum score for this category is **33**. A passing test suite is a prerequisite for scores above 33.

**Evaluation Factors:**
- [ ] **Test suite passes** - All tests in `test_{source_name}_lakeflow_connect.py` pass (see Step 1) — **Required for score > 33**
- [ ] Table coverage (% of official API tables implemented)
- [ ] Schema coverage (% of columns per table)
- [ ] Incremental sync implemented for all CDC-capable tables
- [ ] Delete handling implemented where API supports it
- [ ] Error handling (rate limits, retries, timeouts)
- [ ] All core functionality works end-to-end

**Test Suite Requirements:**
| Test | Must Pass | What It Validates |
|------|-----------|-------------------|
| `test_initialization` | ✅ Required | Connector can be instantiated with valid credentials |
| `test_list_tables` | ✅ Required | Returns valid list of table names |
| `test_get_table_schema` | ✅ Required | Each table returns valid StructType schema, uses LongType (not IntegerType) |
| `test_read_table_metadata` | ✅ Required | Each table has primary_keys, cursor_field (if CDC), valid ingestion_type |
| `test_read_table` | ✅ Required | Each table returns valid records that parse against schema |
| `test_read_table_deletes` | If applicable | Required only if any table has `ingestion_type: cdc_with_deletes` |

**Manual Validation Required (not covered by test suite):**
- [ ] Schema types are appropriate for the data (validated in Code Quality section)

> ℹ️ **Note:** `MapType` vs `StructType` usage is evaluated under **Code Quality & Efficiency**, not here.

---

### 2. Methodology & Reusability (30%)

**Questions:**
- Is the documentation following the public documentation template? Does it miss any important and required sections?
- Is there a source API documentation from the research step following the template?
  - If not, did they describe such a thing in their final slides or presentation?
- Did they add local unit tests to read data from the source system?
  - If not, it means they rely on pipeline to run e2e tests, which is less desirable, unless they describe something unique in their presentation
- Do they describe anything special about their methodology, vibe-coding strategy, testing, or bugs on the template for which they also propose a fix or improvements?

| Score Range | Rating | Description |
|-------------|--------|-------------|
| 1-33 | Needs Work | README missing or incomplete. No source API documentation. No local unit tests (relies only on pipeline e2e). No documentation of methodology or novel approaches. |
| 34-66 | Acceptable | README follows template but may have gaps. Source API doc missing but described in presentation. Local unit tests present (may use mocks only). Methodology documented but no novel approaches. |
| 67-100 | Excellent | README complete and follows template. Source API documentation from research step present. Local unit tests using `test_suite.py` with real API calls. Novel vibe-coding approaches documented with reusable elements. |

**Evaluation Factors:**
- [ ] README documentation under the source directory - quality and completeness
- [ ] Is there a source API documentation from the research step?
- [ ] Local unit tests to read data from the source system using `test_suite.py` provided by the template
- [ ] Unit tests that do not read data from a real system (e.g., using mocks) should be present but are less desirable than template provided unit tests.
- [ ] README documents anything special about their methodology, vibe-coding strategy, testing, or bugs on the template for which they also propose a fix or improvements

---

### 3. Code Quality & Efficiency (20%)

**Questions:**
- Is the code well written, structured, and following best practices?
- Is connector code written to efficiently ingest data from source?

| Score Range | Rating | Description |
|-------------|--------|-------------|
| 1-33 | Needs Work | Code is NOT well written or documented, not following best practices, or is difficult to understand. Code is NOT written to efficiently ingest data. |
| 34-66 | Acceptable | Code is well written and following best practices but NOT written to efficiently ingest data. |
| 67-100 | Excellent | Code is well written and following best practices AND written to efficiently ingest data. |

**Evaluation Factors:**

#### Interface Conformance (per [lakeflow_connect.py](../sources/interface/lakeflow_connect.py))
- [ ] Implements all required methods: `__init__`, `list_tables`, `get_table_schema`, `read_table_metadata`, `read_table`
- [ ] Implements `read_table_deletes()` if `ingestion_type` is `cdc_with_deletes`
- [ ] Each method validates `table_name` exists in supported tables list
- [ ] Returns proper types (StructType for schema, Iterator for records, dict for metadata/offset)

#### Implementation Best Practices (per [implement_connector.md](./implement_connector.md))
- [ ] Uses `StructType` over `MapType` for explicit typing of nested fields
- [ ] Uses `LongType` over `IntegerType` to avoid overflow
- [ ] Does not flatten nested JSON fields
- [ ] Assigns `None` (not `{}`) for absent StructType fields
- [ ] Uses list APIs over get APIs (unless explicitly expanding entries)
- [ ] Handles parent-child relationships correctly (parent ID as required param or auto-discovery)
- [ ] No mock objects in implementation
- [ ] No extra main function - only LakeflowConnect class methods
- [ ] Configuration architecture correct (auth in connection, filtering in table_options)

#### Code Quality
- [ ] Code readability and documentation (docstrings, comments)
- [ ] Consistent coding style and naming conventions
- [ ] Clear error messages with context (table name, API endpoint, etc.)

#### Efficiency & Performance
- [ ] Efficient API usage (pagination, batching, parallel requests)
- [ ] Proper resource management (connection pooling, token caching)
- [ ] Performance optimizations (Delta API, server-side filtering where available)
- [ ] Rate limiting implemented per official API docs
- [ ] Exponential backoff on retryable errors (429, 500, 502, 503)
- [ ] Retry-After header respected
- [ ] Timeout handling implemented

---

## Final Rating Scale

| Score | Rating | Description |
|-------|--------|-------------|
| 90-100 | Excellent | Production-ready, comprehensive coverage, innovative approaches |
| 75-89 | Good | Minor gaps, suitable for most use cases, solid implementation |
| 60-74 | Acceptable | Notable gaps, may need enhancements for production use |
| < 60 | Needs Work | Significant gaps, not production-ready |

---

## Example: Quick Comparison Table

```markdown
## {Source Name} Connector Comparison

| Feature | Official API | Fivetran | Airbyte | This Connector |
|---------|-------------|----------|---------|----------------|
| Total Tables | 15 | 8 | 11 | 5 |
| Incremental Tables | 10 | 5 | 7 | 2 |
| Delete Support | Yes (soft) | Yes | No | Yes |
| Rate Limit Handling | 1000/min | Yes | Yes | Yes |
| API Version | v2.0 | v2.0 | v1.0 | v2.0 |
```

---

## Acceptance Criteria

Before completing a review, verify:

- [ ] All three scoring dimensions evaluated with evidence
- [ ] Research log completed with source URLs
- [ ] Coverage percentages calculated for Completeness & Functionality
- [ ] Gaps identified with specific missing items
- [ ] Summary scorecard completed with weighted final score
- [ ] Recommendations provided for each gap

