---
name: implement-connector
description: "Single step only: implement the connector in Python when the API doc already exists. Do NOT use for full connector creation — use the create-connector agent instead."
disable-model-invocation: true
---

# Implement the Connector 

## Goal
Implement the Python connector for **{{source_name}}** that conforms exactly to the interface defined in  
[lakeflow_connect.py](../src/databricks/labs/community_connector/interface/lakeflow_connect.py). The implementation should be based on the source API documentation in `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md` produced by the `research-source-api` skill.

**CRITICAL REQUIREMENT:** Refer to `src/databricks/labs/community_connector/sources/example/example.py` for concrete examples of all these patterns. You should follow the patterns demonstrated in the example connector.

## File Organization

For simple connectors, keeping everything in a single `{source_name}.py` file is perfectly fine. If the main file grows beyond **1000 lines**, split it into multiple files for better maintainability. 

## Implementation Requirements

- **Interface:** Implement all methods declared in the `LakeflowConnect` interface. Do not add an extra main function.
- **Table Validation:** At the beginning of each function, check if the provided `table_name` exists in the list of supported tables. If it does not, raise an explicit exception (e.g., `ValueError`).
- **Schema:** In `get_table_schema`, prefer `StructType` over `MapType` to enforce explicit typing. Avoid flattening nested fields. Prefer `LongType` over `IntegerType` to avoid overflow. Do not convert the JSON into dictionaries based on the schema before returning in `read_table`; return the raw parsed JSON and let the framework handle type coercion.
- **Metadata:** If `ingestion_type` returned from `read_table_metadata` is `cdc` or `cdc_with_deletes`, both `primary_keys` and `cursor_field` are required.
- **Deletes:** If `ingestion_type` is `cdc_with_deletes`, you must implement `read_table_deletes()`. This method should return records with at minimum the primary key fields and cursor field populated.
- **Data Processing:** If a StructType field is absent in the response, assign `None` as the default value instead of an empty dictionary `{}`. Avoid creating mock objects.
- **Table Options:** The functions `get_table_schema`, `read_table_metadata`, and `read_table` accept a `table_options` dictionary. Do not include parameters required by individual tables in the global connection options; rely on `table_options` instead.
- **API Usage:** If a data source provides both a list API and a get API for the same object, always use the list API. Only call the get API for individual entries if explicitly requested. For child objects that require a parent identifier, list the parent objects first, then list child objects for each parent, and combine the results.

## Incremental read_table with offsets 

For incremental ingestion of tables (`cdc` and `append_only`), the framework calls `read_table` repeatedly within a single trigger run. Each call produces one microbatch. Pagination stops when the returned `end_offset` equals `start_offset`.

### Admission Control: `max_records_per_batch` (always required)

Every incremental table **must** support a `max_records_per_batch` table option. This caps how many records a single `read_table` call returns to the framework, giving Spark a bounded microbatch to process. Without it, a single call could return millions of rows and overwhelm the Spark driver.

This is **orthogonal** to the query-scoping strategies below. Regardless of whether you use a sliding window, a server-side limit, or neither, you must still respect `max_records_per_batch` by stopping record accumulation once the count is reached.

### Ascending Sort Order Requirement

When a connector stops mid-way (due to `max_records_per_batch` or page limits), it checkpoints the max cursor value and resumes with `since=<that value>`. This only works if the API returns records **sorted ascending by the cursor field**. With descending order the max cursor comes from the first record, so `since` never advances — creating an infinite loop of re-fetching the same data.

This can be ignored when the connector always drains all available data in a single batch (no partial reads).

**When the API does not support ascending sort**, use one of:
- **Sliding time-window (Strategy A below):** Query with both `since` and `until` so sort order within the window doesn't matter — the cursor advances to the window end regardless.
- **Client-side sort:** Fetch, sort ascending in Python, then apply batch limits. Only viable for small result sets.

### Query-Scoping Strategies

A common problem with incremental reads is that the query to the source API is too broad. For example, an API that accepts a `since` parameter but no `until` parameter forces the server to scan from `since` all the way to "now" — which can be slow or time out on large accounts. Two strategies exist to scope the query and avoid this:

**Strategy A — Sliding time-window** (for APIs with `since`/`until` or equivalent start/end time parameters):
Add a bounded end-time to the query. Instead of querying from `since` to now, query from `since` to `since + window_seconds`. Paginate all records within that window, then advance the cursor to the window end. If the window is empty, still advance the cursor so the next call slides forward. The `window_seconds` table option controls the window size. Provide a sensible default (e.g. 3600), but note that the optimal value depends on the data volume and it is up to the user to adjust it for their specific use case. For testing, always start with a very small value. Use this when the API supports both start and end time filters. 

**Strategy B — Server-side limit** (for APIs with `limit`/`max_results`/`page_size` parameters):
Pass a small `limit` parameter directly to the API so the server returns a bounded page. This keeps each individual API call small. Calls repeat until `max_records_per_batch` is reached.

**Choosing a strategy:** Start with the simplest approach (standard pagination + `max_records_per_batch`). If the source API doc warns about slow unbounded queries, or testing reveals timeouts/hangs, add Strategy A or B. If the API supports both time-range and limit parameters, prefer the sliding window (Strategy A) as it provides more predictable bounds.

### How to Stop Accumulating Records (Handling Batch Boundaries)

When accumulating records to reach `max_records_per_batch`, how you stop depends on the table's ingestion type. The issue is what happens if you stop in the middle of a set of records that all share the exact same cursor timestamp.

**For CDC / `cdc_with_deletes` tables (Client-side truncation is safe):**
- **What to do:** You can accumulate records and strictly truncate them on the client side to exactly `max_records_per_batch`. The client decides exactly when to stop, and you use the last processed record's timestamp as the offset.
- **Why it's safe:** The next trigger will query starting from that timestamp. It might re-fetch some of the records you already processed in the previous batch. However, CDC tables have primary keys, so Databricks will perform an Upsert (Merge) and safely deduplicate them.

**For Append-Only tables (NO client-side truncation):**
- **What to do:** You **must not** truncate records on the client side. You must process every record returned in the API's page. You stop making *additional* API calls once your total accumulated count reaches or exceeds `max_records_per_batch`.
- **Why truncation fails:** Append-only tables do not have primary keys and use Inserts instead of Upserts. If you stop halfway through a set of records with the same timestamp, the next trigger will query that timestamp again. The server will return those records again, resulting in permanent duplicate rows in Databricks (or data loss if the API uses strict greater-than `>` filtering).
- **How to control batch size:** Because you must process full pages, you must keep the server's pages small. You must use **Strategy B** (pass a small limit parameter, e.g., `limit=50`, directly to the API) so the server controls the boundary. You accumulate these small full pages until you reach or slightly exceed `max_records_per_batch`.

### Guaranteeing Termination

The connector must ensure `read_table` eventually returns `end_offset == start_offset` to signal that all currently available data has been read. This happens in two cases:

1. **End of data:** The API indicates there are no more pages.
2. **Short-circuit at init time:** If a source system has continuous, high-volume updates and we do not short-circuit, the connector will keep fetching new records indefinitely and the trigger/microbatch will never finish. To prevent the connector from indefinitely chasing data that is actively being written, record `datetime.now(UTC)` in `__init__` (`self._init_ts`). At the top of each incremental read, if `start_offset` already has a cursor >= `_init_ts`, return immediately with `(iter([]), start_offset)`. Do **not** cap the cursor itself to `_init_ts` when yielding records; let the cursor be the last record's actual value so it advances naturally until it hits the short-circuit condition.

### Lookback Window

If the source API uses timestamp-based cursors (e.g. `since`/`updated_at`), apply a lookback window **at read time** (subtract N seconds from the cursor when building the API query), not in the stored offset. This avoids drift in the checkpointed cursor while still catching concurrently-updated records. **Important:** The lookback subtraction must only be applied once at the beginning of the trigger (e.g. tracking state on `self` during the first read), rather than re-applying the lookback on every subsequent pagination read within the same trigger. Store the raw `max_updated_at` as the offset.

### Testing Implications

These options are **critical for testing**. Without them, tests may hang or take forever by attempting to read the entire dataset from the source.

- Always configure a **small** `max_records_per_batch` in the test's `dev_table_config.json` (e.g., 5).
- If using a sliding window, start with a **small**: for example `window_seconds` (e.g., 60 or 300).
- If using a server-side limit, start with a **small** for example `limit` (e.g., 5), or `max_pages` (e.g. 1)
- Gradually increase these values only if the small values do not generate enough data for testing.

## API Call Best Practices

- **Always set explicit timeouts:** Every HTTP request must include a `timeout` parameter (e.g., `timeout=20`). Without it, a slow API hangs the connector and tests indefinitely with no error output.
- **Prefer server-side filtering:** Push filters (`since`/`until` etc.) to the API instead of fetching everything and filtering in Python. Client-side filtering still forces the server to scan the full dataset, which can cause timeouts on large accounts.
- **Design for large accounts:** What works on a small dev account may hang on a production account with millions of records. Avoid unbounded full-history parameters like `date_range=all`. Always scope queries to a bounded range.

## Merge files
After completion, run `python tools/scripts/merge_python_source.py {source_name}` to generate the merged connector file `_generated_{source_name}_python_source.py`. 
