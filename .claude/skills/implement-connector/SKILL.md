---
name: implement-connector
description: "Single step only: implement the connector in Python when the API doc already exists. Do NOT use for full connector creation — use the create-connector agent instead."
disable-model-invocation: true
---

# Implement the Connector 

## Goal
Implement the Python connector for **{{source_name}}** that conforms exactly to the interface defined in  
[lakeflow_connect.py](../src/databricks/labs/community_connector/interface/lakeflow_connect.py). The implementation should be based on the source API documentation in `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md` produced by the `research-source-api` skill.

## File Organization

For simple connectors, keeping everything in a single `{source_name}.py` file is perfectly fine. If the main file grows beyond **1000 lines**, consider splitting into multiple files for better maintainability. 

When using multiple files, use absolute imports:
```python
from databricks.labs.community_connector.sources.{source_name}.{util_file_name} import some_helper
```

The merge script (`tools/scripts/merge_python_source.py`) automatically discovers and includes all Python files in the source directory, ordering them by import dependencies.

See `src/databricks/labs/community_connector/sources/github/` for an example of a multi-file connector.

## Implementation Requirements
- Implement all methods declared in the interface.
- At the beginning of each function, check if the provided `table_name` exists in the list of supported tables. If it does not, raise an explicit exception to inform that the table is not supported.
- When returning the schema in the `get_table_schema` function, prefer using StructType over MapType to enforce explicit typing of sub-columns.
- Avoid flattening nested fields when parsing JSON data.
- Prefer using `LongType` over `IntegerType` to avoid overflow.
- If `ingestion_type` returned from `read_table_metadata` is `cdc` or `cdc_with_deletes`, then `primary_keys` and `cursor_field` are both required.
- If `ingestion_type` is `cdc_with_deletes`, you must also implement `read_table_deletes()` to fetch deleted records. This method should return records with at minimum the primary key fields and cursor field populated. Refer to `hubspot/hubspot.py` for an example implementation.
- In logic of processing records, if a StructType field is absent in the response, assign None as the default value instead of an empty dictionary {}.
- Avoid creating mock objects in the implementation.
- Do not add an extra main function - only implement the defined functions within the LakeflowConnect class.
- The functions `get_table_schema`, `read_table_metadata`, and `read_table` accept a dictionary argument that may contain additional parameters for customizing how a particular table is read. Using these extra parameters is optional.
- Do not include parameters and options required by individual tables in the connection settings; instead, assume these will be provided through the table_options.
- Do not convert the JSON into dictionary based on the `get_table_schema` before returning in `read_table`. 
- If a data source provides both a list API and a get API for the same object, always use the list API as the connector is expected to produce a table of objects. Only expand entries by calling the get API when the user explicitly requests this behavior and schema needs to match the read behavior.
- Some objects exist under a parent object, treat the parent object's identifier(s) as required parameters when listing the child objects. If the user wants these parent parameters to be optional, the correct pattern is:
  - list the parent objects
  - for each parent object, list the child objects
  - combine the results into a single output table with the parent object identifier as the extra field.
- Refer to `src/databricks/labs/community_connector/sources/example/example.py` or other connectors under `src/databricks/labs/community_connector/sources` as examples

## read_table Pagination and Termination

For incremental ingestion of table (CDC and Append-only), the framework calls `read_table` repeatedly within a single trigger run. Each call produces one microbatch. Pagination stops when the returned `end_offset` equals `start_offset`.

**Breaking large data into multiple microbatches (CRITICAL for testing):** For any tables that support incremental read (where `read_table` returns a meaningful offset, not `None`), you **must always** support limiting the work per microbatch. Two batching strategies exist — choose based on ingestion type:

- **Limit-on-the-fly** (`max_records_per_batch`, for CDC / cdc_with_deletes tables): Paginate through the API, accumulate records, and stop when the count reaches the limit. The client decides when to stop. CDC tables have primary keys, so upsert semantics tolerate duplicate deliveries if a cut point splits records with the same cursor timestamp.
- **Limit-before-fetch** (`limit` + `max_records_per_batch`, for append_only tables): Pass a small `limit` to each API call so the server controls the batch boundary per call. Repeat calls until the accumulated count reaches `max_records_per_batch` or the last returned record reaches `_init_ts`. Process all returned records — no client-side truncation. The actual total may be approximate since we never cut within a server response. This prevents setting the cursor to a mid-batch timestamp, which would cause duplicates or data loss on the next call (append_only tables have no primary key to deduplicate).

- **Sliding time-window** (`window_seconds`, for large-volume tables with `since`/`until` support): Query data in fixed-size time windows using `since`/`until` parameters, paginate all records within each window, then advance the cursor to the window end. The `window_seconds` table option (default e.g. 3600) controls the window size. If a window contains no records, still advance the cursor to `window_end` so the next call slides forward. Cap `window_end` at `_init_ts`. Use this when the source API doc warns that unbounded queries (e.g. only `since`) are slow on large datasets.

See the example connector's `_read_incremental` (limit-on-the-fly), `_read_incremental_by_limit` (limit-before-fetch), and `_read_incremental_by_window` (sliding window) for all three patterns.

*Why this is emphasized:* This limit is not just for production microbatching; it is **heavily used during testing** to sample a smaller number of rows and return early. Without this limit, tests may hang or take too long by attempting to read the entire dataset. When all API pages are consumed within a call, the cursor stabilizes and the stream stops.

**Guaranteeing termination:** The connector must ensure `read_table` eventually returns `end_offset == start_offset`. Two approaches:
- **Short-circuit at init time (recommended):** Record `datetime.now(UTC)` in `__init__` (`self._init_ts`). At the top of each incremental read (and `read_table_deletes`), if `start_offset` already has a cursor >= `_init_ts`, return immediately with `(iter([]), start_offset)`. Do **not** cap the cursor itself to `_init_ts` — that would cause re-fetching the same post-init records without advancing past them. Let the cursor be the last record's actual value; once it passes `_init_ts` the short-circuit fires and the stream terminates. New data arriving after init is picked up by the next trigger (which creates a fresh connector instance). See the example connector for reference.
- **Single-batch read:** Return `start_offset` as `end_offset` after one read. Simple but prevents splitting into multiple microbatches.

**Lookback window:** If the source API uses timestamp-based cursors (e.g. `since`/`updated_at`), apply a lookback window **at read time** (subtract N seconds from the cursor when building the API query), not in the stored offset. This avoids drift in the checkpointed cursor while still catching concurrently-updated records. Store the raw `max_updated_at` as the offset.

## API Call Best Practices

- **Always set explicit timeouts:** Every HTTP request must include a `timeout` parameter (e.g., `timeout=30`). Without it, a slow API hangs the connector and tests indefinitely with no error output.
- **Prefer server-side filtering:** Push filters (`since`/`until` etc.) to the API instead of fetching everything and filtering in Python. Client-side filtering still forces the server to scan the full dataset, which can cause timeouts on large accounts even with a small `limit`.
- **Design for large accounts:** What works on a small dev account may hang on a production account with millions of records. Avoid unbounded full-history parameters like `date_range=all`. Always scope queries to a bounded range.

## Pagination Patterns

Two common patterns for paginating through API data. Choose based on the source API's behavior:

**Pattern 1 — Cursor-based pagination (default choice):** Paginate through the API using offset or next-page tokens, stop after a batch limit, and store the last record's timestamp or page token as the cursor for the next call. This works well for most APIs and avoids needing to choose a window size upfront. However, it can fail on APIs that perform poorly with unbounded queries — if the API must scan the full dataset to sort or filter, even the first page may time out on large accounts.

**Pattern 2 — Sliding time-window:** Query data in fixed-size time windows (e.g., 1 hour) using `since`/`until` parameters, paginate within each window, then slide forward. The window position is tracked in the offset. This adds a `window_seconds` table option but avoids unbounded queries entirely. Use this when the source API doc warns about large data volume or testing reveals timeouts on unbounded queries. See `_read_incremental_by_window` in the example connector.

Start with Pattern 1. If the source API doc warns about large data volume on unbounded queries, or testing reveals timeouts on the target account (see test-and-fix-connector SKILL for diagnosis steps), switch to Pattern 2.

## Git Commit on Completion

After writing the initial connector implementation, commit it to git before returning:

```bash
git add src/databricks/labs/community_connector/sources/{source_name}/
git commit -m "Add initial {source_name} connector implementation"
```

Use the exact source name in the commit message. Do not push — only commit locally.
