---
name: implement-connector
description: Implement a Python connector that conforms to the LakeflowConnect interface for data ingestion.
---

# Implement the Connector 

## Goal
Implement the Python connector for **{{source_name}}** that conforms exactly to the interface defined in  
[lakeflow_connect.py](../src/databricks/labs/community_connector/interface/lakeflow_connect.py). The implementation should be based on the source API documentation in `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md` produced by "understand-source".

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

**Breaking large data into multiple microbatches (CRITICAL for testing):** For any tables that support incremental read (where `read_table` returns a meaningful offset, not `None`), you **must always** support limiting the number of records or pages per microbatch (e.g., using a `max_records_per_batch` or `limit` parameter). When the limit is hit, return the current cursor as `end_offset` so the framework calls again. 

*Why this is emphasized:* This limit is not just for production microbatching; it is **heavily used during testing** to sample a smaller number of rows and return early. Without this limit, tests may hang or take too long by attempting to read the entire dataset. When all API pages are consumed within a call, the cursor stabilizes and the stream stops.

**Guaranteeing termination:** The connector must ensure `read_table` eventually returns `end_offset == start_offset`. Two approaches:
- **Cap the cursor at init time (recommended):** Record `datetime.now(UTC)` in `__init__` and cap `end_offset` at that timestamp. The cursor never advances past the connector's creation time, so it always converges. New data arriving after init is picked up by the next trigger (which creates a fresh connector instance). See `github/github.py` for an example.
- **Single-batch read:** Return `start_offset` as `end_offset` after one read. Simple but prevents splitting into multiple microbatches.

**Lookback window:** If the source API uses timestamp-based cursors (e.g. `since`/`updated_at`), apply a lookback window **at read time** (subtract N seconds from the cursor when building the API query), not in the stored offset. This avoids drift in the checkpointed cursor while still catching concurrently-updated records. Store the raw `max_updated_at` as the offset.

## Git Commit on Completion

After writing the initial connector implementation, commit it to git before returning:

```bash
git add src/databricks/labs/community_connector/sources/{source_name}/
git commit -m "Add initial {source_name} connector implementation"
```

Use the exact source name in the commit message. Do not push — only commit locally.
