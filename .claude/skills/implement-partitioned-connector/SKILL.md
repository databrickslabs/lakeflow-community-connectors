---
name: implement-partitioned-connector
description: "Single step only: implement a partitioned connector that extends SupportsPartitionedStream (or SupportsPartition) alongside LakeflowConnect. Do NOT use for full connector creation — use the create-connector agent instead."
---

# Implement a Partitioned Connector

## Goal
Implement a Python connector for **{{source_name}}** that conforms to both the `LakeflowConnect` interface and the `SupportsPartitionedStream` mixin (or `SupportsPartition` for batch-only partitioning). The implementation should be based on the source API documentation in `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md`.

**CRITICAL REQUIREMENT:** Read and follow the patterns in the existing `implement-connector` SKILL at `.claude/skills/implement-connector/SKILL.md` — most of its rules (schema, metadata, incremental offsets, API best practices, etc.) apply here as well. This SKILL only covers the **additional** requirements for partitioned connectors.

## Reference Files

- **SupportsPartition / SupportsPartitionedStream interface:** `src/databricks/labs/community_connector/interface/supports_partition.py`
- **LakeflowConnect interface:** `src/databricks/labs/community_connector/interface/lakeflow_connect.py`

## Choosing the Right Mixin

- **`SupportsPartitionedStream`** — Use when the connector needs partitioned **streaming** reads. This also automatically enables partitioned batch reads (inherits from `SupportsPartition`). This is the most common choice.
- **`SupportsPartition`** — Use when the connector only needs partitioned **batch** reads and streaming should remain on the single-driver `SimpleDataSourceStreamReader` path.

## Class Declaration

The connector class must extend both `LakeflowConnect` and the chosen mixin:

```python
from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)

class MyLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    ...
```

## Methods to Implement

### From LakeflowConnect (always required)
- `list_tables()`
- `get_table_schema(table_name, table_options)`
- `read_table_metadata(table_name, table_options)`
- `read_table(table_name, start_offset, table_options)` — still required; used by `simpleStreamReader` for tables where `is_partitioned()` returns `False`

### From SupportsPartition (batch partitioning)

- **`get_partitions(table_name, table_options)`** — Return a list of partition descriptor dicts. Each dict is JSON-serialised and passed to `read_partition` on a Spark executor. Keep descriptors small (e.g., `{"page": 3}` or `{"start": 100, "end": 200}`).
- **`read_partition(table_name, partition, table_options)`** — Read records for one partition. This runs on executors and must be self-contained (re-create API clients, do not rely on driver-side state).

### Additional from SupportsPartitionedStream (streaming partitioning)

- **`latest_offset(table_name, table_options, start_offset=None, max_rows=None)`** — Return the most recent offset available. Called by Spark every micro-batch. `start_offset` is the current committed offset (`{}` on the first call). `max_rows` is an optional admission-control hint: when set, the connector should cap the returned offset so that reading `(start_offset, returned_offset]` yields at most ~`max_rows` records; when `None`, return the full high-water mark (bounded by any init-time cap). Return a dict with primitive values (str, int, bool).
- **`get_partitions(table_name, table_options, start_offset=None, end_offset=None)`** — Overrides the batch version with optional offset params. When `start_offset` and `end_offset` are both `None`, behave as batch (partition the entire table). When offsets are provided, partition only the given range. Return an empty list when `start_offset == end_offset`.
- **`is_partitioned(table_name)`** *(optional override)* — Return `False` for tables that should fall back to `simpleStreamReader`. Default is `True`.

## How Partitioned Streaming Works

Spark drives each micro-batch in three steps:

1. **`latest_offset`** — Called periodically on the driver to discover new data. Must be a **lightweight metadata-only call** (e.g., query the source for a max timestamp or latest event ID). Do NOT read actual records here.
2. **`get_partitions(start, end)`** — Called with two adjacent offsets from successive `latest_offset` calls. The first micro-batch has `start={}` (empty dict from `initialOffset`), meaning "beginning of time". Split the range `(start, end]` into partition descriptors.
3. **`read_partition`** — Called on executors for each partition. Fetch and return the actual records.

**Offset discovery and data reading are separate.** Unlike `SimpleDataSourceStreamReader` where `read()` discovers and returns data together, here `latest_offset` discovers *what* exists (cheap) and `read_partition` *reads* it (expensive, parallelised). Reading records inside `latest_offset` would be catastrophic — it runs on the driver every interval and the records are discarded.

**Typical pattern:** Source APIs with time-range filters (`since`/`until`). `latest_offset` returns the current high-water mark timestamp. `get_partitions` splits the time range into windows. `read_partition` queries each window.

## Guaranteeing Termination (Trigger AvailableNow)

The connector runs under `Trigger.AvailableNow`, which issues microbatches until the source reports "no more data available" — signalled by **`latest_offset` returning the same value as the previous call**. There is no explicit stop signal. If `latest_offset` keeps returning ever-advancing values (e.g. by reflecting continuously-arriving new data), the trigger never terminates and the pipeline hangs.

This is the partitioned-stream equivalent of the `end_offset == start_offset` rule for the `SimpleDataSourceStreamReader` path, and it is just as critical.

**How to guarantee termination:** Cap the offset at init time. Record `datetime.now(UTC)` in `__init__` (`self._init_time`) and have `latest_offset` return `min(source_high_water_mark, self._init_time)`. Once the stream catches up to `_init_time` — either because there was finite data, or because subsequent microbatches have drained everything up to that cap — `latest_offset` stabilises and the trigger terminates. Data arriving after `_init_time` is picked up by the next trigger, which creates a fresh connector instance with a newer `_init_time`.

```python
def __init__(self, options):
    super().__init__(options)
    self._init_time = datetime.now(timezone.utc).isoformat()

def latest_offset(self, table_name, table_options, start_offset=None, max_rows=None):
    source_max = self._query_source_high_water_mark(table_name)  # e.g. max updated_at
    capped = min(source_max, self._init_time) if source_max else self._init_time
    return {"cursor": capped}
```

Do **not** cap the data returned by `read_partition` at `_init_time`; only cap the *offset*. Records whose cursor is greater than `_init_time` may still be read — they will simply be re-read by the next trigger with a larger `_init_time` window. (For CDC tables with primary keys this is safe via upsert; for append-only tables, design partitions so the cap boundary does not produce duplicates.)

## Partitioning Design Guidelines

### Partition Granularity
- Each partition should represent a bounded unit of work (e.g., one API page, one time range, one shard).
- Aim for partitions that each take roughly the same time to process.
- Too few partitions underutilise parallelism; too many create scheduling overhead.

### Partition Descriptors Must Be Self-Contained
- `read_partition` runs on executors without access to driver state.
- The partition dict must contain everything needed to fetch the data (e.g., page number, offset range, API URL parameters).
- Re-create API clients inside `read_partition` using `self.options` for credentials.

### Batch vs Stream Partitioning
- **Batch** (`get_partitions(table_name, table_options)` with no offsets): Partition the entire table. Example: if the API has 10 pages, return 10 partition dicts.
- **Stream** (`get_partitions(table_name, table_options, start, end)`): Partition only the offset range. Example: if `start={"page": 3}` and `end={"page": 7}`, return partitions for pages 4-7.

### Error Handling
- For batch reads, if `get_partitions` throws an exception, the framework automatically falls back to single-partition `read_table`. So it is safe to raise if partitioning is not feasible for a particular call.
- For streaming reads, there is no such fallback within a micro-batch — exceptions propagate. Use `is_partitioned()` to opt out specific tables instead.
