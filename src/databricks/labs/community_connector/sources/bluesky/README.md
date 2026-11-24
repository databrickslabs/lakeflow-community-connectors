# Lakeflow Bluesky Community Connector

Ingest raw Bluesky and AT Protocol repository events from Jetstream's sealed
archive into a triggered Databricks Lakeflow pipeline. The source name is
`bluesky`; its only object is `events`.

## Prerequisites

- A Databricks workspace with Unity Catalog and Community Connectors enabled,
  plus permission to create a connection, pipeline, and destination table.
- Network access from the pipeline to a Jetstream v2 HTTPS archive.
- A Jetstream archive bearer key for the public hosted endpoint. A self-hosted
  archive may allow anonymous reads.
- A triggered pipeline. Continuous pipelines are not supported.

The upstream PR includes a backward-compatible framework hook that checks for
Trigger.AvailableNow before connector I/O. A DBR 19 Dedicated test on 2026-11-17
completed three microbatches with `.trigger(availableNow=True)` using the generated
source. Maintainer review and a full Lakeflow pipeline deployment remain pending.

## Setup

### Required connection parameters

| Parameter | Type | Required | Description | Example |
| --- | --- | --- | --- | --- |
| `endpoint` | string | No | Jetstream v2 HTTPS origin; defaults to `https://jetstream.us-east.bsky.network`. Keep the same origin for an existing checkpoint. | `https://jetstream.us-east.bsky.network` |
| `api_key` | secret string | For the public hosted archive | Raw bearer key used for archive HTTP access. A self-hosted anonymous archive may omit it. | Secret supplied through Unity Catalog |
| `externalOptionsAllowList` | comma-separated string | Yes, when creating the connection directly | Permits every Bluesky-specific table option listed below. The Community Connector UI/CLI can populate this from `connector_spec.yaml`. | Full list below |

The complete Bluesky-specific `externalOptionsAllowList` value is:

```text
starting_cursor,collections,kinds,dids,max_events_per_refresh,request_timeout_seconds,refresh_timeout_seconds,max_retries,include_raw_payload
```

The framework may also add its own standard pipeline option names. Do not
replace the generated allowlist with only a subset of the names above.

### Obtain the archive key

Create an archive access key with the operator of the Jetstream endpoint you
will read. Store it as a secret in the Unity Catalog connection; pass the raw
key without a `Bearer ` prefix. The key is never a table option. If you use an
anonymous self-hosted endpoint, omit it.

### Create a Unity Catalog connection

Use the Community Connector flow from **Add Data**, select Bluesky, and enter
the endpoint and key. You can also create a `COMMUNITY` connection with the
standard Unity Catalog API or the Community Connector CLI using
`connector_spec.yaml`, with `sourceName=bluesky`. When creating the connection
directly, include the full `externalOptionsAllowList` value above so pipeline
table options reach the source. Keep the key in the connection's secret field.

## Supported objects

| Object | Ingestion | Key and cursor | Notes |
| --- | --- | --- | --- |
| `events` | Append-only | `seq` | Includes commit create/update/delete, identity, account, and sync events. |

The `seq` value is an instance-local event identifier. `record` and
`event_payload` contain JSON text for flexible AT Protocol payloads;
`raw_payload` optionally contains the exact archived DAG-CBOR bytes. Repository
events are not hydrated Bluesky views: profiles, threads, and engagement
counts require separate AppView enrichment. Delete and account markers are
appended as rows rather than applied as Lakeflow CDC deletes.

## Table configurations

### Source and destination

| Option | Required | Description |
| --- | --- | --- |
| `source_table` | Yes | Exactly `events`. |
| `destination_catalog` | No | Destination Unity Catalog catalog. |
| `destination_schema` | No | Destination schema. |
| `destination_table` | No | Destination table name; defaults to `events`. |

### Common `table_configuration` options

The pipeline's standard destination options (such as `primary_keys`,
`sequence_by`, and `cluster_by`) are consumed by Lakeflow. This source declares
append ingestion; configure the destination as `APPEND_ONLY` when supplying an
explicit `scd_type`.

### Bluesky-specific `table_configuration` options

Lakeflow commonly supplies strings; booleans from YAML and their textual
`true`/`false` forms are accepted for `include_raw_payload`. Pydantic validates
connector-owned values before a read. Framework metadata is left to Lakeflow.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `starting_cursor` | No | `0` | Exclusive sequence to use without a checkpoint. A zero start can take many refreshes to reach current data. |
| `collections` | No | all | Comma-separated collection NSIDs or namespace wildcards; applies to commits only. |
| `kinds` | No | all | Comma-separated subset of `commit`, `identity`, `account`, `sync`. Must include `commit` when using `collections`. |
| `dids` | No | all | Comma-separated repository DIDs. |
| `max_events_per_refresh` | No | `10000` | Maximum sequence span per microbatch, from 1 to 100,000. A run can contain multiple batches toward one fixed sealed target. Filtering can yield fewer rows. |
| `request_timeout_seconds` | No | `20` | Per-request timeout, from 1 to 120 seconds. |
| `refresh_timeout_seconds` | No | `120` | Planning/read budget, from 1 to 900 seconds. |
| `max_retries` | No | `3` | Additional attempts for transient HTTP failures, from 0 to 8. |
| `include_raw_payload` | No | `true` | `true` or `false`; retain exact event payload bytes. |

## Data type mapping

| Jetstream value | Databricks column/type | Meaning |
| --- | --- | --- |
| Event sequence | `seq BIGINT` | Incremental cursor and event key. |
| Indexed/witness time | `event_time TIMESTAMP`, `witnessed_at TIMESTAMP` | Display and immutable witness timestamps in UTC. |
| Repository and event metadata | `did`, `kind`, `operation`, `collection`, `rkey`, `cid`, `rev` as `STRING` | Identity and commit metadata; some fields are null for non-commit events. |
| Resync kind | `is_resync BOOLEAN` | Marks a resync replacement create. |
| Decoded payload | `record STRING`, `event_payload STRING` | JSON text; `record` is null for deletes and non-commit events. |
| Encoded payload | `raw_payload BINARY` | Optional exact DAG-CBOR event payload. |

## How to run

1. Install this source wheel, the matching Lakeflow Community Connectors
   framework, and the released `jetstream-lakehouse==0.2.0` from PyPI. PySpark
   comes from the Databricks runtime.
2. Create the connection above and add `events` to your pipeline spec. For a
   narrow first run, select one collection and use a recent sealed sequence
   from the **same** endpoint as `starting_cursor`.

```json
{
  "connection_name": "bluesky_jetstream",
  "objects": [{"table": {
    "source_table": "events",
    "destination_catalog": "main",
    "destination_schema": "bluesky",
    "destination_table": "events",
    "table_configuration": {
      "scd_type": "APPEND_ONLY",
      "starting_cursor": "0",
      "collections": "app.bsky.feed.post"
    }
  }}]
}
```

3. Run a small update, inspect the table and checkpoint, then schedule further
   updates. One update can now process multiple bounded batches toward the run's
   fixed sealed target. Preserve the checkpoint and endpoint. The standalone
   project includes a runnable [pipeline example](https://github.com/jesinity/lakeflow-connect-bluesky/blob/integration/jetstream-lakehouse/examples/ingest.py)
   and [SQL for current posts](https://github.com/jesinity/lakeflow-connect-bluesky/blob/integration/jetstream-lakehouse/examples/posts.sql).

### Best practices and troubleshooting

- If a pinned archive generation disappears, use the
  [compaction recovery runbook](compaction-recovery.md).
  It preserves the existing table and checkpoint while recovery runs into a new
  target.
- Start with a small `max_events_per_refresh` and a recent sequence. Filters
  can produce an empty table update while still advancing the checkpoint.
- Archive compaction can remove earlier events before the first read. Successful
  replay is not proof of a complete historical audit log.
- If a pinned segment is compacted before a retry, the connector fails without
  advancing the checkpoint. Investigate the original generation; do not reset
  the checkpoint into an existing append table without reviewing duplicates.
- Authentication failures indicate a missing/invalid archive key. A timeout
  can be addressed by narrowing the sequence span or increasing the bounded
  request budget within the documented limits.
- This reads sealed segments, so it lags the live Jetstream socket. It does
  not offer a continuous firehose or automatic failover between origins.

## References

- [Jetstream archive API research](bluesky_api_doc.md)
- [Connector specification](connector_spec.yaml)
- [Jetstream v2 overview](https://bsky.network/docs/jetstream/)
- [Standalone deployment and operations guide](https://github.com/jesinity/lakeflow-connect-bluesky)
