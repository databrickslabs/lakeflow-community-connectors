# Jetstream archive API research for the Bluesky connector

This connector reads Jetstream **v2 sealed HTTP snapshots**, not the legacy live
WebSocket. The protocol analysis was checked against Jetstream revision
`3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d` and the current Lakeflow
interface revision `ddc153b9741b686759ad85f25d2d45ca8a6ac24b` used by this
project. Recheck both revisions before an upstream contribution.

## Supported source object

The connector exposes one `events` table. Its natural event identifier and
incremental cursor are the instance-local integer `seq`. The table is append-only:
commit create, update, and delete events, plus identity, account, and sync markers
are all rows. A delete marker is not a Lakeflow `read_table_deletes` operation.

The fixed columns are `seq`, `event_time`, `witnessed_at`, `did`, `kind`,
`operation`, `collection`, `rkey`, `cid`, `rev`, `is_resync`, `record`,
`event_payload`, and `raw_payload`. `record` and `event_payload` are JSON strings;
`raw_payload` is the optional exact DAG-CBOR event payload. This is raw repository
activity, not hydrated Bluesky AppView content.

## Authentication and endpoints

The default HTTPS origin is `https://jetstream.us-east.bsky.network`. The archive
transport supplies an optional raw bearer `api_key` in the Authorization header;
the public hosted archive requires a key, while a self-hosted archive may allow
anonymous reads. The connector requires an HTTPS origin without a path, query,
or embedded credentials.

| Method | XRPC path | Purpose |
| --- | --- | --- |
| `POST` | `/xrpc/network.bsky.jetstream.planSnapshot` | Plan a bounded sequence interval over sealed segments. |
| `GET` | `/xrpc/network.bsky.jetstream.getSegment?name=...` | Read a pinned byte range from a segment. |

The planner request uses `afterSeq` (exclusive) and `beforeSeq` (inclusive).
Optional `kinds`, `collections`, and `dids` narrow the source plan. A response
contains `sealedTipSeq`, `plannedThroughSeq`, and segment descriptors with a
name, index, checksum, and either a full-segment or selected-block mode. A plan
may need multiple calls before `plannedThroughSeq` reaches its sealed tip.

For `getSegment`, the reader sends `Range: bytes=<first>-<last>` and
`If-Match: "<plan checksum>"`. It requires a matching ETag and valid
`Content-Range`; a missing or changed pinned generation fails the read. The
`jetstream-lakehouse==0.1.0` package validates jss0/v1 metadata, the xxh3
checksum, Zstandard frame checksums, the block index, and DAG-CBOR payloads.
It decodes the selected blocks only.

## Incremental and replay contract

One `latest_offset` call plans at most `max_events_per_refresh` sequence
positions and pins the first sealed tip for that connector instance. The
checkpoint stores the candidate end sequence plus a serialized, checksummed
plan. Spark commits it only after its Delta batch succeeds. `get_partitions`
emits one ordered partition for that plan; `read_partition` replays precisely
the recorded segments without replanning. An empty or fully filtered interval
can still advance the sequence cursor. When the pinned tip is reached, the
same offset is returned so a triggered pipeline terminates.

Filtering is checked after decoding as well as in the planner. `collections`
applies only to commit events and requires `commit` in `kinds`. `dids` and
`kinds` apply to every event. The connector does not translate timestamps to
archive sequences and does not use the live socket's lookback behavior.

## Source constraints and errors

Sealed snapshots lag the live stream. Archive compaction can remove earlier
records before a first read, and no comprehensive retention floor is exposed.
The connector cannot certify a complete historical audit log. It fails closed
on explicit expired cursors, incompatible plans, checksum changes, missing
generations, invalid ranges, malformed blocks, and unknown event kinds. HTTP
requests have bounded timeouts and transient retries; redirects are rejected.

## Primary references

- [Jetstream v2 overview](https://bsky.network/docs/jetstream/)
- [Replay and snapshots](https://bsky.network/docs/jetstream-replay/)
- [Jetstream archive layout](https://github.com/bluesky-social/jetstream/blob/3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d/docs/README.md)
- [Snapshot lexicon](https://github.com/bluesky-social/jetstream/blob/3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d/lexicons/network/bsky/jetstream/planSnapshot.json)
- [Segment handler](https://github.com/bluesky-social/jetstream/blob/3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d/internal/xrpcapi/getsegment.go)
- [Lakeflow connector interface](https://github.com/databrickslabs/lakeflow-community-connectors/blob/ddc153b9741b686759ad85f25d2d45ca8a6ac24b/src/databricks/labs/community_connector/interface/lakeflow_connect.py)
