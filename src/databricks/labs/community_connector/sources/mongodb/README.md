# MongoDB community connector

Ingest MongoDB collections into Unity Catalog with the Lakeflow community
connector framework. Pure Python (`pymongo`) — runs on **serverless** Spark
Declarative Pipelines, unlike the JVM MongoDB Spark Connector.

> Community connector — community-maintained, not covered by Databricks SLAs.

## Capabilities

| `ingestion_type` | Behavior |
|---|---|
| `snapshot` | Full collection via `find()` each run (batch path, SCD from snapshot). |
| `cdc` | Gapless initial `_id` pagination, then a change-stream tail of insert/update/replace. |
| `cdc_with_deletes` | `cdc` plus deletes via change-stream delete events (pre-image when enabled). |
| `append` | Monotonic `_id` incremental, no change stream. |

The whole document lands in a single **VARIANT** column (`document`), emitted as a
JSON string and materialized by the framework via `VariantVal.parseJson`. Project
typed columns downstream in silver (`document:path::type`). Schema and metadata
are **deterministic** (no live sampling, no topology probing).

## Bronze schema

| Column | Type | Notes |
|---|---|---|
| `_id` | string | Document key (ObjectId rendered as hex). Primary key. |
| `_operation` | string | `snapshot` / `insert` / `update` / `replace` / `delete`. |
| `_cluster_time` | long | Change cluster time (sortable). Cursor field for CDC. |
| `_db` | string | Source database. |
| `_collection` | string | Source collection. |
| `document` | variant | Full document as VARIANT (STRING fallback on DBR < 17.1). |

## Connection options (`connector_spec.yaml`)

| Option | Required | Description |
|---|---|---|
| `connection_uri` | yes | Mongo SRV/standard URI incl. credentials. Store in a secret. |
| `database` | no | Default database; tables addressed by bare collection name. |
| `database_allowlist` | no | Comma-separated databases to expose in discovery. |
| `read_preference` | no | Default **`secondaryPreferred`** — reads (snapshot + change streams) run on secondaries so ingestion never loads the primary the app writes to. Override: `primary`, `primaryPreferred`, `secondary`, `nearest`. |
| `read_concern_level` | no | e.g. `majority` for consistent reads. |
| `retry_reads` | no | Default `true` (driver-level read retries on transient errors). |
| `max_staleness_seconds` | no | Bound secondary staleness (min 90). Recommended for strict "never load primary" setups (use `read_preference=secondary` + a staleness bound). |
| `server_selection_timeout_ms` / `connect_timeout_ms` / `socket_timeout_ms` | no | Connection timeouts (defaults 30000 / 20000 / **120000** — socket timeout is finite by design, unlike pymongo's infinite default). |
| `heartbeat_frequency_ms` / `max_pool_size` / `max_idle_time_ms` | no | SDAM heartbeat + per-executor pool sizing (defaults 5000 / 20 / 300000). Keep `max_pool_size × executors × members` under the Atlas tier connection limit. |
| `compressors` | no | Wire compression, e.g. `zstd,snappy,zlib`. |
| `tls` / `tls_ca_file` | no | Explicit TLS + CA bundle (UC Volume path) when not relying on the OS trust store. |
| `app_name` | no | Defaults `databricks-lakeflow-mongodb` (shows in Atlas Performance Advisor). |
| `pii_drop_fields` | no | Comma-separated dotted paths (e.g. `card.pan, member.ssn`) **dropped before landing** — for PCI fields that must never reach the lakehouse. |
| `pii_hash_fields` | no | Comma-separated dotted paths replaced with `sha256(salt + value)` (pseudonymized). |
| `pii_hash_salt` | no | Salt for `pii_hash_fields`. |

### Read preference — protect the source

Reads default to **`secondaryPreferred`** (Fivetran parity). For a hard "never load
the primary" requirement (e.g. a POS cluster the app writes to directly), set
`read_preference=secondary` + `max_staleness_seconds=120` — `secondaryPreferred`
silently falls back to the primary if all secondaries are unavailable (e.g. during
an election), which `secondary` avoids.

### Resilience — resume-token expiry

If the change-stream resume token ages out of the oplog (`ChangeStreamHistoryLost`,
error 286) after a long outage, the connector does **not** crash — it automatically
re-snapshots the affected collection and reopens the tail from a fresh cluster time
(gapless). Hard deletes that occurred entirely within the outage window may be
missed on the CDC path; run a `snapshot` reconciliation (which deletes rows absent
from the snapshot) if exact delete-consistency after a long outage is required.

## Table options (allowlisted)

`ingestion_type`, `database`, `max_records_per_batch` (default 1000),
`cdc_idle_ms` (change-stream idle window per micro-batch, default 1000),
`num_partitions` (parallel snapshot fan-out, default 8), `delete_mode`
(`hard` physical delete | `soft` `_operation=delete` rows), `schema_mode`.

### Operational

- **Parallel snapshot** — `snapshot` reads split the `_id` keyspace via `$bucketAuto`
  into `num_partitions` ranges, read in parallel across executors (large collections).
- **Resume-token recovery** — on `ChangeStreamHistoryLost` (oplog aged out), the
  connector auto re-snapshots and reopens the tail gaplessly (no crash).
- **Preflight** — `preflight(table, options)` checks replica-set / change-stream
  access (hard error for CDC on a standalone) + pre-image enablement (warning); wired
  best-effort into the pipeline driver.
- **Metrics** — per-batch CDC/delete throughput is logged as a structured line;
  row counts/throughput also come free from the SDP event log (`flow_progress`).

## Requirements

- **Replica set** (Atlas clusters qualify) — change streams require it.
- **`cdc_with_deletes`** needs `changeStreamPreAndPostImages` enabled on the
  collection to capture the deleted document's pre-image; otherwise deletes are
  emitted as key-only tombstones.
- **VARIANT** in the reader needs DBR 17.1+ (serverless current). Older runtimes
  fall back to a STRING `document` (use `parse_json` in silver).

## Semantics & limits

- Change streams are **at-least-once**; downstream upsert by `_id` (SCD Type 1)
  makes ingestion idempotent. Resume tokens are checkpointed in the offset.
- Reads converge under `Trigger.AvailableNow` (a micro-batch returns its input
  offset when no new events are available).
- CDC is a single ordered change-stream cursor (not partitioned). Snapshot of
  very large collections is read in one bounded-cursor pass; partitioned snapshot
  is a future enhancement.
- If a resume token ages out of the oplog, re-snapshot.

## Testing

```bash
# unit (no live Mongo)
uv run python -m pytest tests/unit/sources/mongodb -v
# live (replica set / Atlas)
MONGODB_TEST_URI='mongodb+srv://...' uv run python -m pytest tests/integration/mongodb -v
```

Built against framework commit `0e55073`. Supersedes the closed draft PR #56
(replaces its blocking `watch()` and non-deterministic schema/metadata).
