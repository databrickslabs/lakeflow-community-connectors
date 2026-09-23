# Lakeflow MongoDB Community Connector

**Instalación en minutos:** [QUICKSTART.md](QUICKSTART.md) — clonar, CLI, conexión UC y pipeline **solo con un YAML**.

**Playbook para IA / cliente:** [AI_INGESTION_PLAYBOOK.md](AI_INGESTION_PLAYBOOK.md) — la IA pregunta solo los valores dinámicos y crea el pipeline.

This documentation provides setup instructions and reference information for the MongoDB source connector.

The Lakeflow MongoDB Connector extracts data from MongoDB (including MongoDB Atlas) collections and loads it into Databricks. Every collection is ingested through **change streams**: the first trigger dumps existing documents, then later triggers watch the oplog for inserts, updates, replaces and deletes. Each document is stored as **VARIANT**.

## Features

- **Automatic collection discovery**: Every user collection in the configured database is exposed as an ingestible table.
- **Schema-stable envelope**: Each document is emitted as `_id`, `document` (VARIANT) and `event_time`, so heterogeneous collections keep a stable Spark schema.
- **BSON preservation**: Documents are serialised as MongoDB Extended JSON (Relaxed mode) and stored as VARIANT, preserving BSON types such as `ObjectId`, `Decimal128`, dates and binary data.
- **Change-stream CDC with deletes**: `cdc_with_deletes` on every collection. Inserts, updates and replaces are emitted even when `_id` does not change; deletes are applied from a separate flow.
- **Atlas ready**: Uses the official PyMongo driver with SRV connection strings. Requires a replica set or Atlas cluster.



## Prerequisites

- A MongoDB replica set or MongoDB Atlas cluster (standalone `mongod` is not supported).
- A database user with `find` and `changeStream` on the target database.
- Network access from Databricks to the MongoDB cluster. For Atlas, add the appropriate IP access list entry (or private endpoint) so Databricks can connect.



## Setup



### Connection Parameters


| Parameter        | Type            | Required | Description                                                             | Example                                       |
| ---------------- | --------------- | -------- | ----------------------------------------------------------------------- | --------------------------------------------- |
| `connection_uri` | string (secret) | Yes      | MongoDB connection string. For Atlas, the SRV string from the Atlas UI. | `mongodb+srv://user:pass@cluster.mongodb.net` |
| `database`       | string          | Yes      | Database to read collections from.                                      | `sample_mflix`                                |



### Table Options


| Option                  | Required | Description                                                                                              | Default |
| ----------------------- | -------- | -------------------------------------------------------------------------------------------------------- | ------- |
| `batch_size`            | No       | Number of documents PyMongo fetches per network round-trip. `0` uses the driver default.                 | `0`     |
| `max_records_per_batch` | No       | Maximum documents or change events returned per incremental read (microbatch size).                      | `1000`  |



### How to Obtain the Connection String

For MongoDB Atlas:

1. In the Atlas UI, open your cluster and click **Connect**.
2. Choose **Drivers**.
3. Copy the connection string (starts with `mongodb+srv://`).
4. Replace `<username>` and `<password>` with a database user's credentials.



## Data Model

Each collection produces rows with the following schema:


| Column       | Type      | Description                                                                                          |
| ------------ | --------- | ---------------------------------------------------------------------------------------------------- |
| `_id`        | string    | The document's `_id` rendered as a stable string.                                                    |
| `document`   | variant   | The full document as VARIANT (from MongoDB Extended JSON, Relaxed mode). Null on delete rows.        |
| `event_time` | timestamp | `clusterTime` of the oplog event, used as `sequence_by` for SCD2. Bootstrap rows use connector init. |


The primary key is always `_id`. The ingestion type is always `cdc_with_deletes`.

Query nested fields with VARIANT accessors, for example `document:name` or `document:_id:$oid`.

## Ingestion


Leave this spec in place and schedule the pipeline. The first trigger dumps the collection; later triggers only watch. A ready-to-run SDP notebook is [sample-ingest-change-stream.py](sample-ingest-change-stream.py).

```json
"table_configuration": {
  "scd_type": "SCD_TYPE_2"
}
```

Behaviour:

- **First run (bootstrap):** open `watch()`, capture a resume token, then scan the collection in `_id` pages of `max_records_per_batch`. Dump rows use `event_time` = connector init time so later oplog events sort after them in SCD2.
- **Later runs:** offset is `{"resume_token": "<Extended JSON>"}` and only the oplog is read.
- Bootstrap offset (while paging): `{"phase": "bootstrap", "resume_token": "...", "snapshot_id": "<_id Extended JSON>"}`.
- `event_time` (from `clusterTime` on stream events) is the sequencing column for `apply_changes`.
- Events with `clusterTime` after connector init are left for the next trigger (AvailableNow).
- Invalidating events (`drop`, `rename`, `invalidate`) and stale resume tokens fail the pipeline; full-refresh to recover.
- Delete rows contain `_id` and `event_time` only (`document` is null). Pre-images are not required. Deletes are not part of the dump.


## Limitations

- Change streams require a replica set / Atlas and only see events still in the oplog window after bootstrap.
- Nested fields are not flattened into columns; they remain inside `document`.
