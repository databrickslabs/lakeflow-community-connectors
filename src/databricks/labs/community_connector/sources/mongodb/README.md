# Lakeflow MongoDB Community Connector

This connector ingests MongoDB collections into Databricks. Each collection becomes one table: an initial snapshot, then continuous change data capture from the collection's [change stream](https://www.mongodb.com/docs/manual/changeStreams/), including deletes. It works with MongoDB Atlas and self-managed replica sets. End-to-end runs were done on serverless pipelines; classic pipelines run the same reader code. Sharded collections are not supported yet: the connector refuses them when it can detect them (`collStats`), because `_id` is only unique per shard unless it is the shard key.

## Prerequisites

- MongoDB 6.0 or later, deployed as a **replica set or sharded cluster** (change streams are not available on standalone servers; `ingestion_type=snapshot` works on any server). Tested with MongoDB 8.0 (self-managed replica set and Atlas).
- A database user that can `find`, `listCollections` and `changeStream` on the collections to ingest. The built-in `read` role on the database is enough.
- Network access from the compute that runs the pipeline to MongoDB:
  - Serverless compute reaches MongoDB over the public internet: add the [Databricks serverless outbound IPs](https://docs.databricks.com/aws/en/security/network/serverless-network-security/serverless-firewall) for your region to the MongoDB IP access list, or use private connectivity where your Databricks plan supports it.
  - Classic compute can also reach MongoDB over VPC peering or PrivateLink from the workspace VPC.
- Enough oplog retention to cover the longest time the pipeline may be stopped. If a stopped pipeline falls behind the oplog, the table needs a full refresh.

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `connection_uri` | string (secret) | Yes | MongoDB connection string. For SCRAM include user and password. For `auth_mode=x509` or `aws_service_credential`, give the host part only. | `mongodb+srv://ingest:****@cluster0.abcde.mongodb.net/` |
| `auth_mode` | string | No | `uri` (default), `x509` or `aws_service_credential` (see [Authentication](#authentication)). | `aws_service_credential` |
| `database` | string | No | Default database. When set, tables are the collections of this database. | `shop` |
| `tls_certificate_key_pem` | string (secret) | No | Client certificate and private key in PEM (`auth_mode=x509`). | |
| `tls_ca_pem` | string | No | CA certificate in PEM when the server certificate is not signed by a public CA. | |
| `aws_service_credential` | string | No | Unity Catalog service credential for `auth_mode=aws_service_credential`. | `atlas_ingest_role` |
| `databricks_host` | string | No | Workspace URL for `auth_mode=aws_service_credential`. | `https://dbc-1234abcd-5678.cloud.databricks.com` |
| `databricks_client_id` | string | No | Service principal application ID for `auth_mode=aws_service_credential`. | |
| `databricks_client_secret` | string (secret) | No | OAuth secret of that service principal. | |
| `aws_refresh_margin_seconds` | string | No | Request new AWS keys this many seconds before they expire. Default `600`. | `600` |
| `unicode_decode_error_handler` | string | No | `strict` (default) fails on strings that are not valid UTF-8; `replace` ingests them with U+FFFD. | `strict` |
| `externalOptionsAllowList` | string | Yes | Must be set to the full list of table options below. | `database,collection,ingestion_type,start_mode,start_at_operation_time,full_document,document_format,max_records_per_batch,snapshot_batch_size,max_await_time_ms,delete_lookback_seconds,idle_advance_seconds` |

### Authentication

| Method | `auth_mode` | `connection_uri` | Works on |
|---|---|---|---|
| SCRAM (user / password) | `uri` | `mongodb+srv://user:pass@host/` | Serverless, classic (any access mode) |
| X.509 client certificate | `x509` | `mongodb+srv://host/` + `tls_certificate_key_pem` | Serverless, classic (any access mode) |
| MONGODB-AWS with an instance profile | `uri` | `mongodb+srv://host/?authMechanism=MONGODB-AWS&authSource=%24external` | Classic **dedicated** access mode only |
| MONGODB-AWS with a Unity Catalog service credential | `aws_service_credential` | `mongodb+srv://host/` | Serverless, classic (any access mode) |

Why the last row exists: on standard access mode and serverless compute, user code cannot reach the EC2 instance metadata service, so the driver finds no AWS keys, and Unity Catalog pipelines must run on standard access mode or serverless. In `aws_service_credential` mode the connector asks Unity Catalog for temporary keys of a [service credential](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-services/use-service-credentials) through the [temporary service credentials API](https://docs.databricks.com/api/workspace/credentials/generatetemporaryservicecredential), authenticating as a service principal, and requests new keys before they expire.

Set it up as follows:

1. Create an AWS IAM role for the service credential and a [service credential](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-services/service-credentials) that uses it.
2. In Atlas, add a database user of type **AWS IAM → IAM Role** with that role's ARN and give it the `read` role on the database.
3. Create a service principal with an OAuth secret. Give it the **workspace-access** entitlement and `ACCESS` on the service credential (`GRANT ACCESS ON SERVICE CREDENTIAL ... TO ...`).
4. Create the connection with `auth_mode=aws_service_credential`, `aws_service_credential`, `databricks_host`, `databricks_client_id` and `databricks_client_secret`.

A service principal can hold up to five OAuth secrets. To rotate, add a new secret, update the connection, restart the pipeline, then delete the old secret.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `database,collection,ingestion_type,start_mode,start_at_operation_time,full_document,document_format,max_records_per_batch,snapshot_batch_size,max_await_time_ms,delete_lookback_seconds,idle_advance_seconds`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

Every collection of type `collection` (not views, not time series collections, not `system.*`) is a table. Tables are listed as namespaces (databases) and tables (collections). With the `database` connection parameter or table option, use the collection name as `source_table`; otherwise use `<database>.<collection>`. Prefer the `database` option when collection names contain dots.

Collection names with multibyte or other non-identifier characters (for example `顧客`, `注文-2024`, `🍣`) are supported through the `collection` table option: give the table an ASCII `source_table` and put the real name in `collection`. Database names, field names and values may use any Unicode characters without extra settings.

All tables have the same columns:

| Column | Type | Description |
|---|---|---|
| `_id` | string | Primary key. An ObjectId becomes its 24-character hex string; any other type becomes canonical Extended JSON (a string `_id` `"abc"` is stored as `"\"abc\""`), so different BSON types never collide. |
| `document` | variant | The full document as relaxed Extended JSON. `string` with `document_format=string`. |
| `_cdc_operation` | string | `snapshot`, `insert`, `update`, `replace` or `delete`. |
| `_cdc_sequence` | string | Cursor field and `sequence_by` column: the change stream resume token (hex), which is ordered across the deployment. Snapshot rows carry the token taken before the snapshot started. |
| `_cdc_cluster_time` | timestamp | Cluster time of the change. Null for snapshot rows. |
| `_cdc_wall_time` | timestamp | Wall-clock time of the change (MongoDB 6.0+). Null for snapshot rows. |
| `_cdc_namespace` | string | `<database>.<collection>`. |

### Ingestion types

Set with the `ingestion_type` table option.

| Value | Behavior |
|---|---|
| `cdc_with_deletes` (default) | Snapshot, then inserts, updates and replaces as upserts, and deletes as deletes. |
| `cdc` | Same without deletes: deleted documents stay in the table. |
| `append` | A change log: every snapshot row and change event, including deletes, is appended. |
| `snapshot` | Full read on every run. |

How it stays correct:

- The snapshot pages through the collection by `_id` in BSON sort order, so collections whose `_id` mixes types (ObjectId, strings, numbers) are read completely.
- The change stream starts from a resume token taken **before** the snapshot began. Changes made during the snapshot are replayed afterwards; because every row carries `_cdc_sequence`, `apply_changes` keeps the newest version whatever order the rows arrive in.
- Deletes are read by a separate flow. It starts `delete_lookback_seconds` in the past (at most back to the start of the oplog) so that no delete between the snapshot and the start of the delete flow is missed; older deletes have a smaller sequence than the snapshot rows and are ignored.
- A batch that Spark replays after a restart is re-read exactly between its two offsets, so `append` tables get no duplicates, when the source is registered with `spark.dataSource.register(MongodbDataSource)` (or `find_data_source("mongodb")`). See [Limitations](#limitations) for the single-file deployment.
- Each trigger reads changes up to the time at which it started (by the connector's clock), so `Trigger.AvailableNow` finishes even when the collection is written constantly; the next trigger continues from there.
- On a quiet collection the stored resume token is moved forward every `idle_advance_seconds`, so that a later pause is less likely to fall behind the oplog.
- A dropped or renamed collection, or a resume token that is no longer in the oplog, stops the pipeline with an error that says to run a full refresh.

## Limitations

- **Single-file deployment (`register(spark, "mongodb")`)**: the generic stream reader replays an uncommitted batch by reading again from its start offset, without the end offset. For `cdc` and `cdc_with_deletes` tables the extra rows are merged away by `apply_changes`. For `append` tables a replay can write some rows twice; remove them downstream by `(_cdc_namespace, _id, _cdc_sequence)`, which is unique per row (change events have unique resume tokens, and all snapshot rows of one collection share one token but differ in `_id`). During the initial snapshot of a collection whose `_id` values are not increasing (for example random strings), a replay can also miss documents of the replayed page when new documents were inserted inside its `_id` range before the restart; run a full refresh if a pipeline failed during the initial snapshot. Registering `MongodbDataSource` avoids both.
- **Delete flow start**: the delete flow starts `delete_lookback_seconds` before its own first run. Both flows start in the same pipeline update, so the gap is normally seconds. If the very first update failed after the snapshot began and was retried more than `delete_lookback_seconds` later, deletes in between can be missed: run a full refresh in that case.
- **Continuous mode latency**: each poll reads the changes up to the time it started; a change made while a poll runs is read by the poll after the next one.
- **`updateLookup`**: an update row holds the document as it is when the change is read (see Best Practices for post-images).
- Views, time series collections and `system.*` collections cannot be ingested (no change stream).
- Sharded collections are not supported yet (see above). Unsharded collections on a sharded cluster have not been tested.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Collection name (or `<database>.<collection>`) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### MongoDB `table_configuration` options

| Option | Default | Description |
|---|---|---|
| `database` | connection `database` | Database of the collection. |
| `collection` | `source_table` | Collection to read. Use it when the collection name is not a plain identifier (multibyte characters, spaces, `-`, emoji): set `source_table` to an ASCII alias such as `customers` and `collection` to the real name such as `顧客`. The pipeline builds internal view names from `source_table`, and those must contain only ASCII letters, digits and underscores. |
| `ingestion_type` | `cdc_with_deletes` | See [Ingestion types](#ingestion-types). Changing it needs a full refresh. |
| `start_mode` | `snapshot` | `snapshot`: initial load, then changes. `latest`: only changes from now on. `timestamp`: changes from `start_at_operation_time`. |
| `start_at_operation_time` | | Epoch seconds, for `start_mode=timestamp`. Must be within the oplog window. |
| `full_document` | `updateLookup` | `updateLookup` reads the current document for each update. `required` uses post-images (needs `changeStreamPreAndPostImages` on the collection) and gives the document as of the update, failing when one is missing. `whenAvailable` uses post-images when they exist and otherwise the current document. |
| `document_format` | `variant` | `variant` or `string` (JSON text). |
| `max_records_per_batch` | `10000` | Maximum rows per micro-batch. |
| `snapshot_batch_size` | `10000` | Documents per snapshot page. |
| `max_await_time_ms` | `1000` | How long one change stream poll waits for new events. |
| `delete_lookback_seconds` | `86400` | How far back the delete flow starts on its first run. When the oplog is shorter, it starts at the oldest point the oplog still holds. |
| `idle_advance_seconds` | `300` | How often the resume token of a quiet collection is moved forward. `0` disables it. |

## Data Type Mapping

The document is kept whole in `document` as relaxed Extended JSON:

| BSON type | In `document` |
|---|---|
| String | JSON string (UTF-8 kept as is, including Japanese, emoji and combining characters) |
| Int32, Int64, Double | JSON number |
| Decimal128 | `{"$numberDecimal": "..."}` |
| ObjectId | `{"$oid": "..."}` |
| Date | `{"$date": "2024-01-01T00:00:00Z"}` |
| Binary | `{"$binary": {"base64": "...", "subType": "..."}}` |
| Timestamp, Regex, MinKey, MaxKey, ... | Their Extended JSON form |
| Embedded document, array | JSON object, array |

Field names and collection names may contain any Unicode characters; they are preserved.

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

```json
{
  "pipeline_spec": {
    "connection_name": "mongodb_atlas",
    "object": [
      {
        "table": {
          "source_table": "orders",
          "table_configuration": {
            "database": "shop",
            "max_records_per_batch": "10000"
          }
        }
      },
      {
        "table": {
          "source_table": "order_history",
          "destination_table": "order_history_log",
          "table_configuration": {
            "database": "shop",
            "collection": "注文履歴",
            "ingestion_type": "append"
          }
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start small**: begin with one or two collections and check the row counts against MongoDB.
- **Size the oplog**: the oplog window must exceed the longest planned stop of the pipeline. Check it with `rs.printReplicationInfo()`.
- **Use post-images for exact history**: with `updateLookup`, an update row holds the document as it is when the change is read, which may already include later changes. For an exact per-change history (`append` or `SCD_TYPE_2`), enable `changeStreamPreAndPostImages` on the collection and set `full_document=required`: the server then returns the document as of each update, and the pipeline stops instead of guessing if a post-image has expired. Keep post-images (`changeStreamOptions.preAndPostImages.expireAfterSeconds`) for longer than the pipeline may be stopped. `whenAvailable` is best effort: when a post-image is missing, the connector falls back to the current document.
- **Read the document**: use `document:field::type` (for example `document:customer.name::string`) or `variant_get` to project fields in downstream tables. For field names that are not plain identifiers (multibyte characters, spaces, `-`), use brackets or backquotes: `document:['名前']::string`, ``document:`名前`::string`` or `variant_get(document, '$.名前', 'string')`.

#### Troubleshooting

| Error | Cause and fix |
|---|---|
| `Change streams need a replica set or a sharded cluster` | The server is standalone. Convert it to a replica set, or use `ingestion_type=snapshot`. |
| `... is no longer in the oplog ...` | The pipeline was stopped longer than the oplog window. Run a full refresh of the table. |
| `... ended with a 'drop' event ...` | The collection was dropped or renamed. Run a full refresh after it is recreated. |
| `... not valid UTF-8 ...` | A string in MongoDB has invalid UTF-8 bytes. Fix the data or set `unicode_decode_error_handler=replace`. |
| `temporary-service-credentials ... HTTP 403 ... workspace-access` | Give the service principal the workspace-access entitlement. |
| `[INVALID_IDENTIFIER] The unquoted identifier source_...` | The `source_table` contains characters that cannot be part of a view name. Use an ASCII `source_table` and set `collection` to the real collection name. |
| `ServerSelectionTimeoutError` / TLS handshake errors | The compute cannot reach MongoDB. Check the IP access list and the network path. |

## References

- [Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Resume a Change Stream](https://www.mongodb.com/docs/manual/changeStreams/#resume-a-change-stream)
- [Change Streams with Document Pre- and Post-Images](https://www.mongodb.com/docs/manual/changeStreams/#change-streams-with-document-pre--and-post-images)
- [PyMongo: AWS IAM authentication](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/security/authentication/aws-iam/)
- [Databricks: standard compute limitations](https://docs.databricks.com/aws/en/compute/standard-limitations)
- [Databricks: service credentials](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-services/use-service-credentials)
