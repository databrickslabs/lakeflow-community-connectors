# **MongoDB API Documentation**

MongoDB is not an HTTP API. The connector talks to a MongoDB deployment over the MongoDB wire protocol with [PyMongo](https://pymongo.readthedocs.io/). This document lists the server operations it uses.

## **Authorization**

| Method | How the connector authenticates |
|---|---|
| SCRAM-SHA-256 / SCRAM-SHA-1 | User and password in `connection_uri` ([docs](https://www.mongodb.com/docs/manual/core/security-scram/)) |
| X.509 | Client certificate from `tls_certificate_key_pem` ([docs](https://www.mongodb.com/docs/manual/core/security-x.509/)) |
| MONGODB-AWS | AWS keys: from the instance metadata service through the driver's default credential chain, or from a Unity Catalog service credential through the [temporary service credentials API](https://docs.databricks.com/api/workspace/credentials/generatetemporaryservicecredential) ([driver docs](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/security/authentication/aws-iam/)) |

Required privileges on the database: `find`, `listCollections`, `changeStream`. The built-in `read` role grants them.

## **Object List**

Objects are collections. Namespaces are databases.

| Operation | Command | Used for |
|---|---|---|
| List databases | `listDatabases` (`client.list_database_names()`) | `list_namespaces()`; `admin`, `config` and `local` are excluded |
| List collections | `listCollections` with `filter={"type": "collection"}`, `nameOnly`, `authorizedCollections` | `list_tables_in_namespace()`, `list_tables()`; views, time series collections and `system.*` are excluded because they have no change stream |

## **Object Schema**

Collections are schemaless. The connector does not sample documents; every collection has the same columns:

| Column | Type | Source |
|---|---|---|
| `_id` | string | document `_id` (ObjectId → hex, other types → canonical Extended JSON) |
| `document` | variant (or string) | full document as relaxed Extended JSON |
| `_cdc_operation` | string | `snapshot` or change event `operationType` |
| `_cdc_sequence` | string | change event `_id._data` (resume token) |
| `_cdc_cluster_time` | timestamp | change event `clusterTime` |
| `_cdc_wall_time` | timestamp | change event `wallTime` (MongoDB 6.0+) |
| `_cdc_namespace` | string | `<database>.<collection>` |

## **Get Object Primary Keys**

`_id` is the primary key of every collection; MongoDB enforces a unique index on it ([docs](https://www.mongodb.com/docs/manual/core/document/#the-_id-field)). `documentKey` of change events carries `_id` (plus the shard key on sharded collections; only `_id` is used).

## **Object's ingestion type**

| Type | Default | Reads |
|---|---|---|
| `cdc_with_deletes` | yes | snapshot, then `insert`/`update`/`replace` as upserts and `delete` as deletes |
| `cdc` | | the same without deletes |
| `append` | | every snapshot row and change event |
| `snapshot` | | full `find` on every run |

Cursor field: `_cdc_sequence`.

## **Read API for Data Retrieval**

| Phase | Command | Details |
|---|---|---|
| Start position | `aggregate` with `$changeStream` (`collection.watch`) and a `$match` that matches nothing | Its post-batch resume token is a position that consumes no event; taken before the snapshot |
| Snapshot | `find({"$expr": {"$gt": ["$_id", {"$literal": last_id}]}}).sort({"_id": 1}).limit(n)` | `$expr` compares across BSON types in sort order ([comparison order](https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/)) |
| Changes | `aggregate` with `$changeStream`, `resumeAfter` (or `startAtOperationTime`), `fullDocument` (`updateLookup` / `whenAvailable` / `required`), `$match` on `operationType`; then `getMore` with `maxAwaitTimeMS` | [Change streams](https://www.mongodb.com/docs/manual/changeStreams/), [resume](https://www.mongodb.com/docs/manual/changeStreams/#resume-a-change-stream) |
| Post-image fallback | `find_one({"_id": ...})` | only for `whenAvailable` when the event has no `fullDocument` |

Change events that end a stream (`drop`, `rename`, `dropDatabase`, `invalidate`) are matched so the connector can fail with a clear message. Server error codes handled: `286` (ChangeStreamHistoryLost), `280` (ChangeStreamFatalError), `40573` (change streams not supported on a standalone server).

There is no rate limit; batch size is bounded by `max_records_per_batch` and `snapshot_batch_size`.

## **Field Type Mapping**

| BSON | `document` (relaxed Extended JSON) |
|---|---|
| String | string, UTF-8 kept literally |
| Int32 / Int64 / Double | number |
| Decimal128 | `{"$numberDecimal": "..."}` |
| ObjectId | `{"$oid": "..."}` |
| Date | `{"$date": "...Z"}` |
| Binary | `{"$binary": {"base64": "...", "subType": "..."}}` |
| Embedded document / Array | object / array |

## Sources and References

- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Extended JSON v2](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/)
- [PyMongo ChangeStream](https://pymongo.readthedocs.io/en/stable/api/pymongo/change_stream.html)
- [Built-in role `read`](https://www.mongodb.com/docs/manual/reference/built-in-roles/#mongodb-authrole-read)
