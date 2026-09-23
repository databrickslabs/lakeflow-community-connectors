# MongoDB Source API Notes

Technical reference for the MongoDB connector implementation. MongoDB is
accessed through the official PyMongo driver over the MongoDB wire
protocol, not a REST API.

## Access method

- Driver: `pymongo` (with the `srv` extra for Atlas `mongodb+srv://` URIs).
- Auth: credentials are embedded in the `connection_uri` connection string.
- Client lifecycle: a fresh `MongoClient` is created per operation and
  closed afterwards. Clients are never cached on the connector instance
  because Spark pickles the connector to ship it to executors and a live
  `MongoClient` (background monitoring threads + sockets) is not
  picklable.

## Collection discovery

- `list_tables()` calls `db.list_collection_names()` on the configured
  database and filters out internal `system.*` collections.
- Each remaining collection is treated as one ingestible table.

## Reading

Every collection is ingested via change streams
(`ingestion_type=cdc_with_deletes`). Snapshot and field-based cursor CDC
are not supported.

### Change streams

- `collection.watch(pipeline=[{"$match": {"operationType": {"$in":
  [...]}}}], max_await_time_ms=1000, ...)`.
- Upserts (`read_table`): on an empty offset, capture a resume token
  from `watch().try_next()`, then dump the collection with
  `find({_id: {$gt: last}}).sort(_id).limit(max_records)`. Dump rows
  use `event_time = _init_dt`. When a page is short, the offset
  becomes `{resume_token}` (streaming). While paging:
  `{phase: bootstrap, resume_token, snapshot_id}`.
- After bootstrap, upserts are `insert` / `update` / `replace` with
  `full_document="updateLookup"`. A null `fullDocument` (racy delete) is
  skipped; the delete flow covers it. The resume token still advances.
- Deletes (`read_table_deletes`): `operationType=delete`. No collection
  dump. Rows are `{_id, document: null, event_time}`. The framework
  runs this as a separate stream with its own offset. The first delete
  watch (empty offset) starts at `_init_dt`.
- Resume: `resume_after` from `start_offset["resume_token"]` (Extended
  JSON).
- Drain uses `ChangeStream.try_next()` so an empty getMore after
  `max_await_time_ms` ends the microbatch instead of blocking.
- Events with `clusterTime` later than `_init_dt` are not emitted
  (AvailableNow cap). The offset stays on the last in-cap event.
- Streaming offset: `{"resume_token": "<Extended JSON of event _id>"}`.
- `cursor_field` metadata is `event_time` (from `clusterTime`) for
  `sequence_by`.
- Invalidating ops (`drop`, `dropDatabase`, `rename`, `invalidate`) raise
  `ValueError`. Stale tokens and standalone servers (no replica set) also
  raise `ValueError`.
- Retired table options (`cdc_mode`, `cursor_field`, `cursor_type`,
  `start_timestamp`) raise `ValueError` if set.

## Schema and BSON handling

- Envelope schema: `_id STRING`, `document VARIANT` (nullable),
  `event_time TIMESTAMP`.
- `_id` is rendered with `str()` — for `ObjectId` this yields the 24-char
  hex string.
- The full document is serialised with `bson.json_util.dumps` using
  Relaxed Extended JSON. The framework converts that string to VARIANT
  via `VariantVal.parseJson`, which round-trips BSON types:
  - `ObjectId` -> `{"$oid": "..."}`
  - `Decimal128` -> `{"$numberDecimal": "..."}`
  - dates -> `{"$date": "..."}`
  - binary -> `{"$binary": {...}}`
- The envelope keeps the Spark schema deterministic regardless of
  per-document field/type drift, which is common in MongoDB collections.

## Limitations

- Change streams require a replica set / Atlas and only cover events
  still in the oplog after the first-run dump.
- No nested-field flattening; nested data stays inside `document`.
- No server-side projection; bootstrap scans the entire collection.
