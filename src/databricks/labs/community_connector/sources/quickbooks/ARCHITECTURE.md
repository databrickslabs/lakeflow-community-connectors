# QuickBooks connector architecture

This document captures the initial architectural decisions and the invariants
that must hold as the connector moves from snapshot ingestion to CDC.

## Accepted starting decisions

### One realm per connection

One Unity Catalog COMMUNITY connection represents one Intuit authorization and
one QuickBooks `realm_id`. The connector never multiplexes credentials or
companies inside one connection.

### OAuth stays outside the connector

The connector consumes an injected access token. It does not persist or rotate
refresh tokens and does not implement a browser callback.

The production path is Unity Catalog managed U2M. Spark executors receive only
an injected access token, and logs never contain OAuth client credentials or
refresh tokens.

### At-least-once source delivery

Uncommitted batches may be replayed. Every table uses QuickBooks `Id` as its
stable primary key so destination merges remain idempotent.

### Typed core plus lossless raw payload

The connector exposes stable identity, source metadata, common analytical
fields, and the complete source object in `raw_json`.

### Serial reads within one table

The first version does not partition positional QuickBooks query pages across
Spark executors. Parallel page reads can drift while the source changes and can
amplify API throttling.

## Implemented M3 incremental design

Each table has an independent versioned offset containing its tenant binding,
flow identity, and committed source watermark:

```json
{
  "version": 2,
  "realm_id": "1234567890",
  "table_name": "customers",
  "flow": "updates",
  "updated_through": "2026-07-25T10:00:00Z"
}
```

Each connector instance freezes its initialization timestamp. That timestamp is
the upper bound for the whole AvailableNow run, allowing it to terminate even
when QuickBooks is being updated concurrently.

During each table's bootstrap:

1. Capture the CDC boundary before the first snapshot request.
2. Emit the complete positional snapshot.
3. Commit the captured boundary only if Spark successfully commits the batch.
4. On the next trigger, replay the configured overlap below that boundary so
   changes racing the snapshot are included.

During incremental reads:

1. Query an inclusive `MetaData.LastUpdatedTime` lower and upper bound.
2. Default to a 60-second lower-bound overlap and a one-day maximum window.
3. Paginate the entire bounded query with `STARTPOSITION` / `MAXRESULTS`.
4. Return a new `updated_through` only for the bounded upper timestamp.
5. Let Spark commit that end offset only after the batch succeeds.
6. Merge replayed records by QuickBooks `Id`, sequencing by
   `last_updated_at`, so replay is idempotent.

An ID tie-breaker is not used. QuickBooks query filters permit equality and
`IN` for `Id`, but not range comparisons, and the query language does not
support `OR`. A timestamp overlap therefore protects equal-timestamp
boundaries without relying on an unsupported `(timestamp, Id)` range cursor.

Customers, vendors, accounts, and items advertise `cdc`. Invoices and bills
advertise `cdc_with_deletes`. Checkpoints remain isolated by table and by
normal/delete flow through Spark's per-flow state.

## Implemented M4 deletion design

QuickBooks has two materially different removal models:

| Tables | QuickBooks behavior | Destination behavior |
|---|---|---|
| customers, vendors, accounts, items | Soft delete by setting `Active=false` | Keep the row and ingest `active=false` as an update |
| invoices, bills | Permanent transaction delete | Apply a Lakeflow tombstone keyed by `id` |

List queries explicitly include `Active IN (true, false)`. Without this
predicate QuickBooks defaults to active records, which would make inactive
objects disappear from a fresh snapshot without producing a delete event.

Invoice and Bill delete flows call the QuickBooks CDC endpoint independently
from the bounded Query API update flow. They:

1. Query a five-minute bootstrap lookback to cover snapshot/delete-flow startup
   races.
2. Replay a 60-second overlap after each committed delete checkpoint.
3. Filter CDC changes to `status=Deleted`.
4. Emit a schema-complete tombstone whose non-null fields include `id`,
   `last_updated_at`, and `raw_json`.
5. Advance to the QuickBooks response `time` only after Spark commits the
   batch.

QuickBooks CDC has a 30-day lookback horizon and a 1,000-object response
ceiling. Its API exposes `changedSince` but no upper-bound parameter, so a
saturated response cannot be safely subdivided client-side. The connector
therefore fails without returning a new checkpoint when either limit makes
coverage uncertain. Recovery is an explicit full reconciliation followed by
checkpoint reset; production schedules must poll frequently enough to avoid
these conditions.

## Open decisions

- Confirm how Databricks community OAuth surfaces Intuit's callback `realmId`.
- Re-evaluate the default overlap duration using production latency evidence.
- Decide whether `raw_json` should become `VARIANT` before public release.
- Decide which QuickBooks entities require specialized typed schemas.
- Automate the full-reconciliation runbook for expired or saturated delete
checkpoints.

## Implemented M5 tenant-isolation design

Every normalized row and delete tombstone carries a non-null `realm_id`.
Lakeflow metadata and pipeline specifications use `(realm_id, id)` as the
composite primary key. QuickBooks IDs only have meaning inside one company, so
this prevents identical source IDs in different realms from colliding in a
shared destination.

Production deployments still use a separate schema, Unity Catalog connection,
pipeline, and checkpoint location per realm. The composite key is defense in
depth and permits intentional consolidation; it does not replace Unity Catalog
authorization.

Version-2 offsets bind state to the realm, table, and update/delete flow. The
connector rejects a checkpoint if any binding differs.

M4 offsets are version 1 and cannot be adopted implicitly. Migration uses a new
schema and pipeline bootstrap rather than mutating an existing checkpoint.
