# OData v4 Source API Documentation

This connector targets the **OData v4** protocol as a generic data source. There is no fixed table list bundled with the connector — every table, schema, and primary key is derived at runtime from the target service's `$metadata` document. The same connector binary serves Microsoft Graph, Dynamics 365, SAP S/4HANA Cloud, SAP NetWeaver Gateway, Olingo-based self-hosted services, and the canonical `services.odata.org/V4/Northwind` reference service.

## Overview

### What this connector covers

- Any service that conforms to the OData v4 protocol and exposes a `$metadata` CSDL XML document at the service root.
- All entity sets declared by the service (one per `<EntitySet>` element under an `<EntityContainer>`).
- Snapshot ingestion (full refresh per trigger) and incremental CDC ingestion driven by a per-table cursor field.
- Multi-schema services that publish more than one `<Schema Namespace="...">` block — surfaced as Lakeflow namespaces.

### What this connector does not cover

- **OData v2 / v3.** The connector emits `OData-Version: 4.0` / `OData-MaxVersion: 4.0` headers and parses the v4 CSDL namespace `http://docs.oasis-open.org/odata/ns/edm`. Earlier-protocol services (the older `services.odata.org/V2/...` endpoints, classic SAP NetWeaver v2 endpoints) won't parse correctly.
- **OData functions and actions.** Only entity sets are exposed as tables. Bound and unbound function/action invocations are not surfaced.
- **Hard deletes (`cdc_with_deletes`).** OData v4 has no uniformly-implemented deletion feed (delta links and `@odata.deletedEntity` markers are optional and inconsistently supported). The connector always reports `ingestion_type` as `snapshot` or `cdc`, never `cdc_with_deletes`.
- **Parallel partitioning.** `@odata.nextLink` skiptokens are opaque to the client, so the read path is single-partition. Throughput is bounded by the source.

---

## Discovery model

Tables, schemas, and primary keys are not configured statically. They are pulled from the service's `$metadata` endpoint the first time any discovery or schema method is called, then cached for the lifetime of the connector instance.

### What is fetched

```
GET <service_url>$metadata
Accept: application/xml
```

The response is a CSDL XML document with this shape:

```xml
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="NorthwindModel" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Order">
        <Key>
          <PropertyRef Name="OrderID"/>
        </Key>
        <Property Name="OrderID" Type="Edm.Int32" Nullable="false"/>
        <Property Name="CustomerID" Type="Edm.String"/>
        <Property Name="OrderDate" Type="Edm.DateTimeOffset"/>
        ...
      </EntityType>
    </Schema>
    <Schema Namespace="ODataWeb.Northwind.Model">
      <EntityContainer Name="NorthwindEntities">
        <EntitySet Name="Orders" EntityType="NorthwindModel.Order"/>
        <EntitySet Name="Customers" EntityType="NorthwindModel.Customer"/>
        ...
      </EntityContainer>
    </Schema>
  </DataServices>
</edmx:Edmx>
```

### What is derived from it

| Lakeflow concept | Derived from |
| --- | --- |
| Namespace list | Distinct `Namespace` attribute on every `<Schema>` that contains an `<EntityContainer>` with entity sets. |
| Table list (per namespace) | `<EntitySet>` `Name` attributes inside each schema's `<EntityContainer>`. |
| Table schema | `<Property>` children of the `<EntityType>` referenced by the entity set's `EntityType` attribute. |
| Primary keys | `<PropertyRef Name="..."/>` children of the entity type's `<Key>` element. |
| Column types | `Type` attribute of each `<Property>`, mapped through the EDM → Spark table below. |
| Column nullability | `Nullable` attribute (default `true` when omitted). |

### Disambiguation

When the same `<EntitySet>` name appears in more than one `<Schema>` namespace, `_entity_type_for(...)` raises:

```
ValueError: Entity set 'Customers' is declared in multiple namespaces:
['HR', 'Sales']. Set 'namespace' in table_options to disambiguate.
```

The pipeline resolves this by passing `namespace` in `table_configuration` for the affected table (see *Per-table options* below). When a name is unique across the entire service, `namespace` may be omitted.

---

## Authentication

Authentication is configured on the Unity Catalog connection. The connector picks an auth method from `auth_type`, or — when `auth_type` is not set — infers `bearer` if a `token` option is present.

| Method | `auth_type` | Required option keys | Optional |
| --- | --- | --- | --- |
| Bearer token | `bearer` | `token` | — |
| HTTP Basic | `basic` | `username`, `password` | — |
| API key in custom header | `api_key` | `api_key` | `api_key_header` (default `x-api-key`) |
| OAuth 2.0 client-credentials | `oauth2` | `oauth2_token_url`, `oauth2_client_id`, `oauth2_client_secret` | `oauth2_scope` |

Notes:

- **Bearer.** Sent as `Authorization: Bearer <token>`. Works for most modern OData APIs (Microsoft Graph, Dynamics 365, SAP S/4HANA Cloud).
- **Basic.** Sent as `Authorization: Basic <base64(user:pass)>` via `requests.auth.HTTPBasicAuth`. Common for on-prem SAP NetWeaver / Gateway.
- **API key.** Sent as `<header-name>: <key>`. The header name defaults to `x-api-key` and is configurable per service via `api_key_header`.
- **OAuth2.** A token is fetched at session-construction time with `grant_type=client_credentials` against `oauth2_token_url`. The resulting access token is cached on the session for the lifetime of the connector instance and resent as `Authorization: Bearer <token>`. No refresh is performed; a fresh instance is constructed on every trigger.

Tokens, passwords, API keys, and OAuth client secrets are all declared `secret: true` in `connector_spec.yaml` and are masked by the Unity Catalog connection store.

---

## Connection parameters

These are set on the UC connection (alongside the auth fields above).

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `service_url` | string | Yes | — | OData v4 service root URL. Must end at the service segment; the connector appends entity-set names and `$metadata` directly. Example: `https://services.odata.org/V4/Northwind/Northwind.svc/`. |
| `timeout_seconds` | string | No | `60` | HTTP timeout per request, in seconds. |
| `extra_headers` | string | No | — | Extra request headers as `Key:Value,Key2:Value2`. Useful for tenant IDs, CSRF tokens, or non-standard server discriminators. |

The connector additionally sends these headers on every request:

```
Accept: application/json
OData-Version: 4.0
OData-MaxVersion: 4.0
```

`$metadata` requests override `Accept` to `application/xml`.

---

## Per-table options

These are passed to the connector via the pipeline's `table_configuration` block. Every key listed below must appear in `external_options_allowlist` on the UC connection (the spec already includes all six).

| Option | Default | Description |
| --- | --- | --- |
| `namespace` | — | Selects the `<Schema Namespace="...">` block that declares this entity set. Required only when the same entity-set name appears in multiple schemas. |
| `cursor_field` | — | Column to drive incremental reads. Absent → snapshot. Must be a top-level property of the entity type and naturally ordered by OData `$orderby` (timestamps and monotonic IDs are typical). |
| `select` | all properties | Comma-separated `$select` projection. Both the on-wire OData query and the derived Spark schema are filtered to these columns. |
| `filter` | — | Additional OData `$filter` expression, AND-ed with any cursor filter the connector generates. |
| `page_size` | `1000` | Value of `$top` sent on each HTTP request. Sets the maximum rows per OData page. Some servers cap this server-side (see *Known limits*). |
| `max_records_per_batch` | `5000` | Client-side cap on records returned per `read_table` call. The connector truncates and returns control to the framework once this limit is hit. Independent of `page_size`. |

`namespace` is consumed by the connector before the request is built; the other five all influence the URL or the per-batch loop.

---

## Incremental ingestion contract

When a table's `table_configuration` includes `cursor_field`, the connector switches from snapshot mode to CDC mode. This section is the contract for that mode.

### Query shape

Per batch, the connector issues a request of the form:

```
GET <service_url><entity_set>
  ?$top=<page_size>
  &$select=<select>                          (optional)
  &$filter=(<user filter>) and (<cursor filter>)
  &$orderby=<cursor_field> asc, <pk1> asc, <pk2> asc, ...
```

The cursor filter is:

| State | `$filter` clause for the cursor |
| --- | --- |
| First call (no checkpoint) | `<cursor_field> le <init_ts>` |
| Resume after checkpoint `since` | `<cursor_field> gt <since> and <cursor_field> le <init_ts>` |

`<init_ts>` is captured once when the connector instance is constructed (a fresh instance is created on every trigger). This freezes the upper bound for the duration of one trigger so a long-running read never chases newly-inserted data past the boundary it started with.

### Why primary keys are appended to `$orderby`

Without a fully-unique total ordering, OData servers that paginate internally with a value-based skiptoken (the spec allows opaque tokens of any shape) can split a same-cursor cohort across pages: the server's skiptoken applies strict-`>` semantics on the cursor value alone and silently drops the unread tail. Appending every primary-key column to `$orderby` forces the skiptoken to include the key in its tie-break, so no rows are lost mid-cohort.

### Boundary trim

Every batch, after reading up to `max_records_per_batch` rows, the connector inspects the trailing run of records that share the boundary cursor value and **drops the entire trailing cohort** (function `_trim_to_distinct_cursor_boundary`). The next call resumes from the last *distinct* cursor value seen, not the literal last row.

This trim runs on every batch, not just truncated ones, for two reasons:

1. If the trailing cohort is split across pages, dropping it lets the next call's `cursor gt <prev_distinct>` re-fetch the complete cohort, including the un-read tail.
2. If concurrent writers insert sibling rows with the same cursor value before the next call, those siblings would otherwise be lost — a `cursor gt <last>` filter strictly excludes them. Re-fetching from `<prev_distinct>` picks them up.

Re-fetched rows arriving in subsequent batches are deduped at the destination by `apply_changes` doing a MERGE on the primary key. **This is why CDC mode requires a real primary key in the entity type's `<Key>` element.** A service whose entity type has no `<Key>` will surface as `primary_keys=[]` and incremental ingestion to a Delta table will accumulate duplicates.

### Edge case: every record in the batch shares one cursor value

If `max_records_per_batch` is too small to contain even one same-cursor cohort, the trim returns an empty list. Two paths:

- **Truncated batch** (more records exist on the same cursor value): the connector raises a `RuntimeError` instructing the operator to raise `max_records_per_batch` above the largest same-cursor cohort, or choose a higher-cardinality cursor field.
- **Natural exhaustion** (the server returned no `@odata.nextLink`): the records are emitted as-is. A residual race exists for same-cursor rows inserted between this call and the next — unavoidable without finer cursor resolution.

### Implication for low-cardinality cursors

A date-only cursor (`Edm.Date`) or a one-second-resolution timestamp on a busy table tends to produce large same-cursor cohorts. That's fine — the boundary trim and PK-based MERGE handle it — but operators must size `max_records_per_batch` above the largest expected same-cursor cohort. Picking a finer-resolution cursor (`Edm.DateTimeOffset` with sub-second precision, or a monotonic surrogate key) is the cleanest fix when available.

### Snapshot mode

When `cursor_field` is not set, the connector walks `@odata.nextLink` from the initial `$top=<page_size>` request until the server stops returning a next link, accumulates the full row set, and returns it in one batch. No cursor filter is applied. No primary keys are appended to `$orderby` (none is sent at all).

The OData v4 spec allows `@odata.nextLink` to be either an absolute URL or a relative one resolved against the request URL. Some services (SAP NetWeaver Gateway, certain self-hosted Olingo deployments) return only `Customers?$skiptoken=...`. The connector resolves these via `urllib.parse.urljoin` against `resp.url`, so absolute links pass through unchanged and relative links are prepended with the service root.

### OData control properties

Every row returned to the framework has had OData control properties stripped: keys prefixed with `@odata.` (e.g. `@odata.etag`, `@odata.id`, `@odata.editLink`) are not yielded.

---

## Type mapping

EDM primitive types are mapped to Spark types as follows. Any unrecognized type falls back to `StringType` (the raw JSON representation is preserved on the wire).

| EDM type | Spark type | Notes |
| --- | --- | --- |
| `Edm.String` | `StringType` | |
| `Edm.Boolean` | `BooleanType` | |
| `Edm.Byte` | `ByteType` | Unsigned 8-bit in EDM, signed `ByteType` in Spark. |
| `Edm.SByte` | `ByteType` | |
| `Edm.Int16` | `ShortType` | |
| `Edm.Int32` | `IntegerType` | |
| `Edm.Int64` | `LongType` | |
| `Edm.Single` | `FloatType` | |
| `Edm.Double` | `DoubleType` | |
| `Edm.Decimal` | `DecimalType(38, 18)` | Fixed precision/scale regardless of CSDL-declared precision. |
| `Edm.Date` | `DateType` | Calendar date, no time component. |
| `Edm.DateTime` | `TimestampType` | OData v2 carryover; some v4 services still emit it. |
| `Edm.DateTimeOffset` | `TimestampType` | The standard v4 timestamp type. |
| `Edm.TimeOfDay` | `StringType` | No native Spark `TimeType`. |
| `Edm.Duration` | `StringType` | ISO 8601 duration text. |
| `Edm.Guid` | `StringType` | |
| `Edm.Binary` | `BinaryType` | Base64-encoded on the wire; downstream callers can use `_decode_binary` to materialize bytes. |

Complex types, enum types, and navigation properties are not surfaced — only `<Property>` elements of the entity type are emitted as fields.

---

## Worked example: Northwind

The canonical public OData v4 reference service is `https://services.odata.org/V4/Northwind/Northwind.svc/`. Its `$metadata` declares two schemas:

- `NorthwindModel` — entity types.
- `ODataWeb.Northwind.Model` — the entity container with entity sets `Customers`, `Orders`, `Order_Details`, `Products`, etc.

Because only one schema (`ODataWeb.Northwind.Model`) contains the `<EntityContainer>` with entity sets, the discovery layer returns a single namespace.

### Connection (UC)

```bash
community-connector create_connection odata northwind_connection \
  -o '{
        "service_url": "https://services.odata.org/V4/Northwind/Northwind.svc/"
      }' \
  --spec ./src/databricks/labs/community_connector/sources/odata/connector_spec.yaml
```

(The public Northwind service requires no auth. Real-world services need one of the auth blocks from the *Authentication* section.)

### Pipeline (`ingest.py`)

```python
from databricks.labs.community_connector.pipeline import build_pipeline
from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect

build_pipeline(
    connector_cls=ODataLakeflowConnect,
    tables=[
        # Snapshot ingest — no cursor.
        {
            "table": {
                "source_table": "Customers",
            }
        },
        # Incremental CDC ingest — cursor on OrderDate.
        {
            "table": {
                "source_table": "Orders",
                "primary_keys": ["OrderID"],
                "table_configuration": {
                    "cursor_field": "OrderDate",
                    "max_records_per_batch": "5000",
                    "page_size": "500",
                },
            }
        },
    ],
)
```

### What happens at runtime for `Orders`

1. The connector instance is constructed and captures `init_ts` (current UTC time, ISO-8601).
2. `read_table_metadata("Orders", ...)` reads `$metadata`, finds `EntityType="NorthwindModel.Order"`, returns `primary_keys=["OrderID"]`, `cursor_field="OrderDate"`, `ingestion_type="cdc"`.
3. `get_table_schema("Orders", ...)` returns a `StructType` with `OrderID: int`, `CustomerID: string`, `OrderDate: timestamp`, `ShippedDate: timestamp`, etc., derived from the `<Property>` children of `NorthwindModel.Order`.
4. First call to `read_table` has no `start_offset`. The URL is:
   ```
   .../Orders?$top=500
            &$filter=(OrderDate le <init_ts>)
            &$orderby=OrderDate asc, OrderID asc
   ```
5. Rows stream in via `@odata.nextLink` pagination. The connector accumulates up to 5000 rows.
6. The boundary trim runs. Many Northwind orders share an `OrderDate` (date-precision), so the trailing same-day cohort is dropped. The end offset is the last *distinct* `OrderDate` seen.
7. Next call resumes with `OrderDate gt <prev_distinct> and OrderDate le <init_ts>`. The previously-dropped same-day cohort is re-fetched. `apply_changes` MERGEs them by `OrderID`, so the destination has each order exactly once.

### Why `OrderDate` works as a cursor even though many rows share each date

Northwind `OrderDate` is a date-precision field — dozens of orders can share the same date. Without the boundary trim, a `gt` filter on the next call would skip every order sharing the boundary date. With the trim, the cohort is re-read every batch and MERGE-deduped at the destination. The only sizing requirement is that `max_records_per_batch` (5000 above) exceeds the largest single-day order count — easily true for Northwind.

If `max_records_per_batch` were set to, say, `10`, the connector would raise `RuntimeError` the first time a single `OrderDate` exceeded 10 orders, with a message instructing the operator to either raise the cap or pick a higher-cardinality cursor.

---

## Known limits

- **Server-side `$top` caps.** Some services cap `$top` below the requested value (Microsoft Graph at 999 for most endpoints; certain SAP services at 5000). The connector trusts the server: it asks for `$top=<page_size>` and follows whatever pagination the server emits. If the effective page size is smaller than requested, throughput drops but correctness is unaffected.
- **Opaque `$skiptoken` stability requires a unique total `$orderby`.** As described in *Incremental ingestion contract*, the connector unconditionally appends every primary-key column to `$orderby` in CDC mode. In snapshot mode no `$orderby` is sent and the server's pagination is followed as-is.
- **Relative `@odata.nextLink`.** Handled — resolved against the response URL via `urljoin`. Absolute links pass through unchanged.
- **No CDC deletes.** `ingestion_type` is never `cdc_with_deletes`. OData v4's delta links and `@odata.deletedEntity` markers are optional and inconsistently implemented; this connector does not synthesize tombstones. Soft-deleted rows must be modeled as updates to a status/`is_deleted` column on the entity itself.
- **Single-partition reads.** Skiptokens are opaque, so the connector can't safely split a read across partitions. Throughput is bounded by the source.
- **Schema cache.** `$metadata` is fetched once per connector instance and cached in-memory. Schema drift mid-run is not detected; a new trigger picks up the new shape.
- **Functions / actions not exposed.** Only `<EntitySet>` declarations become tables. Bound and unbound OData functions and actions are ignored.
- **Cursor field must be top-level and orderable.** The connector sends `$orderby=<cursor_field> asc` literally. Complex-typed properties, navigation properties, and computed expressions are not valid cursors.
- **OData v2 / v3 not supported.** The connector parses the v4 CSDL XML namespace and emits v4 protocol headers.
