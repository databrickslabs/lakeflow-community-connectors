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
- **OData functions, actions, and singletons.** Only entity sets are exposed as tables. Bound and unbound function/action invocations are not surfaced, and `<Singleton>` container children (single-entity roots like `/Me`) are not discovered or readable.
- **The `cdc_with_deletes` ingestion type.** The connector always reports `ingestion_type` as `snapshot` or `cdc`. Deletes **are** captured when the server supports change tracking and `delta_tracking` is opted in: server-reported removals arrive as in-band tombstone rows (`_deleted=True`, see [Delta tracking](#delta-tracking-contract)). Without delta tracking (cursor/snapshot reads), deletes are not observable — soft-deleted rows must be modeled as updates to a status/`is_deleted` column on the entity itself.
- **Parallel partitioning of flat entity sets.** `@odata.nextLink` skiptokens are opaque to the client, so a single collection's page walk is serial. Contained-collection paths on the N+1 shape **do** partition — top-level rows are bin-packed into `num_partitions` slices, each Spark task walking its own subtrees (`SupportsPartitionedStream`; see the README's `num_partitions` row).
- **Multipart `$batch`.** The `$batch` optimizations (`contained_fetch=batch`, `cursor_probe=batch`) use the OData **JSON batch format** (`{"requests":[…]}`) exclusively, not the older multipart/MIME format. A server that supports only multipart `$batch` fails the capability preflight, so `auto` silently degrades to plain per-parent GETs and strict `contained_fetch=batch` raises an actionable error — reads are always correct, just unaccelerated.
- **Contained navigation properties named with `__`.** The connector's table-name syntax joins containment segments with `__`, and only declared entity-SET names get longest-prefix disambiguation — a *navigation property* whose own name contains `__` (legal CSDL) cannot be addressed. Discovery skips such collections with a warning instead of listing tables the read path would reject.
- **Query-carrying service roots.** `service_url` must be a bare service root; a query string or fragment (`https://host/svc?sap-client=100`, the SAP Gateway client-selection form) is rejected at construction with a curated error — every URL builder appends entity paths to the root, which would land inside the query and die cryptically at the `$metadata` fetch. SAP client selection works via the header form instead (`extra_headers="sap-client: NNN"`).
- **Cross-origin pagination and redirects.** The connector refuses to follow an `@odata.nextLink` — or a server 3xx **redirect** (`allow_redirects=False`; same-origin redirects are followed manually, off-origin ones raise) — whose scheme/host/port differs from `service_url`. The credential-bearing session must never send its `Authorization`, `api_key`, or `extra_headers` credentials off-origin (a server-supplied off-host link or `Location` would otherwise exfiltrate them; `requests`' own cross-host protection strips only `Authorization`). A service that legitimately paginates or redirects across hosts is unsupported. A 3xx the follow loop *can't* act on (no `Location`, or a non-redirect 3xx like 300/304) raises immediately with the status named, rather than dying later as a bare body-parse error.

**Same-instant cursor matching note (maintainers).** The park-resume identity and the cursor-probe preflight tolerate rendering variance of one instant (`…00Z` vs `…00.000Z` vs `…+00:00`, and IEEE754Compatible numeric-string flips like `5000` vs `"5000"`) via `cursor_same_instant`. One residue is inherently undecidable: a server rendering **naive local time** (no offset — spec-violating for `Edm.DateTimeOffset` JSON) from a non-UTC zone pins to the wrong UTC instant, so its parked cursor never matches and the capped walk re-walks that parent each batch until the rendering stabilizes — no loss, bounded duplicates, not fixable without the server's zone.

**Ordering-key collation note (maintainers).** The park-resume seeks (capped-walk boundaries on the N+1 and `$expand` paths) never trust Python ordinal comparison of text ordering keys: the server orders `$orderby` by ITS collation (case-insensitive SQL Server defaults, `uniqueidentifier` byte-group order, ICU locales), which the client cannot reproduce, and a wrong "already walked" guess is silent subtree loss. Order is only decided client-side for JSON numbers, ISO-rendered instants, and numeric rendering flips (`_order_reproducible`); everything else seeks on the parked row's identity anchor, and a vanished anchor costs one empty batch plus a duplicate-safe full re-walk (logged), never loss (for cdc tables the duplicates dissolve in the destination MERGE; for snapshot tables the snapshot flow's full-refresh semantics absorb them). Near-ISO garbage in a cursor column (dash-shaped but unparseable text) stays raw-ordered and can even order cyclically against parsed instants — the documented degraded shape-mixed domain: bounded duplicates, never a provable-order skip.

**Emit-boundary padding note (maintainers).** Every emitted row is padded to the declared schema with explicit `None` for absent columns (servers may legally omit null-valued properties). A side effect: a *connector-side* stamping bug (a forgotten ancestor-FK or cursor stamp) that previously failed loudly in the framework's absent-column check now degrades to silent `None`s in those columns — keep the stamping paths covered by row-content assertions, not just parse-success assertions.

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
| OAuth (UC-managed) | *(none — omit `auth_type`)* | Connection created with `community_oauth_flow=m2m`/`u2m` + `client_id`, `client_secret`, `token_endpoint` | `oauth_scope`; `authorization_endpoint` (u2m) |

Notes:

- **Bearer.** Sent as `Authorization: Bearer <token>`. Works for most modern OData APIs (Microsoft Graph, Dynamics 365, SAP S/4HANA Cloud).
- **Basic.** Sent as `Authorization: Basic <base64(user:pass)>` via `requests.auth.HTTPBasicAuth`. Common for on-prem SAP NetWeaver / Gateway.
- **API key.** Sent as `<header-name>: <key>`. The header name defaults to `x-api-key` and is configurable per service via `api_key_header`.
- **OAuth (UC-managed).** The connector contains no OAuth code. The Unity Catalog COMMUNITY connection (created with `community_oauth_flow=m2m` or `u2m`) runs the OAuth flow, refreshes the token **server-side** — including rotated refresh tokens — and injects a fresh `access_token` into the connector's options at query time. The connector detects the injected token (no `auth_type` set) and sends it as an opaque bearer credential. There is no connector-side refresh: a token that expires mid-read surfaces as a curated 401 `PermissionError` (the next query start gets a freshly injected token). The retired connector-side `auth_type=oauth2` mode raises a `ValueError` carrying the migration steps.

Tokens, passwords, API keys, and OAuth client secrets are all declared `secret: true` in `connector_spec.yaml` and are masked by the Unity Catalog connection store.

---

## Connection parameters

These are set on the UC connection (alongside the auth fields above).

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `service_url` | string | Yes | — | OData v4 service root URL. Must end at the service segment; the connector appends entity-set names and `$metadata` directly. Example: `https://services.odata.org/V4/Northwind/Northwind.svc/`. Must **not** embed credentials (`user:pass@host` userinfo — rejected at init, since the URL is echoed in logs and error messages); use the auth options instead. |
| `timeout_seconds` | string | No | `180` | HTTP timeout per request, in seconds. |
| `extra_headers` | string | No | — | Extra request headers as `Key:Value,Key2:Value2`. Useful for tenant IDs, CSRF tokens, or non-standard server discriminators. The list splits on `,`, so header values containing commas (e.g. HTTP-dates) can't be expressed. Header names are validated eagerly as RFC 7230 tokens (malformed names fail at setup with the option named, not on the wire). |
| `metadata_cache_ttl_seconds` | string | No | `60` | TTL of the shared `$metadata` cache — both the process dict and the on-disk pickle honour it; `0` disables both. |
| `max_retries` | string | No | `5` | Retry budget for transient failures (408/429/5xx, network errors) on source requests. Backoff is exponential with 50–100 % jitter; server `Retry-After` hints are honoured un-jittered. |
| `retry_max_delay_seconds` | string | No | `60` | Cap on any single retry sleep (exponential backoff / `Retry-After`). |
| `verbose_http_logging` | string | No | `false` | Log every request and response line at INFO. Source data lands in the log stream — debugging only. |
| `verbose_http_log_body_chars` | string | No | `500` | Response-body prefix length included in verbose logs. |

The connector additionally sends these headers on every request:

```
Accept: application/json
OData-Version: 4.0
OData-MaxVersion: 4.0
```

`$metadata` requests override `Accept` to `application/xml`.

---

## Per-table options

These are passed to the connector via the pipeline's `table_configuration` block. Every key listed below must appear in `external_options_allowlist` on the UC connection (the spec already includes all of them).

| Option | Default | Description |
| --- | --- | --- |
| `namespace` | — | Selects the `<Schema Namespace="...">` block that declares this entity set; the schema's `Alias` is accepted interchangeably. Required only when the same entity-set name appears in multiple schemas. |
| `cursor_field` | — | Column to drive incremental reads. Absent → snapshot. Must be naturally ordered by OData `$orderby` (timestamps and monotonic IDs are typical). On flat tables, the column must be a property of the entity. On contained paths, the connector first checks the leaf entity for the column; if missing, it walks leaf→root to find the **closest ancestor** that has it, filters at that ancestor level, and propagates the ancestor's cursor value onto every emitted leaf row. |
| `select` | all properties | Comma-separated `$select` projection. Both the on-wire OData query and the derived Spark schema are filtered to these columns. On contained paths, synthetic ancestor-FK columns (default form `<seg>_<pk>`) are always preserved regardless of `select`. |
| `filter` | — | Additional OData `$filter` expression, AND-ed with any cursor filter the connector generates. Applies to the leaf collection only on contained paths. Spaces and non-ASCII are auto-encoded, but URI-reserved chars inside a string literal must be percent-encoded by the caller (`%`→`%25`, `&`→`%26`, `#`→`%23`, `+`→`%2B`) — left raw they truncate the query (the connector can't encode them without double-encoding a pre-encoded value). |
| `page_size` | `1000` | Value of `$top` sent on each HTTP request. Sets the maximum rows per OData page. Some servers cap this server-side (see *Known limits*). |
| `max_records_per_batch` | `10000` | Client-side cap on records returned per `read_table` call. The connector truncates and returns control to the framework once this limit is hit. Independent of `page_size`. |
| `delta_tracking` | `disabled` | Opt-in OData v4 delta queries. `disabled` keeps the existing snapshot / cursor behavior; `auto` probes the server's `Prefer: odata.track-changes` support once per table and falls back if missing; `enabled` requires support and errors on the first read if the server doesn't acknowledge. See [Delta tracking](#delta-tracking-contract). Mutually exclusive with contained-path tables. |
| `expand_contained` | `auto` | For contained-collection paths (`Parent__Child__...`). `auto` (default) preflights the server's nested-`$expand` support and uses a single `GET Parent?$expand=Child(...)` when verified, else the N+1 per-parent traversal; `true` forces the `$expand` read, `false` forces N+1. See [Contained navigation properties](#contained-navigation-properties). |

The table above covers the core options only. The full allowlisted set also
includes `pagination`, `cursor_probe`, `contained_fetch`, `num_partitions`,
`cursor_nulls`, `filters_at`, `exclude_ancestor_columns`,
`cursor_lookback_seconds`, `cursor_lookback_factor`,
`cursor_lookback_max_seconds`, and `cursor_lookback_dedup` — the **authoritative, complete option
reference is the [README's Per-table options section](README.md)**.

`namespace` is consumed by the connector before the request is built; the rest all influence the URL, the per-batch loop, or the request semantics.

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
| First call (no checkpoint) | *(no cursor filter)* — server returns rows from the natural start of the table |
| Resume after checkpoint `since` | `<cursor_field> gt <since>` |

There is **no wall-clock ceiling** on the cursor. `max_records_per_batch` is the only per-call cap. Two consequences:

* **Continuous SDP pipelines work.** A single connector instance can live for the entire stream and still see fresh source state on every micro-batch, because the connector never freezes a "snapshot at startup" timestamp that would shut out later-arriving rows.
* **The cursor column doesn't have to be a timestamp.** Monotonic integer IDs, lexicographic strings — anything the server can order in `$orderby` and compare in `$filter` works the same way. The connector emits the cursor value verbatim using `_odata_literal`, so an `Edm.Int32` cursor produces `OrderID gt 10248` (no quotes), an `Edm.DateTimeOffset` cursor produces `ModifiedAt gt 2024-03-01T00:00:00Z`, and so on. One caveat: the cursor-watermark path is value-sniffed (watermarks round-trip through offsets and may be synthetic floors, not properties), so an `Edm.Guid` cursor renders **quoted** in the `gt` filter — accepted by many services but rejected by strict stacks; prefer a timestamp or integer cursor there. (Key predicates and keyset-seek boundaries are metadata-typed and render guids bare — see the N+1 section.)

### Why primary keys are appended to `$orderby`

Without a fully-unique total ordering, OData servers that paginate internally with a value-based skiptoken (the spec allows opaque tokens of any shape) can split a same-cursor cohort across pages: the server's skiptoken applies strict-`>` semantics on the cursor value alone and silently drops the unread tail. Appending every primary-key column to `$orderby` forces the skiptoken to include the key in its tie-break, so no rows are lost mid-cohort.

### Boundary trim

Every batch, after reading up to `max_records_per_batch` rows, the connector inspects the trailing run of records that share the boundary cursor value and **drops the entire trailing cohort** (function `_trim_to_distinct_cursor_boundary`). The next call resumes from the last *distinct* cursor value seen, not the literal last row.

This trim runs on every batch, not just truncated ones, for two reasons:

1. If the trailing cohort is split across pages, dropping it lets the next call's `cursor gt <prev_distinct>` re-fetch the complete cohort, including the un-read tail.
2. If concurrent writers insert sibling rows with the same cursor value before the next call, those siblings would otherwise be lost — a `cursor gt <last>` filter strictly excludes them. Re-fetching from `<prev_distinct>` picks them up.

Re-fetched rows arriving in subsequent batches are deduped at the destination by `apply_changes` doing a MERGE on the primary key. **This is why CDC mode requires a real primary key in the entity type's `<Key>` element.** A service whose entity type has no `<Key>` will surface as `primary_keys=[]` and incremental ingestion to a Delta table will accumulate duplicates. On the lookback-overlap paths the connector additionally drops *unchanged* re-fetches before they are emitted at all (`cursor_lookback_dedup`, on by default — an exact `{PK → content-hash}` seen-set riding the offset), so the destination MERGE only sees genuine changes; the MERGE remains the correctness backstop for every re-emit the dedup layer lets through.

### Edge case: every record in the batch shares one cursor value

If `max_records_per_batch` is too small to contain even one same-cursor cohort, the trim returns an empty list. Two paths:

- **Truncated batch** (more records exist on the same cursor value): the connector raises a `RuntimeError` instructing the operator to raise `max_records_per_batch` above the largest same-cursor cohort, or choose a higher-cardinality cursor field.
- **Natural exhaustion** (the server returned no `@odata.nextLink`): the records are emitted as-is. A residual race exists for same-cursor rows inserted between this call and the next — unavoidable without finer cursor resolution.

### Implication for low-cardinality cursors

A date-only cursor (`Edm.Date`) or a one-second-resolution timestamp on a busy table tends to produce large same-cursor cohorts. That's fine — the boundary trim and PK-based MERGE handle it — but operators must size `max_records_per_batch` above the largest expected same-cursor cohort. Picking a finer-resolution cursor (`Edm.DateTimeOffset` with sub-second precision, or a monotonic surrogate key) is the cleanest fix when available.

### Snapshot mode

When `cursor_field` is not set, the connector walks `@odata.nextLink` from the initial `$top=<page_size>` request until the server stops returning a next link, streaming rows lazily one page at a time (the full result set is never materialized in memory). No cursor filter is applied. A PK-only `$orderby` **is** sent whenever the entity declares keys — OData v4 §11.2.5.7 gives no stable default ordering across pages, so skiptoken pagination needs an explicit total order.

The OData v4 spec allows `@odata.nextLink` to be either an absolute URL or a relative one resolved against the request URL. Some services (SAP NetWeaver Gateway, certain self-hosted Olingo deployments) return only `Customers?$skiptoken=...`. The connector resolves these via `urllib.parse.urljoin` against `resp.url`, so absolute links pass through unchanged and relative links are prepended with the service root.

### OData control properties

Every row returned to the framework has had OData control properties stripped: keys prefixed with `@odata.` (e.g. `@odata.etag`, `@odata.id`, `@odata.editLink`) are not yielded.

---

## Delta tracking contract

OData v4 §11.3 ("Requesting Changes") defines a server-driven change-tracking protocol. When opted into via `delta_tracking ∈ {auto, enabled}` and supported by the server, the connector takes this path instead of cursor-based filtering.

### Capability detection

`delta_tracking=auto` performs a one-time probe per `(namespace, table)` pair; the definitive verdict is persisted in the process/file capability cache (15-minute TTL) so schema inference and the streaming read — which run in different forked workers — resolve to the same answer even against a server whose acknowledgement flaps. The probe sends an entity-set GET with `$top=1` and the header `Prefer: odata.track-changes`. The connector inspects the response:

- 200 + `Preference-Applied: odata.track-changes` header → delta supported. Cached.
- 200 + missing `Preference-Applied` header → server silently ignored the prefer. Falls back to whatever cursor/snapshot config is set. Cached.
- non-200 status (400/405 commonly) → server rejected the prefer. Falls back. Cached.
- Hard, non-transient failure (definitive rejection) → falls back. Cached `False`.
- Transient failure (transport error, retryable status incl. 408, non-JSON body) → falls back **for this batch only**; nothing is cached and the next call re-probes.

`delta_tracking=enabled` skips the probe entirely. If the actual bootstrap response is missing `Preference-Applied`, the connector raises a `RuntimeError` pointing the operator at `delta_tracking=disabled` as the fallback.

`delta_tracking=disabled` (the default) never sends the prefer header. Zero behavior change versus pre-delta versions of the connector.

### Offset shape

Three offsets coexist with the existing `{}` (snapshot) and `{"cursor": ...}` (cursor-based) shapes:

- `{"delta_link": "<url>"}` — ready to resume from the server's last-minted delta link.
- `{"next_link": "<url>", "delta_link": "<url>"}` — mid-pagination after a `max_records_per_batch` cap hit. `next_link` is the preferred resume; `delta_link` is the fallback if `next_link` expires.
- `{}` — start a fresh bootstrap (initial run or post-410 reset).

The dispatch in `read_table` recognises any of these and routes through the delta path even if `delta_tracking` is no longer set in `table_options` — checkpointed offsets carry the mode forward across config changes.

### Request shape

Bootstrap (first call, no checkpointed delta state):

```
GET <service_url><entity_set>
Prefer: odata.track-changes[, odata.maxpagesize=<page_size>]
```

No `$top` is ever sent on the delta path: OData `$top` is a **total-result** limit (§11.2.5.3), so it would end change tracking at `page_size` rows and permanently drop the rest of the table from the bootstrap. An explicit user `page_size` is forwarded as `Prefer: odata.maxpagesize` — the spec's per-response sizing hint — instead.

**Driver-memory note.** Unlike the flat / N+1 / expand read shapes — which stream lazily one page at a time under a `LakeflowBatchReader` (full-refresh / snapshot) read — the delta path **accumulates the batch's rows in memory** before returning, because it must reach the terminal `@odata.deltaLink` and stamp `_lc_sequence` ordering across the whole change set. In *streaming* this is bounded by `max_records_per_batch` (enforced at page boundaries). In *batch mode* (a pipeline snapshot-refresh / full-refresh of a delta table) the cap is effectively unlimited, so the **entire entity set is materialized on the driver** for the bootstrap. Budget driver memory accordingly for a large delta table's first full read, or bootstrap it once via a cursor/snapshot table and switch to `delta_tracking` afterward — the same driver-memory caveat as `num_partitions` planning on millions of parents.

Resume (`delta_link` or `next_link` in offset):

```
GET <stored_link>
```

The delta / next links are server-minted opaque URLs; the connector follows them verbatim without re-applying `$filter` / `$orderby` / `$top` from `table_options`.

### Response handling

Each page in the response's `value` array is one of:

- A regular entity → emitted with all `@odata.*` keys stripped, `_deleted=False`, and a fresh `_lc_sequence`.
- A tombstone → emitted with only the primary-key fields populated, `_deleted=True`, and a fresh `_lc_sequence`. Both wire shapes are recognized: the v4.01 `@removed` control property (`{"@removed": {"reason": "deleted"}, ...}`) and the v4.0 `$deletedEntity`-context entry. Keys come from inline properties when present (Microsoft Graph style), else are parsed from the `@odata.id`/`id` entity reference (single, composite `K1=v1,K2=v2`, quoted-string and bare-guid forms, coerced by declared Edm type so they MERGE-match the upserts). A tombstone whose primary keys cannot be resolved raises rather than silently losing the deletion. `@removed` with `reason: "changed"` is treated as a delete (with a server-side `filter` it means the row left the filtered set). Following the `microsoft_teams` precedent, deletions are surfaced in-band rather than via `cdc_with_deletes` + `read_table_deletes`.

The terminal page carries `@odata.deltaLink` (the next resume point). Intermediate pages carry `@odata.nextLink`.

### Synthetic columns

Two columns are appended to the declared schema for delta-active tables:

| Column | Type | Purpose |
| --- | --- | --- |
| `_deleted` | `BooleanType` (non-null) | In-band tombstone flag. `True` only for `@removed` entries; `False` for adds and changes. |
| `_lc_sequence` | `StringType` (non-null) | `read_table_metadata` reports this as `cursor_field`. Format: `<20-digit zero-padded nanoseconds-since-epoch>_<12-digit counter>`. Strictly monotonic per emit per process, so `apply_changes` MERGE-by-PK picks deterministic winners when the same primary key appears multiple times in one batch. |

### Graph deltaLink-rotation guard

Some servers (notably Microsoft Graph) mint a fresh `@odata.deltaLink` on every response, even when the change set is empty. Without compensation, every trigger would advance the offset and the framework would emit empty Delta commits in perpetuity.

The connector detects this case: if a resume call started with `prev_delta_link != None` and produced zero records, it returns the prior link unchanged so `end_offset == start_offset`. The framework treats that as "no progress this trigger" and `Trigger.AvailableNow` terminates cleanly.

### Token expiry (HTTP 410)

When the server returns 410 Gone on a stored link, the connector recovers silently in two tiers: a 410 on a parked mid-pagination `next_link` first retries the retained prior `delta_link` (replaying only the changes-since window); a 410 on the `delta_link` itself re-bootstraps — a fresh `Prefer: odata.track-changes` GET against the entity set, the full snapshot re-emitted as `_deleted=False` upserts, and a brand-new `delta_link`. MERGE-by-PK at the destination reconciles re-fetched rows with what's already there — but rows DELETED at the source while the token was expired are never propagated (the re-bootstrap emits only current rows as upserts; no tombstone arrives for the gap), so the destination retains them until a full refresh. A non-410 4xx on a stored link (gateways answering 404/400 for an expired token) raises an actionable error naming the full-refresh remedy instead of auto-re-bootstrapping. Conversely, a server that returns change records with the SAME `@odata.deltaLink` as the prior batch — or with the terminal delta link omitted entirely — raises a no-progress error (the stream would otherwise re-read that change set forever).

### Sparse-update rejection

OData v4 §11.4 allows delta payloads to return only the *modified* properties on an updated entity. Applying that as-is would write NULLs over good destination values — silent corruption. The connector refuses such payloads.

Detection runs on **every** non-tombstone entry (mixed payloads — full entities for creates, changed-properties-only for updates — are the norm, so first-entry sampling would wave sparse updates through). The expected key set is precomputed once per walk:

- The full declared schema, minus the synthetic `_deleted` / `_lc_sequence` columns.
- Filtered to the `$select` projection if set.

If any expected key is missing from the actual entry, the connector raises `RuntimeError` and points the operator at `delta_tracking=disabled` (or `$select` to narrow the schema).

### Mutual exclusion with `cursor_field`

`delta_tracking=enabled` plus `cursor_field` is a `ValueError` at first metadata-resolution call. The two are conflicting sequencing strategies. `delta_tracking=auto` plus `cursor_field` falls through to the cursor path (cursor wins, no probe).

### Worked example (Microsoft Graph users/delta)

Trigger 1, no offset:

```
GET https://graph.microsoft.com/v1.0/users
Prefer: odata.track-changes
→ 200, Preference-Applied: odata.track-changes
  body: {"value": [...all current users...], "@odata.deltaLink": "https://...users?$deltatoken=A"}
```

Emitted: every user as `_deleted=False` with monotonic `_lc_sequence`.
Offset: `{"delta_link": "https://...users?$deltatoken=A"}`.

Trigger 2, after a user changed their `displayName` and another was deleted:

```
GET https://graph.microsoft.com/v1.0/users?$deltatoken=A
→ 200
  body: {"value": [
    {"id": "u1", "displayName": "New Name", ...},
    {"@removed": {"reason": "deleted"}, "id": "u2"}
  ], "@odata.deltaLink": "https://...users?$deltatoken=B"}
```

Emitted: one row for `u1` with full payload (`_deleted=False`), one row for `u2` carrying only `id` (`_deleted=True`).
Offset: `{"delta_link": "https://...users?$deltatoken=B"}`.

Trigger 3, no changes since `B`:

```
GET https://graph.microsoft.com/v1.0/users?$deltatoken=B
→ 200
  body: {"value": [], "@odata.deltaLink": "https://...users?$deltatoken=C"}
```

Emitted: zero rows. Offset: `{"delta_link": "https://...users?$deltatoken=B"}` — prior link preserved by the rotation guard, so the framework sees no progress and the trigger terminates.

---

## Contained navigation properties

OData v4 §13.4.3 defines `<NavigationProperty ContainsTarget="true">` on an EntityType: a collection that is *owned by* the parent entity rather than declared as a top-level EntitySet. The contained collection is addressed by traversing the parent's key — `GET Parent(<key>)/ContainedNavProp` — and each parent has its own independent contained collection. The protocol allows recursive containment, so a service can declare `Parent → Child → Grandchild → ...` chains.

The connector surfaces these as double-underscore-pathed tables (`__` between segments — slash isn't valid in Spark SQL identifiers, which the framework uses for view names) alongside top-level entity sets, e.g. `Parents__Children__Notes`, up to **10 segments deep** (the depth cap prevents pathological discovery walks on services that declare circular containment; cycles within the cap are also detected and broken). Path parsing rejects empty segments and over-depth paths at `read_table_metadata` / `get_table_schema` time. A name whose longest prefix matches a declared top-level entity set is split there: a set legally named `My__Set` is read flat, and its contained collections (`My__Set__Kids`) resolve with `My__Set` as the root segment — a declared flat set always shadows the containment-path interpretation of the same spelling.

### Discovery

`list_tables_in_namespace([<schema>])` returns both:

- Top-level entity sets declared in the schema's `<EntityContainer>`.
- Every contained-collection path reachable from those sets via a BFS through `ContainsTarget="true"` navigation properties (inherited from base types too), capped at `MAX_CONTAINED_DEPTH = 10` and with cycle detection on the type-qualified name set.

Output is deterministic — flat sets sorted first, then contained paths sorted.

### Schema augmentation

For a path with N segments, the leaf entity's own properties are preceded by synthetic FK columns for **every non-leaf ancestor**. OData v4 §13.4.3 makes contained-entity keys unique only within their immediate parent, so the destination composite key must include the full chain to be globally unique. The default name is `<segment>_<pkname>` (no fixed prefix). When that name would collide with a leaf property or with another FK, the connector prepends a leading `_` until the name is unique.

```
<parent_segment>_<parent_pkname...>   ← primary keys of the leaf's IMMEDIATE parent
<leaf's own properties>
```

The composite primary key reported in `read_table_metadata` is the full chain: every ancestor's FK columns followed by the leaf's own primary keys. This is what makes `apply_changes_from_snapshot` see one row per key on tables where leaf IDs only repeat within a grandparent branch (a common case in services like Intergraph SCApi).

When an ancestor has a composite primary key, every key column gets its own `<seg>_<pk>` field. The URL traversal passes through every ancestor's keys (the OData wire path is `A(a)/B(b)/C(c)/D`), and every ancestor's keys are also materialised as columns on the destination D rows.

**Collision example.** If `Items` has its own property `Owners_Id` and the path is `Owners__Items`, the connector emits `_Owners_Id` (FK, leading underscore) and `Owners_Id` (the leaf's own property, untouched). With multiple collisions, more leading underscores are added until unique.

`select` on a contained path filters only the leaf entity's own properties — every ancestor's FK columns are always preserved (the resolved names are compared against the leaf-only set, not against the input `select` list).

### Read modes

Selected via `expand_contained`:

**N+1 traversal (`expand_contained=false`; `auto`'s fallback).** For a path `A/B/C/D`:

1. `GET A?$select=<A_pks>&$top=<page_size>` — enumerate top-level parent keys.
2. For each `A_key`: `GET A(<A_key>)/B?$select=<B_pks>` — enumerate level-2 parents.
3. For each `(A_key, B_key)`: `GET A(<A_key>)/B(<B_key>)/C?$select=<C_pks>` — enumerate level-3.
4. For each `(A_key, B_key, C_key)`: `GET A(<A_key>)/B(<B_key>)/C(<C_key>)/D?<query>` — fetch leaves.

Pagination (`@odata.nextLink`) walks happen *within* each per-parent fetch. Cost is O(product of parent fanouts) HTTP round trips; bandwidth is proportional to leaf row count plus a small overhead for the PK-only enumerations.

Key predicate quoting: single-key parents use the bare form `(value)`; composite-key parents use the named form `(K1=v1,K2=v2)`. Quoting is decided by the property's **declared Edm type** from `$metadata` (`odata_literal_typed`): `Edm.Guid` (and numeric/date types the server may render as JSON strings) emit **bare** per the OData v4 ABNF — strict stacks (Olingo, SAP) 400 on a quoted guid predicate — while `Edm.String` keys are **always** single-quote-escaped and quoted, even when the value happens to look like a timestamp (`'2024-01-01'`). The same typed rendering covers the per-leaf-parent PK `$filter` and keyset-seek boundaries; only where the type can't be resolved does the older value-sniff (quote anything non-ISO-looking) apply. Properties typed via a `<TypeDefinition>` resolve to their `UnderlyingType` first, so an `Edm.String`-backed definition quotes like any string.

**Single `$expand` chain (`expand_contained=true`; `auto` — the default — on a preflight-verified server).** One HTTP request per pipeline trigger:

```
GET A?$select=...&$top=...&$expand=B($expand=C($expand=D))
```

The connector flattens the nested JSON response recursively: for each top row, descend into the named nav-property array on each level, extracting and propagating ancestor PK values until the leaf level is reached. `@odata.*` control properties are stripped from leaf rows during flattening (the top-level `_fetch_pages` strip is applied only to outermost rows).

Under a `cursor_field` or a `max_records_per_batch` cap, this expand read is not a single request — it runs as a **depth-first resumable stack machine** that streams leaf rows and can span multiple `read_table` calls. Between batches it parks only a **boundary path from the root to the current leaf** into the offset's `pending_fetches` key (a bottom-to-top list of `{url, level, chain, cur_val, skip, boundary}` items), so the serialized offset is **O(path depth), not O(fan-out width)** — a single wide parent with thousands of children still parks a depth-sized offset. Resume re-pushes those items to reconstruct the walk and skips already-emitted rows by each item's chronological `boundary` order-key (churn-safe; positional `$skip` is only a downgrade fallback). Inner-collection `<NavProp>@odata.nextLink` continuations and 404/410 deleted-parent / stale-token recoveries are carried in the same structure. No explicit ceiling is needed — depth-first descent holds one root→leaf path at a time, so both the live frontier and the parked offset stay O(path depth) unconditionally, regardless of fan-out.

Most OData servers cap `$expand` depth at 1; deeper expands surface as HTTP 4xx and propagate verbatim. Known to honor depth ≥ 2: Microsoft Graph (some endpoints), SAP S/4HANA Cloud (per-service configuration). Don't enable against a server you haven't verified.

### Cursor-based incremental on contained paths

Set `cursor_field` to a column on the leaf entity. The connector walks every parent tuple per `read_table` call, applies `$filter=cursor gt since` and `$orderby=cursor asc, leaf_pk asc` to each per-parent fetch, and tracks the global max cursor across all parents in the offset's `cursor` key.

Offset shape: `{"cursor": "<max_seen_value>"}` on natural completion, plus `lb_*` lookback bookkeeping when a lookback window is in play: `lb_history` (the `auto` window's walk-duration samples) and `lb_seen` (the `cursor_lookback_dedup` seen-set, on by default) ride the offset and are excluded from the no-progress comparison. When truncated mid-walk by `max_records_per_batch` the offset parks the truncated parent's **key chain** (`parent_keys`, plus `parent_cursor` on the ancestor-cursor path) alongside a `running_max` accumulator and a legacy `parent_idx` (downgrade fallback only). The resume re-positions by the enumeration's own ordering keys — churn-stable under parent inserts/deletes between batches — and rows already emitted within the resumed parent are elided by the parked continuation filter.

Termination: when an end_offset equal to the start_offset would be returned (no new rows anywhere), the connector emits zero rows and the same offset, satisfying the framework's "no progress" stop condition.

Truncation handling: when `max_records_per_batch` caps the walk mid-parent, the connector trims the trailing same-cursor cohort *within the truncated parent only* (`_trim_to_distinct_cursor_boundary`), and the returned offset carries a `truncated_chain_cursor` alongside `cursor`, `parent_keys` (the key-based resume position), `running_max`, and the legacy `parent_idx`. The resumed call uses `cursor gt truncated_chain_cursor` for the truncated parent (re-picks up its boundary cohort) and `cursor gt cursor` (the original `since`) for every subsequent parent — per-parent cursor distributions are independent, so a single boundary value can't safely cover them all. After the resumed walk completes naturally the offset collapses back to `{"cursor": <max_seen>}`; subsequent batches may re-emit earlier parents' rows whose cursors lie above `max_seen` from the resume — `cursor_lookback_dedup` (on by default) drops the unchanged ones before emit, and `apply_changes` keyed on the composite PK dedupes whatever remains at the destination. If a parent's same-cursor cohort exceeds `max_records_per_batch` *and the server returned that parent's whole leaf collection in one page* (no `@odata.nextLink`), the cohort is complete but has no splittable boundary — the connector emits it in full and continues to the next parent (overshooting the cap for that one parent) rather than failing, advancing the watermark exactly as natural completion would.

### Ancestor-cursor fallback

When the leaf entity doesn't declare `cursor_field` as a property but one of its ancestors does, the connector falls through to **ancestor-level filtering**:

1. `_find_cursor_level` walks `segments` leaf → root and returns the index of the closest segment whose entity type has the column.
2. The chain walk at that level includes `cursor_field` in `$select`, applies `$filter=<cursor> gt <since>` (on resume) and `$orderby=<cursor> asc, <pks> asc`. Other ancestor levels still fetch just their PKs.
3. For each matching ancestor tuple, the leaf collection is fetched **unfiltered** (the leaf doesn't have the column to filter by), and every emitted leaf row is stamped with the ancestor's cursor value under `cursor_field`.
4. `get_table_schema` includes `cursor_field` in the leaf schema with the ancestor's declared type (e.g. `TimestampType` for `Edm.DateTimeOffset`).
5. The offset tracks the max ancestor-cursor seen across the batch, same shape as the leaf-cursor case.

If `cursor_field` isn't a property anywhere along the path, `read_table` raises a `ValueError` naming the table.

### Mutex with delta tracking

`delta_tracking=enabled` on a contained path raises `ValueError` at `read_table` dispatch — server-driven change tracking is defined against top-level entity sets in OData v4 §11.3, not parent-keyed traversals. `delta_tracking=auto` silently resolves to disabled on contained paths (the auto-probe is skipped; the URL shape isn't compatible with the probe's GET).

---

## Type mapping

EDM primitive types are mapped to Spark types as follows. Any unrecognized type falls back to `StringType` (the raw JSON representation is preserved on the wire).

| EDM type | Spark type | Notes |
| --- | --- | --- |
| `Edm.String` | `StringType` | |
| `Edm.Boolean` | `BooleanType` | |
| `Edm.Byte` | `IntegerType` | Widened — the framework's `parse_value` doesn't support `ByteType`/`ShortType`, so the narrow EDM widths map to `IntegerType`. |
| `Edm.SByte` | `IntegerType` | Widened (see `Edm.Byte`). |
| `Edm.Int16` | `IntegerType` | Widened (see `Edm.Byte`). |
| `Edm.Int32` | `IntegerType` | |
| `Edm.Int64` | `LongType` | |
| `Edm.Single` | `FloatType` | |
| `Edm.Double` | `DoubleType` | |
| `Edm.Decimal` | `DecimalType(P, S)` | Honours the CSDL-declared `Precision`/`Scale` facets (clamped to Spark's 38-digit max; `Scale` absent with `Precision` declared → scale 0 per the CSDL default). Absent facets or `Scale="variable"` → the wide `DecimalType(38, 18)` fallback. |
| `Edm.Date` | `DateType` | Calendar date, no time component. |
| `Edm.DateTime` | `TimestampType` | OData v2 carryover; some v4 services still emit it. |
| `Edm.DateTimeOffset` | `TimestampType` | The standard v4 timestamp type. |
| `Edm.TimeOfDay` | `StringType` | No native Spark `TimeType`. |
| `Edm.Duration` | `StringType` | ISO 8601 duration text. |
| `Edm.Guid` | `StringType` | |
| `Edm.Binary` | `BinaryType` | Base64-encoded on the wire; downstream callers can use `_decode_binary` to materialize bytes. |

Complex-typed, enum-typed, `Collection(...)`, and TypeDefinition-typed `<Property>` elements are surfaced as `StringType` columns, with structured (object/array) values rendered as **JSON text** at the emit boundary (parseable downstream with `from_json`). `Edm.Stream` properties surface as always-NULL `StringType` columns, forced nullable regardless of the CSDL `Nullable` attribute (stream values are media references the JSON payload never carries — §11.2.4). Only navigation properties are unsurfaced.

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
                    "max_records_per_batch": "10000",
                    "page_size": "500",
                },
            }
        },
    ],
)
```

### What happens at runtime for `Orders`

1. `read_table_metadata("Orders", ...)` reads `$metadata`, finds `EntityType="NorthwindModel.Order"`, returns `primary_keys=["OrderID"]`, `cursor_field="OrderDate"`, `ingestion_type="cdc"`.
2. `get_table_schema("Orders", ...)` returns a `StructType` with `OrderID: int`, `CustomerID: string`, `OrderDate: timestamp`, `ShippedDate: timestamp`, etc., derived from the `<Property>` children of `NorthwindModel.Order`.
3. First call to `read_table` has no `start_offset`. The URL is:
   ```
   .../Orders?$top=500
            &$orderby=OrderDate asc, OrderID asc
   ```
   No cursor `$filter` on the first call — the connector pulls from the natural start of the table and lets `max_records_per_batch` (10000) cap the call.
4. Rows stream in via `@odata.nextLink` pagination. The connector accumulates up to 10000 rows.
5. The boundary trim runs. Many Northwind orders share an `OrderDate` (date-precision), so the trailing same-day cohort is dropped. The end offset is the last *distinct* `OrderDate` seen.
6. Next call resumes with `OrderDate gt <prev_distinct>`. The previously-dropped same-day cohort is re-fetched. `apply_changes` MERGEs them by `OrderID`, so the destination has each order exactly once.
7. Continuous mode: when the source grows under the running stream, subsequent calls keep advancing `<prev_distinct>` past the new rows. No timestamp ceiling has to expire for that to happen.

### Why `OrderDate` works as a cursor even though many rows share each date

Northwind `OrderDate` is a date-precision field — dozens of orders can share the same date. Without the boundary trim, a `gt` filter on the next call would skip every order sharing the boundary date. With the trim, the cohort is re-read every batch and MERGE-deduped at the destination. The only sizing requirement is that `max_records_per_batch` (10000 above) exceeds the largest single-day order count — easily true for Northwind.

If `max_records_per_batch` were set to, say, `10`, the connector would raise `RuntimeError` the first time a single `OrderDate` exceeded 10 orders, with a message instructing the operator to either raise the cap or pick a higher-cardinality cursor.

---

## Known limits

- **Server-side `$top` caps.** Some services cap `$top` below the requested value (Microsoft Graph at 999 for most endpoints; certain SAP services at 5000). Under the default `pagination=auto` the connector follows the server's `@odata.nextLink` whenever one is emitted, and when a server page-limits *without* emitting a link it falls back to a keyset seek / `$skip` drain until an empty page — so a smaller effective page size costs throughput, never rows. See the README's `pagination` row for the full mode matrix.
- **Opaque `$skiptoken` stability requires a unique total `$orderby`.** As described in *Incremental ingestion contract*, the connector unconditionally appends every primary-key column to `$orderby` in CDC mode. Snapshot reads under the default `pagination=auto` also send `$top=1000` plus a stable PK `$orderby` where one is needed for the client-driven drain; only `pagination=nextlink` sends a `$top`-free snapshot scan that follows server pagination as-is (a PK-only `$orderby` is still sent whenever the entity declares keys — skiptoken stability needs it).
- **Relative `@odata.nextLink`.** Handled — resolved against the response URL via `urljoin`. Absolute links pass through unchanged.
- **The `cdc_with_deletes` ingestion type is never reported.** Deletes **are** captured under `delta_tracking` (in-band `_deleted=True` tombstone rows — see [Delta tracking](#delta-tracking-contract)); cursor/snapshot reads cannot observe deletes, so there soft-deletes must be modeled as updates to a status column.
- **Flat entity sets read single-partition.** Skiptokens are opaque, so one collection's page walk can't be split. Contained N+1 paths partition across top-level subtrees via `num_partitions` (`SupportsPartitionedStream`).
- **Schema cache.** `$metadata` is cached with a TTL (default 60 s) in a process-wide dict plus an on-disk pickle shared across forked PySpark workers. Schema drift mid-run is not detected; a later trigger picks up the new shape once the TTL lapses.
- **Functions / actions not exposed.** Only `<EntitySet>` declarations become tables. Bound and unbound OData functions and actions are ignored.
- **Cursor field must be a plain orderable property.** The connector sends `$orderby=<cursor_field> asc` literally, so complex-typed properties, navigation properties, and computed expressions are not valid cursors. On flat tables it must live on the entity itself; on contained paths the connector resolves the closest level (leaf → root) that declares it.
- **OData v2 / v3 not supported.** The connector parses the v4 CSDL XML namespace and emits v4 protocol headers.
