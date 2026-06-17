# OData v4 Community Connector

A Lakeflow Connect community connector that ingests any OData v4 service
into Databricks. Schemas, table lists, and primary keys are discovered
automatically from the service's `$metadata` endpoint.

## Capabilities

- Discovers all entity sets via `$metadata`.
- Maps EDM primitive types (`Edm.String`, `Edm.Int32`, `Edm.DateTimeOffset`,
  `Edm.Decimal`, etc.) to Spark types.
- Snapshot ingest when no cursor is configured.
- Incremental CDC ingest when a per-table `cursor_field` is set
  (`field gt <last> and field le <now>`).
- Four auth methods: bearer, basic, api_key, oauth2 (client credentials
  *and* authorization-code / user flow with refresh-token rotation).

## Setting up the connection (UC)

The connection must be of type `COMMUNITY` and carry `sourceName: odata`
in its options so the Lakeflow Connect UI lists it under the OData
connector tile.

### Option A — CLI (recommended)

The `community-connector` CLI builds the right payload from the connector
spec and sets the auth-method fields correctly. Authenticate with the
Databricks CLI first.

```bash
pip install -e tools/community_connector
export DATABRICKS_CONFIG_PROFILE=<your-profile>

community-connector create_connection odata odata_connection \
  -o '{
        "service_url": "https://services.odata.org/V4/Northwind/Northwind.svc/",
        "token": "<bearer-token>"
      }' \
  --spec ./src/databricks/labs/community_connector/sources/odata/connector_spec.yaml
```

For other auth methods, swap `token` for the relevant fields:

| Auth method | Required option keys | Notes |
|---|---|---|
| `bearer` | `token` | |
| `basic` | `username`, `password` | |
| `api_key` | `api_key` (optionally `api_key_header`) | |
| `oauth2` (client credentials) | `oauth2_token_url`, `oauth2_client_id`, `oauth2_client_secret` (optionally `oauth2_scope`) | Server-to-server. Mints a fresh access token at session start; re-mints pre-emptively when `expires_in` is exhausted (60 s safety buffer). |
| `oauth2` (authorization code) | Same as above **plus** `oauth2_refresh_token` (and optionally `oauth2_access_token`) | User-delegated. The pre-issued access token is used directly, then refreshed via `grant_type=refresh_token` either pre-emptively when the deadline approaches or reactively on a 401 from the source. Rotated refresh tokens are tracked automatically. |

#### OAuth2 — client credentials (server-to-server)

Use when the connector authenticates as itself (a service principal /
machine identity). The connector mints a fresh access token at session
start by POSTing `grant_type=client_credentials` to the token endpoint
and re-mints pre-emptively when `expires_in` runs out (60 s safety
buffer).

```bash
community-connector create_connection odata odata_connection \
  -o '{
        "service_url": "https://your-host/odata/v4/",
        "auth_type": "oauth2",
        "oauth2_token_url": "https://login.example.com/oauth/token",
        "oauth2_client_id": "<client-id>",
        "oauth2_client_secret": "<client-secret>",
        "oauth2_scope": "read:everything"
      }' \
  --spec ./src/databricks/labs/community_connector/sources/odata/connector_spec.yaml
```

`oauth2_scope` is optional — include it only when the token endpoint
requires (or accepts) a `scope` parameter for client_credentials. For
Azure AD / Entra ID flows the value is typically `<resource>/.default`
(e.g. `https://graph.microsoft.com/.default`).

If you prefer to leave `auth_type` off, the connector auto-detects
`oauth2` from the presence of `oauth2_client_id` + `oauth2_client_secret`.

#### OAuth2 — authorization code (user-delegated, refresh-token rotation)

Use when the source enforces per-user access and you've already run an
interactive consent flow to obtain a refresh token. The connector uses
the pre-issued access token (if provided) until it expires, then
refreshes it via `grant_type=refresh_token`. Rotated refresh tokens
returned in the refresh response are tracked in memory automatically.

```bash
community-connector create_connection odata odata_connection \
  -o '{
        "service_url": "https://graph.microsoft.com/v1.0/",
        "auth_type": "oauth2",
        "oauth2_token_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token",
        "oauth2_client_id": "<app-client-id>",
        "oauth2_client_secret": "<app-client-secret>",
        "oauth2_scope": "https://graph.microsoft.com/.default offline_access",
        "oauth2_refresh_token": "<long-lived-refresh-token>",
        "oauth2_access_token": "<optional-pre-issued-access-token>"
      }' \
  --spec ./src/databricks/labs/community_connector/sources/odata/connector_spec.yaml
```

Notes:

- `oauth2_access_token` is optional. When omitted the connector calls
  the refresh endpoint immediately at session start to mint the first
  access token.
- `offline_access` (or the provider's equivalent) must be in
  `oauth2_scope` if your provider gates refresh-token issuance on it.
- The refresh token isn't persisted back to the UC connection — if your
  provider rotates refresh tokens on each refresh and the connector
  restarts, you may need to obtain a fresh refresh token interactively.
  Most providers accept the same refresh token repeatedly until it's
  explicitly revoked or expires.

#### Python SDK form (any OAuth2 variant)

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

service_url = "https://your-host/odata/v4/"

w.api_client.do(
    "POST",
    "/api/2.1/unity-catalog/connections",
    body={
        "name": "odata_connection",
        "connection_type": "COMMUNITY",
        "comment": f"service_url={service_url}",
        "options": {
            "sourceName": "odata",
            "service_url": service_url,
            "auth_type": "oauth2",
            "oauth2_token_url": "https://login.example.com/oauth/token",
            "oauth2_client_id": "<client-id>",
            "oauth2_client_secret": "<client-secret>",
            "oauth2_scope": "read:everything",
            # For authorization-code flow, add:
            # "oauth2_refresh_token": "<refresh-token>",
            # "oauth2_access_token": "<optional-pre-issued-access-token>",
            "externalOptionsAllowList": (
                "namespace,cursor_field,select,filter,"
                "filter_at_*,page_size,"
                "max_records_per_batch,delta_tracking,expand_contained,"
                "num_partitions"
            ),
        },
    },
)
```

Auth error handling:

- **Token-endpoint 4xx during the OAuth refresh-token grant** raises a
  `ValueError` naming `oauth2_refresh_token` + `oauth2_client_id` as
  the fields to check, with the OAuth `error` + `error_description`
  echoed verbatim from the server.
- **Source 401 *after* a successful OAuth refresh** raises a
  `PermissionError` — the access token isn't the problem; check
  `oauth2_scope`, principal permissions, and any tenant/instance
  identifier in `service_url` or `extra_headers`.
- **Source 401 or 403 with no automatic refresh path available**
  raises a `PermissionError` with auth-mode-specific remediation
  hints, naming the exact connection options to check. The server
  response body is echoed truncated so high-privilege error codes
  (`InvalidAuthenticationToken`, `Forbidden`, scope-related errors)
  come through. Behavior per auth mode:
  - `bearer` — pre-acquired tokens have no refresh path; the error
    suggests replacing `token` with a fresh value or upgrading to
    `auth_type=oauth2` with `oauth2_client_id` + `oauth2_client_secret`
    so the connector mints and refreshes tokens automatically.
  - `basic` — names `username` / `password` and the principal's
    permissions at the source.
  - `api_key` — names `api_key` (may have been rotated) and
    `api_key_header` (some services expect a non-default header).
  - `oauth2` without `oauth2_client_id`/`secret` and without
    `oauth2_refresh_token` — names the missing parameter pairs that
    would enable auto-refresh, plus `oauth2_scope`.
  - No `auth_type` set — names the four valid `auth_type` values so
    the operator can pick the right one for the source.

### Option B — Python SDK

The `ConnectionType` enum in `databricks-sdk` doesn't yet include
`COMMUNITY`, so call the REST endpoint directly via `api_client.do(...)`.

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

service_url = "https://services.odata.org/V4/Northwind/Northwind.svc/"

w.api_client.do(
    "POST",
    "/api/2.1/unity-catalog/connections",
    body={
        "name": "odata_connection",
        "connection_type": "COMMUNITY",
        "comment": f"service_url={service_url}",
        "options": {
            "sourceName": "odata",
            "service_url": service_url,
            "token": "<bearer-token>",
            "externalOptionsAllowList": (
                "namespace,cursor_field,select,filter,"
                "filter_at_*,page_size,"
                "max_records_per_batch,delta_tracking,expand_contained,"
                "num_partitions"
            ),
        },
    },
)
```

The `externalOptionsAllowList` must match the connector spec's
`external_options_allowlist`. The CLI in Option A reads the spec and
sets this automatically; with the SDK you set it explicitly — keep
it in sync with the spec or table-level options like
`delta_tracking` get silently stripped at runtime.

The connector reads its auth credentials directly from these option
keys; no `auth_method` / `auth_type` discriminator is needed when only
one set of credentials is present (the runtime infers `bearer` from the
presence of `token`, `basic` from `username`+`password`, `api_key` from
`api_key`, `oauth2` from `oauth2_client_id`+`oauth2_client_secret`).

### Verifying the connection

```bash
databricks --profile <your-profile> connections get odata_connection
```

The response must show `connection_type: COMMUNITY` and `options.sourceName: odata`.
If either is missing, the UI won't list the connection under the OData tile —
delete it and re-create.

### Optional connection options

| Option                        | Default | Description |
| ----------------------------- | ------- | ----------- |
| `metadata_cache_ttl_seconds`  | 60      | TTL for the on-disk pickle cache of the parsed CSDL (`$metadata`) document at `${TMPDIR}/odata_csdl_<sha256(service_url)>.pickle`. Shared across the forked `pyspark.daemon` workers that SDP spawns for schema inference, so the connector pays the fetch + parse cost once per pipeline init instead of once per `.load()`. Set to `0` to disable file-backed caching (process and instance caches still apply). |
| `max_retries`                 | 5       | Retry budget for transient failures. Two classes covered: (1) **HTTP 429 / 503** — throttling or service unavailable; honours the server's `Retry-After` header when present (integer seconds or HTTP-date), otherwise exponential backoff (1, 2, 4, 8, 16 s …). (2) **Connection-level exceptions** — TCP reset / remote disconnect, read or connect timeout, mid-body chunked-encoding error (the server returned no HTTP response at all); always exponential backoff. After `max_retries` consecutive failures the batch raises — `RuntimeError` for 429/503, the original exception type (`ConnectionError`/`Timeout`/`ChunkedEncodingError`) for network failures. Set to `0` to opt out. |
| `retry_max_delay_seconds`     | 60      | Per-retry sleep cap (seconds). Applied to both server-supplied `Retry-After` values and the exponential-backoff fallback, so a misbehaving source emitting an hour-long `Retry-After` can't pin a Spark task. |

## Pipeline (ingest.py)

```python
from databricks.labs.community_connector.pipeline import build_pipeline
from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect

build_pipeline(
    connector_cls=ODataLakeflowConnect,
    tables=[
        # Snapshot — re-read in full on every trigger. Use for small,
        # mostly-static tables; for large ones run them in a separate
        # triggered pipeline with a lower-frequency schedule.
        {
            "table": {
                "source_table": "Customers",
            }
        },
        # Cursor-based incremental — `cursor_field` drives a
        # `field gt <last>` filter and an `$orderby field, <pk>`
        # request. Works against any OData v4 service.
        {
            "table": {
                "source_table": "Orders",
                "table_configuration": {
                    "cursor_field": "OrderDate",
                    "max_records_per_batch": "10000",
                },
            }
        },
        # Delta tracking — server-driven change stream. Requires the
        # source to honor `Prefer: odata.track-changes` (MS Graph,
        # Dataverse, SAP S/4HANA Cloud …). Emits in-band tombstones
        # via the synthetic `_deleted` column. See "Delta tracking"
        # section below for the contract.
        {
            "table": {
                "source_table": "Suppliers",
                "table_configuration": {
                    "delta_tracking": "enabled",
                },
            }
        },
    ],
)
```

## Per-table options

| Option                  | Default | Description |
| ----------------------- | ------- | ----------- |
| `namespace`             |         | OData schema namespace (e.g. `Sales`, `HR`). Required only when two schemas declare an entity set with the same name. |
| `cursor_field`          |         | Drives incremental reads. Omit for snapshot. |
| `select`                | all     | Comma-separated `$select` projection. |
| `filter`                |         | Extra OData `$filter` expression. Applied to the **leaf segment** in both modes — leaf URL in N+1 mode (`expand_contained=false`), innermost `$expand(...)` clause in expand mode. Equivalent to `filter_at_<leaf-segment>`; AND-composes with it if both are set. For per-segment placement on intermediate ancestors of contained paths, use `filter_at_<segment>` below. |
| `filter_at_<segment>` <br/> `filter_at_<idx>` | | Per-segment `$filter` for contained-path tables. Each entry is applied to the matching walk level — in N+1 mode the ancestor walks at each level get pruned to matching rows, cascading the savings down the children; in `expand_contained=true` mode the filter is injected inside the corresponding `$expand(...)` clause per OData v4 §5.1.1.6. Two equivalent forms: by segment name (`filter_at_Instances=Id eq 5` — must match a segment in the contained path) or by zero-based index (`filter_at_0=Id eq 5`). Index wins on conflict. Composes with cursor filters (AND-ed at the cursor's segment) and with the existing `filter` option (AND-ed at the leaf in N+1 mode, AND-ed at the top in expand mode). Unknown segment names and out-of-range indices raise `ValueError` at read time. |
| `page_size`             | 1000    | Maximum per-response row budget. For flat tables it's the `$top` at the single URL. For `expand_contained=true` paths it's distributed across all `$top` points (top URL + every nested `$expand(...)`) with triangular weights — top gets the largest share, each deeper level proportionally less — so the cross-product `top × inner_1 × inner_2 × …` fits in the budget. Each per-level `$top` is floored at 5 (very small pages amplify the `@odata.nextLink` chase at every level); when a deep level would drop below 5 it's pinned to 5 and the remaining budget is divided back across the upper levels, so the cross-product stays at or under `page_size`. Examples with `page_size=1000`: depth 2 → `[100, 10]` (product 1000), depth 3 → `[34, 5, 5]` (850), depth 4 → `[8, 5, 5, 5]` (1000). For chains so deep that `5 ** N > page_size` the floor unavoidably wins (e.g. `5**5 = 3125`); raise `page_size` to restore the cap, or switch to `expand_contained=false` so the chain becomes N+1 single-segment fetches. |
| `max_records_per_batch` | 10000   | Per-call upper bound on rows returned. The connector has **no wall-clock ceiling** — `max_records_per_batch` is the only cap on a single batch. Each batch fetches `cursor gt <last>` and pulls up to this many rows, then commits the offset. Smaller values give continuous-mode pipelines lower latency per micro-batch at the cost of more round trips; larger values amortize HTTP overhead. Honoured by every read path (flat / contained N+1 / contained `expand_contained=true`). On the `expand_contained=true` path the cap is enforced **per HTTP fetch at any depth** via an iterative work queue: each inner-collection `@odata.nextLink` discovered during inline descent is appended to a queue rather than followed inline, and the cap is checked between fetches. Pending fetches are parked in the offset as `pending_fetches` (a list of `{url, level, chain, cur_val, skip}` items) so the next `read()` resumes the queue verbatim. Cap deviation per batch is bounded by one HTTP response's worth of leaf rows (≤ `page_size`), regardless of how deep the chain is or how wide any single parent's inner collection grows. In cursor mode the watermark only advances once the chain fully drains; until then the running max sits in `running_max_cursor`. The default of 10000 balances commit frequency / visibility against HTTP overhead; lower it (e.g. to 100) for tighter per-batch latency or higher (e.g. 100000+) for throughput-oriented batch backfills. |
| `delta_tracking`        | disabled | Opt-in OData v4 delta queries. Values: `disabled` (default — no behavior change), `auto` (probe once, fall back to cursor/snapshot if the server doesn't acknowledge), `enabled` (require support; error if the server doesn't acknowledge). See [Delta tracking](#delta-tracking) below. |
| `expand_contained`      | false   | For contained-collection tables (`Parent__Child__...` paths). When `true`, the connector issues a single `GET Parent?$expand=Child($expand=...)` per pipeline trigger instead of the default N+1 traversal (one parent fetch + one per-parent leaf fetch). See [Contained navigation properties](#contained-navigation-properties) below. |
| `num_partitions`        | 4       | Number of Spark partitions for parallel reads of contained-collection tables. Honored only when the table qualifies for `SupportsPartitionedStream` (contained path, `expand_contained=false`, `delta_tracking=disabled`, and any `cursor_field` lives on the top-level entity). Top-level rows are bin-packed into this many contiguous slices; each Spark task walks only its assigned subtrees. Ignored for non-partitionable tables (they fall back to single-task reads). |

## Delta tracking

OData v4 §11.3 defines an optional change-tracking protocol: clients
send `Prefer: odata.track-changes` on the initial request; supporting
servers reply with `Preference-Applied: odata.track-changes`, a full
snapshot, and an `@odata.deltaLink` URL. Subsequent calls to the delta
link return only entities that changed since the link was minted —
including deletions, signalled by an `@removed` block.

When `delta_tracking` is active the connector:

- Reads via the delta link instead of `cursor_field` filtering. HTTP
  cost drops from "full snapshot per trigger" to "changes only per
  trigger".
- Emits two synthetic columns into the destination schema:
  - `_deleted` (boolean) — `True` for tombstone rows from `@removed`
    entries, `False` for adds and changes. Downstream consumers filter
    on this to materialise active rows.
  - `_lc_sequence` (string) — strictly monotonic per emitted record,
    used as `apply_changes` sequence_by so the destination MERGE picks
    deterministic winners when the same primary key appears multiple
    times in one batch.
- Surfaces `ingestion_type=cdc` (in-band tombstones via the `_deleted`
  flag, not the framework's `cdc_with_deletes` split). This matches
  the pattern established by `microsoft_teams`.
- Falls back automatically on `delta_tracking=auto` when the server
  doesn't acknowledge the prefer header. `enabled` mode treats the
  same condition as a hard failure with an actionable error.

Known-supporting services (`delta_tracking=enabled` should work):

- Microsoft Graph (`/v1.0/users/delta`, `/v1.0/groups/delta`, …)
- Microsoft Dataverse / Power Platform OData endpoints
- SAP S/4HANA Cloud OData services that have change-tracking
  enabled per entity set

Other services (e.g. Northwind, generic SAP NetWeaver Gateway
deployments) typically ignore the prefer header — `delta_tracking=auto`
detects this and falls back to whatever cursor/snapshot config is set.

### Limitations

- **Sparse property updates are rejected.** OData v4 §11.4 lets the
  server return only the *changed* properties on an update. Applying
  that as-is would write NULLs over good values at the destination, so
  the connector raises `RuntimeError` if a non-tombstone delta entry is
  missing any property the schema declares. Workaround: restrict the
  schema via `$select` to fields the server always returns, or fall
  back with `delta_tracking=disabled`.
- **Token expiry triggers a full re-bootstrap.** When the server
  returns HTTP 410 on a stored delta link, the connector silently
  re-reads the entire entity set via a fresh `Prefer` GET. Re-fetched
  rows MERGE cleanly by primary key at the destination, so no data
  is lost — but HTTP cost spikes for that one batch.
- **`cursor_field` and `delta_tracking=enabled` are mutually
  exclusive.** Delta tracking is the source of truth for change
  ordering; layering a client-side cursor on top over-constrains the
  read.
- **The `_deleted` and `_lc_sequence` columns are synthetic.** They're
  produced by the connector, not the source, and don't exist on the
  origin entity. The destination Delta table carries them; downstream
  transforms should filter on `_deleted=False` when materialising
  active rows.

### Configuration example

```python
{
    "table": {
        "source_table": "users",
        "table_configuration": {
            "delta_tracking": "enabled",
        },
    }
}
```

## Contained navigation properties

OData v4 lets entity types declare ``<NavigationProperty
ContainsTarget="true">`` collections that are *owned* by the parent
entity rather than living as their own top-level entity sets. They're
addressed by traversing the parent's key:
``GET Parent(<key>)/ContainedNavProp``. Common examples: order line
items, address records on a customer, asset documents on an asset.

The connector exposes these as double-underscore-pathed tables alongside the
top-level entity sets, up to 10 segments deep:

```
Parents
Parents__Children
Parents__Children__Notes
Parents__Tags
```

### Schema augmentation

Each contained-collection table prepends synthetic FK columns for
**every non-leaf ancestor**. This is required for global uniqueness —
OData v4 §13.4.3 makes contained-entity keys unique only within their
immediate parent, so collapsing on the leaf's own PK collides across
sibling parent branches. Default name is ``<segment>_<pkname>``; a
leading ``_`` is added only if it would collide with a leaf property.
For ``Parents__Children__Notes``:

```
Parents_Id    Int — top-level ancestor PK
Children_Id   Int — intermediate ancestor PK
Id            Int — Notes' own primary key
Text          String
```

The composite primary key reported in ``read_table_metadata`` is the
full chain: ``[Parents_Id, Children_Id, Id]``. If the leaf had its
own property named ``Children_Id``, the FK would be emitted as
``_Children_Id`` and the leaf property would keep its original name.

### Read modes

Two modes via the ``expand_contained`` table option:

**Default (`expand_contained=false`)** — N+1 traversal. The connector
issues one ``GET Parents?$select=<pks>`` to enumerate parent keys, then
one ``GET Parents(<key>)/Children?...`` per parent. For deep paths the
fan-out multiplies. Works against every OData v4 server.

**Opt-in (`expand_contained=true`)** — single nested ``$expand`` call:
``GET Parents?$expand=Children($expand=Notes)``. The connector flattens
the nested response into leaf rows tagged with all ancestor FKs. Most
servers cap ``$expand`` depth at 1; deeper expands surface server errors
verbatim. Use only against servers known to honor the depth you need
(Microsoft Graph, SAP S/4HANA Cloud).

### Cursor-based incremental on contained tables

Set ``cursor_field`` on the leaf entity's column. The connector walks
every parent tuple per trigger, filters each leaf fetch with
``$filter=<cursor> gt <since>``, and tracks the global max cursor in
the offset. Mid-batch ``max_records_per_batch`` truncation parks a
``parent_idx`` in the offset so the next call resumes at the same
parent.

If the leaf entity doesn't declare ``cursor_field`` as a property,
the connector walks leaf → root looking for the closest ancestor
that does have it, filters at that ancestor level instead, and
stamps the ancestor's cursor value onto every emitted leaf row.
``get_table_schema`` adds the cursor column to the leaf schema with
the ancestor's declared type. Set ``cursor_field`` to a column name
that isn't anywhere on the path → ``ValueError``.

Truncation handling: when a per-parent walk hits the
``max_records_per_batch`` cap, the connector prefers a **nextLink-based
mid-chain resume**. The server's @odata.nextLink for the partially-paged
chain is parked in the offset as ``chain_next_link``; the resumed call
hands that link back to the server, which picks up exactly where it
stopped without reconstructing ``$filter``/``$orderby``/``$select`` state.
Subsequent parents in the same resume keep using the original ``cursor``
``since``. After the resumed walk completes naturally, the offset
collapses back to ``{"cursor": <max>}``; ``apply_changes`` dedupes any
cross-batch repeats.

In leaf-cursor mode, when the chain ended on the truncating page (no
nextLink available), the connector falls back to **Option A trim**: the
trailing same-cursor cohort within that chain's emit is dropped and a
``truncated_chain_cursor`` is parked. The resumed call rebuilds the URL
with ``cursor gt truncated_chain_cursor`` for that one parent. If even
one parent's same-cursor cohort exceeds ``max_records_per_batch``, the
connector raises ``RuntimeError`` — raise the cap, or pick a
higher-cardinality cursor.

In ancestor-cursor mode there is no Option A fallback (every leaf under
a chain shares the chain's stamped cursor by construction, so a
within-chain ``cursor gt`` rebuild can't split it). The mode relies
solely on ``chain_next_link``; if a server doesn't return durable
@odata.nextLink values, raise ``max_records_per_batch`` above the
largest per-chain leaf count.

Cross-chain interleaving: ancestor-cursor mode walks chains depth-first
by top-level parent, so ancestor cursors interleave across top-level
parents (sibling chains under one parent are cursor-ordered, but
Parent 2's lowest cursor can be below Parent 1's highest). On
truncation the connector therefore preserves the **original** ``since``
in the offset rather than advancing to the global max emitted, so the
resumed call's rebuild of ``chains_with_cursor`` includes every chain
that batch 1 saw — including any lower-cursor chains under later
parents that hadn't been reached yet. A ``running_max`` is accumulated
across resume batches so when the resume completes naturally the
next regular trigger gets the actual highest cursor seen as its filter
floor (a fresh first-ever resume that originated from ``since=None``
would otherwise drop the cursor entirely on completion and re-walk
the table on the next trigger). Cross-batch re-emission of
already-seen chains is deduped by ``apply_changes`` on the composite
primary key.

### Disallowed combinations

- ``delta_tracking=enabled`` + a contained path → ``ValueError``.
  Server-driven change tracking is defined against top-level entity
  sets.
- Depth > 10 → ``ValueError`` at parse time. Cap exists to bound
  discovery walks against cyclic schemas (cycles are independently
  detected via target-type tracking); raise if you have a real use case.

### Configuration example

```python
{
    "table": {
        "source_table": "Parents__Children__Notes",
        "table_configuration": {
            "cursor_field": "ModifiedAt",
            "max_records_per_batch": "5000",
            # Optional: expand_contained: "true" for $expand mode
        },
    }
}
```

## Multi-tenant / multi-schema services

Services like SAP S/4HANA OData publish more than one `<Schema>` block in
`$metadata`. The connector implements `SupportsNamespaces`, so each schema
shows up as its own namespace in the catalog browser. When the same
entity set name appears in two schemas, set the `namespace` table option:

```python
{"name": "Customers", "options": {"namespace": "Sales"}}
{"name": "Customers", "options": {"namespace": "HR"}}
```

## Limitations

- Contained-collection tables parallelize reads across Spark executors
  via `SupportsPartitionedStream`: the connector enumerates top-level
  parent rows once, bin-packs them into `num_partitions` contiguous
  slices (default 4), and each task walks only its assigned subtrees.
  Activation is gated to contained paths with `expand_contained=false`
  and `delta_tracking=disabled`; configs outside that envelope fall
  back to a single-task read. Top-level / flat tables stay
  single-partition because `@odata.nextLink` skiptokens are opaque and
  can't be safely split. Throughput on a flat table is bounded by the
  source.
- Delete tombstones are synthesized only when `delta_tracking` is active.
  In snapshot and cursor-based modes, the connector never returns
  `cdc_with_deletes`. OData services without delta-query support don't
  expose deletions uniformly.
- Cursor field (when used) is assumed to be monotonically non-decreasing
  and naturally orderable by `$orderby`. Timestamps and monotonic IDs
  work; arbitrary fields don't. Cursor values must be non-null: if every
  row in a batch has a null cursor the connector raises `RuntimeError`
  rather than silently dropping the rows. Add a server-side `filter`
  to exclude null-cursor rows when the source allows them.
- `max_records_per_batch` on the `expand_contained=true` path is
  enforced **per HTTP fetch**, not per row. Cap deviation per batch
  is bounded by one response's worth of leaf rows (≤ `page_size`);
  the connector cannot interrupt mid-response because the in-flight
  HTTP response body can't be serialized across batches. For tight
  caps, lower `page_size` (shrinks the per-level `$top` cross-product
  and therefore each response's row count) or switch to
  `expand_contained=false` which commits at every parent-walk boundary.
- Batch reads (`LakeflowBatchReader`, used by
  `spark.read.format("lakeflow_connect")`) call `read_table` with
  `start_offset=None` and discard the returned end-offset. The
  connector detects that signal and **auto-disables the cap** when
  `max_records_per_batch` is not explicitly set, so the chain drains
  in one call instead of silently truncating at the default. If the
  user passes `max_records_per_batch` explicitly the override is
  skipped — same-cursor-cohort overflow detection and any other
  cap-driven behaviour stay intact, at the cost of accepting that the
  batch reader may drop continuation state. Streaming triggers
  (default for SDP) always pass a dict and are unaffected.
