# OData v4 Community Connector

A Lakeflow Connect community connector that ingests any OData v4 service
into Databricks. Schemas, table lists, and primary keys are discovered
automatically from the service's `$metadata` endpoint.

## Capabilities

- Discovers all entity sets via `$metadata`. No table-list config needed.
- Maps EDM primitive types (`Edm.String`, `Edm.Int32`, `Edm.DateTimeOffset`,
  `Edm.Decimal`, etc.) to Spark types.
- **Three read modes**: full-table snapshot (default), cursor-based
  incremental (per-table `cursor_field` → each batch fetches
  `cursor gt <last>`), and server-driven delta tracking (`Prefer:
  odata.track-changes` with in-band tombstones).
- **Contained navigation properties** addressed as
  double-underscore-pathed tables (`Parents__Children__Notes`) with
  ancestor FK columns synthesized for global uniqueness. Read via a
  single nested `$expand` or an N+1 traversal — the default `auto`
  preflights the server and picks per table.
- **Auth**: bearer, basic, api_key (static credentials), plus
  **UC-managed OAuth** — the COMMUNITY connection runs the OAuth flow
  (`community_oauth_flow=m2m`/`u2m`), refreshes tokens server-side, and
  injects a fresh `access_token` into the connector at query time; the
  connector holds no OAuth code at all.
- **Parallel reads** for contained tables via
  `SupportsPartitionedStream` (bin-packed top-level parents across
  Spark tasks).

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
| OAuth (UC-managed) | `client_id`, `client_secret`, `token_endpoint` (optionally `oauth_scope`; `authorization_endpoint` for the browser flow) — plus the connection's `community_oauth_flow` set to `m2m` or `u2m` | The connection layer runs the flow, refreshes tokens **server-side**, and injects a fresh `access_token` into the connector at query time. No `auth_type` is set; the connector treats the injected token as an opaque bearer credential. |

#### OAuth — UC-managed (`community_oauth_flow`)

OAuth is owned by the connection layer, not the connector: the UC
COMMUNITY connection (or the labs CLI for local dev) runs the OAuth
flow, refreshes the token **server-side**, and injects a fresh
`access_token` into the connector's options at query time. The
connector never sees your client secret and contains no token-minting
or refresh code — a design shared by every OAuth connector in this
repo.

Because OData is generic, no provider URLs are baked into the spec —
you supply your provider's `token_endpoint` on the connection. The CLI
derives the flow (`m2m`, client credentials) from the connector spec:

```bash
community-connector create_connection odata odata_connection \
  -o '{
        "service_url": "https://your-host/odata/v4/",
        "client_id": "<client-id>",
        "client_secret": "<client-secret>",
        "token_endpoint": "https://login.example.com/oauth/token",
        "oauth_scope": "read:everything"
      }' \
  --spec ./src/databricks/labs/community_connector/sources/odata/connector_spec.yaml
```

`oauth_scope` is optional — include it only when the token endpoint
requires (or accepts) a `scope` for client_credentials. For Azure AD /
Entra ID the value is typically `<resource>/.default`.

For a **user-delegated** source, create the connection with
`--auth-type u2m` and additionally supply `authorization_endpoint`;
the CLI runs the browser consent flow at connection creation and UC
handles refresh (including rotated refresh tokens) from then on.

Do **not** set `auth_type` for OAuth connections, and never supply
`access_token`/`refresh_token` yourself — they are minted by the flow
and injected at runtime. (The old connector-side `auth_type=oauth2`
mode is retired; using it raises an error with these migration steps.)

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
                "filters_at,page_size,max_records_per_batch,cursor_nulls,"
                "delta_tracking,expand_contained,num_partitions,pagination,"
                "exclude_ancestor_columns,cursor_lookback_seconds,"
                "cursor_lookback_factor,cursor_lookback_max_seconds,"
                "cursor_lookback_dedup,cursor_probe,contained_fetch"
            ),
        },
    },
)
```

For UC-managed OAuth (`community_oauth_flow`), prefer the CLI
(Option A) — it validates the options against the connector spec and
derives the flow from it. Via the SDK, replace the `"token"` line
with the OAuth connection options; the `community_oauth_flow`
discriminator is what makes UC run the client-credentials flow,
refresh the token server-side, and inject a fresh `access_token`
into the connector at query time:

```python
w.api_client.do(
    "POST",
    "/api/2.1/unity-catalog/connections",
    body={
        "name": "odata_oauth_connection",
        "connection_type": "COMMUNITY",
        "comment": f"service_url={service_url}",
        "options": {
            "sourceName": "odata",
            "service_url": service_url,
            "community_oauth_flow": "m2m",
            "client_id": client_id,          # load from a secret store,
            "client_secret": client_secret,  # never inline literals
            "token_endpoint": "https://login.example.com/oauth/token",
            "oauth_scope": "read:everything",  # optional
            "externalOptionsAllowList": "...",  # same value as above
        },
    },
)
```

Do **not** include `auth_type`, `access_token`, or `refresh_token` in
the options — the tokens are minted by the flow and injected at
runtime. For the browser-based `u2m` flow the CLI is required (it
runs the loopback authorization-code flow at creation time); the raw
REST path only works for `m2m`.

The `externalOptionsAllowList` must match the connector spec's
`external_options_allowlist`. The CLI in Option A reads the spec and
sets this automatically; with the SDK you set it explicitly — keep
it in sync with the spec or table-level options like
`delta_tracking` get silently stripped at runtime.

The connector reads its static auth credentials directly from these
option keys. Two auto-detections apply when `auth_type` is omitted: a
UC-injected `access_token` (the OAuth connection mode) is used as an
opaque bearer token, and a bare `token` implies `bearer`. `basic` and
`api_key` require an explicit `auth_type`; with none of the above the
connector builds the session with **no auth applied** — which is what
anonymous services such as the public Northwind reference service want.

### Verifying the connection

```bash
databricks --profile <your-profile> connections get odata_connection
```

The response must show `connection_type: COMMUNITY` and `options.sourceName: odata`.
If either is missing, the UI won't list the connection under the OData tile —
delete it and re-create.

### Auth error handling

Failures surface as Python exceptions with the offending connection
option named in the message and the server's response body echoed
verbatim (truncated to keep the trace readable) — with one deliberate
exception: a malformed 200 from the **token endpoint** withholds the
body entirely, because a truncated token response is
`{"access_token": "<live secret>` cut mid-document and the message
lands in pipeline logs. Five classes:

| Symptom | Exception | Remediation hint in the message |
|---|---|---|
| Source 401 under a UC-injected `access_token` | `PermissionError` | The connection layer refreshes the token at query start, so a mid-read 401 usually means the token expired during a long read (retry — the next query gets a fresh token), the OAuth principal lacks access, or the connection's `oauth_scope` is too narrow. |
| Source 403 (any auth mode) | `PermissionError` | Authenticated but not authorized — permissions/scope at the source, not a token problem. |
| `auth_type=oauth2` configured | `ValueError` at session build | Retired mode; the message carries the migration steps to the UC OAuth connection. |
| Source 401 with static credentials | `PermissionError` | Per auth mode (see below). |

The static-credential 401 remediation depends on the configured mode:

- **`bearer`** — pre-acquired tokens have no refresh path. The error
  suggests replacing `token` with a fresh value, or switching to a UC
  COMMUNITY OAuth connection so tokens are refreshed server-side and
  injected fresh every query.
- **`basic`** — names `username` / `password` and the principal's
  permissions at the source.
- **`api_key`** — names `api_key` (may have been rotated) and
  `api_key_header` (some services expect a non-default header).
- **No `auth_type` set** — names the static `auth_type` values and the
  UC-managed OAuth alternative.

### Optional connection options

| Option                        | Default | Description |
| ----------------------------- | ------- | ----------- |
| `timeout_seconds`             | 180     | Per-request HTTP timeout (seconds) applied to every call to the OData service (`$metadata` fetch and all entity reads). Raise it for slow services or large `$metadata`; lower it to fail fast. |
| `metadata_cache_ttl_seconds`  | 60      | TTL (seconds) for the cached parsed `$metadata` document — governs **both** the in-process cache (shared by all connector instances in one driver/worker process) and the on-disk pickle (shared across forked workers), so the fetch + parse cost is paid once per pipeline init and a long-running process still picks up upstream schema changes after the TTL. Set to `0` to disable both layers (every instance re-fetches). |
| `max_retries`                 | 5       | Retry budget for transient failures. Two classes covered: (1) **HTTP 408 / 429 / 500 / 502 / 503 / 504** — request timeout, throttling, service unavailable, and transient gateway/server errors; honours the server's `Retry-After` header when present (integer seconds or HTTP-date), otherwise exponential backoff (1, 2, 4, 8, 16 s …, jittered to 50–100 % so parallel partition tasks knocked back together don't retry in lockstep). (2) **Connection-level exceptions** — TCP reset / remote disconnect, read or connect timeout, mid-body chunked-encoding error (the server returned no HTTP response at all); always (jittered) exponential backoff. After `max_retries` consecutive failures the batch raises — `RuntimeError` for the retryable HTTP statuses, the original exception type (`ConnectionError`/`Timeout`/`ChunkedEncodingError`) for network failures. Set to `0` to opt out. |
| `retry_max_delay_seconds`     | 60      | Per-retry sleep cap (seconds). Applied to both server-supplied `Retry-After` values and the exponential-backoff fallback, so a misbehaving source emitting an hour-long `Retry-After` can't pin a Spark task. |
| `verbose_http_logging`        | false   | When `true`, logs each HTTP request/response (method, URL, status, timing) at INFO for troubleshooting. Off by default to keep logs quiet and avoid leaking URL query values. |
| `verbose_http_log_body_chars` | 500     | When `verbose_http_logging` is on, the maximum number of response-body characters logged per request (truncated beyond this). Only consulted when verbose logging is enabled. |
| `extra_headers`               | (none)  | Comma-separated `Name: value` pairs added to every request's headers (e.g. `"X-Env: prod, Accept-Language: en"`). Values containing commas can't be expressed (the list splits on `,`). Applied before auth headers; a pair without `:` is ignored. Header names are validated eagerly (RFC 7230 token — letters, digits, and `` !#$%&'*+-.^_`|~ ``; no spaces) so a malformed name fails at setup with the option named instead of going out on the wire. |

Note: `service_url` must be a bare service root — no query string or fragment (a curated error rejects them at setup, since the connector appends entity paths to the root). For SAP Gateway client selection (`?sap-client=NNN`), pass the client as a header instead: `extra_headers="sap-client: NNN"`.

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

Passed to the connector via the pipeline's `table_configuration` block.
Every key must appear in the connection's `external_options_allowlist`
(the connector spec already lists all of them).

The table below is the quick reference; every non-trivial option name
links to its detail subsection.

| Option | Default | What it does |
| --- | --- | --- |
| `namespace` | — | Selects the OData schema (e.g. `Sales`, `HR`) when two schemas declare an entity set with the same name. The schema's `Alias` is accepted interchangeably. |
| `cursor_field` | — | Drives incremental reads (`cursor gt <last>` per batch). Omit for snapshot. On contained paths the column may live on the leaf or an ancestor — see [Cursor-based incremental on contained tables](#cursor-based-incremental-on-contained-tables). |
| [`select`](#select) | all | `$select` projection, leaf-scoped on contained paths. |
| [`filter`](#filter-and-filters_at) | — | Extra `$filter` expression, applied to the leaf segment. |
| [`filters_at`](#filter-and-filters_at) | — | JSON object mapping segment names or zero-based indices to per-segment `$filter` expressions. |
| [`page_size`](#page_size) | `1000` | Per-response row budget (`$top`), distributed across `$expand` levels. |
| [`max_records_per_batch`](#max_records_per_batch) | `10000` | Per-batch cap on emitted rows, with resumable park state. |
| [`cursor_nulls`](#cursor_nulls) | `coalesce` | How cursor reads handle rows whose `cursor_field` is null. |
| [`pagination`](#pagination) | `auto` | How collection pages are walked: `auto` / `nextlink` / `keyset` / `skip`. |
| [`delta_tracking`](#delta_tracking) | `disabled` | Opt-in OData v4 delta queries — see [Delta tracking](#delta-tracking). |
| [`expand_contained`](#expand_contained) | `auto` | Nested-`$expand` vs N+1 read for contained paths. |
| [`contained_fetch`](#contained_fetch) | `auto` | `$batch`-packing of the un-cursored contained walks' per-parent fetches. |
| [`cursor_probe`](#cursor_probe) | `auto` | Change-probe acceleration for deep leaf-cursor N+1 reads. |
| [`num_partitions`](#num_partitions) | `4` | Spark-parallel reads of contained N+1 paths. |
| [`cursor_lookback_seconds`](#cursor_lookback_seconds-cursor_lookback_factor-cursor_lookback_max_seconds) | `auto` | Overlap window re-scanning rows that landed mid-walk (with `cursor_lookback_factor`, `cursor_lookback_max_seconds`). |
| [`cursor_lookback_dedup`](#cursor_lookback_dedup) | `on` | Suppress redundant overlap re-emits via an exact, capped seen-set. |
| [`exclude_ancestor_columns`](#exclude_ancestor_columns) | — | Drop synthetic ancestor-FK columns from a contained table. |

### select

Comma-separated `$select` projection, or `*` (all structural properties
— equivalent to omitting the option).

**Leaf-scoped on contained paths** in both read modes: it lands on the
leaf URL in N+1 mode and inside the innermost `$expand(...)` clause in
expand mode. Ancestor levels stay unprojected — their PKs and any
ancestor cursor column are fetched by the machinery regardless — and
the derived Spark schema is filtered to the same leaf columns
(synthetic ancestor-FK columns survive).

Must keep the leaf's primary-key columns and any leaf-level
`cursor_field` (curated error otherwise).

### filter and filters_at

`filter` is an extra OData `$filter` expression applied to the **leaf
segment** in both modes — the leaf URL in N+1 mode
(`expand_contained=false`), the innermost `$expand(...)` clause in
expand mode. It AND-composes with a leaf entry in `filters_at` if both
are set.

`filters_at` is a JSON object that places `$filter` expressions on
specific levels of a contained path. For example:

```json
{"Instances": "Id eq 5", "Projects": "Status eq 'active'"}
```

- **N+1 mode**: the ancestor walk at that level is pruned to matching
  rows, cascading the savings down to every child.
- **Expand mode**: the filter is injected inside the corresponding
  `$expand(...)` clause per OData v4 §5.1.1.6.
- Keys may be segment names (matched case-insensitively) or zero-based
  indices (`{"0": "Id eq 5"}`). Index wins when both forms target the
  same segment.
- Composes with cursor filters (AND-ed at the cursor's segment) and
  with the `filter` option (AND-ed at the leaf in N+1 mode, AND-ed at
  the top in expand mode).
- Unknown segment names and out-of-range indices raise `ValueError` at
  read time.

**Encoding rule (both options):** the connector auto-encodes spaces and
non-ASCII, but a URI-reserved character inside a **string literal**
must be percent-encoded by you: `%`→`%25`, `&`→`%26`, `#`→`%23`,
`+`→`%2B` (e.g. `Name eq 'AT%26T'`, `Grade eq 'A%2B'`,
`Created gt 2024-01-01T00:00:00%2B01:00`). Left raw, an `&`/`#`
truncates the query (usually a server 400; wrong rows on a lenient
server) — the connector can't encode these for you without
double-encoding an already-encoded value.

See [Filtering individual segments](#filtering-individual-segments)
for a worked example.

### page_size

Maximum per-response row budget. Must be a positive integer — `0` or
garbage raises up front (`$top=0` is a valid URL the server answers
with an empty page, which the drain would read as "table exhausted"
and silently emit zero rows).

**When a default applies.** Under the default `pagination=auto`
**every** read — snapshot, cursor, and delta — defaults
`page_size=1000` (→ `$top=1000`), because `auto` needs a `$top` to
detect a page-limited response with no continuation link. The one
exception is `pagination=nextlink`: there a **snapshot read** (no
`cursor_field`, no delta) with `page_size` unset sends **no `$top` at
all** — the server picks its own page size and the connector walks
every page via `@odata.nextLink` (this avoids servers that reject or
mishandle an explicit `$top`, e.g. a value above their per-page cap,
on a full-table scan). Cursor/delta reads still default to `1000` even
under `nextlink`. Setting `page_size` explicitly applies to every mode
and overrides the default.

**Flat tables**: the value becomes the `$top` at the single URL.

**`expand_contained=true` paths**: the budget is distributed across
all `$top` points (top URL + every nested `$expand(...)`) with
triangular weights — top gets the largest share, each deeper level
proportionally less — so the cross-product
`top × inner_1 × inner_2 × …` fits in the budget. Each per-level
`$top` is floored at 5 (very small pages amplify the
`@odata.nextLink` chase at every level); when a deep level would drop
below 5 it's pinned to 5 and the remaining budget is divided back
across the upper levels, so the cross-product stays at or under
`page_size`. Each level additionally carries a stable `$orderby` for
skiptoken-safe paging — see
[Pagination ordering](#pagination-ordering).

Examples with `page_size=1000`: depth 2 → `[100, 10]` (product 1000),
depth 3 → `[34, 5, 5]` (850), depth 4 → `[8, 5, 5, 5]` (1000). For
chains so deep that `5 ** N > page_size` the floor unavoidably wins
(e.g. `5**5 = 3125`); raise `page_size` to restore the cap, or switch
to `expand_contained=false` so the chain becomes N+1 single-segment
fetches.

### max_records_per_batch

Per-call upper bound on rows returned. The connector has **no
wall-clock ceiling** — `max_records_per_batch` is the only cap on a
single batch. Each batch fetches `cursor gt <last>` and pulls up to
this many rows, then commits the offset. Smaller values give
continuous-mode pipelines lower latency per micro-batch at the cost of
more round trips; larger values amortize HTTP overhead. The default of
10000 balances commit frequency / visibility against HTTP overhead;
lower it (e.g. to 100) for tighter per-batch latency or higher (e.g.
100000+) for throughput-oriented batch backfills. Validated up front:
must parse as an integer **≥ 1** (curated error otherwise — the cap
bounds *emitted* rows, so a non-positive value would park forever
without emitting anything).

**Where the cap applies:**

- **Honoured** by every serial **cursor/delta** read path (flat /
  contained N+1) and by the `expand_contained=true` walks (which park
  a resumable depth-first drain — see below).
- **Ignored — with a warning — on flat and contained-N+1 snapshot
  reads** (no `cursor_field`, delta inactive): those shapes' streaming
  offset carries only the quiesce marker, no park state, so a cap
  could only truncate the snapshot and silently drop the remainder;
  their snapshot triggers always read the full table. An
  `expand_contained=true` **snapshot is the exception — it honours the
  cap** (its offset parks the resumable `pending_fetches` drain, so a
  capped snapshot spans multiple triggers and stamps the quiesce
  marker only once the drain completes).
- **Not applied on partitioned streaming** (`num_partitions` with a
  top-level cursor): microbatches are sized by the cursor window
  (fence to fence), not by this cap — the per-partition Spark tasks
  have no shared park state to resume a capped batch from.
- **Delta-tracking reads** enforce the cap at **page boundaries**
  (stop following `@odata.nextLink` once reached) and may overshoot by
  up to one server page — a mid-page stop would permanently skip the
  rest of that page, since the persisted resume link points at the
  *next* page.

**How the `expand_contained=true` drain enforces it.** The cap is
checked **per HTTP fetch at any depth** by a depth-first resumable
stack machine:

- The drainer walks one root→leaf path at a time, holding each
  parent's inline sibling collection as a single in-memory page frame
  and checking the cap between rows/fetches.
- When a batch parks, only the **boundary path from the root to the
  current leaf** is serialized — one item per contained segment on the
  current path — so the parked offset is **O(depth)**, *not*
  O(fan-out width). (An earlier breadth-first design parked one
  continuation per truncated inner collection, so a wide top page over
  an inner-paging server could balloon the offset to thousands of
  URL-carrying items and collapse throughput to ~one row per batch
  once the backlog exceeded its ceiling; depth-first eliminates that.)
- The parked frontier is `pending_fetches` — a list of
  `{url, level, chain, cur_val, skip, boundary}` items in
  bottom-to-top stack order (plus a transient `rebuilt` marker on
  items retried after a 404/410 recovery). The next `read()` re-pushes
  them to reconstruct the DFS path and resumes exactly where it
  stopped, **churn-safe via each item's chronological `boundary`
  order-key** — never a positional `$skip`, which could desync and
  drop rows under between-batch writes (`skip` remains only as a
  legacy downgrade fallback for offsets without a boundary).
- The frontier needs no explicit ceiling: depth-first descent holds
  only one root→leaf path at a time, so both the live stack and the
  parked `pending_fetches` stay O(depth) unconditionally, regardless
  of how wide any parent's inner collection grows.
- Cap deviation per batch is bounded by one HTTP response's worth of
  leaf rows (≤ `page_size`), regardless of how deep the chain is or
  how wide any single parent's inner collection grows.
- In cursor mode the watermark only advances once the chain fully
  drains; until then the running max sits in `running_max_cursor`.

**What counts toward the cap.** Only rows strictly above the committed
watermark: `cursor_lookback_seconds` overlap re-reads ride on top (so
an overlap window holding ≥ `max_records_per_batch` rows can't wedge
the stream into an eternal park/re-read cycle — a pure-overlap batch
completes and idles), meaning the cap may additionally overshoot by
the overlap size, which is bounded by the configured lookback window.

**Sizing note for large contained parent sets:** each resumed batch
re-pages the ancestor enumeration up to its park position (keys must
be fetched to be matched), so a full capped cycle over `P` parents
costs roughly `P²/(2·cap·server_page)` extra ancestor-page requests in
aggregate — negligible at `P=10k`, ~50× the single-pass cost at
`P=100k` with `cap=1000`. Size the cap so a cycle spans few batches on
very large parent sets.

### cursor_nulls

How a cursor read handles rows whose `cursor_field` is **null**:

- **`coalesce`** (default) — substitute a deterministic synthetic
  floor (a `2000-01-01…` timestamp carrying a per-PK sub-second
  offset, or the type's minimum for date/int/string cursors) for
  comparison and the watermark. The emitted row keeps its real
  `null`, the watermark always advances, and null-cursor rows are
  ingested once on the seed pass (server-side `cursor gt` excludes
  them thereafter, so null rows inserted *after* the seed aren't
  re-captured). The temporal floor year is configurable as
  `coalesce:<YYYY>` (e.g. `coalesce:1990`) — lower it below your
  oldest data; default `2000`.
- **`error`** — raise a no-progress `RuntimeError` when a batch's rows
  all have a null cursor (the watermark can't advance). Use to catch a
  misconfigured cursor.
- **`ignore`** — drop null-cursor rows entirely (never emitted).

Applies to **flat and contained leaf-cursor** paths; ancestor-cursor
and `expand_contained=true` paths treat nulls as `error`. For cursor
types with no synthesisable floor (boolean/binary) the default
silently falls back to `error`; setting `cursor_nulls=coalesce`
explicitly on such a cursor raises.

### pagination

How the connector walks a collection's pages. Four modes:

- **`auto`** (default) — follow the server's `@odata.nextLink`
  whenever it emits one (identical to `nextlink` for spec-compliant
  servers). If the server **never** emits a link during a walk, fall
  back to a keyset seek (if the `$orderby` has keys) or `$skip`, and
  drain until an **empty** page — so a server that silently
  page-limits responses *below* the `$top` you request *and* omits
  `@odata.nextLink` (a common non-compliant shape, e.g. one that
  suppresses the link whenever `$top` is sent) is still read in full,
  with no per-table override.
- **`nextlink`** — follow `@odata.nextLink` only. Strictly
  spec-compliant, and the choice when you want a `$top`-free snapshot
  scan (some servers reject or mishandle an explicit `$top`).
- **`keyset`** — ignore `@odata.nextLink` and always seek the next
  page with a `(k gt <last>)` predicate on the `$orderby` key set
  (`cursor`+PK, or PK-only).
- **`skip`** — ignore `@odata.nextLink` and page via `$top`+`$skip`.
  The keyless fallback (entities with no unique sort key); O(n)
  offsets and fragile under concurrent writes.

Use `keyset`/`skip` explicitly only to *force* the seek strategy (e.g.
to ignore a buggy `@odata.nextLink` entirely). `auto`/`keyset`/`skip`
require a `$top`, so they force a default `page_size` when none is set
— including on snapshot scans under the default `auto`.

**Termination differs by mode.** `nextlink` treats a *short* page
(`len < $top`) with no continuation link as the end. `auto`, while the
server is emitting links, trusts that short link-less final page as
the end **only when the chain stopped *before* the `$top` budget was
reached** (`fetched < $top`). OData `$top` is a **total-result limit**
(§11.2.5.3), not a page size, and a spec-compliant server may
propagate the remaining budget through its skiptoken
`@odata.nextLink`s (e.g. Northwind: `$top=1000` → page 1's link
carries `$top=500` → after 1000 rows no further link, though the
collection has more). So if the link chain self-terminates at exactly
the budget (`fetched >= $top`), `auto` does **not** trust the short
final page — it seeks past the budget (keyset or `$skip`) and keeps
draining until empty; otherwise any table larger than `page_size`
would be silently capped at `page_size` rows. `auto` (once it has
fallen back, i.e. the server gave no link), `keyset`, and `skip`
instead drain until an **empty** page — a short non-empty page is NOT
exhaustion, because the server's per-response page size may be
**smaller than the `$top` you request**. The drain costs **one
trailing empty request per collection that ends on a short page**; a
spec-compliant server that keeps emitting `@odata.nextLink` incurs no
extra request.

**Composite keyset seeks and the OR-capability preflight.** A
composite keyset seek (`$orderby` = `cursor`+PK) builds an
OR-across-different-columns `$filter` —
`(cursor gt v) or (cursor eq v and pk gt p)` — which some servers
reject (Hexagon Smart API: "on different columns, only AND operators
are supported"). Before issuing such a seek the connector runs a
one-shot, cached, **auth-aware OR-capability preflight** (`$top=1`
carrying the OR filter). On a **definitive, non-transient 4xx** (e.g.
the "AND operators only" 400) it transparently **falls back to
`$skip`** for that and every later walk, so the OR rejection never
surfaces as an error or a dropped page. It applies the same
definitive-vs-transient discipline as the other capability probes: a
**transient** status (429/5xx) or a **transport/auth failure** (a
`401` is refreshed, not misread as "unsupported") fails *open* for
that seek and records **nothing**, so the next seek re-probes rather
than durably pinning the slower `$skip` walk on a momentary blip. A
single-key `$orderby` builds no OR and is never probed. The verdict is
**threaded into the resume offset** (`or_filter_ok`, alongside
`cursor_probe_ok`/`batch_ok`) so a reader the framework recreates each
microbatch skips re-probing; these capability flags are bookkeeping
and are excluded from the no-progress comparison.

**No-progress guard.** A walk stops with a warning if a continuation
returns a page identical to the one before it (the server ignored the
seek/`$skip`) or if the server hands back a self-referential/cyclic
`@odata.nextLink`. This protects **every** mode against infinite
loops — the client-driven modes, plain `nextlink`, and the
delta-tracking walk.

**Scope.** The pagination mode applies to **every page walk**: flat
reads, the contained leaf-cursor cap walk (the compound
`(cursor eq V and pk gt last)` seek even drains a same-cursor cohort
larger than a page), the ancestor walk, parent/ancestor enumeration,
partitioned discovery, the top-level `$expand` collection, **and a
parent's *inline child* collection inside `expand_contained=true`**.
For the last case: when a parent's inline child page comes back full
(`len == inner $top`) but the server omits the
`<NavProp>@odata.nextLink`, the connector synthesizes a
direct-navigation continuation —
`Parent(key)/Child?$top=…&$expand=<grandchildren>` plus the keyset
seek (or `$skip`) — and drains the rest, with the grandchildren still
expanded and the cursor `$filter`/`$orderby` re-applied. This
continuation flows through the same depth-first resumable drain, so
`max_records_per_batch` and cross-batch `pending_fetches` resume cover
it too. `expand_contained=false` (N+1) with `pagination=keyset`
remains a valid alternative for these servers.

### delta_tracking

Opt-in OData v4 delta queries. Values:

- **`disabled`** (default) — no behavior change.
- **`auto`** — probe once, fall back to cursor/snapshot if the server
  doesn't acknowledge.
- **`enabled`** — require support; error if the server doesn't
  acknowledge.

Under `auto`, a stream **pins its first read shape**: the delta path
is sticky by offset shape (`delta_link`/`next_link`), and a fallback
decision persists as `delta_ok: false` in the resume offset — pinned
by the offset's *shape* alone (any non-empty snapshot/cursor offset)
even when a transient first-batch probe left no stamped verdict — so a
server that flaps its `Preference-Applied` acknowledgement can't flip
an established stream's read shape away from the schema frozen at
setup (which would silently drop the synthetic columns or fail
parsing). Explicit `enabled`/`disabled` scrubs the flag, and switching
back to `auto` re-probes.

See [Delta tracking](#delta-tracking) below for the full contract,
supported services, and caveats.

### expand_contained

For contained-collection tables (`Parent__Child__...` paths):

- **`true`** — one `GET Parent?$expand=Child($expand=...)` per
  pipeline trigger instead of the N+1 traversal (one parent fetch +
  one per-parent leaf fetch).
- **`false`** — force the N+1 traversal.
- **`auto`** (**default** when unset) — attempt the `$expand` read
  behind a one-shot **behavioural preflight**, described next.

**The `auto` preflight.** It issues the *real* nested-`$expand` URL
(small page budget, top-level `$top=1`, with the same inner
`$top`/`$orderby`/`$filter` constructs the read would send — including
a synthetic `cursor gt <floor>` when no watermark exists yet, so the
inner-`$filter` construct is exercised up front) and verifies inline
child collections actually come back at every level, cross-checked
against one direct-navigation `$top=1` GET (carrying the same level
`$filter`) so a server that accepts the URL but **silently ignores
`$expand`** is caught rather than dropping every deep row. **Only a
conclusive pass runs the expand read** — `auto` never assumes
`$expand` works before the verdict is in. Anything else falls back to
the N+1 walks (`expand_contained=false`) for that batch, never raising
on a capability shortfall: a definitive failure (hard 4xx, or
ignored-`$expand`) is recorded so later batches skip the probe; a
transient blip or an inconclusive sample (empty top set, or a
genuinely childless probed branch — indistinguishable from an ignoring
server) records nothing and re-probes next batch. The N+1 shape is
always correct, so an unresolved verdict only costs request shape,
never rows.

**Verdict reset.** The PASS persists in the resume offset as
`expand_ok` — the offset never carries a fail: checkpoints are
immortal, and a baked-in false would skip the preflight even after the
server is fixed; definitive fails live only in the 15-minute
process-wide cache, so a fixed server gets re-probed. Both outcomes
ride the process-wide capability cache (see
[Capability-verdict caching](#capability-verdict-caching)) — that is
what spares **snapshot** (cursorless) contained streams and
batch-reader refreshes, whose offsets stay bare, from re-probing every
trigger. An explicit non-`auto` value (`true`/`false`) scrubs
`expand_ok` from the outgoing offset **and purges the cache entry**,
so re-selecting `auto` (or unsetting the option) re-runs the
preflight. Because both modes share the same `cursor` watermark key,
an `auto` verdict flip mid-stream degrades to a re-read from the held
watermark (MERGE-deduped), never loss.

**Interplay with partitioning and snapshot cadence.** Partition
activation follows `auto`'s **resolved** shape: a verified server
reads via `$expand` (one request — nothing to parallelise, not
partitioned), while a preflight failure keeps the table on the N+1
shape **with its partitioned parallelism intact** (the streaming
`get_partitions` never re-probes — the shape is fixed at stream setup
by `is_partitioned`). For **snapshot** (cursorless) streams the
resolved shape also sets the refresh cadence: the partitioned N+1
snapshot stream re-snapshots **every trigger** (wall-clock epoch
offsets), while the single-`$expand` snapshot stream reads once and
quiesces until its checkpoint is reset (the `snapshot_done` marker
persists in the checkpoint, so a checkpoint-preserving restart stays
quiesced — use a full refresh to re-snapshot). So flipping
`expand_contained` (or a differing preflight verdict) changes how
often a snapshot stream re-reads, not just its request shape.

An explicit `cursor_probe=nested-expand`/`batch` applies only when
`auto` falls back to the N+1 walk. See
[Contained navigation properties](#contained-navigation-properties).

### contained_fetch

How the **full** (un-cursored) contained walks hydrate each
leaf-parent collection — the **snapshot read** (a contained table with
no `cursor_field`) and the **framework batch-reader stream**
(`LakeflowBatchReader`, `start_offset=None`, used for batch /
full-refresh ingest). Accepts `auto`, `batch`, `single`, the
size-suffixed `auto:<N>` / `batch:<N>`, or a positive integer *N*:

- **`auto`** (default) — packs the per-leaf-parent GETs into OData
  **`$batch`** requests (chunked to **1000 operations/request**,
  server-driven paging with `@odata.nextLink` follow-up), collapsing
  *M* leaf-parent round-trips into `ceil(M/1000)`. A one-shot
  capability preflight gates it; on a server without `$batch` it
  transparently **falls back to `single`**. Only a **definitive**
  preflight outcome — a working envelope, or a hard rejection like
  404/405 — is recorded; a transient failure such as a 503 or a
  network blip degrades that batch only and is re-probed on the next,
  so a momentary blip never pins the stream to the slow path.
- **`batch`** — the same hydrate but **strict**: a server that fails
  the `$batch` capability preflight is an **error**, not a silent
  fall-back. Use it when you know the server supports `$batch` and
  want to be told loudly if a deployment doesn't.
- **`auto:<N>`** / **`batch:<N>`** — set the chunk size to `N`
  operations/request (`ceil(M/N)`, e.g. `batch:200` to start smaller)
  while keeping `auto`'s fall-back / `batch`'s strictness
  respectively.
- **A bare positive integer *N*** — like `batch:<N>` (strict); `N=1`
  is equivalent to `single`, `N>1` tunes the batch size.
- **`single`** — the original behaviour: one GET per leaf-parent.

**Adaptive sizing (all batch modes).** If the server rejects a batch
for carrying too many sub-requests — matched across phrasings, e.g.
*"OData batch message contains too many parts"* or *"$batch exceeds
the maximum of N operations"* — the connector shrinks the working size
by 25% and retries (up to 10 times — enough for the geometric shrink
from 1000 to converge below a ~100-part server cap) before falling
back to a plain per-leaf-parent GET. The discovered working size is
**recorded once** and persisted in the resume offset
(`batch_size_ok`, alongside `batch_ok`) so later batches and
framework-recreated readers reuse it. (Strict mode errors only on the
*preflight* — "server can't `$batch` at all"; the adaptive size
give-up is a runtime resilience step that still degrades to GETs.)

**Per-sub-request failures inside a 2xx envelope are never silently
skipped**: a sub-response with an error status (e.g. one throttled
leaf-parent) is re-issued as a plain GET — a transient (429/5xx)
recovers through the normal retry/backoff path, a hard 4xx raises with
the server's actual error — so a failed part can't quietly drop that
parent's rows (which on a cursor walk would otherwise advance the
watermark past them permanently).

**Verdict reset.** The `$batch` capability + size verdicts
(`batch_ok` / `batch_size_ok`) are **shared** with the `cursor_probe`
`auto` cascade's hydrate, so they persist in the offset while **any**
auto-mode consumer is live — `contained_fetch` `auto`/`auto:<N>`, or
`cursor_probe` `auto` with the hydrate not suppressed by an explicit
`single`/`1`. Only when every consumer is pinned non-`auto` are they
scrubbed from the outgoing offset, so switching back to `auto` later
**re-runs the preflight** (an all-pinned config carries no offset
verdict, but the process-wide capability cache still spares the
per-microbatch re-probe within a run).

**Relationship to `cursor_probe`.** This option mostly governs the
**full walks** (no cursor filter / no resume), distinct from
`cursor_probe` which accelerates the *incremental* leaf-cursor read
(streaming, with a watermark). One cross-over: an explicit
`contained_fetch=single` / `1` also forces the incremental leaf-cursor
hydrate down the plain N+1 walk — both the `cursor_probe` probe's
dirty-parent hydrate (the probe still prunes which parents to read;
only its `$batch` hydrate is suppressed) and `auto`'s no-probe
`$batch` cascade. The one exception is an explicit
`cursor_probe=batch`, a direct demand for the `$batch` hydrate that
wins the conflict.

Emitted rows are identical regardless of value — only the request
batching differs. No effect on flat tables or `expand_contained=true`.

### cursor_probe

Request-count optimization for **incremental** reads of **deep**
contained paths with a **leaf** `cursor_field` and **sparse** changes
— the `expand_contained=false` counterpart to `expand_contained=true`.
The plain N+1 incremental read enumerates every leaf-parent and issues
one `cursor gt since` leaf fetch under each — thousands of
mostly-empty requests when few leaves changed. Four values select the
acceleration strategy:

- **`auto`** (default) — a best-effort cascade. Use the
  nested-`$expand` change-probe where it can pay off *and* the server
  is verified to honour `$orderby`/`$top` inside `$expand`; otherwise
  fall back to a `$batch` hydrate (where the server supports `$batch`,
  and unless an explicit `contained_fetch=single`/`1` suppresses it);
  otherwise the plain N+1 walk. **Never raises** on a
  server-capability shortfall — it degrades to a correct, slower
  strategy.
- **`nested-expand`** — strict nested-`$expand` probe. **Raises** a
  clear error if the path can't use it or the server mis-orders inner
  `$expand` ("I require the probe"). The dirty leaf-parents it
  identifies are then hydrated via OData `$batch` when the server
  supports it (a fail-closed preflight falls back to the plain N+1
  walk) — the probe prunes *which* parents to read, `$batch` batches
  the hydrate of whichever remain. An explicit
  `contained_fetch=single` (or `1`) overrides this: the probe still
  prunes, but the dirty parents are hydrated via the plain N+1 walk
  (the `$batch` preflight is skipped entirely).
- **`batch`** (or **`batch:<N>`**) — skip the probe; hydrate the
  changed leaves via OData `$batch` (server-driven paging, no `$top`,
  `@odata.nextLink` follow-up, chunked to 1000 operations/request by
  default, or `N` with the `batch:<N>` form — e.g. `batch:200` to
  start smaller), falling back to the plain N+1 walk if the server
  doesn't support `$batch`. If the server rejects a batch for too many
  sub-requests (matched across phrasings — *"too many parts"* or
  *"exceeds the maximum of N operations"*), the size is shrunk 25% and
  retried (up to 10 times, enough to converge below a ~100-part cap)
  before falling back, and the working size is recorded once in the
  offset (`batch_size_ok`) for reuse. Safe on servers (e.g. Hexagon
  Smart API) that reject nested-`$expand` options, since it relies
  only on top-level single-column `cursor gt` filters.
- **`false`** — force the plain N+1 walk.

**Probe mechanics.** The probe issues **one shallow probe per
leaf-grandparent tuple** —
`…/<LeafParent>?$select=<pk>&$expand=<Leaf>($orderby=<cursor> desc;$top=1;$select=<cursor>)`
— reads each leaf-parent's **newest leaf**, and marks it dirty when
that leaf's cursor is `> since` (compared client-side). It then
hydrates **only** the dirty leaf-parents — via OData `$batch` when the
server supports it (`_verify_batch_support` is fail-closed, so an
unsupported server transparently falls back to the per-parent N+1 walk
over the same dirty set). Emitted rows are identical to `false`
**provided the probe identifies the dirty parents correctly**; the
watermark, `max_records_per_batch` cap, key-based `parent_keys` resume
(churn-stable; `parent_idx` is the legacy fallback), and no-progress
guard are all reused unchanged (a resumed batch re-probes skipped
parents — probes, no leaf fetches). Ordering the inner `$expand` by
the cursor descending means the single returned row is the max-cursor
leaf *by construction*, so a server that applies `$top` before
anything else still returns the right row — and there is **no inner
`$filter`** to mis-order against (the change test is client-side).

**Where the probe engages.** Only where it can pay off: the cursor on
the **leaf**, *and* the **distance from the leaf to the nearest
batch-snapshot (non-cursor) ancestor > 1** — i.e. the leaf's *parent*
collection is itself cursor-bearing (incremental, high-fan-out). So
`…/WorkPackageDetails/WorkPackagesStepDetails` (leaf-parent
`WorkPackageDetails` is cursor-bearing) qualifies, while
`Instances/Projects/WorkPackageDetails` (leaf-parent `Projects` is
snapshot — few rows, all dirty, nothing to skip) does not. Depth alone
is not the criterion.

**`$batch` hydrate.** Issues the same per-leaf-parent
`cursor gt since` reads as the plain walk but packs them into `$batch`
requests, so *M* leaf-parent round-trips collapse to `ceil(M/1000)`
(or `ceil(M/N)` with `cursor_probe=batch:<N>`, auto-reduced on a "too
many parts" rejection); resume is chunk-aligned on the last drained
chain's `parent_keys` (an exclusive, key-matched park; `parent_idx`
rides along for downgrade compatibility) and the cap is overshot by at
most one chunk's worth of changed rows. A 200 `$batch` envelope with a
truncated/malformed JSON body — the largest response the connector
ever receives, and the shape some servers produce under load — is
re-POSTed once (GET-only sub-requests, so the retry is safe) before
raising.

⚠️ **The probe's identify step relies on the server honouring
`$orderby`/`$top` inside `$expand`** (optional OData v4 features). A
server that ignores `$orderby` could return a non-newest leaf and
report a dirty leaf-parent as **clean**, dropping its changed leaves.
The connector runs a **one-time behavioural capability check** before
engaging the probe (it verifies the inner `$expand` returns the true
newest leaf for a real multi-leaf parent, cross-checked against
trusted direct-navigation ordering). This catches **both** failure
modes: a server that silently **mis-orders** inner `$expand` (returns
an older leaf), and one that outright **rejects** the inner-`$expand`
options with an HTTP error (e.g. Hexagon Smart API 400s on inner
`$orderby`/`$top`/`$select`). Under `auto` either failure **cascades
to `$batch`/the plain walk** (a rejection is recorded as a definitive
`cursor_probe_ok=false`); under `nested-expand` either **raises** an
actionable error — never a raw HTTP error.

**Race handling in the capability check.** A mismatch where the
probe-shaped `$expand` returns a leaf **newer** than the
direct-navigation reference is a concurrent-write race (the two
fetches aren't atomic), not mis-ordering evidence — a genuinely
mis-ordering server returns an *older* leaf — so that sample is
**skipped** and the scan moves on to another, exactly like a
non-discriminating one. Only clean evidence (an *older*/missing inner
leaf) is a definitive fail; nothing about a race is recorded, and one
unlucky write can neither abort the whole preflight nor raise strict
mode. A preflight that errors out **before reaching a verdict** — the
parent-enumeration or trusted-reference fetch itself fails
(indistinguishable from a transient blip, unlike the probe-shape
rejection whose sibling fetches just succeeded) — likewise records
**nothing**: under `auto` that read degrades to the `$batch`/plain
cascade and the next batch re-probes; `nested-expand` raises an
actionable error rather than the raw HTTP failure.

**Verdict caching and reset.** Both the probe-verified and
`$batch`-supported verdicts are cached per table and persisted in the
resume offset (`cursor_probe_ok` / `batch_ok`) so a
per-batch-recreated reader skips the capability requests — the
`$batch` preflight persists only **definitive** verdicts (a transient
failure like a 503 degrades that batch to the plain walk and is
re-probed on the next, never pinning the stream to the slow path).
Under `auto`, **both** definitive preflight outcomes additionally ride
the process-wide capability cache (see
[Capability-verdict caching](#capability-verdict-caching)): the offset
only ever carries the *pass*, so without the cache a mis-ordering
server would re-pay the preflight GETs on every framework-recreated
reader before cascading to `$batch`. `cursor_probe_ok` is persisted
only while `cursor_probe` is `auto`; any non-`auto` value
(`nested-expand`, `batch`, `false`) scrubs it from the outgoing offset
**and** purges the per-table cache entry on the next read (so the
reset also reaches the bare-offset snapshot / batch-reader paths), so
switching back to `auto` later **re-runs the probe preflight**. The
strict `nested-expand` mode neither consults nor records the shared
cache — it re-verifies each microbatch so its error always carries
fresh evidence.

**Validation.** An explicit `cursor_probe=nested-expand`/`batch`
raises if combined with `expand_contained=true`, used on a flat table,
or used without a `cursor_field`; `nested-expand` additionally raises
with the cursor on a non-leaf ancestor or where the leaf-parent is a
snapshot level (distance 1).

### num_partitions

Number of Spark partitions for parallel reads of contained-collection
tables. Honored whenever the read resolves to the N+1 walk: a
contained path with `delta_tracking=disabled` and
`expand_contained=false` — or `auto` whose preflight fell back to N+1.
**Streaming** additionally requires any `cursor_field` to live on the
top-level entity (the `SupportsPartitionedStream` gate); **batch**
reads partition any N+1 contained path, leaf-level cursors included.
Top-level rows are bin-packed into this many contiguous slices; each
Spark task walks only its assigned subtrees. Ignored for
non-partitionable tables (they fall back to single-task reads).

- **Null-cursor top parents are rejected** on this path (curated error
  at partition discovery): once a fence is committed, the `cursor gt`
  discovery filter excludes them server-side, so their subtrees'
  future changes would be dropped silently. The first (unfenced) batch
  sees them in discovery, and every later batch runs a one-request
  `eq null` probe so a null-cursor parent *inserted mid-stream* is
  caught too (best-effort: a server rejecting the `eq null` filter
  keeps the first-batch-only check). Exclude them with
  a top-segment `filters_at` entry (`<cursor> ne null`), fix the data, or read
  serially.
- **The fence probe self-checks `$orderby` honoring**: the per-trigger
  watermark comes from one `$top=1&$orderby=<cursor> desc` request,
  and a server that silently ignores `$orderby` would pin the fence at
  a stale value and stall the stream with data pending. So after each
  probe the connector one-time-verifies it: `<cursor> gt <probed max>`
  should return nothing, and a contradiction is disambiguated by one
  desc re-probe (a busy source can legitimately insert a row between
  the two requests — an honoring server's re-probe then returns the
  fresh max, recording the PASS; only a re-probe still below the
  contradicting row proves `$orderby` is ignored). The PASS verdict
  rides the shared capability cache as `fence_desc_ok` (re-checked
  after its 15-min TTL); a proven desc-ignoring server raises an
  actionable error instead of stalling. (Edge: on a desc-ignoring
  server whose churn happens to surface a fresh max to the re-probe,
  the check can cache a false PASS for up to the 15-min TTL — one
  bounded silent-stall window — after which the re-check fires and
  raises the correct error.)
- **A parent deleted mid-batch is skipped with a warning** (its
  404/410 can't fail the frozen partition descriptor's task retries;
  the next batch re-discovers the live parent set — matching the
  serial walks' self-healing).
- **Overlap posture**: `cursor_lookback_seconds=auto` resolves to
  **0** on partitioned streams (no walk-duration history rides the
  offset), so rows landing at-or-below the fence after it was probed
  are only re-scanned if you set an **explicit**
  `cursor_lookback_seconds` — serial cursor streams self-tune this
  overlap, partitioned ones don't.
- **Framework notes**: on pure **batch** reads a partition-planning
  failure (including an invalid `num_partitions`) is swallowed by the
  framework and silently degrades to a serial read; and partition
  planning materializes the discovered parent slice on the driver
  (fine at 10k–100k parents, budget driver memory for millions).

### cursor_lookback_seconds, cursor_lookback_factor, cursor_lookback_max_seconds

Overlap window for incremental **contained** cursor reads over a
**continuously-changing** source. None of the contained cursor walks
is a consistent snapshot — the walk takes many seconds, during which
the source keeps changing — and the connector commits the watermark as
the max cursor it saw. A row inserted *during* the walk under a
leaf-parent the walk already passed (or one the probe already flagged
clean) lands with a cursor below that final max and would be skipped
forever by the next `cursor gt <max>`.

With a window set, each batch reads from
`cursor gt (committed − window)` instead, re-scanning the overlap so
those mid-walk arrivals are captured on the next progressing batch
(re-read rows are idempotent at the destination via `apply_changes`
MERGE on the primary key).

**Applies to all four non-atomic contained cursor walks**:
`expand_contained=true`, the plain N+1 leaf-cursor walk, the
`cursor_probe` walk, and the **ancestor-cursor** walk (there the
window floors the *ancestor enumeration* filter — a dirty ancestor
re-included by the overlap gets its whole subtree re-read,
duplicate-safe — recovering parents whose cursor advanced mid-cycle to
below the cycle's final watermark). For `cursor_probe`, the floored
value also re-arms the probe's dirty-detection, so a leaf-parent whose
newest leaf fell in the overlap is re-flagged dirty and re-hydrated.

The **partitioned streaming** path (`SupportsPartitionedStream`,
level-0 cursor) honors an explicit integer window too — its fence is
probed *before* partition discovery, so the overlap re-scans rows that
landed at-or-below the fence mid-batch (a row landing at exactly the
fence AFTER the stream went quiescent is only recovered once some
newer row advances the fence and unblocks the no-progress gate — on a
permanently quiescent source it stays invisible). `auto` is a no-op
there (no walk-duration history rides the partitioned offset), so set
an integer window explicitly for that shape.

**The committed watermark is never floored** — only the read filter is
— so the offset still advances to the true max, and it never
*regresses* either: a completing batch's max is floored at the prior
watermark, so an overlap re-read whose watermark-defining row was
deleted between batches can't walk the offset backwards; a quiescent
trigger (no row beyond the watermark) idles instead of looping (with
[`cursor_lookback_dedup`](#cursor_lookback_dedup) — the default —
overlap rows not yet tracked, or changed in place, are delivered once
first; already-tracked unchanged rows idle immediately).

**Values for `cursor_lookback_seconds`:**

- **`auto`** (default) — self-sizes the window from the **max** walk
  duration over the last few completed walks (persisted in the offset
  as `lb_history`) × `cursor_lookback_factor` (default 1.5), clamped
  to `cursor_lookback_max_seconds` (default 3600). A walk that
  `max_records_per_batch` caps into several batches records its
  **whole cycle's wall-clock span** — first capped batch to
  completion, trigger intervals included (anchored by
  `lb_cycle_started` in the offset) — since the churn-exposure window
  of a capped cycle is the full span, not one batch's drain time.
  Using the max of recent walks — rather than the last value × a large
  fudge — makes the estimate robust to a single slow spike. No manual
  guess; a no-op until the first walk is measured, outside the
  contained cursor paths, and for non-timestamp cursors.
- **An integer** — a fixed window in cursor units (seconds for a
  timestamp cursor). Set it at or above the worst-case walk duration.
  Requires a timestamp `cursor_field` on a contained path (else
  raises).
- **`off`** (or `0`) — disables the overlap (exact prior behaviour).

Trade-off: a non-zero window re-**fetches** the trailing overlap each
batch — `auto` keeps that HTTP cost proportional to the actual walk
time. The downstream cost (re-emitting and re-MERGing unchanged rows)
is absorbed by [`cursor_lookback_dedup`](#cursor_lookback_dedup),
which is on by default and filters as rows stream in — so peak memory
scales with genuinely-changed rows, not window size. With dedup `off`
every in-window row is delivered (and buffered) each batch; on
high-churn sources size the window via `cursor_lookback_max_seconds`
with that in mind.

**`cursor_lookback_factor`** (default 1.5) — `auto`-mode only:
multiplier applied to the max recent walk duration when sizing the
window. Margin for a walk slower than any recently observed. Must
be > 0; values < 1 risk under-covering (dropped rows). Ignored unless
`cursor_lookback_seconds=auto`.

**`cursor_lookback_max_seconds`** (default 3600) — `auto`-mode only:
ceiling clamp (runaway backstop) on the computed window, in seconds.
Must be > 0. Ignored unless `cursor_lookback_seconds=auto`.

### cursor_lookback_dedup

Suppresses the redundant re-upserts a lookback window produces. With a
window active, every row still inside it is re-fetched **and re-emitted**
each batch — correct (the destination MERGE is idempotent) but
write-amplifying: for SCD_TYPE_1 an identical-values MERGE still rewrites
Delta files. (SCD_TYPE_2 already absorbs most of this via its change
detection.) The re-fetch itself cannot be avoided — the server-side
filter is `cursor gt <floored value>` — so this option saves the
downstream emit/parse/MERGE, not HTTP.

Values: **`on`** (the default — dedup with the default 5000-entry cap),
**`off`** (restore the blind re-emit), or a **positive integer** (dedup
with that entry cap). `true`/`false` are accepted aliases for
`on`/`off`, matching the connector's other flag options. Upgrading an
existing pipeline is seamless: a checkpoint without `lb_seen` just
re-emits one overlap's worth of rows (the pre-dedup behavior) and
tracking engages from that batch on.

When enabled, the offset carries `lb_seen`: an **exact** map of
`{composite-PK → [cursor, content-hash]}` for the rows delivered from
the current window. A re-fetched row whose key **and full content hash**
match its entry was already delivered unchanged and is dropped from the
batch. Hashing the whole row — not just PK+cursor — means a source that
updates columns *without* advancing the cursor still gets its in-window
change delivered, exactly as the blind re-emit would have.

Mechanics and guarantees:

- **Self-pruning**: the set is rebuilt each batch from the rows actually
  fetched — every in-window row is re-fetched every batch by definition,
  so the current fetch *is* the next window's candidate population.
  Aged-out and source-deleted rows drop out with no window arithmetic.
- **Streaming memory bound**: suppression happens **at emit time**, as
  rows stream off the wire — a proven-unchanged overlap row never
  materializes in the batch buffer, only its ~100-byte seen-entry does.
  Peak memory therefore scales with the rows actually delivered
  (genuine changes + new rows — the irreducible floor, since the
  offset requires a full batch scan), not with the window's total
  churn. With dedup `off` the whole window's rows are delivered every
  batch by contract, so memory tracks window churn — size the window
  (`cursor_lookback_max_seconds`, or an explicit value) accordingly.
  (A future refinement could additionally cap *delivered* overlap rows
  per batch with park/resume — sound only on top of the seen-set,
  whose committed entries are what let a paged cycle converge instead
  of re-delivering forever; not built until a real workload churns an
  entire window per trigger.)
- **Bounded offset**: above the entry cap the highest-cursor entries are
  kept (they stay in the window longest) and the remainder degrade to
  plain re-emits — the pre-dedup behavior — with a one-time warning.
  Entries cost roughly 60–100 bytes each; size the cap against your
  checkpoint-size budget.
- **Exactness is mandatory**: every failure direction (capped-out entry,
  mid-cycle resume shrinkage, downgrade to a build without the option)
  is a redundant re-emit, never suppression of an undelivered row. A
  probabilistic structure (e.g. a Bloom filter) is ruled out because a
  false positive would suppress a never-emitted row — silent loss.
- **No semantics shift**: suppressed rows always sit at-or-below the
  committed watermark, so suppression cannot affect watermark
  progression, `max_records_per_batch` accounting (overlap rows are
  already excluded from the cap), or the no-progress guard (`lb_seen`
  is `lb_`-prefixed bookkeeping, stripped from the progress
  comparison). A batch whose rows are all suppressed idles exactly like
  a pre-dedup pure-overlap batch. One deliberate improvement over the blind
  re-emit: a **genuine in-window change is delivered promptly even on a
  quiescent trigger** — the `lb_seen` delta carries the offset progress —
  where the pre-dedup idle rule deferred all overlap rows to the next
  progressing batch.
- **Scope**: the four non-atomic contained cursor walks — the only
  paths a lookback window applies to (see
  [the lookback options](#cursor_lookback_seconds-cursor_lookback_factor-cursor_lookback_max_seconds))
  — whenever the window is active. Inert when the window resolves to 0,
  on flat top-level cursor reads (no lookback, so nothing to dedup),
  and on partitioned streams (per-partition emits share no seen-set;
  their explicit window re-emits the overlap every batch). Only useful
  with a **timestamp** cursor — the only shape that gets an overlap
  window: under `auto` a non-timestamp cursor's read floor is a
  deliberate no-op, so dedup would track rows without ever having a
  re-read to suppress (harmless, but pure offset weight — set it
  `off` there).

### exclude_ancestor_columns

Comma-separated list of synthetic ancestor-FK column names to **drop
from the destination** for a contained-collection table. By default
every non-leaf ancestor's PK is prepended as a `<segment>_<pkname>`
column (see
[Contained navigation properties](#contained-navigation-properties));
list the resolved column names here (e.g. `Instances_Id,Projects_Id`)
to omit them, or a lone `*` to drop **all** ancestor-FK columns at
once.

Excluded columns disappear from the table schema, the stamped rows,
**and the composite primary key** alike — so only exclude columns not
needed for destination-key uniqueness, otherwise distinct leaf rows
under different ancestors can collide on MERGE.

**Only synthetic ancestor-FK columns can be excluded** — naming a real
leaf/own table column (or a name that matches nothing) leaves the
schema untouched and logs a warning, so the option can never drop an
actual source column. No effect on flat tables.

### Capability-verdict caching

The preflight verdicts above — `expand_ok`, `batch_ok`,
`batch_size_ok`, `or_filter_ok`, `cursor_probe_ok`, and `delta_ok`
(the `delta_tracking=auto` probe's pinned fallback decision) — live in
up to three layers, consulted in order:

1. **The resume offset** (streaming cursor reads) — durable across
   restarts via the checkpoint.
2. **The connector instance** — dedupes within one read.
3. **A process-wide cache keyed by `service_url`** with an on-disk
   mirror (tempdir JSON, 15-minute TTL, plain booleans only).

The last layer exists for reads whose offsets *can't* carry verdicts —
streaming **snapshot** offsets hold only the `snapshot_done` quiesce
marker (the pyspark simple-reader wrapper accepts quiescence solely as
an *empty batch with an unchanged offset*, so the marker must stay
byte-stable across triggers and verdicts must not ride it), and the
**batch reader** (pipeline snapshot refresh / full refresh) discards
offsets entirely — which would otherwise re-run their preflights on
every framework-recreated instance, i.e. every microbatch or trigger.

Rules:

- **Only definitive verdicts are recorded** at any layer (a transient
  blip — or a race-contaminated `cursor_probe` sample — records
  nothing and re-probes next batch). `cursor_probe_ok` joins the
  shared cache only under the `auto` cascade (the strict
  `nested-expand` mode neither consults nor records it).
- **The on-disk mirror is written atomically** (per-process,
  per-thread temp file + rename) so a concurrent worker never reads a
  half-written file. It is re-parsed only when its mtime changes
  (otherwise the hot lookup is a single `stat`), and all in-process
  access is serialized by a lock so concurrent streaming queries on
  one driver can't corrupt the shared dict.
- **Reset on a non-`auto` switch:** the **per-table** verdicts
  (`expand_ok` / `cursor_probe_ok`) are purged from the process/file
  cache — scoped to just that table — on the next read whenever their
  governing option is non-`auto`, which covers the bare-offset
  snapshot and batch-reader paths the offset scrub can't see; so for
  those, re-selecting `auto` always re-runs the preflight. The
  **server-wide** `$batch` verdicts (`batch_ok` / `batch_size_ok`)
  can't be table-scoped, so they're purged only on the cursor-stream
  **offset transition** (conservatively, so pinning one table's
  `$batch` off doesn't churn a sibling's live `auto` consumer). The
  OR-keyset verdict (`or_filter_ok`) is likewise server-wide and is
  scrubbed + cache-dropped when an explicit `pagination=skip`/
  `nextlink` is set (modes that never consume it) — pin one of those
  for a batch and unpin to force a re-probe, e.g. to clear a
  wrongly-false verdict persisted in an old checkpoint.
- **The on-disk mirror files are per-user** (owner-tagged filenames +
  an ownership check before reading), so a shared multi-user tempdir
  can neither poison nor leak verdicts across accounts.
- A consequence: a **snapshot-only** table (no cursor, no offset) that
  pins `contained_fetch` off and later returns to `auto` keeps the
  cached `batch_ok` until the disk TTL or a process restart — harmless,
  because the `$batch` path degrades to plain per-parent GETs if a
  batch attempt is ever rejected, so a stale `batch_ok=true` costs at
  most one failed batch, never rows.

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

### Delta-tracking caveats

- **Sparse property updates are rejected.** OData v4 §11.4 lets the
  server return only the *changed* properties on an update. Applying
  that as-is would write NULLs over good values at the destination, so
  the connector raises `RuntimeError` if a non-tombstone delta entry is
  missing any property the schema declares. Workaround: restrict the
  schema via `$select` to fields the server always returns, or fall
  back with `delta_tracking=disabled`.
- **Token expiry triggers a re-read.** When the server returns HTTP 410
  on a stored link the connector recovers silently: a 410 on a parked
  mid-pagination `next_link` first retries the retained prior
  `delta_link` (replaying only the changes-since window); a 410 on the
  delta link itself re-reads the entire entity set via a fresh `Prefer`
  GET. Re-fetched rows MERGE cleanly by primary key at the destination —
  no inserts or updates are lost, but HTTP cost spikes for that batch,
  and **rows deleted at the source while the token was expired are not
  recovered**: the re-bootstrap emits only currently-existing rows as
  upserts, so no tombstone ever arrives for the gap deletions and the
  destination retains them until a full refresh. A **non-410 4xx** on a
  stored link (some gateways answer 404/400 for an expired token) is NOT
  auto-recovered — it raises an actionable error naming the full-refresh
  remedy, since silently full-re-reading on any 4xx could mask genuine
  configuration errors.
- **`page_size` never becomes `$top` on the delta path.** OData `$top`
  is a *total-result* limit (§11.2.5.3): sent on the bootstrap it would
  end change tracking at `page_size` rows and silently drop the rest of
  the table. An explicit `page_size` is instead forwarded as
  `Prefer: odata.maxpagesize=<N>` — the spec's per-response sizing
  hint, which servers may honor or ignore without affecting
  completeness. Use `max_records_per_batch` (page-boundary enforced)
  for per-batch sizing.
- **Tombstone key resolution.** Deletions are recognized in both wire
  shapes — the v4.01 `@removed` control property and the v4.0
  `$deletedEntity`-context entry — and the key is taken from inline
  properties when present (Microsoft Graph style) or parsed from the
  entry's `@odata.id`/`id` entity reference (single, composite
  `K1=v1,K2=v2`, quoted-string and bare-guid forms). A tombstone whose
  primary keys can't be resolved raises rather than emitting a keyless
  no-op tombstone (which would silently lose the deletion). Emitted
  tombstones are padded with explicit NULLs for every non-key column,
  so `Nullable="false"` properties in the source schema don't fail the
  framework's absent-column check on delete rows.
- **The `auto` probe verdict is shared across processes.** Schema
  inference and the streaming read run in different forked workers; the
  definitive probe outcome is persisted in the process/file capability
  cache (15-minute TTL) so both resolve to the same answer even when a
  server (e.g. behind a mixed-version load balancer) flaps its
  `Preference-Applied` acknowledgement.
- **`@removed` with `reason: "changed"` is treated as a delete.** On an
  unfiltered entity set it shouldn't occur; with a server-side `filter`
  it means the row left the filtered set, and deleting downstream is
  the consistent interpretation.
- **Unsetting `delta_tracking` doesn't switch a live stream.** An
  offset that already carries a `delta_link`/`next_link` keeps taking
  the delta path (deliberately — the persisted link is the only safe
  continuation for the in-flight change sequence, and abandoning it
  mid-cycle could drop changes). To move an existing stream to
  cursor/snapshot reads, start it from a fresh checkpoint (full
  refresh).
- **The reverse holds too: a fallback-shaped offset pins the fallback.**
  Under `auto`, a non-empty offset *without* a delta link (a
  `snapshot_done` marker or a cursor watermark) proves earlier batches
  ran the cursor/snapshot shape against the schema frozen at setup —
  even when the first batch's probe failed transiently and no
  `delta_ok` verdict was ever stamped. Such a stream never re-probes
  and persists `delta_ok: false` on its next batch, so a recovered
  server can't flip it onto the delta path mid-stream (which would
  emit synthetic columns the frozen schema lacks — silently dropped by
  the framework parser, turning tombstones into live upserts). To
  adopt delta on such a stream, start a fresh checkpoint.
- **A transient probe failure on a stream frozen *with* the synthetics
  fails that trigger loudly.** If setup's probe said yes (schema
  declares `_deleted`/`_lc_sequence`) but the *first* batch's re-probe
  blips transiently, the batch degrades to a snapshot whose rows lack
  the two non-nullable synthetic columns and the framework parse fails
  the trigger. This is deliberate (padding them would fabricate MERGE
  sequencing); the stream self-heals on the next trigger once the
  probe recovers.
- **A non-advancing (or omitted) delta link raises.** If the server
  returns change records together with the *same* `@odata.deltaLink`
  as the prior batch — or with no terminal delta link at all — the
  stream would re-read that change set forever; the connector raises
  the same style of no-progress error as the cursor paths instead of
  churning.
- **`cursor_field` and `delta_tracking=enabled` are mutually
  exclusive.** Delta tracking is the source of truth for change
  ordering; layering a client-side cursor on top over-constrains the
  read.
- **The `_deleted` and `_lc_sequence` columns are synthetic.** They're
  produced by the connector, not the source, and don't exist on the
  origin entity. The destination Delta table carries them; downstream
  transforms should filter on `_deleted=False` when materialising
  active rows.
- **Don't flip `delta_tracking` off on a live stream without a full
  refresh.** A resumed offset carrying a delta link deliberately keeps
  the CDC read path even after the option changes (so the flip doesn't
  silently restart the read), but the schema API only sees options —
  it drops `_deleted`/`_lc_sequence` the moment the option is
  non-active, so tombstones from the still-CDC read would parse as
  PK-only upserts. Changing `delta_tracking` on an existing table
  needs a full refresh of that flow. The same rule applies generally:
  **changing any option that re-shapes the read** (`cursor_field` or
  its level, `filters_at`, `namespace`,
  `expand_contained`) **over an existing streaming checkpoint needs a
  full refresh** — the parked resume state (cursor watermarks,
  mid-walk checkpoints, continuation links) was written under the old
  shape, and while most mismatches degrade to duplicate-safe re-reads,
  a repositioned checkpoint can also skip rows.

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

A top-level entity set whose *own* name legally contains `__` (CSDL
identifiers allow consecutive underscores, e.g. `My__Set`) always wins over
the containment-path interpretation: the **longest declared prefix** of a
table name becomes the root segment, so `My__Set` is read flat and its
contained collections (`My__Set__Kids`) resolve under it. In the
pathological service that declares both `My__Set` *and* a `My` set with a
contained `Set` collection, the flat set shadows the contained path (and
namespace listings dedup the colliding spelling).

### Structured property values

Complex-typed, enum-typed, `Collection(...)`, and TypeDefinition-typed
CSDL properties map to `StringType` columns, and any structured
(object/array) value in an emitted row is rendered as **JSON text**
(e.g. `{"City":"Y","Zip":10001}`) rather than a Python repr — parse it
downstream with `from_json`. Scalar values pass through untouched.
`Edm.Stream` properties surface as always-NULL `StringType` columns and
are forced nullable regardless of the CSDL `Nullable` attribute —
stream values are media references the JSON payload never carries, so
honoring `Nullable="false"` would fail every row of the table.

### Schema augmentation

Each contained-collection table prepends synthetic FK columns for
**every non-leaf ancestor**. This is required for global uniqueness —
OData v4 §13.4.3 makes contained-entity keys unique only within their
immediate parent, so collapsing on the leaf's own PK collides across
sibling parent branches. Default name is ``<segment>_<pkname>``;
leading ``_`` characters are prepended (one or more, until unique) if it
would collide with a leaf property **or** with another ancestor-FK column —
including when a recursive containment path repeats the same nav-prop name
at two levels (``Nodes__Children__Children__Children`` → ``Nodes_Id``,
``Children_Id``, ``_Children_Id``): each level keeps its own distinct
column and composite-key component.
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

Concretely, a row emitted from ``Parents__Children__Notes`` looks like:

```json
{
  "Parents_Id":  42,
  "Children_Id": 7,
  "Id":          1003,
  "Text":        "Follow up next week"
}
```

`Parents_Id` and `Children_Id` are populated from each ancestor's
primary key as the connector walks the chain; downstream Delta tables
get these columns as ordinary columns and the destination MERGE keys
on the composite PK.

To omit one or more of these synthetic columns from the destination,
list them in the `exclude_ancestor_columns` table option (e.g.
`exclude_ancestor_columns=Parents_Id`). Excluded columns are dropped
from the schema, the emitted rows, and the composite primary key
together — only exclude a column when the remaining key columns still
uniquely identify each leaf row, otherwise sibling branches collide on
MERGE.

### Read modes

Two strategies via the ``expand_contained`` table option (default
``auto`` — a behavioural preflight picks per table; see
[`expand_contained`](#expand_contained) under Per-table options):

**N+1 traversal (`expand_contained=false`, and `auto`'s fallback)** —
the connector issues one ``GET Parents?$select=<pks>`` to enumerate
parent keys, then one ``GET Parents(<key>)/Children?...`` per parent.
For deep paths the fan-out multiplies. Works against every OData v4
server.

**Single nested ``$expand`` (`expand_contained=true`, and `auto` on a
verified server)** — one call:
``GET Parents?$expand=Children($expand=Notes)``. The connector flattens
the nested response into leaf rows tagged with all ancestor FKs. Under a
`cursor_field` or a `max_records_per_batch` cap the flatten runs as a
**depth-first resumable drain** that streams leaf rows and, between
batches, parks only an **O(depth)** boundary path (`pending_fetches`) —
bounded regardless of fan-out width (see [Per-table
options](#per-table-options) → `max_records_per_batch`). Some
servers cap ``$expand`` depth or silently ignore nested options —
`auto`'s preflight detects both and falls back to N+1; an explicit
`true` surfaces server errors verbatim.

### Pagination ordering

OData v4 server-driven paging (§11.2.5.7) doesn't promise a stable row
order across `@odata.nextLink` pages, so a value-based skiptoken over an
unstable sort can silently drop or duplicate rows. To make paging safe,
**every server-paged fetch the connector builds carries an explicit
`$orderby`** over a unique key:

- **PK-only** (`pk asc, …`) on fetches with no cursor filter — flat
  snapshots, contained ancestor-key walks, every leaf-collection fetch,
  and each non-cursor level of an `expand_contained=true` request (the
  top URL and every nested `$expand(...)`).
- **Cursor-first** (`cursor asc, pk asc, …`) on the single fetch whose
  entity owns the `cursor_field`. This isn't only for stability — the
  same-cursor boundary trim and watermark logic require ascending cursor
  order (see [Truncation handling](#truncation-handling)).

The cursor term is added only where the cursor is a declared property of
the fetched entity; other levels stay PK-only (ordering by an absent
column would be invalid OData). Fetches that follow an opaque server
continuation — `@odata.nextLink`, delta links, a parked
`chain_next_link` — carry no `$orderby`: the token already encodes
order/position and the server preserves the original request's
`$orderby` per §11.2.6.1. Servers that ignore `$orderby` inside
`$expand` still receive valid OData v4.

### Cursor-based incremental on contained tables

Set ``cursor_field`` to a column the leaf entity (or one of its
ancestors) declares. Each trigger walks the parent chain and filters
the relevant fetch with ``cursor gt <since>``; the global max cursor
seen is committed to the offset for next time.

Two sub-modes are picked automatically based on where the cursor
lives:

- **Leaf cursor** — `cursor_field` is a property on the leaf entity.
  The filter is applied to every leaf fetch.
- **Ancestor cursor** — `cursor_field` is on a non-leaf ancestor. The
  filter is applied at that ancestor's fetch (pruning entire
  subtrees), and the ancestor's cursor value is stamped onto every
  emitted leaf row. `get_table_schema` adds the cursor column to the
  leaf schema with the ancestor's declared type.

A `cursor_field` that's not declared anywhere along the path raises
`ValueError` at read time.

#### Truncation handling

When a per-parent walk hits `max_records_per_batch` mid-chain, the
offset parks the truncated parent's **key chain** (`parent_keys`; plus
`parent_cursor` on the ancestor-cursor path) and the resume
re-positions by the enumeration's own ordering keys — **churn-stable**:
a parent deleted below the park can't shift the resume onto the wrong
parent, and a parent inserted below a parked continuation link can't
receive it (a vanished parked parent resumes at the next chain with
the original `since`; incomparable orderings degrade to duplicate-safe
re-reads, never skips). A legacy `parent_idx` rides along purely for
downgrade compatibility. A `running_max` accumulator (both leaf- and
ancestor-cursor paths) carries the cycle's max cursor so a resume that
completes empty still commits the truncated batches' progress. Within
the truncated parent, the connector tries the cheapest resume that
won't lose rows:

1. **`@odata.nextLink` resume (preferred)** — if the server returned a
   nextLink on the truncating page, it's parked in the offset as
   `chain_next_link`. The resumed call hands the link back to the
   server, which picks up exactly where it stopped (no
   `$filter`/`$orderby`/`$select` reconstruction). Subsequent parents
   in the resume re-use the original `since`.
2. **Leaf-cursor trim (no nextLink)** — when the truncating page is the
   parent's last (the server returned its whole leaf collection, so the
   cohort is complete), the connector drops the trailing same-cursor
   cohort within that one chain's emit and parks `truncated_chain_cursor`.
   The resumed call rebuilds the URL with
   `cursor gt truncated_chain_cursor` for that parent.
3. **Complete parent, single cursor value** — if that same complete
   parent's entire emitted cohort shares one cursor value, there's no
   splittable boundary to trim. Since the cohort is complete, the
   connector **emits it in full and continues to the next parent**
   (the cap is overshot for that one parent, bounded by a single server
   response) rather than failing. The watermark advances exactly as it
   would on natural completion — `cursor gt <value>` next batch is safe
   because every cursor=`<value>` row under that parent has been read.
   (Earlier versions raised `RuntimeError` here.)
4. **Ancestor-cursor fallback** — none. Every leaf under a chain
   shares the chain's stamped cursor by construction, so a
   within-chain `cursor gt` rebuild can't split it. Ancestor mode
   relies entirely on `chain_next_link`; if your server doesn't
   return durable nextLinks, raise `max_records_per_batch` above the
   largest per-chain leaf count.

After the resumed walk completes naturally, the offset collapses back
to `{"cursor": <max>}`. Cross-batch repeats from any of these
fallbacks are deduplicated by `apply_changes` on the composite
primary key.

#### Watermark behavior on ancestor-cursor mode

Ancestor cursors interleave across top-level parents (sibling chains
under one parent are cursor-ordered, but Parent 2's lowest cursor can
be below Parent 1's highest). To avoid skipping lower-cursor chains
under later parents during resume, the connector:

- **Preserves the original `since`** in the offset on truncation —
  not the global max emitted — so the resumed walk re-enumerates
  every chain that the initial batch saw.
- **Accumulates `running_max`** across resume batches so a resume
  that started from `since=None` doesn't drop the cursor on natural
  completion and re-walk the table on the next trigger. (The
  leaf-cursor path accumulates the same key for the
  empty-resume-completion case.)

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

### Filtering individual segments

When a contained path's intermediate segments carry filterable
properties (status flags, region codes, soft-delete columns), use
`filters_at` to prune the walk before the leaf fetch — this
cascades the savings down to every child.

```python
# Read only Notes under archived=false Children of EMEA Parents.
{
    "table": {
        "source_table": "Parents__Children__Notes",
        "table_configuration": {
            "filters_at": (
                '{"Parents": "Region eq \'EMEA\'", '
                '"Children": "Archived eq false"}'
            ),
            "filter": "Pinned eq true",
        },
    }
}
```

In N+1 mode this turns three coarse walks into three filtered walks
(the connector only enumerates Parents matching `Region eq 'EMEA'`,
only fetches Children where `Archived eq false`, etc.). In
`expand_contained=true` mode each `filters_at` entry becomes a
filter inside the corresponding `$expand(...)` clause per OData v4
§5.1.1.6.

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

- Top-level / flat tables stay single-partition because `@odata.nextLink`
  skiptokens are opaque and can't be safely split — throughput on a flat
  table is bounded by the source. See the `num_partitions` section under
  [Per-table options](#per-table-options) for the contained-table
  parallel-read envelope.
- Delete tombstones are synthesized only when `delta_tracking` is active.
  The connector never uses the framework's `cdc_with_deletes` +
  `read_table_deletes` split in any mode — delta mode emits in-band
  `_deleted` tombstones instead, and snapshot/cursor modes surface no
  deletions at all. OData services without delta-query support don't
  expose deletions uniformly.
- Cursor field (when used) is assumed to be monotonically non-decreasing
  and naturally orderable by `$orderby`. Timestamps and monotonic IDs
  work; arbitrary fields don't. In streaming mode, null-cursor handling
  follows the `cursor_nulls` option (default `coalesce`): null-cursor
  rows are ingested with a synthetic-floor watermark so the offset still
  advances — the connector does **not** raise by default. Set
  `cursor_nulls=error` to instead raise a no-progress `RuntimeError` on a
  batch whose rows all have a null cursor, or `cursor_nulls=ignore` to
  drop null-cursor rows entirely. Ancestor-cursor / `expand_contained=true`
  paths and cursor types with no synthesisable floor (boolean/binary)
  always treat nulls as `error`. Batch reads / `spark.read.format(...)`
  discard the offset regardless. You can also add a server-side `filter`
  to exclude null-cursor rows when the source allows them.
- The `expand_contained=true` per-HTTP-fetch cap (see the
  `max_records_per_batch` section under [Per-table options](#per-table-options))
  cannot interrupt mid-response because the in-flight HTTP response body
  can't be serialized across batches. For tight caps, lower `page_size`
  (shrinks the per-level `$top` cross-product and therefore each
  response's row count) or switch to `expand_contained=false` which
  commits at every parent-walk boundary.
- Batch reads (`LakeflowBatchReader`, used by
  `spark.read.format("lakeflow_connect")`) call `read_table` with
  `start_offset=None` and discard the returned end-offset. Since there
  is no offset to resume from, the connector **reads the whole table in
  one call** rather than across capped batches: `max_records_per_batch`
  is disabled in batch mode (a `WARNING` notes when an explicitly-set
  value is being ignored) so the chain drains fully. To keep memory
  bounded while it does, the cursor read paths **stream lazily** in
  batch mode (flat, contained N+1, and `expand_contained=true`): leaf
  rows are yielded a page (or one flattened `$expand` response) at a
  time instead of being collected into one list, so peak memory is a
  single response, not the whole result set. For a per-batch cap with
  resume across batches, use a **streaming** table (the SDP default) —
  streaming triggers pass a dict offset, honour the cap, and park
  continuation state (`chain_next_link` / `pending_fetches`) across
  micro-batches.
