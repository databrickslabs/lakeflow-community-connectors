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
  ancestor FK columns synthesized for global uniqueness. Read via
  default N+1 traversal or a single nested `$expand`.
- **Four auth methods**: bearer, basic, api_key, oauth2 (client
  credentials *and* authorization-code with refresh-token rotation).
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

For the Python SDK form of any OAuth2 variant, see
[Option B](#option-b--python-sdk) below.

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
                "filter_at_*,page_size,max_records_per_batch,cursor_nulls,"
                "delta_tracking,expand_contained,num_partitions,pagination,"
                "exclude_ancestor_columns,cursor_lookback_seconds"
            ),
        },
    },
)
```

For OAuth2, replace the `"token": "<bearer-token>"` line above with
the matching OAuth2 keys. Client credentials:

```python
"options": {
    "sourceName": "odata",
    "service_url": service_url,
    "auth_type": "oauth2",
    "oauth2_token_url": "https://login.example.com/oauth/token",
    "oauth2_client_id": "<client-id>",
    "oauth2_client_secret": "<client-secret>",
    "oauth2_scope": "read:everything",
    "externalOptionsAllowList": (
        "namespace,cursor_field,select,filter,"
        "filter_at_*,page_size,max_records_per_batch,cursor_nulls,"
        "delta_tracking,expand_contained,num_partitions,pagination,"
        "exclude_ancestor_columns,cursor_lookback_seconds"
    ),
}
```

For the authorization-code variant, add `oauth2_refresh_token` (and
optionally `oauth2_access_token`) to the block above.

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

### Auth error handling

Failures surface as Python exceptions with the offending connection
option named in the message and the server's response body echoed
verbatim (truncated to keep the trace readable). Three classes:

| Symptom | Exception | Remediation hint in the message |
|---|---|---|
| Token-endpoint 4xx during the OAuth refresh-token grant | `ValueError` | Re-check `oauth2_refresh_token` + `oauth2_client_id`; the OAuth `error` / `error_description` is echoed verbatim. |
| Source 401 *after* a successful OAuth refresh | `PermissionError` | The access token isn't the problem — check `oauth2_scope`, the principal's permissions, and any tenant/instance identifier in `service_url`. |
| Source 401/403 with no refresh path available | `PermissionError` | Per auth mode (see below). |

The third row's remediation depends on the configured auth mode:

- **`bearer`** — pre-acquired tokens have no refresh path. The error
  suggests replacing `token` with a fresh value, or upgrading to
  `auth_type=oauth2` with `oauth2_client_id` + `oauth2_client_secret`
  so the connector mints and refreshes tokens automatically.
- **`basic`** — names `username` / `password` and the principal's
  permissions at the source.
- **`api_key`** — names `api_key` (may have been rotated) and
  `api_key_header` (some services expect a non-default header).
- **`oauth2`** with no client credentials and no refresh token —
  names the missing parameter pairs that would enable auto-refresh,
  plus `oauth2_scope`.
- **No `auth_type` set** — names the four valid `auth_type` values.

### Optional connection options

| Option                        | Default | Description |
| ----------------------------- | ------- | ----------- |
| `metadata_cache_ttl_seconds`  | 60      | TTL (seconds) for the on-disk cache of the parsed `$metadata` document, shared across forked workers so the fetch + parse cost is paid once per pipeline init. Set to `0` to disable. |
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
| `page_size`             | 1000 | Maximum per-response row budget. Under the default `pagination=auto` **every** read — snapshot, cursor, and delta — defaults `page_size=1000` (→ `$top=1000`), because `auto` needs a `$top` to detect a page-limited response with no continuation link. The one exception is `pagination=nextlink`: there a **snapshot read** (no `cursor_field`, no delta) with `page_size` unset sends **no `$top` at all** — the server picks its own page size and the connector walks every page via `@odata.nextLink` (this avoids servers that reject or mishandle an explicit `$top`, e.g. a value above their per-page cap, on a full-table scan); cursor/delta reads still default to `1000` even under `nextlink`. Setting `page_size` explicitly applies to every mode and overrides the default. For flat tables the value becomes the `$top` at the single URL. For `expand_contained=true` paths it's distributed across all `$top` points (top URL + every nested `$expand(...)`) with triangular weights — top gets the largest share, each deeper level proportionally less — so the cross-product `top × inner_1 × inner_2 × …` fits in the budget. Each per-level `$top` is floored at 5 (very small pages amplify the `@odata.nextLink` chase at every level); when a deep level would drop below 5 it's pinned to 5 and the remaining budget is divided back across the upper levels, so the cross-product stays at or under `page_size`. Each level (top URL and every nested `$expand(...)`) additionally carries a stable `$orderby` for skiptoken-safe paging — see [Pagination ordering](#pagination-ordering). Examples with `page_size=1000`: depth 2 → `[100, 10]` (product 1000), depth 3 → `[34, 5, 5]` (850), depth 4 → `[8, 5, 5, 5]` (1000). For chains so deep that `5 ** N > page_size` the floor unavoidably wins (e.g. `5**5 = 3125`); raise `page_size` to restore the cap, or switch to `expand_contained=false` so the chain becomes N+1 single-segment fetches. |
| `max_records_per_batch` | 10000   | Per-call upper bound on rows returned. The connector has **no wall-clock ceiling** — `max_records_per_batch` is the only cap on a single batch. Each batch fetches `cursor gt <last>` and pulls up to this many rows, then commits the offset. Smaller values give continuous-mode pipelines lower latency per micro-batch at the cost of more round trips; larger values amortize HTTP overhead. Honoured by every read path (flat / contained N+1 / contained `expand_contained=true`). On the `expand_contained=true` path the cap is enforced **per HTTP fetch at any depth** via an iterative work queue: each inner-collection `@odata.nextLink` discovered during inline descent is appended to a queue rather than followed inline, and the cap is checked between fetches. Pending fetches are parked in the offset as `pending_fetches` (a list of `{url, level, chain, cur_val, skip}` items) so the next `read()` resumes the queue verbatim. Cap deviation per batch is bounded by one HTTP response's worth of leaf rows (≤ `page_size`), regardless of how deep the chain is or how wide any single parent's inner collection grows. In cursor mode the watermark only advances once the chain fully drains; until then the running max sits in `running_max_cursor`. The default of 10000 balances commit frequency / visibility against HTTP overhead; lower it (e.g. to 100) for tighter per-batch latency or higher (e.g. 100000+) for throughput-oriented batch backfills. |
| `cursor_nulls`          | coalesce | How a cursor read handles rows whose `cursor_field` is **null**. `coalesce` (default): substitute a deterministic synthetic floor (a `2000-01-01…` timestamp carrying a per-PK sub-second offset, or the type's minimum for date/int/string cursors) for comparison and the watermark — the emitted row keeps its real `null`, the watermark always advances, and null-cursor rows are ingested once on the seed pass (server-side `cursor gt` excludes them thereafter, so null rows inserted *after* the seed aren't re-captured). The temporal floor year is configurable as `coalesce:<YYYY>` (e.g. `coalesce:1990`) — lower it below your oldest data; default `2000`. `error`: raise a no-progress `RuntimeError` when a batch's rows all have a null cursor (the watermark can't advance) — use to catch a misconfigured cursor. `ignore`: drop null-cursor rows entirely (never emitted). Applies to **flat and contained leaf-cursor** paths; ancestor-cursor and `expand_contained=true` paths treat nulls as `error`. For cursor types with no synthesisable floor (boolean/binary/guid) the default silently falls back to `error`; setting `cursor_nulls=coalesce` explicitly on such a cursor raises. |
| `pagination`            | auto | How the connector walks a collection's pages. `auto` (default): follow the server's `@odata.nextLink` whenever it emits one (identical to `nextlink` for spec-compliant servers); if the server **never** emits a link during a walk, fall back to a keyset seek (if the `$orderby` has keys) or skip and drain until an **empty** page — so a server that silently page-limits responses *below* the `$top` you request *and* omits `@odata.nextLink` (a common non-compliant shape, e.g. one that suppresses the link whenever `$top` is sent) is still read in full, with no per-table override. `nextlink`: follow `@odata.nextLink` only — strictly spec-compliant, and the choice when you want a `$top`-free snapshot scan (some servers reject or mishandle an explicit `$top`). `keyset`: ignore `@odata.nextLink` and always seek the next page with a `(k gt <last>)` predicate on the `$orderby` key set (`cursor`+PK, or PK-only). `skip`: ignore `@odata.nextLink` and page via `$top`+`$skip` — the keyless fallback (entities with no unique sort key); O(n) offsets and fragile under concurrent writes. **Termination differs by mode.** `nextlink` (and `auto` while the server is emitting links) treats a *short* page (`len < $top`) with no continuation link as the end. `auto` (once it has fallen back, i.e. the server gave no link), `keyset`, and `skip` instead drain until an **empty** page — a short non-empty page is NOT exhaustion, because the server's per-response page size may be **smaller than the `$top` you request**. The drain costs **one trailing empty request per collection that ends on a short page**; a spec-compliant server that keeps emitting `@odata.nextLink` incurs no extra request. Use `keyset`/`skip` explicitly only to *force* the seek strategy (e.g. ignore a buggy `@odata.nextLink` entirely). `auto`/`keyset`/`skip` require a `$top`, so they force a default `page_size` when none is set — including on snapshot scans under the default `auto`. A no-progress guard protects **every** mode against infinite loops: a walk stops with a warning if a continuation returns a page identical to the one before it (the server ignored the seek/`$skip`) or if the server hands back a self-referential/cyclic `@odata.nextLink` — this covers the client-driven modes, plain `nextlink`, and the delta-tracking walk. **Applies to every page walk**, including the nested `$expand` chain: flat reads, the contained leaf-cursor cap walk (the compound `(cursor eq V and pk gt last)` seek even drains a same-cursor cohort larger than a page), the ancestor walk, parent/ancestor enumeration, partitioned discovery, the top-level `$expand` collection, **and a parent's *inline child* collection inside `expand_contained=true`**. For the last case: when a parent's inline child page comes back full (`len == inner $top`) but the server omits the `<NavProp>@odata.nextLink`, the connector synthesizes a direct-navigation continuation — `Parent(key)/Child?$top=…&$expand=<grandchildren>` plus the keyset seek (or `$skip`) — and drains the rest, with the grandchildren still expanded and the cursor `$filter`/`$orderby` re-applied. This continuation flows through the same per-fetch work queue, so `max_records_per_batch` and cross-batch `pending_fetches` resume cover it too. `expand_contained=false` (N+1) with `pagination=keyset` remains a valid alternative for these servers. |
| `delta_tracking`        | disabled | Opt-in OData v4 delta queries. Values: `disabled` (default — no behavior change), `auto` (probe once, fall back to cursor/snapshot if the server doesn't acknowledge), `enabled` (require support; error if the server doesn't acknowledge). See [Delta tracking](#delta-tracking) below. |
| `expand_contained`      | false   | For contained-collection tables (`Parent__Child__...` paths). When `true`, the connector issues a single `GET Parent?$expand=Child($expand=...)` per pipeline trigger instead of the default N+1 traversal (one parent fetch + one per-parent leaf fetch). See [Contained navigation properties](#contained-navigation-properties) below. |
| `num_partitions`        | 4       | Number of Spark partitions for parallel reads of contained-collection tables. Honored only when the table qualifies for `SupportsPartitionedStream` (contained path, `expand_contained=false`, `delta_tracking=disabled`, and any `cursor_field` lives on the top-level entity). Top-level rows are bin-packed into this many contiguous slices; each Spark task walks only its assigned subtrees. Ignored for non-partitionable tables (they fall back to single-task reads). |
| `cursor_lookback_seconds` | auto | Overlap window for incremental `expand_contained=true` cursor reads over a **continuously-changing** source. A full nested-`$expand` walk is not a consistent snapshot — it can take many seconds, during which the source keeps changing — and the connector commits the watermark as the max cursor it saw. A row inserted *during* the walk under a parent the walk already passed lands with a cursor below that final max and would be skipped forever by the next `cursor gt <max>`. With a window set, each batch reads from `cursor gt (committed − window)` instead, re-scanning the overlap so those mid-walk arrivals are captured on the next progressing batch (re-read rows are idempotent at the destination via `apply_changes` MERGE on the primary key). The committed watermark is **never** floored — only the read filter is — so the offset still advances to the true max; a quiescent trigger (no row beyond the watermark) idles instead of looping. Values: **`auto`** (default) self-sizes the window from the **max** walk duration over the last few completed walks (persisted in the offset as `lb_history`) × `cursor_lookback_factor` (default 1.5), clamped to `cursor_lookback_max_seconds` (default 3600). Using the max of recent walks — rather than the last value × a large fudge — makes the estimate robust to a single slow spike — no manual guess, and a no-op until the first walk is measured, outside the expand-cursor path, and for non-timestamp cursors; **an integer** sets a fixed window in cursor units (seconds for a timestamp cursor) — set it at or above the worst-case walk duration, and it requires `expand_contained=true` + a timestamp `cursor_field` (else raises); **`off`** (or `0`) disables the overlap (exact prior behaviour). Trade-off: a non-zero window re-reads (and re-MERGEs) the trailing overlap each batch — `auto` keeps that proportional to the actual walk time. |
| `cursor_lookback_factor` | 1.5 | `auto`-mode only: multiplier applied to the max recent walk duration when sizing the overlap window (see `cursor_lookback_seconds`). Margin for a walk slower than any recently observed. Must be > 0; values < 1 risk under-covering (dropped rows). Ignored unless `cursor_lookback_seconds=auto`. |
| `cursor_lookback_max_seconds` | 3600 | `auto`-mode only: ceiling clamp (runaway backstop) on the computed overlap window, in seconds. Must be > 0. Ignored unless `cursor_lookback_seconds=auto`. |
| `exclude_ancestor_columns` | | Comma-separated list of synthetic ancestor-FK column names to **drop from the destination** for a contained-collection table. By default every non-leaf ancestor's PK is prepended as a `<segment>_<pkname>` column (see [Contained navigation properties](#contained-navigation-properties)); list the resolved column names here (e.g. `Instances_Id,Projects_Id`) to omit them, or a lone `*` to drop **all** ancestor-FK columns at once. Excluded columns disappear from the table schema, the stamped rows, **and the composite primary key** alike — so only exclude columns not needed for destination-key uniqueness, otherwise distinct leaf rows under different ancestors can collide on MERGE. **Only synthetic ancestor-FK columns can be excluded** — naming a real leaf/own table column (or a name that matches nothing) leaves the schema untouched and logs a warning, so the option can never drop an actual source column. No effect on flat tables. |

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
connector tries the cheapest resume that won't lose rows:

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
  completion and re-walk the table on the next trigger.

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
`filter_at_<segment>` to prune the walk before the leaf fetch — this
cascades the savings down to every child.

```python
# Read only Notes under archived=false Children of EMEA Parents.
{
    "table": {
        "source_table": "Parents__Children__Notes",
        "table_configuration": {
            "filter_at_Parents": "Region eq 'EMEA'",
            "filter_at_Children": "Archived eq false",
            "filter": "Pinned eq true",  # equivalent to filter_at_Notes
        },
    }
}
```

In N+1 mode this turns three coarse walks into three filtered walks
(the connector only enumerates Parents matching `Region eq 'EMEA'`,
only fetches Children where `Archived eq false`, etc.). In
`expand_contained=true` mode each `filter_at_<segment>` becomes a
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
  table is bounded by the source. See the `num_partitions` row in
  [Per-table options](#per-table-options) for the contained-table
  parallel-read envelope.
- Delete tombstones are synthesized only when `delta_tracking` is active.
  In snapshot and cursor-based modes, the connector never returns
  `cdc_with_deletes`. OData services without delta-query support don't
  expose deletions uniformly.
- Cursor field (when used) is assumed to be monotonically non-decreasing
  and naturally orderable by `$orderby`. Timestamps and monotonic IDs
  work; arbitrary fields don't. In streaming mode, if every row in a
  batch has a null cursor the connector raises `RuntimeError` rather
  than silently committing rows whose offset can't advance (batch reads
  / `spark.read.format(...)` are tolerated — the offset is discarded
  anyway). Add a server-side `filter` to exclude null-cursor rows when
  the source allows them.
- The `expand_contained=true` per-HTTP-fetch cap (see the
  `max_records_per_batch` row in [Per-table options](#per-table-options))
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
