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
                "page_size,max_records_per_batch,delta_tracking"
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
                    "max_records_per_batch": "100000",
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
| `filter`                |         | Extra OData `$filter` expression. |
| `page_size`             | 1000    | `$top` per HTTP request. |
| `max_records_per_batch` | 100000  | Per-call upper bound on rows returned. The connector has **no wall-clock ceiling** — `max_records_per_batch` is the only cap on a single batch. Each batch fetches `cursor gt <last>` and pulls up to this many rows, then commits the offset. Smaller values give continuous-mode pipelines lower latency per micro-batch at the cost of more round trips; larger values amortize HTTP overhead. The default of 100000 fits roughly 100 `$top=1000` pages per batch and prioritises throughput; lower it (e.g. to 5000) if you want tighter per-batch latency in a continuous pipeline. |
| `delta_tracking`        | disabled | Opt-in OData v4 delta queries. Values: `disabled` (default — no behavior change), `auto` (probe once, fall back to cursor/snapshot if the server doesn't acknowledge), `enabled` (require support; error if the server doesn't acknowledge). See [Delta tracking](#delta-tracking) below. |

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

- Single-partition pagination via `@odata.nextLink`. Skiptokens are opaque,
  so we can't safely parallelize. Throughput is bounded by the source.
- Delete tombstones are synthesized only when `delta_tracking` is active.
  In snapshot and cursor-based modes, the connector never returns
  `cdc_with_deletes`. OData services without delta-query support don't
  expose deletions uniformly.
- Cursor field (when used) is assumed to be monotonically non-decreasing
  and naturally orderable by `$orderby`. Timestamps and monotonic IDs
  work; arbitrary fields don't.
