# Lakeflow Collibra Community Connector

This documentation describes how to configure and use the **Collibra** Lakeflow community connector to ingest governed metadata from the [Collibra Data Intelligence Platform](https://www.collibra.com/) Core REST API v2 into Databricks Unity Catalog Delta tables.

The connector performs a **read-only** extract of assets, their attributes (descriptions, certification, and other typed metadata), role assignments (owners/stewards), and the domain taxonomy. It is designed so that downstream jobs can join these tables to hydrate governed metadata onto Unity Catalog objects. The hydration step itself is out of scope for the connector.

## Prerequisites

- **Collibra Cloud instance**: A Collibra Data Intelligence Platform instance reachable at `https://{org}.collibra.com` (for example `https://databricks.collibra.com`), where `{org}` is your Collibra subdomain.
- **A registered OAuth app (client credentials / m2m)**:
  - An OAuth 2.0 client must be registered on the Collibra instance through the Collibra Console UI or the OAuth 2.0 Client Management REST API (v1). Registration issues a `client_id` and `client_secret`. This is a **one-time admin action** per Collibra environment and typically requires a Collibra system admin role.
  - The client must be granted the **`kg.view-all`** scope — this "view any knowledge graph resource" scope is sufficient for all read endpoints used by this connector (assets, attributes, responsibilities, domains).
- **Network access**: The environment running the connector must be able to reach `https://{org}.collibra.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the options read by the connector in `collibra.py`.

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `org` | string | yes (one of `org` / `base_url`) | Collibra Cloud subdomain — the portion before `.collibra.com`. The connector derives the REST base as `https://{org}.collibra.com/rest/2.0`. | `databricks` |
| `base_url` | string | yes (one of `org` / `base_url`) | Full REST base URL, as an alternative to `org`. Use for non-standard hosts. When set, `org` is optional and is derived from the host for row disambiguation. | `https://databricks.collibra.com/rest/2.0` |
| `page_size` | int | no | Records requested per API page. Clamped to the range `1`–`1000` (Collibra caps all endpoints at 1000). Defaults to `1000`. | `1000` |

You must supply **either** `org` **or** `base_url`. If neither is provided the connector raises a `ValueError`.

Every emitted row carries a non-null `collibra_org` column so tables from multiple Collibra instances can be disambiguated downstream.

### Authentication

Collibra's Core REST API v2 uses **OAuth 2.0 client-credentials (machine-to-machine)**. The connector follows the same model as the Azure DevOps `service_principal` method: the Unity Catalog COMMUNITY connection runs the token exchange and refresh **server-side** and injects a fresh bearer token into the connector at query time. The connector simply sends `Authorization: Bearer {access_token}` on every request — it never holds the `client_secret` and never runs the OAuth flow itself.

The connector accepts the token under one of two options; supply **one**:

| Option | Parameters | How it works |
|--------|-----------|--------------|
| `access_token` | `access_token` (secret) | OAuth 2.0 client-credentials bearer token, minted and injected by the UC COMMUNITY connection. This is the recommended production path. |
| `token` | `token` (secret) | A personal / API token fallback, used identically (`Authorization: Bearer {token}`). Intended for ad-hoc or personal-token use. |

If neither `access_token` nor `token` is present the connector raises a `ValueError`.

Under the hood, the UC connection exchanges the registered `client_id` / `client_secret` for a short-lived token at Collibra's token endpoint:

```
POST https://{org}.collibra.com/rest/oauth/v2/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope=kg.view-all
```

The token is short-lived (typically `expires_in: 3600`) and is **not** stored by the connector.

### Obtaining the Required Parameters

- **Collibra org / base URL**:
  1. Sign in to your Collibra instance. The subdomain in the URL `https://{org}.collibra.com` is your `org` (e.g. `databricks`).
  2. Live Swagger/API docs for your instance are available at `https://{org}.collibra.com/docs/index.html`.

- **OAuth `client_id` / `client_secret`** (one-time admin step):
  1. As a Collibra admin, register an OAuth 2.0 client via the Collibra Console UI or the OAuth 2.0 Client Management REST API (v1).
  2. Grant the client the `kg.view-all` scope.
  3. Copy the issued `client_id` and `client_secret`. Store the secret securely — you supply it to the Unity Catalog connection, which performs the token exchange server-side.

### Create a Unity Catalog Connection

Create a Unity Catalog COMMUNITY connection for this connector and provide:

- `org` (or `base_url`): your Collibra subdomain or full REST base URL.
- The OAuth `client_id` / `client_secret` for the m2m connection (UC mints and injects `access_token` from these), **or** a `token` for ad-hoc use.

The connection can be created through the Lakeflow Community Connector UI ("Add data" flow) or via the standard Unity Catalog API / `community-connector` CLI tool.

## Supported Objects

The Collibra connector exposes a **static list** of **4 tables**:

- `assets` — all governed assets (data tables, columns, business terms, reports, etc.). The core entity.
- `attributes` — typed key-value metadata on assets. **Descriptions and Certification status live here**, not on the asset object.
- `responsibilities` — role assignments (Data Owner, Data Steward, SME) on assets, domains, and communities, with inherited resolution.
- `domains` — organizational containers one level below communities (the taxonomy).

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode, primary key, and incremental cursor for each table:

| Table              | Description                                                        | Ingestion Type | Primary Key | Incremental Cursor            |
|--------------------|--------------------------------------------------------------------|----------------|-------------|-------------------------------|
| `assets`           | All governed assets (tables, columns, terms, reports, etc.)        | `cdc`          | `id`        | `lastModifiedOn` (epoch ms)   |
| `attributes`       | Typed metadata on assets — Description, Certification, custom fields | `cdc`          | `id`        | `lastModifiedOn` (epoch ms)   |
| `responsibilities` | Role assignments (Owner, Steward, SME) with inheritance            | `cdc`          | `id`        | `lastModifiedOn` (epoch ms)   |
| `domains`          | Organizational taxonomy, one level below community                 | `snapshot`     | `id`        | n/a                           |

All primary keys are server-assigned UUIDs stored as strings. The three `cdc` tables share the same incremental cursor field, `lastModifiedOn` (int64 UTC epoch milliseconds); `domains` is fetched as a full snapshot each run because the taxonomy changes infrequently.

### How the metadata assembles across tables

Collibra does not put descriptions, certification, or ownership on the asset object. Downstream joins reconstruct per-asset metadata:

- **Descriptions and certification** are `attributes`, not asset fields. An asset's description is a `StringAttribute` whose `type.name` is `"Description"`; certification is a `BooleanAttribute` whose `type.name` is `"Certification"` (or sometimes `"Certified"`). Join `attributes.asset.id → assets.id` and filter by `type.name` to project them onto assets. (Note: `assets.articulationScore` is a 0–100 completeness score, **not** a certification flag.)
- **Owners and stewards** are `responsibilities`, not asset fields. The connector requests `includeInherited=true`, so a Data Owner assigned at the domain or community level correctly appears for child assets. For inherited assignments, `responsibilities.baseResource` points at the **parent** (domain/community) where the role was directly assigned — not the asset — which is how you distinguish direct from inherited ownership. The `owner.resourceDiscriminator` field is `"User"` or `"Group"`; resolve `owner.id` against a user/group directory downstream.
- **Domain and community descriptions** are native string fields on the domain object (`domains.description`), unlike asset descriptions.

### Required and optional table options

All tables work with no options. Optional table-specific options narrow the extract:

| Table              | Required Options | Optional Options                                          | Notes |
|--------------------|------------------|-----------------------------------------------------------|-------|
| `assets`           | None             | `sort_field`, `domain_id`, `community_id` | `sort_field` defaults to `ID` — `/assets` accepts only `NAME` / `DISPLAY_NAME` / `ID` (`LAST_MODIFIED` is rejected 400), so the `lastModifiedOn` cursor is applied client-side. `domain_id` / `community_id` scope to a single domain or community. (`max_records_per_batch` is intentionally not honored here — see note below.) |
| `attributes`       | None             | `asset_id`, `type_public_ids`, `max_records_per_batch`    | `asset_id` fetches attributes for a single asset; `type_public_ids` filters by attribute type public ID (e.g. `Description`, `Certification`). |
| `responsibilities` | None             | `resource_ids`, `role_ids`, `max_records_per_batch`       | `resource_ids` scopes to specific asset/domain/community IDs; `role_ids` filters by role UUID. `includeInherited=true` is always set. |
| `domains`          | None             | `community_id`                                            | Scope to domains within a specific community. |

- **`max_records_per_batch`** (`attributes` / `responsibilities`): caps records emitted per microbatch. Defaults to `5000`; set to `0` (or negative) for no cap. It is a **soft** cap — once reached, the connector keeps draining records that share the boundary `lastModifiedOn` value (the tie group) before stopping, so a batch never splits a group sharing one cursor value (which would silently skip the remainder on the next run). Safe to truncate this way because these endpoints are server-sorted `LAST_MODIFIED ASC` and CDC tables upsert-dedup on the primary key. **Not honored on `assets`**: `/assets` is sorted by `ID` (not `LAST_MODIFIED`), so records don't arrive in cursor order and count-based truncation cannot safely advance the watermark — the `assets` path drains the full collection per run (bounded by the init-time cap). A resumable ID-keyset cursor for `assets` is a planned follow-up.
- To use any of these table options, include them in the connection's `externalOptionsAllowList` so they are passed through.

### Schema highlights

All schemas preserve the nested JSON structure from the Collibra API rather than flattening it. Nested resource references (`domain`, `type`, `status`, `community`, `asset`, `role`, `baseResource`, `owner`) are kept as structs with the shape `{id, name, resourceType, resourceDiscriminator}`.

#### `assets` table (16 fields)
- **Identity**: `id` (UUID string), `name`, `displayName`
- **Audit**: `createdBy`, `createdOn`, `lastModifiedBy`, `lastModifiedOn`, `system`
- **Quality**: `articulationScore` (0–100 completeness, not certification), `avgRating`, `ratingsCount`
- **Flags**: `excludedFromAutoHyperlinking`
- **Nested refs**: `domain`, `type`, `status`
- **Connector-derived**: `collibra_org`

#### `attributes` table (11 fields)
- **Identity**: `id` (UUID string)
- **Audit**: `createdBy`, `createdOn`, `lastModifiedBy`, `lastModifiedOn`, `system`
- **Value**: `attributeDiscriminator` (e.g. `StringAttribute`, `BooleanAttribute`, `MultiValueListAttribute`), `value` (the polymorphic value coerced to a string — see below)
- **Nested refs**: `type` (the attribute type, e.g. Description/Certification), `asset` (the owning asset)
- **Connector-derived**: `collibra_org`

#### `responsibilities` table (10 fields)
- **Identity**: `id` (UUID string)
- **Audit**: `createdBy`, `createdOn`, `lastModifiedBy`, `lastModifiedOn`, `system`
- **Assignment**: `role` (e.g. Data Owner/Steward), `baseResource` (where the role was directly assigned — asset, domain, or community), `owner` (`resourceDiscriminator` is `"User"` or `"Group"`)
- **Connector-derived**: `collibra_org`

#### `domains` table (13 fields)
- **Identity**: `id` (UUID string), `name`
- **Description**: `description` (native field, not an attribute)
- **Audit**: `createdBy`, `createdOn`, `lastModifiedBy`, `lastModifiedOn`, `system`
- **Flags**: `meta`, `excludedFromAutoHyperlinking`
- **Nested refs**: `community` (parent), `type` (domain type)
- **Connector-derived**: `collibra_org`

**The polymorphic `attributes.value` field**: An attribute's value type depends on `attributeDiscriminator`. To keep the column a plain `StringType`, the connector coerces every value to a string — scalars are stringified and list/dict containers are JSON-encoded. Downstream jobs use `attributeDiscriminator` to re-type the value (e.g. parse a `BooleanAttribute` as a boolean, or parse a `MultiValueListAttribute` from JSON).

## Data Type Mapping

Collibra JSON fields are mapped to Spark types as follows:

| Collibra field type | Example fields | Connector Spark type | Notes |
|---------------------|----------------|----------------------|-------|
| UUID (string) | `id`, `createdBy`, `lastModifiedBy`, and the `id` inside nested refs | `StringType` | Never cast UUIDs to binary/long. |
| int64 (epoch ms) | `createdOn`, `lastModifiedOn` | `LongType` | UTC milliseconds since epoch; `LongType` (not `IntegerType`) to avoid overflow. Divide by 1000 to cast to timestamp downstream. |
| string | `name`, `displayName`, `description` | `StringType` | |
| boolean | `system`, `meta`, `excludedFromAutoHyperlinking` | `BooleanType` | |
| double (0–100) | `articulationScore`, `avgRating` | `DoubleType` | |
| int32 | `ratingsCount` | `IntegerType` | |
| polymorphic attribute `value` | `attributes.value` | `StringType` | Coerced to a string; use `attributeDiscriminator` to re-type downstream. |
| nested resource reference | `domain`, `type`, `status`, `community`, `asset`, `role`, `baseResource`, `owner` | `StructType` `{id, name, resourceType, resourceDiscriminator}` | Kept as a struct, not flattened. Absent references are `null`, not empty structs. |

The connector also:

- **Prefers `resourceDiscriminator` over `resourceType`** on nested refs. `resourceType` is a deprecated enum (2024.10+); `resourceDiscriminator` is the forward-compatible string used to tell whether an `owner` is a `"User"` or `"Group"`. Both are retained.
- **Treats absent nested refs as `null`** rather than empty objects.

## How to Run

### Step 1: Reference the connector in your workspace

Use the Lakeflow Community Connector UI to copy or reference the Collibra connector source in your workspace.

### Step 2: Configure your pipeline

In your `ingest.py` (or equivalent), point at the Unity Catalog connection and list the tables to ingest:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
register(spark, "collibra")

pipeline_spec = {
    "connection_name": "my_collibra_connection",
    "objects": [
        {"table": {"source_table": "assets"}},
        {"table": {"source_table": "attributes"}},
        {"table": {"source_table": "responsibilities"}},
        {"table": {"source_table": "domains"}},
    ],
}

ingest(spark, pipeline_spec)
```

To scope a table, add `table_options` (and make sure the option is in the connection's `externalOptionsAllowList`). For example, to fetch only Description and Certification attributes:

```python
pipeline_spec = {
    "connection_name": "my_collibra_connection",
    "objects": [
        {
            "table": {
                "source_table": "attributes",
                "table_options": {"type_public_ids": "Description,Certification"},
            }
        },
    ],
}
```

### Step 3: Run the pipeline

The first run does a full backfill across all tables (the CDC tables fetch all historical records, since they have no checkpoint to resume from). Subsequent runs ingest only records with `lastModifiedOn` greater than each table's saved watermark. `domains` is re-snapshotted in full each run.

## Incremental sync behavior

Because Collibra's Core REST API has **no server-side `lastModifiedAfter` filter**, the connector implements incremental sync client-side:

- The `attributes` and `responsibilities` readers request pages sorted by `LAST_MODIFIED ASC` and apply the saved cursor as a strict `> since` filter, so the boundary record is not re-emitted on resume. `assets` is different: `/assets` rejects `LAST_MODIFIED` (400) and is sorted by `ID`, so its records don't arrive in cursor order — the same strict `> since` and `_init_ts` filters apply client-side, but the batch is not truncated by count (see the `max_records_per_batch` note above).
- At startup the connector records an upper-bound timestamp (`_init_ts`) and skips any record modified after it. This caps a single `Trigger.AvailableNow` microbatch so it terminates; records modified after startup are picked up on the next trigger with a fresh bound.
- When no new records are emitted, the offset is returned unchanged so the framework sees `end_offset == start_offset` and converges.

## Troubleshooting

### Authentication errors (401 / 403)

**Symptoms:** `Collibra API error for {table}: 401 ...` or `403 ...`.

**Causes:**
- The injected bearer token expired or the UC connection could not mint one (bad `client_id` / `client_secret`).
- The OAuth client (Collibra Integration app) is not permissioned for the resources being read. Collibra Integration apps are permissioned by app configuration, **not** by a requested OAuth scope — the token must be minted with **no `scope` parameter** (a `scope` is rejected with `invalid_scope`, live-validated 2026-07-25).
- The `org` / `base_url` does not match the instance the client was registered on.

**Fix:**
- Verify the OAuth client (Integration app) is registered on the correct instance and is granted access to the assets/domains/responsibilities being read.
- Confirm the token endpoint `https://{org}.collibra.com/rest/oauth/v2/token` responds to a client-credentials grant **with no `scope`** (UC mints the token with no scope automatically).
- Double-check `org` (or `base_url`).

### Empty results / no data

**Causes:**
- A `domain_id` / `community_id` / `resource_ids` filter that matches nothing.
- For `responsibilities`, expecting owners on an asset when the owner was assigned at the domain level. The connector sets `includeInherited=true`, so inherited owners should appear — but check `baseResource` to see where the role was actually assigned.

**Fix:**
- Remove table option filters to confirm data exists, then narrow.
- Query `domains` first to discover valid `community_id` / `domain_id` values.

### Slow first run / count queries

**Cause:** Collibra's count query (the `total` field) can be slower than the data query itself in large environments.

**What the connector does:** For `assets` it passes `countLimit=0` to skip the count query entirely, and pages until empty. No action needed.

### Rate limiting (429 / 5xx)

**Symptom:** Slow runs, retry log lines, occasional `429 Too Many Requests`.

**What the connector does:** Retries `429` and `5xx` responses with exponential backoff (1, 2, 4, 8, 16 seconds) for up to 5 attempts, honoring a `Retry-After` header when present. Collibra does not publish per-minute rate limits; the connector serializes requests rather than fanning out concurrently.

## Limitations

- **Read-only** — the connector never writes back to Collibra.
- **No server-side modified-since filter** — the Core REST API has no `lastModifiedAfter` parameter, so incremental sync sorts by `lastModifiedOn ASC` and filters client-side. This means the reader scans forward through pages rather than jumping directly to new records.
- **No delete feed** — Collibra's Core REST API does not expose deleted records. For the `cdc` tables, a deleted asset/attribute/responsibility simply stops appearing in the full scan; the connector does not emit tombstones. Snapshot tables (`domains`) reflect deletions naturally on the next full snapshot, but the `cdc` tables can carry stale rows until a full refresh. Use the framework's full-refresh option to drop them.
- **No partitioned reads** — a single sequential driver-side reader. Time-window partitioning would force each executor to full-scan and filter client-side (there is no server-side time filter), multiplying work for no benefit, so it is intentionally not implemented.
- **Four tables only in v1** — `relations` (edges between assets, e.g. business-term↔table links), `communities` (top-level org units), and `users` / `userGroups` (owner ID resolution) are documented by the API but **deferred to a future version**. Until then, resolve `owner.id` and community lineage with your own directory data or a later connector release.
- **Live-only unknowns pending validation** — a few instance-specific values must be confirmed against a live Collibra instance:
  - The exact `typePublicId` for the **Description** and **Certification** attribute types (commonly `"Description"` and `"Certification"`/`"Certified"`, but environment-specific).
  - The **role UUIDs** for Data Owner / Data Steward / SME (needed for the `role_ids` option), which are environment-specific.
  - The exact **cursor mechanism for `/assets`** — the docs suggest `/assets` may require ID-keyset pagination rather than the `nextCursor` field used by `/attributes` and `/domains`. The connector follows `nextCursor` uniformly for now.

## References

- [Collibra Developer Portal](https://developer.collibra.com/)
- [Collibra Core REST API v2 — Assets](https://developer.collibra.com/api/references/data-governance/assets)
- [Collibra Core REST API v2 — Attributes](https://developer.collibra.com/api/references/data-governance/attributes)
- [Collibra Core REST API v2 — Responsibilities](https://developer.collibra.com/api/references/data-governance/responsibilities)
- [Collibra Core REST API v2 — Domains](https://developer.collibra.com/api/references/data-governance/domains)
- [Collibra OAuth 2.0 Client Management](https://developer.collibra.com/api/references/oauth-client-management)
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Collibra Data Intelligence Platform, Core REST API v2 (`https://{org}.collibra.com/rest/2.0`)
- **Supported Objects**: 4 tables (assets, attributes, responsibilities, domains)
- **Authentication**: OAuth 2.0 client-credentials (m2m), bearer token injected by the Unity Catalog connection; personal `token` fallback
- **Supported Ingestion Types**: cdc, snapshot
- **Delete handling**: no delete feed (snapshot staleness on CDC tables)
