# Lakeflow Microsoft Purview Community Connector

This documentation describes how to configure and use the **Microsoft Purview** Lakeflow community connector to ingest governed business metadata from the [Microsoft Purview Unified Catalog](https://learn.microsoft.com/en-us/purview/unified-catalog) Data Governance REST API into Databricks Unity Catalog Delta tables.

The connector performs a **read-only** extract of governance domains (business domains), data products, and business glossary terms — the curated business-metadata layer of Purview's Unified Catalog. It is designed so that downstream jobs can join these tables to hydrate governed metadata onto Unity Catalog objects. The hydration step itself is out of scope for the connector.

> **Public preview API.** This connector targets the Purview Unified Catalog Data Governance data-plane API at `api-version=2026-03-20-preview`, which is in **public preview**. Field names, endpoint shapes, and the `api-version` string can change before general availability. See [Limitations](#limitations).

## Prerequisites

- **A Microsoft Purview account with the Unified Catalog enabled**, reachable at the shared data-plane endpoint `https://api.purview-service.microsoft.com`.
- **An Azure Entra (AAD) service principal (client credentials / m2m)**:
  - Register an application in Azure Entra ID; this issues an Application (client) ID and lets you create a client secret. This is a **one-time admin action** per tenant.
  - The service principal must be assigned a Purview Unified Catalog **data-plane role** — for read-only extraction, a reader role such as **Data Catalog Reader** at the governance-domain level (or a Data Reader equivalent). Role assignment is done in the Purview governance portal, not in Azure RBAC.
- **Network access**: The environment running the connector must be able to reach `https://api.purview-service.microsoft.com` and (for the UC-side token exchange) `https://login.microsoftonline.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the options read by the connector in `microsoft_purview.py`.

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `tenant_id` | string | yes | Azure Entra (AAD) tenant ID (GUID). Used to build the token endpoint (server-side, by the UC connection) and to stamp each row for multi-tenant disambiguation. | `72f988bf-...` |
| `endpoint` | string | no | Unified Catalog data-plane base URL. Defaults to the shared endpoint `https://api.purview-service.microsoft.com`. Override only for sovereign / non-public clouds. | `https://api.purview-service.microsoft.com` |
| `page_size` | int | no | `top` page size for the skip/top-paginated endpoints (`data_products`, `terms`). Clamped to `1`–`1000`. Defaults to `100`. | `100` |

Every emitted row carries a non-null `purview_tenant_id` column so tables from multiple Purview tenants can be disambiguated downstream.

### Authentication

Purview data-plane APIs use **Azure Entra ID (formerly Azure AD) OAuth 2.0 client-credentials (machine-to-machine)**. The connector follows the same model as the Azure DevOps `service_principal` method: the Unity Catalog COMMUNITY connection runs the token exchange and refresh **server-side** and injects a fresh bearer token into the connector at query time. The connector simply sends `Authorization: Bearer {access_token}` on every request — it never holds the `client_secret` and never runs the OAuth flow itself.

The connector accepts the token under one of two options; supply **one**:

| Option | Parameters | How it works |
|--------|-----------|--------------|
| `access_token` | `access_token` (secret) | Entra OAuth 2.0 client-credentials bearer token, minted and injected by the UC COMMUNITY connection. This is the recommended production path. |
| `token` | `token` (secret) | A personal / API token fallback, used identically (`Authorization: Bearer {token}`). Intended for ad-hoc or personal-token use. |

If neither `access_token` nor `token` is present the connector raises a `ValueError`.

Under the hood, the UC connection exchanges the registered `client_id` / `client_secret` for a short-lived token at the Entra token endpoint:

```
POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope=https://purview.azure.net/.default
```

The Entra token lifetime is approximately **24 hours** (`expires_in` ~86399s) — much longer than Collibra's ~1 hour — so large first-loads are unlikely to hit a mid-run token expiry. The token is **not** stored by the connector.

### Create a Unity Catalog Connection

Create a Unity Catalog COMMUNITY connection for this connector and provide:

- `tenant_id`: your Entra tenant ID.
- The Entra `client_id` / `client_secret` for the m2m connection (UC mints and injects `access_token` from these), **or** a `token` for ad-hoc use.

The connection can be created through the Lakeflow Community Connector UI ("Add data" flow) or via the standard Unity Catalog API / `community-connector` CLI tool.

## Supported Objects

The Microsoft Purview connector exposes a **static list** of **3 tables**:

- `business_domains` — governance domains: the top-level organizational structure of the Unified Catalog (functional units, lines of business, data domains, etc.).
- `data_products` — curated, published collections of data assets with owners, business use, terms of use, documentation, and endorsement status.
- `terms` — business glossary terms with definitions, acronyms, owners, and hierarchy.

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode, primary key, and incremental cursor for each table:

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|-------|-------------|----------------|-------------|--------------------|
| `business_domains` | Governance domain taxonomy | `snapshot` | `id` | n/a |
| `data_products` | Published data products | `cdc` | `id` | `systemData.lastModifiedAt` (ISO-8601) |
| `terms` | Business glossary terms | `cdc` | `id` | `systemData.lastModifiedAt` (ISO-8601) |

All primary keys are server-assigned UUIDs stored as strings. The two `cdc` tables share the same incremental cursor field, `systemData.lastModifiedAt` (a nested, ISO-8601 UTC date-time string — which sorts lexicographically). `business_domains` is fetched as a full snapshot each run because the domain taxonomy changes infrequently.

### How the metadata assembles across tables

- **Data products belong to a domain**: `data_products.domain` is the GUID of a `business_domains.id`. Join to group products by governance domain.
- **Terms belong to a domain and form a hierarchy**: `terms.domain` → `business_domains.id`; `terms.parentId` → another `terms.id` (parent term). `terms.isLeaf` indicates a leaf term.
- **Contacts are Entra user object IDs**: `contacts.owner[].id` / `contacts.expert[].id` / `contacts.databaseAdmin[].id` are Azure Entra (AAD) user object IDs (UUIDs), not emails. Resolve them to display names via the Microsoft Graph API (`/v1.0/users/{id}`) downstream — this is left to the hydration layer.

### Required and optional table options

All tables work with no options. Optional table-specific options narrow the extract:

| Table | Required Options | Optional Options | Notes |
|-------|------------------|------------------|-------|
| `business_domains` | None | `write_only` | `write_only=true` maps to the endpoint's `writeOnly` filter (domains the principal can write). |
| `data_products` | None | `domain_id`, `order_by` | `domain_id` scopes to a single governance domain (server-side `domainId` filter). |
| `terms` | None | `domain_id`, `parent_id`, `keyword`, `order_by` | `domain_id` / `parent_id` scope to a domain or parent term; `keyword` is a server-side search filter. |

To use any of these table options, include them in the connection's `externalOptionsAllowList` so they are passed through. The allowlist declared in `connector_spec.yaml` covers: `domain_id, parent_id, keyword, order_by, write_only, page_size`. (`max_records_per_batch` is intentionally **not** in the allowlist — it is not honored yet on the cdc tables; see the incremental-sync notes below.)

### Schema highlights

All schemas preserve the nested JSON structure from the Purview API rather than flattening it. Nested objects (`systemData`, `contacts`, `thumbnail`, `additionalProperties`) are kept as structs, and repeated objects (`contacts.owner`, `termsOfUse`, `documentation`, `domains`, `managedAttributes`, `resources`) are kept as arrays of structs.

- **`systemData`** (all tables): `{createdAt, createdBy, lastModifiedAt, lastModifiedBy, expiredAt, expiredBy, provisioningState}`. The `*At` fields are ISO-8601 UTC date-time strings; the `*By` fields are Entra user object IDs. `lastModifiedAt` is the incremental cursor for the `cdc` tables.
- **`contacts`** (`data_products`, `terms`): `{owner[], expert[], databaseAdmin[]}`, each an array of `{id, description}`. `id` is an Entra user object ID.
- **`business_domains`**: identity (`id`, `name`, `description`), hierarchy (`parentId`), lifecycle (`status`, `type`), plus `isRestricted`, `thumbnail`, `domains[]` (platform-domain mappings to physical collections), and `managedAttributes[]`.
- **`data_products`**: identity (`id`, `name`, `domain`, `type`), business context (`description`, `businessUse`, `audience[]`, `updateFrequency`), governance (`status`, `endorsed`, `sensitivityLabel`, `dataQualityScore`, `activeSubscriberCount`), `contacts`, `termsOfUse[]`, `documentation[]`, `managedAttributes[]`, and `additionalProperties.assetCount`.
- **`terms`**: identity (`id`, `name`, `description`, `domain`), hierarchy (`parentId`, `isLeaf`), lifecycle (`status`), plus `acronyms[]`, `contacts`, `resources[]`, and `managedAttributes[]`.

## Data Type Mapping

Purview JSON fields are mapped to Spark types as follows:

| Purview field type | Example fields | Connector Spark type | Notes |
|--------------------|----------------|----------------------|-------|
| UUID (string) | `id`, `domain`, `parentId`, contact `id`, `systemData.*By` | `StringType` | Never cast UUIDs to binary/long. |
| ISO-8601 date-time (string) | `systemData.createdAt`, `systemData.lastModifiedAt`, `systemData.expiredAt` | `StringType` | UTC ISO-8601; lexicographically ordered, which is what the incremental cursor relies on. |
| string / enum | `name`, `description`, `status`, `type`, `updateFrequency` | `StringType` | |
| boolean | `endorsed`, `isRestricted`, `isLeaf`, `managedAttributes[].isRequired` | `BooleanType` | |
| int64 | `additionalProperties.assetCount`, `activeSubscriberCount` | `LongType` | `LongType` (not `IntegerType`) to avoid overflow. |
| double | `dataQualityScore` | `DoubleType` | |
| nested object | `systemData`, `contacts`, `thumbnail`, `additionalProperties` | `StructType` | Kept as a struct, not flattened, and not a map. |
| array of objects | `contacts.owner`, `termsOfUse`, `documentation`, `domains`, `managedAttributes`, `resources` | `ArrayType(StructType(...))` | |
| array of strings | `audience`, `acronyms` | `ArrayType(StringType)` | |

## How to Run

### Step 1: Reference the connector in your workspace

Use the Lakeflow Community Connector UI to copy or reference the Microsoft Purview connector source in your workspace.

### Step 2: Configure your pipeline

In your `ingest.py` (or equivalent), point at the Unity Catalog connection and list the tables to ingest:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
register(spark, "microsoft_purview")

pipeline_spec = {
    "connection_name": "my_purview_connection",
    "objects": [
        {"table": {"source_table": "business_domains"}},
        {"table": {"source_table": "data_products"}},
        {"table": {"source_table": "terms"}},
    ],
}

ingest(spark, pipeline_spec)
```

To scope a table, add `table_options` (and make sure the option is in the connection's `externalOptionsAllowList`). For example, to fetch only the data products in one governance domain:

```python
pipeline_spec = {
    "connection_name": "my_purview_connection",
    "objects": [
        {
            "table": {
                "source_table": "data_products",
                "table_options": {"domain_id": "b3b4cc6e-6a91-4d34-8af3-90e2f93d6c7a"},
            }
        },
    ],
}
```

### Step 3: Run the pipeline

The first run does a full backfill across all tables (the CDC tables fetch all historical records, since they have no checkpoint to resume from). Subsequent runs ingest only records with `systemData.lastModifiedAt` greater than each table's saved watermark. `business_domains` is re-snapshotted in full each run.

## Incremental sync behavior

Because the Purview Unified Catalog list endpoints have **no server-side modified-since filter** (they accept only `skip` / `top` / `orderBy` / `$skipToken`), the connector implements incremental sync client-side:

- The `data_products` and `terms` readers page through the full collection and apply the saved cursor as a strict `> since` filter (so the boundary record is not re-emitted on resume).
- At startup the connector records an upper-bound timestamp (`_init_ts`) and skips any record modified after it. This caps a single `Trigger.AvailableNow` microbatch so it terminates; records modified after startup are picked up on the next trigger with a fresh bound.
- When no new records are emitted, the offset is returned unchanged so the framework sees `end_offset == start_offset` and converges.

Because records are not guaranteed to arrive in cursor order (`orderBy` on the nested `systemData/lastModifiedAt` path is not yet verified against a live tenant), the connector drains the full collection per run rather than truncating by count. CDC upsert on the primary key (`id`) tolerates the re-reads. See [Limitations](#limitations).

## Why not partitioned reads?

This connector uses the standard single-driver `LakeflowConnect` path, **not** `SupportsPartitionedStream`. Partitioned streaming pays off only when the source API supports **server-side time-range queries** that executors can run in parallel. The Unified Catalog list endpoints do not offer a `since`/`until` (or `updateTime`) range filter — only `skip`/`top` offset pagination and an opaque `$skipToken`. Time-window partitioning would force every executor to full-scan the entire collection and filter client-side, multiplying total work by the number of partitions for no benefit. (The older Atlas Data Map `/search/query` endpoint *does* support a server-side `updateTime` filter and would be a candidate for partitioning, but this connector targets the newer Unified Catalog governance surface.)

## Troubleshooting

### Authentication errors (401 / 403)

**Symptoms:** `Microsoft Purview API error for {table}: 401 ...` or `403 ...`.

**Causes:**
- The injected bearer token expired or the UC connection could not mint one (bad `client_id` / `client_secret`, wrong `tenant_id`).
- The service principal is not assigned a Purview Unified Catalog data-plane reader role.
- A `403` typically means the token is valid but the principal lacks the governance-domain read permission.

**Fix:**
- Confirm the service principal is assigned a Data Catalog Reader (or equivalent) role in the Purview governance portal at the appropriate scope.
- Verify `tenant_id` and that the token is minted with scope `https://purview.azure.net/.default`.

### Empty results / no data

**Causes:**
- A `domain_id` / `parent_id` / `keyword` filter that matches nothing.
- The Unified Catalog contains no published data products or terms yet (they exist only after being authored in the portal).

**Fix:**
- Remove table option filters to confirm data exists, then narrow.
- Query `business_domains` first to discover valid `domain_id` values.

### Rate limiting (429 / 5xx)

**Symptom:** Slow runs, retry log lines, occasional `429 Too Many Requests`.

**What the connector does:** Retries `429` and `5xx` responses with exponential backoff (1, 2, 4, 8, 16 seconds) for up to 5 attempts, honoring a `Retry-After` header when present. The Unified Catalog endpoints publish per-operation rate windows (e.g. `dataProducts` list = 100 requests / 20-second window; `businessdomains` enumerate = 500 / 20s). The connector serializes requests rather than fanning out concurrently and keeps the default page size (`top=100`) well within these windows.

## Limitations

- **Read-only** — the connector never writes back to Purview.
- **Public-preview API** — targets `api-version=2026-03-20-preview`. The Unified Catalog Data Governance API is in public preview; fields and the `api-version` string may change before GA.
- **No server-side modified-since filter** — the list endpoints have no `updateTime`/`since` parameter, so incremental sync filters `systemData.lastModifiedAt` client-side and scans the full collection each run.
- **No count-based batching yet (`max_records_per_batch` not honored on cdc tables)** — because `orderBy` on the nested cursor path (`systemData/lastModifiedAt`) is unverified against a live tenant, records are treated as unordered and the full collection is drained per run (bounded by the init-time cap). Once ordered reads are confirmed live, a soft, tie-group-aware count cap can be enabled. See the connector docstring.
- **No delete feed** — these list endpoints do not expose a deletion feed. Soft-deleted entities may surface via `systemData.provisioningState = SoftDeleted` / `expiredAt`, but the connector does not emit tombstones; the `cdc` tables use `cdc` (not `cdc_with_deletes`). Use the framework's full-refresh option to drop stale rows.
- **No partitioned reads** — a single sequential driver-side reader (see [Why not partitioned reads?](#why-not-partitioned-reads)).
- **Three tables only in v1** — the older Atlas Data Map surface (entities/assets, classifications, entity-level term assignments) is **not** covered by this connector. OKRs and critical data elements in the Unified Catalog are deferred.
- **Contact IDs are unresolved Entra GUIDs** — `contacts.*[].id` are Entra user object IDs; resolving them to names/emails requires a separate Microsoft Graph call, left to the hydration layer.

### Live-only unknowns pending validation (needs-live-testing)

A few behaviors must be confirmed against a live Purview tenant (flagged in code):

- **`orderBy` on the cursor path** — whether `orderBy=systemData/lastModifiedAt asc` (or an equivalent expression) is accepted, which would let the cdc readers batch safely by count instead of draining the whole collection.
- **`nextLink` semantics** — confirm the `nextLink` URL is absolute and self-contained (embeds `api-version` + `$skipToken`/`skip`) across all three endpoints, as assumed by `next_link_paginate`.
- **Soft-delete surfacing** — whether soft-deleted products/terms appear with `provisioningState = SoftDeleted` and whether `lastModifiedAt` advances on deletion (needed before enabling `cdc_with_deletes`).
- **Rate-limit behavior** — actual `429` / `Retry-After` behavior on a small tenant during large first-loads.

## References

- [Microsoft Purview Unified Catalog overview](https://learn.microsoft.com/en-us/purview/unified-catalog)
- [Purview Unified Catalog REST API — Business Domain](https://learn.microsoft.com/en-us/rest/api/purview/purview-unified-catalog/business-domain?view=rest-purview-purview-unified-catalog-2026-03-20-preview)
- [Purview Unified Catalog REST API — Data Products / List](https://learn.microsoft.com/en-us/rest/api/purview/purview-unified-catalog/data-products/list?view=rest-purview-purview-unified-catalog-2026-03-20-preview)
- [Purview Unified Catalog REST API — Terms / List](https://learn.microsoft.com/en-us/rest/api/purview/purview-unified-catalog/terms/list?view=rest-purview-purview-unified-catalog-2026-03-20-preview)
- [Purview data-plane REST API authentication](https://learn.microsoft.com/en-us/purview/data-gov-api-rest-data-plane)
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Microsoft Purview Unified Catalog, Data Governance data-plane API (`https://api.purview-service.microsoft.com`, `api-version=2026-03-20-preview`, public preview)
- **Supported Objects**: 3 tables (business_domains, data_products, terms)
- **Authentication**: Azure Entra (AAD) OAuth 2.0 client-credentials (m2m), bearer token injected by the Unity Catalog connection; personal `token` fallback
- **Supported Ingestion Types**: cdc, snapshot
- **Delete handling**: no delete feed (snapshot staleness on CDC tables)
</content>
