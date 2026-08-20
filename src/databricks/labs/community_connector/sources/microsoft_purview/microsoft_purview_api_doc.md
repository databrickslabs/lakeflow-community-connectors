# Microsoft Purview API Documentation

> ⚠️ **CORRECTION (2026-08-19) — supersedes stale claims below.** An earlier revision of this
> doc (2026-08-14) stated the Unified Catalog governance layer had **no published REST API**.
> That is **no longer true** (and was already stale by weeks): Microsoft shipped the **Unified
> Catalog / Data Governance REST API** — public preview, api-version `2026-03-20-preview` —
> covering business domains, data products, and terms. Base host
> `https://api.purview-service.microsoft.com`, paths `/datagovernance/catalog/...`, auth via
> Entra OAuth2 (scope `https://purview.azure.net/.default`). **This connector targets that
> Unified Catalog API** (`business_domains`, `data_products`, `terms`). The Atlas Data Map API
> described below still exists and remains a valid *alternative* surface (and the one to use if
> a tenant's metadata lives in the classic Data Map), but the sections below that say "no REST
> API for Unified Catalog" are OUTDATED — read them as "Data Map is one option; Unified Catalog
> now has its own API too." Verify specifics against the live tenant during Phase 2.
>
> **Scope**: READ-ONLY extraction of governed metadata from a Microsoft Purview account. Target
> use case: extract technical and business metadata — entities/assets, classifications, glossary
> terms, owners/experts, and governance-domain structure — so they can be hydrated onto Databricks
> Unity Catalog objects. This mirrors the Collibra connector in intent; only the API surface differs.
>
> **Platform clarification (as of 2025–2026)**: Microsoft Purview is the current branding for the
> unified platform that absorbed "Azure Purview" (rebranded 2022). The underlying metadata store
> is still Apache Atlas–based. Two API surfaces co-exist — see "API Landscape" section below.

---

## Platform History and API Landscape

### Brief History

- **Azure Purview** (2021–2022): original branding; Apache Atlas 2.x REST API.
- **Microsoft Purview** (2022–present): renamed + new governance features added. The Atlas/Data Map
  REST API is unchanged and still the primary programmatic interface for reading assets, glossary,
  classifications, contacts (owners/experts), and search.
- **Unified Catalog** (2024–present): new SaaS-layer UI and concept model (governance domains, data
  products, OKRs, business terms with policies). It renders over the same underlying Data Map. As
  of mid-2026 there is **no published REST API** for Unified Catalog governance domains, data
  products, or OKR objects — these are UI-only in the portal (`purview.microsoft.com`). The Atlas
  Data Map API remains the only stable, documented REST surface.

### Two API Families

| API Family | Base URL pattern | Current version | Status |
|---|---|---|---|
| **Data Map (Atlas) — data plane** | `https://{account}.purview.azure.com/datamap/api/...` | `2023-09-01` (stable); `2024-03-01-preview` | **Current and recommended** |
| **Azure Resource Manager (ARM) — control plane** | `https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Purview/accounts/{account}` | `2021-12-01` | For provisioning only — not needed for reading metadata |

The Data Map data plane is the one to target for metadata extraction. All entity, glossary,
classification, and search endpoints live there.

The **old Azure Purview Atlas URL** (`https://{account}.catalog.purview.azure.com/api/atlas/v2/...`)
was the pre-2023 path; it **still routes** to the same backend but Microsoft now documents the
newer path format `{endpoint}/datamap/api/atlas/v2/...` where `{endpoint}` is
`https://{account}.purview.azure.com`. Use the newer path going forward.

### Unified Catalog — No REST API for Governance Domains or Data Products

As of 2026-08, the Unified Catalog UI objects (governance domains, data products, critical data
elements, OKRs) **do not have a published REST API endpoint**. Microsoft docs reference a
"Unified Catalog data plane" role assignment in the portal but the actual REST API for querying
these objects is not publicly documented or stable. The Atlas Data Map API (`/datamap/api/atlas/v2/`)
is the only fully-documented REST surface for automated metadata extraction.

**TBD**: Monitor Microsoft's public API changelog for a Unified Catalog REST API. As of this
writing, only the Data Map / Atlas path is viable for a connector.

---

## Authorization

### Preferred Method: Azure Entra ID (OAuth 2.0) Client Credentials (m2m)

Microsoft Purview data plane APIs use **Azure Entra ID (formerly Azure AD) OAuth 2.0** with
the client credentials grant. This is the canonical m2m auth flow for service principals.

**Token endpoint**

```
POST https://login.microsoftonline.com/{tenant_id}/oauth2/token
Content-Type: application/x-www-form-urlencoded
```

**Request body parameters**

| Parameter | Value |
|---|---|
| `grant_type` | `client_credentials` |
| `client_id` | Application (client) ID of the registered Entra app |
| `client_secret` | Client secret value |
| `resource` | `https://purview.azure.net` |

Note: use `resource` (v1 endpoint), **not** `scope`. The v1 token endpoint and `resource` parameter
are what the official Purview documentation specifies. The v2 endpoint (`/oauth2/v2.0/token`) with
`scope=https://purview.azure.net/.default` also works but is not what the official tutorial uses.

**Token request example (Python)**

```python
import requests

tenant_id = "12345678-..."
url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
payload = {
    "grant_type": "client_credentials",
    "client_id":     "00001111-aaaa-...",
    "client_secret": "your-secret",
    "resource":      "https://purview.azure.net",
}
response = requests.post(url, data=payload)
token = response.json()["access_token"]
```

**Token response**

```json
{
    "token_type":     "Bearer",
    "expires_in":     "86399",
    "ext_expires_in": "86399",
    "expires_on":     "1621038348",
    "not_before":     "1620951648",
    "resource":       "https://purview.azure.net",
    "access_token":   "eyJ..."
}
```

**Using the token**

```
Authorization: Bearer {access_token}
```

**Token lifetime**: `expires_in` is `86399` seconds (~24 hours) per the official sample —
significantly longer than the ~1 hour seen with Collibra. This reduces (but does not eliminate)
the token-expiry problem on large first-loads. Re-mint proactively before expiry.

**Credential storage model for the connector**

The connector stores `tenant_id`, `client_id`, and `client_secret`. At runtime it exchanges these
for a short-lived bearer token. The token is not stored. No user-facing OAuth flow is needed.

### Service Principal Role Assignment

The service principal must be assigned a Purview data plane role in the Purview governance portal
(not in Azure RBAC — these are Purview-internal roles):

| Role | What it unlocks |
|---|---|
| **Data Curator** | Read/write access to Catalog (Atlas) data plane — entities, glossary, classifications, contacts |
| **Data Reader** | Read-only access to Catalog data plane |
| **Data Source Administrator** | Scanning data plane |

For read-only metadata extraction assign **Data Reader** at the root collection. This is the
minimal permission needed for all `GET` endpoints documented below.

For Unified Catalog objects (governance domains, data products, terms) assign **Data Catalog Reader**
at the governance domain level if/when a REST API for those objects becomes available.

Role assignment path: Purview governance portal → Data Map → Collections → root collection →
Role assignments → add service principal.

---

## Base URL

```
https://{purview_account_name}.purview.azure.com
```

Where `{purview_account_name}` is the name of the Purview account resource (not the full ARM ID,
just the resource name, e.g. `contoso-purview`).

All Data Map API paths below are relative to this base endpoint:

```
{endpoint}/datamap/api/atlas/v2/...    # Atlas entity/glossary/type endpoints
{endpoint}/datamap/api/search/query    # Search/discovery endpoint
```

---

## Object List

The connector targets the following objects. All are available via the Data Map data plane.

| Connector Table | API Object | Path | Notes |
|---|---|---|---|
| `entities` | Atlas Entity | `/datamap/api/atlas/v2/entity/guid/{guid}` (single) or search-then-bulk-get | Core catalog asset. Represents tables, schemas, databases, files, BI reports, etc. |
| `entity_classifications` | Classification instances on entities | `/datamap/api/atlas/v2/entity/guid/{guid}/classifications` | GDPR/sensitivity labels applied to an entity |
| `glossary_terms` | Atlas Glossary Term | `/datamap/api/atlas/v2/glossary/{glossaryId}/terms` | Business vocabulary items with definitions, owners, synonyms |
| `entity_term_assignments` | Term-to-entity links | included in entity `meanings[]` field | Relationship between a term and an entity |
| `collections` | Purview Collection | ARM control plane only | Organizational containers; not in Atlas API |

The object list is **static** — these are fixed Atlas REST resources, not discovered dynamically.

**Objects not currently available via REST API** (Unified Catalog layer, UI-only as of 2026-08):

- Governance domains
- Data products
- OKRs (Objectives and Key Results)
- Critical data elements

---

## Object Schema

### 1. `entities` — Discovery + Bulk Entity Fetch (recommended pattern)

The Atlas API has no "list all entities" endpoint with cursor pagination. The recommended bulk-read
pattern combines two calls:

1. **Search/discovery** (`POST /datamap/api/search/query`) — returns entity IDs with lightweight
   metadata, supports `continuationToken` pagination.
2. **Bulk entity get** (`GET /datamap/api/atlas/v2/entity/bulk?guid=...&guid=...`) — fetches full
   entity objects including attributes, contacts (owners/experts), classifications, and term
   assignments.

#### 1a. Search / Discovery

```
POST {endpoint}/datamap/api/search/query?api-version=2023-09-01
Content-Type: application/json
Authorization: Bearer {token}
```

**Request body (first page, all tables)**

```json
{
  "keywords": null,
  "filter": {
    "objectType": "Tables"
  },
  "limit": 1000,
  "orderby": [
    { "updateTime": "ASC" }
  ]
}
```

**Supported `objectType` values**: `"Tables"`, `"Files"`, `"Reports"`, `"Dashboards"`,
`"Data pipelines"`, `"Folders"`, `"Stored procedures"`, `"Glossary terms"`.

**Supported filter fields**

| Filter key | Description |
|---|---|
| `objectType` | High-level asset category (e.g. `"Tables"`) |
| `entityType` | Atlas type name (e.g. `"databricks_table"`, `"azure_sql_table"`) |
| `assetType` | Source system group (e.g. `"Azure Databricks"`, `"Azure SQL Database"`) |
| `classification` | Sensitivity classification name |
| `term` / `termGuid` | Filter by assigned glossary term name or GUID |
| `collectionId` | Filter to a specific Purview collection |
| `updateTime` | Time-range filter: `"LAST_24H"`, `"LAST_7D"`, `"LAST_30D"`, `"LAST_365D"` or epoch ms with operator |
| `attributeName` + `operator` + `attributeValue` | Generic attribute filter |

**Response schema (QueryResult)**

```json
{
  "@search.count": 5156,
  "@search.count.approximate": false,
  "continuationToken": "<opaque token>",
  "value": [
    {
      "@search.score":    1,
      "id":              "24c16e53-1bfd-4d6c-b4ce-b1f6f6f60000",
      "qualifiedName":   "mssql://server.database.windows.net/db/dbo/table1",
      "name":            "table1",
      "description":     "Customer transactions",
      "owner":           "alice@contoso.com",
      "entityType":      "azure_sql_table",
      "objectType":      "Tables",
      "assetType":       ["Azure SQL Database"],
      "classification":  ["MICROSOFT.PERSONAL.EMAIL"],
      "label":           [],
      "term":            [{ "name": "Customer", "glossaryName": "DefaultGlossary" }],
      "contact":         [{ "id": "user-guid", "info": "", "contactType": "Owner" }],
      "createTime":      1620000000000,
      "updateTime":      1720000000000
    }
  ]
}
```

**Key search-result fields**

| Field | Type | Description |
|---|---|---|
| `id` | string (GUID) | Entity GUID — use as primary key and for bulk-fetch |
| `qualifiedName` | string | Fully-qualified name (source-system path) — stable unique key per source type |
| `name` | string | Human-readable name |
| `description` | string | Short description (may be null) |
| `owner` | string | Legacy string owner field (simple email/name); may be null |
| `contact` | ContactSearchResultValue[] | Structured contacts — includes `contactType` (`"Owner"` or `"Expert"`), `id` (GUID), `info` |
| `entityType` | string | Atlas type name |
| `objectType` | string | Top-level category |
| `classification` | string[] | Applied classification names |
| `term` | TermSearchResultValue[] | Applied glossary terms with name + glossary |
| `createTime` | int64 | Epoch ms |
| `updateTime` | int64 | Epoch ms — use as incremental watermark |

#### 1b. Single Entity (full detail)

```
GET {endpoint}/datamap/api/atlas/v2/entity/guid/{guid}?api-version=2023-09-01
```

**Response schema (AtlasEntityWithExtInfo)**

```json
{
  "entity": {
    "typeName":   "azure_sql_table",
    "guid":       "5cf8a9e5-c9fd-abe0-2e8c-d40024263dcb",
    "status":     "ACTIVE",
    "createdBy":  "scanner-sp",
    "updatedBy":  "alice@contoso.com",
    "createTime": 1553072455110,
    "updateTime": 1720000000000,
    "version":    0,
    "attributes": {
      "qualifiedName": "mssql://server.db.windows.net/mydb/dbo/orders",
      "name":          "orders",
      "description":   "Daily order transactions",
      "owner":         null,
      "userDescription": "Manually curated description"
    },
    "businessAttributes": {
      "MyBizMetadata": { "DataDomain": "Sales", "Sensitivity": "Internal" }
    },
    "classifications": [
      {
        "typeName":     "MICROSOFT.PERSONAL.EMAIL",
        "entityGuid":   "5cf8a9e5-...",
        "entityStatus": "ACTIVE",
        "lastModifiedTS": "3"
      }
    ],
    "meanings": [
      {
        "termGuid":    "54688d39-b298-4104-9e80-f2a16f44aaea",
        "displayText": "Customer Order",
        "status":      "VALIDATED",
        "confidence":  100
      }
    ],
    "contacts": {
      "Owner": [
        { "id": "30435ff9-9b96-44af-a5a9-e05c8b1ae2df", "info": "Primary data owner" }
      ],
      "Expert": [
        { "id": "89abc123-...", "info": "" }
      ]
    },
    "labels":           ["finance", "gdpr-relevant"],
    "collectionId":     "abcdef",
    "isIncomplete":     false,
    "relationshipAttributes": {
      "schema":              [],
      "inputToProcesses":    [],
      "outputFromProcesses": [],
      "meanings":            []
    }
  },
  "referredEntities": {}
}
```

**AtlasEntity key fields**

| Field | Type | Description |
|---|---|---|
| `guid` | string | Primary key (UUID). Globally unique per Purview account. |
| `typeName` | string | Atlas type name (e.g. `azure_sql_table`, `databricks_table`, `hive_table`, `column`) |
| `status` | enum | `ACTIVE` or `DELETED`. Deleted entities are soft-deleted, not removed from API. |
| `createTime` | int64 | Epoch ms |
| `updateTime` | int64 | Epoch ms — use as incremental watermark |
| `createdBy` | string | User/SP that created this entity |
| `updatedBy` | string | User/SP that last updated |
| `attributes` | object | Map of type-specific attributes: `qualifiedName`, `name`, `description`, `owner`, `userDescription`, etc. |
| `businessAttributes` | object | Map of custom business metadata templates → key-value pairs |
| `classifications` | AtlasClassification[] | Applied sensitivity/compliance classifications |
| `meanings` | AtlasTermAssignmentHeader[] | Glossary terms assigned to this entity |
| `contacts` | object | `{"Owner": [{id, info}], "Expert": [{id, info}]}` — Entra user GUIDs |
| `labels` | string[] | Free-form string tags |
| `collectionId` | string | Purview collection this entity belongs to |

#### 1c. Bulk Entity Fetch

```
GET {endpoint}/datamap/api/atlas/v2/entity/bulk?guid=<g1>&guid=<g2>&...&api-version=2023-09-01
```

Pass multiple `guid=` query parameters. Returns `AtlasEntitiesWithExtInfo` with an `entities[]`
array. Practical limit: batch in groups of 50–100 GUIDs per request to avoid URL length limits
(no documented max, but conservative batching is safer).

---

### 2. `entity_classifications` — `GET /entity/guid/{guid}/classifications`

```
GET {endpoint}/datamap/api/atlas/v2/entity/guid/{guid}/classifications?api-version=2023-09-01
```

Returns all classification instances applied to a single entity. For bulk extraction use the
entity's embedded `classifications` array (included in the full entity response above).

**AtlasClassification schema**

| Field | Type | Description |
|---|---|---|
| `typeName` | string | Classification type name (e.g. `MICROSOFT.PERSONAL.EMAIL`, `MICROSOFT.GOVERNMENT.US.SSN_NUMBER`) |
| `entityGuid` | string | GUID of the classified entity |
| `entityStatus` | enum | `ACTIVE` or `DELETED` |
| `lastModifiedTS` | string | ETag/version counter — not epoch ms |
| `removePropagationsOnEntityDelete` | boolean | Whether classifications propagated to children are removed on delete |
| `validityPeriods` | TimeBoundary[] | Optional start/end validity window |

**System classification namespace prefixes**

- `MICROSOFT.PERSONAL.*` — PII (email, name, phone, SSN, passport, etc.)
- `MICROSOFT.GOVERNMENT.*` — government IDs, location names
- `MICROSOFT.SECURITY.*` — security-related
- Custom classifications: no prefix (user-defined names)

---

### 3. `glossary_terms` — `GET /glossary/{glossaryId}/terms`

```
GET {endpoint}/datamap/api/atlas/v2/glossary/{glossaryId}/terms?api-version=2023-09-01&limit=1000&offset=0&sort=ASC
```

`glossaryId` is the GUID of the glossary. List all glossaries first:

```
GET {endpoint}/datamap/api/atlas/v2/glossary?api-version=2023-09-01&ignoreTermsAndCategories=true
```

This returns a JSON array of glossary objects; most environments have one called `"DefaultGlossary"`.

**AtlasGlossaryTerm schema**

| Field | Type | Description |
|---|---|---|
| `guid` | string | Primary key |
| `qualifiedName` | string | `"{termName}@{glossaryName}"` — stable unique identifier |
| `name` | string | Term name |
| `shortDescription` | string | Brief definition |
| `longDescription` | string | Full definition / rich text description |
| `abbreviation` | string | Acronym or short code |
| `status` | enum | `Draft`, `Approved`, `Alert`, `Expired` |
| `createTime` | int64 | Epoch ms |
| `updateTime` | int64 | Epoch ms — incremental watermark |
| `createdBy` | string | User GUID |
| `updatedBy` | string | User GUID |
| `contacts` | object | `{"Expert": [{id, info}], "Steward": [{id, info}]}` |
| `synonyms` | AtlasRelatedTermHeader[] | Synonym term links |
| `seeAlso` | AtlasRelatedTermHeader[] | Related term links |
| `isA` | AtlasRelatedTermHeader[] | Is-a hierarchy links |
| `antonyms` | AtlasRelatedTermHeader[] | Antonym links |
| `categories` | AtlasTermCategorizationHeader[] | Category assignments |
| `assignedEntities` | AtlasRelatedObjectId[] | Entities this term is assigned to (may be expensive for large terms — prefer entity's `meanings` field) |
| `resources` | ResourceLink[] | External links for the term |
| `anchor` | AtlasGlossaryHeader | Which glossary this term belongs to |
| `attributes` | object | Custom term template attributes |

---

## Get Object Primary Keys

| Connector Table | Primary Key | Type | Notes |
|---|---|---|---|
| `entities` | `guid` | string (UUID) | Globally unique per account; stable even after renames |
| `entity_classifications` | composite `(entityGuid, typeName)` | — | Classification has no standalone ID |
| `glossary_terms` | `guid` | string (UUID) | Globally unique |

Secondary stable keys:
- `entity.attributes.qualifiedName` — human-readable unique name per entity type; survives guid recycling if entity is deleted and recreated but not guaranteed stable across Purview migrations
- `glossaryTerm.qualifiedName` — `"{name}@{glossaryName}"` — unique within account

---

## Object Ingestion Types

| Connector Table | Ingestion Type | Incremental Cursor | Notes |
|---|---|---|---|
| `entities` | `cdc` | `updateTime` (epoch ms) | Soft-deleted entities remain with `status = DELETED`. Can detect deletes via status change. |
| `entity_classifications` | `snapshot` | — | Classifications embedded in entity; no separate cursor. Re-extract from entity `updateTime` diff. |
| `glossary_terms` | `cdc` | `updateTime` (epoch ms) | Terms can be created, updated, or deleted. No delete-feed API. |

**Delete handling**: When an entity is deleted in Purview, it is **soft-deleted** — the Atlas API
still returns it with `status = DELETED`. This is a significant advantage over Collibra: you can
detect deletes directly from the incremental scan by checking the `status` field. Entities with
`status = DELETED` should be handled as deletions in the target UC catalog.

**Soft-delete window**: TBD — Microsoft does not document how long soft-deleted entities remain
accessible via the API before they are purged from the index. Do not rely on soft-deleted entities
being available indefinitely.

---

## Read API for Data Retrieval

### Pagination Model

**Search/discovery endpoint (recommended for bulk reads)**

```
POST {endpoint}/datamap/api/search/query?api-version=2023-09-01
```

Uses **continuation token** (`continuationToken` field in request body). This is an opaque string
token returned in the response.

- First page: omit `continuationToken` from request body (or set to `null`).
- Subsequent pages: include the `"continuationToken": "<token from prior response>"` in the request body.
- Last page: `continuationToken` is absent from the response.
- Max page size (`limit`): **1000** records per request (default 50).

**Page-through example**

```python
def search_all_entities(endpoint, token, object_type="Tables", page_size=1000):
    url = f"{endpoint}/datamap/api/search/query?api-version=2023-09-01"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    continuation_token = None
    while True:
        body = {
            "keywords": None,
            "filter": {"objectType": object_type},
            "limit": page_size,
            "orderby": [{"updateTime": "ASC"}],
        }
        if continuation_token:
            body["continuationToken"] = continuation_token
        resp = requests.post(url, json=body, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("value", []):
            yield item
        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break
```

**Glossary terms endpoint — offset/limit only**

```
GET {endpoint}/datamap/api/atlas/v2/glossary/{glossaryId}/terms
    ?api-version=2023-09-01&limit=1000&offset=0&sort=ASC
```

The glossary terms endpoint uses **offset/limit** pagination only — no continuation token.
Increment `offset` by `limit` per page. Stop when the returned array length is less than `limit`.

No maximum offset is documented; deep offset pagination may degrade on very large glossaries
(>10,000 terms). If a glossary is very large, use the search endpoint with
`"filter": {"objectType": "Glossary terms"}` and continuation token instead.

---

### Incremental Read Strategy

#### Entities (incremental)

The search endpoint supports server-side `updateTime` filtering:

```json
{
  "keywords": null,
  "filter": {
    "and": [
      { "objectType": "Tables" },
      {
        "attributeName": "updateTime",
        "operator": "ge",
        "attributeValue": 1700000000000
      }
    ]
  },
  "limit": 1000,
  "orderby": [{ "updateTime": "ASC" }]
}
```

This is **server-side filtering** — unlike Collibra where filtering is client-side only.
`attributeValue` is epoch milliseconds. `operator` values: `eq`, `ne`, `gt`, `ge`, `lt`, `le`.

**Alternatively**, use the `updateTime` time-range shorthand:

```json
{
  "filter": {
    "updateTime": "LAST_7D"
  }
}
```

Supported values: `"LAST_24H"`, `"LAST_7D"`, `"LAST_30D"`, `"LAST_365D"`, `"MORE_THAN_365D"`.
These are relative to request time and cannot be used as a stable cursor; use epoch ms instead.

**Recommended watermark field**: `updateTime` (int64, epoch ms).

**Lookback**: Apply a 5–10 minute lookback to handle clock skew and in-flight writes.

**Delete detection**: Include entities with `status = DELETED` — these are soft-deleted entities
that should be removed from UC. Filter on `status` in the result set, not in the search query
(status is not a searchable filter attribute).

#### Glossary Terms (incremental)

The glossary terms endpoint does not support `updateTime` filtering. To detect updates:
- Fetch all terms via offset pagination, sorted by `updateTime ASC`.
- Apply watermark filter client-side: `term.updateTime >= watermark_epoch_ms`.
- For large glossaries (>5,000 terms), prefer the search endpoint with
  `"objectType": "Glossary terms"` and a `"updateTime"` filter.

**TBD**: Confirm whether the search endpoint returns the full `AtlasGlossaryTerm` schema or only
the lightweight `SearchResultValue` schema (which is missing `contacts`, `synonyms`, etc.).
If the search endpoint returns lightweight results, a two-pass fetch (search for IDs, then
`GET /glossary/term/{guid}` per term) is required for full detail.

---

### Rate Limits

No explicit rate limits are documented in the Microsoft Purview REST API documentation.

The Data Map billing model charges by **operations** (25 ops/sec per capacity unit by default).
Heavy API-driven reads count against the Data Map's capacity budget and can trigger autoscale.

**Practical guidance**:
- Keep search query page size at 1000 (maximum allowed).
- Batch bulk entity fetches in groups of 50–100 GUIDs.
- Serialize requests; avoid high-concurrency fan-out until rate behavior is confirmed on a specific
  account.
- Respect HTTP 429 responses with exponential back-off.
- Monitor the Purview account's Data Map Capacity Units metric in Azure portal if running large
  initial loads.

**TBD**: Confirm whether read requests against a small (1 CU) Purview account cause throttling
(HTTP 429) during large initial full-scans.

---

## Entity Model: How Objects Connect

```
Purview Collection (organizational container)
  └── Entity (many)
        ├── attributes.{qualifiedName, name, description, owner, ...}  (type-specific key-value map)
        ├── businessAttributes.{TemplateName}.{field}                   (custom business metadata)
        ├── classifications[]      classification type names applied
        ├── meanings[]             glossary term assignments
        │     └── termGuid → Glossary Term
        ├── contacts.Owner[]       Entra user GUIDs for owners
        ├── contacts.Expert[]      Entra user GUIDs for experts
        └── labels[]               free-form string tags

Glossary
  └── GlossaryTerm (many)
        ├── contacts.Steward[]     Entra user GUIDs for stewards
        ├── contacts.Expert[]
        ├── synonyms[]             → other GlossaryTerms
        └── assignedEntities[]     → Entities
```

**Join key summary**

| Join | Left key | Right key |
|---|---|---|
| entity → glossary term | `entity.meanings[].termGuid` | `glossaryTerm.guid` |
| entity → classifications | embedded in `entity.classifications[].typeName` | — |
| entity → owner (Entra user) | `entity.contacts.Owner[].id` | Entra user object ID (resolve via Microsoft Graph `/v1.0/users/{id}`) |
| entity → expert (Entra user) | `entity.contacts.Expert[].id` | Entra user object ID |
| glossary term → steward | `glossaryTerm.contacts.Steward[].id` | Entra user object ID |

**Resolving contact IDs to names/emails**

The `contacts[].id` is an Entra ID (Azure AD) user object ID (UUID). To resolve to display name
and email, call the **Microsoft Graph API** (separate from Purview):

```
GET https://graph.microsoft.com/v1.0/users/{objectId}?$select=displayName,mail
Authorization: Bearer {graph_token}   # separate scope: https://graph.microsoft.com/.default
```

This requires a separate Graph API token. The same service principal used for Purview will work
if it has the `User.Read.All` (or `Directory.Read.All`) Graph permission. This is an optional
enrichment step — the connector can store raw Entra user GUIDs and leave resolution to the
hydration layer.

---

## Field Type Mapping

| Purview/Atlas field type | API type | Connector / Spark type | Notes |
|---|---|---|---|
| `guid` fields | string (UUID format) | `StringType` | Store as string |
| `createTime`, `updateTime` | int64 (epoch ms) | `LongType` or `TimestampType` (÷1000) | UTC ms since epoch |
| `name`, `description`, `qualifiedName` | string | `StringType` | |
| `status` | enum string | `StringType` | `"ACTIVE"` or `"DELETED"` |
| `typeName` | string | `StringType` | Atlas entity type name |
| `version` | int64 | `LongType` | Monotonic version counter |
| `classifications[].typeName` | string | `StringType` in `ArrayType` | Array of classification names |
| `meanings[].termGuid` | string (UUID) | `StringType` | Term GUID |
| `meanings[].status` | enum string | `StringType` | `VALIDATED`, `DISCOVERED`, `PROPOSED`, etc. |
| `meanings[].confidence` | int32 (0–100) | `IntegerType` | Confidence of term assignment |
| `contacts.Owner[]` / `contacts.Expert[]` | array of `{id: UUID, info: string}` | `ArrayType(StructType([id: StringType, info: StringType]))` | |
| `labels[]` | string[] | `ArrayType(StringType)` | |
| `businessAttributes` | map of maps | `MapType(StringType, MapType(StringType, StringType))` | Flatten or serialize as JSON string |
| `attributes` | dynamic map | varies per `typeName` | JSON-serialize the whole map; extract `qualifiedName`, `name`, `description` as top-level columns |
| `@search.count` | int32 | `IntegerType` | May be approximate |
| `@search.count.approximate` | boolean | `BooleanType` | True when count is approximated |

---

## Known Gotchas and Implementation Notes

### 1. No Native "List All Entities" Endpoint

Atlas has no `GET /entities?limit=1000&cursor=...` endpoint equivalent to Collibra's `GET /assets`.
The correct bulk-enumeration pattern is:
1. Use `POST /search/query` with continuation token to enumerate entity IDs + lightweight metadata.
2. Use `GET /entity/bulk?guid=...&guid=...` in batches of 50–100 to fetch full entity details.

This two-pass approach is more complex than Collibra but necessary to avoid missing entities.
The search index is eventually consistent — very recently ingested entities may not appear in search
results for a few minutes after scan ingestion.

### 2. Soft-Delete Is an Advantage Over Collibra

Unlike Collibra where deleted assets disappear from the API entirely, Purview soft-deletes entities
(`status = DELETED`). This means an incremental scan that filters by `updateTime` will pick up
deletions if the entity's `updateTime` was updated when it was deleted. The connector must check
`entity.status` and emit a delete event for `DELETED` entities. This enables `cdc_with_deletes`
ingestion type — check during live testing whether `status` and `updateTime` are both updated on
soft-delete.

**TBD**: Confirm whether deleting an entity in Purview (via portal or scan) sets `status=DELETED`
AND updates `updateTime`, or whether it only updates `status` without changing `updateTime` (which
would break incremental detection).

### 3. The `attributes` Map Is Highly Type-Specific

`entity.attributes` is a freeform JSON map — its keys depend on the `typeName`. For
`azure_sql_table` the map contains `{qualifiedName, name, description, type, schema, etc.}`.
For `databricks_table` it contains different keys. The connector cannot assume a fixed schema.

Recommended approach: extract the common fields (`qualifiedName`, `name`, `description`) as
top-level columns and store the rest of `attributes` as a JSON string.

### 4. `contacts` IDs Are Entra User GUIDs, Not Emails

`entity.contacts.Owner[].id` is an Azure Entra ID object ID (UUID), not an email address.
Resolving it to a human-readable name requires a Microsoft Graph API call. The legacy `owner`
field in `entity.attributes` is a simple string (often an email or username) but is inconsistently
populated. Prioritize `contacts.Owner[]` for structured ownership, and fall back to
`attributes.owner` for a plain-text owner string.

### 5. `qualifiedName` Format Is Source-System Specific

`qualifiedName` is unique per entity within a Purview account, but its format varies by source
type. For example:
- Azure SQL table: `mssql://server.database.windows.net/db/schema/table`
- Databricks table: `databricks://{workspace_url}/{catalog}/{schema}/{table}`
- Azure Blob file: `https://account.blob.core.windows.net/container/path/file.ext`

The hydration layer in the connector must parse `qualifiedName` to extract UC-compatible names
(catalog/schema/table). For Databricks Unity Catalog sources this is straightforward; for
third-party sources (SQL Server, S3, etc.) the mapping to UC may not exist. The connector should
allow filtering by `assetType` or `entityType` to scope to UC-relevant sources.

### 6. Search Is Eventually Consistent

The search index (`/search/query`) is fed from the Atlas store asynchronously. New or updated
entities scanned by Purview may take minutes to appear in search results. For real-time lookup of
a known entity by GUID, use `GET /entity/guid/{guid}` directly (which reads from the primary store,
not the search index). For incremental pipelines with long polling intervals (hours/days) this lag
is negligible.

### 7. The Old Atlas URL Still Works But Avoid It

The legacy URL `https://{account}.catalog.purview.azure.com/api/atlas/v2/...` routes to the same
backend as the current `https://{account}.purview.azure.com/datamap/api/atlas/v2/...`. Use the
current URL to avoid depending on a deprecated routing path.

### 8. No REST API for Governance Domains or Data Products (Unified Catalog)

The Unified Catalog SaaS layer (governance domains, data products, OKRs, critical data elements,
policy objects) introduced in 2024–2025 does **not** have a published REST API as of mid-2026.
These objects exist only in the portal UI. The Atlas Data Map API remains the only automated
extraction path. If Microsoft releases a Unified Catalog REST API, the connector can be extended
to pull governance domain assignments.

### 9. `lastModifiedTS` vs `updateTime` — Two Different Fields

Atlas entities carry two time-like fields: `updateTime` (int64 epoch ms — the actual last-modified
timestamp) and `lastModifiedTS` (string, e.g. `"3"` — an ETag/version counter, **not** a
timestamp). Always use `updateTime` for incremental cursor logic. Never use `lastModifiedTS` for
time-based filtering.

### 10. Databricks–Purview Integration Direction

Microsoft Purview **scans from** Databricks/Unity Catalog (not the other way around). Purview's
Azure Databricks Unity Catalog connector connects to a Databricks SQL Warehouse, extracts
technical metadata (catalogs, schemas, tables, columns, tags), and ingests it into the Purview
Data Map. Lineage from Databricks notebook runs is also pulled into Purview.

This means: if your organization runs Purview scanning against Databricks, Purview will contain
a copy of the Databricks technical metadata. Reading this back into UC would be circular (UC →
Purview → UC). The more useful pattern is to extract the **business metadata** layered on top in
Purview (descriptions, glossary terms, classifications, ownership) and hydrate it back to UC.

The connector should therefore filter to the `"Azure Databricks"` and `"Azure Databricks Unity
Catalog"` asset types to find the Databricks-sourced entities:
```json
{
  "filter": {
    "or": [
      { "assetType": "Azure Databricks" },
      { "assetType": "Azure Databricks Unity Catalog" }
    ]
  }
}
```

### 11. Token Lifetime Is ~24 Hours (Not ~1 Hour Like Collibra)

The `expires_in` in the Purview token response is `86399` seconds (24 hours). This is much longer
than Collibra's 3600 seconds. Large first-loads that run longer than 1 hour should still succeed
without a token refresh mid-run (unlike the Collibra m2m issue documented in the live findings).
However, the connector should still handle token refresh defensively for very large accounts.

---

## Recommended Connector Tables

| Table | PK | Incremental Field | Ingestion Type | Description |
|---|---|---|---|---|
| `entities` | `guid` | `updateTime` (epoch ms) | `cdc_with_deletes` | All catalog assets (tables, views, schemas, etc.) with attributes, contacts, classifications, and term assignments |
| `glossary_terms` | `guid` | `updateTime` (epoch ms) | `cdc` | Business glossary vocabulary with definitions, owners, stewards, synonyms |

**Supplementary tables** (optional enrichment, resolve IDs to human-readable names):

| Table | Notes |
|---|---|
| `classifications_catalog` | Enumerate all classification type definitions via `GET /datamap/api/atlas/v2/types/typedefs?type=classification` — static list per account |
| `entity_types_catalog` | Enumerate all entity type definitions — useful for filtering by source system type |

**Deferred Tables** (Unified Catalog layer — no REST API as of 2026-08):

| Object | Reason Deferred |
|---|---|
| Governance Domains | No published REST API; UI-only in `purview.microsoft.com`. Monitor Microsoft changelog. |
| Data Products | Same — no REST API. Conceptually equivalent to Collibra communities/domains but no API access. |
| OKRs | No REST API. |
| Critical Data Elements | No REST API. |

---

## Open Questions for Live Testing

1. **Soft-delete behavior**: When an entity is deleted in Purview, does `updateTime` get updated
   (enabling incremental detection)? Run: delete a test asset, then query
   `GET /entity/guid/{guid}` and confirm `status = DELETED` and check `updateTime` vs. deletion time.

2. **Bulk entity fetch limit**: What is the practical max number of GUIDs in a single
   `GET /entity/bulk?guid=...` request before getting a 400 or URL-too-long error?
   Test with 50, 100, 200 to find a safe batch size.

3. **Search endpoint full entity detail**: Does the search result `value[]` include
   `contacts`, `classifications`, and `meanings` inline, or only in the entity's summary?
   If the search response is lightweight, a two-pass (search + bulk-get) is required.

4. **Incremental scan for glossary terms**: Confirm whether
   `POST /search/query` with `"objectType": "Glossary terms"` and an `updateTime` filter
   returns the full `AtlasGlossaryTerm` schema or only the `TermSearchResultValue` subset
   (which is missing `contacts`, `synonyms`, `longDescription`).

5. **Token expiry on large first-loads**: With a ~24h token lifetime, confirm that a
   large first-load (10,000+ entities) completes without a 401. If Purview uses 1-hour
   tokens in some configurations, the Collibra token-refresh workaround applies here too.

6. **Rate limiting behavior**: Test with 10 concurrent requests against a 1-CU account.
   Does the account return HTTP 429? What is the Retry-After header value?

7. **`assetType` values for Databricks UC entities**: Confirm the exact string values for
   Databricks Unity Catalog entities in a live account. Expected: `"Azure Databricks"` or
   `"Azure Databricks Unity Catalog"`. Verify by running a `GET /types/typedefs?type=entity`
   and checking for `databricks_*` type names.

8. **Contact resolution opt-in**: Confirm whether calling Graph API to resolve user GUIDs
   requires a separate Graph token (different scope) and whether the same service principal
   can be granted `User.Read.All` in Entra ID to support resolution.

9. **Pagination edge case — empty continuation token vs absent**: Confirm whether the
   absence of `continuationToken` in the response (key not present) vs. `continuationToken: null`
   both indicate last page, or only one of them does.

10. **`qualifiedName` format for Databricks UC entities**: Inspect live Purview instance
    entities of type `databricks_table` to confirm the `qualifiedName` format, so the hydration
    layer can parse it into UC `{catalog}.{schema}.{table}` format.

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://learn.microsoft.com/en-us/purview/purview | 2026-08-14 | High | Platform overview: Data Map + Unified Catalog are the two governance solutions; no deprecated APIs flagged |
| Official Docs | https://learn.microsoft.com/en-us/purview/data-governance-overview | 2026-08-14 | High | Data Map + Unified Catalog architecture; roles: Data Curator, Data Reader for API access |
| Official Docs | https://learn.microsoft.com/en-us/purview/unified-catalog | 2026-08-14 | High | Unified Catalog features: governance domains, data products, glossary terms, OKRs, health controls — all UI-only, no published REST API documented |
| Official Docs | https://learn.microsoft.com/en-us/purview/data-gov-api-rest-data-plane | 2026-08-14 | High | Auth model: Entra client credentials, resource=https://purview.azure.net; token lifetime 86399s; Data Map roles vs Unified Catalog roles; service principal creation steps |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/entity | 2026-08-14 | High | Entity operation group, API version 2023-09-01, all operations listed |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/entity/get | 2026-08-14 | High | GET /datamap/api/atlas/v2/entity/guid/{guid} full response schema including contacts (Owner/Expert), classifications, meanings (term assignments), businessAttributes, labels |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/entity/list-by-guids | 2026-08-14 | High | GET /datamap/api/atlas/v2/entity/bulk bulk fetch, multi-guid query params |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/glossary | 2026-08-14 | High | Glossary operation group, all operations listed |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/glossary/list-terms | 2026-08-14 | High | GET /datamap/api/atlas/v2/glossary/{id}/terms full AtlasGlossaryTerm schema, offset/limit pagination, contacts (Expert/Steward) |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/discovery | 2026-08-14 | High | Discovery operation group: Query, AutoComplete, Suggest |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/discovery/query | 2026-08-14 | High | POST /datamap/api/search/query continuation token pagination, max 1000 per page, all filter types, updateTime filter, full SearchResultValue schema, response continuationToken absent on last page |
| Official Docs | https://learn.microsoft.com/en-us/purview/register-scan-azure-databricks | 2026-08-14 | High | Purview scans FROM Databricks Hive metastore; no incremental scan for Hive connector; what metadata is extracted (tables, views, columns, lineage) |
| Official Docs | https://learn.microsoft.com/en-us/purview/register-scan-azure-databricks-unity-catalog | 2026-08-14 | High | Azure Databricks Unity Catalog connector: incremental scan supported; metadata: metastore, catalogs, schemas, tables, views, columns, tags, lineage; auth: PAT or service principal; known limitation: deleted objects not auto-removed from Purview |
| Official Docs | https://learn.microsoft.com/en-us/purview/data-map | 2026-08-14 | High | Data Map billing model: 25 ops/sec per CU, 10 GB metadata storage per CU; autoscale; all read/write/search operations count as Data Map ops |
