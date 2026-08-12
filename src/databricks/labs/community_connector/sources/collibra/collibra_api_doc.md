# Collibra Data Intelligence Platform — REST API Documentation

> **Scope**: READ-ONLY extraction of governed metadata from a Collibra Cloud instance
> (`https://{org}.collibra.com`). Target use case: extract asset descriptions, owners/stewards,
> certification status, business glossary term relations, and domain/community taxonomy so they
> can be hydrated onto Unity Catalog objects. The hydration step is out of scope here.

---

## Authorization

### Preferred Method: OAuth 2.0 Client Credentials (m2m)

Collibra's Core REST API v2 defines an OAuth 2.0 security scheme with a **client credentials
grant** (machine-to-machine, no user interaction required). This is the recommended auth method
for connectors.

**Token endpoint**

```
POST https://{org}.collibra.com/rest/oauth/v2/token
Content-Type: application/x-www-form-urlencoded
```

**Request body parameters**

| Parameter      | Value                              |
|----------------|------------------------------------|
| `grant_type`   | `client_credentials`               |
| `client_id`    | Your registered OAuth app client ID |
| `client_secret`| Your registered OAuth app secret   |
| `scope`        | Space-separated list (see below)   |

**Required scopes for read-only metadata extraction**

| Scope          | Purpose                            |
|----------------|------------------------------------|
| `kg.view-all`  | View any knowledge graph resource — sufficient for all read endpoints (assets, attributes, relations, responsibilities, domains, communities, users) |

**Token response**

```json
{
  "access_token": "eyJ...",
  "token_type":   "Bearer",
  "expires_in":   3600
}
```

**Using the token on subsequent calls**

```
Authorization: Bearer {access_token}
```

**App registration prerequisite**

OAuth clients must be registered through Collibra's OAuth 2.0 Client Management REST API
(v1) or via the Collibra Console UI before use. The registration process issues a `client_id`
and `client_secret`. This is a one-time admin step per Collibra environment.

**Credential storage model for the connector**

The connector stores `client_id` and `client_secret`. At runtime it exchanges these for a
short-lived bearer token; the token is **not** stored. No user-facing OAuth flow is needed.

---

### Fallback Method: Session-Based (JSESSIONID + CSRF)

Suitable for development/testing but **not recommended** for production m2m connectors.

```
POST https://{org}.collibra.com/rest/2.0/auth/sessions
Content-Type: application/json

{"username": "...", "password": "..."}
```

Response sets a `JSESSIONID` cookie and returns a `csrfToken`. Both must be forwarded in
subsequent requests. Creating a new session invalidates any existing session for the same user.

---

### Additional Fallback: HTTP Basic Auth

```
Authorization: Basic Base64(username:password)
```

Collibra's API reference lists `basicAuth` as a supported security scheme alongside OAuth2 and
JWT. Use only for quick ad-hoc calls; not suitable for production connectors.

---

### JWT Bearer (External IdP)

Collibra also supports JWT bearer tokens issued by an external Identity Provider (IdP) configured
in the Collibra Console. This flow requires your IdP to issue tokens with the Collibra user's
`sub` claim. It is **not** the same as the native OAuth2 client-credentials flow above. Only
relevant if your environment mandates IdP-federated auth.

---

## Base URL

```
https://{org}.collibra.com/rest/2.0
```

For the Databricks-internal Collibra instance the base is:

```
https://databricks.collibra.com/rest/2.0
```

All endpoint paths below are relative to this base URL. Instance API docs (live Swagger UI) are
available at `https://{org}.collibra.com/docs/index.html`.

---

## Object List

The following resource types are available via the Core REST API v2. The connector targets the
five primary objects listed first.

| Resource         | REST Path            | Connector Table    | Notes |
|------------------|----------------------|--------------------|-------|
| Assets           | `/assets`            | `assets`           | Core entity — data tables, columns, terms, etc. |
| Attributes       | `/attributes`        | `attributes`       | Key-value metadata on assets (Description, Certification, etc.) |
| Relations        | `/relations`         | `relations`        | Edges between assets (term↔asset, asset↔domain) |
| Responsibilities | `/responsibilities`  | `responsibilities` | Role assignments (Owner, Steward, SME) on assets/domains/communities |
| Domains          | `/domains`           | `domains`          | Organizational containers one level below communities |
| Communities      | `/communities`       | `communities`      | Top-level org units (business unit, department) |
| Users            | `/users`             | *(join table)*     | Resolve user IDs to names/emails in responsibilities |
| User Groups      | `/userGroups`        | *(join table)*     | Resolve group IDs to names in responsibilities |

The object list is **static** — these are fixed REST resources, not discovered dynamically.

---

## Object Schema

### 1. `assets` — `GET /assets`

Lists all assets with optional filtering.

**Key query parameters**

| Parameter         | Type            | Default | Notes |
|-------------------|-----------------|---------|-------|
| `cursor`          | string          | `""`    | Preferred pagination; pass empty string for first page |
| `limit`           | int32           | 0       | 0 = server default; max 1000 per call |
| `offset`          | int32           | 0       | **Deprecated** — prefer `cursor` |
| `countLimit`      | int32           | -1      | -1 = count all (slow); 0 = skip count (fast); see Gotchas |
| `domainId`        | UUID            | —       | Filter to a single domain |
| `communityId`     | UUID            | —       | Filter to a community (includes all sub-domains) |
| `typeIds`         | UUID[]          | —       | Filter by asset type UUIDs |
| `typePublicIds`   | string[]        | —       | Filter by asset type public IDs (e.g. `"Business Term"`) |
| `statusIds`       | UUID[]          | —       | Filter by status UUIDs |
| `sortField`       | enum (required) | —       | `NAME`, `DISPLAY_NAME`, or `ID` |
| `sortOrder`       | enum            | `ASC`   | `ASC` or `DESC` |
| `excludeMeta`     | boolean         | `true`  | Exclude system/meta assets |
| `typeInheritance` | boolean         | `true`  | Apply type hierarchy when filtering |

**Response schema** (`AssetPagedResponse`)

```json
{
  "total":  1500,
  "offset": 0,
  "limit":  200,
  "results": [
    {
      "id":                          "9e2d9f21-...",
      "createdBy":                   "a1b2c3d4-...",
      "createdOn":                   1700000000000,
      "lastModifiedBy":              "a1b2c3d4-...",
      "lastModifiedOn":              1710000000000,
      "system":                      false,
      "name":                        "Customer Orders",
      "displayName":                 "Customer Orders",
      "articulationScore":           72.5,
      "excludedFromAutoHyperlinking":false,
      "domain":  { "id": "d1...", "name": "Sales Domain",   "resourceType": "Domain"  },
      "type":    { "id": "t1...", "name": "Table",           "resourceType": "AssetType" },
      "status":  { "id": "s1...", "name": "Accepted",        "resourceType": "Status"  },
      "avgRating":    4.2,
      "ratingsCount": 8
    }
  ]
}
```

**Important fields**

| Field            | Description |
|------------------|-------------|
| `id`             | Primary key (UUID). Used as `assetId` in all cross-references. |
| `lastModifiedOn` | UTC epoch milliseconds. Use as incremental watermark. |
| `domain.id`      | FK to `domains.id`. |
| `type.id`        | FK to asset type. Use `typePublicIds` param to filter by type name. |
| `status.id`      | FK to status. "Accepted" / "Candidate" / "Rejected" are common. |
| `articulationScore` | 0–100 completeness score (not a certification boolean). |

**NOTE**: Asset descriptions and Certification status are **not** fields on the asset object
itself — they are stored as `attributes` (see below).

---

### 2. `attributes` — `GET /attributes`

Attributes are typed key-value metadata on assets. Description, Certification, and any custom
metadata fields all live here.

**Key query parameters**

| Parameter       | Type     | Default | Notes |
|-----------------|----------|---------|-------|
| `assetId`       | UUID     | —       | Filter to a single asset — use for per-asset lookups |
| `typeIds`       | UUID[]   | —       | Filter to specific attribute types by UUID |
| `typePublicIds` | string[] | —       | Filter to specific attribute types by public ID string |
| `cursor`        | string   | `""`    | Cursor pagination (preferred) |
| `limit`         | int32    | 0       | Max 1000 |
| `sortField`     | enum     | —       | `CREATED_BY`, `CREATED_ON`, `LAST_MODIFIED`, `ID` |
| `sortOrder`     | enum     | `DESC`  | `ASC` or `DESC` |

**Response schema** (`AttributePagedResponse`)

```json
{
  "total": 50,
  "offset": 0,
  "limit": 50,
  "nextCursor": "dXNlcjE=",
  "results": [
    {
      "id":                   "attr-uuid-...",
      "createdBy":            "user-uuid-...",
      "createdOn":            1700000000000,
      "lastModifiedBy":       "user-uuid-...",
      "lastModifiedOn":       1710000000000,
      "system":               false,
      "attributeDiscriminator": "StringAttribute",
      "value":                "Stores daily order transactions from Salesforce.",
      "type": {
        "id":   "attrtype-uuid-...",
        "name": "Description",
        "resourceType": "AttributeType"
      },
      "asset": {
        "id":   "9e2d9f21-...",
        "name": "Customer Orders",
        "resourceType": "Asset"
      }
    }
  ]
}
```

**Attribute discriminator values and value formats**

| `attributeDiscriminator` | `value` type | Notes |
|--------------------------|--------------|-------|
| `StringAttribute`        | string       | General text. `LongExpression` property in Output Module queries holds the string content. |
| `NumericAttribute`       | number or string | Numeric or stringified number |
| `BooleanAttribute`       | boolean or string | `true`/`false` or `"true"`/`"false"` |
| `DateAttribute`          | number or string | Epoch ms or ISO date string |
| `DateTimeAttribute`      | number or string | Epoch ms or ISO datetime string |
| `ScriptAttribute`        | string       | Groovy/script expression result |
| `SingleValueListAttribute` | string     | One value from a defined list |
| `MultiValueListAttribute`  | string[]   | Multiple values from a defined list |

**Identifying "Description" attributes**

Filter by `type.name == "Description"` (case-sensitive). The standard Description attribute
type has `typePublicId` = `"Description"` in most Collibra configurations (verify on live
instance — see Open Questions).

**Identifying "Certification" / "Certified" attributes**

Certification in Collibra is stored as a `BooleanAttribute` with `type.name == "Certification"`
(or sometimes `"Certified"`). The exact `typePublicId` is environment-specific and must be
confirmed on the live instance. A value of `true` means the asset is certified.

To filter attributes to only certification booleans:
```
GET /attributes?assetId={uuid}&typePublicIds=Certification
```

**Bulk attribute fetching strategy**

The `/attributes` endpoint does **not** support `assetIds` (plural). To fetch attributes for
many assets efficiently, use the **Output Module** (see Read API section), which lets you
join Asset + StringAttribute + BooleanAttribute in a single POST request.

For the Core REST API approach: fetch attributes per asset with `GET /attributes?assetId={uuid}`
or page through all attributes sorted by `LAST_MODIFIED` for incremental sync.

---

### 3. `relations` — `GET /relations`

Relations are directed edges between two resources (asset↔asset, asset↔domain, term↔asset).

**Key query parameters**

| Parameter                    | Type   | Default | Notes |
|------------------------------|--------|---------|-------|
| `sourceId`                   | UUID   | —       | Filter by source resource |
| `targetId`                   | UUID   | —       | Filter by target resource |
| `relationTypeId`             | UUID   | —       | Filter by relation type UUID |
| `typePublicId`               | string | —       | Filter by relation type public ID |
| `sourceTargetLogicalOperator`| enum   | `AND`   | `AND` or `OR` for combined source+target filter |
| `cursor`                     | string | `""`    | Cursor pagination |
| `limit`                      | int32  | 0       | Max 1000 |

**Response schema** (`RelationPagedResponse`)

```json
{
  "total": 300,
  "offset": -1,
  "limit":  200,
  "nextCursor": "abc123==",
  "results": [
    {
      "id":           "rel-uuid-...",
      "createdBy":    "user-uuid-...",
      "createdOn":    1700000000000,
      "lastModifiedBy": "user-uuid-...",
      "lastModifiedOn": 1710000000000,
      "system":       false,
      "source": {
        "id":                  "term-uuid-...",
        "name":                "Customer",
        "resourceType":        "Asset",
        "resourceDiscriminator": "Asset"
      },
      "target": {
        "id":                  "table-uuid-...",
        "name":                "customer_dim",
        "resourceType":        "Asset",
        "resourceDiscriminator": "Asset"
      },
      "type": {
        "id":                  "reltype-uuid-...",
        "resourceType":        "RelationType",
        "resourceDiscriminator": "RelationType"
      }
    }
  ]
}
```

**NOTE**: When cursor pagination is used, `total` and `offset` are returned as `-1`. The
`nextCursor` field is omitted on the last page.

**Key relation types for this connector**

| Standard relation type name                   | Meaning |
|-----------------------------------------------|---------|
| `"is related to"`                             | Generic asset↔asset link |
| `"is part of"` / `"has part"`                | Hierarchical containment |
| `"Business Term classifies Data Asset"` (or similar) | Term↔table/column link |

**TBD**: The exact `typePublicId` values for business-term-to-asset relations must be confirmed
on the live instance by listing relation types from the Knowledge Graph API.

---

### 4. `responsibilities` — `GET /responsibilities`

Responsibilities are role assignments: a `(user_or_group, role, resource)` triple. Resources
can be assets, domains, or communities.

**Key query parameters**

| Parameter            | Type     | Default | Notes |
|----------------------|----------|---------|-------|
| `resourceIds`        | UUID[]   | —       | Filter to specific asset/domain/community IDs |
| `ownerIds`           | UUID[]   | —       | Filter to specific user/group IDs |
| `roleIds`            | UUID[]   | —       | Filter to specific roles (Owner, Steward, SME, etc.) |
| `includeInherited`   | boolean  | `true`  | **Critically important** — includes responsibilities assigned at domain/community level that apply to child resources |
| `type`               | enum     | `ALL`   | `ALL`, `GLOBAL`, or `RESOURCE` |
| `excludeEmptyGroups` | boolean  | —       | Exclude group responsibilities with no members |
| `limit`              | int32    | 0       | Max 1000 |
| `offset`             | int32    | 0       | Zero-based offset (cursor not supported for this endpoint) |
| `sortField`          | enum     | `LAST_MODIFIED` | `CREATED_BY`, `CREATED_ON`, `LAST_MODIFIED`, `NAME` |
| `sortOrder`          | enum     | `DESC`  | `ASC` or `DESC` |

**Response schema** (`ResponsibilityPagedResponse`)

```json
{
  "total": 120,
  "offset": 0,
  "limit": 100,
  "results": [
    {
      "id":           "resp-uuid-...",
      "createdBy":    "user-uuid-...",
      "createdOn":    1700000000000,
      "lastModifiedBy": "user-uuid-...",
      "lastModifiedOn": 1710000000000,
      "system":       false,
      "role": {
        "id":                  "role-uuid-...",
        "name":                "Data Owner",
        "resourceType":        "Role",
        "resourceDiscriminator": "Role"
      },
      "baseResource": {
        "id":                  "domain-uuid-...",
        "resourceType":        "Domain",
        "resourceDiscriminator": "Domain"
      },
      "owner": {
        "id":                  "user-uuid-...",
        "resourceType":        "User",
        "resourceDiscriminator": "User"
      }
    }
  ]
}
```

**Understanding `baseResource` vs target asset**

A responsibility's `baseResource` is the resource the role was **directly assigned to** —
this might be a Domain or Community, not the asset itself. When `includeInherited=true`,
the API returns responsibilities assigned at parent levels (domain/community) that propagate
down to assets within them.

To get all effective owners for a specific asset (including inherited):
```
GET /responsibilities?resourceIds={assetId}&includeInherited=true
```

**Resolving the `owner` field**

The `owner` object contains an `id` and a `resourceDiscriminator` of either `"User"` or
`"Group"`. Use this to route the ID to either `GET /users/{id}` or `GET /userGroups/{id}`
for name/email resolution.

**Role name conventions**

Standard Collibra roles include: `Data Owner`, `Data Steward`, `Data Stewardship Manager`,
`Subject Matter Expert`. The exact names and their UUIDs are environment-specific. Role UUIDs
can be listed via `GET /roles` (not documented in detail here — see Open Questions).

---

### 5. `domains` — `GET /domains`

Domains are organizational containers within a community (one level below community).

**Key query parameters**

| Parameter             | Type     | Default | Notes |
|-----------------------|----------|---------|-------|
| `communityId`         | UUID     | —       | Filter to a specific community |
| `typeIds`             | UUID[]   | —       | Filter by domain type |
| `typePublicId`        | string   | —       | Filter by domain type public ID |
| `excludeMeta`         | boolean  | `true`  | Exclude system meta-domains |
| `includeSubCommunities` | boolean | `false` | Include domains in sub-communities |
| `cursor`              | string   | `""`    | Cursor pagination |
| `limit`               | int32    | 0       | Max 1000 |

**Response schema** (`DomainPagedResponse`)

```json
{
  "total": 45,
  "offset": 0,
  "limit": 100,
  "results": [
    {
      "id":           "domain-uuid-...",
      "createdBy":    "user-uuid-...",
      "createdOn":    1700000000000,
      "lastModifiedBy": "user-uuid-...",
      "lastModifiedOn": 1710000000000,
      "system":       false,
      "name":         "Sales Data Assets",
      "description":  "Domain containing all Sales-related data assets.",
      "meta":         false,
      "excludedFromAutoHyperlinking": false,
      "community": {
        "id":   "community-uuid-...",
        "name": "Sales & Marketing",
        "resourceType": "Community"
      },
      "type": {
        "id":   "domaintype-uuid-...",
        "name": "Data Asset Domain",
        "resourceType": "DomainType"
      }
    }
  ]
}
```

---

### 6. `communities` — `GET /communities`

Top-level organizational containers (business units, departments).

**Key query parameters**

| Parameter      | Type    | Default    | Notes |
|----------------|---------|------------|-------|
| `parentId`     | UUID    | —          | Filter to sub-communities of a parent |
| `excludeMeta`  | boolean | `true`     | Exclude system communities |
| `cursor`       | string  | `""`       | Cursor pagination |
| `limit`        | int32   | 0          | Max 1000 |

**Response schema** (`CommunityPagedResponse`)

```json
{
  "total": 12,
  "offset": 0,
  "limit": 100,
  "results": [
    {
      "id":           "community-uuid-...",
      "createdBy":    "user-uuid-...",
      "createdOn":    1700000000000,
      "lastModifiedBy": "user-uuid-...",
      "lastModifiedOn": 1710000000000,
      "system":       false,
      "name":         "Sales & Marketing",
      "description":  "Business unit for Sales and Marketing.",
      "meta":         false,
      "parent": {
        "id":   "parent-community-uuid-...",
        "name": "Corporate",
        "resourceType": "Community",
        "resourceDiscriminator": "Community"
      }
    }
  ]
}
```

The `parent` field is `null` for root-level communities.

---

## Get Object Primary Keys

| Connector Table    | Primary Key       | Notes |
|--------------------|-------------------|-------|
| `assets`           | `id` (UUID)       | Globally unique asset identifier |
| `attributes`       | `id` (UUID)       | Globally unique attribute instance ID |
| `relations`        | `id` (UUID)       | Globally unique relation instance ID |
| `responsibilities` | `id` (UUID)       | Globally unique responsibility instance ID |
| `domains`          | `id` (UUID)       | Globally unique domain identifier |
| `communities`      | `id` (UUID)       | Globally unique community identifier |

All primary keys are server-assigned UUIDs.

---

## Object Ingestion Types

| Connector Table    | Ingestion Type | Incremental Cursor     | Notes |
|--------------------|----------------|------------------------|-------|
| `assets`           | `cdc`          | `lastModifiedOn` (epoch ms) | Assets can be updated but Collibra REST API does not expose a delete feed. Treat removals as "disappeared from full scan". |
| `attributes`       | `cdc`          | `lastModifiedOn` (epoch ms) | Attributes can be created, updated, or deleted (when removed from an asset). Same caveat on deletes. |
| `relations`        | `cdc`          | `lastModifiedOn` (epoch ms) | Relations can be created or deleted. |
| `responsibilities` | `cdc`          | `lastModifiedOn` (epoch ms) | Role assignments change when users/roles are added or removed. |
| `domains`          | `snapshot`     | —                      | Domain taxonomy changes infrequently; full snapshot per sync is acceptable. |
| `communities`      | `snapshot`     | —                      | Same as domains. |

**Delete handling**: The Collibra Core REST API does not expose a deleted-records feed. For
`assets`, `attributes`, `relations`, and `responsibilities`, the connector must either
(a) do a periodic full snapshot to detect disappearances, or (b) accept soft-delete semantics
where deleted objects eventually fall out of the incremental window.

---

## Read API for Data Retrieval

### Pagination Model

The Core REST API v2 supports two pagination strategies:

**1. Cursor-based (preferred)**

```
GET /assets?cursor=&limit=500&sortField=ID&sortOrder=ASC
```

- Pass `cursor=""` (empty string) for the first page.
- The response does NOT include `nextCursor` at the top level for the `/assets` endpoint —
  it uses the implicit cursor derived from the last record's ID when sorted by `ID`.
- The `/attributes` and `/relations` endpoints return a `nextCursor` field explicitly in the
  response body.
- **When using cursor pagination, `total` and `offset` return `-1`.**

**TBD**: The exact cursor field name and mechanism for `/assets` differs from `/attributes` —
must be confirmed live. The `/attributes` response includes `nextCursor` explicitly; for
`/assets`, the recommended approach is ID-based keyset pagination (sort by `ID ASC`, filter
`id > last_seen_id`).

**2. Offset-based (deprecated, avoid for large datasets)**

```
GET /assets?offset=0&limit=1000&sortField=NAME&sortOrder=ASC
```

The Collibra docs flag `offset` as deprecated in favor of cursor. Deep offset pagination is
documented to degrade in performance on large datasets.

**Page size limits**: Maximum 1000 records per request across all endpoints.

---

### Incremental Read Strategy

For `assets`, `attributes`, `relations`, `responsibilities`:

```
GET /assets?sortField=LAST_MODIFIED&sortOrder=ASC&limit=1000
```

Filter results client-side where `lastModifiedOn >= watermark_epoch_ms`. The API does NOT
support a server-side `lastModifiedAfter` parameter on the Core REST endpoints (it is
available in the Output Module — see below).

**Recommended watermark field**: `lastModifiedOn` (int64, UTC milliseconds).

**Lookback**: Apply a 10-minute lookback to the watermark to handle clock skew across
distributed Collibra nodes.

---

### Full Asset + Metadata Extraction via the Output Module (Bulk Path)

The **Output Module** at `POST /rest/2.0/outputModule/export/json` provides a graph query
engine that can join Asset + Attribute + Responsibility + Domain in a single request. This is
the most efficient path for bulk initial loads.

**Endpoint**

```
POST https://{org}.collibra.com/rest/2.0/outputModule/export/json
Content-Type: application/json
Authorization: Bearer {access_token}
```

**Example: assets with descriptions and domain/community**

```json
{
  "ViewConfig": {
    "displayStart":  0,
    "displayLength": 5000,
    "maxCountLimit": 0,
    "queryTimeout":  3600,
    "Resources": {
      "Asset": {
        "Id":        { "name": "assetId" },
        "Signifier": { "name": "assetName" },
        "Domain": {
          "Id":   { "name": "domainId" },
          "Name": { "name": "domainName" },
          "Community": {
            "Id":   { "name": "communityId" },
            "Name": { "name": "communityName" }
          }
        },
        "AssetType": {
          "Name": { "name": "assetTypeName" }
        },
        "Status": {
          "Name": { "name": "statusName" }
        },
        "StringAttribute": {
          "type": "Description",
          "LongExpression": { "name": "description" }
        }
      }
    }
  }
}
```

**Example: assets with boolean Certification attribute**

```json
{
  "ViewConfig": {
    "displayStart":  0,
    "displayLength": 5000,
    "maxCountLimit": 0,
    "Resources": {
      "Asset": {
        "Id":        { "name": "assetId" },
        "Signifier": { "name": "assetName" },
        "BooleanAttribute": {
          "type": "Certification",
          "Value": { "name": "certificationValue" }
        }
      }
    }
  }
}
```

**Example: assets with responsibilities (owners/stewards)**

```json
{
  "ViewConfig": {
    "displayStart":  0,
    "displayLength": 5000,
    "maxCountLimit": 0,
    "Resources": {
      "Asset": {
        "Id":        { "name": "assetId" },
        "Signifier": { "name": "assetName" },
        "Responsibility": {
          "Role": {
            "Name": { "name": "roleName" }
          },
          "User": {
            "Id":           { "name": "userId" },
            "UserName":     { "name": "userName" },
            "EmailAddress": { "name": "userEmail" }
          }
        }
      }
    }
  }
}
```

**Keyset/cursor pagination for the Output Module**

Sort by `Id` and use a `GREATER` filter for cursor-style pagination:

```json
{
  "ViewConfig": {
    "displayLength": 10000,
    "maxCountLimit": 0,
    "Resources": {
      "Asset": {
        "Id": { "name": "assetId" },
        "Order": [
          { "Field": { "name": "assetId", "order": "ASC" } }
        ],
        "Filter": {
          "Field": {
            "name":     "assetId",
            "operator": "GREATER",
            "value":    "00000000-0000-0000-0000-000000000000"
          }
        }
      }
    }
  }
}
```

Increment the `value` to the last seen `assetId` on each subsequent request.

**Output Module node limits**

| Limit                   | Value           | Configurable |
|-------------------------|-----------------|--------------|
| `pageNodesNumberLimit`  | 1,000,000 nodes | Always on    |
| `rootNodesNumberLimit`  | 100,000 rows    | Off by default |
| `queryTimeout`          | 1 min – 24 hrs  | Per-request  |

**Performance warning — `maxCountLimit`**

Setting `maxCountLimit = -1` (the default) triggers a **separate count query** that is often
slower than the data query itself. For large environments, always set `maxCountLimit = 0` to
skip the count query. The `total` field in the response will return `-1` when count is disabled.

---

### Rate Limits

No documented rate limits or per-minute quota were found in Collibra's public developer
documentation. The Output Module documentation references **query timeouts** (1–24 hours per
request) and node limits (1M nodes/page) as the effective ceiling.

**TBD**: Confirm with Collibra support whether Cloud-hosted instances have undocumented rate
limits (requests/minute, concurrent connections, etc.).

Practical guidance:
- Keep individual page sizes at 500–1000 for Core REST endpoints; 5000–10000 for Output Module.
- Serialize requests (no high-concurrency fan-out) until rate limits are confirmed.
- Respect HTTP 429 responses and implement exponential back-off.

---

## Entity Model: How Objects Connect

```
Community (1)
  └── Domain (many)           domain.community.id → community.id
        └── Asset (many)      asset.domain.id → domain.id

Asset (1)
  ├── Attribute (many)        attribute.asset.id → asset.id
  │     ├── StringAttribute   (type.name = "Description")
  │     └── BooleanAttribute  (type.name = "Certification")
  ├── Relation (many)         relation.source.id or relation.target.id → asset.id
  │     └── links to other Assets (Business Terms, other tables, etc.)
  └── Responsibility (many)   responsibility.baseResource.id → asset.id (or domain.id when inherited)
        ├── role.id → Role.id (Data Owner, Steward, SME)
        └── owner.id → User.id or UserGroup.id
```

**Join key summary**

| Join                               | Left key                    | Right key              |
|------------------------------------|-----------------------------|------------------------|
| asset → domain                     | `asset.domain.id`           | `domain.id`            |
| asset → community (via domain)     | `domain.community.id`       | `community.id`         |
| attribute → asset                  | `attribute.asset.id`        | `asset.id`             |
| relation → source asset            | `relation.source.id`        | `asset.id`             |
| relation → target asset            | `relation.target.id`        | `asset.id`             |
| responsibility → asset/domain      | `responsibility.baseResource.id` | `asset.id` or `domain.id` |
| responsibility → user              | `responsibility.owner.id`   | `user.id` (when `owner.resourceDiscriminator == "User"`) |
| responsibility → group             | `responsibility.owner.id`   | `userGroup.id` (when `owner.resourceDiscriminator == "Group"`) |

---

## Field Type Mapping

| Collibra field type    | API type          | Connector / Spark type | Notes |
|------------------------|-------------------|------------------------|-------|
| UUID fields (`id`, `createdBy`, etc.) | string (UUID format) | `StringType` | Store as string; do not cast to binary |
| `createdOn`, `lastModifiedOn` | int64 (epoch ms) | `LongType` (or `TimestampType` after divide by 1000) | UTC milliseconds since epoch |
| `name`, `displayName`, `description` | string | `StringType` | |
| `system`, `meta`, `excludedFromAutoHyperlinking` | boolean | `BooleanType` | |
| `articulationScore`, `avgRating` | double (0–100) | `DoubleType` | |
| `ratingsCount` | int32 | `IntegerType` | |
| `value` on `StringAttribute` / `ScriptAttribute` | string | `StringType` | |
| `value` on `NumericAttribute` | number or string | `DoubleType` (coerce) | May arrive as string representation |
| `value` on `BooleanAttribute` | boolean or string | `BooleanType` (coerce) | May arrive as `"true"/"false"` string |
| `value` on `DateAttribute` | number or string | `DateType` (epoch ms / ISO 8601) | |
| `value` on `DateTimeAttribute` | number or string | `TimestampType` | |
| `value` on `SingleValueListAttribute` | string | `StringType` | |
| `value` on `MultiValueListAttribute` | string[] | `ArrayType(StringType)` | |

---

## Known Gotchas and Implementation Notes

### 1. Inherited Responsibilities — The Most Common Footgun

When a Data Owner is assigned at the **domain** level (not the asset level), `GET
/responsibilities?resourceIds={assetId}` with default parameters will return **zero results**
for that asset. You must pass `includeInherited=true` to get domain- and community-level
owner assignments that apply to the asset.

**Recommended call:**
```
GET /responsibilities?resourceIds={assetId}&includeInherited=true
```

The `baseResource` field tells you where the responsibility was *directly assigned* — it will
point to the domain, not the asset, for inherited responsibilities. This is intentional and
needed to understand whether the responsibility is direct or inherited.

### 2. Output Module `maxCountLimit` Performance Cliff

The count query (total rows) that powers the `total` field in paginated responses requires a
separate, often expensive database scan. In large environments this can be **slower than the
data query itself**. Always set `maxCountLimit=0` (skip count) for production extracts.
Accept that you won't know total row count up-front and just page until empty.

### 3. Attribute Values Are Polymorphic

The `value` field on an attribute object is not a fixed type — it depends on
`attributeDiscriminator`. A `BooleanAttribute`'s `value` may be a JSON boolean (`true`) or a
string (`"true"`). The connector must inspect `attributeDiscriminator` and coerce accordingly.

### 4. Certification Attribute Is Not a Standard Asset Field

`articulationScore` looks like a quality indicator but is **not** the Certification status.
True certification is stored as a `BooleanAttribute` (type name `"Certification"` or
`"Certified"` — verify on instance). There is no `certified: bool` field on the asset object
itself.

### 5. `resourceType` Is Deprecated; Use `resourceDiscriminator`

All resource references include both `resourceType` (deprecated enum) and `resourceDiscriminator`
(string, introduced 2024.10+). Use `resourceDiscriminator` to identify whether an
`owner` in a responsibility is a `"User"` or a `"Group"`. The `resourceType` enum cannot
represent all resource types without breaking changes; `resourceDiscriminator` is the
forward-compatible field.

### 6. Cursor vs Offset Pagination Behavior Differs by Endpoint

- `/assets`, `/domains`, `/communities`: cursor is an opaque string passed as `cursor=` param;
  `nextCursor` is in the response body.
- `/attributes`, `/relations`: same pattern, `nextCursor` returned in response.
- `/responsibilities`: **does not support cursor pagination** — use offset/limit.
  Offset pagination is still documented as "deprecated" in other endpoints but is the
  *only* option for responsibilities.

### 7. GraphQL Knowledge Graph API — N+1 and Performance Warnings

The Knowledge Graph (GraphQL at `/rest/graphql`) is read-only and allows flexible querying
of the same entities. However, Collibra explicitly warns: "Similar to SQL, the structure of
your queries, the number of joins, the complexity of filters, and the amount of data retrieved
will directly affect execution time." Deep multi-hop joins and queries with total-count
aggregations can cause significant performance degradation. For bulk extracts, **prefer the
Output Module** over deep GraphQL queries.

### 8. Business Term Relation Type Public IDs Are Environment-Specific

Standard Collibra ships with a "Business Term classifies Data Asset" (or similarly named)
relation type, but the exact `typePublicId` and UUID vary by configuration. The connector
must either accept a configurable `relationTypePublicId` parameter or enumerate relation types
on startup via the Knowledge Graph API.

### 9. No Server-Side `lastModifiedAfter` Filter on Core REST Endpoints

The Core REST API `/assets`, `/attributes`, `/relations`, and `/responsibilities` endpoints
do not support a `lastModifiedAfter` query parameter. To implement incremental sync, the
connector must fetch pages sorted by `lastModifiedOn ASC` and filter client-side. Alternatively,
use the Output Module with a `GREATER` filter on `lastModified`.

### 10. Domain Description Is a Native Field

Unlike asset descriptions (stored as attributes), domain and community `description` fields
are **native string fields** on the domain/community objects themselves — no attribute lookup
is needed.

---

## Recommended Connector Tables

| Table              | PK    | Incremental Field  | Ingestion Type | Description |
|--------------------|-------|-------------------|----------------|-------------|
| `assets`           | `id`  | `lastModifiedOn`   | `cdc`          | All governed assets (tables, columns, reports, terms, etc.) |
| `attributes`       | `id`  | `lastModifiedOn`   | `cdc`          | Typed metadata on assets — Description, Certification, custom fields |
| `relations`        | `id`  | `lastModifiedOn`   | `cdc`          | Edges between assets (term↔asset, column↔table, etc.) |
| `responsibilities` | `id`  | `lastModifiedOn`   | `cdc`          | Role assignments (Owner, Steward, SME) with inheritance flag |
| `domains`          | `id`  | `lastModifiedOn`   | `snapshot`     | Organizational taxonomy (one level below community) |
| `communities`      | `id`  | `lastModifiedOn`   | `snapshot`     | Top-level org units (business units, departments) |

**Supplementary tables** (resolve IDs to human-readable names):

| Table         | PK    | Ingestion Type | Description |
|---------------|-------|----------------|-------------|
| `users`       | `id`  | `snapshot`     | User profiles — used to resolve `owner.id` in responsibilities |
| `user_groups` | `id`  | `snapshot`     | Group definitions — used to resolve group owner IDs |

---

## Open Questions for Live Testing Against databricks.collibra.com

1. **OAuth2 token endpoint confirmation**: The OpenAPI spec extracted from the developer portal
   lists the token URL as `/rest/oauth/v2/token`. Confirm this exact path on
   `https://databricks.collibra.com/rest/oauth/v2/token` and verify the `kg.view-all` scope
   is sufficient for all required endpoints.

2. **Certification attribute type public ID**: What is the `typePublicId` for the Certification
   boolean attribute on the `databricks.collibra.com` instance? Run:
   ```
   GET /attributes?limit=10&typePublicIds=Certification
   GET /attributes?limit=10&typePublicIds=Certified
   ```
   One of these should return results; if neither does, enumerate attribute types via the
   Knowledge Graph API.

3. **Description attribute type public ID**: Verify the `typePublicId` for the Description
   attribute is literally `"Description"`:
   ```
   GET /attributes?limit=10&typePublicIds=Description
   ```

4. **Business Term relation type public IDs**: Enumerate all relation types and identify the
   public ID for the "Business Term classifies Data Asset" (or equivalent) relation to filter
   term↔asset relations from all relations.

5. **Role UUIDs**: List available roles to identify UUIDs for Data Owner, Data Steward, and
   Subject Matter Expert for filtering responsibilities by role.

6. **Cursor pagination for `/assets`**: Verify the exact cursor field returned by `GET /assets`
   (the assets endpoint may return `nextCursor` or require ID-keyset pagination; confirm which
   approach works in practice).

7. **Rate limits**: Does `databricks.collibra.com` enforce a requests-per-minute or
   concurrent-connection limit? Test with 10 concurrent requests and monitor for 429 responses.

8. **`includeInherited` default behavior**: Confirm whether `includeInherited=true` (the
   documented default) is honored on the live instance and that domain-level owner assignments
   are correctly returned when querying by `resourceIds={assetId}`.

9. **Output Module `type` filter in ViewConfig**: The query examples above use
   `"StringAttribute": { "type": "Description", ... }` — confirm the `type` field accepts
   a public ID string (rather than a UUID) as a shorthand type filter in the Output Module.

10. **OAuth app registration**: Confirm whether registering an OAuth app via the Client
    Management REST API requires a Collibra system admin role and whether Databricks's
    Collibra instance has self-service OAuth app registration enabled.

---

## Research Log

| Source Type    | URL | Accessed (UTC) | Confidence | What it confirmed |
|----------------|-----|----------------|------------|-------------------|
| Official Docs  | https://developer.collibra.com/llms.txt | 2026-07-24 | High | Full list of documentation pages; URLs for endpoint refs |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/assets.md | 2026-07-24 | High | GET /assets params, response schema, cursor pagination, all fields |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/attributes.md | 2026-07-24 | High | GET /attributes params, attribute discriminator types, value formats |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/relations.md | 2026-07-24 | High | GET /relations params, response schema, cursor, source/target structure |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/responsibilities.md | 2026-07-24 | High | GET /responsibilities params incl. `includeInherited`, response schema, baseResource semantics |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/domains.md | 2026-07-24 | High | GET /domains params, response schema, community FK |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/communities.md | 2026-07-24 | High | GET /communities params, response schema, parent nesting |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/users.md | 2026-07-24 | High | GET /users params, user object fields |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/user-groups.md | 2026-07-24 | High | GET /userGroups params, group object fields |
| Official Docs  | https://developer.collibra.com/api/references/data-governance/authentication-sessions.md | 2026-07-24 | High | POST /auth/sessions, JSESSIONID flow, security schemes (basicAuth, jwtAuth, oauth2) |
| Official Docs  | https://developer.collibra.com/llms-full.txt | 2026-07-24 | High | OAuth2 tokenUrl = `/rest/oauth/v2/token`, scopes list, Output Module query syntax, entity model, filter operators, node limits, count performance warning |
| Official Docs  | https://developer.collibra.com/tutorials/collibra-rest-api-authentication-with-json-web-token.md | 2026-07-24 | Medium | JWT bearer auth via external IdP; `Authorization: Bearer {token}` header confirmed |
| Official Docs  | https://developer.collibra.com/tutorials/getting-started-with-collibra-rest-api.md | 2026-07-24 | High | Base URL pattern: `https://{org}.collibra.com/rest/2.0` |
| Official Docs  | https://developer.collibra.com/api/references/oauth-client-management.md | 2026-07-24 | Medium | OAuth2 Client Management API v1 exists; client_credentials grant type; registration required; limited detail available publicly |
| Official Docs  | https://developer.collibra.com/api/guides/knowledge-graph.md | 2026-07-24 | Medium | GraphQL Knowledge Graph is read-only; performance warning for complex joins confirmed |

---

## LIVE FINDINGS (databricks.collibra.com ISV instance, 2026-07-25)

Validated against the real instance with an Integration (m2m) OAuth app:

1. **Scope:** the token endpoint rejects `scope=kg.view-all` (`invalid_scope`).
   **Mint with NO `scope` parameter** — Integration apps are permissioned by app
   config, not a requested scope. Token then reads all endpoints fine.
2. **`/assets` `sortField`:** `LAST_MODIFIED` is INVALID (400). `NAME` is valid;
   omitting `sortField` also works. (Update connector default accordingly.)
3. **Scale:** 264,578 assets · 19,274 Tables · 236,924 Columns. Full unfiltered
   reads are large/slow — the incremental `lastModifiedOn` cursor is essential for
   steady state; confirm `max_records_per_batch` bounds the fetch loop, not just
   the emitted count.
4. **Asset naming (decisive for Block B FQN resolution):** Table/Column names are
   hierarchy paths with `>` separators AND a leading source-system prefix, e.g.
   `851023352977.tpch_sf001>AwsDataCatalog>tpch_sf001>dl_customer`
   Columns carry a `(column)` suffix: `...>dl_customer>c_acctbal(column)`.
   These are NOT UC FQNs — this instance's assets are AWS Glue-sourced. Block B's
   resolver must map source-system hierarchy → UC namespace and RETURN NONE (skip)
   for foreign-source assets rather than mis-hydrate. `Databricks Schema` type
   returned 0 assets in this demo instance.
5. **Type IDs:** Column = `...031008`, Database = `...031006`, Data Element =
   `...031026`, Databricks Schema = `...031413`. (Standard Table id `...031007`.)

---

## e2-dogfood VALIDATION (2026-07-25)

End-to-end on e2-dogfood via the community-connector CLI (COMMUNITY m2m connection):
- ✅ **UC minted the Collibra m2m token** (client-credentials, no-scope) and injected it — `credential_type: OAUTH_M2M`.
- ✅ **`domains` (snapshot): full end-to-end pass** — 3,296 rows landed in `main.collibra_m2m_test.domains`, `collibra_org` stamped. Pipeline COMPLETED, no errors.
- ✅ Read logic correct in isolation: direct + capped calls return the right rows fast (500 records / 0.3s); cursor advances; json-roundtrip offset works.

### Known limitation — m2m token expiry on large full-loads (FRAMEWORK-LEVEL)
- `assets` (264,578 rows) **FAILED**: `401 {"errorCode":"expiredToken"}` mid-run.
- Root cause: the connector reads `access_token` once in `__init__` and caches it in the session header (the standard m2m pattern — it deliberately does NOT hold the client secret to re-mint). Over a `Trigger.AvailableNow` run, that object persists across many microbatches; `max_records_per_batch` caps batch *size*, not total *runtime*, so a large first-load drains all history over many minutes and outlives the ~1h UC token → 401.
- **This is not Collibra-specific** — the Azure DevOps m2m connector caches the token identically; large full-loads there would hit the same wall (masked so far only because prior validations used tiny tables).
- **Needs a framework/UC answer**, not a per-connector patch: either UC re-injects a fresh token per microbatch, or the framework exposes a token-refresh hook the connector can call. Raised with the connectors team.
- Snapshot + small/incremental tables are unaffected. `domains` (and steady-state incremental) work today.

### Workaround + roadmap (confirmed with connectors team, 2026-07-26)
- **Workaround today:** re-run the pipeline. UC re-mints the temp m2m token on a
  new update, and the incremental `lastModifiedOn` cursor resumes where the
  prior run left off — so a large first-load that hits the ~1h token wall is
  completed by re-triggering, not restarted from scratch.
- **Roadmap (framework, not this connector):** a "managed" ingestion-pipeline
  variant for community connectors is being explored that would detect this
  error and auto-retry; the Python-data-source path additionally needs a Python
  Data Source API change (Spark-team discussion). Tracked on the framework side
  — no per-connector fix intended.
