# Azure Data Manager for Energy (ADME) — OSDU API Documentation

> **Source name:** `adme`
> **OSDU milestone compliance:** M25.1 (as of 2026-05)
> **Tables in scope:** Wellbore, Reservoir, Rock_and_Fluid
> **Phase:** 1 (no live credentials — documentation only)

---

## Authorization

### Preferred method: Azure AD OAuth 2.0 Client Credentials Flow

ADME authenticates via Microsoft Entra ID (formerly Azure Active Directory). The connector stores `tenant_id`, `client_id`, and `client_secret` and exchanges them for a short-lived Bearer token at runtime. No user-facing OAuth flow is needed.

**Token endpoint:**

```
POST https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token
Content-Type: application/x-www-form-urlencoded
```

**Request parameters:**

| Parameter | Value |
|-----------|-------|
| `grant_type` | `client_credentials` |
| `client_id` | App registration client ID (same as the one used to provision ADME) |
| `client_secret` | App registration client secret |
| `scope` | `<client-id>/.default` |

**Example request (curl):**

```bash
curl --request POST \
  'https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token' \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=client_credentials' \
  --data-urlencode 'scope=<client-id>/.default' \
  --data-urlencode 'client_id=<client-id>' \
  --data-urlencode 'client_secret=<client-secret>'
```

**Example response:**

```json
{
  "token_type": "Bearer",
  "expires_in": 86399,
  "ext_expires_in": 86399,
  "access_token": "eyJ0eXAiOiJKV1Qi..."
}
```

The `access_token` is a JWT Bearer token valid for ~24 hours (`expires_in` seconds). The connector must refresh it before expiry.

### Using the token on every API call

All ADME data-plane calls require two headers:

```
Authorization: Bearer <access_token>
data-partition-id: <data-partition-id>
```

The `data-partition-id` is the logical partition within the ADME instance (e.g., `opendes`, `dp1`). It is found in the ADME instance overview on the Azure portal under "Data Partitions". Every single API call to the search, storage, and schema services must include this header.

### Alternative auth methods (not used by connector)

- **Managed Identity**: Supported when the compute runs in the same Azure tenant. Uses `managed_identity_client_id`. No client secret needed.
- **Static Bearer token**: For development/testing only. Connector can accept a pre-minted token directly.

The connector uses the client-credentials flow as the primary method because it works cross-tenant and does not require browser interaction.

### Required ADME role for the service principal

The service principal must be granted the role `users.datalake.viewers@<data-partition-id>.dataservices.energy` in the ADME Entitlements service to read data records.

---

## Base URL Pattern

**ADME (Azure-hosted):**

```
https://<instance-name>.energy.azure.com
```

Example: `https://admetest.energy.azure.com`

**Generic OSDU (community/self-hosted):**

```
https://<host>/api/...
```

The ADME base URL serves all OSDU services under the same host; service-specific paths are appended:

| Service | Path prefix |
|---------|-------------|
| Search | `/api/search/v2/` |
| Storage | `/api/storage/v2/` |
| Schema | `/api/schema-service/v1/` |
| Legal | `/api/legal/v1/` |
| Wellbore DDMS | `/api/os-wellbore-ddms/ddms/v3/` |
| Reservoir DDMS | `/api/reservoir-ddms/v2/` |
| RAFS DDMS | `/api/rafs-ddms/v2/` |

The connector uses the **OSDU Search Service** (`/api/search/v2/`) as the primary ingestion path for all three tables in scope, as it provides a uniform, cursor-paginated interface across all record kinds.

---

## Object List

### Static list — OSDU kind strings

OSDU record kinds are the unit of data typing. Each "kind" maps to a table/entity in the connector. The full kind format is:

```
<authority>:<source>:<entity-type>:<major>.<minor>.<patch>
```

For objects registered against the OSDU Working Knowledge Store (WKS), the authority is `osdu` and source is `wks`. Wildcard versions (`*`) can be used in search queries to match all versions of a kind.

The three tables in scope and their canonical kind strings:

| Connector Table | OSDU Kind (exact) | OSDU Kind (wildcard for search) |
|-----------------|-------------------|----------------------------------|
| `Wellbore` | `osdu:wks:master-data--Wellbore:1.0.0` | `osdu:wks:master-data--Wellbore:*` |
| `Reservoir` | `osdu:wks:master-data--Reservoir:1.2.0` | `osdu:wks:master-data--Reservoir:*` |
| `Rock_and_Fluid` | `osdu:wks:master-data--Sample:2.1.0` | `osdu:wks:master-data--Sample:*` |

**Notes on kind version selection:**
- `Wellbore` 1.0.0 is the baseline stable kind used across most ADME deployments; 1.5.1 is also referenced in the RAFS tutorial (where it appears as the kind used when creating Wellbore records that relate to RAFS). The wildcard query form (`osdu:wks:master-data--Wellbore:*`) captures all deployed versions and is preferred.
- `Reservoir` 1.2.0 is the latest schema found in the OSDU data definitions GitLab repository; 1.0.0 is also referenced. The wildcard form is used in practice.
- `Rock_and_Fluid` — See the "Rock_and_Fluid Kind Mapping Decision" note below.

### Rock_and_Fluid Kind Mapping Decision

The prompt requests `Rock_and_Fluid` mapped to `osdu:wks:master-data--RockAndFluid:1.0.0`. Research across the OSDU Data Definitions GitLab, the ADME RAFS tutorial, and the RAFS DDMS services wiki confirms that **no kind named `master-data--RockAndFluid` exists in the standard OSDU WKS schema registry**. The OSDU Rock & Fluid Sample domain uses a dedicated DDMS (RAFS — Rock and Fluid Samples Domain Data Management Services) with the following master-data kinds:

| OSDU Kind | Description |
|-----------|-------------|
| `osdu:wks:master-data--Sample:2.1.0` | The physical sample (core plug, fluid sample, sidewall core, etc.). This is the primary entity. |
| `osdu:wks:master-data--SampleAcquisitionJob:1.0.0` | Job that collected samples (depth range, tool, run number) |
| `osdu:wks:master-data--SampleContainer:1.0.0` | Container holding a sample |
| `osdu:wks:master-data--SampleChainOfCustodyEvent:1.0.0` | Custody transfer events |
| `osdu:wks:master-data--GenericFacility:1.0.0` | Lab facility |
| `osdu:wks:master-data--GenericSite:1.0.0` | Acquisition site |

**Decision:** The connector's `Rock_and_Fluid` table is mapped to `osdu:wks:master-data--Sample:2.1.0`, which is the core OSDU entity representing a physical rock or fluid sample. This kind is the best single-entity analog to "rock and fluid" master data and is the anchor entity for all RAFS DDMS analysis chains. The mapping is documented here as a deliberate choice; the exact kind `osdu:wks:master-data--RockAndFluid:1.0.0` does not exist in any standard OSDU release found in research.

**Instance-specific note:** Some OSDU/ADME instances expose rock & fluid sample data under the work-product family — for example `osdu:wks:work-product--RockSample:*` is used in the internal `dbx-growth-dev` reference accelerator (v1.1.0). If the configured ADME instance returns no records under `osdu:wks:master-data--Sample:*`, operators should verify the actual sample kind in that partition (e.g. via the Schema Service `GET /api/schema-service/v1/schema` enumeration in the next section) before assuming the connector is broken. Switching kinds is a one-line change in `adme_schemas.py:TABLE_TO_KIND_QUERY` plus a re-record of the simulator corpus.

### Discovering available kinds at runtime

The OSDU Schema Service can enumerate registered kinds:

```
GET https://<instance>.energy.azure.com/api/schema-service/v1/schema?authority=osdu&source=wks&entityType=master-data--Wellbore
Authorization: Bearer <token>
data-partition-id: <data-partition-id>
```

However, for the three tables in scope the kind strings are static and enumeration at runtime is not required.

---

## Object Schema

### Universal OSDU record envelope

Every record returned by the Search or Storage service follows this top-level envelope regardless of kind:

```json
{
  "id":         "string — <data-partition-id>:<entity-type>:<natural-key>",
  "kind":       "string — e.g. osdu:wks:master-data--Wellbore:1.0.0",
  "version":    "integer (int64) — monotonically increasing version number",
  "acl": {
    "owners":   ["array of entitlement group emails"],
    "viewers":  ["array of entitlement group emails"]
  },
  "legal": {
    "legaltags":                  ["legal tag names"],
    "otherRelevantDataCountries": ["ISO country codes"],
    "status":                     "compliant | incompliant"
  },
  "meta":       "array — frame-of-reference metadata (CRS, units, etc.)",
  "tags":       "object — arbitrary string key-value pairs",
  "createTime": "ISO-8601 datetime string — record creation time",
  "createUser": "string — user/service principal who created the record",
  "modifyTime": "ISO-8601 datetime string — last modification time (present when record has been updated)",
  "modifyUser": "string — user/service principal who last modified the record",
  "data":       "object — kind-specific payload (fields documented per-table below)"
}
```

**Important:** `modifyTime` is not always present on the first version of a record. If a record has never been updated, `modifyTime` may be absent or equal to `createTime`. The `version` field is always present and increments on every update.

### Schema retrieval via Schema Service

To retrieve the full JSON Schema for a specific kind:

```
GET https://<instance>.energy.azure.com/api/schema-service/v1/schema/osdu:wks:master-data--Wellbore:1.0.0
Authorization: Bearer <token>
data-partition-id: <data-partition-id>
```

Response is a JSON Schema document describing all fields in `data.*`.

---

## Get Object Primary Keys

All OSDU records use `id` as the natural primary key. It is a string with the structure:

```
<data-partition-id>:<entity-type>:<natural-key-suffix>
```

Examples:
- `opendes:master-data--Wellbore:WELL-001-SIDETRACK-A`
- `opendes:master-data--Reservoir:NORTH-SEA-BLOCK-15-R1`
- `opendes:master-data--Sample:KKS-CORE-PLUG-001`

The `version` field (int64) is the surrogate for a specific version of a record. The tuple `(id, version)` uniquely identifies an immutable point-in-time snapshot.

| Table | Primary Key Column | Type | Notes |
|-------|--------------------|------|-------|
| Wellbore | `id` | string | Full qualified record ID |
| Reservoir | `id` | string | Full qualified record ID |
| Rock_and_Fluid (Sample) | `id` | string | Full qualified record ID |

The `id` field is always included in search results. No additional call is needed to obtain the primary key.

---

## Object Ingestion Type

| Table | Ingestion Type | Rationale |
|-------|---------------|-----------|
| Wellbore | `cdc` | Records have `modifyTime` (when updated) and `version`. The Search Service supports filtering by `modifyTime` using Lucene range syntax, enabling upsert-based incremental sync. Deletes are soft-deletions (records marked as inactive in the Storage Service) not propagated to Search, so true delete-sync is not supported. |
| Reservoir | `cdc` | Same pattern as Wellbore. |
| Rock_and_Fluid (Sample) | `cdc` | Same pattern. `Sample` records include `createTime` and `modifyTime` in the top-level envelope; the `data.SampleAcquisition.AcquisitionStartDate` and `data.SampleAcquisition.AcquisitionEndDate` are domain timestamps but are not reliable as sync cursors. |

All three tables use `cdc` (incremental with upserts, no deletes). The incremental cursor is `modifyTime` (falling back to `createTime` for records that have never been modified). See the Read API section for the Lucene filter pattern.

---

## Read API for Data Retrieval

### Primary endpoint: POST /api/search/v2/query_with_cursor

This is the recommended endpoint for bulk data extraction. It supports cursor-based deep pagination without the 10,000-record offset limit of the regular `/query` endpoint.

**URL:**

```
POST https://<instance>.energy.azure.com/api/search/v2/query_with_cursor
```

**Required headers:**

```
Authorization: Bearer <access_token>
Content-Type: application/json
data-partition-id: <data-partition-id>
```

**Request body — initial request (no cursor):**

```json
{
  "kind": "osdu:wks:master-data--Wellbore:*",
  "query": "*",
  "limit": 1000
}
```

**Request body — with cursor (subsequent pages):**

```json
{
  "kind": "osdu:wks:master-data--Wellbore:*",
  "query": "*",
  "limit": 1000,
  "cursor": "<cursor-string-from-previous-response>"
}
```

**Request body — incremental (filtered by modifyTime watermark):**

```json
{
  "kind": "osdu:wks:master-data--Wellbore:*",
  "query": "modifyTime:[\"2025-01-01T00:00:00Z\" TO *]",
  "limit": 1000
}
```

**Full request body parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `kind` | string or array | Yes | Kind string to query. Supports `*` wildcard for version. Array of kinds supported. |
| `query` | string | No | Lucene query string. Use `*` or omit for all records. |
| `limit` | integer | No | Page size. Default: 10. Max: 1000. |
| `cursor` | string | No | Opaque cursor from previous response. Omit on first request. |
| `sort` | object | No | `{"fields": ["<field>"], "order": ["ASC"]}`. Useful with `modifyTime` for stable ordering. |
| `returnedFields` | array | No | Subset of fields to return (e.g., `["id", "version", "data.FacilityName"]`). |
| `spatialFilter` | object | No | Geo-spatial filter by distance, bounding box, or polygon. Field: `data.SpatialLocation.Wgs84Coordinates`. |

**Parameters NOT supported by `query_with_cursor` (unlike `/query`):**
- `offset` — not valid; use cursor for pagination
- `aggregateBy` — not valid
- `trackTotalCount` — always `true` in cursor mode

**Response structure:**

```json
{
  "totalCount": 4821,
  "cursor": "AoE...<opaque-base64-string>",
  "results": [
    {
      "id": "opendes:master-data--Wellbore:WELL-001-A",
      "kind": "osdu:wks:master-data--Wellbore:1.0.0",
      "version": 1753292228903506,
      "createTime": "2024-03-15T10:22:00.000Z",
      "modifyTime": "2024-11-01T08:14:32.000Z",
      "acl": { "owners": [...], "viewers": [...] },
      "legal": { "legaltags": [...], "status": "compliant" },
      "data": { ... }
    }
  ]
}
```

**Cursor semantics:**

1. Send initial request with `kind` and `query` but no `cursor`.
2. Response includes `cursor` (opaque string) and `results`.
3. If `results` is non-empty and `cursor` is present, send a follow-up request with the same `kind`/`query` plus `"cursor": "<value>"`.
4. Continue until `results` is empty or `cursor` is absent/null.
5. `totalCount` is always returned and reflects total matching records at snapshot time.
6. The cursor context is held server-side for **1 minute** of inactivity. If the next page request is not sent within 1 minute of the previous response, the cursor expires and the connector must restart pagination from the beginning (or use a `createTime`/`modifyTime` range query to resume approximately).
7. Maximum **500 concurrent cursors** are allowed per data partition. The connector must not open parallel cursor streams beyond this limit.

**Alternative: search_after mode (OSDU M25+ / ADME 2024+)**

ADME's OSDU M25+ release replaces the Elasticsearch scroll-based cursor with a stateless `search_after` implementation. The API signature is identical — same endpoint, same request/response shape. No connector code change is required; the server transparently uses `search_after` internally, which eliminates the 1-minute expiry constraint. If a cursor-expired error is returned (HTTP 400 with cursor-expiry message), the recommended recovery is to re-issue the query with a Lucene range filter anchoring to the last-seen `createTime` or `modifyTime`.

### Secondary endpoint: GET /api/storage/v2/records/{id}

Use this to retrieve a single record by its fully-qualified `id`. Useful for lookups and validation.

```
GET https://<instance>.energy.azure.com/api/storage/v2/records/<encoded-id>
Authorization: Bearer <token>
data-partition-id: <data-partition-id>
```

Optional query parameter `attribute=data.FacilityName,data.WellID` restricts returned fields from the `data` envelope.

Response is the full record envelope (same structure as search results).

### Incremental sync pattern

The connector uses `modifyTime` as the watermark cursor. Since records can be created without ever being modified, `createTime` is used as a fallback for records where `modifyTime` is absent.

**Recommended approach:**

1. **First run (full load):** Query with `kind` and `query: "*"`, paginate with cursor until exhausted. Record `max(modifyTime, createTime)` across all results as the watermark.

2. **Subsequent runs (incremental):** Filter with the Lucene range:

```lucene
modifyTime:["<last-watermark-ISO>" TO *] OR (createTime:["<last-watermark-ISO>" TO *] AND NOT _exists_:modifyTime)
```

Simplified form used in the reference energy-sandbox connector:

```lucene
data.modifyTime:>="<watermark>"
```

Note: The reference connector (energy-sandbox repo) uses `data.modifyTime` (inside the `data` envelope) rather than the top-level `modifyTime`. Which field is populated depends on the OSDU version and kind; verify against the target instance. Top-level `modifyTime` is set by the Storage Service on every write; `data.modifyTime` is a field within the kind schema set by the producing application.

3. **Lookback window:** Add a small lookback (e.g., subtract 5 minutes from the watermark) to handle records that were in-flight during the last run.

4. **Upsert semantics:** Use `id` as the merge key. Write mode is `MERGE` (upsert) keyed on `id`. The `version` field can be used to deduplicate if the same record is seen multiple times.

### Rate limits and throttling

| Limit | Value | Notes |
|-------|-------|-------|
| Max records per page (`limit`) | 1000 | Hard limit enforced by Search Service |
| Max concurrent cursors per partition | 500 | Default; server returns HTTP 400 on excess |
| Cursor inactivity timeout | 1 minute | Pages must be fetched within 1 minute of previous response |
| Wildcard query rate limit (ADME-specific) | ~12 wildcard queries/minute | Token-bucket: 2 burst tokens, refill rate 1 token/5 s. Use explicit kind strings to avoid this limit. |
| HTTP 429 (Too Many Requests) | Variable | Response includes `Retry-After` header. Use exponential backoff. |

**Recommended approach to avoid rate limits:**
- Always specify a concrete kind string (e.g., `osdu:wks:master-data--Wellbore:*`) rather than fully open wildcards (e.g., `osdu:wks:*:*`).
- Do not open more than one concurrent cursor per table.
- Implement exponential backoff with jitter on HTTP 429, 500, 502, and 503.

---

## Per-Table Details

### Table 1: Wellbore

**OSDU Kind:** `osdu:wks:master-data--Wellbore:1.0.0` (use wildcard `osdu:wks:master-data--Wellbore:*` in queries)

**Natural primary key:** `id`

**Description:** Represents a single drilled or planned borehole. A Wellbore is a child of a Well; one Well may have multiple Wellbores.

**Search query example:**

```json
{
  "kind": "osdu:wks:master-data--Wellbore:*",
  "query": "*",
  "limit": 1000
}
```

**Spatial search example (wellbores within 10 km of a point):**

```json
{
  "kind": "osdu:wks:master-data--Wellbore:*",
  "query": "*",
  "limit": 1000,
  "spatialFilter": {
    "field": "data.SpatialLocation.Wgs84Coordinates",
    "byDistance": {
      "point": {"longitude": 2.3522, "latitude": 48.8566},
      "distance": 10000
    }
  }
}
```

**`data` envelope fields:**

| Field | Type | Description |
|-------|------|-------------|
| `FacilityID` | string | Internal facility identifier |
| `FacilityName` | string | Primary human-readable wellbore name |
| `FacilityNameAliases` | array | List of `{AliasName, AliasNameTypeID}` objects — UWI, API number, regulatory IDs |
| `FacilityTypeID` | string | Reference to facility type (always "Wellbore") |
| `FacilityOperators` | array | `[{FacilityOperatorID, FacilityOperatorOrganisationID, EffectiveDateTime}]` |
| `WellID` | string | Reference to the parent Well record ID |
| `KickOffWellboreID` | string | Reference to the wellbore from which this one was kicked off (for sidetracks) |
| `VerticalMeasurements` | array | `[{VerticalMeasurementID, VerticalMeasurement, VerticalMeasurementTypeID, VerticalMeasurementPathID}]` — e.g., KB, MSL, SF datums |
| `SpatialLocation` | object | `{Wgs84Coordinates: {type: "Point", coordinates: [lon, lat]}}` — surface location |
| `GeoContexts` | array | `[{FieldID, GeoTypeID}]` — geographic context references |
| `DrillingReasons` | array | Reasons the wellbore was drilled |
| `StatusSummary` | string | Current operational status |
| `TargetFormation` | string | Geologic formation targeted |
| `InitialCompletion` | object | First completion details |
| `ExtensionProperties` | object | Arbitrary key-value extension properties |

**Sample record (top-level envelope + abbreviated data):**

```json
{
  "id": "opendes:master-data--Wellbore:WELL-001-MAIN",
  "kind": "osdu:wks:master-data--Wellbore:1.0.0",
  "version": 1753292228903506,
  "createTime": "2024-03-15T10:22:00.000Z",
  "modifyTime": "2024-11-01T08:14:32.000Z",
  "acl": {
    "owners": ["data.default.owners@opendes.dataservices.energy"],
    "viewers": ["data.default.viewers@opendes.dataservices.energy"]
  },
  "legal": {
    "legaltags": ["opendes-demo-legal-tag"],
    "otherRelevantDataCountries": ["US"],
    "status": "compliant"
  },
  "meta": null,
  "data": {
    "FacilityName": "Well-001 Main Bore",
    "FacilityNameAliases": [
      {
        "AliasName": "20-001-00001-00-00",
        "AliasNameTypeID": "opendes:reference-data--AliasNameType:UniqueIdentifier:"
      }
    ],
    "WellID": "opendes:master-data--Well:WELL-001:",
    "SpatialLocation": {
      "Wgs84Coordinates": {
        "type": "Point",
        "coordinates": [2.3522, 48.8566]
      }
    },
    "VerticalMeasurements": [
      {
        "VerticalMeasurementID": "KB",
        "VerticalMeasurement": 50.0,
        "VerticalMeasurementTypeID": "opendes:reference-data--VerticalMeasurementType:KellyBushing:",
        "VerticalMeasurementPathID": "opendes:reference-data--VerticalMeasurementPath:MD:"
      }
    ],
    "ExtensionProperties": {}
  }
}
```

---

### Table 2: Reservoir

**OSDU Kind:** `osdu:wks:master-data--Reservoir:1.2.0` (use wildcard `osdu:wks:master-data--Reservoir:*` in queries)

**Natural primary key:** `id`

**Description:** Represents a porous and permeable subsurface rock unit containing hydrocarbons (or other fluids). A Reservoir is associated with one or more fields and is referenced by wellbore completions.

Note: The Reservoir master-data kind is distinct from the Reservoir DDMS (which stores RESQML-format geological and simulation models). The master-data Reservoir kind stores basic descriptive metadata about the reservoir unit itself and is queryable via the Search Service.

**Search query example:**

```json
{
  "kind": "osdu:wks:master-data--Reservoir:*",
  "query": "*",
  "limit": 1000
}
```

**`data` envelope fields:**

| Field | Type | Description |
|-------|------|-------------|
| `ReservoirID` | string | Unique identifier for the reservoir |
| `ReservoirName` | string | Human-readable name of the reservoir |
| `ReservoirType` | string | Type classification (e.g., "conventional", "tight gas") |
| `ReservoirDescription` | string | Free-text description |
| `FieldID` | string | Reference to the Field record |
| `BasinID` | string | Reference to the Basin record |
| `FormationID` | string | Geologic formation reference |
| `DepthTopMD` | number | Top of reservoir measured depth |
| `DepthBaseMD` | number | Base of reservoir measured depth |
| `DepthTopTVD` | number | Top true vertical depth |
| `DepthBaseTVD` | number | Base true vertical depth |
| `GrossThickness` | number | Gross reservoir thickness (m) |
| `NetPayThickness` | number | Net pay thickness (m) |
| `PorosityAverage` | number | Average porosity (fraction) |
| `WaterSaturationAverage` | number | Average water saturation (fraction) |
| `PermeabilityHorizontal` | number | Horizontal permeability (mD) |
| `PermeabilityVertical` | number | Vertical permeability (mD) |
| `InitialReservoirPressure` | number | Initial reservoir pressure (psi or bar, see `meta` for units) |
| `ReservoirTemperature` | number | Reservoir temperature (°C or °F, see `meta` for units) |
| `FluidTypeID` | string | Primary fluid type reference |
| `GeoContexts` | array | `[{FieldID, GeoTypeID}]` |
| `NameAliases` | array | Alternative names / regulatory identifiers |
| `ExtensionProperties` | object | Arbitrary extension fields |

**TBD:** The exact field names for `PorosityAverage`, `PermeabilityHorizontal`, `NetPayThickness`, etc. are derived from OSDU data-definition conventions and community examples. The canonical JSON Schema for `Reservoir.1.2.0` should be retrieved via the Schema Service at runtime to confirm every field name and type. The Reservoir.1.2.0.json file is registered in the OSDU data-definitions GitLab repository (`sample-image` branch) but was not publicly accessible during research.

**Sample record (abbreviated):**

```json
{
  "id": "opendes:master-data--Reservoir:NORTH-SEA-BLOCK-15-R1",
  "kind": "osdu:wks:master-data--Reservoir:1.2.0",
  "version": 1753000000000001,
  "createTime": "2024-01-10T09:00:00.000Z",
  "acl": {
    "owners": ["data.default.owners@opendes.dataservices.energy"],
    "viewers": ["data.default.viewers@opendes.dataservices.energy"]
  },
  "legal": {"legaltags": ["opendes-demo-legal-tag"], "status": "compliant"},
  "data": {
    "ReservoirName": "North Sea Block 15 R1",
    "ReservoirType": "Conventional",
    "FieldID": "opendes:master-data--Field:NORTH-SEA-BLOCK-15:",
    "DepthTopMD": 2800.0,
    "DepthBaseMD": 3050.0,
    "PorosityAverage": 0.18,
    "PermeabilityHorizontal": 120.0,
    "GeoContexts": [{"FieldID": "opendes:master-data--Field:NORTH-SEA-BLOCK-15:", "GeoTypeID": "Field"}],
    "ExtensionProperties": {}
  }
}
```

---

### Table 3: Rock_and_Fluid

**OSDU Kind:** `osdu:wks:master-data--Sample:2.1.0` (use wildcard `osdu:wks:master-data--Sample:*` in queries)

**Mapping rationale:** No kind `master-data--RockAndFluid` exists in the OSDU WKS standard schema registry. The OSDU Rock & Fluid Sample domain uses the RAFS DDMS, where the primary master-data entity is `master-data--Sample`. This kind represents the physical sample (core plug, fluid sample, sidewall core, cutting, etc.) from which rock and fluid properties are measured. It is the anchor entity for all RAFS analysis records. See "Object List — Rock_and_Fluid Kind Mapping Decision" for full rationale.

**Natural primary key:** `id`

**Description:** A physical sample of rock or fluid acquired from a wellbore or surface location, used for laboratory analysis of petrophysical and fluid properties.

**Search query example:**

```json
{
  "kind": "osdu:wks:master-data--Sample:*",
  "query": "*",
  "limit": 1000
}
```

**Filter to core/fluid samples only (example using SampleAcquisitionType reference):**

```json
{
  "kind": "osdu:wks:master-data--Sample:*",
  "query": "data.SampleAcquisition.SampleAcquisitionTypeID:(*CoreSampleAcquisition* OR *DownholeSampleAcquisition*)",
  "limit": 1000
}
```

**`data` envelope fields:**

| Field | Type | Description |
|-------|------|-------------|
| `SampleAcquisition` | object | Nested object — how/when/where the sample was acquired |
| `SampleAcquisition.SampleAcquisitionJobID` | string | Reference to `master-data--SampleAcquisitionJob` |
| `SampleAcquisition.SampleAcquisitionTypeID` | string | Type of acquisition (core, downhole fluid, etc.) |
| `SampleAcquisition.SampleAcquisitionContainerID` | string | Reference to container used |
| `SampleAcquisition.AcquisitionStartDate` | string (ISO-8601) | Start of acquisition |
| `SampleAcquisition.AcquisitionEndDate` | string (ISO-8601) | End of acquisition |
| `SampleAcquisition.SampleAcquisitionDetail` | object | Domain-specific acquisition detail |
| `SampleAcquisition.SampleAcquisitionDetail.TopDepth` | number | Top depth of sample acquisition (ft or m) |
| `SampleAcquisition.SampleAcquisitionDetail.BaseDepth` | number | Base depth of sample acquisition |
| `SampleAcquisition.SampleAcquisitionDetail.ToolKind` | string | Tool used (e.g., "Wireline Formation Tester") |
| `SampleAcquisition.SampleAcquisitionDetail.RunNumber` | string | Run number |
| `SampleAcquisition.SampleAcquisitionDetail.WellboreID` | string | Reference to parent `master-data--Wellbore` |
| `SampleAcquisition.SampleAcquisitionDetail.VerticalMeasurement` | object | Depth/measurement context |
| `SampleAcquisition.SampleAcquisitionDetail.FormationCondition` | object | `{Pressure, Temperature}` at acquisition |
| `SampleAcquisition.CollectionServiceCompanyID` | string | Lab/service company that collected the sample |
| `SampleAcquisition.HandlingServiceCompanyID` | string | Lab/service company handling the sample |
| `ExtensionProperties` | object | Arbitrary extension fields |

**Sample record (abbreviated):**

```json
{
  "id": "opendes:master-data--Sample:KKS-CORE-PLUG-001",
  "kind": "osdu:wks:master-data--Sample:2.1.0",
  "version": 1758005722706490,
  "createTime": "2024-06-01T12:00:00.000Z",
  "acl": {
    "owners": ["data.default.owners@opendes.dataservices.energy"],
    "viewers": ["data.default.viewers@opendes.dataservices.energy"]
  },
  "legal": {"legaltags": ["opendes-demo-legal-tag"], "status": "compliant"},
  "data": {
    "SampleAcquisition": {
      "SampleAcquisitionJobID": "opendes:master-data--SampleAcquisitionJob:KKS-CORE-20230304-001:",
      "SampleAcquisitionTypeID": "opendes:reference-data--SampleAcquisitionType:DownholeSampleAcquisition:",
      "AcquisitionStartDate": "2023-03-04T00:00:00Z",
      "AcquisitionEndDate": "2023-03-07T00:00:00Z",
      "SampleAcquisitionDetail": {
        "TopDepth": 10000.0,
        "BaseDepth": 10020.0,
        "ToolKind": "Wireline Formation Tester",
        "RunNumber": "22",
        "WellboreID": "opendes:master-data--Wellbore:KKS-1:",
        "FormationCondition": {"Pressure": 120, "Temperature": 60}
      }
    },
    "ExtensionProperties": {}
  }
}
```

---

## Incremental Support

### Summary

Incremental sync is feasible for all three tables. The mechanism is Lucene range-filtering on `modifyTime` (top-level envelope field) in the Search Service.

### How it works

The OSDU Storage Service sets the top-level `modifyTime` field on every record upsert. The Indexer Service re-indexes the record, making the updated `modifyTime` searchable via the Search Service. The field is a string in ISO-8601 format (e.g., `"2024-11-01T08:14:32.000Z"`), and the Search Service supports Lucene range queries on it:

```lucene
modifyTime:["2024-11-01T00:00:00Z" TO *]
```

The reference energy-sandbox connector uses a variant that filters on the `data` envelope field:

```lucene
data.modifyTime:>="2024-11-01T00:00:00Z"
```

**Recommendation:** Use the top-level `modifyTime` for robustness, as it is set by the OSDU platform on every write regardless of whether the kind schema includes a `data.modifyTime` field. Include a 5-minute lookback window on the watermark to handle clock skew and in-flight records.

### Cursor behavior with incremental filter

When using `query_with_cursor` with a `modifyTime` filter:
- The cursor captures a snapshot at the time of the first request.
- All subsequent pages of that cursor will reflect records that matched the filter at snapshot time.
- New records modified after the cursor was created will not appear until the next incremental run.

### Delete handling

OSDU records are logically deleted via the Storage Service (`DELETE /api/storage/v2/records/{id}`). The deleted record is **removed from the Search index** and will not appear in future search queries. However, the Search Service does not provide a "deleted records" feed. Therefore:

- **True delete-sync is not supported** by the Search Service.
- The connector ingestion type is `cdc` (upserts only), not `cdc_with_deletes`.
- If delete tracking is required, the OSDU Notification Service (pub/sub) must be used to subscribe to record deletion events — this is out of scope for Phase 1.

### Practical incremental filter for all three tables

```python
# Pseudocode for constructing the incremental filter
def build_query(watermark_iso: str) -> dict:
    return {
        "kind": "osdu:wks:master-data--Wellbore:*",  # or Reservoir:* or Sample:*
        "query": f'modifyTime:["{watermark_iso}" TO *]',
        "limit": 1000
    }
```

---

## Field Type Mapping

### OSDU to Spark/Delta type mapping

| OSDU field type | Example field | Spark/Delta type | Notes |
|-----------------|---------------|------------------|-------|
| `string` | `id`, `kind`, `FacilityName` | `StringType` | Default for most text fields |
| `integer (int64)` | `version` | `LongType` | OSDU versions are 64-bit |
| `number (float/double)` | `PorosityAverage`, `DepthTopMD` | `DoubleType` | Measurements; units in `meta` array |
| `boolean` | — | `BooleanType` | |
| `string (ISO-8601)` | `createTime`, `modifyTime`, `AcquisitionStartDate` | `TimestampType` | Parse with `pyspark.sql.functions.to_timestamp` |
| `object` | `SpatialLocation`, `SampleAcquisition` | `StructType` or `StringType` (JSON) | Recommend flattening key sub-fields; keep full JSON for complex nested objects |
| `array of strings` | `legaltags`, `otherRelevantDataCountries` | `ArrayType(StringType)` | |
| `array of objects` | `FacilityNameAliases`, `VerticalMeasurements` | `ArrayType(StructType)` or `StringType` (JSON) | |
| `reference string` (ID ref) | `WellID`, `FieldID` | `StringType` | ID references; trailing colon is a convention (e.g., `opendes:master-data--Well:123:`) |

### Special field behaviors

- **`id` format:** Always `<partition>:<entity-type>:<natural-key>`. The trailing colon in references (e.g., `WellID: "opendes:master-data--Well:123:"`) is an OSDU convention for ID references — the trailing colon is an empty version specifier meaning "latest version".
- **`version` (int64):** Microsecond-precision Unix timestamp encoded as an int64. It monotonically increases with each write. Can be used for ordering and deduplication.
- **Units of measure:** Numeric fields (depths, temperatures, pressures, etc.) have associated units defined in the `meta` array of the record. The `meta` array follows the OSDU Frame of Reference specification. Units should be preserved alongside values or normalized to a canonical unit at transform time.
- **Reference fields (IDs ending in `:`):** The trailing colon-terminated IDs are references to other OSDU records. They can be resolved via `GET /api/storage/v2/records/{id}` by stripping the trailing colon.
- **`SpatialLocation.Wgs84Coordinates`:** GeoJSON format (`{"type": "Point", "coordinates": [longitude, latitude]}`). Longitude is first, latitude second (GeoJSON standard).
- **`acl.owners` / `acl.viewers`:** Arrays of OSDU Entitlements email-format group strings. Format: `<group-name>@<partition>.dataservices.energy`.

---

## Error Handling

| HTTP Status | Meaning | Connector Action |
|-------------|---------|------------------|
| `200 OK` | Success | Parse response, continue |
| `400 Bad Request` | Malformed request body, or cursor expired | Log error body. If cursor-expired, rebuild query with `modifyTime` range anchor and restart pagination. |
| `401 Unauthorized` | Invalid or expired Bearer token | Refresh token and retry once. If still failing, raise auth error. |
| `403 Forbidden` | Token valid but missing entitlement group, or wrong `data-partition-id` | Log and raise. Manual remediation required (add service principal to correct entitlement group). |
| `404 Not Found` | Kind not registered in this partition, or record `id` not found | Log warning, skip. Kind may not be deployed in this ADME instance. |
| `429 Too Many Requests` | Rate limit exceeded | Read `Retry-After` header (seconds). Sleep and retry with exponential backoff (base 2s, max 120s, jitter). |
| `500 Internal Server Error` | Transient server error | Exponential backoff, retry up to 3 times. |
| `502 Bad Gateway` | Transient proxy/gateway error | Same as 500. |
| `503 Service Unavailable` | ADME maintenance or overload | Exponential backoff, retry up to 5 times with longer delays. |

**Cursor expiry (HTTP 400 with body containing "cursor" and "expired"):**

```python
# Recovery pattern after cursor expiry
# 1. Record the last-seen modifyTime from successfully processed records
# 2. Re-issue query_with_cursor without cursor, but with modifyTime range filter
# 3. Continue pagination from the new cursor
```

---

## Sources and References

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| User-provided (reference connector) | https://github.com/databricks-industry-solutions/energy-sandbox/blob/main/osdu-app-with-connector/README.md | 2026-05-09 | Highest | Auth modes, cursor pagination style, kind patterns for Wellbore/Reservoir/Rock_and_Fluid, `data.modifyTime` filter pattern, page size 50 default |
| Official Docs (Microsoft Learn) | https://learn.microsoft.com/en-us/azure/energy-data-services/how-to-generate-auth-token | 2026-05-09 | High | OAuth2 client credentials token endpoint, exact `scope` and `resource` parameter values, `data-partition-id` discovery |
| Official Docs (Microsoft Learn) | https://learn.microsoft.com/en-us/azure/energy-data-services/osdu-services-on-adme | 2026-05-09 | High | Complete list of OSDU services on ADME, M25.1 milestone compliance, DDMS service names and API path prefixes |
| Official Docs (Microsoft Learn) | https://learn.microsoft.com/en-us/azure/energy-data-services/tutorial-rock-and-fluid-samples-ddms | 2026-05-09 | High | RAFS kind strings (`master-data--Sample:2.1.0`, `master-data--SampleAcquisitionJob:1.0.0`, etc.), full Sample record structure, `data.SampleAcquisition` nested fields, response format |
| Official Docs (Microsoft Learn) | https://learn.microsoft.com/en-us/azure/energy-data-services/tutorial-wellbore-ddms | 2026-05-09 | High | Wellbore DDMS API path (`/api/os-wellbore-ddms/ddms/v3/`), Well record kind `osdu:wks:master-data--Well:1.1.0`, full record envelope with FacilityName, FacilityNameAliases, createTime |
| Official Docs (Microsoft Learn) | https://learn.microsoft.com/en-us/azure/energy-data-services/tutorial-reservoir-ddms-apis | 2026-05-09 | Medium | Reservoir DDMS API path (`/api/reservoir-ddms/v2/`); note: Reservoir DDMS serves RESQML-format data not master-data records; confirmed distinction from master-data Reservoir kind |
| OSDU Community Docs | https://osdu.pages.opengroup.org/platform/system/search-service/api/ | 2026-05-09 | High | Search Service endpoints (`/api/search/v2/query`, `/api/search/v2/query_with_cursor`), request body parameters, response structure (`results`, `cursor`, `totalCount`), max limit 1000 |
| OSDU Community GitLab (issue) | https://community.opengroup.org/osdu/platform/system/search-service/-/issues/365 | 2026-05-09 | High | Confirmed max cursor limit is 1000 records per page (issue title: "Cursor Not Returning Results When Limit Set Above 1,000") |
| OSDU Community (search results) | https://community.opengroup.org/osdu/platform/system/search-service/-/merge_requests/703 | 2026-05-09 | High | Confirmed `search_after` replaces scroll-based cursor in M25+; same API signature |
| OSDU Workshop (AWS) | https://edi-workshop.awsworkshop.io/3_modulethree/32_search.html | 2026-05-09 | Medium | Full query body example with `kind`, `query`, `offset`, `limit`, `sort`, `spatialFilter`, `returnedFields`, `trackTotalCount`; confirmed `spatialFilter.field = "data.SpatialLocation.Wgs84Coordinates"` |
| OSDU Community JS Client | https://github.com/pariveda/osdujs | 2026-05-09 | Medium | Cursor response structure `{results, cursor, totalCount}`; confirmed max 1000 per page; `queryWithPaging(query, cursor)` pattern |
| OSDU Data Definitions GitLab | https://community.opengroup.org/osdu/data/data-definitions/-/blob/master/E-R/master-data/Wellbore.1.1.0.md | 2026-05-09 | Medium | Kind naming convention `master-data--Wellbore`; version 1.1.0 confirmed in repo master branch |
| OSDU Schema GitLab | https://community.opengroup.org/osdu/data/data-definitions/-/blob/sample-image/SchemaRegistrationResources/shared-schemas/osdu/master-data/Reservoir.1.2.0.json | 2026-05-09 | Medium | Confirmed existence of `Reservoir.1.2.0` schema; file not publicly fetchable but search index confirms it is registered under `sample-image` branch |
| OSDU RAFS GitLab | https://community.opengroup.org/osdu/platform/domain-data-mgmt-services/rock-and-fluid-sample/rafs-ddms-services | 2026-05-09 | High | Confirmed RAFS DDMS is the authoritative service for Rock & Fluid Sample data; `master-data--Sample` is the primary kind; no `master-data--RockAndFluid` kind exists |
| Community (schema generator) | https://github.com/MIVAA-ai/osdu-raw-schema-generator/blob/main/wellbore_schema.json | 2026-05-09 | Medium | Wellbore schema fields: `FacilityName`, `WellID`, `SpatialLocation`, `VerticalMeasurements`, `GeoContexts`, `createTime`, `modifyTime`, `version` |
| AWS Blog | https://aws.amazon.com/blogs/industries/osdu-data-platform-on-aws-ingestion-series-1-overview-of-data-types-for-osdutm-data-platform/ | 2026-05-09 | Medium | OSDU data type classification (master-data, work-product-component); confirmed Wellbore as master data type |
| Microsoft Spark Connector (archived) | https://github.com/microsoft/OSDU-Spark | 2026-05-09 | Low | Confirmed `modifyTime` field in output schema; search API usage patterns |
| Rate limit (OSDU search) | Search result aggregation | 2026-05-09 | Medium | Token-bucket rate limit for wildcard queries (~12/min, 2 burst, 1/5s refill); 500 concurrent cursors; 1-minute cursor timeout; these are defaults and may differ per ADME instance |
| OSDU Storage Service Docs | https://osdu.pages.opengroup.org/platform/system/storage/api/ | 2026-05-09 | High | `GET /api/storage/v2/records/{id}` endpoint; record envelope fields confirmed |

### Confidence notes

- All kind strings for Wellbore and the Sample (Rock_and_Fluid mapping) are **high confidence** — confirmed across Microsoft Learn tutorials and OSDU community repos.
- Reservoir field names (`PorosityAverage`, `PermeabilityHorizontal`, etc.) are **medium confidence** — derived from community conventions and schema naming patterns; the `Reservoir.1.2.0.json` file was not directly fetchable. Verify via the Schema Service before implementation.
- The absence of `master-data--RockAndFluid` as a kind is **high confidence** — confirmed by exhaustive search across OSDU data-definitions GitLab, RAFS DDMS docs, and the ADME RAFS tutorial.
- Rate limits are **medium confidence** — the 500-cursor and 1-minute-timeout values come from OSDU community issue trackers and are not formally published in Microsoft's ADME documentation. Treat as defaults that may be configurable per instance.
