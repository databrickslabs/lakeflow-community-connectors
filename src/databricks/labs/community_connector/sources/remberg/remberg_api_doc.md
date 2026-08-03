# **remberg API Documentation**

remberg is an asset-centric maintenance / field-service platform ("XRM").
Its public REST API lives at `https://api.remberg.de` and is documented at
https://developers.remberg.de (human docs) and
https://developers.remberg.de/openapi (OpenAPI 3.0 JSON, one file per
resource group).

## **Authorization**

- **Single supported method: static API key.**
  The OpenAPI security scheme is `type: apiKey, in: header, name: authorization`
  — i.e. the raw key is sent in an HTTP header literally named
  `authorization` (no `Bearer ` prefix is documented anywhere in the official
  docs or OpenAPI files).
- Keys are created in the remberg web app under **Settings > Data > API**
  ("Add API Key"). The key inherits the access rights of the user who
  created it and **expires after one year** by default.
- There is no OAuth flow.

Example request:

```http
GET /v2/assets?page=1&limit=100 HTTP/1.1
Host: api.remberg.de
authorization: <API_KEY>
Accept: application/json
```

**TBD (needs live verification):** whether the API also accepts
`Authorization: Bearer <key>`. The connector sends the raw key exactly as the
OpenAPI spec describes.

## **Object List**

The object list is **static** — remberg has no metadata/discovery endpoint.
All objects are top-level resource collections with `GET` list endpoints.
The connector exposes the following 9 objects:

| Table | Endpoint | Response records key | `updatedAt` server filter | Sortable by `updatedAt` |
|---|---|---|---|---|
| `assets` | `GET /v2/assets` | `data` | `updatedAtFrom` / `updatedAtUntil` | yes (`sortField=updatedAt`) |
| `work_orders` | `GET /v2/work-orders` | `data` | `updatedAtFrom` / `updatedAtUntil` | no sort parameter |
| `tickets` | `GET /v2/tickets` | `tickets` | `updatedAtFrom` / `updatedAtUntil` | no sort parameter |
| `work_requests` | `GET /v1/work-requests` | `data` | `updatedAtFrom` / `updatedAtUntil` | no sort parameter |
| `organizations` | `GET /v1/organizations` | `organizations` | `updatedAtFrom` / `updatedAtUntil` | yes |
| `parts` | `GET /v2/parts` | `data` | `updatedAtFrom` / `updatedAtUntil` | yes |
| `contacts` | `GET /v1/contacts` | `contacts` | `updatedAtFrom` / `updatedAtUntil` | yes |
| `users` | `GET /v1/users` | `data` | `updatedAtFrom` / `updatedAtTo` (note: `To`, not `Until`) | n/a |
| `forms` | `GET /v1/forms` | `data` | none (only `finalizedAtFrom` / `finalizedAtUntil`) | `sortField=dateModified` |

Note the response envelope is inconsistent across resources: most wrap the
record array in `data`, but tickets/organizations/contacts use a
resource-named key (`tickets` / `organizations` / `contacts`).

### Sub-resource objects

These are served per parent record rather than as a flat list. Endpoint
inventory taken from the API's own documentation index at
<https://developers.remberg.de/llms.txt>, which is authoritative for what
actually has a GET.

| Table | Endpoint | Records key | Pagination | Own filters |
|---|---|---|---|---|
| `work_order_times` | `GET /v2/work-orders/{id}/times` | `timeEntries` | none | none |
| `work_order_stock_changes` | `GET /v2/work-orders/{id}/stock-changes` | `data` | `page`/`limit` | none |
| `part_inventories` | `GET /v2/parts/{partTypeId}/inventories` | `data` | `page`/`limit` | none |
| `part_stock_changes` | `GET /v2/parts/{partTypeId}/stock-changes` | `data` | `page`/`limit` | `createdAt*`, `updatedAt*` |
| `ticket_conversations` | `GET /v2/tickets/{id}/conversations` | `activities` | none | none |
| `asset_status_signals` | `GET /v2/assets/status-signals` | `data` | `page`/`limit` | `createdAt*`, `resolvedAt*` |

Two findings that shaped the implementation:

1. **Asset status signals need no fan-out.** Besides the per-asset
   `/v2/assets/{id}/status-signals`, remberg exposes a flat
   `/v2/assets/status-signals` returning every signal across all assets with
   the asset reference embedded (`AssetStatusSignalWithAssetInfoCfaResponseDto`).
   The connector uses the flat one, so this is an ordinary CDC table.
2. **Work order checklists cannot be read.** `/v2/work-orders/{id}/checklist`
   is **POST-only** (add items); there is no GET, on either the `{id}` or the
   `erp/{externalReference}` variant. The table is therefore not implementable.

The remaining `{id}` endpoints have no list-all variant, so the connector
lists the parent over a bounded `updatedAt` range and issues one request per
parent in that range (`_read_child`). Cost tracks parent churn rather than
table size, which keeps it inside the rate-limit budget in steady state; a
first backfill still costs one request per parent record.

Also not implemented (low-value metadata / different shapes): failure types,
ticket categories, user groups & roles, procedure templates, files
(`/v1/files`, hierarchical), AI endpoints, and the `number/{...}` and
`erp/{...}` lookup variants of the above (same data, keyed differently).

#### Cursor caveats for these tables

- **Child tables have no independent change cursor.** Time entries and
  conversation activities carry no `id` at all; none of the five has a
  filterable `updatedAt`. The connector injects the parent's `id` and
  `updatedAt` onto every child row and uses the injected `updatedAt` as
  `cursor_field`, because that is the value incrementality actually advances
  on. **This assumes editing a sub-resource bumps the parent's `updatedAt`.**
  Not verifiable from the docs — confirm on live data. The
  `full_parent_scan` table option is the fallback if it does not hold.
- **`asset_status_signals` is keyed on `createdAt`,** the only bound the flat
  endpoint offers. A signal's `resolvedAt` is set later, and that transition
  does not move `createdAt`, so resolutions are not picked up by an
  incremental read. Use `full_parent_scan`-style backfills or a periodic full
  refresh if resolution state matters downstream.

#### Doc bugs in these DTOs

`PartStockChangeCfaResponseDto.createdAt` / `.updatedAt` are declared as
free-form objects in the OpenAPI spec but are ISO date-time strings on the
wire — the same bug class as `tickets.createdAt/updatedAt`,
`assets.installationDate` and `work_orders.dueDate`. Typed as timestamps.
`TicketCfaConversationActivityResponseDto.creator` is declared as an object
with no properties at all; it is JSON-serialized to a string rather than
guessed at as a struct.

## **Object Schema**

Schemas are **static**, taken from the response DTOs in the OpenAPI files
(e.g. `AssetCfaResponseDto` inside `https://api.remberg.de/openapi/assets.json`).
There is no schema-discovery API. Top-level fields per object
(`req` = marked required in the DTO):

### `assets` (`AssetCfaResponseDto`)
```
id: string (req)              assetNumber: string (req)
assetType: string (req)       assetCategory: string enum (req)
assetTypeId: string (req)     name: string
createdAt: date-time (req)    updatedAt: date-time (req)
location: {company, street, streetNumber, zipPostCode, city,
           countryProvince, country, other: string}
criticality: string enum (req)   status: string enum (req)
installationDate: <free-form object in OpenAPI — see TBD below>
```

### `work_orders` (`WorkOrderCfaResponseDto`)
```
id: string (req)              createdAt: date-time (req)
createdByType: string enum (req)   updatedAt: date-time (req)
counter: string (req)         subject: string (req)
parentWorkOrderId: string     relatedOrganizationId: string
externalReference: string     statusReference: string (req)
typeReference: string         dueDate: <free-form object — see TBD>
```

### `tickets` (`TicketCfaResponseDto`)
```
id: string (req)              subject: string (req)
createdAt / updatedAt: <typed as free-form objects in OpenAPI — doc bug,
                        actually ISO date-time strings; see TBD>
status: enum new|open|pendingInternal|pendingExternal|solved|closed|moved (req)
ticketID: string (req)        priority: enum 000_low|010_normal|020_high|030_critical (req)
assignedPersonId: string
assignedPerson: {id (req), firstName, lastName, email}
summary: string               solution: string
resolutionTime: number
relatedOrganizationIds: [string] (req)
relatedOrganizations: [{id, organizationName, organizationNumber}]
relatedContactIds: [string] (req)
relatedContacts: [{id, firstName, lastName, email}]
relatedAssetIds: [string] (req)
relatedAssets: [{id, assetNumber, assetType}]
relatedParts: [{partId: string, quantity: number}]
sourceType: string (req)      supportEmailAddress: string
categoryId: string
customPropertyValues: [{reference: string, value: <any>, associationValue: [any]}] (req)
```

### `work_requests` (`WorkRequest2PublicResponseDto`)
```
id: string (req)              counter: string (req)
status: enum 000_new|010_approved|020_declined|030_completed (req)
relatedAssetId: string (req)  assetStatus: string enum
description: string           declineReason: string
externalReference: string     relatedWorkOrderId: string
failureTypeIds: [string]      failureTypes: [{id, reference}]
createdAt / updatedAt: date-time (req)
approvedAt / completedAt: date-time
```

### `organizations` (`OrganizationCfaResponseDto`)
```
id: string (req)              createdAt / updatedAt: date-time (req)
name: string (req)            organizationNumber: string
phoneNumber: {number, countryPrefix}
email: string
shippingAddress: {company, street, streetNumber, zipPostCode, city,
                  countryProvince, country, other}
website: string               lang: string enum (req)   tz: string enum (req)
```

### `contacts` (`ContactCfaResponseDto`)
```
id: string (req)              firstName / lastName: string (req)
rembergUserEmail: string (req)   jobPosition: string
phoneNumber: {number, countryPrefix}
organizationNumber: string (req)
hourlyRates: [{value: number, validFrom: string}]
```
**No `createdAt`/`updatedAt` in the record** even though the list endpoint
accepts `updatedAtFrom`/`updatedAtUntil` filters.

### `users` (`UserPublicResponseDto`)
```
id: string (req)              email: string (req)
firstName / lastName / fullName: string
status: enum notInvited|invited|activated|suspended|permanentlyDisabled
```
No timestamps in the record.

### `forms` (`FormPublicListResponseDto` item)
```
id: string (req)              formTemplateId: string (req)
counter: number (req)         relatedWorkOrderId: string
name: string (req)            status: enum inProgress|finalized (req)
createdAt / updatedAt: date-time (req)   finalizedAt: date-time
```

### `asset_status_signals` (`AssetStatusSignalWithAssetInfoCfaResponseDto`)
```
id: string (req)              createdAt: date-time (req)
reportedAt: date-time (req)   resolvedAt: date-time
status: enum new|running|warning|stopped|inactive (req)
asset: {id, assetNumber, assetType} (req)   # AssetInfoCfaResponseDto
```

### `work_order_times` (`WorkOrderTimeEntryCfaResponseDto`)
```
performingPersonId: string (req)   startTime: date-time (req)
endTime: date-time (req)           durationInSeconds: number (req)
+ injected: workOrderId, workOrderUpdatedAt
```
The response root also carries `totalDurationInSeconds`; it is dropped
because it is exactly `SUM(durationInSeconds)` per work order.

### `work_order_stock_changes` (`WorkOrderCfaPartStockChangeResponseDto`)
```
id: string (req)              createdAt / updatedAt: date-time (req)
change: number (req)
action: enum created|added|taken|reserved|reservationCanceled|planned|plannedCanceled (req)
partType: {id, partNumber, externalReference, name}   # PartTypeInfoCfaResponseDto
storageAsset: {id, assetNumber, assetType}
+ injected: workOrderId, workOrderUpdatedAt
```

### `part_inventories` (`PartInventoryWithStorageCfaResponseDto`)
```
id: string (req)              partTypeId: string (req)
storageAsset: {id, assetNumber, assetType} (req)
availableStock: number (req)  totalStock: number (req)
createdAt: date-time (req)
+ injected: partTypeId (already on the wire), partUpdatedAt
```

### `part_stock_changes` (`PartStockChangeCfaResponseDto`)
```
id: string (req)              change: number (req)   comment: string
createdAt / updatedAt: <free-form objects in OpenAPI — doc bug, ISO strings>
action: enum created|added|taken|reserved|reservationCanceled|planned|plannedCanceled (req)
storageAsset: {id, assetNumber, assetType}
+ injected: partTypeId, partUpdatedAt
```

### `ticket_conversations` (`TicketCfaConversationActivityResponseDto`)
```
creator: <object with no declared properties — JSON-serialized to string> (req)
createdAt: string (req)       kind: enum note|inboundEmail|outboundEmail|portalMessage (req)
plainText: string (req)       subject: string
header: {from: string, to: [string], cc: [string], bcc: [string]}
+ injected: ticketId, ticketUpdatedAt
```

**TBD / OpenAPI doc bugs found during research:**
1. `tickets.createdAt` / `tickets.updatedAt` are declared as unconstrained
   objects in `tickets.json`; every other resource declares them
   `string, format: date-time`, and ticket examples show ISO strings. The
   connector types them as timestamps.
2. `assets.installationDate` and `work_orders.dueDate` are also declared as
   unconstrained objects; they are treated as ISO date-time strings
   (timestamps) pending live verification.
3. The list DTO `ContactsCfaFindManyResponseDto` under-specifies the
   `contacts` array items (bare `string`); the actual record shape is
   `ContactCfaResponseDto` (the get-by-id DTO), which is what the connector
   uses.

## **Get Object Primary Keys**

Static: every object's primary key is `id` (a Mongo-style hex string, e.g.
`"665af0a2c9e77c001df7…"`). Human-readable counters exist
(`tickets.ticketID`, `work_orders.counter`, `work_requests.counter`,
`assets.assetNumber`, `parts.partNumber`) but `id` is the stable key.

## **Object's ingestion type**

| Table | Ingestion type | Rationale |
|---|---|---|
| `assets` | `cdc` | server-side `updatedAtFrom/Until` filter + `updatedAt` in record |
| `work_orders` | `cdc` | same |
| `tickets` | `cdc` | same (timestamps mistyped in OpenAPI, see TBD) |
| `work_requests` | `cdc` | same |
| `organizations` | `cdc` | same |
| `parts` | `cdc` | same |
| `contacts` | `snapshot` | list filter exists but records carry **no** `updatedAt`, so no usable cursor field |
| `users` | `snapshot` | records carry no timestamps |
| `forms` | `snapshot` | no server-side `updatedAt` filter (only `finalizedAt*`); could later become cdc via `sortField=dateModified` descending scan |
| `asset_status_signals` | `cdc` | flat `/v2/assets/status-signals` with `createdAtFrom/Until`; cursor `createdAt` (no `updatedAt` bound — see caveat above) |
| `work_order_times` | `cdc` | fan-out; cursor is the injected `workOrderUpdatedAt`. PK is composite (`workOrderId`, `performingPersonId`, `startTime`) — entries have no `id` |
| `work_order_stock_changes` | `cdc` | fan-out; cursor `workOrderUpdatedAt`, PK `id` |
| `part_inventories` | `cdc` | fan-out; cursor `partUpdatedAt`, PK `id` |
| `part_stock_changes` | `cdc` | fan-out; cursor `partUpdatedAt`, PK `id` |
| `ticket_conversations` | `cdc` | fan-out; cursor `ticketUpdatedAt`. PK is composite (`ticketId`, `createdAt`, `kind`) — activities have no `id` |

No object exposes deleted records (no `deletedAt` flag, no deletions feed),
so `cdc_with_deletes` is not available; deletes in remberg do not propagate.

## **Read API for Data Retrieval**

All reads are plain `GET` list endpoints returning JSON.

### Pagination

Every list endpoint uses **page-number pagination**:

- `page` — 1-indexed page number
- `limit` — page size; default 20, **maximum 1000**

The last page is detected by receiving fewer than `limit` records (responses
do not consistently expose a total count across resources). Example:

```http
GET /v2/work-orders?page=3&limit=1000&updatedAtFrom=2026-01-01T00:00:00.000Z&updatedAtUntil=2026-07-01T00:00:00.000Z
Host: api.remberg.de
authorization: <API_KEY>
```

```json
{
  "data": [
    {
      "id": "665af0a2c9e77c001df70a11",
      "createdAt": "2026-02-03T09:12:44.000Z",
      "createdByType": "user",
      "updatedAt": "2026-03-01T10:00:00.000Z",
      "counter": "WO-1042",
      "subject": "Quarterly maintenance",
      "statusReference": "in_progress",
      "typeReference": "maintenance",
      "relatedOrganizationId": "6543aa…",
      "dueDate": "2026-03-15T00:00:00.000Z"
    }
  ]
}
```

### Incremental retrieval (the six `cdc` tables)

- Filters `updatedAtFrom` / `updatedAtUntil` (ISO-8601 UTC, **inclusive**
  bounds) restrict results by record `updatedAt`.
  (`/v1/users` alone names the upper bound `updatedAtTo` — irrelevant for the
  connector since `users` is a snapshot table.)
- Strategy: **bounded time-range scan with page continuation.** Each sync
  reads `[cursor − lookback, connector-init-time]` page by page; when a page
  returns fewer than `limit` records the range is drained and the cursor
  advances to the range's upper bound. Server-side `updatedAt` sort is not
  available on `work_orders`/`tickets`/`work_requests`, but draining the
  bounded range makes result order irrelevant.
- A record updated *while* the range is being paged moves out of the filtered
  set (its new `updatedAt` exceeds the range's upper bound), which can shift
  pagination; the read-time lookback re-captures such records on the next
  sync, and cdc upsert semantics make re-reads harmless.
- First sync: no `updatedAtFrom` lower bound (full backfill up to init time),
  unless the user supplies `start_timestamp`.

### Deleted records

Not retrievable — no endpoint or field exposes deletions.

### Rate limits (strict — shape the whole connector)

Two sliding-window limits enforced **simultaneously, per user AND per
endpoint** (`/v1/contacts` and `/v2/assets` have independent buckets):

| Throttler | Limit | Window |
|---|---|---|
| Burst | 10 requests | 1 s |
| Base | 25 requests | 5 s |

- Exceeding either → `429` with `Retry-After-Burst` or `Retry-After-Base`
  (seconds). **429 responses themselves count against the limit**, so blind
  retry storms never recover — the connector must honor the header.
- Official guidance: stay at ≤ 4–5 requests/second per endpoint. The
  connector spaces requests ≥ 0.25 s apart per endpoint and honors
  `Retry-After-*` with exponential backoff.
- These limits are why the connector is a standard (non-partitioned)
  `LakeflowConnect`: fanning out parallel readers against a
  25-requests-per-5-seconds budget only manufactures 429s.

## **Field Type Mapping**

| OpenAPI type | Spark type |
|---|---|
| `string` | `StringType` |
| `string, format: date-time` | `TimestampType` (ISO-8601 UTC strings on the wire) |
| `string` + `enum` | `StringType` (enums listed in Object Schema above) |
| `number` | `DoubleType` (counts/quantities may be integral but DTOs only say `number`) |
| `boolean` | `BooleanType` |
| nested object | `StructType` (never `MapType`) |
| array | `ArrayType` of the mapped element type |
| free-form `object` (`tickets.createdAt/updatedAt`, `assets.installationDate`, `work_orders.dueDate`) | `TimestampType` (doc-bug workaround, see TBDs) |
| free-form values (`tickets.customPropertyValues[].value` / `.associationValue[]`) | JSON-serialized `StringType` — custom-property values are user-defined and untyped |

- `id` fields are hex object-id strings → `StringType`.
- Absent optional fields are emitted as `None`, never omitted.
- Field names are kept exactly as the API returns them (camelCase) — no
  renaming, so records map 1:1 to the official API docs.

## **Sources and References**

| Source | URL | Confidence |
|---|---|---|
| Official developer portal (getting started, auth, rate limiting) | https://developers.remberg.de | Highest — official |
| Official OpenAPI 3.0 specs (one per resource) | https://api.remberg.de/openapi/{assets,work-orders,tickets,work-requests,organizations,contacts,users,forms,parts,procedures}.json | Highest — official, machine-generated (three DTO typing bugs found, documented in TBDs above) |
| remberg docs LLM export | https://developers.remberg.de/llms.txt | High — official |

No existing Airbyte / Singer / dlthub connector for remberg was found; this
documentation is derived entirely from the official sources above.

### Research log

- Fetched developer-portal pages: Getting Started (API keys under
  Settings > Data > API, 1-year expiry), Rate Limiting (dual sliding-window
  limits, per-user + per-endpoint, `Retry-After-*` semantics, 429s count),
  OpenAPI page (spec URLs).
- Downloaded and parsed all resource OpenAPI JSON files; extracted list
  endpoints, query parameters (`page`, `limit`, `updatedAtFrom/Until`,
  `sortField`/`sortDirection` where present), response envelope keys, and
  full record DTO schemas (saved during development as `schema_*.json`).
- Confirmed per-resource envelope inconsistency (`data` vs resource-named
  keys) and the `users` endpoint's `updatedAtTo` naming deviation.
- Found the three OpenAPI typing bugs listed under **Object Schema → TBD**;
  worked around them as described.
