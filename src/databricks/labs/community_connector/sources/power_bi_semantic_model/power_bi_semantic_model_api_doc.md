# **Power BI Semantic Model API Documentation**

> **Scope note**: "Semantic model" is the current Microsoft product name for what the
> Power BI REST API still calls a **dataset**. All endpoints below use the API's
> `dataset` / `datasets` terminology; the connector's user-facing naming should say
> "semantic model" per Microsoft's rebrand (see
> https://learn.microsoft.com/power-bi/connect-data/service-datasets-understand).

## **Authorization**

- **Chosen method**: OAuth 2.0 **Client Credentials** flow against Microsoft Entra ID
  (formerly Azure AD), using an Entra **service principal** (app registration + client
  secret or certificate). This is the standard non-interactive pattern for
  server-to-server / connector scenarios and is explicitly supported by nearly every
  Power BI REST endpoint ("This API call can be called by a service principal
  profile" / "authenticate using a service principal").
- **Token endpoint**:

  ```
  POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
  Content-Type: application/x-www-form-urlencoded

  grant_type=client_credentials
  &client_id={client_id}
  &client_secret={client_secret}
  &scope=https://analysis.windows.net/powerbi/api/.default
  ```

  - `scope` **must** be `https://analysis.windows.net/powerbi/api/.default` — this
    tells Entra ID to mint a token for the Power BI resource using whatever
    application permissions/API access is configured, without an interactive
    consent step.
  - Response contains `access_token`, `token_type` (`Bearer`), `expires_in` (seconds,
    typically 3600).
- **Auth placement**: every Power BI REST call sends the bearer token as a header:

  ```
  Authorization: Bearer <access_token>
  ```
- **Important service-principal-specific rule** (documented on nearly every Admin
  endpoint): when authenticating as a service principal, the Entra app registration
  **must not** have any admin-consent-required Power BI delegated/application
  permissions configured in the Azure Portal API permissions blade. Scopes/permissions
  for the service principal are instead granted entirely through the **Power BI
  Admin Portal**, not through Entra API permissions. Adding Entra-side Power BI
  permissions to a service-principal app can break calls in confusing ways.
- **Tenant-side enablement steps** (must be done once by a Fabric/Power BI admin,
  outside of this connector, documented here for completeness — not performed by the
  connector itself):
  1. Register an Entra ID application (the service principal) and create a client
     secret (or certificate).
  2. Create an Entra ID **security group** and add the service principal to it.
  3. In the Power BI Admin Portal → **Tenant settings → Developer settings →
     "Allow service principals to use Power BI APIs"**: enable and scope to that
     security group (recommended over tenant-wide enablement).
  4. For the Admin/scanner APIs (`admin/*`), also enable, under **Admin API
     settings**: **"Enhance admin APIs responses with detailed metadata"** and
     **"Enhance admin APIs responses with DAX and mashup expressions"** (a.k.a. the
     metadata-scanning tenant settings) — required for `datasetSchema` /
     `datasetExpressions` to return data on the scanner API.
  5. For `executeQueries`, also enable **"Dataset Execute Queries REST API"** under
     Integration settings.
  6. Add the service principal as a **Member** (or at least Viewer, for read-only
     per-workspace endpoints) of each workspace it needs to read via the
     non-admin (`/v1.0/myorg/groups/...`) endpoints. This step is **not** required
     for the Admin (`/v1.0/myorg/admin/...`) endpoints, which see the whole tenant.
- **Required scopes/permissions summary by endpoint** (documented per-endpoint below;
  these are **API-level required scopes returned by the API metadata**, not Entra
  API permissions — they are automatically satisfied once the service principal is
  enabled per the steps above):

  | Endpoint family | Required Scope (informational) | Notes |
  |---|---|---|
  | `GET /v1.0/myorg/groups` | `Workspace.Read.All` or `Workspace.ReadWrite.All` | Non-admin; workspace membership required |
  | `GET /v1.0/myorg/admin/groups` | `Tenant.Read.All` or `Tenant.ReadWrite.All` (delegated only; **must be absent** for service principal auth) | Admin; tenant-wide, no membership required |
  | `GET /v1.0/myorg/groups/{groupId}/datasets` | `Dataset.Read.All` or `Dataset.ReadWrite.All` | Non-admin; workspace membership required |
  | `GET /v1.0/myorg/admin/datasets` | `Tenant.Read.All` / `Tenant.ReadWrite.All` (delegated only) | Admin; tenant-wide |
  | `POST /v1.0/myorg/admin/workspaces/getInfo` + `scanStatus` + `scanResult` | `Tenant.Read.All` / `Tenant.ReadWrite.All` (delegated only) | Admin (scanner API); tenant-wide |
  | `GET /v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes` | `Dataset.Read.All` or `Dataset.ReadWrite.All` | Non-admin; caller needs **Write** on the dataset for full response |
  | `POST /v1.0/myorg/groups/{groupId}/datasets/{datasetId}/executeQueries` | `Dataset.Read.All` or `Dataset.ReadWrite.All` | Non-admin; requires the "Dataset Execute Queries REST API" tenant setting enabled |

- **Other supported methods (not used by this connector)**: Delegated (user) OAuth
  Authorization Code / device-code flow, and legacy ADAL user auth. The connector
  will **not** run interactive/user-facing OAuth flows; it stores `tenant_id`,
  `client_id`, and `client_secret` in configuration and exchanges them for an access
  token at runtime via the client-credentials flow above.

Example authenticated request (list workspaces via the admin API):

```bash
curl -X GET \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://api.powerbi.com/v1.0/myorg/admin/groups?%24top=100"
```

## **Base URL**

```
https://api.powerbi.com/v1.0/myorg
```

- Non-admin (workspace-membership-scoped) endpoints live directly under this base,
  e.g. `/groups`, `/groups/{groupId}/datasets`.
- Admin (tenant-wide) endpoints live under `/admin`, e.g. `/admin/groups`,
  `/admin/datasets`, `/admin/workspaces/getInfo`.
- Token issuance uses a **different** host: `https://login.microsoftonline.com`
  (see Authorization section).

## **Rate Limits**

Power BI throttles per-tenant and per-user/app. A `429 Too Many Requests` response
includes a `Retry-After` header (seconds to wait) — the connector should back off for
that duration and retry. Documented limits relevant to this connector's endpoints:

| Endpoint | Limit |
|---|---|
| `GET /admin/groups` (GetGroupsAsAdmin) | 50 requests/hour **or** 15 requests/minute per tenant; 30s server-side timeout |
| `GET /admin/datasets` (GetDatasetsAsAdmin) | 50 requests/hour **or** 5 requests/minute per tenant |
| `POST /admin/workspaces/getInfo` (PostWorkspaceInfo) | 500 requests/hour; max 16 simultaneous requests; max 100 workspace IDs per call |
| `GET /admin/workspaces/scanStatus/{scanId}` | 10,000 requests/hour |
| `GET /admin/workspaces/scanResult/{scanId}` | 500 requests/hour; result available for only 24 hours after scan completion |
| `GET /groups` (non-admin GetGroups) | Not separately documented; subject to general per-user throttling |
| `GET /groups/{groupId}/datasets` (non-admin) | Not separately documented; subject to general per-user throttling |
| `GET /groups/{groupId}/datasets/{datasetId}/refreshes` | Not separately documented; subject to general per-user throttling |
| `POST /groups/{groupId}/datasets/{datasetId}/executeQueries` | 120 query requests/minute per user, **regardless of dataset queried** |

General guidance: keep admin/scanner-API polling infrequent (the scan workflow is
inherently async — see below), stagger `executeQueries` calls across tables/datasets
to stay under 120/minute, and implement exponential backoff on `429`/`5xx`.

## **Object List**

The object list is **static** (defined by the connector); Power BI does have
list-type endpoints for workspaces and datasets, but the connector's fixed table set
maps each REST resource to one Spark table as follows:

| Object Name | Description | Primary Endpoint | Ingestion Type |
|---|---|---|---|
| `workspaces` | Workspaces ("groups") in the tenant — the container for semantic models | `GET /v1.0/myorg/admin/groups` (tenant-wide; falls back to `GET /v1.0/myorg/groups` for delegated/non-admin auth) | `snapshot` |
| `datasets` | Semantic models (datasets), one row per dataset, across all workspaces | `GET /v1.0/myorg/admin/datasets` (tenant-wide; falls back to per-workspace `GET /v1.0/myorg/groups/{groupId}/datasets`) | `snapshot` |
| `dataset_tables` | Tables defined inside each semantic model (from the metadata scanner) | `POST /v1.0/myorg/admin/workspaces/getInfo` → `GET .../scanStatus/{scanId}` → `GET .../scanResult/{scanId}` | `snapshot` |
| `dataset_columns` | Columns defined inside each semantic model table (from the metadata scanner) | same scanner workflow as above (nested under `datasets[].tables[].columns[]`) | `snapshot` |
| `dataset_measures` | DAX measures defined inside each semantic model table (from the metadata scanner) | same scanner workflow as above (nested under `datasets[].tables[].measures[]`) | `snapshot` |
| `dataset_relationships` | Relationships between tables within a semantic model (from the metadata scanner) | same scanner workflow as above (nested under `datasets[].relationships[]`) | `snapshot` |
| `dataset_refresh_history` | Refresh run history for a semantic model (scheduled + on-demand + API-triggered) | `GET /v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes` | `append` |

**Connector scope / how objects relate**:
- `workspaces` is the top-level container. A workspace has zero or more `datasets`.
- `datasets` rows are enumerated either tenant-wide via the Admin API (recommended;
  no workspace membership required) or, if the service principal is not enabled for
  Admin APIs, per-workspace via the non-admin API (requires the service principal to
  be a workspace Member for every workspace it should see).
- `dataset_tables`, `dataset_columns`, `dataset_measures`, and
  `dataset_relationships` are **not** separate REST endpoints — Power BI has no
  `GET .../tables` or `GET .../columns` endpoint. They are all extracted from the
  single **Admin metadata scan** ("Scanner API") response
  (`admin/workspaces/scanResult/{scanId}`), which returns, per workspace, a nested
  `datasets[]` array containing `tables[]`, each with `columns[]`, `measures[]`, and
  the dataset-level `relationships[]`. The connector flattens this nested JSON into
  four separate tabular objects, each carrying `workspace_id` and `dataset_id`
  (and `table_name` where applicable) as connector-derived foreign keys.
- `dataset_refresh_history` is scoped to (`workspaceId`, `datasetId`) and is fetched
  per dataset directly (no scanner step needed) via the standard non-admin refreshes
  endpoint.
- Executing DAX queries (`executeQueries`) against a dataset to pull actual **row
  data** (not just metadata) is documented below in **Read API for Data Retrieval**
  as an optional/advanced retrieval path — see "Known Quirks" for why it is not the
  default ingestion mechanism for `dataset_tables`.

## **Object Schema**

### `workspaces` object

**Source endpoint (recommended, tenant-wide)**:
`GET https://api.powerbi.com/v1.0/myorg/admin/groups?$top={n}&$expand=datasets`

**Fallback (delegated / membership-scoped)**:
`GET https://api.powerbi.com/v1.0/myorg/groups`

**Key behavior**:
- The Admin variant (`/admin/groups`) requires `$top` (mandatory, 1–5000) and
  supports `$skip` for paging beyond 5000, plus OData `$filter` (e.g.
  `state eq 'Active'` to exclude deleted/orphaned workspaces) and `$expand`
  (`users`, `reports`, `dashboards`, `datasets`, `dataflows`, `workbooks`) to inline
  related entities in the same call. The connector requests `$expand=datasets` so
  the `datasets` table can optionally be derived from this same call instead of a
  second round trip.
- No incremental cursor exists; workspaces are always fully re-listed (snapshot).

**High-level schema (connector view)**:

| Column Name | Type | Description |
|---|---|---|
| `id` | string (UUID) | Workspace ID. Primary key. |
| `name` | string | Workspace display name. |
| `description` | string or null | Workspace description (Admin API only). |
| `type` | string | `Workspace`, `PersonalGroup` (a.k.a. "My workspace"), `Personal`, `Group`, or `AdminWorkspace` (Admin API only). |
| `state` | string or null | `Active`, `Deleted`, `Removing`, etc. (Admin API only). |
| `isReadOnly` | boolean | Whether the workspace is read-only. |
| `isOnDedicatedCapacity` | boolean | Whether assigned to a Premium/Fabric capacity. |
| `capacityId` | string (UUID) or null | Capacity ID, present only when on dedicated capacity. |
| `defaultDatasetStorageFormat` | string or null | `Small` or `Large`; present only when `isOnDedicatedCapacity` is true. |
| `dataflowStorageId` | string (UUID) or null | Associated dataflow storage account ID. |
| `hasWorkspaceLevelSettings` | boolean or null | Admin API only. |
| `pipelineId` | string (UUID) or null | Deployment pipeline ID, if assigned (Admin API only). |

**Example response** (`GET /admin/groups?$filter=state eq 'Active'&$top=100`):

```json
{
  "value": [
    {
      "id": "e380d1d0-1fa6-460b-9a90-1a5c6b02414c",
      "isReadOnly": false,
      "isOnDedicatedCapacity": true,
      "capacityId": "0f084df7-c13d-451b-af5f-ed0c466403b2",
      "defaultDatasetStorageFormat": "Small",
      "name": "Sample Group 1",
      "description": "Sample group",
      "type": "Workspace",
      "state": "Active",
      "hasWorkspaceLevelSettings": true
    }
  ]
}
```

**Primary key**: `id`

### `datasets` object

**Source endpoint (recommended, tenant-wide)**:
`GET https://api.powerbi.com/v1.0/myorg/admin/datasets?$top={n}&$skip={n}`

**Fallback (delegated / membership-scoped, per workspace)**:
`GET https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets`

**Key behavior**:
- The Admin variant returns datasets across the whole tenant in one paged call
  (`workspaceId` is included on each row so it can be joined back to `workspaces`);
  the non-admin variant must be called once per workspace ID enumerated from
  `workspaces`, and does **not** return `workspaceId` on each row (the connector
  must add it from the calling context).
- Caller must have **write** access on a dataset for the non-admin call to return
  the full field set; read-only access returns a much smaller subset (`id`, `name`
  only, per the official example).
- No incremental cursor; always a full snapshot re-list.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|---|---|---|
| `id` | string | Dataset (semantic model) ID. Primary key. |
| `name` | string | Dataset display name. |
| `workspaceId` | string (UUID) | Owning workspace ID (present natively on Admin API rows; connector-derived on non-admin rows). |
| `description` | string or null | Dataset description. |
| `configuredBy` | string | Owner's UPN/email. |
| `isRefreshable` | boolean | True if recently refreshed or scheduled for auto-refresh in Import mode. |
| `isEffectiveIdentityRequired` | boolean or null | Whether an effective identity must be sent for embed tokens. |
| `isEffectiveIdentityRolesRequired` | boolean or null | Whether RLS roles are defined in the model. |
| `isOnPremGatewayRequired` | boolean or null | Whether an on-premises gateway is required. |
| `isInPlaceSharingEnabled` | boolean or null | Whether the dataset can be shared to external tenants. |
| `addRowsAPIEnabled` | boolean or null | Whether push-dataset row-add API is enabled. |
| `targetStorageMode` | string or null | e.g. `Abf` (Premium/Fabric storage) or standard. |
| `ContentProviderType` | string or null | e.g. `PbixInImportMode`, `InDirectQueryMode`, `Excel`, `CSV`. |
| `createdDate` | string (ISO 8601) or null | Creation timestamp. |
| `webUrl` | string or null | Web URL of the dataset. |
| `qnaEmbedURL` | string or null | Q&A embed URL. |
| `upstreamDataflows` | array of struct or null | `{groupId, targetDataflowId}` — dataflows this dataset depends on. |
| `queryScaleOutSettings` | struct or null | `{autoSyncReadOnlyReplicas, maxReadOnlyReplicas}`. |
| `encryption` | struct or null | `{encryptionStatus}` — only when `$expand` used upstream. |

**Example response** (`GET /admin/datasets`):

```json
{
  "value": [
    {
      "id": "cfafbeb1-8037-4d0c-896e-a46fb27ff229",
      "name": "SalesMarketing",
      "addRowsAPIEnabled": false,
      "configuredBy": "john@contoso.com",
      "isRefreshable": true,
      "isEffectiveIdentityRequired": false,
      "isEffectiveIdentityRolesRequired": false,
      "isOnPremGatewayRequired": false,
      "isInPlaceSharingEnabled": false,
      "workspaceId": "5c968528-70b6-4588-809f-ce811ffa5c23"
    }
  ]
}
```

**Primary key**: `id` (unique per tenant; join key to `workspaces.id` is `workspaceId`)

### `dataset_tables`, `dataset_columns`, `dataset_measures`, `dataset_relationships` (Metadata Scanner)

**Source endpoint** — a 3-call async workflow (the "Scanner API"):

1. **Trigger scan**:
   ```
   POST https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True
   Body: { "workspaces": ["<workspaceId1>", "<workspaceId2>", ...] }   // 1-100 IDs
   ```
   Returns `202 Accepted` with `{ id: scanId, createdDateTime, status: "NotStarted" }`.
2. **Poll status**:
   ```
   GET https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scanId}
   ```
   Poll until `status` is `"Succeeded"` (also watch for `"Failed"`). Recommended
   poll interval: a few seconds, with backoff, given the 10,000/hour limit is very
   generous but the scan itself can take from seconds to minutes depending on
   workspace/dataset count and complexity.
3. **Fetch result** (only after step 2 succeeds; available for 24 hours):
   ```
   GET https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scanId}
   ```

**Key behavior / prerequisites**:
- `datasetSchema=true` is required to get `tables`/`columns`/`measures`;
  `datasetExpressions=true` additionally returns DAX/M expressions
  (`measures[].expression`, `expressions[]`/M mashup queries,
  `tables[].source[].expression`). **Both require the tenant-level metadata-scanning
  settings to be enabled** (see Authorization section) — otherwise the scan
  succeeds but returns empty/absent schema data, and the dataset row will show
  `schemaMayNotBeUpToDate: true` or `schemaRetrievalError`.
- Only workspaces the caller (service principal) has been granted admin-scope
  visibility into are scanned; because this uses the Admin API, tenant-wide access
  is available without per-workspace membership once the service principal is
  enabled for Admin APIs.
- Max 100 workspace IDs per scan request; for large tenants the connector must
  batch `workspaces` (as enumerated from the `workspaces` table) into groups of
  ≤100 and issue multiple scan requests (respecting the 16-simultaneous-request and
  500/hour caps).
- No incremental cursor for schema; each connector run re-scans and re-derives a
  full snapshot of tables/columns/measures/relationships.

**Nested response shape** (relevant subset, per workspace → per dataset):

```json
{
  "workspaces": [
    {
      "id": "d507422c-8d6d-4361-ac7a-30074a8cd0a1",
      "name": "V2 shared",
      "datasets": [
        {
          "id": "e7e8a355-e77b-4418-a7b8-ae5972fdaa03",
          "name": "ExportB",
          "configuredBy": "john@contoso.com",
          "targetStorageMode": "Abf",
          "tables": [
            {
              "name": "DW_Revenues",
              "isHidden": false,
              "description": "My table",
              "columns": [
                { "name": "RowNumber", "dataType": "Int64", "isHidden": true }
              ],
              "measures": [
                {
                  "name": "MyMeasure",
                  "expression": "CALCULATE(SELECTEDVALUE('DW_Revenues'[Numbers])*10)",
                  "description": "My measure",
                  "isHidden": false
                }
              ],
              "source": [
                { "expression": "let\n  Source = Revenues\nin\n  Source" }
              ]
            }
          ],
          "relationships": []
        }
      ]
    }
  ]
}
```

**`dataset_tables` schema (connector view)**:

| Column Name | Type | Description |
|---|---|---|
| `workspace_id` | string (UUID, connector-derived) | Owning workspace ID. |
| `dataset_id` | string (connector-derived) | Owning dataset ID. |
| `name` | string | Table name. Composite key with `dataset_id`. |
| `isHidden` | boolean or null | Whether the table is hidden in the model. |
| `description` | string or null | Table description. |
| `source` | array of struct or null | `{expression}` — Power Query (M) source expression(s) for the table (only if `datasetExpressions=true`). |

**`dataset_columns` schema (connector view)**:

| Column Name | Type | Description |
|---|---|---|
| `workspace_id` | string (connector-derived) | Owning workspace ID. |
| `dataset_id` | string (connector-derived) | Owning dataset ID. |
| `table_name` | string (connector-derived) | Owning table name. |
| `name` | string | Column name. |
| `dataType` | string | Model data type, e.g. `Int64`, `String`, `Double`, `DateTime`, `Boolean`, `Decimal`. |
| `dataCategory` | string or null | Data category annotation (e.g. `WebUrl`, `Image`). |
| `formatString` | string or null | Display format string. |
| `isHidden` | boolean or null | Whether hidden in the model. Defaults to `false`. |
| `sortByColumn` | string or null | Name of the column used to sort this column. |
| `summarizeBy` | string or null | Default aggregation, e.g. `Sum`, `Count`, `None`. |

**`dataset_measures` schema (connector view)**:

| Column Name | Type | Description |
|---|---|---|
| `workspace_id` | string (connector-derived) | Owning workspace ID. |
| `dataset_id` | string (connector-derived) | Owning dataset ID. |
| `table_name` | string (connector-derived) | Table the measure is defined on. |
| `name` | string | Measure name. |
| `expression` | string | The DAX expression (only populated if `datasetExpressions=true`). |
| `description` | string or null | Measure description. |
| `formatString` | string or null | Display format string. |
| `isHidden` | boolean or null | Whether hidden in the model. |

**`dataset_relationships` schema (connector view)**:

> The Microsoft Learn schema reference documents `relationships` as an array on the
> dataset object, but (as of this research) does not publish a per-field table for
> the relationship object shape the way it does for `Table`/`Column`/`Measure`.
> `TBD:` exact field names for relationship cardinality/cross-filter-direction were
> not found in the official reference; empirically (community write-ups) the object
> commonly includes `name`, `fromTable`, `fromColumn`, `toTable`, `toColumn`,
> `crossFilteringBehavior`, and `isActive`. **Verify against a live scan result
> during implementation** and adjust column list accordingly — do not hard-code
> the community-sourced field names below without confirming against a real
> response.

| Column Name | Type | Description |
|---|---|---|
| `workspace_id` | string (connector-derived) | Owning workspace ID. |
| `dataset_id` | string (connector-derived) | Owning dataset ID. |
| `name` | string or null | Relationship name/ID, if present. |
| `fromTable` | string | `TBD:` unconfirmed field name — table on the "many"/from side. |
| `fromColumn` | string | `TBD:` unconfirmed field name — column on the "many"/from side. |
| `toTable` | string | `TBD:` unconfirmed field name — table on the "one"/to side. |
| `toColumn` | string | `TBD:` unconfirmed field name — column on the "one"/to side. |
| `isActive` | boolean or null | `TBD:` unconfirmed — whether the relationship is active. |
| `crossFilteringBehavior` | string or null | `TBD:` unconfirmed — e.g. `OneDirection`, `BothDirections`. |

**Primary keys**:
- `dataset_tables`: (`dataset_id`, `name`)
- `dataset_columns`: (`dataset_id`, `table_name`, `name`)
- `dataset_measures`: (`dataset_id`, `table_name`, `name`)
- `dataset_relationships`: no natural key confirmed; connector should synthesize one,
  e.g. hash of (`dataset_id`, `fromTable`, `fromColumn`, `toTable`, `toColumn`), until
  the real field names are verified.

### `dataset_refresh_history` object

**Source endpoint**:
`GET https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes?$top={n}`

**Key behavior**:
- Returns most-recent-first; `$top` defaults to the last 60 entries if omitted.
- OneDrive-triggered refreshes are excluded from the response.
- Caller needs **write** access on the dataset (service principal as workspace
  Member or Admin satisfies this).
- Naturally **append-only**: each refresh run gets a new `requestId`; there is no
  update-in-place once a refresh entry is created and its terminal `status` is set
  (a row may transiently show `status: "Unknown"` while in progress, then later
  reappear — near the head of the list — as `Completed`/`Failed`/`Disabled`; the
  connector should treat `requestId` as the identity and re-fetch recent history each
  run to catch these status transitions, since strict append-only replication would
  miss the terminal-status update for runs that were "Unknown" on a prior read).

**High-level schema (connector view)**:

| Column Name | Type | Description |
|---|---|---|
| `workspace_id` | string (connector-derived) | Owning workspace ID. |
| `dataset_id` | string (connector-derived) | Owning dataset ID. |
| `requestId` | string (UUID) | Refresh request identifier. Primary key (composite with `dataset_id`). |
| `refreshType` | string | `Scheduled`, `OnDemand`, `ViaApi`, `ViaXmlaEndpoint`, `ViaEnhancedApi`, `OnDemandTraining`. |
| `status` | string | `Unknown` (in progress), `Completed`, `Failed`, `Disabled`. |
| `startTime` | string (ISO 8601 datetime) | Refresh start (UTC). |
| `endTime` | string (ISO 8601 datetime) or null | Refresh end (UTC); empty while in progress. |
| `serviceExceptionJson` | string or null | JSON-encoded error code/details if `status = Failed`. |
| `refreshAttempts` | array of struct or null | `{attemptId, startTime, endTime, type: Data\|Query, serviceExceptionJson, executionMetrics}` — Power BI's automatic retry attempts within one refresh request. |

**Example response** (completed refresh):

```json
{
  "value": [
    {
      "refreshType": "ViaApi",
      "startTime": "2017-06-13T09:25:43.153Z",
      "endTime": "2017-06-13T09:31:43.153Z",
      "status": "Completed",
      "requestId": "9399bb89-25d1-44f8-8576-136d7e9014b1",
      "refreshAttempts": [
        {"attemptId": 1, "startTime": "2017-06-13T09:25:43.153Z", "endTime": "2017-06-13T09:31:40.153Z", "type": "Data"},
        {"attemptId": 1, "startTime": "2017-06-13T09:31:40.156Z", "endTime": "2017-06-13T09:31:43.153Z", "type": "Query"}
      ]
    }
  ]
}
```

**Primary key**: (`dataset_id`, `requestId`)

**Table options**:
- `top` (integer, optional): number of most-recent refresh entries to request per
  call. Default 60 if omitted.

## **Get Object Primary Keys**

Primary keys are **static** (not separately retrievable via API) and are documented
per-object above. Summary:

| Object | Primary Key |
|---|---|
| `workspaces` | `id` |
| `datasets` | `id` |
| `dataset_tables` | (`dataset_id`, `name`) |
| `dataset_columns` | (`dataset_id`, `table_name`, `name`) |
| `dataset_measures` | (`dataset_id`, `table_name`, `name`) |
| `dataset_relationships` | synthesized (see Known Quirks — unconfirmed native key) |
| `dataset_refresh_history` | (`dataset_id`, `requestId`) |

## **Object's ingestion type**

| Object | Ingestion Type | Rationale |
|---|---|---|
| `workspaces` | `snapshot` | No list-changes/delta API; workspaces can be renamed, deleted (soft), or restored — full re-list needed to catch all state transitions. |
| `datasets` | `snapshot` | No cursor/delta field on the list endpoints; datasets can be created, renamed, or deleted between runs. |
| `dataset_tables` | `snapshot` | Derived from a full metadata re-scan each run; no per-table change tracking exists in the API. |
| `dataset_columns` | `snapshot` | Same as above — full re-scan. |
| `dataset_measures` | `snapshot` | Same as above — full re-scan. |
| `dataset_relationships` | `snapshot` | Same as above — full re-scan. |
| `dataset_refresh_history` | `append` | Each refresh run is an immutable, timestamped, uniquely-identified (`requestId`) event; new runs accumulate over time. Note: a small lookback window (e.g. re-fetch last N entries via `$top`) is recommended to capture terminal-status updates for runs that were `Unknown` (in-progress) on a previous read — see Known Quirks. |

No object in this connector currently supports `cdc` or `cdc_with_deletes`: none of
the list endpoints expose a `lastModifiedDateTime`/delta-link mechanism the way
Microsoft Graph does. Deleted workspaces/datasets are only detectable via
snapshot-diffing on the consumer side (the Admin `groups` endpoint does support
`$filter=state eq 'Deleted'` to explicitly surface soft-deleted workspaces if needed
for delete-detection, but this is not wired up as a distinct ingestion mode here).

## **Read API for Data Retrieval**

This connector's tables are all **metadata about semantic models** (workspaces,
dataset definitions, schema, refresh runs), not the semantic model's underlying data
rows. Retrieval patterns:

1. **`workspaces` / `datasets`** — simple paginated `GET` list calls (OData
   `$top`/`$skip`, or, for the Admin API, mandatory `$top` with `$skip` to page past
   5000). No request body. Full snapshot each run.
2. **`dataset_tables` / `dataset_columns` / `dataset_measures` /
   `dataset_relationships`** — the 3-step async **Scanner API** workflow
   (`POST getInfo` → poll `GET scanStatus` → `GET scanResult`), batched over ≤100
   workspace IDs per call. This is the only way to retrieve table/column/measure/
   relationship-level metadata; there is no synchronous equivalent.
3. **`dataset_refresh_history`** — simple `GET .../refreshes?$top=n` per dataset;
   iterate over all `(workspaceId, datasetId)` pairs discovered from `datasets`.

**Pagination**:
- Non-admin list endpoints (`/groups`, `/groups/{id}/datasets`) do not document
  OData paging in practice (workspace/dataset counts per call are typically small);
  `$top`/`$skip`/`$filter` are accepted per OData conventions if needed.
- Admin list endpoints (`/admin/groups`, `/admin/datasets`) require explicit
  `$top` (max 5000) and use `$skip` to page beyond that — the connector must loop
  incrementing `$skip` by the page size until a page returns fewer than `$top` rows.
- The scanner API has no pagination — a single `scanResult` response contains all
  requested workspaces' full metadata tree.

**Deleted records**: Not directly exposed as a delta/tombstone feed for any object
in this connector. For `workspaces`, the Admin API's `$filter=state eq 'Deleted'`
can be used to explicitly list soft-deleted workspaces if delete-detection is
required in a future iteration; for `datasets`/schema objects, delete detection
must be done by diffing successive full snapshots.

**Rate limits**: see the Rate Limits table above. In particular, batch scanner
requests to ≤16 simultaneous / ≤500 per hour, and paginate Admin list calls
respecting the 50/hour-or-15/minute (`groups`) and 50/hour-or-5/minute (`datasets`)
ceilings — these are tight enough that, for large tenants, the connector should
cache workspace/dataset lists within a run rather than re-querying them per table.

### Optional / advanced: reading actual row data via `executeQueries`

Beyond metadata, the Power BI REST API also allows executing an arbitrary
**DAX query** against a semantic model to retrieve actual data rows:

```
POST https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/executeQueries
Body:
{
  "queries": [ { "query": "EVALUATE VALUES(MyTable)" } ],
  "serializerSettings": { "includeNulls": true },
  "impersonatedUserName": "someuser@mycompany.com"
}
```

- Requires the tenant setting **"Dataset Execute Queries REST API"** to be enabled.
- **One query, one table result, per API call** — DAX queries requesting multiple
  tables or exceeding row/value/size limits are truncated with a warning in the
  response, not a hard error, so response size must still be checked.
- Hard limits: **100,000 rows** OR **1,000,000 total values** per query (whichever
  hits first), and **15 MB** of response data (current row completes, no further
  rows written past that).
- **120 requests per minute per user**, tenant-wide (not per-dataset) — this is a
  much tighter budget than the metadata endpoints and must be pooled across however
  many datasets/tables the connector might pull this way in a given run.
- Not supported for datasets hosted in/live-connected to Azure Analysis Services;
  not supported for RLS-enabled or SSO-enabled datasets when authenticating as a
  service principal (falls back to the identity's own permissions in the latter
  case, per the API's RLS limitations).
- Only DAX `EVALUATE` queries are supported — no MDX, `INFO` DMV functions, or raw
  DMV queries.

**How this connector uses it**: implemented as the **opt-in, user-specified**
`dax_query_result` table. The user supplies `dax_query` plus the `workspace_id` /
`dataset_id` to run it against, and the connector emits that query's rows. It is
deliberately *not* fanned out over discovered datasets/tables:
- The DAX has to be hand-written against a specific model's tables and measures, so
  it can't be derived from the metadata-driven table list the way
  `dataset_tables`/`dataset_columns` are.
- The 120 req/min tenant-wide budget is far more restrictive than the metadata
  endpoints; one configured query costs exactly one request per micro-batch, and
  stacking many DAX tables into one pipeline is the user's budget to manage.
- Result columns vary per query, so the table's Spark schema is resolved from
  config: a `dax_columns` option declares typed columns, and without it the row's
  columns land in a `map<string,string>` alongside the raw `row_json`.

## **Field Type Mapping**

| Power BI / REST API Type | Spark / Standard Type | Notes |
|---|---|---|
| `string` | `string` | Includes UUID-formatted IDs (kept as strings, not native UUID type). |
| `string (uuid)` | `string` | IDs, capacity IDs, scan IDs, etc. |
| `boolean` | `boolean` | |
| `integer` / `integer (int32)` | `int` / `long` | Use `long` for IDs or counts that could exceed 32-bit range (none currently observed, but `$top`/`$skip` params are `int32`). |
| `string (date-time)` | `timestamp` | ISO 8601 UTC strings, e.g. `2017-06-13T09:25:43.153Z`. Parse as UTC. |
| Nested JSON object (e.g. `Encryption`, `queryScaleOutSettings`, `refreshAttempts[]`) | `struct` / `array<struct>` | Modeled as nested structs rather than flattened, consistent with other connectors in this repo (see `azure_devops` for precedent). |
| Dataset column `dataType` (model-level enum: `Int64`, `String`, `Double`, `Decimal`, `DateTime`, `Boolean`, etc.) | `string` (pass-through) | This is the **semantic model's own** column type label (Tabular/AS engine type name), not a Spark type — stored as-is in `dataset_columns.dataType`; the connector does not attempt to map it to a Spark `DataType` since these tables describe metadata about a model, not literal rows conforming to that model's schema. |
| DAX `expression` (measures), M `expression` (table source / expressions) | `string` | Raw DAX/Power Query text, only populated when `datasetExpressions=true` and tenant metadata-scanning settings are enabled; otherwise `null`/absent. |

## Known Quirks

- **Two parallel API surfaces (Admin vs. non-Admin)**: almost every object in this
  connector has both an Admin (`/admin/...`, tenant-wide, no workspace membership
  needed) and non-Admin (`/groups/...`, workspace-membership-scoped) form. This
  connector defaults to the Admin form for `workspaces` and `datasets` because it
  avoids having to individually add the service principal to every workspace, but
  requires the tenant admin to enable "Allow service principals to use Power BI
  APIs" — document this as a setup prerequisite, not something the connector can
  self-serve.
- **Async scanner workflow**: `dataset_tables`/`dataset_columns`/
  `dataset_measures`/`dataset_relationships` require a 3-call poll loop
  (`getInfo` → `scanStatus` → `scanResult`) rather than a single synchronous GET.
  The connector must implement polling with backoff and a reasonable timeout (the
  scan can take from seconds up to a few minutes for large workspaces).
- **`relationships` field shape unconfirmed**: the official Microsoft Learn schema
  reference lists `relationships: []` on the dataset object but — unlike `Table`,
  `Column`, and `Measure` — does not publish a `Relationship` object definition with
  field names. Implementers **must** trigger a real scan against a test tenant
  during implementation to confirm the actual field names before finalizing the
  `dataset_relationships` schema; the field names in this doc are a best-effort
  placeholder based on common community write-ups, not verified against the
  official reference.
- **Metadata-scanning tenant settings must be explicitly enabled** or
  `datasetSchema=true`/`datasetExpressions=true` silently return no schema data
  (with `schemaMayNotBeUpToDate`/`schemaRetrievalError` flags on the dataset row) —
  this is a very common setup gap and should be called out prominently in
  connector setup docs / troubleshooting.
- **`executeQueries` is opt-in, never automatic** — it backs the `dax_query_result`
  table only when the pipeline supplies a `dax_query`; see the Read API section
  above. Its result schema is user-config-dependent, and its truncation is silent
  (a warning in the response body, not an error), so responses must be size-checked
  rather than trusted.
- **Refresh history "in-progress" rows need re-fetching**: a refresh entry can be
  read once with `status: "Unknown"` (still running) and only later, on a
  subsequent poll, show its terminal `Completed`/`Failed` status against the same
  `requestId` — pure append-only replication without periodic re-fetch of the most
  recent window would miss this transition.
- **"Semantic model" vs. "dataset" naming**: Microsoft has been rebranding "dataset"
  to "semantic model" in product UI and newer docs, but the REST API, its object
  names (`Dataset`, `datasets`), and URL paths (`/datasets`) still use the legacy
  term throughout. This doc and the connector's internal table/column names should
  keep using `dataset`/`datasets` to match the API precisely, while
  user/source-facing naming (e.g. the connector's display name) can say "semantic
  model."

## Deferred Tables

The following Power BI entities are supported by the REST API and commonly exposed
by BI/reporting-focused integrations, but are **out of scope for this initial batch**
because the task explicitly scoped research to workspaces, datasets, dataset
metadata (scanner), and refresh history, and because they represent a materially
different API shape (reports/dashboards are separate artifact types with their own
Admin endpoints; activity events are a time-windowed audit-log API with a very
different pagination/continuation-token model):

| Deferred object | Why deferred | Candidate endpoint |
|---|---|---|
| `reports` | Different artifact type (not part of a semantic model itself); would need its own Admin list endpoint and schema. | `GET /v1.0/myorg/admin/reports` (GetReportsAsAdmin) |
| `dashboards` | Same rationale as `reports`; also nests `tiles` as a sub-object. | `GET /v1.0/myorg/admin/dashboards` (GetDashboardsAsAdmin) |
| `dataflows` | Distinct ETL artifact type with its own dependency graph (`upstreamDataflows`); appears in the scanner response but as a sibling artifact, not part of dataset schema. | `GET /v1.0/myorg/admin/dataflows` (GetDataflowsAsAdmin) |
| `datasources` (data source instances / gateway bindings) | Different shape (connection details, not tabular schema); already partially surfaced as `datasourceInstances`/`misconfiguredDatasourceInstances` in scanner output if `datasourceDetails=true`, but not modeled as its own table here. | `GET /v1.0/myorg/admin/datasets/{datasetId}/datasources` or scanner `datasourceDetails=true` |
| `activity_events` (audit log) | Fundamentally different API shape: time-windowed (max 24h span per call, ≤28 days of history), continuation-token pagination rather than OData paging, separate 200 req/hour limit. Valuable for governance use cases but orthogonal to "semantic model metadata." | `GET /v1.0/myorg/admin/activityevents` (GetActivityEvents) |
| `dataset_users` / `workspace_users` (access-control lists) | Per-object user-permission endpoints (`DatasetUser`, `GroupUser`) exist and are even inlined via `$expand=users` on some list calls, but the API docs themselves flag these `users[]` fields as being removed from list responses in favor of dedicated `Get*UsersAsAdmin` calls — worth a dedicated table only if access-governance becomes a connector goal. | `GET /v1.0/myorg/admin/groups/{groupId}/users` (GetGroupUsersAsAdmin), `GET /v1.0/myorg/admin/datasets/{datasetId}/users` (GetDatasetUsersAsAdmin) |
| `datamarts` | Newer artifact type (SQL/Lakehouse-backed) surfaced in scanner output; niche relative to core semantic-model metadata. | Scanner output `datasets[].datamarts[]`; no confirmed standalone list endpoint found in this research pass. |

## Sources and References

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/ | 2026-08-16 | High | API landing page, overall structure |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/groups/get-groups | 2026-08-16 | High | Non-admin `GetGroups` endpoint, params, schema, example response |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/admin/groups-get-groups-as-admin | 2026-08-16 | High | Admin `GetGroupsAsAdmin` endpoint, required scope, rate limit (50/hr or 15/min), `$expand` options, full `AdminGroup`/`AdminDataset` schema |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-datasets-in-group | 2026-08-16 | High | Non-admin `GetDatasetsInGroup` endpoint, scope, read-vs-write response truncation, `Dataset` schema |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/admin/datasets-get-datasets-as-admin | 2026-08-16 | High | Admin `GetDatasetsAsAdmin` endpoint, rate limit (50/hr or 5/min), `workspaceId` field, `AdminDataset` schema |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info | 2026-08-16 | High | Scanner trigger endpoint, query params (`lineage`, `datasourceDetails`, `datasetSchema`, `datasetExpressions`, `getArtifactUsers`), rate limit (500/hr, 16 concurrent), request/response shape |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-status | 2026-08-16 | High | Scan status polling endpoint, rate limit (10,000/hr), response shape |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result | 2026-08-16 | High | Scan result endpoint, 24h availability window, rate limit (500/hr), full nested `Table`/`Column`/`Measure`/`Expression`/`Role` schema and example JSON |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group | 2026-08-16 | High | Refresh history endpoint, `$top` default (60), `Refresh`/`RefreshAttempt`/`RefreshType` schema, completed/failed/in-progress examples |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group | 2026-08-16 | High | `executeQueries` endpoint, DAX-only limitation, row/value/size limits, 120 req/min limit, request/response schema |
| Community/Technical | https://community.fabric.microsoft.com/t5/Data-Engineering/Power-BI-API-using-Service-Principal/td-p/4245591 | 2026-08-16 | Medium | Confirmed `scope=https://analysis.windows.net/powerbi/api/.default` for service-principal token acquisition |
| Community/Technical | https://blog.atwork.at/post/2024/grant-permissions-to-powerbi-rest-api/ | 2026-08-16 | Medium | Cross-check on Admin API permission model |
| Community/Technical | https://medium.com/@kinsun_67689/creating-an-azure-app-or-service-principal-to-call-power-bi-rest-api-c8d604c00720 | 2026-08-16 | Medium | Service principal setup flow: app registration → security group → tenant setting → workspace access |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/power-bi/admin/get-activity-events | 2026-08-16 | High | Activity/audit log endpoint shape (used to justify deferring `activity_events`), rate limit (200/hr), 24h/28-day windowing |
| Reference implementation (attempted) | Fivetran Microsoft Power BI connector docs (`fivetran.com/docs/connectors/applications/microsoft-power-bi`) | 2026-08-16 | Low | Confirmed Fivetran has a Power BI connector using tenant/client credentials, but the JS-rendered schema/ERD page did not yield a readable table list through automated fetch — not used to determine the table list; Microsoft's own documented endpoints and object model were used instead. |
| Reference implementation (attempted) | Airbyte source-powerbi | 2026-08-16 | Low | No dedicated Airbyte **source** connector for Power BI was found (Airbyte's Power BI integrations found are Power BI as a **destination**, not a data source) — could not be used to cross-reference the table list. |
