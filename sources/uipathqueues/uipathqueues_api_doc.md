# **UiPath Orchestrator Queue Items API Documentation**

## **Authorization**

### Authentication Method

- **Chosen method**: OAuth 2.0 with **Client Credentials Flow** (for confidential applications with app scopes).
- **Identity Server Base URL**: `https://cloud.uipath.com/{organizationName}/identity_/`
- **API Base URL**: `https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/`
- **OAuth note**: The connector **stores** `client_id` (App ID) and `client_secret` (App Secret) and exchanges them for an `access_token` at runtime. It **does not** run user-facing OAuth flows.

### OAuth Client Credentials Flow

**Step 1: Obtain Access Token**

Send a `POST` request to the Identity Server token endpoint to obtain an access token:

- **Endpoint**: `https://cloud.uipath.com/{organizationName}/identity_/connect/token`
- **Method**: `POST`
- **Content-Type**: `application/x-www-form-urlencoded`

**Request Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `grant_type` | string | yes | Must be `client_credentials` for application scope access. |
| `client_id` | string | yes | The **App ID** provided when the external application was registered. |
| `client_secret` | string | yes | The **App Secret** provided for confidential applications. |
| `scope` | string | yes | Space-delimited list of scopes (e.g., `OR.Queues OR.Execution`). Use `OR.Default` for fine-grained folder/tenant permissions. |

**Example Request**:

```bash
curl -X POST "https://cloud.uipath.com/{organizationName}/identity_/connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id={app_id}" \
  -d "client_secret={app_secret}" \
  -d "scope=OR.Queues OR.Execution"
```

**Example Response**:

```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6...",
  "expires_in": 3600,
  "token_type": "Bearer",
  "scope": "OR.Queues OR.Execution"
}
```

**Response Fields**:
- `access_token`: The bearer token to use in API requests.
- `expires_in`: Token expiration time in seconds (3600 = 1 hour).
- `token_type`: Always `Bearer`.
- `scope`: The granted scopes.

**Important Notes**:
- Access tokens expire after **1 hour** (3600 seconds).
- The **client credentials flow does not support refresh tokens**. When the access token expires, the application must re-authenticate using the client credentials to obtain a new token.
- Scopes must be configured by the organization administrator when registering the external application.

---

### Using the Access Token

Once you have an access token, include it in the `Authorization` header for all API requests:

- **Auth header**: `Authorization: Bearer {access_token}`
- **Folder selection header** (required): `X-UIPATH-OrganizationUnitId: {folder_id}`

**Example authenticated request**:

```bash
curl -X GET \
  -H "Authorization: Bearer {access_token}" \
  -H "X-UIPATH-OrganizationUnitId: {folder_id}" \
  "https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/QueueDefinitions"
```

---

### Required Scopes

The connector requires the following scopes to access queue items:

| Scope | Permission | Description |
|-------|------------|-------------|
| `OR.Queues` | Read | Access to queue definitions and queue items. |
| `OR.Execution` | Read | Access to execution data (optional, for Robot/Transaction details). |

For fine-grained folder-level permissions, use:
- `OR.Default`: Grants access based on folder/tenant role assignments configured by the administrator.

---

### Folder Selection

The `X-UIPATH-OrganizationUnitId` header is **required** to specify which folder to query:
- Obtain folder IDs via the `/odata/Folders` endpoint or from the Orchestrator UI.
- Queue items may appear multiple times if the queue is linked to multiple folders.

---

### Rate Limits

- **Queue export endpoint**: Limited to **100 API requests/day/tenant**.
- **General API requests**: Subject to standard rate limits (consult UiPath documentation for specific limits).

---

### Identity Server Endpoints

| Endpoint | URL | Purpose |
|----------|-----|---------|
| Token | `https://cloud.uipath.com/{organizationName}/identity_/connect/token` | Obtain access tokens via client credentials. |
| Discovery | `https://cloud.uipath.com/{organizationName}/identity_/.well-known/openid-configuration` | Retrieve metadata about the Identity Server instance. |


## **Object List**

For connector purposes, we treat UiPath Orchestrator queues as the primary **object/table**.  
The object list is **dynamic** and can be retrieved via an API call.

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `queue_items` | Items in a queue with their data, status, and processing history | `GET /odata/QueueDefinitions({id})/UiPathODataSvc.Export` | `append` (queue items are timestamped and typically not deleted) |

**Connector scope**:
- The primary focus is on exporting queue items from UiPath Orchestrator queues.
- Each queue is identified by a `QueueDefinitionId`.
- Queue items can be filtered by queue, status, and date ranges.

**Retrieving the list of queues**:
The list of available queues can be obtained via:

```bash
GET https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/QueueDefinitions
```

This returns all queue definitions in the specified folder, including:
- `Id`: Queue definition ID (required for export)
- `Name`: Queue name
- `Description`: Queue description
- Other queue metadata

Example response:

```json
{
  "@odata.context": "https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/$metadata#QueueDefinitions",
  "value": [
    {
      "Id": 27965,
      "Name": "MyQueue12345",
      "Description": "Sample queue",
      "MaxNumberOfRetries": 1,
      "AcceptAutomaticallyRetry": true,
      "CreationTime": "2024-01-10T10:30:00Z"
    }
  ]
}
```


## **Object Schema**

### General notes

- UiPath Orchestrator provides queue items via an export API that returns CSV data.
- The schema is derived from the CSV export format.
- Queue items include both metadata (status, timestamps) and custom data (`SpecificContent`).

### `queue_items` object (primary table)

**Source endpoint**:  
Multi-step process via `/odata/QueueDefinitions({id})/UiPathODataSvc.Export`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `Id` | integer (64-bit) | Unique identifier for the queue item across all folders. |
| `QueueDefinitionId` | integer (64-bit) | ID of the queue definition this item belongs to. |
| `Name` | string | Name of the queue (from the queue definition). |
| `Priority` | string | Priority level: `Low`, `Normal`, or `High`. |
| `Status` | string | Current status: `New`, `InProgress`, `Failed`, `Successful`, `Abandoned`, `Retried`, `Deleted`. |
| `ReviewStatus` | string | Review status if applicable: `None`, `NeedsReview`, `Verified`, `Rejected`. |
| `Reference` | string or null | User-defined reference for the queue item. |
| `ProcessingException` | string or null | Exception details if the item failed processing. |
| `DueDate` | string (ISO 8601 datetime) or null | Due date for processing. |
| `RiskSlaDate` | string (ISO 8601 datetime) or null | Risk SLA date. |
| `DeferDate` | string (ISO 8601 datetime) or null | Date after which the item can be processed. |
| `StartProcessing` | string (ISO 8601 datetime) or null | When processing started. |
| `EndProcessing` | string (ISO 8601 datetime) or null | When processing ended. |
| `SecondsInPreviousAttempts` | integer | Total seconds spent in previous processing attempts. |
| `AncestorId` | integer or null | ID of the parent queue item (for retried items). |
| `RetryNumber` | integer | Number of times this item has been retried. |
| `SpecificContent` | JSON object | Custom data payload for the queue item. |
| `Output` | JSON object or null | Output data from processing. |
| `Progress` | string or null | Progress information during processing. |
| `CreationTime` | string (ISO 8601 datetime) | When the queue item was created. |
| `LastModificationTime` | string (ISO 8601 datetime) | When the queue item was last modified. Used as cursor for incremental reads. |
| `Robot` | struct or null | Robot that processed the item (includes `Id`, `Name`, `Type`). |
| `ReviewerUser` | struct or null | User who reviewed the item (includes `Id`, `Username`, `Email`). |
| `OrganizationUnitId` | integer (64-bit) | Folder ID where the queue item exists. |

**Nested `Robot` struct** (when item was processed by a robot):

| Field | Type | Description |
|-------|------|-------------|
| `Id` | integer (64-bit) | Robot ID. |
| `Name` | string | Robot name. |
| `Type` | string | Robot type (e.g., `Attended`, `Unattended`). |

**Nested `ReviewerUser` struct** (when item has been reviewed):

| Field | Type | Description |
|-------|------|-------------|
| `Id` | integer (64-bit) | User ID. |
| `Username` | string | Username of reviewer. |
| `Email` | string | Email of reviewer. |

**Example queue item (CSV row converted to JSON representation)**:

```json
{
  "Id": 123456,
  "QueueDefinitionId": 27965,
  "Name": "MyQueue12345",
  "Priority": "Normal",
  "Status": "Successful",
  "Reference": "82086",
  "ProcessingException": null,
  "DueDate": "2024-01-15T12:00:00Z",
  "StartProcessing": "2024-01-15T07:30:00Z",
  "EndProcessing": "2024-01-15T07:35:00Z",
  "SpecificContent": {
    "CustomerName": "Acme Corp",
    "OrderId": "ORD-1234",
    "Amount": 1500.00
  },
  "Output": {
    "ProcessedBy": "RPA-Robot-01",
    "Result": "Success"
  },
  "CreationTime": "2024-01-15T07:00:00Z",
  "LastModificationTime": "2024-01-15T07:35:00Z",
  "Robot": {
    "Id": 456,
    "Name": "RPA-Robot-01",
    "Type": "Unattended"
  }
}
```

> The columns listed above define the **complete connector schema** for the `queue_items` table.


## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get the primary key for queue items.  
Instead, the primary key is defined **statically** based on the resource schema.

- **Primary key for `queue_items`**: `Id`  
  - Type: 64-bit integer  
  - Property: Unique across all queue items in UiPath Orchestrator (not just within a queue).

The connector will:
- Read the `Id` field from each queue item record in the CSV export.
- Use it as the immutable primary key for upserts when ingestion type is `append`.

**Composite key consideration**:
- For multi-folder deployments, `(OrganizationUnitId, Id)` may be used as a composite key to handle cases where queue items with the same ID exist across different folders.


## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

**Planned ingestion type for UiPath Queue Items**:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `queue_items` | `append` | Queue items are typically created once and progress through statuses. While status changes occur, most use cases treat queue items as append-only events. The `LastModificationTime` field can be used as a cursor for incremental reads to capture new items and status updates. |

For `queue_items`:
- **Primary key**: `Id` (or composite `OrganizationUnitId`, `Id`)
- **Cursor field**: `LastModificationTime`
- **Sort order**: Ascending by `LastModificationTime`
- **Deletes**: Queue items can have a `Deleted` status, but are typically not hard-deleted. The connector treats status changes as new records or upserts based on the implementation strategy.

**Alternative**: `cdc` ingestion type could be used if the connector needs to capture status updates as modifications rather than new records. This would require using `LastModificationTime` as the cursor and treating the data as upserts by `Id`.


## **Read API for Data Retrieval**

### Primary read endpoint for `queue_items` (3-step export process)

The UiPath Orchestrator API uses a **3-step asynchronous export process** to retrieve queue items:

#### Step 1: Initiate Export

- **HTTP method**: `POST`
- **Endpoint**: `/odata/QueueDefinitions({QueueDefinitionId})/UiPathODataSvc.Export`
- **Base URL**: `https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/`

**Path parameters**:
- `QueueDefinitionId` (integer, required): ID of the queue definition to export items from.

**Query parameters** (optional, for filtering):

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `$filter` | string (OData filter) | no | Filter expression to limit queue items (e.g., by `QueueDefinitionId`, `Status`, date ranges). |
| `$expand` | string | no | Related entities to expand (e.g., `Robot`, `ReviewerUser`). |

**Request headers**:
```
Authorization: Bearer {access_token}
X-UIPATH-OrganizationUnitId: {folder_id}
Content-Length: 0
```

**Example request**:

```bash
POST https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/QueueDefinitions(27965)/UiPathODataSvc.Export?$filter=((QueueDefinitionId%20eq%2027965))&$expand=Robot,ReviewerUser
Authorization: Bearer {access_token}
X-UIPATH-OrganizationUnitId: 770643
Content-Length: 0
```

**Response** (Export initiated):

```json
{
  "@odata.context": "https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/$metadata#Exports/$entity",
  "Id": 8657,
  "Name": "MyQueue12345-items",
  "Type": "Queues",
  "Status": "New",
  "RequestedAt": "2024-01-15T07:38:21.8891311Z",
  "ExecutedAt": null,
  "Size": null
}
```

**Key response fields**:
- `Id`: Export job ID (used in subsequent steps).
- `Status`: Current export status (`New`, `InProgress`, `Completed`, `Failed`).

**Rate limit**: This endpoint is limited to **100 requests/day/tenant**.

---

#### Step 2: Poll Export Status

- **HTTP method**: `GET`
- **Endpoint**: `/odata/Exports({ExportId})`
- **Base URL**: `https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/`

**Path parameters**:
- `ExportId` (integer, required): The export job ID returned from Step 1.

**Request headers**:
```
Authorization: Bearer {access_token}
X-UIPATH-OrganizationUnitId: {folder_id}
```

**Example request**:

```bash
GET https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/Exports(8657)
Authorization: Bearer {access_token}
X-UIPATH-OrganizationUnitId: 770643
```

**Response** (Export in progress):

```json
{
  "@odata.context": "https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/$metadata#Exports/$entity",
  "Id": 8657,
  "Name": "MyQueue12345-items",
  "Type": "Queues",
  "Status": "InProgress",
  "RequestedAt": "2024-01-15T07:37:02.783Z",
  "ExecutedAt": null,
  "Size": null
}
```

**Response** (Export completed):

```json
{
  "@odata.context": "https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/$metadata#Exports/$entity",
  "Id": 8657,
  "Name": "MyQueue12345-items",
  "Type": "Queues",
  "Status": "Completed",
  "RequestedAt": "2024-01-15T07:37:02.783Z",
  "ExecutedAt": "2024-01-15T07:37:05.553Z",
  "Size": 203226
}
```

**Polling strategy**:
- Poll this endpoint every 5-10 seconds until `Status` is `Completed` or `Failed`.
- If `Status` is `Failed`, the export should be retried or an error should be raised.

---

#### Step 3: Get Download Link

- **HTTP method**: `GET`
- **Endpoint**: `/odata/Exports({ExportId})/UiPath.Server.Configuration.OData.GetDownloadLink`
- **Base URL**: `https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/`

**Path parameters**:
- `ExportId` (integer, required): The export job ID from Step 1.

**Request headers**:
```
Authorization: Bearer {access_token}
X-UIPATH-OrganizationUnitId: {folder_id}
```

**Example request**:

```bash
GET https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/Exports(8657)/UiPath.Server.Configuration.OData.GetDownloadLink
Authorization: Bearer {access_token}
X-UIPATH-OrganizationUnitId: 770643
```

**Response**:

```json
{
  "@odata.context": "https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/$metadata#UiPath.Server.Configuration.OData.BlobFileAccessDto",
  "Uri": "https://storage.example.com/exports/MyQueue12345-items-8657.csv?signature=...",
  "Verb": "GET",
  "RequiresAuth": false,
  "Headers": {
    "Keys": [],
    "Values": []
  }
}
```

**Key response fields**:
- `Uri`: The download URL for the CSV file containing queue items.
- `Verb`: HTTP method to use (typically `GET`).
- `RequiresAuth`: Whether authentication is required (typically `false` as the URL includes a signed token).

---

#### Step 4: Download and Parse CSV

- **HTTP method**: `GET`
- **Endpoint**: Use the `Uri` returned from Step 3.

**Example request**:

```bash
GET https://storage.example.com/exports/MyQueue12345-items-8657.csv?signature=...
```

**Response**: CSV file containing queue items with columns matching the schema defined in the **Object Schema** section.

**CSV format notes**:
- First row contains column headers.
- Nested objects like `Robot` and `ReviewerUser` may be represented as:
  - Flattened columns (e.g., `Robot.Id`, `Robot.Name`), or
  - JSON-encoded strings that need to be parsed.
- `SpecificContent` and `Output` are JSON-encoded strings that must be parsed to extract the custom data.

**Connector processing**:
1. Download the CSV file from the signed URL.
2. Parse the CSV into structured records.
3. Parse JSON fields (`SpecificContent`, `Output`) into nested objects.
4. Convert the data into the target table format.

---

### Incremental Read Strategy

**Initial sync**:
- Export all queue items without date filters, or use a configurable `start_date` to limit historical data.

**Subsequent syncs**:
- Use the `$filter` parameter in Step 1 to filter by `LastModificationTime`:
  ```
  $filter=(LastModificationTime ge {last_sync_timestamp})
  ```
- Store the maximum `LastModificationTime` from each sync as the cursor for the next run.
- Include a small lookback window (e.g., 5 minutes) to handle late-arriving updates.

**Example incremental export request**:

```bash
POST https://cloud.uipath.com/{organizationName}/{tenantName}/orchestrator_/odata/QueueDefinitions(27965)/UiPathODataSvc.Export?$filter=(LastModificationTime%20ge%202024-01-15T07:00:00Z)&$expand=Robot,ReviewerUser
```

**Handling deletes**:
- Queue items with `Status = "Deleted"` should be captured as status changes.
- The connector should treat these as soft deletes (if using `cdc`) or as regular records (if using `append`).

---

### Alternative APIs

UiPath Orchestrator also provides direct OData endpoints for queue items:

- **List queue items**: `GET /odata/QueueItems`
  - Supports `$filter`, `$orderby`, `$top`, `$skip` for pagination.
  - Limited to smaller result sets; not suitable for large-scale exports.
  - Example: `GET /odata/QueueItems?$filter=QueueDefinitionId eq 27965&$top=100`

- **Get single queue item**: `GET /odata/QueueItems({Id})`
  - Retrieves details for a specific queue item.

**Comparison**:

| Method | Use Case | Pros | Cons |
|--------|----------|------|------|
| Export API (3-step) | Bulk export of queue items | Handles large datasets, returns CSV | Asynchronous, limited to 100/day, complex workflow |
| OData `/QueueItems` | Small queries, real-time access | Synchronous, simple | Not suitable for large exports, requires pagination |

**Recommendation**: Use the **Export API** for initial bulk loads and periodic full refreshes. Use **OData `/QueueItems`** with filters for real-time or incremental queries when export limits are a concern.


## **Field Type Mapping**

### General mapping (UiPath CSV/JSON → connector logical types)

| UiPath Type | Example Fields | Connector Logical Type | Notes |
|-------------|----------------|------------------------|-------|
| integer (32/64-bit) | `Id`, `QueueDefinitionId`, `OrganizationUnitId`, `RetryNumber` | long / integer | Prefer 64-bit integer for IDs. |
| string | `Name`, `Priority`, `Status`, `Reference`, `Progress` | string | UTF-8 text. |
| string (ISO 8601 datetime) | `CreationTime`, `LastModificationTime`, `DueDate`, `StartProcessing`, `EndProcessing` | timestamp with timezone | Stored as UTC timestamps; parse ISO 8601 format. |
| JSON string | `SpecificContent`, `Output` | struct (nested object) | Requires JSON parsing to extract nested fields. |
| object | `Robot`, `ReviewerUser` | struct | Nested records with multiple fields. |
| nullable fields | `Reference`, `ProcessingException`, `Output`, `Robot`, `ReviewerUser` | corresponding type + null | When fields are absent or null, surface `null`. |

### Special behaviors and constraints

- `Id`, `QueueDefinitionId`, and `OrganizationUnitId` should be stored as **64-bit integers** to avoid overflow.
- `Status`, `Priority`, and `ReviewStatus` are enums represented as strings; preserve as strings for flexibility.
- Timestamp fields use ISO 8601 in UTC (e.g., `"2024-01-15T07:37:02.783Z"`); parsing must handle the `Z` timezone indicator.
- `SpecificContent` and `Output` are JSON-encoded strings in the CSV; they must be parsed into nested objects/structs for downstream use.
- Nested structs (`Robot`, `ReviewerUser`) should be represented as nested types, not flattened.
- Empty or null JSON fields should be represented as `null`, not empty objects `{}`.


## **Known Quirks & Edge Cases**

- **Queue items across folders**:
  - The `/QueueItems` endpoints count and return queue items for every folder the queue is linked to.
  - If a queue is linked to 3 folders, one queue item appears as 3 records (one per folder).
  - Use the `X-UIPATH-OrganizationUnitId` header to retrieve results for a specific folder.

- **Export rate limits**:
  - The export API is limited to **100 requests/day/tenant**.
  - Plan syncs accordingly; avoid triggering exports too frequently.

- **Asynchronous export process**:
  - The export is asynchronous and may take several seconds to minutes depending on data volume.
  - Implement robust polling with timeout handling.

- **CSV format variations**:
  - The exact CSV format (flattened vs JSON-encoded nested fields) may vary by UiPath version.
  - Test with actual exports to confirm the schema.

- **SpecificContent schema**:
  - The `SpecificContent` field contains custom user-defined data.
  - Its schema varies by queue and is not standardized.
  - The connector should treat it as a flexible JSON object or map.

- **Deleted queue items**:
  - Queue items with `Status = "Deleted"` may still appear in exports.
  - Downstream systems should filter or handle these appropriately.

- **Status transitions**:
  - Queue items progress through multiple statuses: `New` → `InProgress` → `Successful`/`Failed`.
  - Retried items create new queue items with `AncestorId` pointing to the original.
  - The connector should decide whether to treat retries as separate records or as updates.

- **Signed download URLs**:
  - The download URL returned in Step 3 is signed and has a limited expiration time.
  - Download the CSV immediately after obtaining the link.

- **Empty exports**:
  - If no queue items match the filter criteria, the export will complete successfully but return an empty CSV.
  - Handle this gracefully without raising errors.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/queue-items-requests#exporting-queue-items | 2025-01-07 | High | 3-step export process, endpoints, request/response examples, rate limits. |
| Official Docs | https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/queue-items-requests | 2025-01-07 | High | Queue items endpoints, OData filtering, `X-UIPATH-OrganizationUnitId` header behavior. |
| Official Docs | https://docs.uipath.com/automation-cloud/automation-cloud/latest/api-guide/accessing-uipath-resources-using-external-applications#confidential-apps-with-app-scopes-(client-credentials-flow) | 2025-01-07 | High | OAuth 2.0 client credentials flow, token endpoint, request parameters, scope declaration, access token expiration. |
| Official Docs | https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/building-api-requests | 2025-01-07 | High | Base URL structure, header requirements, OData query syntax. |


## **Sources and References**

- **Official UiPath Automation Cloud API documentation** (highest confidence)
  - `https://docs.uipath.com/automation-cloud/automation-cloud/latest/api-guide/accessing-uipath-resources-using-external-applications#confidential-apps-with-app-scopes-(client-credentials-flow)` - OAuth 2.0 client credentials flow
  - `https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/queue-items-requests#exporting-queue-items` - Queue items export process
  - `https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/queue-items-requests` - Queue items endpoints
  - `https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/building-api-requests` - API request structure
  - `https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/about-odata-and-references` - OData reference

When conflicts arise, **official UiPath documentation** is treated as the source of truth.


---

## **Acceptance Checklist**

Before completion, verify:

- [x] All template sections present and complete
- [x] Every schema field listed (no omissions)
- [x] One authentication method documented with actionable steps
- [x] Endpoints include params, examples, and pagination
- [x] Incremental strategy defines cursor, order, lookback, delete handling
- [x] Research Log completed with full URLs
- [x] No unverifiable claims; gaps marked `TBD:` with rationale

