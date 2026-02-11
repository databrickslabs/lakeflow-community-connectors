# Nomination Management API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
The following entities/objects are available in this API:

1. **Nomination** - Successor and talent pool nomination management (action-based, not entity-based)
2. **nominationActionResponse** - Response schema for nomination actions

**Note:** This is an action-based API service for managing nominations. It does not expose entity sets for direct querying. Operations are performed through POST actions.

## Object Schema

### Nomination Request Payload (for upsertNomination)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| nominationTarget | string | Yes | A position code or talent pool code. For successor nominations, must use MDF position-based nomination method |
| userId | string | Yes | User ID of a successor or talent pool nominee |
| readiness | int64 | No | Readiness of a successor or talent pool nominee |
| rank | int32 | No | Rank of a successor or talent pool nominee |
| note | string | No | Note for the nomination |
| emergencyCover | boolean | No | Whether a person is an emergency cover for a position or talent pool |

### Nomination Request Payload (for deleteNomination)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| nominationTarget | string | Yes | A position code or talent pool code |
| userId | string | Yes | User ID of a successor or talent pool nominee |

### nominationActionResponse

| Field | Type | Filterable | Description |
|-------|------|------------|-------------|
| action | string | Yes | The action performed (nullable) |
| value | boolean | Yes | Result of the action (nullable) |

## Get Object Primary Keys

This API is action-based and does not expose traditional entity sets with primary keys. Nominations are identified by the combination of:
- `nominationTarget` (position code or talent pool code)
- `userId` (user being nominated)

## Object's Ingestion Type

| Entity | Ingestion Type | Rationale |
|--------|---------------|-----------|
| Nomination | `N/A` | This is an action-based service, not a read-based entity service. No data retrieval endpoints are available. |

**Important:** This API is designed for write operations (create, update, delete nominations) only. It does not provide GET endpoints for reading nomination data. To read nomination data, use the appropriate OData v2 or v4 succession planning APIs.

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odatav4/talent/succession/NominationService.svc/v1
```

**Note:** This service does not provide read endpoints. It only supports the following action endpoints:

### Available Endpoints

#### 1. Refresh Metadata

**Endpoint:** `POST /refreshMetadata`

**Description:** Refresh the metadata in the Nomination service.

**Response:** `204 No Content` on success

#### 2. Create or Update Nomination (Upsert)

**Endpoint:** `POST /upsertNomination`

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| isPoolNomination | boolean | No | Set to `true` to manage talent pool nomination instead of successor nomination |

**Request Body:**
```json
{
  "nominationTarget": "POSITION_CODE_001",
  "userId": "user123",
  "readiness": 2,
  "rank": 1,
  "note": "High potential candidate",
  "emergencyCover": false
}
```

**Example Request - Successor Nomination:**
```http
POST https://{api-server}/odatav4/talent/succession/NominationService.svc/v1/upsertNomination
Content-Type: application/json
Authorization: Basic base64(username@companyId:password)

{
  "nominationTarget": "POSITION_001",
  "userId": "EMP001",
  "readiness": 2,
  "rank": 1
}
```

**Example Request - Talent Pool Nomination:**
```http
POST https://{api-server}/odatav4/talent/succession/NominationService.svc/v1/upsertNomination?isPoolNomination=true
Content-Type: application/json
Authorization: Basic base64(username@companyId:password)

{
  "nominationTarget": "TALENT_POOL_001",
  "userId": "EMP001",
  "readiness": 1,
  "rank": 2,
  "emergencyCover": true
}
```

**Success Response:**
```json
{
  "action": "upsert",
  "value": true
}
```

#### 3. Delete Nomination

**Endpoint:** `POST /deleteNomination`

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| isPoolNomination | boolean | No | Set to `true` to delete talent pool nomination instead of successor nomination |

**Request Body:**
```json
{
  "nominationTarget": "POSITION_CODE_001",
  "userId": "user123"
}
```

**Example Request:**
```http
POST https://{api-server}/odatav4/talent/succession/NominationService.svc/v1/deleteNomination
Content-Type: application/json
Authorization: Basic base64(username@companyId:password)

{
  "nominationTarget": "POSITION_001",
  "userId": "EMP001"
}
```

**Success Response:**
```json
{
  "action": "delete",
  "value": true
}
```

#### 4. Batch Requests

**Endpoint:** `POST /$batch`

**Description:** Group multiple requests into a single request payload.

**Content-Type:** `multipart/mixed;boundary=request-separator`

**Example Request:**
```http
POST https://{api-server}/odatav4/talent/succession/NominationService.svc/v1/$batch
Content-Type: multipart/mixed;boundary=request-separator
Authorization: Basic base64(username@companyId:password)

--request-separator
Content-Type: application/http
Content-Transfer-Encoding: binary

POST /upsertNomination HTTP/1.1
Content-Type: application/json

{"nominationTarget": "POS001", "userId": "EMP001", "readiness": 2}

--request-separator--
```

### Error Responses

| HTTP Status | Description |
|-------------|-------------|
| 400 | Bad Request - Empty nominationTarget/userId, invalid readiness/rank, or nomination doesn't exist |
| 403 | Forbidden - No permission to access Nomination.svc |
| 404 | Not Found - Invalid nominationTarget or userId value |
| 500 | Internal Server Error |
| 501 | Not Implemented - Nomination method is not MDF position-based |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| boolean | BooleanType |
| int32 | IntegerType |
| int64 | LongType |

## Sources and References
- SAP SuccessFactors API Spec: `SCMNominationService.json`
- SAP Help Portal: [Nomination Service API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/96e18d5a46bb430ea4308b1844c59639.html)
- API Servers List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/af2b8d5437494b12be88fe374eba75b6.html)
