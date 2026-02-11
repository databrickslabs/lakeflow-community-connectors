# Additional Services API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- Service Operations (Unbound Actions/Functions)
- Batch Requests

**Note**: This API does not expose entity sets for data retrieval. It provides service operations for managing onboarding new hire usernames.

## Object Schema

### Request/Response Objects

#### updateUserNamePostHiring Request
| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| userId | string | true | The user ID of the new hire |
| userName | string | true | The new username to assign |

#### getUserNameOfNewlyHiredEmployee Response
| Field | Type | Description |
|-------|------|-------------|
| (return value) | string | The username of the onboarding new hire |

#### Error Response
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| error.code | string | Yes | Error code (e.g., "NotFound", "COE0007") |
| error.message | string | Yes | Error message description |
| error.details | array | No | Additional error details |

## Get Object Primary Keys
- **userId**: Used as the identifier for getUserNameOfNewlyHiredEmployee function

## Object's Ingestion Type
- **Ingestion Type**: `snapshot` (or not applicable for data ingestion)
- **Rationale**: This API provides service operations (actions/functions) rather than entity sets for data retrieval. It is designed for updating and fetching individual user information, not for bulk data extraction.

## Read API for Data Retrieval

### Get Username of Newly Hired Employee

**Endpoint**: `GET /getUserNameOfNewlyHiredEmployee(userId='{userId}')`

**Base URL**: `https://{api-server}/odatav4/onboarding/AdditionalServices.svc/v1/`

**Description**: Fetch the username of an onboarding new hire after they have gone through the Manage Pending Hires process. Applicable to newly hired employees whose joining date is in the future.

**Path Parameters**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| userId | string | Yes | The user ID of the new hire |

**Example Request**:
```http
GET https://apisalesdemo8.successfactors.com/odatav4/onboarding/AdditionalServices.svc/v1/getUserNameOfNewlyHiredEmployee(userId='EMP001')
Authorization: Basic base64(username@companyId:password)
Accept: application/json
```

**Example Response**:
```json
"newUserName123"
```

### Update Username Post Hiring

**Endpoint**: `POST /updateUserNamePostHiring`

**Description**: Update the internal username of new hires with the username that is generated after the hiring process is completed.

**Request Body**:
```json
{
  "userId": "EMP001",
  "userName": "newUserName123"
}
```

**Example Response**:
```json
"Success"
```

### Batch Requests

**Endpoint**: `POST /$batch`

**Description**: Group multiple requests into a single request payload.

**Content-Type**: `multipart/mixed;boundary=request-separator`

## Field Type Mapping
| API Type | Spark Type |
|----------|------------|
| string | StringType |
| string (nullable) | StringType |

## Pagination
This API does not support pagination as it operates on individual records through service operations rather than entity collections.

## Supported Query Parameters
- This API does not expose entity sets, so standard OData query parameters ($select, $filter, $top, $skip, $orderby) are not applicable.

## Error Codes
| HTTP Code | Description |
|-----------|-------------|
| 200 | Success |
| 400 | Error - Invalid request or user doesn't have a valid Onboarding process |
| 403 | Forbidden - No permission to invoke this API |

## Sources and References
- SAP SuccessFactors API Spec: AdditionalServices.json
- SAP Help Portal: [Additional Services on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/8146c98118814cd486feec7b5b60d367.html)
