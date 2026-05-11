# Clock In Clock Out Time Events API Documentation

## Authorization

- **Method**: API Key Authentication
- **Security Scheme**: `ApiKeyAuth`
- **Header Name**: `Authorization`
- **Header Format**: `Bearer {token}`
- **Note**: Enter the API Key in the format: Bearer<space><token>

## Object List

| Object Name | Description |
|-------------|-------------|
| External Time Event | Clock In Clock Out time events for mass deletion operations |

## Object Schema

### TimeEventDeleteRequest

| Field | Type | Description |
|-------|------|-------------|
| value | array[IdAndDeleteRequest] | List of time event IDs to be deleted |

### IdAndDeleteRequest

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| id | string | Time Event ID to be deleted | "1234" |
| deleted | boolean | Set to true for deletion | true |

### TimeEventDeleteResponse

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| status | string | Operation status | "partial_success" |
| message | string | Status message | "A few failures were identified while deleting time events." |
| deletedCount | integer (int32) | Number of successfully deleted events | 2 |
| failedTimeEvents | array[TimeEventDeleteFailedEvent] | List of events that failed to delete | - |

### TimeEventDeleteFailedEvent

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| id | string | ID of time event that was not deleted | "2345" |
| reason | string | Reason for failure | "A time event was not found for this ID." |

### TimeEventDeleteResponseBadRequest

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| status | string | Status | "Bad Request" |
| message | string | Error message | "The maximum number of time events in a request shouldn't exceed 1000." |
| deletedCount | integer (int32) | Number deleted (typically 0) | 0 |
| failedTimeEvents | array | Empty array for bad request | [] |

### InternalServerError

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| error.code | string | Error code | "InternalError" |
| error.message | string | Error message | "An unexpected error has occurred. Please contact the system administrator." |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| TimeEvent | id |

## Object's Ingestion Type

**Recommended Ingestion Type**: `cdc_with_deletes`

**Analysis**:
- This API provides **mass deletion** capability for time events
- Supports tracking of deleted records through deletion operations
- The PATCH method is used to mark records as deleted
- Failed deletions are tracked with reasons
- Requires coordination with other time event APIs for complete data lifecycle

**Note**: This API is primarily for deletion operations. For reading time event data, use the OData API or other REST endpoints.

## Read API for Data Retrieval

### Delete Time Events (PATCH)

**Endpoint**: `PATCH /timeevents`

**Base URL**: `https://{api-server}/rest/timemanagement/timeeventprocessing/clockinclockout/v1`

**Alternative Sandbox URL**: `https://sandbox.api.sap.com/sapsuccessfactorstimetrackingdel`

**Content-Type**: `application/merge-patch+json`

**Request Body**: TimeEventDeleteRequest object

**Batch Limit**: Maximum 1000 time events per request

**Example Request**:
```http
PATCH https://apisalesdemo2.successfactors.eu/rest/timemanagement/timeeventprocessing/clockinclockout/v1/timeevents
Authorization: Bearer {api_token}
Content-Type: application/merge-patch+json

{
  "value": [
    {
      "id": "1234",
      "deleted": true
    },
    {
      "id": "2345",
      "deleted": true
    },
    {
      "id": "3456",
      "deleted": true
    }
  ]
}
```

**Example Response (200 OK - Partial Success)**:
```json
{
  "status": "partial_success",
  "message": "A few failures were identified while deleting time events.",
  "deletedCount": 2,
  "failedTimeEvents": [
    {
      "id": "2345",
      "reason": "A time event was not found for this ID."
    }
  ]
}
```

**Example Response (200 OK - Full Success)**:
```json
{
  "status": "success",
  "message": "All time events were successfully deleted.",
  "deletedCount": 3,
  "failedTimeEvents": []
}
```

**Pagination**: Not applicable - batch delete operation with maximum 1000 items.

**Response Headers**:

| Header | Type | Description |
|--------|------|-------------|
| X-Correlation-ID | string (uuid) | Unique ID to identify the request |
| Retry-After | integer | Wait time before retry (on rate limiting) |

**Response Codes**:

| Code | Description |
|------|-------------|
| 200 | OK - Request processed (check status for partial/full success) |
| 400 | Bad Request - Validation error (e.g., exceeds 1000 items) |
| 500 | Internal Server Error |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| boolean | BooleanType |
| integer | IntegerType |
| int32 | IntegerType |
| array | ArrayType |
| object | StructType |
| uuid | StringType |

## Sources and References

- **SAP SuccessFactors API Spec**: ClockInClockOutTimeEventsRestAPI.json
- **SAP Help Portal**: [Clock In Clock Out Time Events API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_TIME_TRACKING/a2a03e747cb44e4497b392579c89d439/8f7e61ba4a42416e83f3f117180bdeb6.html)
- **API Server List**: [SAP SuccessFactors API Servers](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
- **API Type**: REST
- **API Version**: 1.0
