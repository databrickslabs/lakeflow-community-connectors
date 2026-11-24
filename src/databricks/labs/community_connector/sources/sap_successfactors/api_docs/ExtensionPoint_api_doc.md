# Retrieve and Process Extension Task API Documentation

## Authorization

- **Method**: HTTP Bearer Authentication
- **Security Scheme**: `BearerAuth`
- **Header Name**: `Authorization`
- **Header Format**: `Bearer {token}`

## Object List

| Object Name | Description |
|-------------|-------------|
| Onboarding Extension Point | Extension tasks for onboarding process management |

## Object Schema

### ExtensionPointTaskDetail

| Field | Type | Read/Write | Description | Values |
|-------|------|------------|-------------|--------|
| taskType | string (enum) | Read | Type of extension task | HIRING_MANAGER_REVIEW_EXTENSION, PERSONAL_DATA_COLLECTION_EXTENSION |
| taskId | string | Read | Extension task identifier | - |
| processStatus | string (enum) | Read | Current process status | SCHEDULED, IN_PROGRESS, COMPLETED, CANCELLED_POST_COMPLETION, CANCELLED, SKIPPED, DECLINED, CLOSED |
| status | string (enum) | Write | Action to perform | RESUME, CANCEL |
| comments | string | Write | Comments for the action | - |
| eventReason | string | Write | Reason for the event | - |

### Task Type Enum Values

| Value | Description |
|-------|-------------|
| HIRING_MANAGER_REVIEW_EXTENSION | Extension point for hiring manager review |
| PERSONAL_DATA_COLLECTION_EXTENSION | Extension point for personal data collection |

### Process Status Enum Values

| Value | Description |
|-------|-------------|
| SCHEDULED | Task is scheduled |
| IN_PROGRESS | Task is in progress |
| COMPLETED | Task has been completed |
| CANCELLED_POST_COMPLETION | Task cancelled after completion |
| CANCELLED | Task has been cancelled |
| SKIPPED | Task was skipped |
| DECLINED | Task was declined |
| CLOSED | Task is closed |

### Status Action Enum Values (Write-Only)

| Value | Description |
|-------|-------------|
| RESUME | Resume/complete the extension task |
| CANCEL | Cancel the extension task and stop onboarding |

### ErrorResponse

| Field | Type | Description |
|-------|------|-------------|
| error | Error | Error details |

### Error

| Field | Type | Description |
|-------|------|-------------|
| code | string | Error code |
| message | string | Error message |
| details | array[Detail] | Additional error details |

### Detail

| Field | Type | Description |
|-------|------|-------------|
| code | string | Detail error code |
| message | string | Detail error message |
| target | string | Target of the error |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| ExtensionPointTaskDetail | processId + taskId |

## Object's Ingestion Type

**Recommended Ingestion Type**: `snapshot`

**Analysis**:
- The GET endpoint retrieves open extension tasks for a specific process
- No timestamp-based filtering for incremental change detection
- Tasks are retrieved by processId - point-in-time query
- Status changes tracked through processStatus field
- Limited to open/active tasks per process
- No built-in pagination or cursor support

## Read API for Data Retrieval

### 1. Get Extension Tasks (GET)

**Endpoint**: `GET /processes/{processId}/tasks`

**Base URL**: `https://{apiServer}/rest/onboarding/processes/v1`

**Alternative Sandbox URL**: `https://sandbox.api.sap.com/successfactorsfoundation/rest/onboarding/processes/v1`

**Path Parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| processId | Yes | string | Onboarding process ID |

**Query Parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| $filter | No | string | Task type filter |

**Example Request**:
```http
GET https://apisalesdemo8.successfactors.com/rest/onboarding/processes/v1/processes/PROC123/tasks
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Example Request with Filter**:
```http
GET https://apisalesdemo8.successfactors.com/rest/onboarding/processes/v1/processes/PROC123/tasks?$filter=taskType eq 'HIRING_MANAGER_REVIEW_EXTENSION'
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Example Response**:
```json
[
  {
    "taskType": "HIRING_MANAGER_REVIEW_EXTENSION",
    "taskId": "TASK001",
    "processStatus": "IN_PROGRESS"
  },
  {
    "taskType": "PERSONAL_DATA_COLLECTION_EXTENSION",
    "taskId": "TASK002",
    "processStatus": "SCHEDULED"
  }
]
```

### 2. Update Extension Task (PATCH)

**Endpoint**: `PATCH /processes/{processId}/tasks/{taskId}`

**Base URL**: `https://{apiServer}/rest/onboarding/processes/v1`

**Path Parameters**:

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| processId | Yes | string | Onboarding process ID |
| taskId | Yes | string | Extension task ID |

**Request Body**: ExtensionPointTaskDetail object (write-only fields)

**Example Request (Complete Task)**:
```http
PATCH https://apisalesdemo8.successfactors.com/rest/onboarding/processes/v1/processes/PROC123/tasks/TASK001
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "status": "RESUME",
  "comments": "Background verification completed successfully"
}
```

**Example Request (Cancel Onboarding)**:
```http
PATCH https://apisalesdemo8.successfactors.com/rest/onboarding/processes/v1/processes/PROC123/tasks/TASK001
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "status": "CANCEL",
  "comments": "Background verification failed",
  "eventReason": "VERIFICATION_FAILED"
}
```

**Pagination**: Not applicable - returns all tasks for a process.

**Response Headers**:

| Header | Type | Description |
|--------|------|-------------|
| X-Correlation-ID | integer | Unique ID to identify the request |
| Retry-After | string (uuid) | Wait time before retry (on rate limiting/service unavailable) |

**Response Codes**:

| Code | Description |
|------|-------------|
| 200 | OK - Request successful |
| 400 | Bad Request - Invalid request |
| 401 | Unauthorized - Authentication required |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Process or task not found |
| 409 | Conflict - Resource state conflict |
| 412 | Precondition Failed |
| 413 | Payload Too Large |
| 429 | Too Many Requests - Rate limited |
| 500 | Internal Server Error |
| 503 | Service Unavailable |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| enum | StringType |
| boolean | BooleanType |
| integer | IntegerType |
| array | ArrayType |
| object | StructType |

## Sources and References

- **SAP SuccessFactors API Spec**: ExtensionPoint.json
- **SAP Help Portal**: [Onboarding Extension Point APIs](https://help.sap.com/docs/SAP_SUCCESSFACTORS_ONBOARDING/c94ed5fcb5fe4e0281f396556743812c/2d38cab5171a4d588c2612086ca8fed0.html?version=2411)
- **API Type**: REST
- **API Version**: 1.0

## Use Case Examples

### Background Verification Workflow

1. **Retrieve Open Tasks**: Use GET endpoint to find extension tasks for a new hire's process
2. **Process External Verification**: Perform background check through external system
3. **Complete Task**: If verification passes, call PATCH with status=RESUME
4. **Cancel Onboarding**: If verification fails, call PATCH with status=CANCEL to stop onboarding

### Integration Pattern

```
External System -> GET /processes/{processId}/tasks -> Process Extension Tasks
                -> External Processing (BGV, etc.)
                -> PATCH /processes/{processId}/tasks/{taskId} -> Resume/Cancel
```
