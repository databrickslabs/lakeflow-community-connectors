# Clock In Clock Out API Documentation

## Authorization

- **Method**: API Key Authentication
- **Security Scheme**: `ApiKeyAuth`
- **Header Name**: `Authorization`
- **Header Format**: `Bearer {token}`
- **Note**: Enter the API Key in the format: Bearer<space><token>

## Object List

| Object Name | Description |
|-------------|-------------|
| TimeEvents | Employee time punch events for clock in/clock out tracking |

## Object Schema

### TimeEvents (Request)

| Field | Type | Required | Constraints | Description | Example |
|-------|------|----------|-------------|-------------|---------|
| id | string | Yes | minLength: 1, maxLength: 36, pattern: alphanumeric or UUID | Unique ID of the item in API call | "1" |
| assignmentId | string | Yes | minLength: 1, maxLength: 100, pattern: alphanumeric | Assignment ID of the employee | "cgrant" |
| typeCode | string | Yes | minLength: 1, maxLength: 20, pattern: word chars and hyphen | Code of the time event type | "P10" |
| timestamp | string | Yes | - | Date and time in ISO format | "2020-07-17T12:10:21+02:00" |
| terminalId | string | No | minLength: 1, maxLength: 30 | Identifier of the terminal | "Building 1 Terminal 5" |
| timeTypeCode | string | No | minLength: 1, maxLength: 128 | External code of time type (only for START type events) | "WORKING_TIME" |

### CreateTimeEventTerminalResponse

| Field | Type | Description |
|-------|------|-------------|
| succeededTimeEvents | array[SucceededTimeEvent] | List of successfully created time events |
| failedTimeEvents | array[FailedTimeEvent] | List of failed time events with error details |

### SucceededTimeEvent

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| UUID | string | Internal unique identifier of created Time Event | "4f5a1b86108a4a2195048716593b864e" |
| id | string | Unique ID mapped from the request | "1" |

### FailedTimeEvent

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| errorText | string | Error message describing the failure | "A time event for this employee and timestamp already exists." |
| id | string | Unique ID from the request | "2" |

### TimeEventProcessingApiErrorResponse

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| status | string (enum) | HTTP status code | "BAD_REQUEST" |
| message | string | Error message | "Request Validation Error" |
| errors | array[string] | List of specific errors | ["The id must not be blank."] |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| TimeEvents | id (request), UUID (response) |
| SucceededTimeEvent | UUID |

## Object's Ingestion Type

**Recommended Ingestion Type**: `append`

**Analysis**:
- This API is primarily for **creating** time events (POST only), not reading
- Time events are append-only records tracking employee clock in/out
- No GET endpoint available for retrieving existing time events
- No modification or deletion capabilities in this API
- For reading time events, use the OData API or ClockInClockOutTimeEventsRestAPI

**Note**: This API is write-only and not suitable for data ingestion. Use alternative APIs for reading time event data.

## Read API for Data Retrieval

### Create Time Events (POST)

**Endpoint**: `POST /TimeEvents`

**Base URL**: `https://{api-server}/rest/timemanagement/timeeventprocessing/v1`

**Alternative Sandbox URL**: `https://sandbox.api.sap.com/sapsuccessfactorstimetracking`

**Content-Type**: `application/json`

**Request Body**: Array of TimeEvents objects

**Example Request**:
```http
POST https://apisalesdemo2.successfactors.eu/rest/timemanagement/timeeventprocessing/v1/TimeEvents
Authorization: Bearer {api_token}
Content-Type: application/json

[
  {
    "id": "1",
    "assignmentId": "cgrant",
    "typeCode": "P10",
    "timestamp": "2020-07-17T12:10:21+02:00",
    "terminalId": "Building 1 Terminal 5",
    "timeTypeCode": "WORKING_TIME"
  },
  {
    "id": "2",
    "assignmentId": "jsmith",
    "typeCode": "P20",
    "timestamp": "2020-07-17T17:30:00+02:00"
  }
]
```

**Example Response (201 Created)**:
```json
{
  "succeededTimeEvents": [
    {
      "UUID": "4f5a1b86108a4a2195048716593b864e",
      "id": "1"
    }
  ],
  "failedTimeEvents": [
    {
      "errorText": "A time event for this employee and timestamp already exists.",
      "id": "2"
    }
  ]
}
```

**Pagination**: Not applicable - batch create operation.

**Response Codes**:

| Code | Description |
|------|-------------|
| 201 | Created - Valid time events successfully created |
| 400 | Bad Request - Validation error in request |
| 500 | Internal Server Error |

**Timestamp Format**:
- ISO 8601 format: `yyyy-MM-dd'T'HH:mm:ssZ`
- Examples: `2020-12-31T10:00:12+05:30` or `2020-12-31T10:00:12+0530`

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| boolean | BooleanType |
| array | ArrayType |
| object | StructType |

## Sources and References

- **SAP SuccessFactors API Spec**: ClockInClockOut.json
- **SAP Help Portal**: [Clock In Clock Out API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_TIME_TRACKING/a2a03e747cb44e4497b392579c89d439/21eb213a3c4b442fa875821502b20739.html)
- **API Server List**: [SAP SuccessFactors API Servers](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
- **API Type**: REST
- **API Version**: 1.0.0
