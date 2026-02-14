# Clock In Clock Out Time Events API Documentation

## Authorization
- Method: OAuth 2.0 Bearer Token (or API Key)
- Header: `Authorization: Bearer <token>`
- Note: Export Time Event API respects Role-Based Permissions (RBP) and requires a valid token

## Object List
- timeevents
- Message (nested object for validation messages)

## Object Schema

### timeevents
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| externalId | string | Yes | Yes | Yes | Unique alphanumeric ID for the Time Event |
| workAssignmentId | string | Yes | No | Yes | Assignment ID of the user |
| timestampUTC | date-time | Yes | No | Yes | Timestamp at which Time Event was created |
| timeEventTypeCode | string | Yes | No | Yes | Unique alphanumeric code for the Time Event Type |
| timeTypeCode | string | Yes | No | Yes | Time type code |
| timeZoneOffset | string | Yes | No | Yes | Time zone offset |
| approvalStatusCode | string | Yes | No | Yes | Approval status of the Time Event |
| validationStatusCode | string | Yes | No | Yes | Validation status of the Time Event |
| pairingStatusCode | string | Yes | No | Yes | Pairing status of the Time Event |
| creationSourceCode | string | Yes | No | Yes | Source of created Time Event |
| createdAt | date-time | Yes | No | Yes | Created date |
| createdBy | string | Yes | No | Yes | Created user |
| lastChangedAt | date-time | Yes | No | Yes | Last modified date |
| lastChangedBy | string | Yes | No | Yes | Last modified user |
| comments | string | Yes | No | Yes | Additional information for creation of Manual Time Event |
| reasonForManualTimeEventCode | string | Yes | No | Yes | Reason for creation of Manual Time Event |
| terminalId | string | Yes | No | Yes | Terminal ID of the Time Event |
| validationMessages | array | No | No | Yes | Array of validation messages |

### Message (Validation Message)
| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| code | string | No | Message code |
| message | string | No | Message text |
| target | string | Yes | Target field |
| longtextUrl | string | Yes | URL for long text description |
| numericSeverity | uint8 | No | Numeric severity level |
| transition | boolean | No | Transition flag |
| additionalTargets | array[string] | No | Additional target fields |

## Get Object Primary Keys
| Entity | Primary Key Field(s) |
|--------|---------------------|
| timeevents | externalId |

## Object's Ingestion Type
| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| timeevents | `cdc` | lastChangedAt | Supports $filter and $orderby on lastChangedAt for incremental data extraction |

## Read API for Data Retrieval

### Export Time Events

**Endpoint**: `GET /timeevents`

**Base URL**: `https://{api-server}/odatav4/timemanagement/timeeventprocessing/clockinclockout/v1`

**Sandbox URL**: `https://sandbox.api.sap.com/sapsuccessfactorstimetracking/cico`

**Description**: Fetch and export time events details along with filters. Maximum of 1000 records per request.

**Query Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | integer | Limit number of results (max: 1000) |
| $skip | integer | Skip first n items for pagination |
| $filter | string | Filter expression |
| $orderby | string | Sort order |
| $select | string | Select specific fields |
| $count | boolean | Include total count |

**Supported $select Fields**:
- approvalStatusCode
- comments
- createdAt
- createdBy
- creationSourceCode
- externalId
- lastChangedAt
- lastChangedBy
- pairingStatusCode
- reasonForManualTimeEventCode
- terminalId
- timeEventTypeCode
- timeTypeCode
- timeZoneOffset
- timestampUTC
- validationMessages
- validationStatusCode
- workAssignmentId

**Supported $orderby Values**:
- approvalStatusCode, approvalStatusCode desc
- comments, comments desc
- createdAt, createdAt desc
- createdBy, createdBy desc
- creationSourceCode, creationSourceCode desc
- externalId, externalId desc
- lastChangedAt, lastChangedAt desc
- lastChangedBy, lastChangedBy desc
- pairingStatusCode, pairingStatusCode desc
- reasonForManualTimeEventCode, reasonForManualTimeEventCode desc
- terminalId, terminalId desc
- timeEventTypeCode, timeEventTypeCode desc
- timeTypeCode, timeTypeCode desc
- timeZoneOffset, timeZoneOffset desc
- timestampUTC, timestampUTC desc
- validationMessages, validationMessages desc
- validationStatusCode, validationStatusCode desc
- workAssignmentId, workAssignmentId desc

**Example Request**:
```http
GET https://apisalesdemo2.successfactors.eu/odatav4/timemanagement/timeeventprocessing/clockinclockout/v1/timeevents?$top=100&$filter=lastChangedAt gt 2024-01-01T00:00:00Z&$orderby=lastChangedAt asc&$count=true
Authorization: Bearer <your_access_token>
Accept: application/json
```

**Example Response**:
```json
{
  "@odata.count": 500,
  "value": [
    {
      "externalId": "TE-2024-001",
      "workAssignmentId": "EMP001",
      "timestampUTC": "2024-01-15T08:00:00Z",
      "timeEventTypeCode": "CLOCK_IN",
      "timeTypeCode": "REGULAR",
      "timeZoneOffset": "+01:00",
      "approvalStatusCode": "APPROVED",
      "validationStatusCode": "VALID",
      "pairingStatusCode": "PAIRED",
      "creationSourceCode": "MOBILE_APP",
      "createdAt": "2024-01-15T08:00:05Z",
      "createdBy": "EMP001",
      "lastChangedAt": "2024-01-15T08:00:05Z",
      "lastChangedBy": "EMP001",
      "comments": null,
      "reasonForManualTimeEventCode": null,
      "terminalId": "MOBILE-001",
      "validationMessages": []
    },
    {
      "externalId": "TE-2024-002",
      "workAssignmentId": "EMP001",
      "timestampUTC": "2024-01-15T17:00:00Z",
      "timeEventTypeCode": "CLOCK_OUT",
      "timeTypeCode": "REGULAR",
      "timeZoneOffset": "+01:00",
      "approvalStatusCode": "APPROVED",
      "validationStatusCode": "VALID",
      "pairingStatusCode": "PAIRED",
      "creationSourceCode": "MOBILE_APP",
      "createdAt": "2024-01-15T17:00:03Z",
      "createdBy": "EMP001",
      "lastChangedAt": "2024-01-15T17:00:03Z",
      "lastChangedBy": "EMP001",
      "comments": null,
      "reasonForManualTimeEventCode": null,
      "terminalId": "MOBILE-001",
      "validationMessages": []
    }
  ]
}
```

## Pagination
- Use `$top` and `$skip` for pagination
- Maximum records per request: **1000**
- Use `$count=true` to get total record count

**Example Pagination**:
```http
# First page
GET /timeevents?$top=1000&$skip=0&$count=true

# Second page
GET /timeevents?$top=1000&$skip=1000

# Third page
GET /timeevents?$top=1000&$skip=2000
```

## Incremental Data Retrieval (CDC)
Use `lastChangedAt` field for incremental data extraction:

```http
GET /timeevents?$filter=lastChangedAt gt 2024-01-01T00:00:00Z&$orderby=lastChangedAt asc&$top=1000
```

**Filter Examples**:
```http
# Filter by date range
$filter=lastChangedAt ge 2024-01-01T00:00:00Z and lastChangedAt lt 2024-02-01T00:00:00Z

# Filter by work assignment
$filter=workAssignmentId eq 'EMP001'

# Filter by time event type
$filter=timeEventTypeCode eq 'CLOCK_IN'

# Filter by approval status
$filter=approvalStatusCode eq 'PENDING'

# Combined filters
$filter=lastChangedAt gt 2024-01-01T00:00:00Z and timeEventTypeCode eq 'CLOCK_IN'
```

## Field Type Mapping
| API Type | Spark Type |
|----------|------------|
| string | StringType |
| date-time | TimestampType |
| boolean | BooleanType |
| integer/uint8 | IntegerType |
| array[string] | ArrayType(StringType) |
| array[Message] | ArrayType(StructType) |

## Error Handling
| HTTP Code | Description |
|-----------|-------------|
| 200 | Success - Time events exported successfully OR empty response if RBP permission not enabled |
| 401 | Unauthorized - Invalid bearer token |

**Error Response Example**:
```json
{
  "error": {
    "code": "401",
    "message": "Invalid or expired token",
    "target": "Authorization",
    "details": []
  }
}
```

## Important Notes
1. **Rate Limiting**: Maximum 1000 records per request
2. **RBP Permissions**: API respects Role-Based Permissions; if not enabled, returns empty response with 200 status
3. **Authentication**: Requires valid OAuth 2.0 Bearer token
4. **Time Zone**: All timestamps are in UTC format

## Sources and References
- SAP SuccessFactors API Spec: ClockInClockOutExternal.json
- SAP Help Portal: [Clock In Clock Out API on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/a07ddee2403441118261082dcd14df39.html)
- API Servers: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
