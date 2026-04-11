# Clock In Clock Out Integration API Documentation

## Authorization
- Method: API Key Authentication / Basic Authentication
- Header: `Authorization: Basic base64(username:password)` or API Key
- Username format: `username@companyId`

## Object List
The following entities/objects are available in this API:

1. **ClockInClockOutGroups** - Clock In Clock Out Groups configured in the system
2. **TimeEventTypes** - Time Event Types associated with Clock In Clock Out Groups (nested entity)

## Object Schema

### ClockInClockOutGroups

| Field | Type | Required | Filterable | Description |
|-------|------|----------|------------|-------------|
| code | string | Yes | Yes | Unique code of the Clock In Clock Out Group (Primary Key) |
| createdAt | date-time | Yes | No | Date and time at which the Clock In Clock Out Group was created |
| createdBy | string | Yes | No | Assignment ID of the user who created the Clock In Clock Out Group |
| lastChangedAt | date-time | Yes | No | Date and time at which the Clock In Clock Out Group was last modified |
| lastChangedBy | string | Yes | No | Assignment ID of the user who last modified the Clock In Clock Out Group |
| name | string | No | No | Name of the Clock In Clock Out Group (nullable) |
| timeEventTypeNav | array[TimeEventTypes] | No | No | Time Event Types associated with the Clock In Clock Out Group |

### TimeEventTypes

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| code | string | Yes | Unique code of the Time Event Type (Primary Key) |
| active | boolean | Yes | Specifies whether a Time Event Type is active or inactive |
| event | TimeEventTypeEnum | Yes | Specifies whether it is a START or a STOP Time Event Type |
| name | string | No | Name of the Time Event Type (nullable) |
| description | string | No | Description of the Time Event Type (nullable) |

### TimeEventTypeEnum
- `START` - Start time event
- `STOP` - Stop time event

## Get Object Primary Keys

### ClockInClockOutGroups
- Primary Key: `code`

### TimeEventTypes
- Primary Key: `code`

## Object's Ingestion Type

| Entity | Ingestion Type | Rationale |
|--------|---------------|-----------|
| ClockInClockOutGroups | `cdc` | Supports `$filter` and `$orderby` on `lastChangedAt` field for incremental data retrieval |

**Supported Ordering Fields:**
- `code`, `code desc`
- `createdAt`, `createdAt desc`

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odatav4/timemanagement/timeeventprocessing/ClockInClockOutIntegration.svc/v1
```

### Sandbox URL
```
https://sandbox.api.sap.com:443/sapsuccessfactorstimetracking/cicoIntegration/ClockInClockOutIntegration.svc/v1
```

### Get All Clock In Clock Out Groups

**Endpoint:** `GET /ClockInClockOutGroups`

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | integer | Show only the first n items (pagination) |
| $skip | integer | Skip the first n items (pagination) |
| $filter | string | Filter items by property values |
| $count | boolean | Include count of items |
| $orderby | array | Order items by property values: `code`, `code desc`, `createdAt`, `createdAt desc` |
| $select | array | Select properties to return: `code`, `createdAt`, `createdBy`, `lastChangedAt`, `lastChangedBy`, `name` |
| $expand | array | Expand related entities: `*`, `timeEventTypeNav` |

**Example Request:**
```http
GET https://{api-server}/odatav4/timemanagement/timeeventprocessing/ClockInClockOutIntegration.svc/v1/ClockInClockOutGroups?$top=50&$orderby=lastChangedAt desc&$expand=timeEventTypeNav
Authorization: Basic base64(username@companyId:password)
```

**Example Response:**
```json
{
  "value": [
    {
      "code": "GRP123",
      "createdAt": "2017-04-13T15:51:04Z",
      "createdBy": "admin",
      "lastChangedAt": "2017-04-13T15:51:04Z",
      "lastChangedBy": "admin",
      "name": "CICO Group 123",
      "timeEventTypeNav": [
        {
          "code": "CI",
          "active": true,
          "event": "START",
          "name": "Clock In",
          "description": "This time event type can be for clocking in"
        }
      ]
    }
  ],
  "@odata.count": 1
}
```

### Get Single Clock In Clock Out Group by Code

**Endpoint:** `GET /ClockInClockOutGroups('{code}')`

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| code | string | Yes | Unique code of the Clock In Clock Out Group |

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $select | array | Select properties to return |
| $expand | array | Expand related entities: `timeEventTypeNav` |

**Example Request:**
```http
GET https://{api-server}/odatav4/timemanagement/timeeventprocessing/ClockInClockOutIntegration.svc/v1/ClockInClockOutGroups('GRP123')?$expand=timeEventTypeNav
Authorization: Basic base64(username@companyId:password)
```

### Pagination Approach
- Use `$top` to limit the number of records returned per request
- Use `$skip` to offset and retrieve subsequent pages
- Use `$count=true` to get total count of records
- Recommended page size: 50-100 records

**Example Pagination:**
```http
# First page
GET /ClockInClockOutGroups?$top=50&$skip=0&$count=true

# Second page
GET /ClockInClockOutGroups?$top=50&$skip=50

# Third page
GET /ClockInClockOutGroups?$top=50&$skip=100
```

### Incremental Data Retrieval (CDC)
Use `$filter` on `lastChangedAt` for incremental updates:

```http
GET /ClockInClockOutGroups?$filter=lastChangedAt gt 2024-01-01T00:00:00Z&$orderby=lastChangedAt asc
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| boolean | BooleanType |
| date-time (format) | TimestampType |
| integer | IntegerType |
| number | LongType |
| array | ArrayType |
| enum (TimeEventTypeEnum) | StringType |

## Sources and References
- SAP SuccessFactors API Spec: `ClockInClockOutIntegration.json`
- SAP Help Portal: [Clock In Clock Out Integration Service API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/874d7a4a313b45c7a1057febfece16ee.html)
- API Servers List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
