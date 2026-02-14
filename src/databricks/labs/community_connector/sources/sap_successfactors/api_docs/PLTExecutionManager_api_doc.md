# Execution Manager API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
The following entities are available in this API:

| Entity Name | Description |
|------------|-------------|
| EMEventPayload | Event payload data including file attachments |
| EMMonitoredProcess | Monitored process instances and their states |
| EMEventAttribute | Event attributes (key-value pairs) |
| EMEvent | Execution manager events with timestamps |

## Object Schema

### EMEventPayload
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| id | string | int64 | No | Primary key identifier |
| fileName | string | - | Yes | Name of the file |
| fileType | string | - | Yes | Type of the file |
| mimeType | string | - | Yes | MIME type of the payload |
| payload | string | base64url | No | Base64 encoded payload content |
| type | string | - | No | Type of the event payload |

### EMMonitoredProcess
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| processDefinitionId | string | - | No | Process definition identifier (Key) |
| processInstanceId | string | - | No | Process instance identifier (Key) |
| processType | string | - | No | Type of the process (Key) |
| monitoredProcessId | string | int64 | No | Unique monitored process ID |
| coRelatorId | string | - | Yes | Correlator identifier |
| firstEventTime | string | DateTime | No | Time of the first event |
| lastEventTime | string | DateTime | No | Time of the last event |
| hasErrors | string | - | Yes | Flag indicating if process has errors |
| hasWarnings | string | - | Yes | Flag indicating if process has warnings |
| moduleName | string | - | Yes | Name of the module |
| processDefinitionName | string | - | Yes | Name of the process definition |
| processInstanceName | string | - | Yes | Name of the process instance |
| processState | string | - | No | Current state of the process |

### EMEventAttribute
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| id | string | int64 | No | Primary key identifier |
| name | string | - | No | Attribute name |
| value | string | - | Yes | Attribute value |

### EMEvent
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| id | string | int64 | No | Primary key identifier |
| eventName | string | - | No | Name of the event |
| eventDescription | string | - | Yes | Description of the event |
| eventDescriptionMsgKey | string | - | Yes | Message key for event description |
| eventTime | string | DateTime | Yes | Time when the event occurred |
| eventType | string | - | Yes | Type of the event |
| eventAttributes | collection | - | - | Related event attributes |
| eventPayload | navigation | - | - | Related event payload |
| process | navigation | - | - | Related monitored process |

## Get Object Primary Keys

| Entity | Primary Key Field(s) |
|--------|---------------------|
| EMEventPayload | id |
| EMMonitoredProcess | processDefinitionId, processInstanceId, processType |
| EMEventAttribute | id |
| EMEvent | id |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| EMEventPayload | `snapshot` | No lastModifiedDateTime field available |
| EMMonitoredProcess | `cdc` | Has firstEventTime and lastEventTime for filtering |
| EMEventAttribute | `snapshot` | No timestamp fields available for incremental filtering |
| EMEvent | `cdc` | Has eventTime field for filtering changes |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All EMEventPayload Entities
```http
GET /EMEventPayload
```

### Get All EMMonitoredProcess Entities
```http
GET /EMMonitoredProcess
```

### Get All EMEventAttribute Entities
```http
GET /EMEventAttribute
```

### Get All EMEvent Entities
```http
GET /EMEvent
```

**Common Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $filter | string | Filter items by property values |
| $search | string | Search items by search phrases |
| $count | boolean | Include count of items |
| $orderby | array | Order items by property values |
| $select | array | Select properties to be returned |
| $expand | array | Expand related entities (EMEvent only) |

### Get Single Entity by Key

**EMEventPayload:**
```http
GET /EMEventPayload({id})
```

**EMMonitoredProcess:**
```http
GET /EMMonitoredProcess(processDefinitionId='{processDefinitionId}',processInstanceId='{processInstanceId}',processType='{processType}')
```

**EMEventAttribute:**
```http
GET /EMEventAttribute({id})
```

**EMEvent:**
```http
GET /EMEvent({id})
```

### EMEvent $expand Options
- eventAttributes
- eventPayload
- process
- * (all)

### Example Requests

**List all events with expanded attributes:**
```http
GET https://{api-server}/odata/v2/EMEvent?$expand=eventAttributes,eventPayload
```

**Filter events by time:**
```http
GET https://{api-server}/odata/v2/EMEvent?$filter=eventTime gt datetime'2024-01-01T00:00:00'
```

**Get monitored processes with errors:**
```http
GET https://{api-server}/odata/v2/EMMonitoredProcess?$filter=hasErrors eq 'true'
```

**Filter by lastEventTime for incremental sync:**
```http
GET https://{api-server}/odata/v2/EMMonitoredProcess?$filter=lastEventTime gt datetime'2024-01-01T00:00:00'&$orderby=lastEventTime asc
```

### Pagination
Use `$top` and `$skip` for pagination:
```http
GET /EMEvent?$top=100&$skip=0
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "id": "12345",
        "eventName": "ProcessStarted",
        "eventDescription": "Process has started",
        "eventTime": "/Date(1492098664000)/",
        "eventType": "INFO"
      }
    ]
  }
}
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| string (int64) | LongType |
| string (DateTime) | TimestampType |
| string (base64url) | BinaryType |

## Sources and References
- SAP SuccessFactors API Spec: PLTExecutionManager.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/65a0dc3b8a4942ba8de38fcfd064559c.html
