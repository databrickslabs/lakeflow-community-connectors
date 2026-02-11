# Onboarding and Offboarding Custom Tasks API Documentation

## Authorization
- **Method**: X.509 Certificate Authentication
- **Security Scheme**: BasicAuth with X.509
- **Headers**:
  - Certificate-based authentication required
- **Notes**: Authentication uses X.509 certificates as specified in the security scheme

## Object List
The API provides access to the following resources:

| Resource | Description |
|----------|-------------|
| Custom Tasks | Onboarding and Offboarding custom tasks for third-party integration |

## Object Schema

### customTasks
| Field | Type | Description |
|-------|------|-------------|
| id | string | ID of Custom Task |
| title | string | Title of Custom Task |
| config | string | Config ID |
| status | string | Status of Task (e.g., "OPEN", "COMPLETED") |
| user | object (user) | User associated with the task |
| processId | string | Onboarding Process ID |
| dueDate | string (date) | Due Date |
| optional | boolean | Whether task is optional |
| responsibleUsers | array[user] | Responsible Users |
| completedDate | string (date-time) | Completed Date |
| completedBy | object (user) | User who completed the task |
| customResource | object (customResource) | Custom MDF resource information |

### user
| Field | Type | Description |
|-------|------|-------------|
| id | string | User ID |
| displayName | string | Display Name |

### customResource
| Field | Type | Description |
|-------|------|-------------|
| id | string | Custom MDF Internal ID |
| type | string | Custom MDF Type |

### error
| Field | Type | Description |
|-------|------|-------------|
| code | string | Error Code |
| message | string | Detailed Error Message |

## Get Object Primary Keys
| Object | Primary Key Field(s) |
|--------|---------------------|
| customTasks | id (taskDataId) |

## Object's Ingestion Type
| Object | Ingestion Type | Reasoning |
|--------|----------------|-----------|
| customTasks | `snapshot` | No timestamp filtering available. Filter only by processId. Each query returns current state of tasks. |

**Notes**:
- Tasks can be filtered by processId to get all tasks for a specific onboarding/offboarding process
- No incremental or timestamp-based querying is supported
- Status field can be used to track task completion but not for incremental data retrieval

## Read API for Data Retrieval

### Get Custom Task by ID
- **Endpoint**: `GET /rest/onboarding/customtasks/v2/customTasks/{taskDataId}`
- **Base URL**: `https://{api-server}`
- **Full URL**: `https://{api-server}/rest/onboarding/customtasks/v2/customTasks/{taskDataId}`

#### Path Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| taskDataId | string | Yes | ID of Custom Task |

#### Example Request
```http
GET https://apisalesdemo2.successfactors.com/rest/onboarding/customtasks/v2/customTasks/task123
```

#### Response Structure
```json
{
  "id": "task123",
  "title": "Complete Background Check",
  "config": "config001",
  "status": "OPEN",
  "user": {
    "id": "user001",
    "displayName": "John Doe"
  },
  "processId": "process001",
  "dueDate": "2024-01-15",
  "optional": false,
  "responsibleUsers": [
    {
      "id": "hr001",
      "displayName": "HR Manager"
    }
  ],
  "completedDate": null,
  "completedBy": null,
  "customResource": {
    "id": "mdf001",
    "type": "BackgroundCheck"
  }
}
```

### Get Custom Tasks by Process ID
- **Endpoint**: `GET /rest/onboarding/customtasks/v2/customTasks`
- **Base URL**: `https://{api-server}`
- **Full URL**: `https://{api-server}/rest/onboarding/customtasks/v2/customTasks?$filter=processId eq '{processId}'`

#### Query Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| $filter | string | No | Filter by processId (e.g., `processId eq 'process001'`) |

#### Example Request
```http
GET https://apisalesdemo2.successfactors.com/rest/onboarding/customtasks/v2/customTasks?$filter=processId eq 'process001'
```

#### Response Structure
```json
{
  "value": [
    {
      "id": "task123",
      "title": "Complete Background Check",
      "config": "config001",
      "status": "OPEN",
      "user": {
        "id": "user001",
        "displayName": "John Doe"
      },
      "processId": "process001",
      "dueDate": "2024-01-15",
      "optional": false,
      "responsibleUsers": [],
      "completedDate": null,
      "completedBy": null,
      "customResource": null
    }
  ]
}
```

#### Pagination
- **Pagination**: Not explicitly defined in the spec
- Results are returned in a `value` array

## Field Type Mapping
| API Type | Spark Type |
|----------|------------|
| string | StringType |
| string (date) | DateType |
| string (date-time) | TimestampType |
| boolean | BooleanType |
| array | ArrayType |
| object | StructType |

## Sources and References
- **SAP SuccessFactors API Spec**: sap-sf-customTasks-v2.json
- **SAP Help Portal**: [ONB Custom Tasks API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_RELEASE_INFORMATION/8e0d540f96474717bbf18df51e54e522/a262abc93c9b49ae8ef8a0e6653f5f48.html)
