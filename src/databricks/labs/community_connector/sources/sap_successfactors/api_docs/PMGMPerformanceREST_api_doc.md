# Performance Management API Documentation

## Authorization
- **Method**: Bearer Token Authentication (OAuth 2.0)
- **Security Scheme**: BearerAuth
- **Headers**:
  - `Authorization: Bearer <access_token>`
- **Token Endpoint**: Obtain token via SAP SuccessFactors OAuth 2.0 flow

## Object List
The API provides access to the following resources:

| Resource | Description |
|----------|-------------|
| Review Route Maps | Route map steps of a Performance Management form |

## Object Schema

### reviewRouteMap
| Field | Type | Description |
|-------|------|-------------|
| reviewId | integer (int64) | Form ID |
| modifySteps | array[reviewRouteStep] | Steps in the modify stage |
| signoffSteps | array[reviewRouteStep] | Steps in the signature stage |

### reviewRouteStep
| Field | Type | Description |
|-------|------|-------------|
| stepId | string | Route map step ID (Key field) |
| stepType | string | Step type and roles (e.g., "EM", "I E EM EX", "C E EM EX") |
| stepName | string | Step name |
| stepOrder | integer (int32) | Step order (e.g., 0) |
| current | boolean | Whether the step is the current step |
| completed | boolean | Whether the step is completed |
| status | string | Step status (e.g., "skipped") |
| userId | string | User ID in a step (e.g., "cgrant1") |
| subSteps | array[reviewRouteSubStep] | Substeps for iterative/collaboration steps |

### reviewRouteSubStep
| Field | Type | Description |
|-------|------|-------------|
| subStepOrder | integer (int32) | Substep order |
| entryUser | boolean | Whether user is the entry user of a step (iterative/collaboration only) |
| exitUser | boolean | Whether user is the exit user of a step (iterative/collaboration only) |
| userId | string | User ID in a step |
| userRole | string | Role in a multiple-role step (e.g., "EM") |

## Get Object Primary Keys
| Object | Primary Key Field(s) |
|--------|---------------------|
| reviewRouteMap | reviewId (path parameter) |
| reviewRouteStep | stepId |

## Object's Ingestion Type
| Object | Ingestion Type | Reasoning |
|--------|----------------|-----------|
| reviewRouteMap | `snapshot` | No timestamp filtering or incremental query support. Data retrieved by form ID only. |

**Notes**:
- The API retrieves route map steps for a specific form (reviewId)
- No cursor field or timestamp-based filtering is available
- Each request fetches the current state of the route map for the given form

## Read API for Data Retrieval

### Get Route Map Steps of a Form
- **Endpoint**: `GET /reviewRouteMaps/{reviewId}`
- **Base URL**: `https://{api-server}/rest/talent/performance/admin/v1`
- **Full URL**: `https://{api-server}/rest/talent/performance/admin/v1/reviewRouteMaps/{reviewId}`

#### Path Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| reviewId | string | Yes | Form ID |

#### Query Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| locale | string | No | User locale |

#### Example Request
```http
GET https://api.successfactors.com/rest/talent/performance/admin/v1/reviewRouteMaps/12345?locale=en_US
Authorization: Bearer <access_token>
```

#### Response Structure
```json
{
  "reviewId": 12345,
  "modifySteps": [
    {
      "stepId": "step1",
      "stepType": "EM",
      "stepName": "Employee Review",
      "stepOrder": 0,
      "current": true,
      "completed": false,
      "status": "in_progress",
      "userId": "emp001",
      "subSteps": []
    }
  ],
  "signoffSteps": [
    {
      "stepId": "step2",
      "stepType": "EM",
      "stepName": "Manager Signoff",
      "stepOrder": 1,
      "current": false,
      "completed": false,
      "status": "pending",
      "userId": "mgr001",
      "subSteps": []
    }
  ]
}
```

#### Pagination
- **Pagination**: Not supported
- This API retrieves all route map steps for a single form in one request

## Field Type Mapping
| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer (int32) | IntegerType |
| integer (int64) | LongType |
| boolean | BooleanType |
| array | ArrayType |
| object | StructType |

## Sources and References
- **SAP SuccessFactors API Spec**: PMGMPerformanceREST.json
- **SAP Help Portal**: [Performance Management API Documentation](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PERFORMANCE_AND_GOALS/8aaa9ac9177d482e8e6597e509237656/50786a0ba63d4d0cacbd4db7cca2f9d2.html?latest)
