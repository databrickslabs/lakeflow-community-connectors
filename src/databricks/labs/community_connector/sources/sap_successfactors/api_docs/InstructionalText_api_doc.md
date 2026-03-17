# Instructional Text API Documentation

## Authorization

- **Method**: OAuth 2.0 (Client Credentials Flow)
- **Token Endpoint**: `https://{api-server}/oauth/token`
- **Security Scheme**: sfOauth
- **Documentation**: [SuccessFactors OAuth 2.0](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/d9a9545305004187986c866de2b66987.html)

### OAuth Flow
1. Obtain access token using client credentials from the token endpoint
2. Include the access token in the Authorization header for API calls
3. Token format: `Bearer <access_token>`

## Object List

| Resource | Description |
|----------|-------------|
| Instructional Text | Localized instructional text from time profile objects for absence requests |

### Endpoints

| Path | Methods | Description |
|------|---------|-------------|
| `/rest/timemanagement/absence/v1/instructionalTextsForEmployeeSelfService` | GET | Get localized instructional text |

## Object Schema

### InstructionalTextEO

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| instructionalText | string | No | Localized instructional text content |

### Example Response
```json
{
  "instructionalText": "LocalizedInstructionalText"
}
```

## Get Object Primary Keys

This API does not return a traditional entity with primary keys. The instructional text is retrieved based on query parameters (date) and returns a single text value.

| Entity | Primary Key | Description |
|--------|-------------|-------------|
| InstructionalTextEO | N/A | Text content based on date parameter |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| InstructionalTextEO | `snapshot` | This is a lookup/reference API that returns instructional text based on a specific date. No incremental or CDC capabilities are available. Each request returns the current state for the given date. |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/rest/timemanagement/absence/v1
```

### Get Instructional Text for Employee Self Service
```
GET /instructionalTextsForEmployeeSelfService
```

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| $at | string | Yes | Date for which instructional text is requested. Format: YYYY-MM-DD |

### Example Request
```http
GET https://apisalesdemo2.successfactors.eu/rest/timemanagement/absence/v1/instructionalTextsForEmployeeSelfService?$at=2020-03-17
Authorization: Bearer <access_token>
Content-Type: application/json
```

### Example Response
```json
{
  "instructionalText": "Please enter your absence request with the appropriate time type. Ensure all required fields are completed before submission."
}
```

### Response Codes

| Code | Description |
|------|-------------|
| 200 | Success - Returns the instructional text |
| 400 | Bad Request - Invalid date format or missing required parameter |
| 403 | Forbidden - User does not have permission to access the resource |
| 404 | Not Found - No instructional text found for the specified date/profile |
| 500 | Internal Server Error |

### Error Response Example
```json
{
  "error": {
    "code": "NotFound",
    "message": "Record for entity TimeProfile with key 2020-03-17 does not exist. Please check the key value.",
    "details": [
      {
        "code": "COE0028",
        "message": "Record for entity TimeProfile with key 2020-03-17 does not exist. Please check the key value."
      }
    ]
  }
}
```

## Pagination

This API does not support pagination as it returns a single instructional text record based on the date parameter.

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |

## Sources and References

- **SAP SuccessFactors API Spec**: InstructionalText.json
- **SAP Help Portal**: [SAP SuccessFactors Time Management](https://help.sap.com/docs/SAP_SUCCESSFACTORS_EMPLOYEE_CENTRAL/68de09dff990417b9f0acf6ccc13a14d/867005999407412aa6ae8bb65138c000.html)
