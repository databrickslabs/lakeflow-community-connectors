# Higher Duty Employment API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Object Name | Description |
|-------------|-------------|
| EmpEmploymentHigherDuty | Employment higher duty data for a person |

## Object Schema

### EmpEmploymentHigherDuty

| Field Name | Type | Required | Description |
|------------|------|----------|-------------|
| personIdExternal | string | Yes | External person identifier |
| userId | string | Yes | User identifier |
| homeAssignment | string | Yes | Home assignment identifier |
| plannedEndDate | string (date-time) | Yes | Planned end date of higher duty assignment |
| startDate | string (date-time) | Yes | Start date of higher duty assignment |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| EmpEmploymentHigherDuty | personIdExternal, userId, homeAssignment, startDate |

**Note**: The composite key consists of `personIdExternal`, `userId`, `homeAssignment`, and `startDate` based on the required fields in the schema.

## Object's Ingestion Type

| Object | Ingestion Type | Rationale |
|--------|---------------|-----------|
| EmpEmploymentHigherDuty | `snapshot` | No lastModifiedDateTime field available for incremental filtering. The API supports basic $filter but lacks timestamp-based change tracking. |

## Read API for Data Retrieval

### Get All EmpEmploymentHigherDuty Records

**Endpoint**: `GET https://{api-server}/odata/v2/EmpEmploymentHigherDuty`

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| $filter | string | Filter items by property values |
| $select | string | Select properties to be returned |
| $orderby | string | Order items by property values |
| $top | integer | Show only the first n items |
| $skip | integer | Skip the first n items |

**Example Requests**:

```http
# Get all higher duty employment records
GET https://{api-server}/odata/v2/EmpEmploymentHigherDuty

# Get with pagination
GET https://{api-server}/odata/v2/EmpEmploymentHigherDuty?$top=100&$skip=0

# Filter by userId
GET https://{api-server}/odata/v2/EmpEmploymentHigherDuty?$filter=userId eq '1807'

# Select specific fields
GET https://{api-server}/odata/v2/EmpEmploymentHigherDuty?$select=personIdExternal,userId,startDate,plannedEndDate

# Order by start date
GET https://{api-server}/odata/v2/EmpEmploymentHigherDuty?$orderby=startDate desc
```

**Response Structure**:
```json
{
  "d": {
    "results": [
      {
        "personIdExternal": "emp3fd",
        "userId": "1807",
        "homeAssignment": "emp3fd",
        "plannedEndDate": "/Date(1748640000000)/",
        "startDate": "/Date(1748131200000)/"
      }
    ]
  }
}
```

**Pagination Approach**:
- Use `$top` and `$skip` for offset-based pagination
- Default page size is typically 20 records
- Continue fetching until results array is empty or less than $top

**HTTP Response Codes**:

| Code | Description |
|------|-------------|
| 200 | Success |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | Not Found |
| 500 | Internal Server Error |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| string (date-time) | TimestampType |

## Sources and References
- SAP SuccessFactors API Spec: sap-sf-EmpEmploymentHigherDuty-v1.json
- SAP Help Portal: https://help.sap.com/docs/successfactors-platform/sap-successfactors-api-reference-guide-odata-v2/empemploymenthigherduty
