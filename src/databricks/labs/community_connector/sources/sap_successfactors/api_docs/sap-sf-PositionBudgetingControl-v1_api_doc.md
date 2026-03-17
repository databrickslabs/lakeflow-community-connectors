# Position Budgeting Control for Cloud API Documentation

## Authorization
- **Method**: Bearer Token (JWT)
- **Headers**:
  - `Authorization`: Bearer {jwt_token}
- **Security Scheme**: JWT Bearer Token (HTTP Bearer)

## Object List

| Tag/Resource | Description |
|--------------|-------------|
| Employee Financing | Operations related to employee financing data |
| Symbolic Accounts | Operations related to symbolic account mappings |
| Employee Groupings | Operations related to employee grouping rules |
| Replication Statuses | Operations related to replication statuses rules |

## Object Schema

### PBCReplicationData (Employee Financing)
| Field | Type | Description |
|-------|------|-------------|
| id | string | User ID (max 100 chars) |
| objectType | string | Object type (max 3 chars, e.g., "EMP") |
| startDate | date | Financing period start date |
| endDate | date | Financing period end date |
| companyCode | string | Company code |
| creationDate | date | Run creation date |
| status | string | Financing status (max 3 chars, e.g., "ERR") |
| payLocked | string | Payroll lock (max 1 char) |
| recalActive | string | Recalculation active (max 1 char) |
| financingStatusEffectiveDate | date | Financing status effective date |
| allDocsReleased | boolean | Overall commitment document completion indicator |
| replicationFinancingHeaders | array | List of financing headers |

### PBCReplicationFinancingHeader
| Field | Type | Description |
|-------|------|-------------|
| id | string | User ID |
| objectType | string | Object type |
| startDate | date | Start date |
| endDate | date | End date |
| hrDocRefNumber | string | HR Reference document number |
| earmarkedFundDocumentId | string | Reference Earmarked document number |
| status | string | Status |
| logicalSystemOfSourceDoc | string | Logical system of source document |
| documentType | string | Document type (e.g., "COM") |
| replicationFinancingItems | array | List of financing items |

### PBCReplicationFinancingItem
| Field | Type | Description |
|-------|------|-------------|
| id | string | User ID |
| objectType | string | Object type |
| startDate | date | Start date |
| endDate | date | End date |
| glAccount | string | GL Account |
| earmarkedFundsAmount | number | Earmarked funds amount |
| currency | string | Currency code |
| completionIndicator | string | Completion indicator |
| companyCode | string | Company code |
| costCenter | string | Cost center |
| wbsElement | string | WBS Element |
| grant | string | Grant |
| fund | string | Fund |
| budgetPeriod | string | Budget period |
| functionalArea | string | Functional area |
| fundsCenter | string | Funds center |
| hrDocRefItemId | string | HR document reference item ID |
| earmarkedFundDocumentItemId | string | Earmarked fund document item ID |
| hrDocReferenceId | string | HR document reference ID |

### SymbolicAccountData
| Field | Type | Description |
|-------|------|-------------|
| id | string | Unique identifier |
| countryCode | string | Country code |
| payComponentId | string | Pay component ID |
| accountAssignmentType | string | Account assignment type |
| validFrom | date | Valid from date |
| validTo | date | Valid to date |
| symbolicAccount | string | Symbolic account |
| lastModifiedTimestamp | datetime | Last modified timestamp |

### EmployeeGroupingData
| Field | Type | Description |
|-------|------|-------------|
| id | string | Unique identifier |
| countryCode | string | Country code |
| employeeClass | string | Employee class |
| employmentType | string | Employment type |
| validFrom | date | Valid from date |
| validTo | date | Valid to date |
| employeeGroup | string | Employee group |
| lastModifiedTimestamp | datetime | Last modified timestamp |

### ReplicationStatusesData
| Field | Type | Description |
|-------|------|-------------|
| objectId | string | Object ID |
| objectType | string | Object type |
| startDate | date | Start date |
| endDate | date | End date |
| status | string | Status |
| timestamp | datetime | Timestamp |
| allDocsReleased | boolean | All documents released indicator |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| Employee Financing | id + objectType + startDate + endDate |
| Symbolic Accounts | id |
| Employee Groupings | id |
| Replication Statuses | objectId + objectType + startDate + endDate |

## Object's Ingestion Type

| Object | Ingestion Type | Reasoning |
|--------|---------------|-----------|
| Employee Financing | `cdc` | Supports `lastKnownReplicationTimeStamp` for incremental reads |
| Symbolic Accounts | `cdc` | Has `lastModifiedTimestamp` and supports date range filtering (validFrom/validTo) |
| Employee Groupings | `cdc` | Has `lastModifiedTimestamp` and supports date range filtering (validFrom/validTo) |
| Replication Statuses | N/A | Write-only endpoint (PATCH) |

## Read API for Data Retrieval

### Get Employee Financing Data
- **Method**: GET
- **URL**: `https://{api-server}/rest/workforce/positionbudgetcontrol/v1/employeeFinancingData`
- **Query Parameters**:
  - `lastKnownReplicationTimeStamp` (optional): ISO 8601 datetime for incremental reads
  - `objectIds` (optional): Array of object IDs to filter
  - `financingPeriod` (optional): Filter by financing period
  - `$top` (optional, default: 100): Maximum number of records to return
  - `$skip` (optional, default: 0): Number of records to skip
- **Headers**:
  - `X-Correlation-ID`: Returned in response for request tracking
- **Example Request**:
```
GET https://api.successfactors.com/rest/workforce/positionbudgetcontrol/v1/employeeFinancingData?$top=100&$skip=0&lastKnownReplicationTimeStamp=2024-01-01T00:00:00Z
```
- **Example Response**:
```json
{
  "message": {
    "messageKey": "POSITION_BUDGET_CONTROL_REPLICATION_RECORDS_FOUND",
    "messageString": "The requested employee records were found."
  },
  "employeeFinancingList": [
    {
      "id": "Carla Grant",
      "objectType": "EMP",
      "startDate": "2024-04-01",
      "endDate": "2025-03-31",
      "companyCode": "1710",
      "status": "ERR"
    }
  ]
}
```

### Get Symbolic Accounts
- **Method**: GET
- **URL**: `https://{api-server}/rest/workforce/positionbudgetcontrol/v1/symbolicAccounts`
- **Query Parameters**:
  - `countryCode` (optional): Filter by country code
  - `payComponentId` (optional): Array of pay component IDs
  - `accountAssignmentType` (optional): Account assignment type filter
  - `validFrom` (optional): Valid from date filter
  - `validTo` (optional): Valid to date filter
  - `symbolicAccount` (optional): Array of symbolic accounts
- **Example Request**:
```
GET https://api.successfactors.com/rest/workforce/positionbudgetcontrol/v1/symbolicAccounts?countryCode=US
```

### Get Employee Groupings
- **Method**: GET
- **URL**: `https://{api-server}/rest/workforce/positionbudgetcontrol/v1/employeeGroupings`
- **Query Parameters**:
  - `countryCode` (optional): Filter by country code
  - `employeeClass` (optional): Array of employee classes
  - `employmentType` (optional): Array of employment types
  - `validFrom` (optional): Valid from date filter
  - `validTo` (optional): Valid to date filter
  - `employeeGroup` (optional): Array of employee groups
- **Example Request**:
```
GET https://api.successfactors.com/rest/workforce/positionbudgetcontrol/v1/employeeGroupings?countryCode=US
```

### Pagination
- Uses OData-style pagination with `$top` and `$skip` parameters
- Default page size: 100 (for employeeFinancingData)
- Increment `$skip` by `$top` value for next page

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer | IntegerType |
| int32 | IntegerType |
| int64 | LongType |
| number | DoubleType |
| boolean | BooleanType |
| date | DateType |
| date-time | TimestampType |
| array | ArrayType |
| object | StructType |

## Sources and References
- SAP SuccessFactors API Spec: sap-sf-PositionBudgetingControl-v1.json
- SAP Help Portal: https://help.sap.com/docs/successfactors-employee-central/implementing-position-budgeting-control-for-cloud/public-apis-for-position-budgeting-control-for-cloud
- API Server List: https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html
