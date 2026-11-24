# Budget Period API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- Budget Period (BudgetPeriodGO)

## Object Schema

### BudgetPeriodGO

| Field Name | Type | Max Length | Required | Read Only | Description |
|------------|------|------------|----------|-----------|-------------|
| budgetPeriodId | string | 10 | Yes | No | Budget period identifier |
| effectiveStartDate | datetime | - | Yes | No | Effective start date |
| effectiveStatus | string (enum: A, I) | 128 | Yes | No | Status: A - Active, I - InActive |
| budgetPeriodDescription_defaultValue | string | 255 | No | No | Default description value |
| budgetPeriodDescription_en_US | string | 255 | No | No | English (US) description |
| budgetPeriodDescription_localized | string | 255 | No | No | Localized description |
| budgetPeriodExpirationDate | datetime | - | No | No | Budget period expiration date |
| createdBy | string | 100 | No | Yes | User who created the record |
| createdDateTime | datetime | - | No | Yes | Creation date and time |
| effectiveEndDate | datetime | - | No | Yes | Effective end date |
| entityOID | string | 70 | No | Yes | Entity OID |
| entityUUID | string | 70 | No | Yes | Entity UUID |
| fbudgetPeriodPeriodicity | string | 10 | No | No | Budget period periodicity |
| lastModifiedBy | string | 100 | No | Yes | User who last modified |
| lastModifiedDateTime | datetime | - | No | Yes | Last modification date and time |
| mdfSystemRecordStatus | string (enum) | 255 | No | Yes | Record status: C - Correction, D - Soft deleted, F - Full Purge Import, N - Normal, P - Pending, PH - Pending history |
| reversalDate | datetime | - | No | No | Reversal date |

## Get Object Primary Keys

### BudgetPeriodGO
- `budgetPeriodId` (string)
- `effectiveStartDate` (datetime)

Composite key format: `BudgetPeriodGO(budgetPeriodId='{budgetPeriodId}',effectiveStartDate=datetime'{effectiveStartDate}')`

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| BudgetPeriodGO | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime for incremental data retrieval |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2/BudgetPeriodGO
```

### Get All Budget Periods
```http
GET /BudgetPeriodGO
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| $top | integer | 20 | Maximum number of records to return |
| $skip | integer | - | Number of records to skip |
| $filter | string | - | OData filter expression |
| $orderby | string[] | - | Sort order for results |
| $select | string[] | - | Fields to include in response |
| $count | boolean | - | Include total count of records |

**Available $orderby Fields:**
- budgetPeriodDescription_defaultValue, budgetPeriodDescription_en_US
- budgetPeriodExpirationDate, budgetPeriodId
- createdBy, createdDateTime
- effectiveEndDate, effectiveStartDate, effectiveStatus
- entityOID, entityUUID
- fbudgetPeriodPeriodicity
- lastModifiedBy, lastModifiedDateTime
- mdfSystemRecordStatus, reversalDate

**Available $select Fields:**
- budgetPeriodDescription_defaultValue, budgetPeriodDescription_en_US, budgetPeriodDescription_localized
- budgetPeriodExpirationDate, budgetPeriodId
- createdBy, createdDateTime
- effectiveEndDate, effectiveStartDate, effectiveStatus
- entityOID, entityUUID
- fbudgetPeriodPeriodicity
- lastModifiedBy, lastModifiedDateTime
- mdfSystemRecordStatus, reversalDate

### Get Budget Period by Key
```http
GET /BudgetPeriodGO(budgetPeriodId='{budgetPeriodId}',effectiveStartDate=datetime'{effectiveStartDate}')
```

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| budgetPeriodId | string | Yes | Budget period identifier |
| effectiveStartDate | datetime | Yes | Format: YYYY-MM-DDTHH:MM:SS |

### Example Requests

**Get all budget periods with pagination:**
```http
GET /BudgetPeriodGO?$top=100&$skip=0
```

**Get budget periods modified after a specific date (CDC):**
```http
GET /BudgetPeriodGO?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get specific fields only:**
```http
GET /BudgetPeriodGO?$select=budgetPeriodId,effectiveStartDate,effectiveStatus,lastModifiedDateTime
```

**Get active budget periods:**
```http
GET /BudgetPeriodGO?$filter=effectiveStatus eq 'A'
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "budgetPeriodId": "BP001",
        "effectiveStartDate": "/Date(1609459200000)/",
        "effectiveStatus": "A",
        "budgetPeriodDescription_defaultValue": "Budget Period 2021",
        "lastModifiedDateTime": "/Date(1609459200000)/",
        ...
      }
    ]
  }
}
```

### Pagination
- Default page size: 20 records
- Use `$top` and `$skip` for pagination
- Iterate with increasing `$skip` until no more results

## Field Type Mapping

| API Type | Spark Type | Notes |
|----------|------------|-------|
| string | StringType | Basic string fields |
| string (enum) | StringType | Enumerated values stored as strings |
| datetime (/Date(...)/) | TimestampType | SAP OData datetime format |
| integer | IntegerType | Not used in this entity |
| boolean | BooleanType | Not used in this entity |

## Sources and References
- SAP SuccessFactors API Spec: BudgetPeriodGO.json
- SAP Help Portal: [OData API: Budget Period](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/b2b06831c2cb4d5facd1dfde49a7aab5/31287f1a79e944f387f904916d8e77f2.html)
