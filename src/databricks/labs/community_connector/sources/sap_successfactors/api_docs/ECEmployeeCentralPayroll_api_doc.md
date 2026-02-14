# Employee Central Payroll API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- EmployeePayrollRunResultsItems
- EmployeePayrollRunResults

## Object Schema

### EmployeePayrollRunResultsItems

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| EmployeePayrollRunResults_externalCode | string | 128 | Parent payroll run external code (key) |
| EmployeePayrollRunResults_mdfSystemEffectiveStartDate | datetime | - | Parent effective start date (key) |
| externalCode | string | 128 | Item external code (key) |
| amount | decimal | - | Amount value |
| createdBy | string | 255 | Created by user |
| createdDate | datetime | - | Creation date |
| createdDateTime | datetime | - | Creation timestamp |
| endDateWhenEarned | datetime | - | End date when earned |
| externalName | string | 128 | External name |
| groupingReason | string | 128 | Grouping reason |
| lastModifiedBy | string | 255 | Last modified by user |
| lastModifiedDate | datetime | - | Last modified date |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| lastModifiedDateWithTZ | datetime | - | Last modified with timezone |
| mdfSystemEffectiveEndDate | datetime | - | MDF effective end date |
| mdfSystemEffectiveStartDate | datetime | - | MDF effective start date |
| mdfSystemEntityId | string | 255 | MDF entity ID |
| mdfSystemObjectType | string | 255 | MDF object type |
| mdfSystemRecordId | string | 255 | MDF record ID |
| mdfSystemRecordStatus | string | 255 | MDF record status |
| mdfSystemStatus | string | 255 | MDF system status |
| mdfSystemTransactionSequence | int64 | - | MDF transaction sequence |
| mdfSystemVersionId | int64 | - | MDF version ID |
| payrollProviderGroupingReason | string | 255 | Provider grouping reason |
| payrollProviderGroupingValue | string | 255 | Provider grouping value |
| payrollProviderUnitOfMeasurement | string | 255 | Provider unit of measurement |
| payrollProviderWageType | string | 255 | Provider wage type |
| quantity | decimal | - | Quantity value |
| startDateWhenEarned | datetime | - | Start date when earned |
| unitOfMeasurement | string | 128 | Unit of measurement |
| wageType | string | 128 | Wage type |

### EmployeePayrollRunResults

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| externalCode | string | 128 | External code (key) |
| mdfSystemEffectiveStartDate | datetime | - | Effective start date (key) |
| clientId | string | 255 | Client ID |
| companyId | string | 128 | Company ID |
| createdBy | string | 255 | Created by user |
| createdDate | datetime | - | Creation date |
| createdDateTime | datetime | - | Creation timestamp |
| currency | string | 128 | Currency code |
| employmentId | string | 255 | Employment ID |
| endDateWhenPaid | datetime | - | End date when paid |
| externalName | string | 128 | External name |
| isVoid | boolean | - | Is void flag |
| lastModifiedBy | string | 255 | Last modified by user |
| lastModifiedDate | datetime | - | Last modified date |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| lastModifiedDateWithTZ | datetime | - | Last modified with timezone |
| mdfSystemEffectiveEndDate | datetime | - | MDF effective end date |
| mdfSystemEntityId | string | 255 | MDF entity ID |
| mdfSystemObjectType | string | 255 | MDF object type |
| mdfSystemRecordId | string | 255 | MDF record ID |
| mdfSystemRecordStatus | string | 255 | MDF record status |
| mdfSystemStatus | string | 255 | MDF system status |
| mdfSystemTransactionSequence | int64 | - | MDF transaction sequence |
| mdfSystemVersionId | int64 | - | MDF version ID |
| payDate | datetime | - | Pay date |
| payrollId | string | 255 | Payroll ID |
| payrollProviderId | string | 255 | Payroll provider ID |
| payrollProviderPayrollRunType | string | 255 | Provider payroll run type |
| payrollRunType | string | 128 | Payroll run type |
| personId | string | 255 | Person ID |
| sequenceNumber | string | 255 | Sequence number |
| startDateWhenPaid | datetime | - | Start date when paid |
| systemId | string | 255 | System ID |
| userId | string | 100 | User ID |

**Related Entities:**
- employeePayrollRunResultsItems -> EmployeePayrollRunResultsItems

## Get Object Primary Keys

### EmployeePayrollRunResultsItems
- `EmployeePayrollRunResults_externalCode` (string)
- `EmployeePayrollRunResults_mdfSystemEffectiveStartDate` (datetime)
- `externalCode` (string)

### EmployeePayrollRunResults
- `externalCode` (string)
- `mdfSystemEffectiveStartDate` (datetime)

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| EmployeePayrollRunResultsItems | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| EmployeePayrollRunResults | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Entity Endpoints

#### EmployeePayrollRunResultsItems
```http
GET /EmployeePayrollRunResultsItems
GET /EmployeePayrollRunResultsItems(EmployeePayrollRunResults_externalCode='{externalCode}',EmployeePayrollRunResults_mdfSystemEffectiveStartDate={date},externalCode='{itemExternalCode}')
```

#### EmployeePayrollRunResults
```http
GET /EmployeePayrollRunResults
GET /EmployeePayrollRunResults(externalCode='{externalCode}',mdfSystemEffectiveStartDate={date})
```

### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| $top | integer | 20 | Maximum number of records to return |
| $skip | integer | - | Number of records to skip |
| $filter | string | - | OData filter expression |
| $orderby | string[] | - | Sort order for results |
| $select | string[] | - | Fields to include in response |
| $count | boolean | - | Include total count of records |
| $search | string | - | Search items by phrases |
| $expand | string[] | - | Expand related entities (EmployeePayrollRunResults only) |

### Available $orderby Fields

**EmployeePayrollRunResultsItems:**
- EmployeePayrollRunResults_externalCode
- EmployeePayrollRunResults_mdfSystemEffectiveStartDate
- amount, quantity
- createdBy, createdDate, createdDateTime
- endDateWhenEarned, startDateWhenEarned
- externalCode, externalName
- groupingReason
- lastModifiedBy, lastModifiedDate, lastModifiedDateTime, lastModifiedDateWithTZ
- mdfSystemEffectiveEndDate, mdfSystemEffectiveStartDate
- mdfSystemEntityId, mdfSystemObjectType, mdfSystemRecordId
- mdfSystemRecordStatus, mdfSystemStatus
- mdfSystemTransactionSequence, mdfSystemVersionId
- payrollProviderGroupingReason, payrollProviderGroupingValue
- payrollProviderUnitOfMeasurement, payrollProviderWageType
- unitOfMeasurement, wageType

**EmployeePayrollRunResults:**
- clientId, companyId
- createdBy, createdDate, createdDateTime
- currency
- employmentId
- endDateWhenPaid, startDateWhenPaid
- externalCode, externalName
- isVoid
- lastModifiedBy, lastModifiedDate, lastModifiedDateTime, lastModifiedDateWithTZ
- mdfSystemEffectiveEndDate, mdfSystemEffectiveStartDate
- mdfSystemEntityId, mdfSystemObjectType, mdfSystemRecordId
- mdfSystemRecordStatus, mdfSystemStatus
- mdfSystemTransactionSequence, mdfSystemVersionId
- payDate
- payrollId, payrollProviderId, payrollProviderPayrollRunType, payrollRunType
- personId
- sequenceNumber
- systemId, userId

### Example Requests

**Get all payroll run results with pagination:**
```http
GET /EmployeePayrollRunResults?$top=100&$skip=0
```

**Get payroll results modified after a specific date (CDC):**
```http
GET /EmployeePayrollRunResults?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get payroll results with line items:**
```http
GET /EmployeePayrollRunResults?$expand=employeePayrollRunResultsItems
```

**Get payroll results for a specific user:**
```http
GET /EmployeePayrollRunResults?$filter=userId eq 'USER001'
```

**Get payroll results by pay date range:**
```http
GET /EmployeePayrollRunResults?$filter=payDate ge datetime'2024-01-01T00:00:00' and payDate le datetime'2024-12-31T00:00:00'
```

**Get payroll results by company:**
```http
GET /EmployeePayrollRunResults?$filter=companyId eq 'COMPANY001'
```

**Get non-void payroll results:**
```http
GET /EmployeePayrollRunResults?$filter=isVoid eq false
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "externalCode": "PRR001",
        "mdfSystemEffectiveStartDate": "/Date(1609459200000)/",
        "clientId": "CLIENT001",
        "companyId": "COMPANY001",
        "currency": "USD",
        "employmentId": "EMP001",
        "payDate": "/Date(1609459200000)/",
        "payrollRunType": "REGULAR",
        "userId": "USER001",
        "isVoid": false,
        "lastModifiedDateTime": "/Date(1609459200000)/",
        "employeePayrollRunResultsItems": {
          "results": [
            {
              "EmployeePayrollRunResults_externalCode": "PRR001",
              "externalCode": "ITEM001",
              "amount": "5000.00",
              "wageType": "SALARY",
              "quantity": "1"
            }
          ]
        }
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
| string (maxLength) | StringType | String with length constraint |
| decimal | DecimalType | Monetary and numeric values |
| int64 | LongType | Large integers |
| boolean | BooleanType | True/false values |
| datetime (/Date(...)/) | TimestampType | SAP OData datetime format |

## Sources and References
- SAP SuccessFactors API Spec: ECEmployeeCentralPayroll.json
- SAP Help Portal: [Employee Central Payroll on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/01ffc910ca4845caac0547a4fbed67e8.html)
