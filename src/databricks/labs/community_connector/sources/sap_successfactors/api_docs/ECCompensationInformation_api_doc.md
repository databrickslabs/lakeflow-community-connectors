# Compensation Information API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- OneTimeDeduction
- RecurringDeductionItem
- EmpCompensationCalculated
- EmpPayCompRecurring
- DeductionScreenId
- RecurringDeduction
- EmpCompensation
- EmpPayCompNonRecurring
- EmpCompensationGroupSumCalculated

## Object Schema

### OneTimeDeduction

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| externalCode | int64 | - | External code (key) |
| additionalInfo | string | 40 | Additional information |
| advanceId | string | 128 | Advance identifier |
| amount | decimal | - | Deduction amount |
| auditUserSysId | string | 100 | Audit user system ID |
| createdBy | string | 255 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| currency | string | 128 | Currency code |
| deductionDate | datetime | - | Deduction date |
| equivalentAmount | decimal | - | Equivalent amount |
| lastModifiedBy | string | 255 | Last modified by user |
| lastModifiedDate | datetime | - | Last modified date |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| lastModifiedDateWithTZ | datetime | - | Last modified with timezone |
| mdfSystemCreatedBy | string | 255 | MDF created by |
| mdfSystemCreatedDate | datetime | - | MDF creation date |
| mdfSystemEffectiveEndDate | datetime | - | MDF effective end date |
| mdfSystemEffectiveStartDate | datetime | - | MDF effective start date |
| mdfSystemEntityId | string | 255 | MDF entity ID |
| mdfSystemLastModifiedBy | string | 255 | MDF last modified by |
| mdfSystemObjectType | string | 255 | MDF object type |
| mdfSystemRecordId | string | 255 | MDF record ID |
| mdfSystemRecordStatus | string | 255 | MDF record status |
| mdfSystemStatus | string | 255 | MDF system status |
| mdfSystemTransactionSequence | int64 | - | MDF transaction sequence |
| mdfSystemVersionId | int64 | - | MDF version ID |
| payComponentType | string | 32 | Pay component type |
| referenceId | string | 20 | Reference ID |
| unitOfMeasure | string | 128 | Unit of measure |
| userSysId | string | 100 | User system ID |

### RecurringDeductionItem

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| RecurringDeduction_effectiveStartDate | datetime | - | Parent effective start date (key) |
| RecurringDeduction_userSysId | string | 100 | Parent user system ID (key) |
| payComponentType | string | 32 | Pay component type (key) |
| additionalInfo | string | 40 | Additional information |
| advanceId | string | 128 | Advance identifier |
| amount | decimal | - | Deduction amount |
| createdBy | string | 255 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| currency | string | 128 | Currency code |
| editPermission | string | 255 | Edit permission |
| endDate | datetime | - | End date |
| equivalentAmount | decimal | - | Equivalent amount |
| frequency | string | 32 | Frequency |
| lastModifiedBy | string | 255 | Last modified by user |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| mdfSystemCreatedBy | string | 255 | MDF created by |
| mdfSystemCreatedDate | datetime | - | MDF creation date |
| mdfSystemEffectiveEndDate | datetime | - | MDF effective end date |
| mdfSystemEffectiveStartDate | datetime | - | MDF effective start date |
| mdfSystemEntityId | string | 255 | MDF entity ID |
| mdfSystemLastModifiedBy | string | 255 | MDF last modified by |
| mdfSystemLastModifiedDate | datetime | - | MDF last modified date |
| mdfSystemLastModifiedDateWithTZ | datetime | - | MDF last modified with timezone |
| mdfSystemObjectType | string | 255 | MDF object type |
| mdfSystemRecordId | string | 255 | MDF record ID |
| mdfSystemRecordStatus | string | 255 | MDF record status |
| mdfSystemStatus | string | 255 | MDF system status |
| mdfSystemTransactionSequence | int64 | - | MDF transaction sequence |
| mdfSystemVersionId | int64 | - | MDF version ID |
| referenceId | string | 20 | Reference ID |
| unitOfMeasure | string | 128 | Unit of measure |

### EmpCompensationCalculated

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| seqNumber | int64 | - | Sequence number (key) |
| startDate | datetime | - | Start date (key) |
| userId | string | - | User ID (key) |
| compaRatio | double | - | Compa-ratio |
| currency | string | - | Currency |
| errorCode | string | - | Error code |
| errorMessage | string | - | Error message |
| payRange | string | - | Pay range |
| proratedMaxPointOfPayRange | decimal | - | Prorated max pay range |
| proratedMidPointOfPayRange | decimal | - | Prorated mid pay range |
| proratedMinPointOfPayRange | decimal | - | Prorated min pay range |
| rangePenetration | double | - | Range penetration |
| yearlyBaseSalary | decimal | - | Yearly base salary |

### EmpPayCompRecurring

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| seqNumber | int64 | - | Sequence number (key) |
| startDate | datetime | - | Start date (key) |
| userId | string | 100 | User ID (key) |
| payComponent | string | - | Pay component (key) |
| createdBy | string | 100 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| createdOn | datetime | - | Created on date |
| currencyCode | string | 32 | Currency code |
| endDate | datetime | - | End date |
| frequency | string | 30 | Frequency |
| lastModifiedBy | string | 100 | Last modified by user |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| lastModifiedOn | datetime | - | Last modified on date |
| notes | string | 4000 | Notes |
| operation | string | - | Operation |
| paycompvalue | double | - | Pay component value |

### DeductionScreenId

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| externalCode | string | 128 | External code (key) |
| createdBy | string | 255 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| dummyFieldValue | string | 255 | Dummy field value |
| lastModifiedBy | string | 255 | Last modified by user |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| mdfSystemCreatedBy | string | 255 | MDF created by |
| mdfSystemCreatedDate | datetime | - | MDF creation date |
| mdfSystemEffectiveEndDate | datetime | - | MDF effective end date |
| mdfSystemEffectiveStartDate | datetime | - | MDF effective start date |
| mdfSystemEntityId | string | 255 | MDF entity ID |
| mdfSystemLastModifiedBy | string | 255 | MDF last modified by |
| mdfSystemLastModifiedDate | datetime | - | MDF last modified date |
| mdfSystemLastModifiedDateWithTZ | datetime | - | MDF last modified with timezone |
| mdfSystemObjectType | string | 255 | MDF object type |
| mdfSystemRecordId | string | 255 | MDF record ID |
| mdfSystemRecordStatus | string | 255 | MDF record status |
| mdfSystemStatus | string | 255 | MDF system status |
| mdfSystemTransactionSequence | int64 | - | MDF transaction sequence |
| mdfSystemVersionId | int64 | - | MDF version ID |
| onetimeDeductionId | string | 255 | One-time deduction ID |
| onetimeDeductionUserGoAdminId | string | 255 | Admin deduction ID |
| onetimeDeductionUserGoEmployeeEditId | string | 255 | Employee edit deduction ID |
| onetimeDeductionUserGoEmployeeId | string | 255 | Employee deduction ID |
| recurringDeductionId | string | 255 | Recurring deduction ID |

### RecurringDeduction

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| effectiveStartDate | datetime | - | Effective start date (key) |
| userSysId | string | 100 | User system ID (key) |
| createdBy | string | 255 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| effectiveEndDate | datetime | - | Effective end date |
| lastModifiedBy | string | 255 | Last modified by user |
| lastModifiedDate | datetime | - | Last modified date |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| lastModifiedDateWithTZ | datetime | - | Last modified with timezone |
| mdfSystemCreatedBy | string | 255 | MDF created by |
| mdfSystemCreatedDate | datetime | - | MDF creation date |
| mdfSystemEntityId | string | 255 | MDF entity ID |
| mdfSystemLastModifiedBy | string | 255 | MDF last modified by |
| mdfSystemObjectType | string | 255 | MDF object type |
| mdfSystemRecordId | string | 255 | MDF record ID |
| mdfSystemRecordStatus | string | 255 | MDF record status |
| mdfSystemStatus | string | 255 | MDF system status |
| mdfSystemTransactionSequence | int64 | - | MDF transaction sequence |
| mdfSystemVersionId | int64 | - | MDF version ID |

**Related Entities:**
- recurringItems -> RecurringDeductionItem

### EmpCompensation

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| startDate | datetime | - | Start date (key) |
| userId | string | 100 | User ID (key) |
| benefitsRate | double | - | Benefits rate |
| bonusTarget | double | - | Bonus target |
| createdBy | string | 100 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| createdOn | datetime | - | Created on date |
| customDouble5 | double | - | Custom double field 5 |
| customDouble6 | double | - | Custom double field 6 |
| customDouble7 | double | - | Custom double field 7 |
| customString20 | string | 256 | Custom string field 20 |
| endDate | datetime | - | End date |
| event | string | 45 | Event |
| eventReason | string | - | Event reason |
| isEligibleForBenefits | boolean | - | Eligible for benefits flag |
| isEligibleForCar | boolean | - | Eligible for car flag |
| isHighlyCompensatedEmployee | boolean | - | Highly compensated flag |
| isInsider | boolean | - | Insider flag |
| lastModifiedBy | string | 100 | Last modified by user |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| lastModifiedOn | datetime | - | Last modified on date |
| payGrade | string | 256 | Pay grade |
| payGroup | string | 32 | Pay group |
| payrollSystemId | string | 32 | Payroll system ID |

**Related Entities:**
- empCompensationCalculatedNav -> EmpCompensationCalculated
- empCompensationGroupSumCalculatedNav -> EmpCompensationGroupSumCalculated
- empPayCompRecurringNav -> EmpPayCompRecurring

### EmpPayCompNonRecurring

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| payComponentCode | string | - | Pay component code (key) |
| payDate | datetime | - | Pay date (key) |
| userId | string | 100 | User ID (key) |
| alternativeCostCenter | string | 32 | Alternative cost center |
| createdBy | string | 100 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| createdOn | datetime | - | Created on date |
| currencyCode | string | 20 | Currency code |
| lastModifiedBy | string | 100 | Last modified by user |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| lastModifiedOn | datetime | - | Last modified on date |
| notes | string | 4000 | Notes |
| operation | string | - | Operation |
| value | double | - | Value |

### EmpCompensationGroupSumCalculated

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| seqNumber | int64 | - | Sequence number (key) |
| startDate | datetime | - | Start date (key) |
| userId | string | - | User ID (key) |
| amount | double | - | Amount |
| currencyCode | string | - | Currency code |
| errorCode | string | - | Error code |
| errorMessage | string | - | Error message |
| payComponentGroupId | string | - | Pay component group ID |

## Get Object Primary Keys

### OneTimeDeduction
- `externalCode` (int64)

### RecurringDeductionItem
- `RecurringDeduction_effectiveStartDate` (datetime)
- `RecurringDeduction_userSysId` (string)
- `payComponentType` (string)

### EmpCompensationCalculated
- `seqNumber` (int64)
- `startDate` (datetime)
- `userId` (string)

### EmpPayCompRecurring
- `seqNumber` (int64)
- `startDate` (datetime)
- `userId` (string)
- `payComponent` (string)

### DeductionScreenId
- `externalCode` (string)

### RecurringDeduction
- `effectiveStartDate` (datetime)
- `userSysId` (string)

### EmpCompensation
- `startDate` (datetime)
- `userId` (string)

### EmpPayCompNonRecurring
- `payComponentCode` (string)
- `payDate` (datetime)
- `userId` (string)

### EmpCompensationGroupSumCalculated
- `seqNumber` (int64)
- `startDate` (datetime)
- `userId` (string)

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| OneTimeDeduction | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| RecurringDeductionItem | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| EmpCompensationCalculated | `snapshot` | - | Read-only calculated entity, no modification tracking |
| EmpPayCompRecurring | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| DeductionScreenId | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| RecurringDeduction | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| EmpCompensation | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| EmpPayCompNonRecurring | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| EmpCompensationGroupSumCalculated | `snapshot` | - | Read-only calculated entity, no modification tracking |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Entity Endpoints

#### OneTimeDeduction
```http
GET /OneTimeDeduction
GET /OneTimeDeduction({externalCode})
```

#### RecurringDeductionItem
```http
GET /RecurringDeductionItem
GET /RecurringDeductionItem(RecurringDeduction_effectiveStartDate={date},RecurringDeduction_userSysId='{userSysId}',payComponentType='{payComponentType}')
```

#### EmpCompensationCalculated
```http
GET /EmpCompensationCalculated
GET /EmpCompensationCalculated(seqNumber={seqNumber},startDate={startDate},userId='{userId}')
```

#### EmpPayCompRecurring
```http
GET /EmpPayCompRecurring
GET /EmpPayCompRecurring(seqNumber={seqNumber},startDate={startDate},userId='{userId}',payComponent='{payComponent}')
```

#### DeductionScreenId
```http
GET /DeductionScreenId
GET /DeductionScreenId('{externalCode}')
```

#### RecurringDeduction
```http
GET /RecurringDeduction
GET /RecurringDeduction(effectiveStartDate={date},userSysId='{userSysId}')
```

#### EmpCompensation
```http
GET /EmpCompensation
GET /EmpCompensation(startDate={startDate},userId='{userId}')
```

#### EmpPayCompNonRecurring
```http
GET /EmpPayCompNonRecurring
GET /EmpPayCompNonRecurring(payComponentCode='{payComponentCode}',payDate={payDate},userId='{userId}')
```

#### EmpCompensationGroupSumCalculated
```http
GET /EmpCompensationGroupSumCalculated
GET /EmpCompensationGroupSumCalculated(seqNumber={seqNumber},startDate={startDate},userId='{userId}')
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
| $expand | string[] | - | Expand related entities |

### Example Requests

**Get all compensation records with pagination:**
```http
GET /EmpCompensation?$top=100&$skip=0
```

**Get compensation modified after a specific date (CDC):**
```http
GET /EmpCompensation?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get compensation with calculated data:**
```http
GET /EmpCompensation?$expand=empCompensationCalculatedNav,empPayCompRecurringNav
```

**Get recurring deductions for a user:**
```http
GET /RecurringDeduction?$filter=userSysId eq 'USER001'&$expand=recurringItems
```

**Get one-time deductions by date range:**
```http
GET /OneTimeDeduction?$filter=deductionDate ge datetime'2024-01-01T00:00:00' and deductionDate le datetime'2024-12-31T00:00:00'
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "startDate": "/Date(1609459200000)/",
        "userId": "USER001",
        "payGrade": "GRADE01",
        "payGroup": "PAYGRP01",
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
| string (maxLength) | StringType | String with length constraint |
| decimal | DecimalType | Monetary and numeric values |
| double | DoubleType | Floating point numbers |
| int64 | LongType | Large integers |
| boolean | BooleanType | True/false values |
| datetime (/Date(...)/) | TimestampType | SAP OData datetime format |

## Sources and References
- SAP SuccessFactors API Spec: ECCompensationInformation.json
- SAP Help Portal: [Compensation Information on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/22367ec45dbe409aa5f75f36a760ee06.html)
