# Advances API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- AdvancesInstallments
- AdvancesEligibility
- AdvancesAccumulation
- Advance

## Object Schema

### AdvancesInstallments

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| Advance_externalCode | string | 128 | Parent Advance external code (key) |
| NonRecurringPayment_externalCode | string | 128 | Non-recurring payment external code (key) |
| externalCode | string | 128 | Installment external code (key) |
| amortization | decimal | - | Amortization amount |
| amortizationTotal | decimal | - | Total amortization amount |
| balanceRemaining | decimal | - | Remaining balance |
| createdBy | string | 255 | User who created the record |
| createdDateTime | datetime | - | Creation timestamp |
| currency | string | 255 | Currency code |
| currencyGO | string | 128 | Currency GO reference |
| effectiveStatus | string | 255 | Status of the installment |
| installmentAmount | decimal | - | Amount per installment |
| installmentStatus | string | 255 | Status of installment |
| interestAmount | decimal | - | Interest amount |
| lastModifiedBy | string | 255 | User who last modified |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| mdfSystemCreatedBy | string | 255 | MDF system created by |
| mdfSystemCreatedDate | datetime | - | MDF system creation date |
| mdfSystemEffectiveEndDate | datetime | - | MDF effective end date |
| mdfSystemEffectiveStartDate | datetime | - | MDF effective start date |
| mdfSystemEntityId | string | 255 | MDF entity ID |
| mdfSystemLastModifiedBy | string | 255 | MDF last modified by |
| mdfSystemLastModifiedDate | datetime | - | MDF last modified date |
| mdfSystemLastModifiedDateWithTZ | datetime | - | MDF last modified date with timezone |
| mdfSystemObjectType | string | 255 | MDF object type |
| mdfSystemRecordId | string | 255 | MDF record ID |
| mdfSystemRecordStatus | string | 255 | MDF record status |
| mdfSystemTransactionSequence | int64 | - | MDF transaction sequence |
| mdfSystemVersionId | int64 | - | MDF version ID |
| paymentDate | datetime | - | Payment date |

### AdvancesEligibility

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| externalCode | string | 128 | External code (key) |
| effectiveStartDate | datetime | - | Effective start date (key) |
| advanceType | string | 32 | Type of advance |
| basePayComponent | string | 32 | Base pay component |
| basePayComponentGroup | string | 32 | Base pay component group |
| calculationPercentageOn | string | 128 | Calculation percentage basis |
| company | string | 128 | Company code |
| createdBy | string | 255 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| currency | string | 128 | Currency |
| currencyGO | string | 128 | Currency GO reference |
| dayOfDeduction | string | 255 | Day of deduction |
| deductionDateFormat | string | 255 | Deduction date format |
| deductionPayCompOTD | string | 255 | Deduction pay component OTD |
| deductionPayCompRD | string | 255 | Deduction pay component RD |
| defaultWorkflow | string | 32 | Default workflow |
| department | string | 128 | Department |
| effectiveEndDate | datetime | - | Effective end date |
| effectiveStatus | string | 255 | Effective status |
| eligibilityAmount | decimal | - | Eligibility amount |
| enableAutoRecovery | string | 255 | Enable auto recovery flag |
| exceptionForNumberOfInstallments | boolean | - | Exception for installment count |
| exceptionForRequestedAmount | boolean | - | Exception for requested amount |
| exceptionWorkflow | string | 32 | Exception workflow |
| externalName | string | 128 | External name |
| firstOccurenceStartDate | datetime | - | First occurrence start date |
| installmentAmount | decimal | - | Default installment amount |
| installmentFrequency | string | 32 | Installment frequency |
| interestRate | decimal | - | Interest rate |
| interestType | string | 255 | Interest type |
| lastModifiedBy | string | 255 | Last modified by user |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| maximumEligibilityAmount | decimal | - | Maximum eligibility amount |
| numberOfInstallments | int64 | - | Number of installments |
| numberOfInstallmentsEditableByEmployee | string | 255 | Installment count editable flag |
| numberOfOccurences | int64 | - | Number of occurrences |
| occuranceOfDay | string | 255 | Occurrence day |
| payComponentType | string | 255 | Pay component type |
| paygrade | string | 32 | Pay grade |
| periodEndDate | datetime | - | Period end date |
| periodStartDate | datetime | - | Period start date |
| recoveryMode | string | 255 | Recovery mode |
| recoveryModeEditableByEmployee | string | 255 | Recovery mode editable flag |
| unitOfPeriod | string | 255 | Unit of period |
| validityPeriod | int64 | - | Validity period |

### AdvancesAccumulation

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| externalCode | string | 128 | External code (key) |
| accumulatedAmount | decimal | - | Accumulated amount |
| advanceType | string | 32 | Type of advance |
| createdBy | string | 255 | Created by user |
| createdDate | datetime | - | Creation date |
| createdDateTime | datetime | - | Creation timestamp |
| currency | string | 128 | Currency |
| currencyGO | string | 128 | Currency GO reference |
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
| numberOfOccurances | int64 | - | Number of occurrences |
| periodEndDate | datetime | - | Period end date |
| periodStartDate | datetime | - | Period start date |
| remainingEligibleAmount | decimal | - | Remaining eligible amount |
| remainingNumberOfOccurances | int64 | - | Remaining occurrences |
| userSysId | string | 100 | User system ID |

### Advance

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| NonRecurringPayment_externalCode | string | 128 | Non-recurring payment code (key) |
| externalCode | string | 128 | Advance external code (key) |
| advanceEligibilityCode | string | 128 | Advance eligibility code |
| advanceType | string | 32 | Type of advance |
| approvalStatus | string | 255 | Approval status |
| approver | string | 100 | Approver user |
| createdBy | string | 255 | Created by user |
| createdDate | datetime | - | Creation date |
| createdDateTime | datetime | - | Creation timestamp |
| currencyCode | string | 255 | Currency code |
| currencyGO | string | 128 | Currency GO reference |
| eligibileAmount | decimal | - | Eligible amount |
| eligibilityAmount | decimal | - | Eligibility amount |
| eligibleAdvanceType | string | 255 | Eligible advance type |
| installmentAmount | decimal | - | Installment amount |
| installmentFrequency | string | 32 | Installment frequency |
| interestRate | decimal | - | Interest rate |
| interestType | string | 255 | Interest type |
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
| notesForApprover | string | 255 | Notes for approver |
| numberOfInstallments | int64 | - | Number of installments |
| paymentMode | string | 128 | Payment mode |
| pendingAmount | decimal | - | Pending amount |
| periodEndDate | datetime | - | Period end date |
| periodStartDate | datetime | - | Period start date |
| recoveryMode | string | 255 | Recovery mode |
| recoveryStatus | string | 255 | Recovery status |
| remainingRequests | int64 | - | Remaining requests |
| requestDate | datetime | - | Request date |
| requestedAmount | decimal | - | Requested amount |
| totalRepaymentAmount | decimal | - | Total repayment amount |

**Related Entities:**
- advanceEligibilityCodeNav -> AdvancesEligibility
- advancesInstallments -> AdvancesInstallments

## Get Object Primary Keys

### AdvancesInstallments
- `Advance_externalCode` (string)
- `NonRecurringPayment_externalCode` (string)
- `externalCode` (string)

### AdvancesEligibility
- `effectiveStartDate` (datetime)
- `externalCode` (string)

### AdvancesAccumulation
- `externalCode` (string)

### Advance
- `NonRecurringPayment_externalCode` (string)
- `externalCode` (string)

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| AdvancesInstallments | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| AdvancesEligibility | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| AdvancesAccumulation | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| Advance | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Entity Endpoints

#### AdvancesInstallments
```http
GET /AdvancesInstallments
GET /AdvancesInstallments(Advance_externalCode='{Advance_externalCode}',NonRecurringPayment_externalCode='{NonRecurringPayment_externalCode}',externalCode='{externalCode}')
```

#### AdvancesEligibility
```http
GET /AdvancesEligibility
GET /AdvancesEligibility(effectiveStartDate={effectiveStartDate},externalCode='{externalCode}')
```

#### AdvancesAccumulation
```http
GET /AdvancesAccumulation
GET /AdvancesAccumulation('{externalCode}')
```

#### Advance
```http
GET /Advance
GET /Advance(NonRecurringPayment_externalCode='{NonRecurringPayment_externalCode}',externalCode='{externalCode}')
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
| $expand | string[] | - | Expand related entities (Advance only) |

### Example Requests

**Get all advances with pagination:**
```http
GET /Advance?$top=100&$skip=0
```

**Get advances modified after a specific date (CDC):**
```http
GET /Advance?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get advance with related entities:**
```http
GET /Advance?$expand=advanceEligibilityCodeNav,advancesInstallments
```

**Get advances by approval status:**
```http
GET /Advance?$filter=approvalStatus eq 'Approved'
```

**Get eligibility rules for a specific company:**
```http
GET /AdvancesEligibility?$filter=company eq 'COMP001'
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "NonRecurringPayment_externalCode": "NRP001",
        "externalCode": "ADV001",
        "advanceType": "Salary",
        "approvalStatus": "Approved",
        "requestedAmount": "5000",
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
| int64 | LongType | Large integers |
| boolean | BooleanType | True/false values |
| datetime (/Date(...)/) | TimestampType | SAP OData datetime format |

## Sources and References
- SAP SuccessFactors API Spec: ECAdvances.json
- SAP Help Portal: [Advances on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/6bbac0e709a345848d215ecd879fcf59.html)
