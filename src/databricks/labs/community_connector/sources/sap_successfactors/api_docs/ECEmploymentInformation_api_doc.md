# Employment Information API Documentation

## Authorization

- **Method**: Basic Authentication
- **Header**: `Authorization: Basic base64(username:password)`
- **Username format**: `username@companyId`

## Object List

The following entities/objects are available in this API:

- EmpBeneficiary
- EmpEmployment
- EmpEmploymentTermination
- EmpJob
- EmpJobRelationships
- EmpPensionPayout
- EmpWorkPermit
- Hire Date Change
- PersonEmpTerminationInfo

## Object Schema

Detailed schema for each entity:

### EmpBeneficiary

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| employmentNav | Ref(EmpEmployment) | - | - | No |
| endDate | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| payrollEndDate | string | - | - | Yes |
| personIdExternal | string | - | 100 | No |
| plannedEndDate | string | - | - | Yes |
| startDate | string | - | - | Yes |
| userId | string | - | 100 | No |

### EmpEmployment

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| StockEndDate | string | - | - | Yes |
| assignmentClass | string | - | 128 | Yes |
| benefitsEligibilityStartDate | string | - | - | Yes |
| benefitsEndDate | string | - | - | Yes |
| bonusPayExpirationDate | string | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| eligibleForSalContinuation | boolean | - | - | Yes |
| eligibleForStock | boolean | - | - | Yes |
| empBeneficiaryNav | Ref(EmpBeneficiary) | - | - | No |
| empJobRelationshipNav | Object | - | - | Yes |
| empPensionPayoutNav | Ref(EmpPensionPayout) | - | - | No |
| empWorkPermitNav | Object | - | - | Yes |
| employeeFirstEmployment | boolean | - | - | Yes |
| endDate | string | - | - | Yes |
| firstDateWorked | string | - | - | Yes |
| includeAllRecords | boolean | - | - | Yes |
| initialOptionGrant | number | double | - | Yes |
| initialStockGrant | number | double | - | Yes |
| isContingentWorker | boolean | - | - | Yes |
| isECRecord | boolean | - | - | Yes |
| jobInfoNav | Object | - | - | Yes |
| jobNumber | string | int64 | - | Yes |
| lastDateWorked | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| okToRehire | boolean | - | - | Yes |
| originalStartDate | string | - | - | Yes |
| payrollEndDate | string | - | - | Yes |
| personIdExternal | string | - | 100 | No |
| prevEmployeeId | string | - | 256 | Yes |
| professionalServiceDate | string | - | - | Yes |
| regretTermination | boolean | - | - | Yes |
| salaryEndDate | string | - | - | Yes |
| seniorityDate | string | - | - | Yes |
| serviceDate | string | - | - | Yes |
| startDate | string | - | - | Yes |
| userId | string | - | 100 | No |

### EmpEmploymentTermination

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| StockEndDate | string | - | - | Yes |
| attachmentId | string | - | - | Yes |
| benefitsEndDate | string | - | - | Yes |
| bonusPayExpirationDate | string | - | - | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| eligibleForSalContinuation | boolean | - | - | Yes |
| employmentNav | Ref(EmpEmployment) | - | - | No |
| endDate | string | - | - | No |
| eventReason | string | - | - | Yes |
| jobInfoNav | Ref(EmpJob) | - | - | No |
| lastDateWorked | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| newMainEmploymentId | string | decimal | - | Yes |
| notes | string | - | 4000 | Yes |
| okToRehire | boolean | - | - | Yes |
| payrollEndDate | string | - | - | Yes |
| personIdExternal | string | - | 100 | No |
| regretTermination | boolean | - | - | Yes |
| salaryEndDate | string | - | - | Yes |
| userId | string | - | 100 | No |

### EmpJob

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| assedicCertInitialStateNum | string | int64 | - | Yes |
| assedicCertObjectNum | string | int64 | - | Yes |
| businessUnit | string | - | 32 | Yes |
| calcMethodIndicator | boolean | - | - | Yes |
| commitmentIndicator | string | - | 256 | Yes |
| company | string | - | 32 | Yes |
| contractReferenceForAed | string | - | 256 | Yes |
| contractType | string | - | 256 | Yes |
| costCenter | string | - | 32 | Yes |
| countryOfCompany | string | - | 256 | Yes |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| defaultOvertimeCompensationVariant | string | - | - | Yes |
| department | string | - | 32 | Yes |
| division | string | - | 32 | Yes |
| eeo1JobCategory | string | - | 256 | Yes |
| eeo4JobCategory | string | - | 256 | Yes |
| eeo5JobCategory | string | - | 256 | Yes |
| eeo6JobCategory | string | - | 256 | Yes |
| eeoClass | string | - | 256 | Yes |
| electoralCollegeForWorkersRepresentatives | string | - | 256 | Yes |
| electoralCollegeForWorksCouncil | string | - | 256 | Yes |
| empRelationship | string | - | 256 | Yes |
| emplStatus | string | - | 32 | Yes |
| employeeClass | string | - | 256 | Yes |
| employeeWorkgroupMembership | string | - | 60 | Yes |
| employmentNav | Ref(EmpEmployment) | - | - | No |
| employmentType | string | - | 32 | Yes |
| endDate | string | - | - | Yes |
| event | string | - | 32 | Yes |
| eventReason | string | - | - | Yes |
| exclExecutiveSector | boolean | - | - | Yes |
| expectedReturnDate | string | - | - | Yes |
| familyRelationshipWithEmployer | string | - | 256 | Yes |
| fgtsDate | string | - | - | Yes |
| fgtsOptant | boolean | - | - | Yes |
| fgtsPercent | number | double | - | Yes |
| flsaStatus | string | - | 256 | Yes |
| fte | number | double | - | Yes |
| harmfulAgentExposure | string | - | 256 | Yes |
| hazard | boolean | - | - | Yes |
| healthRisk | boolean | - | - | Yes |
| holidayCalendarCode | string | - | 128 | Yes |
| isCompetitionClauseActive | boolean | - | - | Yes |
| isFulltimeEmployee | boolean | - | - | Yes |
| isSideLineJobAllowed | boolean | - | - | Yes |
| jobCode | string | - | 32 | Yes |
| jobTitle | string | - | 256 | Yes |
| laborProtection | boolean | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| location | string | - | 128 | Yes |
| managerEmploymentNav | Ref(EmpEmployment) | - | - | No |
| managerId | string | - | 256 | Yes |
| mandatoryWorkBreakRecord | string | int64 | - | Yes |
| municipalInseeCode | string | int64 | - | Yes |
| notes | string | - | 4000 | Yes |
| operation | string | - | - | Yes |
| payGrade | string | - | 256 | Yes |
| payScaleArea | string | - | 128 | Yes |
| payScaleGroup | string | - | 128 | Yes |
| payScaleLevel | string | - | 128 | Yes |
| payScaleType | string | - | 128 | Yes |
| pcfm | boolean | - | - | Yes |
| pensionProtection | boolean | - | - | Yes |
| permitIndicator | boolean | - | - | Yes |
| position | string | - | 128 | Yes |
| positionEntryDate | string | - | - | Yes |
| probationPeriodEndDate | string | - | - | Yes |
| regularTemp | string | - | 32 | Yes |
| residentVote | boolean | - | - | Yes |
| retired | boolean | - | - | Yes |
| seqNumber | string | int64 | - | No |
| sickPaySupplement | string | - | 256 | Yes |
| standardHours | number | double | - | Yes |
| startDate | string | - | - | No |
| teachersPension | boolean | - | - | Yes |
| timeRecordingAdmissibilityCode | string | - | 128 | Yes |
| timeRecordingProfileCode | string | - | 128 | Yes |
| timeRecordingVariant | string | - | - | Yes |
| timeTypeProfileCode | string | - | 128 | Yes |
| timezone | string | - | 128 | Yes |
| travelDistance | number | double | - | Yes |
| tupeOrgNumber | string | - | 256 | Yes |
| userId | string | - | 100 | No |
| workLocation | string | - | 256 | Yes |
| workerCategory | string | - | 256 | Yes |
| workingDaysPerWeek | number | double | - | Yes |
| workingTimeDirective | boolean | - | - | Yes |
| workscheduleCode | string | - | 128 | Yes |
| wtdHoursLimit | string | - | 256 | Yes |

### EmpJobRelationships

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| employmentNav | Ref(EmpEmployment) | - | - | No |
| endDate | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| operation | string | - | - | Yes |
| relEmploymentNav | Ref(EmpEmployment) | - | - | No |
| relUserId | string | - | 384 | Yes |
| relationshipType | string | - | 100 | No |
| startDate | string | - | - | No |
| userId | string | - | 100 | No |

### EmpPensionPayout

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| employmentNav | Ref(EmpEmployment) | - | - | No |
| endDate | string | - | - | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| payrollEndDate | string | - | - | Yes |
| personIdExternal | string | - | 100 | No |
| plannedEndDate | string | - | - | Yes |
| startDate | string | - | - | Yes |
| userId | string | - | 100 | No |

### EmpWorkPermit

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| attachment | string | base64url | - | Yes |
| attachmentFileName | string | - | 256 | Yes |
| attachmentFileSize | string | decimal | - | Yes |
| attachmentFileType | string | - | 5 | Yes |
| attachmentId | string | - | - | Yes |
| attachmentMimeType | string | - | 256 | Yes |
| attachmentStatus | string | decimal | - | Yes |
| country | string | - | 256 | No |
| createdBy | string | - | 100 | Yes |
| createdDateTime | string | - | - | Yes |
| createdOn | string | - | - | Yes |
| documentNumber | string | - | 256 | No |
| documentTitle | string | - | 256 | Yes |
| documentType | string | - | 256 | No |
| employmentNav | Ref(EmpEmployment) | - | - | No |
| expirationDate | string | - | - | Yes |
| isValidated | boolean | - | - | Yes |
| issueDate | string | - | - | Yes |
| issuePlace | string | - | 256 | Yes |
| issuingAuthority | string | - | 256 | Yes |
| lastModifiedBy | string | - | 100 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| lastModifiedOn | string | - | - | Yes |
| notes | string | - | 4000 | Yes |
| userId | string | - | 100 | No |

### HireDateChange

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| code | string | - | 128 | No |
| createdBy | string | - | 255 | Yes |
| createdDateTime | string | - | - | Yes |
| lastModifiedBy | string | - | 255 | Yes |
| lastModifiedDateTime | string | - | - | Yes |
| mdfSystemRecordStatus | string | - | 255 | Yes |
| newHireDate | string | - | - | Yes |
| originalHireDate | string | - | - | Yes |
| processingStatus | string | - | 128 | Yes |
| usersSysId | string | - | 100 | Yes |

### PersonEmpTerminationInfo

| Field | Type | Format | Max Length | Nullable |
|-------|------|--------|------------|----------|
| activeEmploymentsCount | integer | int32 | - | Yes |
| latestTerminationDate | string | - | - | Yes |
| personIdExternal | string | - | 100 | No |

## Object Primary Keys

Primary key fields identified for each entity:

- **EmpBeneficiary**: userId, personIdExternal
- **EmpEmployment**: prevEmployeeId, userId, personIdExternal
- **EmpEmploymentTermination**: userId, attachmentId, personIdExternal, newMainEmploymentId
- **EmpJob**: userId, managerId
- **EmpJobRelationships**: relUserId, userId
- **EmpPensionPayout**: userId, personIdExternal
- **EmpWorkPermit**: attachmentId, userId
- **HireDateChange**: usersSysId
- **PersonEmpTerminationInfo**: personIdExternal

## Object's Ingestion Type

Ingestion types are determined based on API capabilities:


| Entity | Ingestion Type | Cursor Field |
|--------|---------------|--------------|
| EmpBeneficiary | cdc | lastModifiedDateTime |
| EmpEmployment | cdc | lastModifiedDateTime |
| EmpEmploymentTermination | cdc | lastModifiedDateTime |
| EmpJob | cdc | lastModifiedDateTime |
| EmpJobRelationships | cdc | lastModifiedDateTime |
| EmpPensionPayout | cdc | lastModifiedDateTime |
| EmpWorkPermit | cdc | lastModifiedDateTime |
| HireDateChange | cdc | lastModifiedDateTime |
| PersonEmpTerminationInfo | snapshot | - |

**Ingestion Type Definitions:**
- `cdc`: Change Data Capture - supports incremental reads using lastModifiedDateTime filter
- `snapshot`: Full snapshot - requires full table refresh each sync
- `cdc_with_deletes`: CDC with delete tracking (not commonly supported)

## Read API for Data Retrieval

### Base URL Pattern

```
https://{api-server}/odata/v2/{EntitySet}
```

### Available API Servers

Find your company's API server at: [SAP SuccessFactors API Servers](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)

### Query Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `$select` | Select specific fields | `$select=userId,firstName,lastName` |
| `$filter` | Filter results | `$filter=lastModifiedDateTime ge datetime'2024-01-01T00:00:00'` |
| `$top` | Limit number of results | `$top=100` |
| `$skip` | Skip N results (pagination) | `$skip=100` |
| `$orderby` | Sort results | `$orderby=lastModifiedDateTime asc` |
| `$expand` | Include related entities | `$expand=employmentNav` |
| `$inlinecount` | Include total count | `$inlinecount=allpages` |

### Pagination

SAP SuccessFactors OData v2 uses server-side pagination:
- Default page size varies by entity
- Use `$top` and `$skip` for client-side pagination
- Response includes `__next` link for server-side pagination
- Always check for `d.results` array in response

### Example Requests

**Get all records from EmpEmployment:**
```http
GET https://{api-server}/odata/v2/EmpEmployment
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get records modified after a date (for CDC):**
```http
GET https://{api-server}/odata/v2/EmpEmployment?$filter=lastModifiedDateTime ge datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
Authorization: Basic {base64_credentials}
Accept: application/json
```

**Get specific fields with pagination:**
```http
GET https://{api-server}/odata/v2/EmpEmployment?$select=externalCode,createdDateTime,lastModifiedDateTime&$top=100&$skip=0&$inlinecount=allpages
Authorization: Basic {base64_credentials}
Accept: application/json
```

### Response Format

```json
{
  "d": {
    "__count": "1000",
    "results": [
      {
        "__metadata": {
          "uri": "https://{api-server}/odata/v2/EmpEmployment('key')",
          "type": "SFOData.EmpEmployment"
        },
        "externalCode": "ABC123",
        "lastModifiedDateTime": "/Date(1704067200000)/"
      }
    ],
    "__next": "https://{api-server}/odata/v2/{EntitySet}?$skiptoken=..."
  }
}
```

## Field Type Mapping

| OData Type | Spark Type | Notes |
|------------|------------|-------|
| Edm.String | StringType | Default string type |
| Edm.Int32 | IntegerType | 32-bit integer |
| Edm.Int64 | LongType | 64-bit integer |
| Edm.Boolean | BooleanType | True/False |
| Edm.DateTime | TimestampType | Format: `/Date(milliseconds)/` |
| Edm.DateTimeOffset | TimestampType | DateTime with timezone |
| Edm.Decimal | DecimalType | Decimal numbers |
| Edm.Double | DoubleType | Double precision float |
| Edm.Binary | BinaryType | Base64 encoded |

### DateTime Parsing

SAP SuccessFactors returns dates in OData format:
- Format: `/Date(milliseconds)/` or `/Date(milliseconds+offset)/`
- Example: `/Date(1704067200000)/` = 2024-01-01T00:00:00Z
- Parse by extracting milliseconds and converting to timestamp

## Sources and References

- **SAP SuccessFactors API Spec**: `ECEmploymentInformation.json`
- **SAP Help Portal**: [Employment Information on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/d91ecc323849441cb2773fc86f0eff0f.html)
- **SAP API Business Hub**: [SAP SuccessFactors APIs](https://api.sap.com/package/SuccessFactorsFoundation/overview)
- **Authentication Guide**: [SAP SuccessFactors OData API Authentication](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/5c8bca0af1654b05a83193b2922dcee2.html)
