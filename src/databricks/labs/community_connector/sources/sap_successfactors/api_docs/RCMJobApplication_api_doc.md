# Job Application API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **JobApplication** - Main job application entity with applicant details
2. **JobApplicationAssessmentReport** - Assessment reports for applications
3. **JobApplicationAssessmentReportDetail** - Detailed assessment report data
4. **JobApplicationAssessmentOrder** - Assessment orders for applications
5. **JobApplicationOnboardingData** - Onboarding data for hired candidates
6. **JobApplicationOnboardingStatus** - Status of onboarding process
7. **JobApplicationStatusLabel** - Labels for application statuses
8. **JobApplicationStatus** - Application status definitions
9. **JobApplicationQuestionResponse** - Responses to application questions
10. **JobApplicationStatusAuditTrail** - Audit trail of status changes
11. **JobApplicationBackgroundCheckRequest** - Background check requests
12. **JobApplicationBackgroundCheckResult** - Background check results
13. **JobApplicationComments** - Comments on applications

## Object Schema

### JobApplication
| Field | Type | Description |
|-------|------|-------------|
| applicationId | int64 | Primary key |
| candidateId | int64 | Reference to candidate |
| jobReqId | int64 | Reference to job requisition |
| applicationTemplateId | int64 | Application template ID |
| firstName | string (nullable) | Applicant first name |
| lastName | string (nullable) | Applicant last name |
| middleName | string (nullable) | Applicant middle name |
| contactEmail | string (nullable) | Contact email |
| cellPhone | string (nullable) | Cell phone |
| homePhone | string (nullable) | Home phone |
| address | string (nullable) | Address line 1 |
| address2 | string (nullable) | Address line 2 |
| city | string (nullable) | City |
| country | string (nullable) | Country |
| countryCode | string (nullable) | Country code |
| zip | string (nullable) | ZIP/Postal code |
| stateOther | string (nullable) | State/Province |
| currentTitle | string (nullable) | Current job title |
| currentLocation | string (nullable) | Current location |
| status | string (nullable) | Application status |
| statusComments | string (nullable) | Status comments |
| appStatusSetItemId | int64 (nullable) | Status set item ID |
| appLocale | string (nullable) | Application locale |
| source | string (nullable) | Application source |
| sourceLabel | string (nullable) | Source label |
| referredBy | string (nullable) | Referrer |
| rating | string (nullable) | Overall rating |
| averageRating | decimal (nullable) | Average rating score |
| owner | string (nullable) | Application owner |
| ownershpDate | datetime (nullable) | Ownership date |
| dateOfBirth | datetime (nullable) | Date of birth |
| gender | string (nullable) | Gender |
| formerEmployee | boolean (nullable) | Former employee flag |
| hireDate | datetime (nullable) | Hire date |
| hiredOn | datetime (nullable) | Hired on date |
| jobStartDate | datetime (nullable) | Job start date |
| jobofferdate | datetime (nullable) | Job offer date |
| dateAvail | datetime (nullable) | Date available |
| startDate | datetime (nullable) | Application start date |
| baseSalary | decimal (nullable) | Base salary |
| jobRateOfPay | decimal (nullable) | Rate of pay |
| timeToHire | double (nullable) | Time to hire (days) |
| dataSource | string (nullable) | Data source |
| resumeUploadDate | datetime (nullable) | Resume upload date |
| snapShotDate | datetime (nullable) | Snapshot date |
| confnumber | string (nullable) | Confirmation number |
| exportedOn | datetime (nullable) | Export date |
| jobAppGuid | string (nullable) | Application GUID |
| anonymizedFlag | string (nullable) | Anonymization flag |
| anonymizedDate | datetime (nullable) | Anonymization date |
| lastModifiedDateTime | datetime (nullable) | Last modified timestamp |
| lastModifiedBy | string (nullable) | Last modified by user |
| lastModifiedByProxy | string (nullable) | Last modified by proxy |
| nonApplicantStatus | string (nullable) | Non-applicant status |
| profileUpdated | string (nullable) | Profile update flag |
| duplicateProfile | string (nullable) | Duplicate profile flag |
| usersSysId | string (nullable) | System user ID |

### JobApplicationAssessmentReport
| Field | Type | Description |
|-------|------|-------------|
| id | int64 | Primary key |
| orderId | int64 | Reference to assessment order |
| status | int64 (nullable) | Report status |
| statusDate | datetime (nullable) | Status date |
| statusDetails | string | Status details |
| reportURL | string (nullable) | URL to report |

### JobApplicationAssessmentReportDetail
| Field | Type | Description |
|-------|------|-------------|
| id | int64 | Primary key |
| scoreComponent | string | Score component name |
| scoreValue | string | Score value |

### JobApplicationAssessmentOrder
| Field | Type | Description |
|-------|------|-------------|
| id | int64 | Primary key |
| applicationId | int64 | Reference to application |
| vendorCode | string (nullable) | Vendor code |
| createdBy | string (nullable) | Created by user |
| createdDateTime | datetime (nullable) | Creation timestamp |
| lastModifiedBy | string | Last modified by |
| lastModifiedDateTime | datetime (nullable) | Last modified timestamp |

### JobApplicationOnboardingData
| Field | Type | Description |
|-------|------|-------------|
| onboardingId | int64 | Primary key |
| applicationId | int64 (nullable) | Reference to application |
| status | string | Onboarding status |
| submittedBy | string | Submitted by user |
| submittedOn | datetime | Submission timestamp |

### JobApplicationOnboardingStatus
| Field | Type | Description |
|-------|------|-------------|
| onboardingStatusId | int64 | Primary key |
| onboardingId | int64 | Reference to onboarding data |
| name | string | Status name |
| status | string | Current status |
| statusType | string | Status type |
| type | string | Type |
| message | string | Status message |
| url | string | Related URL |
| createdDate | datetime | Creation timestamp |
| lastModifiedDate | datetime | Last modified timestamp |

### JobApplicationStatus
| Field | Type | Description |
|-------|------|-------------|
| appStatusSetId | int64 | Primary key (composite) |
| appStatusId | int64 (nullable) | Status ID |
| appStatusName | string | Status name |

### JobApplicationStatusLabel
| Field | Type | Description |
|-------|------|-------------|
| appStatusId | int64 | Reference to status (composite key) |
| locale | string | Locale code (composite key) |
| statusLabel | string | Localized status label |
| candidateLabel | string (nullable) | Candidate-facing label |

### JobApplicationQuestionResponse
| Field | Type | Description |
|-------|------|-------------|
| applicationId | int64 | Reference to application (composite key) |
| order | int64 | Question order (composite key) |
| question | string (nullable) | Question text |
| answer | string (nullable) | Answer text |
| questionResponse | string (nullable) | Response value |
| expectedAnswer | string (nullable) | Expected answer |
| expectedAnswerValue | double (nullable) | Expected answer value |
| highLow | string (nullable) | High/Low indicator |
| type | string (nullable) | Question type |
| maxLength | double (nullable) | Maximum length |

### JobApplicationStatusAuditTrail
| Field | Type | Description |
|-------|------|-------------|
| revNumber | int64 | Revision number (primary key) |
| revType | int32 | Revision type |
| skippedStatus | int32 (nullable) | Skipped status flag |
| statusComments | string (nullable) | Status comments |
| createdBy | string (nullable) | Created by |
| createdDateTime | datetime (nullable) | Creation timestamp |
| lastModifiedBy | string (nullable) | Last modified by |
| lastModifiedDateTime | datetime (nullable) | Last modified timestamp |
| lastModifiedProxyUser | string (nullable) | Proxy user |

### JobApplicationBackgroundCheckRequest
| Field | Type | Description |
|-------|------|-------------|
| requestId | int64 | Primary key |
| applicationId | int64 | Reference to application |
| vendorCode | string | Vendor code |
| vendorOrderNo | string (nullable) | Vendor order number |
| orderStatus | string (nullable) | Order status |
| responseCode | string (nullable) | Response code |
| responseDetail | string (nullable) | Response details |
| createdByUser | string | Created by user |
| createdDateTime | datetime | Creation timestamp |
| lastModifiedDateTime | datetime | Last modified timestamp |

### JobApplicationBackgroundCheckResult
| Field | Type | Description |
|-------|------|-------------|
| statusId | int64 | Primary key |
| vendorCode | string | Vendor code |
| vendorOrderNo | string | Vendor order number |
| stepName | string (nullable) | Step name |
| stepStatus | string (nullable) | Step status |
| stepMessage | string (nullable) | Step message |
| reportUrl | string (nullable) | Report URL |
| finalStep | boolean (nullable) | Final step flag |
| createdDateTime | datetime | Creation timestamp |

### JobApplicationComments
| Field | Type | Description |
|-------|------|-------------|
| commentId | int64 | Primary key |
| applicationId | int64 (nullable) | Reference to application |
| associatedId | string (nullable) | Associated entity ID |
| associatedCommentId | int64 (nullable) | Associated comment ID |
| commentator | string (nullable) | Commentator user |
| content | string (nullable) | Comment content |
| hasAssociatedComment | string (nullable) | Has associated comment |
| refType | string (nullable) | Reference type |
| migratedCommentatorUserName | string (nullable) | Migrated commentator |

## Get Object Primary Keys

| Entity | Primary Key(s) |
|--------|----------------|
| JobApplication | applicationId |
| JobApplicationAssessmentReport | id |
| JobApplicationAssessmentReportDetail | id |
| JobApplicationAssessmentOrder | id |
| JobApplicationOnboardingData | onboardingId |
| JobApplicationOnboardingStatus | onboardingStatusId |
| JobApplicationStatus | appStatusSetId |
| JobApplicationStatusLabel | appStatusId, locale |
| JobApplicationQuestionResponse | applicationId, order |
| JobApplicationStatusAuditTrail | revNumber |
| JobApplicationBackgroundCheckRequest | requestId |
| JobApplicationBackgroundCheckResult | statusId |
| JobApplicationComments | commentId |

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Notes |
|--------|---------------|--------------|-------|
| JobApplication | cdc | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| JobApplicationAssessmentOrder | cdc | lastModifiedDateTime | Supports incremental sync |
| JobApplicationBackgroundCheckRequest | cdc | lastModifiedDateTime | Supports incremental sync |
| JobApplicationOnboardingStatus | cdc | lastModifiedDate | Supports incremental sync |
| JobApplicationStatusAuditTrail | cdc | lastModifiedDateTime | Audit records with timestamps |
| JobApplicationAssessmentReport | snapshot | N/A | Status-based, no modification timestamp |
| JobApplicationAssessmentReportDetail | snapshot | N/A | Linked to assessment report |
| JobApplicationOnboardingData | snapshot | N/A | Use submittedOn for filtering |
| JobApplicationStatus | snapshot | N/A | Reference data |
| JobApplicationStatusLabel | snapshot | N/A | Reference data |
| JobApplicationQuestionResponse | snapshot | N/A | Linked to application |
| JobApplicationBackgroundCheckResult | snapshot | N/A | Status updates |
| JobApplicationComments | snapshot | N/A | No timestamp field |

## Read API for Data Retrieval

### Base URL Pattern
```
https://{api-server}/odata/v2/{EntitySet}
```

### Supported Query Parameters
| Parameter | Description |
|-----------|-------------|
| $select | Select specific properties to return |
| $filter | Filter results by property values |
| $top | Limit number of results (default: 20) |
| $skip | Skip first n results for pagination |
| $orderby | Order results by property |
| $expand | Expand related entities |
| $count | Include total count of items |
| $search | Search items by search phrases |

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- Default page size: 20 records
- Recommended: Use `$orderby` with `$top` and `$skip` for consistent pagination

### Example Requests

**Get all job applications:**
```http
GET https://{api-server}/odata/v2/JobApplication?$top=100
```

**Get job application by ID:**
```http
GET https://{api-server}/odata/v2/JobApplication({applicationId})
```

**Get applications modified after a date (incremental sync):**
```http
GET https://{api-server}/odata/v2/JobApplication?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get applications for a specific job requisition:**
```http
GET https://{api-server}/odata/v2/JobApplication?$filter=jobReqId eq {jobReqId}
```

**Get application with expanded status and comments:**
```http
GET https://{api-server}/odata/v2/JobApplication({applicationId})?$expand=jobAppStatus,jobApplicationComments
```

**Get application status audit trail:**
```http
GET https://{api-server}/odata/v2/JobApplicationStatusAuditTrail?$filter=jobApplication/applicationId eq {applicationId}
```

**Select specific fields:**
```http
GET https://{api-server}/odata/v2/JobApplication?$select=applicationId,candidateId,jobReqId,firstName,lastName,status,lastModifiedDateTime
```

**Get onboarding data:**
```http
GET https://{api-server}/odata/v2/JobApplicationOnboardingData?$expand=onboardingStatus
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| Edm.String | StringType |
| Edm.Int32 | IntegerType |
| Edm.Int64 | LongType |
| Edm.Boolean | BooleanType |
| Edm.DateTime | TimestampType |
| Edm.DateTimeOffset | TimestampType |
| Edm.Decimal | DecimalType |
| Edm.Double | DoubleType |

## Sources and References

- SAP SuccessFactors API Spec: RCMJobApplication.json
- SAP Help Portal: [Job Application APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/95d27806037b457dad027a4c6981630d.htmll)
- API Server List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
