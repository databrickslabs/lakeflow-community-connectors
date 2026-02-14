# Job Requisition API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **JobRequisition** - Main job requisition entity with position details
2. **JobRequisitionLocale** - Localized job descriptions and titles
3. **JobRequisitionPosting** - Job posting information
4. **JobRequisitionOperator** - Individual operators (recruiters, hiring managers, etc.)
5. **JobRequisitionGroupOperator** - Group operators for requisitions
6. **JobReqQuestion** - Questions associated with job requisitions
7. **JobReqScreeningQuestion** - Screening questions for candidates
8. **JobReqScreeningQuestionChoice** - Answer choices for screening questions
9. **JobReqGOPosition** - Position object list for requisitions

## Object Schema

### JobRequisition
| Field | Type | Description |
|-------|------|-------------|
| jobReqId | int64 | Primary key |
| jobReqGUId | string | Requisition GUID |
| templateId | int64 | Template ID |
| templateType | string | Template type |
| appTemplateId | int64 | Application template ID |
| formDataId | int64 | Form data ID |
| statusSetId | int64 | Status set ID |
| internalStatus | string | Internal status |
| deleted | string | Deleted flag |
| isDraft | boolean (nullable) | Draft flag |
| positionNumber | string | Position number |
| jobCode | string (nullable) | Job code |
| jobRole | string (nullable) | Job role |
| department | string (nullable) | Department |
| division | string (nullable) | Division |
| function | string (nullable) | Function |
| facility | string (nullable) | Facility |
| costCenterId | string (nullable) | Cost center ID |
| legentity | string (nullable) | Legal entity |
| location | string (nullable) | Location |
| city | string (nullable) | City |
| stateProvince | string (nullable) | State/Province |
| postalcode | string (nullable) | Postal code |
| country | string (nullable) | Country |
| industry | string (nullable) | Industry |
| currency | string (nullable) | Currency |
| numberOpenings | decimal (nullable) | Number of openings |
| openingsFilled | int64 (nullable) | Openings filled |
| candidateProgress | int64 | Candidate progress count |
| ratedApplicantCount | int64 | Rated applicant count |
| timeToFill | int64 | Time to fill (days) |
| age | int64 | Requisition age (days) |
| costOfHire | decimal (nullable) | Cost of hire |
| defaultLanguage | string (nullable) | Default language |
| corporatePosting | boolean (nullable) | Corporate posting flag |
| intranetPosting | boolean (nullable) | Intranet posting flag |
| salaryBase | decimal (nullable) | Base salary |
| salaryMin | decimal (nullable) | Minimum salary |
| salaryMax | decimal (nullable) | Maximum salary |
| salRateType | string (nullable) | Salary rate type |
| salaryComment | string (nullable) | Salary comments |
| commission | decimal (nullable) | Commission |
| targetBonusAmount | decimal (nullable) | Target bonus amount |
| stockPackage | string (nullable) | Stock package |
| otherBonus | string (nullable) | Other bonus |
| otherCompensation | string (nullable) | Other compensation |
| annual_FB | decimal (nullable) | Annual flexible benefits |
| annual_PF | decimal (nullable) | Annual provident fund |
| annual_SA | decimal (nullable) | Annual special allowance |
| annual_cashPay | decimal (nullable) | Annual cash pay |
| annual_gratuity | decimal (nullable) | Annual gratuity |
| annual_retirals | decimal (nullable) | Annual retirals |
| monthly_FB | decimal (nullable) | Monthly flexible benefits |
| monthly_PF | decimal (nullable) | Monthly provident fund |
| monthly_SA | decimal (nullable) | Monthly special allowance |
| monthly_cashPay | decimal (nullable) | Monthly cash pay |
| monthly_gratuity | decimal (nullable) | Monthly gratuity |
| monthly_retirals | decimal (nullable) | Monthly retirals |
| monthly_salary | decimal (nullable) | Monthly salary |
| total_earnings | decimal (nullable) | Total earnings |
| total_fixed_pay | decimal (nullable) | Total fixed pay |
| total_hire_cost | string (nullable) | Total hire cost |
| accommodation_cost | string (nullable) | Accommodation cost |
| agency_fee | string (nullable) | Agency fee |
| travel_cost | string (nullable) | Travel cost |
| misc_cost | string (nullable) | Miscellaneous cost |
| jobStartDate | datetime (nullable) | Job start date |
| formDueDate | datetime (nullable) | Form due date |
| tempDate | datetime (nullable) | Temporary date |
| closedDateTime | datetime | Closed date/time |
| createdDateTime | datetime | Creation timestamp |
| lastModifiedDateTime | datetime | Last modified timestamp |
| lastModifiedBy | string | Last modified by user |
| lastModifiedProxyUserId | string | Proxy user ID |
| hiringManagerNote | string (nullable) | Hiring manager notes |
| comment | string (nullable) | Comments |
| recruitJust | string (nullable) | Recruitment justification |
| replforwhom | string (nullable) | Replacement for whom |
| workHours | string (nullable) | Work hours |
| classificationType | string (nullable) | Classification type |
| classificationTime | string (nullable) | Classification time |
| assessRatingScaleName | string | Assessment rating scale |
| overallScaleName | string | Overall scale name |
| reverseScale | string | Reverse scale |

### JobRequisitionLocale
| Field | Type | Description |
|-------|------|-------------|
| jobReqLocalId | int64 | Primary key |
| jobReqId | int64 | Reference to requisition |
| locale | string | Locale code |
| status | string | Status |
| jobTitle | string | Job title |
| externalTitle | string | External job title |
| jobDescription | string | Internal job description |
| externalJobDescription | string | External job description |
| intJobDescHeader | string | Internal description header |
| intJobDescFooter | string | Internal description footer |
| extJobDescHeader | string | External description header |
| extJobDescFooter | string | External description footer |
| templateHeaderFooter | string (nullable) | Template header/footer |

### JobRequisitionPosting
| Field | Type | Description |
|-------|------|-------------|
| jobPostingId | int64 | Primary key |
| jobReqId | int64 | Reference to requisition |
| boardId | string | Board ID |
| boardName | string | Board name |
| channelId | string | Channel ID |
| extPartnerAccountId | int64 | External partner account ID |
| postingStatus | string | Posting status |
| postedBy | string | Posted by user |
| postStartDate | datetime | Posting start date |
| postStartDateOffset | datetime | Start date with offset |
| postEndDate | datetime | Posting end date |
| postEndDateOffset | datetime | End date with offset |
| agencyComments | string | Agency comments |
| lastModifiedBy | string | Last modified by |
| lastModifiedDateTime | datetime | Last modified timestamp |

### JobRequisitionOperator
| Field | Type | Description |
|-------|------|-------------|
| jobReqId | int64 | Reference to requisition (composite key) |
| operatorRole | string | Operator role (composite key) |
| userName | string | Username (composite key) |
| usersSysId | string | System user ID |
| firstName | string | First name |
| lastName | string | Last name |
| email | string | Email address |
| phone | string | Phone number |
| fax | string | Fax number |
| isOwner | boolean | Owner flag |
| isAdminSelected | boolean | Admin selected flag |
| adminSelectedUserToBeRemoved | boolean (nullable) | Removal flag |

### JobRequisitionGroupOperator
| Field | Type | Description |
|-------|------|-------------|
| jobReqId | int64 | Reference to requisition (composite key) |
| operatorRole | string | Operator role (composite key) |
| userGroupId | int64 | User group ID (composite key) |
| userGroupName | string | User group name |
| isAdminSelected | boolean | Admin selected flag |
| isDisabled | boolean (nullable) | Disabled flag |
| adminSelectedGroupToBeRemoved | boolean (nullable) | Removal flag |

### JobReqQuestion
| Field | Type | Description |
|-------|------|-------------|
| questionId | int64 | Primary key |
| questionName | string | Question name |
| questionCategory | string | Question category |
| questionSource | string | Question source |
| questionType | string | Question type |

### JobReqScreeningQuestion
| Field | Type | Description |
|-------|------|-------------|
| questionId | int64 | Primary key |
| jobReqId | int64 | Reference to requisition |
| locale | string | Locale code |
| order | int64 | Question order |
| questionName | string | Question name |
| questionDescription | string | Question description |
| questionType | string | Question type |
| jobReqContent | string | Job requisition content |
| expectedDir | string | Expected direction |
| expectedAnswerValue | double | Expected answer value |
| questionWeight | double | Question weight |
| ratingScale | string | Rating scale |
| maxLength | int64 | Maximum length |
| required | boolean | Required flag |
| score | boolean | Score flag |
| disqualifier | boolean | Disqualifier flag |
| questionParentId | int64 | Parent question ID |
| questionParentResponse | string | Parent question response |

### JobReqScreeningQuestionChoice
| Field | Type | Description |
|-------|------|-------------|
| optionId | int64 | Primary key |
| locale | string | Locale code |
| optionLabel | string | Option label |
| optionValue | double | Option value |

## Get Object Primary Keys

| Entity | Primary Key(s) |
|--------|----------------|
| JobRequisition | jobReqId |
| JobRequisitionLocale | jobReqLocalId |
| JobRequisitionPosting | jobPostingId |
| JobRequisitionOperator | jobReqId, operatorRole, userName |
| JobRequisitionGroupOperator | jobReqId, operatorRole, userGroupId |
| JobReqQuestion | questionId |
| JobReqScreeningQuestion | questionId |
| JobReqScreeningQuestionChoice | optionId |

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Notes |
|--------|---------------|--------------|-------|
| JobRequisition | cdc | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| JobRequisitionPosting | cdc | lastModifiedDateTime | Supports incremental sync |
| JobRequisitionLocale | snapshot | N/A | Linked to requisition, use requisition cursor |
| JobRequisitionOperator | snapshot | N/A | Reference data linked to requisition |
| JobRequisitionGroupOperator | snapshot | N/A | Reference data linked to requisition |
| JobReqQuestion | snapshot | N/A | Reference/configuration data |
| JobReqScreeningQuestion | snapshot | N/A | Linked to requisition |
| JobReqScreeningQuestionChoice | snapshot | N/A | Linked to screening questions |

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

**Get all job requisitions:**
```http
GET https://{api-server}/odata/v2/JobRequisition?$top=100
```

**Get job requisition by ID:**
```http
GET https://{api-server}/odata/v2/JobRequisition({jobReqId})
```

**Get requisitions modified after a date (incremental sync):**
```http
GET https://{api-server}/odata/v2/JobRequisition?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get open requisitions:**
```http
GET https://{api-server}/odata/v2/JobRequisition?$filter=internalStatus eq 'Open'
```

**Get requisition with expanded locales and postings:**
```http
GET https://{api-server}/odata/v2/JobRequisition({jobReqId})?$expand=jobReqLocale,jobReqPostings
```

**Get requisition with all operators:**
```http
GET https://{api-server}/odata/v2/JobRequisition({jobReqId})?$expand=recruiter,hiringManager,coordinator,approver
```

**Get requisition postings:**
```http
GET https://{api-server}/odata/v2/JobRequisitionPosting?$filter=jobReqId eq {jobReqId}
```

**Get screening questions for a requisition:**
```http
GET https://{api-server}/odata/v2/JobReqScreeningQuestion?$filter=jobReqId eq {jobReqId}&$expand=choices
```

**Select specific fields:**
```http
GET https://{api-server}/odata/v2/JobRequisition?$select=jobReqId,positionNumber,department,location,internalStatus,lastModifiedDateTime
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

- SAP SuccessFactors API Spec: RCMJobRequisition.json
- SAP Help Portal: [Job Requisition APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/5511f05d636f414084a22909bc35f509.html)
- API Server List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
