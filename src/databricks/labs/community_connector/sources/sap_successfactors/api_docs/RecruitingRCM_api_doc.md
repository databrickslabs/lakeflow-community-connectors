# Job Application Interview API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **JobApplicationInterview** - Job application interview records
2. **InterviewOverallAssessment** - Overall interview assessment scores
3. **InterviewIndividualAssessment** - Individual competency assessments
4. **RcmCompetency** - RCM competency definitions
5. **JobReqFwdCandidates** - Candidates forwarded to job requisitions
6. **JobApplicationAudit** - Audit trail for job application changes

## Object Schema

### JobApplicationInterview
| Field | Type | Description |
|-------|------|-------------|
| applicationInterviewId | int64 | Primary key |
| applicationId | int64 | Reference to job application |
| candSlotMapId | int64 | Candidate slot map ID |
| recruitEventStaffId | int64 | Recruiting event staff ID |
| source | string | Interview source |
| status | string | Interview status |
| templateType | string | Template type |
| startDate | datetime | Interview start date/time |
| endDate | datetime | Interview end date/time |
| isTimeSet | int32 | Time set flag |
| notes | string (nullable) | Interview notes |

### InterviewOverallAssessment
| Field | Type | Description |
|-------|------|-------------|
| interviewOverallAssessmentId | int64 | Primary key |
| interviewRefId | int64 | Reference to interview |
| overallRating | string | Overall rating |
| averageRating | double (nullable) | Average rating score |
| type | string (nullable) | Assessment type |
| comments | string (nullable) | Assessment comments |

### InterviewIndividualAssessment
| Field | Type | Description |
|-------|------|-------------|
| interviewIndividualAssessmentId | int64 | Primary key |
| interviewOverallAssessmentId | int64 | Reference to overall assessment |
| refId | int64 | Reference ID |
| type | string | Assessment type |
| rating | string | Rating value |
| comments | string (nullable) | Assessment comments |
| isDeleted | string (nullable) | Deleted flag |

### RcmCompetency
| Field | Type | Description |
|-------|------|-------------|
| rcmCompetencyId | int64 | Primary key |
| commonCompetencyId | int64 | Common competency ID |
| name | string | Competency name |
| desc | string | Competency description |
| category | string | Competency category |
| type | string | Competency type |
| source | string | Competency source |
| locale | string | Locale code |

### JobReqFwdCandidates
| Field | Type | Description |
|-------|------|-------------|
| candidateId | int64 | Candidate ID (composite key) |
| jobReqId | int64 | Job requisition ID (composite key) |
| referralId | int64 | Referral ID |
| referralKey | string (nullable) | Referral key |
| referredBy | string (nullable) | Referred by user |
| type | string | Forwarding type |
| status | string (nullable) | Status |
| jobBoardName | string (nullable) | Job board name |
| candidateSiteid | int64 (nullable) | Candidate site ID |
| extRecruiterId | int64 (nullable) | External recruiter ID |
| rcmAppStatusSetItemId | int64 (nullable) | App status set item ID |
| expirationDate | datetime (nullable) | Expiration date |
| createdDate | datetime | Creation timestamp |
| lastModifiedDate | datetime | Last modified timestamp |

### JobApplicationAudit
| Field | Type | Description |
|-------|------|-------------|
| fieldOrderPos | int64 | Field order position (composite key) |
| revNumber | int64 | Revision number (composite key) |
| revType | int32 (nullable) | Revision type |
| fieldId | string (nullable) | Field identifier |
| fieldType | string (nullable) | Field type |
| refType | string (nullable) | Reference type |
| source | string (nullable) | Change source |
| oldValue | string (nullable) | Previous value |
| newValue | string (nullable) | New value |
| dateOldValue | datetime (nullable) | Previous date value |
| dateNewValue | datetime (nullable) | New date value |
| clobFieldRef | string (nullable) | CLOB field reference |
| clobOldValueXML | string (nullable) | Previous CLOB value (XML) |
| clobNewValueXML | string (nullable) | New CLOB value (XML) |
| changedBy | string (nullable) | Changed by user |
| mergedFrom | string (nullable) | Merged from |
| jobPostingId | int64 (nullable) | Job posting ID |
| jobPostStartDate | datetime (nullable) | Job post start date |
| jobPostEndDate | datetime (nullable) | Job post end date |
| lastModifiedBy | string (nullable) | Last modified by |
| lastModifiedDate | datetime (nullable) | Last modified timestamp |
| lastModifiedExtId | int64 (nullable) | Last modified external ID |

## Get Object Primary Keys

| Entity | Primary Key(s) |
|--------|----------------|
| JobApplicationInterview | applicationInterviewId |
| InterviewOverallAssessment | interviewOverallAssessmentId |
| InterviewIndividualAssessment | interviewIndividualAssessmentId |
| RcmCompetency | rcmCompetencyId |
| JobReqFwdCandidates | candidateId, jobReqId |
| JobApplicationAudit | fieldOrderPos, revNumber |

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Notes |
|--------|---------------|--------------|-------|
| JobApplicationInterview | snapshot | N/A | Use startDate/endDate for filtering |
| InterviewOverallAssessment | snapshot | N/A | Assessment records |
| InterviewIndividualAssessment | snapshot | N/A | Individual assessment records |
| RcmCompetency | snapshot | N/A | Reference/configuration data |
| JobReqFwdCandidates | cdc | lastModifiedDate | Supports incremental sync |
| JobApplicationAudit | cdc | lastModifiedDate | Audit records with timestamps |

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

**Get all job application interviews:**
```http
GET https://{api-server}/odata/v2/JobApplicationInterview?$top=100
```

**Get interview by ID:**
```http
GET https://{api-server}/odata/v2/JobApplicationInterview({applicationInterviewId})
```

**Get interviews for a specific application:**
```http
GET https://{api-server}/odata/v2/JobApplicationInterview?$filter=applicationId eq {applicationId}
```

**Get interview with overall assessment:**
```http
GET https://{api-server}/odata/v2/JobApplicationInterview({applicationInterviewId})?$expand=interviewOverallAssessment
```

**Get overall assessment with individual assessments:**
```http
GET https://{api-server}/odata/v2/InterviewOverallAssessment({interviewOverallAssessmentId})?$expand=interviewIndividualAssessment
```

**Get individual assessment with competency:**
```http
GET https://{api-server}/odata/v2/InterviewIndividualAssessment({id})?$expand=rcmCompetency
```

**Get forwarded candidates for a job requisition:**
```http
GET https://{api-server}/odata/v2/JobReqFwdCandidates?$filter=jobReqId eq {jobReqId}
```

**Get forwarded candidates modified after a date (incremental sync):**
```http
GET https://{api-server}/odata/v2/JobReqFwdCandidates?$filter=lastModifiedDate gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDate asc
```

**Get job application audit records:**
```http
GET https://{api-server}/odata/v2/JobApplicationAudit?$filter=lastModifiedDate gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDate asc
```

**Get all RCM competencies:**
```http
GET https://{api-server}/odata/v2/RcmCompetency?$top=1000
```

**Select specific fields:**
```http
GET https://{api-server}/odata/v2/JobApplicationInterview?$select=applicationInterviewId,applicationId,status,startDate,endDate
```

### Service Operations (Actions/Functions)

**Forward candidate to colleague:**
```http
GET https://{api-server}/odata/v2/fwdCandidateToColleague?candidateId={candidateId}&referredTo='{userId}'
```

**Initiate onboarding:**
```http
GET https://{api-server}/odata/v2/initiateOnboarding?applicationId={applicationId}
```

**Get recruiting template:**
```http
GET https://{api-server}/odata/v2/getRecruitingTemplate?templateName='{templateName}'&templateType='{templateType}'
```

**Get offer letter template:**
```http
GET https://{api-server}/odata/v2/getOfferLetterTemplate?templateName='{templateName}'&templateType='{templateType}'
```

**Invite candidates to apply (POST):**
```http
POST https://{api-server}/odata/v2/inviteToApply?jobReqId={jobReqId}&candidateIds='{candidateIds}'
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

- SAP SuccessFactors API Spec: RecruitingRCM.json
- SAP Help Portal: [Job Application Interview APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/54256a02616448f6855cfbf6320bb97c.html)
- API Server List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
