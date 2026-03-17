# Job Offer API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **JobOffer** - Main job offer entity with compensation details
2. **JobOfferApprover** - Approvers for job offers
3. **OfferLetter** - Offer letter documents and details
4. **RCMAdminReassignOfferApprover** - Admin tool for reassigning offer approvers

## Object Schema

### JobOffer
| Field | Type | Description |
|-------|------|-------------|
| offerApprovalId | int64 | Primary key |
| applicationId | int64 | Reference to job application |
| candidateName | string (nullable) | Candidate name |
| currency | string (nullable) | Currency code |
| location | string (nullable) | Job location |
| currentLocation | string (nullable) | Current location |
| internalStatus | string (nullable) | Internal approval status |
| candJust | string (nullable) | Candidate justification |
| initialComment | string (nullable) | Initial comments |
| salaryBase | decimal (nullable) | Base salary |
| jobRateOfPay | decimal (nullable) | Rate of pay |
| commission | decimal (nullable) | Commission |
| targetBonusAmount | decimal (nullable) | Target bonus |
| stockPackage | string (nullable) | Stock package details |
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
| templateId | int64 (nullable) | Offer template ID |
| formTemplateId | int64 (nullable) | Form template ID |
| formDataId | int64 (nullable) | Form data ID |
| version | int64 (nullable) | Offer version |
| tempDate | string (nullable) | Temporary date |
| redefineTemplateApprovers | string (nullable) | Redefine approvers flag |
| anonymizedFlag | string (nullable) | Anonymization flag |
| anonymizedDate | datetime (nullable) | Anonymization date |
| createdBy | string | Created by user |
| createdDate | datetime | Creation timestamp |
| lastModifiedBy | string | Last modified by user |
| lastModifiedDate | datetime | Last modified timestamp |

### JobOfferApprover
| Field | Type | Description |
|-------|------|-------------|
| offerApproverId | int64 | Primary key |
| offerApprovalId | int64 | Reference to job offer |
| username | string | Approver username |
| approverFirstName | string | Approver first name |
| approverLastName | string | Approver last name |
| approverOrder | int64 | Approval order |
| approvalStepId | string (nullable) | Approval step ID |
| approverAction | string (nullable) | Approver action taken |
| approverActionDate | datetime (nullable) | Action date |
| comment | string (nullable) | Approver comment |
| createdBy | string | Created by user |
| createdDate | datetime | Creation timestamp |
| lastModifiedBy | string | Last modified by user |
| lastModifiedDate | datetime | Last modified timestamp |

### OfferLetter
| Field | Type | Description |
|-------|------|-------------|
| offerLetterId | int64 | Primary key |
| applicationId | int64 | Reference to job application |
| templateId | int64 (nullable) | Letter template ID |
| templateName | string (nullable) | Template name |
| bodyTemplateId | int64 (nullable) | Body template ID |
| body | string (nullable) | Letter body content |
| bodyLocale | string (nullable) | Body locale |
| locale | string (nullable) | Offer locale |
| localeCode | string (nullable) | Locale code |
| subject | string (nullable) | Email subject |
| status | string (nullable) | Letter status |
| sendMode | string (nullable) | Send mode |
| jobTitle | string (nullable) | Job title |
| jobStartDate | datetime (nullable) | Job start date |
| offerExpirationDate | datetime (nullable) | Offer expiration date |
| offerSentDate | datetime (nullable) | Offer sent date |
| currencyCode | string (nullable) | Currency code |
| countryCode | string (nullable) | Country code |
| countryName | string (nullable) | Country name |
| salaryRate | int64 (nullable) | Salary rate |
| salaryRateType | string (nullable) | Salary rate type |
| overtimeRate | int64 (nullable) | Overtime rate |
| bonusPayoutFreq | string (nullable) | Bonus payout frequency |
| targetBonusAmount | int64 (nullable) | Target bonus amount |
| targetBonusPercent | int64 (nullable) | Target bonus percentage |
| stockGrant | int64 (nullable) | Stock grant |
| stockOption | int64 (nullable) | Stock options |
| tokens | string (nullable) | Template tokens |
| mailboxes | string (nullable) | Email mailboxes |
| offerLetter | binary (nullable) | Offer letter document (base64) |
| comments | string (nullable) | Comments |
| candResponseComments | string (nullable) | Candidate response comments |
| candResponseDate | datetime (nullable) | Candidate response date |
| createdBy | string (nullable) | Created by user |
| createDate | datetime (nullable) | Creation timestamp |
| lastModifiedBy | string (nullable) | Last modified by user |
| lastModifiedDate | datetime (nullable) | Last modified timestamp |

### RCMAdminReassignOfferApprover
| Field | Type | Description |
|-------|------|-------------|
| applicationId | int64 | Application ID (composite key) |
| currUserId | string | Current user ID (composite key) |
| offerDetailId | int64 | Offer detail ID (composite key) |
| candidateName | string (nullable) | Candidate name |
| jobReqId | int64 (nullable) | Job requisition ID |
| jobReqTitle | string (nullable) | Job requisition title |
| targetUserId | string (nullable) | Target user for reassignment |

## Get Object Primary Keys

| Entity | Primary Key(s) |
|--------|----------------|
| JobOffer | offerApprovalId |
| JobOfferApprover | offerApproverId |
| OfferLetter | offerLetterId |
| RCMAdminReassignOfferApprover | applicationId, currUserId, offerDetailId |

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Notes |
|--------|---------------|--------------|-------|
| JobOffer | cdc | lastModifiedDate | Supports $filter and $orderby on lastModifiedDate |
| JobOfferApprover | cdc | lastModifiedDate | Supports incremental sync |
| OfferLetter | cdc | lastModifiedDate | Supports incremental sync |
| RCMAdminReassignOfferApprover | snapshot | N/A | Admin utility entity |

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

**Get all job offers:**
```http
GET https://{api-server}/odata/v2/JobOffer?$top=100
```

**Get job offer by ID:**
```http
GET https://{api-server}/odata/v2/JobOffer({offerApprovalId})
```

**Get offers modified after a date (incremental sync):**
```http
GET https://{api-server}/odata/v2/JobOffer?$filter=lastModifiedDate gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDate asc
```

**Get job offer with approvers:**
```http
GET https://{api-server}/odata/v2/JobOffer({offerApprovalId})?$expand=approvers
```

**Get offer letter by application ID:**
```http
GET https://{api-server}/odata/v2/OfferLetter?$filter=applicationId eq {applicationId}
```

**Get offer approvers for a specific offer:**
```http
GET https://{api-server}/odata/v2/JobOfferApprover?$filter=offerApprovalId eq {offerApprovalId}&$orderby=approverOrder asc
```

**Select specific fields:**
```http
GET https://{api-server}/odata/v2/JobOffer?$select=offerApprovalId,applicationId,candidateName,salaryBase,internalStatus,lastModifiedDate
```

### Service Operations (Actions/Functions)

**Approve an offer:**
```http
GET https://{api-server}/odata/v2/approveOffer?applicationId={applicationId}&comment='{comment}'
```

**Decline an offer:**
```http
GET https://{api-server}/odata/v2/declineOffer?applicationId={applicationId}&comment='{comment}'
```

**Send offer for approval:**
```http
GET https://{api-server}/odata/v2/sendOfferForApproval?applicationId={applicationId}
```

**Send offer letter email:**
```http
GET https://{api-server}/odata/v2/sendMailOfferLetter?offerLetterId={offerLetterId}&sendMode='{sendMode}'&bodyTemplateId={bodyTemplateId}&bodyLocale='{bodyLocale}'
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
| Edm.Binary | BinaryType |

## Sources and References

- SAP SuccessFactors API Spec: RCMOffer.json
- SAP Help Portal: [Job Offer APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/f8d99c65e2f14237a329dbbd29760f61.html)
- API Server List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
