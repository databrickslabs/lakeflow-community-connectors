# Compensation Management API Documentation

## Authorization
- **Method**: OAuth 2.0 (Client Credentials Flow)
- **Security Scheme**: sfOauth
- **Token Endpoint**: `https://{api-server}/oauth/token`
- **Headers**:
  - `Authorization: Bearer <access_token>`
- **Documentation**: [SAP SuccessFactors OAuth 2.0](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/d9a9545305004187986c866de2b66987.html?locale=en-US)

## Object List
The API provides access to the following resources:

| Resource | Description |
|----------|-------------|
| Employee Compensation | Compensation records from salary, stock, bonus, and variable pay worksheets |

## Object Schema

### EmployeeCompEntryDTO (Main Object)
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| compEntryId | integer (int64) | Yes | Compensation entry ID |
| compPlanId | integer (int64) | Yes | Compensation plan ID |
| formDataId | integer (int64) | Yes | Compensation form data ID |
| compTemplateId | integer (int64) | No | Compensation template ID |
| entryUserId | string | No | Entry user ID |
| entryUserGroupName | string | No | Entry user group name |
| entryUserName | string | No | Entry username |
| entryUserFirstName | string | No | Entry user first name |
| entryUserLastName | string | No | Entry user last name |
| entryUserMiddleName | string | No | Entry user middle name |
| revieweeMgrUserId | string | No | Manager user ID |
| revieweeMgrUserName | string | No | Manager username |
| revieweeMgrUserFirstName | string | No | Manager user first name |
| revieweeMgrUserLastName | string | No | Manager user last name |
| revieweeMgrUserMiddleName | string | No | Manager user middle name |
| department | string | No | Department |
| division | string | No | Division |
| location | string | No | Location |
| localCurrencyCode | string | No | Entry local currency code |
| functionalCurrencyCode | string | No | Template functional currency code |
| isECUser | boolean | No | Indicates if Employee Central user |
| isTotalComp | boolean | No | Indicates if total compensation plan |
| salary | object (Salary) | No | Salary details |
| stock | object (Stock) | No | Stock details |
| bonus | object (Bonus) | No | Bonus details |
| varpay | object (VariablePay) | No | Variable pay details |
| ratingInfos | array[RatingInformation] | No | Rating information |
| commentList | array[ForceComment] | No | Force comments |

### EmployeeCompSalaryEntryDTO (Salary)
| Field | Type | Description |
|-------|------|-------------|
| compEntryId | integer (int64) | Compensation entry ID |
| startDate | string (date-time) | Date of current position |
| prorating | number (double) | Prorating |
| curSalary | number (double) | Current salary |
| curSalaryLocal | number (double) | Current salary in local currency |
| promo | number (double) | Promotion |
| merit | number (double) | Merit |
| meritTarget | number (double) | Merit target |
| extra | number (double) | Adjustment |
| extra2 | number (double) | Adjustment2 |
| raiseProrating | number (double) | Total raise prorating |
| raise | number (double) | Total raise |
| finalSalary | number (double) | Final salary |
| finalSalaryLocal | number (double) | Final salary in local currency |
| salaryRateLocalCurrency | number (double) | Salary rate in local currency |
| salaryRatePlanCurrency | number (double) | Salary rate in plan currency |
| salaryRateUnits | number (double) | Units per year |
| salaryRateType | string | Salary type (e.g., "ANNUAL") |
| salaryRateFinal | number (double) | Final salary rate |
| jobLevel | string | Job level |
| jobTitle | string | Title |
| payGrade | string | Current pay grade |
| finalPayGrade | string | Final pay grade |
| jobFamily | string | Current job family |
| jobRole | string | Current job role |
| jobCode | string | Current job code |
| finalJobFamily | string | Final job family |
| finalJobRole | string | Final job role |
| finalJobCode | string | Final job code |
| comparisonRatio | number (double) | Final compa ratio |
| rangePenetration | number (double) | Final position in range |
| curComparisonRatio | number (double) | Current compa ratio |
| curRangePenetration | number (double) | Current position in range |
| lumpSum | number (double) | Lump sum |
| lumpSumTarget | number (double) | Lump sum target |
| lumpSum2 | number (double) | Lump sum 2 |
| lumpSum2Target | number (double) | Lump sum target 2 |
| totalLumpSum | number (double) | Total lump sum |
| totalIncrease | number (double) | Total increase |
| totalComp | number (double) | Total compensation |
| totalCompLocal | number (double) | Total compensation in local currency |
| promoDate | string (date-time) | Promotion effective date |
| meritDate | string (date-time) | Merit raise effective date |
| extraDate | string (date-time) | Adjustment effective date |
| extra2Date | string (date-time) | Adjustment2 effective date |
| lumpsumDate | string (date-time) | Lumpsum effective date |
| lumpsum2Date | string (date-time) | Lumpsum2 effective date |
| promoNet | number (double) | Promotion cash flow impact |
| meritNet | number (double) | Merit raise cash flow impact |
| extraNet | number (double) | Adjustment cash flow impact |
| extra2Net | number (double) | Adjustment2 cash flow impact |
| proratingStartDate | string (date-time) | Salary prorating start date |
| proratingEndDate | string (date-time) | Salary prorating end date |
| raiseProratingStartDate | string (date-time) | Total raise prorating start date |
| raiseProratingEndDate | string (date-time) | Total raise prorating end date |
| isIneligible | boolean | Indicates if entry is ineligible |
| isPromoIneligible | boolean | Indicates if ineligible for promotion |
| isMeritIneligible | boolean | Indicates if ineligible for merit raise |
| isExtraIneligible | boolean | Indicates if ineligible for adjustment |
| isExtra2Ineligible | boolean | Indicates if ineligible for adjustment2 |
| isLumpsumIneligible | boolean | Indicates if ineligible for lump sum |
| isLumpsum2Ineligible | boolean | Indicates if ineligible for lump sum2 |
| payGuideMin | number (double) | Current salary range minimum |
| payGuideMid | number (double) | Current salary range midpoint |
| payGuideMax | number (double) | Current salary range maximum |
| finalPayGuideMin | number (double) | Final salary range minimum |
| finalPayGuideMid | number (double) | Final salary range midpoint |
| finalPayGuideMax | number (double) | Final salary range maximum |
| promoGuideline | object (Guideline) | Promotion guideline |
| meritGuideline | object (Guideline) | Merit guideline |
| extraGuideline | object (Guideline) | Adjustment guideline |
| extra2Guideline | object (Guideline) | Adjustment2 guideline |
| lumpSumGuideline | object (Guideline) | Lump sum guideline |
| lumpSum2Guideline | object (Guideline) | Lump sum2 guideline |
| salaryNotes | string | Salary notes |
| customFields | object | Reportable custom fields in salary tab |

### EmployeeCompBonusEntryDTO (Bonus)
| Field | Type | Description |
|-------|------|-------------|
| compEntryId | integer (int64) | Compensation entry ID |
| target | number (double) | Target |
| extraPercent | number (double) | User modifier percent |
| extra | number (double) | Adjustment |
| total | number (double) | Total |
| isIneligible | boolean | Indicates if entry is ineligible |
| bonusNotes | string | Bonus notes |
| customFields | object | Reportable custom fields in bonus tab |

### EmployeeCompStockEntryDTO (Stock)
| Field | Type | Description |
|-------|------|-------------|
| compEntryId | integer (int64) | Compensation entry ID |
| stock | number (double) | Stock |
| grantDate | string (date-time) | Date of grant |
| option | number (double) | Options |
| units | number (double) | Units |
| other1 | number (double) | Other1 |
| other2 | number (double) | Other2 |
| other3 | number (double) | Other3 |
| isIneligible | boolean | Indicates if entry is ineligible |
| optionGuideline | object (Guideline) | Options guideline |
| stockGuideline | object (Guideline) | Stock guideline |
| unitsGuideline | object (Guideline) | Units guideline |
| other1Guideline | object (Guideline) | Other1 guideline |
| other2Guideline | object (Guideline) | Other2 guideline |
| other3Guideline | object (Guideline) | Other3 guideline |
| stockNotes | string | Stock notes |
| customFields | object | Reportable custom fields in stock tab |

### EmployeeCompVarpayEntryDTO (Variable Pay)
| Field | Type | Description |
|-------|------|-------------|
| compEntryId | integer (int64) | Compensation entry ID |
| performancePer | number (double) | Individual percent |
| teamPerformancePer | number (double) | Team percent |
| varpayTeamRating | number (double) | Team rating |
| varpayIndividualRating | number (double) | Individual rating |
| overwriteAmt | number (double) | Override |
| overwriteDesc | string | Override description |
| overwriteLastModified | string (date-time) | Last modified date of override |
| notes | string | Variable pay notes |
| teamPayoutAmt | number (double) | Team amount |
| finalForecastPayoutAmt | number (double) | Forecast amount |
| finalForecast2PayoutAmt | number (double) | Total |
| finalPayoutAmt | number (double) | Final payout |
| customFields | object | Reportable custom fields in variable pay tab |

### EmployeeCompRatingInfoDTO (Rating Information)
| Field | Type | Description |
|-------|------|-------------|
| compEntryId | integer (int64) | Compensation entry ID |
| ratingSource | string | Rating source (PM, EmployeeProfile) |
| ratingType | string | Rating type |
| rating | number (double) | Rating |
| ratingDesc | string | Rating description |
| compRating | number (double) | Comp rating |
| pmFormTemplateId | integer (int64) | Performance form template ID |
| pmFormDataId | integer (int64) | Performance form data ID |
| feedbackId | integer (int64) | Feedback ID |

### EmployeeCompForceCommentDTO (Force Comment)
| Field | Type | Description |
|-------|------|-------------|
| compCommentId | integer (int64) | Force comment ID |
| compEntryId | integer (int64) | Compensation entry ID |
| compCommentType | integer (int32) | Force comment type |
| compCommentField | string | Force comment field |
| compCommentComment | string | Force comment content |
| createdDate | string (date-time) | Created date |
| createdBy | string | Author |
| lastModified | string (date-time) | Last modified date |
| lastModifiedBy | string | Author of last modification |

### EmployeeCompGuidelineDTO (Guideline)
| Field | Type | Description |
|-------|------|-------------|
| useAmt | boolean | Indicates if guideline uses amount |
| min | number (double) | Minimum |
| mid | number (double) | Midpoint |
| max | number (double) | Maximum |
| low | number (double) | Low |
| high | number (double) | High |
| formattedString | string | Formatted guideline information |

## Get Object Primary Keys
| Object | Primary Key Field(s) |
|--------|---------------------|
| EmployeeCompEntryDTO | compEntryId, compPlanId, formDataId |
| Salary | compEntryId |
| Bonus | compEntryId |
| Stock | compEntryId |
| VariablePay | compEntryId |

## Object's Ingestion Type
| Object | Ingestion Type | Reasoning |
|--------|----------------|-----------|
| EmployeeCompensation | `snapshot` | No timestamp-based filtering. Filter by templateId, compPlanId, formDataId, location, department, division. ForceComment has lastModified but no API filter support for it. |

**Notes**:
- Data is retrieved by templateId (required)
- Supports filtering by compPlanId, formDataId, location, department, and division
- No timestamp-based incremental query support available
- ForceComment objects contain lastModified field but cannot be used for incremental queries

## Read API for Data Retrieval

### Get Employee Compensation Records
- **Endpoint**: `GET /employeeCompensations`
- **Base URL**: `https://{api-server}/rest/rewards/compensation/v1`
- **Full URL**: `https://{api-server}/rest/rewards/compensation/v1/employeeCompensations`

#### Query Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| templateId | integer (int64) | Yes | Template ID |
| $skip | integer (int32) | No | Number of records to skip |
| $top | integer (int32) | No | Maximum number of records to return |
| $filter | string | No | Filter by compPlanId, formDataId, location, department, division (supports `eq` and `in`) |
| $expand | string | No | Expand related entities: salary, bonus, stock, varpay, commentList, ratingInfos |
| $select | string | No | Select specific fields |

#### Example Requests

**Basic Request**:
```http
GET https://apisalesdemo8.successfactors.com/rest/rewards/compensation/v1/employeeCompensations?templateId=100
Authorization: Bearer <access_token>
```

**With Filtering**:
```http
GET https://apisalesdemo8.successfactors.com/rest/rewards/compensation/v1/employeeCompensations?templateId=100&$filter=compPlanId eq 100 and department in ('Engineering','HR')
Authorization: Bearer <access_token>
```

**With Expansion and Selection**:
```http
GET https://apisalesdemo8.successfactors.com/rest/rewards/compensation/v1/employeeCompensations?templateId=100&$expand=salary($select=curSalary,finalSalary)&$select=entryUserId,department
Authorization: Bearer <access_token>
```

**With Pagination**:
```http
GET https://apisalesdemo8.successfactors.com/rest/rewards/compensation/v1/employeeCompensations?templateId=100&$skip=200&$top=25
Authorization: Bearer <access_token>
```

#### Response Structure
```json
{
  "count": 100,
  "value": [
    {
      "compEntryId": 2000,
      "compPlanId": 100,
      "formDataId": 200,
      "compTemplateId": 500,
      "entryUserId": "user1",
      "entryUserName": "adminUser",
      "entryUserFirstName": "John",
      "entryUserLastName": "Doe",
      "department": "Engineering",
      "division": "Human Resource",
      "location": "USA",
      "localCurrencyCode": "USD",
      "functionalCurrencyCode": "EUR",
      "isECUser": true,
      "isTotalComp": true,
      "salary": {
        "compEntryId": 1000,
        "curSalary": 10000.0,
        "finalSalary": 20000.0
      },
      "bonus": {
        "compEntryId": 1000,
        "target": 100.0,
        "total": 50.0
      },
      "stock": null,
      "varpay": null,
      "ratingInfos": [],
      "commentList": []
    }
  ]
}
```

#### Pagination
- **Method**: Offset-based pagination using `$skip` and `$top`
- **$skip**: Number of records to skip (default: 0)
- **$top**: Maximum records per page (default varies)
- **Total Count**: Available in response `count` field

## Field Type Mapping
| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer (int32) | IntegerType |
| integer (int64) | LongType |
| number (double) | DoubleType |
| boolean | BooleanType |
| string (date) | DateType |
| string (date-time) | TimestampType |
| array | ArrayType |
| object | StructType |

## Sources and References
- **SAP SuccessFactors API Spec**: sap-sf-employeeCompensation-v1.json
- **SAP Help Portal**: [Compensation Management API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_RELEASE_INFORMATION/8974cf00008b4e209398478ca43bcba7/4c437eb16661469ab37e7e6377949567.html)
