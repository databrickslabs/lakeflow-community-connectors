# Lakeflow SAP SuccessFactors Community Connector

This documentation provides setup instructions and reference information for the SAP SuccessFactors source connector.

The Lakeflow SAP SuccessFactors Connector enables you to extract HR and talent management data from your SAP SuccessFactors instance and load it into Databricks. SAP SuccessFactors is a cloud-based Human Capital Management (HCM) suite that provides comprehensive HR solutions including employee data, organizational structures, compensation, performance, and learning management. This connector leverages the SAP SuccessFactors OData v2 API to synchronize data efficiently using incremental ingestion patterns.

## Prerequisites

Before configuring the connector, ensure you have the following:

- Access to a SAP SuccessFactors instance with API access enabled
- A user account with appropriate API permissions to read the required data entities
- Your SAP SuccessFactors Company ID (available in your instance URL or Admin Center)
- Username credentials in the format `username@companyId`
- Password for the user account

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `endpoint_url` | string | Yes | SAP SuccessFactors API endpoint URL. This is typically the OData API base URL for your datacenter region. | `https://api.successfactors.com/` |
| `username` | string | Yes | Username in the format `username@companyId`. The Company ID identifies your SAP SuccessFactors tenant. | `sfadmin@COMPANY123` |
| `password` | string | Yes | Password for the user account with API access permissions. | `********` |

**Note:** The `externalOptionsAllowList` must include `tableConfigs,tableNameList` - these are required framework options that the pipeline uses for metadata retrieval.

### Getting Your Credentials

#### Finding Your Company ID

Your SAP SuccessFactors Company ID can be found in several locations:

1. **From the URL**: When logged into SAP SuccessFactors, your Company ID appears in the browser URL, typically in the format `https://{datacenter}.successfactors.com/sf/home?company={COMPANY_ID}`

2. **From Admin Center**:
   - Navigate to **Admin Center** > **Company Settings** > **Company System and Logo Settings**
   - The Company ID is displayed in the Company Information section

3. **From Your SAP Administrator**: Contact your SAP SuccessFactors administrator who can provide the Company ID from the provisioning settings

#### Getting API User Credentials

To obtain API access credentials:

1. **Using an Existing User Account**:
   - Ensure the user has the appropriate permissions to access OData API endpoints
   - Required permissions typically include: `API User`, `OData API General Access`, and read permissions for specific entities

2. **Creating a Dedicated API User** (Recommended):
   - Navigate to **Admin Center** > **Manage Permission Roles**
   - Create a new permission role with OData API access
   - Go to **Admin Center** > **Manage Users**
   - Create a dedicated integration user with the API permission role
   - Enable API login for this user

3. **Verifying API Access**:
   - Log into SAP SuccessFactors Admin Center
   - Navigate to **Admin Center** > **Manage Permission Roles**
   - Verify the user's role includes `OData API General Access` permission
   - Check that entity-specific read permissions are granted for the data you need to sync

#### Finding Your API Endpoint URL

The API endpoint URL depends on your SAP SuccessFactors datacenter location:

| Datacenter | API Endpoint URL |
|------------|------------------|
| US (DC2) | `https://api2.successfactors.com/` |
| US (DC4) | `https://api4.successfactors.com/` |
| EU (DC5) | `https://api5.successfactors.eu/` |
| EU (DC15/Frankfurt) | `https://api15.sapsf.eu/` |
| APAC (DC8) | `https://api8.successfactors.com/` |
| APAC (DC10/Sydney) | `https://api10.successfactors.com/` |
| APAC (DC12/Singapore) | `https://api12.sapsf.cn/` |

Contact your SAP administrator or check your instance URL to determine your datacenter location.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways:

#### Option 1: Using the UI

1. Follow the Lakeflow Community Connector UI flow from the **Add Data** page in your Databricks workspace
2. Select **SAP SuccessFactors** as the source type
3. Create a new connection or select an existing Lakeflow Community Connector connection
4. Enter your credentials (endpoint URL, username, password)
5. Save the connection

#### Option 2: Using Databricks CLI

The connection can also be created using the Databricks CLI:

```bash
databricks connections create \
  --json '{
    "name": "sap_successfactors_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
      "sourceName": "sap_successfactors",
      "endpoint_url": "https://api.successfactors.com/",
      "username": "your-username@COMPANY_ID",
      "password": "your-password",
      "externalOptionsAllowList": "tableConfigs,tableNameList"
    }
  }'
```

**Important Notes:**

- `sourceName`: Must be set to `"sap_successfactors"` exactly as shown. This identifies the connector to use.
- `externalOptionsAllowList`: Must include `"tableConfigs,tableNameList"` - these are **required framework options** that the pipeline uses for metadata retrieval. Without these, the pipeline will fail with `DATA_SOURCE_OPTION_NOT_ALLOWED_BY_CONNECTION` error.

#### Managing Connections

**List existing connections:**
```bash
databricks connections list
```

**View connection details:**
```bash
databricks connections get "sap_successfactors_connection"
```

**Delete a connection:**
```bash
databricks connections delete "sap_successfactors_connection"
```

---

## Supported Objects

This connector supports **149 tables** across 9 SAP SuccessFactors modules. Tables support either **CDC (Change Data Capture)** for incremental synchronization or **Snapshot** mode for full refresh.

### 1. Employee Central Core (27 tables)

Core HR data including employment records, compensation, benefits, payroll, and payment information.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| Advance | NonRecurringPayment_externalCode, externalCode | lastModifiedDateTime | CDC |
| AdvancesAccumulation | externalCode | lastModifiedDateTime | CDC |
| AdvancesEligibility | effectiveStartDate, externalCode | lastModifiedDateTime | CDC |
| AdvancesInstallments | Advance_externalCode, NonRecurringPayment_externalCode, externalCode | lastModifiedDateTime | CDC |
| Benefit | benefitId, effectiveStartDate | lastModifiedDateTime | CDC |
| CustomPayType | externalCode | lastModifiedDateTime | CDC |
| CustomPayTypeAssignment | externalCode, CustomPayType_externalCode | lastModifiedDateTime | CDC |
| DeductionScreenId | externalCode | lastModifiedDateTime | CDC |
| EmpCompensation | startDate, userId | lastModifiedDateTime | CDC |
| EmpEmployment | prevEmployeeId, userId, personIdExternal | lastModifiedDateTime | CDC |
| EmpEmploymentHigherDuty | personIdExternal, userId, homeAssignment, startDate | - | Snapshot |
| EmpEmploymentTermination | userId, attachmentId, personIdExternal, newMainEmploymentId | lastModifiedDateTime | CDC |
| EmpJob | userId, managerId | lastModifiedDateTime | CDC |
| EmpJobRelationships | relUserId, userId | lastModifiedDateTime | CDC |
| EmpPayCompNonRecurring | payComponentCode, payDate, userId | lastModifiedDateTime | CDC |
| EmpPayCompRecurring | seqNumber, startDate, userId, payComponent | lastModifiedDateTime | CDC |
| EmpWorkPermit | attachmentId, userId | lastModifiedDateTime | CDC |
| EmployeePayrollRunResults | externalCode, mdfSystemEffectiveStartDate | lastModifiedDateTime | CDC |
| EmployeePayrollRunResultsItems | EmployeePayrollRunResults_externalCode, EmployeePayrollRunResults_mdfSystemEffectiveStartDate, externalCode | lastModifiedDateTime | CDC |
| OneTimeDeduction | externalCode | lastModifiedDateTime | CDC |
| PaymentInformationDetailV3 | externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate | lastModifiedDateTime | CDC |
| PaymentInformationDetailV3USA | externalCode, PaymentInformationV3_worker, PaymentInformationV3_effectiveStartDate, PaymentInformationDetailV3_externalCode | lastModifiedDateTime | CDC |
| PaymentInformationV3 | worker, effectiveStartDate | lastModifiedDateTime | CDC |
| PaymentMethodAssignmentV3 | externalCode, PaymentMethodV3_externalCode | lastModifiedDateTime | CDC |
| PaymentMethodV3 | externalCode | lastModifiedDateTime | CDC |
| RecurringDeduction | effectiveStartDate, userSysId | lastModifiedDateTime | CDC |
| RecurringDeductionItem | RecurringDeduction_effectiveStartDate, RecurringDeduction_userSysId, payComponentType | lastModifiedDateTime | CDC |

### 2. Foundation Objects (28 tables)

Organizational structure data including business units, companies, departments, locations, job codes, pay grades, and competencies.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| CompetencyEntity | externalCode | lastModifiedDateTime | CDC |
| FOBusinessUnit | externalCode, startDate | lastModifiedDateTime | CDC |
| FOCompany | externalCode, startDate | lastModifiedDateTime | CDC |
| FOCorporateAddressDEFLT | addressId | lastModifiedDateTime | CDC |
| FOCostCenter | externalCode, startDate | lastModifiedDateTime | CDC |
| FODepartment | externalCode, startDate | lastModifiedDateTime | CDC |
| FODivision | externalCode, startDate | lastModifiedDateTime | CDC |
| FODynamicRole | dynamicRoleAssignmentId | lastModifiedDateTime | CDC |
| FOEventReason | externalCode, startDate | lastModifiedDateTime | CDC |
| FOFrequency | externalCode | lastModifiedDateTime | CDC |
| FOGeozone | externalCode, startDate | lastModifiedDateTime | CDC |
| FOJobClassLocalAUS | externalCode, startDate, country | lastModifiedDateTime | CDC |
| FOJobClassLocalBRA | externalCode, startDate, country | lastModifiedDateTime | CDC |
| FOJobClassLocalCAN | externalCode, startDate, country | lastModifiedDateTime | CDC |
| FOJobClassLocalDEFLT | externalCode, startDate, country | lastModifiedDateTime | CDC |
| FOJobClassLocalFRA | externalCode, startDate, country | lastModifiedDateTime | CDC |
| FOJobClassLocalGBR | externalCode, startDate, country | lastModifiedDateTime | CDC |
| FOJobClassLocalITA | externalCode, startDate, country | lastModifiedDateTime | CDC |
| FOJobClassLocalUSA | externalCode, startDate, country | lastModifiedDateTime | CDC |
| FOJobCode | externalCode, startDate | lastModifiedDateTime | CDC |
| FOJobFunction | externalCode, startDate | lastModifiedDateTime | CDC |
| FOLocation | externalCode, startDate | lastModifiedDateTime | CDC |
| FOLocationGroup | externalCode, startDate | lastModifiedDateTime | CDC |
| FOPayComponent | externalCode, startDate | lastModifiedDateTime | CDC |
| FOPayComponentGroup | externalCode, startDate | lastModifiedDateTime | CDC |
| FOPayGrade | externalCode, startDate | lastModifiedDateTime | CDC |
| FOPayGroup | externalCode, startDate | lastModifiedDateTime | CDC |
| FOPayRange | externalCode, startDate | lastModifiedDateTime | CDC |

### 3. Recruiting (34 tables)

Recruitment data including candidates, job applications, job requisitions, interviews, offers, and background checks.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| Candidate | candidateId | lastModifiedDateTime | CDC |
| CandidateComments | commentId | - | Snapshot |
| CandidateLight | candidateId | lastModifiedDateTime | CDC |
| InterviewIndividualAssessment | interviewIndividualAssessmentId | - | Snapshot |
| InterviewOverallAssessment | interviewOverallAssessmentId | - | Snapshot |
| JobApplication | applicationId | lastModifiedDateTime | CDC |
| JobApplicationAssessmentOrder | id | lastModifiedDateTime | CDC |
| JobApplicationAssessmentReport | id | - | Snapshot |
| JobApplicationAssessmentReportDetail | id | - | Snapshot |
| JobApplicationAudit | fieldOrderPos, revNumber | lastModifiedDate | CDC |
| JobApplicationBackgroundCheckRequest | requestId | lastModifiedDateTime | CDC |
| JobApplicationBackgroundCheckResult | statusId | - | Snapshot |
| JobApplicationComments | commentId | - | Snapshot |
| JobApplicationInterview | applicationInterviewId | - | Snapshot |
| JobApplicationOnboardingData | onboardingId | - | Snapshot |
| JobApplicationOnboardingStatus | onboardingStatusId | - | Snapshot |
| JobApplicationQuestionResponse | applicationId, order | - | Snapshot |
| JobApplicationStatus | appStatusSetId | - | Snapshot |
| JobApplicationStatusAuditTrail | revNumber | lastModifiedDateTime | CDC |
| JobApplicationStatusLabel | appStatusId, locale | - | Snapshot |
| JobOffer | offerApprovalId | lastModifiedDate | CDC |
| JobOfferApprover | offerApproverId | lastModifiedDate | CDC |
| JobReqFwdCandidates | candidateId, jobReqId | lastModifiedDate | CDC |
| JobReqQuestion | questionId | - | Snapshot |
| JobReqScreeningQuestion | jobReqId, locale, order | - | Snapshot |
| JobReqScreeningQuestionChoice | locale, optionId, optionValue | - | Snapshot |
| JobRequisition | jobReqId | lastModifiedDateTime | CDC |
| JobRequisitionGroupOperator | jobReqId, operatorRole, userGroupId | - | Snapshot |
| JobRequisitionLocale | jobReqLocalId | - | Snapshot |
| JobRequisitionOperator | jobReqId, operatorRole, userName | - | Snapshot |
| JobRequisitionPosting | jobPostingId | lastModifiedDateTime | CDC |
| OfferLetter | offerLetterId | lastModifiedDate | CDC |
| RCMAdminReassignOfferApprover | applicationId, currUserId, offerDetailId | - | Snapshot |
| RcmCompetency | rcmCompetencyId, locale | - | Snapshot |

### 4. Platform Core (10 tables)

Core platform data including users, permissions, groups, and picklist configurations.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| DGField | name | - | Snapshot |
| DynamicGroup | groupID | lastModifiedDate | CDC |
| LocalizedData | localizedDataCode, localizedDataLocale | - | Snapshot |
| PickListV2 | effectiveStartDate, id | lastModifiedDateTime | CDC |
| PickListValueV2 | PickListV2_effectiveStartDate, PickListV2_id, externalCode | lastModifiedDateTime | CDC |
| PicklistOption | id, externalCode, optionValue, sortOrder, status | - | Snapshot |
| RBPBasicPermission | permissionId | - | Snapshot |
| RBPRole | roleId | lastModifiedDate | CDC |
| RBPRule | ruleId | - | Snapshot |
| User | userId | lastModifiedDateTime | CDC |

### 5. Performance & Goals (13 tables)

Performance management data including goals, forms, calibration, and talent ratings.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| CalibrationSubjectRank | subjectRankId | - | Snapshot |
| TalentGraphicOption | dataIndex, optionKey | - | Snapshot |
| TalentPool | code, effectiveStartDate | lastModifiedDateTime | CDC |
| TalentRatings | feedbackId, feedbackModule, formDataId, formContentId, feedbackType, feedbackSource, feedbackScaleId, employeeId | - | Snapshot |
| form_customized_weighted_rating_section | formContentId, formDataId | - | Snapshot |
| form_folder | folderId | - | Snapshot |
| form_header | formDataId | formLastModifiedDate | CDC |
| form_objective | formContentId, formDataId, itemId, sectionIndex | - | Snapshot |
| form_perf_pot_summary_section | formContentId, formDataId | - | Snapshot |
| form_template | formTemplateId | - | Snapshot |
| goal_1 | id | lastModified | CDC |
| goal_comment_1 | id | lastModified | CDC |
| goal_plan_template | id | - | Snapshot |

### 6. Onboarding & Succession (7 tables)

Onboarding processes and succession planning data.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| NominationTarget | nominationId | - | Snapshot |
| ONB2EquipmentActivity | activityId | lastModifiedDateTime | CDC |
| ONB2EquipmentType | code | lastModifiedDateTime | CDC |
| ONB2EquipmentTypeValue | code | lastModifiedDateTime | CDC |
| ONB2Process | processId | lastModifiedDateTime | CDC |
| ONB2ProcessTask | ONB2Process_processId, taskId | lastModifiedDateTime | CDC |
| ONB2ProcessTrigger | triggerId | lastModifiedDateTime | CDC |

### 7. Compliance & Documents (13 tables)

Compliance management data including compliance forms, document flows, and attachments.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| AssignedComplianceForm | id | lastModifiedDateTime | CDC |
| Attachment | attachmentId | lastModifiedDateTime | CDC |
| ComplianceDocumentFlow | documentFlowCode | lastModifiedDateTime | CDC |
| ComplianceFormData | externalCode | lastModifiedDateTime | CDC |
| ComplianceFormSignature | ComplianceFormData_externalCode, externalCode | lastModifiedDateTime | CDC |
| ComplianceProcess | processId | lastModifiedDateTime | CDC |
| ComplianceProcessTask | taskId | lastModifiedDateTime | CDC |
| Photo | photoId | lastModifiedDateTime | CDC |
| cust_CommutingAllowance | effectiveStartDate, externalCode | lastModifiedDateTime | CDC |
| cust_ProgressiveDisciplinaryAction | effectiveStartDate, externalCode | lastModifiedDateTime | CDC |
| cust_auth_sign | externalCode | lastModifiedDateTime | CDC |
| cust_grievances | effectiveStartDate, externalCode | lastModifiedDateTime | CDC |
| cust_voluntarySeparationRequest | effectiveStartDate, externalCode | lastModifiedDateTime | CDC |

### 8. Platform Extended (7 tables)

Extended platform data including events and configurations.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| EMEvent | id | eventTime | CDC |
| EMEventAttribute | id | - | Snapshot |
| EMEventPayload | id | - | Snapshot |
| custom_nav | title | - | Snapshot |
| theme_config | id | - | Snapshot |
| todo | categoryId | - | Snapshot |
| todo_entry_v2 | todoEntryId | lastModifiedDateTime | CDC |

### 9. Miscellaneous (10 tables)

Additional configuration and reference data.

| Table Name | Primary Keys | Cursor Field | Ingestion Type |
|------------|--------------|--------------|----------------|
| Bank | externalCode | lastModifiedDateTime | CDC |
| BudgetGroup | externalCode, effectiveStartDate | lastModifiedDateTime | CDC |
| Country | code, effectiveStartDate | lastModifiedDateTime | CDC |
| CurrencyConversion | code, effectiveStartDate | lastModifiedDateTime | CDC |
| DevLearningCertifications | certificateId, learningId | certificateLastModifiedDate | CDC |
| DevLearning_4201 | learningId | - | Snapshot |
| ExternalUser | userId | lastModifiedDateTime | CDC |
| HireDateChange | usersSysId | lastModifiedDateTime | CDC |
| LegacyPositionEntity | positionId | - | Snapshot |
| Territory | territoryCode | - | Snapshot |

### Summary by Ingestion Type

| Module | CDC Tables | Snapshot Tables | Total |
|--------|------------|-----------------|-------|
| Employee Central Core | 26 | 1 | 27 |
| Foundation Objects | 28 | 0 | 28 |
| Recruiting | 20 | 14 | 34 |
| Platform Core | 6 | 4 | 10 |
| Performance & Goals | 4 | 9 | 13 |
| Onboarding & Succession | 6 | 1 | 7 |
| Compliance & Documents | 13 | 0 | 13 |
| Platform Extended | 2 | 5 | 7 |
| Miscellaneous | 7 | 3 | 10 |
| **Total** | **112** | **37** | **149** |

### Ingestion Type Definitions

- **CDC (Change Data Capture)**: Supports incremental synchronization using the cursor field to track changes. Only modified records since the last sync are fetched.
- **Snapshot**: Full table refresh on each sync. All records are fetched regardless of modification time.

---

## Data Type Mapping

The following table shows how SAP SuccessFactors OData types are mapped to Spark/Databricks types:

| SAP SuccessFactors Type | Databricks Type | Notes |
|------------------------|-----------------|-------|
| Edm.String | STRING | Text fields, IDs, codes |
| Edm.Int64 | BIGINT | Numeric IDs, large counts |
| Edm.Int32 | BIGINT | Always use BIGINT for safety |
| Edm.Double | DOUBLE | Decimal values, percentages |
| Edm.Boolean | BOOLEAN | True/false flags |
| Edm.DateTime | TIMESTAMP | SAP format: `/Date(milliseconds)/` - automatically converted |
| Edm.Binary | BINARY | Attachment content, binary data |
| Edm.Decimal | DOUBLE | Compensation values, monetary amounts |
| Navigation Property | STRING | JSON-serialized nested objects |

### Date/Time Handling

SAP SuccessFactors uses a specific date format in OData responses: `/Date(1704067200000)/`

This represents milliseconds since Unix epoch (January 1, 1970). The connector automatically converts these to standard Spark TIMESTAMP format.

---

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI flow, which will guide you through setting up a pipeline using the SAP SuccessFactors source connector code. The UI will:

1. Help you create a Unity Catalog connection with your SAP SuccessFactors credentials
2. Copy the connector source code to your workspace
3. Set up the DLT pipeline configuration

### Step 2: Configure Your Pipeline

Update the `pipeline_spec` in your `ingest.py` file based on your use case. See the examples below.

### Step 3: Run and Schedule the Pipeline

1. **Create/Update the DLT Pipeline**: Use Databricks CLI or UI to create a DLT pipeline pointing to your `ingest.py` file
2. **Start the Pipeline**: Run the pipeline in development mode first to validate the configuration
3. **Schedule**: Once validated, set up a schedule based on your data freshness requirements

---

## Pipeline Examples

### Example 1: Employee Central Core HR

Most common use case - core employee data including employment, jobs, compensation, and payment information.

```python
"""
SAP SuccessFactors - Employee Central Core HR
"""
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "sap_successfactors"
connection_name = "sap_successfactors_connection"

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {"table": {"source_table": "User"}},
        {"table": {"source_table": "EmpEmployment"}},
        {"table": {"source_table": "EmpJob"}},
        {"table": {"source_table": "EmpJobRelationships"}},
        {"table": {"source_table": "EmpCompensation"}},
        {"table": {"source_table": "EmpEmploymentTermination"}},
        {"table": {"source_table": "PaymentInformationV3"}},
        {"table": {"source_table": "PaymentMethodV3"}},
    ]
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### Example 2: Foundation/Organization Objects

Organization hierarchy and structure - companies, departments, locations, job codes.

```python
"""
SAP SuccessFactors - Foundation Objects
"""
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "sap_successfactors"
connection_name = "sap_successfactors_connection"

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {"table": {"source_table": "FOCompany"}},
        {"table": {"source_table": "FOBusinessUnit"}},
        {"table": {"source_table": "FODepartment"}},
        {"table": {"source_table": "FODivision"}},
        {"table": {"source_table": "FOCostCenter"}},
        {"table": {"source_table": "FOLocation"}},
        {"table": {"source_table": "FOJobCode"}},
        {"table": {"source_table": "FOJobFunction"}},
        {"table": {"source_table": "FOPayGrade"}},
        {"table": {"source_table": "FOPayGroup"}},
    ]
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### Example 3: Recruiting & Talent Acquisition

Complete recruiting pipeline - requisitions, candidates, applications, offers.

```python
"""
SAP SuccessFactors - Recruiting
"""
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "sap_successfactors"
connection_name = "sap_successfactors_connection"

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {"table": {"source_table": "JobRequisition"}},
        {"table": {"source_table": "JobRequisitionPosting"}},
        {"table": {"source_table": "Candidate"}},
        {"table": {"source_table": "CandidateLight"}},
        {"table": {"source_table": "JobApplication"}},
        {"table": {"source_table": "JobApplicationStatus"}},
        {"table": {"source_table": "JobApplicationAudit"}},
        {"table": {"source_table": "JobOffer"}},
        {"table": {"source_table": "OfferLetter"}},
    ]
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### Example 4: Performance & Goals

Performance management - forms, goals, talent pools.

```python
"""
SAP SuccessFactors - Performance & Goals
"""
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "sap_successfactors"
connection_name = "sap_successfactors_connection"

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {"table": {"source_table": "form_header"}},
        {"table": {"source_table": "form_template"}},
        {"table": {"source_table": "form_objective"}},
        {"table": {"source_table": "goal_1"}},
        {"table": {"source_table": "goal_comment_1"}},
        {"table": {"source_table": "goal_plan_template"}},
        {"table": {"source_table": "TalentPool"}},
    ]
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### Example 5: Payroll Integration

Payroll data for downstream processing.

```python
"""
SAP SuccessFactors - Payroll
"""
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "sap_successfactors"
connection_name = "sap_successfactors_connection"

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {"table": {"source_table": "EmployeePayrollRunResults"}},
        {"table": {"source_table": "EmployeePayrollRunResultsItems"}},
        {"table": {"source_table": "EmpPayCompRecurring"}},
        {"table": {"source_table": "EmpPayCompNonRecurring"}},
        {"table": {"source_table": "RecurringDeduction"}},
        {"table": {"source_table": "RecurringDeductionItem"}},
        {"table": {"source_table": "OneTimeDeduction"}},
        {"table": {"source_table": "Advance"}},
    ]
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

---

## Best Practices

### Initial Setup

1. **Start with Small Table Sets**: Begin with 5-10 tables to validate connectivity and performance before expanding
2. **Validate Credentials First**: Test authentication with a simple table like `User` or `PicklistOption`
3. **Review Available Entities**: Check SAP Admin Center OData Data Dictionary to confirm which entities are available

### Performance Optimization

4. **Use Incremental Sync (CDC)**: For large tables, always use Change Data Capture mode
5. **Monitor API Rate Limits**: SAP SuccessFactors has API rate limits; monitor response headers
6. **Schedule During Off-Peak Hours**: Run large synchronizations during off-peak hours

### Data Organization

7. **Use Module-Based Groupings**: Organize tables by functional module (Employee Central, Recruiting, Performance)
8. **Primary Key Awareness**: Understand primary keys for each table to properly handle updates

---

## Troubleshooting

### Authentication Errors

**Symptom**: 401 Unauthorized or "Authentication failed" errors

**Solutions**:
1. Verify username format: `username@companyId`
2. Check that the password is correct and has not expired
3. Ensure the user has API access permissions enabled

### API Rate Limiting

**Symptom**: 429 Too Many Requests or intermittent connection failures

**Solutions**:
1. Reduce batch size
2. Schedule during off-peak hours
3. Contact SAP support for rate limit increases

### 404 Not Found Errors

**Symptom**: Specific tables return 404 errors while others work fine

**Solutions**:
1. The entity may not be available on your SAP instance
2. Check SAP Admin Center > OData API Data Dictionary
3. See Appendix A for known unavailable entities on demo instances

### 403 Forbidden Errors

**Symptom**: Access denied to specific tables

**Solutions**:
1. Check user permissions in SAP SuccessFactors Role-Based Permissions
2. Some entities may require specific module licenses

---

## Appendix A: Tables with API Limitations

> **IMPORTANT**: This appendix documents 98 tables that return API-level errors on the SAP SuccessFactors demo instance. These tables **may work correctly on production instances** with proper module licensing and configuration.

### 404 Not Found (58 tables)

These entities are not available on demo instances but should work with appropriate SAP license:

| Category | Tables |
|----------|--------|
| EC-Core | EmpBeneficiary, EmpPensionPayout, EmployeeCompensation, EmployeeDismissalProtection, EmployeeDismissalProtectionDetail |
| EC-Extended | EmpCostAssignment |
| General-Objects | budget_period, functional_area, fund, fund_center, grant, pbc_employee_grouping, pbc_symbolic_account, project_controlling_object |
| Onboarding | JourneyDetails, OnboardingEquipment, OnboardingEquipmentActivity, OnboardingEquipmentType, OnboardingEquipmentTypeValue, OnboardingGoal, OnboardingGoalActivity, OnboardingGoalCategory, OnboardingMeetingActivity, OnboardingMeetingEvent, OnboardingNewHireActivitiesStep, OnboardingProcess |
| Performance | calibration_competency_rating, calibration_executive_reviewer, calibration_rating, calibration_rating_option, calibration_subject, cpm_achievements, cpm_activities, cpm_activity_status, cpm_activity_updates, feedback, feedback_requests, review_route_map, supporter_feedback |
| PLT-Core | DynamicGroupDefinition, ScimGroup, ScimUser, cpm_user_permissions, user_capabilities |
| Recruiting | CandidateBackground_Certificates, CandidateBackground_Languages, CandidateBackground_TalentPool, CandidateBackground_TalentPoolcorp, JobApplicationSnapshot_Certificates, JobApplicationSnapshot_Languages, JobApplicationSnapshot_TalentPool, JobApplicationSnapshot_TalentPoolcorp, OnboardingCandidateInfo |
| Specialized | custom_tasks, extension_point_task, i9_audit_trail_record |
| Time-Core | clock_in_clock_out_groups, time_event_types |

### 403 Forbidden (2 tables)

These entities require elevated permissions on SAP SuccessFactors:

| Table Name | Description |
|-----------|-------------|
| Successor | Succession planning data |
| NomineeHistory | Nomination history records |

---

## References

- [SAP SuccessFactors API Documentation](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM)
- [OData v2 API Reference](https://api.sap.com/products/SAPSuccessFactors/apis/all)
- [SAP SuccessFactors Admin Guide](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/caf8c0e655d84cd5ad0e7ebb933c0da6)
