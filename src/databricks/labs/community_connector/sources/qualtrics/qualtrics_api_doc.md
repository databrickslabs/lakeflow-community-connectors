# Qualtrics API Documentation

## Authorization

- **Chosen method**: API Token Authentication for the Qualtrics REST API v3.
- **Base URL**: `https://{datacenterid}.qualtrics.com/API/v3/`
  - The `{datacenterid}` is specific to your Qualtrics account (e.g., `yourdatacenterid`, `ca1`, `eu`, etc.)
  - The datacenter ID can be found in your Qualtrics account settings under "Qualtrics IDs"
- **Auth placement**:
  - HTTP header: `X-API-TOKEN: <api_token>`
  - Each API request must include this header for authentication
- **Token generation**:
  - Log in to Qualtrics account → Account Settings → Qualtrics IDs → Generate Token
  - The token is user-specific and should be kept secure

**Example authenticated request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/surveys"
```

### Rate Limits

| Limit Type | Value |
|-----------|-------|
| Requests per minute per brand | 3000 ([official docs](https://api.qualtrics.com/a5e9a1a304902-limits)) |
| Response on limit exceed | `429 Too Many Requests` |
| Retry-After header | Seconds to wait before retrying |
| API call timeout | 5 seconds |

**Endpoint-specific limits** (lower than brand limit):

| Endpoint | Limit/min |
|----------|-----------|
| POST /surveys/{surveyId}/export-responses | 100 |
| GET /surveys/{surveyId}/export-responses/{progressId} | 1000 |
| GET /surveys/{surveyId}/export-responses/{fileId}/file | 100 |
| GET /distributions/{distributionId}/links | 500 |
| GET /distributions/{distributionId}/history | 300 |

**Other limits**: Max 1.8GB per export file, 100 char mailing list names, 5MB file uploads.

**Best practices**: Implement exponential backoff for 429 responses, respect `Retry-After` header, use pagination.

## Object List

The Qualtrics API provides access to various objects/resources. The object list is **static** (defined by this connector).

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `surveys` | Survey definitions and metadata | `GET /surveys` | `cdc` (upserts based on `lastModified`) |
| `survey_definitions` | Full survey structure with questions, blocks, and flow | `GET /survey-definitions/{surveyId}` | `cdc` (upserts based on `lastModified`) |
| `survey_responses` | Individual responses to surveys | Response Export API (multi-step) | `append` (incremental based on `recordedDate`) |
| `distributions` | Distribution records for survey invitations | `GET /distributions` | `cdc` (upserts based on `modifiedDate`) |
| `directories` | XM Directory folders and structure | `GET /directories` | `snapshot` |
| `mailing_lists` | Contact mailing lists | `GET /directories/{directoryId}/mailinglists` | `snapshot` |
| `mailing_list_contacts` | Contacts within a specific mailing list | `GET /directories/{directoryId}/mailinglists/{mailingListId}/contacts` | `snapshot` |
| `directory_contacts` | All contacts across all mailing lists in a directory | `GET /directories/{directoryId}/contacts` | `snapshot` |
| `users` | Organization users (admins, survey creators) | `GET /users` | `snapshot` |

**Connector scope for initial implementation**:
- Step 1 focuses on the `surveys` and `survey_responses` objects in detail
- Other objects are listed for future extension

**High-level notes on objects**:
- **surveys**: Core survey metadata including name, creation date, modification date, and status
- **survey_definitions**: Complete survey structure including all questions, response choices, blocks, survey flow, and embedded data definitions. Provides the detailed schema needed to interpret survey responses.
- **survey_responses**: Actual response data from survey participants; requires multi-step export process
- **distributions**: Tracks how surveys were distributed (email, SMS, anonymous link, etc.)
- **directories**: Organizational structure for managing contacts and other XM data
- **mailing_lists**: Collections of contacts used for survey distribution
- **mailing_list_contacts**: Individual contact records within a specific mailing list
- **directory_contacts**: All contact records across all mailing lists in a directory
- **users**: Organization users including brand admins and survey creators; useful for joining with survey owner data

## Object Schema

### General notes

- Qualtrics provides JSON responses for all REST API endpoints
- For the connector, we define **tabular schemas** derived from the JSON representation
- Nested JSON objects are modeled as **nested structures** rather than being fully flattened
- Field names and types are derived from official Qualtrics API documentation

### `surveys` object (primary table)

**Source endpoint**:  
`GET /surveys`

**Key behavior**:
- Returns metadata about surveys owned by the authenticated user or organization
- Supports pagination using `skipToken` and `pageSize` parameters
- Can be filtered by `isActive` status

**High-level schema (actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique survey identifier (e.g., `SV_abc123xyz`). Primary key. |
| `name` | string | Survey name/title. |
| `ownerId` | string | Qualtrics user ID of the survey owner. |
| `isActive` | boolean | Whether the survey is currently active. |
| `creationDate` | string (ISO 8601 datetime) | When the survey was created. |
| `lastModified` | string (ISO 8601 datetime) | Last modification timestamp. Used as incremental cursor. |

**Example request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/surveys?pageSize=100"
```

**Example response (actual API response)**:

```json
{
  "result": {
    "elements": [
      {
        "id": "SV_abc123xyz",
        "name": "Customer Satisfaction Survey",
        "ownerId": "UR_123abc",
        "isActive": true,
        "creationDate": "2024-01-15T10:30:00Z",
        "lastModified": "2024-12-20T14:22:33Z"
      }
    ],
    "nextPage": "https://yourdatacenterid.qualtrics.com/API/v3/surveys?pageSize=100&skipToken=xyz789"
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

### `survey_responses` object (response data table)

**Source workflow**:  
Survey responses use a **multi-step export process**:

1. **Create export**: `POST /surveys/{surveyId}/export-responses`
2. **Check progress**: `GET /surveys/{surveyId}/export-responses/{exportProgressId}`
3. **Download file**: `GET /surveys/{surveyId}/export-responses/{fileId}/file`

**Key behavior**:
- Responses cannot be retrieved in a single API call
- Must create an export job, wait for completion, then download the result file
- Supports incremental retrieval using `startDate` and `endDate` filters
- Export format can be JSON, CSV, SPSS, or other formats; connector uses JSON

**High-level schema (actual API response)**:

Core response fields (always present):

| Column Name | Type | Description |
|------------|------|-------------|
| `responseId` | string | Unique response identifier. Primary key. |
| `surveyId` | string | Survey ID this response belongs to. |
| `recordedDate` | string (ISO 8601 datetime) | When the response was recorded. Used as incremental cursor. |
| `startDate` | string (ISO 8601 datetime) | When respondent started the survey. |
| `endDate` | string (ISO 8601 datetime) | When respondent completed the survey. |
| `status` | integer | Response type: 0=Normal, 1=Preview, 2=Test, 4=Imported, 16=Offline, 256=Synthetic. Note: Use `finished` field to check completion status. |
| `ipAddress` | string or null | Respondent's IP address (if collected). |
| `progress` | integer | Percentage of survey completed (0-100). |
| `duration` | integer | Time spent in seconds. |
| `finished` | boolean | Whether the response is finished. |
| `distributionChannel` | string | How the survey was distributed (e.g., `email`, `anonymous`). |
| `userLanguage` | string | Language code used by respondent. |
| `locationLatitude` | string or null | Latitude (if location collected). |
| `locationLongitude` | string or null | Longitude (if location collected). |

Response values (dynamic based on survey questions):

| Column Name | Type | Description |
|------------|------|-------------|
| `values` | map\<string, struct\> | Question responses keyed by Question ID (e.g., `QID1`, `QID2`). Each value contains answer data. |
| `labels` | map\<string, string\> | Human-readable labels for question responses. Note: `useLabels` param not valid with JSON format. |
| `displayedFields` | array\<string\> | List of fields displayed to respondent. |
| `displayedValues` | map\<string, struct\> | Displayed values for each question. |

Embedded data fields (custom fields set during distribution):

| Column Name | Type | Description |
|------------|------|-------------|
| `embeddedData` | map\<string, string\> | Custom embedded data fields (e.g., `userId`, `transactionId`). |

**Question value struct** (elements of `values` map):

| Field | Type | Description |
|-------|------|-------------|
| `choiceText` | string or null | Text of the selected choice. |
| `choiceId` | string or null | ID of the selected choice. |
| `textEntry` | string or null | Free text entry (for text questions). |

**Example export creation request**:

```bash
curl -X POST \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "format": "json",
    "startDate": "2024-12-01T00:00:00Z",
    "endDate": "2024-12-23T23:59:59Z"
  }' \
  "https://yourdatacenterid.qualtrics.com/API/v3/surveys/SV_abc123xyz/export-responses"
```

**Example export creation response**:

```json
{
  "result": {
    "progressId": "ES_abc123xyz",
    "percentComplete": 0.0,
    "status": "inProgress"
  },
  "meta": {
    "requestId": "req123",
    "httpStatus": "200 - OK"
  }
}
```

**Example progress check request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  "https://yourdatacenterid.qualtrics.com/API/v3/surveys/SV_abc123xyz/export-responses/ES_abc123xyz"
```

**Example progress check response (completed)**:

```json
{
  "result": {
    "fileId": "file123abc",
    "percentComplete": 100.0,
    "status": "complete"
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

**Example download request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  "https://yourdatacenterid.qualtrics.com/API/v3/surveys/SV_abc123xyz/export-responses/file123abc/file" \
  --output responses.zip
```

**Example response data (after extracting JSON from zip)**:

```json
{
  "responses": [
    {
      "responseId": "R_abc123xyz",
      "surveyId": null,
      "recordedDate": "2024-12-20T10:30:45Z",
      "startDate": "2024-12-20T10:28:12Z",
      "endDate": "2024-12-20T10:30:45Z",
      "status": 1,
      "ipAddress": "192.168.1.100",
      "progress": 100,
      "duration": 153,
      "finished": true,
      "distributionChannel": "email",
      "userLanguage": "EN",
      "values": {
        "QID1": {
          "choiceText": "Very Satisfied",
          "choiceId": "1"
        },
        "QID2": {
          "textEntry": "Great service!"
        }
      },
      "embeddedData": {
        "userId": "user123",
        "segment": "enterprise"
      }
    }
  ]
}
```

### `distributions` object

**Source endpoint**:  
`GET /distributions?surveyId={surveyId}`

**Key behavior**:
- Returns distribution records for a specific survey
- Distributions represent survey sends via email, SMS, or anonymous links
- Supports pagination

**High-level schema (actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique distribution identifier. Primary key. |
| `parentDistributionId` | string or null | Parent distribution ID (for follow-ups/reminders). |
| `ownerId` | string | User ID who created the distribution. |
| `organizationId` | string | Organization ID. |
| `requestType` | string | Distribution method (e.g., `GeneratedInvite`, `Invite`, `Reminder`). |
| `requestStatus` | string | Status of the distribution (e.g., `Generated`, `pending`, `complete`). |
| `sendDate` | string (ISO 8601 datetime) | When distribution was sent/scheduled. |
| `createdDate` | string (ISO 8601 datetime) | When distribution was created. |
| `modifiedDate` | string (ISO 8601 datetime) | Last modification date. Used as incremental cursor. |
| `headers` | struct | Email headers (fromEmail, fromName, replyToEmail, subject). |
| `recipients` | struct | Recipient information (mailingListId, contactId, libraryId, sampleId). |
| `message` | struct | Message details (libraryId, messageId, messageType). |
| `surveyLink` | struct | Survey link information (surveyId, expirationDate, linkType). |
| `stats` | struct | Distribution statistics (sent, failed, started, bounced, opened, skipped, finished, complaints, blocked). |

### `mailing_lists` object

**Source endpoint**:  
`GET /directories/{directoryId}/mailinglists`

**Key behavior**:
- Returns metadata about mailing lists (contact lists) used for survey distribution within a specific directory
- Mailing lists are collections of contacts organized for managing survey invitations
- Requires `directoryId` as a path parameter (obtained from `directories` table or Qualtrics account settings)
- Supports pagination using `skipToken` and `pageSize` parameters (similar to surveys endpoint)
- List endpoint returns basic mailing list metadata
- Individual mailing list details can be retrieved via `GET /mailinglists/{mailingListId}`
- Only available for XM Directory users (not XM Directory Lite)

**High-level schema (actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `mailingListId` | string | Unique mailing list identifier (e.g., `CG_abc123xyz`). Primary key. |
| `name` | string | Mailing list name/title. |
| `ownerId` | string | Qualtrics user ID of the mailing list owner. |
| `creationDate` | long (epoch milliseconds) | Timestamp when the mailing list was created. |
| `lastModifiedDate` | long (epoch milliseconds) | Last modification timestamp. |
| `contactCount` | long | Number of contacts in the mailing list. |

**Example request (list all mailing lists in a directory)**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/directories/{directoryId}/mailinglists?pageSize=100"
```

**Example request (get specific mailing list)**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/mailinglists/{mailingListId}"
```

**Expected response structure (list endpoint)**:

```json
{
  "result": {
    "elements": [
      {
        "mailingListId": "CG_abc123xyz",
        "name": "Customer Feedback Panel",
        "ownerId": "UR_123abc",
        "creationDate": 1705317000000,
        "lastModifiedDate": 1734704553000,
        "contactCount": 1500
      }
    ],
    "nextPage": "https://yourdatacenterid.qualtrics.com/API/v3/directories/POOL_abc123/mailinglists?pageSize=100&skipToken=xyz789"
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

### `mailing_list_contacts` object

**Source endpoint**:
`GET /directories/{directoryId}/mailinglists/{mailingListId}/contacts`

**Key behavior**:
- Returns contacts within a specific mailing list in a directory
- Requires both `directoryId` and `mailingListId` as table-level parameters
- Contacts can have custom embedded data fields
- Supports pagination
- Only available for XM Directory users (not XM Directory Lite)

**High-level schema (actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `contactId` | string | Unique contact identifier. Primary key. |
| `firstName` | string or null | Contact's first name. |
| `lastName` | string or null | Contact's last name. |
| `email` | string or null | Contact's email address. |
| `phone` | string or null | Contact's phone number. |
| `extRef` | string or null | External reference ID. |
| `language` | string or null | Preferred language code. |
| `unsubscribed` | boolean | Whether contact is unsubscribed globally. |
| `mailingListUnsubscribed` | boolean | Whether contact is unsubscribed from this specific mailing list. |
| `contactLookupId` | string or null | Contact lookup identifier for cross-referencing. |

### `directory_contacts` object

**Source endpoint**:
`GET /directories/{directoryId}/contacts`

**Key behavior**:
- Returns all contacts across all mailing lists in a directory
- Requires only `directoryId` as table-level parameter
- Schema differs from mailing_list_contacts (has `embeddedData`, no `mailingListUnsubscribed` or `contactLookupId`)
- Supports pagination
- Only available for XM Directory users (not XM Directory Lite)
- Use this endpoint when you want all contacts from a directory without filtering by mailing list

**High-level schema (actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `contactId` | string | Unique contact identifier. Primary key. |
| `firstName` | string or null | Contact's first name. |
| `lastName` | string or null | Contact's last name. |
| `email` | string or null | Contact's email address. |
| `phone` | string or null | Contact's phone number. |
| `extRef` | string or null | External reference ID. |
| `language` | string or null | Preferred language code. |
| `unsubscribed` | boolean | Whether contact is unsubscribed globally. |
| `embeddedData` | object | Custom embedded data fields for the contact (key-value pairs). |

### `directories` object

**Source endpoint**:  
`GET /directories`

**Key behavior**:
- Returns a list of XM Directory instances (also known as "Pools") accessible to the authenticated user
- Directories are organizational containers for contacts, mailing lists, and other XM data
- The `directoryId` (also called `poolId`) is required for accessing contacts and mailing lists within a directory
- Supports pagination using `skipToken` and `pageSize` parameters
- Read-only endpoint for listing directories

**High-level schema (actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `directoryId` | string | Unique directory identifier (e.g., `POOL_abc123xyz`). Also known as poolId. Primary key. |
| `name` | string or null | Display name of the directory (may be null for default directories). |
| `contactCount` | long | Number of contacts in the directory. |
| `isDefault` | boolean | Whether this is the default directory for the account. |
| `deduplicationCriteria` | struct | Criteria used for contact deduplication (see below). |

**Deduplication Criteria structure**:

| Field | Type | Description |
|-------|------|-------------|
| `email` | boolean | Whether to deduplicate by email address. |
| `firstName` | boolean | Whether to deduplicate by first name. |
| `lastName` | boolean | Whether to deduplicate by last name. |
| `externalDataReference` | boolean | Whether to deduplicate by external reference ID. |
| `phone` | boolean | Whether to deduplicate by phone number. |

### `users` object

**Source endpoint**:  
`GET /users`

**Key behavior**:
- Returns a list of users in the organization accessible to the authenticated user
- Includes brand administrators, survey creators, and other user types
- Supports pagination using `skipToken` and `pageSize` parameters
- Read-only endpoint for listing users

**High-level schema (actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique user identifier (e.g., `UR_abc123xyz`). Primary key. |
| `username` | string | Username (typically email address). |
| `email` | string | User's email address. |
| `firstName` | string | User's first name. |
| `lastName` | string | User's last name. |
| `userType` | string | User type code (e.g., `UT_BRANDADMIN` for brand administrators). |
| `divisionId` | string or null | Division identifier if user belongs to a specific division. |
| `accountStatus` | string | Account status (e.g., `active`, `disabled`). |

**User Type values**:

| User Type Code | Description |
|----------------|-------------|
| `UT_BRANDADMIN` | Brand Administrator - full administrative access |
| `UT_PARTICIPANT` | Participant - limited survey-taking access |
| `UT_FULLUSER` | Full User - standard user with survey creation privileges |

**Example request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/users?pageSize=100"
```

**Example response**:

```json
{
  "result": {
    "elements": [
      {
        "id": "UR_abc123xyz",
        "username": "admin@company.com",
        "email": "admin@company.com",
        "firstName": "John",
        "lastName": "Admin",
        "userType": "UT_BRANDADMIN",
        "divisionId": null,
        "accountStatus": "active"
      }
    ],
    "nextPage": "https://...?skipToken=xyz"
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

### `survey_definitions` object

**Source endpoint**:
`GET /survey-definitions/{surveyId}`

**Key behavior**:
- Returns the complete survey structure including questions, blocks, flow, and embedded data definitions
- Requires `surveyId` as a path parameter (obtained from the `surveys` table)
- One API call per survey - no pagination (returns complete definition in single response)
- Provides detailed question structure needed to interpret survey response data
- Useful for building data dictionaries and understanding response field mappings

**High-level schema (actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `SurveyID` | string | Unique survey identifier. Primary key. |
| `SurveyName` | string | Name/title of the survey. |
| `SurveyStatus` | string | Survey status (e.g., "Inactive", "Active"). |
| `OwnerID` | string | User ID of the survey owner. |
| `CreatorID` | string | User ID who created the survey. |
| `BrandID` | string | Brand identifier. |
| `BrandBaseURL` | string | Brand base URL (e.g., `https://yourbrand.qualtrics.com`). |
| `LastModified` | string | Last modification timestamp (ISO 8601). Used as incremental cursor. |
| `LastAccessed` | string or null | Last access timestamp (ISO 8601). |
| `LastActivated` | string or null | Last activation timestamp (ISO 8601). |
| `QuestionCount` | string | Number of questions in the survey. |

**Nested objects** (stored as JSON strings in connector):

| Column Name | Type | Description |
|------------|------|-------------|
| `Questions` | object (JSON) | Map of question IDs (e.g., `QID1`, `QID2`) to question definitions. |
| `Blocks` | object (JSON) | Map of block IDs to block definitions containing question order. |
| `SurveyFlow` | object (JSON) | Object containing `Flow` array defining survey navigation logic. |
| `SurveyOptions` | object (JSON) | Survey-level options and settings. |
| `ResponseSets` | object (JSON) | Map of response set IDs to response set definitions. |
| `Scoring` | object (JSON) | Scoring definitions if configured. |
| `ProjectInfo` | object (JSON) | Project metadata including ProjectCategory, ProjectType, etc. |

**Question struct** (elements of `Questions` map):

| Field | Type | Description |
|-------|------|-------------|
| `QuestionID` | string | Unique question identifier (e.g., `QID1`). |
| `QuestionText` | string | The question text/prompt displayed to respondents. |
| `QuestionDescription` | string | Internal description of the question. |
| `DataExportTag` | string | Tag used in data exports (often same as QuestionID). |
| `QuestionType` | string | Type of question (e.g., `MC`, `TE`, `Matrix`, `Slider`). |
| `Selector` | string | Question selector/subtype (e.g., `SAVR`, `SACOL`, `SL`). |
| `SubSelector` | string or null | Sub-selector for additional variations. |
| `Configuration` | struct | Question-specific configuration options. |
| `QuestionText_Unsafe` | string or null | Raw HTML version of question text. |
| `Validation` | struct | Validation settings (force response, etc.). |
| `Language` | array\<struct\> or null | Multi-language translations. |
| `NextChoiceId` | integer | Next available choice ID for new choices. |
| `NextAnswerId` | integer | Next available answer ID. |
| `Choices` | map\<string, struct\> or null | Map of choice IDs to choice definitions (for MC questions). |
| `ChoiceOrder` | array\<string\> or null | Order of choice IDs for display. |
| `Answers` | map\<string, struct\> or null | Map of answer IDs to answer definitions (for Matrix questions). |
| `AnswerOrder` | array\<string\> or null | Order of answer IDs for display. |
| `RecodeValues` | map\<string, string\> or null | Recode mappings for choice values. |
| `VariableNaming` | map\<string, string\> or null | Variable naming conventions for export. |
| `DisplayLogic` | struct or null | Display logic conditions. |
| `SkipLogic` | struct or null | Skip logic conditions. |
| `GradingData` | array\<struct\> or null | Grading configuration for scored questions. |
| `Randomization` | struct or null | Randomization settings for choices. |

**Common QuestionType values**:

| QuestionType | Description |
|-------------|-------------|
| `MC` | Multiple Choice |
| `TE` | Text Entry |
| `Matrix` | Matrix/Likert Scale |
| `Slider` | Slider |
| `RO` | Rank Order |
| `CS` | Constant Sum |
| `DD` | Drill Down |
| `PGR` | Pick, Group, and Rank |
| `SBS` | Side by Side |
| `HotSpot` | Hot Spot |
| `HeatMap` | Heat Map |
| `GAP` | Graphic Association (Gap Analysis) |
| `DB` | Descriptive Text / Graphic |
| `Timing` | Timing Question |
| `Meta` | Meta Info Question |
| `Captcha` | Captcha Verification |

**Block struct** (elements of `Blocks` map):

| Field | Type | Description |
|-------|------|-------------|
| `Type` | string | Block type (e.g., `Default`, `Standard`, `Trash`). |
| `Description` | string | Block description/name. |
| `ID` | string | Block identifier. |
| `BlockElements` | array\<struct\> | Array of elements in the block (questions, page breaks). |
| `Options` | struct | Block options (randomization, looping). |

**Flow element struct** (elements of `SurveyFlow.Flow` array):

| Field | Type | Description |
|-------|------|-------------|
| `Type` | string | Flow element type (e.g., `Block`, `EmbeddedData`, `Branch`, `EndSurvey`). |
| `ID` | string | Block ID or element identifier. |
| `FlowID` | string | Unique flow element ID. |
| `Autofill` | array\<struct\> or null | Autofill configurations. |

**Example request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/survey-definitions/SV_abc123xyz"
```

**Example response** (abbreviated):

```json
{
  "result": {
    "SurveyID": "SV_abc123xyz",
    "SurveyName": "Customer Satisfaction Survey",
    "SurveyStatus": "Active",
    "OwnerID": "UR_123abc",
    "CreatorID": "UR_123abc",
    "BrandID": "yourbrand",
    "BrandBaseURL": "https://yourbrand.qualtrics.com",
    "LastModified": "2024-12-20T14:22:33Z",
    "LastAccessed": "2024-12-20T14:22:33Z",
    "LastActivated": "2024-01-15T10:30:00Z",
    "QuestionCount": "5",
    "ProjectInfo": {
      "SurveyID": "SV_abc123xyz",
      "ProjectCategory": "CORE",
      "ProjectType": "NonDistributedSurvey"
    },
    "SurveyFlow": {
      "Flow": [
        {"Type": "Block", "ID": "BL_abc123", "FlowID": "FL_1"}
      ],
      "FlowID": "FL_1",
      "Type": "Root"
    },
    "Questions": {
      "QID1": {
        "QuestionID": "QID1",
        "QuestionText": "How satisfied are you with our service?",
        "QuestionDescription": "Satisfaction Rating",
        "DataExportTag": "Q1",
        "QuestionType": "MC",
        "Selector": "SAVR",
        "SubSelector": "TX",
        "Choices": {
          "1": { "Display": "Very Satisfied", "RecodeValue": "5" },
          "2": { "Display": "Satisfied", "RecodeValue": "4" },
          "3": { "Display": "Neutral", "RecodeValue": "3" },
          "4": { "Display": "Dissatisfied", "RecodeValue": "2" },
          "5": { "Display": "Very Dissatisfied", "RecodeValue": "1" }
        },
        "ChoiceOrder": ["1", "2", "3", "4", "5"],
        "Validation": {
          "Settings": {
            "ForceResponse": "OFF",
            "Type": "None"
          }
        }
      },
      "QID2": {
        "QuestionID": "QID2",
        "QuestionText": "Please share any additional feedback:",
        "QuestionDescription": "Open Feedback",
        "DataExportTag": "Q2",
        "QuestionType": "TE",
        "Selector": "SL",
        "Validation": {
          "Settings": {
            "ForceResponse": "OFF",
            "Type": "None"
          }
        }
      }
    },
    "Blocks": {
      "BL_abc123": {
        "Type": "Default",
        "Description": "Default Question Block",
        "ID": "BL_abc123",
        "BlockElements": [
          { "Type": "Question", "QuestionID": "QID1" },
          { "Type": "Question", "QuestionID": "QID2" }
        ]
      }
    },
    "Flow": [
      {
        "Type": "Block",
        "ID": "BL_abc123",
        "FlowID": "FL_1"
      },
      {
        "Type": "EndSurvey",
        "FlowID": "FL_2"
      }
    ],
    "EmbeddedData": [
      {
        "Description": "userId",
        "Type": "Custom",
        "Field": "userId",
        "VariableType": "String"
      }
    ],
    "SurveyOptions": {
      "BackButton": "false",
      "SaveAndContinue": "true",
      "SurveyProtection": "PublicSurvey",
      "BallotBoxStuffingPrevention": "false",
      "NoIndex": "Yes",
      "SecureResponseFiles": "true",
      "SurveyExpiration": "None",
      "SurveyTermination": "DefaultMessage",
      "Header": "",
      "Footer": "",
      "ProgressBarDisplay": "None",
      "PartialData": "+1 week",
      "ValidationMessage": "",
      "PreviousButton": "",
      "NextButton": "",
      "SurveyTitle": "Customer Satisfaction Survey"
    }
  },
  "meta": {
    "httpStatus": "200 - OK",
    "requestId": "abc123-def456"
  }
}
```

## Get Object Primary Keys

Primary keys for each object are static and defined by the connector:

| Object Name | Primary Key Column(s) |
|------------|----------------------|
| `surveys` | `id` |
| `survey_definitions` | `SurveyID` |
| `survey_responses` | `responseId` |
| `distributions` | `id` |
| `directories` | `directoryId` |
| `mailing_lists` | `mailingListId` |
| `mailing_list_contacts` | `contactId` |
| `directory_contacts` | `contactId` |

## Object Ingestion Types

| Object Name | Ingestion Type | Cursor Field | Notes |
|------------|---------------|--------------|-------|
| `surveys` | `cdc` | `lastModified` | Supports incremental updates based on modification timestamp. |
| `survey_definitions` | `cdc` | `LastModified` | Supports incremental updates; only fetches definitions modified since last sync. |
| `survey_responses` | `append` | `recordedDate` | New responses are appended; edits made in UI are not detected (API does not expose modification timestamp). |
| `distributions` | `cdc` | `modifiedDate` | Distribution records can be updated (e.g., status changes). |
| `directories` | `snapshot` | N/A | Full refresh; organizational structure. API does not return modification timestamps. |
| `mailing_lists` | `snapshot` | N/A | Full refresh; relatively small dataset. Requires directoryId. |
| `mailing_list_contacts` | `snapshot` | N/A | Full refresh; API does not return lastModifiedDate field. Requires directoryId and mailingListId. |
| `directory_contacts` | `snapshot` | N/A | Full refresh; API does not return lastModifiedDate field. Requires only directoryId. |

**Incremental sync strategy**:
- For `cdc` objects: Store the maximum cursor value from previous sync; on next sync, filter by `cursor_field >= last_max_value`
- For `append` objects: Similar to CDC, but records are never updated once inserted
- For `snapshot` objects: Full table refresh on each sync

**Delete handling**:
- Qualtrics API does not provide explicit delete flags in responses
- Deleted surveys: No longer appear in `/surveys` endpoint
- Deleted responses: Remain in export but may have status changes
- Recommendation: Use full refresh periodically to detect deletions

## Read API for Data Retrieval

### `surveys` endpoint

**Endpoint**: `GET /surveys`

**Method**: GET

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pageSize` | integer | No | Number of results per page (default: 100, max: 100). |
| `skipToken` | string | No | Token for pagination (obtained from `nextPage` in response). |

**Response structure**:

```json
{
  "result": {
    "elements": [...],
    "nextPage": "https://...?skipToken=xyz"
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

**Pagination**:
- Uses cursor-based pagination with `skipToken`
- The `nextPage` URL in the response contains the full URL for the next page
- When `nextPage` is absent, you've reached the last page

**Incremental retrieval**:
- Filter surveys using `lastModified >= last_sync_time` (client-side filtering after retrieval)
- Note: The API does not support server-side filtering by date on `/surveys`

### `survey_definitions` endpoint

**Endpoint**: `GET /survey-definitions/{surveyId}`

**Method**: GET

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `surveyId` | string | Yes | The unique survey identifier (e.g., `SV_abc123xyz`). Obtain from `surveys` table. |

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `format` | string | No | Response format. Options: `qsf` (Qualtrics Survey Format, default is JSON). |

**Response structure**:

```json
{
  "result": {
    "SurveyID": "SV_abc123xyz",
    "SurveyName": "...",
    "Questions": { "QID1": {...}, "QID2": {...} },
    "Blocks": { "BL_abc": {...} },
    "Flow": [...],
    "EmbeddedData": [...],
    "SurveyOptions": {...},
    ...
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

**Pagination**:
- None - Returns complete survey definition in a single response
- Each survey must be fetched individually by its `surveyId`

**Incremental retrieval**:
- Uses CDC mode with `last_modified` as the cursor field
- Only fetches survey definitions that have been modified since the last sync
- Supports SCD Type 2 for tracking survey structure changes over time

**Usage pattern for connector**:
1. First sync the `surveys` table to get list of all survey IDs
2. For each survey ID, call `GET /survey-definitions/{surveyId}`
3. Store the complete definition as a single record per survey
4. Use the definitions to build data dictionaries for interpreting `survey_responses`

### `survey_responses` export workflow

**Step 1: Create export**

**Endpoint**: `POST /surveys/{surveyId}/export-responses`

**Method**: POST

**Request body parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `format` | string | Yes | Export format: `json`, `csv`, `spss`, `ndjson`. Connector uses `json`. |
| `useLabels` | boolean | No | Use human-readable labels instead of IDs (default: false). **Not valid with JSON/NDJSON format.** |
| `startDate` | string (ISO 8601) | No | Filter responses recorded on or after this date. Used for incremental sync. |
| `endDate` | string (ISO 8601) | No | Filter responses recorded on or before this date. |
| `limit` | integer | No | Maximum number of responses to export. |
| `includedQuestionIds` | array\<string\> | No | Specific question IDs to include. |
| `compress` | boolean | No | Whether to compress the export file (default: true). |

**Response**: Returns `progressId` to track export progress.

**Step 2: Check export progress**

**Endpoint**: `GET /surveys/{surveyId}/export-responses/{progressId}`

**Method**: GET

**Response fields**:

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Export status: `inProgress`, `complete`, `failed`. |
| `percentComplete` | number | Progress percentage (0-100). |
| `fileId` | string | File ID (only present when `status=complete`). |

**Polling strategy**:
- Poll every 1-2 seconds for small exports
- Poll every 5-10 seconds for large exports (>10,000 responses)
- Maximum wait time: 5-10 minutes

**Step 3: Download export file**

**Endpoint**: `GET /surveys/{surveyId}/export-responses/{fileId}/file`

**Method**: GET

**Response**: Binary ZIP file containing JSON data

**Processing**:
1. Download ZIP file
2. Extract JSON file from ZIP
3. Parse JSON to extract `responses` array
4. Each element in `responses` is a response record

### `distributions` endpoint

**Endpoint**: `GET /distributions?surveyId={surveyId}`

**Method**: GET

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `surveyId` | string | Yes | Survey ID to filter distributions. |
| `pageSize` | integer | No | Results per page (default: 100). |
| `skipToken` | string | No | Pagination token. |

**Incremental retrieval**:
- Filter by `modifiedDate >= last_sync_time` (client-side after retrieval)

### `directories` endpoint

**Endpoint**: `GET /directories`

**Method**: GET

**Path Parameters**: None

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pageSize` | integer | No | Number of results per page (default: 100, max: 100). |
| `skipToken` | string | No | Token for pagination (obtained from `nextPage` in response). |

**Response structure**:

```json
{
  "result": {
    "elements": [
      {
        "directoryId": "POOL_2s74jRYyTkLxXl5",
        "name": "My XM Directory",
        "contactCount": 150,
        "isDefault": true,
        "deduplicationCriteria": {
          "email": true,
          "firstName": false,
          "lastName": false,
          "externalDataReference": false,
          "phone": false
        }
      }
    ],
    "nextPage": "https://...?skipToken=xyz"
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

**Pagination**:
- Uses cursor-based pagination with `skipToken`
- The `nextPage` URL in the response contains the full URL for the next page
- When `nextPage` is absent, you've reached the last page
- Same pagination pattern as surveys endpoint

**Example request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/directories?pageSize=100"
```

### `users` endpoint

**Endpoint**: `GET /users`

**Method**: GET

**Path Parameters**: None

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pageSize` | integer | No | Number of results per page (default: 100). |
| `skipToken` | string | No | Token for pagination (obtained from `nextPage` in response). |

**Response structure**:

```json
{
  "result": {
    "elements": [
      {
        "id": "UR_5178684143e64cc",
        "divisionId": null,
        "username": "admin@company.com",
        "firstName": "John",
        "lastName": "Admin",
        "userType": "UT_BRANDADMIN",
        "email": "admin@company.com",
        "accountStatus": "active"
      }
    ],
    "nextPage": "https://...?skipToken=xyz"
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

**Pagination**:
- Uses cursor-based pagination with `skipToken`
- The `nextPage` URL in the response contains the full URL for the next page
- When `nextPage` is absent, you've reached the last page
- Same pagination pattern as surveys endpoint

**Example request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/users?pageSize=100"
```

### `mailing_lists` endpoint

**Endpoint**: `GET /directories/{directoryId}/mailinglists`

**Method**: GET

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `directoryId` | string | Yes | Directory ID (from table configuration or directories table). |

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pageSize` | integer | No | Number of results per page (default: 100, max: 100). |
| `skipToken` | string | No | Token for pagination (obtained from `nextPage` in response). |

**Response structure**:

```json
{
  "result": {
    "elements": [
      {
        "mailingListId": "CG_abc123xyz",
        "name": "Customer Feedback Panel",
        "ownerId": "UR_123abc",
        "creationDate": 1705317000000,
        "lastModifiedDate": 1734704553000,
        "contactCount": 1500
      }
    ],
    "nextPage": "https://yourdatacenterid.qualtrics.com/API/v3/directories/POOL_abc123/mailinglists?pageSize=100&skipToken=xyz789"
  },
  "meta": {
    "httpStatus": "200 - OK"
  }
}
```

**Pagination**:
- Uses cursor-based pagination with `skipToken` (similar to surveys endpoint)
- The `nextPage` URL in the response contains the full URL for the next page
- When `nextPage` is absent, you've reached the last page

**Get individual mailing list details**:

**Endpoint**: `GET /mailinglists/{mailingListId}`

**Method**: GET

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `mailingListId` | string | Yes | The unique mailing list identifier (e.g., `CG_abc123xyz`). |

### `mailing_list_contacts` endpoint

**Endpoint**: `GET /directories/{directoryId}/mailinglists/{mailingListId}/contacts`

**Method**: GET

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `directoryId` | string | Yes | Directory ID (from table configuration). |
| `mailingListId` | string | Yes | Mailing List ID (from table configuration). |

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pageSize` | integer | No | Results per page (default: 100, max: 500). |
| `skipToken` | string | No | Pagination token. |

### `directory_contacts` endpoint

**Endpoint**: `GET /directories/{directoryId}/contacts`

**Method**: GET

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `directoryId` | string | Yes | Directory ID (from table configuration). |

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pageSize` | integer | No | Results per page (default: 100, max: 500). |
| `skipToken` | string | No | Pagination token. |

## Field Type Mapping

Mapping from Qualtrics API JSON types to Spark SQL types:

| Qualtrics API Type | Spark SQL Type | Notes |
|-------------------|----------------|-------|
| `string` | `StringType` | All string fields including IDs |
| `integer` | `LongType` | Use Long for safety (e.g., `progress`, `duration`) |
| `number` (float) | `DoubleType` | Decimal values (e.g., `percentComplete`) |
| `boolean` | `BooleanType` | True/false fields |
| ISO 8601 datetime string | `StringType` | Store as string; can be cast to timestamp in downstream processing |
| `object` (nested) | `StructType` | Nested structures (e.g., `expiration`, `headers`) |
| `array` | `ArrayType` | Arrays of values |
| `null` | Nullable field | All fields are nullable unless documented otherwise |
| `map` (dynamic keys) | `MapType` | For `embeddedData`, `values`, `labels` with dynamic keys |

## Write API

This connector is **read-only**. Write-back APIs below are documented for testing purposes only.

---

## Write-Back APIs (For Testing Only)

These write endpoints enable automated testing by creating test survey responses for validation.

### Create Survey Response (via Sessions API)

The Qualtrics Sessions API allows programmatic creation of survey responses for testing purposes.

- **Method**: POST
- **Endpoint v1**: `https://{datacenterid}.qualtrics.com/API/v3/surveys/{surveyId}/sessions`
- **Endpoint v2**: `https://{datacenterid}.qualtrics.com/API/v3/surveys/{surveyId}/sessions-v2` (more question types)
- **Authentication**: Same as read operations (X-API-TOKEN header)
- **Purpose**: Creates a new response session that can be populated with answer data
- **Timeout**: 10 seconds (instead of default 5 seconds)

**Supported Question Types**:

| API Version | Supported Question Types |
|-------------|-------------------------|
| v1 (`/sessions`) | Multiple Choice, Text Entry, NPS only |
| v2 (`/sessions-v2`) | TE, MC, NPS, Matrix, Form, Slider, Rank Order, Descriptive Block |

**Workflow**:

1. **Create Session**: POST to `/surveys/{surveyId}/sessions`
2. **Update Session Data**: POST answer data to the session
3. **Close Session**: Mark session as complete

**Example: Create Session**

```bash
curl -X POST \
  -H "X-API-TOKEN: YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  "https://fra1.qualtrics.com/API/v3/surveys/SV_abc123xyz/sessions" \
  -d '{
    "language": "EN",
    "embeddedData": {
      "test_id": "test_123",
      "source": "automated_test"
    }
  }'
```

**Response**:
```json
{
  "result": {
    "sessionId": "SESSION_abc123",
    "surveySessionId": "SS_xyz789"
  },
  "meta": {
    "requestId": "req_123",
    "httpStatus": "200 - OK"
  }
}
```

**Example: Submit Response Data**

```bash
curl -X POST \
  -H "X-API-TOKEN: YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  "https://fra1.qualtrics.com/API/v3/surveys/SV_abc123xyz/sessions/SESSION_abc123" \
  -d '{
    "responses": {
      "QID1": {
        "choiceId": "1"
      },
      "QID2": {
        "text": "Test response text"
      }
    },
    "finished": true
  }'
```

**Required Fields for Testing**:
- `language`: Language code (e.g., "EN")
- `embeddedData` (optional but recommended): Custom fields for identifying test data
- `responses`: Map of question IDs to answer values
- `finished`: Boolean indicating if response is complete

#### Alternative: Distribution-Based Response Creation

For more realistic testing, you can create distributions and collect responses through actual survey links.

- **Method**: POST
- **Endpoint**: `https://{datacenterid}.qualtrics.com/API/v3/distributions`
- **Purpose**: Creates a distribution that generates unique survey links

**Example**:
```bash
curl -X POST \
  -H "X-API-TOKEN: YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  "https://fra1.qualtrics.com/API/v3/distributions" \
  -d '{
    "surveyId": "SV_abc123xyz",
    "linkType": "Individual",
    "description": "Test distribution",
    "action": "CreateDistribution",
    "mailingListId": "ML_test123"
  }'
```

### Field Name Transformations

Field names are generally consistent between write and read operations for survey responses. However, note the following:

| Write Field Name | Read Field Name | Notes |
|------------------|-----------------|-------|
| `language` | `userLanguage` | Language code field name differs |
| `finished` | `finished` | Consistent - boolean completion status |
| `responses` | `values` | Write uses "responses", read returns as "values" |
| `embeddedData` | `embeddedData` | Consistent - custom field container |

**Key Transformation Notes**:
- Question IDs (e.g., `QID1`, `QID2`) remain consistent between write and read
- Answer structure differs: write may use simpler format (`{"choiceId": "1"}`), read returns full structure (`{"choiceText": "...", "choiceId": "1", "textEntry": null}`)
- Timestamps are auto-generated on write (recordedDate, startDate, endDate)

### Write Constraints

- **Eventual Consistency**: Responses take 5-30 seconds to appear in export API. Wait 30-60 seconds before validation reads.
- **Response Editing**: Once `finished: true`, responses cannot be edited via API.
- **Test Environment**: Use dedicated test surveys, tag with `test_id` embedded data.

---

## Research Log

| Source Type | URL | Accessed | Confidence | What it confirmed |
|------------|-----|----------|-----------|-------------------|
| Official Docs | https://api.qualtrics.com/a5e9a1a304902-limits | 2026-01-08 | High | Rate limits, endpoint-specific limits, timeout, file limits |
| Official Docs | https://www.qualtrics.com/support/survey-platform/data-and-analysis-module/data/download-data/understanding-your-dataset/ | 2026-01-08 | High | Response status codes, useLabels invalid with JSON |
| Reference Impl | https://fivetran.com/docs/connectors/applications/qualtrics | 2026-01-08 | High | Delete handling: No delete flags; use full re-sync |
| Official Docs | https://api.qualtrics.com/ | 2026-01-08 | High | Sessions API v1/v2, question type restrictions |
| Official Docs | https://www.qualtrics.com/support/integrations/api-integration/overview/ | 2024-12-23 | High | Authentication, header format, permissions |
| Official Docs | https://api.qualtrics.com/ZG9jOjg0MDczOA-api-reference | 2026-01-05 | High | Survey-definitions endpoint, Questions/Blocks/Flow schema |
| Official Docs | https://api.qualtrics.com/dd83f1535056c-list-mailing-lists | 2026-01-06 | High | Mailing lists endpoint, pagination |
| API Reference | Stoplight API docs | 2024-12-31 | High | Full endpoint paths for contacts |
| Python Lib | qualtricsapi-pydocs.com | 2024-12-31 | High | Distributions, contacts endpoint structure |
| Community | Qualtrics Community | 2024-12-31 | Medium | DirectoryId requirements |
| Live API Test | Direct API call | 2026-01-08 | High | Users endpoint schema (8 fields verified) |

**Research approach**: Primary source is official Qualtrics API documentation. Secondary sources include developer community posts and reference implementations. All schemas validated against live API.

## Sources and References

- **Official API**: https://api.qualtrics.com/
- **Getting Started**: https://www.qualtrics.com/support/integrations/api-integration/overview/
- **Developer Portal**: https://www.qualtrics.com/support/integrations/developer-portal/

All schemas validated against live API (2024-12-31 through 2026-01-08).

---

## Schema Validation Against Live API

**Validation Date**: January 8, 2026

| Table | Status | Action Taken |
|-------|--------|--------------|
| `surveys` | ✅ Fixed | Removed 4 fields not in API |
| `survey_definitions` | ✅ Perfect Match | No changes |
| `survey_responses` | ✅ Correct | MapType fields correctly designed |
| `distributions` | ✅ Fixed | Schema updated with nested structs |
| `mailing_lists` | ✅ Validated | Removed libraryId, category, folder |
| `mailing_list_contacts` | ✅ Perfect Match | No changes |
| `directory_contacts` | ✅ Fixed | Added embeddedData, removed 2 fields |
| `directories` | ✅ Perfect Match | No changes |
| `users` | ✅ Validated | Schema matches live API (8 fields) |

**Validation script**: `sources/qualtrics/test/validate_qualtrics_schemas.py`

---

