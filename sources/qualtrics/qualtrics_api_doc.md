# **Qualtrics API Documentation**

## **Authorization**

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

**Note on Directory ID**: The `{directoryid}` (also called Pool ID) is required for contacts endpoints. It is provided as a table-level parameter, not a connection-level parameter, for maximum flexibility.

**Example authenticated request**:

```bash
curl -X GET \
  -H "X-API-TOKEN: <YOUR_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourdatacenterid.qualtrics.com/API/v3/surveys"
```

**Notes**:
- Rate limiting: **500 requests per minute** for most endpoints
- **20 concurrent requests** maximum per API token
- Exceeding rate limits returns `429 Too Many Requests` with `Retry-After` header

## **Object List**

The Qualtrics API provides access to various objects/resources. The object list is **static** (defined by this connector).

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `surveys` | Survey definitions and metadata | `GET /surveys` | `cdc` (upserts based on `lastModified`) |
| `survey_responses` | Individual responses to surveys | Response Export API (multi-step) | `append` (incremental based on `recordedDate`) |
| `distributions` | Distribution records for survey invitations | `GET /distributions` | `cdc` (upserts based on `modifiedDate`) |
| `mailing_lists` | Contact mailing lists | `GET /mailinglists` | `snapshot` |
| `contacts` | Contacts within mailing lists | `GET /directories/{directoryId}/mailinglists/{mailingListId}/contacts` | `snapshot` (full refresh, no lastModifiedDate available) |
| `directories` | XM Directory folders and structure | `GET /directories` | `snapshot` |

**Connector scope for initial implementation**:
- Step 1 focuses on the `surveys` and `survey_responses` objects in detail
- Other objects are listed for future extension

**High-level notes on objects**:
- **surveys**: Core survey metadata including name, creation date, modification date, and status
- **survey_responses**: Actual response data from survey participants; requires multi-step export process
- **distributions**: Tracks how surveys were distributed (email, SMS, anonymous link, etc.)
- **mailing_lists**: Collections of contacts used for survey distribution
- **contacts**: Individual contact records with custom attributes and embedded data
- **directories**: Organizational structure for managing contacts and other XM data

## **Object Schema**

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

**High-level schema (connector view - actual API response)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique survey identifier (e.g., `SV_abc123xyz`). Primary key. |
| `name` | string | Survey name/title. |
| `ownerId` | string | Qualtrics user ID of the survey owner. |
| `isActive` | boolean | Whether the survey is currently active. |
| `creationDate` | string (ISO 8601 datetime) | When the survey was created. |
| `lastModified` | string (ISO 8601 datetime) | Last modification timestamp. Used as incremental cursor. |

**⚠️ Schema Validation Note**: The following fields are documented in some API references but are **NOT** returned by the `GET /surveys` list endpoint:
- `organizationId`: Organization ID (not in list response)
- `expiration`: Expiration date struct (not in list response)
- `brandId`: Brand ID (not in list response)
- `brandBaseURL`: Base URL (not in list response)

These fields may be available via the `GET /surveys/{surveyId}` detail endpoint if needed.

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

**High-level schema (connector view)**:

Core response fields (always present):

| Column Name | Type | Description |
|------------|------|-------------|
| `responseId` | string | Unique response identifier. Primary key. |
| `surveyId` | string | Survey ID this response belongs to. |
| `recordedDate` | string (ISO 8601 datetime) | When the response was recorded. Used as incremental cursor. |
| `startDate` | string (ISO 8601 datetime) | When respondent started the survey. |
| `endDate` | string (ISO 8601 datetime) | When respondent completed the survey. |
| `status` | integer | Response status: 0=In Progress, 1=Completed, 2=Screen Out, etc. |
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
| `labels` | map\<string, string\> | Human-readable labels for question responses (if `useLabels=true`). |
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
    "useLabels": false,
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

**⚠️ Important**: The API does NOT return `surveyId` in the response records, even though it's shown in API documentation examples. Since we know which survey we're querying (passed as parameter), the connector manually adds `surveyId` to each response record.

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
| `headers` | struct | Email headers (fromEmail, fromName, replyToEmail). |
| `recipients` | struct | Recipient information (mailingListId, contactId, libraryId, sampleId). |
| `message` | struct | Message details (libraryId, messageId, messageType). |
| `surveyLink` | struct | Survey link information (surveyId, expirationDate, linkType). |
| `stats` | struct | Distribution statistics (sent, failed, started, bounced, opened, skipped, finished, complaints, blocked). |

**⚠️ Schema Validation Note**: The following field names differ from some documentation:
- **Actual**: `sendDate` (not `sentDate`)
- **Actual**: `surveyLink.surveyId` nested (not root-level `surveyId`)
- **Actual**: headers does NOT include `subject` field

### `contacts` object

**Source endpoint**:
`GET /directories/{directoryId}/mailinglists/{mailingListId}/contacts`

**Key behavior**:
- Returns contacts within a specific mailing list in a directory
- Requires both `directoryId` and `mailingListId` as table-level parameters
- Contacts can have custom embedded data fields
- Supports pagination
- Only available for XM Directory users (not XM Directory Lite)

**High-level schema**:

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

**Note**: The API does not return `lastModifiedDate`, `creationDate`, `embeddedData`, `responseHistory`, or `emailHistory` fields. Therefore, contacts table uses **snapshot mode** (full refresh) instead of CDC.

## **Get Object Primary Keys**

Primary keys for each object are static and defined by the connector:

| Object Name | Primary Key Column(s) |
|------------|----------------------|
| `surveys` | `id` |
| `survey_responses` | `responseId` |
| `distributions` | `id` |
| `mailing_lists` | `id` |
| `contacts` | `contactId` |
| `directories` | `id` |

**Notes**:
- All primary keys are string type
- Primary keys are globally unique within Qualtrics
- No compound primary keys are needed

## **Object's ingestion type**

| Object Name | Ingestion Type | Cursor Field | Notes |
|------------|---------------|--------------|-------|
| `surveys` | `cdc` | `lastModified` | Supports incremental updates based on modification timestamp. |
| `survey_responses` | `append` | `recordedDate` | New responses are appended; existing responses are immutable once completed. |
| `distributions` | `cdc` | `modifiedDate` | Distribution records can be updated (e.g., status changes). |
| `mailing_lists` | `snapshot` | N/A | Full refresh; relatively small dataset. |
| `contacts` | `snapshot` | N/A | Full refresh; API does not return lastModifiedDate field. |
| `directories` | `snapshot` | N/A | Full refresh; organizational structure. |

**Incremental sync strategy**:
- For `cdc` objects: Store the maximum cursor value from previous sync; on next sync, filter by `cursor_field >= last_max_value`
- For `append` objects: Similar to CDC, but records are never updated once inserted
- For `snapshot` objects: Full table refresh on each sync

**Delete handling**:
- Qualtrics API does not provide explicit delete flags in responses
- Deleted surveys: No longer appear in `/surveys` endpoint
- Deleted responses: Remain in export but may have status changes
- Recommendation: Use full refresh periodically to detect deletions

## **Read API for Data Retrieval**

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

### `survey_responses` export workflow

**Step 1: Create export**

**Endpoint**: `POST /surveys/{surveyId}/export-responses`

**Method**: POST

**Request body parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `format` | string | Yes | Export format: `json`, `csv`, `spss`, `ndjson`. Connector uses `json`. |
| `useLabels` | boolean | No | Use human-readable labels instead of IDs (default: false). |
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

### `contacts` endpoint

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

**Notes**:
- Requires both `directoryId` and `mailingListId` as table-level parameters
- Each mailing list is treated as a separate table of contacts
- Only available for XM Directory users (not XM Directory Lite)

**Incremental retrieval**:
- Not supported - API does not return `lastModifiedDate` field
- Use snapshot mode (full refresh on each run)

### Rate Limits

| Limit Type | Value |
|-----------|-------|
| Requests per minute | 500 |
| Concurrent requests | 20 |
| Response on limit exceed | `429 Too Many Requests` |
| Retry-After header | Seconds to wait before retrying |

**Best practices**:
- Implement exponential backoff for 429 responses
- Respect `Retry-After` header when present
- Use pagination to avoid large single requests
- Consider queueing requests to stay under concurrent limit

## **Field Type Mapping**

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

**Special behaviors**:
- **embeddedData**: Keys are dynamic (user-defined); use `MapType(StringType, StringType)`
- **values**: Question IDs are dynamic; use `MapType(StringType, StructType(...))`
- **Dates**: Keep as `StringType` to preserve ISO 8601 format; convert to `TimestampType` in transformations

## **Write API**

**Note**: This connector is **read-only** and does not implement write operations. Write APIs are documented in a separate section if needed for testing purposes.

The Qualtrics API supports write operations for:
- Creating surveys
- Creating distributions
- Adding/updating contacts
- Creating response exports (which is a read-preparation operation)

For connector purposes, we focus exclusively on **READ operations** to ingest data into the lake.

---

## **Write-Back APIs (For Testing Only)**

**⚠️ WARNING: These APIs are documented solely for test data generation. They are NOT part of the connector's read functionality.**

### Purpose
These write endpoints enable automated testing by:
1. Creating test survey responses in the source system
2. Validating that incremental sync picks up newly created records
3. Verifying field mappings and schema correctness end-to-end

### Write Endpoints

#### Create Survey Response (via Sessions API)

The Qualtrics Sessions API allows programmatic creation of survey responses for testing purposes.

- **Method**: POST
- **Endpoint**: `https://{datacenterid}.qualtrics.com/API/v3/surveys/{surveyId}/sessions`
- **Authentication**: Same as read operations (X-API-TOKEN header)
- **Purpose**: Creates a new response session that can be populated with answer data

**Required Permissions**: 
- API access enabled
- Response creation permissions for the target survey
- Survey must be active

**Workflow for Creating a Complete Response**:

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

### Write-Specific Constraints

- **Rate Limits**: Same as read operations - 500 requests per minute, 20 concurrent requests
- **Eventual Consistency**: 
  - Responses may take 5-30 seconds to appear in export API after creation
  - **Recommended wait time**: 30-60 seconds after writing before attempting to read for validation
- **Required Delays**: 
  - Wait at least 30 seconds after session completion before exporting responses
  - For bulk writes, add delays between writes to avoid rate limiting
- **Unique Constraints**: 
  - Response IDs are auto-generated and globally unique
  - Session IDs must be unique per survey
- **Test Environment**: 
  - Use dedicated test surveys for write operations
  - Tag test data with identifiable embedded data fields (e.g., `test_id`, `source: "automated_test"`)
  - Test surveys should be isolated from production data collection
- **Data Cleanup**:
  - Qualtrics does not provide bulk delete APIs for responses
  - Test responses remain in the system unless manually deleted via UI
  - Recommendation: Use dedicated test surveys that can be archived after testing

### Write Operation Limitations

1. **Session API Availability**: The Sessions API may not be available on all Qualtrics license tiers. Verify availability in your account.

2. **Response Editing**: Once a response is marked as `finished: true`, it typically cannot be edited via API. Only new responses can be created.

3. **Question Type Constraints**: 
   - Complex question types (Matrix, Heat Map, etc.) may require specific answer formats
   - Text entry questions accept simple string values
   - Multiple choice questions require valid choice IDs

4. **Embedded Data**: 
   - Custom embedded data fields must be defined in the survey before use
   - Undefined embedded data fields may be ignored or cause errors

### Research Log for Write APIs

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://api.qualtrics.com/ | 2024-12-29 | High | Sessions API endpoint structure |
| Official Docs | https://www.qualtrics.com/support/integrations/api-integration/overview/ | 2024-12-29 | High | Authentication and permissions |
| Web Search | Qualtrics API documentation searches | 2024-12-29 | Medium | Distribution creation and response workflow |
| Inferred | Based on REST API patterns | 2024-12-29 | Medium | Field transformations and eventual consistency behavior |

**Testing Recommendations**:
1. Start with manual response creation via Qualtrics UI to understand expected formats
2. Use a simple test survey with 2-3 basic questions (text, multiple choice)
3. Implement write operations with explicit test data markers
4. Add adequate wait times (30-60 seconds) before validation reads
5. Consider using distribution-based approach for more realistic testing scenarios

---

## **Research Log**

### Read Operations Research

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|-----------|-------------------|
| Official Docs | https://www.qualtrics.com/support/integrations/api-integration/overview/ | 2024-12-23 | High | Authentication method (API token), header format |
| Official Docs | https://www.qualtrics.com/support/integrations/api-integration/using-qualtrics-api-documentation/ | 2024-12-23 | High | Base URL structure, datacenter ID location |
| Official Docs | https://api.qualtrics.com/ | 2024-12-23 | High | API reference, endpoints, response structures |
| Web Search | Multiple searches for specific endpoints | 2024-12-23 | Medium | Pagination mechanism, export workflow, rate limits |
| YouTube Tutorial | https://www.youtube.com/watch?v=_uhY_a4NgNc | 2024-12-23 | Medium | Response export workflow confirmation |
| Python Lib Docs | https://www.qualtricsapi-pydocs.com/distributions.html | 2024-12-31 | High | Distributions endpoint requires surveyId parameter |
| Python Lib Docs | https://www.qualtricsapi-pydocs.com/mailinglist(XM%20Subscribers).html | 2024-12-31 | High | Contacts endpoint structure, mailingListId parameter |
| Community | Qualtrics Community discussions | 2024-12-31 | Medium | DirectoryId requirement for contacts endpoint |
| API Reference | Stoplight API docs | 2024-12-31 | High | Full endpoint path: /directories/{directoryId}/mailinglists/{mailingListId}/contacts |

### Write Operations Research (Testing Only)

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|-----------|-------------------|
| Official Docs | https://api.qualtrics.com/ | 2024-12-29 | High | Sessions API structure and availability |
| Official Docs | https://www.qualtrics.com/support/integrations/api-integration/overview/ | 2024-12-29 | High | Write permissions and authentication |
| Web Search | Qualtrics API create response searches | 2024-12-29 | Medium | Distribution-based response creation |
| Web Search | Qualtrics API sessions endpoint documentation | 2024-12-29 | Medium | Response creation workflow and constraints |

**Note on research approach**:
- Primary source: Official Qualtrics API documentation and support pages
- Secondary sources: Developer community posts and tutorials
- Specific field schemas derived from API reference documentation
- Export workflow confirmed through multiple sources including video tutorials
- Rate limits confirmed through official documentation references

**Gaps and TBD items**:
- Specific response status codes beyond 0 (in progress) and 1 (completed) need verification
- Some nested structures in distributions and mailing lists may have additional fields not documented here
- Exact behavior of delete detection may require testing
- Mailing lists endpoint (`GET /directories/{directoryId}/mailinglists`) documented but not yet implemented
- Directories endpoint (`GET /directories`) documented but not yet implemented

## **Sources and References**

1. **Official Qualtrics API Documentation**
   - Main API Reference: https://api.qualtrics.com/
   - Getting Started: https://www.qualtrics.com/support/integrations/api-integration/overview/
   - Using API Documentation: https://www.qualtrics.com/support/integrations/api-integration/using-qualtrics-api-documentation/
   - **Confidence**: High - Official documentation is the primary source of truth

2. **Developer Portal**
   - Qualtrics Developer Portal: https://www.qualtrics.com/support/integrations/developer-portal/
   - **Confidence**: High - Official developer resources

3. **Educational Resources**
   - YouTube Tutorial: Download Qualtrics survey responses using Python - https://www.youtube.com/watch?v=_uhY_a4NgNc
   - **Confidence**: Medium - Community tutorial confirming export workflow

**Reference implementations** (searched but not found with sufficient detail):
- Airbyte Qualtrics connector: Searched but specific implementation details not accessible through web search
- Singer tap-qualtrics: Searched but specific schema definitions not found

**Conflict resolution**:
- All information prioritized from official Qualtrics documentation
- Where official docs were incomplete, reasonable defaults were applied based on RESTful API conventions
- Any assumptions are noted in the "Gaps and TBD items" section

**Documentation completeness**:
- ✅ Authentication method documented (API token + datacenter ID + directory ID)
- ✅ Primary objects (surveys, responses) fully documented
- ✅ Secondary objects (distributions, contacts) documented with correct endpoints
- ✅ Pagination mechanism described
- ✅ Incremental sync strategy defined
- ✅ Field schemas include all known fields
- ✅ Rate limits documented
- ✅ Response export workflow fully described
- ✅ Write-back APIs documented for testing purposes (Step 5 complete)
- ✅ Field transformations between write and read operations documented
- ✅ Write-specific constraints and eventual consistency delays documented
- ✅ Directory ID requirement for contacts endpoint documented (corrected 2024-12-31)
- ✅ Survey ID requirement for distributions endpoint verified (2024-12-31)
- ✅ **Schema validation completed against live API** (2024-12-31) - see section below
- ⚠️ Sessions API availability may vary by Qualtrics license tier - requires verification
- ⚠️ Mailing lists and directories endpoints documented but not yet implemented

---

## **Schema Validation Against Live API**

**Validation Date**: December 31, 2024
**Method**: Called all connector APIs and compared actual responses with documented schemas

### Validation Results Summary

| Table | Status | Discrepancies | Action Taken |
|-------|--------|---------------|--------------|
| `surveys` | ✅ Fixed | 4 fields not in API | Removed from schema |
| `survey_responses` | ✅ Correct | 0 (MapType fields correctly designed) | No changes needed |
| `distributions` | ✅ Fixed | 29 fields missing/incorrect | Schema completely updated |
| `contacts` | ✅ Perfect Match | 0 | No changes needed |

### Detailed Findings

#### 1. surveys Table Discrepancies (FIXED)

**Fields documented but NOT in actual API response:**
- `organizationId` (string): Not returned by GET /surveys
- `expiration` (struct): Not returned by GET /surveys
- `brandId` (string): Not returned by GET /surveys
- `brandBaseURL` (string): Not returned by GET /surveys

**Resolution**: Removed these 4 fields from connector schema. They may be available via GET /surveys/{surveyId} detail endpoint if needed in the future.

**Final Schema** (6 fields):
```
id, name, ownerId, isActive, creationDate, lastModified
```

#### 2. survey_responses Table (NO CHANGES NEEDED)

**Analysis**: The validation script reported 28 "missing" fields, but these are all nested fields within MapType structures (e.g., `values.QID3.choiceId`, `labels.finished`). These are correctly represented by:
- `values`: MapType(StringType, StructType(...))
- `labels`: MapType(StringType, StringType)
- `displayedValues`: MapType(StringType, StringType)

**Conclusion**: Schema design is correct - no changes needed.

#### 3. distributions Table Discrepancies (FIXED)

**Fields in API but NOT in documented schema:**
- `parentDistributionId` (string): Parent distribution for reminders
- `message` (struct): Message details with libraryId, messageId, messageType
- `recipients` (struct): Recipient info with mailingListId, contactId, libraryId, sampleId
- `surveyLink` (struct): Survey link with surveyId, expirationDate, linkType
- `sendDate` (string): Actual send date (not `sentDate`)

**Fields documented but NOT in API response:**
- `surveyId` (root level): Actually nested in `surveyLink.surveyId`
- `sentDate`: Actual field name is `sendDate`
- `headers.subject`: Not included in API response

**Resolution**: Completely updated distributions schema to match actual API response with 4 additional nested structs.

**Final Schema** (14 fields):
```
id, parentDistributionId, ownerId, organizationId, requestType, requestStatus,
sendDate, createdDate, modifiedDate, headers (struct), recipients (struct),
message (struct), surveyLink (struct), stats (struct)
```

#### 4. contacts Table (PERFECT MATCH)

**Status**: ✅ All 10 fields match exactly between documentation and API response

**Fields**:
```
contactId, firstName, lastName, email, phone, extRef, language,
unsubscribed, mailingListUnsubscribed, contactLookupId
```

**Note**: This table was previously fixed on 2024-12-31 when we discovered the schema mismatch during Databricks testing.

### Validation Methodology

1. **Created validation script** (`validate_schemas.py`) that:
   - Calls each API endpoint with real credentials
   - Retrieves actual data records
   - Extracts all field names from JSON responses
   - Compares with connector schemas
   - Reports discrepancies

2. **Analyzed sample records**:
   - surveys: 2 records
   - survey_responses: 2 records
   - distributions: 1 record
   - contacts: 2 records

3. **Updated all files**:
   - `qualtrics.py`: Schema definitions
   - `README.md`: User-facing schema documentation
   - `qualtrics_api_doc.md`: Technical API documentation
   - `IMPLEMENTATION_COMPLETE.md`: Implementation notes

### Key Lessons Learned

1. **Official API documentation may not match actual responses**: Several documented fields (organizationId, expiration, brandId, brandBaseURL) are not returned by list endpoints.

2. **Field name variations**: `sentDate` vs `sendDate` - always validate actual API responses.

3. **Nested structures**: The distributions endpoint returns much richer nested data than documented in some API references.

4. **MapType vs explicit fields**: Dynamic fields (question IDs, labels) are correctly handled with MapType - validation scripts may flag nested keys as "missing" but this is expected behavior.

5. **Endpoint-specific schemas**: List endpoints may return fewer fields than detail endpoints (GET /surveys vs GET /surveys/{id}).

### Verification Commands

To reproduce this validation:
```bash
# Run validation script
python3 sources/qualtrics/validate_schemas.py

# Results saved to:
sources/qualtrics/schema_validation_results.json
```

---

