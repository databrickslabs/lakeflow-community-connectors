# Lakeflow Qualtrics Community Connector

This documentation describes how to configure and use the **Qualtrics** Lakeflow community connector to ingest survey data from Qualtrics into Databricks.

## Prerequisites

- **Qualtrics account**: You need access to a Qualtrics account with API permissions enabled
- **API Token**: 
  - Must be generated in your Qualtrics account settings
  - Requires "Access API" permission granted by your Brand Administrator
  - Minimum permissions needed:
    - Read surveys
    - Export survey responses
- **Datacenter ID**: Your Qualtrics datacenter identifier (e.g., `fra1`, `ca1`, `yourdatacenterid`)
- **Network access**: The environment running the connector must be able to reach `https://{datacenter}.qualtrics.com`
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_token` | string | yes | Qualtrics API token for authentication | `YOUR_QUALTRICS_API_TOKEN` |
| `datacenter_id` | string | yes | Qualtrics datacenter identifier where your account is hosted | `fra1`, `ca1`, `yourdatacenterid` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names. This connector requires table-specific options for some tables. | `surveyId,mailingListId,directoryId,only_active_surveys,max_surveys` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`surveyId,mailingListId,directoryId,only_active_surveys,max_surveys`

> **Note**: Table-specific options such as `surveyId` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. The option name must be included in `externalOptionsAllowList` for the connection to allow it.

### Obtaining the Required Parameters

#### API Token

1. **Log into Qualtrics**: Sign in to your Qualtrics account
2. **Navigate to Account Settings**:
   - Click on your account name in the top-right corner
   - Select "Account Settings"
3. **Access Qualtrics IDs**:
   - Navigate to the "Qualtrics IDs" section
4. **Generate API Token**:
   - Under the "API" section, click "Generate Token"
   - If this option is unavailable, contact your Brand Administrator to enable API access
5. **Copy and Store**: Save the generated token securely - you'll use this as the `api_token` connection option

> **Security Note**: Never share your API token or commit it to version control. Store it securely using Databricks secrets or similar secure credential storage.

#### Datacenter ID

The datacenter ID identifies where your Qualtrics account is hosted:

1. **From the URL**: When logged into Qualtrics, look at your browser URL
   - Format: `https://{datacenterid}.qualtrics.com/...`
   - Example: If your URL is `https://fra1.qualtrics.com/...`, your datacenter ID is `fra1`
2. **From Account Settings**: Also visible in Account Settings → Qualtrics IDs section

Common datacenter IDs:
- `fra1` - Europe (Frankfurt)
- `ca1` - Canada
- `au1` - Australia  
- `sjc1` - US West
- Custom datacenter IDs for enterprise accounts

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page
2. Select the Qualtrics connector or create a new connection
3. Provide the required parameters:
   - `api_token`: Your Qualtrics API token
   - `datacenter_id`: Your datacenter identifier
   - `externalOptionsAllowList`: Set to `surveyId,mailingListId,directoryId` to enable all table-specific options

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Qualtrics connector exposes a **static list** of tables:

- `surveys` - Survey definitions and metadata
- `survey_definitions` - Full survey structure including questions, blocks, and flow
- `survey_responses` - Individual survey response data
- `distributions` - Survey distribution records (email sends, SMS, anonymous links)
- `contacts` - Contact records within mailing lists

### Object Summary, Primary Keys, and Ingestion Mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|-------|-------------|----------------|-------------|-------------------|
| `surveys` | Survey metadata including name, status, creation/modification dates | `cdc` | `id` | `last_modified` |
| `survey_definitions` | Full survey structure with questions, blocks, flow, and options | `cdc` | `survey_id` | `last_modified` |
| `survey_responses` | Individual responses to surveys including all question answers | `append` | `response_id` | `recorded_date` |
| `distributions` | Distribution records for survey invitations and sends | `cdc` | `id` | `modified_date` |
| `contacts` | Contact records within mailing lists | `snapshot` | `contact_id` | N/A (full refresh) |

### Required and Optional Table Options

Table-specific options are passed via the pipeline spec under `table` in `objects`:

#### `surveys` table
- **No table-specific options required**
- Automatically retrieves all surveys accessible to the authenticated account
- Supports incremental sync based on `lastModified` timestamp

#### `survey_definitions` table
- **`surveyId`** (string, **optional**): The Survey ID to retrieve the definition for
  - Format: `SV_...` (e.g., `SV_abc123xyz`)
  - Returns the complete survey structure including all questions, choices, blocks, and flow
  - Useful for building data dictionaries to interpret survey response values
  - **If not provided**: Auto-consolidates definitions from all surveys (see Auto-Consolidation below)

#### `survey_responses` table
- **`surveyId`** (string, **optional**): The Survey ID to export responses from
  - Format: `SV_...` (e.g., `SV_abc123xyz`)
  - Can be found in Qualtrics UI under: Survey → Tools → Survey IDs
  - Or in the browser URL when editing a survey
  - **If not provided**: Auto-consolidates responses from all surveys (see Auto-Consolidation below)

#### `distributions` table
- **`surveyId`** (string, **optional**): The Survey ID to retrieve distributions for
  - Format: `SV_...` (e.g., `SV_abc123xyz`)
  - Same format as survey_responses table
  - Retrieves all distributions (email sends, SMS, etc.) for the specified survey
  - **If not provided**: Auto-consolidates distributions from all surveys (see Auto-Consolidation below)

#### `contacts` table
- **`directoryId`** (string, **required**): The Directory ID (also called Pool ID)
  - Format: `POOL_...` (e.g., `POOL_abc123xyz`)
  - Can be found in Qualtrics UI: Account Settings → Qualtrics IDs
  - Identifies your XM Directory
- **`mailingListId`** (string, **required**): The Mailing List ID
  - Format: `CG_...` (e.g., `CG_def456xyz`)
  - Can be found in: Contacts → Lists → Select a list → check URL or list details
  - Or via API: GET `/directories/{directoryId}/mailinglists`

> **Note**: The `contacts` table is only available for XM Directory users (not XM Directory Lite accounts).

### Auto-Consolidation Feature

**New in v1.1**: The connector now supports automatic consolidation of data from multiple surveys into a single table. When `surveyId` is **not provided** for `survey_definitions`, `survey_responses`, or `distributions` tables, the connector will automatically:

1. Retrieve all survey IDs from your account
2. Fetch data for each survey
3. Consolidate all records into a single table

This eliminates the need to manually union data from multiple surveys in your downstream analytics.

#### Auto-Consolidation Options

When using auto-consolidation (no `surveyId` specified), you can control the behavior with these additional table options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `only_active_surveys` | string | `"true"` | Set to `"true"` to only include active surveys, `"false"` to include all surveys |
| `max_surveys` | string | `"100"` | Maximum number of surveys to process (safety limit to prevent excessive API calls) |

#### Auto-Consolidation Examples

**Example 1: Consolidate responses from all active surveys**
```json
{
  "table": {
    "source_table": "survey_responses"
  }
}
```

**Example 2: Consolidate from all surveys (including inactive)**
```json
{
  "table": {
    "source_table": "survey_responses",
    "only_active_surveys": "false"
  }
}
```

**Example 3: Consolidate from first 50 surveys only**
```json
{
  "table": {
    "source_table": "survey_responses",
    "max_surveys": "50"
  }
}
```

**Example 4: Use specific survey (backward compatible)**
```json
{
  "table": {
    "source_table": "survey_responses",
    "surveyId": "SV_abc123xyz"
  }
}
```

#### Performance Considerations for Auto-Consolidation

When using auto-consolidation:

- **API Calls**: The connector makes one API call per survey (e.g., 10 surveys = 10 API calls)
- **Rate Limiting**: Built-in delays (0.5 seconds) between surveys to respect Qualtrics rate limits
- **Incremental Sync**: For `survey_responses` and `distributions`, the connector tracks offsets per survey to support incremental updates
- **Error Handling**: If one survey fails, the connector continues with others and logs warnings
- **Recommended for**: Most use cases with <100 surveys. For larger deployments, consider filtering with `max_surveys` or using specific `surveyId` per table

### Schema Highlights

#### `surveys` table schema:
- `id` (string): Unique survey identifier (primary key)
- `name` (string): Survey name/title
- `owner_id` (string): User ID of survey owner
- `is_active` (boolean): Whether survey is currently active
- `creation_date` (string): ISO 8601 timestamp when survey was created
- `last_modified` (string): ISO 8601 timestamp of last modification (incremental cursor)

> **Note**: Fields like `brand_id`, `brand_base_url`, `organization_id`, and `expiration` are not returned by the list surveys endpoint. Use the `survey_definitions` table if you need detailed survey structure.

#### `survey_definitions` table schema:
- `survey_id` (string): Unique survey identifier (primary key)
- `survey_name` (string): Survey name/title
- `survey_status` (string): Survey status (e.g., Active, Inactive)
- `owner_id` (string): User ID of survey owner
- `creator_id` (string): User ID who created the survey
- `brand_id` (string): Brand identifier
- `brand_base_url` (string): Brand base URL (e.g., `https://yourbrand.qualtrics.com`)
- `last_modified` (string): ISO 8601 timestamp of last modification
- `last_accessed` (string): ISO 8601 timestamp of last access
- `last_activated` (string): ISO 8601 timestamp of last activation
- `question_count` (string): Number of questions in the survey
- `questions` (string): JSON string containing map of question IDs to question definitions
- `blocks` (string): JSON string containing block definitions
- `survey_flow` (string): JSON string containing flow elements defining survey navigation
- `survey_options` (string): JSON string containing survey-level settings
- `response_sets` (string): JSON string containing response set definitions
- `scoring` (string): JSON string containing scoring configuration
- `project_info` (string): JSON string containing project metadata (ProjectCategory, ProjectType, etc.)

> **Note**: Complex nested fields (`questions`, `blocks`, `survey_flow`, etc.) are stored as JSON strings because the Qualtrics API returns variable structures. Use Spark's `from_json()` function to parse them:
> ```sql
> SELECT survey_id, from_json(questions, 'MAP<STRING, STRUCT<QuestionText: STRING>>') FROM survey_definitions
> ```

#### `survey_responses` table schema:
- `response_id` (string): Unique response identifier (primary key)
- `survey_id` (string): Survey ID this response belongs to
- `recorded_date` (string): ISO 8601 timestamp when response was recorded (incremental cursor)
- `start_date` (string): When respondent started the survey
- `end_date` (string): When respondent completed the survey
- `status` (long): Response status (0=In Progress, 1=Completed, etc.)
- `ip_address` (string): Respondent's IP address (if collected)
- `progress` (long): Percentage completed (0-100)
- `duration` (long): Time spent in seconds
- `finished` (boolean): Whether response is finished (true/false)
- `distribution_channel` (string): How survey was distributed (email, anonymous, etc.)
- `user_language` (string): Language code used by respondent
- `location_latitude` (string): Latitude (if location collected)
- `location_longitude` (string): Longitude (if location collected)
- `values` (map<string, struct>): Question responses keyed by Question ID
  - Each value contains:
    - `choice_text` (string): Text of selected choice
    - `choice_id` (string): ID of selected choice
    - `text_entry` (string): Free text entry
- `labels` (map<string, string>): Human-readable labels for responses
- `displayed_fields` (array<string>): Fields displayed to respondent
- `displayed_values` (map<string, string>): Displayed values
- `embedded_data` (map<string, string>): Custom embedded data fields

> **Note**: Question IDs (e.g., `QID1`, `QID2`) are dynamic and specific to each survey. The `values` field uses a map type to accommodate any question structure.

#### `distributions` table schema:
- `id` (string): Unique distribution identifier (primary key)
- `parent_distribution_id` (string): Parent distribution ID (for follow-ups/reminders)
- `owner_id` (string): User ID who created the distribution
- `organization_id` (string): Organization ID
- `request_type` (string): Distribution method (e.g., GeneratedInvite, Invite, Reminder)
- `request_status` (string): Status (e.g., Generated, pending, inProgress, complete)
- `send_date` (string): ISO 8601 timestamp when sent/scheduled
- `created_date` (string): ISO 8601 timestamp when created
- `modified_date` (string): ISO 8601 timestamp of last modification (incremental cursor)
- `headers` (struct): Email distribution headers
  - `from_email` (string): Sender email address
  - `from_name` (string): Sender name
  - `reply_to_email` (string): Reply-to email address
- `recipients` (struct): Recipient information
  - `mailing_list_id` (string): Mailing list ID
  - `contact_id` (string): Specific contact ID (if targeted)
  - `library_id` (string): Library ID
  - `sample_id` (string): Sample ID (if using sample)
- `message` (struct): Message details
  - `library_id` (string): Message library ID
  - `message_id` (string): Message template ID
  - `message_type` (string): Type (e.g., Inline, InviteEmail)
- `survey_link` (struct): Survey link information
  - `survey_id` (string): Survey ID this distribution belongs to
  - `expiration_date` (string): When survey link expires
  - `link_type` (string): Link type (e.g., Individual, Multiple, Anonymous)
- `stats` (struct): Distribution statistics
  - `sent` (long): Number of emails/SMS sent
  - `failed` (long): Number of send failures
  - `started` (long): Number of surveys started
  - `bounced` (long): Number of bounced emails
  - `opened` (long): Number of emails opened
  - `skipped` (long): Number skipped
  - `finished` (long): Number of surveys completed
  - `complaints` (long): Number of complaints
  - `blocked` (long): Number blocked

#### `contacts` table schema:
- `contact_id` (string): Unique contact identifier (primary key)
- `first_name` (string): Contact's first name
- `last_name` (string): Contact's last name
- `email` (string): Contact's email address
- `phone` (string): Contact's phone number
- `ext_ref` (string): External reference ID
- `language` (string): Preferred language code
- `unsubscribed` (boolean): Whether contact has unsubscribed globally
- `mailing_list_unsubscribed` (boolean): Whether contact has unsubscribed from this mailing list
- `contact_lookup_id` (string): Contact lookup identifier

## Data Type Mapping

Qualtrics JSON fields are mapped to Spark types as follows:

| Qualtrics API Type | Example Fields | Spark Type | Notes |
|--------------------|----------------|------------|-------|
| string | `id`, `name`, `responseId`, dates | `StringType` | All identifiers and ISO 8601 dates stored as strings |
| integer | `status`, `progress`, `duration` | `LongType` | All numeric fields use `LongType` to avoid overflow |
| boolean | `isActive`, `finished` | `BooleanType` | Standard true/false values |
| ISO 8601 datetime (string) | `creationDate`, `recordedDate`, `lastModified` | `StringType` | Stored as UTC strings; can be cast to timestamp downstream |
| object (nested) | `expiration` | `StructType` | Nested objects preserved instead of flattened |
| map (dynamic keys) | `values`, `embeddedData`, `labels` | `MapType(StringType, StructType/StringType)` | Used for dynamic question responses and custom fields |
| array | `displayedFields` | `ArrayType(StringType)` | Arrays preserved as nested collections |
| nullable fields | `brandId`, `ipAddress`, `embeddedData` | Same type + `null` | Missing fields surfaced as `null`, not empty objects |

The connector is designed to:
- Use `LongType` for all numeric fields (status, progress, duration)
- Preserve nested structures (expiration, question values)
- Use `MapType` for dynamic fields (question responses, embedded data)
- Store dates as strings in ISO 8601 format for consistency

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Qualtrics connector source in your workspace.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingestion_pipeline.py`), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Qualtrics connector
- One or more **tables** to ingest, with required table options

Example `pipeline_spec` with auto-consolidation (recommended):

```json
{
  "pipeline_spec": {
    "connection_name": "qualtrics_connection",
    "object": [
      {
        "table": {
          "source_table": "surveys"
        }
      },
      {
        "table": {
          "source_table": "survey_definitions"
        }
      },
      {
        "table": {
          "source_table": "survey_responses"
        }
      },
      {
        "table": {
          "source_table": "distributions"
        }
      },
      {
        "table": {
          "source_table": "contacts",
          "directoryId": "POOL_abc123xyz",
          "mailingListId": "CG_def456xyz"
        }
      }
    ]
  }
}
```

Example `pipeline_spec` with specific surveys (backward compatible):

```json
{
  "pipeline_spec": {
    "connection_name": "qualtrics_connection",
    "object": [
      {
        "table": {
          "source_table": "surveys"
        }
      },
      {
        "table": {
          "source_table": "survey_definitions",
          "surveyId": "SV_abc123xyz"
        }
      },
      {
        "table": {
          "source_table": "survey_responses",
          "surveyId": "SV_abc123xyz"
        }
      },
      {
        "table": {
          "source_table": "distributions",
          "surveyId": "SV_abc123xyz"
        }
      },
      {
        "table": {
          "source_table": "contacts",
          "directoryId": "POOL_abc123xyz",
          "mailingListId": "CG_def456xyz"
        }
      }
    ]
  }
}
```

Configuration notes:
- `connection_name` must point to the UC connection configured with your Qualtrics `api_token` and `datacenter_id`
- For `surveys` table: No additional options needed
- For `survey_definitions` table: `surveyId` is **optional** (omit for auto-consolidation)
- For `survey_responses` table: `surveyId` is **optional** (omit for auto-consolidation)
- For `distributions` table: `surveyId` is **optional** (omit for auto-consolidation)
- For `contacts` table: Both `directoryId` and `mailingListId` are **required**
- When using auto-consolidation, all surveys' data is consolidated into a single table automatically

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., scheduled job or workflow).

#### Incremental Sync Behavior

**For `surveys` table (CDC)**:
- **First run**: Retrieves all surveys
- **Subsequent runs**: Only fetches surveys modified since last sync (based on `last_modified` field)
- Automatically maintains cursor state

**For `survey_definitions` table (CDC)**:
- **First run**: Retrieves all survey definitions
- **Subsequent runs**: Only fetches definitions modified since last sync (based on `last_modified` field)
- Supports SCD Type 2 for tracking survey structure changes over time
- Useful for auditing when questions, blocks, or flow were modified
- Returns questions, blocks, flow, and all survey structure in a single record
- Automatically maintains cursor state per survey (when using auto-consolidation)

**For `survey_responses` table (Append)**:
- **First run**: Exports all responses for the specified survey
- **Subsequent runs**: Only exports responses recorded since last sync (based on `recorded_date` field)
- New responses are appended; existing responses are immutable
- Export process uses Qualtrics 3-step workflow:
  1. Create export job
  2. Poll for completion
  3. Download and parse results

**For `distributions` table (CDC)**:
- **First run**: Retrieves all distributions for the specified survey
- **Subsequent runs**: Only fetches distributions modified since last sync (based on `modified_date` field)
- Supports tracking email sends, SMS, and other distribution methods

**For `contacts` table (Snapshot)**:
- **All runs**: Performs full refresh of all contacts in the specified mailing list
- **Note**: The Qualtrics API does not return `last_modified_date` for contacts, so incremental sync is not supported
- Requires XM Directory (not available for XM Directory Lite)

> **Note**: Survey response exports can take 30-90 seconds to complete depending on response count. The connector handles this automatically with appropriate wait times and polling.

#### Best Practices

- **Start with surveys table first**: Retrieve survey list to identify Survey IDs before configuring response exports
- **Use incremental sync**: Both tables support incremental patterns to minimize API calls and export time
- **Monitor response export times**: Large surveys (>10,000 responses) may take several minutes to export
- **Respect rate limits**: 
  - Qualtrics enforces **500 requests per minute** and **20 concurrent requests** maximum
  - The connector implements automatic retry with exponential backoff for rate limiting
- **Handle eventual consistency**: 
  - New survey responses may take 30-60 seconds to become available in exports
  - Incremental syncs account for this with appropriate lookback windows
- **Set appropriate schedules**:
  - For active surveys collecting responses: Schedule every 15-30 minutes
  - For survey metadata only: Schedule daily or weekly
  - Balance data freshness requirements with API usage

#### Troubleshooting

**Common Issues:**

**Authentication Failures (`401 Unauthorized` / `403 Forbidden`)**:
- **Cause**: Invalid API token or insufficient permissions
- **Solution**:
  - Verify the `api_token` is correct and not expired
  - Confirm "Access API" permission is enabled for your account
  - Contact your Qualtrics Brand Administrator to grant API access
  - Regenerate token if needed (note: this invalidates the old token)

**`400 Bad Request` errors**:
- **Cause**: Invalid `surveyId` or survey incompatibility
- **Solution**:
  - Verify the Survey ID format (`SV_...`)
  - Confirm the survey exists and is accessible to your account
  - Check that the survey has the "Active" status if trying to collect responses

**Empty response exports**:
- **Cause**: Survey has no responses yet, or responses not yet available
- **Solution**:
  - Verify survey has collected responses in Qualtrics UI
  - Wait 1-2 minutes after response submission for export availability
  - Check survey distribution settings

**Rate Limiting (`429 Too Many Requests`)**:
- **Cause**: Exceeded 500 requests/minute or 20 concurrent requests
- **Solution**:
  - The connector automatically retries with backoff
  - Reduce pipeline concurrency if running multiple surveys in parallel
  - Widen schedule intervals
  - Contact Qualtrics support for rate limit increases if needed

**Export timeout errors**:
- **Cause**: Large survey export taking longer than expected
- **Solution**:
  - This is normal for surveys with >10,000 responses
  - The connector waits up to 5-10 minutes; most exports complete within this time
  - Consider breaking very large surveys into smaller date ranges if possible

**Datacenter ID errors**:
- **Cause**: Incorrect `datacenter_id` parameter
- **Solution**:
  - Verify your datacenter ID from Qualtrics URL or Account Settings
  - Common IDs: `fra1` (Europe), `ca1` (Canada), `sjc1` (US West), `au1` (Australia)
  - Use your organization's custom datacenter ID if applicable

**Schema or parsing errors**:
- **Cause**: Unexpected response format from Qualtrics API
- **Solution**:
  - The connector handles standard Qualtrics response formats
  - Check Databricks logs for specific parsing errors
  - Verify survey questions use standard question types
  - Some advanced question types may require custom handling

## References

- **Connector Implementation**: `sources/qualtrics/qualtrics.py`
- **API Documentation**: `sources/qualtrics/qualtrics_api_doc.md`
- **Official Qualtrics API Documentation**:
  - Main API Reference: https://api.qualtrics.com/
  - Getting Started: https://www.qualtrics.com/support/integrations/api-integration/overview/
  - API Best Practices: https://www.qualtrics.com/support/integrations/api-integration/overview/
  - Response Exports: https://api.qualtrics.com/guides/docs/Instructions/response-exports.md
- **Qualtrics Support**:
  - Developer Portal: https://www.qualtrics.com/support/integrations/developer-portal/
  - Community Forums: https://community.qualtrics.com/

---

## Additional Notes

### Test Environment Setup

For development and testing:
1. Create a test survey with 2-3 simple questions (text entry, multiple choice)
2. Collect 1-2 test responses
3. Use this survey for initial pipeline validation
4. Graduate to production surveys once validated

### Performance Considerations

- **Surveys table**: Lightweight API calls, fast retrieval
- **Survey responses**: Heavy operations due to export workflow
  - Small surveys (<1000 responses): ~30-60 seconds
  - Medium surveys (1000-10,000 responses): ~1-3 minutes
  - Large surveys (>10,000 responses): ~3-10 minutes
- Plan pipeline schedules accordingly based on survey sizes

### Data Retention

- Survey metadata persists indefinitely in Qualtrics
- Survey responses persist according to your Qualtrics plan
- Exported data in Databricks persists according to your retention policies
- Incremental sync maintains cursor state automatically

---

**Version**: 1.2.0
**Last Updated**: January 2026
**Connector Status**: Production-ready ✅

**New in v1.2**:
- Survey definitions now support CDC mode with SCD Type 2 for tracking historical changes to survey structure
- Only fetch definitions modified since last sync, improving efficiency

**New in v1.1**:
- Auto-consolidation feature - automatically fetch data from all surveys without manually specifying surveyId

