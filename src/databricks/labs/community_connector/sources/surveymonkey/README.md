# Lakeflow SurveyMonkey Community Connector

This documentation describes how to configure and use the **SurveyMonkey** Lakeflow community connector to ingest survey data from the SurveyMonkey API v3 into Databricks.


## Prerequisites

- **SurveyMonkey account**: You need a SurveyMonkey account with access to the surveys and data you want to ingest.
- **OAuth 2.0 Access Token**:
  - Must be obtained via SurveyMonkey's OAuth flow and supplied to the connector as the `access_token` option.
  - Required scopes depend on what data you need:
    - `View Surveys`: Read survey metadata
    - `View Responses`: Read survey responses
    - `View Collectors`: Read collector information
    - `View Contacts`: Read contact lists
    - `View Users`: Read user information
- **Network access**: The environment running the connector must be able to reach `https://api.surveymonkey.com` (US) or `https://api.eu.surveymonkey.com` (EU).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.


## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name           | Type   | Required | Description                                                                                                    | Example                                  |
|----------------|--------|----------|----------------------------------------------------------------------------------------------------------------|------------------------------------------|
| `access_token` | string | yes      | SurveyMonkey OAuth 2.0 access token for authentication.                                                        | `SjM5Y...xxxxx`                          |
| `base_url`     | string | no       | Base URL for the SurveyMonkey API. Defaults to `https://api.surveymonkey.com/v3`. Use `https://api.eu.surveymonkey.com/v3` for EU data center. | `https://api.surveymonkey.com/v3` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to be passed to the connector. Required for child tables. | `survey_id,page_id,group_id` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`survey_id,page_id,group_id`

> **Note**: Table-specific options such as `survey_id`, `page_id`, and `group_id` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.


### Obtaining the Access Token

SurveyMonkey uses OAuth 2.0 for authentication. To obtain an access token:

1. **Register an App**:
   - Sign in to SurveyMonkey.
   - Navigate to **Developer Portal** → **My Apps**.
   - Create a new app or use an existing one.
   - Note the `Client ID` and `Client Secret`.

2. **Configure OAuth Scopes**:
   - Enable the required scopes for your app:
     - `View Surveys` for survey metadata
     - `View Responses` for survey responses
     - `View Collectors` for collectors
     - `View Contacts` for contacts
     - `View Users` for user information

3. **Authorize and Obtain Token**:
   - Direct users to the authorization URL:
     ```
     https://api.surveymonkey.com/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&response_type=code
     ```
   - After user authorization, exchange the code for an access token:
     ```bash
     curl -X POST https://api.surveymonkey.com/oauth/token \
       -H "Content-Type: application/x-www-form-urlencoded" \
       -d "client_id={client_id}&client_secret={client_secret}&code={code}&redirect_uri={redirect_uri}&grant_type=authorization_code"
     ```
   - Store the returned `access_token` securely and use it as the connection option.

4. **EU Data Center**:
   - If your SurveyMonkey account is hosted in the EU data center, set `base_url` to `https://api.eu.surveymonkey.com/v3`.


### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `survey_id,page_id,group_id` (required to pass table-specific options for child tables).

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The SurveyMonkey connector exposes a **static list** of tables:

- `surveys`
- `survey_responses`
- `survey_pages`
- `survey_questions`
- `collectors`
- `contact_lists`
- `contacts`
- `users`
- `groups`
- `group_members`
- `workgroups`
- `survey_folders`
- `survey_categories`
- `survey_templates`
- `survey_languages`
- `webhooks`
- `survey_rollups`
- `benchmark_bundles`


### Object Summary, Primary Keys, and Ingestion Mode

| Table                | Description                                      | Ingestion Type | Primary Key | Incremental Cursor |
|----------------------|--------------------------------------------------|----------------|-------------|---------------------|
| `surveys`            | Survey metadata and configuration                | `cdc`          | `id`        | `date_modified`     |
| `survey_responses`   | Individual responses to a survey                 | `cdc`          | `id`        | `date_modified`     |
| `survey_pages`       | Pages within a survey                            | `snapshot`     | `id`        | N/A                 |
| `survey_questions`   | Questions within survey pages                    | `snapshot`     | `id`        | N/A                 |
| `collectors`         | Collection channels for surveys                  | `cdc`          | `id`        | `date_modified`     |
| `contact_lists`      | Contact list metadata                            | `snapshot`     | `id`        | N/A                 |
| `contacts`           | Individual contacts                              | `snapshot`     | `id`        | N/A                 |
| `users`              | Current authenticated user information           | `snapshot`     | `id`        | N/A                 |
| `groups`             | Team/group information                           | `snapshot`     | `id`        | N/A                 |
| `group_members`      | Members within a group                           | `snapshot`     | `id`        | N/A                 |
| `workgroups`         | Workgroup configuration and membership           | `snapshot`     | `id`        | N/A                 |
| `survey_folders`     | Folder structure for organizing surveys          | `snapshot`     | `id`        | N/A                 |
| `survey_categories`  | Available survey categories                      | `snapshot`     | `id`        | N/A                 |
| `survey_templates`   | Available survey templates                       | `snapshot`     | `id`        | N/A                 |
| `survey_languages`   | Supported survey languages                       | `snapshot`     | `id`        | N/A                 |
| `webhooks`           | Configured webhooks                              | `snapshot`     | `id`        | N/A                 |
| `survey_rollups`     | Aggregated survey response statistics            | `snapshot`     | `id`        | N/A                 |
| `benchmark_bundles`  | Benchmark data bundles for comparison            | `snapshot`     | `id`        | N/A                 |


## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

Some tables accept additional configuration via table options:

| Table                | Required Options     | Optional Options       | Notes |
|----------------------|---------------------|------------------------|-------|
| `surveys`            | None                | None                   | Lists all surveys accessible to the authenticated user. |
| `survey_responses`   | None                | `survey_id`            | If `survey_id` is provided, reads responses for that survey only. If omitted, iterates through all surveys and combines responses. |
| `survey_pages`       | None                | `survey_id`            | If `survey_id` is provided, reads pages for that survey only. If omitted, iterates through all surveys. |
| `survey_questions`   | None                | `survey_id`, `page_id` | If both are provided, reads questions for that specific page. If only `survey_id` is provided, iterates through all pages of that survey. If neither is provided, iterates through all surveys and their pages. |
| `collectors`         | None                | `survey_id`            | If `survey_id` is provided, reads collectors for that survey only. If omitted, iterates through all surveys. |
| `contact_lists`      | None                | None                   | Lists all contact lists. |
| `contacts`           | None                | None                   | Lists all contacts. |
| `users`              | None                | None                   | Returns the authenticated user's information. |
| `groups`             | None                | None                   | Lists all groups in the account. |
| `group_members`      | None                | `group_id`             | If `group_id` is provided, reads members for that group only. If omitted, iterates through all groups. |
| `workgroups`         | None                | None                   | Lists all workgroups. |
| `survey_folders`     | None                | None                   | Lists all survey folders. |
| `survey_categories`  | None                | None                   | Lists available survey categories. |
| `survey_templates`   | None                | None                   | Lists available survey templates. |
| `survey_languages`   | None                | None                   | Lists supported survey languages. |
| `webhooks`           | None                | None                   | Lists configured webhooks. |
| `survey_rollups`     | None                | `survey_id`            | If `survey_id` is provided, reads rollups for that survey only. If omitted, iterates through all surveys. |
| `benchmark_bundles`  | None                | None                   | Lists available benchmark bundles. |


### Object Hierarchy Notes

- `survey_responses`, `survey_pages`, `survey_questions`, `collectors`, and `survey_rollups` are child objects of `surveys`.
- `group_members` is a child object of `groups`.
- When `survey_id` is not provided for child tables, the connector will:
  1. List all parent surveys.
  2. For each survey, fetch the child objects.
  3. Combine results into a single output with `survey_id` added to each record.
- For `survey_questions`, when `page_id` is also omitted, the connector iterates through all pages of each survey.
- When `group_id` is not provided for `group_members`, the connector iterates through all groups.


### Schema Highlights

- **`surveys`**: Contains survey metadata including `title`, `nickname`, `language`, `question_count`, `page_count`, `response_count`, and `date_modified`. Includes nested `buttons_text` struct for custom button labels.

- **`survey_responses`**: Contains response data with nested structures:
  - `pages`: Array of page objects, each containing `questions` array
  - `metadata`: Struct with optional `contact` information
  - `logic_path`: Struct tracking response path
  - Each question in `pages.questions` contains an `answers` array with choice selections

- **`survey_pages`**: Contains page metadata including `title`, `description`, `position`, and `question_count`.

- **`survey_questions`**: Contains question definitions with:
  - `family` and `subtype` indicating question type (e.g., `single_choice`, `multiple_choice`, `open_ended`, `matrix`)
  - `answers` struct containing available choices, rows, and columns
  - `validation` and `required` structs for question rules

- **`collectors`**: Contains collector configuration including `name`, `type`, `status`, `url`, and response tracking fields.


## Data Type Mapping

SurveyMonkey JSON fields are mapped to Spark types as follows:

| SurveyMonkey JSON Type   | Example Fields                                          | Spark Type           | Notes |
|--------------------------|--------------------------------------------------------|----------------------|-------|
| integer                  | `question_count`, `page_count`, `response_count`, `position` | `LongType`           | All integers stored as 64-bit to avoid overflow. |
| string                   | `id`, `title`, `nickname`, `status`, `href`            | `StringType`         | Identifiers and text fields. |
| boolean                  | `is_owner`, `footer`, `visible`, `forced_ranking`      | `BooleanType`        | Standard `true`/`false` values. |
| ISO 8601 datetime        | `date_created`, `date_modified`                        | `StringType`         | Stored as UTC strings; downstream processing can cast to timestamp. |
| object                   | `buttons_text`, `metadata`, `answers`, `validation`    | `StructType`         | Nested objects preserved as structs. |
| array                    | `pages`, `questions`, `choices`                        | `ArrayType`          | Arrays preserved as nested collections. |
| map                      | `custom_variables`                                     | `MapType<String, String>` | Key-value pairs preserved as maps. |
| nullable fields          | `folder_id`, `category`, `description`                 | Same type + `null`   | Missing or null values surfaced as `null`, not `{}`. |

The connector:
- Prefers `LongType` for all numeric fields.
- Preserves nested JSON structures instead of flattening.
- Converts empty dictionaries `{}` from the API to `null` to conform to schema expectations.


## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the SurveyMonkey connector source in your workspace. This will typically place the connector code (for example, `surveymonkey.py`) under a project path that Lakeflow can load.


### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingestion_pipeline.py`), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this SurveyMonkey connector.
- One or more **tables** to ingest, each with optional `table_configuration`.

**Example: Ingest all surveys and their responses**

```json
{
  "pipeline_spec": {
    "connection_name": "surveymonkey_connection",
    "object": [
      {
        "table": {
          "source_table": "surveys"
        }
      },
      {
        "table": {
          "source_table": "survey_responses"
        }
      },
      {
        "table": {
          "source_table": "users"
        }
      }
    ]
  }
}
```

**Example: Ingest responses for a specific survey**

```json
{
  "pipeline_spec": {
    "connection_name": "surveymonkey_connection",
    "object": [
      {
        "table": {
          "source_table": "survey_responses",
          "table_configuration": {
            "survey_id": "421006327"
          }
        }
      },
      {
        "table": {
          "source_table": "survey_pages",
          "table_configuration": {
            "survey_id": "421006327"
          }
        }
      },
      {
        "table": {
          "source_table": "survey_questions",
          "table_configuration": {
            "survey_id": "421006327"
          }
        }
      }
    ]
  }
}
```

**Example: Ingest members for a specific group**

```json
{
  "pipeline_spec": {
    "connection_name": "surveymonkey_connection",
    "object": [
      {
        "table": {
          "source_table": "groups"
        }
      },
      {
        "table": {
          "source_table": "group_members",
          "table_configuration": {
            "group_id": "12345"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your SurveyMonkey `access_token`.
- For each `table`:
  - `source_table` must be one of the supported table names listed above.
  - Custom table options like `survey_id`, `page_id`, and `group_id` must be placed under `table_configuration`.


### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

For incremental tables (`surveys`, `survey_responses`, `collectors`):
- The connector uses `date_modified` as the cursor for CDC ingestion.
- On subsequent runs, only records modified after the last sync are fetched.


#### Best Practices

- **Start small**:
  - Begin with `surveys` and `users` to validate connection and data shape.
  - Then add `survey_responses` for a single survey using `survey_id` before ingesting all responses.

- **Use incremental sync where possible**:
  - For `surveys`, `survey_responses`, and `collectors`, rely on the `cdc` pattern with `date_modified` to minimize API calls.

- **Specify survey_id for large accounts**:
  - If you have many surveys, specifying `survey_id` for child tables avoids iterating through all surveys on each sync.

- **Respect rate limits**:
  - SurveyMonkey enforces rate limits (120 requests/minute for most apps).
  - The connector automatically handles rate limiting with exponential backoff.
  - Consider staggering syncs if you have multiple pipelines.


#### Troubleshooting

**Common Issues:**

- **Authentication failures (`401`)**:
  - Verify that the `access_token` is correct and not expired.
  - Ensure the token has the required scopes for the data being accessed.

- **`403 Forbidden`**:
  - The token may not have permission to access certain resources.
  - Check that the user who authorized the app has access to the surveys/data.

- **Rate limiting (`429`)**:
  - The connector retries automatically on rate limit errors.
  - If persistent, reduce concurrency or schedule syncs during off-peak hours.
  - Check the `X-Ratelimit-App-Global-Minute-Remaining` header in logs.

- **Empty responses for child tables**:
  - Verify the `survey_id` or `page_id` is correct.
  - Check that the survey has responses/pages/questions.

- **Schema mismatches downstream**:
  - The connector uses nested structs extensively.
  - Ensure downstream tables accept nested types, or cast/flatten as needed in transformations.


## References

- Connector implementation: `src/databricks/labs/community_connector/sources/surveymonkey/surveymonkey.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/surveymonkey/surveymonkey_api_doc.md`
- Official SurveyMonkey API documentation:
  - `https://developer.surveymonkey.com/api/v3/`
  - `https://developer.surveymonkey.com/api/v3/#surveys`
  - `https://developer.surveymonkey.com/api/v3/#survey-responses`
  - `https://developer.surveymonkey.com/api/v3/#collectors`
  - `https://developer.surveymonkey.com/api/v3/#contacts-and-contact-lists`

