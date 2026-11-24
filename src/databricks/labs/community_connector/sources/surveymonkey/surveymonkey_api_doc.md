# **SurveyMonkey API Documentation**

## **Authorization**

- **Chosen method**: OAuth 2.0 Bearer Token
- **Base URL**: `https://api.surveymonkey.com/v3`
- **EU Data Center Base URL**: `https://api.eu.surveymonkey.com/v3`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <access_token>`
  - Required scopes for reading data:
    - `View Surveys`: Read survey metadata
    - `View Responses`: Read survey responses
    - `View Collectors`: Read collector information
    - `View Contacts`: Read contact lists
    - `View Users`: Read user information
    - `View Teams`: Read team/group information
    - `View Workgroups`: Read workgroup information
    - `View Webhooks`: Read webhook configurations

**OAuth note**: The connector **stores** `access_token` (long-lived) and uses it directly for API calls. For apps that need token refresh, store `client_id`, `client_secret`, and `refresh_token` to exchange for access tokens at runtime. The connector **does not** run user-facing OAuth flows.

**Token acquisition flow** (performed outside the connector):
1. Direct user to authorization page: `https://api.surveymonkey.com/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&response_type=code`
2. User authorizes and receives a short-lived code at redirect URI
3. Exchange code for long-lived access token via `POST https://api.surveymonkey.com/oauth/token`

**Example authenticated request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/surveys?per_page=50"
```

**Rate Limits**:
- Draft apps: 120 requests per minute
- Private apps: 120 requests per minute (upgradable)
- Public apps: 500,000 requests per day

**Headers in Response** (useful for rate limit tracking):
- `X-Ratelimit-App-Global-Day-Limit`: Daily limit
- `X-Ratelimit-App-Global-Day-Remaining`: Remaining requests today
- `X-Ratelimit-App-Global-Day-Reset`: Seconds until daily limit resets
- `X-Ratelimit-App-Global-Minute-Limit`: Per-minute limit
- `X-Ratelimit-App-Global-Minute-Remaining`: Remaining requests this minute
- `X-Ratelimit-App-Global-Minute-Reset`: Seconds until minute limit resets


## **Object List**

For connector purposes, we treat specific SurveyMonkey REST resources as **objects/tables**.
The object list is **static** (defined by the connector), not discovered dynamically from an API.

| Object Name | Description | Primary Endpoint | Parent Object | Ingestion Type |
|------------|-------------|------------------|---------------|----------------|
| `surveys` | Survey metadata and configuration | `GET /surveys` | None | `cdc` (upserts based on `date_modified`) |
| `survey_responses` | Individual responses to a survey | `GET /surveys/{survey_id}/responses/bulk` | `surveys` | `cdc` (upserts based on `date_modified`) |
| `survey_pages` | Pages within a survey | `GET /surveys/{survey_id}/pages` | `surveys` | `snapshot` |
| `survey_questions` | Questions within survey pages | `GET /surveys/{survey_id}/pages/{page_id}/questions` | `survey_pages` | `snapshot` |
| `collectors` | Collection channels for surveys | `GET /surveys/{survey_id}/collectors` | `surveys` | `cdc` (based on `date_modified`) |
| `contact_lists` | Contact list metadata | `GET /contact_lists` | None | `snapshot` |
| `contacts` | Individual contacts | `GET /contacts` or `GET /contact_lists/{id}/contacts` | `contact_lists` (optional) | `snapshot` |
| `users` | Current user information | `GET /users/me` | None | `snapshot` |
| `groups` | Team/group information | `GET /groups` | None | `snapshot` |
| `group_members` | Members within a group/team | `GET /groups/{group_id}/members` | `groups` | `snapshot` |
| `workgroups` | Workgroup information | `GET /workgroups` | None | `snapshot` |
| `survey_folders` | Folders organizing surveys | `GET /survey_folders` | None | `snapshot` |
| `survey_categories` | Available survey categories | `GET /survey_categories` | None | `snapshot` |
| `survey_templates` | Available survey templates | `GET /survey_templates` | None | `snapshot` |
| `survey_languages` | Supported survey languages | `GET /survey_languages` | None | `snapshot` |
| `webhooks` | Webhook configurations | `GET /webhooks` | None | `snapshot` |
| `survey_rollups` | Aggregated response statistics | `GET /surveys/{survey_id}/rollups` | `surveys` | `snapshot` |
| `benchmark_bundles` | Benchmark data bundles | `GET /benchmark_bundles` | None | `snapshot` |

**Object hierarchy notes**:
- `survey_responses` are children of `surveys` - require `survey_id` parameter
- `survey_pages` are children of `surveys` - require `survey_id` parameter
- `survey_questions` are children of `survey_pages` - require both `survey_id` and `page_id` parameters
- `collectors` are children of `surveys` - require `survey_id` parameter
- `group_members` are children of `groups` - require `group_id` parameter
- `contacts` can be listed globally or under a specific `contact_list`
- `survey_rollups` are children of `surveys` - require `survey_id` parameter


## **Object Schema**

### General notes

- SurveyMonkey returns JSON responses with nested objects.
- For the connector, we define **tabular schemas** per object, derived from the JSON representation.
- Nested JSON objects are modeled as **nested structures/arrays** rather than being fully flattened.

### `surveys` object (primary table)

**Source endpoint**:
`GET /surveys` (list) and `GET /surveys/{id}` (single survey)

**Key behavior**:
- List endpoint returns minimal survey metadata
- Use `GET /surveys/{id}/details` for full survey structure including pages and questions
- Supports filtering by folder, title, and time range

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique identifier for the survey |
| `title` | string | Survey title |
| `nickname` | string | Survey nickname (internal name) |
| `href` | string | API resource URL for this survey |
| `folder_id` | string or null | ID of the folder containing this survey |
| `category` | string or null | Survey category |
| `language` | string | Language code (e.g., "en") |
| `question_count` | integer | Number of questions in the survey |
| `page_count` | integer | Number of pages in the survey |
| `date_created` | string (ISO 8601 datetime) | When the survey was created |
| `date_modified` | string (ISO 8601 datetime) | Last modification time (used as cursor) |
| `response_count` | integer | Number of responses received |
| `buttons_text` | struct | Custom button text configuration |
| `is_owner` | boolean | Whether the authenticated user owns this survey |
| `footer` | boolean | Whether footer is enabled |
| `custom_variables` | map<string, string> or null | Custom variables defined for the survey |
| `preview` | string | Preview URL for the survey |
| `edit_url` | string | Edit URL for the survey |
| `collect_url` | string | URL to create a new collector |
| `analyze_url` | string | URL to view analysis |
| `summary_url` | string | URL to view summary |

**Nested `buttons_text` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `next_button` | string | Text for "Next" button |
| `prev_button` | string | Text for "Previous" button |
| `done_button` | string | Text for "Done" button |
| `exit_button` | string | Text for "Exit" button |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/surveys?per_page=50"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "1234567890",
      "title": "Customer Satisfaction Survey",
      "nickname": "Q4 CSAT",
      "href": "https://api.surveymonkey.com/v3/surveys/1234567890"
    }
  ],
  "per_page": 50,
  "page": 1,
  "total": 1,
  "links": {
    "self": "https://api.surveymonkey.com/v3/surveys?page=1&per_page=50"
  }
}
```

**Detailed survey response** (`GET /surveys/{id}`):

```json
{
  "id": "1234567890",
  "title": "Customer Satisfaction Survey",
  "nickname": "Q4 CSAT",
  "language": "en",
  "folder_id": "0",
  "category": "",
  "question_count": 10,
  "page_count": 2,
  "date_created": "2024-01-15T10:30:00+00:00",
  "date_modified": "2024-03-20T14:45:00+00:00",
  "response_count": 150,
  "buttons_text": {
    "next_button": "Next",
    "prev_button": "Previous", 
    "done_button": "Done",
    "exit_button": "Exit"
  },
  "is_owner": true,
  "footer": true,
  "custom_variables": {},
  "href": "https://api.surveymonkey.com/v3/surveys/1234567890",
  "preview": "https://www.surveymonkey.com/r/Preview/?sm=xxxxx",
  "edit_url": "https://www.surveymonkey.com/create/?sm=xxxxx",
  "collect_url": "https://www.surveymonkey.com/collect/list?sm=xxxxx",
  "analyze_url": "https://www.surveymonkey.com/analyze/?sm=xxxxx",
  "summary_url": "https://www.surveymonkey.com/summary/?sm=xxxxx"
}
```


### `survey_responses` object (child of surveys)

**Source endpoint**:
- `GET /surveys/{survey_id}/responses` - List response IDs (minimal)
- `GET /surveys/{survey_id}/responses/bulk` - Bulk fetch responses with details (recommended)
- `GET /surveys/{survey_id}/responses/{response_id}/details` - Single response with full details

**Key behavior**:
- The `/responses/bulk` endpoint returns complete response data including answers
- Supports filtering by date range, status, and collector
- Pagination via `page` and `per_page` parameters

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique response identifier |
| `survey_id` | string (connector-derived) | ID of the survey this response belongs to |
| `recipient_id` | string or null | ID of the contact who received the survey |
| `collector_id` | string | ID of the collector through which response was submitted |
| `custom_value` | string or null | Custom value passed with the response |
| `edit_url` | string | URL to edit the response |
| `analyze_url` | string | URL to analyze the response |
| `total_time` | integer | Total time spent on survey in seconds |
| `date_created` | string (ISO 8601 datetime) | When the response was started |
| `date_modified` | string (ISO 8601 datetime) | Last modification time (used as cursor) |
| `response_status` | string | Status: `completed`, `partial`, `overquota`, `disqualified` |
| `collection_mode` | string | How response was collected: `default`, `preview`, `data_entry`, `survey_preview`, `edit` |
| `ip_address` | string or null | IP address of respondent (if collected) |
| `logic_path` | struct or null | Survey logic path taken |
| `metadata` | struct | Response metadata |
| `page_path` | array<string> | List of page IDs visited |
| `pages` | array<struct> | Array of pages with questions and answers |

**Nested `metadata` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `contact` | struct or null | Contact information if available |
| `contact.first_name` | string or null | Contact's first name |
| `contact.last_name` | string or null | Contact's last name |
| `contact.email` | string or null | Contact's email address |

**Nested `pages` array element struct**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Page ID |
| `questions` | array<struct> | Array of questions with answers |

**Nested `questions` array element struct** (within pages):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Question ID |
| `variable_id` | string or null | Variable ID for the question |
| `answers` | array<struct> | Array of answer objects |

**Nested `answers` array element struct**:

| Field | Type | Description |
|-------|------|-------------|
| `choice_id` | string or null | Selected choice ID (for choice-based questions) |
| `choice_metadata` | struct or null | Metadata about the choice |
| `row_id` | string or null | Row ID (for matrix questions) |
| `col_id` | string or null | Column ID (for matrix questions) |
| `other_id` | string or null | Other option ID |
| `text` | string or null | Text response (for open-ended or "other" answers) |
| `download_url` | string or null | File download URL (for file upload questions) |
| `content_type` | string or null | File content type |
| `tag_data` | array<struct> or null | Tag data for the answer |

**Example request** (bulk responses):

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/surveys/1234567890/responses/bulk?per_page=100"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "9876543210",
      "recipient_id": "",
      "collector_id": "123456789",
      "custom_value": "",
      "edit_url": "https://www.surveymonkey.com/r/?sm=xxxxx",
      "analyze_url": "https://www.surveymonkey.com/analyze/browse/xxxxx",
      "total_time": 120,
      "date_created": "2024-03-15T09:00:00+00:00",
      "date_modified": "2024-03-15T09:02:00+00:00",
      "response_status": "completed",
      "collection_mode": "default",
      "ip_address": "192.168.1.1",
      "page_path": ["12345", "12346"],
      "metadata": {
        "contact": {
          "first_name": "John",
          "last_name": "Doe",
          "email": "john.doe@example.com"
        }
      },
      "pages": [
        {
          "id": "12345",
          "questions": [
            {
              "id": "111111",
              "variable_id": "q1",
              "answers": [
                {
                  "choice_id": "222222",
                  "choice_metadata": {
                    "weight": 5
                  }
                }
              ]
            }
          ]
        }
      ]
    }
  ],
  "per_page": 100,
  "page": 1,
  "total": 150,
  "links": {
    "self": "https://api.surveymonkey.com/v3/surveys/1234567890/responses/bulk?page=1&per_page=100",
    "next": "https://api.surveymonkey.com/v3/surveys/1234567890/responses/bulk?page=2&per_page=100"
  }
}
```


### `survey_pages` object

**Source endpoint**:
`GET /surveys/{survey_id}/pages`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique page identifier |
| `survey_id` | string (connector-derived) | Survey ID |
| `title` | string | Page title |
| `description` | string or null | Page description |
| `position` | integer | Page position (1-indexed) |
| `question_count` | integer | Number of questions on the page |
| `href` | string | API resource URL |


### `survey_questions` object

**Source endpoint**:
`GET /surveys/{survey_id}/pages/{page_id}/questions`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique question identifier |
| `survey_id` | string (connector-derived) | Survey ID |
| `page_id` | string (connector-derived) | Page ID |
| `position` | integer | Question position within the page |
| `visible` | boolean | Whether question is visible |
| `family` | string | Question family (e.g., `single_choice`, `multiple_choice`, `open_ended`, `matrix`, `demographic`, `datetime`, `presentation`) |
| `subtype` | string | Question subtype (varies by family) |
| `heading` | string | Question text/heading |
| `href` | string | API resource URL |
| `sorting` | struct or null | Sorting configuration |
| `required` | struct or null | Required answer configuration |
| `validation` | struct or null | Validation rules |
| `forced_ranking` | boolean | For matrix questions, whether forced ranking is enabled |
| `answers` | struct | Available answer options (choices, rows, cols) |

**Nested `answers` struct** (varies by question type):

| Field | Type | Description |
|-------|------|-------------|
| `choices` | array<struct> or null | Available choices for single/multiple choice questions |
| `rows` | array<struct> or null | Row options for matrix questions |
| `cols` | array<struct> or null | Column options for matrix questions |
| `other` | struct or null | "Other" option configuration |

**Nested `choices` array element**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Choice ID |
| `position` | integer | Position in list |
| `text` | string | Choice text |
| `visible` | boolean | Whether choice is visible |
| `is_na` | boolean | Whether this is an N/A option |
| `weight` | integer or null | Numeric weight for scoring |


### `collectors` object

**Source endpoint**:
`GET /surveys/{survey_id}/collectors`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique collector identifier |
| `survey_id` | string (connector-derived) | Survey ID |
| `name` | string | Collector name |
| `type` | string | Collector type: `weblink`, `email`, `popup`, `embedded`, `facebook` |
| `status` | string | Status: `open`, `closed`, `new` |
| `redirect_url` | string or null | URL to redirect after completion |
| `thank_you_page` | struct or null | Thank you page configuration |
| `response_count` | integer | Number of responses via this collector |
| `date_created` | string (ISO 8601 datetime) | Creation time |
| `date_modified` | string (ISO 8601 datetime) | Last modification time |
| `url` | string or null | Collector URL (for weblink type) |
| `href` | string | API resource URL |


### `contact_lists` object

**Source endpoint**:
`GET /contact_lists`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique contact list identifier |
| `name` | string | Contact list name |
| `href` | string | API resource URL |


### `contacts` object

**Source endpoint**:
`GET /contacts` or `GET /contact_lists/{contact_list_id}/contacts`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique contact identifier |
| `email` | string | Contact email address |
| `first_name` | string or null | First name |
| `last_name` | string or null | Last name |
| `custom_fields` | map<string, string> or null | Custom field values |
| `href` | string | API resource URL |


### `users` object

**Source endpoint**:
`GET /users/me`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique user identifier |
| `username` | string | Username |
| `first_name` | string or null | First name |
| `last_name` | string or null | Last name |
| `email` | string | Email address |
| `email_verified` | boolean | Whether email is verified |
| `account_type` | string | Account type (e.g., `basic`, `standard`, `advantage`, `premier`, `enterprise`) |
| `language` | string | Preferred language |
| `date_created` | string (ISO 8601 datetime) | Account creation date |
| `date_last_login` | string (ISO 8601 datetime) | Last login time |
| `question_types` | struct or null | Available question types for the user's plan |
| `scopes` | struct or null | Available and granted OAuth scopes |
| `sso_connections` | array<string> or null | SSO connection identifiers |
| `features` | struct or null | Feature flags enabled for the account |
| `href` | string | API resource URL |

**Example response** (`GET /users/me`):

```json
{
  "id": "1234567890",
  "username": "user@example.com",
  "first_name": "John",
  "last_name": "Doe",
  "email": "user@example.com",
  "email_verified": true,
  "account_type": "enterprise",
  "language": "en",
  "date_created": "2020-01-15T10:00:00+00:00",
  "date_last_login": "2024-03-20T14:00:00+00:00",
  "href": "https://api.surveymonkey.com/v3/users/me"
}
```


### `groups` object

**Source endpoint**:
`GET /groups`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique group identifier |
| `name` | string | Group/team name |
| `member_count` | integer | Number of members in the group |
| `max_invites` | integer | Maximum allowed invites |
| `date_created` | string (ISO 8601 datetime) | When the group was created |
| `href` | string | API resource URL |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/groups"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "1234567890",
      "name": "Marketing Team",
      "member_count": 15,
      "max_invites": 50,
      "date_created": "2023-01-10T09:00:00+00:00",
      "href": "https://api.surveymonkey.com/v3/groups/1234567890"
    }
  ],
  "per_page": 50,
  "page": 1,
  "total": 1,
  "links": {
    "self": "https://api.surveymonkey.com/v3/groups?page=1&per_page=50"
  }
}
```


### `group_members` object

**Source endpoint**:
`GET /groups/{group_id}/members`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique member identifier |
| `group_id` | string (connector-derived) | Group ID |
| `username` | string | Member's username |
| `email` | string | Member's email address |
| `type` | string | Member type: `regular`, `admin` |
| `status` | string | Member status: `active`, `pending` |
| `date_created` | string (ISO 8601 datetime) | When the member was added |
| `href` | string | API resource URL |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/groups/1234567890/members"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "9876543210",
      "username": "member@example.com",
      "email": "member@example.com",
      "type": "regular",
      "status": "active",
      "date_created": "2023-02-15T10:00:00+00:00",
      "href": "https://api.surveymonkey.com/v3/groups/1234567890/members/9876543210"
    }
  ],
  "per_page": 50,
  "page": 1,
  "total": 15,
  "links": {
    "self": "https://api.surveymonkey.com/v3/groups/1234567890/members?page=1&per_page=50"
  }
}
```


### `workgroups` object

**Source endpoint**:
`GET /workgroups`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique workgroup identifier |
| `name` | string | Workgroup name |
| `description` | string or null | Workgroup description |
| `is_visible` | boolean | Whether the workgroup is visible |
| `metadata` | struct or null | Additional metadata |
| `created_at` | string (ISO 8601 datetime) | Creation timestamp |
| `updated_at` | string (ISO 8601 datetime) | Last update timestamp |
| `members` | array<struct> or null | List of workgroup members |
| `members_count` | integer | Number of members |
| `shares_count` | integer | Number of shared resources |
| `default_role` | struct or null | Default role configuration |
| `organization_id` | string | Organization identifier |
| `href` | string | API resource URL |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/workgroups"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "abc123",
      "name": "Product Research",
      "description": "Workgroup for product research surveys",
      "is_visible": true,
      "created_at": "2023-06-01T12:00:00+00:00",
      "updated_at": "2024-01-15T09:30:00+00:00",
      "members_count": 8,
      "shares_count": 12,
      "organization_id": "org123",
      "href": "https://api.surveymonkey.com/v3/workgroups/abc123"
    }
  ],
  "per_page": 50,
  "page": 1,
  "total": 3,
  "links": {
    "self": "https://api.surveymonkey.com/v3/workgroups?page=1&per_page=50"
  }
}
```


### `survey_folders` object

**Source endpoint**:
`GET /survey_folders`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique folder identifier |
| `title` | string | Folder title/name |
| `num_surveys` | integer | Number of surveys in the folder |
| `href` | string | API resource URL |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/survey_folders"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "folder123",
      "title": "Customer Feedback",
      "num_surveys": 25,
      "href": "https://api.surveymonkey.com/v3/survey_folders/folder123"
    }
  ],
  "per_page": 50,
  "page": 1,
  "total": 5,
  "links": {
    "self": "https://api.surveymonkey.com/v3/survey_folders?page=1&per_page=50"
  }
}
```


### `survey_categories` object

**Source endpoint**:
`GET /survey_categories`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique category identifier |
| `name` | string | Category display name |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/survey_categories"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "community",
      "name": "Community"
    },
    {
      "id": "customer_feedback",
      "name": "Customer Feedback"
    },
    {
      "id": "education",
      "name": "Education"
    },
    {
      "id": "events",
      "name": "Events"
    },
    {
      "id": "healthcare",
      "name": "Healthcare"
    },
    {
      "id": "human_resources",
      "name": "Human Resources"
    },
    {
      "id": "marketing",
      "name": "Marketing"
    },
    {
      "id": "non_profit",
      "name": "Non-Profit"
    }
  ],
  "per_page": 1000,
  "page": 1,
  "total": 8,
  "links": {
    "self": "https://api.surveymonkey.com/v3/survey_categories?page=1&per_page=1000"
  }
}
```


### `survey_templates` object

**Source endpoint**:
`GET /survey_templates`

**Query parameters**:
- `page`: Page number (default: 1)
- `per_page`: Results per page (default: 50)
- `language`: Filter by language code
- `category`: Filter by category ID

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique template identifier |
| `name` | string | Template name |
| `title` | string | Template title |
| `description` | string | Template description |
| `category` | string | Category the template belongs to |
| `available` | boolean | Whether the template is available for use |
| `num_questions` | integer | Number of questions in the template |
| `preview_link` | string | URL to preview the template |
| `href` | string | API resource URL |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/survey_templates?category=customer_feedback"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "template123",
      "name": "Customer Satisfaction (CSAT)",
      "title": "Customer Satisfaction Survey",
      "description": "Measure how satisfied customers are with your products or services",
      "category": "customer_feedback",
      "available": true,
      "num_questions": 10,
      "preview_link": "https://www.surveymonkey.com/r/Preview/?sm=xxxxx",
      "href": "https://api.surveymonkey.com/v3/survey_templates/template123"
    }
  ],
  "per_page": 50,
  "page": 1,
  "total": 25,
  "links": {
    "self": "https://api.surveymonkey.com/v3/survey_templates?page=1&per_page=50"
  }
}
```


### `survey_languages` object

**Source endpoint**:
`GET /survey_languages`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Language code (e.g., "en", "fr", "de") |
| `name` | string | Language display name |
| `native_name` | string | Language name in native script |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/survey_languages"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "en",
      "name": "English",
      "native_name": "English"
    },
    {
      "id": "es",
      "name": "Spanish",
      "native_name": "Español"
    },
    {
      "id": "fr",
      "name": "French",
      "native_name": "Français"
    },
    {
      "id": "de",
      "name": "German",
      "native_name": "Deutsch"
    },
    {
      "id": "ja",
      "name": "Japanese",
      "native_name": "日本語"
    }
  ],
  "per_page": 1000,
  "page": 1,
  "total": 57,
  "links": {
    "self": "https://api.surveymonkey.com/v3/survey_languages?page=1&per_page=1000"
  }
}
```


### `webhooks` object

**Source endpoint**:
`GET /webhooks`

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique webhook identifier |
| `name` | string | Webhook name |
| `event_type` | string | Event type: `response_completed`, `response_updated`, `response_disqualified`, `response_deleted`, `collector_created`, `collector_updated`, `collector_deleted` |
| `object_type` | string | Object type: `survey`, `collector` |
| `object_ids` | array<string> | IDs of objects this webhook monitors |
| `subscription_url` | string | URL to receive webhook callbacks |
| `authorization` | string or null | Authorization header value for callbacks |
| `href` | string | API resource URL |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/webhooks"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "webhook123",
      "name": "Response Notifications",
      "event_type": "response_completed",
      "object_type": "survey",
      "object_ids": ["survey123", "survey456"],
      "subscription_url": "https://myapp.example.com/webhooks/surveymonkey",
      "authorization": null,
      "href": "https://api.surveymonkey.com/v3/webhooks/webhook123"
    }
  ],
  "per_page": 50,
  "page": 1,
  "total": 2,
  "links": {
    "self": "https://api.surveymonkey.com/v3/webhooks?page=1&per_page=50"
  }
}
```


### `survey_rollups` object

**Source endpoint**:
`GET /surveys/{survey_id}/rollups`

**Key behavior**:
- Returns aggregated response statistics for each question in a survey
- Provides counts and percentages for answer choices
- Useful for quick analysis without fetching individual responses

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Question ID (primary key) |
| `survey_id` | string (connector-derived) | Survey ID |
| `family` | string | Question family (e.g., `single_choice`, `matrix`, `open_ended`) |
| `subtype` | string | Question subtype |
| `href` | string | API resource URL for this rollup |
| `summary` | array<map> | Summary statistics for the question (structure varies by question type) |

**Note**: The `summary` field structure varies by question type. Common fields include:
- `answered`: Number of respondents who answered
- `skipped`: Number of respondents who skipped
- `choices`: Array of choice counts with `id` and `count`
- `stats`: For numeric questions, includes `min`, `max`, `mean`, `median`, `std`

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/surveys/1234567890/rollups"
```

**Example response**:

```json
{
  "data": [
    {
      "page_id": "page123",
      "question_id": "question456",
      "subtype": "single",
      "heading": "How satisfied are you with our service?",
      "summary": [
        {
          "text": "Very Satisfied",
          "choice_id": "choice1",
          "count": 45,
          "percentage": 30.0
        },
        {
          "text": "Satisfied",
          "choice_id": "choice2",
          "count": 60,
          "percentage": 40.0
        },
        {
          "text": "Neutral",
          "choice_id": "choice3",
          "count": 30,
          "percentage": 20.0
        },
        {
          "text": "Dissatisfied",
          "choice_id": "choice4",
          "count": 10,
          "percentage": 6.67
        },
        {
          "text": "Very Dissatisfied",
          "choice_id": "choice5",
          "count": 5,
          "percentage": 3.33
        }
      ]
    }
  ],
  "per_page": 100,
  "page": 1,
  "total": 10,
  "links": {
    "self": "https://api.surveymonkey.com/v3/surveys/1234567890/rollups?page=1&per_page=100"
  }
}
```


### `benchmark_bundles` object

**Source endpoint**:
`GET /benchmark_bundles`

**Note**: Benchmarks are available as an add-on for Private apps. Contact benchmarksales@surveymonkey.com to enable.

**High-level schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique bundle identifier |
| `name` | string | Bundle name |
| `description` | string | Bundle description |
| `title` | string | Bundle title |
| `details` | struct or null | Additional bundle details |
| `country_code` | string | Country code for the benchmark data |
| `href` | string | API resource URL |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/benchmark_bundles"
```

**Example response**:

```json
{
  "data": [
    {
      "id": "bundle123",
      "name": "Customer Satisfaction Benchmark",
      "description": "Compare your CSAT scores against industry benchmarks",
      "title": "CSAT Industry Benchmark",
      "details": {
        "industry": "technology",
        "sample_size": 50000
      },
      "country_code": "US",
      "href": "https://api.surveymonkey.com/v3/benchmark_bundles/bundle123"
    }
  ],
  "per_page": 50,
  "page": 1,
  "total": 5,
  "links": {
    "self": "https://api.surveymonkey.com/v3/benchmark_bundles?page=1&per_page=50"
  }
}
```


## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get the primary key for SurveyMonkey objects.
Primary keys are defined **statically** based on the resource schema.

| Object | Primary Key(s) | Type | Notes |
|--------|---------------|------|-------|
| `surveys` | `id` | string | Unique across all surveys for the user |
| `survey_responses` | `id` | string | Unique across all responses; `survey_id` is connector-derived parent reference |
| `survey_pages` | `id` | string | Unique within a survey; composite key could be `(survey_id, id)` |
| `survey_questions` | `id` | string | Unique within a survey; composite key could be `(survey_id, page_id, id)` |
| `collectors` | `id` | string | Unique across all collectors |
| `contact_lists` | `id` | string | Unique across all contact lists |
| `contacts` | `id` | string | Unique across all contacts |
| `users` | `id` | string | Single record for authenticated user |
| `groups` | `id` | string | Unique across all groups |
| `group_members` | `id` | string | Unique across all group members; `group_id` is parent reference |
| `workgroups` | `id` | string | Unique across all workgroups |
| `survey_folders` | `id` | string | Unique across all folders |
| `survey_categories` | `id` | string | Static category identifiers |
| `survey_templates` | `id` | string | Unique template identifiers |
| `survey_languages` | `id` | string | Language code as identifier |
| `webhooks` | `id` | string | Unique webhook identifiers |
| `survey_rollups` | `id` | string | Question ID (unique per question within survey) |
| `benchmark_bundles` | `id` | string | Unique bundle identifiers |

**Example showing primary key in response**:

```json
{
  "id": "1234567890",
  "title": "Customer Satisfaction Survey",
  "date_modified": "2024-03-20T14:45:00+00:00"
}
```


## **Object's Ingestion Type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Planned ingestion types for SurveyMonkey objects:

| Object | Ingestion Type | Cursor Field | Rationale |
|--------|----------------|--------------|-----------|
| `surveys` | `cdc` | `date_modified` | Surveys have a stable `id` and `date_modified` field for incremental syncs. Surveys can be edited but are not typically deleted. |
| `survey_responses` | `cdc` | `date_modified` | Responses have a stable `id` and `date_modified` that updates when responses are edited or status changes. Supports filtering by date range. |
| `survey_pages` | `snapshot` | N/A | Pages are part of survey structure; refreshed with full sync per survey. |
| `survey_questions` | `snapshot` | N/A | Questions are part of survey structure; refreshed with full sync per survey. |
| `collectors` | `cdc` | `date_modified` | Collectors can be created/modified; have `date_modified` field. |
| `contact_lists` | `snapshot` | N/A | Small metadata objects; full refresh is sufficient. |
| `contacts` | `snapshot` | N/A | No `date_modified` field exposed; full refresh required. |
| `users` | `snapshot` | N/A | Single user record; full refresh on each sync. |
| `groups` | `snapshot` | N/A | Team/group metadata; full refresh sufficient. |
| `group_members` | `snapshot` | N/A | Member list per group; full refresh sufficient. |
| `workgroups` | `snapshot` | N/A | Workgroup metadata; full refresh sufficient. |
| `survey_folders` | `snapshot` | N/A | Folder metadata; full refresh sufficient. |
| `survey_categories` | `snapshot` | N/A | Static categories; rarely change. |
| `survey_templates` | `snapshot` | N/A | Template catalog; refreshed fully. |
| `survey_languages` | `snapshot` | N/A | Static language list; rarely change. |
| `webhooks` | `snapshot` | N/A | Webhook configurations; full refresh. |
| `survey_rollups` | `snapshot` | N/A | Aggregated stats; computed on each sync. |
| `benchmark_bundles` | `snapshot` | N/A | Benchmark catalog; refreshed fully. |

**For `surveys`**:
- **Primary key**: `id`
- **Cursor field**: `date_modified`
- **Sort order**: API supports sorting by date fields
- **Deletes**: Surveys can be deleted; API does not provide a deleted items endpoint. Consider periodic full refresh or handling missing IDs as deletes.

**For `survey_responses`**:
- **Primary key**: `id`
- **Cursor field**: `date_modified`
- **Lookback**: Recommended 1-hour lookback to handle late-arriving modifications
- **Deletes**: Responses can be deleted; no deleted items endpoint available


## **Read API for Data Retrieval**

### Primary read endpoint for `surveys`

- **HTTP method**: `GET`
- **Endpoint**: `/surveys`
- **Base URL**: `https://api.surveymonkey.com/v3`

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `page` | integer | no | 1 | Page number to return |
| `per_page` | integer | no | 50 | Number of resources per page (max 1000) |
| `sort_by` | string | no | `date_modified` | Field to sort by: `title`, `date_modified`, `num_responses` |
| `sort_order` | string | no | `DESC` | Sort direction: `ASC` or `DESC` |
| `include` | string | no | none | Additional fields to include: `response_count`, `date_created`, `date_modified`, `language`, `question_count`, `analyze_url`, `preview`, `collect_url`, `edit_url`, `summary_url` |
| `start_modified_at` | string (ISO 8601) | no | none | Filter: surveys modified after this datetime |
| `end_modified_at` | string (ISO 8601) | no | none | Filter: surveys modified before this datetime |
| `title` | string | no | none | Filter by survey title (case insensitive substring match) |
| `folder_id` | string | no | none | Filter by folder ID |

**Pagination strategy**:
- SurveyMonkey uses page-based pagination with `page` and `per_page` parameters.
- Response includes `links` object with `self`, `prev`, `next`, `first`, `last` URLs.
- Maximum `per_page` is 1000 for most endpoints.
- The connector should:
  - Request with `per_page=1000` for efficiency
  - Follow pagination until no `next` link is present

**Example incremental read**:

```bash
SINCE_TS="2024-01-01T00:00:00+00:00"
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/surveys?per_page=1000&sort_by=date_modified&sort_order=ASC&start_modified_at=${SINCE_TS}&include=response_count,date_created,date_modified,language,question_count"
```

**Getting full survey details**:

To get complete survey structure including pages and questions, use the `/details` endpoint:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/surveys/1234567890/details"
```


### Primary read endpoint for `survey_responses`

- **HTTP method**: `GET`
- **Endpoint**: `/surveys/{survey_id}/responses/bulk`
- **Base URL**: `https://api.surveymonkey.com/v3`

**Path parameters**:
- `survey_id` (string, required): The survey ID to get responses for

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `page` | integer | no | 1 | Page number |
| `per_page` | integer | no | 50 | Results per page (max 100 for responses) |
| `collector_ids` | string | no | none | Comma-separated collector IDs to filter by |
| `start_created_at` | string (ISO 8601) | no | none | Filter: responses created after this datetime |
| `end_created_at` | string (ISO 8601) | no | none | Filter: responses created before this datetime |
| `start_modified_at` | string (ISO 8601) | no | none | Filter: responses modified after this datetime |
| `end_modified_at` | string (ISO 8601) | no | none | Filter: responses modified before this datetime |
| `status` | string | no | none | Filter by status: `completed`, `partial`, `overquota`, `disqualified` |
| `email` | string | no | none | Filter by respondent email |
| `first_name` | string | no | none | Filter by respondent first name |
| `last_name` | string | no | none | Filter by respondent last name |
| `ip` | string | no | none | Filter by IP address |
| `custom` | string | no | none | Filter by custom variable value |
| `total_time_max` | integer | no | none | Filter: max total time in seconds |
| `total_time_min` | integer | no | none | Filter: min total time in seconds |
| `total_time_units` | string | no | `second` | Units for time filters |
| `sort_order` | string | no | `DESC` | Sort direction: `ASC` or `DESC` |
| `sort_by` | string | no | `date_modified` | Sort field: `date_modified` |

**Important**: For `survey_responses`, `per_page` maximum is **100** (not 1000 like other endpoints).

**Example incremental read**:

```bash
SURVEY_ID="1234567890"
SINCE_TS="2024-01-01T00:00:00+00:00"
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.surveymonkey.com/v3/surveys/${SURVEY_ID}/responses/bulk?per_page=100&sort_by=date_modified&sort_order=ASC&start_modified_at=${SINCE_TS}"
```

**Incremental strategy**:
- On first run: perform full historical backfill or use configurable `start_date`
- On subsequent runs:
  - Use max `date_modified` from previous sync (minus lookback window) as `start_modified_at`
  - Sort by `date_modified` ascending to process oldest changes first
  - Upsert records by `id`


### Reading child objects pattern

For objects that exist under a parent (like `survey_responses` under `surveys`), the connector pattern is:

1. List parent objects (`surveys`)
2. For each parent, list child objects (`survey_responses`)
3. Add parent identifier (`survey_id`) as an extra field in the output

**Example: Reading all responses across all surveys**:

```python
import requests

def read_all_responses(access_token, since=None):
    headers = {"Authorization": f"Bearer {access_token}"}
    base_url = "https://api.surveymonkey.com/v3"
    
    # Step 1: List all surveys
    surveys = []
    page = 1
    while True:
        resp = requests.get(
            f"{base_url}/surveys",
            headers=headers,
            params={"page": page, "per_page": 1000}
        )
        data = resp.json()
        surveys.extend(data["data"])
        if "next" not in data.get("links", {}):
            break
        page += 1
    
    # Step 2: For each survey, list responses
    all_responses = []
    for survey in surveys:
        survey_id = survey["id"]
        page = 1
        params = {"page": page, "per_page": 100}
        if since:
            params["start_modified_at"] = since
        
        while True:
            resp = requests.get(
                f"{base_url}/surveys/{survey_id}/responses/bulk",
                headers=headers,
                params=params
            )
            data = resp.json()
            for response in data["data"]:
                response["survey_id"] = survey_id  # Add parent reference
                all_responses.append(response)
            if "next" not in data.get("links", {}):
                break
            params["page"] += 1
    
    return all_responses
```


## **Field Type Mapping**

### General mapping (SurveyMonkey JSON → connector logical types)

| SurveyMonkey JSON Type | Example Fields | Connector Logical Type | Notes |
|------------------------|----------------|------------------------|-------|
| string (ID) | `id`, `survey_id`, `collector_id` | string | IDs are returned as strings, not integers |
| string | `title`, `nickname`, `name` | string | UTF-8 text |
| boolean | `is_owner`, `visible`, `footer` | boolean | Standard true/false |
| integer | `total_time`, `response_count`, `question_count` | long | Use 64-bit integer for safety |
| string (ISO 8601 datetime) | `date_created`, `date_modified` | timestamp with timezone | Format: `2024-01-15T10:30:00+00:00` |
| object | `buttons_text`, `metadata` | struct | Nested structures |
| array | `pages`, `questions`, `answers` | array<struct> | Arrays of nested objects |
| nullable fields | `custom_value`, `description`, `redirect_url` | corresponding type + null | When absent, return `null` not `{}` |
| map/dictionary | `custom_variables`, `custom_fields` | map<string, string> | Key-value pairs |

### Special behaviors and constraints

- **IDs are strings**: Unlike many APIs, SurveyMonkey IDs are returned as strings (e.g., `"1234567890"`) not integers. Store as strings.
- **Timestamps**: All timestamps include timezone offset (typically `+00:00` for UTC). Parse accordingly.
- **Nested answers structure**: Response answers are deeply nested: `pages[].questions[].answers[]`. Preserve this structure rather than flattening.
- **Empty vs null**: Empty strings `""` and empty arrays `[]` are distinct from `null`. The connector should preserve this distinction.
- **Question types affect answer structure**: The `answers` array content varies by question `family` and `subtype`. Some have `choice_id`, others have `text`, matrix questions have `row_id` and `col_id`.


## **Write API**

The initial connector implementation is primarily **read-only**. However, for completeness, SurveyMonkey API supports write operations.

### Create a survey

- **HTTP method**: `POST`
- **Endpoint**: `/surveys`
- **Required scope**: `Create/Modify Surveys`

**Request body**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `title` | string | yes | Survey title |
| `nickname` | string | no | Internal nickname |
| `language` | string | no | Language code (default: "en") |
| `folder_id` | string | no | Folder to place survey in |
| `from_template_id` | string | no | Create from template |
| `from_survey_id` | string | no | Copy from existing survey |

**Example request**:

```bash
curl -X POST \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "New Customer Survey",
    "nickname": "Q1 2024 Survey"
  }' \
  "https://api.surveymonkey.com/v3/surveys"
```

### Delete a response

- **HTTP method**: `DELETE`
- **Endpoint**: `/surveys/{survey_id}/responses/{response_id}`
- **Required scope**: `Create/Modify Responses`

```bash
curl -X DELETE \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.surveymonkey.com/v3/surveys/1234567890/responses/9876543210"
```

**Validation / read-after-write**:
- Use `GET /surveys/{id}` to verify survey creation/updates
- Use `GET /surveys/{survey_id}/responses/{response_id}` to verify response changes
- Incremental reads will pick up changes via `date_modified`


## **Known Quirks & Edge Cases**

- **Response per_page limit**: The `/responses/bulk` endpoint has a maximum `per_page` of 100, unlike other endpoints which allow up to 1000.

- **Question structure in responses**: Response data references questions by ID but does not include question text. To get human-readable question text, join with data from `/surveys/{id}/details`.

- **Answer structure varies by question type**:
  - Single/multiple choice: `choice_id` present
  - Open-ended: `text` present
  - Matrix: `row_id` and `col_id` present
  - File upload: `download_url` present

- **Date format**: Timestamps include timezone offset (`+00:00`) rather than `Z` suffix. Both represent UTC but parsing must handle the offset format.

- **Empty responses**: Partial responses may have empty `pages` array or questions without `answers`. Handle gracefully.

- **Deleted surveys/responses**: No "trash" or "deleted items" API endpoint. Items removed from API responses are permanently gone. Consider:
  - Tracking known IDs to detect deletions
  - Periodic full refresh to sync state

- **Custom variables**: Surveys can have custom variables that appear in response URLs. These are stored in `custom_variables` and `custom_value` fields.

- **Regional data centers**: US and EU data centers have different base URLs. The connector should support both:
  - US: `https://api.surveymonkey.com/v3`
  - EU: `https://api.eu.surveymonkey.com/v3`

- **Rate limit handling**: When rate limited, the API returns HTTP 429. Implement exponential backoff. Check `X-Ratelimit-App-Global-Minute-Reset` header for wait time.

- **Survey details vs list**: The `GET /surveys` list endpoint returns minimal data. Use `GET /surveys/{id}` or `GET /surveys/{id}/details` for full metadata.

- **Benchmarks availability**: Benchmark endpoints require an add-on for Private apps. Contact benchmarksales@surveymonkey.com to enable. Returns 403 if not enabled.

- **Groups vs Workgroups**: SurveyMonkey uses both concepts. Groups (teams) are for billing/organization, while Workgroups are for sharing resources within an organization.

- **Survey rollups computation**: Rollups are computed server-side and may have slight delays after new responses are submitted.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://api.surveymonkey.com/v3/docs#api-endpoints | 2024-12-18 | High | All endpoint paths, available objects, pagination, authentication, rate limits |
| Official Docs | https://api.surveymonkey.com/v3/docs#authentication | 2024-12-18 | High | OAuth 2.0 flow, token format, authorization header |
| Official Docs | https://api.surveymonkey.com/v3/docs#scopes | 2024-12-18 | High | Available scopes and permissions required |
| Official Docs | https://api.surveymonkey.com/v3/docs#pagination | 2024-12-18 | High | Pagination parameters, response structure with links |
| Official Docs | https://api.surveymonkey.com/v3/docs#headers | 2024-12-18 | High | Rate limit headers, request/response formats |
| Official Docs | https://api.surveymonkey.com/v3/docs#survey-responses | 2024-12-18 | High | Response bulk endpoint, filtering parameters, response schema |
| Official Docs | https://api.surveymonkey.com/v3/docs#surveys | 2024-12-18 | High | Survey endpoints, query parameters, schema |
| Official Docs | https://api.surveymonkey.com/v3/docs#users-and-teams | 2024-12-18 | High | Users, groups, workgroups endpoints and schemas |
| Official Docs | https://api.surveymonkey.com/v3/docs#benchmarks | 2024-12-18 | High | Benchmark bundles endpoint, availability restrictions |
| Official Docs | https://api.surveymonkey.com/v3/docs#webhooks | 2024-12-18 | High | Webhook endpoints, event types |
| Official Docs | https://api.surveymonkey.com/v3/docs#response-counts-and-trends | 2024-12-18 | High | Rollups and trends endpoints |
| Developer Portal | https://developer.surveymonkey.com/ | 2024-12-18 | High | App registration, OAuth setup |


## **Sources and References**

- **Official SurveyMonkey API documentation** (highest confidence)
  - https://api.surveymonkey.com/v3/docs#api-endpoints
  - https://api.surveymonkey.com/v3/docs#authentication
  - https://api.surveymonkey.com/v3/docs#scopes
  - https://api.surveymonkey.com/v3/docs#pagination
  - https://api.surveymonkey.com/v3/docs#headers
  - https://api.surveymonkey.com/v3/docs#surveys
  - https://api.surveymonkey.com/v3/docs#survey-responses
  - https://api.surveymonkey.com/v3/docs#survey-pages-and-questions
  - https://api.surveymonkey.com/v3/docs#collectors-and-invite-messages
  - https://api.surveymonkey.com/v3/docs#contacts-and-contact-lists
  - https://api.surveymonkey.com/v3/docs#users-and-teams
  - https://api.surveymonkey.com/v3/docs#webhooks
  - https://api.surveymonkey.com/v3/docs#response-counts-and-trends
  - https://api.surveymonkey.com/v3/docs#benchmarks
  - https://api.surveymonkey.com/v3/docs#survey-folders

- **SurveyMonkey Developer Portal** (high confidence)
  - https://developer.surveymonkey.com/
  - https://developer.eu.surveymonkey.com/ (EU data center)

When conflicts arise, **official SurveyMonkey API documentation** is treated as the source of truth.

---

**Document Status**: Complete with expanded object coverage including groups, workgroups, webhooks, rollups, and benchmarks.

**Acceptance Checklist**:
- [x] All required headings present and in order
- [x] Every field in each schema is listed (primary objects)
- [x] Exactly one authentication method documented (OAuth 2.0 Bearer Token)
- [x] Endpoints include params, examples, and pagination details
- [x] Incremental strategy defines cursor (`date_modified`), order, lookback, and delete handling
- [x] Research Log completed; Sources include full URLs
- [x] No unverifiable claims; gaps noted where applicable
