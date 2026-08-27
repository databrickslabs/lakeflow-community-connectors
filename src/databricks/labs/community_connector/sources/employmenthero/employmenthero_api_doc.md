# **Employment Hero API Documentation**

This document describes the Employment Hero REST API information relevant to the Employment Hero Lakeflow connector. The connector uses the official Employment Hero API (OAuth 2.0, v1) for snapshot ingestion: a **top-level organisations endpoint** (no organisation ID in the path) and **organisation-scoped list endpoints** that require an organisation ID.

**Official reference**: [Employment Hero API References](https://developer.employmenthero.com/api-references)


## **Authorization**

- **Method**: OAuth 2.0 (authorization code flow). The connector obtains an access token using client credentials and an authorization code, then uses Bearer token authentication for all API requests.
- **Base URL**: `https://api.employmenthero.com`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <access_token>`
  - Access tokens expire after **15 minutes**; the connector uses the **refresh token** to obtain new access tokens. The refresh token is returned on token exchange and on each refresh; clients must store and use the latest refresh token.
- **OAuth endpoints** (token exchange and refresh):
  - `POST https://oauth.employmenthero.com/oauth2/token` — exchange `authorization_code` for `access_token` and `refresh_token`, or send `refresh_token` with `grant_type=refresh_token` to get a new access token.
- **Required connection parameters**: `client_id`, `client_secret`, `redirect_uri`, `authorization_code` (one-time; thereafter only refresh token is used).
- **Scopes**: Defined when registering the OAuth application in the [Employment Hero Developer Portal](https://developer.employmenthero.com/api-references/#developer-portal-access); typical read scopes include organisation and employee read access (e.g. `urn:mainapp:organisations:read`, `urn:mainapp:employees:read`). Scopes are fixed at application creation.

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.employmenthero.com/api/v1/organisations/<organisation_id>/employees"
```

**Rate limits** (per official docs): **20 requests per second** and **100 requests per minute**. Higher limits may be requested via your admin.


## **Object List**

The connector exposes the following **tables**. The object list is **static** (defined by the connector), not discovered dynamically from the API.

- **`organisations`** — Top-level endpoint; **does not require** `organisation_id`. Use it to discover organisation IDs for the other tables.
- **All other tables** — Organisation-scoped; require `organisation_id` in connector table options (path parameter).

| Object Name        | Description                                              | Primary Endpoint                                                    | Ingestion Type |
|--------------------|----------------------------------------------------------|---------------------------------------------------------------------|----------------|
| `organisations`    | Top-level list of organisations (no organisation_id)    | `GET /api/v1/organisations`                                         | `snapshot`     |
| `employees`        | Employees for the organisation                           | `GET /api/v1/organisations/{organisation_id}/employees`             | `snapshot`     |
| `certifications`   | Certifications (checks/training) for the organisation     | `GET /api/v1/organisations/{organisation_id}/certifications`       | `snapshot`     |
| `cost_centres`     | Cost centres for the organisation                        | `GET /api/v1/organisations/{organisation_id}/cost_centres`           | `snapshot`     |
| `custom_fields`    | Custom field definitions for the organisation            | `GET /api/v1/organisations/{organisation_id}/custom_fields`          | `snapshot`     |
| `employing_entities` | Employing entities for the organisation               | `GET /api/v1/organisations/{organisation_id}/employing_entities`     | `snapshot`     |
| `leave_categories` | Leave categories for the organisation                    | `GET /api/v1/organisations/{organisation_id}/leave_categories`       | `snapshot`     |
| `leave_requests`   | Leave requests for the organisation (optional date filter) | `GET /api/v1/organisations/{organisation_id}/leave_requests`         | `snapshot`     |
| `policies`         | Policies for the organisation                            | `GET /api/v1/organisations/{organisation_id}/policies`               | `snapshot`     |
| `roles`            | Roles/tags for the organisation                           | `GET /api/v1/organisations/{organisation_id}/roles`                  | `snapshot`     |
| `teams`            | Teams (shown as Groups in the UI) for the organisation    | `GET /api/v1/organisations/{organisation_id}/teams`                   | `snapshot`     |
| `timesheet_entries`| Timesheet entries across all employees (optional date range) | `GET /api/v1/organisations/{organisation_id}/employees/-/timesheet_entries` | `snapshot`     |
| `work_locations`   | Work locations for the organisation                       | `GET /api/v1/organisations/{organisation_id}/work_locations`         | `snapshot`     |
| `work_sites`       | Work sites (with departments, HR positions, address)      | `GET /api/v1/organisations/{organisation_id}/work_sites`              | `snapshot`     |

**Connector scope**: All listed tables are implemented as **snapshot** ingestion: full read per table on each sync. For organisation-scoped tables, each sync is per organisation. No incremental cursor is used; optional date-based filtering (`start_date`) is supported for `leave_requests` and `timesheet_entries`.


## **Object Schema**

### General notes

- The Employment Hero API returns JSON. List endpoints return a wrapper object with a `data` property; the connector expects `data` to contain either an array of items or an object with an `items` array and pagination fields (`page_index`, `item_per_page`, `total_items`, `total_pages`). The connector iterates over the items across all pages.
- Identifiers are **UUIDs** (strings). The connector models them as string type.
- Nested JSON objects (e.g. `residential_address`, `primary_manager`, `custom_field_options`) are represented as **nested structs or arrays** in the connector schema rather than flattened.
- Some API fields are **regional**: they appear only for certain regions (e.g. AU, UK, NZ, CA, SG, MY). For employees outside that region, regional fields may be omitted from the response. The connector schema includes them as optional (nullable) where applicable.


### `organisations` object

**Source endpoint**:  
`GET /api/v1/organisations`

**Key behavior**: Top-level endpoint that returns a paginated list of organisations the authenticated user can access. **No organisation ID is required** in the path or in table options. Use this table to discover organisation IDs for all other (organisation-scoped) tables.

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Organisation identifier. Use this as `organisation_id` for other tables. |
| `name` | string | Organisation name. |
| `phone` | string | Phone number. |
| `country` | string | Country code (e.g. AU). |
| `logo_url` | string | URL of the organisation logo. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.employmenthero.com/api/v1/organisations?page_index=1&item_per_page=20"
```


### `employees` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/employees`

**Key behavior**: Returns a paginated list of employees for the organisation. Response can include regional fields; the connector schema covers common and commonly used regional fields.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Unique employee identifier. |
| `account_email` | string | Account email. |
| `email` | string | Email. |
| `title` | string | Title. |
| `role` | string | Role. |
| `first_name` | string | First name. |
| `last_name` | string | Last name. |
| `middle_name` | string | Middle name. |
| `known_as` | string | Preferred name. |
| `full_name` | string | Full name. |
| `full_legal_name` | string | Full legal name. |
| `legal_name` | string | Legal name. |
| `pronouns` | string | Pronouns. |
| `avatar_url` | string | Avatar URL. |
| `address` | string | Address (legacy/simple). |
| `personal_email` | string | Personal email. |
| `personal_mobile_number` | string | Personal mobile. |
| `home_phone` | string | Home phone. |
| `company_email` | string | Company email. |
| `company_mobile` | string | Company mobile. |
| `company_landline` | string | Company landline. |
| `display_mobile_in_staff_directory` | boolean | Whether to show mobile in directory. |
| `job_title` | string | Job title. |
| `code` | string | Employee code. |
| `location` | string | Location. |
| `employing_entity` | string | Employing entity. |
| `start_date` | string | Start date. |
| `termination_date` | string | Termination date. |
| `status` | string | Employment status. |
| `termination_summary` | string | Termination summary. |
| `employment_type` | string | Employment type. |
| `typical_work_day` | string | Typical work day. |
| `roster` | string | Roster. |
| `trial_or_probation_type` | string | Trial/probation type. |
| `trial_length` | long | Trial length. |
| `probation_length` | long | Probation length. |
| `global_teams_start_date` | string | Global teams start date. |
| `global_teams_probation_end_date` | string | Global teams probation end date. |
| `external_id` | string | External ID. |
| `work_country` | string | Work country. |
| `payroll_type` | string | Payroll type. |
| `time_zone` | string | Time zone. |
| `nationality` | string | Nationality. |
| `gender` | string | Gender. |
| `country` | string | Country. |
| `date_of_birth` | string | Date of birth. |
| `marital_status` | string | Marital status. |
| `aboriginal_torres_strait_islander` | boolean | Aboriginal/Torres Strait Islander (AU). |
| `previous_surname` | string | Previous surname. |
| `biography` | string | Biography. |
| `instapay_referral_opted_out` | boolean | Instapay referral opt-out. |
| `independent_contractor` | boolean | Whether employee is independent contractor. |
| `trading_name` | string | Trading name (contractor). |
| `abn` | string | ABN (contractor). |
| `business_detail` | struct | Business details (country, number, business_type). |
| `uk_tax_and_national_insurance` | struct | UK tax/NI (id, unique_taxpayer_reference, national_insurance_number). |
| `teams` | array\<struct\> | Teams (Groups); elements have `id`, `name`. |
| `primary_cost_centre` | struct | Primary cost centre (`id`, `name`). |
| `secondary_cost_centres` | array\<struct\> | Secondary cost centres. |
| `primary_manager` | struct | Primary manager reference. |
| `secondary_manager` | struct | Secondary manager reference. |
| `residential_address` | struct | Residential address (address_type, line_1, city, postcode, country, etc.). |
| `postal_address` | struct | Postal address (same shape as residential). |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.employmenthero.com/api/v1/organisations/<organisation_id>/employees?page_index=1&item_per_page=20"
```


### `certifications` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/certifications`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Certification ID. |
| `name` | string | Name of certification. |
| `type` | string (enum) | Type, e.g. `check`, `training`. |


### `cost_centres` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/cost_centres`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Cost centre ID. |
| `name` | string | Name of cost centre. |


### `custom_fields` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/custom_fields`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Custom field ID. |
| `name` | string | Name. |
| `hint` | string | Hint text. |
| `description` | string | Description. |
| `type` | string (enum) | e.g. `free_text`, `single_select`, `multi_select`. |
| `in_onboarding` | boolean | Whether captured during onboarding. |
| `required` | boolean | Whether mandatory. |
| `custom_field_permissions` | array\<struct\> | Permissions (id, permission, role). |
| `custom_field_options` | array\<struct\> | Options when type is single/multi select (id, value). |


### `employing_entities` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/employing_entities`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Employing entity ID. |
| `name` | string | Name. |


### `leave_categories` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/leave_categories`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Leave category ID. |
| `name` | string | Name. |
| `unit_type` | string | e.g. `days`, `hours`. |


### `leave_requests` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/leave_requests`

**Key behavior**: Returns a paginated list of leave requests for the organisation. The API supports optional query parameters **`start_date`** and **`end_date`** (`YYYY-MM-DD`) to filter by leave request start/end date. The connector passes `start_date` from table options when provided. See [Get Leave Requests](https://developer.employmenthero.com/api-references#get-leave-requests).

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Leave request ID. |
| `start_date` | string | Start date of the leave request. |
| `end_date` | string | End date of the leave request. |
| `total_hours` | double | Total hours of leave. |
| `comment` | string | Comment (e.g. from owner/admin). |
| `status` | string | e.g. `Approved`, `Pending`. |
| `leave_balance_amount` | double | Leave balance amount. |
| `leave_category_name` | string | Category name (e.g. Annual Leave). |
| `reason` | string | Reason for leave. |
| `employee_id` | string (UUID) | Employee who requested leave. |
| `hours_per_day` | array\<struct\> | Custom hours per date; elements have `date` (string), `hours` (double). |


### `policies` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/policies`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Policy ID. |
| `name` | string | Name. |
| `induction` | boolean | Whether used in induction. |
| `created_at` | string | Creation time. |


### `roles` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/roles`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Role ID. |
| `name` | string | Name. |


### `teams` object (Groups in UI)

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/teams`

The API uses "teams"; the Employment Hero UI uses "Groups". Same resource.

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Team ID. |
| `name` | string | Name. |
| `status` | string | e.g. `active`. |


### `timesheet_entries` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/employees/{employee_id}/timesheet_entries`

The connector uses **`employee_id = "-"`** (wildcard) to fetch timesheet entries for **all employees** in one table:  
`GET /api/v1/organisations/{organisation_id}/employees/-/timesheet_entries`

**Key behavior**: Returns a paginated list of timesheet entries. The API supports optional query parameters **`start_date`** and **`end_date`** (format `dd/mm/yyyy`) to filter by date range. The connector passes `start_date` from table options when provided. See [Get Timesheet Entries](https://developer.employmenthero.com/api-references#get-timesheet-entries).

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Timesheet entry ID. |
| `date` | string | Date of the timesheet entry. |
| `start_time` | string | Start time. |
| `end_time` | string | End time. |
| `status` | string | e.g. `pending`, `approved`. |
| `units` | double | Productive working hours (excludes breaks). |
| `unit_type` | string | e.g. `hours`. |
| `break_units` | double | Total break hours. |
| `breaks` | array\<struct\> | Break periods; elements have `start_time`, `end_time`. |
| `reason` | string | Reason. |
| `comment` | string | Comment. |
| `time` | long | Time in milliseconds (when start/end time empty). |
| `cost_centre` | struct | Cost centre (`id`, `name`). |
| `work_site_id` | string (UUID) | Work site ID. |
| `work_site_name` | string | Work site name. |
| `position_id` | string (UUID) | Position ID. |
| `position_name` | string | Position name. |


### `work_locations` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/work_locations`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Work location ID. |
| `name` | string | Name. |
| `country` | string | Country. |


### `work_sites` object

**Source endpoint**:  
`GET /api/v1/organisations/{organisation_id}/work_sites`

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Work site ID. |
| `name` | string | Name. |
| `status` | string | e.g. `active`. |
| `roster_positions_count` | long | Roster positions count. |
| `hr_positions` | array\<struct\> | HR positions (id, status, cost_centre, color, team_assign_mode, etc.). |
| `address` | struct | Address (same shape as employee address structs). |
| `departments` | array\<struct\> | Departments (id, name, assignment_count). |


## **Primary keys and ingestion type**

- All connector tables use **primary key**: `id` (UUID string).
- **Ingestion type**: All tables are **snapshot**. The **organisations** table is read from the top-level endpoint (full list in one sync). Organisation-scoped tables are read per organisation on each sync. There is no incremental cursor or delete stream for these endpoints.


## **Read API for Data Retrieval**

### Top-level organisations endpoint

For the **organisations** table only:

- **Endpoint**: `GET /api/v1/organisations`
- **Path parameters**: None. No `organisation_id` is required.
- **Query parameters**: Same pagination as below (`page_index`, `item_per_page`).
- **Response**: Same `data` wrapper with `items` and pagination fields. Use the `id` of each organisation as `organisation_id` when reading other tables.

### Common pattern for organisation-scoped list endpoints

For all other tables (employees, teams, certifications, etc.):

- **HTTP method**: `GET`
- **Endpoint pattern**: `/api/v1/organisations/{organisation_id}/{resource}`
- **Base URL**: `https://api.employmenthero.com`

**Path parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `organisation_id` | string (UUID) | yes | Organisation ID. Supplied via connector **table options** (`organisation_id`), not connection parameters. Obtain from the **organisations** table or your admin. |

**Query parameters (pagination)**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `page_index` | integer | no | 1 | Page index (min 1). |
| `item_per_page` | integer | no | 20 | Items per page (max 100 per API; connector may use a smaller page size for reliability). |

**Response body**: A JSON object with a `data` property. For list endpoints, `data` typically has:

- `items`: array of resource objects.
- `page_index`, `item_per_page`, `total_items`, `total_pages`: pagination metadata.

The connector paginates by requesting successive `page_index` until no more pages, and yields each item from `data.items` (or equivalent) as a record.

**Example**:

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.employmenthero.com/api/v1/organisations/<organisation_id>/employees?page_index=1&item_per_page=20"
```

**Example response (truncated)**:

```json
{
  "data": {
    "items": [
      {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "first_name": "Jane",
        "last_name": "Doe",
        "email": "jane.doe@example.com",
        "status": "active"
      }
    ],
    "page_index": 1,
    "item_per_page": 20,
    "total_items": 1,
    "total_pages": 1
  }
}
```


## **Errors and rate limits**

- **HTTP status codes**: `2xx` success; `4xx` client error (e.g. 400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found, 422 Unprocessable Entity); `5xx` server error. On 401, the connector should refresh the access token and retry if appropriate.
- **Rate limits**: **20 requests per second** and **100 requests per minute**. 429 Too Many Requests indicates the limit was exceeded; the connector should honour rate limiting (e.g. backoff or throttling).
- **Error response body**: Includes `code` (HTTP status) and an `error` / `errors` structure with details. The connector should surface these to the user or logs when a request fails.


## **Field type mapping**

| Employment Hero API | Connector type | Notes |
|---------------------|----------------|-------|
| UUID | string | All IDs are UUIDs, stored as string. |
| string | string | Text, enums, dates/datetimes as returned by API. |
| number (integer) | long | e.g. counts, lengths, milliseconds; use 64-bit where applicable. |
| number (decimal) | double | e.g. `total_hours`, `units`, `break_units`, `hours` in leave_requests/timesheet_entries. |
| boolean | boolean | Standard true/false. |
| object | struct | Nested struct with fixed fields (e.g. address, reference). |
| array of objects | array\<struct\> | e.g. teams, custom_field_options, departments. |
| datetime / date | string | Connector does not parse; stored as string (ISO 8601 or API format). |

**Regional fields**: Some fields appear only for certain regions (AU, UK, NZ, CA, SG, MY). The connector schema marks these as nullable; missing fields are represented as null.


## **Terminology: Teams vs Groups**

The Employment Hero UI uses **Groups**; the API and connector use **teams**. Endpoints and field names use "team". No code change is required for existing integrations; this is a UI-only naming difference.


## **Sources and references**

- **Employment Hero API References** (official): [https://developer.employmenthero.com/api-references](https://developer.employmenthero.com/api-references)
  - Introduction, API versioning, regional data, terminology (Teams/Groups).
  - Authentication (OAuth 2.0, authorization, token exchange, refresh, use of Bearer token).
  - Errors and rate limits.
  - Endpoint documentation for certifications, cost centres, custom fields, employees, employing entities, leave categories, leave requests, policies, roles, teams, timesheet entries, work locations, work sites, and related resources.

When in doubt, the **official Employment Hero API documentation** is the source of truth for endpoint behaviour, request/response shape, and rate limits.
