# Lakeflow Employment Hero Community Connector

This documentation describes how to configure and use the **Employment Hero** Lakeflow community connector to ingest data from the [Employment Hero REST API](https://developer.employmenthero.com/api-references) into Databricks.


## Prerequisites

- **Employment Hero account**: You need an Employment Hero account with API access (Platinum subscription and above). API access is required to use the connector.
- **OAuth 2.0 application**: Register an application in the Employment Hero Developer Portal to obtain client credentials and configure redirect URIs.
- **Authorization**: You must complete the OAuth flow at least once to obtain an authorization code for the connector to authenticate.
- **Network access**: The environment running the connector must be able to reach `https://api.employmenthero.com` and `https://oauth.employmenthero.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the connection parameters defined in the connector spec.

| Name                 | Type   | Required | Description                                                                                       |
|----------------------|--------|----------|---------------------------------------------------------------------------------------------------|
| `client_id`          | string | yes      | Client ID from your OAuth 2.0 application credentials (Employment Hero Developer Portal).         |
| `client_secret`      | string | yes      | Client secret from your OAuth 2.0 application credentials.                                       |
| `redirect_uri`       | string | yes      | Redirect URI registered with the OAuth 2.0 application (must match the callback URL used to obtain the authorization code). E.g. `https://<workspace>.cloud.databricks.com/oauth/callback` |
| `authorization_code` | string | yes      | Authorization code from the OAuth callback (redirect after the user authorises the app).          |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to be passed through. Use `organisation_id,start_date` so org-scoped tables can receive `organisation_id` (and optional `start_date` where supported). The top-level `organisations` table does not require `organisation_id`. | `organisation_id,start_date` |

The supported table-specific options for `externalOptionsAllowList` are:

`organisation_id,start_date`

> **Note**: The table-specific option `organisation_id` is **not** a connection parameter. It is provided per table via table options in the pipeline specification for **organisation-scoped** tables. The top-level **`organisations`** table does not need `organisation_id`. The option name must be included in `externalOptionsAllowList` for the connection to allow it when you use org-scoped tables.

### Obtaining the Required Parameters

- **OAuth 2.0 credentials and authorization**:
  1. Sign in to Employment Hero and open the **Developer Portal** (under your profile in the top right).
  2. Register a new OAuth 2.0 application with a **Name**, **Scope** (provided by Employment Hero), and **Redirect URI(s)** (HTTPS). Scopes are fixed at creation time.
  3. From the application details page, copy the **Client ID** and **Client Secret**.
  4. Use the Authorisation Server URL to obtain user consent:  
     `https://oauth.employmenthero.com/oauth2/authorize?client_id=<client_id>&redirect_uri=<redirect_uri>&response_type=code`
  5. After the user grants access, the browser redirects to your redirect URI with an `code` query parameter. Use this as the initial `authorization_code` connection option.
  6. The connector exchanges the authorization code for an access token and refresh token. Access tokens expire after **15 minutes**; the connector uses the refresh token to obtain new access tokens. Store and reuse the refresh token when reconfiguring the connection if your setup supports it.

- **Organisation ID**: Most tables are scoped to an organisation and require an `organisation_id` table option. You can obtain organisation IDs from the **`organisations`** table (top-level endpoint; no `organisation_id` required) or from your Employment Hero admin. Pass `organisation_id` when configuring each organisation-scoped table in the pipeline.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select or create a connection that uses this Employment Hero connector.
3. Set `externalOptionsAllowList` to `organisation_id,start_date` so that the connector can receive the organisation ID per table, and start date for tables that use it.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Employment Hero connector exposes a **static list** of tables:

- **`organisations`** — Top-level list of organisations the authenticated user can access. **Does not require** `organisation_id`; use this table to discover organisation IDs for the other tables.
- **Organisation-scoped tables** — Each of the following corresponds to a list endpoint under `/api/v1/organisations/{organisation_id}/...` and **requires** `organisation_id` in the table options:
  - `employees`
  - `certifications`
  - `cost_centres`
  - `custom_fields`
  - `employing_entities`
  - `leave_categories`
  - `leave_requests`
  - `policies`
  - `roles`
  - `teams`
  - `timesheet_entries`
  - `work_locations`
  - `work_sites`

### Object summary, primary keys, and ingestion mode

All supported tables use **snapshot** ingestion and are keyed by `id`:

| Table               | Description                                                                 | Ingestion Type | Primary Key | Table options |
|---------------------|-----------------------------------------------------------------------------|----------------|-------------|----------------|
| `organisations`     | Top-level list of organisations (no `organisation_id` required)              | `snapshot`     | `id`        | None           |
| `employees`         | Employees (and contractors) for the organisation                            | `snapshot`     | `id`        | `organisation_id` |
| `certifications`    | Certifications defined for the organisation                                | `snapshot`     | `id`        | `organisation_id` |
| `cost_centres`      | Cost centres in the organisation                                            | `snapshot`     | `id`        | `organisation_id` |
| `custom_fields`     | Custom field definitions for the organisation                              | `snapshot`     | `id`        | `organisation_id` |
| `employing_entities`| Employing entities in the organisation                                     | `snapshot`     | `id`        | `organisation_id` |
| `leave_categories`  | Leave categories (e.g. Annual Leave, Sick Leave)                            | `snapshot`     | `id`        | `organisation_id` |
| `leave_requests`    | Leave requests for the organisation (supports optional `start_date`)       | `snapshot`     | `id`        | `organisation_id` |
| `policies`          | Policies (e.g. induction policies)                                          | `snapshot`     | `id`        | `organisation_id` |
| `roles`             | Roles/tags (standalone HR or payroll-connected)                             | `snapshot`     | `id`        | `organisation_id` |
| `teams`             | Teams (shown as Groups in the Employment Hero UI)                           | `snapshot`     | `id`        | `organisation_id` |
| `timesheet_entries` | Timesheet entries across all employees (supports optional `start_date`)    | `snapshot`     | `id`        | `organisation_id` |
| `work_locations`    | Work locations (e.g. offices)                                               | `snapshot`     | `id`        | `organisation_id` |
| `work_sites`        | Work sites with address, departments, and HR positions                      | `snapshot`     | `id`        | `organisation_id` |

### Required table options

- **`organisations`** table: No table options are required. The connector calls the top-level endpoint `GET /api/v1/organisations` and returns all organisations the authenticated user can access. Use this table to discover organisation IDs for the other tables.

- **All other (organisation-scoped) tables**: You must provide **`organisation_id`** (string, required) in the pipeline table configuration. The connector calls the API with path `/api/v1/organisations/{organisation_id}/{resource}`. Obtain organisation IDs from the `organisations` table or your Employment Hero admin.

No other table-specific options are required for basic use. Omitted optional API query parameters (e.g. filters) use their defaults in the connector.

### Optional table options

The following tables support an optional **`start_date`** table option. When provided, the connector passes it to the API as a date filter so only records on or after that date are returned:

- **`leave_requests`**: `start_date` (string, `YYYY-MM-DD`) — filter leave requests by start date. See [Get Leave Requests](https://developer.employmenthero.com/api-references#get-leave-requests).
- **`timesheet_entries`**: `start_date` (string, `dd/mm/yyyy` per API) — start of the date range for timesheet entries. The API also supports `end_date`; the connector currently forwards `start_date` when present. See [Get Timesheet Entries](https://developer.employmenthero.com/api-references#get-timesheet-entries).

Include `start_date` in `externalOptionsAllowList` (e.g. `organisation_id,start_date`) if you use it. Example table config:

```json
{
  "table": {
    "source_table": "leave_requests",
    "organisation_id": "bdfcb02b-fcc3-4f09-8636-c06c14345b86",
    "start_date": "2025-01-01"
  }
}
```

### Schema highlights

Schemas are defined in `employmenthero_schemas.py` and align with the [Employment Hero API documentation](https://developer.employmenthero.com/api-references):

- **`organisations`**: Top-level organisations list with `id`, `name`, `phone`, `country`, `logo_url`, and related fields. No `organisation_id` in the path.
- **`employees`**: Rich schema including identity, contact, employment, and contractor fields; nested structs for `teams`, `primary_cost_centre`, `primary_manager`, `business_detail`, addresses, etc.
- **`certifications`**, **`cost_centres`**, **`employing_entities`**, **`roles`**, **`work_locations`**: Simple tables with `id` and `name` (and, where applicable, `type`, `country`, or `status`).
- **`custom_fields`**: Includes `custom_field_permissions` and `custom_field_options` as array-of-struct fields.
- **`leave_categories`**: `id`, `name`, `unit_type` (e.g. `days`, `hours`).
- **`leave_requests`**: Leave requests with `id`, `start_date`, `end_date`, `total_hours`, `comment`, `status`, `leave_balance_amount`, `leave_category_name`, `reason`, `employee_id`, and `hours_per_day` (array of `{date`, `hours}`). Supports optional table option `start_date` to filter by start date.
- **`policies`**: `id`, `name`, `induction`, `created_at`.
- **`teams`**: `id`, `name`, `status` (API uses “team”; UI shows “Groups”).
- **`timesheet_entries`**: Timesheet entries with `id`, `date`, `start_time`, `end_time`, `status`, `units`, `unit_type`, `break_units`, `breaks` (array of `{start_time`, `end_time}`), `reason`, `comment`, `time` (milliseconds), `cost_centre`, and work site/position fields. Fetched for all employees via `employees/-/timesheet_entries`. Supports optional table option `start_date` for the date range.
- **`work_sites`**: `id`, `name`, `status`, `roster_positions_count`, plus nested `hr_positions`, `address`, and `departments`.

You do not need to customize the schema; it is static and defined by the connector.

## Data Type Mapping

Employment Hero API JSON is mapped to Spark types as follows:

| Employment Hero API / JSON   | Example fields                    | Connector / Spark type   | Notes |
|------------------------------|-----------------------------------|--------------------------|-------|
| UUID / string                | `id`, `name`, `status`, `type`    | `StringType`             | IDs and enums are strings. |
| number                       | `trial_length`, `probation_length`, counts | `LongType`        | Used for integer counts and lengths. |
| boolean                      | `in_onboarding`, `required`, `induction`   | `BooleanType`     | Standard booleans. |
| datetime / ISO 8601 string   | `created_at`, `date_of_birth`      | `StringType`             | Stored as strings; downstream can cast to timestamp. |
| object                       | `primary_cost_centre`, `address`, `business_detail` | `StructType` | Nested objects preserved. |
| array                        | `teams`, `custom_field_options`, `departments` | `ArrayType(...)`   | Arrays of primitives or structs. |
| nullable / omitted           | Optional or region-specific fields | same as base type, nullable | Absent fields are `null`. |

The connector preserves nested structures and uses nullable types where the API may omit fields (e.g. regional fields).

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Employment Hero connector source in your workspace. This typically places the connector code (`employmenthero.py`, `employmenthero_schemas.py`, `employmenthero_client.py`, and related files) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline configuration (e.g. pipeline spec or ingestion_pipeline entrypoint), reference:

- A **Unity Catalog connection** that uses this Employment Hero connector and has `externalOptionsAllowList` set to `organisation_id,start_date` (so org-scoped tables can receive `organisation_id` and optional `start_date`).
- One or more **tables** to ingest. The **`organisations`** table needs no table options; all other tables need `organisation_id` (and optionally `start_date` where supported).

Example pipeline spec snippet: ingest the top-level **organisations** table (no options) and several organisation-scoped tables:

```json
{
  "pipeline_spec": {
    "connection_name": "employmenthero_connection",
    "object": [
      {
        "table": {
          "source_table": "organisations"
        }
      },
      {
        "table": {
          "source_table": "employees",
          "organisation_id": "bdfcb02b-fcc3-4f09-8636-c06c14345b86"
        }
      },
      {
        "table": {
          "source_table": "teams",
          "organisation_id": "bdfcb02b-fcc3-4f09-8636-c06c14345b86"
        }
      },
      {
        "table": {
          "source_table": "cost_centres",
          "organisation_id": "bdfcb02b-fcc3-4f09-8636-c06c14345b86"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Employment Hero OAuth credentials (`client_id`, `client_secret`, `redirect_uri`, `authorization_code`).
- For **`organisations`**, no table options are required.
- For each **organisation-scoped** `table`, `source_table` must be one of the supported table names (e.g. `employees`, `teams`), and `organisation_id` must be the UUID of the organisation to sync. You can obtain these IDs from the `organisations` table.

You can add more tables (e.g. `certifications`, `custom_fields`, `leave_categories`, `policies`, `roles`, `work_locations`, `work_sites`) by adding more `table` entries with the same `organisation_id` (or a different one for multi-org setups).

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your usual Lakeflow or Databricks orchestration (e.g. scheduled job or workflow). All tables are **snapshot**-based; each run reads the current state of the selected tables for the given organisation(s).

#### Best practices

- **Start with one organisation**: Configure a single `organisation_id` and one or two tables (e.g. `employees`, `teams`) to verify credentials and data shape.
- **Respect rate limits**: The Employment Hero API enforces rate limits (e.g. 20 requests per second and 100 per minute). The connector uses server-side pagination (`page_index`, `item_per_page`); avoid scheduling many large syncs in a short window.
- **Token handling**: Access tokens expire after 15 minutes. The connector refreshes using the refresh token; ensure the connection stores the latest refresh token if your platform updates it after use.
- **Organisation ID**: Use the same organisation IDs you see in the Employment Hero product or from the Get Organisations API so that table options match the correct tenant.

#### Troubleshooting

- **Authentication failures (401 / 403)**  
  - Check that `client_id`, `client_secret`, `redirect_uri`, and `authorization_code` are correct and that the authorization code has not already been used (use the resulting refresh token for subsequent runs if supported).  
  - Ensure the OAuth application scopes include access to the endpoints you need (e.g. employees, organisations).

- **Missing or invalid `organisation_id`**  
  - The connector raises an error if `organisation_id` is missing for an organisation-scoped table. Add `organisation_id` to the table options and ensure it is in `externalOptionsAllowList`. The **`organisations`** table does not require `organisation_id`.

- **404 or empty data**  
  - Confirm the organisation ID exists and that the authenticated user has access to that organisation.  
  - For SSO organisations, note that only one SSO organisation may be authorised per token; use separate connections/tokens if you need multiple SSO orgs.

- **Rate limiting (429)**  
  - Reduce concurrency or schedule syncs less frequently. Stay within the published limits (e.g. 20 req/s, 100 req/min) per [Employment Hero API documentation](https://developer.employmenthero.com/api-references).

- **Schema or type mismatches downstream**  
  - The connector uses nested structs and arrays; ensure downstream tables or views accept these types or add transforms to flatten/cast as needed.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/employmenthero/employmenthero.py`
- Schemas and table metadata: `src/databricks/labs/community_connector/sources/employmenthero/employmenthero_schemas.py`
- Connector spec (connection parameters and allowlist): `src/databricks/labs/community_connector/sources/employmenthero/connector_spec.yaml`
- Employment Hero API reference: [https://developer.employmenthero.com/api-references](https://developer.employmenthero.com/api-references)
