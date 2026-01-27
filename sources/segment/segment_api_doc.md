# **Segment API Documentation**

## **Authorization**

- **Chosen method**: Public API Access Token (Bearer Token)
- **Base URL**: 
  - US region: `https://api.segmentapis.com`
  - EU region: `https://eu1.api.segmentapis.com`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <PUBLIC_API_TOKEN>`
- **Token creation**:
  1. Navigate to **Settings** > **Workspace settings** > **Access Management** > **Tokens**
  2. Click **+ Create Token**
  3. Create a description and assign **Workspace Owner** or **Workspace Member** access
  4. Click **Create** and copy the token to a secure location
- **Requirements**: 
  - Only Workspace Owners can create Public API tokens
  - Available to Team and Business tier customers only
- **Connector configuration parameters**:
  - `api_token` (required): Public API token from Segment's Workspace settings
  - `region` (required): API region — `api` for US or `eu1` for EU (default: `api`)
  - `start_date` (required): Start date for incremental sync streams (usage data)
- **Other supported methods (not used by this connector)**:
  - OAuth is not supported by the Public API; tokens must be created through the Segment UI

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/sources"
```

Notes:
- Rate limiting is **200 requests per minute** for the Public API (higher than the legacy Config API)
- Tokens cannot be retrieved after creation; store them securely
- Tokens have granular permission scopes — assign only the least permissions needed
- **Token security**: Segment partners with GitHub to scan public repositories for exposed API tokens; detected tokens are automatically revoked and workspace owners are notified
- **Server-side only**: The Public API is designed for server-side use only; attempting to call from a frontend/browser will result in CORS errors

**Public API vs Config API (Legacy)**:

| Benefit | Details |
|---------|---------|
| Future Enhancements | Future improvements will be added to the Public API only |
| Improved error handling | More specific error messages for faster issue resolution |
| Versioning | Each endpoint can have multiple versions (stable, beta, alpha) |
| Higher rate limits | Can offer higher rate limits when needed |
| Improved architecture | Built with improved security, authentication, authorization, input validation |
| Cleaner mapping | Uses unique IDs for reference instead of slugs |
| Available in Europe | Accessible to both US and EU-based workspaces |
| Increased reliability | 99.8% success rate |


## **Data Types Available from Segment**

Segment provides two distinct types of data that can be ingested:

### 1. Public API Data (Workspace Configuration)
The Segment Public API provides access to **workspace configuration and metadata**:
- Sources, Destinations, Warehouses configuration
- Tracking Plans, Users, Labels
- Audit events, Spaces, Transformations
- Catalog information

This is accessed via the REST API at `api.segmentapis.com`.

### 2. Event Data (Analytics Events)
Segment also collects **event data** from your applications — the actual analytics events like page views, user identifies, and custom track events. This data can be accessed via:
- **Webhooks**: Segment can send events to a webhook endpoint in real-time
- **AWS S3 Export**: Segment can export events to an S3 bucket

**This connector focuses on the Public API (Type 1).** For event data ingestion (Type 2), consider using Segment's warehouse destinations or webhook integrations.


## **Object List**

The object list for the Segment connector is **static** (defined by the connector based on available API endpoints).

### Public API Objects (Primary Focus)

| Object Name | Description | Primary Endpoint | Ingestion Type |
|-------------|-------------|------------------|----------------|
| `sources` | Data sources configured in the workspace | `GET /sources` | `snapshot` |
| `destinations` | Destinations configured in the workspace | `GET /destinations` | `snapshot` |
| `warehouses` | Data warehouses connected to the workspace | `GET /warehouses` | `snapshot` |
| `catalog_sources` | Available source integrations in the catalog | `GET /catalog/sources` | `snapshot` |
| `catalog_destinations` | Available destination integrations in the catalog | `GET /catalog/destinations` | `snapshot` |
| `catalog_warehouses` | Available warehouse integrations in the catalog | `GET /catalog/warehouses` | `snapshot` |
| `tracking_plans` | Tracking plans defined in the workspace | `GET /tracking-plans` | `snapshot` |
| `users` | Users with access to the workspace | `GET /users` | `snapshot` |
| `labels` | Labels defined in the workspace | `GET /labels` | `snapshot` |
| `audit_events` | Audit trail events for workspace activity | `GET /audit-events` | `snapshot` |
| `spaces` | Engage spaces in the workspace | `GET /spaces` | `snapshot` |
| `reverse_etl_models` | Reverse ETL model configurations | `GET /reverse-etl-models` | `snapshot` |
| `transformations` | Transformation configurations | `GET /transformations` | `snapshot` |
| `usage_api_calls_daily` | Daily API call usage metrics | `GET /usage/api-calls/daily` | `cdc` |
| `usage_mtu_daily` | Daily MTU (Monthly Tracked Users) usage | `GET /usage/mtu/daily` | `cdc` |

### Event Data Objects (Future Extension via Webhooks/S3)

If event data ingestion is added in the future, the following standard tables would be available:

| Object Name | Description | Data Source | Ingestion Type |
|-------------|-------------|-------------|----------------|
| `tracks` | Custom track events from applications | Webhooks/S3 | `append` |
| `identifies` | User identification events | Webhooks/S3 | `append` |
| `pages` | Page view events (web) | Webhooks/S3 | `append` |
| `screens` | Screen view events (mobile) | Webhooks/S3 | `append` |
| `groups` | Group/account association events | Webhooks/S3 | `append` |
| `users` | User traits and properties | Webhooks/S3 | `cdc` |
| `deletes` | Deletion request events | Webhooks | `append` |

**Connector scope for initial implementation**:
- Step 1 focuses on Public API workspace resources: `sources`, `destinations`, `warehouses`, and `tracking_plans`
- Event data tables are documented for future extension


## **Object Schema**

### General Notes

- Segment provides JSON responses for all API endpoints
- Schemas are derived from the API response structure
- Nested JSON objects are modeled as **nested structures** rather than being fully flattened

### `sources` object

**Source endpoint**:  
`GET /sources`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the source |
| `slug` | string | URL-friendly identifier for the source |
| `name` | string | Display name of the source |
| `workspaceId` | string | ID of the workspace containing this source |
| `enabled` | boolean | Whether the source is enabled |
| `writeKeys` | array\<string\> | Write keys associated with this source |
| `metadata` | struct | Metadata about the source integration |
| `metadata.id` | string | Catalog ID of the source type |
| `metadata.slug` | string | Slug of the source type |
| `metadata.name` | string | Name of the source type |
| `metadata.categories` | array\<string\> | Categories for the source type |
| `metadata.description` | string | Description of the source type |
| `metadata.logos` | struct | Logo URLs for the source type |
| `settings` | struct | Source-specific configuration settings |
| `labels` | array\<struct\> | Labels applied to the source |
| `createdAt` | string (ISO 8601 datetime) | Source creation timestamp |
| `updatedAt` | string (ISO 8601 datetime) | Last update timestamp |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/sources"
```

**Example response (truncated)**:

```json
{
  "data": {
    "sources": [
      {
        "id": "qQN7xfkNBn",
        "slug": "my-website",
        "name": "My Website",
        "workspaceId": "9aQ1Lj62S4",
        "enabled": true,
        "writeKeys": ["wk_abc123"],
        "metadata": {
          "id": "javascript",
          "slug": "javascript",
          "name": "JavaScript",
          "categories": ["Website"],
          "description": "Track events from your website"
        },
        "labels": [],
        "createdAt": "2024-01-15T10:30:00Z",
        "updatedAt": "2024-06-20T14:45:00Z"
      }
    ]
  },
  "pagination": {
    "current": "cursor_abc123",
    "totalEntries": 25
  }
}
```


### `destinations` object

**Source endpoint**:  
`GET /destinations`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the destination (Instance ID — specific to this destination in the workspace) |
| `name` | string | Display name of the destination |
| `enabled` | boolean | Whether the destination is enabled |
| `workspaceId` | string | ID of the workspace containing this destination |
| `sourceId` | string | ID of the source connected to this destination |
| `metadata` | struct | Metadata about the destination integration |
| `metadata.id` | string | Catalog ID of the destination type (Meta ID — identifies the integration type, e.g., "Mixpanel") |
| `metadata.slug` | string | Slug of the destination type |
| `metadata.name` | string | Name of the destination type |
| `metadata.description` | string | Description of the destination type |
| `metadata.logos` | struct | Logo URLs for the destination type |
| `metadata.categories` | array\<string\> | Categories for the destination type |
| `settings` | struct | Destination-specific configuration settings |
| `createdAt` | string (ISO 8601 datetime) | Destination creation timestamp |
| `updatedAt` | string (ISO 8601 datetime) | Last update timestamp |

**Important distinction**:
- **Instance ID** (`id`): Specific to a single destination within your workspace
- **Meta ID** (`metadata.id`): Identifies which integration type is set up (e.g., if you have a `dev` Mixpanel and `prod` Mixpanel destination, they share the same Meta ID but have different Instance IDs)

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/destinations"
```


### `warehouses` object

**Source endpoint**:  
`GET /warehouses`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the warehouse |
| `name` | string | Display name of the warehouse |
| `workspaceId` | string | ID of the workspace containing this warehouse |
| `enabled` | boolean | Whether the warehouse is enabled |
| `metadata` | struct | Metadata about the warehouse integration |
| `metadata.id` | string | Catalog ID of the warehouse type |
| `metadata.slug` | string | Slug of the warehouse type (e.g., `bigquery`, `snowflake`) |
| `metadata.name` | string | Name of the warehouse type |
| `metadata.description` | string | Description of the warehouse type |
| `metadata.logos` | struct | Logo URLs for the warehouse type |
| `settings` | struct | Warehouse-specific configuration settings |
| `createdAt` | string (ISO 8601 datetime) | Warehouse creation timestamp |
| `updatedAt` | string (ISO 8601 datetime) | Last update timestamp |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/warehouses"
```


### `tracking_plans` object

**Source endpoint**:  
`GET /tracking-plans`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the tracking plan |
| `slug` | string | URL-friendly identifier for the tracking plan |
| `name` | string | Display name of the tracking plan |
| `description` | string or null | Description of the tracking plan |
| `type` | string | Type of tracking plan (e.g., `LIVE`, `PROPERTY_LIBRARY`) |
| `workspaceId` | string | ID of the workspace containing this tracking plan |
| `createdAt` | string (ISO 8601 datetime) | Tracking plan creation timestamp |
| `updatedAt` | string (ISO 8601 datetime) | Last update timestamp |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/tracking-plans"
```


### `users` object

**Source endpoint**:  
`GET /users`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the user |
| `name` | string | Display name of the user |
| `email` | string | Email address of the user |
| `permissions` | array\<struct\> | Permissions granted to the user |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/users"
```


### `labels` object

**Source endpoint**:  
`GET /labels`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `key` | string | Label key |
| `value` | string | Label value |
| `description` | string or null | Description of the label |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/labels"
```


### `audit_events` object

**Source endpoint**:  
`GET /audit-events`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the audit event |
| `timestamp` | string (ISO 8601 datetime) | When the event occurred |
| `type` | string | Type of audit event (e.g., `source.created`, `destination.updated`) |
| `actor` | struct | Information about who performed the action |
| `actor.id` | string | ID of the actor |
| `actor.type` | string | Type of actor (e.g., `user`, `token`) |
| `actor.email` | string or null | Email of the actor (if user) |
| `resource` | struct | Information about the affected resource |
| `resource.id` | string | ID of the affected resource |
| `resource.type` | string | Type of resource (e.g., `source`, `destination`) |
| `resource.name` | string or null | Name of the affected resource |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/audit-events"
```


### `catalog_sources` object

**Source endpoint**:  
`GET /catalog/sources`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique catalog identifier for the source type |
| `slug` | string | URL-friendly identifier |
| `name` | string | Display name of the source type |
| `description` | string | Description of the source type |
| `categories` | array\<string\> | Categories for the source type |
| `logos` | struct | Logo URLs (default, mark, alt) |
| `isCloudEventSource` | boolean | Whether this is a cloud event source |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/catalog/sources"
```


### `catalog_destinations` object

**Source endpoint**:  
`GET /catalog/destinations`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique catalog identifier for the destination type |
| `slug` | string | URL-friendly identifier |
| `name` | string | Display name of the destination type |
| `description` | string | Description of the destination type |
| `categories` | array\<string\> | Categories for the destination type |
| `logos` | struct | Logo URLs (default, mark, alt) |
| `status` | string | Status of the destination (e.g., `PUBLIC`) |
| `supportedMethods` | struct | Supported tracking methods |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/catalog/destinations"
```


### `spaces` object

**Source endpoint**:  
`GET /spaces`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the space |
| `slug` | string | URL-friendly identifier |
| `name` | string | Display name of the space |
| `createdAt` | string (ISO 8601 datetime) | Space creation timestamp |
| `updatedAt` | string (ISO 8601 datetime) | Last update timestamp |


### `reverse_etl_models` object

**Source endpoint**:  
`GET /reverse-etl-models`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the model |
| `name` | string | Display name of the model |
| `description` | string or null | Description of the model |
| `enabled` | boolean | Whether the model is enabled |
| `sourceId` | string | ID of the associated source |
| `scheduleStrategy` | string | Sync schedule strategy |
| `query` | string | SQL query for the model |
| `queryIdentifierColumn` | string | Column used as identifier |
| `createdAt` | string (ISO 8601 datetime) | Model creation timestamp |
| `updatedAt` | string (ISO 8601 datetime) | Last update timestamp |


### `transformations` object

**Source endpoint**:  
`GET /transformations`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique identifier for the transformation |
| `name` | string | Display name of the transformation |
| `sourceId` | string | ID of the associated source |
| `destinationMetadataId` | string | Destination metadata ID |
| `enabled` | boolean | Whether the transformation is enabled |
| `code` | string | Transformation code/logic |
| `createdAt` | string (ISO 8601 datetime) | Transformation creation timestamp |
| `updatedAt` | string (ISO 8601 datetime) | Last update timestamp |


### `usage_api_calls_daily` object

**Source endpoint**:  
`GET /usage/api-calls/daily`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `timestamp` | string (ISO 8601 datetime) | Date of the usage data (Primary Key) |
| `apiCalls` | integer | Number of API calls made on this date |
| `sourceId` | string or null | Source ID if applicable |
| `workspaceId` | string | Workspace ID |

**Incremental sync**: This stream supports incremental sync using `timestamp` as the cursor field.


### `usage_mtu_daily` object

**Source endpoint**:  
`GET /usage/mtu/daily`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `timestamp` | string (ISO 8601 datetime) | Date of the usage data (Primary Key) |
| `mtu` | integer | Monthly Tracked Users count for this date |
| `sourceId` | string or null | Source ID if applicable |
| `workspaceId` | string | Workspace ID |

**Incremental sync**: This stream supports incremental sync using `timestamp` as the cursor field.


---

## **Event Data Schemas (Future Extension)**

The following schemas document the event data tables that could be ingested via webhooks or S3 export. These are provided for reference if event data ingestion is added as a future extension.

**Data source options**:
- **Webhooks**: Real-time event delivery to a webhook endpoint
- **AWS S3 Export**: Batch export of events to an S3 bucket (30-day retention)

### `tracks` object (Event Data)

The central table containing all custom track events.

| Column Name | Type | Description |
|-------------|------|-------------|
| `message_id` | string | Unique identifier for the event (Primary Key) |
| `anonymous_id` | string | Anonymous user identifier |
| `user_id` | string | Authenticated user identifier |
| `event` | string | Name of the tracked event |
| `timestamp` | timestamp | When the event occurred |
| `sent_at` | timestamp | When the event was sent from the client |
| `received_at` | timestamp | When Segment received the event |
| `context_app_name` | string | Application name |
| `context_app_version` | string | Application version |
| `context_app_build` | string | Application build number |
| `context_device_id` | string | Device identifier |
| `context_device_type` | string | Device type (e.g., ios, android) |
| `context_device_manufacturer` | string | Device manufacturer |
| `context_device_model` | string | Device model |
| `context_ip` | string | IP address |
| `context_library_name` | string | Segment library name |
| `context_library_version` | string | Segment library version |
| `context_locale` | string | User locale |
| `context_os_name` | string | Operating system name |
| `context_os_version` | string | Operating system version |
| `context_page_path` | string | Page path (web) |
| `context_page_referrer` | string | Page referrer URL |
| `context_page_title` | string | Page title |
| `context_page_url` | string | Full page URL |
| `context_user_agent` | string | Browser user agent |
| `context_campaign_name` | string | UTM campaign name |
| `context_campaign_source` | string | UTM campaign source |
| `context_campaign_medium` | string | UTM campaign medium |
| `context_campaign_content` | string | UTM campaign content |
| `context_campaign_term` | string | UTM campaign term |
| `properties_*` | varies | Custom event properties (dynamically added) |


### `identifies` object (Event Data)

User identification events with traits.

| Column Name | Type | Description |
|-------------|------|-------------|
| `message_id` | string | Unique identifier for the event (Primary Key) |
| `anonymous_id` | string | Anonymous user identifier |
| `user_id` | string | Authenticated user identifier |
| `timestamp` | timestamp | When the event occurred |
| `sent_at` | timestamp | When the event was sent |
| `received_at` | timestamp | When Segment received the event |
| `email` | string | User email address |
| `name` | string | User full name |
| `first_name` | string | User first name |
| `last_name` | string | User last name |
| `phone` | string | User phone number |
| `username` | string | Username |
| `gender` | string | User gender |
| `age` | integer | User age |
| `birthday` | timestamp | User birthday |
| `address_city` | string | City |
| `address_country` | string | Country |
| `address_postal_code` | string | Postal code |
| `address_state` | string | State/region |
| `address_street` | string | Street address |
| `avatar` | string | Avatar URL |
| `description` | string | User description |
| `website` | string | User website |
| `context_*` | varies | Context fields (same as tracks) |
| `traits_*` | varies | Custom user traits (dynamically added) |


### `pages` object (Event Data)

Page view events from web applications.

| Column Name | Type | Description |
|-------------|------|-------------|
| `message_id` | string | Unique identifier for the event (Primary Key) |
| `anonymous_id` | string | Anonymous user identifier |
| `user_id` | string | Authenticated user identifier |
| `timestamp` | timestamp | When the event occurred |
| `sent_at` | timestamp | When the event was sent |
| `received_at` | timestamp | When Segment received the event |
| `name` | string | Page name |
| `path` | string | Page path |
| `title` | string | Page title |
| `url` | string | Full page URL |
| `referrer` | string | Referrer URL |
| `search` | string | Search query string |
| `context_*` | varies | Context fields (same as tracks) |
| `properties_*` | varies | Custom page properties (dynamically added) |


### `screens` object (Event Data)

Screen view events from mobile applications.

| Column Name | Type | Description |
|-------------|------|-------------|
| `message_id` | string | Unique identifier for the event (Primary Key) |
| `anonymous_id` | string | Anonymous user identifier |
| `user_id` | string | Authenticated user identifier |
| `timestamp` | timestamp | When the event occurred |
| `sent_at` | timestamp | When the event was sent |
| `received_at` | timestamp | When Segment received the event |
| `name` | string | Screen name |
| `title` | string | Screen title |
| `context_*` | varies | Context fields (same as tracks) |
| `properties_*` | varies | Custom screen properties (dynamically added) |


### `groups` object (Event Data)

Group/account association events.

| Column Name | Type | Description |
|-------------|------|-------------|
| `message_id` | string | Unique identifier for the event (Primary Key) |
| `anonymous_id` | string | Anonymous user identifier |
| `user_id` | string | Authenticated user identifier |
| `group_id` | string | Group/account identifier |
| `timestamp` | timestamp | When the event occurred |
| `sent_at` | timestamp | When the event was sent |
| `received_at` | timestamp | When Segment received the event |
| `name` | string | Group name |
| `industry` | string | Group industry |
| `employees` | string | Number of employees |
| `email` | string | Group email |
| `phone` | string | Group phone |
| `website` | string | Group website |
| `description` | string | Group description |
| `address_city` | string | City |
| `address_country` | string | Country |
| `address_postal_code` | string | Postal code |
| `address_state` | string | State/region |
| `address_street` | string | Street address |
| `context_*` | varies | Context fields (same as tracks) |
| `traits_*` | varies | Custom group traits (dynamically added) |


### `users` object (Event Data)

Aggregated user traits (derived from identifies).

| Column Name | Type | Description |
|-------------|------|-------------|
| `user_id` | string | Authenticated user identifier (Primary Key) |
| `anonymous_id` | string | Last known anonymous identifier |
| `email` | string | User email address |
| `name` | string | User full name |
| `first_name` | string | User first name |
| `last_name` | string | User last name |
| `created_at` | timestamp | When the user was first seen |
| `received_at` | timestamp | When the last identify was received |
| `traits_*` | varies | Custom user traits (dynamically added) |


### Event Data Nested Columns

Event data tables support nested dynamic columns. Custom properties and traits are flattened with prefixes to avoid naming conflicts:
- `properties_*` — Custom event properties from track/page/screen calls
- `traits_*` — Custom user traits from identify calls

For example, a nested property:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "properties": {
    "timestamp": 1600000000
  }
}
```

Is flattened to:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "properties_timestamp": 1600000000
}
```


### Event Data Limitations

- **S3 Export delay**: Events are delayed by ~30 minutes to maintain data integrity
- **Webhook processing**: Events sent to webhook endpoints may take a few minutes to process
- **Data retention**: S3 exports and webhook data typically have a 30-day retention period
- **Re-sync limitation**: Only the last 30 days of data can be re-synced


## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get primary keys for objects.
Instead, primary keys are defined **statically** based on the resource schema.

| Object | Primary Key | Type | Notes |
|--------|-------------|------|-------|
| `sources` | `id` | string | Unique across all sources in the workspace |
| `destinations` | `id` | string | Unique across all destinations in the workspace |
| `warehouses` | `id` | string | Unique across all warehouses in the workspace |
| `tracking_plans` | `id` | string | Unique across all tracking plans in the workspace |
| `users` | `id` | string | Unique across all users |
| `labels` | `key.value` | string (composite) | Labels are keyed by key-value pairs (format: `key.value`) |
| `audit_events` | `id` | string | Unique event identifier |
| `catalog_sources` | `id` | string | Unique catalog identifier |
| `catalog_destinations` | `id` | string | Unique catalog identifier |
| `catalog_warehouses` | `id` | string | Unique catalog identifier |
| `spaces` | `id` | string | Unique space identifier |
| `reverse_etl_models` | `id` | string | Unique model identifier |
| `transformations` | `id` | string | Unique transformation identifier |
| `usage_api_calls_daily` | `timestamp` | string (ISO 8601) | Daily timestamp for usage data |
| `usage_mtu_daily` | `timestamp` | string (ISO 8601) | Daily timestamp for MTU data |


## **Object's Ingestion Type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally
- `snapshot`: Full replacement snapshot; no inherent incremental support
- `append`: Incremental but append-only (no updates/deletes)

Planned ingestion types for Segment objects:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `sources` | `snapshot` | Source configurations change infrequently; full refresh is acceptable |
| `destinations` | `snapshot` | Destination configurations change infrequently; full refresh is acceptable |
| `warehouses` | `snapshot` | Warehouse configurations change infrequently; full refresh is acceptable |
| `tracking_plans` | `snapshot` | Tracking plan metadata changes infrequently; full refresh is acceptable |
| `users` | `snapshot` | User list changes infrequently; full refresh is acceptable |
| `labels` | `snapshot` | Labels change infrequently; full refresh is acceptable |
| `audit_events` | `snapshot` | Audit events are synced as full refresh (no incremental support in API) |
| `catalog_sources` | `snapshot` | Catalog is relatively stable; periodic refresh is sufficient |
| `catalog_destinations` | `snapshot` | Catalog is relatively stable; periodic refresh is sufficient |
| `catalog_warehouses` | `snapshot` | Catalog is relatively stable; periodic refresh is sufficient |
| `spaces` | `snapshot` | Space metadata changes infrequently; full refresh is acceptable |
| `reverse_etl_models` | `snapshot` | Model configurations change infrequently; full refresh is acceptable |
| `transformations` | `snapshot` | Transformation configurations change infrequently; full refresh is acceptable |
| `usage_api_calls_daily` | `cdc` | Usage data supports incremental sync by timestamp |
| `usage_mtu_daily` | `cdc` | Usage data supports incremental sync by timestamp |

For `usage_api_calls_daily` and `usage_mtu_daily`:
- **Primary key**: `timestamp`
- **Cursor field**: `timestamp`
- **Incremental support**: Yes — these are the only streams that support incremental sync
- **Sort order**: Ascending by `timestamp`


## **Read API for Data Retrieval**

### Primary read endpoint for `sources`

- **HTTP method**: `GET`
- **Endpoint**: `/sources`
- **Base URL**: `https://api.segmentapis.com` (US) or `https://eu1.api.segmentapis.com` (EU)

**Query parameters** (for pagination):

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pagination.cursor` | string | no | Cursor for paginating through results |
| `pagination.count` | integer | no | Number of results per page (default varies) |

**Pagination strategy**:
- Segment uses cursor-based pagination
- Response includes a `pagination` object with `current` cursor and `totalEntries`
- To get the next page, use the cursor value from the response in the next request
- Continue until no more results are returned

**Example paginated request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/sources?pagination.count=100"
```

**Rate limits**:
- 200 requests per minute for the Public API
- If rate limited (HTTP 429), implement exponential backoff


### Read endpoints for other objects

All workspace resource endpoints follow a similar pattern:

| Object | Endpoint | Method | Incremental |
|--------|----------|--------|-------------|
| `sources` | `/sources` | GET | No |
| `destinations` | `/destinations` | GET | No |
| `warehouses` | `/warehouses` | GET | No |
| `tracking_plans` | `/tracking-plans` | GET | No |
| `users` | `/users` | GET | No |
| `labels` | `/labels` | GET | No |
| `audit_events` | `/audit-events` | GET | No |
| `catalog_sources` | `/catalog/sources` | GET | No |
| `catalog_destinations` | `/catalog/destinations` | GET | No |
| `catalog_warehouses` | `/catalog/warehouses` | GET | No |
| `spaces` | `/spaces` | GET | No |
| `reverse_etl_models` | `/reverse-etl-models` | GET | No |
| `transformations` | `/transformations` | GET | No |
| `usage_api_calls_daily` | `/usage/api-calls/daily` | GET | Yes |
| `usage_mtu_daily` | `/usage/mtu/daily` | GET | Yes |


### Get single resource by ID

For detailed information about a specific resource:

| Object | Endpoint | Method |
|--------|----------|--------|
| `sources` | `/sources/{sourceId}` | GET |
| `destinations` | `/destinations/{destinationId}` | GET |
| `warehouses` | `/warehouses/{warehouseId}` | GET |
| `tracking_plans` | `/tracking-plans/{trackingPlanId}` | GET |

**Example**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/sources/qQN7xfkNBn"
```


### Incremental read for `audit_events`

The `audit_events` endpoint supports filtering by timestamp for incremental reads:

**Query parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `startTime` | string (ISO 8601) | Filter events after this timestamp |
| `endTime` | string (ISO 8601) | Filter events before this timestamp |
| `resourceId` | string | Filter by affected resource ID |
| `resourceType` | string | Filter by resource type |

**Example incremental request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PUBLIC_API_TOKEN>" \
  -H "Content-Type: application/json" \
  "https://api.segmentapis.com/audit-events?startTime=2024-01-01T00:00:00Z"
```


### Connected resources

Some resources have relationship endpoints:

| Endpoint | Description |
|----------|-------------|
| `/warehouses/{warehouseId}/connected-sources` | List sources connected to a warehouse |
| `/sources/{sourceId}/destinations` | List destinations for a source |
| `/tracking-plans/{trackingPlanId}/sources` | List sources connected to a tracking plan |


## **Field Type Mapping**

### General mapping (Segment JSON → connector logical types)

| Segment JSON Type | Example Fields | Connector Logical Type | Notes |
|-------------------|----------------|------------------------|-------|
| string | `id`, `name`, `slug`, `email` | string | UTF-8 text |
| boolean | `enabled` | boolean | Standard true/false |
| string (ISO 8601 datetime) | `createdAt`, `updatedAt`, `timestamp` | timestamp with timezone | Stored as UTC timestamps |
| object | `metadata`, `settings`, `actor`, `resource` | struct | Represented as nested records |
| array | `writeKeys`, `categories`, `labels`, `permissions` | array\<type\> | Arrays of primitives or nested objects |
| nullable fields | `description` | corresponding type + null | When fields are absent or null |

### Special behaviors and constraints

- All `id` fields are opaque strings (not integers); they should not be parsed or cast
- Timestamp fields use ISO 8601 format in UTC (e.g., `"2024-01-15T10:30:00Z"`)
- Nested structs (`metadata`, `settings`, `actor`, `resource`, etc.) should be represented as nested types
- The `settings` field varies by integration type and contains integration-specific configuration
- Write keys are sensitive credentials and should be handled securely


## **Known Quirks & Edge Cases**

- **Regional endpoints**:
  - US and EU workspaces require different base URLs
  - Using the wrong regional endpoint may result in authentication failures or missing data
  - The connector should accept a `region` configuration parameter (`us` or `eu`)

- **Cursor-based pagination**:
  - Pagination uses opaque cursor strings, not page numbers
  - The connector must store and pass cursor values between requests
  - Empty results or missing cursor indicate the end of pagination

- **Rate limiting**:
  - 200 requests per minute limit
  - HTTP 429 responses require exponential backoff
  - Consider implementing request queuing for large workspaces

- **Token permissions**:
  - Tokens with `workspace:read` scope cannot create or update resources
  - Some endpoints may require higher permission levels
  - Missing permissions result in HTTP 403 errors

- **Catalog vs workspace resources**:
  - Catalog endpoints (`/catalog/*`) return available integration types
  - Workspace endpoints (`/sources`, `/destinations`, etc.) return configured instances
  - These are separate concepts and should not be confused

- **Metadata structure**:
  - The `metadata` field contains information about the integration type from the catalog
  - The `settings` field contains instance-specific configuration
  - Both are nested objects with varying structures

- **API versioning**:
  - The API uses endpoint-level versioning (stable, beta, alpha)
  - Current stable version is v62.0.6
  - Use stable endpoints where available for production connectors

- **CORS restrictions**:
  - The Public API is server-side only
  - Attempting to call from a browser/frontend will result in CORS errors
  - Move all API requests to a server-side implementation

- **Instance ID vs Meta ID (destinations)**:
  - A destination's Instance ID (`id`) is specific to that single destination in your workspace
  - A destination's Meta ID (`metadata.id`) identifies the integration type
  - Multiple destinations of the same type share the same Meta ID but have unique Instance IDs

- **Token exposure and automatic revocation**:
  - Segment partners with GitHub to scan public repositories for exposed tokens
  - Exposed tokens are automatically revoked within seconds
  - Workspace owners receive notifications when tokens are revoked
  - Check the audit trail if a token is revoked to ensure no unauthorized actions were taken

- **Update Schema Settings endpoint quirk**:
  - When using `PATCH /sources/{sourceId}/settings`, if you don't have a source to forward violations or blocked events to, exclude the fields `forwardingViolationsTo` or `forwardingBlockedEventsTo` entirely from the request (do not set them to null)


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://docs.segmentapis.com/ | 2026-01-27 | High | API structure, endpoint categories, versioning (v62.0.6) |
| Official Docs | https://docs.segmentapis.com/tag/Sources/ | 2026-01-27 | High | Sources endpoint availability and structure |
| Official Docs | https://docs.segmentapis.com/tag/Destinations/ | 2026-01-27 | High | Destinations endpoint availability |
| Official Docs | https://docs.segmentapis.com/tag/Warehouses/ | 2026-01-27 | High | Warehouses endpoint and operations |
| Official Docs | https://docs.segmentapis.com/tag/Tracking-Plans/ | 2026-01-27 | High | Tracking Plans endpoint |
| Official Docs | https://docs.segmentapis.com/tag/Workspaces/ | 2026-01-27 | High | Workspaces endpoint |
| Official Docs | https://docs.segmentapis.com/tag/Getting-Started/ | 2026-01-27 | High | Token creation, authentication setup |
| Official Docs | https://docs.segmentapis.com/tag/Pagination/ | 2026-01-27 | High | Cursor-based pagination parameters |
| Official Docs | https://docs.segmentapis.com/tag/Rate-Limits/ | 2026-01-27 | High | Rate limit details (200 req/min) |
| Official Docs | https://docs.segmentapis.com/tag/Error-Handling/ | 2026-01-27 | High | Error codes and response format |
| Official Docs | https://docs.segmentapis.com/tag/Audit-Trail/ | 2026-01-27 | High | Audit events endpoint |
| Official Docs | https://docs.segmentapis.com/tag/Spaces/ | 2026-01-27 | High | Spaces endpoint for Engage |
| Official Docs | https://segment.com/docs/api/public-api/ | 2026-01-27 | High | API overview, token creation, bearer auth |
| Official Docs | https://segment.com/docs/guides/regional-segment | 2026-01-27 | High | Regional endpoints (US vs EU) |
| Official Docs | https://segment.com/docs/connections/rate-limits | 2026-01-27 | High | Rate limits for data ingestion (1000 events/sec) |
| Airbyte Docs | https://docs.airbyte.com/integrations/sources/segment | 2026-01-27 | High | Supported streams, primary keys, sync modes |
| GitHub SDK | https://github.com/segmentio/public-api-sdk-go | 2026-01-27 | Medium | SDK structure, available operations |
| GitHub SDK | https://github.com/segmentio/public-api-sdk-typescript | 2026-01-27 | Medium | SDK structure, model definitions |
| Twilio Docs | https://twilio.com/docs/segment/api/public-api | 2026-01-27 | High | Token creation, CORS restrictions, Instance ID vs Meta ID, token security, troubleshooting |
| Fivetran Docs | https://fivetran.com/docs/connectors/applications/segment | 2026-01-27 | High | Event data schemas (tracks, identifies, pages, screens, groups, users), webhook/S3 ingestion, nested column handling |


## **Sources and References**

- **Official Segment Public API documentation** (highest confidence)
  - `https://docs.segmentapis.com/`
  - `https://docs.segmentapis.com/tag/Sources/`
  - `https://docs.segmentapis.com/tag/Destinations/`
  - `https://docs.segmentapis.com/tag/Warehouses/`
  - `https://docs.segmentapis.com/tag/Tracking-Plans/`
  - `https://docs.segmentapis.com/tag/Getting-Started/`
  - `https://docs.segmentapis.com/tag/Pagination/`
  - `https://docs.segmentapis.com/tag/Rate-Limits/`
  - `https://docs.segmentapis.com/tag/Error-Handling/`
  - `https://docs.segmentapis.com/tag/Audit-Trail/`

- **Official Segment documentation** (highest confidence)
  - `https://segment.com/docs/api/public-api/`
  - `https://segment.com/docs/guides/regional-segment`
  - `https://segment.com/docs/connections/rate-limits`

- **Twilio Segment documentation** (highest confidence)
  - `https://twilio.com/docs/segment/api/public-api` — Token creation, CORS, troubleshooting, security

- **Airbyte Segment source connector** (high confidence)
  - `https://docs.airbyte.com/integrations/sources/segment`
  - Confirmed 14 streams: warehouses, sources, destinations, reverse_etl_models, catalog_destinations, catalog_sources, catalog_warehouses, users, labels, audit_events, transformations, spaces, usage_api_calls_daily, usage_mtu_daily
  - Only usage streams support incremental sync; all others are full sync only
  - Connector version: 0.0.41

- **Fivetran Segment connector** (high confidence)
  - `https://fivetran.com/docs/connectors/applications/segment`
  - Event data schemas for tracks, identifies, pages, screens, groups, users
  - Webhook and S3 ingestion methods, nested column handling with prefixes

- **Official Segment SDKs** (medium confidence)
  - JavaScript/TypeScript: `https://github.com/segmentio/public-api-sdk-typescript`
  - Go: `https://github.com/segmentio/public-api-sdk-go`
  - Java: `https://github.com/segmentio/public-api-sdk-java`
  - Swift and C# SDKs are also available (per Twilio documentation)

When conflicts arise, **official Segment documentation** is treated as the source of truth, with the Airbyte connector and SDKs used to validate practical details like available streams and field structures.
