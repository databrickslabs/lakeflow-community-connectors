# Lakeflow Zendesk Community Connector

This documentation provides setup instructions and reference information for the Zendesk source connector.

The Lakeflow Zendesk Connector allows you to extract data from your Zendesk instance and load it into your data lake or warehouse. This connector supports both incremental and full refresh synchronization patterns for various Zendesk objects.

## Prerequisites

- Access to a Zendesk instance with API permissions
- Zendesk API token with appropriate read permissions
- Admin or agent role in Zendesk (depending on the data you want to access)

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `subdomain` | string | Yes | Your Zendesk subdomain (the part before .zendesk.com) | `mycompany` |
| `email` | string | Yes | Email address of the Zendesk user with API access | `admin@mycompany.com` |
| `api_token` | string | Yes | Zendesk API token for authentication | `abc123def456ghi789` |

### Table-Level Options

The following options can be set per table via `table_options`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_records_per_batch` | integer | `100000` | Maximum number of records per microbatch. Controls memory usage for incremental tables (tickets, organizations, users, ticket_comments). |

To use table-level options, include them in the `externalOptionsAllowList` connection parameter:

```
max_records_per_batch
```

### Getting Your API Token

1. Log in to your Zendesk instance as an admin
2. Navigate to Admin Center → Apps and integrations → APIs → Zendesk API
3. Enable token access if not already enabled
4. Click the "+" button to create a new API token
5. Copy the generated token (you won't be able to see it again)

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The Zendesk connector supports the following objects with their respective schemas, primary keys, and incremental strategies:

### Tickets
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Complete ticket information including subject, description, status, priority, assignee, requester, custom fields, and metadata
- **Key Fields**: `id`, `subject`, `description`, `status`, `priority`, `requester_id`, `assignee_id`, `organization_id`, `created_at`, `updated_at`

### Organizations
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Organization details including name, domain names, group associations, and custom fields
- **Key Fields**: `id`, `name`, `domain_names`, `group_id`, `created_at`, `updated_at`

### Users
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: User profile information including contact details, roles, permissions, and custom fields
- **Key Fields**: `id`, `email`, `name`, `role`, `organization_id`, `active`, `created_at`, `updated_at`

### Ticket Comments
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Comments and internal notes on tickets with author and timestamp information
- **Key Fields**: `id`, `ticket_id`, `type`, `body`, `author_id`, `created_at`, `updated_at`

### Articles (Help Center)
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Knowledge base articles including content, metadata, and publishing information
- **Key Fields**: `id`, `title`, `body`, `section_id`, `author_id`, `created_at`, `updated_at`

### Groups
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Agent groups for ticket assignment and organization
- **Key Fields**: `id`, `name`, `description`, `default`, `created_at`, `updated_at`

### Brands
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Brand configuration for multi-brand Zendesk instances
- **Key Fields**: `id`, `name`, `brand_url`, `subdomain`, `active`, `created_at`, `updated_at`

### Topics (Community)
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Community forum topics and discussions
- **Key Fields**: `id`, `name`, `description`, `community_id`, `created_at`, `updated_at`


## Data Type Mapping

The Zendesk connector maps source data types to Databricks data types as follows:

| Zendesk Type | Databricks Type | Notes |
|--------------|-----------------|-------|
| String | STRING | Text fields, URLs, email addresses |
| Integer | BIGINT | IDs, counts, numeric values |
| Float | DOUBLE | Numeric values with decimals |
| Boolean | BOOLEAN | True/false flags |
| Date/DateTime | TIMESTAMP | All timestamp fields (created_at, updated_at, etc.) |
| Array | ARRAY | Lists of values (e.g., domain_names, tags) |
| Object/JSON | STRING | Complex nested objects are stored as JSON strings |


## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline
- **Monitor API Limits**: Zendesk has rate limits (200 requests per minute for most endpoints)
- **Use Incremental Sync**: Reduces API calls and improves performance
- **Set Appropriate Schedules**: Balance data freshness requirements with API usage limits
- **Test Thoroughly**: Validate data accuracy and completeness after initial setup

#### Troubleshooting

**Common Issues:**
- **Authentication Errors**: Verify API token and email are correct
- **Rate Limiting**: Reduce sync frequency or implement exponential backoff
- **Missing Data**: Check user permissions and API access levels
- **Schema Changes**: Update connector code if Zendesk adds new fields

**Error Handling:**
The connector includes built-in error handling for common scenarios like rate limiting, network issues, and API errors. Check the pipeline logs for detailed error information and recommended actions.


## References

- [Zendesk API Documentation](https://developer.zendesk.com/api-reference/)
- [Zendesk API Authentication](https://developer.zendesk.com/api-reference/introduction/security-and-auth/)
- [Zendesk API Rate Limits](https://developer.zendesk.com/api-reference/introduction/rate-limits/)
