# Lakeflow UiPath Orchestrator Queue Items Community Connector

This documentation describes how to configure and use the **UiPath Orchestrator Queue Items** Lakeflow community connector to ingest queue item data from UiPath Orchestrator into Databricks.


## Prerequisites

- **UiPath Orchestrator Account**: You need access to a UiPath Orchestrator instance (Automation Cloud or standalone).
- **External Application Registration**: 
  - Must be registered as a confidential application in UiPath with application scopes.
  - Requires `client_id` (App ID) and `client_secret` (App Secret).
  - Minimum scopes:
    - `OR.Queues` - Access to queue definitions and queue items
    - `OR.Execution` - Access to execution data (optional, for Robot/Transaction details)
- **Folder Access**: The application must have access to the specific folder (Organization Unit) where the queues exist.
- **Network Access**: The environment running the connector must be able to reach `https://cloud.uipath.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.


## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name                | Type   | Required | Description                                                                                 | Example                            |
|---------------------|--------|----------|---------------------------------------------------------------------------------------------|-----------------------------------|
| `organization_name` | string | yes      | UiPath organization name                                                                    | `my-organization`                 |
| `tenant_name`       | string | yes      | UiPath tenant name                                                                          | `my-tenant`                       |
| `client_id`         | string | yes      | OAuth App ID from external application registration                                         | `abc123...`                       |
| `client_secret`     | string | yes      | OAuth App Secret from external application registration                                     | `xyz789...`                       |
| `folder_id`         | string | yes      | Organization Unit ID (folder) to query queues from                                          | `770643`                          |
| `scope`             | string | no       | OAuth scopes (defaults to "OR.Queues OR.Execution")                                         | `OR.Queues OR.Execution OR.Default` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed. Required: `queue_definition_id,filter,expand` | `queue_definition_id,filter,expand` |

> **Note**: Table-specific options such as `queue_definition_id` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.


### Obtaining the Required Parameters

1. **Register External Application**:
   - Navigate to your UiPath Orchestrator Admin Console
   - Go to **External Applications** and create a new application
   - Select **Confidential Application** with **Application Scopes**
   - Grant the required scopes: `OR.Queues`, `OR.Execution`
   - Note down the **App ID** (`client_id`) and **App Secret** (`client_secret`)

2. **Get Organization and Tenant Names**:
   - These appear in your UiPath Cloud URL: `https://cloud.uipath.com/{organization_name}/{tenant_name}/`

3. **Get Folder ID**:
   - In Orchestrator, navigate to **Admin → Folders**
   - Select the folder containing your queues
   - The folder ID appears in the URL or folder properties
   - Alternatively, call the `/odata/Folders` API endpoint to list folder IDs

4. **Get Queue Definition ID**:
   - Navigate to **Orchestrator → Queues**
   - Select the queue you want to export
   - The queue definition ID appears in the URL or queue properties
   - Alternatively, call the `/odata/QueueDefinitions` API endpoint to list queues


### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page
2. Select or create a new connection for UiPath Orchestrator
3. Set `externalOptionsAllowList` to `queue_definition_id,filter,expand` (required for this connector to pass table-specific options)

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The UiPath Orchestrator connector currently exposes:

- `queue_items` - Queue items with their data, status, and processing history


### Object Summary, Primary Keys, and Ingestion Mode

| Table         | Description                                           | Ingestion Type | Primary Key           | Incremental Cursor       |
|---------------|-------------------------------------------------------|----------------|-----------------------|--------------------------|
| `queue_items` | Items in a queue with metadata and custom data        | `append`       | `Id` (64-bit integer) | `LastModificationTime`   |

> **Note**: The ingestion type is `append` by default, treating queue items as append-only events. For use cases requiring status updates as modifications, the connector can be configured to use `cdc` ingestion type.


### Required and Optional Table Options

Table-specific options are passed via the pipeline spec under `table` in `objects`:

| Option                 | Type   | Required | Description                                                                                 | Example                          |
|------------------------|--------|----------|---------------------------------------------------------------------------------------------|----------------------------------|
| `queue_definition_id`  | string | yes      | ID of the queue definition to export items from                                             | `"27965"`                        |
| `expand`               | string | no       | Related entities to expand (defaults to "Robot,ReviewerUser")                               | `"Robot,ReviewerUser"`           |
| `filter`               | string | no       | OData filter expression to limit queue items                                                | `"Status eq 'Successful'"`       |


### Schema Highlights

The `queue_items` table includes:

**Core Fields**:
- `Id` - Unique queue item identifier (primary key)
- `QueueDefinitionId` - ID of the queue this item belongs to
- `Name` - Queue name
- `Priority` - Priority level (Low, Normal, High)
- `Status` - Current status (New, InProgress, Failed, Successful, Abandoned, Retried, Deleted)
- `Reference` - User-defined reference

**Timestamps**:
- `CreationTime` - When the item was created
- `LastModificationTime` - Last modification time (incremental cursor)
- `DueDate`, `DeferDate`, `RiskSlaDate` - Scheduling timestamps
- `StartProcessing`, `EndProcessing` - Processing timestamps

**Data Fields**:
- `SpecificContent` - Custom JSON data payload (JSON string)
- `Output` - Processing output data (JSON string)
- `Progress` - Progress information
- `ProcessingException` - Exception details if failed

**Processing Metadata**:
- `SecondsInPreviousAttempts` - Total seconds in previous attempts
- `AncestorId` - Parent queue item ID for retried items
- `RetryNumber` - Number of retries

**Nested Structures**:
- `Robot` - Robot that processed the item (struct with Id, Name, Type)
- `ReviewerUser` - User who reviewed the item (struct with Id, Username, Email)

**Folder Info**:
- `OrganizationUnitId` - Folder ID where the item exists


## Data Type Mapping

UiPath API fields are mapped to Spark types as follows:

| UiPath Type           | Example Fields                                | Connector Type        | Notes |
|-----------------------|-----------------------------------------------|-----------------------|-------|
| integer (32/64-bit)   | `Id`, `QueueDefinitionId`, `OrganizationUnitId`, `RetryNumber` | `LongType` / `IntegerType` | IDs use `LongType` to avoid overflow |
| string                | `Name`, `Priority`, `Status`, `Reference`    | `StringType`          | UTF-8 text |
| ISO 8601 datetime     | `CreationTime`, `LastModificationTime`, `DueDate` | `TimestampType`   | Parsed from ISO 8601 strings |
| JSON string           | `SpecificContent`, `Output`                  | `StringType`          | JSON stored as strings for flexibility |
| object                | `Robot`, `ReviewerUser`                      | `StructType`          | Nested structures preserved |
| nullable fields       | `Reference`, `Output`, `Robot`               | same type + null      | Missing fields are `null`, not `{}` |


## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the UiPath connector source in your workspace.


### Step 2: Configure Your Pipeline

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "uipath_connection",
    "object": [
      {
        "table": {
          "source_table": "queue_items",
          "queue_definition_id": "27965",
          "expand": "Robot,ReviewerUser",
          "filter": ""
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your UiPath credentials
- For each `table`:
  - `source_table` must be `queue_items`
  - `queue_definition_id` is required and identifies which queue to export
  - `expand` and `filter` are optional OData parameters


### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration:

- On the **first run**, all queue items matching the filter will be exported
- On **subsequent runs**, the connector uses `LastModificationTime` as the cursor to fetch only new or modified items


## Export Process Details

The connector uses UiPath's 3-step asynchronous export API:

1. **Initiate Export**: POST request to create an export job
2. **Poll Status**: Repeatedly check export status until completed
3. **Download CSV**: Get signed download URL and retrieve CSV data

**Important Notes**:
- Export API is limited to **100 requests/day/tenant** - plan your sync schedule accordingly
- Exports are asynchronous and may take seconds to minutes depending on data volume
- CSV download URLs are signed and expire quickly - downloads happen immediately after obtaining the link


## Best Practices

- **Start small**: Begin with a single queue to validate configuration
- **Use filters wisely**: Apply OData filters to reduce export size and API usage
- **Monitor rate limits**: The export API has a daily limit of 100 requests per tenant
- **Schedule appropriately**: Given the export limit, consider running syncs once or twice daily rather than hourly
- **Handle large queues**: For queues with millions of items, consider filtering by date ranges or status


## Troubleshooting

Common issues and solutions:

- **Authentication failures (`401` / `403`)**:
  - Verify `client_id` and `client_secret` are correct
  - Ensure the external application has the required scopes (`OR.Queues`, `OR.Execution`)
  - Check that the application has access to the specified folder

- **Export rate limit exceeded**:
  - You've exceeded 100 export requests per day
  - Reduce sync frequency or consolidate multiple queue exports

- **Export timeout**:
  - Large queues may take several minutes to export
  - Increase `max_wait_seconds` parameter if needed
  - Consider adding filters to reduce export size

- **Missing queue definition ID**:
  - Verify the queue exists and you have access to it
  - Call `/odata/QueueDefinitions` API to list available queues

- **Empty exports**:
  - Check your filter expression - it may be excluding all items
  - Verify the queue actually contains items
  - Check that the folder_id is correct


## Known Limitations

- **Daily export limit**: 100 export requests per day per tenant
- **Folder-specific**: Queue items must be queried from a specific folder
- **No incremental updates**: Connector treats data as `append` by default (status changes create new records)
- **CSV format dependency**: Export format depends on UiPath's CSV export structure
- **SpecificContent schema**: Custom data in `SpecificContent` is stored as a JSON string (schema varies by queue)


## References

- Connector implementation: `sources/uipathqueues/uipathqueues.py`
- Connector API documentation: `sources/uipathqueues/uipathqueues_api_doc.md`
- Official UiPath Orchestrator API documentation:
  - `https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/queue-items-requests#exporting-queue-items`
  - `https://docs.uipath.com/automation-cloud/automation-cloud/latest/api-guide/accessing-uipath-resources-using-external-applications`

