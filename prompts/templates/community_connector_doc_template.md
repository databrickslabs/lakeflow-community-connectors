# Lakeflow {{ source name }} Community Connector

This documentation provides setup instructions and reference information for the {{ source name }} source connector.

## Prerequisites
<Please list the prerequisites required to use this connector>
<Example: An account with admin permissions, read permissions, etc.>
<Example: API token or username/password credentials for the account>

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:
<List the parameters with name, type, whether required, description, and example in a table>
<If this connector supports any extra table-specific options (such as options that must be set per table when reading data), list every allowed option name here as a comma-separated string. 
Document these in the `externalOptionsAllowList` connection option. 
If there are such table-specific options, clearly state that `externalOptionsAllowList` is a required connection option, and provide the full, definitive list of all supported options — do not mark this option as optional or provide a sample value.
If no extra table-specific options are supported, make it clear that `externalOptionsAllowList` does not need to be included as a connection parameter.>

### <Add a section describing how to obtain the required parameters>

### Create a Unity Catalog Connection 
A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one. 
3. <Identify any extra table-specific options that can be configured (especially any that are required). List these option names as a comma-separated string in the `externalOptionsAllowList`. Do this for the specific source.>

The connection can also be created using the standard Unity Catalog API.


## Supported Objects
<Describe which objects are supported by this connector.>
<When listing supported object names, specify them using the exact casing required by the implementation.>
<If only a static list of objects is supported, list all of them. Otherwise, describe the supported objects by category or provide a general description>
<Describe the primary key columns of each object, if available>
<Describe the incremental ingestion strategy and cursor field for objects that support incremental reads>
<If the connector supports delete synchronization (cdc_with_deletes), list which objects support it and explain how deleted records are detected (e.g., archived flag, soft delete endpoint)>
<Describe the required and optional configurations needed for each object>
<You don't need to describe the full schema, but highlight any special columns that require attention.>


## Data Type Mapping
<Describe the type mapping between the source system and the ingested table in Databricks using a table format.>


## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. <If an object can have extra configurable options, describe how to use them.> 
   <The example would be things like below>
```json
{
  "pipeline_spec": {
      "connection_name": "...",
      "object": [
        {
            "table": {
                "source_table": "<YOUR_TABLE_NAME>",
                "<option_a>": "...",
                "<option_b>": "...",
            }
        },
        {
            "table": {...}
        }
      ]
  }
}
```
3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline
- **Use Incremental Sync**: Reduces API calls and improves performance
- **Set Appropriate Schedules**: Balance data freshness requirements with API usage limits
- <Add other relevant considerations, such as API rate limits, when applicable to the source>

#### Troubleshooting

**Common Issues:**
<List common issues, if any. Examples: authentication errors, rate limiting, missing data>


## References
<List documentation references>