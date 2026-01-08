# UiPath Orchestrator Queue Items Connector - Implementation Summary

## Overview

This connector implements data ingestion from UiPath Orchestrator Queue Items into Databricks using the Lakeflow Community Connector framework.

## Files Created

### Core Implementation
- **`uipathqueues.py`** - Main connector implementation following the LakeflowConnect interface
  - OAuth 2.0 Client Credentials Flow authentication
  - 3-step asynchronous export process (initiate, poll, download)
  - CSV parsing with nested structure support
  - Incremental reads using `LastModificationTime` cursor

### Documentation
- **`uipathqueues_api_doc.md`** - Comprehensive API documentation including:
  - OAuth client credentials flow details
  - 3-step export endpoint documentation
  - Complete schema definitions
  - Field type mappings
  - Known quirks and edge cases
  - Research log with citations

- **`README.md`** - User-facing documentation covering:
  - Prerequisites and setup instructions
  - Connection parameter details
  - Table options and configuration
  - Pipeline examples
  - Best practices and troubleshooting

### Configuration
- **`configs/dev_config.json`** - Example connection configuration
- **`configs/dev_table_config.json`** - Example table-specific options

### Testing
- **`test/test_uipathqueues_lakeflow_connect.py`** - Test harness using the standard test suite

## Key Features Implemented

### 1. Authentication
- OAuth 2.0 Client Credentials Flow
- Automatic token refresh when expired (1-hour expiration)
- Token caching to minimize authentication requests
- Support for custom scopes

### 2. Data Retrieval
- **3-Step Export Process**:
  1. **Initiate Export**: POST to `/odata/QueueDefinitions({id})/UiPathODataSvc.Export`
  2. **Poll Status**: GET `/odata/Exports({id})` until status is "Completed"
  3. **Download CSV**: GET download link and retrieve signed CSV

### 3. Schema Support
- Complete schema matching API documentation
- Nested structures for `Robot` and `ReviewerUser`
- Proper type handling (LongType for IDs, TimestampType for dates)
- JSON string fields for flexible `SpecificContent` and `Output`

### 4. Incremental Sync
- Uses `LastModificationTime` as cursor field
- Supports OData filter expressions for efficient incremental reads
- Append-only ingestion type (configurable to CDC if needed)

### 5. Error Handling
- Authentication failures with clear error messages
- Export timeout handling with configurable wait times
- Polling with exponential backoff capability
- CSV parsing error handling

## Implementation Details

### Interface Compliance
✅ All required methods implemented:
- `__init__(options)` - Connection initialization with OAuth setup
- `list_tables()` - Returns `["queue_items"]`
- `get_table_schema(table_name, table_options)` - Returns complete StructType schema
- `read_table_metadata(table_name, table_options)` - Returns primary_keys, cursor_field, ingestion_type
- `read_table(table_name, start_offset, table_options)` - Returns iterator and offset

### Requirements Met
✅ Table name validation at function entry
✅ StructType used for nested structures (not MapType)
✅ No flattening of nested fields
✅ LongType for ID fields to avoid overflow
✅ Cursor field and primary keys for append ingestion type
✅ None assigned for absent StructType fields (not {})
✅ No mock objects
✅ No extra main function
✅ Table-specific parameters in table_options (not connection settings)
✅ Returns raw JSON (no schema conversion in read_table)
✅ Follows patterns from example.py

## Configuration Requirements

### Connection-Level Options (Required)
```json
{
  "organization_name": "your_organization",
  "tenant_name": "your_tenant", 
  "client_id": "your_client_id",
  "client_secret": "your_client_secret",
  "folder_id": "your_folder_id",
  "scope": "OR.Queues OR.Execution"
}
```

### Table-Level Options (Required per table)
```json
{
  "queue_items": {
    "queue_definition_id": "27965",
    "expand": "Robot,ReviewerUser",
    "filter": ""
  }
}
```

## Supported Table

| Table Name    | Ingestion Type | Primary Key | Cursor Field            |
|---------------|----------------|-------------|-------------------------|
| queue_items   | append         | Id          | LastModificationTime    |

## API Rate Limits

- **Export API**: 100 requests/day/tenant
- **Recommendation**: Schedule syncs once or twice daily, not hourly

## Testing

To test the connector:

1. Update `configs/dev_config.json` with real credentials
2. Update `configs/dev_table_config.json` with a valid queue_definition_id
3. Run: `pytest sources/uipathqueues/test/test_uipathqueues_lakeflow_connect.py`

## Known Limitations

1. **Export Rate Limit**: Limited to 100 export requests per day per tenant
2. **Async Export**: Export process is asynchronous and may take several minutes for large queues
3. **Folder-Scoped**: Must specify a folder_id; cannot query across all folders simultaneously
4. **CSV Format Dependency**: Schema depends on UiPath's CSV export format
5. **SpecificContent Schema**: Custom data is stored as JSON string (variable schema)

## Future Enhancements

Potential improvements for future versions:

1. **Multiple Queue Support**: Automatically iterate through all queues in a folder
2. **CDC Mode**: Support treating status changes as updates rather than appends
3. **Direct OData API**: Alternative to export API for smaller, real-time queries
4. **Queue Definition Table**: Add support for reading queue metadata
5. **Transaction Logs**: Support for reading detailed transaction logs
6. **Retry Logic**: Enhanced retry logic for transient export failures
7. **Parallel Exports**: Support for exporting multiple queues in parallel

## References

- API Documentation: `uipathqueues_api_doc.md`
- User Guide: `README.md`
- Implementation: `uipathqueues.py`
- Official Docs: https://docs.uipath.com/orchestrator/automation-cloud/latest/api-guide/queue-items-requests

