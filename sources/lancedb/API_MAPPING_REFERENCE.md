# LanceDB API Endpoint and Response Key Mapping Reference

## Quick Reference: Table Name to API Endpoint Mapping

### Overview
LanceDB uses a **consistent REST pattern** where table names are directly embedded in URL paths. There is a **1:1 mapping** between table names and endpoints - no special configuration needed.

### Endpoint Pattern Table

| Operation | Method | Endpoint Pattern | Table Name Usage | Response Keys |
|-----------|--------|------------------|------------------|---------------|
| **List Tables** | GET | `/v1/table/` | N/A | `tables` (array), `page_token` |
| **Describe Schema** | POST | `/v1/table/{table_name}/describe/` | URL path | `name`, `schema`, `num_rows`, `version` |
| **Query/Read Data** | POST | `/v1/table/{table_name}/query/` | URL path | `data` (array), `num_rows` |
| **Insert Data** | POST | `/v1/table/{table_name}/insert/` | URL path | Status/error |
| **Create Table** | POST | `/v1/table/{table_name}/` | URL path | Status/error |
| **Drop Table** | POST | `/v1/table/{table_name}/drop/` | URL path | Status/error |

### Concrete Examples

#### Example 1: Table named "documents"
```
List:     GET  /v1/table/  
          → {"tables": ["documents", "users", "logs"], "page_token": null}

Describe: POST /v1/table/documents/describe/
          → {"name": "documents", "schema": {...}, "num_rows": 15000}

Query:    POST /v1/table/documents/query/
          Body: {"limit": 1000, "offset": 0}
          → {"data": [{...}, {...}, ...], "num_rows": 1000}
```

#### Example 2: Table named "user_embeddings"
```
Describe: POST /v1/table/user_embeddings/describe/
          → {"name": "user_embeddings", "schema": {...}, "num_rows": 50000}

Query:    POST /v1/table/user_embeddings/query/
          Body: {"limit": 500, "filter": "user_id > 1000"}
          → {"data": [{...}, {...}, ...], "num_rows": 500}
```

#### Example 3: Table named "logs-2024" (with hyphen)
```
Describe: POST /v1/table/logs-2024/describe/
Query:    POST /v1/table/logs-2024/query/
```

### Response Key Details

#### List Tables Response
```json
{
  "tables": ["table1", "table2", "table3"],  // Array of table names
  "page_token": "next_page_token_or_null"    // Pagination token
}
```

#### Describe Table Response
```json
{
  "name": "documents",                       // Table name
  "schema": {
    "fields": [                              // Array of field definitions
      {
        "name": "id",                        // Column name
        "type": "int64",                     // Arrow type
        "nullable": false                    // Can be null?
      },
      {
        "name": "text",
        "type": "utf8",
        "nullable": true
      },
      {
        "name": "vector",
        "type": "fixed_size_list<float32>[768]",
        "nullable": false
      }
    ]
  },
  "num_rows": 15000,                         // Total row count
  "version": 5                               // Table version
}
```

#### Query Table Response
```json
{
  "data": [                                  // Array of records
    {
      "id": 1,
      "text": "Sample document",
      "vector": [0.1, 0.2, 0.3, ...]
    },
    {
      "id": 2,
      "text": "Another document",
      "vector": [0.4, 0.5, 0.6, ...]
    }
  ],
  "num_rows": 2                              // Count of returned rows
}
```

## Connector Implementation Mapping

### LakeflowConnect Method to API Endpoint

| Connector Method | API Endpoint | Response Key Used | Purpose |
|------------------|--------------|-------------------|---------|
| `list_tables()` | `GET /v1/table/` | `response['tables']` | Get all table names |
| `get_table_schema(table_name, ...)` | `POST /v1/table/{table_name}/describe/` | `response['schema']['fields']` | Get table schema |
| `read_table(table_name, ...)` | `POST /v1/table/{table_name}/query/` | `response['data']` | Iterate records |

### Code Examples from Implementation

#### list_tables() - Extracting table names
```python
response = self._make_request("GET", "/v1/table/", params=params)
data = response.json()
# Extract from 'tables' key
tables.extend(data.get("tables", []))
page_token = data.get("page_token")
```

#### get_table_schema() - Extracting schema fields
```python
endpoint = f"/v1/table/{quote(table_name)}/describe/"
response = self._make_request("POST", endpoint)
data = response.json()
# Extract from 'schema.fields' nested key
arrow_schema = data.get("schema", {})
fields = arrow_schema.get("fields", [])
```

#### read_table() - Extracting data records
```python
endpoint = f"/v1/table/{quote(table_name)}/query/"
response = self._make_request("POST", endpoint, json_data=query_payload)
data = response.json()
# Extract from 'data' key
records = data.get("data", [])
```

## Key Implementation Details

### URL Encoding
- Table names are URL-encoded using `urllib.parse.quote()`
- Safe characters: alphanumeric, hyphen, underscore
- Example: `my_table-2024` → `my_table-2024` (no change needed)

### Security Validation
Before using table names in URLs, they are validated:
```python
# Only alphanumeric, hyphens, and underscores allowed
if not identifier.replace("-", "").replace("_", "").isalnum():
    raise ValueError(f"Invalid identifier: {identifier}")
```

### Pagination Pattern
```python
# List tables with pagination
while True:
    params = {"limit": 100, "page_token": page_token}
    response = self._make_request("GET", "/v1/table/", params=params)
    data = response.json()
    tables.extend(data.get("tables", []))
    page_token = data.get("page_token")
    if not page_token:
        break
```

### Query Pattern with Filters
```python
# Query table with filters
query_payload = {
    "limit": 1000,
    "offset": 0,
    "filter": "created_at > '2024-01-01'",
    "columns": ["id", "name", "created_at"]
}
endpoint = f"/v1/table/{quote(table_name)}/query/"
response = self._make_request("POST", endpoint, json_data=query_payload)
records = response.json()["data"]
```

## Summary

✅ **1:1 Mapping**: Each table name maps directly to a URL path parameter  
✅ **Consistent Response Keys**: All endpoints use predictable response keys  
✅ **No Configuration**: No need to configure table-to-endpoint mappings  
✅ **URL Safe**: Table names are validated and URL-encoded  
✅ **Well Documented**: Every endpoint and response key is documented in code

---

**Related Documentation:**
- Full API Documentation: `lancedb_api_doc.md`
- Implementation Code: `lancedb.py`
- User Guide: `README.md`

