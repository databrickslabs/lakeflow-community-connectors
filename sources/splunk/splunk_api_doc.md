# **Splunk API Documentation**

This document provides implementation-ready API documentation for building a Splunk Metrics Catalog connector. It covers authentication, available objects, schemas, data retrieval, and incremental strategies.

---

## **Authorization**

### Preferred Method: Basic Authentication

Splunk REST API uses HTTP Basic Authentication with username and password credentials. This is the recommended method for connector implementation.

**Authentication Parameters:**

| Parameter | Location | Required | Description |
|-----------|----------|----------|-------------|
| `username` | Basic Auth Header | Yes | Splunk user account with appropriate permissions |
| `password` | Basic Auth Header | Yes | Password for the Splunk user account |

**Required Capabilities:**
- `list_metrics_catalog` - Required for GET operations on metrics catalog endpoints
- `edit_metrics_rollup` - Required for POST and DELETE operations on rollup policies

**Base URL Format:**
```
https://<splunk-instance>:8089/services/
```

- Replace `<splunk-instance>` with your Splunk server's hostname or IP address
- Default management port is `8089`
- HTTPS is required; use SSL certificate validation in production

**Example API Request:**

```bash
curl -k -u admin:changeme \
  "https://<splunk-instance>:8089/services/catalog/metricstore/metrics"
```

**Python HTTP Example:**

```python
import requests
from requests.auth import HTTPBasicAuth

base_url = "https://<splunk-instance>:8089"
auth = HTTPBasicAuth("admin", "changeme")

response = requests.get(
    f"{base_url}/services/catalog/metricstore/metrics",
    auth=auth,
    verify=False  # Set to True with proper SSL certs in production
)
```

**Note:** Alternative token-based authentication via `/services/auth/login` exists but Basic Auth is simpler for connector implementation and doesn't require token refresh logic.

---

## **Object List**

The Splunk Metrics Catalog connector provides access to the following objects:

| Object Name | Description | API Endpoint | List Retrieval |
|-------------|-------------|--------------|----------------|
| `metrics` | All metric names in the metric store | `GET /services/catalog/metricstore/metrics` | Dynamic via API |
| `dimensions` | Dimension names associated with metrics | `GET /services/catalog/metricstore/dimensions` | Dynamic via API |
| `dimension_values` | Values for a specific dimension | `GET /services/catalog/metricstore/dimensions/{dimension-name}/values` | Dynamic via API |
| `rollup_policies` | Metric indexes and their rollup summaries | `GET /services/catalog/metricstore/rollup` | Dynamic via API |

**Object List is Dynamic:** The available metrics, dimensions, and their values depend on what data has been ingested into your Splunk instance. Use the API endpoints to discover available objects.

**Example: List All Metrics**

```bash
curl -k -u admin:changeme \
  "https://<splunk-instance>:8089/services/catalog/metricstore/metrics"
```

**Example Response:**
```xml
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>cpu.utilization</title>
    <content type="text/xml">
      <s:dict xmlns:s="http://dev.splunk.com/ns/rest">
        <s:key name="metric_name">cpu.utilization</s:key>
        <s:key name="index">main</s:key>
      </s:dict>
    </content>
  </entry>
  <entry>
    <title>memory.usage</title>
    <content type="text/xml">
      <s:dict xmlns:s="http://dev.splunk.com/ns/rest">
        <s:key name="metric_name">memory.usage</s:key>
        <s:key name="index">main</s:key>
      </s:dict>
    </content>
  </entry>
</feed>
```

---

## **Object Schema**

Schemas for Splunk Metrics Catalog objects are determined by the API response structure. There is no separate schema retrieval endpoint; the schema is embedded in the response format.

### Metrics Object Schema

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `metric_name` | STRING | No | Unique name of the metric |
| `index` | STRING | No | Index containing the metric |
| `dimensions` | OBJECT | Yes | Key-value pairs of dimension names and values |

**Example Response:**
```xml
<feed>
  <entry>
    <title>cpu.utilization</title>
    <content type="text/xml">
      <s:dict>
        <s:key name="metric_name">cpu.utilization</s:key>
        <s:key name="index">main</s:key>
        <s:key name="dimensions">
          <s:dict>
            <s:key name="host">server1</s:key>
            <s:key name="region">us-west</s:key>
          </s:dict>
        </s:key>
      </s:dict>
    </content>
  </entry>
</feed>
```

### Dimensions Object Schema

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `dimension_name` | STRING | No | Unique name of the dimension |

**Example Response:**
```xml
<feed>
  <entry>
    <title>host</title>
    <content type="text/xml">
      <s:dict>
        <s:key name="dimension_name">host</s:key>
      </s:dict>
    </content>
  </entry>
</feed>
```

### Dimension Values Object Schema

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `value` | STRING | No | The dimension value |

**Example Response:**
```xml
<feed>
  <entry>
    <title>server1</title>
    <content type="text/xml">
      <s:dict>
        <s:key name="value">server1</s:key>
      </s:dict>
    </content>
  </entry>
</feed>
```

### Rollup Policy Object Schema

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `index` | STRING | No | Source metric index name |
| `defaultAggregation` | STRING | Yes | Default aggregation functions separated by `#` |
| `metricList` | STRING | Yes | Comma-separated list of metric names |
| `metricListType` | STRING | Yes | `included` or `excluded` |
| `dimensionList` | STRING | Yes | Comma-separated list of dimensions |
| `dimensionListType` | STRING | Yes | `included` or `excluded` |
| `summaries` | ARRAY[OBJECT] | No | Rollup summary configurations |
| `summaries[].rollupIndex` | STRING | No | Target rollup index name |
| `summaries[].span` | STRING | No | Time span for rollup (e.g., `1h`, `1d`) |

**Example Response:**
```xml
<entry>
  <title>index_s</title>
  <content type="text/xml">
    <s:dict>
      <s:key name="defaultAggregation">avg#max</s:key>
      <s:key name="metricList">foo3,foo4</s:key>
      <s:key name="metricListType">excluded</s:key>
      <s:key name="dimensionList">app,region</s:key>
      <s:key name="dimensionListType">included</s:key>
      <s:key name="summaries">
        <s:dict>
          <s:key name="0">
            <s:dict>
              <s:key name="rollupIndex">index_d_1h</s:key>
              <s:key name="span">1h</s:key>
            </s:dict>
          </s:key>
        </s:dict>
      </s:key>
    </s:dict>
  </content>
</entry>
```

---

## **Get Object Primary Key**

Primary keys are static and determined by the object type:

| Object | Primary Key Field | Type | Description |
|--------|-------------------|------|-------------|
| `metrics` | `metric_name` | STRING | Unique identifier for a metric |
| `dimensions` | `dimension_name` | STRING | Unique identifier for a dimension |
| `dimension_values` | `value` | STRING | The dimension value itself (unique per dimension) |
| `rollup_policies` | `index` | STRING | Source metric index name |

There is no API to retrieve primary keys dynamically; they are embedded in the response structure as shown above.

---

## **Object's Ingestion Type**

| Object | Ingestion Type | Cursor Field | Notes |
|--------|----------------|--------------|-------|
| `metrics` | `snapshot` | N/A | Full list refresh; no incremental cursor available |
| `dimensions` | `snapshot` | N/A | Full list refresh; no incremental cursor available |
| `dimension_values` | `snapshot` | N/A | Full list refresh; no incremental cursor available |
| `rollup_policies` | `snapshot` | N/A | Full list refresh; policies can be created/updated/deleted but no change tracking |

**Incremental Strategy:**
- The Metrics Catalog endpoints do not provide change tracking (no `updated_at` or similar fields)
- Each sync should perform a full snapshot of the catalog data
- For `rollup_policies`, changes are managed via POST/DELETE but there's no audit log

**Delete Handling:**
- Deleted metrics/dimensions naturally disappear from subsequent API responses
- No separate "deleted records" endpoint exists
- Compare current snapshot with previous to detect deletions

---

## **Read API for Data Retrieval**

### List Metrics

**Endpoint:** `GET /services/catalog/metricstore/metrics`

**Description:** Retrieves all metric names in the metric store.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `index` | string | No | Filter metrics by index name |
| `metric_name` | string | No | Filter by specific metric name |
| `dimension` | string | No | Filter by dimension name |
| `dimension_value` | string | No | Filter by dimension value |
| `count` | integer | No | Number of entries to return (pagination) |
| `offset` | integer | No | Starting index for pagination (0-based) |
| `output_mode` | string | No | Response format: `xml` (default), `json` |

**Example Request:**
```bash
curl -k -u admin:changeme \
  "https://<splunk-instance>:8089/services/catalog/metricstore/metrics?output_mode=json&count=100&offset=0"
```

**Python Example:**
```python
import requests
from requests.auth import HTTPBasicAuth

def get_metrics(base_url, auth, index=None, count=100, offset=0):
    params = {
        "output_mode": "json",
        "count": count,
        "offset": offset
    }
    if index:
        params["index"] = index
    
    response = requests.get(
        f"{base_url}/services/catalog/metricstore/metrics",
        auth=auth,
        params=params,
        verify=False
    )
    response.raise_for_status()
    return response.json()
```

---

### List Dimensions

**Endpoint:** `GET /services/catalog/metricstore/dimensions`

**Description:** Retrieves all dimension names associated with metrics.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `index` | string | No | Filter dimensions by index |
| `metric_name` | string | No | Filter by specific metric name |
| `count` | integer | No | Number of entries to return |
| `offset` | integer | No | Starting index for pagination |
| `output_mode` | string | No | Response format: `xml` (default), `json` |

**Example Request:**
```bash
curl -k -u admin:changeme \
  "https://<splunk-instance>:8089/services/catalog/metricstore/dimensions?output_mode=json"
```

---

### List Dimension Values

**Endpoint:** `GET /services/catalog/metricstore/dimensions/{dimension-name}/values`

**Description:** Retrieves all values for a specified dimension.

**Path Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dimension-name` | string | Yes | The name of the dimension |

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `index` | string | No | Filter by index |
| `metric_name` | string | No | Filter by specific metric name |
| `count` | integer | No | Number of entries to return |
| `offset` | integer | No | Starting index for pagination |
| `output_mode` | string | No | Response format: `xml` (default), `json` |

**Example Request:**
```bash
curl -k -u admin:changeme \
  "https://<splunk-instance>:8089/services/catalog/metricstore/dimensions/host/values?output_mode=json"
```

---

### List Rollup Policies

**Endpoint:** `GET /services/catalog/metricstore/rollup`

**Description:** Retrieves all rollup policies.

**Example Request:**
```bash
curl -k -u admin:changeme \
  "https://<splunk-instance>:8089/services/catalog/metricstore/rollup?output_mode=json"
```

---

### Get Rollup Policy for Index

**Endpoint:** `GET /services/catalog/metricstore/rollup/{index}`

**Description:** Retrieves rollup policy for a specific source index.

**Path Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `index` | string | Yes | The source metric index name |

**Example Request:**
```bash
curl -k -u admin:changeme \
  "https://<splunk-instance>:8089/services/catalog/metricstore/rollup/my_metrics_index?output_mode=json"
```

---

### Pagination

All list endpoints support pagination using `count` and `offset` parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `count` | integer | 30 | Number of entries to return per request |
| `offset` | integer | 0 | Starting index (0-based) |

**Pagination Strategy:**
1. Start with `offset=0` and desired `count`
2. Check response for total count or if results returned equals `count`
3. Increment `offset` by `count` for next page
4. Continue until fewer results than `count` are returned

**Python Pagination Example:**
```python
def get_all_metrics(base_url, auth, page_size=100):
    all_metrics = []
    offset = 0
    
    while True:
        response = requests.get(
            f"{base_url}/services/catalog/metricstore/metrics",
            auth=auth,
            params={"output_mode": "json", "count": page_size, "offset": offset},
            verify=False
        )
        response.raise_for_status()
        data = response.json()
        
        entries = data.get("entry", [])
        all_metrics.extend(entries)
        
        if len(entries) < page_size:
            break
        offset += page_size
    
    return all_metrics
```

---

## **Field Type Mapping**

| Splunk API Type | Spark/Databricks Type | Description |
|-----------------|----------------------|-------------|
| String | StringType | Text fields (metric names, dimension names, values) |
| Integer | LongType | Numeric identifiers, counts |
| Float/Double | DoubleType | Metric values with decimal precision |
| Boolean | BooleanType | True/false flags |
| Timestamp | TimestampType | Time-related fields (when present) |
| XML Dict/Object | StructType or StringType (JSON) | Nested objects; use StructType for known structure |
| Array/List | ArrayType | Collections (e.g., summaries list) |

**Special Field Behaviors:**
- **Aggregation functions**: Stored as `#`-separated strings (e.g., `avg#max#min`)
- **List fields**: Stored as comma-separated strings (e.g., `app,region`)
- **Nested summaries**: Array of objects with `span` and `rollupIndex` properties

---

## **Write API**

### Create Rollup Policy

**Endpoint:** `POST /services/catalog/metricstore/rollup`

**Description:** Creates a new rollup policy for a metric index.

**Required Capability:** `edit_metrics_rollup`

**Request Parameters (form-encoded):**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Source metric index name |
| `summaries` | string | Yes | Rollup summaries in format `<span>\|<rollup_index>` (comma-separated for multiple) |
| `default_agg` | string | No | Aggregation functions separated by `#` (default: `avg`) |
| `metric_list` | string | No | Comma-separated list of metric names |
| `metric_list_type` | string | No | `included` or `excluded` (default: `excluded`) |
| `dimension_list` | string | No | Comma-separated list of dimensions |
| `dimension_list_type` | string | No | `included` or `excluded` |
| `metric_overrides` | string | No | Metric-specific aggregations: `<metric>\|<agg1>#<agg2>` (comma-separated) |

**Example Request:**
```bash
curl -k -u admin:changeme \
  "https://<splunk-instance>:8089/services/catalog/metricstore/rollup" \
  -d name=my_metrics_index \
  -d default_agg=avg#max \
  -d dimension_list="host,region" \
  -d dimension_list_type=included \
  -d summaries="1h|my_metrics_1h,1d|my_metrics_1d"
```

---

### Update Rollup Policy

**Endpoint:** `POST /services/catalog/metricstore/rollup/{index}`

**Description:** Updates an existing rollup policy.

**Path Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `index` | string | Yes | Source metric index name |

**Request Parameters:** Same as Create Rollup Policy.

---

### Delete Rollup Policy

**Endpoint:** `DELETE /services/catalog/metricstore/rollup/{index}`

**Description:** Deletes a rollup policy.

**Example Request:**
```bash
curl -k -u admin:changeme -X DELETE \
  "https://<splunk-instance>:8089/services/catalog/metricstore/rollup/my_metrics_index"
```

---

## **Known Quirks & Edge Cases**

1. **XML Default Response**: Splunk REST API returns XML by default. Use `output_mode=json` parameter for JSON responses.

2. **SSL Certificate**: Self-signed certificates are common in Splunk deployments. Handle SSL verification appropriately.

3. **No Incremental Cursors**: The Metrics Catalog API doesn't provide change tracking. Full snapshot sync is required.

4. **Rate Limiting**: Splunk doesn't have documented rate limits for these endpoints, but excessive requests may impact performance.

5. **Capability Requirements**: Ensure the authenticated user has required capabilities (`list_metrics_catalog`, `edit_metrics_rollup`).

---

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://help.splunk.com/en/splunk-enterprise/rest-api-reference/10.0/metrics-catalog-endpoints/metrics-catalog-endpoint-descriptions | 2025-12-17 | High | Metrics Catalog endpoint structure, parameters |
| Official Docs | https://docs.splunk.com/Documentation/Splunk/9.4.2/RESTREF/RESTlist | 2025-12-17 | High | REST API endpoint list, authentication |
| Official Docs | https://docs.splunk.com/Documentation/Splunk/9.4.2/RESTREF/RESTmetrics | 2025-12-17 | High | Metrics-specific endpoints |
| Official Docs | https://docs.splunk.com/Documentation/Splunk/9.4.2/Metrics/MrollupAppContext | 2025-12-17 | High | Rollup policy creation, parameters |
| Official Docs | https://docs.splunk.com/Documentation/Splunk/latest/RESTREF | 2025-12-17 | High | General REST API reference |

**Note:** No Airbyte, Singer, or dltHub implementations for Splunk Metrics Catalog were found during research. The documentation is based entirely on official Splunk documentation.

---

## **Sources and References**

| Source | Confidence Level | URL |
|--------|------------------|-----|
| Splunk REST API Reference Manual | Highest - Official | https://docs.splunk.com/Documentation/Splunk/latest/RESTREF |
| Metrics Catalog Endpoint Descriptions | Highest - Official | https://help.splunk.com/en/splunk-enterprise/rest-api-reference/10.0/metrics-catalog-endpoints/metrics-catalog-endpoint-descriptions |
| Metric Rollup Policies Documentation | Highest - Official | https://docs.splunk.com/Documentation/Splunk/9.4.2/Metrics/MrollupAppContext |
| Splunk REST API Endpoints Reference List | High - Official | https://docs.splunk.com/Documentation/Splunk/9.4.2/RESTREF/RESTlist |
| Splunk REST API Metrics Reference | High - Official | https://docs.splunk.com/Documentation/Splunk/9.4.2/RESTREF/RESTmetrics |

**Conflict Resolution:** All information is sourced from official Splunk documentation. No conflicts were encountered.

---

## **Acceptance Checklist**

- [x] All required headings present and in order
- [x] Every field in each schema is listed
- [x] Exactly one authentication method is documented (Basic Auth)
- [x] Endpoints include params, examples, and pagination details
- [x] Incremental strategy defined (snapshot-based, no cursor)
- [x] Research Log completed with sources and timestamps
- [x] Sources include full URLs with confidence levels
- [ ] TBD items: None identified

