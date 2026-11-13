# **ArcGIS Feature Service API Documentation**

## **Authorization**

- **Chosen method**: Token-based authentication (OAuth 2.0 or API Key)
- **Base URL**: `https://<server>/arcgis/rest/services` (configurable for ArcGIS Enterprise or ArcGIS Online)
- **ArcGIS Online Base URL**: `https://services.arcgis.com/<org_id>/arcgis/rest/services`

**Auth placement**:
- Query parameter: `token=<access_token>`
- For public services, no token is required

**Token Generation** (for ArcGIS Online):
- Endpoint: `https://www.arcgis.com/sharing/rest/generateToken`
- Method: POST
- Parameters: `username`, `password`, `client=referer`, `referer=<your_app>`, `expiration=<minutes>`, `f=json`

**API Key authentication** (recommended for production):
- Generate API keys from ArcGIS Developer Dashboard
- Pass via `token` parameter or `X-Esri-Authorization` header

Example authenticated request:

```bash
curl -X GET \
  "https://services.arcgis.com/<org_id>/arcgis/rest/services/<service_name>/FeatureServer/0/query?where=1=1&outFields=*&f=json&token=<ACCESS_TOKEN>"
```

Notes:
- Rate limiting varies by subscription tier (free tier: 2 million basemap tiles/month, 20,000 geocodes/month)
- Token expiration depends on settings (default: 2 hours, max: 2 weeks)


## **Object List**

For connector purposes, we treat ArcGIS Feature Service layers and tables as **objects/tables**.
The object list is **dynamic** (discovered from the Feature Service endpoint).

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `features` | Feature layer records (spatial data) | `GET /<service>/FeatureServer/<layer_id>/query` | `cdc` (using OBJECTID or GlobalID + last_edit_date) |
| `attachments` | File attachments for features | `GET /<service>/FeatureServer/<layer_id>/<feature_id>/attachments` | `snapshot` |

**Connector scope for initial implementation**:
- Primary focus on `features` table from Feature Service layers
- Each Feature Service can have multiple layers (layer_id: 0, 1, 2, etc.)


## **Object Schema**

### General notes

- ArcGIS Feature Services expose schemas dynamically via the layer metadata endpoint
- Schemas are retrieved from `/<service>/FeatureServer/<layer_id>?f=json`
- Fields are typed with Esri field types that map to Spark types

### `features` object (primary table)

**Source endpoint**:
`GET /<service>/FeatureServer/<layer_id>/query`

**Key behavior**:
- Returns features with attributes and optional geometry
- Supports filtering via SQL `where` clause
- Supports pagination via `resultOffset` and `resultRecordCount`

**Dynamic schema** - retrieved from layer metadata:

| Field Property | Description |
|----------------|-------------|
| `name` | Field name |
| `type` | Esri field type (esriFieldTypeOID, esriFieldTypeString, etc.) |
| `alias` | Human-readable field name |
| `length` | Max length for string fields |
| `nullable` | Whether field allows nulls |

**Esri Field Type Mapping**:

| Esri Type | Spark Type | Notes |
|-----------|------------|-------|
| `esriFieldTypeOID` | LongType | Object ID (primary key) |
| `esriFieldTypeGlobalID` | StringType | GUID format |
| `esriFieldTypeString` | StringType | Text fields |
| `esriFieldTypeInteger` | LongType | 32-bit integers |
| `esriFieldTypeSmallInteger` | LongType | 16-bit integers |
| `esriFieldTypeDouble` | DoubleType | Double precision |
| `esriFieldTypeSingle` | DoubleType | Single precision |
| `esriFieldTypeDate` | LongType | Epoch milliseconds |
| `esriFieldTypeGeometry` | StringType (JSON) | Geometry as JSON |
| `esriFieldTypeBlob` | StringType (Base64) | Binary data |
| `esriFieldTypeRaster` | StringType | Raster data |
| `esriFieldTypeGUID` | StringType | GUID format |
| `esriFieldTypeXML` | StringType | XML data |

**Standard fields typically present**:

| Column Name | Type | Description |
|------------|------|-------------|
| `OBJECTID` | LongType | Unique identifier for the feature (primary key) |
| `GlobalID` | StringType (optional) | GUID-based unique identifier |
| `geometry` | StringType (JSON) | Spatial geometry in JSON format |
| `created_date` | LongType (optional) | Creation timestamp (epoch ms) |
| `created_user` | StringType (optional) | User who created the feature |
| `last_edited_date` | LongType (optional) | Last edit timestamp (epoch ms) - cursor field |
| `last_edited_user` | StringType (optional) | User who last edited the feature |

**Example request**:

```bash
curl -X GET \
  "https://services.arcgis.com/<org_id>/arcgis/rest/services/MyService/FeatureServer/0/query?where=1=1&outFields=*&returnGeometry=true&f=json&resultOffset=0&resultRecordCount=1000"
```

**Example response**:

```json
{
  "objectIdFieldName": "OBJECTID",
  "globalIdFieldName": "GlobalID",
  "geometryType": "esriGeometryPoint",
  "spatialReference": {"wkid": 4326},
  "fields": [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID"},
    {"name": "Name", "type": "esriFieldTypeString", "alias": "Name", "length": 255},
    {"name": "Value", "type": "esriFieldTypeDouble", "alias": "Value"}
  ],
  "features": [
    {
      "attributes": {
        "OBJECTID": 1,
        "Name": "Feature 1",
        "Value": 42.5
      },
      "geometry": {
        "x": -122.4194,
        "y": 37.7749,
        "spatialReference": {"wkid": 4326}
      }
    }
  ],
  "exceededTransferLimit": false
}
```


## **Get Object Primary Key**

Primary key information is retrieved from the layer metadata endpoint:

- **Endpoint**: `GET /<service>/FeatureServer/<layer_id>?f=json`
- **Field**: `objectIdField` or `globalIdField`

**Primary key for features**: `OBJECTID` (or field specified by `objectIdFieldName`)
- Type: Long integer
- Property: Unique within the layer

Example from layer metadata:

```json
{
  "id": 0,
  "name": "Points",
  "objectIdField": "OBJECTID",
  "globalIdField": "GlobalID",
  "fields": [...],
  "hasAttachments": false,
  "supportsStatistics": true,
  "supportsAdvancedQueries": true
}
```


## **Object's ingestion type**

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `features` | `cdc` | Features have a stable primary key (OBJECTID) and optionally `last_edited_date` for incremental sync. Supports upserts. |
| `attachments` | `snapshot` | Attachments are typically small and can be re-snapshotted per feature. |

For `features`:
- **Primary key**: `OBJECTID` (or custom objectIdField)
- **Cursor field**: `last_edited_date` (if editor tracking is enabled) or `OBJECTID` for append-only patterns
- **Deletes**: ArcGIS tracks deletes via `queryForDeletes` if archiving is enabled; otherwise treated as CDC updates


## **Read API for Data Retrieval**

### Primary read endpoint for features

- **HTTP method**: GET (or POST for large queries)
- **Endpoint**: `/<service>/FeatureServer/<layer_id>/query`

**Key query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `where` | string | yes | `1=1` | SQL where clause for filtering |
| `outFields` | string | no | `*` | Comma-separated list of fields to return |
| `returnGeometry` | boolean | no | `true` | Whether to include geometry |
| `resultOffset` | integer | no | 0 | Starting record for pagination |
| `resultRecordCount` | integer | no | varies | Number of records per page (max varies by server, typically 1000-2000) |
| `orderByFields` | string | no | none | Fields to sort by (e.g., "OBJECTID ASC") |
| `f` | string | yes | `json` | Output format (json, geojson, pjson) |
| `token` | string | no* | none | Authentication token (*required for secured services) |
| `outSR` | integer | no | none | Output spatial reference WKID |
| `returnIdsOnly` | boolean | no | `false` | Return only OBJECTIDs |
| `returnCountOnly` | boolean | no | `false` | Return only count of matching features |
| `objectIds` | string | no | none | Comma-separated list of specific OBJECTIDs to query |

**Pagination strategy**:
- Use `resultOffset` and `resultRecordCount` for pagination
- Check `exceededTransferLimit` in response; if `true`, more records are available
- Alternatively, query with `returnIdsOnly=true` first, then fetch in batches by `objectIds`

**Incremental strategy** (if editor tracking enabled):
- Use `where=last_edited_date > <cursor>` for CDC
- Track the maximum `last_edited_date` from each batch as the next cursor
- Apply a small lookback window to handle clock skew

**Incremental strategy** (OBJECTID-based):
- For append-only patterns, use `where=OBJECTID > <last_objectid>`
- Track the maximum `OBJECTID` as the cursor

Example incremental read:

```bash
CURSOR_DATE="2024-01-01 00:00:00"
curl -X GET \
  "https://services.arcgis.com/<org_id>/arcgis/rest/services/MyService/FeatureServer/0/query?where=last_edited_date>'${CURSOR_DATE}'&outFields=*&orderByFields=last_edited_date ASC&f=json&resultRecordCount=1000"
```


## **Alternative REST APIs**

Beyond Feature Service, other ArcGIS REST APIs that may be useful for data ingestion:

### 1. Map Service API
- **Use case**: Read-only access to map layers, typically for visualization
- **Endpoint**: `/<service>/MapServer/<layer_id>/query`
- **Note**: Similar query interface but read-only; no editing capabilities

### 2. Table Service (within Feature Service)
- **Use case**: Non-spatial tables within a Feature Service
- **Endpoint**: Same as Feature Service but for layers that are tables (no geometry)
- **Note**: Accessed the same way as features but without geometry

### 3. Geocode Service API
- **Use case**: Convert addresses to coordinates (geocoding) or vice versa
- **Endpoint**: `/findAddressCandidates`, `/reverseGeocode`
- **Note**: Useful for enriching data with location information

### 4. Geometry Service API
- **Use case**: Geometric operations (buffer, intersect, project)
- **Endpoint**: `/buffer`, `/project`, `/simplify`
- **Note**: Useful for post-processing spatial data

### 5. Portal REST API (ArcGIS Online/Enterprise)
- **Use case**: List items, services, users, groups
- **Endpoint**: `https://www.arcgis.com/sharing/rest/content/users/<username>/items`
- **Note**: Useful for discovering available Feature Services

### 6. Query for IDs and Extract
- **Use case**: Large dataset extraction
- **Pattern**: 
  1. Query with `returnIdsOnly=true` to get all OBJECTIDs
  2. Batch fetch features by `objectIds` parameter in chunks


## **Field Type Mapping**

| Esri Field Type | Example Fields | Connector Logical Type | Notes |
|-----------------|----------------|------------------------|-------|
| `esriFieldTypeOID` | `OBJECTID` | LongType | Auto-incrementing ID |
| `esriFieldTypeGlobalID` | `GlobalID` | StringType | GUID format |
| `esriFieldTypeString` | `Name`, `Description` | StringType | Max length defined in schema |
| `esriFieldTypeInteger` | `Count`, `Code` | LongType | Use LongType for safety |
| `esriFieldTypeSmallInteger` | `Priority` | LongType | 16-bit but use LongType |
| `esriFieldTypeDouble` | `Latitude`, `Value` | DoubleType | Double precision |
| `esriFieldTypeSingle` | `Measurement` | DoubleType | Single precision |
| `esriFieldTypeDate` | `created_date`, `last_edited_date` | LongType | Epoch milliseconds |
| `esriFieldTypeGeometry` | `Shape` | StringType | JSON geometry |


## **Known Quirks & Edge Cases**

- **Pagination limits**: `maxRecordCount` varies by server configuration (typically 1000-2000)
- **Date format**: Dates are returned as epoch milliseconds, not ISO strings
- **Geometry formats**: Can be Esri JSON or GeoJSON depending on `f` parameter
- **Null geometry**: Some features may have null geometry (valid for tables or incomplete data)
- **Editor tracking**: `last_edited_date` only available if editor tracking is enabled on the layer
- **Service security**: Some services require tokens even for public data
- **CORS**: Browser-based access may require proxy; direct API calls work fine
- **Spatial reference**: Geometry may be in different coordinate systems; use `outSR` to transform
- **Feature attachments**: Must be queried separately per feature


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.arcgis.com/rest/services-reference/enterprise/feature-service/ | 2024-12-10 | High | Feature Service endpoint structure and capabilities |
| Official Docs | https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/ | 2024-12-10 | High | Query operation parameters and pagination |
| Official Docs | https://developers.arcgis.com/documentation/mapping-apis-and-services/security/ | 2024-12-10 | High | Authentication methods and token management |
| OSS Reference | https://github.com/Esri/arcgis-python-api | 2024-12-10 | High | Python API patterns for querying features |


## **Sources and References**

- **Official ArcGIS REST API documentation**
  - https://developers.arcgis.com/rest/services-reference/enterprise/feature-service/
  - https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/
  - https://developers.arcgis.com/documentation/mapping-apis-and-services/security/
- **ArcGIS Python API** (reference implementation)
  - https://github.com/Esri/arcgis-python-api
  - https://developers.arcgis.com/python/

