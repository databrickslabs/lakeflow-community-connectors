# **OSIPI/AVEVA PI Web API Documentation**

## **Authorization**

PI Web API supports multiple authentication methods. For security best practices, Kerberos is recommended within corporate networks.

### Supported Authentication Methods

**1. Basic Authentication**
- Simplest to implement for many use cases
- Uses standard HTTP Basic authentication
- Credentials sent in Authorization header: `Authorization: Basic <base64(username:password)>`

**Example API Request:**
```bash
curl -X GET "https://your-pi-server/piwebapi/assetservers" \
  -H "Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=" \
  -H "Content-Type: application/json"
```

**2. Kerberos Authentication**
- Recommended for production use within corporate networks
- Provides Single Sign-On (SSO) capabilities
- Requires domain configuration and proper delegation setup
- The account running PI Web API must be trusted for delegation and accept protocol transition

**Example API Request:**
```bash
curl -X GET "https://your-pi-server/piwebapi/assetservers" \
  --negotiate -u : \
  -H "Content-Type: application/json"
```

**Configuration Notes:**
- Authentication methods are configured in PI System Explorer under:
  `\\<AFServer>\Configuration\OSIsoft\PI Web API\<PI Web API Instance>\System Configuration`
- Set `AuthenticationMethods` attribute to include "Basic" or "Kerberos"
- For OAuth workflows: The connector **stores** `client_id`, `client_secret`, and `refresh_token`, exchanging them for access tokens at runtime. It **does not** run user-facing OAuth flows.

## **Object List**

The PI System organizes data in a hierarchical structure. Objects are navigable through the PI Web API following this hierarchy:

### Hierarchy Structure
```
AssetServer (AF Server)
  └── AssetDatabase
        └── Element
              └── Attribute (Stream)
                    └── TimeSeries Data

DataServer (PI Data Archive)
  └── Point (PI Tag)
        └── TimeSeries Data
```

### Key Object Types

**1. AssetServer**
- Root container for PI Asset Framework (AF) data
- Contains AssetDatabases
- Endpoint: `GET /assetservers`

**2. AssetDatabase**
- Organizational container within an AssetServer
- Contains Elements, Templates, and hierarchies
- Endpoint: `GET /assetservers/{webId}/assetdatabases`

**3. Element**
- Individual asset or logical grouping
- Contains Attributes and can have child Elements
- Elements represent physical or logical entities (e.g., pumps, buildings, processes)
- Endpoint: `GET /assetdatabases/{webId}/elements`

**4. Attribute (Stream)**
- Data points attached to Elements
- Attributes with Data References are considered "Streams"
- Contain time-series data
- Endpoint: `GET /elements/{webId}/attributes`

**5. DataServer (PI Data Archive)**
- Legacy PI System data storage
- Contains PI Points (tags)
- Endpoint: `GET /dataservers`

**6. Point (PI Tag)**
- Time-series data points in PI Data Archive
- Direct tag-based data storage
- Endpoint: `GET /dataservers/{webId}/points`

### Retrieving Object Lists

**List AssetServers:**
```bash
GET https://{server}/piwebapi/assetservers
```

**List AssetDatabases:**
```bash
GET https://{server}/piwebapi/assetservers/{webId}/assetdatabases
```

**List Elements (children of a database):**
```bash
GET https://{server}/piwebapi/assetdatabases/{webId}/elements
```

**List Elements (children of another element):**
```bash
GET https://{server}/piwebapi/elements/{webId}/elements?selectedfields=items.webid;items.name;items.description;items.path;items.haschildren;items.templatename
```

**Search for Elements:**
```bash
GET https://{server}/piwebapi/elements/search?query=name:Pump*
```

**List Attributes of an Element:**
```bash
GET https://{server}/piwebapi/elements/{webId}/attributes
```

**List PI Points:**
```bash
GET https://{server}/piwebapi/dataservers/{webId}/points?nameFilter=*&maxCount=1000
```

**Example Response (Element List):**
```json
{
  "Items": [
    {
      "WebId": "F1DP...",
      "Name": "Pump001",
      "Description": "Primary circulation pump",
      "Path": "\\\\AFServer\\Database\\Plant\\Pump001",
      "HasChildren": true,
      "TemplateName": "PumpTemplate"
    }
  ]
}
```

## **Object Schema**

Object schemas can be retrieved dynamically using the PI Web API. Each object type exposes its properties and structure.

### Retrieve Element Schema

Elements inherit structure from ElementTemplates. To get the schema:

**Get Element Details:**
```bash
GET https://{server}/piwebapi/elements/{webId}
```

**Response:**
```json
{
  "WebId": "F1DP...",
  "Name": "Pump001",
  "Description": "Primary circulation pump",
  "Path": "\\\\AFServer\\Database\\Plant\\Pump001",
  "TemplateName": "PumpTemplate",
  "HasChildren": true,
  "CategoryNames": ["Equipment"]
}
```

**Get Attributes (Schema Fields):**
```bash
GET https://{server}/piwebapi/elements/{webId}/attributes
```

**Response:**
```json
{
  "Items": [
    {
      "WebId": "A1Ab...",
      "Name": "Temperature",
      "Description": "Operating temperature",
      "Path": "\\\\AFServer\\Database\\Plant\\Pump001|Temperature",
      "Type": "Double",
      "DefaultUnitsName": "deg F",
      "DataReferencePlugIn": "PI Point"
    },
    {
      "WebId": "A1Ac...",
      "Name": "Status",
      "Description": "Operational status",
      "Path": "\\\\AFServer\\Database\\Plant\\Pump001|Status",
      "Type": "String",
      "DefaultUnitsName": ""
    }
  ]
}
```

### Retrieve Attribute (Stream) Schema

**Get Attribute Details:**
```bash
GET https://{server}/piwebapi/attributes/{webId}
```

**Response includes:**
- `Name`: Attribute name
- `Type`: Data type (Int16, Int32, Float32, Float64, String, Boolean, DateTime)
- `DefaultUnitsName`: Unit of measurement
- `DataReferencePlugIn`: Data source type

### Retrieve Point Schema

**Get PI Point Details:**
```bash
GET https://{server}/piwebapi/points/{webId}
```

**Response:**
```json
{
  "WebId": "P1Ab...",
  "Name": "SINUSOID",
  "Path": "\\\\PIServer\\SINUSOID",
  "Descriptor": "Test point",
  "PointClass": "classic",
  "PointType": "Float32",
  "EngineeringUnits": "deg F"
}
```

### Static Schema Considerations

- Element schemas are defined by **ElementTemplates** and can vary
- Attribute data types are constrained by PI System types
- Common types: Int16, Int32, Int64, Float32, Float64, String, Boolean, DateTime
- PI Points have a `PointType` property that defines their data type

## **Get Object Primary Keys**

### WebID: The Universal Primary Key

In PI Web API, the **WebID** serves as the unique identifier for all objects. WebIDs are globally unique, encoded strings that identify resources across the PI System.

### WebID Structure

- WebIDs are base64-encoded strings containing:
  - Object type identifier (first 4 characters)
  - Server identifier
  - Object GUID/UUID
- WebIDs are stable and can be used for persistent references
- Example: `F1DPmygzuFQ0BGql-KH9L0jgVQAVFTQ0VSXUElU0VSVkVS`

### Retrieving WebIDs

**By Path:**
```bash
GET https://{server}/piwebapi/elements?path=\\AFServer\Database\Plant\Pump001
```

**By Search:**
```bash
GET https://{server}/piwebapi/elements/search?query=name:Pump001
```

**Response includes WebId:**
```json
{
  "WebId": "F1DPmygzuFQ0BGql-KH9L0jgVQA...",
  "Name": "Pump001",
  "Path": "\\\\AFServer\\Database\\Plant\\Pump001"
}
```

### Object-Specific Primary Keys

**For Time-Series Data:**
- Primary key is combination of: `WebId` + `Timestamp`
- Each data point has a unique timestamp
- Timestamps are in ISO 8601 format

**For Elements:**
- `WebId` is the primary key
- `Path` provides an alternative human-readable identifier

**For Attributes:**
- `WebId` is the primary key
- Combination of `Element.WebId` + `Attribute.Name` is also unique

## **Object's Ingestion Type**

PI Web API provides time-series data with different retrieval patterns suitable for various ingestion types.

### Ingestion Type Mapping

**1. Append (`append`)**
- **Best fit for:** PI Points and Attributes with continuous data collection
- **Use case:** New data points are continuously added with timestamps
- **Characteristics:**
  - Data is ordered by timestamp
  - Use `startTime` cursor for incremental reads
  - No updates to historical data
  - No native delete tracking

**Example:** Sensor readings, event logs

**2. CDC (Change Data Capture) (`cdc`)**
- **Best fit for:** AF Attributes where values may be updated
- **Use case:** Data points that can be modified after initial write
- **Characteristics:**
  - Use `startTime` cursor to get all values after a point in time
  - May include updates to previously retrieved timestamps
  - No native delete tracking

**Example:** Configuration values, calculated attributes

**3. Snapshot (`snapshot`)**
- **Best fit for:** Static hierarchical data (Elements, AssetDatabases)
- **Use case:** Full refresh of object metadata
- **Characteristics:**
  - Complete data set retrieved each time
  - No incremental read support
  - Typically used for metadata, not time-series data

**Example:** Element hierarchies, attribute definitions

**4. CDC with Deletes (`cdc_with_deletes`)**
- **Not natively supported** by PI Web API
- PI System does not provide a separate API for tracking deleted data points
- Deleted objects (Elements, Attributes) would require full scan to detect

### Recommended Ingestion Types by Object

| Object Type | Recommended Ingestion Type | Notes |
|-------------|---------------------------|-------|
| Element | `snapshot` | Hierarchical metadata |
| Attribute (metadata) | `snapshot` | Schema information |
| Attribute (stream data) | `append` or `cdc` | Time-series values |
| PI Point (stream data) | `append` | Historical data |
| EventFrame | `cdc` | Events with start/end times |

## **Read API for Data Retrieval**

PI Web API provides multiple endpoints for retrieving time-series data, each optimized for different use cases.

### Stream API (Single Stream)

Used for retrieving data from a single attribute or PI point.

**Base URL Pattern:** `https://{server}/piwebapi/streams/{webId}/{method}`

#### 1. Get Recorded Values

Retrieves compressed, stored values within a time range.

**Endpoint:**
```
GET /streams/{webId}/recorded
```

**Parameters:**
- `startTime` (string, optional): Start time (default: `*-1d` for 1 day ago)
- `endTime` (string, optional): End time (default: `*` for current time)
- `maxCount` (integer, optional): Maximum values returned (default: 1000)
- `boundaryType` (string, optional): How to handle boundaries
  - `Inside`: First value after start, last value before end (default)
  - `Outside`: Last value before start, first value after end
  - `Interpolated`: Interpolate values at exact start/end times
- `filterExpression` (string, optional): Filter expression for values
- `includeFilteredValues` (boolean, optional): Include filtered values with flag
- `selectedFields` (string, optional): Semicolon-separated field list
- `timeZone` (string, optional): Time zone for interpreting times

**Sort Order (IMPORTANT):**
- Results are **always returned in ascending timestamp order** when `startTime < endTime`
- This is the default and **cannot be changed** via parameters
- No `sortOrder` or `sortField` parameters exist in the API
- If `endTime < startTime`, results will be in descending order (reverse chronological)

**Example Request:**
```bash
GET https://{server}/piwebapi/streams/{webId}/recorded?startTime=2024-01-01T00:00:00Z&endTime=2024-01-02T00:00:00Z&maxCount=10000
```

**Example Response:**
```json
{
  "Items": [
    {
      "Timestamp": "2024-01-01T00:00:00Z",
      "Value": 72.5,
      "UnitsAbbreviation": "deg F",
      "Good": true,
      "Questionable": false,
      "Substituted": false
    },
    {
      "Timestamp": "2024-01-01T00:01:00Z",
      "Value": 72.8,
      "UnitsAbbreviation": "deg F",
      "Good": true,
      "Questionable": false,
      "Substituted": false
    }
  ]
}
```

#### 2. Get Interpolated Values

Retrieves interpolated values at regular intervals.

**Endpoint:**
```
GET /streams/{webId}/interpolated
```

**Parameters:**
- `startTime` (string, optional): Start time (default: `*-1d`)
- `endTime` (string, optional): End time (default: `*`)
- `interval` (string, optional): Sample interval (e.g., "1h", "30m", "1d")
- `filterExpression` (string, optional): Filter expression
- `includeFilteredValues` (boolean, optional): Include filtered values
- `selectedFields` (string, optional): Field list
- `timeZone` (string, optional): Time zone

**Example Request:**
```bash
GET https://{server}/piwebapi/streams/{webId}/interpolated?startTime=*-7d&endTime=*&interval=1h
```

#### 3. Get Plot Values

Retrieves values optimized for plotting/visualization (typically min/max per interval).

**Endpoint:**
```
GET /streams/{webId}/plot
```

**Parameters:**
- `startTime` (string, optional): Start time
- `endTime` (string, optional): End time
- `intervals` (integer, optional): Number of plot intervals (default: 24, typically matches pixel width)
- `selectedFields` (string, optional): Field list
- `timeZone` (string, optional): Time zone

**Example Request:**
```bash
GET https://{server}/piwebapi/streams/{webId}/plot?startTime=*-30d&endTime=*&intervals=1000
```

#### 4. Get Summary Statistics

Retrieves statistical summaries over a time range.

**Endpoint:**
```
GET /streams/{webId}/summary
```

**Parameters:**
- `startTime` (string, optional): Start time
- `endTime` (string, optional): End time
- `summaryType` (string, optional): Type of summary (Total, Average, Minimum, Maximum, Count, etc.)
- `calculationBasis` (string, optional): Time-weighted or event-weighted
- `timeType` (string, optional): Auto, Most Recent, Current Time

**Example Request:**
```bash
GET https://{server}/piwebapi/streams/{webId}/summary?startTime=*-1d&endTime=*&summaryType=Average
```

#### 5. Get Current Value

Retrieves the most recent value.

**Endpoint:**
```
GET /streams/{webId}/value
```

**Parameters:**
- `time` (string, optional): Time for value (default: current time)
- `desiredUnits` (string, optional): Unit conversion
- `selectedFields` (string, optional): Field list

**Example Request:**
```bash
GET https://{server}/piwebapi/streams/{webId}/value
```

**Example Response:**
```json
{
  "Timestamp": "2024-01-08T15:30:00Z",
  "Value": 73.2,
  "UnitsAbbreviation": "deg F",
  "Good": true
}
```

### StreamSet API (Multiple Streams)

Used for retrieving data from multiple attributes/streams simultaneously. More efficient than individual requests.

**Base URL Pattern:** `https://{server}/piwebapi/streamsets/{operation}`

#### 1. Get Recorded Values (Multiple Streams)

**Endpoint:**
```
GET /streamsets/recorded
```

**Parameters:**
- `webId` (string[], required): Array of stream WebIds
- `startTime` (string, optional): Start time (default: `*-1d`)
- `endTime` (string, optional): End time (default: `*`)
- `maxCount` (integer, optional): Maximum values **TOTAL across all streams** (default: 1000)
  - **IMPORTANT**: When querying N streams with maxCount=M, the limit is divided: each stream gets approximately M/N records
  - Example: maxCount=1000 with 10 streams = ~100 records per stream
- `boundaryType` (string, optional): Boundary handling
- `filterExpression` (string, optional): Filter expression
- `includeFilteredValues` (boolean, optional): Include filtered values
- `selectedFields` (string, optional): Field list
- `timeZone` (string, optional): Time zone

**Sort Order:**
- Results are returned in ascending timestamp order (default)
- This applies across all streams in the streamset

**Example Request:**
```bash
GET https://{server}/piwebapi/streamsets/recorded?webId=A1Ab...&webId=A1Ac...&webId=A1Ad...&startTime=*-1d&endTime=*&maxCount=10000
```

**Example Response:**
```json
{
  "Items": [
    {
      "WebId": "A1Ab...",
      "Name": "Temperature",
      "Path": "\\\\AFServer\\Database\\Plant\\Pump001|Temperature",
      "Items": [
        {
          "Timestamp": "2024-01-01T00:00:00Z",
          "Value": 72.5,
          "Good": true
        }
      ]
    },
    {
      "WebId": "A1Ac...",
      "Name": "Pressure",
      "Path": "\\\\AFServer\\Database\\Plant\\Pump001|Pressure",
      "Items": [
        {
          "Timestamp": "2024-01-01T00:00:00Z",
          "Value": 145.2,
          "Good": true
        }
      ]
    }
  ]
}
```

#### 2. Get Recorded Values (Ad Hoc - Bulk by Element)

Retrieve recorded values for all attributes of elements without specifying individual stream WebIds.

**Endpoint:**
```
GET /streamsets/recorded
```

**Use element WebId instead of individual stream WebIds**

**Example Request:**
```bash
GET https://{server}/piwebapi/streamsets/recorded?webId={elementWebId1}&webId={elementWebId2}&startTime=*-1d&endTime=*
```

This returns all attributes (streams) for the specified elements.

#### 3. Get Interpolated Values (Multiple Streams)

**Endpoint:**
```
GET /streamsets/interpolated
```

**Parameters:** Same as single stream interpolated, plus `webId[]` array

#### 4. Get Values (Current - Multiple Streams)

**Endpoint:**
```
GET /streamsets/value
```

**Parameters:**
- `webId` (string[], required): Array of stream WebIds
- `selectedFields` (string, optional): Field list

### Incremental Data Retrieval (Pagination & Cursors)

**Using Time-based Cursors:**

The PI Web API uses time-based cursors for incremental reads. The `startTime` parameter acts as the cursor.

**Pattern:**
1. Initial read: `startTime=*-30d&endTime=*`
2. Store the timestamp of the last record retrieved
3. Next read: `startTime=2024-01-01T15:30:00Z&endTime=*`

**Important Notes:**
- Always use `maxCount` to limit response size
- Default `maxCount` is 1000, maximum configurable via server settings
- For boundaries, use `boundaryType=Outside` to capture edge values
- Time format: ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) or PI Time expressions (`*-1d`, `*-7d`)

**PI Time Expressions:**
- `*`: Current time
- `*-1d`: 1 day ago
- `*-7d`: 7 days ago
- `*-1h`: 1 hour ago
- `t`: Beginning of today
- `y`: Beginning of yesterday

### Pagination with maxCount

When retrieving large datasets:

```bash
# First page
GET /streams/{webId}/recorded?startTime=*-30d&endTime=*&maxCount=10000

# Check last timestamp in response
# Next page
GET /streams/{webId}/recorded?startTime=2024-01-15T12:00:00Z&endTime=*&maxCount=10000
```

### Deleted Records

PI Web API **does not provide a native API for tracking deleted time-series data points**.

**Deleted Elements/Attributes:**
- No separate endpoint for deleted objects
- Must perform full scan and compare with previous state to detect deletions
- Consider using EventFrames or PI Notifications for tracking changes

### Comparing Read API Options

| Method | Use Case | Pros | Cons |
|--------|----------|------|------|
| **Recorded** | Historical data retrieval | Exact stored values, efficient for compression | May have irregular timestamps |
| **Interpolated** | Regular sampling, reporting | Regular intervals, predictable output size | Not actual stored values |
| **Plot** | Visualization, dashboards | Optimized for charts, min/max per interval | Lossy compression |
| **Summary** | Aggregations, analytics | Statistical insights, reduced data volume | Single value per period |
| **Value** | Current state monitoring | Latest value, minimal payload | Only one value |

### Required Parameters per Table

Different PI Point types may require additional configuration:

**Numeric Points (Float32, Int32, etc.):**
- `desiredUnits`: For unit conversion (e.g., "deg C" to "deg F")

**String Points:**
- No special parameters

**Digital States:**
- Values returned as integers representing state codes
- Use attribute definition to map codes to names

## **Field Type Mapping**

PI Web API uses strongly-typed data based on the PI System's type system.

### PI System Data Types

| PI System Type | Description | Example Values | Spark SQL Equivalent |
|----------------|-------------|----------------|---------------------|
| **Int16** | 16-bit signed integer | -32768 to 32767 | ShortType |
| **Int32** | 32-bit signed integer | -2147483648 to 2147483647 | IntegerType |
| **Int64** | 64-bit signed integer | Large integers | LongType |
| **Float32** | 32-bit floating point | 3.14159, -273.15 | FloatType |
| **Float64** | 64-bit floating point | High-precision decimals | DoubleType |
| **String** | Variable-length text | "Running", "Error" | StringType |
| **Boolean** | True/False | true, false | BooleanType |
| **DateTime** | ISO 8601 timestamp | "2024-01-08T15:30:00Z" | TimestampType |
| **Digital** | Enumerated state | 0, 1, 2 (maps to state names) | IntegerType |
| **Blob** | Binary large object | Binary data | BinaryType |

### OMF Type Formats

OSIsoft Message Format (OMF) specifies detailed type information:

**Integer Types:**
- `int16`: 16-bit signed
- `int32`: 32-bit signed (default)
- `int64`: 64-bit signed

**Floating Point Types:**
- `float32`: Single precision
- `float64`: Double precision (default)

**String Types:**
- `string`: Variable length text

**Timestamp Types:**
- `date-time`: ISO 8601 format with timezone

**Boolean Types:**
- `boolean`: true/false

### Nullable Fields

PI System supports "null" values for data quality:
- `Good`: Valid value present
- `Bad`: No valid value (null equivalent)
- `Questionable`: Value present but uncertain quality

**JSON Representation:**
```json
{
  "Timestamp": "2024-01-08T15:30:00Z",
  "Value": null,
  "Good": false,
  "Questionable": false,
  "Substituted": false
}
```

### Special Field Behaviors

**1. Timestamp Fields:**
- Always in ISO 8601 format
- UTC timezone recommended
- Supports millisecond precision
- Format: `YYYY-MM-DDTHH:MM:SS.fffZ`

**2. Quality Flags:**
- `Good` (boolean): Value is valid
- `Questionable` (boolean): Value quality is uncertain
- `Substituted` (boolean): Value was filled in (not actually recorded)
- `Annotated` (boolean): Value has annotations

**3. UnitsAbbreviation:**
- String field indicating unit of measurement
- Examples: "deg F", "psi", "gpm", "kW"

**4. Digital State Mapping:**
- Digital points store integer values
- State names mapped via DigitalSet configuration
- Example: 0="Off", 1="On", 2="Auto"

### Auto-generated Fields

When retrieving time-series data, these fields are automatically included:

- `Timestamp`: Automatically generated by PI System
- `Good`, `Questionable`, `Substituted`: Auto-calculated quality indicators
- `UnitsAbbreviation`: From point/attribute configuration

### Field Constraints

**Timestamp:**
- Must be unique per stream (primary key component)
- Must be in valid ISO 8601 format
- Cannot be in the future (for recorded values)

**Numeric Values:**
- Must conform to specified type range
- Float32 range: ±3.4E+38
- Int32 range: -2,147,483,648 to 2,147,483,647

**String Values:**
- Maximum length typically 1024 characters
- UTF-8 encoding supported

## **Rate Limits**

PI Web API does not enforce strict, documented rate limits like modern REST APIs (e.g., requests per minute). However, there are practical limits and best practices:

### Server-Side Limits

**1. MaxReturnedItemsPerCall**
- Configuration parameter limiting response size
- Default: **150,000 items** if not configured
- Configurable via PI Web API Admin Utility
- Prevents single requests from overwhelming the server

**2. IIS Concurrent Connection Limits**
- PI Web API runs on IIS (Internet Information Services)
- Default IIS limit: **5,000 concurrent connections**
- Requests exceeding this are queued
- Queue length configurable; excess requests receive HTTP 503

**3. PI Data Archive Limits**
- Backend PI Data Archive has its own performance characteristics
- Requesting millions of data points in a single call will be slow
- Complex queries across many streams can impact server performance

### Best Practices to Avoid Throttling

**1. Use Pagination:**
- Always specify `maxCount` parameter
- Recommended: 1,000 - 10,000 records per request
- Make multiple smaller requests instead of one large request

**2. Use StreamSet API for Bulk Operations:**
- Retrieve multiple streams in one request
- More efficient than N individual requests
- Reduces network overhead

**3. Use Appropriate Time Ranges:**
- Don't request years of data in a single call
- Break large time ranges into chunks (e.g., 1 day, 1 week)

**4. Use Plot or Summary for Large Time Ranges:**
- `plot` endpoint: Get min/max per interval for visualization
- `summary` endpoint: Get aggregates instead of raw data
- Dramatically reduces data volume

**5. Implement Exponential Backoff:**
- If receiving HTTP 503 (Service Unavailable), back off
- Retry with exponential delays: 1s, 2s, 4s, 8s, etc.

**6. Connection Pooling:**
- Reuse HTTP connections
- Don't create new connections for every request

### Performance Recommendations

| Scenario | Recommended Approach | maxCount |
|----------|---------------------|----------|
| Real-time monitoring | Use `value` endpoint, poll every 1-60s | 1 |
| Historical data ingestion | Use `recorded` with time-based pagination | 5,000-10,000 |
| Bulk backfill | Use `recorded` with 1-day chunks | 10,000 |
| Visualization | Use `plot` with intervals matching display | 500-2,000 |
| Reporting/analytics | Use `summary` or `interpolated` | 100-1,000 |
| Multi-stream retrieval | Use `streamsets` API | 1,000-5,000 per stream |

### Error Responses

**HTTP 503 - Service Unavailable:**
- Server is overloaded or queue is full
- **Action:** Implement retry with backoff

**HTTP 429 - Too Many Requests:**
- (Rare) Custom rate limiting configured
- **Action:** Check response headers for retry-after

**HTTP 400 - Bad Request:**
- `maxCount` exceeds server-configured maximum
- **Action:** Reduce `maxCount` parameter

### Monitoring and Diagnostics

- Monitor response times to detect performance degradation
- Track HTTP 503 responses as indicator of overload
- PI Web API exposes performance metrics via:
  - `GET /system/status`: Server health check
  - IIS logs: Track request patterns and errors

## **Sources and References**

### Official API Documentation (Highest Confidence)

1. **AVEVA PI Web API Reference**
   - [StreamSet GetRecordedAdHoc](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/streamset/actions/getrecordedadhoc.html)
   - [Getting Started with PI Web API](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/getting-started.html)
   - [PI Web API Reference - Main](https://docs.aveva.com/bundle/pi-web-api-reference/page/help.html)
   - [StreamSet GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/streamset/actions/getrecorded.html)
   - [StreamSet Controller](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/streamset.html)
   - Confidence: **Official documentation** - Highest

2. **AVEVA PI Web API User Guide**
   - [Authentication Methods](https://docs.aveva.com/bundle/pi-web-api/page/1023024.html)
   - [Restrictions on Request Data Rate](https://docs.aveva.com/bundle/pi-web-api/page/1023029.html)
   - [Configuration Properties](https://docs.aveva.com/bundle/pi-web-api/page/1023116.html)
   - [PI Web API Overview](https://docs.aveva.com/bundle/pi-web-api/page/1023073.html)
   - Confidence: **Official documentation** - Highest

3. **AVEVA PI Server Documentation**
   - [Point Types](https://docs.aveva.com/bundle/pi-server-s-da-admin/page/1022449.html)
   - [Value Types](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/value-types.html)
   - Confidence: **Official documentation** - Highest

### Official Training Materials (High Confidence)

4. **PI World 2020 Lab - Using PI Web API from Beginner to Advanced**
   - [PDF Link](https://osicdn.blob.core.windows.net/learningcontent/PI World 2020 and beyond/PIWorld 2020 Using PI Web API From Beginner to Advanced.pdf)
   - Comprehensive hands-on lab guide
   - Confidence: **Official AVEVA training** - High

5. **OSIsoft Learning Platform**
   - [Using PI Web API from Beginner to Advanced](https://learning.osisoft.com/using-pi-web-api-from-beginner-to-advanced)
   - [Programming in PI Web API](https://learning.osisoft.com/programming-in-pi-web-api)
   - Confidence: **Official training** - High

### Client Library Documentation (High Confidence)

6. **PI Web API Client - Python (SwatiAcharjee)**
   - [StreamSetApi.md](https://github.com/SwatiAcharjee/PI-Web-API-Client-Python/blob/master/docs/api/StreamSetApi.md)
   - [StreamApi.md](https://github.com/SwatiAcharjee/PI-Web-API-Client-Python/blob/master/docs/api/StreamApi.md)
   - Detailed method signatures and parameters
   - Confidence: **Community implementation based on official Swagger spec** - High

7. **PI Web API Client - Python (dcbark01)**
   - [DOCUMENTATION.md](https://github.com/dcbark01/PI-Web-API-Client-Python/blob/master/DOCUMENTATION.md)
   - [StreamApi.md](https://github.com/dcbark01/PI-Web-API-Client-Python/blob/master/docs/api/StreamApi.md)
   - Generated from official Swagger specification
   - Confidence: **Generated from official API spec** - High

8. **PI Web API Client - .NET (jbrabant)**
   - [GitHub Repository](https://github.com/jbrabant/PI-Web-API-Client-DotNet-Standard)
   - .NET Core implementation examples
   - Confidence: **Community implementation** - Medium-High

9. **piwebapi R Package (hongyuanjia)**
   - [GitHub Repository](https://github.com/hongyuanjia/piwebapi)
   - [README.md](https://github.com/hongyuanjia/piwebapi/blob/master/README.md)
   - R client library with examples
   - Confidence: **Community implementation** - Medium

### Community Resources (Medium Confidence)

10. **AVEVA Community (PI Square)**
    - [PI Web API: Quick Start](https://pisquare.osisoft.com/docs/DOC-2512)
    - [How to Retrieve Full Asset Tree](https://pisquare.osisoft.com/thread/9268)
    - [PI Web API Configuration - Max Results](https://community.aveva.com/pi-square-community/f/forum/96416/pi-web-api-configuration-of-maximum-allowed-results)
    - [Using Web ID 2.0 to Optimize Applications](https://community.aveva.com/pi-square-community/b/aveva-blog/posts/pi-web-api-using-web-id-20-to-optimize-your-applications)
    - Confidence: **Community discussions and blogs** - Medium

11. **Sample Code Repositories**
    - [AVEVA Samples - PI System](https://github.com/AVEVA/AVEVA-Samples-PI-System/blob/main/docs/PI-Web-API-Docs/COMMON_ACTION_README.md)
    - [PI Web API Python Examples (bzshang)](https://github.com/bzshang/piwebapi-python-examples)
    - [PI Web API Common Actions Sample](https://github.com/AVEVA/sample-pi_web_api-common_actions-python)
    - Confidence: **Official and community code examples** - High to Medium

### Third-Party Integration Documentation (Medium Confidence)

12. **Integration Guides**
    - [MuleSoft - OSIsoft PI Setup Guide](https://docs.mulesoft.com/manufacturing/latest/osisoft-pi-setup-guide)
    - [Cuurios Blog - Performance Boost with PI Web API](https://cuurios.com/blog/how-to-achieve-a-performance-boost-with-the-pi-web-api)
    - [Software Athlete - Kerberos Delegation Configuration](https://www.software-athlete.com/blogs/resources/how-to-configure-kerberos-for-pi-web-api)
    - Confidence: **Third-party integration guides** - Medium

### Technical Specifications (Medium Confidence)

13. **OMF (OSIsoft Message Format) Documentation**
    - [Type Properties and Formats](https://omf-docs.osisoft.com/documentation_v11/Types/Type_Properties_and_Formats.html)
    - [OMF ReadTheDocs](https://omf-docs.readthedocs.io/en/v1.0/Type_Properties_and_Formats.html)
    - Data type specifications for PI System
    - Confidence: **Official specification** - High

### Legacy Documentation (Medium Confidence)

14. **OSIsoft Legacy Documentation**
    - [PI Web API - OSIsoft TechSupport](https://techsupport.osisoft.com/Documentation/PI-Web-API/)
    - [GetRecordedAdHoc (Legacy)](https://techsupport.osisoft.com/Documentation/PI-Web-API/help/controllers/streamset/actions/getrecordedadhoc.html)
    - Older documentation site, may not reflect latest features
    - Confidence: **Official legacy docs** - High

### Additional Learning Resources

15. **Fledge IoT Documentation**
    - [PI Web API OMF Endpoint](https://fledge-iot.readthedocs.io/en/v2.0.1/OMF.html)
    - [OMF Kerberos Authentication](https://fledge-iot.readthedocs.io/en/latest/KERBEROS.html)
    - Third-party integration examples
    - Confidence: **Third-party implementation** - Medium

### Information Conflicts and Prioritization

**Authentication Methods:**
- All sources consistently mention Basic and Kerberos authentication
- Some sources mention Windows Authentication as a variant of Kerberos
- **Priority:** Official AVEVA documentation

**Rate Limits:**
- Official documentation does not specify hard rate limits
- Community sources mention IIS-level limits and MaxReturnedItemsPerCall
- **Priority:** Official configuration documentation for MaxReturnedItemsPerCall, IIS documentation for connection limits

**Data Types:**
- OMF specification provides most detailed type information
- PI Point documentation specifies available PointTypes
- **Priority:** OMF specification for type formats, PI Point docs for supported types

**WebID Structure:**
- Multiple sources describe WebID encoding
- WebID 2.0 introduced in PI Web API 2017 R2 (1.10)
- **Priority:** AVEVA blog post and official documentation

### Summary of Source Confidence

| Source Category | Confidence Level | Usage |
|----------------|------------------|-------|
| AVEVA Official API Docs | **Highest** | Primary reference |
| AVEVA Official Training | **High** | Best practices, examples |
| Generated Client Libraries (from Swagger) | **High** | Method signatures, parameters |
| AVEVA Community (PI Square) | **Medium-High** | Real-world usage patterns |
| Third-party Integration Guides | **Medium** | Implementation examples |
| Community Implementations | **Medium** | Code patterns, workarounds |

All information in this documentation prioritizes official AVEVA sources. Where community sources provided additional context or examples, they were cross-referenced with official documentation for accuracy.
