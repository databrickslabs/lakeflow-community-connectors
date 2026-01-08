# **MongoDB API Documentation**

## **Authorization**

- **Chosen method**: MongoDB Connection URI (connection string) for PyMongo driver authentication.
- **Driver**: PyMongo (official Python driver for MongoDB)
- **Connection URI format**: `mongodb://[username:password@]host[:port][/[defaultauthdb][?options]]`
- **Auth placement**: Authentication credentials are embedded in the connection URI string.

**Supported authentication mechanisms**:
- `SCRAM-SHA-256` (default for MongoDB 4.0+)
- `SCRAM-SHA-1` (legacy, MongoDB 3.0+)
- `MONGODB-CR` (deprecated, pre-3.0)
- `MONGODB-X509` (certificate-based authentication)
- `GSSAPI` (Kerberos)
- `PLAIN` (LDAP SASL)

For this connector, we prioritize **SCRAM-SHA-256** as the preferred authentication method, which is the default mechanism in modern MongoDB deployments.

**Connection URI examples**:

```python
# Standard authentication with username and password
connection_uri = "mongodb://myUser:myPassword@localhost:27017/myDatabase"

# MongoDB Atlas cloud connection (with TLS)
connection_uri = "mongodb+srv://myUser:myPassword@cluster0.mongodb.net/myDatabase"

# Replica set connection
connection_uri = "mongodb://myUser:myPassword@host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet"

# With explicit authentication database
connection_uri = "mongodb://myUser:myPassword@localhost:27017/myDatabase?authSource=admin"
```

**PyMongo client initialization**:

```python
from pymongo import MongoClient

client = MongoClient(connection_uri)
db = client.get_database()  # Uses database from URI
# or
db = client['myDatabase']  # Explicit database name
```

**Required permissions** (MongoDB user roles):
- `read` role on target database(s) for basic data retrieval
- `readAnyDatabase` role for listing all databases
- For Change Streams (CDC): requires replica set or sharded cluster, and `read` access to the oplog

**Connection options** (common URI parameters):
- `authSource`: Database name for authentication (default: database in URI or `admin`)
- `replicaSet`: Replica set name (required for Change Streams)
- `tls` / `ssl`: Enable TLS/SSL encryption (default: `false`)
- `retryWrites`: Enable retryable writes (default: `true` for MongoDB 3.6+)
- `maxPoolSize`: Maximum connection pool size (default: `100`)
- `serverSelectionTimeoutMS`: Timeout for server selection (default: `30000` ms)

**Notes**:
- The connector will **not** perform interactive authentication flows; the connection URI with embedded credentials must be provided in configuration.
- For MongoDB Atlas, use the `mongodb+srv://` protocol which automatically handles TLS and replica set discovery.
- Rate limiting is not enforced by MongoDB at the API level, but network and cluster resource limits apply.


## **Object List**

MongoDB organizes data into **databases** containing **collections** (analogous to tables in relational databases). The object list for a MongoDB connector is **dynamic** and discovered at runtime using the PyMongo API.

**Discovery approach**:
1. **List databases**: `client.list_database_names()`
2. **List collections per database**: `db.list_collection_names()`

**Example discovery code**:

```python
from pymongo import MongoClient

client = MongoClient(connection_uri)

# List all databases
database_names = client.list_database_names()
print(f"Databases: {database_names}")

# List collections in a specific database
db = client['myDatabase']
collection_names = db.list_collection_names()
print(f"Collections in myDatabase: {collection_names}")
```

**Example output**:

```python
Databases: ['admin', 'config', 'local', 'myDatabase', 'testDB']
Collections in myDatabase: ['users', 'orders', 'products', 'logs']
```

**System databases and collections** (typically excluded from connectors):
- Databases: `admin`, `config`, `local`
- Collections starting with `system.` (e.g., `system.indexes`, `system.users`)

**Layering**: Collections are always layered under a specific database. When configuring the connector, the user must specify:
- `database`: The database name (required)
- Optionally, a list of `collections` to sync (if not specified, sync all non-system collections)

**Ingestion type determination** (per collection):
- If MongoDB deployment is a **replica set or sharded cluster**, collections support **Change Streams** → `cdc` or `cdc_with_deletes`
- If MongoDB deployment is a **standalone instance**, collections only support **snapshot** or **append** ingestion


## **Object Schema**

MongoDB is a **schema-less** (flexible schema) database. Each document in a collection can have different fields and structure. The connector must dynamically infer or sample the schema.

**Schema discovery approaches**:

1. **Sample-based inference**: Read a sample of documents (e.g., first 100-1000) and infer the schema by examining field names and types.
2. **Full scan inference** (expensive): Scan all documents to build a comprehensive schema.
3. **User-provided schema** (recommended for production): Allow users to define expected schema in configuration.

**PyMongo schema sampling example**:

```python
from pymongo import MongoClient
from collections import defaultdict

client = MongoClient(connection_uri)
db = client['myDatabase']
collection = db['users']

# Sample first 100 documents
sample_docs = list(collection.find().limit(100))

# Infer schema
field_types = defaultdict(set)
for doc in sample_docs:
    for field, value in doc.items():
        field_types[field].add(type(value).__name__)

print(f"Inferred schema for 'users' collection:")
for field, types in field_types.items():
    print(f"  {field}: {', '.join(types)}")
```

**Example output**:

```
Inferred schema for 'users' collection:
  _id: ObjectId
  name: str
  email: str
  age: int
  created_at: datetime
  tags: list
  address: dict
```

**Special considerations**:
- **`_id` field**: Every MongoDB document has an `_id` field, which is the primary key. If not provided by the user, MongoDB auto-generates an `ObjectId`.
- **Nested documents**: MongoDB supports nested documents (objects) and arrays. These should be represented as nested structs in the connector schema.
- **Mixed types**: A single field might have different types across documents (e.g., `age` could be `int` in some docs, `str` in others). The connector should handle this gracefully, either by coercing types or using a flexible type like `string`.

**BSON types** (see Field Type Mapping section for detailed mapping):
- `ObjectId`: 12-byte unique identifier
- `String`, `Int32`, `Int64`, `Double`, `Decimal128`
- `Boolean`, `Date` (UTC datetime)
- `Array`, `Object` (embedded document)
- `Binary`, `Null`, `Regex`, `Timestamp`


## **Get Object Primary Keys**

In MongoDB, the primary key for every document is **always the `_id` field**. This is enforced by MongoDB at the database level.

**Primary key properties**:
- **Field name**: `_id`
- **Type**: Any BSON type (most commonly `ObjectId`, but can be `String`, `Int32`, `Int64`, `UUID`, etc.)
- **Uniqueness**: Guaranteed unique within a collection
- **Immutability**: Cannot be changed after document creation
- **Auto-generation**: If not provided during insert, MongoDB generates an `ObjectId` automatically

**Static primary key definition**:

For all MongoDB collections, the connector should define:

```python
primary_keys = ["_id"]
```

**Example document**:

```json
{
  "_id": ObjectId("507f1f77bcf86cd799439011"),
  "name": "John Doe",
  "email": "john@example.com",
  "age": 30,
  "created_at": ISODate("2024-01-15T10:30:00Z")
}
```

**Primary key retrieval** (no API call needed):

```python
from pymongo import MongoClient

client = MongoClient(connection_uri)
db = client['myDatabase']
collection = db['users']

# The _id field is always present in every document
doc = collection.find_one()
primary_key_value = doc['_id']
print(f"Primary key: {primary_key_value}")
# Output: Primary key: 507f1f77bcf86cd799439011
```

**Notes**:
- MongoDB does **not** support composite primary keys (unlike relational databases).
- While `_id` can be any type, best practice is to use `ObjectId` for auto-generated IDs, or `String`/`Int64` for user-defined IDs.
- The connector should always include `_id` in the schema and use it as the primary key for upsert operations.


## **Object's ingestion type**

The ingestion type for MongoDB collections depends on the **deployment topology** and the connector's capabilities:

| Ingestion Type | Description | MongoDB Requirements | Use Case |
|----------------|-------------|----------------------|----------|
| `cdc` | Change data capture with upserts only | Replica set or sharded cluster; Change Streams support | Real-time sync of inserts and updates |
| `cdc_with_deletes` | CDC with delete tracking | Replica set or sharded cluster; Change Streams support | Real-time sync with delete handling |
| `snapshot` | Full table snapshot on each sync | Any MongoDB deployment (standalone, replica set, sharded) | Small collections or infrequent full refreshes |
| `append` | Append-only incremental reads | Any MongoDB deployment with a timestamp/cursor field | Event logs, append-only data |

**Ingestion type determination logic**:

1. **Check deployment topology** (replica set vs. standalone):
   ```python
   client = MongoClient(connection_uri)
   is_replica_set = client.topology_description.topology_type_name in ['ReplicaSetWithPrimary', 'Sharded']
   ```

2. **If replica set or sharded cluster**: 
   - Default to `cdc_with_deletes` (using Change Streams)
   - User can override to `snapshot` or `append` if desired

3. **If standalone**: 
   - Default to `snapshot`
   - Support `append` if user provides a cursor field (e.g., `created_at`, `updated_at`)

**Change Streams (CDC) support**:

Change Streams allow applications to access real-time data changes on collections, databases, or an entire deployment. They return change events for insert, update, replace, and delete operations.

**Requirements**:
- MongoDB 3.6+ (4.0+ for full support)
- Replica set or sharded cluster deployment
- Sufficient oplog size to handle lag between syncs
- `read` permissions on the target database

**Change Streams example**:

```python
from pymongo import MongoClient

client = MongoClient(connection_uri)
db = client['myDatabase']
collection = db['users']

# Open a change stream
with collection.watch() as change_stream:
    for change in change_stream:
        print(change)
```

**Example change event**:

```json
{
  "_id": {"_data": "8261..."},
  "operationType": "insert",
  "clusterTime": Timestamp(1642123456, 1),
  "ns": {"db": "myDatabase", "coll": "users"},
  "documentKey": {"_id": ObjectId("507f1f77bcf86cd799439011")},
  "fullDocument": {
    "_id": ObjectId("507f1f77bcf86cd799439011"),
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }
}
```

**Append-only ingestion** (for standalone or user preference):

If the collection has a timestamp or monotonically increasing field (e.g., `created_at`, `_id` with `ObjectId`), the connector can perform incremental reads:

```python
# First sync: read all documents
cursor = None
docs = list(collection.find().sort('_id', 1).limit(1000))

# Track the last _id
if docs:
    cursor = docs[-1]['_id']

# Subsequent sync: read documents after last cursor
new_docs = list(collection.find({'_id': {'$gt': cursor}}).sort('_id', 1).limit(1000))
```

**Recommended default ingestion types** (per connector implementation):

```python
def get_ingestion_type(client, database_name, collection_name):
    is_replica_set = client.topology_description.topology_type_name in ['ReplicaSetWithPrimary', 'Sharded']
    
    if is_replica_set:
        return "cdc_with_deletes"
    else:
        # For standalone, default to snapshot, but allow append if cursor field exists
        return "snapshot"
```


## **Read API for Data Retrieval**

MongoDB provides flexible data retrieval through the PyMongo driver. The primary methods for reading data are:

### **1. Full Scan (Snapshot Read)**

**Method**: `collection.find(filter=None, projection=None)`

**Parameters**:
- `filter` (dict): Query filter to match documents (default: `{}` matches all)
- `projection` (dict): Fields to include/exclude (e.g., `{'name': 1, 'email': 1, '_id': 0}`)
- `limit` (int): Maximum number of documents to return
- `skip` (int): Number of documents to skip (not recommended for large offsets)
- `sort` (list of tuples): Sort order (e.g., `[('_id', 1)]` for ascending by `_id`)
- `batch_size` (int): Number of documents per batch (default: 101)

**Example**:

```python
from pymongo import MongoClient

client = MongoClient(connection_uri)
db = client['myDatabase']
collection = db['users']

# Read all documents (cursor)
cursor = collection.find()
for doc in cursor:
    print(doc)

# Read with filter and projection
cursor = collection.find(
    filter={'age': {'$gte': 18}},
    projection={'name': 1, 'email': 1, '_id': 1}
)

# Read with limit and sort
cursor = collection.find().sort('_id', 1).limit(1000)
```

**Pagination** (cursor-based):

MongoDB cursors automatically handle pagination internally. For explicit pagination using `_id`:

```python
# First batch
cursor = collection.find().sort('_id', 1).limit(1000)
docs = list(cursor)

if docs:
    last_id = docs[-1]['_id']
    
    # Next batch
    next_cursor = collection.find({'_id': {'$gt': last_id}}).sort('_id', 1).limit(1000)
    next_docs = list(next_cursor)
```

**Estimated document count** (for progress tracking):

```python
# Fast estimate (uses metadata)
estimated_count = collection.estimated_document_count()

# Accurate count (scans collection)
accurate_count = collection.count_documents({})
```

### **2. Incremental Read (Append-Only)**

For collections with a timestamp or monotonically increasing field, use cursor-based incremental reads:

**Cursor field options**:
- `_id` (if using `ObjectId`, which contains timestamp)
- `created_at` or `updated_at` (datetime fields)
- `timestamp` or `_ts` (numeric timestamp)

**Example** (using `_id` as cursor):

```python
from bson import ObjectId

# First sync
cursor = collection.find().sort('_id', 1).limit(1000)
docs = list(cursor)

# Store last _id as checkpoint
last_id = docs[-1]['_id'] if docs else None

# Subsequent sync (incremental)
if last_id:
    new_docs = list(
        collection.find({'_id': {'$gt': last_id}})
        .sort('_id', 1)
        .limit(1000)
    )
```

**Example** (using datetime cursor with lookback):

```python
from datetime import datetime, timedelta

# Store last sync time
last_sync_time = datetime(2024, 1, 1, 0, 0, 0)

# Apply lookback window (e.g., 5 minutes) to handle late-arriving data
lookback_seconds = 300
cursor_time = last_sync_time - timedelta(seconds=lookback_seconds)

# Incremental read
new_docs = list(
    collection.find({'created_at': {'$gte': cursor_time}})
    .sort('created_at', 1)
    .limit(1000)
)

# Update cursor for next sync
if new_docs:
    last_sync_time = max(doc['created_at'] for doc in new_docs)
```

### **3. Change Streams (CDC)**

For replica sets and sharded clusters, use Change Streams for real-time CDC:

**Method**: `collection.watch(pipeline=None, resume_after=None, start_at_operation_time=None)`

**Parameters**:
- `pipeline` (list): Aggregation pipeline to filter change events
- `resume_after` (dict): Resume token to continue from a specific point
- `start_at_operation_time` (Timestamp): Start watching from a specific cluster time
- `start_after` (dict): Similar to `resume_after` but invalidates the stream
- `full_document` (str): Options: `'default'`, `'updateLookup'` (fetch full document after update)

**Example** (basic change stream):

```python
from pymongo import MongoClient

client = MongoClient(connection_uri)
db = client['myDatabase']
collection = db['users']

# Open change stream
with collection.watch(full_document='updateLookup') as stream:
    for change in stream:
        operation_type = change['operationType']  # 'insert', 'update', 'replace', 'delete'
        document_key = change['documentKey']      # {'_id': ...}
        full_document = change.get('fullDocument')  # Complete document (for insert/update)
        
        print(f"Operation: {operation_type}")
        print(f"Document ID: {document_key['_id']}")
        if full_document:
            print(f"Full document: {full_document}")
```

**Example change event structure**:

```json
{
  "_id": {
    "_data": "82617F5B8D000000012B022C0100296E5A1004..."
  },
  "operationType": "update",
  "clusterTime": Timestamp(1642123456, 1),
  "ns": {
    "db": "myDatabase",
    "coll": "users"
  },
  "documentKey": {
    "_id": ObjectId("507f1f77bcf86cd799439011")
  },
  "updateDescription": {
    "updatedFields": {"age": 31},
    "removedFields": []
  },
  "fullDocument": {
    "_id": ObjectId("507f1f77bcf86cd799439011"),
    "name": "John Doe",
    "email": "john@example.com",
    "age": 31,
    "created_at": ISODate("2024-01-15T10:30:00Z")
  }
}
```

**Resume tokens** (for fault tolerance):

Change streams provide resume tokens to resume from the last processed change:

```python
resume_token = None

try:
    with collection.watch(resume_after=resume_token) as stream:
        for change in stream:
            # Process change
            process_change(change)
            
            # Store resume token for next run
            resume_token = stream.resume_token
except Exception as e:
    print(f"Error: {e}")
    # On next run, resume from last saved token
```

**Handling deletes**:

Delete operations in change streams only include the `_id`:

```json
{
  "operationType": "delete",
  "ns": {"db": "myDatabase", "coll": "users"},
  "documentKey": {"_id": ObjectId("507f1f77bcf86cd799439011")}
}
```

For `cdc_with_deletes` ingestion type, the connector should:
1. Detect `operationType == 'delete'`
2. Mark the record as deleted (e.g., set a `_deleted` flag or emit a delete event)
3. Propagate the deletion to the target system

**Filtering change streams** (using aggregation pipeline):

```python
# Only track insert and update operations
pipeline = [
    {'$match': {'operationType': {'$in': ['insert', 'update']}}}
]

with collection.watch(pipeline=pipeline) as stream:
    for change in stream:
        print(change)
```

**Rate limits and performance**:
- MongoDB does **not** enforce API-level rate limits for reads
- Performance is bounded by:
  - Cluster resources (CPU, memory, IOPS)
  - Network bandwidth
  - Query complexity and index usage
  - Batch size and concurrency
- **Best practices**:
  - Use indexes on cursor fields (e.g., `_id`, `created_at`)
  - Batch reads with `limit()` to control memory usage
  - Use `projection` to fetch only required fields
  - Monitor oplog size for Change Streams to avoid falling behind


## **Field Type Mapping**

MongoDB uses BSON (Binary JSON) for document storage. The connector must map BSON types to logical types supported by the target system (e.g., Spark DataFrames).

### **BSON to Logical Type Mapping**

| BSON Type | PyMongo Python Type | Spark/Logical Type | Notes |
|-----------|---------------------|-------------------|-------|
| `ObjectId` | `bson.ObjectId` | `String` | Convert to 24-character hex string |
| `String` | `str` | `String` | UTF-8 encoded text |
| `Int32` | `int` | `Integer` / `Int` | 32-bit signed integer |
| `Int64` | `int` | `Long` / `BigInt` | 64-bit signed integer |
| `Double` | `float` | `Double` | 64-bit IEEE 754 floating point |
| `Decimal128` | `bson.Decimal128` | `Decimal` | 128-bit decimal (high precision) |
| `Boolean` | `bool` | `Boolean` | `true` or `false` |
| `Date` | `datetime.datetime` | `Timestamp` | UTC datetime; stored as milliseconds since epoch |
| `Timestamp` | `bson.Timestamp` | `Struct` or `Long` | MongoDB internal replication timestamp (time, increment) |
| `Array` | `list` | `Array<T>` | Homogeneous or heterogeneous array |
| `Object` | `dict` | `Struct` | Embedded document (nested object) |
| `Binary` | `bson.Binary` | `Binary` | Binary data (byte array) |
| `Null` | `None` | `Null` | Absence of value |
| `Regex` | `bson.Regex` | `String` | Convert to regex pattern string |
| `JavaScript` | `bson.Code` | `String` | JavaScript code (rarely used) |
| `DBRef` | `bson.DBRef` | `Struct` | Reference to another document (database, collection, id) |
| `MinKey` / `MaxKey` | `bson.MinKey` / `bson.MaxKey` | `String` | Special comparison values (convert to sentinel strings) |

### **Special Field Behaviors**

**1. ObjectId Conversion**:

```python
from bson import ObjectId

object_id = ObjectId("507f1f77bcf86cd799439011")

# Convert to string for storage
object_id_str = str(object_id)  # "507f1f77bcf86cd799439011"

# Parse back to ObjectId
parsed_object_id = ObjectId(object_id_str)
```

**2. Datetime Handling**:

MongoDB stores dates as UTC milliseconds since epoch. PyMongo returns Python `datetime` objects (timezone-aware or naive depending on configuration).

```python
from datetime import datetime

# MongoDB BSON Date
bson_date = datetime(2024, 1, 15, 10, 30, 0)

# Convert to ISO 8601 string for display
iso_string = bson_date.isoformat()  # "2024-01-15T10:30:00"

# Convert to UTC timestamp
timestamp_ms = int(bson_date.timestamp() * 1000)
```

**3. Embedded Documents (Objects)**:

```python
# MongoDB document with nested object
doc = {
    "_id": ObjectId("507f1f77bcf86cd799439011"),
    "name": "John Doe",
    "address": {
        "street": "123 Main St",
        "city": "New York",
        "zip": "10001"
    }
}

# Map to Spark StructType
from pyspark.sql.types import StructType, StructField, StringType

address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True)
])
```

**4. Arrays**:

MongoDB arrays can be **homogeneous** (all elements same type) or **heterogeneous** (mixed types).

```python
# Homogeneous array
doc = {
    "_id": ObjectId("..."),
    "tags": ["python", "mongodb", "database"]
}
# Map to: Array<String>

# Heterogeneous array (problematic for strongly-typed systems)
doc = {
    "_id": ObjectId("..."),
    "mixed": [1, "two", 3.0, {"key": "value"}]
}
# Options:
# - Convert all elements to String
# - Use Array<Struct<type: String, value: String>>
# - Skip or flatten
```

**5. Null vs Missing Fields**:

In MongoDB, a field can be:
- **Missing** (not present in document)
- **Explicitly null** (`"field": null`)

The connector should treat both as `null` in the target schema for consistency.

**6. Decimal128 (High Precision Decimals)**:

```python
from bson import Decimal128

# MongoDB Decimal128
decimal_value = Decimal128("123.456789012345678901234567890")

# Convert to Python Decimal
from decimal import Decimal
python_decimal = decimal_value.to_decimal()

# Convert to string for systems without decimal support
decimal_str = str(decimal_value)
```

### **Type Inference and Schema Flexibility**

Since MongoDB is schema-less, the connector should:
1. **Sample documents** to infer field types
2. **Handle type mismatches** gracefully (e.g., if `age` is `int` in some docs, `str` in others):
   - Option A: Coerce to most common type
   - Option B: Coerce to `String` for mixed types
   - Option C: Use union types (if target system supports)
3. **Support user-provided schemas** for production use cases where schema is known


## **Sources and References**

### **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://www.mongodb.com/docs/manual/reference/connection-string/ | 2025-01-08 | High | Connection URI format, authentication mechanisms, connection options |
| Official Docs | https://www.mongodb.com/docs/manual/reference/method/db.collection.find/ | 2025-01-08 | High | `find()` method parameters, cursor behavior, pagination |
| Official Docs | https://www.mongodb.com/docs/manual/changeStreams/ | 2025-01-08 | High | Change Streams architecture, requirements (replica set), resume tokens, operation types |
| Official Docs | https://www.mongodb.com/docs/manual/reference/bson-types/ | 2025-01-08 | High | BSON data types, ObjectId structure, Decimal128, Timestamp |
| Official Docs | https://www.mongodb.com/docs/manual/core/security-users/ | 2025-01-08 | High | User roles, authentication methods, required permissions |
| PyMongo Docs | https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html | 2025-01-08 | High | PyMongo Collection API methods: `find()`, `watch()`, `list_collection_names()` |
| PyMongo Docs | https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html | 2025-01-08 | High | MongoClient initialization, connection URI handling, topology detection |
| PyMongo Docs | https://pymongo.readthedocs.io/en/stable/api/bson/index.html | 2025-01-08 | High | BSON types in Python: ObjectId, Decimal128, Binary, Timestamp, etc. |
| Airbyte Docs | https://docs.airbyte.com/integrations/sources/mongodb-v2 | 2025-01-08 | High | Airbyte MongoDB connector behavior, CDC using Change Streams, incremental sync strategies |
| Airbyte Code | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-mongodb-v2 | 2025-01-08 | High | Reference implementation for schema inference, Change Streams cursor management, error handling |
| Singer Docs | https://github.com/singer-io/tap-mongodb | 2025-01-08 | Medium | Singer MongoDB tap, oplog-based CDC approach (older method), primary key handling |

### **Official Documentation Links**

- **MongoDB Manual** (highest confidence):
  - Connection Strings: https://www.mongodb.com/docs/manual/reference/connection-string/
  - Authentication: https://www.mongodb.com/docs/manual/core/authentication/
  - Change Streams: https://www.mongodb.com/docs/manual/changeStreams/
  - BSON Types: https://www.mongodb.com/docs/manual/reference/bson-types/
  - Query Methods: https://www.mongodb.com/docs/manual/reference/method/db.collection.find/
  - Indexes: https://www.mongodb.com/docs/manual/indexes/
  - Aggregation: https://www.mongodb.com/docs/manual/aggregation/
  - Replication: https://www.mongodb.com/docs/manual/replication/
  - User Roles: https://www.mongodb.com/docs/manual/reference/built-in-roles/

- **PyMongo Documentation** (high confidence):
  - PyMongo API: https://pymongo.readthedocs.io/en/stable/api/
  - MongoClient: https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
  - Collection API: https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html
  - Change Streams: https://pymongo.readthedocs.io/en/stable/examples/change_streams.html
  - BSON Types: https://pymongo.readthedocs.io/en/stable/api/bson/index.html

- **Reference Implementations** (high confidence for practical patterns):
  - Airbyte MongoDB Source: https://docs.airbyte.com/integrations/sources/mongodb-v2
  - Airbyte Code: https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-mongodb-v2
  - Singer MongoDB Tap: https://github.com/singer-io/tap-mongodb

### **Conflict Resolution**

When conflicts arise:
- **Official MongoDB documentation** takes precedence over all other sources
- **PyMongo official documentation** is authoritative for Python-specific implementation details
- **Airbyte/Singer implementations** are used to validate practical patterns and edge case handling
- For gaps or ambiguities, testing with a live MongoDB instance is recommended

### **Known Gaps and TBD Items**

- TBD: Optimal batch size for large collections (requires performance testing)
- TBD: Handling of very large embedded documents (>16MB BSON limit)
- TBD: Support for MongoDB Atlas-specific features (e.g., Atlas Search, Data Lake)
- TBD: Handling of MongoDB 5.0+ time series collections (special collection type)
- TBD: Support for encrypted fields (MongoDB 6.0+ Queryable Encryption)

### **Additional Notes**

- This documentation focuses on **read operations** only. MongoDB supports extensive write operations (`insert`, `update`, `delete`, `replace`), but these are out of scope for a source connector.
- **MongoDB Atlas** (cloud offering) uses the same API as self-hosted MongoDB, but with additional features (e.g., `mongodb+srv://` connection strings, automated TLS).
- **MongoDB versions**: This documentation primarily targets MongoDB 4.0+ (current stable versions as of 2025). Legacy versions (3.x) have limited Change Streams support.
- **Performance best practices**: Always create indexes on fields used for filtering and sorting (especially cursor fields like `_id`, `created_at`).

