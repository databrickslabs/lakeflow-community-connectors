# Lakeflow MongoDB Community Connector

This documentation provides setup instructions and reference information for the MongoDB source connector. Use this connector to ingest data from MongoDB databases into Databricks using Lakeflow.

## Prerequisites

- **MongoDB Instance**: Access to a MongoDB database (standalone, replica set, or sharded cluster)
  - Minimum version: MongoDB 3.6+ (MongoDB 4.0+ recommended)
  - For Change Data Capture (CDC): MongoDB must be deployed as a replica set or sharded cluster
- **MongoDB User Credentials**: A MongoDB user account with read permissions on the databases and collections you want to sync
  - Required role: `read` on target databases
  - For listing all databases: `readAnyDatabase` role
- **Connection Details**: MongoDB connection URI with host, port, and authentication details
- **Network Access**: The Databricks environment must be able to reach your MongoDB instance
- **Lakeflow / Databricks Environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `connection_uri` | string | yes | MongoDB connection string in standard URI format. Includes host, port, database, credentials, and connection options. | `mongodb://user:pass@localhost:27017/mydb` |
| `schema_sample_size` | string | no | Number of documents to sample when inferring schema for each collection. Higher values provide more accurate schemas but slower initialization. | `"100"` (default) |
| `batch_size` | string | no | Number of documents to fetch per batch during data reading. Tune based on document size and network bandwidth. | `"1000"` (default) |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to be passed to the connector. **Required for this connector.** | `database` |

The full list of supported table-specific options for `externalOptionsAllowList` is: **`database`**

> **Note**: The `database` option is table-specific and must be provided per collection in the pipeline specification. It must be included in `externalOptionsAllowList` for the connection to allow it.

### Obtaining the Required Parameters

#### MongoDB Connection URI

The connection URI format follows the MongoDB standard:

```
mongodb://[username:password@]host[:port][/[database][?options]]
```

**For MongoDB Atlas (Cloud)**:
```
mongodb+srv://username:password@cluster.mongodb.net/database
```

**For Replica Set**:
```
mongodb://host1:27017,host2:27017,host3:27017/database?replicaSet=rs0
```

**Connection URI Components**:
- **username:password** (optional): Authentication credentials
- **host:port**: MongoDB server address(es)
- **database** (optional): Default database name
- **options** (optional): Query parameters like `authSource`, `replicaSet`, `tls`, etc.

**Common Connection Options**:
- `authSource=admin` - Authentication database (typically `admin`)
- `replicaSet=rs0` - Replica set name (required for CDC)
- `tls=true` - Enable TLS/SSL encryption
- `retryWrites=true` - Enable retryable writes

**Creating MongoDB User**:

```javascript
// Connect to MongoDB as admin
use admin

// Create read-only user for specific database
db.createUser({
  user: "lakeflow_reader",
  pwd: "secure_password",
  roles: [
    { role: "read", db: "myDatabase" }
  ]
})

// Or grant read access to all databases
db.createUser({
  user: "lakeflow_reader",
  pwd: "secure_password",
  roles: [
    { role: "readAnyDatabase", db: "admin" }
  ]
})
```

**Testing Connection**:

You can test your connection URI using the MongoDB shell:

```bash
mongosh "mongodb://username:password@host:27017/database"
```

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `database` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

**Example Connection Configuration**:

```json
{
  "connection_uri": "mongodb://lakeflow_reader:password@localhost:27017/mydb?authSource=admin",
  "schema_sample_size": "100",
  "batch_size": "1000",
  "externalOptionsAllowList": "database"
}
```

## Supported Objects

The MongoDB connector **dynamically discovers** all databases and collections in your MongoDB instance. Collections are exposed as tables in the format `database.collection`.

### Object Discovery

When you connect to MongoDB, the connector automatically:
1. Lists all databases (excluding system databases: `admin`, `config`, `local`)
2. Lists all collections within each database (excluding system collections like `system.indexes`)
3. Exposes each collection as a table using the naming convention: `database.collection`

**Example Discovered Tables**:
- `myDatabase.users`
- `myDatabase.orders`
- `myDatabase.products`
- `analytics.events`
- `logs.application_logs`

### Ingestion Modes

The connector automatically determines the appropriate ingestion mode based on your MongoDB deployment:

| Deployment Type | Ingestion Mode | Primary Key | Incremental Cursor | Supports Deletes |
|----------------|----------------|-------------|-------------------|------------------|
| **Replica Set or Sharded Cluster** | `cdc_with_deletes` | `_id` | Change Streams resume token | Yes |
| **Standalone** | `append` | `_id` | `_id` field value | No |

#### CDC (Change Data Capture) - Replica Set/Sharded Only

When MongoDB is deployed as a replica set or sharded cluster:
- **Real-time sync**: Captures inserts, updates, and deletes using MongoDB Change Streams
- **Resume capability**: Uses resume tokens for fault-tolerant synchronization
- **Delete tracking**: Captures and propagates delete operations
- **Recommended for**: Production environments requiring real-time data synchronization

#### Append-Only - All Deployments

When MongoDB is standalone or when you prefer cursor-based reads:
- **Incremental reads**: Uses `_id` field for cursor-based pagination
- **New data only**: Fetches only documents with `_id` greater than last seen
- **Efficient batching**: Reads data in configurable batch sizes
- **Recommended for**: Development environments, historical loads, or append-only collections

### Primary Keys and Schema

**All MongoDB collections have**:
- **Primary Key**: `_id` field (guaranteed unique, immutable)
- **Schema**: Dynamically inferred by sampling documents
- **Nested Data**: Preserved as nested structures (not flattened)

**Schema Inference**:
- The connector samples documents (default: 100) from each collection
- Infers field types based on observed BSON types
- Handles schema variations across documents
- All fields except `_id` are nullable (MongoDB's schema-less nature)

### Required and Optional Table Configurations

Each collection requires the `database` parameter to be specified:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `database` | string | yes | Name of the MongoDB database containing the collection | `"myDatabase"` |

> **Note**: If you use the `database.collection` format for table names, the database name can be inferred, but explicitly providing it via `database` option is recommended for clarity.

### Special Considerations

**MongoDB-Specific Features**:
- **Schema flexibility**: Documents in the same collection can have different fields
- **BSON data types**: All MongoDB BSON types are supported and converted appropriately
- **Nested documents**: Preserved as nested structures in the schema
- **Arrays**: Supported, including arrays of objects
- **Large documents**: Documents up to 16MB BSON limit are supported

**Change Streams Requirements (for CDC)**:
- MongoDB must be running as a replica set or sharded cluster
- Oplog must be enabled (default for replica sets)
- Sufficient oplog size to handle synchronization lag
- Read permissions on the database

## Data Type Mapping

MongoDB BSON types are mapped to Databricks types as follows:

| MongoDB BSON Type | Example Fields | Databricks Type | Notes |
|-------------------|----------------|-----------------|-------|
| ObjectId | `_id`, generated IDs | STRING | Converted to 24-character hexadecimal string |
| String | `name`, `email`, text fields | STRING | UTF-8 encoded text |
| Int32 | Small integers, counts | LONG | 32-bit integers promoted to LONG |
| Int64 | Large integers, timestamps | LONG | 64-bit integers |
| Double | Floating point numbers | DOUBLE | 64-bit IEEE 754 |
| Decimal128 | High-precision decimals | DOUBLE | 128-bit decimals converted to DOUBLE |
| Boolean | Flags, boolean values | BOOLEAN | true/false values |
| Date | Created dates, timestamps | STRING | ISO 8601 format (e.g., `2024-01-15T10:30:00Z`) |
| Timestamp | MongoDB internal timestamps | LONG | Stored as Unix timestamp |
| Array | Tags, lists, collections | ARRAY<...> | Homogeneous or mixed arrays |
| Object | Embedded documents, nested data | STRUCT | Nested objects preserved as structs |
| Binary | Binary data, files | BINARY | Byte arrays |
| Null | Absent or null values | NULL | Missing or explicitly null fields |

**Type Conversion Notes**:
- **ObjectId to String**: Enables compatibility and readability; can be converted back if needed
- **Dates as Strings**: Stored in ISO 8601 format; convert to TIMESTAMP type downstream as needed
- **Nested Preservation**: Unlike some connectors, nested documents and arrays are NOT flattened
- **Mixed Types**: If a field has different types across documents, the most common or safest type is used
- **Decimal Precision**: Decimal128 values are converted to DOUBLE; precision may be reduced for very large numbers

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the MongoDB connector code.

### Step 2: Configure Your Pipeline

Update the `pipeline_spec` in your main pipeline file (e.g., `ingest.py`) to specify which MongoDB collections to sync.

**Example Pipeline Configuration**:

```json
{
  "pipeline_spec": {
    "connection_name": "mongodb_connection",
    "object": [
      {
        "table": {
          "source_table": "myDatabase.users",
          "database": "myDatabase"
        }
      },
      {
        "table": {
          "source_table": "myDatabase.orders",
          "database": "myDatabase"
        }
      },
      {
        "table": {
          "source_table": "analytics.events",
          "database": "analytics"
        }
      }
    ]
  }
}
```

**Configuration Fields**:
- `connection_name`: Name of your Unity Catalog connection (configured with `connection_uri`)
- `object`: Array of collections to sync
- `source_table`: Collection name in format `database.collection`
- `database`: MongoDB database name (required)

**Multiple Databases Example**:

```json
{
  "pipeline_spec": {
    "connection_name": "mongodb_connection",
    "object": [
      {
        "table": {
          "source_table": "production.users",
          "database": "production"
        }
      },
      {
        "table": {
          "source_table": "production.orders",
          "database": "production"
        }
      },
      {
        "table": {
          "source_table": "analytics.reports",
          "database": "analytics"
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

Execute your Lakeflow pipeline using Databricks workflows or job scheduling. The connector will:

1. **Connect** to MongoDB using the provided URI
2. **Discover** the specified collections
3. **Infer schemas** by sampling documents
4. **Read data** using the appropriate ingestion mode (CDC or append)
5. **Handle incremental updates** automatically on subsequent runs

**For Replica Set (CDC Mode)**:
- First run performs initial snapshot of all existing data
- Subsequent runs use Change Streams to capture only new/modified/deleted data
- Resume tokens enable fault-tolerant synchronization

**For Standalone (Append Mode)**:
- First run reads all documents
- Subsequent runs read only documents with `_id` greater than last synced value
- Efficient for append-only or infrequently updated collections

#### Best Practices

- **Start Small**: Begin with one or two collections to validate your pipeline configuration
- **Test Schema Inference**: Review inferred schemas to ensure they match your expectations
- **Use Replica Sets in Production**: Deploy MongoDB as a replica set for CDC capabilities and better reliability
- **Monitor Batch Sizes**: Adjust `batch_size` based on document size and network performance
- **Sample Size Tuning**: Increase `schema_sample_size` for collections with high schema variability
- **Network Security**: Use TLS (`tls=true`) for encrypted connections, especially over public networks
- **Authentication Best Practices**: 
  - Create dedicated read-only users for Lakeflow
  - Use `authSource=admin` for user authentication
  - Never commit credentials to version control
- **Oplog Sizing (for CDC)**: Ensure your MongoDB oplog is large enough to cover synchronization intervals
- **Index Optimization**: MongoDB automatically indexes `_id`; no additional indexes needed for basic syncing

#### Troubleshooting

**Common Issues:**

**Connection Failures**:
```
ConnectionFailure: Failed to connect to MongoDB
```
- **Solution**: Verify `connection_uri` is correct and MongoDB is accessible from Databricks
- Check network connectivity, firewalls, and security groups
- Test connection using MongoDB shell: `mongosh "your-connection-uri"`

**Authentication Errors**:
```
OperationFailure: Authentication failed
```
- **Solution**: Verify username and password in connection URI
- Ensure user has `read` permissions on target database
- Check `authSource` parameter (usually `admin`)
- Verify user exists: `db.getUsers()` in MongoDB shell

**No Tables Found**:
```
No tables available
```
- **Solution**: Verify database name is correct
- Ensure collections exist in the database
- Check that user has permission to list collections
- System databases (`admin`, `config`, `local`) are excluded by default

**Schema Inference Issues**:
```
Empty collection or schema inference failed
```
- **Solution**: Ensure collection contains documents
- Increase `schema_sample_size` for better schema coverage
- Check for collections with only empty documents

**Change Streams Not Working (CDC)**:
```
Change streams not available or not supported
```
- **Solution**: Verify MongoDB is deployed as replica set or sharded cluster
- Check replica set status: `rs.status()` in MongoDB shell
- Ensure oplog is enabled (default for replica sets)
- Standalone MongoDB does not support Change Streams (will use append mode)

**Performance Issues**:
```
Slow data reading or timeouts
```
- **Solution**: Reduce `batch_size` for large documents
- Increase `batch_size` for small documents
- Ensure `_id` field is indexed (default)
- Check MongoDB server performance (CPU, memory, disk I/O)
- Consider network latency between Databricks and MongoDB

**Type Conversion Errors**:
```
Failed to parse record with schema
```
- **Solution**: Review inferred schema and actual document structure
- Check for fields with inconsistent types across documents
- Increase `schema_sample_size` to capture more type variations
- Consider type coercion or filtering in downstream transformations

**Rate Limiting or Resource Exhaustion**:
```
MongoDB server overload or too many connections
```
- **Solution**: Reduce concurrent pipeline executions
- Adjust `batch_size` to reduce query frequency
- Monitor MongoDB server metrics
- Consider read replicas for heavy analytical workloads

## References

- **Connector Implementation**: `sources/mongodb/mongodb.py`
- **API Documentation**: `sources/mongodb/mongodb_api_doc.md`
- **Test Setup Guide**: `sources/mongodb/TEST_SETUP.md`
- **MongoDB Official Documentation**:
  - [MongoDB Manual](https://www.mongodb.com/docs/manual/)
  - [Connection Strings](https://www.mongodb.com/docs/manual/reference/connection-string/)
  - [Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
  - [BSON Types](https://www.mongodb.com/docs/manual/reference/bson-types/)
  - [Replica Sets](https://www.mongodb.com/docs/manual/replication/)
  - [Authentication](https://www.mongodb.com/docs/manual/core/authentication/)
  - [Security Best Practices](https://www.mongodb.com/docs/manual/administration/security-checklist/)
- **PyMongo Driver Documentation**:
  - [PyMongo Documentation](https://pymongo.readthedocs.io/)
  - [MongoClient](https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html)
  - [Change Streams Examples](https://pymongo.readthedocs.io/en/stable/examples/change_streams.html)
