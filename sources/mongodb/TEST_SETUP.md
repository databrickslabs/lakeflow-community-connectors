# MongoDB Connector Test Setup Guide

This guide provides instructions for setting up and running tests for the MongoDB connector.

## Quick Start

### Option 1: Using Docker (Recommended)

```bash
# 1. Start MongoDB with Docker
docker run -d --name mongodb-test \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=testuser \
  -e MONGO_INITDB_ROOT_PASSWORD=testpass \
  mongo:7.0

# 2. Wait for MongoDB to start (5-10 seconds)
sleep 10

# 3. Create test database and data
docker exec -it mongodb-test mongosh \
  -u testuser -p testpass \
  --authenticationDatabase admin \
  --eval "
    use testdb;
    db.users.insertMany([
      {name: 'Alice', email: 'alice@test.com', age: 28, active: true, created_at: new Date()},
      {name: 'Bob', email: 'bob@test.com', age: 35, active: true, created_at: new Date()},
      {name: 'Charlie', email: 'charlie@test.com', age: 42, active: false, created_at: new Date()}
    ]);
    db.orders.insertMany([
      {order_id: 'ORD-001', user_id: 1, total: 99.99, items: ['item1', 'item2'], created_at: new Date()},
      {order_id: 'ORD-002', user_id: 2, total: 149.99, items: ['item3'], created_at: new Date()}
    ]);
    db.products.insertMany([
      {sku: 'SKU-001', name: 'Widget', price: 19.99, stock: 100, tags: ['new', 'featured']},
      {sku: 'SKU-002', name: 'Gadget', price: 29.99, stock: 50, tags: ['sale']}
    ]);
    print('Test data created successfully');
  "

# 4. Update dev_config.json
cat > sources/mongodb/configs/dev_config.json << 'EOF'
{
  "connection_uri": "mongodb://testuser:testpass@localhost:27017/testdb?authSource=admin",
  "schema_sample_size": "100",
  "batch_size": "1000"
}
EOF

# 5. Run tests
pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v

# 6. Clean up after testing
docker stop mongodb-test && docker rm mongodb-test
rm sources/mongodb/configs/dev_config.json
rm sources/mongodb/configs/dev_table_config.json
```

### Option 2: Using Existing MongoDB Instance

If you have MongoDB already running:

```bash
# 1. Create dev_config.json with your connection details
cat > sources/mongodb/configs/dev_config.json << 'EOF'
{
  "connection_uri": "mongodb://your-username:your-password@your-host:27017/your-database",
  "schema_sample_size": "100",
  "batch_size": "1000"
}
EOF

# 2. Create test data in your MongoDB
mongosh "your-connection-string" --eval "
  use testdb;
  db.users.insertMany([
    {name: 'Alice', email: 'alice@test.com', age: 28, created_at: new Date()},
    {name: 'Bob', email: 'bob@test.com', age: 35, created_at: new Date()}
  ]);
  db.orders.insertMany([
    {order_id: 'ORD-001', total: 99.99, created_at: new Date()}
  ]);
"

# 3. Run tests
pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v

# 4. Clean up
rm sources/mongodb/configs/dev_config.json
rm sources/mongodb/configs/dev_table_config.json
```

## Testing with Replica Set (for CDC Features)

To test Change Streams functionality, you need a replica set:

```bash
# 1. Start MongoDB replica set with Docker
docker run -d --name mongodb-rs \
  -p 27017:27017 \
  mongo:7.0 --replSet rs0

# 2. Initialize replica set
docker exec mongodb-rs mongosh --eval "
  rs.initiate({
    _id: 'rs0',
    members: [{_id: 0, host: 'localhost:27017'}]
  })
"

# 3. Wait for replica set to initialize (10 seconds)
sleep 10

# 4. Create test data
docker exec mongodb-rs mongosh --eval "
  use testdb;
  db.users.insertMany([
    {name: 'Alice', email: 'alice@test.com', age: 28, created_at: new Date()},
    {name: 'Bob', email: 'bob@test.com', age: 35, created_at: new Date()}
  ]);
"

# 5. Update dev_config.json for replica set
cat > sources/mongodb/configs/dev_config.json << 'EOF'
{
  "connection_uri": "mongodb://localhost:27017/testdb?replicaSet=rs0",
  "schema_sample_size": "100",
  "batch_size": "1000"
}
EOF

# 6. Run tests (will now test CDC features)
pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v

# 7. Clean up
docker stop mongodb-rs && docker rm mongodb-rs
rm sources/mongodb/configs/dev_config.json
rm sources/mongodb/configs/dev_table_config.json
```

## Configuration Files

### dev_config.json

Contains connection-level configuration:

```json
{
  "connection_uri": "mongodb://username:password@host:port/database",
  "schema_sample_size": "100",
  "batch_size": "1000"
}
```

**Fields:**
- `connection_uri` (required): MongoDB connection string
- `schema_sample_size` (optional): Number of documents to sample for schema inference (default: 100)
- `batch_size` (optional): Batch size for reads (default: 1000)

### dev_table_config.json

Contains table-specific configuration:

```json
{
  "testdb.users": {
    "database": "testdb"
  },
  "testdb.orders": {
    "database": "testdb"
  }
}
```

**Fields:**
- Key: Table name in format `database.collection`
- `database` (required): Database name containing the collection

## Running Tests

### Run all tests:
```bash
pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v
```

### Run with detailed output:
```bash
pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v -s
```

### Run specific test:
```bash
pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py::test_mongodb_connector -v
```

## Expected Test Results

The test suite will run the following tests:

1. **test_initialization** - Verifies connector can be initialized with valid config
2. **test_list_tables** - Verifies tables can be listed from MongoDB
3. **test_get_table_schema** - Verifies schema can be inferred for each table
4. **test_read_table_metadata** - Verifies metadata (primary keys, cursor field, ingestion type) is correct
5. **test_read_table** - Verifies data can be read from each table
6. **test_read_table_deletes** - Verifies delete tracking (only for replica sets with CDC)

### Successful Output Example:

```
==================================================
LAKEFLOW CONNECT TEST REPORT
==================================================
Connector Class: LakeflowConnect
Timestamp: 2025-01-08T10:30:00

SUMMARY:
  Total Tests: 6
  Passed: 6
  Failed: 0
  Errors: 0
  Success Rate: 100.0%

TEST RESULTS:
--------------------------------------------------
✅ test_initialization
  Status: PASSED
  Message: Connector initialized successfully

✅ test_list_tables
  Status: PASSED
  Message: Successfully retrieved 3 tables

✅ test_get_table_schema
  Status: PASSED
  Message: Successfully retrieved table schema for all 3 tables

✅ test_read_table_metadata
  Status: PASSED
  Message: Successfully retrieved table metadata for all 3 tables

✅ test_read_table
  Status: PASSED
  Message: Successfully read all 3 tables

✅ test_read_table_deletes
  Status: PASSED
  Message: Successfully tested read_table_deletes on 3 tables
```

## Troubleshooting

### Test Failures

**Connection errors:**
```
ConnectionFailure: Failed to connect to MongoDB
```
- Verify MongoDB is running: `docker ps` or `mongosh`
- Check connection string in `dev_config.json`
- Ensure network connectivity to MongoDB host

**Authentication errors:**
```
OperationFailure: Authentication failed
```
- Verify username and password in connection URI
- Check `authSource` parameter in connection URI (usually `admin`)
- Ensure user has `read` permissions on test database

**Schema inference failures:**
```
Test failed: No documents found in collection
```
- Verify test data was created: `mongosh` → `use testdb` → `db.users.find()`
- Check database name in `dev_table_config.json` matches actual database

**CDC test failures (replica set required):**
```
Test skipped: No tables with ingestion_type 'cdc_with_deletes'
```
- This is expected for standalone MongoDB
- Use replica set setup (see "Testing with Replica Set" section above)
- Verify replica set is initialized: `rs.status()`

### MongoDB Connection String Examples

**Local without auth:**
```
mongodb://localhost:27017/testdb
```

**Local with auth:**
```
mongodb://username:password@localhost:27017/testdb?authSource=admin
```

**Atlas (cloud):**
```
mongodb+srv://username:password@cluster.mongodb.net/testdb
```

**Replica set:**
```
mongodb://host1:27017,host2:27017,host3:27017/testdb?replicaSet=rs0
```

## Clean Up

**Always remove config files after testing:**

```bash
rm sources/mongodb/configs/dev_config.json
rm sources/mongodb/configs/dev_table_config.json
```

**Stop and remove Docker containers:**

```bash
docker stop mongodb-test && docker rm mongodb-test
# or
docker stop mongodb-rs && docker rm mongodb-rs
```

## CI/CD Integration

For automated testing in CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Start MongoDB
  run: |
    docker run -d --name mongodb-test \
      -p 27017:27017 \
      mongo:7.0
    sleep 10

- name: Setup test data
  run: |
    docker exec mongodb-test mongosh --eval "
      use testdb;
      db.users.insertMany([...]);
    "

- name: Run tests
  run: |
    echo '{"connection_uri": "mongodb://localhost:27017/testdb"}' > sources/mongodb/configs/dev_config.json
    pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v

- name: Cleanup
  run: |
    docker stop mongodb-test && docker rm mongodb-test
```

## Developer Notes

- Test config files are gitignored (`.gitignore` includes `**/configs/dev_*.json`)
- Never commit connection credentials
- Use mock credentials or test instances only
- Tests should pass with any valid MongoDB instance containing appropriate test data
- For local development, Docker is the easiest way to spin up a test MongoDB instance

