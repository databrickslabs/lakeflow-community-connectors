# MongoDB Atlas SRV Connection Setup

This guide explains how to use MongoDB Atlas (cloud) with the MongoDB connector.

## Prerequisites

MongoDB Atlas SRV connection strings (`mongodb+srv://...`) require the **`dnspython`** package for DNS SRV record resolution.

## Installation

```bash
# Install required packages
pip install pymongo dnspython

# Or install all project dependencies
pip install -e .
```

## Connection URI Format

MongoDB Atlas uses SRV connection strings:

```
mongodb+srv://<username>:<password>@<cluster>.mongodb.net/<database>?<options>
```

### Example URIs

**Basic Atlas Connection:**
```
mongodb+srv://myuser:mypassword@cluster0.mongodb.net/mydb
```

**With Additional Options:**
```
mongodb+srv://myuser:mypassword@cluster0.mongodb.net/mydb?retryWrites=true&w=majority
```

**Common Options:**
- `retryWrites=true` - Enable retryable writes
- `w=majority` - Write concern
- `appName=MyApp` - Application name for monitoring

## Testing Your Atlas Connection

### Method 1: Use the Test Script

```bash
# Test your Atlas connection
python sources/mongodb/test_atlas_connection.py "mongodb+srv://user:pass@cluster.mongodb.net/database"
```

This will verify:
- ✓ DNS SRV record resolution
- ✓ Authentication
- ✓ Network connectivity
- ✓ Database access
- ✓ CDC support (replica set detection)

### Method 2: Use MongoDB Shell

```bash
# Test with mongosh
mongosh "mongodb+srv://user:pass@cluster.mongodb.net/database"
```

### Method 3: Python Quick Test

```python
from pymongo import MongoClient

uri = "mongodb+srv://user:pass@cluster.mongodb.net/database"
client = MongoClient(uri)

# Test connection
client.admin.command('ping')
print("✓ Connected successfully!")

# List databases
print(f"Databases: {client.list_database_names()}")
```

## Configuring the Connector

Update your `dev_config.json` with your Atlas URI:

```json
{
  "connection_uri": "mongodb+srv://username:password@cluster0.mongodb.net/database",
  "schema_sample_size": "100",
  "batch_size": "1000"
}
```

## Running Tests with Atlas

```bash
# 1. Ensure test data exists in your Atlas cluster
mongosh "mongodb+srv://user:pass@cluster.mongodb.net/testdb" --eval "
  db.users.insertMany([
    {name: 'Alice', email: 'alice@test.com', age: 28},
    {name: 'Bob', email: 'bob@test.com', age: 35}
  ]);
  db.orders.insertMany([
    {order_id: 'ORD-001', total: 99.99}
  ]);
  db.products.insertMany([
    {sku: 'SKU-001', name: 'Widget', price: 19.99}
  ]);
"

# 2. Update dev_config.json with Atlas URI

# 3. Run tests
pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v
```

## Troubleshooting

### Error: "dnspython must be installed"

**Solution:**
```bash
pip install dnspython
```

### Error: "Authentication failed"

**Solutions:**
1. Verify username and password are correct
2. Check if special characters in password are URL-encoded:
   - `@` → `%40`
   - `:` → `%3A`
   - `/` → `%2F`
3. Verify user has database permissions in Atlas console

**URL Encoding Example:**
```
Password: my@pass:word
Encoded:  my%40pass%3Aword
URI: mongodb+srv://user:my%40pass%3Aword@cluster.mongodb.net/db
```

### Error: "Connection timeout"

**Solutions:**
1. Check MongoDB Atlas Network Access (IP Whitelist):
   - Go to Atlas Console → Network Access
   - Add your IP address or use `0.0.0.0/0` for testing (not recommended for production)
2. Verify network connectivity
3. Check firewall settings

### Error: "Server selection timeout"

**Solutions:**
1. Verify cluster is running in Atlas console
2. Check DNS resolution: `nslookup cluster0.mongodb.net`
3. Try increasing timeout in code:
   ```python
   client = MongoClient(uri, serverSelectionTimeoutMS=30000)
   ```

### CDC Not Working

**Atlas clusters are replica sets by default**, so CDC should work automatically.

Verify:
```python
from pymongo import MongoClient

client = MongoClient("mongodb+srv://...")
topology = client.topology_description.topology_type_name
print(f"Topology: {topology}")

# Should see: "ReplicaSetWithPrimary"
```

## Atlas-Specific Features

### Automatic Replica Set

All Atlas clusters are replica sets, enabling:
- ✓ Change Streams (CDC)
- ✓ Delete tracking
- ✓ High availability
- ✓ Automatic failover

### SRV Records Benefits

Using `mongodb+srv://` provides:
- ✓ Automatic server discovery
- ✓ Simplified connection strings
- ✓ Automatic TLS/SSL
- ✓ Load balancing across replica set members

### Performance Considerations

1. **Network Latency**: Atlas is cloud-based, expect 10-100ms latency
2. **Region Selection**: Deploy Atlas in same region as Databricks
3. **Connection Pooling**: PyMongo handles this automatically
4. **Batch Sizes**: May need smaller batches than local MongoDB

## Security Best Practices

1. **Never commit credentials**:
   - Use environment variables
   - Keep `dev_config.json` gitignored
   
2. **IP Whitelisting**:
   - Add specific IP ranges
   - Avoid `0.0.0.0/0` in production

3. **User Permissions**:
   - Create read-only user for connector
   - Grant minimal required permissions:
     ```javascript
     use admin
     db.createUser({
       user: "lakeflow_reader",
       pwd: "secure_password",
       roles: [
         { role: "read", db: "myDatabase" }
       ]
     })
     ```

4. **Connection String Storage**:
   - Use secrets management (Databricks Secrets)
   - Rotate credentials regularly

## Atlas Console Setup

1. **Create Cluster** (if not exists)
2. **Create Database User**:
   - Security → Database Access → Add New Database User
   - Authentication: Password
   - Database User Privileges: Read any database (or specific database)
3. **Configure Network Access**:
   - Security → Network Access → Add IP Address
   - Add your Databricks workspace IPs
4. **Get Connection String**:
   - Database → Connect → Connect your application
   - Copy the `mongodb+srv://` connection string
   - Replace `<password>` with your actual password

## Example: Complete Setup

```bash
# 1. Install dependencies
pip install pymongo dnspython

# 2. Test connection
python sources/mongodb/test_atlas_connection.py \
  "mongodb+srv://myuser:mypass@cluster0.mongodb.net/mydb"

# 3. Create config
cat > sources/mongodb/configs/dev_config.json << EOF
{
  "connection_uri": "mongodb+srv://myuser:mypass@cluster0.mongodb.net/mydb",
  "schema_sample_size": "100",
  "batch_size": "1000"
}
EOF

# 4. Run tests
pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v

# 5. Clean up
rm sources/mongodb/configs/dev_*.json
```

## References

- [MongoDB Atlas Documentation](https://www.mongodb.com/docs/atlas/)
- [Connection Strings](https://www.mongodb.com/docs/manual/reference/connection-string/)
- [SRV Connection Format](https://www.mongodb.com/docs/manual/reference/connection-string/#dns-seed-list-connection-format)
- [dnspython Documentation](https://dnspython.readthedocs.io/)

