# LanceDB Connector

## Overview

The LanceDB connector enables you to read data from LanceDB Cloud vector databases. LanceDB is a vector database optimized for AI applications, providing fast similarity search over embeddings.

**Supported Features:**
- Table discovery and listing
- Schema introspection
- Vector similarity search
- Full table scans
- Batch data reading

**Connector Type:** Vector Database  
**API Type:** REST API with Apache Arrow format

---

## Why Offload LanceDB to a Lakehouse?

### The Challenge

While LanceDB excels at fast vector similarity search for AI applications, organizations often need to:
- **Analyze vector data alongside traditional business data** (customer records, transactions, logs)
- **Run complex SQL analytics** on embeddings and their associated metadata
- **Maintain historical snapshots** of vector data for compliance and auditing
- **Build comprehensive data pipelines** that combine vector and structured data
- **Enable BI tools access** to embedding metadata and search results
- **Reduce costs** by moving cold/historical vector data to cheaper storage

### The Solution: Lakehouse Integration

This connector enables seamless offload of LanceDB data to your lakehouse (Delta Lake, Iceberg, Hudi), providing:

**🔄 Unified Analytics**
- Combine vector embeddings with customer data, transactions, and events
- Run SQL queries across all your data in one place
- Join vector similarity results with business metrics

**📊 Advanced Analytics & BI**
- Query embedding metadata using standard SQL
- Visualize vector data trends in your favorite BI tools (Tableau, PowerBI, Looker)
- Build dashboards showing embedding quality metrics and usage patterns

**🗄️ Cost Optimization**
- Archive historical embeddings to low-cost object storage
- Keep only "hot" vectors in LanceDB for real-time search
- Maintain full history in lakehouse for analysis and compliance

**🔒 Governance & Compliance**
- Centralize data governance across all data sources
- Maintain audit trails of embedding changes
- Apply consistent security policies

**🔧 Data Engineering**
- Build end-to-end pipelines: data → embeddings → LanceDB → lakehouse → analytics
- Schedule regular syncs to keep lakehouse updated
- Enable incremental data loads for efficiency

### Common Use Cases

**1. AI Application Analytics**
```
Scenario: Track and analyze semantic search behavior
Flow: User queries → Embeddings → LanceDB search → Results + metrics → Lakehouse → BI Dashboard
Insight: Which queries perform best? What are common search patterns?
```

**2. Embedding Quality Monitoring**
```
Scenario: Monitor ML model performance over time
Flow: Model updates → New embeddings → LanceDB → Lakehouse (versioned) → Quality metrics
Insight: Has model drift occurred? Are embeddings improving?
```

**3. Customer 360 with AI Context**
```
Scenario: Enrich customer profiles with semantic understanding
Flow: Customer data + Document embeddings → Lakehouse → Unified view
Insight: What documents are most relevant to each customer segment?
```

**4. RAG (Retrieval Augmented Generation) Analytics**
```
Scenario: Optimize RAG system performance
Flow: RAG queries + retrieved chunks → LanceDB → Lakehouse → Analysis
Insight: Which documents are retrieved most? What's the average relevance score?
```

**5. Compliance & Auditing**
```
Scenario: Maintain complete history of vector data
Flow: LanceDB (operational) → Lakehouse (historical archive)
Benefit: Meet regulatory requirements while keeping LanceDB performant
```

### Architecture Pattern

```
┌─────────────┐
│  AI Models  │ (Generate embeddings)
└──────┬──────┘
       │
       ↓
┌─────────────┐
│  LanceDB    │ (Fast vector search)
│   Cloud     │ ← Real-time queries
└──────┬──────┘
       │
       ↓ (Lakeflow Community Connector)
       │
┌──────┴──────┐
│  Lakehouse  │ (Delta/Iceberg/Hudi)
│             │
│  • Analytics│
│  • BI Tools │
│  • Archive  │
│  • Audit    │
└─────────────┘
```

### Benefits Summary

| Benefit | Description |
|---------|-------------|
| **Unified Data Platform** | All your data (vectors + structured) in one place |
| **Cost Efficiency** | Archive to cheap storage, keep hot data in LanceDB |
| **Advanced Analytics** | SQL + BI tools on embedding metadata |
| **Governance** | Centralized policies, audit trails, compliance |
| **Scalability** | Lakehouse handles massive historical data |
| **Flexibility** | Use best tool for each job (LanceDB for search, lakehouse for analytics) |

---

## Prerequisites

Before using this connector, you need:

1. **LanceDB Cloud Account**: Sign up at [lancedb.com](https://lancedb.com)
2. **API Key**: Generate an API key from your LanceDB Cloud dashboard
3. **Project Details**: Your project name and region

---

## Configuration

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `api_key` | string | Your LanceDB Cloud API key | `sk_abc123...` |
| `project_name` | string | Your LanceDB project/database name | `my-project-xyz` |
| `region` | string | Cloud region where your project is hosted | `us-east-1` |
| `externalOptionsAllowList` | string | Comma-separated list of table-specific options that can be passed through. **Required** for this connector. | `columns,use_full_scan,batch_size,query_vector,filter_expression,cursor_field,vector_column,distance_type,nprobes,ef,refine_factor,fast_search,bypass_vector_index,prefilter,lower_bound,upper_bound,with_row_id,offset,version` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`columns,use_full_scan,batch_size,query_vector,filter_expression,cursor_field,vector_column,distance_type,nprobes,ef,refine_factor,fast_search,bypass_vector_index,prefilter,lower_bound,upper_bound,with_row_id,offset,version`

> **Note**: Table-specific options such as `columns`, `use_full_scan`, or `filter_expression` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Configuration Example

```json
{
  "api_key": "sk_abc123def456...",
  "project_name": "my-lancedb-project",
  "region": "us-east-1",
  "externalOptionsAllowList": "columns,use_full_scan,batch_size,query_vector,filter_expression,cursor_field,vector_column,distance_type,nprobes,ef,refine_factor,fast_search,bypass_vector_index,prefilter,lower_bound,upper_bound,with_row_id,offset,version"
}
```

### Advanced Configuration (Optional)

The connector supports configurable retry and timeout behavior for improved reliability:

**Advanced Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_retries` | integer | 5 | Maximum number of retry attempts for failed requests |
| `initial_retry_delay` | float | 1.0 | Initial delay between retries (doubles each attempt) |
| `request_timeout` | integer | 30 | HTTP request timeout in seconds |

**Note**: These advanced parameters are typically set during connector initialization and apply to all operations.

### Obtaining Your Credentials

1. **Log in to LanceDB Cloud**: Visit [cloud.lancedb.com](https://cloud.lancedb.com)
2. **Create or Select Project**: Navigate to your project or create a new one
3. **Generate API Key**: 
   - Go to Settings → API Keys
   - Click "Create New API Key"
   - Copy the key immediately (it won't be shown again)
4. **Note Your Details**:
   - Project name is shown in your project dashboard
   - Region is displayed next to your project name

---

## Supported Tables

The connector automatically discovers all tables in your LanceDB project. Each table corresponds to a vector collection in your database.

**Common Table Types:**
- Document embeddings
- Image vectors
- User/product embeddings
- Any custom vector collections

---

## Table-Specific Options

When reading from a specific table, you can customize the behavior with these options.

For full API documentation, see: https://docs.lancedb.com/api-reference/rest/table/query-a-table

### Basic Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch_size` | integer | 1000 | Number of records to fetch per batch (1-10,000) |
| `use_full_scan` | boolean | true | Enable full table scan (generates dummy vector if needed) |
| `columns` | array | null | Specific columns to retrieve (for performance) |
| `filter_expression` | string | null | SQL-like filter expression |
| `cursor_field` | string | null | Field to use for incremental reads |

### Vector Search Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `query_vector` | array | null | Vector for similarity search (list of floats) |
| `vector_column` | string | null | Name of vector column to search (auto-detected if not specified) |
| `distance_type` | string | null | Distance metric: `cosine`, `l2`, `dot` |

### Index Optimization Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `nprobes` | integer | null | Number of probes for IVF index (higher = more accurate, slower) |
| `ef` | integer | null | Search effort for HNSW index (higher = more accurate, slower) |
| `refine_factor` | integer | null | Refine factor for search quality |
| `fast_search` | boolean | null | Use fast search mode |
| `bypass_vector_index` | boolean | null | Skip vector index (perform full scan) |

### Filtering Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `prefilter` | boolean | true | Apply filter before vector search (recommended for large datasets) |
| `lower_bound` | float | null | Lower bound for distance filtering |
| `upper_bound` | float | null | Upper bound for distance filtering |

### Result Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `with_row_id` | boolean | null | Include `_rowid` column in results |
| `offset` | integer | null | Number of results to skip (for pagination) |
| `version` | integer | null | Query specific table version |

### Examples

**Full Table Scan:**
```json
{
  "batch_size": "1000",
  "use_full_scan": "true"
}
```

**Select Specific Columns (Performance Optimization):**
```json
{
  "batch_size": "1000",
  "use_full_scan": "true",
  "columns": "[\"id\", \"name\", \"vector\", \"created_at\"]"
}
```

**Vector Similarity Search:**
```json
{
  "batch_size": "100",
  "query_vector": [0.1, 0.2, 0.3, ..., 0.768],
  "distance_type": "cosine"
}
```

**Optimized Vector Search with IVF Index:**
```json
{
  "batch_size": "100",
  "query_vector": [0.1, 0.2, 0.3, ..., 0.768],
  "nprobes": "20",
  "refine_factor": "10",
  "distance_type": "cosine"
}
```

**Pre-filtered Vector Search:**
```json
{
  "batch_size": "100",
  "query_vector": [0.1, 0.2, 0.3, ..., 0.768],
  "filter_expression": "price > 100 AND category = 'electronics'",
  "prefilter": "true"
}
```

**Filtered Read:**
```json
{
  "batch_size": "500",
  "filter_expression": "price > 100",
  "use_full_scan": "true"
}
```

**Incremental Read:**
```json
{
  "batch_size": "1000",
  "cursor_field": "updated_at",
  "use_full_scan": "true"
}
```

---

## Data Types

The connector maps LanceDB data types to standard types as follows:

| LanceDB Type | Mapped Type | Notes |
|--------------|-------------|-------|
| `string`, `utf8` | String | Text data |
| `int32`, `int` | Integer | 32-bit integers |
| `int64`, `long` | Long | 64-bit integers |
| `float32`, `float` | Float | Single precision |
| `float64`, `double` | Double | Double precision |
| `bool`, `boolean` | Boolean | True/false values |
| `binary` | Binary | Binary data |
| `fixed_size_list` | Array | Vector embeddings |
| `list` | Array | Variable-length arrays |
| `date` | Date | Date values |
| `timestamp` | Timestamp | Date and time |

---

## Features & Limitations

### Supported Features ✅

- **Dynamic Table Discovery**: Automatically finds all tables
- **Schema Introspection**: Retrieves complete table schemas
- **Vector Search**: Supports similarity search with query vectors
- **Full Scans**: Read entire tables without vectors
- **Batch Processing**: Configurable batch sizes for efficient reads
- **Incremental Reads**: Cursor-based incremental data sync
- **Filtering**: SQL-like filter expressions
- **Column Selection**: Read specific columns only

### Limitations ⚠️

- **Vector Requirements**: LanceDB is a vector database. For best performance, provide query vectors for similarity search
- **API Format**: Data is returned in Apache Arrow format (handled automatically)
- **Batch Size Limits**: Maximum 10,000 records per batch
- **Rate Limits**: Subject to LanceDB Cloud rate limits

---

## Performance Considerations

### Best Practices

1. **Use Appropriate Batch Sizes**:
   - Small datasets: 100-1,000 records
   - Large datasets: 1,000-10,000 records
   - Balance between memory and network efficiency

2. **Leverage Vector Search**:
   - Provide query vectors for faster, more relevant results
   - Use filters to narrow down results

3. **Incremental Reads**:
   - Use `cursor_field` for large tables
   - Reduces data transfer and processing time

4. **Column Selection**:
   - Specify only needed columns
   - Reduces bandwidth and parsing time

### Expected Performance

- **Table Listing**: < 1 second
- **Schema Retrieval**: < 1 second per table
- **Data Reading**: Varies by table size and batch size
  - Small tables (< 1,000 rows): < 2 seconds
  - Medium tables (1,000-10,000 rows): 2-10 seconds
  - Large tables (> 10,000 rows): Pagination recommended

---

## Troubleshooting

### Common Issues

#### 1. Authentication Errors

**Error**: `Missing required parameter: api_key`

**Solution**:
- Verify your API key is correctly set in configuration
- Ensure the key hasn't been revoked or expired
- Check for extra spaces or characters in the key

#### 2. Connection Failures

**Error**: `400 Bad Request` or `401 Unauthorized`

**Solution**:
- Verify project name and region are correct
- Check API key permissions
- Ensure project exists and is accessible

#### 3. Vector Dimension Mismatch

**Error**: `No vector column found to match with the query vector dimension`

**Solution**:
- Ensure query vector dimension matches table's vector column
- Use `use_full_scan: true` for non-vector queries
- Check table schema to verify vector dimensions

#### 4. Empty Results

**Possible Causes**:
- Table is empty
- Filter expression too restrictive
- Incorrect table name

**Solution**:
- Verify table has data using LanceDB Cloud UI
- Check filter expression syntax
- List all tables to confirm table name

#### 5. Timeout Errors

**Solution**:
- Reduce batch size
- Use pagination for large tables
- Check network connectivity

---

## Security Notes

- **Never commit credentials**: Keep `api_key` in secure configuration files
- **Use environment variables**: Store sensitive data outside of code
- **Rotate keys regularly**: Generate new API keys periodically
- **Minimal permissions**: Use keys with least necessary privileges
- **Monitor usage**: Track API key usage in LanceDB Cloud dashboard

---

## Example Usage

### Basic Connection and Data Reading

Once configured, the connector allows you to:
1. **Discover available tables** in your LanceDB project
2. **Inspect table schemas** to understand data structure
3. **Read data** from tables with various options

**Workflow Example:**

```python
# Step 1: Discover tables
# The connector will list all available tables in your project
# Example output: ['documents', 'embeddings', 'user_vectors']

# Step 2: Understand table structure
# Retrieve schema information for a specific table
# Shows column names, data types, and vector dimensions

# Step 3: Read data
# Configure how you want to read the data:
table_options = {
    "batch_size": "100",      # Number of records per batch
    "use_full_scan": "true"   # Enable full table scan
}

# The connector will fetch data in batches
# Returns records as dictionaries with all columns
```

**Reading All Data:**

```python
# For small to medium tables, read all data at once
table_options = {
    "batch_size": "1000",
    "use_full_scan": "true"
}

# Data is returned as a list of records (dictionaries)
# Each record contains all columns defined in the schema
```

### Vector Similarity Search

For AI/ML use cases, you can perform similarity searches using query vectors:

```python
# Prepare your query vector
# This should come from your embedding model (e.g., OpenAI, HuggingFace)
# Dimension must match the table's vector column
query_embedding = [0.1, 0.2, 0.3, ...]  # Example: 768 dimensions

# Configure similarity search
table_options = {
    "batch_size": "10",          # Return top 10 most similar results
    "query_vector": query_embedding
}

# Results are automatically sorted by similarity
# Most similar vectors appear first
# Each result includes a '_distance' field indicating similarity score
```

**Use Cases:**
- Semantic search over documents
- Finding similar images
- Product recommendations
- Question answering systems

### Incremental Sync

For large datasets that update frequently, use incremental synchronization:

```python
# Configure incremental read using a timestamp or version field
table_options = {
    "cursor_field": "updated_at",    # Field that tracks updates
    "batch_size": "1000",
    "use_full_scan": "true"
}

# First sync: Read all existing data
# The connector tracks the highest 'updated_at' value seen

# Subsequent syncs: Only read new or updated records
# Automatically uses the last known cursor value
# Significantly reduces data transfer and processing time
```

**Benefits:**
- Efficient synchronization of large tables
- Automatic cursor management
- Reduced bandwidth and processing
- Keeps data up-to-date with minimal overhead

**Best Practices:**
- Choose a monotonically increasing field (timestamp, version number)
- Ensure the cursor field is indexed in LanceDB
- Run incremental syncs on a schedule (e.g., hourly, daily)

---

## Support & Resources

- **LanceDB Documentation**: [docs.lancedb.com](https://docs.lancedb.com)
- **LanceDB Cloud**: [cloud.lancedb.com](https://cloud.lancedb.com)
- **API Reference**: [docs.lancedb.com/api-reference](https://docs.lancedb.com/api-reference)
- **Community**: [Discord](https://discord.gg/lancedb)

---

## Version History

- **v1.0** (Current): Initial production release
  - Full table discovery and schema introspection
  - Vector search and full scan support
  - Apache Arrow format parsing
  - Batch processing and pagination
  - Incremental read support

---

## License

This connector is part of the Lakeflow Community Connectors project.
