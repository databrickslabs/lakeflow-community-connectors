# Lakeflow Community Connectors APIs

Lakeflow Community Connectors are built on top of the Python Data Source API. Each source connector is implemented as a Spark batch and/or streaming source and is integrated with a configurable, shared Spark Declarative Pipeline (SDP).

## Lakeflow Community Connectors Template
The template introduces an abstraction layer that simplifies the implementation of Python data sources. It provides shared libraries and common utilities, so developers only need to implement a single class with a small set of Python data source functions.
We strongly recommend using this approach to implement community connectors, especially those built on REST APIs.

Please refer to [lakeflow_connect.py](lakeflow_connect.py) for more details.


## Direct Implementation of Python Data Source API
While it is not recommended, developers can also choose to implement the Python Data Source API directly if they need greater flexibility, for example: 
 - Customizing the logic for data partitioning
 - Customizing how data is parsed into a specialized schema 
 - Supporting other advanced or source-specific requirements 

To integrate with the Spark Declarative Pipeline (SDP) used by Lakeflow Community Connectors, a Python Data Source implementation must support the following Spark APIs:

```python
# API to read Metadata
# Schema:  
#  tableName STRING NOT NULL, 
#  primary_keys ARRAY<STRING>, 
#  cursor_field STRING,
#  ingestion_type STRING
# ingestion_type: snapshot, cdc, cdc_with_deletes, append

spark.read.format("lakeflow_connect")
     .option("databricks.connection", connection_name)
     .option("tableName", "_community_table_metadata")
     .option("tableNameList", json.dumps(table_list))
     .load()

# API to discover namespaces (only for connectors that implement SupportsNamespaces)
# Schema:
#   namespace ARRAY<STRING>
# Returns the immediate child namespaces under the given prefix.
# Walk the full tree by recursing on each returned row.
# Connectors without SupportsNamespaces return zero rows.
spark.read.format("lakeflow_connect")
     .option("databricks.connection", connection_name)
     .option("tableName", "_community_namespaces")
     # Optional. JSON-encoded list[str]. Absent / "[]" = root namespaces.
     .option("namespacePrefix", json.dumps(["orgA"]))
     .load()

# API to list tables in one namespace
# Schema:
#   namespace ARRAY<STRING>,
#   table_name STRING
# For connectors that implement SupportsNamespaces, the `namespace` option is required.
# Use "[]" for root-level tables. Walk the tree via _community_namespaces to find namespaces.
# Flat connectors (no SupportsNamespaces) ignore the option and return every table with an empty namespace.
spark.read.format("lakeflow_connect")
     .option("databricks.connection", connection_name)
     .option("tableName", "_community_tables")
     .option("namespace", json.dumps(["orgA", "repo1"]))
     .load()


# API to batch read
# required if the table supports snapshot ingestion type
spark.read.format("lakeflow_connect")
     .option("databricks.connection", connection_name)
     .option("tableName", source_table)
     .options(<other custom options>)
     .load()

# API to streaming read
# required if the table supports cdc, cdc_with_deletes, or append ingestion type
spark.readStream.format("lakeflow_connect")
     .option("databricks.connection", connection_name)
     .option("tableName", source_table)
     .options(<other custom options>)
     .load()

# API to streaming read for delete flow
# required if the table supports cdc_with_deletes ingestion type
# The isDeleteFlow option triggers the connector to return deleted records
# which are then applied as deletes to the destination table
spark.readStream.format("lakeflow_connect")
     .option("databricks.connection", connection_name)
     .option("tableName", source_table)
     .option("isDeleteFlow", "true")
     .options(<other custom options>)
     .load()
```

The connection must be a dedicated Unity Catalog (UC) connection for community connectors. The Python data source format must be set to “lakeflow_connect” so that key-value options from the UC connection are automatically injected into the Python Data Source API classes.
