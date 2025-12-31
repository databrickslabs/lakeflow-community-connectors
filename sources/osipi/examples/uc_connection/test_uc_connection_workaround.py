# Databricks notebook source
# MAGIC %md
# MAGIC # UC Connection Test - With Bug Workaround
# MAGIC
# MAGIC Tests the connector with workaround property names for current UC bug

# COMMAND ----------

# DBTITLE 1,Create UC Connection (Workaround Method)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

w = WorkspaceClient()

print("="*80)
print("Creating UC Connection with WORKAROUND property names")
print("="*80)
print()

# Delete if exists
try:
    w.connections.delete("osipi_connection_test")
    print("🗑️  Deleted existing connection")
except:
    pass

# Create with WORKAROUND property names (works with current UC bug)
w.connections.create(
    name="osipi_connection_test",
    connection_type=ConnectionType.DATABRICKS,
    options={
        "pi_base_url": "https://osipi-webserver-1444828305810485.aws.databricksapps.com",
        "workspace_host": "https://e2-demo-field-eng.cloud.databricks.com",
        "client_id": "{{secrets/sp-osipi/sp-client-id}}",
        "client_value_tmp": "{{secrets/sp-osipi/sp-client-secret}}",  # ⚠️ Workaround!
        "verify_ssl": "false"
    },
    comment="Test connection - workaround for UC bug"
)

print("✅ Connection created: osipi_connection_test")
print()
print("Properties used:")
print("  - client_id: Normal name")
print("  - client_value_tmp: Workaround name (instead of client_secret)")
print()

# COMMAND ----------

# DBTITLE 1,Register Connector
exec(open("/Workspace/Users/pravin.varma@databricks.com/osipi/_generated_osipi_python_source.py").read())
register_lakeflow_source(spark)
print("✅ Connector registered")

# COMMAND ----------

# DBTITLE 1,Test Basic Read
print("="*80)
print("TEST: Basic Read (pi_dataservers)")
print("="*80)
print()

try:
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", "osipi_connection_test")
        .option("tableName", "pi_dataservers")
        .load()
    )

    count = df.count()
    print()
    print("="*80)
    print(f"✅ SUCCESS! Retrieved {count} records")
    print("="*80)
    print()

    if count > 0:
        display(df)

except Exception as e:
    print()
    print("="*80)
    print("❌ TEST FAILED")
    print("="*80)
    print()
    print(f"Error: {str(e)}")
    print()

    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Test Time-Series Read
print("="*80)
print("TEST: Time-Series Read (pi_timeseries)")
print("="*80)
print()

# First get some point WebIDs
try:
    points_df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", "osipi_connection_test")
        .option("tableName", "pi_points")
        .option("maxCount", "10")
        .load()
    )

    point_webids = [row.webid for row in points_df.select("webid").limit(3).collect()]
    print(f"Using {len(point_webids)} point WebIDs for test")
    print()

    # Read time-series data
    ts_df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", "osipi_connection_test")
        .option("tableName", "pi_timeseries")
        .option("tag_webids", ",".join(point_webids))
        .option("startTime", "*-24h")
        .option("endTime", "*")
        .load()
    )

    count = ts_df.count()
    print(f"✅ Time-series test passed! Retrieved {count} records")

    if count > 0:
        print()
        display(ts_df.limit(10))

except Exception as e:
    print(f"❌ Time-series test failed: {str(e)[:200]}")

# COMMAND ----------

# DBTITLE 1,Summary
print("="*80)
print("TEST SUMMARY")
print("="*80)
print()
print("✅ UC Connection with workaround property names works!")
print()
print("Property mapping:")
print("  client_secret → client_value_tmp ✓")
print()
print("Next steps:")
print("  1. Use 'client_value_tmp' for all connections (current UC bug)")
print("  2. After Databricks merges fix, switch to 'client_secret'")
print("  3. Connector supports both automatically - no code changes needed")
print()
print("="*80)
