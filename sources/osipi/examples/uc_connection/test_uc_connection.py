# Databricks notebook source
# MAGIC %md
# MAGIC # Test UC Connection (No Fallbacks)
# MAGIC
# MAGIC This tests the recommended UC Connection pattern with NO fallback to dbutils.secrets

# COMMAND ----------

# DBTITLE 1,Step 1: Create UC Connection
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

w = WorkspaceClient()

# Delete if exists
try:
    w.connections.delete("osipi_connection")
    print("🗑️ Deleted existing connection")
except Exception:
    print("ℹ️ No existing connection to delete")

# Create with normal property names (using secret references)
w.connections.create(
    name="osipi_connection",
    connection_type=ConnectionType.DATABRICKS,
    options={
        "pi_base_url": "https://osipi-webserver-1444828305810485.aws.databricksapps.com",
        "workspace_host": "https://e2-demo-field-eng.cloud.databricks.com",
        "client_id": "{{secrets/sp-osipi/sp-client-id}}",
        "client_secret": "{{secrets/sp-osipi/sp-client-secret}}",
        "verify_ssl": "false",
    },
    comment="OSI PI connector test - UC Connection only (no fallbacks)",
)

print("✅ UC Connection created: osipi_connection")

# COMMAND ----------

# DBTITLE 1,Step 2: Verify Connection Properties
# Read connection back to see what's stored
conn = w.connections.get("osipi_connection")
print("📋 Connection options stored:")
for k, v in (conn.options or {}).items():
    if k in ("client_secret", "password"):
        print(f"  {k}: {'***' if v else 'MISSING'}")
    else:
        print(f"  {k}: {v}")

# COMMAND ----------

# DBTITLE 1,Step 3: Register Connector
exec(open("/Workspace/Users/pravin.varma@databricks.com/osipi/_generated_osipi_python_source.py").read())
register_lakeflow_source(spark)
print("✅ Connector registered")

# COMMAND ----------

# DBTITLE 1,Step 4: Test Read (NO manual credentials!)
print("=" * 80)
print("TESTING UC CONNECTION READ")
print("=" * 80)
print()

try:
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", "osipi_connection")  # Only this!
        .option("tableName", "pi_dataservers")
        .load()
    )

    print()
    print("=" * 80)
    print("✅ SUCCESS! UC Connection works!")
    print("=" * 80)
    print()

    count = df.count()
    print(f"Retrieved {count} records")

    if count > 0:
        print()
        display(df)

except Exception as e:
    print()
    print("=" * 80)
    print("❌ FAILED!")
    print("=" * 80)
    print()
    print(f"Error: {str(e)}")
    print()
    print("Check the output above to see what properties were received")
    print("from the UC Connection.")

    import traceback

    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Step 5: Test Multiple Tables
if "df" in locals():
    print("Testing additional tables...")

    tables_to_test = [
        "pi_points",
        "pi_current_value",
        "pi_assetservers",
    ]

    for table in tables_to_test:
        try:
            test_df = (
                spark.read.format("lakeflow_connect")
                .option("databricks.connection", "osipi_connection")
                .option("tableName", table)
                .load()
            )
            count = test_df.count()
            print(f"✅ {table}: {count} records")
        except Exception as e:
            print(f"❌ {table}: {str(e)[:100]}")
