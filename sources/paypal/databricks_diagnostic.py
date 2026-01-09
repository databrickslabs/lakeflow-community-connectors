"""
Run this in your Databricks notebook to diagnose the subscriptions issue

Copy this entire file content and paste into a Databricks notebook cell
"""

# Test 1: Check connection
print("="*70)
print("TEST 1: Connection Configuration")
print("="*70)

try:
    connection_info = spark.sql("DESCRIBE CONNECTION paypal_v2").collect()
    print("\n✅ Connection exists:")
    for row in connection_info:
        print(f"  {row.info_name}: {row.info_value}")
    
    # Check if subscription_ids is in allowlist
    allowlist_rows = [row for row in connection_info if row.info_name == "externalOptionsAllowList"]
    if allowlist_rows:
        allowlist = allowlist_rows[0].info_value
        if "subscription_ids" in allowlist:
            print("\n✅ subscription_ids IS in externalOptionsAllowList")
        else:
            print("\n❌ PROBLEM: subscription_ids NOT in externalOptionsAllowList")
            print(f"   Current allowlist: {allowlist}")
            print("\n   FIX: Run this SQL:")
            print("""
DROP CONNECTION IF EXISTS paypal_v2;
CREATE CONNECTION paypal_v2 TYPE LAKEFLOW OPTIONS (
    client_id = 'Acpqp1DwjKoGnOxGJllN1BeS0PBc-thgcOMy2cgnSm0o67X8ReSGCSA2DBQ0_wstTV16AoolSc_l3Ja5',
    client_secret = 'EPJMwfXdAM-bUftXFuaLYziuTTWvwwzaC3ym4dCmBc4FWz7BMDvuyr3dyoFwZz8-PV7oh8WpfKnEDCF8',
    environment = 'sandbox',
    externalOptionsAllowList = 'start_date,end_date,page_size,subscription_ids,include_transactions,product_id,plan_ids'
);
            """)
except Exception as e:
    print(f"\n❌ Connection error: {e}")
    print("\n   Connection might not exist or you don't have permission to view it")

# Test 2: Load connector
print("\n" + "="*70)
print("TEST 2: Load PayPal Connector")
print("="*70)

try:
    from pipeline.ingestion_pipeline import ingest
    from libs.source_loader import get_register_function
    
    source_name = "paypal"
    register_lakeflow_source = get_register_function(source_name)
    register_lakeflow_source(spark)
    
    print("✅ PayPal connector loaded successfully")
except Exception as e:
    print(f"❌ Error loading connector: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Test subscriptions table directly
print("\n" + "="*70)
print("TEST 3: Test Subscriptions Table Directly")
print("="*70)

subscription_ids = ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]

try:
    # Try to read subscriptions using Spark
    df = spark.read.format("lakeflow") \
        .option("connection", "paypal_v2") \
        .option("sourceTable", "subscriptions") \
        .option("subscription_ids", ",".join(subscription_ids)) \
        .load()
    
    count = df.count()
    print(f"\n✅ Subscriptions read: {count} records")
    
    if count > 0:
        print("\nFirst 5 records:")
        df.select("id", "status", "plan_id").show(5, truncate=False)
    else:
        print("\n❌ PROBLEM: 0 records retrieved")
        print("\nPossible causes:")
        print("  1. subscription_ids not being passed correctly")
        print("  2. Connector code not updated in workspace")
        print("  3. Connection not allowing external options")
        
except Exception as e:
    print(f"\n❌ Error reading subscriptions: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Check existing table
print("\n" + "="*70)
print("TEST 4: Check Existing Subscriptions Table")
print("="*70)

try:
    count = spark.sql("""
        SELECT COUNT(*) as count 
        FROM alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions
    """).collect()[0].count
    
    print(f"Current records in table: {count}")
    
    if count > 0:
        print("\n✅ Table has data! Showing sample:")
        spark.sql("""
            SELECT id, status, plan_id, update_time
            FROM alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions
            LIMIT 5
        """).show(truncate=False)
    else:
        print("\n❌ Table is empty")
        
except Exception as e:
    print(f"Error checking table: {e}")
    print("Table might not exist yet")

# Test 5: Check pipeline spec
print("\n" + "="*70)
print("TEST 5: Check Pipeline Configuration")
print("="*70)

print("\nYour ingest.py should have:")
print("""
{
    "table": {
        "source_table": "subscriptions",
        "table_configuration": {
            "subscription_ids": ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]
        }
    }
}
""")

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print("""
If all tests pass but table is still empty:

1. Check Databricks job/pipeline logs for errors
2. Verify the pipeline is actually running the subscriptions table
3. Try running just subscriptions table alone:

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

register_lakeflow_source = get_register_function("paypal")
register_lakeflow_source(spark)

pipeline_spec = {
    "connection_name": "paypal_v2",
    "objects": [
        {
            "table": {
                "source_table": "subscriptions",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "subscriptions",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "subscription_ids": ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]
                }
            }
        }
    ]
}

ingest(spark, pipeline_spec)

Then check: SELECT * FROM alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions
""")
print("="*70)

