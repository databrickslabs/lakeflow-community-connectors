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
    print("\n✅ Connection exists")
    print("\nConnection details:")
    for row in connection_info:
        print(f"  {row}")
    
    print("\nNOTE: To manually check if subscription_ids is allowed:")
    print("  Look at the connection definition when it was created")
    print("  Or try using subscription_ids - if it fails, it's not in allowlist")
    
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

# Test 3: Test if subscription_ids is allowed (REAL TEST)
print("\n" + "="*70)
print("TEST 3: Test if subscription_ids Option is Allowed")
print("="*70)

subscription_ids = ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]

print("\nAttempting to read with subscription_ids option...")
try:
    # Try to read subscriptions using Spark
    df = spark.read.format("lakeflow") \
        .option("connection", "paypal_v2") \
        .option("sourceTable", "subscriptions") \
        .option("subscription_ids", ",".join(subscription_ids)) \
        .load()
    
    print("✅ subscription_ids option was ACCEPTED by connection")
    print("   (This means it's in the externalOptionsAllowList)")
    
    count = df.count()
    print(f"\n✅ Retrieved {count} subscription record(s)")
    
    if count > 0:
        print("\nFirst 5 records:")
        df.select("id", "status", "plan_id").show(5, truncate=False)
    else:
        print("\n❌ PROBLEM: 0 records retrieved")
        print("\nThe option is allowed, but no data returned. Possible causes:")
        print("  1. Connector code issue (pagination not working)")
        print("  2. Subscription IDs are invalid")
        print("  3. API authentication problem")
        
except Exception as e:
    error_msg = str(e)
    
    if "not allowed by connection" in error_msg or "externalOptionsAllowList" in error_msg:
        print("\n❌ PROBLEM FOUND: subscription_ids NOT in externalOptionsAllowList")
        print(f"\n   Error: {error_msg}")
        print("\n   FIX: Update your connection with this SQL:")
        print("""
DROP CONNECTION IF EXISTS paypal_v2;
CREATE CONNECTION paypal_v2 TYPE LAKEFLOW OPTIONS (
    client_id = 'Acpqp1DwjKoGnOxGJllN1BeS0PBc-thgcOMy2cgnSm0o67X8ReSGCSA2DBQ0_wstTV16AoolSc_l3Ja5',
    client_secret = 'EPJMwfXdAM-bUftXFuaLYziuTTWvwwzaC3ym4dCmBc4FWz7BMDvuyr3dyoFwZz8-PV7oh8WpfKnEDCF8',
    environment = 'sandbox',
    externalOptionsAllowList = 'start_date,end_date,page_size,subscription_ids,include_transactions,product_id,plan_ids'
);
        """)
    else:
        print(f"\n❌ Different error: {error_msg}")
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

