"""
Verify the subscriptions fix is loaded in Databricks
Copy this entire cell and run it in your Databricks notebook
"""

print("="*70)
print("VERIFYING SUBSCRIPTIONS FIX IS LOADED")
print("="*70)

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# Step 1: Register the connector
print("\n[STEP 1] Registering PayPal connector...")
register_lakeflow_source = get_register_function("paypal")
register_lakeflow_source(spark)

# Step 2: Get the connector instance
print("\n[STEP 2] Getting connector instance...")
try:
    # Get the registered connector class
    connector = spark._jvm.io.delta.lakeflow.python.PythonSourceRegistry.getSource("paypal")
    print("✅ Connector retrieved from registry")
except Exception as e:
    print(f"⚠️  Cannot access connector via JVM: {e}")
    print("   This is OK - testing differently...")

# Step 3: Test actual behavior (this is the real test!)
print("\n[STEP 3] Testing actual subscriptions read behavior...")

subscription_ids = ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]

pipeline_spec = {
    "connection_name": "paypal_v2",
    "objects": [{
        "table": {
            "source_table": "subscriptions",
            "destination_catalog": "alex_owen_the_unity_catalog",
            "destination_schema": "paypal_comunity_connector_a0",
            "destination_table": "subscriptions_version_test",
            "table_configuration": {
                "scd_type": "SCD_TYPE_1",
                "subscription_ids": subscription_ids
            }
        }
    }]
}

try:
    print("   Running ingestion to test table...")
    ingest(spark, pipeline_spec)
    
    # Check results
    result_df = spark.sql("""
        SELECT * FROM alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions_version_test
    """)
    
    count = result_df.count()
    
    print(f"\n   Retrieved {count} records")
    
    if count == 5:
        print("\n" + "="*70)
        print("✅✅✅ NEW CODE IS LOADED! FIX IS WORKING!")
        print("="*70)
        print("\nSubscriptions found:")
        result_df.select("id", "status", "plan_id").show(5, truncate=False)
        
        print("\nNow your main subscriptions table should work!")
        print("Clean up test table:")
        print("DROP TABLE alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions_version_test")
        
    elif count == 1:
        print("\n" + "="*70)
        print("❌ OLD CODE STILL RUNNING!")
        print("="*70)
        print("\nOnly 1 record means the old pagination bug is still active")
        print("\nFIX:")
        print("1. Make sure you ran: git pull origin master")
        print("2. RESTART YOUR CLUSTER (Compute → Restart)")
        print("3. Re-run this test")
        
    elif count == 0:
        print("\n" + "="*70)
        print("❌ 0 RECORDS - DIFFERENT ISSUE")
        print("="*70)
        print("\nPossible causes:")
        print("1. Subscription IDs are invalid")
        print("2. API credentials issue")
        print("3. Connection not configured correctly")
        
    else:
        print(f"\n⚠️  Unexpected: {count} records")
        
except Exception as e:
    error_msg = str(e)
    print(f"\n❌ ERROR: {error_msg}")
    
    if "not allowed by connection" in error_msg:
        print("\n" + "="*70)
        print("CONNECTION ISSUE")
        print("="*70)
        print("\nRun this SQL to fix:")
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
        import traceback
        traceback.print_exc()

print("\n" + "="*70)
print("VERIFICATION COMPLETE")
print("="*70)

