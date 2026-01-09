"""
DATABRICKS SUBSCRIPTIONS TEST
Copy everything below and paste into a Databricks notebook
"""

# ============================================================================
# STEP 1: Set up paths (UPDATE THIS!)
# ============================================================================
import sys

# UPDATE THIS PATH to where your repo is in Databricks
# Examples:
#   Repos: '/Workspace/Repos/alex.owen@databricks.com/lakeflow-community-connectors'
#   Workspace: '/Workspace/Users/alex.owen@databricks.com/lakeflow-community-connectors'
REPO_PATH = '/Workspace/Repos/YOUR_USERNAME/lakeflow-community-connectors'  # UPDATE THIS!

sys.path.insert(0, REPO_PATH)

print(f"Using repo path: {REPO_PATH}")

# ============================================================================
# STEP 2: Import and test
# ============================================================================
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

print("✅ Imports successful!")

# ============================================================================
# STEP 3: Register connector
# ============================================================================
register_lakeflow_source = get_register_function("paypal")
register_lakeflow_source(spark)

print("✅ Connector registered!")

# ============================================================================
# STEP 4: Test subscriptions with small pipeline
# ============================================================================
subscription_ids = ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]

pipeline_spec = {
    "connection_name": "paypal_v2",
    "objects": [{
        "table": {
            "source_table": "subscriptions",
            "destination_catalog": "alex_owen_the_unity_catalog",
            "destination_schema": "paypal_comunity_connector_a0",
            "destination_table": "subscriptions_test_fix",
            "table_configuration": {
                "scd_type": "SCD_TYPE_1",
                "subscription_ids": subscription_ids
            }
        }
    }]
}

print("\n" + "="*70)
print("Running ingestion...")
print("="*70)

ingest(spark, pipeline_spec)

# ============================================================================
# STEP 5: Check results
# ============================================================================
print("\n" + "="*70)
print("Checking results...")
print("="*70)

result_df = spark.sql("""
    SELECT * FROM alex_owen_the_unity_catalog.paypal_comunity_connector_a0.subscriptions_test_fix
""")

count = result_df.count()

print(f"\nRecords found: {count}")

if count == 5:
    print("\n" + "🎉"*20)
    print("✅✅✅ SUCCESS! NEW CODE IS WORKING!")
    print("🎉"*20)
    result_df.select("id", "status", "plan_id", "create_time").show(10, truncate=False)
    print("\n✅ Your subscriptions table should now work!")
    
elif count == 1:
    print("\n❌ OLD CODE STILL RUNNING!")
    print("Fix: RESTART YOUR CLUSTER, then re-run this test")
    
elif count == 0:
    print("\n❌ NO RECORDS")
    print("Check:")
    print("  1. Are subscription IDs valid?")
    print("  2. Is connection configured correctly?")
    print("  3. Check logs above for API errors")
    
else:
    print(f"\n⚠️  Unexpected: {count} records")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)

