"""
Quick test to verify the subscriptions fix is working
Run this in Databricks to test the connector directly
"""

# Test the connector directly (not through SDP)
import sys
sys.path.insert(0, '/Workspace/path/to/your/repo')  # Update this path!

from sources.paypal.paypal import LakeflowConnect

# Initialize connector
connector = LakeflowConnect({
    "client_id": "Acpqp1DwjKoGnOxGJllN1BeS0PBc-thgcOMy2cgnSm0o67X8ReSGCSA2DBQ0_wstTV16AoolSc_l3Ja5",
    "client_secret": "EPJMwfXdAM-bUftXFuaLYziuTTWvwwzaC3ym4dCmBc4FWz7BMDvuyr3dyoFwZz8-PV7oh8WpfKnEDCF8",
    "environment": "sandbox"
})

print("="*70)
print("TESTING SUBSCRIPTIONS FIX")
print("="*70)

# Test table options
table_options = {
    "subscription_ids": ["I-S45EDF98N3AV", "I-FWEC6DA9AKVJ", "I-D6JVS5Y2V12P", "I-GN66ML8DL6NC", "I-BJK104AU5722"]
}

print("\n[TEST 1] Calling read with start_offset=None")
try:
    records_iter, next_offset = connector.read("subscriptions", None, table_options)
    records = list(records_iter)
    
    print(f"✅ Returned {len(records)} records")
    print(f"✅ Next offset: {next_offset}")
    
    if len(records) == 5:
        print("\n✅✅✅ SUCCESS! All 5 subscriptions returned!")
        for rec in records:
            print(f"  - {rec.get('id')}: {rec.get('status')}")
    elif len(records) == 1:
        print("\n❌ PROBLEM: Only 1 record returned (old code still running)")
        print("   You need to pull the latest code!")
    elif len(records) == 0:
        print("\n❌ PROBLEM: 0 records returned")
        print("   Check API credentials or subscription IDs")
    else:
        print(f"\n⚠️  Unexpected: {len(records)} records returned")
        
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

print("\n[TEST 2] Calling read with start_offset={'done': True}")
try:
    records_iter, next_offset = connector.read("subscriptions", {"done": True}, table_options)
    records = list(records_iter)
    
    print(f"✅ Returned {len(records)} records (should be 0)")
    print(f"✅ Next offset: {next_offset}")
    
    if len(records) == 0 and next_offset.get("done"):
        print("\n✅ Pagination working correctly!")
    else:
        print("\n❌ Pagination issue detected")
        
except Exception as e:
    print(f"\n❌ ERROR: {e}")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)

