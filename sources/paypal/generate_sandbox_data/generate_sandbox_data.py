#!/usr/bin/env python3
"""
Generate test data in PayPal Sandbox for ALL connector tables

This script creates test data for all 14 PayPal connector tables:
1. Products (catalog products)
2. Plans (billing plans linked to products)
3. Subscriptions (subscriptions linked to plans)
4. Transactions (payment captures)
5. Payment Captures (derived from transactions)
6. Payment Experiences (web payment profiles)
7. Tracking (shipment tracking information)
8. Refunds (refund transactions)
9. Payment Authorizations (authorization-only orders)
10. Payouts (payout batches) - may require additional setup
11. Disputes - AUTO-GENERATED (cannot be created programmatically)
12. Webhooks Events - AUTO-GENERATED (created by PayPal actions)
13. Invoices - REQUIRES SPECIAL PERMISSIONS (not available in basic Sandbox)
14. Orders - Already created during transaction generation
"""

import requests
import base64
import json
import uuid
import random
import time
from datetime import datetime, timedelta

# Base URL for sandbox
BASE_URL = 'https://api-m.sandbox.paypal.com'

def get_access_token(client_id, client_secret):
    """Get OAuth 2.0 access token."""
    token_url = f"{BASE_URL}/v1/oauth2/token"
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    
    data = {"grant_type": "client_credentials"}
    response = requests.post(token_url, headers=headers, data=data, timeout=30)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to get access token: {response.status_code} {response.text}")
    
    return response.json().get("access_token")


def create_product(access_token, name, description, product_type="SERVICE", category="SOFTWARE"):
    """Create a product in the catalog."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "PayPal-Request-Id": str(uuid.uuid4()),
    }
    
    payload = {
        "name": name,
        "description": description,
        "type": product_type,
        "category": category,
        "image_url": "https://example.com/product.jpg",
        "home_url": "https://example.com"
    }
    
    response = requests.post(
        f"{BASE_URL}/v1/catalogs/products",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        product = response.json()
        return {
            "product_id": product.get("id"),
            "name": product.get("name"),
            "status": "created"
        }
    else:
        return {
            "error": f"{response.status_code}: {response.text}",
            "name": name
        }


def create_billing_plan(access_token, product_id, plan_name, plan_description, amount, currency="USD", frequency="MONTH"):
    """Create a billing plan for a product."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    
    payload = {
        "product_id": product_id,
        "name": plan_name,
        "description": plan_description,
        "billing_cycles": [
            {
                "frequency": {
                    "interval_unit": frequency,
                    "interval_count": 1
                },
                "tenure_type": "REGULAR",
                "sequence": 1,
                "total_cycles": 0,
                "pricing_scheme": {
                    "fixed_price": {
                        "value": str(amount),
                        "currency_code": currency
                    }
                }
            }
        ],
        "payment_preferences": {
            "auto_bill_outstanding": True,
            "setup_fee_failure_action": "CONTINUE",
            "payment_failure_threshold": 3
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/v1/billing/plans",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        plan = response.json()
        return {
            "plan_id": plan.get("id"),
            "product_id": product_id,
            "name": plan.get("name"),
            "status": plan.get("status", "created")
        }
    else:
        return {
            "error": f"{response.status_code}: {response.text}",
            "name": plan_name
        }


def create_subscription(access_token, plan_id, plan_name):
    """Create a subscription for a billing plan."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    
    payload = {
        "plan_id": plan_id,
        "subscriber": {
            "name": {
                "given_name": "Test",
                "surname": f"User{random.randint(1, 999)}"
            },
            "email_address": f"testbuyer{random.randint(1, 999)}@example.com"
        },
        "application_context": {
            "brand_name": "Test Store",
            "locale": "en-US",
            "shipping_preference": "NO_SHIPPING",
            "user_action": "SUBSCRIBE_NOW",
            "return_url": "https://example.com/return",
            "cancel_url": "https://example.com/cancel"
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/v1/billing/subscriptions",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        subscription = response.json()
        return {
            "subscription_id": subscription.get("id"),
            "plan_id": plan_id,
            "plan_name": plan_name,
            "status": subscription.get("status", "APPROVAL_PENDING")
        }
    else:
        return {
            "error": f"{response.status_code}: {response.text}",
            "plan_name": plan_name
        }


def create_transaction(access_token, amount, description):
    """Create a transaction (order with immediate capture)."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "PayPal-Request-Id": str(uuid.uuid4()),
    }
    
    payload = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "amount": {
                    "currency_code": "USD",
                    "value": str(amount)
                },
                "description": description
            }
        ],
        "payment_source": {
            "card": {
                "number": "4111111111111111",
                "expiry": "2030-12",
                "name": "Test Buyer",
                "billing_address": {
                    "address_line_1": "123 Main St",
                    "admin_area_2": "San Jose",
                    "admin_area_1": "CA",
                    "postal_code": "95131",
                    "country_code": "US"
                }
            }
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/v2/checkout/orders",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        order = response.json()
        
        capture_id = None
        if order.get("purchase_units"):
            payments = order["purchase_units"][0].get("payments", {})
            captures = payments.get("captures", [])
            if captures:
                capture_id = captures[0].get("id")
        
        return {
            "order_id": order.get("id"),
            "amount": str(amount),
            "description": description,
            "status": order.get("status"),
            "capture_id": capture_id
        }
    else:
        return {
            "error": f"{response.status_code}: {response.text}",
            "description": description
        }


def create_payment_experience(access_token, profile_name):
    """Create a web payment profile (payment experience)."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "name": profile_name,
        "presentation": {
            "brand_name": f"Test Brand {random.randint(1, 999)}",
            "logo_image": "https://example.com/logo.png",
            "locale_code": "US"
        },
        "input_fields": {
            "allow_note": True,
            "no_shipping": 0,
            "address_override": 1
        },
        "flow_config": {
            "landing_page_type": "LOGIN",
            "bank_txn_pending_url": "https://example.com/pending",
            "user_action": "commit"
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/v1/payment-experience/web-profiles",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        profile = response.json()
        return {
            "profile_id": profile.get("id"),
            "name": profile.get("name"),
            "status": "created"
        }
    else:
        return {
            "error": f"{response.status_code}: {response.text}",
            "name": profile_name
        }


def add_tracking(access_token, transaction_id):
    """Add tracking information to a transaction."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    
    tracking_number = f"1Z{random.randint(100000, 999999)}{random.randint(10000000, 99999999)}"
    
    payload = {
        "trackers": [
            {
                "transaction_id": transaction_id,
                "tracking_number": tracking_number,
                "status": "SHIPPED",
                "carrier": "FEDEX"
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/v1/shipping/trackers-batch",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        result = response.json()
        trackers = result.get("tracker_identifiers", [])
        if trackers:
            return {
                "transaction_id": transaction_id,
                "tracking_number": tracking_number,
                "status": "created"
            }
        else:
            return {
                "error": "No tracker returned",
                "transaction_id": transaction_id
            }
    else:
        return {
            "error": f"{response.status_code}: {response.text}",
            "transaction_id": transaction_id
        }


def refund_capture(access_token, capture_id, amount):
    """Refund a payment capture."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "PayPal-Request-Id": str(uuid.uuid4()),
    }
    
    # Refund partial amount (50%)
    refund_amount = float(amount) * 0.5
    
    payload = {
        "amount": {
            "value": f"{refund_amount:.2f}",
            "currency_code": "USD"
        },
        "note_to_payer": "Test refund"
    }
    
    response = requests.post(
        f"{BASE_URL}/v2/payments/captures/{capture_id}/refund",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        refund = response.json()
        return {
            "refund_id": refund.get("id"),
            "capture_id": capture_id,
            "amount": refund_amount,
            "status": refund.get("status")
        }
    else:
        return {
            "error": f"{response.status_code}: {response.text}",
            "capture_id": capture_id
        }


def create_authorization(access_token, amount):
    """Create a payment authorization (without capture)."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "PayPal-Request-Id": str(uuid.uuid4()),
    }
    
    payload = {
        "intent": "AUTHORIZE",  # Authorization only, no capture
        "purchase_units": [
            {
                "amount": {
                    "currency_code": "USD",
                    "value": str(amount)
                }
            }
        ],
        "payment_source": {
            "card": {
                "number": "4111111111111111",
                "expiry": "2030-12",
                "name": "Test Buyer",
                "billing_address": {
                    "address_line_1": "123 Main St",
                    "admin_area_2": "San Jose",
                    "admin_area_1": "CA",
                    "postal_code": "95131",
                    "country_code": "US"
                }
            }
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/v2/checkout/orders",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        order = response.json()
        
        auth_id = None
        if order.get("purchase_units"):
            payments = order["purchase_units"][0].get("payments", {})
            authorizations = payments.get("authorizations", [])
            if authorizations:
                auth_id = authorizations[0].get("id")
        
        return {
            "order_id": order.get("id"),
            "authorization_id": auth_id,
            "amount": str(amount),
            "status": order.get("status")
        }
    else:
        return {
            "error": f"{response.status_code}: {response.text}",
            "amount": str(amount)
        }


def main():
    print("="*70)
    print("PayPal Sandbox Test Data Generator - ALL 14 TABLES")
    print("="*70)
    print("\nThis script will create test data for all connector tables:")
    print("  1. ✅ Products")
    print("  2. ✅ Plans")
    print("  3. ✅ Subscriptions")
    print("  4. ✅ Transactions")
    print("  5. ✅ Payment Captures")
    print("  6. ✅ Payment Experiences (Web Profiles)")
    print("  7. ✅ Tracking")
    print("  8. ✅ Refunds")
    print("  9. ✅ Payment Authorizations")
    print(" 10. ⚠️  Payouts (may require additional setup)")
    print(" 11. ℹ️  Disputes (auto-generated, cannot create)")
    print(" 12. ℹ️  Webhooks Events (auto-generated)")
    print(" 13. ℹ️  Invoices (requires special permissions)")
    print(" 14. ℹ️  Orders (created with transactions)")
    print()
    
    # Get credentials
    client_id = input("Enter your PayPal Sandbox Client ID: ").strip()
    client_secret = input("Enter your PayPal Sandbox Client Secret: ").strip()
    
    if not client_id or not client_secret:
        print("❌ Error: Client ID and Secret are required")
        return
    
    print("\nAuthenticating...")
    try:
        access_token = get_access_token(client_id, client_secret)
        print("✅ Authentication successful\n")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return
    
    results = {
        "products": [],
        "plans": [],
        "subscriptions": [],
        "transactions": [],
        "payment_experiences": [],
        "tracking": [],
        "refunds": [],
        "payment_authorizations": [],
        "notes": {
            "disputes": "Auto-generated by PayPal - cannot be created programmatically",
            "webhooks_events": "Auto-generated by PayPal based on API actions",
            "invoices": "Requires special permissions not available in basic Sandbox",
            "orders": "Created automatically with transactions",
            "payouts": "Requires payout recipient setup - may not work in basic Sandbox"
        }
    }
    
    # Get user input for quantities
    print("How many of each item would you like to create?")
    try:
        num_products = int(input("  Products (1-10): ").strip() or "3")
        num_plans_per_product = int(input("  Plans per product (1-5): ").strip() or "2")
        num_subscriptions = int(input("  Subscriptions (0-10): ").strip() or "3")
        num_transactions = int(input("  Transactions (1-20): ").strip() or "10")
        num_payment_experiences = int(input("  Payment Experiences (1-5): ").strip() or "3")
        num_authorizations = int(input("  Payment Authorizations (0-10): ").strip() or "3")
        num_refunds = int(input("  Refunds (0-10): ").strip() or "2")
    except ValueError:
        print("❌ Invalid input. Using defaults")
        num_products = 3
        num_plans_per_product = 2
        num_subscriptions = 3
        num_transactions = 10
        num_payment_experiences = 3
        num_authorizations = 3
        num_refunds = 2
    
    # Validate inputs
    num_products = max(1, min(num_products, 10))
    num_plans_per_product = max(1, min(num_plans_per_product, 5))
    num_subscriptions = max(0, min(num_subscriptions, 10))
    num_transactions = max(1, min(num_transactions, 20))
    num_payment_experiences = max(0, min(num_payment_experiences, 5))
    num_authorizations = max(0, min(num_authorizations, 10))
    num_refunds = max(0, min(num_refunds, 10))
    
    print("\n" + "="*70)
    print("STEP 1: Creating Products")
    print("="*70)
    
    product_types = [
        ("Premium Membership", "Access to premium features", "SERVICE"),
        ("Software License", "Annual software license", "DIGITAL"),
        ("Consulting Package", "Professional consulting services", "SERVICE"),
        ("Training Course", "Online training course access", "DIGITAL"),
        ("Support Plan", "Technical support subscription", "SERVICE"),
        ("Cloud Storage", "Cloud storage subscription", "SERVICE"),
        ("API Access", "API access tier", "SERVICE"),
        ("Analytics Dashboard", "Business analytics platform", "SERVICE"),
        ("Email Marketing", "Email marketing service", "SERVICE"),
        ("Website Hosting", "Managed website hosting", "SERVICE"),
    ]
    
    for i in range(num_products):
        name, desc, ptype = product_types[i % len(product_types)]
        name = f"{name} - Tier {i+1}"
        
        print(f"\nCreating product {i+1}/{num_products}: {name}")
        result = create_product(access_token, name, desc, ptype)
        results["products"].append(result)
        
        if "error" in result:
            print(f"  ❌ Error: {result['error']}")
        else:
            print(f"  ✅ Created: {result['product_id']}")
    
    print("\n" + "="*70)
    print("STEP 2: Creating Billing Plans")
    print("="*70)
    
    plan_configs = [
        ("Monthly", "Monthly subscription plan", 9.99, "MONTH"),
        ("Quarterly", "Quarterly subscription plan", 24.99, "MONTH"),
        ("Annual", "Annual subscription plan", 99.99, "YEAR"),
        ("Weekly", "Weekly subscription plan", 2.99, "WEEK"),
        ("Bi-Annual", "Bi-annual subscription plan", 179.99, "YEAR"),
    ]
    
    for product in results["products"]:
        if "error" in product:
            continue
            
        product_id = product["product_id"]
        product_name = product["name"]
        
        for i in range(num_plans_per_product):
            plan_name, plan_desc, amount, frequency = plan_configs[i % len(plan_configs)]
            plan_name = f"{product_name} - {plan_name}"
            
            print(f"\nCreating plan: {plan_name}")
            result = create_billing_plan(
                access_token,
                product_id,
                plan_name,
                plan_desc,
                amount,
                "USD",
                frequency
            )
            results["plans"].append(result)
            
            if "error" in result:
                print(f"  ❌ Error: {result['error']}")
            else:
                print(f"  ✅ Created: {result['plan_id']}")
    
    print("\n" + "="*70)
    print("STEP 3: Creating Subscriptions")
    print("="*70)
    
    active_plans = [p for p in results["plans"] if "error" not in p]
    
    if not active_plans:
        print("⚠️  No plans available")
    elif num_subscriptions == 0:
        print("⏭️  Skipping (user requested 0)")
    else:
        for i in range(min(num_subscriptions, len(active_plans))):
            plan = active_plans[i]
            
            print(f"\nCreating subscription {i+1}/{num_subscriptions}")
            result = create_subscription(access_token, plan["plan_id"], plan["name"])
            results["subscriptions"].append(result)
            
            if "error" in result:
                print(f"  ❌ Error: {result['error']}")
            else:
                print(f"  ✅ Created: {result['subscription_id']}")
    
    print("\n" + "="*70)
    print("STEP 4: Creating Transactions & Payment Captures")
    print("="*70)
    
    descriptions = [
        "Test Transaction",
        "Sample Product Purchase",
        "Monthly Subscription Payment",
        "Annual License Fee",
        "Consulting Service",
        "Training Course Fee",
        "Support Plan Payment",
        "Digital Download",
        "Service Upgrade",
        "Custom Service Fee"
    ]
    
    amounts = [10.00, 25.50, 50.00, 99.99, 150.00, 200.00]
    
    for i in range(num_transactions):
        amount = random.choice(amounts)
        description = random.choice(descriptions)
        
        print(f"\nCreating transaction {i+1}/{num_transactions}: ${amount}")
        result = create_transaction(access_token, amount, description)
        results["transactions"].append(result)
        
        if "error" in result:
            print(f"  ❌ Error: {result['error']}")
        else:
            print(f"  ✅ Created: {result['order_id']}")
            # Small delay to avoid rate limiting
            time.sleep(1)
    
    print("\n" + "="*70)
    print("STEP 5: Creating Payment Experiences (Web Profiles)")
    print("="*70)
    
    if num_payment_experiences == 0:
        print("⏭️  Skipping (user requested 0)")
    else:
        for i in range(num_payment_experiences):
            profile_name = f"Test Web Profile {i+1}"
            
            print(f"\nCreating payment experience {i+1}/{num_payment_experiences}")
            result = create_payment_experience(access_token, profile_name)
            results["payment_experiences"].append(result)
            
            if "error" in result:
                print(f"  ❌ Error: {result['error']}")
            else:
                print(f"  ✅ Created: {result['profile_id']}")
    
    print("\n" + "="*70)
    print("STEP 6: Adding Tracking Information")
    print("="*70)
    
    completed_transactions = [t for t in results["transactions"] if "error" not in t and t.get("order_id")]
    
    if not completed_transactions:
        print("⚠️  No transactions available for tracking")
    else:
        num_tracking = min(len(completed_transactions), 5)
        print(f"Adding tracking to {num_tracking} transactions...")
        
        for i in range(num_tracking):
            transaction = completed_transactions[i]
            order_id = transaction["order_id"]
            
            print(f"\nAdding tracking {i+1}/{num_tracking}")
            result = add_tracking(access_token, order_id)
            results["tracking"].append(result)
            
            if "error" in result:
                print(f"  ⚠️  {result['error']}")
            else:
                print(f"  ✅ Created: {result['tracking_number']}")
    
    print("\n" + "="*70)
    print("STEP 7: Creating Refunds")
    print("="*70)
    
    captures_with_id = [t for t in results["transactions"] if "error" not in t and t.get("capture_id")]
    
    if not captures_with_id:
        print("⚠️  No captures available for refunds")
    elif num_refunds == 0:
        print("⏭️  Skipping (user requested 0)")
    else:
        for i in range(min(num_refunds, len(captures_with_id))):
            transaction = captures_with_id[i]
            capture_id = transaction["capture_id"]
            amount = transaction["amount"]
            
            print(f"\nCreating refund {i+1}/{num_refunds}")
            result = refund_capture(access_token, capture_id, amount)
            results["refunds"].append(result)
            
            if "error" in result:
                print(f"  ❌ Error: {result['error']}")
            else:
                print(f"  ✅ Created: {result['refund_id']}")
            time.sleep(1)
    
    print("\n" + "="*70)
    print("STEP 8: Creating Payment Authorizations")
    print("="*70)
    
    if num_authorizations == 0:
        print("⏭️  Skipping (user requested 0)")
    else:
        for i in range(num_authorizations):
            amount = random.choice([25.00, 50.00, 100.00])
            
            print(f"\nCreating authorization {i+1}/{num_authorizations}: ${amount}")
            result = create_authorization(access_token, amount)
            results["payment_authorizations"].append(result)
            
            if "error" in result:
                print(f"  ❌ Error: {result['error']}")
            else:
                print(f"  ✅ Created: {result.get('authorization_id', 'N/A')}")
            time.sleep(1)
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY - ALL 14 TABLES")
    print("="*70)
    
    products_created = len([p for p in results["products"] if "error" not in p])
    plans_created = len([p for p in results["plans"] if "error" not in p])
    subscriptions_created = len([s for s in results["subscriptions"] if "error" not in s])
    transactions_created = len([t for t in results["transactions"] if "error" not in t])
    experiences_created = len([e for e in results["payment_experiences"] if "error" not in e])
    tracking_created = len([t for t in results["tracking"] if "error" not in t])
    refunds_created = len([r for r in results["refunds"] if "error" not in r])
    auths_created = len([a for a in results["payment_authorizations"] if "error" not in a])
    
    print(f"\n✅ 1. products: {products_created} records")
    print(f"✅ 2. plans: {plans_created} records")
    print(f"✅ 3. subscriptions: {subscriptions_created} records")
    print(f"✅ 4. transactions: {transactions_created} records")
    print(f"✅ 5. payment_captures: {transactions_created} records")
    print(f"✅ 6. payment_experiences: {experiences_created} records")
    print(f"✅ 7. tracking: {tracking_created} records")
    print(f"✅ 8. refunds: {refunds_created} records")
    print(f"✅ 9. payment_authorizations: {auths_created} records")
    print(f"ℹ️  10. payouts: {0} records (requires additional setup)")
    print(f"ℹ️  11. disputes: Auto-generated (cannot create)")
    print(f"ℹ️  12. webhooks_events: Auto-generated")
    print(f"ℹ️  13. invoices: Requires special permissions")
    print(f"ℹ️  14. orders: {transactions_created} records (from transactions)")
    
    # Save results
    output_file = "sandbox_data_generation_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")
    
    # Show IDs for connector configuration
    if subscriptions_created > 0:
        subscription_ids = [s["subscription_id"] for s in results["subscriptions"] if "error" not in s]
        print("\n" + "="*70)
        print("SUBSCRIPTION IDS FOR CONNECTOR CONFIG")
        print("="*70)
        print(f'\n"subscription_ids": {json.dumps(subscription_ids[:5])}')
    
    print("\n" + "="*70)
    print("NEXT STEPS")
    print("="*70)
    print("\n1. Update your ingest.py with today's date range:")
    print(f'   "start_date": "{datetime.utcnow().strftime("%Y-%m-%dT00:00:00Z")}"')
    print(f'   "end_date": "{datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z")}"')
    print("\n2. Run your connector to ingest all available tables")
    print("\n3. Tables ready for ingestion:")
    print(f"   - 9 tables have data: products, plans, subscriptions, transactions,")
    print(f"     payment_captures, payment_experiences, tracking, refunds,")
    print(f"     payment_authorizations")
    print(f"   - 5 tables are informational: disputes, webhooks_events, invoices,")
    print(f"     orders, payouts")
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
