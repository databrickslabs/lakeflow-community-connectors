#!/usr/bin/env python3
"""
Generate test data in PayPal Sandbox for all connector tables

This script creates test data for:
1. Products (catalog products)
2. Plans (billing plans linked to products)
3. Subscriptions (subscriptions linked to plans)
4. Transactions (payment captures)
5. Payment Captures (derived from transactions)
"""

import requests
import base64
import json
import uuid
import random
from datetime import datetime

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
    """
    Create a product in the catalog.
    
    Products are the foundation for billing plans and subscriptions.
    """
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
    """
    Create a billing plan for a product.
    
    Plans define pricing and billing cycles for subscriptions.
    """
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
                "total_cycles": 0,  # 0 = infinite
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
    """
    Create a subscription for a billing plan.
    
    Note: Subscriptions will be in APPROVAL_PENDING state until a buyer approves them.
    """
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
    """
    Create a transaction (order with immediate capture).
    
    This creates both a transaction and a payment capture record.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "PayPal-Request-Id": str(uuid.uuid4()),
    }
    
    # Test card that works in sandbox
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
        
        # Extract capture info if available
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


def main():
    print("="*70)
    print("PayPal Sandbox Test Data Generator")
    print("="*70)
    print("\nThis script will create test data for all connector tables:")
    print("  1. Products")
    print("  2. Plans")
    print("  3. Subscriptions")
    print("  4. Transactions")
    print("  5. Payment Captures (derived from transactions)")
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
        "transactions": []
    }
    
    # Get user input for quantities
    print("How many of each item would you like to create?")
    try:
        num_products = int(input("  Products (1-10): ").strip() or "3")
        num_plans_per_product = int(input("  Plans per product (1-5): ").strip() or "2")
        num_subscriptions = int(input("  Subscriptions (0-10): ").strip() or "3")
        num_transactions = int(input("  Transactions (1-20): ").strip() or "10")
    except ValueError:
        print("❌ Invalid input. Using defaults: 3 products, 2 plans each, 3 subscriptions, 10 transactions")
        num_products = 3
        num_plans_per_product = 2
        num_subscriptions = 3
        num_transactions = 10
    
    # Validate inputs
    num_products = max(1, min(num_products, 10))
    num_plans_per_product = max(1, min(num_plans_per_product, 5))
    num_subscriptions = max(0, min(num_subscriptions, 10))
    num_transactions = max(1, min(num_transactions, 20))
    
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
            
            print(f"\nCreating plan for {product_name}: {plan_name}")
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
                print(f"  ✅ Created: {result['plan_id']} (${amount}/{frequency})")
    
    print("\n" + "="*70)
    print("STEP 3: Creating Subscriptions")
    print("="*70)
    
    # Get active plans
    active_plans = [p for p in results["plans"] if "error" not in p]
    
    if not active_plans:
        print("⚠️  No plans available to create subscriptions")
    elif num_subscriptions == 0:
        print("⏭️  Skipping subscription creation (user requested 0)")
    else:
        for i in range(min(num_subscriptions, len(active_plans))):
            plan = active_plans[i]
            plan_id = plan["plan_id"]
            plan_name = plan["name"]
            
            print(f"\nCreating subscription {i+1}/{num_subscriptions} for: {plan_name}")
            result = create_subscription(access_token, plan_id, plan_name)
            results["subscriptions"].append(result)
            
            if "error" in result:
                print(f"  ❌ Error: {result['error']}")
            else:
                print(f"  ✅ Created: {result['subscription_id']} (Status: {result['status']})")
    
    print("\n" + "="*70)
    print("STEP 4: Creating Transactions (Payment Captures)")
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
        
        print(f"\nCreating transaction {i+1}/{num_transactions}: ${amount} - {description}")
        result = create_transaction(access_token, amount, description)
        results["transactions"].append(result)
        
        if "error" in result:
            print(f"  ❌ Error: {result['error']}")
        else:
            print(f"  ✅ Created: {result['order_id']} (Capture: {result.get('capture_id', 'N/A')})")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    products_created = len([p for p in results["products"] if "error" not in p])
    plans_created = len([p for p in results["plans"] if "error" not in p])
    subscriptions_created = len([s for s in results["subscriptions"] if "error" not in s])
    transactions_created = len([t for t in results["transactions"] if "error" not in t])
    
    print(f"\n✅ Products created: {products_created}/{num_products}")
    print(f"✅ Plans created: {plans_created}/{num_products * num_plans_per_product}")
    print(f"✅ Subscriptions created: {subscriptions_created}/{num_subscriptions}")
    print(f"✅ Transactions created: {transactions_created}/{num_transactions}")
    
    print("\n" + "="*70)
    print("CONNECTOR TABLE DATA AVAILABILITY")
    print("="*70)
    print(f"\n1. products table: {products_created} records available")
    print(f"2. plans table: {plans_created} records available")
    print(f"3. subscriptions table: {subscriptions_created} records available")
    print(f"4. transactions table: {transactions_created} records available")
    print(f"5. payment_captures table: {transactions_created} records available (from transactions)")
    
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
        print("\nTo use the subscriptions table, add these IDs to your config:")
        print(f'\n"subscription_ids": {json.dumps(subscription_ids[:5])}')
    
    print("\n" + "="*70)
    print("NEXT STEPS")
    print("="*70)
    print("\n1. Update your table config with date ranges:")
    print(f'   "start_date": "{datetime.utcnow().strftime("%Y-%m-%dT00:00:00Z")}"')
    print(f'   "end_date": "{datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z")}"')
    print("\n2. Run your connector to ingest the data")
    print("\n3. Data should be available in all 5 tables!")
    print("\n" + "="*70)


if __name__ == "__main__":
    main()

