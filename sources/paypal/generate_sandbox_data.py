#!/usr/bin/env python3
"""
PayPal Sandbox Data Generator

This script creates test transactions and subscriptions in your PayPal Sandbox environment.
It uses the PayPal Orders API to create transactions and the Subscriptions API to create subscriptions.

Requirements:
    - PayPal Sandbox credentials (Client ID and Client Secret)
    - requests library: pip install requests

Usage:
    python generate_sandbox_data.py

Then follow the prompts to enter your credentials and configure the data generation.
"""

import requests
import base64
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any


class PayPalSandboxDataGenerator:
    """Generate test data in PayPal Sandbox environment."""
    
    def __init__(self, client_id: str, client_secret: str):
        """
        Initialize the PayPal Sandbox data generator.
        
        Args:
            client_id: PayPal Sandbox Client ID
            client_secret: PayPal Sandbox Client Secret
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api-m.sandbox.paypal.com"
        self._access_token = None
        self._token_expires_at = None
        
    def _get_access_token(self) -> str:
        """Get OAuth 2.0 access token."""
        # Check if cached token is still valid
        if self._access_token and self._token_expires_at:
            if datetime.now() < self._token_expires_at:
                return self._access_token
        
        # Request new token
        token_url = f"{self.base_url}/v1/oauth2/token"
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        
        data = {"grant_type": "client_credentials"}
        
        response = requests.post(token_url, headers=headers, data=data, timeout=30)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to get access token: {response.status_code} {response.text}"
            )
        
        token_data = response.json()
        self._access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 32400)
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        
        return self._access_token
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        include_request_id: bool = False
    ) -> requests.Response:
        """Make an authenticated API request."""
        access_token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        
        # Add PayPal-Request-Id for idempotency when creating orders with payment_source
        if include_request_id:
            headers["PayPal-Request-Id"] = str(uuid.uuid4())
        
        url = f"{self.base_url}{endpoint}"
        
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data,
            params=params,
            timeout=30
        )
        
        return response
    
    def create_order(
        self,
        amount: str,
        currency: str = "USD",
        description: str = "Test Order"
    ) -> Dict[str, Any]:
        """
        Create a PayPal order with payment source for immediate capture.
        
        Args:
            amount: Order amount (e.g., "100.00")
            currency: Currency code (default: "USD")
            description: Order description
            
        Returns:
            Order details including order ID
        """
        # Include payment_source to enable immediate capture without approval
        order_data = {
            "intent": "CAPTURE",
            "purchase_units": [
                {
                    "description": description,
                    "amount": {
                        "currency_code": currency,
                        "value": amount
                    }
                }
            ],
            "payment_source": {
                "card": {
                    "number": "4111111111111111",  # PayPal Sandbox test Visa card
                    "expiry": "2027-12",
                    "security_code": "123",
                    "name": "Test Buyer",
                    "billing_address": {
                        "address_line_1": "123 Test Street",
                        "admin_area_2": "San Jose",
                        "admin_area_1": "CA",
                        "postal_code": "95131",
                        "country_code": "US"
                    }
                }
            }
        }
        
        response = self._make_request("POST", "/v2/checkout/orders", data=order_data, include_request_id=True)
        
        if response.status_code not in [200, 201]:
            raise RuntimeError(
                f"Failed to create order: {response.status_code} {response.text}"
            )
        
        return response.json()
    
    def capture_order(self, order_id: str) -> Dict[str, Any]:
        """
        Capture (complete) a PayPal order to create a transaction.
        
        Args:
            order_id: The order ID to capture
            
        Returns:
            Capture details
        """
        response = self._make_request(
            "POST",
            f"/v2/checkout/orders/{order_id}/capture"
        )
        
        if response.status_code not in [200, 201]:
            raise RuntimeError(
                f"Failed to capture order: {response.status_code} {response.text}"
            )
        
        return response.json()
    
    def create_product(self, name: str, description: str) -> Dict[str, Any]:
        """
        Create a product (required for subscription plans).
        
        Args:
            name: Product name
            description: Product description
            
        Returns:
            Product details including product ID
        """
        product_data = {
            "name": name,
            "description": description,
            "type": "SERVICE",
            "category": "SOFTWARE"
        }
        
        response = self._make_request(
            "POST",
            "/v1/catalogs/products",
            data=product_data
        )
        
        if response.status_code not in [200, 201]:
            raise RuntimeError(
                f"Failed to create product: {response.status_code} {response.text}"
            )
        
        return response.json()
    
    def create_billing_plan(
        self,
        product_id: str,
        plan_name: str,
        amount: str,
        currency: str = "USD",
        interval_unit: str = "MONTH"
    ) -> Dict[str, Any]:
        """
        Create a subscription billing plan.
        
        Args:
            product_id: Product ID for this plan
            plan_name: Plan name
            amount: Billing amount per cycle
            currency: Currency code
            interval_unit: MONTH, WEEK, or YEAR
            
        Returns:
            Plan details including plan ID
        """
        plan_data = {
            "product_id": product_id,
            "name": plan_name,
            "description": f"Test {plan_name}",
            "status": "ACTIVE",
            "billing_cycles": [
                {
                    "frequency": {
                        "interval_unit": interval_unit,
                        "interval_count": 1
                    },
                    "tenure_type": "REGULAR",
                    "sequence": 1,
                    "total_cycles": 0,  # 0 = infinite
                    "pricing_scheme": {
                        "fixed_price": {
                            "value": amount,
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
        
        response = self._make_request(
            "POST",
            "/v1/billing/plans",
            data=plan_data
        )
        
        if response.status_code not in [200, 201]:
            raise RuntimeError(
                f"Failed to create billing plan: {response.status_code} {response.text}"
            )
        
        return response.json()
    
    def create_subscription(
        self,
        plan_id: str,
        subscriber_email: str = "buyer@example.com"
    ) -> Dict[str, Any]:
        """
        Create a subscription.
        
        Args:
            plan_id: Billing plan ID
            subscriber_email: Subscriber email address
            
        Returns:
            Subscription details including subscription ID
        """
        subscription_data = {
            "plan_id": plan_id,
            "subscriber": {
                "email_address": subscriber_email,
                "name": {
                    "given_name": "Test",
                    "surname": "Subscriber"
                }
            },
            "application_context": {
                "brand_name": "Test Company",
                "shipping_preference": "NO_SHIPPING",
                "user_action": "SUBSCRIBE_NOW"
            }
        }
        
        response = self._make_request(
            "POST",
            "/v1/billing/subscriptions",
            data=subscription_data
        )
        
        if response.status_code not in [200, 201]:
            raise RuntimeError(
                f"Failed to create subscription: {response.status_code} {response.text}"
            )
        
        return response.json()
    
    def generate_transactions(self, count: int = 10) -> list:
        """
        Generate test transactions by creating and capturing orders.
        
        Args:
            count: Number of transactions to generate
            
        Returns:
            List of transaction details
        """
        print(f"\n🔄 Generating {count} test transactions...")
        transactions = []
        
        # Sample amounts and descriptions
        amounts = ["10.00", "25.50", "50.00", "99.99", "150.00", "200.00"]
        descriptions = [
            "Test Product Purchase",
            "Sample Service Fee",
            "Digital Download",
            "Monthly Subscription",
            "One-time Payment",
            "Test Transaction"
        ]
        
        for i in range(count):
            try:
                amount = random.choice(amounts)
                description = random.choice(descriptions)
                
                # Create and auto-capture order (payment_source triggers automatic capture)
                print(f"  [{i+1}/{count}] Creating and capturing order for ${amount}...")
                order = self.create_order(amount=amount, description=description)
                order_id = order.get("id")
                order_status = order.get("status")
                
                # Extract capture info from the created order (already captured)
                purchase_units = order.get("purchase_units", [{}])
                payments = purchase_units[0].get("payments", {}) if purchase_units else {}
                captures = payments.get("captures", [])
                capture_id = captures[0].get("id") if captures else None
                
                transactions.append({
                    "order_id": order_id,
                    "amount": amount,
                    "description": description,
                    "status": order_status,
                    "capture_id": capture_id
                })
                
                print(f"  ✅ Transaction {i+1} created successfully (Order ID: {order_id})")
                
                # Small delay between transactions to avoid rate limiting
                time.sleep(1)
                
            except Exception as e:
                print(f"  ❌ Error creating transaction {i+1}: {e}")
                continue
        
        print(f"\n✅ Generated {len(transactions)} transactions successfully!")
        return transactions
    
    def generate_subscriptions(self, count: int = 5) -> list:
        """
        Generate test subscriptions.
        
        Args:
            count: Number of subscriptions to generate
            
        Returns:
            List of subscription details
        """
        print(f"\n🔄 Generating {count} test subscriptions...")
        subscriptions = []
        
        try:
            # Create a product first (required for billing plans)
            print("  Creating test product...")
            product = self.create_product(
                name="Test Subscription Product",
                description="Product for testing subscriptions"
            )
            product_id = product.get("id")
            print(f"  ✅ Product created: {product_id}")
            
            time.sleep(1)
            
            # Create billing plans
            plan_configs = [
                ("Basic Plan", "9.99", "MONTH"),
                ("Pro Plan", "29.99", "MONTH"),
                ("Annual Plan", "99.99", "YEAR"),
            ]
            
            plans = []
            for plan_name, amount, interval in plan_configs:
                try:
                    print(f"  Creating {plan_name} (${amount}/{interval})...")
                    plan = self.create_billing_plan(
                        product_id=product_id,
                        plan_name=plan_name,
                        amount=amount,
                        interval_unit=interval
                    )
                    plans.append(plan)
                    print(f"  ✅ Plan created: {plan.get('id')}")
                    time.sleep(1)
                except Exception as e:
                    print(f"  ⚠️  Failed to create {plan_name}: {e}")
            
            if not plans:
                print("  ❌ No billing plans created. Cannot create subscriptions.")
                return []
            
            # Create subscriptions using the plans
            for i in range(min(count, len(plans) * 3)):  # Multiple subs per plan
                try:
                    plan = plans[i % len(plans)]
                    plan_id = plan.get("id")
                    
                    print(f"  [{i+1}/{count}] Creating subscription for plan {plan.get('name')}...")
                    subscription = self.create_subscription(
                        plan_id=plan_id,
                        subscriber_email=f"testbuyer{i+1}@example.com"
                    )
                    
                    subscriptions.append({
                        "subscription_id": subscription.get("id"),
                        "plan_id": plan_id,
                        "plan_name": plan.get("name"),
                        "status": subscription.get("status")
                    })
                    
                    print(f"  ✅ Subscription {i+1} created successfully")
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"  ❌ Error creating subscription {i+1}: {e}")
                    continue
            
        except Exception as e:
            print(f"  ❌ Error in subscription generation: {e}")
        
        print(f"\n✅ Generated {len(subscriptions)} subscriptions successfully!")
        return subscriptions


def main():
    """Main function to run the data generator."""
    print("=" * 70)
    print("PayPal Sandbox Data Generator")
    print("=" * 70)
    print("\nThis script will create test transactions and subscriptions")
    print("in your PayPal Sandbox environment.\n")
    
    # Get credentials from user
    print("📋 Please provide your PayPal Sandbox credentials:")
    print("   (Get these from: https://developer.paypal.com/dashboard/)\n")
    
    client_id = input("Enter Sandbox Client ID: ").strip()
    client_secret = input("Enter Sandbox Client Secret: ").strip()
    
    if not client_id or not client_secret:
        print("\n❌ Error: Both Client ID and Client Secret are required.")
        return
    
    print("\n📊 Configure data generation:")
    
    try:
        num_transactions = int(input("Number of transactions to create (default 10): ").strip() or "10")
        num_subscriptions = int(input("Number of subscriptions to create (default 5): ").strip() or "5")
    except ValueError:
        print("❌ Invalid number. Using defaults.")
        num_transactions = 10
        num_subscriptions = 5
    
    print("\n" + "=" * 70)
    print(f"Configuration:")
    print(f"  - Transactions: {num_transactions}")
    print(f"  - Subscriptions: {num_subscriptions}")
    print("=" * 70)
    
    confirm = input("\nProceed with data generation? (yes/no): ").strip().lower()
    if confirm not in ['yes', 'y']:
        print("❌ Cancelled by user.")
        return
    
    # Initialize generator
    try:
        generator = PayPalSandboxDataGenerator(client_id, client_secret)
        print("\n✅ Successfully authenticated with PayPal Sandbox")
    except Exception as e:
        print(f"\n❌ Authentication failed: {e}")
        return
    
    results = {
        "transactions": [],
        "subscriptions": []
    }
    
    # Generate transactions
    if num_transactions > 0:
        try:
            results["transactions"] = generator.generate_transactions(num_transactions)
        except Exception as e:
            print(f"\n❌ Transaction generation failed: {e}")
    
    # Generate subscriptions
    if num_subscriptions > 0:
        try:
            results["subscriptions"] = generator.generate_subscriptions(num_subscriptions)
        except Exception as e:
            print(f"\n❌ Subscription generation failed: {e}")
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 GENERATION SUMMARY")
    print("=" * 70)
    print(f"✅ Transactions created: {len(results['transactions'])}")
    print(f"✅ Subscriptions created: {len(results['subscriptions'])}")
    print("\n📝 Note: Transactions may take a few minutes to appear in")
    print("   the Transaction Search API results.")
    print("\n🔍 You can now test your connector with:")
    print("   - Date range: Last 7 days to now")
    print("   - Tables: transactions, subscriptions")
    print("=" * 70)
    
    # Save results to file
    output_file = "sandbox_data_generation_results.json"
    try:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n💾 Results saved to: {output_file}")
    except Exception as e:
        print(f"\n⚠️  Could not save results file: {e}")


if __name__ == "__main__":
    main()

