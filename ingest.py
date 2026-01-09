
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "paypal"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
#
# pipeline_spec
# ├── connection_name (required): The Unity Catalog connection name
# └── objects[]: List of tables to ingest
#     └── table
#         ├── source_table (required): The table name in the source system
#         ├── destination_catalog (optional): Target catalog (defaults to pipeline's default)
#         ├── destination_schema (optional): Target schema (defaults to pipeline's default)
#         ├── destination_table (optional): Target table name (defaults to source_table)
#         └── table_configuration (optional)
#             ├── scd_type: "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
#             ├── primary_keys: List of columns to override connector's default keys
#             └── (other options): See source connector's README
# =============================================================================

# Please update the spec below to configure your ingestion pipeline.

pipeline_spec = {
    "connection_name": "paypal_v2",
    "objects": [
        # Table 1: Transactions - Payment transaction history
        {
            "table": {
                "source_table": "transactions",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "transactions",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                    "start_date": "2026-01-09T00:00:00Z",
                    "end_date": "2026-01-09T23:59:59Z",
                    "page_size": 100
                },
            }
        },
        # Table 2: Subscriptions - Subscription data (requires subscription IDs)
        {
            "table": {
                "source_table": "subscriptions",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "subscriptions",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "subscription_ids": ["I-SNUCVEVHXJDA", "I-PCFYTVKEERG5", "I-W0Y9D1HNV5VG"]
                },
            }
        },
        # Table 3: Products - Catalog products
        {
            "table": {
                "source_table": "products",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "products",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "page_size": 20
                },
            }
        },
        # Table 4: Plans - Billing plans for subscriptions
        {
            "table": {
                "source_table": "plans",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "plans",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "page_size": 20
                },
            }
        },
        # Table 5: Payment Captures - Payment capture transaction details
        {
            "table": {
                "source_table": "payment_captures",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "payment_captures",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                    "start_date": "2026-01-09T00:00:00Z",
                    "end_date": "2026-01-09T23:59:59Z",
                    "page_size": 100
                },
            }
        },
        # Table 6: Disputes - Customer disputes and chargebacks
        {
            "table": {
                "source_table": "disputes",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "disputes",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "page_size": 10
                },
            }
        },
        # Table 7: Payouts - Payout batch information
        {
            "table": {
                "source_table": "payouts",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "payouts",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "start_date": "2026-01-01",
                    "end_date": "2026-01-09",
                    "page_size": 10
                },
            }
        },
        # Table 8: Refunds - Refund transactions
        {
            "table": {
                "source_table": "refunds",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "refunds",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                    "start_date": "2026-01-09T00:00:00Z",
                    "end_date": "2026-01-09T23:59:59Z"
                },
            }
        },
        # Table 9: Payment Authorizations - Payment authorization records
        {
            "table": {
                "source_table": "payment_authorizations",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "payment_authorizations",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                    "start_date": "2026-01-09T00:00:00Z",
                    "end_date": "2026-01-09T23:59:59Z"
                },
            }
        },
        # Table 10: Webhooks Events - Webhook event history
        {
            "table": {
                "source_table": "webhooks_events",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "webhooks_events",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "page_size": 10
                },
            }
        },
        # Table 11: Invoices - Invoice data (requires special permissions)
        # Note: May return empty in Sandbox without proper permissions
        {
            "table": {
                "source_table": "invoices",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "invoices",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "page_size": 20
                },
            }
        },
        # Table 12: Orders - Order data (limited - no bulk listing)
        # Note: Will return empty - use transactions table instead
        {
            "table": {
                "source_table": "orders",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "orders",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1"
                },
            }
        },
        # Table 13: Payment Experiences - Web payment profiles
        {
            "table": {
                "source_table": "payment_experiences",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "payment_experiences",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1"
                },
            }
        },
        # Table 14: Tracking - Shipment tracking information
        # Note: Requires at least one filter parameter
        {
            "table": {
                "source_table": "tracking",
                "destination_catalog": "alex_owen_the_unity_catalog",
                "destination_schema": "paypal_comunity_connector_a0",
                "destination_table": "tracking",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "page_size": 10
                    # Add transaction_id, tracking_number, or date range as needed
                },
            }
        },
    ],
}


# Dynamically import and register the LakeFlow source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
