
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
                    "subscription_ids": ["I-UGRPYC05ATB2", "I-8C7CWF5AB208", "I-N4D2YUNDR8BW", "I-FLKBV5UEJ5SJ", "I-V8PCU045H25R"]
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
    ],
}


# Dynamically import and register the LakeFlow source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
