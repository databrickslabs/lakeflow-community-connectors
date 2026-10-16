"""Static schema definitions and table metadata for the PayPal connector.

Derived from the PayPal REST API OpenAPI specifications (Transaction Search v1,
Customer Disputes v1, Invoicing v2, Catalog Products v1). PayPal has no runtime
schema discovery API, so every schema is hard-coded here.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# =============================================================================
# Reusable nested structs
# =============================================================================

MONEY_STRUCT = StructType(
    [
        StructField("currency_code", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

LINK_STRUCT = StructType(
    [
        StructField("href", StringType(), True),
        StructField("rel", StringType(), True),
        StructField("method", StringType(), True),
    ]
)

ADDRESS_STRUCT = StructType(
    [
        StructField("line1", StringType(), True),
        StructField("line2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("postal_code", StringType(), True),
    ]
)

NAME_STRUCT = StructType(
    [
        StructField("prefix", StringType(), True),
        StructField("given_name", StringType(), True),
        StructField("surname", StringType(), True),
        StructField("middle_name", StringType(), True),
        StructField("suffix", StringType(), True),
        StructField("alternate_full_name", StringType(), True),
        StructField("full_name", StringType(), True),
    ]
)

PHONE_STRUCT = StructType(
    [
        StructField("country_code", StringType(), True),
        StructField("national_number", StringType(), True),
        StructField("extension_number", StringType(), True),
    ]
)

TAX_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("percent", StringType(), True),
        StructField("amount", MONEY_STRUCT, True),
    ]
)

DISCOUNT_STRUCT = StructType(
    [
        StructField("percent", StringType(), True),
        StructField("amount", MONEY_STRUCT, True),
    ]
)


# =============================================================================
# transactions
# =============================================================================

TRANSACTION_INFO_STRUCT = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("paypal_account_id", StringType(), True),
        StructField("paypal_reference_id", StringType(), True),
        StructField("paypal_reference_id_type", StringType(), True),
        StructField("transaction_event_code", StringType(), True),
        StructField("transaction_initiation_date", TimestampType(), True),
        StructField("transaction_updated_date", TimestampType(), True),
        StructField("transaction_amount", MONEY_STRUCT, True),
        StructField("fee_amount", MONEY_STRUCT, True),
        StructField("discount_amount", MONEY_STRUCT, True),
        StructField("insurance_amount", MONEY_STRUCT, True),
        StructField("sales_tax_amount", MONEY_STRUCT, True),
        StructField("shipping_amount", MONEY_STRUCT, True),
        StructField("shipping_discount_amount", MONEY_STRUCT, True),
        StructField("shipping_tax_amount", MONEY_STRUCT, True),
        StructField("other_amount", MONEY_STRUCT, True),
        StructField("tip_amount", MONEY_STRUCT, True),
        StructField("transaction_status", StringType(), True),
        StructField("transaction_subject", StringType(), True),
        StructField("transaction_note", StringType(), True),
        StructField("payment_tracking_id", StringType(), True),
        StructField("bank_reference_id", StringType(), True),
        StructField("ending_balance", MONEY_STRUCT, True),
        StructField("available_balance", MONEY_STRUCT, True),
        StructField("invoice_id", StringType(), True),
        StructField("custom_field", StringType(), True),
        StructField("protection_eligibility", StringType(), True),
        StructField("credit_term", StringType(), True),
        StructField("instrument_type", StringType(), True),
        StructField("instrument_sub_type", StringType(), True),
    ]
)

PAYER_INFO_STRUCT = StructType(
    [
        StructField("account_id", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("phone_number", PHONE_STRUCT, True),
        StructField("address_status", StringType(), True),
        StructField("payer_status", StringType(), True),
        StructField("payer_name", NAME_STRUCT, True),
        StructField("country_code", StringType(), True),
        StructField("address", ADDRESS_STRUCT, True),
    ]
)

SHIPPING_INFO_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("method", StringType(), True),
        StructField("address", ADDRESS_STRUCT, True),
        StructField("secondary_shipping_address", ADDRESS_STRUCT, True),
    ]
)

CART_ITEM_STRUCT = StructType(
    [
        StructField("item_code", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("item_description", StringType(), True),
        StructField("item_options", StringType(), True),
        StructField("item_quantity", StringType(), True),
        StructField("item_unit_price", MONEY_STRUCT, True),
        StructField("item_amount", MONEY_STRUCT, True),
        StructField("discount_amount", MONEY_STRUCT, True),
        StructField("adjustment_amount", MONEY_STRUCT, True),
        StructField("gift_wrap_amount", MONEY_STRUCT, True),
        StructField("tax_percentage", StringType(), True),
        StructField(
            "tax_amounts",
            ArrayType(StructType([StructField("tax_amount", MONEY_STRUCT, True)])),
            True,
        ),
        StructField("basic_shipping_amount", MONEY_STRUCT, True),
        StructField("extra_shipping_amount", MONEY_STRUCT, True),
        StructField("handling_amount", MONEY_STRUCT, True),
        StructField("insurance_amount", MONEY_STRUCT, True),
        StructField("total_item_amount", MONEY_STRUCT, True),
        StructField("invoice_number", StringType(), True),
        StructField(
            "checkout_options",
            ArrayType(
                StructType(
                    [
                        StructField("checkout_option_name", StringType(), True),
                        StructField("checkout_option_value", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

CART_INFO_STRUCT = StructType(
    [
        StructField("item_details", ArrayType(CART_ITEM_STRUCT), True),
        StructField("tax_inclusive", BooleanType(), True),
        StructField("paypal_invoice_id", StringType(), True),
    ]
)

STORE_INFO_STRUCT = StructType(
    [
        StructField("store_id", StringType(), True),
        StructField("terminal_id", StringType(), True),
    ]
)

AUCTION_INFO_STRUCT = StructType(
    [
        StructField("auction_site", StringType(), True),
        StructField("auction_item_site", StringType(), True),
        StructField("auction_buyer_id", StringType(), True),
        StructField("auction_closing_date", TimestampType(), True),
    ]
)

INCENTIVE_INFO_STRUCT = StructType(
    [
        StructField(
            "incentive_details",
            ArrayType(
                StructType(
                    [
                        StructField("incentive_type", StringType(), True),
                        StructField("incentive_code", StringType(), True),
                        StructField("incentive_amount", MONEY_STRUCT, True),
                        StructField("incentive_program_code", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

# ``transaction_id`` and ``transaction_updated_date`` are promoted from
# ``transaction_info`` so they can serve as primary key and cursor.
TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("transaction_updated_date", TimestampType(), True),
        StructField("transaction_info", TRANSACTION_INFO_STRUCT, True),
        StructField("payer_info", PAYER_INFO_STRUCT, True),
        StructField("shipping_info", SHIPPING_INFO_STRUCT, True),
        StructField("cart_info", CART_INFO_STRUCT, True),
        StructField("store_info", STORE_INFO_STRUCT, True),
        StructField("auction_info", AUCTION_INFO_STRUCT, True),
        StructField("incentive_info", INCENTIVE_INFO_STRUCT, True),
    ]
)


# =============================================================================
# balances
# =============================================================================

BALANCES_SCHEMA = StructType(
    [
        StructField("currency", StringType(), False),
        StructField("primary", BooleanType(), True),
        StructField("total_balance", MONEY_STRUCT, True),
        StructField("available_balance", MONEY_STRUCT, True),
        StructField("withheld_balance", MONEY_STRUCT, True),
        StructField("account_id", StringType(), True),
        StructField("as_of_time", TimestampType(), True),
        StructField("last_refresh_time", TimestampType(), True),
    ]
)


# =============================================================================
# disputes
# =============================================================================

DISPUTES_SCHEMA = StructType(
    [
        StructField("dispute_id", StringType(), False),
        StructField("create_time", TimestampType(), True),
        StructField("update_time", TimestampType(), True),
        StructField("reason", StringType(), True),
        StructField("status", StringType(), True),
        StructField("dispute_state", StringType(), True),
        StructField("dispute_life_cycle_stage", StringType(), True),
        StructField("dispute_channel", StringType(), True),
        StructField("dispute_amount", MONEY_STRUCT, True),
        StructField("buyer_response_due_date", TimestampType(), True),
        StructField("seller_response_due_date", TimestampType(), True),
        StructField("links", ArrayType(LINK_STRUCT), True),
    ]
)


# =============================================================================
# invoices
# =============================================================================

INVOICE_METADATA_STRUCT = StructType(
    [
        StructField("create_time", TimestampType(), True),
        StructField("created_by", StringType(), True),
        StructField("last_update_time", TimestampType(), True),
        StructField("last_updated_by", StringType(), True),
        StructField("cancel_time", TimestampType(), True),
        StructField("cancelled_by", StringType(), True),
        StructField("first_sent_time", TimestampType(), True),
        StructField("last_sent_time", TimestampType(), True),
        StructField("last_sent_by", StringType(), True),
        StructField("created_by_flow", StringType(), True),
        StructField("recipient_view_url", StringType(), True),
        StructField("invoicer_view_url", StringType(), True),
    ]
)

INVOICE_DETAIL_STRUCT = StructType(
    [
        StructField("invoice_number", StringType(), True),
        StructField("reference", StringType(), True),
        StructField("invoice_date", DateType(), True),
        StructField("currency_code", StringType(), True),
        StructField("note", StringType(), True),
        StructField("term", StringType(), True),
        StructField("memo", StringType(), True),
        StructField(
            "payment_term",
            StructType(
                [
                    StructField("term_type", StringType(), True),
                    StructField("due_date", DateType(), True),
                ]
            ),
            True,
        ),
        StructField("metadata", INVOICE_METADATA_STRUCT, True),
    ]
)

INVOICER_STRUCT = StructType(
    [
        StructField("name", NAME_STRUCT, True),
        StructField("email_address", StringType(), True),
        StructField("website", StringType(), True),
        StructField("tax_id", StringType(), True),
        StructField("logo_url", StringType(), True),
        StructField("additional_notes", StringType(), True),
    ]
)

INVOICE_RECIPIENT_STRUCT = StructType(
    [
        StructField(
            "billing_info",
            StructType(
                [
                    StructField("name", NAME_STRUCT, True),
                    StructField("email_address", StringType(), True),
                    StructField("language", StringType(), True),
                    StructField("additional_info", StringType(), True),
                    StructField("address", ADDRESS_STRUCT, True),
                ]
            ),
            True,
        ),
        StructField(
            "shipping_info",
            StructType(
                [
                    StructField("name", NAME_STRUCT, True),
                    StructField("address", ADDRESS_STRUCT, True),
                ]
            ),
            True,
        ),
    ]
)

INVOICE_AMOUNT_STRUCT = StructType(
    [
        StructField("currency_code", StringType(), True),
        StructField("value", StringType(), True),
        StructField(
            "breakdown",
            StructType(
                [
                    StructField("item_total", MONEY_STRUCT, True),
                    StructField(
                        "discount",
                        StructType(
                            [
                                StructField("invoice_discount", DISCOUNT_STRUCT, True),
                                StructField("item_discount", MONEY_STRUCT, True),
                            ]
                        ),
                        True,
                    ),
                    StructField("tax_total", MONEY_STRUCT, True),
                    StructField(
                        "shipping",
                        StructType(
                            [
                                StructField("amount", MONEY_STRUCT, True),
                                StructField("tax", TAX_STRUCT, True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "custom",
                        StructType(
                            [
                                StructField("label", StringType(), True),
                                StructField("amount", MONEY_STRUCT, True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

INVOICE_ITEM_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_amount", MONEY_STRUCT, True),
        StructField("tax", TAX_STRUCT, True),
        StructField("item_date", DateType(), True),
        StructField("discount", DISCOUNT_STRUCT, True),
        StructField("unit_of_measure", StringType(), True),
    ]
)

INVOICE_PAYMENT_TXN_STRUCT = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("method", StringType(), True),
        StructField("payment_date", DateType(), True),
        StructField("note", StringType(), True),
        StructField("amount", MONEY_STRUCT, True),
    ]
)

INVOICE_REFUND_TXN_STRUCT = StructType(
    [
        StructField("refund_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("method", StringType(), True),
        StructField("refund_date", DateType(), True),
        StructField("amount", MONEY_STRUCT, True),
    ]
)

INVOICES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("status", StringType(), True),
        StructField("detail", INVOICE_DETAIL_STRUCT, True),
        StructField("invoicer", INVOICER_STRUCT, True),
        StructField("primary_recipients", ArrayType(INVOICE_RECIPIENT_STRUCT), True),
        StructField("amount", INVOICE_AMOUNT_STRUCT, True),
        StructField("due_amount", MONEY_STRUCT, True),
        StructField("gratuity", MONEY_STRUCT, True),
        StructField("items", ArrayType(INVOICE_ITEM_STRUCT), True),
        StructField(
            "payments",
            StructType(
                [
                    StructField("paid_amount", MONEY_STRUCT, True),
                    StructField("transactions", ArrayType(INVOICE_PAYMENT_TXN_STRUCT), True),
                ]
            ),
            True,
        ),
        StructField(
            "refunds",
            StructType(
                [
                    StructField("refund_amount", MONEY_STRUCT, True),
                    StructField("transactions", ArrayType(INVOICE_REFUND_TXN_STRUCT), True),
                ]
            ),
            True,
        ),
        StructField("links", ArrayType(LINK_STRUCT), True),
    ]
)


# =============================================================================
# products / product_details
# =============================================================================

PRODUCTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("create_time", TimestampType(), True),
        StructField("links", ArrayType(LINK_STRUCT), True),
    ]
)

PRODUCT_DETAILS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", StringType(), True),
        StructField("category", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("home_url", StringType(), True),
        StructField("create_time", TimestampType(), True),
        StructField("update_time", TimestampType(), True),
        StructField("links", ArrayType(LINK_STRUCT), True),
    ]
)


# =============================================================================
# Table registry
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    "transactions": TRANSACTIONS_SCHEMA,
    "balances": BALANCES_SCHEMA,
    "disputes": DISPUTES_SCHEMA,
    "invoices": INVOICES_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "product_details": PRODUCT_DETAILS_SCHEMA,
}

TABLE_METADATA: dict[str, dict] = {
    "transactions": {
        "primary_keys": ["transaction_id"],
        "cursor_field": "transaction_updated_date",
        "ingestion_type": "cdc",
    },
    "balances": {
        "primary_keys": ["currency"],
        "ingestion_type": "snapshot",
    },
    "disputes": {
        "primary_keys": ["dispute_id"],
        "cursor_field": "update_time",
        "ingestion_type": "cdc",
    },
    "invoices": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "products": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "product_details": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
}

SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
