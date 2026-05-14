"""Static schema definitions for Shopify connector tables.

Schemas are derived from the Shopify Admin REST API (version 2026-04).
See ``shopify_api_doc.md`` for field references and per-table edge
cases.

Pragmatic field coverage: top-level scalars + nested objects/arrays
that don't have their own dedicated table. The ``*_set`` mirror
fields (e.g. ``total_price_set``) are dropped — they duplicate the
scalar field with currency metadata. Nested resources that have a
dedicated table (e.g. orders.refunds, orders.fulfillments) are also
dropped from the parent.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# Reusable nested struct definitions
# =============================================================================

ADDRESS_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("name", StringType(), True),
        StructField("company", StringType(), True),
        StructField("address1", StringType(), True),
        StructField("address2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("province", StringType(), True),
        StructField("country", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("province_code", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("default", BooleanType(), True),
    ]
)

# Marketing-consent dict shape used by both email and SMS
MARKETING_CONSENT_STRUCT = StructType(
    [
        StructField("state", StringType(), True),
        StructField("opt_in_level", StringType(), True),
        StructField("consent_updated_at", StringType(), True),
        StructField("consent_collected_from", StringType(), True),
    ]
)

# Slimmed customer reference embedded on orders. The full Customer
# record lives in the `customers` table — keep just enough here to
# join cleanly without explosion.
ORDER_CUSTOMER_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("note", StringType(), True),
        StructField("verified_email", BooleanType(), True),
        StructField("currency", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

# Single line item on an order
ORDER_LINE_ITEM_STRUCT = StructType(
    [
        StructField("id", LongType(), False),
        StructField("admin_graphql_api_id", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("variant_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("variant_title", StringType(), True),
        StructField("name", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("vendor", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("current_quantity", LongType(), True),
        StructField("fulfillable_quantity", LongType(), True),
        StructField("fulfillment_service", StringType(), True),
        StructField("fulfillment_status", StringType(), True),
        StructField("price", StringType(), True),
        StructField("total_discount", StringType(), True),
        StructField("requires_shipping", BooleanType(), True),
        StructField("taxable", BooleanType(), True),
        StructField("gift_card", BooleanType(), True),
        StructField("grams", LongType(), True),
        StructField("variant_inventory_management", StringType(), True),
        StructField("product_exists", BooleanType(), True),
    ]
)

PRODUCT_VARIANT_STRUCT = StructType(
    [
        StructField("id", LongType(), False),
        StructField("admin_graphql_api_id", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("position", LongType(), True),
        StructField("inventory_policy", StringType(), True),
        StructField("compare_at_price", StringType(), True),
        StructField("option1", StringType(), True),
        StructField("option2", StringType(), True),
        StructField("option3", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("taxable", BooleanType(), True),
        StructField("barcode", StringType(), True),
        StructField("fulfillment_service", StringType(), True),
        StructField("grams", LongType(), True),
        StructField("inventory_management", StringType(), True),
        StructField("requires_shipping", BooleanType(), True),
        StructField("sku", StringType(), True),
        StructField("weight", DoubleType(), True),
        StructField("weight_unit", StringType(), True),
        StructField("inventory_item_id", LongType(), True),
        StructField("inventory_quantity", LongType(), True),
        StructField("image_id", LongType(), True),
    ]
)

PRODUCT_OPTION_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("position", LongType(), True),
        StructField("values", ArrayType(StringType()), True),
    ]
)

PRODUCT_IMAGE_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("admin_graphql_api_id", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("position", LongType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("alt", StringType(), True),
        StructField("width", LongType(), True),
        StructField("height", LongType(), True),
        StructField("src", StringType(), True),
        StructField("variant_ids", ArrayType(LongType()), True),
    ]
)

# Slim refund line item — drops the deeply-nested ``line_item`` mirror
# (use ``line_item_id`` to join back to ``orders.line_items``).
REFUND_LINE_ITEM_STRUCT = StructType(
    [
        StructField("id", LongType(), False),
        StructField("line_item_id", LongType(), True),
        StructField("location_id", LongType(), True),
        StructField("quantity", LongType(), True),
        StructField("restock_type", StringType(), True),
        StructField("subtotal", DoubleType(), True),
        StructField("total_tax", DoubleType(), True),
    ]
)


# =============================================================================
# Table Schemas
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    "locations": StructType(
        [
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("address1", StringType(), True),
            StructField("address2", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("province", StringType(), True),
            StructField("country", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("province_code", StringType(), True),
            StructField("legacy", BooleanType(), True),
            StructField("active", BooleanType(), True),
            StructField("admin_graphql_api_id", StringType(), True),
            StructField("localized_country_name", StringType(), True),
            StructField("localized_province_name", StringType(), True),
            StructField("shop", StringType(), False),
        ]
    ),
    "customers": StructType(
        [
            StructField("id", LongType(), False),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("orders_count", LongType(), True),
            StructField("state", StringType(), True),
            StructField("total_spent", StringType(), True),
            StructField("last_order_id", LongType(), True),
            StructField("last_order_name", StringType(), True),
            StructField("note", StringType(), True),
            StructField("verified_email", BooleanType(), True),
            StructField("multipass_identifier", StringType(), True),
            StructField("tax_exempt", BooleanType(), True),
            StructField("tags", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("addresses", ArrayType(ADDRESS_STRUCT), True),
            StructField("default_address", ADDRESS_STRUCT, True),
            StructField("tax_exemptions", ArrayType(StringType()), True),
            StructField(
                "email_marketing_consent", MARKETING_CONSENT_STRUCT, True
            ),
            StructField(
                "sms_marketing_consent", MARKETING_CONSENT_STRUCT, True
            ),
            StructField("admin_graphql_api_id", StringType(), True),
            StructField("shop", StringType(), False),
        ]
    ),
    "products": StructType(
        [
            StructField("id", LongType(), False),
            StructField("title", StringType(), True),
            StructField("body_html", StringType(), True),
            StructField("vendor", StringType(), True),
            StructField("product_type", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("published_at", StringType(), True),
            StructField("handle", StringType(), True),
            StructField("template_suffix", StringType(), True),
            StructField("published_scope", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("status", StringType(), True),
            StructField("admin_graphql_api_id", StringType(), True),
            StructField(
                "variants", ArrayType(PRODUCT_VARIANT_STRUCT), True
            ),
            StructField(
                "options", ArrayType(PRODUCT_OPTION_STRUCT), True
            ),
            StructField(
                "images", ArrayType(PRODUCT_IMAGE_STRUCT), True
            ),
            StructField("image", PRODUCT_IMAGE_STRUCT, True),
            StructField("shop", StringType(), False),
        ]
    ),
    "refunds": StructType(
        [
            StructField("id", LongType(), False),
            StructField("order_id", LongType(), False),
            StructField("created_at", StringType(), True),
            StructField("processed_at", StringType(), True),
            StructField("note", StringType(), True),
            StructField("user_id", LongType(), True),
            StructField("restock", BooleanType(), True),
            StructField("admin_graphql_api_id", StringType(), True),
            StructField(
                "refund_line_items",
                ArrayType(REFUND_LINE_ITEM_STRUCT),
                True,
            ),
            StructField("shop", StringType(), False),
        ]
    ),
    "fulfillments": StructType(
        [
            StructField("id", LongType(), False),
            StructField("order_id", LongType(), False),
            StructField("status", StringType(), True),
            StructField("shipment_status", StringType(), True),
            StructField("service", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("location_id", LongType(), True),
            StructField("tracking_company", StringType(), True),
            StructField("tracking_number", StringType(), True),
            StructField(
                "tracking_numbers", ArrayType(StringType()), True
            ),
            StructField("tracking_url", StringType(), True),
            StructField(
                "tracking_urls", ArrayType(StringType()), True
            ),
            StructField("admin_graphql_api_id", StringType(), True),
            StructField(
                "line_items", ArrayType(ORDER_LINE_ITEM_STRUCT), True
            ),
            StructField("shop", StringType(), False),
        ]
    ),
    "inventory_levels": StructType(
        [
            StructField("inventory_item_id", LongType(), False),
            StructField("location_id", LongType(), False),
            StructField("available", LongType(), True),
            StructField("updated_at", StringType(), True),
            StructField("admin_graphql_api_id", StringType(), True),
            StructField("shop", StringType(), False),
        ]
    ),
    "orders": StructType(
        [
            StructField("id", LongType(), False),
            StructField("admin_graphql_api_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("number", LongType(), True),
            StructField("order_number", LongType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("processed_at", StringType(), True),
            StructField("closed_at", StringType(), True),
            StructField("cancelled_at", StringType(), True),
            StructField("cancel_reason", StringType(), True),
            StructField("financial_status", StringType(), True),
            StructField("fulfillment_status", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("presentment_currency", StringType(), True),
            StructField("total_price", StringType(), True),
            StructField("subtotal_price", StringType(), True),
            StructField("total_tax", StringType(), True),
            StructField("total_discounts", StringType(), True),
            StructField("total_line_items_price", StringType(), True),
            StructField("total_outstanding", StringType(), True),
            StructField("total_tip_received", StringType(), True),
            StructField("total_weight", LongType(), True),
            StructField("taxes_included", BooleanType(), True),
            StructField("estimated_taxes", BooleanType(), True),
            StructField("duties_included", BooleanType(), True),
            StructField("confirmed", BooleanType(), True),
            StructField("test", BooleanType(), True),
            StructField("buyer_accepts_marketing", BooleanType(), True),
            StructField("tax_exempt", BooleanType(), True),
            StructField("tags", StringType(), True),
            StructField("note", StringType(), True),
            StructField("token", StringType(), True),
            StructField("cart_token", StringType(), True),
            StructField("checkout_id", LongType(), True),
            StructField("checkout_token", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("source_identifier", StringType(), True),
            StructField("source_name", StringType(), True),
            StructField("source_url", StringType(), True),
            StructField("landing_site", StringType(), True),
            StructField("landing_site_ref", StringType(), True),
            StructField("referring_site", StringType(), True),
            StructField("browser_ip", StringType(), True),
            StructField("customer_locale", StringType(), True),
            StructField("location_id", LongType(), True),
            StructField("user_id", LongType(), True),
            StructField("device_id", LongType(), True),
            StructField("app_id", LongType(), True),
            StructField("po_number", StringType(), True),
            StructField("confirmation_number", StringType(), True),
            StructField(
                "payment_gateway_names", ArrayType(StringType()), True
            ),
            StructField("customer", ORDER_CUSTOMER_STRUCT, True),
            StructField("shipping_address", ADDRESS_STRUCT, True),
            StructField("billing_address", ADDRESS_STRUCT, True),
            StructField(
                "line_items", ArrayType(ORDER_LINE_ITEM_STRUCT), True
            ),
            StructField("shop", StringType(), False),
        ]
    ),
}


# =============================================================================
# Table Metadata
# =============================================================================

TABLE_METADATA: dict[str, dict] = {
    "locations": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "customers": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "products": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "orders": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "refunds": {
        "primary_keys": ["id"],
        "cursor_field": "created_at",
        "ingestion_type": "append",
    },
    "fulfillments": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "inventory_levels": {
        "primary_keys": ["inventory_item_id", "location_id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
