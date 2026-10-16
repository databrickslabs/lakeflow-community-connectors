"""Stable Spark schemas for the QuickBooks Online ingestion tables."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_DECIMAL = DecimalType(38, 9)


def _common_fields() -> list[StructField]:
    return [
        StructField("realm_id", StringType(), nullable=False),
        StructField("id", StringType(), nullable=False),
        StructField("sync_token", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("last_updated_at", TimestampType(), nullable=True),
    ]


def _schema(*fields: StructField) -> StructType:
    return StructType([*_common_fields(), *fields, StructField("raw_json", StringType(), False)])


TABLE_SCHEMAS = {
    "customers": _schema(
        StructField("display_name", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("given_name", StringType(), True),
        StructField("family_name", StringType(), True),
        StructField("primary_email", StringType(), True),
        StructField("primary_phone", StringType(), True),
        StructField("balance", _DECIMAL, True),
        StructField("currency_ref", StringType(), True),
        StructField("active", BooleanType(), True),
    ),
    "vendors": _schema(
        StructField("display_name", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("given_name", StringType(), True),
        StructField("family_name", StringType(), True),
        StructField("primary_email", StringType(), True),
        StructField("primary_phone", StringType(), True),
        StructField("balance", _DECIMAL, True),
        StructField("vendor_1099", BooleanType(), True),
        StructField("currency_ref", StringType(), True),
        StructField("active", BooleanType(), True),
    ),
    "accounts": _schema(
        StructField("name", StringType(), True),
        StructField("fully_qualified_name", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("account_sub_type", StringType(), True),
        StructField("classification", StringType(), True),
        StructField("current_balance", _DECIMAL, True),
        StructField("currency_ref", StringType(), True),
        StructField("active", BooleanType(), True),
    ),
    "items": _schema(
        StructField("name", StringType(), True),
        StructField("fully_qualified_name", StringType(), True),
        StructField("item_type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("unit_price", _DECIMAL, True),
        StructField("purchase_cost", _DECIMAL, True),
        StructField("quantity_on_hand", _DECIMAL, True),
        StructField("income_account_ref", StringType(), True),
        StructField("expense_account_ref", StringType(), True),
        StructField("asset_account_ref", StringType(), True),
        StructField("active", BooleanType(), True),
    ),
    "invoices": _schema(
        StructField("doc_number", StringType(), True),
        StructField("txn_date", DateType(), True),
        StructField("due_date", DateType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("total_amount", _DECIMAL, True),
        StructField("balance", _DECIMAL, True),
        StructField("currency_ref", StringType(), True),
        StructField("email_status", StringType(), True),
        StructField("print_status", StringType(), True),
        StructField("line_json", StringType(), True),
    ),
    "bills": _schema(
        StructField("doc_number", StringType(), True),
        StructField("txn_date", DateType(), True),
        StructField("due_date", DateType(), True),
        StructField("vendor_ref", StringType(), True),
        StructField("total_amount", _DECIMAL, True),
        StructField("balance", _DECIMAL, True),
        StructField("currency_ref", StringType(), True),
        StructField("ap_account_ref", StringType(), True),
        StructField("line_json", StringType(), True),
    ),
}
