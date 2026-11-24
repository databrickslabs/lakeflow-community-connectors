"""Deployment-path tests for the generated QuickBooks Spark data source."""

from unittest.mock import Mock

from databricks.labs.community_connector.sparkpds.registry import register


def test_legacy_registration_constructs_quickbooks_connector() -> None:
    spark = Mock()

    register(spark, "quickbooks")

    generated_data_source = spark.dataSource.register.call_args.args[0]
    source = generated_data_source(
        {
            "access_token": "test-token",
            "realm_id": "test-realm",
            "environment": "sandbox",
            "tableName": "customers",
        }
    )

    assert source.lakeflow_connect.list_tables() == [
        "customers",
        "vendors",
        "accounts",
        "items",
        "invoices",
        "bills",
    ]
    assert all(
        source.lakeflow_connect.read_table_metadata(table, {})["ingestion_type"] == "cdc"
        for table in ("customers", "vendors", "accounts", "items")
    )
    assert all(
        source.lakeflow_connect.read_table_metadata(table, {})["ingestion_type"]
        == "cdc_with_deletes"
        for table in ("invoices", "bills")
    )
    assert source.schema().fieldNames() == [
        "realm_id",
        "id",
        "sync_token",
        "created_at",
        "last_updated_at",
        "display_name",
        "company_name",
        "given_name",
        "family_name",
        "primary_email",
        "primary_phone",
        "balance",
        "currency_ref",
        "active",
        "raw_json",
    ]
