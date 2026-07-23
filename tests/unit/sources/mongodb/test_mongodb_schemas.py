"""Unit tests for the deterministic bronze schema."""

from pyspark.sql.types import StringType

from databricks.labs.community_connector.sources.mongodb import mongodb_utils as U
from databricks.labs.community_connector.sources.mongodb.mongodb_schemas import (
    HAS_VARIANT,
    variant_bronze_schema,
)


def test_schema_is_deterministic():
    assert variant_bronze_schema() == variant_bronze_schema()


def test_schema_field_order():
    fields = [f.name for f in variant_bronze_schema().fields]
    assert fields == [
        U.COL_ID,
        U.COL_OP,
        U.COL_CLUSTER_TIME,
        U.COL_DB,
        U.COL_COLLECTION,
        U.COL_DOCUMENT,
    ]


def test_id_not_nullable():
    assert variant_bronze_schema()[U.COL_ID].nullable is False


def test_document_defaults_to_string():
    # STRING by default — streaming-safe (VARIANT unsupported in streaming PDS scans).
    assert isinstance(variant_bronze_schema()[U.COL_DOCUMENT].dataType, StringType)


def test_document_variant_when_opted_in():
    doc_type = variant_bronze_schema(as_variant=True)[U.COL_DOCUMENT].dataType
    if HAS_VARIANT:
        assert doc_type.simpleString() == "variant"
    else:
        assert isinstance(doc_type, StringType)
