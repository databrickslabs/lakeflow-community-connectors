"""Tests for KDB schema inference helpers."""

from databricks.labs.community_connector.sources.kx_kdb.runtime import PyKxRuntimeConfig
from databricks.labs.community_connector.sources.kx_kdb.schema import (
    _dedupe_column_defs,
    _map_meta_type_char,
    columns_to_spark_schema,
    deserialize_columns,
    infer_schema_from_partition,
    serialize_columns,
)


def test_map_meta_type_char_uses_connector_safe_mappings():
    assert _map_meta_type_char("s") == "StringType"
    assert _map_meta_type_char("f") == "DoubleType"
    assert _map_meta_type_char(b"f") == "DoubleType"
    assert _map_meta_type_char("b'f'") == "DoubleType"
    assert _map_meta_type_char("d") == "StringType"
    assert _map_meta_type_char("p") == "TimestampType"
    assert _map_meta_type_char("n") == "LongType"


def test_dedupe_column_defs_is_case_insensitive():
    assert _dedupe_column_defs(
        [
            {"name": "date", "spark_type": "StringType"},
            {"name": "DATE", "spark_type": "StringType"},
            {"name": "sym", "spark_type": "StringType"},
        ]
    ) == [
        {"name": "date", "spark_type": "StringType"},
        {"name": "sym", "spark_type": "StringType"},
    ]


def test_columns_to_spark_schema_builds_struct_type():
    schema = columns_to_spark_schema(
        [
            {"name": "date", "spark_type": "StringType"},
            {"name": "price", "spark_type": "DoubleType"},
            {"name": "event_ts", "spark_type": "TimestampType"},
        ]
    )
    assert [field.name for field in schema.fields] == ["date", "price", "event_ts"]
    assert str(schema["price"].dataType) == "DoubleType()"
    assert str(schema["event_ts"].dataType) == "TimestampType()"


def test_serialize_columns_roundtrips():
    columns = [{"name": "price", "spark_type": "DoubleType"}]
    assert deserialize_columns(serialize_columns(columns)) == columns


def test_infer_schema_uses_fuse_partition_path(monkeypatch):
    class _FakeKx:
        def __init__(self):
            self.queries = []

        def DB(self, path):
            raise RuntimeError(f"no DB for {path}")

        def q(self, query, *args):
            self.queries.append((query, args))
            if query == "{[p] __lakeflow_schema_tmp: get hsym p}":
                return None
            if query == "string (0!meta __lakeflow_schema_tmp)`c":
                return ["sym", "price"]
            if query == "string (0!meta __lakeflow_schema_tmp)`t":
                return ["s", "f"]
            if query == "delete __lakeflow_schema_tmp from `.":
                return None
            raise AssertionError(f"unexpected query: {query!r}")

    fake_kx = _FakeKx()
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: fake_kx,
    )
    columns = infer_schema_from_partition(
        hdb_root_path="/Volumes/main/rkh/rbc_kx/kdb_format",
        kdb_table_name="TRADES",
        date_partition="2024.01.01",
        runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
    )

    assert fake_kx.queries[0] == (
        "{[p] __lakeflow_schema_tmp: get hsym p}",
        ("/Volumes/main/rkh/rbc_kx/kdb_format/2024.01.01/TRADES",),
    )
    assert columns == [
        {"name": "date", "spark_type": "StringType"},
        {"name": "sym", "spark_type": "StringType"},
        {"name": "price", "spark_type": "DoubleType"},
    ]


def test_infer_schema_falls_back_to_splayed_column_files(monkeypatch):
    class _FakeKx:
        def __init__(self):
            self.queries = []

        def DB(self, path):
            raise RuntimeError(f"no DB for {path}")

        def q(self, query, *args):
            self.queries.append((query, args))
            if query == "{[p] __lakeflow_schema_tmp: get hsym p}":
                raise RuntimeError("cannot load table")
            if query == "{[p] get hsym p}":
                return ["sym", "price"]
            if query == "{[p] string .Q.ty type get hsym p}":
                return {
                    "/Volumes/main/rkh/rbc_kx/kdb_format/2024.01.01/TRADES/sym": "s",
                    "/Volumes/main/rkh/rbc_kx/kdb_format/2024.01.01/TRADES/price": "f",
                }[args[0]]
            raise AssertionError(f"unexpected query: {query!r}")

    fake_kx = _FakeKx()
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: fake_kx,
    )
    columns = infer_schema_from_partition(
        hdb_root_path="/Volumes/main/rkh/rbc_kx/kdb_format",
        kdb_table_name="TRADES",
        date_partition="2024.01.01",
        runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
    )

    assert fake_kx.queries[1] == (
        "{[p] get hsym p}",
        ("/Volumes/main/rkh/rbc_kx/kdb_format/2024.01.01/TRADES/.d",),
    )
    assert columns == [
        {"name": "date", "spark_type": "StringType"},
        {"name": "sym", "spark_type": "StringType"},
        {"name": "price", "spark_type": "DoubleType"},
    ]


def test_infer_schema_reports_fuse_partition_path(monkeypatch):
    class _FailingKx:
        def DB(self, path):
            raise RuntimeError(f"missing db: {path}")

        def q(self, query, *args):
            raise RuntimeError(f"missing path: {args[0]}")

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: _FailingKx(),
    )

    try:
        infer_schema_from_partition(
            hdb_root_path="/Volumes/main/rkh/rbc_kx/kdb_format",
            kdb_table_name="TRADES",
            date_partition="2024.01.01",
            runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
        )
    except RuntimeError as exc:
        message = str(exc)
        assert "/Volumes/main/rkh/rbc_kx/kdb_format/2024.01.01/TRADES" in message
    else:
        raise AssertionError("expected schema inference failure")
