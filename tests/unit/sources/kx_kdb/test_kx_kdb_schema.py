"""Tests for KDB schema inference helpers."""

import pytest

from databricks.labs.community_connector.sources.kx_kdb import sym_reader
from databricks.labs.community_connector.sources.kx_kdb.runtime import PyKxRuntimeConfig
from databricks.labs.community_connector.sources.kx_kdb.schema import (
    _dedupe_column_defs,
    _map_meta_type_char,
    columns_to_spark_schema,
    infer_schema_from_partition,
)


@pytest.fixture(autouse=True)
def _reset_sym_domain():
    sym_reader._LOADED_SYM_DOMAIN.clear()
    yield
    sym_reader._LOADED_SYM_DOMAIN.clear()


def test_map_meta_type_char_uses_connector_safe_mappings():
    assert _map_meta_type_char("s") == "StringType"
    assert _map_meta_type_char("f") == "DoubleType"
    assert _map_meta_type_char(b"f") == "DoubleType"
    assert _map_meta_type_char("b'f'") == "DoubleType"
    assert _map_meta_type_char("d") == "StringType"
    assert _map_meta_type_char("p") == "TimestampType"
    assert _map_meta_type_char("n") == "LongType"
    assert _map_meta_type_char(" ") == "StringType"


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


class _MetaKx:
    def __init__(self, meta):
        self.meta = meta
        self.calls = []

    def q(self, query, *args):
        self.calls.append((query, args))
        if query == "{[p] `sym set get hsym p}":
            return None
        if query == "{[p] m:0!meta get hsym p; (string m`c; m`t)}":
            return self.meta
        raise AssertionError(f"unexpected query: {query!r}")


def _hdb(tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    return tmp_path


def test_infer_schema_reads_splayed_partition_meta(monkeypatch, tmp_path):
    root = _hdb(tmp_path)
    fake_kx = _MetaKx([[b"sym", b"time", b"price", b"size"], b"spfj"])
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: fake_kx,
    )

    columns = infer_schema_from_partition(
        hdb_root_path=str(root),
        kdb_table_name="TRADES",
        date_partition="2024.01.01",
        runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
    )

    assert fake_kx.calls == [
        ("{[p] `sym set get hsym p}", (f"{root}/sym",)),
        (
            "{[p] m:0!meta get hsym p; (string m`c; m`t)}",
            (f"{root}/2024.01.01/TRADES/",),
        ),
    ]
    assert columns == [
        {"name": "date", "spark_type": "StringType"},
        {"name": "sym", "spark_type": "StringType"},
        {"name": "time", "spark_type": "TimestampType"},
        {"name": "price", "spark_type": "DoubleType"},
        {"name": "size", "spark_type": "LongType"},
    ]


def test_infer_schema_rejects_misaligned_meta(monkeypatch, tmp_path):
    root = _hdb(tmp_path)
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: _MetaKx([["sym", "price"], "s"]),
    )

    with pytest.raises(RuntimeError, match="2 column names and 1 type codes"):
        infer_schema_from_partition(
            hdb_root_path=str(root),
            kdb_table_name="TRADES",
            date_partition="2024.01.01",
            runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
        )


def test_infer_schema_reports_partition_path(monkeypatch, tmp_path):
    root = _hdb(tmp_path)

    class _FailingKx:
        def q(self, query, *args):
            if "meta" in query:
                raise RuntimeError("nyi")
            return None

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: _FailingKx(),
    )

    with pytest.raises(RuntimeError, match=f"{root}/2024.01.01/TRADES"):
        infer_schema_from_partition(
            hdb_root_path=str(root),
            kdb_table_name="TRADES",
            date_partition="2024.01.01",
            runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
        )
