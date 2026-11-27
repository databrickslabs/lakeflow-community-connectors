"""Regression tests for KX KDB read correctness and trust boundaries."""

from __future__ import annotations

import base64
import json
import os
import pickle
import stat
import subprocess
import zipfile
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from pyspark import cloudpickle
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.libs.utils import parse_value
from databricks.labs.community_connector.sources.kx_kdb import conversion, filesystem, sym_reader
from databricks.labs.community_connector.sources.kx_kdb import runtime as runtime_mod
from databricks.labs.community_connector.sources.kx_kdb.kx_kdb import KxKdbLakeflowConnect
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    REMOVED_BOOTSTRAP_OPTIONS,
    PyKxRuntimeConfig,
    build_runtime_config,
)
from databricks.labs.community_connector.sources.kx_kdb.schema import (
    infer_schema_from_partition,
)
from databricks.labs.community_connector.sources.kx_kdb.sym_reader import PartitionShape

_SPEC_PATH = (
    Path(__file__).parents[4]
    / "src"
    / "databricks"
    / "labs"
    / "community_connector"
    / "sources"
    / "kx_kdb"
    / "connector_spec.yaml"
)
_LICENSE_B64 = base64.b64encode(b"license-SENTINEL-9a2e").decode("ascii")
_MODULE = "databricks.labs.community_connector.sources.kx_kdb.kx_kdb"


@pytest.fixture(autouse=True)
def _reset_caches(monkeypatch, tmp_path):
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOMES.clear()
    sym_reader._LOADED_SYM_DOMAIN.clear()
    filesystem.clear_discovery_cache()
    monkeypatch.setenv(runtime_mod._RUNTIME_ROOT_ENV, str(tmp_path / "runtime-root"))
    for name in ("HOME", "QLIC", "QHOME", "PYKX_LICENSED", "PATH"):
        monkeypatch.setenv(name, os.environ.get(name, ""))
    yield
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOMES.clear()
    sym_reader._LOADED_SYM_DOMAIN.clear()
    filesystem.clear_discovery_cache()


def _make_partition(root: Path, date: str, table: str, columns=("sym", "price")) -> Path:
    partition = root / date / table
    partition.mkdir(parents=True)
    for column_name in columns:
        (partition / column_name).write_bytes(b"stub")
    return partition


class _RangeKx:
    """Fake q session holding ``row_count`` rows of a sym/price partition."""

    def __init__(self, row_count: int):
        self.row_count = row_count
        self.queries: list[str] = []
        self.range_requests: list[tuple[int, int]] = []

    def q(self, query, *args):
        self.queries.append(query)
        if query == sym_reader._LOAD_SYM_DOMAIN_Q:
            return None
        if query == sym_reader._MAP_COLUMNS_Q:
            return [f"mapped:{path}" for path in args[0]]
        if query == sym_reader._MAPPED_COUNTS_Q:
            return [self.row_count] * len(args[0])
        if query == sym_reader._READ_ROWS_Q:
            _mapped, offset, size = args
            self.range_requests.append((offset, size))
            return [["AAPL"] * size, [float(index) for index in range(offset, offset + size)]]
        raise AssertionError(f"unexpected q query: {query!r}")


def _read_records(tmp_path, monkeypatch, fake_kx, **overrides):
    if not (tmp_path / "sym").exists():
        (tmp_path / "sym").write_bytes(b"stub")
    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: fake_kx)
    kwargs = {
        "hdb_root_path": str(tmp_path),
        "kdb_table_name": "TRADES",
        "date_partition": "2024.01.01",
        "row_offset": 0,
        "row_count": fake_kx.row_count,
        "total_rows": fake_kx.row_count,
        "runtime_config": PyKxRuntimeConfig(license_directory="/tmp/lic"),
        "column_defs": [
            {"name": "date", "spark_type": "StringType"},
            {"name": "sym", "spark_type": "StringType", "q_type": "s"},
            {"name": "price", "spark_type": "DoubleType", "q_type": "f"},
        ],
    }
    kwargs.update(overrides)
    return list(sym_reader.read_kdb_row_range(**kwargs))


# ---------------------------------------------------------------------------
# Read correctness
# ---------------------------------------------------------------------------


def test_chunk_slices_request_exactly_the_remaining_rows():
    slices = list(sym_reader._chunk_slices(847_133, 10_000))

    assert slices[0] == (0, 10_000)
    assert slices[-1] == (840_000, 7_133)
    assert sum(size for _, size in slices) == 847_133
    assert list(sym_reader._chunk_slices(0, 10_000)) == []
    assert list(sym_reader._chunk_slices(10_000, 10_000)) == [(0, 10_000)]


def test_reader_emits_no_phantom_rows_for_partial_last_chunk(monkeypatch, tmp_path):
    _make_partition(tmp_path, "2024.01.01", "TRADES")
    monkeypatch.setattr(sym_reader, "_ROW_CHUNK_SIZE", 3)
    fake_kx = _RangeKx(row_count=8)

    records = _read_records(tmp_path, monkeypatch, fake_kx)

    assert fake_kx.range_requests == [(0, 3), (3, 3), (6, 2)]
    assert [record["price"] for record in records] == [float(index) for index in range(8)]


def test_row_read_projects_row_indices_into_the_per_column_lambda():
    # q lambdas do not close over outer locals; an unprojected inner lambda
    # returns projections instead of column values.
    assert "idx: off + til m" in sym_reader._READ_ROWS_Q
    assert "{[idx; c]" in sym_reader._READ_ROWS_Q
    assert "[idx] each columns" in sym_reader._READ_ROWS_Q
    assert "16h=type v; `long$v" in sym_reader._READ_ROWS_Q
    assert "(type v) in 13 14 17 18 19h" in sym_reader._READ_ROWS_Q


def test_q_string_and_char_columns_decode_to_text():
    frame = sym_reader._column_vectors_to_frame(
        ["note", "side", "size"],
        [[b"alpha", b"", "caf\u00e9".encode()], b"BSB", [1, 2, 3]],
    )

    assert frame.to_dict(orient="list") == {
        "note": ["alpha", "", "caf\u00e9"],
        "side": ["B", "S", "B"],
        "size": [1, 2, 3],
    }


def test_executor_loads_sym_domain_before_reading_enumerated_columns(monkeypatch, tmp_path):
    _make_partition(tmp_path, "2024.01.01", "TRADES")
    fake_kx = _RangeKx(row_count=1)

    _read_records(tmp_path, monkeypatch, fake_kx)

    assert fake_kx.queries[0] == sym_reader._LOAD_SYM_DOMAIN_Q


def test_process_local_state_serializes_empty_for_executors(tmp_path):
    sym_reader._LOADED_SYM_DOMAIN[f"{tmp_path}/sym"] = (10, 20)
    runtime_mod._PREPARED_RUNTIME_KEYS.add("driver-runtime-key")
    runtime_mod._RUNTIME_HOMES["driver-runtime-key"] = tmp_path / "driver-home"

    for cache in (
        sym_reader._LOADED_SYM_DOMAIN,
        runtime_mod._PREPARED_RUNTIME_KEYS,
        runtime_mod._RUNTIME_HOMES,
    ):
        assert cache
        for dumps in (pickle.dumps, cloudpickle.dumps):
            restored = pickle.loads(dumps(cache))
            assert type(restored) is type(cache)
            assert len(restored) == 0
    for lock in (runtime_mod._RUNTIME_LOCK, sym_reader._SYM_DOMAIN_LOCK):
        with pickle.loads(cloudpickle.dumps(lock)):
            pass


def test_executor_reloads_sym_domain_after_cache_is_shipped(monkeypatch, tmp_path):
    _make_partition(tmp_path, "2024.01.01", "TRADES")
    (tmp_path / "sym").write_bytes(b"stub")
    sym_reader._ensure_sym_domain(_RangeKx(row_count=0), str(tmp_path))
    shipped = pickle.loads(pickle.dumps(sym_reader._LOADED_SYM_DOMAIN))
    monkeypatch.setattr(sym_reader, "_LOADED_SYM_DOMAIN", shipped)
    executor_kx = _RangeKx(row_count=1)

    _read_records(tmp_path, monkeypatch, executor_kx)

    assert executor_kx.queries[0] == sym_reader._LOAD_SYM_DOMAIN_Q


def test_sym_domain_reloads_when_the_sym_file_changes(tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")
    fake_kx = _RangeKx(row_count=0)

    sym_reader._ensure_sym_domain(fake_kx, str(tmp_path))
    sym_reader._ensure_sym_domain(fake_kx, str(tmp_path))
    (tmp_path / "sym").write_bytes(b"stub-appended")
    sym_reader._ensure_sym_domain(fake_kx, str(tmp_path))

    assert fake_kx.queries.count(sym_reader._LOAD_SYM_DOMAIN_Q) == 2


def test_unconverted_pykx_values_fail_instead_of_becoming_text():
    projection = type("Projection", (), {"__module__": "pykx.wrappers"})()

    with pytest.raises(TypeError, match="unconverted PyKX"):
        sym_reader._plain_python_value(projection)


def test_reader_yields_framework_parseable_dicts_only(monkeypatch, tmp_path):
    _make_partition(tmp_path, "2024.01.01", "TRADES")
    schema = StructType(
        [
            StructField("date", StringType()),
            StructField("sym", StringType()),
            StructField("price", DoubleType()),
        ]
    )

    records = _read_records(tmp_path, monkeypatch, _RangeKx(row_count=2))

    assert records and all(isinstance(record, dict) for record in records)
    parsed = [parse_value(record, schema) for record in records]
    assert parsed[0].asDict() == {"date": "2024.01.01", "sym": "AAPL", "price": 0.0}


class _TypedVector:
    """Fake q vector with a q type number and raw/non-raw conversions."""

    def __init__(self, q_type, raw, converted=None):
        self.t = q_type
        self._raw = raw
        self._converted = raw if converted is None else converted

    def py(self, raw=False):
        return self._raw if raw else self._converted


def test_every_q_type_reaches_spark_with_its_schema_type():
    """One row per q type goes through the single read path to typed Spark values."""
    columns = [
        ("flag", "BooleanType", _TypedVector(1, [True, False])),
        ("tiny", "IntegerType", _TypedVector(4, b"\x01\xff")),
        ("qty", "IntegerType", _TypedVector(5, [3, -(2**15)])),
        ("lots", "IntegerType", _TypedVector(6, [7, -(2**31)])),
        ("size", "LongType", _TypedVector(7, [2**53 + 1, -(2**63)])),
        ("price", "DoubleType", [1.5, float("nan")]),
        ("ts", "TimestampType", [datetime(2024, 1, 1, 9, 30, 0, 123456), None]),
        ("side", "StringType", b"BS"),
        ("note", "StringType", [b"caf\xc3\xa9", b""]),
        ("day", "StringType", ["2024.01.31", None]),
    ]
    names = [name for name, _, _ in columns]
    column_defs = [{"name": name, "spark_type": spark} for name, spark, _ in columns]
    spark_types = {
        "BooleanType": BooleanType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "DoubleType": DoubleType(),
        "TimestampType": TimestampType(),
        "StringType": StringType(),
    }
    schema = StructType([StructField(name, spark_types[spark]) for name, spark, _ in columns])

    frame = sym_reader._column_vectors_to_frame(
        names, [vector for _, _, vector in columns], expected_rows=2
    )
    records = list(
        conversion.iter_records(
            conversion.normalize_partition_frame(
                frame=frame,
                date_partition="2024.01.01",
                column_names=names,
                column_defs=column_defs,
            )
        )
    )
    rows = [parse_value(record, schema).asDict() for record in records]

    assert rows[0] == {
        "flag": True,
        "tiny": 1,
        "qty": 3,
        "lots": 7,
        "size": 2**53 + 1,
        "price": 1.5,
        "ts": datetime(2024, 1, 1, 9, 30, 0, 123456),
        "side": "B",
        "note": "caf\u00e9",
        "day": "2024.01.31",
    }
    assert rows[1] == {
        "flag": False,
        "tiny": 255,
        "qty": None,
        "lots": None,
        "size": None,
        "price": None,
        "ts": None,
        "side": "S",
        "note": "",
        "day": None,
    }


def test_spec_allowlist_contains_only_table_options():
    spec = yaml.safe_load(_SPEC_PATH.read_text(encoding="utf-8"))

    assert set(spec["external_options_allowlist"].split(",")) == {
        "start_date",
        "end_date",
        "ingestion_mode",
        "max_dates_per_batch",
        "max_tasks_per_batch",
        "max_rows_per_task",
        "min_partition_age_minutes",
        "missing_columns",
    }


def test_spec_declares_no_removed_bootstrap_option():
    spec = yaml.safe_load(_SPEC_PATH.read_text(encoding="utf-8"))
    names = {parameter["name"] for parameter in spec["connection"]["parameters"]}

    assert names.isdisjoint(REMOVED_BOOTSTRAP_OPTIONS)


# ---------------------------------------------------------------------------
# Option and path trust boundaries
# ---------------------------------------------------------------------------


def _merged_options(tmp_path, **table_options):
    """Connection options plus flat table options, as managed ingestion merges them."""
    (tmp_path / "hdb" / "2024.01.01" / "TRADES").mkdir(parents=True, exist_ok=True)
    keys = tmp_path / "vol" / "keys"
    keys.mkdir(parents=True, exist_ok=True)
    (keys / "kc.lic").write_bytes(b"connection-license")
    with zipfile.ZipFile(keys / "l64-bundle.zip", "w") as archive:
        archive.writestr("install_kdb.sh", "#!/bin/bash\n")
    return {
        "hdb_root_path": str(tmp_path / "hdb"),
        "license_volume_path": str(keys),
        **table_options,
    }


@pytest.mark.parametrize("option", REMOVED_BOOTSTRAP_OPTIONS)
def test_flat_pipeline_options_cannot_reach_removed_bootstrap_paths(tmp_path, option):
    with pytest.raises(ValueError, match="no longer supported"):
        KxKdbLakeflowConnect(_merged_options(tmp_path, **{option: "online"}))


def test_flat_pipeline_options_keep_the_connection_license_and_bundle(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod.platform, "machine", lambda: "x86_64")
    connector = KxKdbLakeflowConnect(
        _merged_options(tmp_path, start_date="2024.01.01", max_rows_per_task="10")
    )
    keys = tmp_path / "vol" / "keys"

    config = connector.runtime_config
    assert config.license_directory == str(keys)
    assert config.license_b64 == base64.b64encode(b"connection-license").decode("ascii")
    assert config.offline_bundle_path == str(keys / "l64-bundle.zip")
    assert config.uses_offline_bundle is True


@pytest.mark.parametrize("option", ["kdbx_offline_bundle_path", "kdbx_license_file_path"])
def test_pipeline_supplied_bootstrap_files_outside_license_volume_are_rejected(tmp_path, option):
    outside = tmp_path / "attacker" / "file.zip"

    with pytest.raises(ValueError, match=option):
        KxKdbLakeflowConnect(_merged_options(tmp_path, **{option: str(outside)}))


@pytest.mark.parametrize(
    ("path", "allowed"),
    [
        ("/Volumes/main/kx/files/kdbx/l64arm-bundle.zip", True),
        ("dbfs:/Volumes/main/kx/files/kdbx/l64arm-bundle.zip", True),
        ("/Volumes/main/kx/files/keys/kc.lic", True),
        ("/Volumes/main/kx/other/l64arm-bundle.zip", False),
        ("/Volumes/main/kx/files/../other/bundle.zip", False),
        ("relative/bundle.zip", False),
    ],
)
def test_bootstrap_files_must_share_the_license_volume(path, allowed):
    license_directory = "/Volumes/main/kx/files/keys"
    if allowed:
        assert runtime_mod._within_license_volume(path, license_directory, "opt") == path
    else:
        with pytest.raises(ValueError, match="opt"):
            runtime_mod._within_license_volume(path, license_directory, "opt")


def test_table_configs_json_is_not_parsed_into_connection_options(tmp_path):
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    hostile = {
        "hdb_root_path": "/evil/hdb",
        "license_volume_path": "/evil/lic",
        "kdbx_offline_bundle_path": "/evil/bundle.zip",
        "kdbx_install_mode": "online",
        "kdbx_license_b64": "ZXZpbA==",
    }

    connector = KxKdbLakeflowConnect(
        {
            "hdb_root_path": str(tmp_path),
            "license_volume_path": "/tmp/lic",
            "tableName": "_lakeflow_metadata",
            "tableNameList": "trades",
            "tableConfigs": json.dumps({"trades": hostile}),
        }
    )

    assert connector.hdb_root_path == str(tmp_path)
    assert connector.runtime_config.license_directory == "/tmp/lic"
    assert connector.runtime_config.offline_bundle_path is None
    assert connector.runtime_config.license_b64 is None


def test_read_partition_ignores_connection_keys_in_table_options(monkeypatch, tmp_path):
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    connector = KxKdbLakeflowConnect(
        {"hdb_root_path": str(tmp_path), "license_volume_path": "/tmp/lic"}
    )
    captured = {}
    monkeypatch.setattr(
        f"{_MODULE}.infer_schema_from_partition",
        lambda **_: [
            {"name": "date", "spark_type": "StringType"},
            {"name": "sym", "spark_type": "StringType", "q_type": "s"},
        ],
    )
    monkeypatch.setattr(
        f"{_MODULE}.read_kdb_row_range", lambda **kwargs: captured.update(kwargs) or iter(())
    )

    list(
        connector.read_partition(
            "trades",
            {"date_partition": "2024.01.01", "row_offset": 0, "row_count": 1, "total_rows": 1},
            {"hdb_root_path": "/evil/hdb", "license_volume_path": "/evil/lic"},
        )
    )

    assert captured["hdb_root_path"] == str(tmp_path)
    assert captured["runtime_config"].license_directory == "/tmp/lic"


class _SchemaKx:
    def __init__(self, names, types):
        self.names = names
        self.types = types
        self.queries: list[str] = []
        self.args: list[tuple] = []

    def DB(self, path):  # noqa: N802 - PyKX API name
        raise AssertionError("schema inference must not load the whole HDB")

    def q(self, query, *args):
        self.queries.append(query)
        self.args.append(args)
        if "`sym set get hsym p" in query:
            return None
        if "meta" in query:
            return [self.names, self.types]
        raise AssertionError(f"unexpected q query: {query!r}")


def test_schema_inference_keeps_hdb_names_out_of_q_source(monkeypatch, tmp_path):
    hostile_table = 'T;system"touch pwned";'
    _make_partition(tmp_path, "2024.01.01", hostile_table)
    (tmp_path / "sym").write_bytes(b"stub")
    fake_kx = _SchemaKx(["sym", "price"], ["s", "f"])
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: fake_kx,
    )

    columns = infer_schema_from_partition(
        hdb_root_path=str(tmp_path),
        kdb_table_name=hostile_table,
        date_partition="2024.01.01",
        runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
    )

    assert [column["name"] for column in columns] == ["date", "sym", "price"]
    assert fake_kx.queries
    assert all("system" not in query and hostile_table not in query for query in fake_kx.queries)
    assert any(hostile_table in str(args) for args in fake_kx.args)


def test_schema_type_chars_stay_aligned_with_names(monkeypatch, tmp_path):
    _make_partition(tmp_path, "2024.01.01", "TRADES")
    (tmp_path / "sym").write_bytes(b"stub")
    fake_kx = _SchemaKx(["sym", "blob", "price"], ["s", " ", "f"])
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: fake_kx,
    )

    columns = infer_schema_from_partition(
        hdb_root_path=str(tmp_path),
        kdb_table_name="TRADES",
        date_partition="2024.01.01",
        runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
    )

    assert {column["name"]: column["spark_type"] for column in columns} == {
        "date": "StringType",
        "sym": "StringType",
        "blob": "StringType",
        "price": "DoubleType",
    }


@pytest.mark.parametrize("column_name", ["../../outside", "a/b", "..", ""])
def test_schema_rejects_column_names_that_escape_the_partition(monkeypatch, tmp_path, column_name):
    _make_partition(tmp_path, "2024.01.01", "TRADES")
    (tmp_path / "sym").write_bytes(b"stub")
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: _SchemaKx(["sym", column_name], ["s", "f"]),
    )

    with pytest.raises(ValueError, match="column"):
        infer_schema_from_partition(
            hdb_root_path=str(tmp_path),
            kdb_table_name="TRADES",
            date_partition="2024.01.01",
            runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
        )


def test_symlinked_date_partition_is_rejected(tmp_path):
    root = tmp_path / "hdb"
    outside = tmp_path / "outside"
    (root / "2024.01.01" / "TRADES").mkdir(parents=True)
    (outside / "TRADES").mkdir(parents=True)
    (root / "2024.01.02").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symbolic link"):
        filesystem.discover_table_partitions(str(root), "trades")


def test_reader_rejects_symlinked_table_directory(monkeypatch, tmp_path):
    outside = tmp_path / "outside" / "TRADES"
    outside.mkdir(parents=True)
    (outside / "sym").write_bytes(b"stub")
    (tmp_path / "2024.01.01").mkdir()
    (tmp_path / "2024.01.01" / "TRADES").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symbolic link"):
        _read_records(tmp_path, monkeypatch, _RangeKx(row_count=1))


def test_reader_rejects_symlinked_column_file(monkeypatch, tmp_path):
    partition = _make_partition(tmp_path, "2024.01.01", "TRADES", columns=("sym",))
    secret = tmp_path / "secret.bin"
    secret.write_bytes(b"secret")
    (partition / "price").symlink_to(secret)

    with pytest.raises(ValueError, match="symbolic link"):
        _read_records(tmp_path, monkeypatch, _RangeKx(row_count=1))


def test_symlinked_nested_column_companion_file_is_rejected(monkeypatch, tmp_path):
    partition = _make_partition(tmp_path, "2024.01.01", "TRADES", columns=("sym", "price"))
    secret = tmp_path / "secret.bin"
    secret.write_bytes(b"secret")
    (partition / "price#").symlink_to(secret)

    with pytest.raises(ValueError, match="symbolic link"):
        _read_records(tmp_path, monkeypatch, _RangeKx(row_count=1))


def test_shape_check_rejects_symlinked_dot_d(monkeypatch, tmp_path):
    partition = _make_partition(tmp_path, "2024.01.01", "TRADES")
    (tmp_path / "sym").write_bytes(b"stub")
    (partition / ".d").symlink_to(tmp_path / "sym")
    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: pytest.fail("no q for symlinks"))

    with pytest.raises(ValueError, match="symbolic link"):
        sym_reader.read_partition_shape(
            hdb_root_path=str(tmp_path),
            kdb_table_name="TRADES",
            date_partition="2024.01.01",
            runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
            check_all_columns=True,
        )


def test_schema_inference_rejects_symlinked_table_content(monkeypatch, tmp_path):
    partition = _make_partition(tmp_path, "2024.01.01", "TRADES")
    (tmp_path / "sym").write_bytes(b"stub")
    (partition / ".d").symlink_to(tmp_path / "sym")
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.schema.prepare_pykx",
        lambda _: pytest.fail("q must not open symlinked table content"),
    )

    with pytest.raises(ValueError, match="symbolic link"):
        infer_schema_from_partition(
            hdb_root_path=str(tmp_path),
            kdb_table_name="TRADES",
            date_partition="2024.01.01",
            runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
        )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("2024.01.02", "2024.01.02"), ("2024-01-02", "2024.01.02"), ("2024/01/02", "2024.01.02")],
)
def test_normalize_partition_date_accepts_supported_formats(raw, expected):
    assert filesystem.normalize_partition_date(raw) == expected


@pytest.mark.parametrize("raw", ["garbage", "2024.13.45", "2024.02.30", "24-01-02", "../2024"])
def test_normalize_partition_date_rejects_invalid_values(raw):
    with pytest.raises(ValueError, match="date"):
        filesystem.normalize_partition_date(raw)


def test_discovery_cache_ttl_bounds_staleness():
    assert filesystem._DISCOVERY_CACHE_TTL_SECONDS <= 300


def test_latest_offset_sees_new_dates_after_discovery_ttl(monkeypatch, tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")
    for date in ("2024.01.01", "2024.01.02"):
        (tmp_path / date / "TRADES").mkdir(parents=True)
    for name in ("read_partition_shape", "probe_partition_shape"):
        monkeypatch.setattr(
            f"{_MODULE}.{name}",
            lambda **_: PartitionShape(present=True, complete=True, row_count=1),
        )
    connector = KxKdbLakeflowConnect(
        {"hdb_root_path": str(tmp_path), "license_volume_path": "/tmp/lic"}
    )
    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.02"}

    (tmp_path / "2024.01.03" / "TRADES").mkdir(parents=True)
    monkeypatch.setattr(filesystem, "_DISCOVERY_CACHE_TTL_SECONDS", 0)

    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.03"}


def test_schema_uses_first_selected_date_that_contains_the_table(monkeypatch, tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    (tmp_path / "2024.01.02").mkdir()
    (tmp_path / "2024.01.03" / "TRADES").mkdir(parents=True)
    seen = []
    monkeypatch.setattr(
        f"{_MODULE}.infer_schema_from_partition",
        lambda **kwargs: (
            seen.append(kwargs["date_partition"])
            or [{"name": "sym", "spark_type": "StringType", "q_type": "s"}]
        ),
    )
    connector = KxKdbLakeflowConnect(
        {"hdb_root_path": str(tmp_path), "license_volume_path": "/tmp/lic"}
    )

    connector.get_table_schema("trades", {"start_date": "2024.01.02"})

    assert seen == ["2024.01.03"]


def test_byte_and_short_columns_widen_to_integers_the_parser_accepts():
    from databricks.labs.community_connector.sources.kx_kdb.schema import _map_meta_type_char

    assert _map_meta_type_char("x") == "IntegerType"
    assert _map_meta_type_char("h") == "IntegerType"
    assert parse_value(5, IntegerType()) == 5


def test_q_integer_nulls_and_infinities_keep_exact_values():
    class _LongVector:
        t = 7

        def py(self, raw=False):
            assert raw is True
            return [1, -(2**63), 2**63 - 1, 2**53 + 1]

    frame = sym_reader._column_vectors_to_frame(["size"], [_LongVector()])
    records = list(
        conversion.iter_records(
            conversion.normalize_partition_frame(
                frame=frame,
                date_partition="2024.01.01",
                column_names=["size"],
                column_defs=[{"name": "size", "spark_type": "LongType"}],
            )
        )
    )

    assert [record["size"] for record in records] == [1, None, 2**63 - 1, 2**53 + 1]


def test_column_vectors_are_converted_one_by_one_to_keep_q_types():
    class _LongVector:
        t = 7

        def py(self, raw=False):
            return [2**63 - 1] if raw else [float("inf")]

    class _QList:
        def __iter__(self):
            return iter([_LongVector()])

        def py(self):
            raise AssertionError("the outer q list must not be converted as a whole")

    frame = sym_reader._column_vectors_to_frame(["big"], _QList())

    assert frame["big"].tolist() == [2**63 - 1]


def test_pandas_na_becomes_none_not_text():
    import pandas as pd

    assert sym_reader._plain_python_value(pd.NA) is None


# ---------------------------------------------------------------------------
# Secrets, subprocesses, and runtime materialization
# ---------------------------------------------------------------------------


class _Recorder:
    def __init__(self, results):
        self._results = list(results)
        self.calls: list[dict] = []

    def __call__(self, args, **kwargs):
        rc, stdout, stderr = self._results.pop(0)
        self.calls.append({"args": list(args), **kwargs})
        return subprocess.CompletedProcess(args, rc, stdout=stdout, stderr=stderr)


def _bundle(path: Path, members=()) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("install_kdb.sh", "#!/bin/bash\n")
        for member in members:
            if isinstance(member, zipfile.ZipInfo):
                archive.writestr(member, "/etc/passwd")
            else:
                archive.writestr(member, "owned")
    return path


def _offline_config(tmp_path, **overrides):
    values = {
        "license_directory": str(tmp_path / "lic"),
        "license_b64": _LICENSE_B64,
        "offline_bundle_path": str(_bundle(tmp_path / "bundle.zip")),
    }
    values.update(overrides)
    return PyKxRuntimeConfig(**values)


def _install(config):
    runtime_mod._install_kdbx_offline(config, runtime_mod._runtime_home(config))


def test_installer_environment_is_allowlisted(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABRICKS_TOKEN", "workspace-token")
    monkeypatch.setenv("KDB_LICENSE_B64", _LICENSE_B64)
    monkeypatch.setenv("UNRELATED_COPY", _LICENSE_B64)
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:3128")
    recorder = _Recorder([(0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = _offline_config(tmp_path)

    _install(config)

    env = recorder.calls[0]["env"]
    assert "DATABRICKS_TOKEN" not in env
    assert "KDB_LICENSE_B64" not in env
    assert _LICENSE_B64 not in env.values()
    assert env["HTTPS_PROXY"] == "http://proxy:3128"
    assert env["HOME"] == str(runtime_mod._runtime_home(config))
    assert env["TERM"] == "dumb"


def test_installer_never_reads_the_parent_stdin(monkeypatch, tmp_path):
    recorder = _Recorder([(0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    _install(_offline_config(tmp_path))

    assert recorder.calls[0]["stdin"] is subprocess.DEVNULL


def test_installer_failures_redact_the_license(monkeypatch, tmp_path):
    leak = f"Using --b64lic option:\n{_LICENSE_B64}\nfailed"
    monkeypatch.setattr(runtime_mod.subprocess, "run", _Recorder([(1, leak, leak)]))

    with pytest.raises(RuntimeError) as excinfo:
        _install(_offline_config(tmp_path))

    assert _LICENSE_B64 not in str(excinfo.value)
    assert "[REDACTED]" in str(excinfo.value)


def test_installer_timeout_does_not_leak_license_from_argv(monkeypatch, tmp_path):
    def _run(args, **_kwargs):
        raise subprocess.TimeoutExpired(args, 1200, output=_LICENSE_B64.encode())

    monkeypatch.setattr(runtime_mod.subprocess, "run", _run)

    with pytest.raises(RuntimeError, match="timed out") as excinfo:
        _install(_offline_config(tmp_path))

    assert _LICENSE_B64 not in str(excinfo.value)
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None


def test_runtime_config_repr_hides_the_license(tmp_path):
    assert _LICENSE_B64 not in repr(_offline_config(tmp_path))


def test_license_is_materialized_owner_only_and_not_exported(monkeypatch, tmp_path):
    monkeypatch.delenv("KDB_LICENSE_B64", raising=False)
    config = PyKxRuntimeConfig(
        license_directory="/Volumes/main/kx/files/keys", license_b64=_LICENSE_B64
    )
    home = runtime_mod._runtime_home(config)

    runtime_mod._apply_pykx_environment(config, home)

    license_file = home / "qlic" / "kc.lic"
    assert license_file.read_bytes() == base64.b64decode(_LICENSE_B64)
    assert stat.S_IMODE(license_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(license_file.parent.stat().st_mode) == 0o700
    assert os.environ["QLIC"] == str(license_file.parent)
    assert "KDB_LICENSE_B64" not in os.environ
    assert _LICENSE_B64 not in os.environ.values()


def test_runtime_key_distinguishes_same_length_licenses_without_storing_them(tmp_path):
    first = runtime_mod._runtime_key(_offline_config(tmp_path, license_b64="QUFBQQ=="))
    second = runtime_mod._runtime_key(_offline_config(tmp_path, license_b64="QkJCQg=="))

    assert first != second
    assert "QUFBQQ==" not in first


@pytest.mark.parametrize("member", ["../escape.sh", "/abs/escape.sh", "nested/../../escape.sh"])
def test_offline_bundle_rejects_members_outside_extract_dir(monkeypatch, tmp_path, member):
    recorder = _Recorder([(0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = _offline_config(
        tmp_path, offline_bundle_path=str(_bundle(tmp_path / "evil.zip", [member]))
    )

    with pytest.raises(RuntimeError, match="unsafe"):
        _install(config)
    assert recorder.calls == []
    assert not (tmp_path / "escape.sh").exists()


def test_offline_bundle_rejects_symlink_members(monkeypatch, tmp_path):
    link = zipfile.ZipInfo("l64arm.zip")
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    monkeypatch.setattr(runtime_mod.subprocess, "run", _Recorder([]))
    config = _offline_config(
        tmp_path, offline_bundle_path=str(_bundle(tmp_path / "evil.zip", [link]))
    )

    with pytest.raises(RuntimeError, match="unsafe"):
        _install(config)


def test_commercial_k4_license_file_uses_k4_installer_flag(monkeypatch, tmp_path):
    license_dir = tmp_path / "lic"
    license_dir.mkdir()
    (license_dir / "k4.lic").write_bytes(b"commercial-license")
    bundle = _bundle(tmp_path / "bundle.zip")
    config = build_runtime_config(
        {
            "license_volume_path": str(license_dir),
            "kdbx_license_file_path": str(license_dir / "k4.lic"),
            "kdbx_offline_bundle_path": str(bundle),
        }
    )
    assert config.license_file_name == "k4.lic"
    recorder = _Recorder([(0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    _install(config)

    args = recorder.calls[0]["args"]
    assert "--k4b64lic" in args
    assert "--b64lic" not in args


def test_explicit_k4_license_kind_materializes_k4_file(tmp_path):
    license_dir = tmp_path / "lic"
    license_dir.mkdir()
    (license_dir / "license.txt").write_bytes(b"commercial-license")
    config = build_runtime_config(
        {
            "license_volume_path": str(license_dir),
            "kdbx_license_file_path": str(license_dir / "license.txt"),
            "kdbx_license_kind": "k4",
        }
    )
    assert config.license_file_name == "k4.lic"
    home = runtime_mod._runtime_home(config)

    runtime_mod._apply_pykx_environment(config, home)

    assert (home / "qlic" / "k4.lic").is_file()
    assert not (home / "qlic" / "kc.lic").exists()
