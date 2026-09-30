"""Regression tests for KX KDB read correctness and trust boundaries."""

from __future__ import annotations

import base64
import importlib
import inspect
import json
import os
import stat
import subprocess
import zipfile
from pathlib import Path

import pytest
import yaml
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from databricks.labs.community_connector.libs.utils import parse_value
from databricks.labs.community_connector.sources.kx_kdb import conversion, filesystem, sym_reader
from databricks.labs.community_connector.sources.kx_kdb import runtime as runtime_mod
from databricks.labs.community_connector.sources.kx_kdb.kx_kdb import KxKdbLakeflowConnect
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    PyKxRuntimeConfig,
    build_runtime_config,
)
from databricks.labs.community_connector.sources.kx_kdb.schema import (
    infer_schema_from_partition,
)

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
_TOKEN = "tok-SENTINEL-4f1c"
_LICENSE_B64 = base64.b64encode(b"license-SENTINEL-9a2e").decode("ascii")


@pytest.fixture(autouse=True)
def _reset_caches(monkeypatch):
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOME_CACHE.clear()
    runtime_mod._LOCAL_BUNDLE_DIR_CACHE.clear()
    sym_reader._LOADED_SYM_DOMAIN.clear()
    filesystem.clear_discovery_cache()
    monkeypatch.delenv(runtime_mod._RUNTIME_HOME_OVERRIDE_ENV, raising=False)
    for name in ("HOME", "QLIC", "PYKX_LICENSED", "PATH"):
        monkeypatch.setenv(name, os.environ.get(name, ""))
    yield
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOME_CACHE.clear()
    runtime_mod._LOCAL_BUNDLE_DIR_CACHE.clear()
    sym_reader._LOADED_SYM_DOMAIN.clear()
    filesystem.clear_discovery_cache()


def _make_partition(root: Path, date: str, table: str, columns=("sym", "price")) -> Path:
    partition = root / date / table
    partition.mkdir(parents=True)
    for column_name in columns:
        (partition / column_name).write_bytes(b"stub")
    return partition


class _ChunkKx:
    """Fake q session holding one symbol's row indices for a partition."""

    def __init__(self, row_count: int):
        self.row_count = row_count
        self.queries: list[str] = []
        self.chunk_requests: list[tuple[int, int]] = []

    def q(self, query, *args):
        self.queries.append(query)
        if query == "{[p] `sym set get hsym p}":
            return None
        if query == "{[sympath; symi] `chunk_idx set where symi=`long$get hsym sympath}":
            return None
        if query == "count chunk_idx":
            return self.row_count
        if "chunk_idx[row_off+til m]" in query:
            row_off, size = int(args[0]), int(args[1])
            self.chunk_requests.append((row_off, size))
            available = max(0, min(size, self.row_count - row_off))
            return list(range(row_off, row_off + available))
        if "each column_paths" in query:
            indices = list(args[1])
            return [["AAPL"] * len(indices), [float(index) for index in indices]]
        raise AssertionError(f"unexpected q query: {query!r}")


def _read_records(tmp_path, monkeypatch, fake_kx, **overrides):
    if not (tmp_path / "sym").exists():
        (tmp_path / "sym").write_bytes(b"stub")
    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: fake_kx)
    kwargs = {
        "hdb_root_path": str(tmp_path),
        "kdb_table_name": "TRADES",
        "date_partition": "2024.01.01",
        "sym_index": 1,
        "runtime_config": PyKxRuntimeConfig(license_directory="/tmp/lic"),
        "column_defs": [
            {"name": "date", "spark_type": "StringType"},
            {"name": "sym", "spark_type": "StringType"},
            {"name": "price", "spark_type": "DoubleType"},
        ],
    }
    kwargs.update(overrides)
    return list(sym_reader.read_kdb_date_sym_records(**kwargs))


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
    fake_kx = _ChunkKx(row_count=8)

    records = _read_records(tmp_path, monkeypatch, fake_kx)

    assert fake_kx.chunk_requests == [(0, 3), (3, 3), (6, 2)]
    assert len(records) == 8
    assert all(record["price"] is not None for record in records)
    assert not any("count chunk_idx - row_off" in query for query in fake_kx.queries)


def test_column_read_projects_row_indices_into_the_per_column_lambda(monkeypatch, tmp_path):
    _make_partition(tmp_path, "2024.01.01", "TRADES")
    fake_kx = _ChunkKx(row_count=1)

    _read_records(tmp_path, monkeypatch, fake_kx)

    column_reads = [query for query in fake_kx.queries if "each column_paths" in query]
    # q lambdas do not close over outer locals; an unprojected inner lambda
    # returns projections instead of column values.
    assert column_reads == [sym_reader._READ_COLUMNS_Q]
    assert "{[idx; p]" in sym_reader._READ_COLUMNS_Q
    assert "[idx] each column_paths" in sym_reader._READ_COLUMNS_Q
    assert "16h=type v; `long$v" in sym_reader._READ_COLUMNS_Q
    assert "(type v) in 13 14 17 18 19h" in sym_reader._READ_COLUMNS_Q


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
    fake_kx = _ChunkKx(row_count=1)

    _read_records(tmp_path, monkeypatch, fake_kx)

    assert fake_kx.queries[0] == "{[p] `sym set get hsym p}"


def test_process_local_caches_serialize_empty_for_executors(tmp_path):
    import pickle

    from pyspark import cloudpickle

    sym_reader._LOADED_SYM_DOMAIN[f"{tmp_path}/sym"] = (10, 20)
    runtime_mod._PREPARED_RUNTIME_KEYS.add("driver-runtime-key")
    runtime_mod._RUNTIME_HOME_CACHE["path"] = tmp_path / "driver-home"
    runtime_mod._LOCAL_BUNDLE_DIR_CACHE["path"] = tmp_path / "driver-bundles"

    for cache in (
        sym_reader._LOADED_SYM_DOMAIN,
        runtime_mod._PREPARED_RUNTIME_KEYS,
        runtime_mod._RUNTIME_HOME_CACHE,
        runtime_mod._LOCAL_BUNDLE_DIR_CACHE,
    ):
        assert cache
        for dumps in (pickle.dumps, cloudpickle.dumps):
            restored = pickle.loads(dumps(cache))
            assert type(restored) is type(cache)
            assert len(restored) == 0


def test_executor_reloads_sym_domain_after_cache_is_shipped(monkeypatch, tmp_path):
    import pickle

    _make_partition(tmp_path, "2024.01.01", "TRADES")
    (tmp_path / "sym").write_bytes(b"stub")
    sym_reader._ensure_sym_domain(_ChunkKx(row_count=0), str(tmp_path))
    shipped = pickle.loads(pickle.dumps(sym_reader._LOADED_SYM_DOMAIN))
    monkeypatch.setattr(sym_reader, "_LOADED_SYM_DOMAIN", shipped)
    executor_kx = _ChunkKx(row_count=1)

    _read_records(tmp_path, monkeypatch, executor_kx)

    assert executor_kx.queries[0] == "{[p] `sym set get hsym p}"


def test_unconverted_pykx_values_fail_instead_of_becoming_text():
    projection = type("Projection", (), {"__module__": "pykx.wrappers"})()

    with pytest.raises(TypeError, match="unconverted PyKX"):
        sym_reader._plain_python_value(projection)


def test_sym_enumeration_keeps_empty_and_slash_symbols_positional(monkeypatch, tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")

    class _SymKx:
        def q(self, query, *args):
            if query == "{[p] `sym set get hsym p}":
                return None
            if query == "sym":
                return ["", "DBRX", "AMZ", "BRK/B", "BRK/A"]
            raise AssertionError(f"unexpected q query: {query!r}")

    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: _SymKx())

    symbols = sym_reader.load_sym_enumeration(
        str(tmp_path), PyKxRuntimeConfig(license_directory="/tmp/lic")
    )

    assert symbols == ["", "DBRX", "AMZ", "BRK/B", "BRK/A"]


def test_partition_descriptors_use_kdb_enumeration_positions(monkeypatch, tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")
    for date in ("2024.01.01", "2024.01.02"):
        (tmp_path / date / "TRADES").mkdir(parents=True)
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.load_sym_enumeration",
        lambda *_: ["", "DBRX", "AMZ", "BRK/B"],
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.infer_schema_from_partition",
        lambda **_: [{"name": "sym", "spark_type": "StringType", "q_type": "s"}],
    )
    connector = KxKdbLakeflowConnect(
        {"hdb_root_path": str(tmp_path), "license_volume_path": "/tmp/lic"}
    )

    descriptors = connector.get_partitions(
        "trades", {}, {"date_partition": "2024.01.01"}, {"date_partition": "2024.01.02"}
    )

    assert descriptors == [
        {"date_partition": "2024.01.02", "sym": "", "sym_index": 0},
        {"date_partition": "2024.01.02", "sym": "DBRX", "sym_index": 1},
        {"date_partition": "2024.01.02", "sym": "AMZ", "sym_index": 2},
        {"date_partition": "2024.01.02", "sym": "BRK/B", "sym_index": 3},
        {"date_partition": "2024.01.02", "sym": None, "sym_index": None, "sym_count": 4},
    ]


def test_reader_yields_framework_parseable_dicts_only(monkeypatch, tmp_path):
    _make_partition(tmp_path, "2024.01.01", "TRADES")
    schema = StructType(
        [
            StructField("date", StringType()),
            StructField("sym", StringType()),
            StructField("price", DoubleType()),
        ]
    )

    records = _read_records(tmp_path, monkeypatch, _ChunkKx(row_count=2))

    assert records and all(isinstance(record, dict) for record in records)
    parsed = [parse_value(record, schema) for record in records]
    assert parsed[0].asDict() == {"date": "2024.01.01", "sym": "AAPL", "price": 0.0}
    assert "conversion_mode" not in inspect.signature(
        sym_reader.read_kdb_date_sym_records
    ).parameters


def test_shadow_conversion_modes_are_removed():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(
            "databricks.labs.community_connector.sources.kx_kdb.conversion_metrics"
        )
    for removed in (
        "result_to_frame",
        "result_len",
        "normalize_conversion_mode",
        "PARTITION_CONVERSION_MODES",
    ):
        assert not hasattr(conversion, removed)


def test_spec_allowlist_contains_only_table_options():
    spec = yaml.safe_load(_SPEC_PATH.read_text(encoding="utf-8"))

    assert set(spec["external_options_allowlist"].split(",")) == {
        "start_date",
        "end_date",
        "ingestion_mode",
        "partition_strategy",
        "sym_column",
    }


# ---------------------------------------------------------------------------
# Option and path trust boundaries
# ---------------------------------------------------------------------------


def test_table_configs_cannot_replace_connection_or_bootstrap_options(tmp_path):
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    hostile = {
        "hdb_root_path": "/evil/hdb",
        "license_volume_path": "/evil/lic",
        "pykx_install_spec": "/evil/pykx.whl",
        "kdbx_offline_bundle_path": "/evil/bundle.zip",
        "kdbx_install_mode": "online",
        "kdbx_license_b64": "ZXZpbA==",
        "kdbx_install_bearer_token": "evil-token",
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
    assert not hasattr(connector.runtime_config, "pykx_install_spec")
    assert connector.runtime_config.offline_bundle_path is None
    assert connector.runtime_config.license_b64 is None
    assert connector.runtime_config.installer_bearer_token is None


def test_read_partition_ignores_connection_keys_in_table_options(monkeypatch, tmp_path):
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    connector = KxKdbLakeflowConnect(
        {"hdb_root_path": str(tmp_path), "license_volume_path": "/tmp/lic"}
    )
    captured = {}
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.infer_schema_from_partition",
        lambda **_: [
            {"name": "date", "spark_type": "StringType"},
            {"name": "sym", "spark_type": "StringType", "q_type": "s"},
        ],
    )

    def _fake_reader(**kwargs):
        captured.update(kwargs)
        return iter(())

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.read_kdb_date_sym_records",
        _fake_reader,
    )

    list(
        connector.read_partition(
            "trades",
            {"date_partition": "2024.01.01", "sym": "a", "sym_index": 0},
            {"hdb_root_path": "/evil/hdb", "pykx_install_spec": "/evil/pykx.whl"},
        )
    )

    assert captured["hdb_root_path"] == str(tmp_path)
    assert captured["runtime_config"].offline_bundle_path is None


@pytest.mark.parametrize("sym_column", ["../other", "a/b", "..", "x\x00y", "a\nb"])
def test_unsafe_sym_column_is_rejected(tmp_path, sym_column):
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    connector = KxKdbLakeflowConnect(
        {"hdb_root_path": str(tmp_path), "license_volume_path": "/tmp/lic"}
    )

    with pytest.raises(ValueError, match="sym_column"):
        connector.read_partition(
            "trades",
            {"date_partition": "2024.01.01", "sym": "a", "sym_index": 0},
            {"sym_column": sym_column},
        )


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
def test_schema_rejects_column_names_that_escape_the_partition(
    monkeypatch, tmp_path, column_name
):
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
        filesystem.list_date_partitions(str(root))


def test_reader_rejects_symlinked_table_directory(monkeypatch, tmp_path):
    outside = tmp_path / "outside" / "TRADES"
    outside.mkdir(parents=True)
    (outside / "sym").write_bytes(b"stub")
    (tmp_path / "2024.01.01").mkdir()
    (tmp_path / "2024.01.01" / "TRADES").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symbolic link"):
        _read_records(tmp_path, monkeypatch, _ChunkKx(row_count=1))


def test_reader_rejects_symlinked_column_file(monkeypatch, tmp_path):
    partition = _make_partition(tmp_path, "2024.01.01", "TRADES", columns=("sym",))
    secret = tmp_path / "secret.bin"
    secret.write_bytes(b"secret")
    (partition / "price").symlink_to(secret)

    with pytest.raises(ValueError, match="symbolic link"):
        _read_records(tmp_path, monkeypatch, _ChunkKx(row_count=1))


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


def _online_config(tmp_path, **overrides):
    values = {
        "license_directory": str(tmp_path / "lic"),
        "installer_bearer_token": _TOKEN,
        "license_b64": _LICENSE_B64,
    }
    values.update(overrides)
    return PyKxRuntimeConfig(**values)


def test_curl_receives_bearer_token_on_stdin_not_argv(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    recorder = _Recorder([(0, "", ""), (0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    runtime_mod._install_kdbx(_online_config(tmp_path))

    curl_call = recorder.calls[0]
    assert curl_call["args"][:2] == ["curl", "-q"]
    assert "--config" in curl_call["args"]
    assert all(_TOKEN not in arg for arg in curl_call["args"])
    assert _TOKEN in curl_call["input"]
    assert _TOKEN not in json.dumps(curl_call.get("env") or {})


@pytest.mark.parametrize("token", ['bad"token', "bad\ntoken", "bad token", "bad\\token"])
def test_unsafe_bearer_token_characters_are_rejected(monkeypatch, tmp_path, token):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    monkeypatch.setattr(runtime_mod.subprocess, "run", _Recorder([]))

    with pytest.raises(ValueError, match="bearer token") as excinfo:
        runtime_mod._install_kdbx(_online_config(tmp_path, installer_bearer_token=token))
    assert token not in str(excinfo.value)


def test_child_processes_do_not_inherit_license_environment(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    monkeypatch.setenv("KDB_LICENSE_B64", _LICENSE_B64)
    monkeypatch.setenv("KDB_K4LICENSE_B64", _LICENSE_B64)
    monkeypatch.setenv("UNRELATED_COPY", _TOKEN)
    recorder = _Recorder([(0, "", ""), (0, "", ""), (0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = _online_config(tmp_path)

    runtime_mod._install_kdbx(config)
    runtime_mod._ensure_pykx_package(config)

    for call in recorder.calls:
        env = call["env"]
        assert "KDB_LICENSE_B64" not in env
        assert "KDB_K4LICENSE_B64" not in env
        assert _TOKEN not in env.values()
        assert _LICENSE_B64 not in env.values()


def test_subprocess_failures_redact_secrets(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    leak = f"Using --b64lic option:\n{_LICENSE_B64}\nauth {_TOKEN} failed"
    monkeypatch.setattr(runtime_mod.subprocess, "run", _Recorder([(0, "", ""), (1, leak, leak)]))

    with pytest.raises(RuntimeError) as excinfo:
        runtime_mod._install_kdbx(_online_config(tmp_path))

    message = str(excinfo.value)
    assert _LICENSE_B64 not in message
    assert _TOKEN not in message
    assert "[REDACTED]" in message

    monkeypatch.setattr(runtime_mod.subprocess, "run", _Recorder([(22, "", f"401 {_TOKEN}")]))
    with pytest.raises(RuntimeError) as excinfo:
        runtime_mod._install_kdbx(_online_config(tmp_path))
    assert _TOKEN not in str(excinfo.value)


def test_installer_and_pip_never_read_the_parent_stdin(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    recorder = _Recorder([(0, "", ""), (0, "", ""), (0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = _online_config(tmp_path)

    runtime_mod._install_kdbx(config)
    runtime_mod._ensure_pykx_package(config)

    curl_call, install_call, pip_call = recorder.calls
    assert "stdin" not in curl_call and curl_call["input"]
    assert install_call["stdin"] is subprocess.DEVNULL
    assert pip_call["stdin"] is subprocess.DEVNULL


def test_installer_timeout_does_not_leak_license_from_argv(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    calls = []

    def _run(args, **kwargs):
        calls.append(args)
        if args[0] == "bash":
            raise subprocess.TimeoutExpired(args, 1200, output=f"{_LICENSE_B64}".encode())
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(runtime_mod.subprocess, "run", _run)

    with pytest.raises(RuntimeError, match="timed out") as excinfo:
        runtime_mod._install_kdbx(_online_config(tmp_path))

    assert _LICENSE_B64 not in str(excinfo.value)
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None


def test_runtime_config_repr_hides_secrets(tmp_path):
    text = repr(_online_config(tmp_path))

    assert _TOKEN not in text
    assert _LICENSE_B64 not in text


def test_license_is_materialized_owner_only_and_not_exported(monkeypatch, tmp_path):
    runtime_home = tmp_path / "home"
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: runtime_home)
    monkeypatch.delenv("KDB_LICENSE_B64", raising=False)
    original_cwd = os.getcwd()
    try:
        runtime_mod._apply_pykx_environment(
            PyKxRuntimeConfig(license_directory="", license_b64=_LICENSE_B64)
        )
    finally:
        os.chdir(original_cwd)

    license_file = runtime_home / "qlic" / "kc.lic"
    assert license_file.read_bytes() == base64.b64decode(_LICENSE_B64)
    assert stat.S_IMODE(license_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(license_file.parent.stat().st_mode) == 0o700
    assert "KDB_LICENSE_B64" not in os.environ
    assert _LICENSE_B64 not in os.environ.values()


def test_runtime_key_distinguishes_same_length_secrets_without_storing_them(tmp_path):
    first = runtime_mod._runtime_key(_online_config(tmp_path, license_b64="QUFBQQ=="))
    second = runtime_mod._runtime_key(_online_config(tmp_path, license_b64="QkJCQg=="))

    assert first != second
    assert "QUFBQQ==" not in first
    assert _TOKEN not in first


def test_offline_bundle_cache_is_keyed_by_source(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_local_bundle_directory", lambda: tmp_path / "cache")
    (tmp_path / "cache").mkdir()
    first = tmp_path / "a" / "l64-bundle.zip"
    second = tmp_path / "b" / "l64-bundle.zip"
    for path, payload in ((first, b"first"), (second, b"second")):
        path.parent.mkdir()
        path.write_bytes(payload)

    assert runtime_mod._localize_offline_bundle(str(first)).read_bytes() == b"first"
    assert runtime_mod._localize_offline_bundle(str(second)).read_bytes() == b"second"


@pytest.mark.parametrize("member", ["../escape.sh", "/abs/escape.sh", "nested/../../escape.sh"])
def test_offline_bundle_rejects_members_outside_extract_dir(monkeypatch, tmp_path, member):
    bundle = tmp_path / "bundle.zip"
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("install_kdb.sh", "#!/bin/bash\n")
        archive.writestr(member, "owned")
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    recorder = _Recorder([(0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        license_b64=_LICENSE_B64,
        offline_bundle_path=str(bundle),
    )

    with pytest.raises(RuntimeError, match="unsafe"):
        runtime_mod._install_kdbx(config)
    assert recorder.calls == []
    assert not (tmp_path / "escape.sh").exists()


def test_offline_bundle_rejects_symlink_members(monkeypatch, tmp_path):
    bundle = tmp_path / "bundle.zip"
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("install_kdb.sh", "#!/bin/bash\n")
        link = zipfile.ZipInfo("l64arm.zip")
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(link, "/etc/passwd")
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    monkeypatch.setattr(runtime_mod.subprocess, "run", _Recorder([]))
    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        license_b64=_LICENSE_B64,
        offline_bundle_path=str(bundle),
    )

    with pytest.raises(RuntimeError, match="unsafe"):
        runtime_mod._install_kdbx(config)


def test_commercial_k4_license_file_uses_k4_installer_flag(monkeypatch, tmp_path):
    license_dir = tmp_path / "lic"
    license_dir.mkdir()
    (license_dir / "k4.lic").write_bytes(b"commercial-license")
    bundle = tmp_path / "bundle.zip"
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("install_kdb.sh", "#!/bin/bash\n")

    config = build_runtime_config(
        {
            "license_volume_path": str(license_dir),
            "kdbx_license_file_path": str(license_dir / "k4.lic"),
            "kdbx_offline_bundle_path": str(bundle),
        }
    )
    assert config.license_file_name == "k4.lic"

    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    recorder = _Recorder([(0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    runtime_mod._install_kdbx(config)

    args = recorder.calls[0]["args"]
    assert "--k4b64lic" in args
    assert "--b64lic" not in args


def test_explicit_k4_license_kind_materializes_k4_file(monkeypatch, tmp_path):
    config = build_runtime_config(
        {
            "license_volume_path": "",
            "kdbx_install_bearer_token": _TOKEN,
            "kdbx_license_b64": _LICENSE_B64,
            "kdbx_license_kind": "k4",
            "kdbx_install_mode": "online",
        }
    )
    assert config.license_file_name == "k4.lic"

    runtime_home = tmp_path / "home"
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: runtime_home)
    original_cwd = os.getcwd()
    try:
        runtime_mod._apply_pykx_environment(config)
    finally:
        os.chdir(original_cwd)
    assert (runtime_home / "qlic" / "k4.lic").is_file()
    assert not (runtime_home / "qlic" / "kc.lic").exists()


def test_unknown_license_kind_is_rejected():
    with pytest.raises(ValueError, match="kdbx_license_kind"):
        build_runtime_config(
            {
                "license_volume_path": "/tmp/lic",
                "kdbx_install_bearer_token": _TOKEN,
                "kdbx_license_b64": _LICENSE_B64,
                "kdbx_license_kind": "personal",
            }
        )


# ---------------------------------------------------------------------------
# Review follow-ups
# ---------------------------------------------------------------------------


def _connector_with_schema(monkeypatch, tmp_path, columns, symbols=("AAPL", "MSFT")):
    (tmp_path / "sym").write_bytes(b"stub")
    for date in ("2024.01.01", "2024.01.02"):
        (tmp_path / date / "TRADES").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.load_sym_enumeration",
        lambda *_: list(symbols),
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.infer_schema_from_partition",
        lambda **_: columns,
    )
    return KxKdbLakeflowConnect({"hdb_root_path": str(tmp_path), "license_volume_path": "/tmp/lic"})


def test_unmatched_symbol_rows_are_read_by_index_range(monkeypatch, tmp_path):
    _make_partition(tmp_path, "2024.01.01", "TRADES")

    class _UnmatchedKx(_ChunkKx):
        def q(self, query, *args):
            if query == sym_reader._UNMATCHED_ROWS_Q:
                self.queries.append(query)
                self.unmatched_args = args
                return None
            return super().q(query, *args)

    fake_kx = _UnmatchedKx(row_count=2)
    records = _read_records(tmp_path, monkeypatch, fake_kx, sym_index=None, sym_count=11)

    assert fake_kx.unmatched_args == (f"{tmp_path}/2024.01.01/TRADES/sym", 11)
    assert len(records) == 2
    assert not any("chunk_idx set where symi=" in query for query in fake_kx.queries)


def test_unmatched_symbol_read_requires_symbol_count(tmp_path):
    with pytest.raises(ValueError, match="sym_count"):
        list(
            sym_reader.read_kdb_date_sym_records(
                hdb_root_path=str(tmp_path),
                kdb_table_name="TRADES",
                date_partition="2024.01.01",
                sym_index=None,
                runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
                column_defs=[],
            )
        )


def test_sym_column_must_be_an_enumerated_symbol_column(monkeypatch, tmp_path):
    connector = _connector_with_schema(
        monkeypatch,
        tmp_path,
        [
            {"name": "sym", "spark_type": "StringType", "q_type": "s"},
            {"name": "price", "spark_type": "DoubleType", "q_type": "f"},
        ],
    )

    with pytest.raises(ValueError, match="not a column"):
        connector.get_partitions("trades", {"sym_column": "ticker"})
    with pytest.raises(ValueError, match="enumerated symbol column"):
        connector.get_partitions("trades", {"sym_column": "price"})


def test_byte_and_short_columns_widen_to_integers_the_parser_accepts():
    from pyspark.sql.types import IntegerType

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


def test_symlinked_nested_column_companion_file_is_rejected(monkeypatch, tmp_path):
    partition = _make_partition(tmp_path, "2024.01.01", "TRADES", columns=("sym", "note"))
    secret = tmp_path / "secret.bin"
    secret.write_bytes(b"secret")
    (partition / "note#").symlink_to(secret)

    with pytest.raises(ValueError, match="symbolic link"):
        _read_records(tmp_path, monkeypatch, _ChunkKx(row_count=1))


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


def test_latest_offset_sees_new_dates_after_discovery_ttl(monkeypatch, tmp_path):
    connector = _connector_with_schema(
        monkeypatch, tmp_path, [{"name": "sym", "spark_type": "StringType", "q_type": "s"}]
    )
    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.02"}

    (tmp_path / "2024.01.03" / "TRADES").mkdir(parents=True)
    monkeypatch.setattr(filesystem, "_DISCOVERY_CACHE_TTL_SECONDS", 0)

    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.03"}


def test_symbols_reload_when_the_sym_file_changes(monkeypatch, tmp_path):
    connector = _connector_with_schema(
        monkeypatch, tmp_path, [{"name": "sym", "spark_type": "StringType", "q_type": "s"}]
    )
    loads = []
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.load_sym_enumeration",
        lambda *_: loads.append(1) or ["A"] * len(loads),
    )

    assert connector._symbols() == ["A"]
    assert connector._symbols() == ["A"]
    (tmp_path / "sym").write_bytes(b"stub-appended")
    assert connector._symbols() == ["A", "A"]
    assert len(loads) == 2


def test_schema_uses_first_selected_date_that_contains_the_table(monkeypatch, tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    (tmp_path / "2024.01.02").mkdir()
    (tmp_path / "2024.01.03" / "TRADES").mkdir(parents=True)
    seen = []
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.infer_schema_from_partition",
        lambda **kwargs: seen.append(kwargs["date_partition"])
        or [{"name": "sym", "spark_type": "StringType", "q_type": "s"}],
    )
    connector = KxKdbLakeflowConnect(
        {"hdb_root_path": str(tmp_path), "license_volume_path": "/tmp/lic"}
    )

    connector.get_table_schema("trades", {"start_date": "2024.01.02"})

    assert seen == ["2024.01.03"]


def test_absent_table_partition_yields_nothing_without_warning(monkeypatch, tmp_path, caplog):
    (tmp_path / "sym").write_bytes(b"stub")
    (tmp_path / "2024.01.01").mkdir()
    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: pytest.fail("no q for absent table"))

    with caplog.at_level("WARNING"):
        records = _read_records(tmp_path, monkeypatch, _ChunkKx(row_count=1))

    assert records == []
    assert not caplog.records


def test_bearer_token_with_trailing_newline_is_rejected(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    monkeypatch.setattr(runtime_mod.subprocess, "run", _Recorder([]))

    with pytest.raises(ValueError, match="bearer token"):
        runtime_mod._install_kdbx(_online_config(tmp_path, installer_bearer_token="abc\n"))


def test_installer_environment_is_allowlisted(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    monkeypatch.setenv("DATABRICKS_TOKEN", "workspace-token")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:3128")
    recorder = _Recorder([(0, "", ""), (0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    runtime_mod._install_kdbx(_online_config(tmp_path))

    install_env = recorder.calls[1]["env"]
    assert "DATABRICKS_TOKEN" not in install_env
    assert install_env["HTTPS_PROXY"] == "http://proxy:3128"
    assert install_env["HOME"] == str(tmp_path / "home")
    assert install_env["TERM"] == "dumb"


# ---------------------------------------------------------------------------
# Options merged from pipeline table configuration (managed ingestion path)
# ---------------------------------------------------------------------------


def _merged_options(tmp_path, **table_options):
    (tmp_path / "hdb" / "2024.01.01" / "TRADES").mkdir(parents=True, exist_ok=True)
    keys = tmp_path / "vol" / "keys"
    keys.mkdir(parents=True, exist_ok=True)
    (keys / "kc.lic").write_bytes(b"license")
    return {
        "hdb_root_path": str(tmp_path / "hdb"),
        "license_volume_path": str(keys),
        "kdbx_license_file_path": str(keys / "kc.lic"),
        **table_options,
    }


def test_pipeline_supplied_package_spec_is_ignored(monkeypatch, tmp_path):
    connector = KxKdbLakeflowConnect(
        _merged_options(tmp_path, pykx_install_spec=str(tmp_path / "evil.whl"))
    )
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    recorder = _Recorder([(0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    runtime_mod._ensure_pykx_package(connector.runtime_config)

    assert all("evil.whl" not in arg for arg in recorder.calls[0]["args"])
    assert recorder.calls[0]["args"][-1] == runtime_mod.PYKX_PIP_SPEC


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
