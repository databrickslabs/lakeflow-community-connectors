"""Offline contract tests for the file-backed KX KDB HDB connector.

The setup follows the HL7 v2 Volume precedent. A temporary directory exercises
the production filesystem and offset logic while deterministic fakes replace
only the licensed PyKX schema, symbol-enumeration, and column-read boundaries.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from databricks.labs.community_connector.sources.kx_kdb import KxKdbDataSource
from databricks.labs.community_connector.sources.kx_kdb.kx_kdb import (
    KxKdbLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import (
    SupportsPartitionedStreamTests,
)
from tests.unit.sources.test_suite import LakeflowConnectTests

_PROJECT_ROOT = Path(__file__).parents[4]
_CORPUS_PATH = (
    _PROJECT_ROOT
    / "src"
    / "databricks"
    / "labs"
    / "community_connector"
    / "source_simulator"
    / "specs"
    / "kx_kdb"
    / "corpus"
    / "hdb_records.json"
)

_SCHEMAS = {
    "TRADES": [
        {"name": "date", "spark_type": "StringType"},
        {"name": "sym", "spark_type": "StringType"},
        {"name": "time", "spark_type": "StringType"},
        {"name": "price", "spark_type": "DoubleType"},
        {"name": "size", "spark_type": "LongType"},
    ],
    "QUOTES": [
        {"name": "date", "spark_type": "StringType"},
        {"name": "sym", "spark_type": "StringType"},
        {"name": "time", "spark_type": "StringType"},
        {"name": "bid", "spark_type": "DoubleType"},
        {"name": "ask", "spark_type": "DoubleType"},
        {"name": "bsize", "spark_type": "LongType"},
        {"name": "asize", "spark_type": "LongType"},
    ],
}


class TestKxKdbConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = KxKdbLakeflowConnect
    simulator_source = "kx_kdb"
    replay_config = {
        "hdb_root_path": "/tmp/kx-kdb-ci/hdb",
        "license_volume_path": "/tmp/kx-kdb-ci/keys",
    }
    sample_records = 8

    _fixture_dir: tempfile.TemporaryDirectory | None = None
    _corpus: list[dict] = []
    _patches: list = []

    @classmethod
    def setup_class(cls):
        cls._fixture_dir = tempfile.TemporaryDirectory(prefix="kx-kdb-hdb-")
        fixture_root = Path(cls._fixture_dir.name)
        cls._populate_hdb(fixture_root / "hdb")
        (fixture_root / "keys").mkdir()
        cls._corpus = json.loads(_CORPUS_PATH.read_text(encoding="utf-8"))
        cls._start_pykx_boundary_patches()
        try:
            super().setup_class()
        except Exception:
            cls._stop_pykx_boundary_patches()
            cls._cleanup_fixture()
            raise

    @classmethod
    def teardown_class(cls):
        try:
            super().teardown_class()
        finally:
            cls._stop_pykx_boundary_patches()
            cls._cleanup_fixture()

    @classmethod
    def _replay_config(cls):
        assert cls._fixture_dir is not None
        fixture_root = Path(cls._fixture_dir.name)
        return {
            "hdb_root_path": str(fixture_root / "hdb"),
            "license_volume_path": str(fixture_root / "keys"),
        }

    @classmethod
    def _populate_hdb(cls, hdb_root: Path) -> None:
        hdb_root.mkdir()
        (hdb_root / "sym").write_bytes(b"offline-ci-placeholder")
        for date in ("2024.01.01", "2024.01.02"):
            for table in _SCHEMAS:
                table_dir = hdb_root / date / table
                table_dir.mkdir(parents=True)
                (table_dir / ".d").write_bytes(b"offline-ci-placeholder")

    @classmethod
    def _start_pykx_boundary_patches(cls) -> None:
        cls._patches = [
            patch(
                "databricks.labs.community_connector.sources.kx_kdb.kx_kdb."
                "load_sym_enumeration_with_indices",
                return_value=(["AAPL", "MSFT"], {"AAPL": 0, "MSFT": 1}),
            ),
            patch(
                "databricks.labs.community_connector.sources.kx_kdb.kx_kdb."
                "infer_schema_from_partition",
                side_effect=cls._infer_schema,
            ),
            patch(
                "databricks.labs.community_connector.sources.kx_kdb.kx_kdb."
                "read_kdb_date_sym_records",
                side_effect=cls._read_records,
            ),
        ]
        for active_patch in cls._patches:
            active_patch.start()

    @classmethod
    def _stop_pykx_boundary_patches(cls) -> None:
        for active_patch in reversed(cls._patches):
            active_patch.stop()
        cls._patches = []

    @classmethod
    def _cleanup_fixture(cls) -> None:
        fixture = cls._fixture_dir
        cls._fixture_dir = None
        if fixture is not None:
            fixture.cleanup()

    @staticmethod
    def _infer_schema(**kwargs):
        return list(_SCHEMAS[kwargs["kdb_table_name"].upper()])

    @classmethod
    def _read_records(cls, **kwargs):
        table = kwargs["kdb_table_name"].upper()
        date = kwargs["date_partition"]
        symbol = kwargs["symbol"]
        return iter(
            [
                {key: value for key, value in row.items() if key != "table"}
                for row in cls._corpus
                if row["table"] == table
                and row["date"] == date
                and row["sym"] == symbol
            ]
        )

    def test_offline_fixture_covers_two_tables_dates_and_symbols(self):
        assert len(self._corpus) == 8
        assert {row["table"] for row in self._corpus} == {"TRADES", "QUOTES"}
        assert {row["date"] for row in self._corpus} == {
            "2024.01.01",
            "2024.01.02",
        }
        assert {row["sym"] for row in self._corpus} == {"AAPL", "MSFT"}

    def test_source_package_binds_connector_to_lakeflow_data_source(self):
        assert KxKdbDataSource._lakeflow_connect_cls is KxKdbLakeflowConnect
        assert KxKdbDataSource.name() == "lakeflow_connect"
