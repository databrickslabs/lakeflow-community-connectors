"""End-to-end: drive a minimal HTTP-based connector through the test harness.

This proves two things:

1. ``LakeflowConnectTests`` activates record/replay automatically — connector
   code requires **zero** changes; only the env var flips behavior.
2. Cassettes recorded from a real source are sufficient to re-run the full
   test suite offline.

A throw-away in-process HTTP server stands in for a "real" source system.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Iterator, Tuple

import pytest
import requests
from pyspark.sql.types import LongType, StringType, StructField, StructType

from databricks.labs.community_connector.interface import LakeflowConnect


# ---------------------------------------------------------------------------
# A toy "real" source: one table, paginated rows over plain HTTP
# ---------------------------------------------------------------------------


_ROWS = [
    {"id": 1, "name": "alice"},
    {"id": 2, "name": "bob"},
    {"id": 3, "name": "carol"},
    {"id": 4, "name": "dave"},
    {"id": 5, "name": "eve"},
]
_PAGE_SIZE = 2


class _ToySourceHandler(BaseHTTPRequestHandler):
    def log_message(self, *args, **kwargs):
        pass

    def do_GET(self):  # noqa: N802
        # /tables -> {"tables": ["users"]}
        # /tables/users/records?page=N -> {"records":[...], "next_page": N|None}
        if self.path == "/tables":
            return self._json(200, {"tables": ["users"]})

        if self.path.startswith("/tables/users/records"):
            query = self.path.split("?", 1)[1] if "?" in self.path else ""
            params = dict(
                p.split("=", 1) for p in query.split("&") if "=" in p
            )
            page = int(params.get("page", "1"))
            start = (page - 1) * _PAGE_SIZE
            end = start + _PAGE_SIZE
            chunk = _ROWS[start:end]
            next_page = page + 1 if end < len(_ROWS) else None
            return self._json(200, {"records": chunk, "next_page": next_page})

        self.send_response(404)
        self.end_headers()

    def _json(self, status: int, payload: dict):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _ServerHandle:
    def __init__(self, server: HTTPServer, thread: threading.Thread) -> None:
        self.server = server
        self.thread = thread
        self.url = f"http://127.0.0.1:{server.server_port}"
        self.stopped = False

    def stop(self) -> None:
        if not self.stopped:
            self.server.shutdown()
            self.server.server_close()
            self.stopped = True


@pytest.fixture
def toy_server() -> Iterator[_ServerHandle]:
    server = HTTPServer(("127.0.0.1", 0), _ToySourceHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    handle = _ServerHandle(server, thread)
    try:
        yield handle
    finally:
        handle.stop()


# ---------------------------------------------------------------------------
# A minimal connector that uses `requests` — indistinguishable from a real one
# from the framework's perspective.
# ---------------------------------------------------------------------------


class ToyLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict) -> None:
        super().__init__(options)
        self._base = options["base_url"]
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {options.get('token', '')}"})

    def list_tables(self) -> list[str]:
        return self._session.get(f"{self._base}/tables").json()["tables"]

    def get_table_schema(self, table_name: str, table_options: dict) -> StructType:
        if table_name != "users":
            raise ValueError(f"unknown table {table_name!r}")
        return StructType(
            [
                StructField("id", LongType(), nullable=False),
                StructField("name", StringType(), nullable=True),
            ]
        )

    def read_table_metadata(self, table_name: str, table_options: dict) -> dict:
        if table_name != "users":
            raise ValueError(f"unknown table {table_name!r}")
        return {"ingestion_type": "snapshot", "primary_keys": ["id"]}

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict
    ) -> Tuple[iter, dict]:
        if table_name != "users":
            raise ValueError(f"unknown table {table_name!r}")
        records: list[dict] = []
        page = 1
        while True:
            resp = self._session.get(
                f"{self._base}/tables/users/records", params={"page": str(page)}
            )
            body = resp.json()
            records.extend(body["records"])
            if body["next_page"] is None:
                break
            page = body["next_page"]
        return iter(records), {}


# ---------------------------------------------------------------------------
# Drive the full LakeflowConnectTests harness through a subprocess so the
# CONNECTOR_TEST_MODE env var and cassette path take effect cleanly.
# ---------------------------------------------------------------------------


# Written into a tmp directory by _scaffold_connector_test_dir.
_TEST_MODULE = '''\
import os

import requests
from pyspark.sql.types import LongType, StringType, StructField, StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests

from tests.unit.sources.mock_framework.test_harness_integration import ToyLakeflowConnect


class TestToyConnector(LakeflowConnectTests):
    connector_class = ToyLakeflowConnect
    sample_records = 10
'''


def _scaffold_connector_test_dir(
    tmp_path: Path, base_url: str, token: str = "FAKE-TOKEN"
) -> Path:
    """Build a throw-away {tmp}/toy_source/ tree that looks like a real connector test."""
    src = tmp_path / "toy_source"
    (src / "configs").mkdir(parents=True)
    (src / "__init__.py").write_text("")
    (src / "configs" / "dev_config.json").write_text(
        json.dumps({"base_url": base_url, "token": token})
    )
    (src / "test_toy.py").write_text(_TEST_MODULE)
    return src


def _run_pytest(test_dir: Path, mode: str, repo_root: Path) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["CONNECTOR_TEST_MODE"] = mode
    # Make the tmp test module importable under its real path.
    env["PYTHONPATH"] = f"{repo_root}:{env.get('PYTHONPATH', '')}"
    return subprocess.run(
        [sys.executable, "-m", "pytest", str(test_dir), "-q"],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_root),
    )


class TestHarnessIntegration:
    """End-to-end: record a cassette from the toy server, then replay offline."""

    def test_record_then_replay(self, toy_server: _ServerHandle, tmp_path: Path):
        repo_root = Path(__file__).resolve().parents[4]
        assert (repo_root / "pyproject.toml").exists(), f"repo root wrong: {repo_root}"

        test_dir = _scaffold_connector_test_dir(tmp_path, base_url=toy_server.url)

        # 1) Record against the live toy server.
        rec = _run_pytest(test_dir, mode="record", repo_root=repo_root)
        assert rec.returncode == 0, (
            f"record run failed:\nSTDOUT:\n{rec.stdout}\nSTDERR:\n{rec.stderr}"
        )
        cassette = test_dir / "cassettes" / "TestToyConnector.json"
        assert cassette.exists(), "cassette not written during record"
        data = json.loads(cassette.read_text())
        assert data["version"] == 1
        # At minimum: list_tables + 3 pages of records; typically many more.
        assert len(data["interactions"]) >= 4

        # 2) Shut the server down and re-run in replay mode. The connector
        #    code is identical; only the env var flipped. If any request
        #    actually touched the network, the run would fail with a
        #    connection refused.
        toy_server.stop()

        rep = _run_pytest(test_dir, mode="replay", repo_root=repo_root)
        assert rep.returncode == 0, (
            f"replay run (offline) failed:\nSTDOUT:\n{rep.stdout}\nSTDERR:\n{rep.stderr}"
        )
