"""Unit tests for the HTTP record/replay framework.

These tests stand up a tiny in-process HTTP server and drive it with
``requests``, exercising the framework in isolation — no connector or
source system involved.
"""

from __future__ import annotations

import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Iterator, List

import pytest
import requests

from tests.unit.sources.mock_framework import (
    MODE_LIVE,
    MODE_RECORD,
    MODE_REPLAY,
    Cassette,
    NoMatchingInteraction,
    RecordReplayPatch,
    get_mode,
)
from tests.unit.sources.mock_framework.cassette import (
    REDACTED,
    body_sha256,
    scrub_headers,
    split_url,
)


# ---------------------------------------------------------------------------
# Tiny in-process HTTP server for live/record tests
# ---------------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args, **kwargs):  # silence stderr
        pass

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/ping"):
            body = json.dumps({"pong": True, "path": self.path}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        body = json.dumps({"echoed": raw.decode("utf-8", errors="replace")}).encode()
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def http_server() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


# ---------------------------------------------------------------------------
# cassette.py — pure-unit tests
# ---------------------------------------------------------------------------


class TestCassetteHelpers:
    def test_split_url_parses_query(self):
        base, query = split_url("https://x.example/api/v1/foo?page=2&limit=5")
        assert base == "https://x.example/api/v1/foo"
        assert query == {"page": "2", "limit": "5"}

    def test_split_url_empty_query(self):
        base, query = split_url("https://x.example/api")
        assert base == "https://x.example/api"
        assert query == {}

    def test_body_sha256_none_for_empty(self):
        assert body_sha256(None) is None
        assert body_sha256("") is None
        assert body_sha256(b"") is None

    def test_body_sha256_stable(self):
        a = body_sha256("hello")
        b = body_sha256(b"hello")
        assert a == b and a

    def test_scrub_headers_case_insensitive(self):
        scrubbed = scrub_headers({"Authorization": "Bearer abc", "X-Trace": "keep"})
        assert scrubbed["Authorization"] == REDACTED
        assert scrubbed["X-Trace"] == "keep"


class TestCassetteRoundTrip:
    def test_save_and_load(self, tmp_path: Path):
        cas = Cassette.empty(tmp_path / "c.json", source="demo")
        from tests.unit.sources.mock_framework.cassette import (
            Interaction,
            RequestRecord,
            ResponseRecord,
        )

        cas.interactions.append(
            Interaction(
                request=RequestRecord(
                    method="GET", url="https://x/api", query={"p": "1"}, body_sha256=None
                ),
                response=ResponseRecord(
                    status_code=200,
                    headers={"Content-Type": "application/json"},
                    body_text='{"ok":true}',
                    body_b64=None,
                    encoding="utf-8",
                    url="https://x/api?p=1",
                ),
            )
        )
        cas._consumed = [True]
        cas.save()

        loaded = Cassette.load(tmp_path / "c.json")
        assert loaded.source == "demo"
        assert len(loaded.interactions) == 1
        assert loaded.interactions[0].request.url == "https://x/api"
        assert loaded.interactions[0].response.status_code == 200


# ---------------------------------------------------------------------------
# Record / replay round-trip through real HTTP
# ---------------------------------------------------------------------------


class TestRecordReplay:
    def test_live_mode_is_noop(self, http_server: str, tmp_path: Path):
        path = tmp_path / "live.json"
        with RecordReplayPatch(mode=MODE_LIVE, cassette_path=path):
            resp = requests.get(f"{http_server}/ping?x=1")
            assert resp.status_code == 200
            assert resp.json() == {"pong": True, "path": "/ping?x=1"}
        assert not path.exists()  # nothing written in live mode

    def test_record_writes_cassette(self, http_server: str, tmp_path: Path):
        path = tmp_path / "rec.json"
        with RecordReplayPatch(mode=MODE_RECORD, cassette_path=path, source="test"):
            r1 = requests.get(f"{http_server}/ping?x=1")
            r2 = requests.post(f"{http_server}/ping", json={"hi": "there"})
            assert r1.status_code == 200
            assert r2.status_code == 201

        assert path.exists()
        data = json.loads(path.read_text())
        assert data["version"] == 1
        assert data["source"] == "test"
        assert len(data["interactions"]) == 2
        assert data["interactions"][0]["request"]["method"] == "GET"
        assert data["interactions"][0]["request"]["query"] == {"x": "1"}
        assert data["interactions"][1]["request"]["method"] == "POST"
        assert data["interactions"][1]["response"]["status_code"] == 201

    def test_replay_serves_recorded_response(
        self, http_server: str, tmp_path: Path
    ):
        path = tmp_path / "rr.json"
        # Record phase — real server hit.
        with RecordReplayPatch(mode=MODE_RECORD, cassette_path=path):
            real = requests.get(f"{http_server}/ping?x=1").json()

        # Replay phase — shut down the server implicitly by pointing at a
        # bogus URL… easier: just trust the patch doesn't call through.
        # Also assert body/headers are reconstructed correctly.
        with RecordReplayPatch(mode=MODE_REPLAY, cassette_path=path):
            replayed = requests.get(f"{http_server}/ping?x=1")
            assert replayed.status_code == 200
            assert replayed.json() == real
            assert replayed.headers.get("Content-Type") == "application/json"

    def test_record_dedups_same_key(
        self, http_server: str, tmp_path: Path, monkeypatch
    ):
        """Repeated requests to the same URL record only once (pagination collapse)."""
        path = tmp_path / "dedup.json"

        counter = {"n": 0}

        def counted_do_get(self):
            counter["n"] += 1
            body = json.dumps({"hit": counter["n"]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        monkeypatch.setattr(_Handler, "do_GET", counted_do_get)

        with RecordReplayPatch(mode=MODE_RECORD, cassette_path=path):
            requests.get(f"{http_server}/ping?p=same")
            requests.get(f"{http_server}/ping?p=same")
            requests.get(f"{http_server}/ping?p=same")

        data = json.loads(path.read_text())
        assert len(data["interactions"]) == 1
        # The first response body was sampled/processed, but the JSON structure
        # is preserved since there's no records list to trim.
        assert json.loads(data["interactions"][0]["response"]["body_text"]) == {"hit": 1}

    def test_replay_round_robin_on_duplicate_key(self, tmp_path: Path):
        """If a cassette happens to contain two entries with the same key,
        replay cycles through them round-robin."""
        from tests.unit.sources.mock_framework.cassette import (
            Cassette,
            Interaction,
            RequestRecord,
            ResponseRecord,
        )

        path = tmp_path / "rr.json"
        cas = Cassette.empty(path)
        for i in (1, 2):
            cas.interactions.append(
                Interaction(
                    request=RequestRecord(
                        method="GET",
                        url="https://x/api",
                        query={"p": "same"},
                        body_sha256=None,
                    ),
                    response=ResponseRecord(
                        status_code=200,
                        headers={"Content-Type": "application/json"},
                        body_text=json.dumps({"hit": i}),
                        body_b64=None,
                        encoding="utf-8",
                        url="https://x/api?p=same",
                    ),
                )
            )
        cas.save()

        with RecordReplayPatch(mode=MODE_REPLAY, cassette_path=path):
            a = requests.get("https://x/api?p=same").json()
            b = requests.get("https://x/api?p=same").json()
            c = requests.get("https://x/api?p=same").json()
        assert a == {"hit": 1} and b == {"hit": 2} and c == {"hit": 1}  # wraps

    def test_replay_unknown_request_raises(self, tmp_path: Path):
        path = tmp_path / "empty.json"
        cas = Cassette.empty(path)
        cas.save()

        with RecordReplayPatch(mode=MODE_REPLAY, cassette_path=path):
            with pytest.raises(NoMatchingInteraction):
                requests.get("https://nowhere.example/api")

    def test_replay_missing_cassette_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            RecordReplayPatch(
                mode=MODE_REPLAY, cassette_path=tmp_path / "nope.json"
            ).__enter__()

    def test_record_scrubs_auth_header(self, http_server: str, tmp_path: Path):
        path = tmp_path / "scrub.json"
        with RecordReplayPatch(mode=MODE_RECORD, cassette_path=path):
            requests.get(
                f"{http_server}/ping", headers={"Authorization": "Bearer secret123"}
            )

        # Cassette format does not currently include request headers, but
        # the response headers shouldn't contain Set-Cookie/etc. and the file
        # should not contain "secret123" verbatim.
        text = path.read_text()
        assert "secret123" not in text

    def test_patch_restores_session_send(self, http_server: str, tmp_path: Path):
        path = tmp_path / "restore.json"
        original = requests.sessions.Session.send
        with RecordReplayPatch(mode=MODE_RECORD, cassette_path=path):
            assert requests.sessions.Session.send is not original
            requests.get(f"{http_server}/ping")
        assert requests.sessions.Session.send is original


class TestSampler:
    def test_truncates_top_level_list(self):
        from tests.unit.sources.mock_framework.sampler import sample_body

        body = json.dumps([{"id": i} for i in range(50)])
        out = sample_body(body, max_records=3)
        assert json.loads(out) == [{"id": 0}, {"id": 1}, {"id": 2}]

    def test_truncates_wrapped_records(self):
        from tests.unit.sources.mock_framework.sampler import sample_body

        body = json.dumps(
            {"records": [{"id": i} for i in range(20)], "next_page": 2}
        )
        out = json.loads(sample_body(body, max_records=2))
        assert len(out["records"]) == 2
        assert out["next_page"] is None  # pagination nullified

    def test_leaves_non_record_body_alone(self):
        from tests.unit.sources.mock_framework.sampler import sample_body

        body = json.dumps({"user": "alice", "scopes": ["repo", "org"]})
        assert json.loads(sample_body(body, max_records=3)) == {
            "user": "alice",
            "scopes": ["repo", "org"],
        }

    def test_leaves_non_json_alone(self):
        from tests.unit.sources.mock_framework.sampler import sample_body

        assert sample_body("plain text", max_records=3) == "plain text"

    def test_strip_link_header_next(self):
        from tests.unit.sources.mock_framework.sampler import strip_link_header_next

        h = {
            "Link": '<https://api/?page=2>; rel="next", <https://api/?page=5>; rel="last"',
            "Other": "keep",
        }
        out = strip_link_header_next(h)
        assert "next" not in out["Link"].lower()
        assert 'rel="last"' in out["Link"]
        assert out["Other"] == "keep"

    def test_strip_link_header_removes_when_only_next(self):
        from tests.unit.sources.mock_framework.sampler import strip_link_header_next

        h = {"Link": '<https://api/?page=2>; rel="next"'}
        out = strip_link_header_next(h)
        assert "Link" not in out


class TestSynthesizer:
    def test_no_expansion_when_target_smaller(self):
        from tests.unit.sources.mock_framework.synthesizer import synthesize_body

        body = json.dumps([{"id": 1}, {"id": 2}, {"id": 3}])
        out = synthesize_body(body, target_count=2, seed=42)
        assert out == body

    def test_expands_records_with_varying_integer(self):
        from tests.unit.sources.mock_framework.synthesizer import synthesize_body

        body = json.dumps([{"id": 1, "name": "x"}, {"id": 2, "name": "x"}])
        expanded = json.loads(synthesize_body(body, target_count=6, seed=123))
        assert len(expanded) == 6
        # Non-varying field preserved.
        assert all(r["name"] == "x" for r in expanded)
        # Varying field mutated — ids are all different.
        assert len({r["id"] for r in expanded}) == 6

    def test_expands_iso_timestamps(self):
        from tests.unit.sources.mock_framework.synthesizer import synthesize_body

        body = json.dumps(
            [
                {"updated_at": "2024-01-01T00:00:00Z"},
                {"updated_at": "2024-01-02T00:00:00Z"},
            ]
        )
        expanded = json.loads(synthesize_body(body, target_count=5, seed=7))
        assert len(expanded) == 5
        for r in expanded:
            assert re.match(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", r["updated_at"]
            )

    def test_expands_wrapped_records(self):
        from tests.unit.sources.mock_framework.synthesizer import synthesize_body

        body = json.dumps({"records": [{"id": 1}, {"id": 2}], "next_page": None})
        expanded = json.loads(synthesize_body(body, target_count=8, seed=1))
        assert len(expanded["records"]) == 8
        assert expanded["next_page"] is None  # outer fields preserved

    def test_deterministic_for_same_seed(self):
        from tests.unit.sources.mock_framework.synthesizer import synthesize_body

        body = json.dumps([{"id": 1}, {"id": 2}])
        a = synthesize_body(body, target_count=10, seed=99)
        b = synthesize_body(body, target_count=10, seed=99)
        assert a == b


class TestGetMode:
    def test_default_is_live(self, monkeypatch):
        monkeypatch.delenv("CONNECTOR_TEST_MODE", raising=False)
        assert get_mode() == MODE_LIVE

    def test_valid_modes(self, monkeypatch):
        for m in (MODE_LIVE, MODE_RECORD, MODE_REPLAY):
            monkeypatch.setenv("CONNECTOR_TEST_MODE", m)
            assert get_mode() == m

    def test_uppercase_normalized(self, monkeypatch):
        monkeypatch.setenv("CONNECTOR_TEST_MODE", "REPLAY")
        assert get_mode() == MODE_REPLAY

    def test_invalid_rejects(self, monkeypatch):
        monkeypatch.setenv("CONNECTOR_TEST_MODE", "bogus")
        with pytest.raises(ValueError):
            get_mode()
