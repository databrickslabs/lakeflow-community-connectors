"""Tests for the cassette_to_corpus extractor."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from databricks.labs.community_connector.source_simulator.cassette import (
    Cassette,
    Interaction,
    RequestRecord,
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    load_specs,
)
from databricks.labs.community_connector.source_simulator.tools.cassette_to_corpus import (
    extract_corpus,
    main,
    write_corpus,
)


def _cassette_with(interactions: list[tuple[str, str, dict, dict]], path: Path) -> Path:
    """Build a cassette file from (method, url, query, response_body) tuples."""
    cas = Cassette.empty(path, source="test")
    for method, url, query, body in interactions:
        cas.interactions.append(
            Interaction(
                request=RequestRecord(method=method, url=url, query=query, body_sha256=None),
                response=ResponseRecord(
                    status_code=200,
                    headers={"Content-Type": "application/json"},
                    body_text=json.dumps(body),
                    body_b64=None,
                    encoding="utf-8",
                    url=None,
                ),
            )
        )
    cas.save()
    return path


def _spec(path: Path, yaml_text: str) -> Path:
    path.write_text(yaml_text)
    return path


class TestExtractCorpus:
    def test_extracts_array_records(self, tmp_path: Path):
        cassette = _cassette_with(
            [
                (
                    "GET",
                    "https://api.example/things",
                    {"page": "1"},
                    [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                ),
                (
                    "GET",
                    "https://api.example/things",
                    {"page": "2"},
                    [{"id": 3, "name": "c"}],
                ),
            ],
            tmp_path / "cassette.json",
        )
        spec_path = _spec(
            tmp_path / "endpoints.yaml",
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
""",
        )
        cas = Cassette.load(cassette)
        out = extract_corpus(cas, load_specs(spec_path))
        assert "things" in out
        assert {r["id"] for r in out["things"]} == {1, 2, 3}

    def test_dedups_by_eq_filter_field(self, tmp_path: Path):
        cassette = _cassette_with(
            [
                ("GET", "https://api.example/things", {}, [{"id": 1, "v": "a"}]),
                ("GET", "https://api.example/things", {}, [{"id": 1, "v": "a"}]),
            ],
            tmp_path / "cassette.json",
        )
        spec_path = _spec(
            tmp_path / "endpoints.yaml",
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
    params:
      id: {role: filter, field: id, op: eq}
""",
        )
        cas = Cassette.load(cassette)
        out = extract_corpus(cas, load_specs(spec_path))
        assert len(out["things"]) == 1

    def test_dedups_by_id_default_when_no_pk_filter(self, tmp_path: Path):
        cassette = _cassette_with(
            [
                ("GET", "https://api.example/things", {}, [{"id": 5, "v": "a"}]),
                ("GET", "https://api.example/things", {}, [{"id": 5, "v": "a"}]),
            ],
            tmp_path / "cassette.json",
        )
        spec_path = _spec(
            tmp_path / "endpoints.yaml",
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
""",
        )
        cas = Cassette.load(cassette)
        out = extract_corpus(cas, load_specs(spec_path))
        assert len(out["things"]) == 1

    def test_single_entity(self, tmp_path: Path):
        cassette = _cassette_with(
            [("GET", "https://api.example/me", {}, {"login": "alice", "id": 7})],
            tmp_path / "cassette.json",
        )
        spec_path = _spec(
            tmp_path / "endpoints.yaml",
            """
endpoints:
  - path: "/me"
    method: GET
    corpus: user
    response:
      single_entity: true
""",
        )
        cas = Cassette.load(cassette)
        out = extract_corpus(cas, load_specs(spec_path))
        assert out["user"] == {"login": "alice", "id": 7}

    def test_skips_unmatched_endpoints(self, tmp_path: Path):
        cassette = _cassette_with(
            [
                ("GET", "https://api.example/things", {}, [{"id": 1}]),
                ("GET", "https://api.example/other", {}, [{"id": 2}]),
            ],
            tmp_path / "cassette.json",
        )
        spec_path = _spec(
            tmp_path / "endpoints.yaml",
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
""",
        )
        cas = Cassette.load(cassette)
        out = extract_corpus(cas, load_specs(spec_path))
        assert "things" in out
        assert "other" not in out


class TestWriteAndCli:
    def test_write_corpus(self, tmp_path: Path):
        out_dir = tmp_path / "corpus"
        written = write_corpus(
            {"things": [{"id": 1}], "user": {"login": "alice"}}, out_dir
        )
        assert len(written) == 2
        assert json.loads((out_dir / "things.json").read_text()) == [{"id": 1}]
        assert json.loads((out_dir / "user.json").read_text()) == {"login": "alice"}

    def test_cli_end_to_end(self, tmp_path: Path, capsys):
        cassette = _cassette_with(
            [("GET", "https://api.example/things", {}, [{"id": 1}, {"id": 2}])],
            tmp_path / "cassette.json",
        )
        spec_path = _spec(
            tmp_path / "endpoints.yaml",
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
""",
        )
        out_dir = tmp_path / "out"
        rc = main(
            [
                "--cassette", str(cassette),
                "--spec", str(spec_path),
                "--output", str(out_dir),
            ]
        )
        assert rc == 0
        captured = capsys.readouterr()
        assert "things.json" in captured.out
        assert (out_dir / "things.json").exists()
