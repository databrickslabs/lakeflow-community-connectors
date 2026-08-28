"""Tests for simulate mode: endpoint_spec + corpus + pagination + handler + Simulator wiring."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from databricks.labs.community_connector.source_simulator import (
    MODE_SIMULATE,
    CorpusStore,
    EndpointSpec,
    Simulator,
    UnknownEndpoint,
    load_specs,
    match_endpoint,
)
from databricks.labs.community_connector.source_simulator.corpus import (
    apply_filters,
    apply_sort,
    get_field,
    slice_page,
)
from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    FilterOp,
    FilterParam,
)
from databricks.labs.community_connector.source_simulator.pagination import (
    NonePagination,
    OffsetLimit,
    PageNumber,
    PageNumberWithLinkHeader,
    get_style,
)


# ---------------------------------------------------------------------------
# endpoint_spec.py
# ---------------------------------------------------------------------------


class TestEndpointSpec:
    def _write(self, tmp_path: Path, yaml_text: str) -> Path:
        p = tmp_path / "endpoints.yaml"
        p.write_text(yaml_text)
        return p

    def test_load_minimal(self, tmp_path: Path):
        p = self._write(
            tmp_path,
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
""",
        )
        specs = load_specs(p)
        assert len(specs) == 1
        s = specs[0]
        assert s.method == "GET"
        assert s.path == "/things"
        assert s.corpus == "things"
        assert s.response.pagination_style == "none"
        assert s.response.single_entity is False

    def test_load_with_params(self, tmp_path: Path):
        p = self._write(
            tmp_path,
            """
endpoints:
  - path: "/repos/{owner}/{repo}/commits"
    method: GET
    corpus: commits
    response:
      pagination_style: page_number
      default_sort: "commit.author.date desc"
    params:
      since:    {role: filter,   field: commit.author.date, op: gte}
      author:   {role: filter,   field: author.login,        op: eq}
      sort:     {role: sort_by,  options: [author, date]}
      page:     {role: page}
      per_page: {role: per_page, default: 30, max: 100}
""",
        )
        s = load_specs(p)[0]
        assert len(s.filters) == 2
        names = [f.name for f in s.filters]
        assert names == ["since", "author"]
        assert s.filters[0].op == FilterOp.GTE
        assert s.sort_by is not None
        assert s.sort_by.options == ("author", "date")
        assert s.page is not None
        assert s.per_page is not None
        assert s.per_page.default == 30
        assert s.per_page.max == 100
        assert s.response.default_sort == ("commit.author.date", "desc")

    def test_invalid_role_raises(self, tmp_path: Path):
        p = self._write(
            tmp_path,
            """
endpoints:
  - path: "/x"
    method: GET
    corpus: x
    params:
      foo: {role: bogus}
""",
        )
        with pytest.raises(ValueError, match="unknown role"):
            load_specs(p)

    def test_filter_requires_field(self, tmp_path: Path):
        p = self._write(
            tmp_path,
            """
endpoints:
  - path: "/x"
    method: GET
    corpus: x
    params:
      foo: {role: filter, op: eq}
""",
        )
        with pytest.raises(ValueError, match="filter requires 'field'"):
            load_specs(p)

    def test_match_endpoint_extracts_path_params(self, tmp_path: Path):
        p = self._write(
            tmp_path,
            """
endpoints:
  - path: "/repos/{owner}/{repo}/commits"
    method: GET
    corpus: commits
""",
        )
        specs = load_specs(p)
        match = match_endpoint(specs, "GET", "https://api.example/repos/alice/proj/commits?since=2024")
        assert match is not None
        spec, params = match
        assert spec.corpus == "commits"
        assert params == {"owner": "alice", "repo": "proj"}

    def test_match_endpoint_no_match(self, tmp_path: Path):
        p = self._write(
            tmp_path,
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
""",
        )
        specs = load_specs(p)
        assert match_endpoint(specs, "GET", "https://x/other") is None
        assert match_endpoint(specs, "POST", "https://x/things") is None  # method mismatch


# ---------------------------------------------------------------------------
# corpus.py — query pipeline
# ---------------------------------------------------------------------------


class TestCorpusQueryPipeline:
    def test_get_field_dotted_path(self):
        rec = {"author": {"login": "alice", "id": 7}}
        assert get_field(rec, "author.login") == "alice"
        assert get_field(rec, "author.id") == 7
        assert get_field(rec, "author.missing") is None
        assert get_field(rec, "missing.x") is None

    def test_filter_eq(self):
        records = [{"id": 1, "state": "open"}, {"id": 2, "state": "closed"}]
        flt = [FilterParam(name="state", field="state", op=FilterOp.EQ)]
        out = apply_filters(records, flt, {"state": "open"})
        assert len(out) == 1 and out[0]["id"] == 1

    def test_filter_gte_iso_timestamp(self):
        records = [
            {"updated_at": "2024-01-01T00:00:00Z"},
            {"updated_at": "2024-06-01T00:00:00Z"},
        ]
        flt = [FilterParam(name="since", field="updated_at", op=FilterOp.GTE)]
        out = apply_filters(records, flt, {"since": "2024-03-01T00:00:00Z"})
        assert len(out) == 1
        assert out[0]["updated_at"].startswith("2024-06")

    def test_filter_in(self):
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        flt = [FilterParam(name="ids", field="id", op=FilterOp.IN)]
        out = apply_filters(records, flt, {"ids": "1,3"})
        assert {r["id"] for r in out} == {1, 3}

    def test_filter_skipped_when_param_absent(self):
        records = [{"id": 1, "state": "open"}, {"id": 2, "state": "closed"}]
        flt = [FilterParam(name="state", field="state", op=FilterOp.EQ)]
        out = apply_filters(records, flt, {})  # no state param given
        assert len(out) == 2

    def test_sort_asc_desc(self):
        records = [{"id": 2}, {"id": 1}, {"id": 3}]
        asc = apply_sort(records, sort_field="id", sort_order="asc")
        desc = apply_sort(records, sort_field="id", sort_order="desc")
        assert [r["id"] for r in asc] == [1, 2, 3]
        assert [r["id"] for r in desc] == [3, 2, 1]

    def test_sort_dotted_path(self):
        records = [
            {"meta": {"updated_at": "2024-06"}},
            {"meta": {"updated_at": "2024-01"}},
        ]
        out = apply_sort(records, sort_field="meta.updated_at", sort_order="asc")
        assert out[0]["meta"]["updated_at"] == "2024-01"

    def test_slice_page(self):
        records = [{"i": i} for i in range(10)]
        assert [r["i"] for r in slice_page(records, 0, 3)] == [0, 1, 2]
        assert [r["i"] for r in slice_page(records, 3, 3)] == [3, 4, 5]
        assert slice_page(records, 100, 3) == []


class TestCorpusStore:
    def test_load_directory(self, tmp_path: Path):
        d = tmp_path / "corpus"
        d.mkdir()
        (d / "issues.json").write_text('[{"id": 1}, {"id": 2}]')
        (d / "user.json").write_text('{"login": "alice"}')

        store = CorpusStore.load(d)
        assert len(store.tables) == 2
        assert store.get("issues") == [{"id": 1}, {"id": 2}]
        assert store.get("user") == {"login": "alice"}
        assert store.get("missing") is None

    def test_load_missing_dir(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            CorpusStore.load(tmp_path / "nope")


# ---------------------------------------------------------------------------
# pagination.py
# ---------------------------------------------------------------------------


class TestPaginationStyles:
    def test_none(self):
        style = NonePagination()
        offset, limit = style.parse_request({}, {}, _empty_spec())
        assert offset == 0 and limit > 0
        body, headers = style.render_response(
            page=[{"a": 1}], total=1, offset=0, limit=10, request_url="https://x"
        )
        assert body == [{"a": 1}]
        assert headers == {}

    def test_page_number(self):
        style = PageNumber()
        spec = _spec_with_per_page()
        offset, limit = style.parse_request({"page": "2", "per_page": "10"}, {}, spec)
        assert (offset, limit) == (10, 10)

    def test_page_number_default(self):
        style = PageNumber()
        spec = _spec_with_per_page()
        offset, limit = style.parse_request({}, {}, spec)
        assert (offset, limit) == (0, 30)

    def test_page_number_with_link_header_emits_next_and_last(self):
        style = PageNumberWithLinkHeader()
        body, headers = style.render_response(
            page=[{"i": i} for i in range(10)],
            total=25,
            offset=0,
            limit=10,
            request_url="https://api.x/things?per_page=10",
        )
        assert "Link" in headers
        assert 'rel="next"' in headers["Link"]
        assert 'rel="last"' in headers["Link"]
        assert "page=2" in headers["Link"]
        assert "page=3" in headers["Link"]  # last page index for 25/10

    def test_page_number_with_link_header_no_more_pages(self):
        style = PageNumberWithLinkHeader()
        body, headers = style.render_response(
            page=[{"i": 0}],
            total=1,
            offset=0,
            limit=10,
            request_url="https://api.x/things",
        )
        # Single-page result: no Link header at all.
        assert headers == {}

    def test_offset_limit(self):
        style = OffsetLimit()
        spec = _spec_with_offset_limit()
        offset, limit = style.parse_request({"offset": "5", "limit": "20"}, {}, spec)
        assert (offset, limit) == (5, 20)

    def test_get_style_unknown(self):
        with pytest.raises(ValueError, match="Unknown pagination_style"):
            get_style("nonsense")


def _empty_spec() -> EndpointSpec:
    from databricks.labs.community_connector.source_simulator.endpoint_spec import (
        ResponseShape,
    )
    import re

    return EndpointSpec(
        method="GET",
        path="/x",
        path_regex=re.compile("^/x$"),
        corpus="x",
        response=ResponseShape(),
    )


def _spec_with_per_page() -> EndpointSpec:
    from databricks.labs.community_connector.source_simulator.endpoint_spec import (
        PageParam,
        PerPageParam,
    )

    spec = _empty_spec()
    spec.page = PageParam(name="page")
    spec.per_page = PerPageParam(name="per_page", default=30, max=100)
    return spec


def _spec_with_offset_limit() -> EndpointSpec:
    from databricks.labs.community_connector.source_simulator.endpoint_spec import (
        LimitParam,
        OffsetParam,
    )

    spec = _empty_spec()
    spec.offset = OffsetParam(name="offset")
    spec.limit = LimitParam(name="limit", default=20, max=100)
    return spec


# ---------------------------------------------------------------------------
# Simulator end-to-end in MODE_SIMULATE
# ---------------------------------------------------------------------------


class TestSimulateModeEndToEnd:
    """Drive Simulator(MODE_SIMULATE) with a small spec + corpus and exercise
    the full request → response path."""

    def _setup(self, tmp_path: Path) -> tuple[Path, Path]:
        spec_path = tmp_path / "endpoints.yaml"
        corpus_dir = tmp_path / "corpus"
        corpus_dir.mkdir()

        spec_path.write_text(
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
    response:
      pagination_style: page_number_with_link_header
      default_sort: "id asc"
    params:
      state:    {role: filter,   field: state, op: eq}
      since:    {role: filter,   field: updated_at, op: gte}
      page:     {role: page}
      per_page: {role: per_page, default: 5, max: 100}

  - path: "/me"
    method: GET
    corpus: user
    response:
      single_entity: true
"""
        )
        (corpus_dir / "things.json").write_text(
            json.dumps(
                [
                    {"id": i, "state": "open" if i % 2 else "closed", "updated_at": f"2024-{i:02d}-01T00:00:00Z"}
                    for i in range(1, 13)
                ]
            )
        )
        (corpus_dir / "user.json").write_text(
            json.dumps({"login": "alice", "id": 42})
        )
        return spec_path, corpus_dir

    def test_basic_list_with_default_pagination(self, tmp_path: Path):
        spec_path, corpus_dir = self._setup(tmp_path)
        with Simulator(mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir):
            resp = requests.get("https://api.example/things")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 5  # default per_page
        assert body[0]["id"] == 1  # asc sort
        # Multi-page response includes Link with rel="next"
        assert "Link" in resp.headers
        assert 'rel="next"' in resp.headers["Link"]

    def test_pagination_walks_all_pages(self, tmp_path: Path):
        spec_path, corpus_dir = self._setup(tmp_path)
        with Simulator(mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir):
            page1 = requests.get("https://api.example/things?per_page=5&page=1").json()
            page2 = requests.get("https://api.example/things?per_page=5&page=2").json()
            page3 = requests.get("https://api.example/things?per_page=5&page=3").json()
        assert len(page1) == 5 and page1[0]["id"] == 1
        assert len(page2) == 5 and page2[0]["id"] == 6
        assert len(page3) == 2 and page3[0]["id"] == 11  # remainder

    def test_filter_eq(self, tmp_path: Path):
        spec_path, corpus_dir = self._setup(tmp_path)
        with Simulator(
            mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir
        ):
            resp = requests.get(
                "https://api.example/things?state=open&per_page=100"
            ).json()
        assert all(r["state"] == "open" for r in resp)

    def test_filter_since_gte(self, tmp_path: Path):
        spec_path, corpus_dir = self._setup(tmp_path)
        with Simulator(
            mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir
        ):
            resp = requests.get(
                "https://api.example/things?since=2024-06-01T00:00:00Z&per_page=100"
            ).json()
        for r in resp:
            assert r["updated_at"] >= "2024-06-01T00:00:00Z"

    def test_single_entity_endpoint(self, tmp_path: Path):
        spec_path, corpus_dir = self._setup(tmp_path)
        with Simulator(
            mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir
        ):
            resp = requests.get("https://api.example/me").json()
        assert resp == {"login": "alice", "id": 42}

    def test_unknown_endpoint_raises(self, tmp_path: Path):
        spec_path, corpus_dir = self._setup(tmp_path)
        with Simulator(
            mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir
        ):
            with pytest.raises(UnknownEndpoint):
                requests.get("https://api.example/nope")

    def test_strict_params_rejects_unknown_query_param(self, tmp_path: Path):
        """When ``strict_params: true``, an unknown query param produces 400."""
        spec_path = tmp_path / "endpoints.yaml"
        corpus_dir = tmp_path / "corpus"
        corpus_dir.mkdir()
        spec_path.write_text(
            """
endpoints:
  - path: "/things"
    method: GET
    corpus: things
    strict_params: true
    response:
      pagination_style: none
    params:
      page:     {role: page}
      per_page: {role: per_page, default: 5, max: 100}
"""
        )
        (corpus_dir / "things.json").write_text('[{"id": 1}]')

        with Simulator(
            mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir
        ):
            ok = requests.get("https://api.example/things?page=1")
            bad = requests.get("https://api.example/things?bogus=value")
        assert ok.status_code == 200
        assert bad.status_code == 400
        assert "Unknown query parameter" in bad.json()["error"]
        assert "bogus" in bad.json()["error"]

    def test_strict_params_off_by_default_accepts_extras(self, tmp_path: Path):
        """Default (strict_params off) ignores unknown params silently."""
        spec_path, corpus_dir = self._setup(tmp_path)
        with Simulator(
            mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir
        ):
            resp = requests.get(
                "https://api.example/things?total_random_param=1"
            )
        assert resp.status_code == 200
