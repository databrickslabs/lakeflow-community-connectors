"""Opaque live cursors must map to observed synthetic continuations, not be ignored."""

import json
from urllib.parse import parse_qs

import pytest
import requests

from databricks.labs.community_connector.source_simulator import CorpusStore, load_specs
from databricks.labs.community_connector.source_simulator.validator import LiveValidator


def response(next_cursor, extra=False):
    row = {"id": "example"}
    if extra:
        row["relationships"] = {}
    result = requests.Response()
    result.status_code = 200
    result._content = json.dumps({"data": [row], "meta": {"next_cursor": next_cursor}}).encode()
    return result


def request(cursor=None, size=2):
    params = {"page[size]": size}
    params.update({"page[after]": cursor} if cursor else {"page[number]": 1})
    return requests.Request("GET", "https://example.invalid/items", params=params).prepare()


@pytest.fixture
def validator(tmp_path):
    spec = tmp_path / "endpoints.yaml"
    spec.write_text("""
endpoints:
  - path: /items
    method: GET
    corpus: items
    validation_cursor:
      request_param: page[after]
      response_path: meta.next_cursor
    params:
      'page[number]': {role: page}
      'page[size]': {role: per_page}
      'page[after]': {role: ignore}
    response:
      wrapper: {records_key: data}
""")
    return LiveValidator(load_specs(spec), CorpusStore(tmp_path))


def serve(prep):
    cursor = parse_qs(prep.url.split("?", 1)[1]).get("page[after]", [None])[0]
    next_cursor = {None: "synthetic-1", "synthetic-1": "synthetic-2", "synthetic-2": None}[cursor]
    return response(next_cursor)


def test_translates_only_observed_tokens_and_preserves_real_request(validator, monkeypatch):
    monkeypatch.setattr(validator._handler, "handle", serve)
    for current, following in [(None, "opaque+/=one"), ("opaque+/=one", "two"), ("two", None)]:
        prep = request(current)
        original_url = prep.url
        validator.observe(prep, response(following))
        assert prep.url == original_url
    assert validator.to_json()["summary"]["endpoints_validated"] == 1
    assert validator.report()[0].matched == 3


def test_unknown_cursor_fails_without_disclosing_it(validator, monkeypatch):
    monkeypatch.setattr(validator._handler, "handle", serve)
    validator.observe(request("private-unseen-token"), response(None))
    assert validator.report()[0].mismatched == 1
    assert "unobserved" in str(validator.report()[0].issues)
    assert "private-unseen-token" not in str(validator.report()[0].issues)


def test_cursor_mapping_is_scoped_to_query(validator, monkeypatch):
    monkeypatch.setattr(validator._handler, "handle", serve)
    validator.observe(request(size=2), response("one"))
    validator.observe(request("one", size=3), response(None))
    assert validator.report()[0].mismatched == 1


def test_drift_is_still_reported_on_continuation(validator, monkeypatch):
    monkeypatch.setattr(validator._handler, "handle", serve)
    validator.observe(request(), response("one"))
    validator.observe(request("one"), response(None, extra=True))
    assert validator.report()[0].mismatched == 1
    assert "relationships" in str(validator.report()[0].issues)


def test_exhausted_corpus_does_not_silently_reuse_pages(validator, monkeypatch):
    monkeypatch.setattr(validator._handler, "handle", lambda prep: response(None))
    validator.observe(request(), response("one"))
    validator.observe(request("one"), response(None))
    assert validator.report()[0].mismatched == 1
    assert "exhausted" in str(validator.report()[0].issues)


def test_unconfigured_endpoints_keep_existing_behavior(validator, monkeypatch):
    validator.specs[0].validation_cursor = None
    monkeypatch.setattr(validator._handler, "handle", serve)
    validator.observe(request("synthetic-1"), response(None))
    assert validator.report()[0].matched == 1
