"""Unit tests for the expected_status_codes simulator framework feature.

These tests verify that the validator correctly treats an HTTP 404 from
/api/2/export as "validated" (not "mismatched") when the endpoint spec
declares 404 in expected_status_codes.

This is the exact path exercised by the Amplitude Export endpoint:
  - Live returns 404 when the requested time window contains no events.
  - Connector handles it as records = [].
  - Spec corpus always returns 200 with data (the success path).
  - Declaring expected_status_codes: [200, 404] in endpoints.yaml tells the
    validator to accept 404 without flagging it as a structural mismatch.
"""
import json
from unittest.mock import Mock

from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    EndpointSpec,
    ResponseShape,
    ResponseWrapper,
    _parse_endpoint,
)
from databricks.labs.community_connector.source_simulator.validator import _diff


# ── helpers ───────────────────────────────────────────────────────────────────

def _mock_response(status_code: int, body=None) -> Mock:
    resp = Mock()
    resp.status_code = status_code
    resp.headers = {"Content-Type": "application/json"}
    if body is None:
        resp.text = ""
        resp.content = b""
        resp.json.side_effect = ValueError("no body")
    elif isinstance(body, (dict, list)):
        resp.text = json.dumps(body)
        resp.content = resp.text.encode()
        resp.json.return_value = body
    else:
        resp.text = str(body)
        resp.content = resp.text.encode()
        resp.json.side_effect = ValueError("not JSON")
    return resp


def _export_spec(expected_status_codes: list | None = None) -> EndpointSpec:
    """Build a minimal EndpointSpec for /api/2/export via the YAML parser."""
    raw = {
        "path": "/api/2/export",
        "method": "GET",
        "corpus": "events",
        "response": {
            "pagination_style": "none",
            "expected_status_codes": expected_status_codes or [],
        },
        "params": {
            "start": {"role": "ignore"},
            "end":   {"role": "ignore"},
        },
    }
    return _parse_endpoint(raw)


def _events_list_spec(ignore_extra_keys: bool) -> EndpointSpec:
    """Build a minimal EndpointSpec for /api/2/events/list via the YAML parser."""
    raw = {
        "path": "/api/2/events/list",
        "method": "GET",
        "corpus": "events_list",
        "response": {
            "pagination_style": "none",
            "wrapper": {
                "records_key": "data",
                "ignore_extra_keys": ignore_extra_keys,
            },
        },
        "params": {},
    }
    return _parse_endpoint(raw)


def _corpus_200() -> Mock:
    """The spec always serves 200 + a list of events from the corpus."""
    return _mock_response(200, [{"uuid": "evt-1", "event_type": "page_view"}])


def _wrapped_corpus_200() -> Mock:
    return _mock_response(200, {"data": [{"value": "page_view"}]})


# ── expected_status_codes tests ───────────────────────────────────────────────

class TestExpectedStatusCodes:
    def test_200_from_live_is_always_validated(self):
        """A 200 live response matching the corpus 200 spec → no issues."""
        spec = _export_spec(expected_status_codes=[200, 404])
        issues = _diff(_corpus_200(), _corpus_200(), spec)
        assert issues == []

    def test_404_from_live_is_validated_when_declared(self):
        """404 live response is accepted when declared in expected_status_codes."""
        spec = _export_spec(expected_status_codes=[200, 404])
        live_404 = _mock_response(404, {"error": "No data in the requested time range"})
        issues = _diff(live_404, _corpus_200(), spec)
        assert issues == [], f"Unexpected issues: {issues}"

    def test_404_body_shape_diff_is_suppressed(self):
        """Even though a 404 body is a dict and the corpus is a list, the body
        diff must be skipped entirely for an accepted alternative status code."""
        spec = _export_spec(expected_status_codes=[200, 404])
        live_404 = _mock_response(404, {"error": "No data"})
        issues = _diff(live_404, _corpus_200(), spec)
        assert not any("body shape" in i for i in issues)

    def test_404_without_declaration_is_a_mismatch(self):
        """Without expected_status_codes the 404 must still be flagged."""
        spec = _export_spec(expected_status_codes=[])
        live_404 = _mock_response(404, {"error": "No data"})
        issues = _diff(live_404, _corpus_200(), spec)
        assert any("status_code" in i for i in issues)

    def test_unexpected_500_is_flagged_even_when_404_declared(self):
        """A 500 must still be flagged even if 404 is in expected_status_codes."""
        spec = _export_spec(expected_status_codes=[200, 404])
        live_500 = _mock_response(500, {"error": "Internal Server Error"})
        issues = _diff(live_500, _corpus_200(), spec)
        assert any("status_code" in i for i in issues)

    def test_empty_expected_status_codes_is_equivalent_to_none(self):
        """An empty list behaves the same as no declaration at all."""
        spec_none  = _export_spec(expected_status_codes=None)
        spec_empty = _export_spec(expected_status_codes=[])
        live_404   = _mock_response(404, {"error": "No data"})

        issues_none  = _diff(live_404, _corpus_200(), spec_none)
        issues_empty = _diff(live_404, _corpus_200(), spec_empty)

        assert issues_none == issues_empty
        assert len(issues_none) > 0  # both flag it


# ── ignore_extra_keys tests ───────────────────────────────────────────────────

class TestIgnoreExtraKeys:
    def test_extra_top_level_keys_ignored_when_flag_set(self):
        """Dynamic metadata keys (cacheFreshness, novaCost, …) must not
        produce issues when ignore_extra_keys=True."""
        spec = _events_list_spec(ignore_extra_keys=True)
        corpus_resp = _wrapped_corpus_200()
        live_resp = _mock_response(200, {
            "data":           [{"value": "page_view"}],
            "cacheFreshness": "fresh",
            "novaCost":       0.001,
        })
        issues = _diff(live_resp, corpus_resp, spec)
        assert issues == []

    def test_extra_top_level_keys_flagged_when_flag_not_set(self):
        """Without ignore_extra_keys the dynamic metadata keys are flagged."""
        spec = _events_list_spec(ignore_extra_keys=False)
        corpus_resp = _wrapped_corpus_200()
        live_resp = _mock_response(200, {
            "data":           [{"value": "page_view"}],
            "cacheFreshness": "fresh",
        })
        issues = _diff(live_resp, corpus_resp, spec)
        assert any("top-level keys in live missing from spec" in i for i in issues)
