"""Lakeflow Connect tests for the Xquik connector."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from databricks.labs.community_connector.sources.xquik.xquik import (
    REQUEST_TIMEOUT_SECONDS,
    XquikLakeflowConnect,
    _normalize_profile,
    _response_error,
    _retry_delay,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestXquikConnector(LakeflowConnectTests):
    connector_class = XquikLakeflowConnect
    simulator_source = "xquik"
    replay_config = {"api_key": "simulator-fake-api-key"}
    table_configs = {
        "tweets_search": {
            "q": '"open source" lang:en',
            "query_type": "Latest",
            "limit": "2",
            "max_pages": "2",
            "since_time": "2026-01-01T00:00:00Z",
            "until_time": "2026-08-01T00:00:00Z",
        },
        "user_profiles": {"usernames": "databricks"},
        "user_tweets": {
            "usernames": "databricks",
            "page_size": "2",
            "include_replies": "false",
            "max_pages": "2",
        },
        "trends": {"woeids": "1", "count": "2"},
    }
    sample_records = 5


def test_api_key_is_required():
    with pytest.raises(ValueError, match="requires 'api_key'"):
        XquikLakeflowConnect({})


def test_usernames_reject_at_prefix():
    connector = XquikLakeflowConnect({"api_key": "test-key"})

    with pytest.raises(ValueError, match="must omit @"):
        connector.read_table("user_profiles", {}, {"usernames": "@databricks"})


def test_retry_delay_supports_seconds_and_http_dates():
    seconds = MagicMock(headers={"Retry-After": "9"})
    future_date = MagicMock(headers={"Retry-After": "Wed, 21 Oct 2099 07:28:00 GMT"})
    malformed = MagicMock(headers={"Retry-After": "eventually"})

    assert _retry_delay(seconds, 1.0) == 9.0
    assert _retry_delay(future_date, 1.0) == 60.0
    assert _retry_delay(malformed, 3.0) == 3.0


def test_response_error_handles_non_object_json():
    response = MagicMock(reason="Bad Gateway")
    response.json.return_value = ["not", "an", "object"]

    assert _response_error(response) == ("unknown_error", "Bad Gateway")


def test_profile_normalization_replaces_empty_nested_structs():
    profile = {
        "id": "1",
        "profile_bio": {
            "description": "Data engineering",
            "entities": {"description": {}, "url": {"urls": []}},
        },
    }

    normalized = _normalize_profile(profile)

    assert normalized["profile_bio"]["entities"] == {
        "description": None,
        "url": {"urls": []},
    }
    assert profile["profile_bio"]["entities"]["description"] == {}


def test_request_retries_and_preserves_auth_timeout():
    connector = XquikLakeflowConnect({"api_key": "test-key"})
    rate_limited = MagicMock(status_code=429, headers={"Retry-After": "2"}, ok=False)
    success = MagicMock(status_code=200, headers={}, ok=True)
    success.json.return_value = {"tweets": []}

    with patch.object(connector._session, "get", side_effect=[rate_limited, success]) as get:
        with patch("databricks.labs.community_connector.sources.xquik.xquik.time.sleep") as sleep:
            payload = connector._request("/x/tweets/search", {"q": "lakehouse"})

    assert payload == {"tweets": []}
    assert get.call_count == 2
    assert get.call_args.kwargs["headers"] == {"x-api-key": "test-key"}
    assert get.call_args.kwargs["timeout"] == REQUEST_TIMEOUT_SECONDS
    sleep.assert_called_once_with(2.0)


def test_request_retries_network_failures_then_raises_without_secret():
    connector = XquikLakeflowConnect({"api_key": "never-print-this"})

    with patch.object(
        connector._session,
        "get",
        side_effect=requests.ConnectionError("network unavailable"),
    ):
        with patch("databricks.labs.community_connector.sources.xquik.xquik.time.sleep"):
            with pytest.raises(RuntimeError, match="failed after retries") as error:
                connector._request("/x/trends")

    assert "never-print-this" not in str(error.value)


@pytest.mark.parametrize("status_code", [301, 302, 307, 308])
def test_request_rejects_redirects_without_forwarding_api_key(status_code):
    connector = XquikLakeflowConnect({"api_key": "test-key"})
    redirect = MagicMock(
        status_code=status_code,
        headers={"Location": "https://example.invalid/collect"},
        ok=True,
    )

    with patch.object(connector._session, "get", return_value=redirect) as get:
        with pytest.raises(ValueError, match="refused HTTP redirect"):
            connector._request("/x/trends")

    assert get.call_count == 1
    assert get.call_args.kwargs["allow_redirects"] is False


def test_request_rejects_invalid_success_json():
    connector = XquikLakeflowConnect({"api_key": "test-key"})
    response = MagicMock(status_code=200, headers={}, ok=True)
    response.json.side_effect = ValueError("invalid json")

    with patch.object(connector._session, "get", return_value=response):
        with pytest.raises(ValueError, match="returned invalid JSON"):
            connector._request("/x/trends")


def test_search_pagination_forwards_cursor_and_adds_lineage():
    connector = XquikLakeflowConnect({"api_key": "test-key"})
    first = {
        "tweets": [{"id": "1"}],
        "has_next_page": True,
        "next_cursor": "page-2",
    }
    second = {"tweets": [{"id": "2"}], "has_next_page": False}

    with patch.object(connector, "_request", side_effect=[first, second]) as request:
        records, offset = connector.read_table(
            "tweets_search", {}, {"q": "twitter advanced search", "max_pages": "2"}
        )

    assert list(records) == [
        {"id": "1", "search_query": "twitter advanced search"},
        {"id": "2", "search_query": "twitter advanced search"},
    ]
    assert offset is None
    assert request.call_args_list[1].args[1]["cursor"] == "page-2"
