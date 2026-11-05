"""All idempotent HubSpot reads share one bounded transient-retry policy."""

from unittest.mock import Mock, patch

import pytest
import requests

from tests.unit.sources.hubspot.test_hubspot_custom_objects import _connector_class


def _response(status, payload=None, headers=None, text=""):
    response = Mock(status_code=status, headers=headers or {}, text=text)
    response.json.return_value = payload if payload is not None else {"results": []}
    return response


@pytest.mark.parametrize("generated", [False, True])
def test_connector_read_families_share_one_transport(generated):
    connector = _connector_class(generated)({"access_token": "PRIVATE_TOKEN"})
    assert connector._history._http is connector._http
    assert connector._non_crm._http is connector._http


@pytest.mark.parametrize("generated", [False, True])
def test_primary_object_get_retries_transient_status_with_safe_backoff(generated):
    transient = _response(503, headers={"Retry-After": "not-a-delay"}, text="PRIVATE_BODY")
    success = _response(200)
    connector = _connector_class(generated)({"access_token": "PRIVATE_TOKEN"})
    connector._history._sleep = sleep = Mock()
    with patch("requests.get", side_effect=[transient, success]) as get:
        rows, after = connector._fetch_full_refresh_batch("deals", [], [])

    assert rows == [] and after is None
    assert get.call_count == 2
    sleep.assert_called_once_with(1.0)


@pytest.mark.parametrize("generated", [False, True])
def test_search_post_honors_retry_after_then_succeeds(generated):
    limited = _response(429, headers={"Retry-After": "2"})
    success = _response(200)
    connector = _connector_class(generated)({"access_token": "PRIVATE_TOKEN"})
    connector._history._sleep = sleep = Mock()
    with patch("requests.post", side_effect=[limited, success]) as post:
        rows, after, latest = connector._fetch_incremental_batch(
            "deals", [], "hs_lastmodifieddate", {"updatedAt": "2024-01-01T00:00:00.000Z"}
        )

    assert rows == [] and after is None and latest == "2024-01-01T00:00:00.000Z"
    assert post.call_count == 2
    sleep.assert_called_once_with(2.0)


@pytest.mark.parametrize("generated", [False, True])
def test_non_crm_get_uses_the_same_server_error_policy(generated):
    transient = _response(500)
    empty_page = _response(200)
    connector = _connector_class(generated)({"access_token": "PRIVATE_TOKEN"})
    connector._history._sleep = sleep = Mock()
    with patch("requests.get", side_effect=[transient, empty_page, empty_page]) as get:
        assert connector._non_crm.read_table("owners", {}) == []

    assert get.call_count == 3
    assert sleep.call_args_list[0].args == (1.0,)


@pytest.mark.parametrize("generated", [False, True])
def test_safe_get_retries_request_exception_without_leaking_it(generated):
    success = _response(200)
    connector = _connector_class(generated)({"access_token": "PRIVATE_TOKEN"})
    connector._history._sleep = sleep = Mock()
    with patch("requests.get", side_effect=[requests.RequestException("PRIVATE_EXCEPTION"), success]) as get:
        rows, after = connector._fetch_full_refresh_batch("deals", [], [])

    assert rows == [] and after is None
    assert get.call_count == 2
    sleep.assert_called_once_with(1.0)


@pytest.mark.parametrize("generated", [False, True])
def test_permanent_failure_is_immediate_and_sanitized(generated):
    rejected = _response(400, text="PRIVATE_BODY PRIVATE_TOKEN")
    with patch("requests.get", return_value=rejected) as get:
        connector = _connector_class(generated)({"access_token": "PRIVATE_TOKEN"})
        with pytest.raises(RuntimeError) as raised:
            connector._fetch_full_refresh_batch("deals", [], [])

    assert get.call_count == 1
    assert "400" in str(raised.value)
    assert "PRIVATE" not in str(raised.value)
    assert raised.value.__suppress_context__ or raised.value.__context__ is None


@pytest.mark.parametrize("generated", [False, True])
def test_transient_exhaustion_is_bounded_and_sanitized(generated):
    unavailable = _response(502, text="PRIVATE_BODY PRIVATE_TOKEN")
    connector = _connector_class(generated)({"access_token": "PRIVATE_TOKEN"})
    connector._history._sleep = sleep = Mock()
    with patch("requests.post", return_value=unavailable) as post:
        with pytest.raises(RuntimeError) as raised:
            connector._fetch_incremental_batch(
                "deals", [], "hs_lastmodifieddate", {"updatedAt": "2024-01-01T00:00:00.000Z"}
            )

    assert post.call_count == 6
    assert sleep.call_count == 5
    assert "502" in str(raised.value)
    assert "PRIVATE" not in str(raised.value)
