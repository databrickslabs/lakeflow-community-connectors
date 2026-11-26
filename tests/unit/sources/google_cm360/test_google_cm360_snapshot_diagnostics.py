"""Offline regression checks for period snapshot diagnostics."""

import logging
from unittest.mock import Mock

import pytest

from databricks.labs.community_connector.sources.google_cm360 import google_cm360 as module


OPTIONS = {
    "sync_start_date": "2025-01-01",
    "sync_end_date": "2026-10-13",
    "delivery_country_codes": '["CH"]',
}


def test_401_is_classified_without_retry_or_secret_leak(monkeypatch, caplog):
    client = module.Cm360Client("credential-secret", "123456")
    client._snapshot_deadline = 100.0
    monkeypatch.setattr(module.time, "monotonic", lambda: 1.0)
    request = Mock(return_value=Mock(status_code=401, text="response-secret"))
    monkeypatch.setattr(client._session, "request", request)
    with caplog.at_level(logging.INFO), pytest.raises(module.Cm360ApiError) as raised:
        client.get_profile()
    assert type(raised.value).__name__ == "Cm360AuthenticationError"
    assert "HTTP 401" in str(raised.value)
    assert request.call_count == 1
    assert "credential-secret" not in str(raised.value) + caplog.text
    assert "response-secret" not in str(raised.value) + caplog.text


def test_expired_snapshot_makes_no_http_request(monkeypatch):
    client = module.Cm360Client("test", "123456")
    client._snapshot_deadline = 2.0
    monkeypatch.setattr(module.time, "monotonic", lambda: 2.0)
    request = Mock()
    monkeypatch.setattr(client._session, "request", request)
    with pytest.raises(module.Cm360ApiError, match="snapshot execution deadline"):
        client.get_profile()
    request.assert_not_called()


def test_request_timeout_uses_remaining_budget(monkeypatch, caplog):
    client = module.Cm360Client("test", "123456")
    client._snapshot_deadline = 2.0
    monkeypatch.setattr(module.time, "monotonic", lambda: 1.0)
    response = Mock(status_code=200)
    response.json.return_value = {"profileId": "123456"}
    request = Mock(return_value=response)
    monkeypatch.setattr(client._session, "request", request)
    with caplog.at_level(logging.INFO):
        client.get_profile()
    assert request.call_args.kwargs["timeout"] == 1.0
    assert "request started" in caplog.text
    assert "status=200" in caplog.text


def test_polling_stops_at_snapshot_deadline(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    client = Mock()
    client.get_file.return_value = {"status": "PROCESSING"}
    with pytest.raises(module.Cm360ApiError, match="snapshot execution deadline"):
        module._poll_until_available(client, "1", "2", None, deadline=1.0)
    assert clock[0] == 1.0
    assert client.get_file.call_count == 1


def test_snapshot_timeout_does_not_return_partial_rows(monkeypatch):
    source = module.GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    clock = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    source._period_campaigns = Mock(return_value=[])
    def late_rows(*_):
        clock[0] = 2701.0
        return [{"campaign_id": "1"}]
    source._period_campaign_rows = late_rows
    with pytest.raises(module.Cm360ApiError, match="snapshot execution deadline"):
        source.read_table("reach_report_period", None, OPTIONS)
    assert source._client._snapshot_deadline is None


def test_snapshot_preserves_authentication_type_and_phase():
    source = module.GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    error_type = getattr(module, "Cm360AuthenticationError", module.Cm360ApiError)
    source._period_campaigns = Mock(side_effect=error_type("HTTP 401"))
    with pytest.raises(module.Cm360ApiError, match="phase=metadata") as raised:
        source.read_table("reach_report_period", None, OPTIONS)
    assert type(raised.value).__name__ == "Cm360AuthenticationError"


def test_cleanup_does_not_replace_authentication_failure(caplog):
    source = module.GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    client = Mock()
    source._client_instance = client
    error_type = getattr(module, "Cm360AuthenticationError", module.Cm360ApiError)
    client.run_report.side_effect = error_type("HTTP 401")
    client.delete_report.side_effect = module.Cm360ApiError("cleanup-secret")
    with pytest.raises(module.Cm360ApiError, match="HTTP 401"):
        source._period_export({}, module._parse_read_options(OPTIONS), ())
    assert "cleanup-secret" not in caplog.text


def test_success_logs_metadata_and_preserves_rows(caplog):
    source = module.GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    source._period_campaigns = Mock(return_value=[])
    rows = [{"unique_reach_impression_reach": "-"}]
    source._period_campaign_rows = Mock(return_value=rows)
    with caplog.at_level(logging.INFO):
        result, offset = source.read_table("reach_report_period", None, OPTIONS)
    assert list(result) == rows and offset == {}
    assert "phase=metadata" in caplog.text and "snapshot completed" in caplog.text


def test_cleanup_is_sent_after_the_deadline_expires(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    source = module.GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    client = source._client
    client._snapshot_deadline = 10.0
    client.validate_compatible_fields = Mock()
    client.insert_report = Mock(return_value="77")

    def expire(_report_id):
        clock[0] = 11.0
        module._snapshot_timeout(client._snapshot_deadline)

    client.run_report = Mock(side_effect=expire)
    response = Mock(status_code=204)
    request = Mock(return_value=response)
    monkeypatch.setattr(client._session, "request", request)
    with pytest.raises(module.Cm360SnapshotTimeout):
        source._period_export({}, module._parse_read_options(OPTIONS), ())
    assert [call.args[0] for call in request.call_args_list] == ["DELETE"]
    assert request.call_args.args[1].endswith("/reports/77")
    assert request.call_args.kwargs["timeout"] <= module._HTTP_TIMEOUT_SECONDS
    assert client._snapshot_deadline == 10.0


def test_timeout_message_does_not_name_a_fixed_budget():
    with pytest.raises(module.Cm360SnapshotTimeout) as raised:
        module._snapshot_timeout(-1.0)
    assert "2700" not in str(raised.value)
    assert "execution deadline" in str(raised.value)
