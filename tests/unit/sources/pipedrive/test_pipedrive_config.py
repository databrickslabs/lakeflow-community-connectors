"""Connection-option parsing and optional-table skip logging.

The optional-table tests use a stand-in client that raises the same
``PipedriveAPIError`` the real client raises for a given status.  A mock is
needed because there is no Pipedrive account to produce the 402 / 404 responses.
"""

import logging

import pytest

from databricks.labs.community_connector.sources.pipedrive.pipedrive import (
    PipedriveLakeflowConnect,
)
from databricks.labs.community_connector.sources.pipedrive.pipedrive_utils import (
    PipedriveAPIError,
)


@pytest.mark.parametrize(
    "company_domain, expected",
    [
        ("mycompany", "https://mycompany.pipedrive.com"),
        ("mycompany.pipedrive.com", "https://mycompany.pipedrive.com"),
        ("https://mycompany.pipedrive.com", "https://mycompany.pipedrive.com"),
        ("https://mycompany.pipedrive.com/api", "https://mycompany.pipedrive.com"),
        ("https://mycompany.pipedrive.com/api/v2/deals?x=1", "https://mycompany.pipedrive.com"),
        ("  MyCompany.pipedrive.com/ ", "https://mycompany.pipedrive.com"),
    ],
)
def test_company_domain_resolves_to_hostname(company_domain, expected):
    connector = PipedriveLakeflowConnect(
        {"api_token": "fake", "company_domain": company_domain}
    )
    assert connector._client._base_url == expected  # pylint: disable=protected-access


def test_missing_company_domain_uses_public_host():
    connector = PipedriveLakeflowConnect({"api_token": "fake"})
    assert connector._client._base_url == "https://api.pipedrive.com"  # pylint: disable=protected-access


class _RaisingClient:
    """Every list request fails with the given status (JSON or HTML body)."""

    def __init__(self, status_code, is_json):
        self._status_code = status_code
        self._is_json = is_json

    def _fail(self):
        raise PipedriveAPIError(
            "Pipedrive API GET /api/v2/projects failed with HTTP",
            status_code=self._status_code,
            is_json=self._is_json,
        )

    def iter_v2_pages(self, path, params):  # pylint: disable=unused-argument
        self._fail()
        yield []  # pragma: no cover  (makes this a generator)


@pytest.mark.parametrize("status_code", [402, 403, 415, 404])
def test_projects_feature_absent_is_skipped_with_warning(status_code, caplog):
    connector = PipedriveLakeflowConnect({"api_token": "secret-token-value"})
    connector._client = _RaisingClient(status_code, is_json=True)  # pylint: disable=protected-access

    with caplog.at_level(logging.WARNING):
        records, _ = connector.read_table("projects", {}, {})
        assert list(records) == []

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "projects" in message
    assert str(status_code) in message
    assert "secret-token-value" not in message


def test_tasks_feature_absent_is_skipped_with_warning(caplog):
    connector = PipedriveLakeflowConnect({"api_token": "fake"})
    connector._client = _RaisingClient(402, is_json=True)  # pylint: disable=protected-access

    with caplog.at_level(logging.WARNING):
        records, _ = connector.read_table("tasks", {}, {})
        assert list(records) == []
    assert any("tasks" in r.getMessage() for r in caplog.records)


def test_html_404_is_a_real_failure_not_a_skip():
    connector = PipedriveLakeflowConnect({"api_token": "fake"})
    connector._client = _RaisingClient(404, is_json=False)  # pylint: disable=protected-access

    records, _ = connector.read_table("projects", {}, {})
    with pytest.raises(PipedriveAPIError):
        list(records)


@pytest.mark.parametrize("company_domain", ["evil.example.com", "pipedrive.com",
                                            "https://evil.example.com/x", "a.b.pipedrive.com"])
def test_company_domain_outside_pipedrive_is_rejected(company_domain):
    # The API token is sent to this host, so anything but a company subdomain fails fast.
    with pytest.raises(ValueError, match="company_domain"):
        PipedriveLakeflowConnect({"api_token": "t", "company_domain": company_domain})
