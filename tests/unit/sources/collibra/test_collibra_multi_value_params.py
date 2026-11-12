"""Multi-value table options must be sent as repeated query params (#314).

Collibra's Core REST API v2 expects multi-value filters repeated
(``typePublicIds=A&typePublicIds=B``). A single comma-joined value is treated as
one id and rejected with ``400 attTypeNotFoundPublicId``. These tests drive the
real readers and capture the URL ``requests`` actually prepares, with no network.
"""

from urllib.parse import parse_qs, urlsplit

import requests

from databricks.labs.community_connector.sources.collibra.collibra import (
    CollibraLakeflowConnect,
)


class _FakeResponse:
    status_code = 200
    headers: dict = {}
    text = ""

    def json(self):
        return {"results": [], "total": 0}


class _CapturingSession(requests.Session):
    """Records the fully-prepared URL of every GET instead of sending it."""

    def __init__(self):
        super().__init__()
        self.urls: list[str] = []

    def get(self, url, params=None, **kwargs):  # noqa: D401 - mirrors requests API
        self.urls.append(requests.Request("GET", url, params=params).prepare().url)
        return _FakeResponse()


def _read(table: str, options: dict) -> dict[str, list[str]]:
    conn = CollibraLakeflowConnect({"org": "simulator", "access_token": "fake-token"})
    session = _CapturingSession()
    conn._session = session
    records, _ = conn.read_table(table, {}, options)
    list(records)  # drain so the first page request is issued
    assert session.urls, "expected at least one request"
    return parse_qs(urlsplit(session.urls[0]).query)


def test_attributes_type_public_ids_sent_as_repeated_params():
    q = _read("attributes", {"type_public_ids": "Definition,Description,Certified"})
    assert q["typePublicIds"] == ["Definition", "Description", "Certified"]


def test_attributes_single_type_public_id_unchanged():
    q = _read("attributes", {"type_public_ids": "Description"})
    assert q["typePublicIds"] == ["Description"]


def test_split_ignores_whitespace_and_empty_parts():
    q = _read("attributes", {"type_public_ids": " Definition , ,Certified, "})
    assert q["typePublicIds"] == ["Definition", "Certified"]


def test_responsibilities_resource_and_role_ids_sent_as_repeated_params():
    q = _read(
        "responsibilities",
        {"resource_ids": "r-1,r-2", "role_ids": "role-a,role-b"},
    )
    assert q["resourceIds"] == ["r-1", "r-2"]
    assert q["roleIds"] == ["role-a", "role-b"]
    assert q["includeInherited"] == ["true"]


def test_responsibilities_single_values_unchanged():
    q = _read("responsibilities", {"resource_ids": "r-1", "role_ids": "role-a"})
    assert q["resourceIds"] == ["r-1"]
    assert q["roleIds"] == ["role-a"]
