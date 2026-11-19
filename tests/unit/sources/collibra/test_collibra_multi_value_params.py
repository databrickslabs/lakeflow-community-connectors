"""Multi-value table options must be sent as repeated query params (#314).

Collibra's Core REST API v2 expects multi-value filters repeated
(``typePublicIds=A&typePublicIds=B``). A single comma-joined value is treated as
one id and rejected with ``400 attTypeNotFoundPublicId``. These tests drive the
real readers and capture the URL ``requests`` actually prepares, with no network.
"""

from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from databricks.labs.community_connector.sources.collibra.collibra import (
    CollibraLakeflowConnect,
)


class _FakeResponse:
    status_code = 200
    headers: dict = {}
    text = ""

    def __init__(self, body=None):
        self._body = body if body is not None else {"results": [], "total": 0}

    def json(self):
        return self._body


class _CapturingSession(requests.Session):
    """Records the fully-prepared URL of every GET and replays queued bodies.

    Once the queue is empty every further GET returns an empty page, which ends
    pagination.
    """

    def __init__(self, bodies=None):
        super().__init__()
        self.urls: list[str] = []
        self._bodies = list(bodies or [])

    def get(self, url, params=None, **kwargs):  # noqa: D401 - mirrors requests API
        self.urls.append(requests.Request("GET", url, params=params).prepare().url)
        return _FakeResponse(self._bodies.pop(0) if self._bodies else None)


def _connector(**options) -> CollibraLakeflowConnect:
    return CollibraLakeflowConnect(
        {"org": "simulator", "access_token": "fake-token", **options}
    )


def _queries(table: str, options: dict, conn=None, bodies=None) -> list[dict]:
    """Drain one read and return the parsed query string of every request made."""
    conn = conn or _connector()
    session = _CapturingSession(bodies)
    conn._session = session
    records, _ = conn.read_table(table, {}, options)
    list(records)
    assert session.urls, "expected at least one request"
    return [parse_qs(urlsplit(u).query) for u in session.urls]


def _read(table: str, options: dict) -> dict[str, list[str]]:
    return _queries(table, options)[0]


# --------------------------------------------------------------------------- #
# First page: repeated params, single values, whitespace
# --------------------------------------------------------------------------- #

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


# --------------------------------------------------------------------------- #
# Separator-only values must fail loudly, not silently drop the filter
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "table,option",
    [
        ("attributes", "type_public_ids"),
        ("responsibilities", "resource_ids"),
        ("responsibilities", "role_ids"),
    ],
)
def test_separator_only_option_raises(table, option):
    conn = _connector()
    conn._session = _CapturingSession()
    with pytest.raises(ValueError, match=option):
        conn.read_table(table, {}, {option: " , ,"})
    assert not conn._session.urls, "no request may be sent with the filter dropped"


# --------------------------------------------------------------------------- #
# Later pages keep the repeated params (cursor and offset pagination)
# --------------------------------------------------------------------------- #

def _attribute(rec_id: str) -> dict:
    return {
        "id": rec_id,
        "lastModifiedOn": 1_000,
        "value": "v",
        "type": {"id": "t-1", "name": "Definition"},
        "asset": {"id": "a-1", "name": "asset"},
    }


def _responsibility(rec_id: str) -> dict:
    return {
        "id": rec_id,
        "lastModifiedOn": 1_000,
        "role": {"id": "role-a", "name": "Owner"},
        "baseResource": {"id": "r-1", "name": "res"},
        "owner": {"id": "u-1", "name": "user"},
    }


def test_attributes_second_page_keeps_repeated_params():
    """``/attributes`` is cursor-paginated: page 2 must still filter by every type."""
    queries = _queries(
        "attributes",
        {"type_public_ids": "Definition,Description"},
        bodies=[{"results": [_attribute("x-1")], "nextCursor": "c-2"}],
    )
    assert len(queries) >= 2, "expected a second-page request"
    assert queries[1]["cursor"] == ["c-2"]
    for q in queries:
        assert q["typePublicIds"] == ["Definition", "Description"]


def test_responsibilities_second_page_keeps_repeated_params():
    """``/responsibilities`` is offset-paginated: page 2 must keep both filters."""
    queries = _queries(
        "responsibilities",
        {"resource_ids": "r-1,r-2", "role_ids": "role-a,role-b"},
        conn=_connector(page_size="1"),  # a full first page of 1 forces a second request
        bodies=[{"results": [_responsibility("x-1")], "total": 2}],
    )
    assert len(queries) >= 2, "expected a second-page request"
    assert queries[1]["offset"] == ["1"]
    for q in queries:
        assert q["resourceIds"] == ["r-1", "r-2"]
        assert q["roleIds"] == ["role-a", "role-b"]
