"""Unit tests for the Microsoft Purview connector's source-specific logic.

These exercise the pure logic directly with mocked HTTP — no network, no
simulator. The full read cycle against the community-connector API is covered
by ``LakeflowConnectTests`` (``test_microsoft_purview_lakeflow_connect.py``);
this file targets the pieces that harness does not fully drive:

* auth / connection configuration (token precedence, required options),
* the ``nextLink`` pagination helper (all three endpoints share it),
* retry + non-200 error handling,
* the client-side incremental cursor engine (strict ``> since`` boundary so a
  non-empty batch always advances the offset, the
  init-time upper cap, and convergence when no forward progress is made),
* record shaping (``purview_tenant_id`` stamping, contacts normalization).
"""

import pytest

from databricks.labs.community_connector.sources.microsoft_purview import (
    microsoft_purview as mp,
)
from databricks.labs.community_connector.sources.microsoft_purview import (
    microsoft_purview_utils as mp_utils,
)
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview import (  # noqa: E501
    MicrosoftPurviewLakeflowConnect,
)
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_schemas import (  # noqa: E501
    API_VERSION,
    DEFAULT_ENDPOINT,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
)
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_utils import (  # noqa: E501
    api_get,
    next_link_paginate,
    normalize_contacts,
    request_with_retry,
)

# An init cap comfortably after any record timestamp used here, so the
# init-time upper bound never filters test records unless a test sets it.
_FAR_FUTURE_TS = "9999-12-31T23:59:59+00:00"


# --------------------------------------------------------------------------- #
# Test doubles
# --------------------------------------------------------------------------- #


class _Resp:
    """Minimal stand-in for a ``requests.Response``."""

    def __init__(self, status=200, json_body=None, headers=None, text=""):
        self.status_code = status
        self._json = {} if json_body is None else json_body
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json


class _Session:
    """Pops queued responses on ``.get`` and records each call."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []  # list of (url, params)

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return self._responses.pop(0)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _connector(**overrides) -> MicrosoftPurviewLakeflowConnect:
    opts = {"tenant_id": "tenant-abc", "access_token": "fake-token"}
    opts.update(overrides)
    conn = MicrosoftPurviewLakeflowConnect(opts)
    # Neutralize the init-time upper cap so tests control emission purely via
    # the cursor / sort logic under test (individual tests may override it).
    conn._init_ts = _FAR_FUTURE_TS
    return conn


def _rec(rec_id: str, last_modified: str | None) -> dict:
    system_data = {} if last_modified is None else {"lastModifiedAt": last_modified}
    return {"id": rec_id, "systemData": system_data}


def _drain(result):
    records, end_offset = result
    return list(records), end_offset


# --------------------------------------------------------------------------- #
# Auth / connection configuration
# --------------------------------------------------------------------------- #


class TestAuthAndConfig:
    def test_tenant_id_required(self):
        with pytest.raises(ValueError, match="tenant_id"):
            MicrosoftPurviewLakeflowConnect({"access_token": "t"})

    def test_account_is_accepted_as_tenant_id_alias(self):
        conn = MicrosoftPurviewLakeflowConnect(
            {"account": "tenant-xyz", "access_token": "t"}
        )
        assert conn.tenant_id == "tenant-xyz"

    def test_access_token_sets_bearer_header(self):
        conn = _connector(access_token="abc123")
        assert conn._session.headers["Authorization"] == "Bearer abc123"

    def test_token_is_accepted_as_fallback(self):
        conn = MicrosoftPurviewLakeflowConnect(
            {"tenant_id": "t", "token": "personal"}
        )
        assert conn._session.headers["Authorization"] == "Bearer personal"

    def test_access_token_takes_precedence_over_token(self):
        conn = MicrosoftPurviewLakeflowConnect(
            {"tenant_id": "t", "access_token": "injected", "token": "personal"}
        )
        assert conn._session.headers["Authorization"] == "Bearer injected"

    def test_missing_token_raises(self):
        with pytest.raises(ValueError, match="access_token"):
            MicrosoftPurviewLakeflowConnect({"tenant_id": "t"})

    def test_endpoint_defaults_and_strips_trailing_slash(self):
        assert _connector().endpoint == DEFAULT_ENDPOINT
        custom = _connector(endpoint="https://host.example.com/")
        assert custom.endpoint == "https://host.example.com"

    def test_page_size_is_clamped(self):
        assert _connector(page_size="0")._page_size == 1
        assert _connector(page_size="100000")._page_size == MAX_PAGE_SIZE
        assert _connector(page_size="not-a-number")._page_size == DEFAULT_PAGE_SIZE
        assert _connector(page_size="250")._page_size == 250


# --------------------------------------------------------------------------- #
# Pagination: next_link_paginate
# --------------------------------------------------------------------------- #


class TestNextLinkPaginate:
    def test_follows_nextlink_across_pages(self):
        session = _Session(
            [
                _Resp(json_body={"value": [{"id": "1"}], "nextLink": "https://x/p2"}),
                _Resp(json_body={"value": [{"id": "2"}, {"id": "3"}]}),
            ]
        )
        out = list(
            next_link_paginate(session, "https://x/p1", {"api-version": API_VERSION}, "t")
        )
        assert [r["id"] for r in out] == ["1", "2", "3"]

    def test_stops_when_nextlink_absent(self):
        session = _Session([_Resp(json_body={"value": [{"id": "1"}]})])
        out = list(next_link_paginate(session, "https://x/p1", {}, "t"))
        assert [r["id"] for r in out] == ["1"]
        assert len(session.calls) == 1

    def test_params_only_applied_to_first_call(self):
        """nextLink is an absolute URL that already embeds the query, so params
        must be sent on the first call only."""
        session = _Session(
            [
                _Resp(json_body={"value": [{"id": "1"}], "nextLink": "https://x/p2"}),
                _Resp(json_body={"value": [{"id": "2"}]}),
            ]
        )
        params = {"api-version": API_VERSION, "top": "100"}
        list(next_link_paginate(session, "https://x/p1", params, "t"))
        assert session.calls[0] == ("https://x/p1", params)
        assert session.calls[1] == ("https://x/p2", None)

    def test_empty_value_page_stops(self):
        session = _Session([_Resp(json_body={"value": [], "nextLink": "https://x/p2"})])
        out = list(next_link_paginate(session, "https://x/p1", {}, "t"))
        assert out == []
        # Must not follow nextLink after an empty page.
        assert len(session.calls) == 1


# --------------------------------------------------------------------------- #
# Retry + error handling
# --------------------------------------------------------------------------- #


class TestRetryAndErrors:
    def test_retries_retriable_status_then_succeeds(self, monkeypatch):
        monkeypatch.setattr(mp_utils.time, "sleep", lambda _s: None)
        session = _Session(
            [
                _Resp(status=503),
                _Resp(status=429),
                _Resp(status=200, json_body={"ok": True}),
            ]
        )
        resp = request_with_retry(session, "https://x")
        assert resp.status_code == 200
        assert len(session.calls) == 3

    def test_retry_honors_retry_after_header(self, monkeypatch):
        slept = []
        monkeypatch.setattr(mp_utils.time, "sleep", lambda s: slept.append(s))
        session = _Session(
            [
                _Resp(status=503, headers={"Retry-After": "7"}),
                _Resp(status=200, json_body={}),
            ]
        )
        request_with_retry(session, "https://x")
        assert slept == [7.0]

    def test_api_get_returns_json_on_200(self):
        session = _Session([_Resp(status=200, json_body={"value": [1, 2]})])
        assert api_get(session, "https://x", None, "t") == {"value": [1, 2]}

    def test_api_get_raises_on_non_200(self, monkeypatch):
        monkeypatch.setattr(mp_utils.time, "sleep", lambda _s: None)
        session = _Session([_Resp(status=404, text="not found")] * 6)
        with pytest.raises(RuntimeError, match="404"):
            api_get(session, "https://x", None, "terms")


# --------------------------------------------------------------------------- #
# Incremental cursor engine: _incremental_from_iter
# --------------------------------------------------------------------------- #


class TestIncrementalEngine:
    def test_strict_boundary_does_not_reemit_and_offset_advances(self):
        """Strict ``> since``: the boundary record (cursor == since) is NOT
        re-emitted, so a non-empty batch always advances the end offset past the
        start. Re-emitting the boundary while keeping the offset would violate the
        SimpleDataSourceStreamReader contract (SIMPLE_STREAM_READER_OFFSET_DID_NOT_
        ADVANCE). ``lastModifiedAt`` has sub-second (100-ns) precision, so distinct
        records don't share a cursor value — nothing is lost by the strict skip."""
        conn = _connector()
        arrival = [
            _rec("a", "2026-01-01T00:00:00+00:00"),  # == since -> skipped (strict)
            _rec("b", "2026-02-01T00:00:00+00:00"),  # > since -> emitted
        ]
        records, end_offset = _drain(
            conn._incremental_from_iter(
                iter(arrival),
                start_offset={"cursor": "2026-01-01T00:00:00+00:00"},
                transform=lambda r: r,
            )
        )
        assert [r["id"] for r in records] == ["b"]
        # Non-empty batch -> offset advanced past start (contract).
        assert end_offset == {"cursor": "2026-02-01T00:00:00+00:00"}
        assert end_offset != {"cursor": "2026-01-01T00:00:00+00:00"}

    def test_only_boundary_records_yields_empty_batch_and_converges(self):
        """When the only matching records are AT the watermark (cursor == since),
        strict `>` skips them all -> empty batch + unchanged offset, which
        converges cleanly (an empty batch may keep the same offset; a non-empty
        one may not)."""
        conn = _connector()
        arrival = [
            _rec("old", "2025-12-31T23:59:59+00:00"),  # < since -> skipped
            _rec("edge", "2026-01-01T00:00:00+00:00"),  # == since -> skipped (strict)
        ]
        records, end_offset = _drain(
            conn._incremental_from_iter(
                iter(arrival),
                start_offset={"cursor": "2026-01-01T00:00:00+00:00"},
                transform=lambda r: r,
            )
        )
        assert records == []
        assert end_offset == {"cursor": "2026-01-01T00:00:00+00:00"}

    def test_null_cursor_only_batch_converges_with_empty(self):
        """A batch whose only records have no cursor can't advance the offset;
        returning them non-empty against an unchanged offset would violate the
        SimpleDataSourceStreamReader contract, so the engine converges with an
        EMPTY batch instead. (Null-cursor records don't occur in normal Purview
        data — systemData.lastModifiedAt is always present.)"""
        conn = _connector()
        arrival = [_rec("n1", None), _rec("n2", None)]
        records, end_offset = _drain(
            conn._incremental_from_iter(iter(arrival), {}, transform=lambda r: r)
        )
        assert records == []
        assert end_offset == {}

    def test_z_and_offset_suffix_cursors_compare_by_datetime_not_string(self):
        """_init_ts uses '+00:00' but Purview cursors use 'Z'; comparison must be
        by parsed datetime, not lexicographic. A whole-second 'Z' cursor 0.5s
        BEFORE init must NOT be capped — lexicographically 'Z'(0x5A) > '.'(0x2E),
        which would wrongly cap it, but by datetime it is correctly < init."""
        conn = _connector()
        conn._init_ts = "2026-09-29T15:30:00.500000+00:00"  # datetime.isoformat spelling
        arrival = [_rec("before", "2026-09-29T15:30:00Z")]  # 0.5s before init, Purview 'Z'
        records, end_offset = _drain(
            conn._incremental_from_iter(iter(arrival), {}, transform=lambda r: r)
        )
        assert [r["id"] for r in records] == ["before"]  # emitted, not spuriously capped
        assert end_offset == {"cursor": "2026-09-29T15:30:00Z"}

    def test_init_ts_cap_skips_records_modified_after_start(self):
        conn = _connector()
        conn._init_ts = "2026-06-01T00:00:00+00:00"
        arrival = [
            _rec("past", "2026-01-01T00:00:00+00:00"),  # <= cap -> emitted
            _rec("future", "2026-09-01T00:00:00+00:00"),  # > cap -> skipped
        ]
        records, end_offset = _drain(
            conn._incremental_from_iter(iter(arrival), {}, transform=lambda r: r)
        )
        assert [r["id"] for r in records] == ["past"]
        assert end_offset == {"cursor": "2026-01-01T00:00:00+00:00"}

    def test_no_forward_progress_returns_start_offset(self):
        """When nothing new is emitted, end_offset must equal start_offset so the
        Trigger.AvailableNow microbatch converges."""
        conn = _connector()
        start = {"cursor": "2026-05-01T00:00:00+00:00"}
        # All records at/below `since` -> filtered out.
        arrival = [_rec("a", "2026-01-01T00:00:00+00:00")]
        records, end_offset = _drain(
            conn._incremental_from_iter(iter(arrival), start, transform=lambda r: r)
        )
        assert records == []
        assert end_offset == start

    def test_watermark_advances_to_max_seen(self):
        conn = _connector()
        arrival = [
            _rec("a", "2026-03-01T00:00:00+00:00"),
            _rec("b", "2026-01-01T00:00:00+00:00"),
            _rec("c", "2026-05-01T00:00:00+00:00"),
        ]
        records, end_offset = _drain(
            conn._incremental_from_iter(iter(arrival), {}, transform=lambda r: r)
        )
        assert len(records) == 3
        assert end_offset == {"cursor": "2026-05-01T00:00:00+00:00"}

    def test_since_at_or_after_init_short_circuits(self):
        conn = _connector()
        conn._init_ts = "2026-06-01T00:00:00+00:00"
        # Already caught up: since >= init cap -> nothing can be emitted, and the
        # iterator must not even be consumed.
        def _boom():
            raise AssertionError("iterator should not be consumed")
            yield  # pragma: no cover

        start = {"cursor": "2026-06-01T00:00:00+00:00"}
        records, end_offset = _drain(
            conn._incremental_from_iter(_boom(), start, transform=lambda r: r)
        )
        assert records == []
        assert end_offset == start

    def test_record_with_no_cursor_is_emitted_but_does_not_move_watermark(self):
        conn = _connector()
        arrival = [
            _rec("a", "2026-02-01T00:00:00+00:00"),
            _rec("nocursor", None),  # no systemData.lastModifiedAt
        ]
        records, end_offset = _drain(
            conn._incremental_from_iter(iter(arrival), {}, transform=lambda r: r)
        )
        assert {r["id"] for r in records} == {"a", "nocursor"}
        assert end_offset == {"cursor": "2026-02-01T00:00:00+00:00"}


class TestRecordCursor:
    def test_extracts_last_modified_at(self):
        raw = {"systemData": {"lastModifiedAt": "2026-01-01T00:00:00+00:00"}}
        assert (
            MicrosoftPurviewLakeflowConnect._record_cursor(raw)
            == "2026-01-01T00:00:00+00:00"
        )

    def test_missing_system_data_returns_none(self):
        assert MicrosoftPurviewLakeflowConnect._record_cursor({}) is None

    def test_non_string_cursor_returns_none(self):
        raw = {"systemData": {"lastModifiedAt": 123}}
        assert MicrosoftPurviewLakeflowConnect._record_cursor(raw) is None


# --------------------------------------------------------------------------- #
# Record shaping
# --------------------------------------------------------------------------- #


class TestShaping:
    def test_shapers_stamp_tenant_id(self):
        conn = _connector(tenant_id="tid-1")
        assert conn._shape_business_domain({"id": "d"})["purview_tenant_id"] == "tid-1"
        assert conn._shape_data_product({"id": "p"})["purview_tenant_id"] == "tid-1"
        assert conn._shape_term({"id": "t"})["purview_tenant_id"] == "tid-1"

    def test_cdc_shapers_promote_cursor_to_top_level(self):
        """data_products/terms promote systemData.lastModifiedAt to a top-level
        ``last_modified_at`` column so it can serve as the cdc sequence_by
        (SDP APPLY CHANGES can't resolve a nested sequence_by path)."""
        conn = _connector()
        raw = {"id": "x", "systemData": {"lastModifiedAt": "2026-09-18T00:00:00+00:00"}}
        assert conn._shape_data_product(raw)["last_modified_at"] == "2026-09-18T00:00:00+00:00"
        assert conn._shape_term(raw)["last_modified_at"] == "2026-09-18T00:00:00+00:00"
        # business_domains is a snapshot — no cursor column promoted.
        assert "last_modified_at" not in conn._shape_business_domain({"id": "d"})

    def test_normalize_contacts_keeps_declared_roles_and_coerces(self):
        value = {
            "owner": [{"id": "o1", "description": "d", "extra": "dropped"}],
            "expert": [{"id": "e1", "description": None}],
            "unknownRole": [{"id": "x"}],  # not a declared role -> dropped
        }
        out = normalize_contacts(value)
        assert out["owner"] == [{"id": "o1", "description": "d"}]
        assert out["expert"] == [{"id": "e1", "description": None}]
        assert out["databaseAdmin"] is None
        assert "unknownRole" not in out

    def test_normalize_contacts_none_on_empty_or_non_dict(self):
        assert normalize_contacts(None) is None
        assert normalize_contacts({}) is None
        assert normalize_contacts("nope") is None


# --------------------------------------------------------------------------- #
# read_table dispatch
# --------------------------------------------------------------------------- #


class TestReadTableDispatch:
    def test_business_domains_is_snapshot_with_empty_offset(self, monkeypatch):
        monkeypatch.setattr(
            mp,
            "next_link_paginate",
            lambda *a, **k: iter([{"id": "d1"}, {"id": "d2"}]),
        )
        conn = _connector(tenant_id="tid-1")
        records, end_offset = _drain(conn.read_table("business_domains", {}, {}))
        assert [r["id"] for r in records] == ["d1", "d2"]
        assert all(r["purview_tenant_id"] == "tid-1" for r in records)
        # Snapshot: no incremental offset is carried.
        assert end_offset == {}

    def test_unsupported_table_raises(self):
        conn = _connector()
        with pytest.raises(ValueError, match="Unsupported table"):
            conn.read_table("not_a_table", {}, {})


# --------------------------------------------------------------------------- #
# Table options -> API query params
# --------------------------------------------------------------------------- #


class TestTableOptionsToQueryParams:
    """Each reader must translate its supported table_options into the correct
    Purview query-param names (domain_id -> domainId, etc.) and omit params for
    options that weren't supplied."""

    @staticmethod
    def _capture(monkeypatch):
        calls = []

        def fake(session, url, params, label, **kwargs):
            calls.append({"url": url, "params": dict(params), "label": label})
            return iter([])

        monkeypatch.setattr(mp, "next_link_paginate", fake)
        return calls

    def test_business_domains_translates_write_only(self, monkeypatch):
        calls = self._capture(monkeypatch)
        _drain(
            _connector().read_table(
                "business_domains", {}, {"write_only": "true"}
            )
        )
        assert calls[0]["url"].endswith("/datagovernance/catalog/businessdomains")
        assert calls[0]["params"].get("writeOnly") == "true"
        assert calls[0]["params"].get("api-version")

    def test_business_domains_omits_write_only_when_absent(self, monkeypatch):
        calls = self._capture(monkeypatch)
        _drain(_connector().read_table("business_domains", {}, {}))
        assert "writeOnly" not in calls[0]["params"]

    def test_data_products_translates_domain_and_order_by(self, monkeypatch):
        calls = self._capture(monkeypatch)
        _drain(
            _connector().read_table(
                "data_products", {}, {"domain_id": "dom-1", "order_by": "name asc"}
            )
        )
        params = calls[0]["params"]
        assert calls[0]["url"].endswith("/datagovernance/catalog/dataProducts")
        assert params.get("domainId") == "dom-1"
        assert params.get("orderBy") == "name asc"
        assert params.get("top")  # page size always set on the skip/top endpoints

    def test_data_products_omits_optional_params_when_absent(self, monkeypatch):
        calls = self._capture(monkeypatch)
        _drain(_connector().read_table("data_products", {}, {}))
        params = calls[0]["params"]
        assert "domainId" not in params
        assert "orderBy" not in params

    def test_terms_translates_all_filters(self, monkeypatch):
        calls = self._capture(monkeypatch)
        _drain(
            _connector().read_table(
                "terms",
                {},
                {
                    "domain_id": "dom-1",
                    "parent_id": "par-1",
                    "keyword": "pii",
                    "order_by": "name asc",
                },
            )
        )
        params = calls[0]["params"]
        assert calls[0]["url"].endswith("/datagovernance/catalog/terms")
        assert params.get("domainId") == "dom-1"
        assert params.get("parentId") == "par-1"
        assert params.get("keyword") == "pii"
        assert params.get("orderBy") == "name asc"

    def test_terms_omits_optional_params_when_absent(self, monkeypatch):
        calls = self._capture(monkeypatch)
        _drain(_connector().read_table("terms", {}, {}))
        params = calls[0]["params"]
        for key in ("domainId", "parentId", "keyword", "orderBy"):
            assert key not in params
