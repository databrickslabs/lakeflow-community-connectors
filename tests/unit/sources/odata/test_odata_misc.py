"""OData connector unit tests — misc group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import logging
import os
import re

import pytest
import responses
from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect
from databricks.labs.community_connector.sources.odata.odata import _odata_literal

from tests.unit.sources.odata._odata_test_helpers import (
    _COLLISION_MD,
    _FK_NULL_MD,
    COLLIDE_METADATA_XML,
    DUNDER_SET_METADATA_XML,
    NONNULL_FLAT_METADATA_XML,
    NONNULL_METADATA_XML,
    PROBE_TABLE,
    R41_INT64_METADATA,
    SERVICE_URL,
    _drop_lb,
    _expand_auto_roots_callback,
    _leaves_or_probe_callback,
    _make,
    _mock_guid_metadata,
    _mock_metadata,
    _mock_multi_metadata,
    _mock_nested_metadata,
    _mock_probe_metadata,
    _patch_sleep,
)

# ---------------------------------------------------------------------------
# Static helpers
# ---------------------------------------------------------------------------


def test_odata_literal_quotes_strings_and_escapes():
    assert _odata_literal("O'Brien") == "'O''Brien'"
    assert _odata_literal(5) == "5"
    assert _odata_literal(True) == "true"


def test_odata_literal_passes_iso_timestamps_bare():
    assert _odata_literal("2024-05-01T00:00:00Z") == "2024-05-01T00:00:00Z"
    # Odd-digit fractions must stay bare too — on Python 3.10 (DBR 13.3 LTS)
    # a bare fromisoformat rejects '.5', which would QUOTE the watermark in
    # $filter and 400 every incremental batch. parse_iso8601 normalizes the
    # digit count so the sniff verdict is version-uniform.
    assert _odata_literal("2024-05-01T00:00:00.5Z") == "2024-05-01T00:00:00.5Z"
    assert _odata_literal("2024-05-01T00:00:00.1234567Z") == "2024-05-01T00:00:00.1234567Z"


def test_odata_literal_percent_encodes_url_reserved_characters():
    """Generated literals ride into URL strings that ``requests`` sends
    without encoding reserved characters: a raw ``+`` is decoded as a SPACE
    by form-decoding servers (a non-UTC ISO watermark → malformed timestamp
    → 400 every batch; ``+`` in a quoted seek boundary → silent wrong
    comparison), ``&`` splits the query, ``#`` truncates the request, and
    ``?`` starts the query when the literal sits in a key-predicate path
    segment. ``odata_literal`` must pre-encode them (requests preserves
    existing escapes, so this decodes correctly server-side)."""
    from datetime import datetime, timedelta, timezone

    # The bug case: a non-UTC ISO watermark string keeps its offset ``+``.
    assert _odata_literal("2025-06-01T12:00:00+10:00") == "2025-06-01T12:00:00%2B10:00"
    # Same via a tz-aware datetime; UTC still normalizes to a bare Z.
    tz10 = timezone(timedelta(hours=10))
    assert _odata_literal(datetime(2025, 6, 1, 12, tzinfo=tz10)) == "2025-06-01T12:00:00%2B10:00"
    assert _odata_literal(datetime(2025, 6, 1, 12, tzinfo=timezone.utc)) == "2025-06-01T12:00:00Z"
    # Reserved characters inside quoted string values.
    assert _odata_literal("A&B") == "'A%26B'"
    assert _odata_literal("A#B") == "'A%23B'"
    assert _odata_literal("AB+1") == "'AB%2B1'"
    assert _odata_literal("A?B") == "'A%3FB'"
    assert _odata_literal("100%") == "'100%25'"
    # Quote doubling still composes with the encoding.
    assert _odata_literal("O'Brien & sons") == "'O''Brien %26 sons'"


@responses.activate
def test_read_table_metadata_snapshot_when_no_cursor():
    _mock_metadata()
    c = _make()
    meta = c.read_table_metadata("Customers", {})
    assert meta == {
        "primary_keys": ["Id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    }


@responses.activate
def test_read_table_metadata_cdc_when_cursor_set():
    _mock_metadata()
    c = _make()
    meta = c.read_table_metadata("Customers", {"cursor_field": "ModifiedAt"})
    assert meta["ingestion_type"] == "cdc"
    assert meta["cursor_field"] == "ModifiedAt"


@responses.activate
def test_unknown_entity_set_raises():
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="not found"):
        c.get_table_schema("Nope", {})


# ---------------------------------------------------------------------------
# Auth wiring
# ---------------------------------------------------------------------------


@responses.activate
def test_bearer_auth_attaches_header():
    _mock_metadata()
    c = _make({"auth_type": "bearer", "token": "abc"})
    # Trigger session creation via list_tables.
    c.list_tables()
    assert c._get_session().headers["Authorization"] == "Bearer abc"


@responses.activate
def test_api_key_custom_header():
    _mock_metadata()
    c = _make(
        {
            "auth_type": "api_key",
            "api_key": "k",
            "api_key_header": "X-My-Key",
        }
    )
    c.list_tables()
    assert c._get_session().headers["X-My-Key"] == "k"


# ---------------------------------------------------------------------------
# 401 / 403 UX when there's no OAuth refresh path
# ---------------------------------------------------------------------------

# The connector never refreshes tokens itself — OAuth is owned by the
# Unity Catalog COMMUNITY connection, which runs the flow, refreshes
# server-side, and injects a fresh ``access_token`` at query time. A
# 401/403 is therefore always terminal for the read, and the raw
# HTTPError gives the operator nothing actionable — the connector
# raises PermissionError with auth-mode-specific remediation instead.


@responses.activate
def test_bearer_401_without_refresh_raises_actionable_permission_error():
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=401,
        json={"error": {"code": "InvalidAuthenticationToken"}},
    )
    c = _make({"auth_type": "bearer", "token": "stale"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    # Diagnostics that triage the failure for a bearer-auth operator
    # without making them dig into the request/response cycle.
    assert "auth_type=bearer" in msg
    assert "expired" in msg
    assert "COMMUNITY OAuth connection" in msg  # suggested upgrade path
    assert "community_oauth_flow" in msg
    assert "InvalidAuthenticationToken" in msg  # server body echoed


@responses.activate
def test_basic_401_without_refresh_raises_actionable_permission_error():
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="denied")
    c = _make({"auth_type": "basic", "username": "u", "password": "p"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "auth_type=basic" in msg
    assert "username" in msg
    assert "password" in msg


@responses.activate
def test_api_key_401_without_refresh_raises_actionable_permission_error():
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="denied")
    c = _make({"auth_type": "api_key", "api_key": "k"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "auth_type=api_key" in msg
    assert "api_key" in msg
    assert "api_key_header" in msg


def test_service_url_with_embedded_credentials_rejected():
    """The service URL is echoed verbatim in logs and error messages on
    every request — embedded userinfo credentials would leak everywhere.
    Reject up front with the remediation (auth_type=basic options)."""
    for bad in (
        "https://user:hunter2@example.com/odata/",
        "https://tokenuser@example.com/odata/",
    ):
        with pytest.raises(ValueError, match="must not embed credentials"):
            _make({"service_url": bad})


@responses.activate
def test_403_on_bearer_raises_permission_error():
    """403 means authenticated-but-not-authorized — an *authorization*
    failure, so the message must point at permissions/scope, not at the
    per-mode token-expiry hints, and must NOT claim "no automatic
    token-refresh path is configured" (false on a UC OAuth connection
    whose principal is simply forbidden)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=403,
        json={"error": {"code": "Forbidden"}},
    )
    c = _make({"auth_type": "bearer", "token": "valid-but-no-scope"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "403" in msg
    assert "not authorized" in msg
    assert "no automatic token-refresh path" not in msg


@responses.activate
def test_uc_injected_access_token_authenticates():
    """A UC COMMUNITY OAuth connection injects ``access_token`` into the
    options at query time (no ``auth_type``). The connector must use it as
    an opaque bearer token — no minting, no refresh, no client creds."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"CustomerID": "A", "CompanyName": "Acme"}]},
    )
    c = _make({"access_token": "uc-injected-token"})
    rows = list(c.read_table("Customers", None, {})[0])
    assert [r["CustomerID"] for r in rows] == ["A"]
    sent = {
        call.request.headers.get("Authorization")
        for call in responses.calls
        if "Customers" in call.request.url
    }
    assert sent == {"Bearer uc-injected-token"}  # every request carried it


def test_auth_type_oauth2_retired_raises_actionable():
    """``auth_type=oauth2`` (the old connector-side minting/refresh mode)
    is retired: the curated error names the UC COMMUNITY OAuth connection,
    the flow option, and the parameters the user must move to."""
    c = _make({"auth_type": "oauth2", "oauth2_client_id": "x", "oauth2_client_secret": "y"})
    with pytest.raises(ValueError) as ei:
        c._get_session()
    msg = str(ei.value)
    assert "retired" in msg
    assert "community_oauth_flow" in msg
    assert "token_endpoint" in msg
    assert "access_token" in msg


@responses.activate
def test_uc_injected_token_401_names_the_connection_layer():
    """A 401 under an injected token must say the token came from the UC
    OAuth connection and that the connection layer refreshes it at query
    start (mid-read expiry → retry gets a fresh token) — not suggest any
    connector-side refresh configuration."""
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="expired")
    c = _make({"access_token": "was-fresh-at-query-start"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "Unity Catalog OAuth connection" in msg
    assert "refreshes the token at query start" in msg
    assert "no automatic token-refresh path" not in msg


def test_unknown_auth_type_names_static_modes_and_uc_oauth():
    """The unknown-auth_type error lists the static modes and points at
    the UC-managed OAuth alternative (oauth2 is gone from the list)."""
    c = _make({"auth_type": "kerberos"})
    with pytest.raises(ValueError) as ei:
        c._get_session()
    msg = str(ei.value)
    assert "bearer" in msg and "basic" in msg and "api_key" in msg
    assert "oauth2" not in msg
    assert "COMMUNITY OAuth connection" in msg


@responses.activate
def test_unknown_namespace_lists_available_entities():
    _mock_multi_metadata()
    c = _make()
    with pytest.raises(ValueError, match=r"namespace 'Nope'"):
        c.get_table_schema("Customers", {"namespace": "Nope"})


@responses.activate
def test_read_table_metadata_picks_correct_primary_key_per_namespace():
    _mock_multi_metadata()
    c = _make()
    sales = c.read_table_metadata("Customers", {"namespace": "Sales"})
    hr = c.read_table_metadata("Customers", {"namespace": "HR"})
    assert sales["primary_keys"] == ["Id"]
    assert hr["primary_keys"] == ["EmployeeId"]


def test_apply_cursor_lookback_returns_bare_iso_string():
    """The floor stays in raw cursor value space — bare ISO text, not an
    escaped OData literal — so client-side row comparisons and the single
    escape at URL build both see consistent input."""
    c = _make()
    c.__dict__["_active_lookback_seconds"] = 3600
    assert c._apply_cursor_lookback("2024-01-02T00:00:00+10:00") == "2024-01-01T23:00:00+10:00"
    assert c._apply_cursor_lookback("2024-01-02T00:00:00Z") == "2024-01-01T23:00:00Z"


def test_rewrite_top_in_url():
    """Inner-collection nextLink continuations inherit the small
    per-level ``$top`` from the original ``$expand`` clause. The
    rewrite helper bumps that ``$top`` so paging through a wide inner
    collection doesn't take 100s of round trips at the dynamic per-
    level value."""
    from databricks.labs.community_connector.sources.odata._contained import (
        rewrite_top_in_url,
    )

    # Bare $top
    assert (
        rewrite_top_in_url("https://x.com/A?$top=10&$skip=100", 1000)
        == "https://x.com/A?$top=1000&$skip=100"
    )
    # URL-encoded %24top
    assert (
        rewrite_top_in_url("https://x.com/A?%24top=10&%24skip=100", 500)
        == "https://x.com/A?%24top=500&%24skip=100"
    )
    # Preserves other params verbatim
    assert (
        rewrite_top_in_url("https://x.com/A?$filter=Id+eq+5&$top=10&$skip=20", 200)
        == "https://x.com/A?$filter=Id+eq+5&$top=200&$skip=20"
    )
    # No $top → unchanged
    assert (
        rewrite_top_in_url("https://x.com/A?$skiptoken=abc", 1000)
        == "https://x.com/A?$skiptoken=abc"
    )


@responses.activate
def test_read_table_disables_cap_when_start_offset_none_and_cap_unset(caplog):
    """Spark's batch reader (``LakeflowBatchReader``) calls
    ``read_table`` with ``start_offset=None`` and discards the
    returned end-offset. ``read_table`` detects that signal and
    raises ``max_records_per_batch`` to a near-infinite sentinel so
    the cap can't fire and the chain drains fully in one call —
    parked ``pending_fetches`` would otherwise be silently dropped.

    Streaming readers always pass a dict (``{}`` initial or parked
    offset), so this override does not touch the streaming path.

    A user-set ``max_records_per_batch`` is **also** overridden in
    batch mode (with a warning), because the discarded offset means a
    cap there can only truncate-and-lose — honouring it would silently
    drop the remainder. Resumable caps only make sense for streaming."""
    _mock_nested_metadata()
    captured: list[dict] = []

    def _spy(self_, table_name, start_offset, table_options):
        captured.append(dict(table_options))
        return iter([]), {}

    c = _make()
    # start_offset=None, cap unset → override applies.
    from databricks.labs.community_connector.sources.odata.odata import (
        _BATCH_UNCAPPED,
        ODataLakeflowConnect,
    )

    original = ODataLakeflowConnect._read_contained_expand
    ODataLakeflowConnect._read_contained_expand = _spy  # type: ignore[assignment]
    try:
        c.read_table("Parents__Children", None, {"expand_contained": "true"})
    finally:
        ODataLakeflowConnect._read_contained_expand = original  # type: ignore[assignment]

    assert captured[0]["max_records_per_batch"] == str(_BATCH_UNCAPPED)

    # start_offset=None AND cap explicitly set → still overridden to the
    # uncapped sentinel, and a warning names the ignored value.
    captured.clear()
    ODataLakeflowConnect._read_contained_expand = _spy  # type: ignore[assignment]
    with caplog.at_level(logging.WARNING):
        try:
            c.read_table(
                "Parents__Children",
                None,
                {"expand_contained": "true", "max_records_per_batch": "50"},
            )
        finally:
            ODataLakeflowConnect._read_contained_expand = original  # type: ignore[assignment]
    assert captured[0]["max_records_per_batch"] == str(_BATCH_UNCAPPED)
    assert any("max_records_per_batch=50 ignored" in r.getMessage() for r in caplog.records)

    # start_offset={} (streaming) → override never applies.
    captured.clear()
    ODataLakeflowConnect._read_contained_expand = _spy  # type: ignore[assignment]
    try:
        c.read_table("Parents__Children", {}, {"expand_contained": "true"})
    finally:
        ODataLakeflowConnect._read_contained_expand = original  # type: ignore[assignment]
    assert "max_records_per_batch" not in captured[0]


def test_combine_filters():
    from databricks.labs.community_connector.sources.odata._contained import (
        combine_filters,
    )

    assert combine_filters(None, None) is None
    assert combine_filters("A", None) == "A"
    assert combine_filters(None, "B") == "B"
    assert combine_filters("A", "B") == "(A) and (B)"
    assert combine_filters("A", None, "C") == "(A) and (C)"


# --- Flat table ---


@responses.activate
def test_flat_filter_at_segment_applies_to_flat_table_read():
    """For a flat (non-contained) table, ``filter_at_<table>`` is
    equivalent to the existing ``filter`` option — both AND into the
    single URL's ``$filter`` clause."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"CustomerID": "ALFKI", "CompanyName": "Alfreds"}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$filter": "CustomerID eq 'ALFKI'",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Customers", json={"value": []})
    c = _make()
    records, _ = c.read_table("Customers", None, {"filter_at_Customers": "CustomerID eq 'ALFKI'"})
    assert len(list(records)) == 1


# --- Errors ---


@responses.activate
def test_filter_at_unknown_segment_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Bogus"):
        records, _ = c.read_table("Parents__Children", None, {"filter_at_Bogus": "Id eq 5"})
        list(records)


@responses.activate
def test_filter_at_out_of_range_index_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="out of range"):
        records, _ = c.read_table("Parents__Children", None, {"filter_at_5": "Id eq 5"})
        list(records)


@responses.activate
def test_lookup_in_type_only_namespace_lists_namespaces_with_entity_sets():
    """When the user picks a type-only namespace (no <EntityContainer>),
    "Available in this namespace: []" is unhelpful. The error should
    list the namespaces that DO contain entity sets so the user can
    pick the right one."""
    type_only_xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="My.Types.V1">
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
    </Schema>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="My.Service.V1">
      <EntityContainer Name="Container">
        <EntitySet Name="Things" EntityType="My.Types.V1.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    responses.get(f"{SERVICE_URL}$metadata", body=type_only_xml, status=200)
    c = _make()
    with pytest.raises(
        ValueError,
        match=r"declares no entity sets.*Namespaces with entity sets:.*My\.Service\.V1",
    ):
        c.read_table_metadata("Things", {"namespace": "My.Types.V1"})


@responses.activate
def test_500_exhausted_error_message_calls_out_request_shape(monkeypatch):
    """After ``max_retries`` consecutive 500s, the raised RuntimeError
    must mention that a deterministic 500 likely points at a request
    shape the source can't handle — e.g. ``$top`` above SCApi's
    per-page cap — and surface the server response body. Without this
    hint the operator chases retry-budget knobs instead of the actual
    cause."""
    _mock_metadata()
    _patch_sleep(monkeypatch)
    server_body = (
        '{"error":{"code":"500","message":"Unexpected server failure. '
        'Error ID: [2026-06-24T05:16:46Z]."}}'
    )
    for _ in range(3):  # max_retries=2 → 3 attempts total
        responses.add(
            responses.GET,
            f"{SERVICE_URL}Customers",
            body=server_body,
            status=500,
        )
    c = _make({"token": "t", "max_retries": "2"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(RuntimeError) as ei:
        list(rows)
    msg = str(ei.value)
    assert "500" in msg
    assert "page_size" in msg  # remediation hint
    assert "Unexpected server failure" in msg  # body echoed


@responses.activate
def test_verbose_http_logging_off_by_default_no_info_logs(caplog):
    """Without ``verbose_http_logging=true``, per-request INFO logs
    must not appear. Diagnostic noise should be opt-in — every request
    in a streaming pipeline shouldn't flood the log stream by
    default."""
    import logging as _logging

    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"value": [{"Id": 1}]})
    c = _make({"token": "t"})
    with caplog.at_level(
        _logging.INFO, logger="databricks.labs.community_connector.sources.odata.odata"
    ):
        rows, _ = c.read_table("Customers", None, {})
        list(rows)
    info_lines = [r.getMessage() for r in caplog.records if r.levelno == _logging.INFO]
    assert not any("OData GET" in m for m in info_lines)


@responses.activate
def test_verbose_http_logging_on_emits_request_and_response(caplog):
    """``verbose_http_logging=true`` emits one INFO line per request
    URL and one INFO line per response (status + body snippet). Used
    for triaging silent partial-data or under-row-count problems
    against flaky upstream sources."""
    import logging as _logging

    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 42, "Name": "Acme"}]},
    )
    c = _make({"token": "t", "verbose_http_logging": "true"})
    with caplog.at_level(
        _logging.INFO, logger="databricks.labs.community_connector.sources.odata.odata"
    ):
        rows, _ = c.read_table("Customers", None, {})
        list(rows)
    messages = [r.getMessage() for r in caplog.records]
    # Outgoing request URL line.
    assert any("OData GET" in m and "/Customers" in m for m in messages)
    # Response line includes status + body snippet (we just need the
    # source row to be visible somewhere in the log stream).
    assert any("→ 200" in m for m in messages)
    assert any('"Id": 42' in m or "Id': 42" in m or "Acme" in m for m in messages)


@responses.activate
def test_json_decode_error_exhausted_includes_body_in_message(monkeypatch):
    """After max_retries exhausted JSON decode failures, the raised
    JSONDecodeError must include the offending URL + a truncated
    response body so the operator can escalate to the upstream owner
    with concrete evidence — not just the bare "Expecting property
    name" parser message."""
    import requests as _requests

    _mock_metadata()
    _patch_sleep(monkeypatch)
    body = "{<unexpected-html-error-page-from-proxy>"
    for _ in range(3):  # max_retries=2 → 3 attempts total
        responses.add(
            responses.GET,
            f"{SERVICE_URL}Customers",
            body=body,
            status=200,
            content_type="application/json",
        )
    c = _make({"token": "t", "max_retries": "2"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(_requests.exceptions.JSONDecodeError) as ei:
        list(rows)
    msg = str(ei.value)
    assert f"{SERVICE_URL}Customers" in msg
    assert "Server response body" in msg
    assert "<unexpected-html-error-page-from-proxy>" in msg


@responses.activate
def test_400_error_message_includes_server_body():
    """4xx that the retry layer doesn't handle (anything other than
    401/403/429/503) must surface the server's response body in the
    raised exception — otherwise downstream pipeline logs show a
    cryptic ``400 Client Error: Bad Request for url ...`` with no
    indication of *why* the server rejected the request."""
    import requests as _requests

    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"error": {"code": "BadRequest", "message": "Page size 1000 exceeds maximum 500"}},
        status=400,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(_requests.HTTPError) as ei:
        list(rows)
    msg = str(ei.value)
    assert "400" in msg
    assert "Page size 1000 exceeds maximum 500" in msg
    assert SERVICE_URL in msg


def test_generated_bundle_registers_and_connector_survives_cloudpickle():
    """The merged single-file bundle is the artifact that actually deploys
    (SDP pipelines can't import package modules), yet the unit suite runs
    against the modules. Execute the bundle for real: register against a
    fake Spark, instantiate the connector, spot-check behavioral parity,
    and cloudpickle-round-trip the connector — which is what PySpark does
    to ship readers to executors. In the bundle every class is
    function-local, so cloudpickle serializes it BY VALUE, walking closure
    cells: a module-level ``itertools.count`` there is a TypeError on
    Python >= 3.14 (this venv) that the module-layout tests can never see."""
    import os
    import types

    import databricks.labs.community_connector.sources.odata as odata_pkg
    from pyspark import cloudpickle

    bundle_path = os.path.join(
        os.path.dirname(odata_pkg.__file__), "_generated_odata_python_source.py"
    )
    ns: dict = {"__name__": "_odata_bundle_under_test"}
    with open(bundle_path, encoding="utf-8") as fh:
        exec(compile(fh.read(), bundle_path, "exec"), ns)  # pylint: disable=exec-used

    captured: dict = {}
    fake_spark = types.SimpleNamespace(
        dataSource=types.SimpleNamespace(register=lambda cls: captured.setdefault("cls", cls))
    )
    ns["register_lakeflow_source"](fake_spark)
    source_cls = captured["cls"]

    ds = source_cls({"service_url": SERVICE_URL})
    connector = ds.lakeflow_connect
    assert type(connector).__name__ == "ODataLakeflowConnect"
    assert connector.service_url == SERVICE_URL
    # Behavioral parity spot-checks running the BUNDLE's own code: the
    # round-11 literal encoding through the bundle's _cursor_filter, and
    # the userinfo rejection through the bundle's __init__.
    assert (
        connector._cursor_filter("F", "2025-06-01T12:00:00+10:00")
        == "F gt 2025-06-01T12:00:00%2B10:00"
    )
    with pytest.raises(ValueError, match="must not embed credentials"):
        source_cls({"service_url": "https://user:secret@example.com/odata/"})

    # The executor-shipping round trip: by-value class serialization.
    clone = cloudpickle.loads(cloudpickle.dumps(connector))
    assert clone.service_url == SERVICE_URL
    assert type(clone).__name__ == "ODataLakeflowConnect"


@responses.activate
def test_plain_get_fallback_leaves_continuation_links_untouched():
    """The plain-GET fall-back injects the default ``$top`` only into fresh
    collection URLs. A server-issued continuation (``$skiptoken``/``$skip``) —
    which can reach the fall-back when the ``$batch`` give-up sentinel fires
    after a nextLink was re-queued — is used AS-IS (OData v4 §11.2.5.7):
    appending an option to an opaque skiptoken URL can 400 or corrupt the
    server's paging state."""
    seen: list[str] = []

    def _cb(request):
        seen.append(request.url)
        return (200, {"Content-Type": "application/json"}, json.dumps({"value": []}))

    responses.add_callback(
        responses.GET, re.compile(rf"{re.escape(SERVICE_URL)}Parents\(1\)/Children.*"), callback=_cb
    )
    c = _make()
    c.__dict__["_pagination"] = "auto"  # client-driven mode → injection active
    c._get_as_batch_response(f"{SERVICE_URL}Parents(1)/Children")
    c._get_as_batch_response(f"{SERVICE_URL}Parents(1)/Children?$skiptoken=opaque-42")
    fresh = [u for u in seen if "skiptoken" not in u]
    continuations = [u for u in seen if "skiptoken" in u]
    assert fresh and all("$top=" in u for u in fresh)  # fresh URL: $top injected
    assert continuations and all("$top=" not in u for u in continuations)  # as-is


def test_scrub_nonauto_strips_offset_and_purges_server_wide_batch_cache():
    """The offset scrub owns two things: (1) strip every non-``auto`` verdict
    from the outgoing offset; (2) purge the SERVER-WIDE ``$batch`` verdicts from
    the shared cache, but only on the transition (the offset still carries them)
    — conservative, since they aren't table-scoped and a sibling table may have
    a live ``auto`` consumer. The per-table verdicts are purged elsewhere (see
    ``_purge_nonauto_table_verdicts``), so scrub must leave them in the cache."""
    c = _make()
    c._store_capability("expand_ok", True, table_name=PROBE_TABLE)
    c._store_capability("batch_ok", True)
    pinned = {"expand_contained": "false", "contained_fetch": "single", "cursor_probe": "false"}

    # Offset always stripped of the pinned keys.
    assert c._scrub_nonauto_verdicts({"cursor": "x", "expand_ok": True}, pinned) == {"cursor": "x"}
    # Per-table ``expand_ok`` is NOT the offset scrub's job → left in the cache.
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is True

    # Steady state (no batch verdict in the offset) → server-wide cache kept.
    assert c._scrub_nonauto_verdicts({"cursor": "x"}, pinned) == {"cursor": "x"}
    assert c._cached_capability("batch_ok") is True

    # Transition (offset carries batch_ok) → server-wide cache purged.
    assert c._scrub_nonauto_verdicts({"cursor": "x", "batch_ok": True}, pinned) == {"cursor": "x"}
    assert c._cached_capability("batch_ok") is None


@responses.activate
def test_is_partitioned_expand_auto_follows_preflight_verdict():
    """``expand_contained=auto`` partition activation follows the RESOLVED
    shape: a verified server (expand read, no fan-out) is not partitioned;
    explicit ``true`` never is; explicit ``false``/unset always may be."""
    _mock_probe_metadata()
    tree = {
        "value": [
            {
                "Id": 1,
                "Mids": [
                    {
                        "Id": 10,
                        "Leaves": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    }
                ],
            }
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda request: (200, {}, json.dumps(tree))
    )
    c = _make({"expand_contained": "auto"})
    assert c.is_partitioned(PROBE_TABLE) is False  # preflight verified → expand shape
    # The batch get_partitions reuses the cached verdict → serial deferral.
    assert c.get_partitions(PROBE_TABLE, {"expand_contained": "auto"}) == [{}]
    assert _make({"expand_contained": "true"}).is_partitioned(PROBE_TABLE) is False
    assert _make().is_partitioned(PROBE_TABLE) is False  # unset default = auto → verified
    assert _make({"expand_contained": "false"}).is_partitioned(PROBE_TABLE) is True


@responses.activate
def test_is_partitioned_expand_auto_fallback_stays_partitionable():
    """When the ``auto`` preflight fails (server ignores ``$expand``), the
    table resolves to the N+1 shape and KEEPS its partitioned parallelism —
    both activation and the batch ``get_partitions`` fan-out."""
    _mock_probe_metadata()
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=_expand_auto_roots_callback()
    )
    # Preflight cross-check finds real children → definitive ignored-$expand.
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    c = _make({"expand_contained": "auto"})
    assert c.is_partitioned(PROBE_TABLE) is True
    parts = c.get_partitions(PROBE_TABLE, {"expand_contained": "auto"})
    assert parts and "top_parent_rows" in parts[0]  # real partition fan-out


@responses.activate
def test_auto_retains_recorded_preflight_verdicts():
    """``auto`` (default) keeps its recorded verdicts in the offset so a
    recreated reader skips the preflight — the counterpart to the scrub."""
    _mock_probe_metadata()
    c = _make()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    _, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z", "batch_ok": True, "batch_size_ok": 200},
        {"cursor_field": "RecordLastModified", "cursor_probe": "auto", "pagination": "nextlink"},
    )
    # Both options default/auto → the seeded verdicts survive.
    assert offset.get("batch_ok") is True
    assert offset.get("batch_size_ok") == 200


@responses.activate
def test_or_filter_preflight_falls_back_to_skip_when_rejected():
    """When the composite keyset seek's OR-across-columns probe is rejected
    (400), the walk drops to `$skip` paging (mode B) instead — no data lost,
    no crash."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    seen = {"or_probe": 0, "keyset_seek": 0, "skip_seek": 0}
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        callback=_leaves_or_probe_callback(seen, reject_or=True),
    )
    c = _make()
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z"},
        {"cursor_field": "RecordLastModified", "cursor_probe": "false", "pagination": "auto"},
    )
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert seen["or_probe"] == 1  # OR preflight fired and was rejected
    assert seen["skip_seek"] >= 1  # fell back to $skip (mode B)
    assert seen["keyset_seek"] == 0  # never issued the rejected OR seek for real


@responses.activate
def test_or_filter_preflight_uses_keyset_when_supported():
    """When the OR probe succeeds, the walk uses the composite keyset seek as
    before (no `$skip` fallback)."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    seen = {"or_probe": 0, "keyset_seek": 0, "skip_seek": 0}
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        callback=_leaves_or_probe_callback(seen, reject_or=False),
    )
    c = _make()
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z"},
        {"cursor_field": "RecordLastModified", "cursor_probe": "false", "pagination": "auto"},
    )
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert seen["or_probe"] == 1  # probe fired
    assert seen["keyset_seek"] >= 1  # used the keyset OR seek
    assert seen["skip_seek"] == 0  # no $skip fallback


@responses.activate
def test_or_filter_probe_transient_fails_open_without_persisting():
    """A transient (429/5xx) on the OR-across-columns probe is NOT evidence
    about OR support: fail OPEN (True) for this seek and record NOTHING (no
    instance verdict, no shared-cache verdict), so the next seek re-probes
    instead of durably pinning the slower $skip walk on a momentary throttle."""
    calls = {"n": 0}

    def _cb(_request):
        calls["n"] += 1
        return (429, {}, json.dumps({"error": "slow down"}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Coll", callback=_cb)
    c = _make()
    assert c._verify_or_filter_support(f"{SERVICE_URL}Coll", ["a", "b"], {"a": 1, "b": 2}) is True
    assert calls["n"] == 1  # probed once (single attempt, no retry storm)
    assert "_or_filter_ok" not in c.__dict__  # nothing cached on the instance
    assert c._cached_capability("or_filter_ok") is None  # nothing persisted


@responses.activate
def test_or_filter_probe_408_is_transient_not_a_verdict():
    """A 408 (request timeout) is transient like 429/5xx but sits outside the
    retry set — it must still fail OPEN and record nothing. Pre-fix it fell
    through to the 4xx test and persisted a definitive or_filter_ok=False,
    which has NO reset path: one timeout durably pinned the $skip walk."""
    responses.add_callback(responses.GET, f"{SERVICE_URL}Coll", callback=lambda _r: (408, {}, ""))
    c = _make()
    assert c._verify_or_filter_support(f"{SERVICE_URL}Coll", ["a", "b"], {"a": 1, "b": 2}) is True
    assert "_or_filter_ok" not in c.__dict__  # nothing cached on the instance
    assert c._cached_capability("or_filter_ok") is None  # nothing persisted


@responses.activate
def test_or_filter_probe_auth_401_not_mislabeled_as_unsupported():
    """A 401 (expired token) on the OR probe must NOT be read as 'OR
    unsupported'. Routed through the auth-aware _http_get_once, a 401 without an
    OAuth refresh path raises PermissionError, which fails open (True) and
    records nothing — rather than the pre-fix raw session.get that treated the
    401 as a definitive 4xx rejection and pinned $skip."""
    responses.add_callback(responses.GET, f"{SERVICE_URL}Coll", callback=lambda _r: (401, {}, ""))
    c = _make()  # bearer auth → no OAuth refresh path
    assert c._verify_or_filter_support(f"{SERVICE_URL}Coll", ["a", "b"], {"a": 1, "b": 2}) is True
    assert "_or_filter_ok" not in c.__dict__
    assert c._cached_capability("or_filter_ok") is None


@responses.activate
def test_or_filter_probe_definitive_400_still_falls_back_and_persists():
    """Regression: a genuine non-transient 4xx (the 'only AND operators are
    supported' 400) is still a definitive rejection — cached False on the
    instance AND persisted to the shared cache so later seeks skip the probe."""
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Coll",
        callback=lambda _r: (400, {}, json.dumps({"error": "only AND operators are supported"})),
    )
    c = _make()
    assert c._verify_or_filter_support(f"{SERVICE_URL}Coll", ["a", "b"], {"a": 1, "b": 2}) is False
    assert c.__dict__["_or_filter_ok"] is False
    assert c._cached_capability("or_filter_ok") is False


def test_scrub_batch_verdicts_kept_while_auto_consumer_live():
    """The shared ``$batch`` verdicts (``batch_ok`` / ``batch_size_ok``) survive
    a pinned ``contained_fetch`` as long as the ``cursor_probe`` auto cascade
    still consumes them; they are scrubbed only when every consumer is pinned
    non-auto or the hydrate is suppressed by an explicit ``single``."""
    c = _make()
    off = {"cursor": "x", "batch_ok": True, "batch_size_ok": 200}
    # contained_fetch pinned, but default cursor_probe (auto) still consumes
    # and refreshes the verdicts → kept (no per-microbatch re-discovery churn).
    assert c._scrub_nonauto_verdicts(off, {"contained_fetch": "batch:200"}) == off
    # Explicit single suppresses the auto hydrate → no live consumer → scrub.
    assert c._scrub_nonauto_verdicts(off, {"contained_fetch": "single"}) == {"cursor": "x"}
    # Every consumer pinned non-auto → scrub.
    assert c._scrub_nonauto_verdicts(
        off, {"contained_fetch": "batch", "cursor_probe": "false"}
    ) == {"cursor": "x"}
    # contained_fetch auto keeps the batch verdicts regardless of cursor_probe.
    assert c._scrub_nonauto_verdicts(off, {"cursor_probe": "false"}) == off


@responses.activate
def test_string_key_iso_lookalike_stays_quoted():
    """The inverse hole: an ``Edm.String`` key whose VALUE happens to look
    ISO-8601 (``"2024-01-01"``) passed the bare-timestamp sniff and rendered
    UNQUOTED — an invalid key predicate for a string-typed key."""
    from urllib.parse import unquote

    _mock_guid_metadata()
    responses.get(
        f"{SERVICE_URL}DayBatches", json={"value": [{"Day": "2024-01-01"}]}, match_querystring=False
    )
    responses.get(
        f"{SERVICE_URL}DayBatches('2024-01-01')/Items",
        json={"value": [{"Id": 7}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table(
        "DayBatches__Items", {}, {"contained_fetch": "single", "pagination": "nextlink"}
    )
    assert [r["Id"] for r in recs] == [7]
    urls = [unquote(call.request.url) for call in responses.calls]
    assert any("DayBatches('2024-01-01')/Items" in u for u in urls)


def test_odata_literal_numeric_and_slash_edges():
    """Exponent ``+`` percent-encoded (form-decoding servers read a raw
    ``+`` as a space), non-finite floats use the OData spellings, and ``/``
    in a string literal can't split a path segment."""
    assert _odata_literal(1e20) == "1e%2B20"
    assert _odata_literal(float("inf")) == "INF"
    assert _odata_literal(float("-inf")) == "-INF"
    assert _odata_literal(float("nan")) == "NaN"
    assert _odata_literal("A/B") == "'A%2FB'"


@responses.activate
def test_post_batch_corrupt_200_then_error_surfaces_status():
    """Round-28: when the corrupt-200 re-POST comes back a real 4xx, the
    status handling must repeat — a plain 400 carries its status/body (not a
    misleading "missing sub-response id"), and a "too many parts" 400 still
    raises the adaptive-shrink trigger."""
    from databricks.labs.community_connector.sources.odata._contained import _BatchTooManyParts

    state = {"n": 0}

    def _cb_plain_400(request):
        state["n"] += 1
        if state["n"] == 1:
            return (200, {"Content-Type": "application/json"}, "{trunc")
        return (400, {}, json.dumps({"error": {"message": "bad request"}}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb_plain_400)
    c = _make()
    with pytest.raises(RuntimeError, match="failed: 400"):
        c._post_batch([f"{SERVICE_URL}Roots"])

    responses.reset()
    state["n"] = 0

    def _cb_too_many(request):
        state["n"] += 1
        if state["n"] == 1:
            return (200, {"Content-Type": "application/json"}, "{trunc")
        return (400, {}, json.dumps({"error": {"message": "contains too many parts"}}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb_too_many)
    with pytest.raises(_BatchTooManyParts):
        c._post_batch([f"{SERVICE_URL}Roots"])


@responses.activate
def test_select_omitting_pk_or_cursor_raises():
    """A user select that strips the PK desyncs schema from
    read_table_metadata's MERGE keys; one that strips the cursor_field
    silently re-reads the whole table forever under coalesce. Both raise."""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="primary-key"):
        c.read_table(
            "Customers",
            {"cursor": "x"},
            {"cursor_field": "ModifiedAt", "select": "Name,ModifiedAt"},
        )
    with pytest.raises(ValueError, match="cursor_field"):
        c.read_table(
            "Customers", {"cursor": "x"}, {"cursor_field": "ModifiedAt", "select": "Id,Name"}
        )


def test_connection_int_options_curated_validation():
    """Connection-level numerics get the same curated validation as the
    per-table numeric options — a negative max_retries previously made the
    retry loops run zero iterations (UnboundLocalError on resp)."""
    for key, bad in (
        ("max_retries", "-1"),
        ("timeout_seconds", "0"),
        ("timeout_seconds", "abc"),
        ("retry_max_delay_seconds", "-5"),
    ):
        with pytest.raises(ValueError, match=key):
            ODataLakeflowConnect({"service_url": SERVICE_URL, key: bad})


def test_or_filter_ok_scrubbed_on_explicit_nonconsuming_pagination():
    """`or_filter_ok` previously had NO reset path — a wrongly-false verdict
    (e.g. persisted by a pre-typed-seek build's quoted-guid probe) pinned the
    fragile $skip walk forever. An explicit pagination mode that never
    consumes the verdict (skip / nextlink) now scrubs it, giving checkpoints
    an escape hatch."""
    c = _make()
    off = {"cursor": "x", "or_filter_ok": False}
    c.__dict__["_or_filter_ok"] = False
    assert c._scrub_nonauto_verdicts(dict(off), {"pagination": "skip"}) == {"cursor": "x"}
    assert "_or_filter_ok" not in c.__dict__  # instance memo cleared too
    c.__dict__["_or_filter_ok"] = False
    assert c._scrub_nonauto_verdicts(dict(off), {"pagination": "nextlink"}) == {"cursor": "x"}
    # Modes that CONSUME the verdict keep it.
    assert c._scrub_nonauto_verdicts(dict(off), {"pagination": "keyset"})["or_filter_ok"] is False
    assert c._scrub_nonauto_verdicts(dict(off), {"pagination": "auto"})["or_filter_ok"] is False
    assert c._scrub_nonauto_verdicts(dict(off), {})["or_filter_ok"] is False


def test_private_tmp_write_refuses_preplanted_symlink(tmp_path, monkeypatch):
    """Cache writers previously opened a PREDICTABLE `{path}.{pid}.tmp` name
    with plain open() — a pre-planted symlink there redirected the write onto
    any victim-writable file (and os.replace then hid the evidence). The tmp
    name now embeds os.urandom and opens O_CREAT|O_EXCL|O_NOFOLLOW, so a
    planted name makes the write fail instead of following the link."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    victim = tmp_path / "victim.txt"
    victim.write_bytes(b"precious")
    target = tmp_path / "cache.json"
    real_urandom = os.urandom
    # Deterministic tmp name so the attacker (this test) can pre-plant it.
    monkeypatch.setattr(odata_mod.os, "urandom", lambda n: b"\x00" * n)
    planted = f"{target}.{odata_mod.os.getpid()}.{'00000000'}.tmp"
    odata_mod.os.symlink(victim, planted)
    try:
        assert odata_mod._replace_with_private_tmp(str(target), b"attacker-view") is False
        assert victim.read_bytes() == b"precious"  # not clobbered
        assert not target.exists()
    finally:
        if odata_mod.os.path.lexists(planted):
            odata_mod.os.remove(planted)

    # Clean path: write lands atomically with owner-only permissions.
    monkeypatch.setattr(odata_mod.os, "urandom", real_urandom)
    assert odata_mod._replace_with_private_tmp(str(target), b"payload") is True
    assert target.read_bytes() == b"payload"
    assert (target.stat().st_mode & 0o777) == 0o600


@responses.activate
def test_flat_entity_set_with_double_underscore_is_readable():
    """CSDL SimpleIdentifiers legally allow consecutive underscores, so
    list_tables can emit `My__Set` — but the read path previously split it
    into a nonexistent containment path and failed with a misleading
    "Entity set 'My' not found". A verbatim flat declaration now wins over
    the containment-path interpretation everywhere."""
    responses.get(f"{SERVICE_URL}$metadata", body=DUNDER_SET_METADATA_XML, status=200)
    responses.get(
        f"{SERVICE_URL}My__Set",
        json={"value": [{"Id": 1, "Name": "x"}]},
    )
    c = _make()
    assert "My__Set" in c.list_tables()
    assert {f.name for f in c.get_table_schema("My__Set", {}).fields} == {"Id", "Name"}
    rows, _ = c.read_table("My__Set", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in rows] == [1]


@responses.activate
def test_flat_set_shadows_colliding_containment_path_and_listing_dedups():
    """When a declared flat `My__Set` collides with the containment-path
    spelling of `My`→`Set`, the flat set wins (documented shadowing) and
    namespace listings must not fabricate a duplicate table entry."""
    responses.get(f"{SERVICE_URL}$metadata", body=COLLIDE_METADATA_XML, status=200)
    c = _make()
    listed = c.list_tables_in_namespace(["D"])
    assert listed.count("My__Set") == 1
    assert c._table_segments("My__Set") is None  # flat wins


@responses.activate
def test_malformed_delta_entry_raises_precise_error():
    """A null entry in a delta `value` array previously died with an
    AttributeError inside the tombstone sniff; it now raises a precise
    RuntimeError naming the malformed entry."""
    _mock_metadata()
    delta_link = f"{SERVICE_URL}Customers?$deltatoken=t1"
    responses.add(
        responses.GET,
        delta_link,
        json={"value": [None], "@odata.deltaLink": f"{SERVICE_URL}Customers?$deltatoken=t2"},
    )
    c = _make()
    with pytest.raises(RuntimeError, match="malformed entry"):
        records, _ = c.read_table(
            "Customers", {"delta_link": delta_link}, {"delta_tracking": "enabled"}
        )
        list(records)


@responses.activate
def test_select_restricted_delta_tombstone_pads_to_selected_schema():
    """With `select`, the tombstone pad set and the declared schema must be
    the SAME subset: selected non-key columns padded to None, non-selected
    columns absent from both — the framework parse is the referee."""
    from databricks.labs.community_connector.libs.utils import parse_value

    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_METADATA_XML, status=200)
    delta_link = f"{SERVICE_URL}Customers?$deltatoken=t1"
    responses.add(
        responses.GET,
        delta_link,
        json={
            "value": [{"Id": 3, "@removed": {"reason": "deleted"}}],
            "@odata.deltaLink": f"{SERVICE_URL}Customers?$deltatoken=t2",
        },
    )
    c = _make()
    opts = {"delta_tracking": "enabled", "select": "Id,Name"}
    schema = c.get_table_schema("Customers", opts)
    assert {f.name for f in schema.fields} == {"Id", "Name", "_deleted", "_lc_sequence"}
    records, _ = c.read_table("Customers", {"delta_link": delta_link}, opts)
    (tombstone,) = list(records)
    assert tombstone["Name"] is None and "ModifiedAt" not in tombstone
    parsed = parse_value(tombstone, schema)
    assert parsed["Id"] == 3 and parsed["_deleted"] is True


# ---------------------------------------------------------------------------
# Round-34 fixes: cross-origin credential guard, $batch non-dict body re-issue,
# partitioned contained_fetch validation
# ---------------------------------------------------------------------------


@responses.activate
def test_cross_host_nextlink_refused_not_credential_leak():
    """A server-supplied @odata.nextLink pointing at a DIFFERENT host must be
    refused — following it would send the session's Authorization header to
    that host (requests' own cross-host redirect auth-stripping never engages
    because the connector builds the next request directly). Same-origin
    nextLinks still work."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={
            "value": [{"Id": 1, "Name": "A"}],
            "@odata.nextLink": "https://evil.attacker.com/collect?p=2",
        },
        match_querystring=False,
    )
    c = _make({"token": "secret-xyz"})
    with pytest.raises(PermissionError, match="different origin"):
        rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
        list(rows)
    # The attacker host was never contacted.
    assert not any("evil.attacker.com" in call.request.url for call in responses.calls)


# ---------------------------------------------------------------------------
# Round-35 fixes: cross-host redirect credential guard, $batch error-body
# re-issue, non-delta schema padding
# ---------------------------------------------------------------------------


@responses.activate
def test_cross_host_redirect_refused_not_credential_leak():
    """Round 34 guarded the @odata.nextLink vector but session.request
    followed 3xx redirects internally with the credential attached — and
    requests strips only Authorization cross-host, leaking api_key /
    extra_headers. allow_redirects=False + the origin check now refuses an
    off-host Location."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=302,
        headers={"Location": "https://evil.attacker.com/collect"},
    )
    captured = {}

    def _evil(request):
        captured["x-api-key"] = request.headers.get("x-api-key")
        captured["X-Tenant"] = request.headers.get("X-Tenant")
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, "https://evil.attacker.com/collect", callback=_evil)
    c = _make(
        {
            "auth_type": "api_key",
            "api_key": "SECRET-APIKEY",
            "extra_headers": "X-Tenant:SECRET-TENANT",
        }
    )
    with pytest.raises(PermissionError, match="different origin"):
        rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
        list(rows)
    # The attacker host never received the request (never followed).
    assert captured == {}
    assert not any("evil.attacker.com" in call.request.url for call in responses.calls)


@responses.activate
def test_non_delta_read_pads_omitted_nonnullable_property():
    """A non-delta read no longer hard-fails when the server omits a
    Nullable="false" property (OData servers may omit null-valued
    properties) — the emit boundary pads it to explicit None, which
    parse_value accepts, so the row flows instead of killing the batch."""
    from databricks.labs.community_connector.libs.utils import parse_value

    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_FLAT_METADATA_XML, status=200)
    responses.get(f"{SERVICE_URL}Items", json={"value": [{"Id": 2, "Opt": "y"}]})  # Req omitted
    c = _make({"token": "t"})
    schema = c.get_table_schema("Items", {})
    rows, _ = c.read_table("Items", None, {"pagination": "nextlink"})
    (row,) = list(rows)
    assert row["Req"] is None and row["Opt"] == "y"
    # And the padded row parses against the (non-nullable Req) schema.
    parsed = parse_value(row, schema)
    assert parsed["Id"] == 2 and parsed["Req"] is None


@responses.activate
def test_data_url_3xx_without_location_curated_error():
    """A 3xx the same-origin follow loop can't act on (no Location header)
    must raise naming the status — not flow onward and die as a bare
    'Expecting value' JSON parse error on the empty body."""
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=302)
    c = _make({"token": "t"})
    with pytest.raises(RuntimeError, match="HTTP 302 without a followable"):
        rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
        list(rows)


def test_api_key_header_stripped_and_empty_defaults():
    """A padded api_key_header is stripped (requests raises an uncurated
    InvalidHeader on whitespace); an explicitly-empty one falls back to the
    documented x-api-key default instead of an empty header name."""
    c = _make({"auth_type": "api_key", "api_key": "k", "api_key_header": " X-My-Key "})
    assert c._get_session().headers["X-My-Key"] == "k"
    c2 = _make({"auth_type": "api_key", "api_key": "k", "api_key_header": ""})
    assert c2._get_session().headers["x-api-key"] == "k"


def test_api_key_header_invalid_raises_curated():
    """Garbage header names fail fast with the option named, not at first
    request deep inside requests."""
    c = _make({"auth_type": "api_key", "api_key": "k", "api_key_header": "bad header"})
    with pytest.raises(ValueError, match="api_key_header"):
        c._get_session()


@responses.activate
def test_select_star_keeps_full_schema():
    """``select=*`` is valid OData (§11.2.4.2.1: all structural properties)
    and passes option validation — the schema must stay unfiltered too. The
    literal-name filter used to drop every non-FK column (flat tables then
    failed the non-empty-schema check outright)."""
    _mock_metadata()
    c = _make({"token": "t"})
    schema = c.get_table_schema("Customers", {"select": "*"})
    assert [f.name for f in schema.fields] == ["Id", "Name", "ModifiedAt"]


def test_verbose_http_logging_strict_parse():
    """Enum-strict like every other option: a typo'd "1"/"yes" would
    otherwise silently mean OFF — the opposite of what a triaging user
    asked for."""
    with pytest.raises(ValueError, match="verbose_http_logging"):
        _make({"token": "t", "verbose_http_logging": "1"})


def test_service_url_query_or_fragment_rejected():
    """A query-carrying service root (SAP Gateway '?sap-client=100') breaks
    every built URL (the entity path lands inside the query) and used to die
    as a bare $metadata ParseError. It must fail at construction with the
    header-form alternative named."""
    with pytest.raises(ValueError, match="sap-client"):
        _make({"service_url": "https://example.com/odata?sap-client=100"})
    with pytest.raises(ValueError, match="query string or fragment"):
        _make({"service_url": "https://example.com/odata#frag"})


@responses.activate
def test_flat_select_whitespace_stripped_on_wire():
    """User whitespace in ``select`` ("Id, ModifiedAt") is stripped on the
    flat wire path — the validation set and the expand-leaf merge already
    strip, and a strict server may 400 the padded $select=Id,%20ModifiedAt
    form."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"value": []}, match_querystring=False)
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {"select": "Id, ModifiedAt"})
    list(rows)
    data_urls = [call.request.url for call in responses.calls if "Customers" in call.request.url]
    assert data_urls
    for u in data_urls:
        select_param = u.split("$select=")[1].split("&")[0]
        assert select_param == "Id,ModifiedAt"


def test_service_url_bare_trailing_question_rejected():
    """A bare trailing '?' has an EMPTY urlparse query but still corrupts
    every built URL (svc?/Customers) — the raw-char check catches it."""
    with pytest.raises(ValueError, match="query string or fragment"):
        _make({"service_url": "https://example.com/odata?"})


@responses.activate
def test_select_strips_to_empty_omits_param():
    """select=',' is rejected upstream by the PK validation (it strips to no
    columns, so the PKs are 'omitted') — and the wire builder independently
    guards the empty case: a select that strips to nothing emits no $select
    param at all rather than an empty '$select='."""
    _mock_metadata()
    c = _make({"token": "t"})
    with pytest.raises(ValueError, match="omits primary-key"):
        c.read_table("Customers", None, {"select": ","})
    # Defense-in-depth at the URL builder (other callers bypass validation).
    assert "$select" not in c._format_query_params({"select": ", ,"})
    assert "$select=Id" in c._format_query_params({"select": " Id ,"})


@responses.activate
def test_flat_cursor_survives_permanent_int_to_string_rendering_switch():
    """End-to-end: int checkpoint {"cursor": 5000} + a server now rendering
    the cursor as strings — rows must emit and the watermark must advance
    (pre-fix: every batch dropped all rows client-side, offset frozen)."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 7, "Name": "N", "ModifiedAt": "6000"},
                {"Id": 8, "Name": "M", "ModifiedAt": "7000"},
            ]
        },
        match_querystring=False,
    )
    c = _make({"token": "t"})
    rows, offset = c.read_table(
        "Customers", {"cursor": 5000}, {"cursor_field": "ModifiedAt", "pagination": "nextlink"}
    )
    # Row 8 is the designed same-cursor boundary trim (re-fetched next batch
    # via gt "6000"); the stall signature was ZERO rows and a frozen cursor.
    assert [r["Id"] for r in rows] == [7]
    assert offset["cursor"] == "6000"


@responses.activate
def test_flat_cursor_filter_typed_rendering_numeric_string_watermark():
    """A numeric-string watermark against an Edm.Int64-declared cursor
    renders BARE on the wire (Seq gt 7000) — the untyped sniff quoted it
    (Seq gt '7000'), which strict servers 400. This is batch 2 of the
    round-40 rendering-switch scenario."""
    from urllib.parse import unquote

    responses.get(f"{SERVICE_URL}$metadata", body=R41_INT64_METADATA, status=200)
    responses.get(f"{SERVICE_URL}Events", json={"value": []}, match_querystring=False)
    c = _make({"token": "t"})
    rows, _ = c.read_table(
        "Events", {"cursor": "7000"}, {"cursor_field": "Seq", "pagination": "nextlink"}
    )
    list(rows)
    data_urls = [
        unquote(call.request.url) for call in responses.calls if "Events" in call.request.url
    ]
    assert data_urls and all("Seq gt 7000" in u for u in data_urls)
    assert not any("'7000'" in u for u in data_urls)


def test_extra_headers_invalid_name_fails_eagerly():
    """Header NAMES in extra_headers get the same eager RFC 7230 token check
    as api_key_header — http.client's send-time regex tolerates interior
    spaces, so a malformed name used to go out on the wire as-is and fail at
    a strict server with nothing pointing at the option."""
    c = _make({"extra_headers": "bad name: 1", "token": "t"})
    with pytest.raises(ValueError, match="extra_headers"):
        c._get_session()
    ok = _make({"extra_headers": "sap-client: 100", "token": "t"})
    assert ok._get_session().headers["sap-client"] == "100"


def test_service_url_malformed_port_curated_error():
    """urlparse defers port validation to the ``.port`` accessor, so a
    malformed port used to escape as a bare "Port could not be cast to
    integer value" with no hint of which URL carried it."""
    with pytest.raises(ValueError, match="Invalid port in URL"):
        _make({"service_url": "https://example.com:banana/odata"})


@responses.activate
def test_flat_stream_numeric_string_cursor_crosses_digit_boundary():
    """E2E pin of the silent-stall shape: watermark "999", the server
    correctly answers `Seq gt 999` with Seq="1000" (IEEE754Compatible
    string rendering) — the client re-filter used to drop it every batch,
    freezing the offset with data pending forever."""
    responses.get(f"{SERVICE_URL}$metadata", body=R41_INT64_METADATA, status=200)
    responses.get(
        f"{SERVICE_URL}Events",
        json={"value": [{"Id": 3, "Seq": "1000"}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Events", {"cursor": "999"}, {"cursor_field": "Seq", "pagination": "nextlink"}
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [3]
    assert _drop_lb(offset) == {"cursor": "1000"}


def test_basic_format_iso_strings_stay_numeric():
    """`fromisoformat` on Python >= 3.11 parses BASIC format ("20240101"),
    which the 3.10 floor rejects — without the structural guard the
    comparison keys were version-divergent, and "20240101" aliased
    "2024-01-01" as the same instant. 8-digit yyyymmdd string cursors now
    key as Decimals uniformly (numeric order == chronological order)."""
    from datetime import datetime

    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_newer,
        cursor_same_instant,
        cursor_sort_key,
    )

    assert not isinstance(cursor_sort_key("20240101"), datetime)
    assert cursor_same_instant("20240101", "2024-01-01") is False
    assert cursor_newer("20240102", "20240101") is True
    # Extended format still datetime-keys.
    assert isinstance(cursor_sort_key("2024-01-01"), datetime)


@responses.activate
def test_empty_completion_strips_lb_cycle_started():
    """The leaf-cursor walk's vanished-checkpoint empty completion bypasses
    _attach_lookback_state; the anchor used to leak, so the NEXT progressing
    walk recorded the whole idle gap as a "cycle span" — lb_history then
    carried a bogus multi-hour entry and the auto window pinned at the
    ceiling for 5 walks."""
    _mock_nested_metadata()
    # The parked ANCHOR is still enumerable (so the round-43 reset does NOT
    # fire) but its checkpointed rows vanished — the resume completes empty
    # through the `if not emitted:` early return.
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]}, match_querystring=False)
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []}, match_querystring=False)
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {
            "parent_idx": 0,
            "parent_keys": [{"Id": 1}],
            "truncated_chain_cursor": "2024-01-01T00:00:00Z",
            "running_max": "2024-01-01T00:00:00Z",
            "lb_cycle_started": 1000.0,
        },
        {"cursor_field": "ModifiedAt", "pagination": "nextlink", "cursor_probe": "false"},
    )
    assert list(records) == []
    assert "lb_cycle_started" not in offset
    assert offset.get("cursor") == "2024-01-01T00:00:00Z"


@responses.activate
def test_valid_ancestor_fk_still_stamped():
    """A well-formed parent (key present) still stamps the FK — the guard
    only fires on a null ancestor key."""
    responses.get(f"{SERVICE_URL}$metadata", body=_FK_NULL_MD, status=200)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 7, "Name": "P1", "Children": [{"Id": 11, "Label": "a"}]}]},
        match_querystring=False,
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    responses.get(f"{SERVICE_URL}Parents(7)/Children", json={"value": []})
    c = _make()
    rows = list(c.read_table("Parents__Children", None, {"expand_contained": "true"})[0])
    assert rows == [{"Id": 11, "Label": "a", "Parents_Id": 7}]


@responses.activate
def test_reserved_column_guard_also_in_read_table_metadata():
    """The reserved delta-synthetic collision must fail as loudly from
    read_table_metadata as from read_table (consistent ordering), not report
    cdc success for a table the later read would reject."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    with pytest.raises(ValueError, match="reserved delta synthetic"):
        c.read_table_metadata("Widgets", {"delta_tracking": "enabled"})


# ---------------------------------------------------------------------------
# Round 51 — metadata reserved-column guard mirrors the read's select filter,
# keyless entity set + cursor_field fails loudly (no keyless CDC contract)
# ---------------------------------------------------------------------------


@responses.activate
def test_reserved_column_metadata_respects_select_drop():
    """When `select` drops the colliding source column, the synthetic takes
    its place and the READ succeeds — so read_table_metadata must NOT raise
    (it now mirrors get_table_schema's select-filtered check, not raw
    _fields_for). Round-50's guard over-fired here."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    opts = {"delta_tracking": "enabled", "select": "Id"}
    # The read path is happy — source _deleted/_lc_sequence dropped by select,
    # synthetics appended.
    names = [f.name for f in c.get_table_schema("Widgets", opts).fields]
    assert names == ["Id", "_deleted", "_lc_sequence"]
    # Metadata must agree (no false-positive raise).
    meta = c.read_table_metadata("Widgets", opts)
    assert meta["ingestion_type"] == "cdc"
    assert meta["cursor_field"] == "_lc_sequence"


@responses.activate
def test_reserved_column_metadata_raises_when_collision_kept():
    """With no select (colliding source column survives), metadata still
    raises — the guard fires exactly when the read would."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    with pytest.raises(ValueError, match="reserved delta synthetic"):
        c.read_table_metadata("Widgets", {"delta_tracking": "enabled"})
