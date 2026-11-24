"""OData connector unit tests — connector-side OAuth (auth_type=oauth2).

Restored alongside the reintroduction of connector-side OAuth2 (token
minting + refresh). Covers the client-credentials and authorization-code
refresh flows, pre-emptive + on-401 refresh, rotated-refresh-token
handling across SDP-recreated instances, and the curated token-endpoint
error messages. Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import time

import pytest
import responses

from tests.unit.sources.odata._odata_test_helpers import (
    SERVICE_URL,
    _make,
    _mock_metadata,
)


@responses.activate
def test_oauth2_fetches_token():
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "minted", "token_type": "Bearer"},
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    c.list_tables()
    assert c._get_session().headers["Authorization"] == "Bearer minted"

@responses.activate
def test_oauth2_client_credentials_uses_client_credentials_grant():
    """No refresh_token on the connection → client_credentials grant."""
    captured = {}

    def _token_callback(request):
        captured["body"] = request.body
        return (200, {}, '{"access_token": "cc-minted", "token_type": "Bearer"}')

    responses.add_callback(
        responses.POST, "https://idp.example.com/token", callback=_token_callback
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    c.list_tables()
    assert "grant_type=client_credentials" in captured["body"]
    assert c._get_session().headers["Authorization"] == "Bearer cc-minted"

@responses.activate
def test_oauth2_malformed_token_response_never_echoes_the_body():
    """A truncated 200 from the token endpoint is exactly
    ``{"access_token": "<live secret>`` cut mid-document. The raised error
    must diagnose without echoing the body — the message lands in pipeline
    logs, and echoing it would publish a working credential."""
    responses.post(
        "https://idp.example.com/token",
        body='{"access_token": "SECRET-LIVE-TOKEN-XYZ", "expi',  # truncated
        status=200,
        content_type="application/json",
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        c.list_tables()
    message = str(excinfo.value)
    assert "SECRET-LIVE-TOKEN-XYZ" not in message
    assert "withheld" in message
    # And nothing rides in via exception chaining either (__cause__ severed;
    # the decoder error's .doc attribute carries the full body).
    assert excinfo.value.__cause__ is None

@responses.activate
def test_oauth2_token_endpoint_retries_transient_errors():
    """The token endpoint gets the same transient tolerance as the source: a
    momentary 503 there (including mid-read via the 401-refresh path) must be
    retried, not kill the whole read while source requests enjoy the full
    retry budget."""
    responses.post("https://idp.example.com/token", json={"error": "busy"}, status=503)
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "after-retry", "token_type": "Bearer", "expires_in": 3600},
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "retry_max_delay_seconds": "0",  # keep the backoff sleep at 0s
        }
    )
    assert c._oauth2_token() == "after-retry"
    assert sum(1 for call in responses.calls if call.request.method == "POST") == 2

@responses.activate
def test_oauth2_token_endpoint_hard_error_still_raises_actionable():
    """A non-transient token-endpoint rejection (401) must NOT be retried —
    it raises the same actionable credential message immediately."""
    responses.post(
        "https://idp.example.com/token",
        json={"error": "invalid_client"},
        status=401,
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "wrong",
            "retry_max_delay_seconds": "0",
        }
    )
    with pytest.raises(ValueError, match="client_credentials grant"):
        c._oauth2_token()
    assert sum(1 for call in responses.calls if call.request.method == "POST") == 1

@responses.activate
def test_oauth2_user_flow_uses_pre_supplied_access_token():
    """When `oauth2_access_token` is provided, the connector uses it
    directly and does NOT hit the token endpoint at startup."""
    # Register the token URL but don't expect it to be called.
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "should-not-be-used", "token_type": "Bearer"},
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_access_token": "user-flow-access",
            "oauth2_refresh_token": "user-flow-refresh",
        }
    )
    c.list_tables()
    assert c._get_session().headers["Authorization"] == "Bearer user-flow-access"
    # The token endpoint must not have been called during list_tables.
    token_calls = [c for c in responses.calls if c.request.url == "https://idp.example.com/token"]
    assert token_calls == []

@responses.activate
def test_oauth2_user_flow_refreshes_on_401_and_retries():
    """An expired access token surfaces as 401. The connector refreshes
    via `grant_type=refresh_token`, swaps the header, and retries the
    request once before raising."""
    _mock_metadata()
    captured_token_bodies = []

    def _token_callback(request):
        captured_token_bodies.append(request.body)
        return (200, {}, '{"access_token": "refreshed-access", "token_type": "Bearer"}')

    responses.add_callback(
        responses.POST, "https://idp.example.com/token", callback=_token_callback
    )

    call_count = {"n": 0}

    def _customers_callback(request):
        call_count["n"] += 1
        auth = request.headers.get("Authorization", "")
        if call_count["n"] == 1:
            # First call: stale token → 401.
            assert auth == "Bearer stale-access"
            return (401, {}, '{"error": "expired"}')
        # Second call: must carry the refreshed token.
        assert auth == "Bearer refreshed-access"
        return (
            200,
            {},
            '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"}]}',
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers_callback)

    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_access_token": "stale-access",
            "oauth2_refresh_token": "user-flow-refresh",
        }
    )
    # pagination=nextlink: focus on the 401-refresh-retry flow, not the
    # default auto drain probe (which would add a GET after the short page).
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in rows] == [1]
    assert call_count["n"] == 2
    assert len(captured_token_bodies) == 1
    body = captured_token_bodies[0]
    assert "grant_type=refresh_token" in body
    assert "refresh_token=user-flow-refresh" in body
    # Session's Authorization header must now carry the refreshed token.
    assert c._get_session().headers["Authorization"] == "Bearer refreshed-access"

@responses.activate
def test_oauth2_user_flow_tracks_rotated_refresh_token():
    """Some providers rotate the refresh token on every refresh. The
    new value must be picked up so the next refresh doesn't use the
    already-invalidated one."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={
            "access_token": "rotated-access",
            "refresh_token": "rotated-refresh",
            "token_type": "Bearer",
        },
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_refresh_token": "initial-refresh",
        }
    )
    c.list_tables()
    assert c.options["oauth2_refresh_token"] == "rotated-refresh"

@responses.activate
def test_oauth2_captures_expires_in_from_token_response():
    """`expires_in` from the token endpoint is stored as a WALL-CLOCK
    deadline so the next request can pre-emptively refresh. Wall clock,
    not monotonic: the deadline rides the pickled connector to executors,
    where the monotonic epoch is a different arbitrary origin — only
    ``time.time()`` compares meaningfully across hosts."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "minted", "expires_in": 3600, "token_type": "Bearer"},
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    before = time.time()
    c.list_tables()  # triggers session creation which mints the token
    after = time.time()
    # Expires_at should be ~ now + 3600 - 60s buffer, accounting for test time.
    assert c._access_token_expires_at is not None
    assert before + 3600 - 60 - 1 <= c._access_token_expires_at <= after + 3600 - 60

@responses.activate
def test_oauth2_preemptively_refreshes_when_token_near_expiry():
    """When the recorded deadline has passed, `_http_get` mints a fresh
    token BEFORE issuing the request — no 401 round-trip needed."""
    _mock_metadata()

    token_responses = iter(
        [
            '{"access_token": "first", "expires_in": 3600, "token_type": "Bearer"}',
            '{"access_token": "second", "expires_in": 3600, "token_type": "Bearer"}',
        ]
    )

    def _token_callback(request):
        return (200, {}, next(token_responses))

    responses.add_callback(
        responses.POST, "https://idp.example.com/token", callback=_token_callback
    )

    request_auths = []

    def _customers_callback(request):
        request_auths.append(request.headers.get("Authorization"))
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers_callback)

    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    # Force session creation (mints the first token), then yank the deadline
    # into the past to simulate post-expiry on the next request.
    session = c._get_session()
    assert session.headers["Authorization"] == "Bearer first"
    c._access_token_expires_at = time.time() - 1.0

    list(c.read_table("Customers", None, {})[0])
    # No 401 in this scenario — pre-emptive refresh happened before send,
    # so the single Customers request carries the refreshed token.
    assert request_auths == ["Bearer second"]

@responses.activate
def test_oauth2_handles_token_endpoint_without_expires_in():
    """Some token endpoints omit `expires_in`. Treat that as 'unknown
    expiry' and fall back to the 401-retry path — no exception, just no
    pre-emptive refresh."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "minted", "token_type": "Bearer"},  # no expires_in
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    c.list_tables()
    assert c._access_token_expires_at is None

@responses.activate
def test_oauth2_refresh_failure_raises_actionable_error():
    """A 401 from the token endpoint during a refresh-token grant
    surfaces the OAuth2 error code + description, and names the
    `oauth2_refresh_token` / `oauth2_client_id` fields the user
    should check."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"error": "invalid_grant", "error_description": "refresh_token expired"},
        status=401,
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_refresh_token": "stale",
        }
    )
    with pytest.raises(ValueError) as ei:
        c.list_tables()
    msg = str(ei.value)
    assert "refreshing the access token" in msg
    assert "oauth2_refresh_token" in msg
    assert "oauth2_client_id" in msg
    assert "invalid_grant" in msg
    assert "refresh_token expired" in msg

@responses.activate
def test_oauth2_client_credentials_failure_raises_actionable_error():
    """A 401 from the token endpoint during client_credentials names
    the client_id / client_secret / token_url / scope fields."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"error": "invalid_client"},
        status=401,
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "wrong-secret",
        }
    )
    with pytest.raises(ValueError) as ei:
        c.list_tables()
    msg = str(ei.value)
    assert "client_credentials grant" in msg
    assert "oauth2_client_secret" in msg
    assert "invalid_client" in msg

@responses.activate
def test_oauth2_persistent_401_after_refresh_raises_permission_error():
    """If the source keeps returning 401 even after a fresh token
    arrives, the access token isn't the problem. Surface a
    PermissionError that points at scope / principal / tenant rather
    than the token itself."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "fresh", "token_type": "Bearer"},
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=401,
        json={"error": "AccessDenied", "message": "principal lacks read on Customers"},
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=401,
        json={"error": "AccessDenied", "message": "principal lacks read on Customers"},
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_refresh_token": "valid",
        }
    )
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "even after refreshing" in msg
    assert "oauth2_scope" in msg
    assert "service_url" in msg

@responses.activate
def test_oauth2_without_refresh_path_401_raises_actionable_permission_error():
    """auth_type=oauth2 + pre-supplied access_token + no refresh_token +
    no client_id/secret is a legitimate config — but means there's no
    refresh path. A 401 here can't be auto-fixed; surface the auth
    options that need attention."""
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="expired")
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_access_token": "stale-access",
            # No client_id / client_secret → no refresh path.
        }
    )
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "auth_type=oauth2" in msg
    assert "oauth2_refresh_token" in msg
    assert "oauth2_access_token" in msg
    assert "oauth2_scope" in msg

@responses.activate
def test_oauth2_with_refresh_path_still_uses_existing_flow():
    """A 401 with an OAuth refresh path goes through the existing
    refresh-and-retry logic, NOT the new no-refresh-path error. This
    is the regression-guard for the existing OAuth UX."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "fresh", "token_type": "Bearer"},
    )
    call = {"n": 0}

    def _customers(request):
        call["n"] += 1
        if call["n"] == 1:
            return (401, {}, '{"error": "expired"}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "x", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    # Refreshable 401 → resolves cleanly via the existing path. New
    # PermissionError code path is bypassed. pagination=nextlink keeps the
    # call count focused on the refresh-retry, not the default auto drain probe.
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in list(rows)] == [1]
    assert call["n"] == 2  # 401 then 200 after refresh

@responses.activate
def test_token_endpoint_403_raises_actionable_error():
    """Non-400/401 token-endpoint rejections (403 policy blocks, retry-
    exhausted 5xx) used to surface as raise_for_status()'s terse one-liner.
    They get the same actionable shape as the 400/401 branches — and never
    echo the client secret."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"error": "forbidden_by_policy"},
        status=403,
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "s3cr3t-value",
        }
    )
    with pytest.raises(ValueError) as ei:
        c.list_tables()
    msg = str(ei.value)
    assert "403" in msg
    assert "forbidden_by_policy" in msg
    assert "s3cr3t-value" not in msg

@responses.activate
def test_basic_auth_401_with_leftover_oauth_options_stays_actionable():
    """With auth_type=basic plus leftover oauth2 options, a 401 previously
    minted a useless token (session.auth overwrites the header at prepare
    time) and blamed 'the refreshed OAuth2 access token'. The refresh path
    is now gated on auth_type=oauth2: no token mint, and the error points
    at the basic credentials."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"error": "denied"}, status=401)
    token_calls = {"n": 0}

    def _token(request):  # pylint: disable=unused-argument
        token_calls["n"] += 1
        return (200, {}, '{"access_token": "minted"}')

    responses.add_callback(responses.POST, "https://idp.example.com/token", callback=_token)
    c = _make(
        {
            "auth_type": "basic",
            "username": "u",
            "password": "p",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    with pytest.raises(PermissionError) as exc_info:
        rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
        list(rows)
    assert "refreshing the OAuth2 access token" not in str(exc_info.value)
    assert token_calls["n"] == 0

@responses.activate
def test_rotated_refresh_token_survives_instance_recreation():
    """SDP builds a FRESH connector from the connection's original options
    every microbatch. With single-use-rotation providers the pre-fix
    instance-local write-back replayed the revoked original token on the
    next batch; the process-wide stash now hands every recreated instance
    the latest rotation."""
    _mock_metadata()
    seen_refresh_tokens = []

    def _token(request):
        body = request.body.decode() if isinstance(request.body, bytes) else request.body
        for pair in body.split("&"):
            k, _, v = pair.partition("=")
            if k == "refresh_token":
                seen_refresh_tokens.append(v)
        return (
            200,
            {},
            json.dumps(
                {
                    "access_token": f"at-{len(seen_refresh_tokens)}",
                    "refresh_token": f"rot-{len(seen_refresh_tokens)}",
                    "token_type": "Bearer",
                }
            ),
        )

    responses.add_callback(responses.POST, "https://idp.example.com/token", callback=_token)
    opts = {
        "auth_type": "oauth2",
        "oauth2_token_url": "https://idp.example.com/token",
        "oauth2_client_id": "id",
        "oauth2_client_secret": "secret",
        "oauth2_refresh_token": "original",
    }
    c1 = _make(opts)
    c1._get_session()  # session construction mints via refresh grant → rotation recorded
    # Fresh instance from the ORIGINAL options — exactly how SDP rebuilds
    # the connector each microbatch. (list_tables would be served from the
    # process metadata cache without ever building a session, so exercise
    # the session mint directly.)
    c2 = _make(opts)
    c2._get_session()
    assert seen_refresh_tokens == ["original", "rot-1"]

@responses.activate
def test_token_endpoint_redirect_curated_error():
    """A 3xx from the OAuth2 token endpoint (allow_redirects=False) must
    surface as a curated config error naming the Location — not fall through
    to resp.json() on the empty redirect body and mis-diagnose as
    "malformed JSON … escalate to the identity provider"."""
    responses.post(
        "https://idp.example.com/token",
        status=301,
        headers={"Location": "https://idp.example.com/v2/token"},
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    with pytest.raises(ValueError, match="redirect.*oauth2_token_url|oauth2_token_url"):
        c.list_tables()

@responses.activate
def test_oauth_refresh_failure_names_parallel_rotation():
    """The refresh-grant failure error names concurrent rotation by a
    parallel reader process as a likely cause (single-use-rotation providers
    + partitioned reads), so users don't chase a nonexistent credential
    problem."""
    responses.post(
        "https://idp.example.com/token",
        status=400,
        json={"error": "invalid_grant"},
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_refresh_token": "rt",
        }
    )
    with pytest.raises(ValueError, match="parallel reader.*num_partitions=1"):
        c.list_tables()
