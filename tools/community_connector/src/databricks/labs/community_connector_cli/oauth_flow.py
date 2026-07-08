"""OAuth helpers for the community-connector CLI.

The COMMUNITY connection type discriminates auth modes via the
``community_oauth_flow`` option:

- omitted / empty → static-credential mode (arbitrary key/value options)
- ``m2m``         → client-credentials OAuth flow
- ``u2m``         → authorization-code OAuth flow (handled live by the CLI
                    via a loopback redirect)
- ``u2m_per_user``→ per-user OAuth (per-end-user authorization happens at
                    runtime; the connection itself only stores app config)

This module owns the constants describing each mode and the in-process
loopback redirect server used to capture the U2M authorization code.
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import secrets
import socket
import threading
import time
import urllib.parse
import webbrowser
from dataclasses import dataclass
from typing import Optional, Tuple


AUTH_TYPE_STATIC = "static"
AUTH_TYPE_M2M = "m2m"
AUTH_TYPE_U2M = "u2m"
AUTH_TYPE_U2M_PER_USER = "u2m_per_user"

AUTH_TYPE_CHOICES = (
    AUTH_TYPE_STATIC,
    AUTH_TYPE_M2M,
    AUTH_TYPE_U2M,
    AUTH_TYPE_U2M_PER_USER,
)

# Wire-level value to put in the ``community_oauth_flow`` option.
# Static mode intentionally has no entry: the option is *omitted* so the
# connection resolves to the plain CONNECTION_COMMUNITY securable kind.
AUTH_TYPE_OAUTH_FLOW_VALUE = {
    AUTH_TYPE_M2M: "m2m",
    AUTH_TYPE_U2M: "u2m",
    AUTH_TYPE_U2M_PER_USER: "u2m_per_user",
}

# User-supplied connection options required per auth_type. The CLI fills in
# the OAuth-flow-generated values (authorization_code, pkce_verifier,
# oauth_redirect_uri) for u2m after the loopback redirect completes.
AUTH_TYPE_REQUIRED_OPTIONS = {
    AUTH_TYPE_STATIC: (),
    AUTH_TYPE_M2M: ("client_id", "client_secret", "token_endpoint"),
    AUTH_TYPE_U2M: (
        "client_id",
        "client_secret",
        "authorization_endpoint",
        "token_endpoint",
    ),
    AUTH_TYPE_U2M_PER_USER: (
        "client_id",
        "client_secret",
        "authorization_endpoint",
        "token_endpoint",
    ),
}

# All option keys recognized at the connection (auth) layer for COMMUNITY OAuth
# modes. Used to separate auth options from connector-runtime options when
# validating against the connector_spec.yaml.
OAUTH_OPTION_KEYS = frozenset(
    {
        "community_oauth_flow",
        "client_id",
        "client_secret",
        "oauth_scope",
        "token_endpoint",
        "authorization_endpoint",
        "authorization_code",
        "pkce_verifier",
        "oauth_redirect_uri",
        "oauth_provider",
        "oauth_credential_exchange_method",
        "expires_in_secs",
    }
)


@dataclass
class _CallbackResult:
    code: Optional[str] = None
    state: Optional[str] = None
    error: Optional[str] = None


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _generate_pkce() -> Tuple[str, str]:
    # RFC 7636: verifier is 43-128 chars of unreserved URL chars.
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    # Set by run_u2m_authorization_code_flow before serving.
    result: _CallbackResult = None  # type: ignore[assignment]
    expected_state: str = ""

    def log_message(self, format, *args):  # noqa: A002 - signature dictated by stdlib
        pass

    def do_GET(self):  # noqa: N802  # pylint: disable=invalid-name
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        error = params.get("error", [None])[0]
        code = params.get("code", [None])[0]
        state = params.get("state", [None])[0]

        # Browsers routinely hit the loopback redirect URI with extra
        # requests besides the real OAuth callback — favicon probes, link
        # previews, prefetches, the user typing the host directly. Treat
        # anything that carries neither `code` nor `error` as noise so a
        # background hit doesn't preempt the real authorization redirect.
        if not error and not code:
            self.send_response(204)
            self.end_headers()
            return

        if error:
            self.result.error = (
                f"{error}: {params.get('error_description', ['(no description)'])[0]}"
            )
        elif state != self.expected_state:
            self.result.error = "OAuth callback 'state' did not match — possible CSRF."
        else:
            self.result.code = code
            self.result.state = state

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if self.result.error:
            body = (
                "<html><body><h2>Authorization failed</h2>"
                f"<p>{self.result.error}</p>"
                "<p>You can close this tab.</p></body></html>"
            )
        else:
            body = (
                "<html><body><h2>Authorization complete</h2>"
                "<p>You can close this tab and return to the terminal.</p>"
                "</body></html>"
            )
        self.wfile.write(body.encode("utf-8"))


def _merge_extra_auth_params(auth_params: dict, extra_auth_params: Optional[dict]) -> dict:
    """Fold provider-specific extras into ``auth_params`` without letting them
    clobber the flow-critical params already set (response_type, client_id,
    redirect_uri, state, PKCE, scope). Returns ``auth_params`` for chaining."""
    if extra_auth_params:
        for key, value in extra_auth_params.items():
            auth_params.setdefault(str(key), str(value))
    return auth_params


def run_u2m_authorization_code_flow(
    *,
    client_id: str,
    authorization_endpoint: str,
    scope: Optional[str],
    redirect_port: Optional[int] = None,
    extra_auth_params: Optional[dict] = None,
    use_pkce: bool = True,
    open_browser: bool = True,
    timeout_seconds: int = 300,
    echo=print,
) -> Tuple[str, str, str]:
    """Drive the OAuth 2.0 authorization-code flow against a loopback redirect.

    Returns ``(authorization_code, pkce_verifier, redirect_uri)``. When
    ``use_pkce`` is False the flow omits the PKCE challenge and the returned
    ``pkce_verifier`` is an empty string.

    ``use_pkce`` defaults to True (PKCE is harmless for confidential clients
    and required for public ones); pass False to honor a connector spec's
    ``oauth.pkce: false``.

    ``extra_auth_params`` are provider-specific knobs (e.g.
    ``access_type=offline``, ``prompt=consent``) folded into the authorization
    request. They never override the flow's own parameters (response_type,
    client_id, redirect_uri, state, PKCE, scope).

    Raises ``RuntimeError`` if the user does not complete the flow within
    ``timeout_seconds`` or the IdP returns an error / mismatched state.
    """
    if redirect_port is None or redirect_port == 0:
        redirect_port = _pick_free_port()
    # RFC 8252 §7.3 and Google's desktop-app docs both recommend the IPv4
    # loopback literal here, not "localhost". The server below binds to
    # 127.0.0.1 — using "localhost" in the redirect URI breaks on systems
    # whose resolver returns ::1 first (browser hits IPv6 ::1 while our
    # server only listens on 127.0.0.1 → ERR_CONNECTION_REFUSED).
    redirect_uri = f"http://127.0.0.1:{redirect_port}/callback"

    verifier = ""
    state = secrets.token_urlsafe(24)

    auth_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
    }
    if use_pkce:
        verifier, challenge = _generate_pkce()
        auth_params["code_challenge"] = challenge
        auth_params["code_challenge_method"] = "S256"
    if scope:
        auth_params["scope"] = scope

    _merge_extra_auth_params(auth_params, extra_auth_params)

    separator = "&" if "?" in authorization_endpoint else "?"
    auth_url = f"{authorization_endpoint}{separator}{urllib.parse.urlencode(auth_params)}"

    result = _CallbackResult()
    _CallbackHandler.result = result
    _CallbackHandler.expected_state = state

    server = http.server.HTTPServer(("127.0.0.1", redirect_port), _CallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        echo(f"Listening for OAuth redirect on {redirect_uri}")
        echo("Open this URL in a browser to authorize (it will be opened for you if possible):")
        echo(f"  {auth_url}")
        if open_browser:
            try:
                webbrowser.open(auth_url)
            except Exception:  # pylint: disable=broad-except
                # webbrowser can raise on headless boxes; the URL is already printed.
                pass

        deadline = time.monotonic() + timeout_seconds
        while result.code is None and result.error is None:
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"Timed out after {timeout_seconds}s waiting for OAuth authorization."
                )
            time.sleep(0.25)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    if result.error:
        raise RuntimeError(f"OAuth authorization failed: {result.error}")
    assert result.code is not None  # for type-checkers; guaranteed by the loop above

    return result.code, verifier, redirect_uri
