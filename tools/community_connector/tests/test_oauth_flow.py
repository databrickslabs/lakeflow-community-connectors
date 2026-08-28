"""Unit tests for the OAuth helper module used by the community-connector CLI."""

import base64
import hashlib
import threading
import time
import urllib.request

import pytest

from databricks.labs.community_connector_cli import oauth_flow
from databricks.labs.community_connector_cli.oauth_flow import (
    _generate_pkce,
    _merge_extra_auth_params,
    _pick_free_port,
    run_u2m_authorization_code_flow,
)


def test_generate_pkce_pair_is_rfc7636_compliant():
    verifier, challenge = _generate_pkce()
    # Verifier length in [43, 128] and uses URL-safe charset.
    assert 43 <= len(verifier) <= 128
    assert all(c.isalnum() or c in "-._~" for c in verifier)
    # Challenge is base64url-no-padding of SHA256(verifier).
    expected = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
        .rstrip(b"=")
        .decode("ascii")
    )
    assert challenge == expected


def test_pick_free_port_returns_usable_port():
    port = _pick_free_port()
    assert 1024 <= port <= 65535


def test_run_u2m_flow_round_trip(monkeypatch):
    """Simulate the IdP redirecting back to the loopback server with a code."""
    port = _pick_free_port()
    captured = {}

    def fake_open(url):
        # Extract state from the auth URL, then hit the loopback with code+state.
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(url).query)
        captured["state"] = qs["state"][0]
        captured["redirect_uri"] = qs["redirect_uri"][0]

        def hit_callback():
            # Give the server a brief moment to start serving.
            time.sleep(0.1)
            url = (
                f"{captured['redirect_uri']}"
                f"?code=THE_CODE&state={captured['state']}"
            )
            urllib.request.urlopen(url, timeout=5).read()

        threading.Thread(target=hit_callback, daemon=True).start()
        return True

    monkeypatch.setattr(oauth_flow.webbrowser, "open", fake_open)

    code, verifier, redirect_uri = run_u2m_authorization_code_flow(
        client_id="cid",
        authorization_endpoint="https://example.com/authorize",
        scope="repo",
        redirect_port=port,
        open_browser=True,
        timeout_seconds=5,
        echo=lambda *_args, **_kw: None,
    )

    assert code == "THE_CODE"
    assert redirect_uri == f"http://127.0.0.1:{port}/callback"
    # Verifier should still pass PKCE shape requirements.
    assert 43 <= len(verifier) <= 128


def test_run_u2m_flow_rejects_state_mismatch(monkeypatch):
    """A state value that does not match must be reported as a CSRF-style error."""
    port = _pick_free_port()

    def fake_open(url):
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(url).query)
        redirect_uri = qs["redirect_uri"][0]

        def hit_callback():
            time.sleep(0.1)
            urllib.request.urlopen(
                f"{redirect_uri}?code=THE_CODE&state=wrong",
                timeout=5,
            ).read()

        threading.Thread(target=hit_callback, daemon=True).start()
        return True

    monkeypatch.setattr(oauth_flow.webbrowser, "open", fake_open)

    with pytest.raises(RuntimeError, match="state"):
        run_u2m_authorization_code_flow(
            client_id="cid",
            authorization_endpoint="https://example.com/authorize",
            scope=None,
            redirect_port=port,
            open_browser=True,
            timeout_seconds=5,
            echo=lambda *_args, **_kw: None,
        )


def test_run_u2m_flow_ignores_unrelated_requests(monkeypatch):
    """A probe hit (no code/error) must not preempt the real OAuth callback."""
    port = _pick_free_port()

    def fake_open(url):
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(url).query)
        state = qs["state"][0]
        redirect_uri = qs["redirect_uri"][0]

        def hit_callback():
            # Simulate browsers hitting the loopback server with extra
            # requests (favicon probe, root path) before the real redirect.
            time.sleep(0.1)
            for noise in (f"{redirect_uri.replace('/callback', '/favicon.ico')}",
                          f"{redirect_uri.replace('/callback', '/')}",
                          redirect_uri):
                try:
                    urllib.request.urlopen(noise, timeout=5).read()
                except Exception:  # pylint: disable=broad-except
                    pass
            # Then send the actual OAuth callback.
            time.sleep(0.1)
            urllib.request.urlopen(
                f"{redirect_uri}?code=THE_CODE&state={state}", timeout=5
            ).read()

        threading.Thread(target=hit_callback, daemon=True).start()
        return True

    monkeypatch.setattr(oauth_flow.webbrowser, "open", fake_open)

    code, _verifier, _redirect_uri = run_u2m_authorization_code_flow(
        client_id="cid",
        authorization_endpoint="https://example.com/authorize",
        scope=None,
        redirect_port=port,
        open_browser=True,
        timeout_seconds=5,
        echo=lambda *_args, **_kw: None,
    )

    assert code == "THE_CODE"


def test_run_u2m_flow_propagates_idp_error(monkeypatch):
    """An ``error`` query parameter from the IdP must surface to the caller."""
    port = _pick_free_port()

    def fake_open(url):
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(url).query)
        redirect_uri = qs["redirect_uri"][0]

        def hit_callback():
            time.sleep(0.1)
            urllib.request.urlopen(
                f"{redirect_uri}?error=access_denied&error_description=user+said+no",
                timeout=5,
            ).read()

        threading.Thread(target=hit_callback, daemon=True).start()
        return True

    monkeypatch.setattr(oauth_flow.webbrowser, "open", fake_open)

    with pytest.raises(RuntimeError, match="access_denied"):
        run_u2m_authorization_code_flow(
            client_id="cid",
            authorization_endpoint="https://example.com/authorize",
            scope=None,
            redirect_port=port,
            open_browser=True,
            timeout_seconds=5,
            echo=lambda *_args, **_kw: None,
        )


def test_merge_extra_auth_params_does_not_clobber_flow_params():
    """Extra params fill gaps but never overwrite the flow-critical params."""
    base = {"response_type": "code", "client_id": "cid", "scope": "repo"}
    merged = _merge_extra_auth_params(
        base,
        {"access_type": "offline", "client_id": "EVIL", "prompt": "consent"},
    )
    assert merged["access_type"] == "offline"
    assert merged["prompt"] == "consent"
    # Attempt to clobber a flow-critical param is ignored.
    assert merged["client_id"] == "cid"


def test_merge_extra_auth_params_handles_none():
    base = {"response_type": "code"}
    assert _merge_extra_auth_params(base, None) == {"response_type": "code"}


def _run_flow_capturing_auth_url(monkeypatch, **kwargs):
    """Drive run_u2m with a fake browser that completes the flow and returns the
    captured authorization-URL query string."""
    port = _pick_free_port()
    captured = {}

    def fake_open(url):
        from urllib.parse import urlparse, parse_qs

        qs = parse_qs(urlparse(url).query)
        captured["qs"] = qs

        def hit_callback():
            time.sleep(0.1)
            cb = f"{qs['redirect_uri'][0]}?code=THE_CODE&state={qs['state'][0]}"
            urllib.request.urlopen(cb, timeout=5).read()

        threading.Thread(target=hit_callback, daemon=True).start()
        return True

    monkeypatch.setattr(oauth_flow.webbrowser, "open", fake_open)
    result = run_u2m_authorization_code_flow(
        client_id="cid",
        authorization_endpoint="https://example.com/authorize",
        scope="repo",
        redirect_port=port,
        open_browser=True,
        timeout_seconds=5,
        echo=lambda *_a, **_k: None,
        **kwargs,
    )
    return result, captured["qs"]


def test_run_u2m_without_pkce_omits_challenge_and_returns_empty_verifier(monkeypatch):
    (_, verifier, _), qs = _run_flow_capturing_auth_url(monkeypatch, use_pkce=False)
    assert verifier == ""
    assert "code_challenge" not in qs
    assert "code_challenge_method" not in qs


def test_run_u2m_with_pkce_includes_challenge(monkeypatch):
    (_, verifier, _), qs = _run_flow_capturing_auth_url(monkeypatch, use_pkce=True)
    assert 43 <= len(verifier) <= 128
    assert qs["code_challenge_method"] == ["S256"]


def test_run_u2m_folds_extra_auth_params_into_url(monkeypatch):
    _, qs = _run_flow_capturing_auth_url(
        monkeypatch, extra_auth_params={"access_type": "offline", "prompt": "consent"}
    )
    assert qs["access_type"] == ["offline"]
    assert qs["prompt"] == ["consent"]
