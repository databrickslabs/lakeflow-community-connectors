"""Unit tests for the ADME connector's auth-mode dispatch.

Exercises ``_build_token_provider`` for the four supported modes:

  * service_principal (default, backwards-compatible)
  * static_token
  * managed_identity (skipped if azure-identity not installed)
  * federated_identity (skipped if azure-identity not installed)

No network calls; each mode is verified by constructing the provider and
inspecting its type / fields. Service-principal mode reuses the existing
in-memory ``_TokenCache`` so its OAuth2 call is never made during tests.
"""

from __future__ import annotations

import importlib
import tempfile

import pytest

from databricks.labs.community_connector.sources.adme.adme import (
    AUTH_MODE_FEDERATED_IDENTITY,
    AUTH_MODE_MANAGED_IDENTITY,
    AUTH_MODE_SERVICE_PRINCIPAL,
    AUTH_MODE_STATIC_TOKEN,
    _AzureIdentityTokenProvider,
    _StaticTokenProvider,
    _TokenCache,
    _build_token_provider,
)


_BASE = {
    "tenant_id": "00000000-0000-0000-0000-000000000000",
    "client_id": "11111111-1111-1111-1111-111111111111",
    "data_partition_id": "opendes",
}


def _has_azure_identity() -> bool:
    try:
        importlib.import_module("azure.identity")
        return True
    except ImportError:
        return False


def test_service_principal_is_default():
    opts = {**_BASE, "client_secret": "shh"}
    provider, audience = _build_token_provider(opts)
    assert isinstance(provider, _TokenCache)
    # Audience defaults to client_id when adme_api_client_id is unset.
    assert audience == _BASE["client_id"]


def test_service_principal_uses_explicit_audience():
    opts = {
        **_BASE,
        "client_secret": "shh",
        "adme_api_client_id": "22222222-2222-2222-2222-222222222222",
    }
    _provider, audience = _build_token_provider(opts)
    assert audience == "22222222-2222-2222-2222-222222222222"


def test_service_principal_requires_secret():
    opts = {**_BASE, "auth_mode": AUTH_MODE_SERVICE_PRINCIPAL}
    with pytest.raises(ValueError, match="client_secret"):
        _build_token_provider(opts)


def test_static_token_mode():
    opts = {
        **_BASE,
        "auth_mode": AUTH_MODE_STATIC_TOKEN,
        "access_token": "pre-issued-jwt",
    }
    provider, _audience = _build_token_provider(opts)
    assert isinstance(provider, _StaticTokenProvider)
    assert provider.get() == "pre-issued-jwt"
    # invalidate() is a no-op for static tokens — should not raise.
    provider.invalidate()
    assert provider.get() == "pre-issued-jwt"


def test_static_token_requires_value():
    opts = {**_BASE, "auth_mode": AUTH_MODE_STATIC_TOKEN}
    with pytest.raises(ValueError, match="static_token"):
        _build_token_provider(opts)


def test_unknown_auth_mode_rejected():
    opts = {**_BASE, "auth_mode": "carrier-pigeon"}
    with pytest.raises(ValueError, match="unknown auth_mode"):
        _build_token_provider(opts)


@pytest.mark.skipif(
    not _has_azure_identity(),
    reason="azure-identity not installed; install with `pip install azure-identity`",
)
def test_managed_identity_mode():
    opts = {**_BASE, "auth_mode": AUTH_MODE_MANAGED_IDENTITY}
    provider, _audience = _build_token_provider(opts)
    assert isinstance(provider, _AzureIdentityTokenProvider)


@pytest.mark.skipif(
    not _has_azure_identity(),
    reason="azure-identity not installed; install with `pip install azure-identity`",
)
def test_managed_identity_requires_audience_when_client_id_unset():
    opts = {
        "data_partition_id": "opendes",
        "auth_mode": AUTH_MODE_MANAGED_IDENTITY,
        "managed_identity_client_id": "33333333-3333-3333-3333-333333333333",
    }
    with pytest.raises(ValueError, match="adme_api_client_id"):
        _build_token_provider(opts)


@pytest.mark.skipif(
    not _has_azure_identity(),
    reason="azure-identity not installed; install with `pip install azure-identity`",
)
def test_federated_identity_inline_token():
    opts = {
        **_BASE,
        "auth_mode": AUTH_MODE_FEDERATED_IDENTITY,
        "federated_token": "oidc-assertion-jwt",
    }
    provider, _audience = _build_token_provider(opts)
    assert isinstance(provider, _AzureIdentityTokenProvider)


@pytest.mark.skipif(
    not _has_azure_identity(),
    reason="azure-identity not installed; install with `pip install azure-identity`",
)
def test_federated_identity_token_file(tmp_path):
    token_path = tmp_path / "assertion.jwt"
    token_path.write_text("oidc-assertion-from-file")
    opts = {
        **_BASE,
        "auth_mode": AUTH_MODE_FEDERATED_IDENTITY,
        "federated_token_file": str(token_path),
    }
    provider, _audience = _build_token_provider(opts)
    assert isinstance(provider, _AzureIdentityTokenProvider)


@pytest.mark.skipif(
    not _has_azure_identity(),
    reason="azure-identity not installed; install with `pip install azure-identity`",
)
def test_federated_identity_requires_token_source():
    opts = {**_BASE, "auth_mode": AUTH_MODE_FEDERATED_IDENTITY}
    with pytest.raises(ValueError, match="federated_token"):
        _build_token_provider(opts)
