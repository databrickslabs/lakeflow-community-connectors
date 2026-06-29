"""Auth verification test for the Amplitude connector.

Loads credentials from CONNECTOR_TEST_CONFIG_PATH (or CONNECTOR_TEST_CONFIG_JSON),
instantiates the connector, and makes one real HTTP call per API family to confirm
the api_key / secret_key pair is accepted by Amplitude. Fails on 401/403 and any
non-retriable HTTP error.
"""

import json
import os
import sys
from pathlib import Path

import pytest
import requests
from requests.auth import HTTPBasicAuth


def _load_config() -> dict:
    inline = os.environ.get("CONNECTOR_TEST_CONFIG_JSON", "").strip()
    if inline:
        return json.loads(inline)
    path_env = os.environ.get("CONNECTOR_TEST_CONFIG_PATH", "").strip()
    if path_env:
        p = Path(path_env)
        assert p.exists(), f"CONNECTOR_TEST_CONFIG_PATH not found: {p}"
        with open(p) as f:
            return json.load(f)
    # Live-only test: with no credentials (e.g. the default simulate-mode CI
    # run) there is nothing to verify, so skip rather than fail.
    pytest.skip(
        "No credentials found. Set CONNECTOR_TEST_CONFIG_PATH or "
        "CONNECTOR_TEST_CONFIG_JSON to run the live Amplitude auth check."
    )


BASE_URLS = {
    "standard": "https://amplitude.com",
    "eu": "https://analytics.eu.amplitude.com",
}

# Lightweight probe endpoints — one per API family.
PROBE_ENDPOINTS = [
    # Dashboard REST API (events list)
    ("/api/2/events/list", "Dashboard REST API (/api/2/events/list)"),
    # Behavioral Cohorts API
    ("/api/3/cohorts", "Behavioral Cohorts API (/api/3/cohorts)"),
    # Chart Annotations API
    ("/api/3/annotations", "Chart Annotations API (/api/3/annotations)"),
]


def test_amplitude_auth():
    cfg = _load_config()

    api_key = cfg.get("api_key", "")
    secret_key = cfg.get("secret_key", "")
    region = cfg.get("data_region", "standard").lower()

    assert api_key, "Config missing 'api_key'"
    assert secret_key, "Config missing 'secret_key'"
    assert region in BASE_URLS, (
        f"Unsupported data_region '{region}'; expected one of {sorted(BASE_URLS)}"
    )

    base_url = BASE_URLS[region]
    auth = HTTPBasicAuth(api_key, secret_key)

    failures = []
    for path, label in PROBE_ENDPOINTS:
        url = f"{base_url}{path}"
        try:
            resp = requests.get(url, auth=auth, timeout=30)
        except Exception as exc:
            failures.append(f"  NETWORK ERROR  {label}: {exc}")
            continue

        if resp.status_code in (401, 403):
            failures.append(
                f"  AUTH REJECTED  {label}: HTTP {resp.status_code} — "
                f"api_key/secret_key rejected by Amplitude. "
                f"Body: {resp.text[:300]}"
            )
        elif resp.status_code not in (200, 404):
            # 404 is legitimate for empty projects; anything else is unexpected
            failures.append(
                f"  UNEXPECTED HTTP  {label}: HTTP {resp.status_code}. "
                f"Body: {resp.text[:300]}"
            )
        else:
            print(f"  PASS  {label}: HTTP {resp.status_code}")

    if failures:
        raise AssertionError(
            "Amplitude auth verification FAILED:\n" + "\n".join(failures)
        )
