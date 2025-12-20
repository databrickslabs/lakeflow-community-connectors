"""
Helper script for hackathon demos (not used by the connector itself).

Purpose:
- Mint a short-lived Databricks OIDC access token (client-credentials) that can be used to call
  a Databricks App protected by workspace OAuth.

How it is used in this connector demo:
- Set `pi_base_url` to the Databricks App URL (e.g. https://<app>.databricksapps.com)
- Set `access_token` to the minted token (passed as: Authorization: Bearer <token>)

Credentials:
- The submitter stores client credentials in Databricks Secrets:
  - scope: sp-osipi
  - keys: sp-client-id, sp-client-secret

This script is intentionally lightweight and avoids committing any secrets.
"""

from __future__ import annotations

import os
from typing import Optional

import requests


def mint_workspace_oidc_token(
    workspace_host: str,
    client_id: str,
    client_secret: str,
    scope: str = "all-apis",
    timeout_seconds: int = 30,
) -> str:
    workspace_host = (workspace_host or "").rstrip("/")
    if not workspace_host.startswith("http://") and not workspace_host.startswith("https://"):
        workspace_host = "https://" + workspace_host

    token_url = f"{workspace_host}/oidc/v1/token"
    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout_seconds,
    )
    resp.raise_for_status()
    token = (resp.json() or {}).get("access_token")
    if not token:
        raise RuntimeError("OIDC token endpoint did not return access_token")
    return token


def _try_get_dbutils() -> Optional[object]:
    try:
        # Only exists in Databricks notebook contexts
        return dbutils  # type: ignore[name-defined]
    except Exception:
        return None


def _infer_workspace_host_from_dbutils(dbutils_obj: object) -> Optional[str]:
    try:
        ctx = dbutils_obj.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore[attr-defined]
        try:
            return "https://" + ctx.browserHostName().get()
        except Exception:
            pass
        try:
            return ctx.apiUrl().get()
        except Exception:
            return None
    except Exception:
        return None


def main() -> None:
    """
    Recommended usage (Databricks notebook):
    - Put `sp-client-id` and `sp-client-secret` in secrets scope `sp-osipi`
    - Run this script (or copy/paste the minting call) to print a token for the demo
    """

    # 1) Prefer Databricks secrets when available
    dbutils_obj = _try_get_dbutils()
    workspace_host = None
    client_id = None
    client_secret = None

    if dbutils_obj is not None:
        workspace_host = _infer_workspace_host_from_dbutils(dbutils_obj)
        try:
            client_id = dbutils_obj.secrets.get(scope="sp-osipi", key="sp-client-id")  # type: ignore[attr-defined]
            client_secret = dbutils_obj.secrets.get(scope="sp-osipi", key="sp-client-secret")  # type: ignore[attr-defined]
        except Exception:
            client_id = None
            client_secret = None

    # 2) Fallback to environment variables (for local use)
    workspace_host = workspace_host or os.environ.get("DATABRICKS_WORKSPACE_HOST")
    client_id = client_id or os.environ.get("DATABRICKS_OIDC_CLIENT_ID")
    client_secret = client_secret or os.environ.get("DATABRICKS_OIDC_CLIENT_SECRET")

    if not workspace_host:
        raise SystemExit("Missing workspace host. Set DATABRICKS_WORKSPACE_HOST or run in a Databricks notebook.")
    if not client_id or not client_secret:
        raise SystemExit(
            "Missing client credentials. Set DATABRICKS_OIDC_CLIENT_ID / DATABRICKS_OIDC_CLIENT_SECRET "
            "or store secrets in Databricks scope sp-osipi."
        )

    token = mint_workspace_oidc_token(workspace_host, client_id, client_secret)
    print(token)


if __name__ == "__main__":
    main()


