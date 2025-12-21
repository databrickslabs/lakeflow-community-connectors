"""Print sample rows for OSIPI connector tables.

This is a validation and troubleshooting utility. It uses the connector directly
(no Spark required) and prints N records per table in a readable format.

Typical usage (PI Web API endpoint):
  python3 sources/osipi/test/print_osipi_samples.py \
    --pi-base-url https://<pi-web-api-host> \
    --mint-databricks-app-token \
    --verbose \
    --probe \
    --max-records 5

Typical usage (local mock server):
  python3 sources/osipi/test/print_osipi_samples.py \
    --pi-base-url http://127.0.0.1:8000 \
    --max-records 5 \
    --verbose

Notes:
- If you pass --mint-databricks-app-token, this script:
  1) Uses your ~/.databrickscfg DEFAULT profile PAT to call the Databricks Secrets API
  2) Reads secrets from scope `sp-osipi` keys `sp-client-id` and `sp-client-secret`
  3) Optionally base64-decodes them (depending on how secrets are stored)
  4) Calls https://<workspace-host>/oidc/v1/token (client_credentials) to mint a Bearer token
- No secrets are printed.
"""

from __future__ import annotations

import argparse
import base64
import configparser
import json
import os
import sys
import time
import traceback
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# Ensure repo root is importable when running as a script (so `import sources...` works).
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from sources.osipi.osipi import LakeflowConnect


def _log(enabled: bool, msg: str) -> None:
    if enabled:
        print(f"[osipi-samples] {msg}")


def _mask_token(token: Optional[str]) -> str:
    if not token:
        return "<empty>"
    if len(token) <= 16:
        return "<redacted>"
    return token[:10] + "…" + token[-6:]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _b64decode_str(s: str) -> str:
    pad = "=" * (-len(s) % 4)
    return base64.b64decode(s + pad).decode("utf-8")


def _read_databricks_cfg(profile: str = "DEFAULT") -> Tuple[str, str]:
    cfg_path = os.path.expanduser("~/.databrickscfg")
    cp = configparser.ConfigParser()
    cp.read(cfg_path)
    if profile not in cp:
        raise RuntimeError(f"Profile {profile!r} not found in {cfg_path}")
    host = cp[profile]["host"].rstrip("/")
    token = cp[profile]["token"].strip()
    return host, token


def _get_secret(workspace_host: str, pat: str, scope: str, key: str) -> str:
    r = requests.get(
        f"{workspace_host}/api/2.0/secrets/get",
        headers={"Authorization": f"Bearer {pat}"},
        params={"scope": scope, "key": key},
        timeout=30,
    )
    r.raise_for_status()
    val = (r.json() or {}).get("value")
    if not val:
        raise RuntimeError(f"secrets/get returned empty value for {scope}/{key}")
    return val


def mint_databricks_oidc_token_from_secrets(
    *,
    databricks_profile: str = "DEFAULT",
    secrets_scope: str = "sp-osipi",
    client_id_key: str = "sp-client-id",
    client_secret_key: str = "sp-client-secret",
    scope: str = "all-apis",
) -> str:
    workspace_host, pat = _read_databricks_cfg(databricks_profile)
    raw_client_id = _get_secret(workspace_host, pat, secrets_scope, client_id_key)
    raw_client_secret = _get_secret(workspace_host, pat, secrets_scope, client_secret_key)

    # Demo convention in this project: stored base64-encoded
    client_id = _b64decode_str(raw_client_id)
    client_secret = _b64decode_str(raw_client_secret)

    resp = requests.post(
        f"{workspace_host}/oidc/v1/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    token = (resp.json() or {}).get("access_token")
    if not token:
        raise RuntimeError("OIDC token endpoint did not return access_token")
    return token


def _iter_n(it: Iterable[dict], n: int) -> List[dict]:
    out: List[dict] = []
    for i, row in enumerate(it):
        out.append(row)
        if i >= n - 1:
            break
    return out


def _print_table_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _print_records(records: List[dict]) -> None:
    if not records:
        print("(no records)")
        return
    for i, r in enumerate(records, start=1):
        print(f"- record {i}:")
        print(json.dumps(r, indent=2, default=str))


def _probe(base_url: str, access_token: Optional[str], verbose: bool) -> None:
    url = base_url.rstrip("/") + "/piwebapi/dataservers"
    headers: Dict[str, str] = {"Accept": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    _log(verbose, f"Probe GET {url}")
    t0 = perf_counter()
    r = requests.get(url, headers=headers, timeout=60)
    dt = perf_counter() - t0
    _log(verbose, f"Probe status={r.status_code} ct={r.headers.get('content-type','')} dt={dt:.2f}s")
    # Don't raise here; just a probe.


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pi-base-url", dest="pi_base_url", default=None)
    parser.add_argument("--access-token", dest="access_token", default=None)
    parser.add_argument("--max-records", dest="max_records", type=int, default=5)
    parser.add_argument(
        "--tables",
        dest="tables",
        default=None,
        help="Comma-separated table names (default: all)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print step-by-step telemetry logs")
    parser.add_argument("--probe", action="store_true", help="Probe connectivity before reading")

    # Optional: mint token via workspace secrets (Databricks OIDC client_credentials)
    parser.add_argument("--mint-databricks-app-token", action="store_true")
    parser.add_argument("--databricks-profile", default="DEFAULT")
    parser.add_argument("--secrets-scope", default="sp-osipi")
    parser.add_argument("--client-id-key", default="sp-client-id")
    parser.add_argument("--client-secret-key", default="sp-client-secret")

    args = parser.parse_args()

    parent_dir = Path(__file__).parent.parent
    dev_cfg_path = parent_dir / "configs" / "dev_config.json"
    table_cfg_path = parent_dir / "configs" / "dev_table_config.json"

    dev_cfg: Dict[str, str] = dict(_load_json(dev_cfg_path))
    table_cfg: Dict[str, Dict[str, Any]] = dict(_load_json(table_cfg_path))

    _log(args.verbose, f"Loaded dev_config.json: {dev_cfg_path}")
    _log(args.verbose, f"Loaded dev_table_config.json: {table_cfg_path}")

    if args.pi_base_url:
        dev_cfg["pi_base_url"] = args.pi_base_url
    if args.access_token is not None:
        dev_cfg["access_token"] = args.access_token

    if args.mint_databricks_app_token:
        _log(
            args.verbose,
            f"Minting Databricks OIDC token via Secrets API (profile={args.databricks_profile}, scope={args.secrets_scope})",
        )
        t0 = perf_counter()
        token = mint_databricks_oidc_token_from_secrets(
            databricks_profile=args.databricks_profile,
            secrets_scope=args.secrets_scope,
            client_id_key=args.client_id_key,
            client_secret_key=args.client_secret_key,
        )
        dev_cfg["access_token"] = token
        _log(args.verbose, f"Minted access_token={_mask_token(token)} dt={perf_counter()-t0:.2f}s")

    base_url = (dev_cfg.get("pi_base_url") or "").strip()
    if not base_url:
        raise SystemExit(
            "Missing pi_base_url. Pass --pi-base-url or set it in sources/osipi/configs/dev_config.json"
        )

    access_token = (dev_cfg.get("access_token") or "").strip() or None
    _log(args.verbose, f"Using pi_base_url={base_url}")
    _log(args.verbose, f"Using access_token={_mask_token(access_token)}")

    if args.probe:
        _probe(base_url, access_token, args.verbose)

    _log(args.verbose, "Initializing LakeflowConnect...")
    t0 = perf_counter()
    conn = LakeflowConnect({"pi_base_url": base_url, "access_token": access_token or ""})
    _log(args.verbose, f"Initialized LakeflowConnect dt={perf_counter()-t0:.2f}s")

    tables = conn.list_tables()
    selected = tables
    if args.tables:
        want = {t.strip() for t in args.tables.split(",") if t.strip()}
        selected = [t for t in tables if t in want]

    _print_table_header("OSIPI connector sample output")
    print(f"pi_base_url = {base_url}")
    print(f"tables = {selected}")
    print(f"max_records_per_table = {args.max_records}")

    for t in selected:
        opts = table_cfg.get(t, {})
        _print_table_header(f"table: {t}")
        print(f"table_options = {json.dumps(opts, indent=2, default=str)}")

        try:
            _log(args.verbose, f"Reading table={t} ...")
            t0 = perf_counter()
            it, off = conn.read_table(t, {}, opts)
            rows = _iter_n(it, args.max_records)
            dt = perf_counter() - t0
            _log(
                args.verbose,
                f"Read table={t} ok: sampled={len(rows)} next_offset={off} dt={dt:.2f}s",
            )
            print(f"next_offset = {off}")
            _print_records(rows)
        except Exception as e:
            _log(True, f"ERROR reading table {t}: {type(e).__name__}: {e}")
            if args.verbose:
                print(traceback.format_exc())
            time.sleep(0.5)


if __name__ == "__main__":
    main()
