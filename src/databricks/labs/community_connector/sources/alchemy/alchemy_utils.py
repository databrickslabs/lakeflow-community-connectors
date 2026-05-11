"""Utility functions for the Alchemy connector.

Centralises:
- exponential-backoff retry around the three retriable HTTP codes
  (429 / 500 / 503).
- POST / GET wrappers that always set a connection + read timeout.
- Response-shape normalisation (e.g. ``openSeaMetadata`` →
  ``openseaMetadata``) so the static schema doesn't have to track
  Alchemy's casing inconsistencies across API families.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

from databricks.labs.community_connector.sources.alchemy.alchemy_schemas import (
    INITIAL_BACKOFF,
    MAX_RETRIES,
    NATIVE_TOKEN_SENTINEL,
    REQUEST_CONNECT_TIMEOUT,
    REQUEST_READ_TIMEOUT,
    RETRIABLE_STATUS_CODES,
)


_LOG = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #

_TIMEOUT = (REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT)


def _sleep_with_retry_after(resp: requests.Response, attempt: int, backoff: float) -> float:
    """Sleep before retrying.

    Honours ``Retry-After`` (Alchemy returns it on 429) when parseable,
    otherwise applies exponential backoff with jitter to avoid the
    thundering-herd Alchemy explicitly calls out in their docs.  The
    next backoff value is returned so the caller can keep doubling.
    """
    retry_after = resp.headers.get("Retry-After") if resp is not None else None
    try:
        wait = float(retry_after) if retry_after else backoff
    except (TypeError, ValueError):
        wait = backoff
    # Random jitter (0-1s) per Alchemy's retry guidance.
    wait += random.uniform(0, 1)
    # Hard cap per Alchemy docs.
    wait = min(wait, 64.0)
    _LOG.debug(
        "Alchemy retry attempt %d: sleeping %.2fs before retry",
        attempt + 1,
        wait,
    )
    time.sleep(wait)
    return backoff * 2


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> requests.Response:
    """Issue a request, retrying on 429 / 500 / 503.

    Args:
        session: ``requests.Session`` reused across calls.
        method: ``"GET"`` or ``"POST"``.
        url: Fully qualified URL.
        params: Query string parameters (for GET).
        json_body: JSON body (for POST).
    """
    backoff = INITIAL_BACKOFF
    resp: requests.Response | None = None
    for attempt in range(MAX_RETRIES):
        if method == "GET":
            resp = session.get(url, params=params, timeout=_TIMEOUT)
        elif method == "POST":
            resp = session.post(url, params=params, json=json_body, timeout=_TIMEOUT)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        if resp.status_code not in RETRIABLE_STATUS_CODES:
            return resp

        if attempt < MAX_RETRIES - 1:
            backoff = _sleep_with_retry_after(resp, attempt, backoff)

    # All retries exhausted — return the last response so the caller
    # can include the failure body in the RuntimeError it raises.
    return resp  # type: ignore[return-value]


def api_call(
    session: requests.Session,
    method: str,
    url: str,
    label: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Issue a request and return the decoded JSON body.

    Raises ``RuntimeError`` on any non-2xx status with the response
    body included for debugging.
    """
    resp = request_with_retry(session, method, url, params=params, json_body=json_body)
    if resp.status_code >= 400:
        raise RuntimeError(f"Alchemy API error for {label}: {resp.status_code} {resp.text}")
    try:
        return resp.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Alchemy API returned non-JSON for {label}: {resp.text[:200]!r}"
        ) from exc


# --------------------------------------------------------------------------- #
# Table option parsing
# --------------------------------------------------------------------------- #


def parse_max_records(table_options: dict[str, str]) -> int | None:
    """Parse the optional ``max_records_per_batch`` admission cap.

    Returns ``None`` when unset or non-numeric (uncapped).
    """
    raw = table_options.get("max_records_per_batch")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def parse_networks(table_options: dict[str, str], default_network: str) -> list[str]:
    """Parse the ``network`` table option into a list of network ids.

    Accepts either a single network id (``"eth-mainnet"``) or a
    comma-separated list (``"eth-mainnet,base-mainnet"``).  Falls back
    to the connector-level default when unset.
    """
    raw = table_options.get("network", default_network)
    if not raw:
        return [default_network]
    return [n.strip() for n in raw.split(",") if n.strip()]


def parse_csv_option(table_options: dict[str, str], key: str) -> list[str]:
    """Parse a comma-separated option (e.g. ``symbols=ETH,BTC``)."""
    raw = table_options.get(key, "")
    if not raw:
        return []
    return [v.strip() for v in raw.split(",") if v.strip()]


def require_option(table_options: dict[str, str], key: str, table_name: str) -> str:
    """Return a required table option, raising ``ValueError`` if missing."""
    value = table_options.get(key)
    if not value:
        raise ValueError(f"Table '{table_name}' requires table_options['{key}']")
    return value


# --------------------------------------------------------------------------- #
# Response normalisation
# --------------------------------------------------------------------------- #


def normalise_contract(contract: dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalise NFT contract metadata across endpoints.

    Different Alchemy endpoints alternate between ``openSeaMetadata``
    (NFT v3 ``getNFTsForOwner``, ``getNFTMetadata``) and
    ``openseaMetadata`` (Portfolio ``assets/nfts/contracts/by-address``
    and ``getContractMetadata``).  Normalise to the lowercase form so
    the static struct works for all of them.
    """
    if not contract:
        return contract
    out = dict(contract)
    if "openSeaMetadata" in out and "openseaMetadata" not in out:
        out["openseaMetadata"] = out.pop("openSeaMetadata")
    elif "openSeaMetadata" in out:
        # Both present — drop the camelCase variant; the lowercase one
        # is canonical for our schema.
        out.pop("openSeaMetadata")
    return out


def normalise_native_token_address(token_address: Any) -> str:
    """Replace null tokenAddress with the ``NATIVE`` sentinel.

    Keeps the primary key non-null and joinable.  See
    ``alchemy_api_doc.md`` for the rationale.
    """
    if token_address is None or token_address == "":
        return NATIVE_TOKEN_SENTINEL
    return token_address


def stringify_metadata_map(metadata: Any) -> dict[str, str] | None:
    """Coerce an open-ended NFT ``raw.metadata`` dict into ``str → str``.

    The Spark schema declares the field as ``MapType(StringType,
    StringType)`` to tolerate the per-collection variation in metadata
    layout.  Non-string values are JSON-serialised inline.
    """
    if metadata is None:
        return None
    if not isinstance(metadata, dict):
        # Some contracts return a string blob; preserve it as a single
        # ``__raw__`` entry rather than dropping it.
        return {"__raw__": str(metadata)}

    import json

    out: dict[str, str] = {}
    for k, v in metadata.items():
        if v is None:
            out[str(k)] = ""
        elif isinstance(v, str):
            out[str(k)] = v
        else:
            try:
                out[str(k)] = json.dumps(v, default=str)
            except (TypeError, ValueError):
                out[str(k)] = str(v)
    return out


def normalise_nft_record(record: dict[str, Any]) -> dict[str, Any]:
    """Apply normalisation common to all NFT-shaped records.

    - Lowercase ``openSeaMetadata`` on the embedded contract.
    - Coerce ``raw.metadata`` to a string map.
    """
    if not record:
        return record
    out = dict(record)
    if "contract" in out:
        out["contract"] = normalise_contract(out.get("contract"))
    raw = out.get("raw")
    if isinstance(raw, dict):
        new_raw = dict(raw)
        new_raw["metadata"] = stringify_metadata_map(raw.get("metadata"))
        out["raw"] = new_raw
    return out
