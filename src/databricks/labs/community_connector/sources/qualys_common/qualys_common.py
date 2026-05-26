"""Shared auth/config/request/xml helpers for Qualys connectors."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping
from xml.etree import ElementTree

import requests


class QualysClient:
    """Thin HTTP client wrapper for Basic/Bearer Qualys API patterns."""

    def __init__(
        self,
        *,
        base_url: str,
        auth_mode: str,
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
        timeout_seconds: int = 30,
        extra_headers: Mapping[str, str] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

        headers = {"Accept": "application/json"}
        if extra_headers:
            headers.update(dict(extra_headers))

        if auth_mode == "basic":
            if not username or not password:
                raise ValueError("Qualys basic auth requires username and password")
            self.session.auth = (username, password)
            headers.setdefault("X-Requested-With", "LakeflowConnector")
        elif auth_mode == "bearer":
            if not token:
                raise ValueError("Qualys bearer auth requires token")
            headers["Authorization"] = f"Bearer {token}"
        else:
            raise ValueError(f"Unsupported auth mode: {auth_mode}")

        self.session.headers.update(headers)

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        ok_statuses: tuple[int, ...] = (200,),
    ) -> Any:
        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            params=params,
            json=payload,
            data=data,
            timeout=self.timeout_seconds,
        )
        if response.status_code not in ok_statuses:
            raise RuntimeError(
                f"Qualys API error {response.status_code} for {method} {path}: {response.text}"
            )

        if not response.content:
            return {}
        return response.json()

    def request_text(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        ok_statuses: tuple[int, ...] = (200,),
    ) -> str:
        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            params=params,
            data=data,
            timeout=self.timeout_seconds,
        )
        if response.status_code not in ok_statuses:
            raise RuntimeError(
                f"Qualys API error {response.status_code} for {method} {path}: {response.text}"
            )
        return response.text


def parse_xml(text: str) -> ElementTree.Element:
    return ElementTree.fromstring(text.encode("utf-8"))


def get_text(node: ElementTree.Element, path: str) -> str | None:
    target = node.find(path)
    if target is None or target.text is None:
        return None
    value = target.text.strip()
    return value if value else None


def coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def as_iso8601(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            return datetime.fromtimestamp(int(stripped), tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        if stripped.endswith("Z"):
            return stripped
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return stripped

    if isinstance(value, (int, float)):
        # Qualys often uses epoch seconds in FO APIs.
        return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    return str(value)


def epoch_millis_to_iso(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        ivalue = int(value)
    except (TypeError, ValueError):
        return as_iso8601(value)
    # CS commonly emits epoch milliseconds.
    return datetime.fromtimestamp(ivalue / 1000.0, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
