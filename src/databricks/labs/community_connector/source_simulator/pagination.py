"""Pluggable pagination styles for the simulator.

Each style implements two methods:

- ``parse_request(query, headers, spec)`` → ``(offset, limit)`` into the corpus.
- ``render_response(page, total, offset, limit, request_url)`` →
  ``(body, headers)`` for the rendered HTTP response.

Connectors choose a style per endpoint via ``response.pagination_style``.
Built-in styles cover the connectors in this repo today; new styles drop in
as one class each.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    EndpointSpec,
)


class PaginationStyle(Protocol):
    """Protocol every pagination style implements."""

    name: str

    def parse_request(
        self, query: Dict[str, str], headers: Dict[str, str], spec: EndpointSpec
    ) -> Tuple[int, int]:
        ...

    def render_response(
        self,
        page: List[Dict[str, Any]],
        total: int,
        offset: int,
        limit: int,
        request_url: str,
    ) -> Tuple[Any, Dict[str, str]]:
        ...


# ----- concrete styles --------------------------------------------------


class NonePagination:
    """No pagination: serve the whole corpus."""

    name = "none"

    def parse_request(
        self, query: Dict[str, str], headers: Dict[str, str], spec: EndpointSpec
    ) -> Tuple[int, int]:
        return 0, 10**9  # effectively unlimited

    def render_response(
        self, page, total, offset, limit, request_url
    ) -> Tuple[Any, Dict[str, str]]:
        return page, {}


class PageNumber:
    """``?page=N&per_page=M``. Body is the array; no link header."""

    name = "page_number"

    def parse_request(self, query, headers, spec) -> Tuple[int, int]:
        per_page = _resolve_per_page(query, spec)
        page = _resolve_page(query, spec)
        return (page - 1) * per_page, per_page

    def render_response(
        self, page, total, offset, limit, request_url
    ) -> Tuple[Any, Dict[str, str]]:
        return page, {}


class PageNumberWithLinkHeader:
    """``?page=N&per_page=M`` + GitHub-style ``Link`` header."""

    name = "page_number_with_link_header"

    def parse_request(self, query, headers, spec) -> Tuple[int, int]:
        per_page = _resolve_per_page(query, spec)
        page = _resolve_page(query, spec)
        return (page - 1) * per_page, per_page

    def render_response(
        self, page, total, offset, limit, request_url
    ) -> Tuple[Any, Dict[str, str]]:
        if limit <= 0:
            return page, {}
        current_page = (offset // limit) + 1
        last_page = max(1, (total + limit - 1) // limit)
        links = []
        if current_page < last_page:
            links.append(_link(request_url, current_page + 1, "next"))
            links.append(_link(request_url, last_page, "last"))
        if current_page > 1:
            links.append(_link(request_url, 1, "first"))
            links.append(_link(request_url, current_page - 1, "prev"))
        headers = {"Link": ", ".join(links)} if links else {}
        return page, headers


class OffsetLimit:
    """``?offset=N&limit=M``. Body is the array; no link header."""

    name = "offset_limit"

    def parse_request(self, query, headers, spec) -> Tuple[int, int]:
        offset = _resolve_offset(query, spec)
        limit = _resolve_limit(query, spec)
        return offset, limit

    def render_response(
        self, page, total, offset, limit, request_url
    ) -> Tuple[Any, Dict[str, str]]:
        return page, {}


# ----- registry ---------------------------------------------------------


_REGISTRY: Dict[str, PaginationStyle] = {
    "none": NonePagination(),
    "page_number": PageNumber(),
    "page_number_with_link_header": PageNumberWithLinkHeader(),
    "offset_limit": OffsetLimit(),
}


def get_style(name: str) -> PaginationStyle:
    if name not in _REGISTRY:
        raise ValueError(
            f"Unknown pagination_style {name!r}; "
            f"registered styles: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


def register_style(style: PaginationStyle) -> None:
    """Add a custom pagination style. Use for connector-specific behavior."""
    _REGISTRY[style.name] = style


# ----- helpers ----------------------------------------------------------


def _resolve_per_page(query: Dict[str, str], spec: EndpointSpec) -> int:
    if spec.per_page is None:
        return 30
    raw = query.get(spec.per_page.name)
    if raw is None:
        return spec.per_page.default
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return spec.per_page.default
    return max(1, min(v, spec.per_page.max))


def _resolve_page(query: Dict[str, str], spec: EndpointSpec) -> int:
    name = spec.page.name if spec.page else "page"
    raw = query.get(name, "1")
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return 1
    return max(1, v)


def _resolve_offset(query: Dict[str, str], spec: EndpointSpec) -> int:
    name = spec.offset.name if spec.offset else "offset"
    raw = query.get(name, "0")
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return 0
    return max(0, v)


def _resolve_limit(query: Dict[str, str], spec: EndpointSpec) -> int:
    if spec.limit is None:
        return 30
    raw = query.get(spec.limit.name)
    if raw is None:
        return spec.limit.default
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return spec.limit.default
    return max(1, min(v, spec.limit.max))


def _link(request_url: str, page: int, rel: str) -> str:
    """Build a single GitHub-style ``Link`` header entry."""
    parts = urlsplit(request_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["page"] = str(page)
    new_url = urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
    )
    return f'<{new_url}>; rel="{rel}"'
