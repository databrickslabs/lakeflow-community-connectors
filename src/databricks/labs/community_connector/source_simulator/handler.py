"""Match incoming requests to endpoint specs and serve from the corpus."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.corpus import (
    CorpusStore,
    apply_filters,
    apply_sort,
    slice_page,
)
from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    EndpointSpec,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    request_record_from_prepared,
    response_from_record,
)
from databricks.labs.community_connector.source_simulator.pagination import (
    get_style,
)


class UnknownEndpoint(LookupError):
    """Raised when an incoming request has no matching endpoint spec."""


@dataclass
class SimulateHandler:
    """Serve responses from a corpus + endpoint spec.

    The handler is mode-agnostic: it just turns a request into a response.
    The Simulator orchestrator decides when to call it (in ``MODE_SIMULATE``).
    """

    specs: List[EndpointSpec]
    corpus: CorpusStore

    def handle(self, prep: PreparedRequest) -> Response:
        from databricks.labs.community_connector.source_simulator.endpoint_spec import (
            match_endpoint,
        )

        req_rec = request_record_from_prepared(prep)
        match = match_endpoint(self.specs, req_rec.method, prep.url or "")
        if match is None:
            raise UnknownEndpoint(
                f"No endpoint spec matches {req_rec.method} {req_rec.url}\n"
                f"  Fix: add an entry under 'endpoints:' in the spec file, "
                "or fall back to a cassette."
            )

        spec, _path_params = match

        if spec.handler is not None:
            # Custom handler escape hatch — defer to the user-supplied function.
            return _invoke_custom_handler(spec.handler, prep, spec, self.corpus)

        # Strict-params validation: reject unknown query params just like a
        # real server would. Off by default; opt in per endpoint.
        if spec.strict_params:
            unknown = sorted(set(req_rec.query.keys()) - spec.known_param_names())
            if unknown:
                return _build_error_response(
                    prep,
                    status_code=400,
                    message=(
                        f"Unknown query parameter(s): {unknown}. "
                        f"Allowed: {sorted(spec.known_param_names())}"
                    ),
                )

        body, headers = self._serve(spec, req_rec.query, prep.url or "")
        body_bytes, content_type = _encode_body(body)
        resp_headers = {"Content-Type": content_type, **headers}
        rec = ResponseRecord(
            status_code=200,
            headers=resp_headers,
            body_text=(
                body_bytes.decode("utf-8")
                if content_type.startswith("application/json")
                else None
            ),
            body_b64=None,
            encoding="utf-8",
            url=prep.url,
        )
        return response_from_record(rec, prep)

    def _serve(
        self, spec: EndpointSpec, query: Dict[str, str], request_url: str
    ) -> Tuple[Any, Dict[str, str]]:
        # Single-entity endpoints: just return the corpus dict.
        if spec.response.single_entity:
            entity = self.corpus.get(spec.corpus) if spec.corpus else None
            if entity is None:
                return {}, {}
            return entity, {}

        records = self.corpus.get(spec.corpus) if spec.corpus else []
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError(
                f"Corpus for {spec.corpus!r} is not a list, but endpoint "
                f"{spec.method} {spec.path} expects an array."
            )

        # Filter, sort, paginate — in that order.
        filtered = apply_filters(records, spec.filters, query)

        sort_field, sort_order = _resolve_sort(spec, query)
        sorted_records = apply_sort(
            filtered, sort_field=sort_field, sort_order=sort_order
        )

        style = get_style(spec.response.pagination_style)
        offset, limit = style.parse_request(query, {}, spec)
        page = slice_page(sorted_records, offset, limit)
        body, headers = style.render_response(
            page, total=len(sorted_records), offset=offset, limit=limit,
            request_url=request_url,
        )

        # Wrap the body if the spec asks for it. Supports dotted records_key
        # for nested wrappers like ``result.elements``.
        wrapper = spec.response.wrapper
        if wrapper is not None and isinstance(body, list):
            parts = wrapper.records_key.split(".")
            inner: Dict[str, Any] = {parts[-1]: body}
            inner.update(wrapper.extras)
            wrapped: Dict[str, Any] = inner
            for key in reversed(parts[:-1]):
                wrapped = {key: wrapped}
            body = wrapped

        return body, headers


# ----- helpers ----------------------------------------------------------


def _resolve_sort(spec: EndpointSpec, query: Dict[str, str]) -> Tuple[Optional[str], str]:
    sort_field: Optional[str] = None
    sort_order = "asc"

    # Default first.
    if spec.response.default_sort is not None:
        sort_field, sort_order = spec.response.default_sort

    # Allow query-param overrides.
    if spec.sort_by is not None and spec.sort_by.name in query:
        candidate = query[spec.sort_by.name]
        if not spec.sort_by.options or candidate in spec.sort_by.options:
            sort_field = candidate
    if spec.sort_order is not None and spec.sort_order.name in query:
        candidate = query[spec.sort_order.name].lower()
        if candidate in spec.sort_order.options:
            sort_order = candidate

    return sort_field, sort_order


def _encode_body(body: Any) -> Tuple[bytes, str]:
    if isinstance(body, (dict, list)):
        return json.dumps(body, ensure_ascii=False).encode("utf-8"), "application/json"
    if isinstance(body, bytes):
        return body, "application/octet-stream"
    if body is None:
        return b"", "application/json"
    return str(body).encode("utf-8"), "text/plain"


def _build_error_response(
    prep: PreparedRequest, *, status_code: int, message: str
) -> Response:
    """Synthesize an HTTP error response with a JSON-encoded ``error`` body.

    Mirrors the shape most REST APIs use for client-error responses; lets the
    connector surface it via standard ``response.raise_for_status()`` flows.
    """
    body = json.dumps({"error": message, "status": status_code}).encode("utf-8")
    rec = ResponseRecord(
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def _invoke_custom_handler(
    spec_path: str, prep: PreparedRequest, spec: EndpointSpec, corpus: CorpusStore
) -> Response:
    """Resolve a ``module.path:function`` string and call it."""
    if ":" not in spec_path:
        raise ValueError(
            f"handler must be 'module.path:function', got {spec_path!r}"
        )
    module_name, func_name = spec_path.split(":", 1)
    import importlib

    module = importlib.import_module(module_name)
    func = getattr(module, func_name, None)
    if func is None:
        raise ValueError(f"handler function {func_name!r} not found in {module_name}")
    return func(prep, spec, corpus)
