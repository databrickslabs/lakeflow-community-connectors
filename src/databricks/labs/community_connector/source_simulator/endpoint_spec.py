"""Endpoint spec format + YAML loader.

Each connector's spec lives at ``source_simulator/specs/<source>/endpoints.yaml``
and declares the endpoints the simulator should serve. See ``DESIGN.md`` for
the spec format and the worked example.

Loaded into a list of ``EndpointSpec`` objects. Path strings with
``{placeholders}`` are compiled to a regex for matching incoming requests.
The handler buckets each endpoint's params by role (filter / sort / page /
per_page / offset / limit / ignore) at load time so the request-handling
hot path stays simple.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import yaml


class FilterOp(str, Enum):
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"


class ParamRole(str, Enum):
    FILTER = "filter"
    SORT_BY = "sort_by"
    SORT_ORDER = "sort_order"
    PAGE = "page"
    PER_PAGE = "per_page"
    OFFSET = "offset"
    LIMIT = "limit"
    IGNORE = "ignore"


@dataclass
class FilterParam:
    name: str  # query param name
    field: str  # dotted path into the record
    op: FilterOp


@dataclass
class SortByParam:
    name: str
    options: Tuple[str, ...] = ()  # whitelist of allowed values; empty = no constraint


@dataclass
class SortOrderParam:
    name: str
    options: Tuple[str, ...] = ("asc", "desc")


@dataclass
class PageParam:
    name: str = "page"


@dataclass
class PerPageParam:
    name: str = "per_page"
    default: int = 30
    max: int = 100


@dataclass
class OffsetParam:
    name: str = "offset"


@dataclass
class LimitParam:
    name: str = "limit"
    default: int = 30
    max: int = 100


@dataclass
class ResponseWrapper:
    """Optional response-body wrapper.

    Many APIs wrap record arrays in a dict, e.g. zendesk:
        {"tickets": [...], "next_page": null, "count": 42}
    Or oData:
        {"value": [...], "@odata.nextLink": null}

    ``records_key`` is the field name that holds the records array.
    ``extras`` is a dict of fixed fields to include in the wrapper.
    """

    records_key: str
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResponseShape:
    pagination_style: str = "none"
    default_sort: Optional[Tuple[str, str]] = None  # (field, "asc"|"desc")
    single_entity: bool = False
    wrapper: Optional[ResponseWrapper] = None


@dataclass
class FutureRecordsSynthesis:
    """Spec directive for injecting cap-validation records into a corpus.

    When the simulator runs with future-record injection enabled, the
    corpus for this endpoint is augmented with ``count`` clones of an
    existing record whose ``cursor_field`` (dotted path) is rewritten to
    ISO-8601 timestamps strictly past wall-clock now(). A connector with
    correct ``until=<init_time>`` cap behavior will exclude these from
    its returned offset; an uncapped connector will leak them and the
    termination test will detect non-convergence.
    """

    cursor_field: str
    count: int = 3


@dataclass
class EndpointSpec:
    """A single endpoint's spec entry, post-load."""

    method: str
    path: str  # original path with placeholders, e.g. "/repos/{owner}/{repo}/issues"
    path_regex: re.Pattern  # compiled to match request URLs
    corpus: Optional[str]  # name of corpus table; None for endpoints with no records
    response: ResponseShape
    handler: Optional[str] = None  # "module.path:function" — escape hatch
    # When True, requests with query params not declared in this spec are
    # rejected with a 400 response — mirrors how a real API rejects unknown
    # params. Default False for backward compat with permissive specs.
    strict_params: bool = False
    # Param buckets, populated at load time:
    filters: List[FilterParam] = field(default_factory=list)
    sort_by: Optional[SortByParam] = None
    sort_order: Optional[SortOrderParam] = None
    page: Optional[PageParam] = None
    per_page: Optional[PerPageParam] = None
    offset: Optional[OffsetParam] = None
    limit: Optional[LimitParam] = None
    ignored: List[str] = field(default_factory=list)
    # Optional cap-validation directive — see ``FutureRecordsSynthesis``.
    synthesize_future_records: Optional[FutureRecordsSynthesis] = None

    def known_param_names(self) -> set:
        """All query-param names this endpoint declares."""
        names: set = set()
        for fp in self.filters:
            names.add(fp.name)
        if self.sort_by:
            names.add(self.sort_by.name)
        if self.sort_order:
            names.add(self.sort_order.name)
        if self.page:
            names.add(self.page.name)
        if self.per_page:
            names.add(self.per_page.name)
        if self.offset:
            names.add(self.offset.name)
        if self.limit:
            names.add(self.limit.name)
        names.update(self.ignored)
        return names


def load_specs(path: Path) -> List[EndpointSpec]:
    """Load and validate ``endpoints.yaml`` from ``path``."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict) or "endpoints" not in data:
        raise ValueError(f"{path}: top-level must be a dict with an 'endpoints' key")
    specs: List[EndpointSpec] = []
    for i, raw in enumerate(data["endpoints"]):
        try:
            specs.append(_parse_endpoint(raw))
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"{path}: endpoint #{i}: {e}") from e
    return specs


def match_endpoint(
    specs: List[EndpointSpec], method: str, url: str
) -> Optional[Tuple[EndpointSpec, Dict[str, str]]]:
    """Return ``(spec, path_params)`` for the first endpoint matching the URL.

    ``url`` is the full URL — only the path is matched against the spec's
    pattern. Returns ``None`` if no spec matches.
    """
    method_upper = method.upper()
    parsed = urlsplit(url)
    for spec in specs:
        if spec.method != method_upper:
            continue
        m = spec.path_regex.match(parsed.path)
        if m:
            return spec, m.groupdict()
    return None


# ----- internals ---------------------------------------------------------


_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _path_to_regex(path: str) -> re.Pattern:
    """Convert ``/repos/{owner}/{repo}/issues`` to a ``re.Pattern``."""
    pattern_parts: List[str] = []
    last = 0
    for m in _PLACEHOLDER_RE.finditer(path):
        pattern_parts.append(re.escape(path[last:m.start()]))
        pattern_parts.append(rf"(?P<{m.group(1)}>[^/]+)")
        last = m.end()
    pattern_parts.append(re.escape(path[last:]))
    return re.compile("^" + "".join(pattern_parts) + "$")


def _parse_endpoint(raw: dict) -> EndpointSpec:
    method = str(raw["method"]).upper()
    path = str(raw["path"])
    corpus = raw.get("corpus")  # may be None for handler-only endpoints
    response = _parse_response(raw.get("response") or {})
    handler = raw.get("handler")

    spec = EndpointSpec(
        method=method,
        path=path,
        path_regex=_path_to_regex(path),
        corpus=corpus,
        response=response,
        handler=handler,
        strict_params=bool(raw.get("strict_params", False)),
    )

    for name, info in (raw.get("params") or {}).items():
        _attach_param(spec, name, info)

    sfr_raw = raw.get("synthesize_future_records")
    if sfr_raw is not None:
        if not isinstance(sfr_raw, dict) or "cursor_field" not in sfr_raw:
            raise ValueError(
                "synthesize_future_records requires a 'cursor_field' field"
            )
        spec.synthesize_future_records = FutureRecordsSynthesis(
            cursor_field=str(sfr_raw["cursor_field"]),
            count=int(sfr_raw.get("count", 3)),
        )

    return spec


def _parse_response(raw: dict) -> ResponseShape:
    default_sort_raw = raw.get("default_sort")
    default_sort: Optional[Tuple[str, str]] = None
    if default_sort_raw:
        parts = str(default_sort_raw).split()
        if len(parts) == 1:
            default_sort = (parts[0], "asc")
        elif len(parts) == 2:
            order = parts[1].lower()
            if order not in ("asc", "desc"):
                raise ValueError(f"default_sort order must be asc/desc, got {order!r}")
            default_sort = (parts[0], order)
        else:
            raise ValueError(f"default_sort: expected 'field [asc|desc]', got {default_sort_raw!r}")

    wrapper: Optional[ResponseWrapper] = None
    raw_wrapper = raw.get("wrapper")
    if raw_wrapper:
        if not isinstance(raw_wrapper, dict) or "records_key" not in raw_wrapper:
            raise ValueError("response.wrapper requires a 'records_key' field")
        wrapper = ResponseWrapper(
            records_key=str(raw_wrapper["records_key"]),
            extras=dict(raw_wrapper.get("extras", {})),
        )

    return ResponseShape(
        pagination_style=str(raw.get("pagination_style", "none")),
        default_sort=default_sort,
        single_entity=bool(raw.get("single_entity", False)),
        wrapper=wrapper,
    )


def _attach_param(spec: EndpointSpec, name: str, info: Any) -> None:
    if not isinstance(info, dict):
        raise ValueError(f"param {name!r}: expected mapping, got {type(info).__name__}")
    role_str = str(info.get("role", "")).lower()
    try:
        role = ParamRole(role_str)
    except ValueError:
        raise ValueError(
            f"param {name!r}: unknown role {role_str!r}; "
            f"expected one of {[r.value for r in ParamRole]}"
        )

    if role == ParamRole.FILTER:
        op_str = str(info.get("op", "")).lower()
        try:
            op = FilterOp(op_str)
        except ValueError:
            raise ValueError(
                f"param {name!r}: unknown filter op {op_str!r}; "
                f"expected one of {[o.value for o in FilterOp]}"
            )
        if "field" not in info:
            raise ValueError(f"param {name!r}: filter requires 'field'")
        spec.filters.append(FilterParam(name=name, field=str(info["field"]), op=op))

    elif role == ParamRole.SORT_BY:
        if spec.sort_by is not None:
            raise ValueError(f"only one sort_by param allowed; second was {name!r}")
        opts = tuple(str(x) for x in info.get("options", []))
        spec.sort_by = SortByParam(name=name, options=opts)

    elif role == ParamRole.SORT_ORDER:
        if spec.sort_order is not None:
            raise ValueError(f"only one sort_order param allowed; second was {name!r}")
        opts = tuple(str(x).lower() for x in info.get("options", ["asc", "desc"]))
        spec.sort_order = SortOrderParam(name=name, options=opts)

    elif role == ParamRole.PAGE:
        if spec.page is not None:
            raise ValueError(f"only one page param allowed; second was {name!r}")
        spec.page = PageParam(name=name)

    elif role == ParamRole.PER_PAGE:
        if spec.per_page is not None:
            raise ValueError(f"only one per_page param allowed; second was {name!r}")
        spec.per_page = PerPageParam(
            name=name,
            default=int(info.get("default", 30)),
            max=int(info.get("max", 100)),
        )

    elif role == ParamRole.OFFSET:
        if spec.offset is not None:
            raise ValueError(f"only one offset param allowed; second was {name!r}")
        spec.offset = OffsetParam(name=name)

    elif role == ParamRole.LIMIT:
        if spec.limit is not None:
            raise ValueError(f"only one limit param allowed; second was {name!r}")
        spec.limit = LimitParam(
            name=name,
            default=int(info.get("default", 30)),
            max=int(info.get("max", 100)),
        )

    elif role == ParamRole.IGNORE:
        spec.ignored.append(name)
