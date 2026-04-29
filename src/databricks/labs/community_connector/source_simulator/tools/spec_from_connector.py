"""Extract endpoint URL patterns from a connector's Python source.

Walks the connector's AST, finds HTTP call sites (``requests.get``,
``session.post``, ``_make_request``, etc.), and pulls the URL template
out of the call's first argument. Templates that are f-strings have
their placeholders preserved as ``{name}`` segments — directly usable
in ``endpoints.yaml``.

This is the *URL discovery* half of bootstrapping a spec. The other
half (pagination style, response wrapper, filter/sort/page roles)
still needs hand-authoring — those aren't recoverable from connector
source alone.

Usage::

    from pathlib import Path
    from databricks.labs.community_connector.source_simulator.tools.spec_from_connector import (
        extract_url_patterns_from_file,
        write_draft_spec,
    )

    patterns = extract_url_patterns_from_file(
        Path("src/.../sources/microsoft_teams/microsoft_teams.py")
    )
    write_draft_spec(patterns, output_path=Path(".../specs/microsoft_teams/endpoints.yaml"))
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml


# Method-resolving call patterns we recognize.
HTTP_METHOD_NAMES = {"get", "post", "put", "delete", "patch", "head", "options", "request"}

# Attribute names that hold an HTTP "session" object (anything callable like .get(...)).
SESSION_ATTRS = {"_session", "session", "client", "_client", "_http", "http"}

# Attribute names commonly used to hold the connector's base URL — stripped from
# extracted paths.
BASE_URL_ATTRS = frozenset(
    {"base_url", "BASE_URL", "_base_url", "api_url", "_api_url", "endpoint", "_endpoint",
     "endpoint_url", "_endpoint_url", "cohorts_base_url"}
)

# Helper-method names connectors commonly route HTTP through. The first arg is
# typically the HTTP method (string), the second is the URL — but several of
# these (``make_request``, ``_fetch``) are called with just the URL and assume
# GET. The visitor handles both shapes.
HELPER_METHODS = {
    "_make_request", "make_request",
    "_request", "request",
    "_request_with_retry",
    "_fetch", "fetch",
    "_call_api", "call_api",
    "_get", "_post", "_put", "_delete",  # lowercase wrapper methods
}


@dataclass
class UrlPattern:
    method: str
    path: str  # canonical with {placeholders} or wildcards
    source_line: int = 0
    raw_template: str = ""  # original f-string before normalization


@dataclass
class ExtractionResult:
    patterns: List[UrlPattern] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)  # human-readable notes


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def extract_url_patterns(source_code: str) -> ExtractionResult:
    """Walk the AST of ``source_code`` and return the URL patterns it issues."""
    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        return ExtractionResult(skipped=[f"parse error: {e}"])

    visitor = _HttpCallVisitor()
    visitor.visit(tree)
    # Dedup by (method, path) pair — same URL hit from many code paths is
    # one endpoint to model.
    seen: Set[Tuple[str, str]] = set()
    deduped: List[UrlPattern] = []
    for p in visitor.patterns:
        key = (p.method, p.path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)
    return ExtractionResult(patterns=deduped, skipped=visitor.skipped)


def extract_url_patterns_from_file(path: Path) -> ExtractionResult:
    return extract_url_patterns(Path(path).read_text(encoding="utf-8"))


def write_draft_spec(
    result: ExtractionResult,
    output_path: Path,
    *,
    table_for_path: Optional[Dict[str, str]] = None,
    response_wrapper_for_path: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """Write a draft ``endpoints.yaml`` from extracted patterns.

    ``table_for_path`` maps a path string to the corpus table name. If a
    pattern's path isn't present, the spec entry omits ``corpus`` (handler
    will return an empty list — useful as a placeholder until the developer
    fills it in).

    ``response_wrapper_for_path`` lets you specify per-path wrapper config.
    """
    table_for_path = table_for_path or {}
    response_wrapper_for_path = response_wrapper_for_path or {}

    endpoints: List[Dict[str, Any]] = []
    for p in result.patterns:
        entry: Dict[str, Any] = {"path": p.path, "method": p.method}
        if p.path in table_for_path:
            entry["corpus"] = table_for_path[p.path]
        response: Dict[str, Any] = {"pagination_style": "none"}
        if p.path in response_wrapper_for_path:
            response["wrapper"] = response_wrapper_for_path[p.path]
        entry["response"] = response
        endpoints.append(entry)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Draft spec generated from connector source. URL paths are\n")
        f.write("# extracted; corpus / pagination / wrapper still need hand-tuning.\n")
        if result.skipped:
            f.write("# Notes from extraction:\n")
            for note in result.skipped:
                f.write(f"#   - {note}\n")
        yaml.safe_dump({"endpoints": endpoints}, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# AST visitor
# ---------------------------------------------------------------------------


class _HttpCallVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.patterns: List[UrlPattern] = []
        self.skipped: List[str] = []
        # Per-function symbol tables, reset on entering each function:
        #   _assignments: name -> AST node it was assigned to (for url=...).
        #   _base_aliases: names whose RHS resolves to a base_url-like attr
        #     (so f-string ``{base}/foo`` is treated as just ``/foo``).
        self._assignments: Dict[str, ast.AST] = {}
        self._base_aliases: Set[str] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        saved_assigns = self._assignments
        saved_bases = self._base_aliases
        self._assignments = {}
        self._base_aliases = set()
        self.generic_visit(node)
        self._assignments = saved_assigns
        self._base_aliases = saved_bases

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self.visit_FunctionDef(node)  # same scope handling

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target = node.targets[0].id
            self._assignments[target] = node.value
            # Also detect ``base = self._client.base_url`` style aliasing.
            if _is_base_url_expr(node.value):
                self._base_aliases.add(target)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        method, url_node = self._resolve_call(node)
        if method is None or url_node is None:
            self.generic_visit(node)
            return

        # If url_node is a Name, look up its assignment in the current scope.
        resolved = self._resolve_name(url_node)
        path, raw = _path_from_url_node(resolved, self._base_aliases)
        if path is None:
            self.skipped.append(
                f"line {node.lineno}: HTTP call but URL not extractable "
                f"(non-literal expression)"
            )
        else:
            self.patterns.append(
                UrlPattern(
                    method=method.upper(),
                    path=path,
                    source_line=node.lineno,
                    raw_template=raw,
                )
            )
        self.generic_visit(node)

    def _resolve_name(self, node: ast.AST, depth: int = 0) -> ast.AST:
        """If ``node`` is a Name with a tracked assignment, follow it.

        Limited recursion to avoid loops in pathological code.
        """
        if depth > 4:
            return node
        if isinstance(node, ast.Name) and node.id in self._assignments:
            return self._resolve_name(self._assignments[node.id], depth + 1)
        return node

    # --- call-shape recognition ---

    def _resolve_call(self, node: ast.Call) -> Tuple[Optional[str], Optional[ast.AST]]:
        """Return (http_method, url_node) for recognized call shapes."""
        func = node.func
        if not isinstance(func, ast.Attribute):
            return None, None

        attr = func.attr

        # Pattern: requests.get(url, ...) or requests.request("GET", url, ...)
        if isinstance(func.value, ast.Name) and func.value.id == "requests":
            return self._method_and_url_from_method_call(attr, node)

        # Pattern: <something>.<method>(url, ...) where <something> looks
        # like a session/client (self._session.get, self.client.post, …)
        if attr in HTTP_METHOD_NAMES and _looks_like_session(func.value):
            return self._method_and_url_from_method_call(attr, node)

        # Pattern: self._make_request("GET", url, ...) or ._fetch(...)
        if attr in HELPER_METHODS:
            return self._method_and_url_from_helper(node)

        return None, None

    def _method_and_url_from_method_call(
        self, method_name: str, node: ast.Call
    ) -> Tuple[Optional[str], Optional[ast.AST]]:
        if method_name == "request":
            # requests.request("GET", url, …)
            if len(node.args) >= 2 and isinstance(node.args[0], ast.Constant):
                method_val = node.args[0].value
                if isinstance(method_val, str):
                    return method_val.upper(), node.args[1]
            return None, None
        if not node.args:
            return None, None
        return method_name.upper(), node.args[0]

    def _method_and_url_from_helper(
        self, node: ast.Call
    ) -> Tuple[Optional[str], Optional[ast.AST]]:
        # Helper called with just URL: ``self._client.make_request(url)``.
        # Method comes from the helper name when possible (``_get``, ``_post``).
        if not node.args:
            return None, None

        helper_name = node.func.attr if isinstance(node.func, ast.Attribute) else ""

        # ``self._get(url)`` / ``self._post(url, body)`` style.
        if helper_name in {"_get", "_post", "_put", "_delete", "_patch"}:
            return helper_name.lstrip("_").upper(), node.args[0]

        # ``self._make_request(method, url, ...)`` — first arg literal method.
        first = node.args[0]
        if (
            isinstance(first, ast.Constant)
            and isinstance(first.value, str)
            and first.value.upper() in HTTP_METHOD_NAMES_UPPER
            and len(node.args) >= 2
        ):
            return first.value.upper(), node.args[1]

        # Fallback: assume GET on a single-URL helper like ``make_request(url)``.
        return "GET", node.args[0]


HTTP_METHOD_NAMES_UPPER = {m.upper() for m in HTTP_METHOD_NAMES}


def _looks_like_session(value: ast.AST) -> bool:
    """True if ``value`` looks like a session/client expression.

    Recognized:
        self._session, self.client, self._http
        session, client (bare names)
    """
    if isinstance(value, ast.Attribute):
        return value.attr in SESSION_ATTRS
    if isinstance(value, ast.Name):
        return value.id in SESSION_ATTRS
    return False


# ---------------------------------------------------------------------------
# URL string assembly from f-string nodes
# ---------------------------------------------------------------------------


_LEADING_HOST = re.compile(r"^https?://[^/]+")


def _path_from_url_node(
    node: ast.AST, base_aliases: Optional[Set[str]] = None
) -> Tuple[Optional[str], str]:
    """Return ``(canonical_path, raw_template)`` extracted from a URL node.

    ``canonical_path`` has placeholders preserved as ``{name}``. The raw
    template is the source representation (for debugging / notes).
    ``base_aliases`` is the set of local variable names that have been
    aliased to a base-url attribute (e.g. ``base = self._client.base_url``);
    those names are stripped from the path rather than turned into
    placeholders.
    """
    base_aliases = base_aliases or set()
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        raw = node.value
        return _normalize_path(raw), raw

    if isinstance(node, ast.JoinedStr):
        return _path_from_fstring(node, base_aliases)

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        # urls built via ``self.base_url + "/foo"`` etc.
        left, _ = _path_from_url_node(node.left, base_aliases)
        right, _ = _path_from_url_node(node.right, base_aliases)
        if left is None and right is None:
            return None, ""
        combined = (left or "") + (right or "")
        return _normalize_path(combined), combined

    return None, ""


def _path_from_fstring(
    node: ast.JoinedStr, base_aliases: Set[str]
) -> Tuple[Optional[str], str]:
    parts: List[str] = []
    raw_parts: List[str] = []
    for v in node.values:
        if isinstance(v, ast.Constant) and isinstance(v.value, str):
            parts.append(v.value)
            raw_parts.append(v.value)
            continue
        if isinstance(v, ast.FormattedValue):
            inner = v.value
            placeholder, raw = _placeholder_for(inner, base_aliases)
            raw_parts.append("{" + raw + "}")
            if placeholder is None:
                # unresolvable expression — emit a wildcard segment marker
                parts.append("{?}")
            else:
                parts.append(placeholder)
        else:
            raw_parts.append("?")
    raw = "".join(raw_parts)
    text = "".join(parts)
    return _normalize_path(text), raw


def _placeholder_for(expr: ast.AST, base_aliases: Set[str]) -> Tuple[Optional[str], str]:
    """Map an f-string-formatted expression to a path placeholder.

    Returns ``(placeholder_or_None, raw_repr)``. Empty string means "drop
    this segment entirely" — used for the connector's own ``base_url``
    reference (either as ``self.base_url`` directly, or via a local
    variable aliased to it).
    """
    if isinstance(expr, ast.Attribute):
        if expr.attr in BASE_URL_ATTRS:
            return "", _attr_chain_repr(expr)  # drop
        return f"{{{expr.attr}}}", _attr_chain_repr(expr)
    if isinstance(expr, ast.Name):
        if expr.id in base_aliases:
            return "", expr.id  # drop — it's an alias of a base url
        return f"{{{expr.id}}}", expr.id
    return None, ast.unparse(expr) if hasattr(ast, "unparse") else "?"


def _is_base_url_expr(expr: ast.AST) -> bool:
    """True if ``expr`` is a reference to a base-url-like attribute."""
    if isinstance(expr, ast.Attribute) and expr.attr in BASE_URL_ATTRS:
        return True
    return False


def _attr_chain_repr(expr: ast.Attribute) -> str:
    """Reasonable text for ``self.base_url`` style chains."""
    parts: List[str] = []
    cur: ast.AST = expr
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
    return ".".join(reversed(parts))


def _normalize_path(raw: str) -> Optional[str]:
    """Drop scheme+host, ensure leading slash, collapse repeated slashes."""
    if not raw:
        return None
    # Drop full scheme://host prefix if present.
    s = _LEADING_HOST.sub("", raw)
    # If still starts with non-slash, the connector probably built url as
    # base_url + "endpoint" — assume leading slash.
    if not s.startswith("/"):
        s = "/" + s
    # Drop any trailing query string (fragment, etc.) — we model query
    # params via the ``params`` block, not in the path.
    s = s.split("?", 1)[0]
    s = s.split("#", 1)[0]
    # Collapse duplicate slashes.
    s = re.sub(r"//+", "/", s)
    # Trim trailing slash unless path is exactly "/".
    if len(s) > 1 and s.endswith("/"):
        s = s.rstrip("/")
    return s
