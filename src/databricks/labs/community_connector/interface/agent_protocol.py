"""Protocol vocabulary for the ingestion-agent surface.

Framework-wide constants and types used by the agent operation API
(error codes, parameter declarations, the protocol version). Lives in
``interface/`` because it's part of the contract — independent of the
Spark binding that consumes it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


class ErrorCode:
    """Canonical error codes for ``_meta.code``.

    Use one of these strings when raising :class:`AgentError` from an
    :class:`AgentOperation`. The framework defaults to ``internal_error``
    for uncategorised exceptions and ``bad_request`` for its own
    detected option errors.

    ``CONNECTION_FAILED`` is the conventional code that
    :class:`ValidateConnectionOp` implementations raise on a failed
    health check — keep it consistent across sources so agents can
    dispatch on it.
    """

    AUTH_FAILED = "auth_failed"
    NOT_FOUND = "not_found"
    PERMISSION_DENIED = "permission_denied"
    RATE_LIMITED = "rate_limited"
    BAD_REQUEST = "bad_request"
    UNSUPPORTED = "unsupported"
    INTERNAL_ERROR = "internal_error"
    CONNECTION_FAILED = "connection_failed"


class AgentError(Exception):
    """Connector-raised error with a canonical agent error code.

    Raise from :meth:`AgentOperation.pull` to communicate a typed error
    code to the agent. The framework populates ``_meta.code`` from
    ``self.code`` and ``_meta.message`` from the exception message.

    Args:
        code: One of the :class:`ErrorCode` constants.
        message: Human-readable detail; falls back to ``code`` if empty.
    """

    def __init__(self, code: str, message: str = ""):
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True)
class Parameter:
    """Typed declaration of an :class:`AgentOperation` input option.

    Exposed via ``list_operations.parameters_json`` so agents can plan
    calls without reading source code. The framework validates required
    parameters before invoking the op.

    Attributes:
        name: The option key callers pass on the Spark options dict.
        type: One of ``"string"``, ``"integer"``, ``"boolean"``, ``"json"``.
            Spark options arrive as strings; the op is responsible for
            parsing per the declared type. ``"json"`` indicates the
            value is a JSON-encoded list/dict packed into the
            string-typed option.
        description: One-line text shown to the agent planner.
        required: If True, the framework rejects calls missing this key
            with ``bad_request`` before invoking the op.
        default: Default value when the key is absent. Type-erased; the
            op interprets it.
        enum: If set, the value must be one of these strings.
    """

    name: str
    type: str = "string"
    description: str = ""
    required: bool = False
    default: Optional[Any] = None
    enum: Optional[tuple[str, ...]] = None
