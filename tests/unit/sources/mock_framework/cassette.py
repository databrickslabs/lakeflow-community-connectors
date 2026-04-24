"""Cassette format, serialization, and request matching."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlsplit

CASSETTE_VERSION = 1

# Header names redacted at record time. Lowercased for comparison.
SCRUBBED_REQUEST_HEADERS = frozenset(
    {
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-api-token",
        "x-api-key",
        "x-auth-token",
    }
)
REDACTED = "***REDACTED***"


class NoMatchingInteraction(AssertionError):
    """Raised in replay mode when an incoming request has no cassette match."""


@dataclass
class RequestRecord:
    method: str
    url: str  # scheme + host + path; query is captured separately
    query: Dict[str, str]
    body_sha256: Optional[str]

    def match_key(
        self, ignore_query_params: frozenset[str] = frozenset()
    ) -> Tuple[str, str, Tuple[Tuple[str, str], ...], Optional[str]]:
        filtered = {
            k: v for k, v in self.query.items() if k not in ignore_query_params
        }
        return (
            self.method.upper(),
            self.url,
            tuple(sorted(filtered.items())),
            self.body_sha256,
        )

    def to_json(self) -> dict:
        return {
            "method": self.method.upper(),
            "url": self.url,
            "query": self.query,
            "body_sha256": self.body_sha256,
        }

    @classmethod
    def from_json(cls, data: dict) -> "RequestRecord":
        return cls(
            method=data["method"],
            url=data["url"],
            query=data.get("query") or {},
            body_sha256=data.get("body_sha256"),
        )


@dataclass
class ResponseRecord:
    status_code: int
    headers: Dict[str, str]
    body_text: Optional[str]  # utf-8 string when body decodes cleanly
    body_b64: Optional[str]  # base64 fallback for binary bodies
    encoding: Optional[str]
    url: Optional[str]  # final URL (may differ from request.url on redirects)

    def content_bytes(self) -> bytes:
        if self.body_text is not None:
            enc = self.encoding or "utf-8"
            return self.body_text.encode(enc)
        if self.body_b64 is not None:
            return base64.b64decode(self.body_b64)
        return b""

    def to_json(self) -> dict:
        return {
            "status_code": self.status_code,
            "headers": self.headers,
            "body_text": self.body_text,
            "body_b64": self.body_b64,
            "encoding": self.encoding,
            "url": self.url,
        }

    @classmethod
    def from_json(cls, data: dict) -> "ResponseRecord":
        return cls(
            status_code=data["status_code"],
            headers=data.get("headers") or {},
            body_text=data.get("body_text"),
            body_b64=data.get("body_b64"),
            encoding=data.get("encoding"),
            url=data.get("url"),
        )


@dataclass
class Interaction:
    request: RequestRecord
    response: ResponseRecord

    def to_json(self) -> dict:
        return {"request": self.request.to_json(), "response": self.response.to_json()}

    @classmethod
    def from_json(cls, data: dict) -> "Interaction":
        return cls(
            request=RequestRecord.from_json(data["request"]),
            response=ResponseRecord.from_json(data["response"]),
        )


@dataclass
class Cassette:
    """An ordered list of request/response interactions, persisted as JSON.

    Matching is round-robin within a match-key bucket: the first recorded
    interaction with a matching key is returned first, the second on the
    next call, and so on — wrapping back to the first when exhausted. This
    lets the same endpoint be called many times across tests while still
    letting multi-recording sequences (rare, e.g. write-then-read) replay
    in order.

    ``ignore_query_params`` strips named query params from both sides of
    the match — use it for non-deterministic values like ``now()``-based
    ``until`` timestamps, request IDs, or nonces.
    """

    path: Path
    source: str = ""
    interactions: List[Interaction] = field(default_factory=list)
    ignore_query_params: frozenset[str] = field(default_factory=frozenset)
    _next_index: Dict[tuple, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        pass

    @classmethod
    def load(cls, path: Path) -> "Cassette":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        version = data.get("version")
        if version != CASSETTE_VERSION:
            raise ValueError(
                f"Unsupported cassette version {version!r} at {path} "
                f"(expected {CASSETTE_VERSION})"
            )
        interactions = [Interaction.from_json(i) for i in data.get("interactions", [])]
        return cls(
            path=path,
            source=data.get("source", ""),
            interactions=interactions,
        )

    @classmethod
    def empty(cls, path: Path, source: str = "") -> "Cassette":
        return cls(path=path, source=source, interactions=[])

    def append(self, interaction: Interaction) -> None:
        self.interactions.append(interaction)

    def has_key(self, req: RequestRecord) -> bool:
        """Check whether any recorded interaction matches ``req``'s key."""
        key = req.match_key(self.ignore_query_params)
        return any(
            ix.request.match_key(self.ignore_query_params) == key
            for ix in self.interactions
        )

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": CASSETTE_VERSION,
            "source": self.source,
            "interactions": [i.to_json() for i in self.interactions],
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
            f.write("\n")

    def match(self, req: RequestRecord) -> ResponseRecord:
        """Return the next recorded response matching ``req`` (round-robin)."""
        key = req.match_key(self.ignore_query_params)
        matching_indices = [
            i
            for i, ix in enumerate(self.interactions)
            if ix.request.match_key(self.ignore_query_params) == key
        ]
        if not matching_indices:
            sample_keys = [
                ix.request.match_key(self.ignore_query_params)
                for ix in self.interactions[:5]
            ]
            raise NoMatchingInteraction(
                f"No cassette match for request {key} in {self.path}.\n"
                f"  Sample of {len(self.interactions)} recorded keys:\n"
                + "\n".join(f"    {k}" for k in sample_keys)
                + ("\n    ..." if len(self.interactions) > 5 else "")
            )

        pos = self._next_index.get(key, 0) % len(matching_indices)
        self._next_index[key] = pos + 1
        return self.interactions[matching_indices[pos]].response


# ----- helpers -----------------------------------------------------------


def split_url(url: str) -> Tuple[str, Dict[str, str]]:
    """Split a URL into (scheme+netloc+path, query-dict)."""
    parts = urlsplit(url)
    base = f"{parts.scheme}://{parts.netloc}{parts.path}"
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    return base, query


def body_sha256(body: Any) -> Optional[str]:
    """Hash a request body. None/empty bodies return None."""
    if body is None:
        return None
    if isinstance(body, str):
        if not body:
            return None
        raw = body.encode("utf-8")
    elif isinstance(body, (bytes, bytearray)):
        if not body:
            return None
        raw = bytes(body)
    else:
        # Let requests normalize other shapes — hash their repr as a fallback.
        raw = repr(body).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def scrub_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Return a copy of ``headers`` with sensitive values redacted."""
    out: Dict[str, str] = {}
    for k, v in (headers or {}).items():
        if k.lower() in SCRUBBED_REQUEST_HEADERS:
            out[k] = REDACTED
        else:
            out[k] = v
    return out


def encode_body(raw: bytes, encoding: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Encode a response body as (text, base64). Exactly one is non-None."""
    if not raw:
        return "", None
    enc = encoding or "utf-8"
    try:
        return raw.decode(enc), None
    except (UnicodeDecodeError, LookupError):
        return None, base64.b64encode(raw).decode("ascii")
