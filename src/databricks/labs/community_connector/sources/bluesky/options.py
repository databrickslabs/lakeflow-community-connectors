"""Configuration shared by discovery, planning, and executor reads."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, cast
from urllib.parse import urlsplit

MAX_SEQ = (1 << 63) - 1
EventKind = Literal["commit", "identity", "account", "sync"]
EVENT_KINDS: tuple[EventKind, ...] = ("commit", "identity", "account", "sync")
TABLE_OPTIONS = {
    "starting_cursor",
    "collections",
    "kinds",
    "dids",
    "max_events_per_refresh",
    "request_timeout_seconds",
    "refresh_timeout_seconds",
    "max_retries",
    "include_raw_payload",
}
CONNECTION_OPTIONS = {"endpoint", "api_key"}
FRAMEWORK_OPTIONS = {
    "tableName",
    "table_name",
    "source",
    "connection_name",
    "databricks.connection",
    "externalOptionsAllowList",
    "external_options_allowlist",
    "operation",
    "sourceName",
    "auth_type",
    "tableNameList",
    "tableConfigs",
    "isDeleteFlow",
}


def integer(value: Any, name: str, minimum: int = 0, maximum: int = MAX_SEQ) -> int:
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
        raise ValueError(f"{name} must be an integer")
    result = int(value)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


def selection(value: Any, name: str, limit: int, pattern: str) -> tuple[str, ...]:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a comma-separated string")
    if not value.strip():
        return ()
    result = tuple(sorted(set(part.strip() for part in value.split(","))))
    if len(result) > limit or any(not re.fullmatch(pattern, part) for part in result):
        raise ValueError(f"{name} contains invalid entries or exceeds {limit} entries")
    return result


@dataclass(frozen=True)
class Options:
    """Validated settings used throughout one connector read.

    Attributes
    ----------
    endpoint : str
        HTTPS origin hosting the Jetstream archive API.
    api_key : str
        Optional bearer token for archive access. Hidden from the dataclass repr.
    starting_cursor : int
        Exclusive sequence cursor used when no checkpoint is available.
    collections : tuple[str, ...]
        Commit collections or namespace wildcards selected for the read.
    kinds : tuple[EventKind, ...]
        Jetstream event kinds selected for the read.
    dids : tuple[str, ...]
        Repository DIDs selected for the read.
    max_events_per_refresh : int
        Maximum sequence span planned for one refresh.
    request_timeout_seconds : int
        Per-request timeout limit.
    refresh_timeout_seconds : int
        Overall planning and reading time limit.
    max_retries : int
        Number of retries allowed for transient request failures.
    include_raw_payload : bool
        Whether decoded rows retain the encoded event payload.

    Notes
    -----
    Use :meth:`parse` to construct settings from Lakeflow or Dynaconf mappings.
    """

    endpoint: str
    api_key: str = field(repr=False)
    starting_cursor: int = 0
    collections: tuple[str, ...] = ()
    kinds: tuple[EventKind, ...] = EVENT_KINDS
    dids: tuple[str, ...] = ()
    max_events_per_refresh: int = 10000
    request_timeout_seconds: int = 20
    refresh_timeout_seconds: int = 120
    max_retries: int = 3
    include_raw_payload: bool = True

    @classmethod
    def parse(cls, connection: Mapping[str, Any], table: Mapping[str, Any]) -> "Options":
        # Spark lowercases keys in CaseInsensitiveDict, including framework options.
        connection = {key.lower(): value for key, value in connection.items()}
        table = {key.lower(): value for key, value in table.items()}
        unknown = (set(connection) | set(table)) - (
            TABLE_OPTIONS | CONNECTION_OPTIONS | {key.lower() for key in FRAMEWORK_OPTIONS}
        )
        if unknown:
            raise ValueError("Unsupported connector option names")
        if set(table) & CONNECTION_OPTIONS:
            # Framework forwards merged connection options to read methods.
            if any(table[k] != connection.get(k) for k in set(table) & CONNECTION_OPTIONS):
                raise ValueError("Connection options cannot be overridden per table")
        values = {**connection, **table}
        endpoint = values.get("endpoint", "https://jetstream.us-east.bsky.network").rstrip("/")
        url = urlsplit(endpoint)
        if (
            url.scheme != "https"
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.path
        ):
            raise ValueError("endpoint must be an HTTPS origin without credentials, path or query")
        key = values.get("api_key", "")
        if not isinstance(key, str) or any(c.isspace() for c in key):
            raise ValueError("api_key must be a raw bearer token without whitespace")
        raw = values.get("include_raw_payload", "true")
        if raw not in ("true", "false"):
            raise ValueError("include_raw_payload must be 'true' or 'false'")
        collections = selection(
            values.get("collections", ""),
            "collections",
            100,
            r"[a-zA-Z][a-zA-Z0-9-]*(?:\.[a-zA-Z][a-zA-Z0-9-]*)+\.(?:[a-zA-Z][a-zA-Z0-9]*|\*)",
        )
        kinds = selection(values.get("kinds", ",".join(EVENT_KINDS)), "kinds", 4, r"[a-z]+")
        if not kinds or any(kind not in EVENT_KINDS for kind in kinds):
            raise ValueError("kinds must contain one or more of: " + ", ".join(EVENT_KINDS))
        if collections and "commit" not in kinds:
            raise ValueError("collections can only be used when kinds includes commit")
        return cls(
            endpoint=endpoint,
            api_key=key,
            starting_cursor=integer(values.get("starting_cursor", "0"), "starting_cursor"),
            collections=collections,
            kinds=cast(tuple[EventKind, ...], kinds),
            dids=selection(
                values.get("dids", ""), "dids", 10000, r"did:[a-z0-9]+:[A-Za-z0-9._:%-]+"
            ),
            max_events_per_refresh=integer(
                values.get("max_events_per_refresh", "10000"), "max_events_per_refresh", 1, 100000
            ),
            request_timeout_seconds=integer(
                values.get("request_timeout_seconds", "20"), "request_timeout_seconds", 1, 120
            ),
            refresh_timeout_seconds=integer(
                values.get("refresh_timeout_seconds", "120"), "refresh_timeout_seconds", 1, 900
            ),
            max_retries=integer(values.get("max_retries", "3"), "max_retries", 0, 8),
            include_raw_payload=raw == "true",
        )

    def fingerprint(self) -> str:
        # Credentials and operational limits can rotate without invalidating a checkpoint.
        scope = [
            self.endpoint,
            self.starting_cursor,
            self.collections,
            self.kinds,
            self.dids,
            self.include_raw_payload,
        ]
        return hashlib.sha256(json.dumps(scope).encode()).hexdigest()

    def matches(self, event: Mapping[str, Any]) -> bool:
        if event["kind"] not in self.kinds:
            return False
        if self.dids and event["did"] not in self.dids:
            return False
        if event["kind"] != "commit" or not self.collections:
            return True
        collection = event["collection"]
        return any(
            collection == c or (c.endswith(".*") and collection.startswith(c[:-1]))
            for c in self.collections
        )

    def to_options_dict(self) -> dict[str, str]:
        """Return string options for the Lakeflow base interface boundary."""
        return {
            "endpoint": self.endpoint,
            "api_key": self.api_key,
            "starting_cursor": str(self.starting_cursor),
            "collections": ",".join(self.collections),
            "kinds": ",".join(self.kinds),
            "dids": ",".join(self.dids),
            "max_events_per_refresh": str(self.max_events_per_refresh),
            "request_timeout_seconds": str(self.request_timeout_seconds),
            "refresh_timeout_seconds": str(self.refresh_timeout_seconds),
            "max_retries": str(self.max_retries),
            "include_raw_payload": str(self.include_raw_payload).lower(),
        }

    def with_table_options(self, table: Mapping[str, Any]) -> "Options":
        """Return a validated config with table values layered over this config."""
        return type(self).parse(self.to_options_dict(), table)
