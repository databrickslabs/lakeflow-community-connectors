"""Configuration shared by discovery, planning, and executor reads."""

import hashlib
import json
import re
from typing import Annotated, Any, Literal, Mapping
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

MAX_SEQ = (1 << 63) - 1
EventKind = Literal["commit", "identity", "account", "sync"]
EVENT_KINDS: tuple[EventKind, ...] = ("commit", "identity", "account", "sync")
Collection = Annotated[
    str,
    StringConstraints(
        pattern=r"^[a-zA-Z][a-zA-Z0-9-]*(?:\.[a-zA-Z][a-zA-Z0-9-]*)+\.(?:[a-zA-Z][a-zA-Z0-9]*|\*)$"
    ),
]
Did = Annotated[str, StringConstraints(pattern=r"^did:[a-z0-9]+:[A-Za-z0-9._:%-]+$")]
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


class Options(BaseModel):
    """Validated settings used throughout one connector read.

    Attributes
    ----------
    endpoint : str
        HTTPS origin hosting the Jetstream archive API.
    api_key : str
        Optional bearer token for archive access. Hidden from the model repr.
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

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    endpoint: str = "https://jetstream.us-east.bsky.network"
    api_key: str = Field(default="", repr=False, exclude=True)
    starting_cursor: int = Field(default=0, ge=0, le=MAX_SEQ)
    collections: tuple[Collection, ...] = Field(default=(), max_length=100)
    kinds: tuple[EventKind, ...] = Field(
        default=tuple(sorted(EVENT_KINDS)), min_length=1, max_length=4
    )
    dids: tuple[Did, ...] = Field(default=(), max_length=10000)
    max_events_per_refresh: int = Field(default=10000, ge=1, le=100000)
    request_timeout_seconds: int = Field(default=20, ge=1, le=120)
    refresh_timeout_seconds: int = Field(default=120, ge=1, le=900)
    max_retries: int = Field(default=3, ge=0, le=8)
    include_raw_payload: bool = True

    @field_validator("endpoint", mode="before")
    @classmethod
    def validate_endpoint(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("endpoint must be an HTTPS origin")
        endpoint = value.rstrip("/")
        url = urlsplit(endpoint)
        if (  # pylint: disable=too-many-boolean-expressions
            url.scheme != "https"
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.path
        ):
            raise ValueError("endpoint must be an HTTPS origin without credentials, path or query")
        return endpoint

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: str) -> str:
        if any(char.isspace() for char in value):
            raise ValueError("api_key must be a raw bearer token without whitespace")
        return value

    @field_validator(
        "starting_cursor",
        "max_events_per_refresh",
        "request_timeout_seconds",
        "refresh_timeout_seconds",
        "max_retries",
        mode="before",
    )
    @classmethod
    def validate_decimal(cls, value: Any) -> Any:
        if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
            raise ValueError("must be an unsigned decimal integer")
        return value

    @field_validator("collections", "kinds", "dids", mode="before")
    @classmethod
    def parse_selection(cls, value: Any) -> tuple[str, ...]:
        if not isinstance(value, str):
            raise ValueError("must be a comma-separated string")
        if not value.strip():
            return ()
        return tuple(sorted(set(part.strip() for part in value.split(","))))

    @field_validator("include_raw_payload", mode="before")
    @classmethod
    def validate_raw_payload(cls, value: Any) -> bool:
        if value not in ("true", "false"):
            raise ValueError("include_raw_payload must be 'true' or 'false'")
        return value == "true"

    @model_validator(mode="after")
    def validate_scope(self) -> "Options":
        if self.collections and "commit" not in self.kinds:
            raise ValueError("collections can only be used when kinds includes commit")
        return self

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
        values = {
            key: value
            for key, value in {**connection, **table}.items()
            if key in TABLE_OPTIONS | CONNECTION_OPTIONS
        }
        return cls.model_validate(values)

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
