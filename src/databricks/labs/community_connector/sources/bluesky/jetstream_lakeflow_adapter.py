"""Adapt the public Jetstream client API to Lakeflow's row and error contracts."""

import hashlib
import json
from contextlib import contextmanager
from typing import Any, Iterator
from urllib.parse import urlsplit

from jetstream_lakehouse import ArchivePlan, Client, Config
from jetstream_lakehouse import errors as library_errors

from databricks.labs.community_connector.sources.bluesky.errors import (
    ArchiveChanged,
    CursorTooOld,
    JetstreamError,
    ProtocolError,
    RefreshTimeout,
)
from databricks.labs.community_connector.sources.bluesky.options import Options


def lakeflow_row(event: Any, include_raw_payload: bool) -> dict[str, Any]:
    """Restore Lakeflow's base64 and non-null JSON values from a library event."""
    row = event.as_dict()
    raw = row.pop("raw_payload_base64")
    # Spark's BinaryType parser decodes base64; the existing connector returns text.
    row["raw_payload"] = raw if raw is not None else "" if include_raw_payload else None
    # Lakeflow's event_payload column is non-nullable; empty CBOR is JSON null.
    row["event_payload"] = row["event_payload"] or "null"
    return row


def client_for_options(options: Options, max_batch_bytes: int) -> Client:
    """Build a library client from validated connector settings."""
    config = Config(
        endpoint=options.endpoint,
        api_key=options.api_key,
        collections=options.collections,
        kinds=options.kinds,
        dids=options.dids,
        max_sequence_span=options.max_events_per_refresh,
        max_batch_bytes=max_batch_bytes,
        request_timeout_seconds=options.request_timeout_seconds,
        refresh_timeout_seconds=options.refresh_timeout_seconds,
        max_retries=options.max_retries,
        include_raw_payload=options.include_raw_payload,
    )
    return Client(config)


@contextmanager
def translated_errors() -> Iterator[None]:
    """Keep connector exception types while suppressing potentially unsafe causes."""
    try:
        yield
    except library_errors.ArchiveChanged as exc:
        raise ArchiveChanged(str(exc)) from None
    except library_errors.CursorTooOld as exc:
        raise CursorTooOld(str(exc)) from None
    except library_errors.RefreshTimeout as exc:
        raise RefreshTimeout(str(exc)) from None
    except library_errors.ProtocolError as exc:
        raise ProtocolError(str(exc)) from None
    except library_errors.JetstreamError as exc:
        raise JetstreamError(str(exc)) from None


@contextmanager
def operation_context(
    stage: str, options: Options, after: int, through: int | None = None
) -> Iterator[None]:
    """Add safe operation and sequence context to a public client failure."""
    bounds = f"({after}, {through}]" if through is not None else f"after {after}"
    context = f"{stage} failed for sequence {bounds} at {options.endpoint}"
    try:
        yield
    except ArchiveChanged as exc:
        raise ArchiveChanged(f"{context}: {exc}") from None
    except CursorTooOld as exc:
        raise CursorTooOld(f"{context}: {exc}") from None
    except RefreshTimeout as exc:
        raise RefreshTimeout(f"{context}: {exc}") from None
    except ProtocolError as exc:
        raise ProtocolError(f"{context}: {exc}") from None
    except JetstreamError as exc:
        raise JetstreamError(f"{context}: {exc}") from None


def plan_from_json(value: str) -> ArchivePlan:
    """Restore and validate a persisted library plan at the connector boundary."""
    try:
        with translated_errors():
            return ArchivePlan.from_json(value)
    except ValueError:
        raise ProtocolError("Invalid saved archive plan") from None


def migrate_legacy_plan(value: str, options: Options, after: int, through: int) -> ArchivePlan:
    """Convert v1 planner pages through the released plan validation contract.

    The old Lakeflow envelope is already validated by its caller. This conversion
    performs no network access and preserves every recorded descriptor and bound.
    """
    try:
        pages = json.loads(value)
        if not isinstance(pages, list):
            raise ValueError
        converted = [
            {
                "afterSeq": page["after"],
                "throughSeq": page["through"],
                "segments": page["segments"],
            }
            for page in pages
        ]
        endpoint = urlsplit(options.endpoint)
        hostname = endpoint.hostname.encode("idna").decode("ascii").lower()
        if ":" in hostname:
            hostname = f"[{hostname}]"
        port = endpoint.port
        normalized_endpoint = f"https://{hostname}" + (
            f":{port}" if port not in (None, 443) else ""
        )
        scope = {
            "endpoint": normalized_endpoint,
            "kinds": sorted(set(options.kinds)),
            "collections": sorted(set(options.collections)),
            "dids": sorted(set(options.dids)),
            "includeRawPayload": options.include_raw_payload,
        }
        encoded_scope = json.dumps(scope, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        fingerprint = "sha256-v1:" + hashlib.sha256(encoded_scope.encode("utf-8")).hexdigest()
        payload = {
            "formatVersion": 1,
            "scopeFingerprint": fingerprint,
            "afterSeq": after,
            "throughSeq": through,
            "pages": converted,
        }
        with translated_errors():
            plan = ArchivePlan.from_json(json.dumps(payload, separators=(",", ":")))
        if plan.after_seq != after or plan.through_seq != through:
            raise ValueError
        return plan
    except (KeyError, TypeError, ValueError, RecursionError, ProtocolError):
        raise ProtocolError("Legacy checkpoint plan cannot be migrated safely") from None
