# Gmail agent operations: typed filter read + per-message tools.
#
# Exposed via ``GmailLakeflowConnect.agent_operations`` and dispatched
# through the lakeflow_connect format with ``operation=<name>``. Agents
# call ``list_operations`` to discover names, parameters, and result
# schemas.
#
# Five operations:
#   - read_table (override): adds typed filters that compose into Gmail's
#     ``q`` syntax (after_date, before_date, newer_than, subject, from_address,
#     to_address, label, has_attachment, is_unread, query). Other tables use
#     the framework default. Caller chains ``.limit(N)`` on the DataFrame.
#   - search_messages: lightweight triage — id/threadId/from/subject/date/snippet
#     without fetching message bodies. Same typed filters as read_table.
#   - get_message: fetch one message by id with decoded plain-text + HTML.
#   - list_attachments: enumerate attachments on a message (gmail-native plus
#     Drive-hosted links extracted from the body).
#   - download_attachment: write attachment bytes to a Databricks volume path.
#     Routes through Gmail's attachments.get or the Drive API depending on
#     which ID is supplied. Requires the OAuth refresh token to have been
#     granted both gmail.readonly and drive.readonly at consent time.

from __future__ import annotations

import os
from typing import Any, Iterable, Mapping, Optional

from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import (
    AgentError,
    AgentOperation,
    ErrorCode,
    Parameter,
)
from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import (
    ReadTableOp,
    _connector_options,
)
from databricks.labs.community_connector.sources.gmail.gmail_utils import (
    GmailApiError,
    b64url_decode,
    extract_drive_file_ids,
)


# ---------------------------------------------------------------------------
# Filter composition: typed agent params → Gmail q syntax
# ---------------------------------------------------------------------------

# Tables that accept Gmail search filters. Anything outside this set is
# previewed with the framework default (full table snapshot).
_FILTERABLE_TABLES = frozenset({"messages", "threads"})

# Typed filter parameters shared by read_table override and search_messages.
# Each tuple is (param, render_fn) where render_fn(value) → q fragment.
def _q_quote(value: str) -> str:
    """Wrap a multi-word value in parentheses so Gmail treats it as one operand."""
    value = value.strip()
    if not value:
        return ""
    if " " in value and not (value.startswith("(") and value.endswith(")")):
        return f"({value})"
    return value


def _render_after(v: str) -> str:
    return f"after:{v}"


def _render_before(v: str) -> str:
    return f"before:{v}"


def _render_newer_than(v: str) -> str:
    return f"newer_than:{v}"


def _render_subject(v: str) -> str:
    return f"subject:{_q_quote(v)}"


def _render_from(v: str) -> str:
    return f"from:{v}"


def _render_to(v: str) -> str:
    return f"to:{v}"


def _render_label(v: str) -> str:
    return f"label:{v}"


def _render_has_attachment(v: str) -> str:
    return "has:attachment" if v.lower() == "true" else ""


def _render_is_unread(v: str) -> str:
    return "is:unread" if v.lower() == "true" else ""


def _render_passthrough(v: str) -> str:
    # Free-form query — let the caller author Gmail syntax directly.
    return v.strip()


_FILTER_RENDERERS = (
    ("after_date", _render_after),
    ("before_date", _render_before),
    ("newer_than", _render_newer_than),
    ("subject", _render_subject),
    ("from_address", _render_from),
    ("to_address", _render_to),
    ("label", _render_label),
    ("has_attachment", _render_has_attachment),
    ("is_unread", _render_is_unread),
    ("query", _render_passthrough),
)


_FILTER_PARAMETERS = (
    Parameter(
        name="after_date",
        description="Date filter — messages on or after YYYY/MM/DD (day granularity).",
    ),
    Parameter(
        name="before_date",
        description="Date filter — messages strictly before YYYY/MM/DD (day granularity).",
    ),
    Parameter(
        name="newer_than",
        description="Relative window like '7d', '2m', '1y' (no hours/seconds).",
    ),
    Parameter(name="subject", description="Subject keyword or phrase."),
    Parameter(name="from_address", description="Sender email or substring."),
    Parameter(name="to_address", description="Recipient email or substring."),
    Parameter(
        name="label",
        description="Gmail label name (e.g. 'inbox', 'work/urgent'). Spaces become '-'.",
    ),
    Parameter(
        name="has_attachment",
        type="boolean",
        description="If true, restrict to messages with attachments.",
    ),
    Parameter(
        name="is_unread",
        type="boolean",
        description="If true, restrict to unread messages.",
    ),
    Parameter(
        name="query",
        description=(
            "Free-form Gmail search expression appended verbatim. "
            "Combined with the typed filters via AND."
        ),
    ),
)


def _compose_query(options: Mapping[str, str]) -> Optional[str]:
    """Compose typed filter params into a Gmail ``q`` string.

    Returns ``None`` when no filters were supplied (preserves the
    connector's default listing behaviour). Multiple typed filters are
    joined with implicit AND, which is Gmail's default conjunction.
    """
    fragments: list[str] = []
    for name, render in _FILTER_RENDERERS:
        raw = options.get(name)
        if raw is None or raw == "":
            continue
        fragment = render(raw)
        if fragment:
            fragments.append(fragment)
    # Caller's pre-existing q passthrough on the connector — preserve it.
    raw_q = options.get("q")
    if raw_q:
        fragments.append(raw_q)
    if not fragments:
        return None
    return " ".join(fragments)


def _table_options_with_query(
    options: Mapping[str, str], table_name: str
) -> dict:
    """Return connector-side options with the composed ``q`` injected.

    Stripped of the typed filter keys (consumed here) and the agent-reserved
    keys; the connector sees only its own option namespace plus the merged
    ``q``.
    """
    base = dict(_connector_options(options))
    typed_keys = {name for name, _ in _FILTER_RENDERERS}
    for key in typed_keys:
        base.pop(key, None)
    if table_name in _FILTERABLE_TABLES:
        composed = _compose_query(options)
        if composed:
            base["q"] = composed
    return base


# ---------------------------------------------------------------------------
# Header extraction helpers
# ---------------------------------------------------------------------------

def _header_value(payload: Mapping[str, Any], name: str) -> Optional[str]:
    """Look up a header by case-insensitive name on a parsed payload."""
    if not payload:
        return None
    target = name.lower()
    for header in payload.get("headers", []) or []:
        if (header.get("name") or "").lower() == target:
            return header.get("value")
    return None


def _walk_parts(payload: Mapping[str, Any]):
    """Depth-first iterate every MessagePart in a parsed payload."""
    if not payload:
        return
    stack = [payload]
    while stack:
        part = stack.pop()
        yield part
        for child in part.get("parts", []) or []:
            stack.append(child)


def _decode_body_text(payload: Mapping[str, Any], mime_target: str) -> Optional[str]:
    """Decode the first body part matching ``mime_target`` to text.

    Gmail returns part bodies as base64url; we decode then UTF-8 with
    ``errors='replace'`` so caller never crashes on legacy encodings.
    """
    for part in _walk_parts(payload):
        if part.get("mimeType") != mime_target:
            continue
        body = part.get("body") or {}
        data = body.get("data")
        if not data:
            continue
        try:
            return b64url_decode(data).decode("utf-8", errors="replace")
        except Exception:  # pylint: disable=broad-except
            return None
    return None


def _attachment_parts(payload: Mapping[str, Any]):
    """Yield (filename, mimeType, attachmentId, size) for parts with attachments."""
    for part in _walk_parts(payload):
        body = part.get("body") or {}
        attachment_id = body.get("attachmentId")
        if not attachment_id:
            continue
        yield {
            "filename": part.get("filename") or "",
            "mime_type": part.get("mimeType") or "application/octet-stream",
            "attachment_id": attachment_id,
            "size_bytes": int(body.get("size") or 0),
        }


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# read_table override: apply typed filters to messages / threads
# ---------------------------------------------------------------------------

class GmailReadTableOp(ReadTableOp):
    """read_table that composes typed filters into Gmail's ``q`` syntax.

    For ``messages`` and ``threads``, the typed parameters below are joined
    via AND and become the ``q`` option the connector sends to Gmail. For
    every other table, behaviour matches the framework default. There is
    no row cap — callers chain ``.limit(N)`` on the resulting DataFrame.
    """

    description = (
        "Read a tabular object. For messages/threads, accepts typed "
        "filters (after_date, before_date, newer_than, subject, "
        "from_address, to_address, label, has_attachment, is_unread, "
        "query) that compose into Gmail search syntax."
    )
    parameters = ReadTableOp.parameters + _FILTER_PARAMETERS

    def pull(self, connector, options):
        table_name = options["tableName"]
        gmail_options = _table_options_with_query(options, table_name)
        records, _offset = connector.read_table(table_name, None, gmail_options)
        return records


# ---------------------------------------------------------------------------
# search_messages: lightweight triage view (no body, no payload)
# ---------------------------------------------------------------------------

_SEARCH_MESSAGES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("threadId", StringType(), True),
        StructField("from_address", StringType(), True),
        StructField("to_address", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("date", StringType(), True),
        StructField("snippet", StringType(), True),
        StructField("labelIds", ArrayType(StringType()), True),
        StructField("has_attachment", StringType(), True),
        StructField("size_estimate", LongType(), True),
    ]
)


class SearchMessagesOp(AgentOperation):
    """Lightweight search returning header-level info, no message bodies.

    Designed for agent triage flows where the planner wants to scan many
    messages cheaply, then call ``get_message`` only on the ones it picks.
    Uses Gmail's ``format=metadata`` so each message costs 5 quota units
    instead of 20 for ``format=full``.
    """

    name = "search_messages"
    description = (
        "Search messages and return one row per match with id, threadId, "
        "from, to, subject, date, snippet, labels, has_attachment, "
        "size_estimate. No message bodies."
    )
    kind = "data"
    schema = _SEARCH_MESSAGES_SCHEMA
    parameters = (
        Parameter(
            name="limit",
            type="integer",
            default=50,
            description="Maximum messages to return (default 50, max 500).",
        ),
    ) + _FILTER_PARAMETERS

    def pull(self, connector, options):
        api = connector.api
        user_id = connector.user_id

        limit = self._int_limit(options)
        composed_q = _compose_query(options)

        params: dict = {"maxResults": min(max(limit, 1), 500)}
        if composed_q:
            params["q"] = composed_q

        # First list to get IDs, then fetch metadata-format for headers only.
        listing = api.make_request("GET", f"/users/{user_id}/messages", params=params)
        if not listing or "messages" not in listing:
            return iter([])

        message_ids = [m["id"] for m in listing.get("messages", [])][:limit]
        endpoints = [f"/users/{user_id}/messages/{mid}" for mid in message_ids]
        meta_params = [{"format": "metadata"}] * len(message_ids)
        results = api.make_batch_request(endpoints, meta_params)

        rows = []
        for msg in results:
            if not msg:
                continue
            payload = msg.get("payload") or {}
            label_ids = msg.get("labelIds") or []
            rows.append(
                {
                    "id": msg.get("id"),
                    "threadId": msg.get("threadId"),
                    "from_address": _header_value(payload, "From"),
                    "to_address": _header_value(payload, "To"),
                    "subject": _header_value(payload, "Subject"),
                    "date": _header_value(payload, "Date"),
                    "snippet": msg.get("snippet"),
                    "labelIds": label_ids,
                    "has_attachment": "true"
                    if any(_attachment_parts(payload))
                    else "false",
                    "size_estimate": _safe_int(msg.get("sizeEstimate")),
                }
            )
        return iter(rows)

    @staticmethod
    def _int_limit(options: Mapping[str, str]) -> int:
        raw = options.get("limit")
        if raw is None or raw == "":
            return 50
        try:
            return int(raw)
        except (TypeError, ValueError) as exc:
            raise AgentError(
                ErrorCode.BAD_REQUEST,
                f"Option 'limit' must be an integer, got {raw!r}.",
            ) from exc


# ---------------------------------------------------------------------------
# get_message: fetch a single message with decoded plain + html bodies
# ---------------------------------------------------------------------------

_GET_MESSAGE_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("threadId", StringType(), True),
        StructField("from_address", StringType(), True),
        StructField("to_address", StringType(), True),
        StructField("cc_address", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("date", StringType(), True),
        StructField("snippet", StringType(), True),
        StructField("labelIds", ArrayType(StringType()), True),
        StructField("body_text", StringType(), True),
        StructField("body_html", StringType(), True),
        StructField("size_estimate", LongType(), True),
        StructField(
            "attachments",
            ArrayType(
                StructType(
                    [
                        StructField("filename", StringType(), True),
                        StructField("mime_type", StringType(), True),
                        StructField("attachment_id", StringType(), True),
                        StructField("size_bytes", LongType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("drive_file_ids", ArrayType(StringType()), True),
    ]
)


class GetMessageOp(AgentOperation):
    """Fetch one message by id with decoded plain-text and HTML bodies.

    Returns a single row. Body parts arrive base64url-encoded from Gmail;
    this op decodes them so the agent sees ready-to-read text. Drive-
    hosted attachment IDs are extracted from the HTML body and surfaced
    in ``drive_file_ids`` so the agent can chain ``download_attachment``.
    """

    name = "get_message"
    description = (
        "Fetch one message by id with decoded body_text/body_html, "
        "headers, attachments list, and any Drive file IDs referenced "
        "in the body."
    )
    kind = "data"
    schema = _GET_MESSAGE_SCHEMA
    parameters = (
        Parameter(
            name="message_id",
            required=True,
            description="Gmail message id to fetch.",
        ),
    )

    def pull(self, connector, options):
        message_id = options["message_id"]
        api = connector.api
        user_id = connector.user_id

        msg = api.make_request(
            "GET",
            f"/users/{user_id}/messages/{message_id}",
            {"format": "full"},
        )
        if not msg:
            raise AgentError(
                ErrorCode.NOT_FOUND,
                f"Message '{message_id}' not found or inaccessible.",
            )
        payload = msg.get("payload") or {}
        body_text = _decode_body_text(payload, "text/plain")
        body_html = _decode_body_text(payload, "text/html")
        drive_ids = extract_drive_file_ids(body_html or body_text or "")

        return iter(
            [
                {
                    "id": msg.get("id"),
                    "threadId": msg.get("threadId"),
                    "from_address": _header_value(payload, "From"),
                    "to_address": _header_value(payload, "To"),
                    "cc_address": _header_value(payload, "Cc"),
                    "subject": _header_value(payload, "Subject"),
                    "date": _header_value(payload, "Date"),
                    "snippet": msg.get("snippet"),
                    "labelIds": msg.get("labelIds") or [],
                    "body_text": body_text,
                    "body_html": body_html,
                    "size_estimate": _safe_int(msg.get("sizeEstimate")),
                    "attachments": list(_attachment_parts(payload)),
                    "drive_file_ids": drive_ids,
                }
            ]
        )


# ---------------------------------------------------------------------------
# list_attachments: enumerate gmail-native + Drive-hosted attachments
# ---------------------------------------------------------------------------

_LIST_ATTACHMENTS_SCHEMA = StructType(
    [
        StructField("message_id", StringType(), False),
        StructField("kind", StringType(), False),  # "gmail" or "drive"
        StructField("attachment_id", StringType(), True),
        StructField("drive_file_id", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("mime_type", StringType(), True),
        StructField("size_bytes", LongType(), True),
    ]
)


class ListAttachmentsOp(AgentOperation):
    """List attachments for a message.

    Surfaces two kinds in the same result set:
      - ``kind = "gmail"``: parts with an ``attachment_id`` from the
        message payload. Fetch via ``download_attachment`` with
        ``attachment_id``.
      - ``kind = "drive"``: file IDs extracted from Drive/Docs links in
        the message body. Fetch via ``download_attachment`` with
        ``drive_file_id``. Filename / size enriched via Drive metadata.
    """

    name = "list_attachments"
    description = (
        "List Gmail-native and Drive-hosted attachments for a message id. "
        "Returns one row per attachment with the IDs the "
        "download_attachment op needs."
    )
    kind = "data"
    schema = _LIST_ATTACHMENTS_SCHEMA
    parameters = (
        Parameter(
            name="message_id",
            required=True,
            description="Gmail message id whose attachments to enumerate.",
        ),
    )

    def pull(self, connector, options):
        message_id = options["message_id"]
        api = connector.api
        user_id = connector.user_id

        msg = api.make_request(
            "GET",
            f"/users/{user_id}/messages/{message_id}",
            {"format": "full"},
        )
        if not msg:
            raise AgentError(
                ErrorCode.NOT_FOUND,
                f"Message '{message_id}' not found or inaccessible.",
            )
        payload = msg.get("payload") or {}

        rows: list[dict] = []
        for part in _attachment_parts(payload):
            rows.append(
                {
                    "message_id": message_id,
                    "kind": "gmail",
                    "attachment_id": part["attachment_id"],
                    "drive_file_id": None,
                    "filename": part["filename"],
                    "mime_type": part["mime_type"],
                    "size_bytes": part["size_bytes"],
                }
            )

        body_html = _decode_body_text(payload, "text/html")
        body_text = _decode_body_text(payload, "text/plain")
        drive_ids = extract_drive_file_ids(body_html or body_text or "")
        for file_id in drive_ids:
            # Drive metadata lookup is best-effort; permission errors
            # surface in the row's mime_type so the agent sees them.
            try:
                meta = api.get_drive_file_metadata(file_id)
                rows.append(
                    {
                        "message_id": message_id,
                        "kind": "drive",
                        "attachment_id": None,
                        "drive_file_id": file_id,
                        "filename": meta.get("name"),
                        "mime_type": meta.get("mimeType"),
                        "size_bytes": _safe_int(meta.get("size")),
                    }
                )
            except GmailApiError as exc:
                rows.append(
                    {
                        "message_id": message_id,
                        "kind": "drive",
                        "attachment_id": None,
                        "drive_file_id": file_id,
                        "filename": None,
                        "mime_type": f"error:http_{exc.status}",
                        "size_bytes": None,
                    }
                )
        return iter(rows)


# ---------------------------------------------------------------------------
# download_attachment: write bytes to a Databricks volume path
# ---------------------------------------------------------------------------

_DOWNLOAD_ATTACHMENT_SCHEMA = StructType(
    [
        StructField("volume_path", StringType(), False),
        StructField("size_bytes", LongType(), True),
        StructField("mime_type", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("source", StringType(), False),  # "gmail" or "drive"
    ]
)


class DownloadAttachmentOp(AgentOperation):
    """Download an attachment to a Databricks Volume path.

    Two input shapes; exactly one ID must be supplied:
      - ``attachment_id`` + ``message_id`` → Gmail's
        ``users.messages.attachments.get``. Decoded bytes are written
        to ``volume_path``.
      - ``drive_file_id`` → Drive ``files.get?alt=media`` (binary) or
        ``files.export`` (Google-native types: Docs/Sheets/Slides).
        ``export_mime_type`` overrides the default ``application/pdf``
        export target.

    ``volume_path`` must be a writable path the executor can ``open()``
    in write-binary mode. The canonical shape is
    ``/Volumes/<catalog>/<schema>/<volume>/<filename>`` — the op
    creates the parent directory if needed but does not create the
    volume itself. The path is required to start with ``/Volumes/`` so
    the op can't accidentally write outside Unity Catalog (override
    intentionally via the framework's option dict only).
    """

    name = "download_attachment"
    description = (
        "Download a Gmail or Drive-hosted attachment to a Databricks "
        "Volume path. Supply attachment_id+message_id for Gmail or "
        "drive_file_id for Drive."
    )
    kind = "metadata"
    schema = _DOWNLOAD_ATTACHMENT_SCHEMA
    parameters = (
        Parameter(
            name="volume_path",
            required=True,
            description=(
                "Destination path under /Volumes/<catalog>/<schema>/<volume>/. "
                "The parent directory is created if missing."
            ),
        ),
        Parameter(
            name="message_id",
            description=(
                "Gmail message id. Required when attachment_id is supplied."
            ),
        ),
        Parameter(
            name="attachment_id",
            description=(
                "Gmail attachment id (from list_attachments where kind=gmail)."
            ),
        ),
        Parameter(
            name="drive_file_id",
            description=(
                "Drive file id (from list_attachments where kind=drive)."
            ),
        ),
        Parameter(
            name="export_mime_type",
            description=(
                "For Google-native Drive files, the export MIME type "
                "(default 'application/pdf'). Ignored for binary files."
            ),
        ),
    )

    _VOLUME_PREFIX = "/Volumes/"

    def pull(self, connector, options):
        volume_path = options["volume_path"]
        if not volume_path.startswith(self._VOLUME_PREFIX):
            raise AgentError(
                ErrorCode.BAD_REQUEST,
                f"volume_path must start with '{self._VOLUME_PREFIX}'; "
                f"got {volume_path!r}.",
            )

        attachment_id = options.get("attachment_id") or None
        message_id = options.get("message_id") or None
        drive_file_id = options.get("drive_file_id") or None

        if attachment_id and drive_file_id:
            raise AgentError(
                ErrorCode.BAD_REQUEST,
                "Supply attachment_id OR drive_file_id, not both.",
            )
        if not attachment_id and not drive_file_id:
            raise AgentError(
                ErrorCode.BAD_REQUEST,
                "Either attachment_id (with message_id) or drive_file_id "
                "is required.",
            )
        if attachment_id and not message_id:
            raise AgentError(
                ErrorCode.BAD_REQUEST,
                "message_id is required when attachment_id is supplied.",
            )

        parent = os.path.dirname(volume_path)
        if parent:
            try:
                os.makedirs(parent, exist_ok=True)
            except OSError as exc:
                raise AgentError(
                    ErrorCode.PERMISSION_DENIED,
                    f"Cannot create parent directory '{parent}': {exc}",
                ) from exc

        if attachment_id:
            return iter([self._download_gmail(connector, message_id, attachment_id, volume_path)])
        return iter([self._download_drive(connector, drive_file_id, volume_path, options)])

    @staticmethod
    def _download_gmail(connector, message_id, attachment_id, volume_path):
        try:
            data = connector.api.get_attachment(message_id, attachment_id)
        except GmailApiError as exc:
            raise _agent_error_from_status(exc) from exc

        # Look up filename + mime from the parent message payload so the
        # row carries it. One extra API call (format=metadata is cheap at
        # 5 quota units) — worth it for downstream UX.
        filename = None
        mime_type = None
        meta = connector.api.make_request(
            "GET",
            f"/users/{connector.user_id}/messages/{message_id}",
            {"format": "full"},
        )
        if meta:
            for part in _attachment_parts(meta.get("payload") or {}):
                if part["attachment_id"] == attachment_id:
                    filename = part["filename"] or None
                    mime_type = part["mime_type"]
                    break

        with open(volume_path, "wb") as fh:
            fh.write(data)

        return {
            "volume_path": volume_path,
            "size_bytes": len(data),
            "mime_type": mime_type,
            "filename": filename,
            "source": "gmail",
        }

    @staticmethod
    def _download_drive(connector, drive_file_id, volume_path, options):
        try:
            size, name, mime = connector.api.download_drive_file(
                drive_file_id,
                volume_path,
                export_mime_type=options.get("export_mime_type") or None,
            )
        except GmailApiError as exc:
            raise _agent_error_from_status(exc) from exc

        return {
            "volume_path": volume_path,
            "size_bytes": size,
            "mime_type": mime,
            "filename": name,
            "source": "drive",
        }


def _agent_error_from_status(exc: GmailApiError) -> AgentError:
    """Map HTTP status to a canonical agent ErrorCode."""
    if exc.status == 401:
        return AgentError(
            ErrorCode.AUTH_FAILED,
            f"Authentication failed: {exc}",
        )
    if exc.status == 403:
        return AgentError(
            ErrorCode.PERMISSION_DENIED,
            f"Permission denied (does the OAuth token include drive.readonly?): {exc}",
        )
    if exc.status == 404:
        return AgentError(ErrorCode.NOT_FOUND, str(exc))
    if exc.status == 429:
        return AgentError(ErrorCode.RATE_LIMITED, str(exc))
    return AgentError(ErrorCode.INTERNAL_ERROR, str(exc))


# ---------------------------------------------------------------------------
# Entry point — what GmailLakeflowConnect.agent_operations returns
# ---------------------------------------------------------------------------

def build_gmail_agent_operations() -> dict:
    """Return the agent-operation map for the Gmail connector.

    Wired in via :meth:`GmailLakeflowConnect.agent_operations`. The
    ``read_table`` entry overrides the framework built-in.
    """
    ops = [
        GmailReadTableOp(),
        SearchMessagesOp(),
        GetMessageOp(),
        ListAttachmentsOp(),
        DownloadAttachmentOp(),
    ]
    return {op.name: op for op in ops}
