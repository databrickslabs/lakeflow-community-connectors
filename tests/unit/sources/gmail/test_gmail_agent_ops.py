"""Unit tests for the Gmail ingestion-agent operations.

Covers the Gmail-specific operations dispatched through
``IngestionAgentDispatcher``:

  - read_table (override): typed filters compose into Gmail q syntax
  - search_messages: triage view, metadata-format messages
  - search_threads: thread-level triage, one row per conversation
  - get_message: single-message fetch with decoded bodies + drive IDs
  - get_thread: full conversation with decoded message bodies
  - list_attachments: gmail-native + Drive-hosted enumeration
  - download_attachment: Volume-path writes for both kinds
  - mailbox_overview: per-label message/thread counts

Mocks ``GmailApiClient`` so tests stay offline.
"""

from __future__ import annotations

import base64
import os

import pytest

from databricks.labs.community_connector.interface import AgentError, ErrorCode
from databricks.labs.community_connector.sources.gmail.gmail import GmailLakeflowConnect
from databricks.labs.community_connector.sources.gmail.gmail_agent_ops import (
    _compose_query,
    extract_drive_file_ids,
)
from databricks.labs.community_connector.sources.gmail.gmail_utils import GmailApiError
from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import (
    IngestionAgentDispatcher,
    OP_LIST_OPERATIONS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _b64u(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii").rstrip("=")


@pytest.fixture
def connector(monkeypatch):
    """Gmail connector with the make_request entrypoint stubbed offline."""
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, method, path, **kwargs: {"historyId": "1"},
    )
    return GmailLakeflowConnect(
        {
            "access_token": "x",
        }
    )


def _dispatch(operation: str, connector, **options):
    opts = {"operation": operation, **{k: str(v) for k, v in options.items()}}
    dispatcher = IngestionAgentDispatcher(options=opts, connector=connector)
    schema = dispatcher.schema()
    reader = dispatcher.reader(schema)
    return list(reader.read(next(iter(reader.partitions()))))


# ---------------------------------------------------------------------------
# Query composition
# ---------------------------------------------------------------------------

def test_compose_query_combines_typed_filters_with_and():
    q = _compose_query(
        {
            "after_date": "2024/01/15",
            "before_date": "2024/02/01",
            "from_address": "alice@example.com",
            "has_attachment": "true",
        }
    )
    assert "after:2024/01/15" in q
    assert "before:2024/02/01" in q
    assert "from:alice@example.com" in q
    assert "has:attachment" in q


def test_compose_query_returns_none_when_no_filters():
    assert _compose_query({}) is None
    assert _compose_query({"unrelated": "x"}) is None


def test_compose_query_quotes_multi_word_subject():
    q = _compose_query({"subject": "quarterly report"})
    assert q == "subject:(quarterly report)"


def test_compose_query_passes_through_query_param_and_existing_q():
    q = _compose_query({"query": "AROUND 5 holiday", "q": "label:work"})
    # Order: typed filters first, then raw q
    assert q == "AROUND 5 holiday label:work"


def test_compose_query_skips_false_boolean_flags():
    q = _compose_query({"has_attachment": "false", "is_unread": "false"})
    assert q is None


def test_extract_drive_file_ids_finds_common_url_shapes():
    body = """
    Check this: https://drive.google.com/file/d/1AbCdEf_GhIjKlMnOpQrStUvWxYz12345/view
    And this Doc: https://docs.google.com/document/d/0BzABCDEFghijKLMNopqrstu1234567890XY/edit
    Same as above: https://drive.google.com/open?id=1AbCdEf_GhIjKlMnOpQrStUvWxYz12345
    """
    ids = extract_drive_file_ids(body)
    assert ids == [
        "1AbCdEf_GhIjKlMnOpQrStUvWxYz12345",
        "0BzABCDEFghijKLMNopqrstu1234567890XY",
    ]


# ---------------------------------------------------------------------------
# list_operations exposes the gmail-specific catalog
# ---------------------------------------------------------------------------

def test_list_operations_includes_gmail_specific_ops(connector):
    rows = _dispatch(OP_LIST_OPERATIONS, connector)
    names = {r["name"] for r in rows}
    assert {
        "read_table",
        "search_messages",
        "get_message",
        "list_attachments",
        "download_attachment",
    }.issubset(names)


def test_read_table_description_mentions_gmail_filters(connector):
    rows = _dispatch(OP_LIST_OPERATIONS, connector)
    op = next(r for r in rows if r["name"] == "read_table")
    assert "Gmail search syntax" in op["description"]
    # Typed filter params show up in parameters_json
    assert "after_date" in op["parameters_json"]
    assert "from_address" in op["parameters_json"]


# ---------------------------------------------------------------------------
# read_table override: routes typed filters into the connector q option
# ---------------------------------------------------------------------------

def test_read_table_messages_routes_typed_filters_into_q(connector, monkeypatch):
    captured: dict = {}

    def fake_read_table(table_name, start_offset, table_options):
        captured["table_name"] = table_name
        captured["table_options"] = dict(table_options)
        return iter([]), {}

    monkeypatch.setattr(connector, "read_table", fake_read_table)
    rows = _dispatch(
        "read_table",
        connector,
        tableName="messages",
        after_date="2024/01/15",
        subject="invoice",
        has_attachment="true",
    )
    assert rows == []
    assert captured["table_name"] == "messages"
    q = captured["table_options"].get("q")
    assert q is not None
    assert "after:2024/01/15" in q
    assert "subject:invoice" in q
    assert "has:attachment" in q
    # Typed filter keys should not leak through to the connector.
    # ``tableName`` is allowed through by the framework's ``connector_options``;
    # only operation-layer reserved keys (operation/search/path/etc.) are stripped.
    for stripped in (
        "after_date",
        "subject",
        "has_attachment",
        "operation",
    ):
        assert stripped not in captured["table_options"]


def test_read_table_non_filterable_table_ignores_filters(connector, monkeypatch):
    captured: dict = {}

    def fake_read_table(table_name, start_offset, table_options):
        captured["q"] = table_options.get("q")
        return iter([{"id": "a"}, {"id": "b"}]), {}

    monkeypatch.setattr(connector, "read_table", fake_read_table)
    rows = _dispatch(
        "read_table",
        connector,
        tableName="labels",
        after_date="2024/01/15",  # ignored — labels isn't a filterable table
    )
    assert len(rows) == 2
    assert captured["q"] is None


def test_read_table_returns_all_rows_no_framework_cap(connector, monkeypatch):
    # The Scala-aligned ReadTableOp dropped framework-side row capping —
    # callers chain ``.limit(N)`` on the resulting DataFrame instead. The
    # override must surface every row the source produces.
    monkeypatch.setattr(
        connector,
        "read_table",
        lambda t, s, o: (iter([{"id": str(i)} for i in range(20)]), {}),
    )
    rows = _dispatch("read_table", connector, tableName="messages")
    assert len(rows) == 20


# ---------------------------------------------------------------------------
# search_messages: list + metadata-format batch fetch → typed rows
# ---------------------------------------------------------------------------

def _make_message_payload(
    *,
    msg_id: str,
    thread_id: str,
    subject: str,
    sender: str,
    attachments: int = 0,
):
    parts = []
    for i in range(attachments):
        parts.append(
            {
                "partId": f"a{i}",
                "mimeType": "application/pdf",
                "filename": f"file{i}.pdf",
                "body": {"attachmentId": f"att{i}", "size": 1024},
            }
        )
    return {
        "id": msg_id,
        "threadId": thread_id,
        "labelIds": ["INBOX"],
        "snippet": f"snippet for {msg_id}",
        "sizeEstimate": 4096,
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": [
                {"name": "From", "value": sender},
                {"name": "To", "value": "me@example.com"},
                {"name": "Subject", "value": subject},
                {"name": "Date", "value": "Wed, 1 Jan 2024 00:00:00 +0000"},
            ],
            "parts": parts,
        },
    }


def test_search_messages_typed_filters_drive_listing_q(connector, monkeypatch):
    requests_log: list = []

    def fake_make_request(self, method, path, params=None, **_):
        requests_log.append((method, path, dict(params or {})))
        if path.endswith("/messages"):
            return {"messages": [{"id": "m1"}, {"id": "m2"}]}
        return None

    def fake_batch(self, endpoints, params_list=None):
        return [
            _make_message_payload(
                msg_id="m1",
                thread_id="t1",
                subject="hi",
                sender="alice@example.com",
                attachments=1,
            ),
            _make_message_payload(
                msg_id="m2",
                thread_id="t2",
                subject="hello",
                sender="bob@example.com",
            ),
        ]

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        fake_make_request,
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_batch_request",
        fake_batch,
    )

    rows = _dispatch(
        "search_messages",
        connector,
        from_address="alice@example.com",
        newer_than="7d",
        limit=10,
    )
    # 2 rows, header fields decoded, has_attachment toggled per payload.
    assert len(rows) == 2
    by_id = {r["id"]: r for r in rows}
    assert by_id["m1"]["from_address"] == "alice@example.com"
    assert by_id["m1"]["subject"] == "hi"
    assert by_id["m1"]["has_attachment"] == "true"
    assert by_id["m2"]["has_attachment"] == "false"
    assert by_id["m1"]["size_estimate"] == 4096

    # The list call must have received a composed q.
    listing = next(call for call in requests_log if call[1].endswith("/messages"))
    q = listing[2].get("q")
    assert "from:alice@example.com" in q
    assert "newer_than:7d" in q


def test_search_messages_returns_empty_when_no_matches(connector, monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, *a, **kw: {"resultSizeEstimate": 0},
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_batch_request",
        lambda self, *a, **kw: [],
    )
    rows = _dispatch("search_messages", connector, query="from:nobody")
    assert rows == []


# ---------------------------------------------------------------------------
# get_message: decodes plain + html and surfaces drive IDs
# ---------------------------------------------------------------------------

def test_get_message_decodes_bodies_and_extracts_drive_links(connector, monkeypatch):
    html = (
        '<a href="https://drive.google.com/file/d/'
        '1AbCdEf_GhIjKlMnOpQrStUvWxYz12345/view">file</a>'
    )
    text = "plain text fallback"

    payload = {
        "id": "m1",
        "threadId": "t1",
        "labelIds": ["INBOX"],
        "snippet": "preview",
        "sizeEstimate": 2048,
        "payload": {
            "mimeType": "multipart/alternative",
            "headers": [
                {"name": "From", "value": "alice@example.com"},
                {"name": "To", "value": "bob@example.com"},
                {"name": "Subject", "value": "subj"},
                {"name": "Date", "value": "now"},
            ],
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": _b64u(text)},
                },
                {
                    "mimeType": "text/html",
                    "body": {"data": _b64u(html)},
                },
                {
                    "mimeType": "application/pdf",
                    "filename": "doc.pdf",
                    "body": {"attachmentId": "att1", "size": 1234},
                },
            ],
        },
    }

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, method, path, params=None, **_: payload
        if path.endswith("/messages/m1")
        else {"historyId": "1"},
    )

    rows = _dispatch("get_message", connector, message_id="m1")
    assert len(rows) == 1
    row = rows[0]
    assert row["body_text"] == text
    assert "drive.google.com" in row["body_html"]
    assert row["drive_file_ids"] == ["1AbCdEf_GhIjKlMnOpQrStUvWxYz12345"]
    assert len(row["attachments"]) == 1
    assert row["attachments"][0]["filename"] == "doc.pdf"


def test_get_message_missing_id_raises_bad_request(connector):
    with pytest.raises(AgentError) as info:
        _dispatch("get_message", connector)  # no message_id
    assert info.value.code == ErrorCode.BAD_REQUEST
    assert "message_id" in str(info.value)


def test_get_message_not_found_yields_not_found_error(connector, monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, method, path, params=None, **_: None,
    )
    with pytest.raises(AgentError) as info:
        _dispatch("get_message", connector, message_id="ghost")
    assert info.value.code == ErrorCode.NOT_FOUND


# ---------------------------------------------------------------------------
# list_attachments: gmail-native + Drive metadata enrichment
# ---------------------------------------------------------------------------

def test_list_attachments_mixes_gmail_and_drive_rows(connector, monkeypatch):
    payload = {
        "id": "m1",
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": [],
            "parts": [
                {
                    "mimeType": "text/html",
                    "body": {
                        "data": _b64u(
                            'see <a href="https://drive.google.com/file/d/'
                            "1AbCdEf_GhIjKlMnOpQrStUvWxYz12345/view"
                            '">doc</a>'
                        )
                    },
                },
                {
                    "mimeType": "application/pdf",
                    "filename": "report.pdf",
                    "body": {"attachmentId": "gmail_att_1", "size": 2048},
                },
            ],
        },
    }

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, method, path, params=None, **_: payload,
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.get_drive_file_metadata",
        lambda self, file_id: {
            "id": file_id,
            "name": "shared.pdf",
            "mimeType": "application/pdf",
            "size": "5120",
        },
    )

    rows = _dispatch("list_attachments", connector, message_id="m1")
    by_kind = {r["kind"]: r for r in rows}
    assert by_kind["gmail"]["filename"] == "report.pdf"
    assert by_kind["gmail"]["attachment_id"] == "gmail_att_1"
    assert by_kind["drive"]["drive_file_id"] == "1AbCdEf_GhIjKlMnOpQrStUvWxYz12345"
    assert by_kind["drive"]["filename"] == "shared.pdf"
    assert by_kind["drive"]["size_bytes"] == 5120


def test_list_attachments_drive_403_surfaces_error_marker(connector, monkeypatch):
    payload = {
        "id": "m1",
        "payload": {
            "headers": [],
            "parts": [
                {
                    "mimeType": "text/html",
                    "body": {
                        "data": _b64u(
                            "https://drive.google.com/file/d/"
                            "1AbCdEf_GhIjKlMnOpQrStUvWxYz12345/view"
                        )
                    },
                }
            ],
        },
    }
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, method, path, params=None, **_: payload,
    )

    def _raise_403(self, file_id):
        raise GmailApiError(403, "not shared with user")

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.get_drive_file_metadata",
        _raise_403,
    )
    rows = _dispatch("list_attachments", connector, message_id="m1")
    drive_rows = [r for r in rows if r["kind"] == "drive"]
    assert len(drive_rows) == 1
    assert drive_rows[0]["mime_type"] == "error:http_403"
    assert drive_rows[0]["filename"] is None


# ---------------------------------------------------------------------------
# download_attachment: validates inputs, writes to volume path
# ---------------------------------------------------------------------------

def test_download_attachment_rejects_non_volume_path(connector):
    rows = _dispatch(
        "download_attachment",
        connector,
        volume_path="/tmp/x",
        attachment_id="a",
        message_id="m1",
    )
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert rows[0]["_meta"]["code"] == ErrorCode.BAD_REQUEST
    assert "Volumes" in rows[0]["_meta"]["message"]


def test_download_attachment_requires_one_id(connector, tmp_path, monkeypatch):
    # Both supplied → bad_request.
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_agent_ops."
        "DownloadAttachmentOp._VOLUME_PREFIX",
        str(tmp_path) + "/",
    )
    target = tmp_path / "f.bin"
    rows = _dispatch(
        "download_attachment",
        connector,
        volume_path=str(target),
        attachment_id="a",
        drive_file_id="d",
        message_id="m1",
    )
    assert rows[0]["_meta"]["code"] == ErrorCode.BAD_REQUEST


def test_download_attachment_gmail_writes_bytes(connector, tmp_path, monkeypatch):
    payload = {
        "payload": {
            "headers": [],
            "parts": [
                {
                    "mimeType": "application/pdf",
                    "filename": "x.pdf",
                    "body": {"attachmentId": "att1", "size": 11},
                }
            ],
        }
    }
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, method, path, params=None, **_: payload,
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.get_attachment",
        lambda self, mid, aid: b"hello world",
    )
    # Allow the test to use a tmp path instead of /Volumes/.
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_agent_ops."
        "DownloadAttachmentOp._VOLUME_PREFIX",
        str(tmp_path) + "/",
    )
    target = tmp_path / "subdir" / "x.pdf"
    rows = _dispatch(
        "download_attachment",
        connector,
        volume_path=str(target),
        attachment_id="att1",
        message_id="m1",
    )
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "ok"
    assert rows[0]["size_bytes"] == 11
    assert rows[0]["filename"] == "x.pdf"
    assert rows[0]["mime_type"] == "application/pdf"
    assert rows[0]["source"] == "gmail"
    assert os.path.exists(target)
    assert target.read_bytes() == b"hello world"


def test_download_attachment_drive_streams_and_returns_metadata(
    connector, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.download_drive_file",
        lambda self, fid, dest, export_mime_type=None, chunk_size=None: (
            42,
            "shared.pdf",
            "application/pdf",
        ),
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_agent_ops."
        "DownloadAttachmentOp._VOLUME_PREFIX",
        str(tmp_path) + "/",
    )
    target = tmp_path / "shared.pdf"
    rows = _dispatch(
        "download_attachment",
        connector,
        volume_path=str(target),
        drive_file_id="1AbCdEf_GhIjKlMnOpQrStUvWxYz12345",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["source"] == "drive"
    assert row["size_bytes"] == 42
    assert row["filename"] == "shared.pdf"
    assert row["mime_type"] == "application/pdf"


def test_download_attachment_drive_403_yields_permission_denied(
    connector, tmp_path, monkeypatch
):
    def _raise_403(self, fid, dest, export_mime_type=None, chunk_size=None):
        raise GmailApiError(403, "not shared")

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.download_drive_file",
        _raise_403,
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_agent_ops."
        "DownloadAttachmentOp._VOLUME_PREFIX",
        str(tmp_path) + "/",
    )
    target = tmp_path / "shared.pdf"
    rows = _dispatch(
        "download_attachment",
        connector,
        volume_path=str(target),
        drive_file_id="1AbCdEf_GhIjKlMnOpQrStUvWxYz12345",
    )
    assert rows[0]["_meta"]["code"] == ErrorCode.PERMISSION_DENIED


# ---------------------------------------------------------------------------
# read_table default row cap
# ---------------------------------------------------------------------------

def _install_paginated_messages_api(monkeypatch, total_available: int):
    """Stub the Gmail API to act like a mailbox with ``total_available`` messages
    served via pagination. Tracks how many list+detail calls were issued."""
    counters = {"list_calls": 0, "detail_calls": 0}

    def fake_make_request(self, method, path, params=None, **kwargs):
        if path.endswith("/profile"):
            return {"historyId": "1"}
        if path.endswith("/messages") and (params or {}).get("pageToken") is None:
            counters["list_calls"] += 1
            page_size = int((params or {}).get("maxResults", 100))
            start = 0
            end = min(page_size, total_available)
            msgs = [{"id": f"m{i}"} for i in range(start, end)]
            next_token = "T1" if end < total_available else None
            return {"messages": msgs, "nextPageToken": next_token} if next_token else {"messages": msgs}
        if path.endswith("/messages") and (params or {}).get("pageToken"):
            counters["list_calls"] += 1
            token = params["pageToken"]
            prev_end = int(token[1:]) * int((params or {}).get("maxResults", 100))
            page_size = int((params or {}).get("maxResults", 100))
            start = prev_end
            end = min(prev_end + page_size, total_available)
            msgs = [{"id": f"m{i}"} for i in range(start, end)]
            next_idx = int(token[1:]) + 1
            next_token = f"T{next_idx}" if end < total_available else None
            return {"messages": msgs, "nextPageToken": next_token} if next_token else {"messages": msgs}
        if "/messages/" in path:
            counters["detail_calls"] += 1
            mid = path.rsplit("/", 1)[-1]
            return {"id": mid, "historyId": "1"}
        return {}

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        fake_make_request,
    )
    return counters


def test_read_table_default_cap_is_50(monkeypatch):
    """No maxResults → agent read_table returns 50 messages by default."""
    counters = _install_paginated_messages_api(monkeypatch, total_available=200)
    connector = GmailLakeflowConnect(
        {"client_id": "x", "client_secret": "x", "refresh_token": "x"}
    )
    rows = _dispatch("read_table", connector, tableName="messages")
    assert len(rows) == 50, f"expected 50, got {len(rows)}"
    # Pagination must stop early; never fetch all 200 details.
    assert counters["detail_calls"] == 50


def test_read_table_max_results_minus_one_returns_all(monkeypatch):
    counters = _install_paginated_messages_api(monkeypatch, total_available=120)
    connector = GmailLakeflowConnect(
        {"client_id": "x", "client_secret": "x", "refresh_token": "x"}
    )
    rows = _dispatch("read_table", connector, tableName="messages", maxResults="-1")
    assert len(rows) == 120
    assert counters["detail_calls"] == 120


def test_read_table_respects_explicit_max_results(monkeypatch):
    counters = _install_paginated_messages_api(monkeypatch, total_available=500)
    connector = GmailLakeflowConnect(
        {"client_id": "x", "client_secret": "x", "refresh_token": "x"}
    )
    rows = _dispatch("read_table", connector, tableName="messages", maxResults="10")
    assert len(rows) == 10
    assert counters["detail_calls"] == 10


# ---------------------------------------------------------------------------
# get_thread: full conversation with decoded message bodies
# ---------------------------------------------------------------------------

def _thread_msg(msg_id, sender, subject, body_text, date, to="me@example.com", labels=None):
    return {
        "id": msg_id,
        "threadId": "t1",
        "labelIds": labels if labels is not None else ["INBOX"],
        "snippet": f"snip {msg_id}",
        "sizeEstimate": 100,
        "payload": {
            "mimeType": "multipart/alternative",
            "headers": [
                {"name": "From", "value": sender},
                {"name": "To", "value": to},
                {"name": "Subject", "value": subject},
                {"name": "Date", "value": date},
            ],
            "parts": [{"mimeType": "text/plain", "body": {"data": _b64u(body_text)}}],
        },
    }


def test_get_thread_returns_one_row_with_decoded_messages(connector, monkeypatch):
    thread = {
        "id": "t1",
        "historyId": "555",
        "messages": [
            _thread_msg("m1", "alice@example.com", "Kickoff", "first body",
                        "Wed, 1 Jan 2024 00:00:00 +0000"),
            _thread_msg("m2", "bob@example.com", "Re: Kickoff", "second body",
                        "Thu, 2 Jan 2024 00:00:00 +0000", labels=["INBOX", "IMPORTANT"]),
        ],
    }

    def fake_make_request(self, method, path, params=None, **_):
        assert path.endswith("/threads/t1")
        assert (params or {}).get("format") == "full"
        return thread

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        fake_make_request,
    )

    rows = _dispatch("get_thread", connector, thread_id="t1")
    assert len(rows) == 1
    row = rows[0]
    assert row["thread_id"] == "t1"
    assert row["history_id"] == "555"
    assert row["subject"] == "Kickoff"          # first message's subject
    assert row["message_count"] == 2
    assert set(row["participants"]) == {
        "alice@example.com", "bob@example.com", "me@example.com",
    }
    assert set(row["labelIds"]) == {"INBOX", "IMPORTANT"}
    assert [m["id"] for m in row["messages"]] == ["m1", "m2"]
    assert row["messages"][0]["body_text"] == "first body"
    assert row["messages"][1]["from_address"] == "bob@example.com"


def test_get_thread_not_found_raises(connector, monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, *a, **kw: None,
    )
    with pytest.raises(AgentError) as info:
        _dispatch("get_thread", connector, thread_id="missing")
    assert info.value.code == ErrorCode.NOT_FOUND


# ---------------------------------------------------------------------------
# search_threads: one row per conversation, typed filters compose into q
# ---------------------------------------------------------------------------

def test_search_threads_returns_one_row_per_thread(connector, monkeypatch):
    requests_log: list = []

    def fake_make_request(self, method, path, params=None, **_):
        requests_log.append((method, path, dict(params or {})))
        if path.endswith("/threads"):
            return {"threads": [
                {"id": "t1", "snippet": "snippet one"},
                {"id": "t2", "snippet": "snippet two"},
            ]}
        return None

    def fake_batch(self, endpoints, params_list=None):
        return [
            {"id": "t1", "historyId": "10", "messages": [
                _thread_msg("m1", "alice@example.com", "Kickoff", "b1",
                            "Wed, 1 Jan 2024 00:00:00 +0000"),
                _thread_msg("m2", "bob@example.com", "Re: Kickoff", "b2",
                            "Thu, 2 Jan 2024 00:00:00 +0000"),
            ]},
            {"id": "t2", "historyId": "11", "messages": [
                _thread_msg("m3", "carol@example.com", "Lunch?", "b3",
                            "Fri, 3 Jan 2024 00:00:00 +0000"),
            ]},
        ]

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        fake_make_request,
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_batch_request",
        fake_batch,
    )

    rows = _dispatch("search_threads", connector, from_address="alice@example.com",
                     newer_than="30d", limit=10)
    assert len(rows) == 2
    by_id = {r["id"]: r for r in rows}
    assert by_id["t1"]["subject"] == "Kickoff"
    assert by_id["t1"]["snippet"] == "snippet one"
    assert by_id["t1"]["message_count"] == 2
    assert by_id["t1"]["last_date"] == "Thu, 2 Jan 2024 00:00:00 +0000"
    assert "alice@example.com" in by_id["t1"]["participants"]
    assert by_id["t2"]["message_count"] == 1

    listing = next(c for c in requests_log if c[1].endswith("/threads"))
    q = listing[2].get("q")
    assert "from:alice@example.com" in q
    assert "newer_than:30d" in q


def test_search_threads_empty_when_no_matches(connector, monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        lambda self, *a, **kw: {"resultSizeEstimate": 0},
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_batch_request",
        lambda self, *a, **kw: [],
    )
    rows = _dispatch("search_threads", connector, query="from:nobody")
    assert rows == []


# ---------------------------------------------------------------------------
# mailbox_overview: per-label counts
# ---------------------------------------------------------------------------

def test_mailbox_overview_returns_per_label_counts(connector, monkeypatch):
    def fake_make_request(self, method, path, params=None, **_):
        if path.endswith("/labels"):
            return {"labels": [{"id": "INBOX"}, {"id": "Label_1"}]}
        return None

    def fake_batch(self, endpoints, params_list=None):
        return [
            {"id": "INBOX", "name": "INBOX", "type": "system",
             "messagesTotal": 120, "messagesUnread": 4,
             "threadsTotal": 100, "threadsUnread": 3},
            {"id": "Label_1", "name": "Work", "type": "user",
             "messagesTotal": 30, "messagesUnread": 0,
             "threadsTotal": 25, "threadsUnread": 0},
        ]

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        fake_make_request,
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_batch_request",
        fake_batch,
    )

    rows = _dispatch("mailbox_overview", connector)
    assert len(rows) == 2
    by_id = {r["label_id"]: r for r in rows}
    assert by_id["INBOX"]["messages_total"] == 120
    assert by_id["INBOX"]["messages_unread"] == 4
    assert by_id["Label_1"]["name"] == "Work"
    assert by_id["Label_1"]["type"] == "user"
    assert by_id["Label_1"]["threads_total"] == 25


def test_search_messages_has_attachment_under_metadata_format(connector, monkeypatch):
    """Regression: format=metadata omits body.attachmentId, so has_attachment
    must be derived from the part filename, not attachmentId."""

    def fake_make_request(self, method, path, params=None, **_):
        if path.endswith("/messages"):
            return {"messages": [{"id": "m1"}, {"id": "m2"}]}
        return None

    def fake_batch(self, endpoints, params_list=None):
        # m1: attachment part has a filename but NO attachmentId (metadata shape)
        with_attach = {
            "id": "m1",
            "threadId": "t1",
            "labelIds": ["INBOX"],
            "snippet": "has a pdf",
            "sizeEstimate": 5000,
            "payload": {
                "mimeType": "multipart/mixed",
                "headers": [{"name": "Subject", "value": "report"}],
                "parts": [
                    {"mimeType": "text/plain", "body": {"size": 10}},
                    {
                        "mimeType": "application/pdf",
                        "filename": "report.pdf",
                        "body": {"size": 1024},  # note: no attachmentId
                    },
                ],
            },
        }
        # m2: no attachment parts at all
        without_attach = {
            "id": "m2",
            "threadId": "t2",
            "labelIds": ["INBOX"],
            "snippet": "plain note",
            "sizeEstimate": 200,
            "payload": {
                "mimeType": "text/plain",
                "headers": [{"name": "Subject", "value": "note"}],
                "body": {"size": 20},
            },
        }
        return [with_attach, without_attach]

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_request",
        fake_make_request,
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.gmail.gmail_utils."
        "GmailApiClient.make_batch_request",
        fake_batch,
    )

    rows = _dispatch("search_messages", connector, query="report")
    by_id = {r["id"]: r for r in rows}
    assert by_id["m1"]["has_attachment"] == "true"   # filename present, no attachmentId
    assert by_id["m2"]["has_attachment"] == "false"
