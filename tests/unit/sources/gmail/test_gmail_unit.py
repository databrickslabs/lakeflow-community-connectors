import pytest
from pyspark.sql.types import IntegerType

from databricks.labs.community_connector.sources.gmail.gmail import GmailLakeflowConnect
from databricks.labs.community_connector.sources.gmail.gmail_utils import GmailApiClient


class TestGmailConnectorUnit:
    """Unit tests that validate connector structure without API calls."""

    @pytest.fixture
    def connector(self, monkeypatch):
        """Create connector with dummy credentials for structure tests.

        ``__init__`` snapshots the mailbox ``historyId`` via the profile
        endpoint (admission-control cap for AvailableNow termination), so
        constructing the connector requires either real OAuth credentials
        or a mocked HTTP layer.  Patch ``make_request`` so structural
        tests stay offline.
        """
        monkeypatch.setattr(
            "databricks.labs.community_connector.sources.gmail.gmail_utils."
            "GmailApiClient.make_request",
            lambda self, method, path, **kwargs: {"historyId": "1"},
        )
        return GmailLakeflowConnect(
            {
                "client_id": "dummy_client_id",
                "client_secret": "dummy_client_secret",
                "refresh_token": "dummy_refresh_token",
            }
        )

    def test_unit_initialization_with_valid_options(self, connector):
        """Test connector initializes with all required options."""
        assert connector.client_id == "dummy_client_id"
        assert connector.client_secret == "dummy_client_secret"
        assert connector.refresh_token == "dummy_refresh_token"
        assert connector.user_id == "me"  # default

    def test_unit_initialization_missing_client_id(self):
        """Test initialization fails without client_id."""
        with pytest.raises(ValueError, match="client_id"):
            GmailLakeflowConnect(
                {
                    "client_secret": "secret",
                    "refresh_token": "token",
                }
            )

    def test_unit_initialization_missing_client_secret(self):
        """Test initialization fails without client_secret."""
        with pytest.raises(ValueError, match="client_secret"):
            GmailLakeflowConnect(
                {
                    "client_id": "id",
                    "refresh_token": "token",
                }
            )

    def test_unit_initialization_missing_refresh_token(self):
        """Test initialization fails without refresh_token."""
        with pytest.raises(ValueError, match="refresh_token"):
            GmailLakeflowConnect(
                {
                    "client_id": "id",
                    "client_secret": "secret",
                }
            )

    def test_unit_schemas_use_long_type_not_integer(self, connector):
        """Test all numeric fields use LongType instead of IntegerType."""
        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            for field in schema.fields:
                assert not isinstance(field.dataType, IntegerType), (
                    f"Table '{table_name}' field '{field.name}' uses IntegerType. "
                    "Use LongType instead per best practices."
                )

    def test_unit_invalid_table_raises_error(self, connector):
        """Test invalid table name raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported table"):
            connector.get_table_schema("invalid_table", {})

    def test_unit_messages_schema_has_required_fields(self, connector):
        """Test messages schema has essential fields."""
        schema = connector.get_table_schema("messages", {})
        field_names = [f.name for f in schema.fields]

        required_fields = ["id", "threadId", "labelIds", "snippet"]
        for field in required_fields:
            assert field in field_names, f"messages schema missing '{field}'"

    def test_unit_profile_schema_has_email(self, connector):
        """Test profile schema has emailAddress field."""
        schema = connector.get_table_schema("profile", {})
        field_names = [f.name for f in schema.fields]

        assert "emailAddress" in field_names


class _FakeResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


def _valid_batch_body(boundary, payloads):
    """Build a multipart/mixed batch response body the parser can read."""
    parts = []
    for payload in payloads:
        parts.append(
            f"--{boundary}\r\n"
            "Content-Type: application/http\r\n\r\n"
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n\r\n"
            f"{payload}\r\n"
        )
    return "".join(parts) + f"--{boundary}--"


class TestBatchRequestFallback:
    """make_batch_request must not silently swallow a broken batch response.

    Google's global Gmail batch endpoint can answer 200 with a body the
    multipart parser cannot read. Without a guard that produces zero rows
    and no error, silently emptying every batch-backed operation
    (search_messages, incremental message/thread fetches).
    """

    def test_unit_empty_parse_on_200_falls_back_to_sequential(self, monkeypatch):
        """200 + unparseable body → fall back to per-request fetches."""
        client = GmailApiClient("cid", "secret", "refresh")
        # Short-circuit the token-endpoint exchange: seed a cached token.
        monkeypatch.setattr(client, "get_access_token", lambda: "tok")
        # Batch endpoint answers 200 but with a body carrying no JSON parts.
        monkeypatch.setattr(
            client._session,
            "post",
            lambda *a, **k: _FakeResponse(200, "<html>Service unavailable</html>"),
        )
        # Sequential fallback fetches each endpoint individually.
        monkeypatch.setattr(
            client,
            "make_request",
            lambda method, endpoint, params=None: {"id": endpoint.rsplit("/", 1)[-1]},
        )

        endpoints = ["/users/me/messages/a", "/users/me/messages/b"]
        results = client.make_batch_request(endpoints, [{"format": "metadata"}] * 2)

        assert [r["id"] for r in results] == ["a", "b"]

    def test_unit_non_200_falls_back_to_sequential(self, monkeypatch):
        """A non-200 batch response still falls back to per-request fetches."""
        client = GmailApiClient("cid", "secret", "refresh")
        # Short-circuit the token-endpoint exchange: seed a cached token.
        monkeypatch.setattr(client, "get_access_token", lambda: "tok")
        monkeypatch.setattr(
            client._session, "post", lambda *a, **k: _FakeResponse(503, "")
        )
        monkeypatch.setattr(
            client,
            "make_request",
            lambda method, endpoint, params=None: {"id": endpoint.rsplit("/", 1)[-1]},
        )

        results = client.make_batch_request(["/users/me/messages/x"])

        assert [r["id"] for r in results] == ["x"]

    def test_unit_valid_batch_response_is_used_without_fallback(self, monkeypatch):
        """A well-formed 200 batch body is parsed; no sequential fallback."""
        client = GmailApiClient("cid", "secret", "refresh")
        # Short-circuit the token-endpoint exchange: seed a cached token.
        monkeypatch.setattr(client, "get_access_token", lambda: "tok")
        body = _valid_batch_body(
            "batch_gmail_connector", ['{"id": "a"}', '{"id": "b"}']
        )
        monkeypatch.setattr(
            client._session, "post", lambda *a, **k: _FakeResponse(200, body)
        )

        def _should_not_fetch(*a, **k):  # pragma: no cover - guard
            raise AssertionError("sequential fallback ran on a valid batch body")

        monkeypatch.setattr(client, "make_request", _should_not_fetch)

        results = client.make_batch_request(
            ["/users/me/messages/a", "/users/me/messages/b"]
        )

        assert [r["id"] for r in results] == ["a", "b"]
