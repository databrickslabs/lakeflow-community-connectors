import pytest
from pyspark.sql.types import IntegerType

from databricks.labs.community_connector.sources.gmail.gmail import GmailLakeflowConnect


class TestGmailConnectorUnit:
    """Unit tests that validate connector structure without API calls."""

    @pytest.fixture
    def connector(self, monkeypatch):
        """Create connector with a dummy access token for structure tests.

        ``__init__`` snapshots the mailbox ``historyId`` via the profile
        endpoint (admission-control cap for AvailableNow termination), so
        constructing the connector requires either a real bearer token
        or a mocked HTTP layer.  Patch ``make_request`` so structural
        tests stay offline.
        """
        monkeypatch.setattr(
            "databricks.labs.community_connector.sources.gmail.gmail_utils."
            "GmailApiClient.make_request",
            lambda self, method, path, **kwargs: {"historyId": "1"},
        )
        return GmailLakeflowConnect({"access_token": "dummy-access-token"})

    def test_unit_initialization_with_valid_options(self, connector):
        """Connector initializes with the access_token injected by UC."""
        assert connector.access_token == "dummy-access-token"
        assert connector.user_id == "me"  # default

    def test_unit_initialization_missing_access_token(self):
        """__init__ fails fast with a COMMUNITY-pointing message."""
        with pytest.raises(ValueError, match="access_token") as info:
            GmailLakeflowConnect({})
        # Error points the user at the right knob to turn — the UC
        # connection's OAuth flow, not a local credential to set.
        assert "COMMUNITY" in str(info.value)

    def test_unit_initialization_with_refresh_token(self, monkeypatch):
        """Backward-compat: a connection with refresh_token + client creds
        uses the in-code refresh path instead of an injected access_token."""
        monkeypatch.setattr(
            "databricks.labs.community_connector.sources.gmail.gmail_utils."
            "GmailApiClient.make_request",
            lambda self, method, path, **kwargs: {"historyId": "1"},
        )
        connector = GmailLakeflowConnect(
            {
                "client_id": "cid",
                "client_secret": "secret",
                "refresh_token": "refresh",
            }
        )
        assert connector.access_token is None
        assert connector.refresh_token == "refresh"
        assert connector.api.client_id == "cid"
        assert connector.api.refresh_token == "refresh"

    def test_unit_refresh_token_requires_client_credentials(self):
        """refresh_token without client_id/client_secret fails fast."""
        with pytest.raises(ValueError, match="client_id"):
            GmailLakeflowConnect({"refresh_token": "refresh"})

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


class TestBatchRequestFallback:
    """make_batch_request must fall back to sequential when a 200 response
    parses to zero rows (Gmail's batch endpoint can answer 200 with a body
    the multipart parser can't read)."""

    class _FakeResp:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

    def _client(self):
        from databricks.labs.community_connector.sources.gmail.gmail_utils import (
            GmailApiClient,
        )
        return GmailApiClient(access_token="tok")

    def test_empty_parse_falls_back_to_sequential(self, monkeypatch):
        client = self._client()
        # Batch returns 200 but an unparseable body -> _parse_batch_response == [].
        monkeypatch.setattr(
            client._session, "post",
            lambda *a, **kw: self._FakeResp(200, "HTTP/1.1 200 OK\r\n\r\n<not multipart>"),
        )
        seen = []
        def fake_get(method, endpoint, params=None, retry_count=3):
            seen.append(endpoint)
            return {"id": endpoint.rsplit("/", 1)[-1]}
        monkeypatch.setattr(client, "make_request", fake_get)

        out = client.make_batch_request(["/users/me/messages/m1", "/users/me/messages/m2"])
        assert [r["id"] for r in out] == ["m1", "m2"]   # came from the sequential fallback
        assert len(seen) == 2

    def test_non_200_falls_back_to_sequential(self, monkeypatch):
        client = self._client()
        monkeypatch.setattr(
            client._session, "post", lambda *a, **kw: self._FakeResp(503, "unavailable")
        )
        monkeypatch.setattr(
            client, "make_request",
            lambda method, endpoint, params=None, retry_count=3: {"id": "x"},
        )
        out = client.make_batch_request(["/users/me/messages/m1"])
        assert out == [{"id": "x"}]
