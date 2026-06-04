from databricks.labs.community_connector.sources.gmail.gmail import GmailLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


# =============================================================================
# Standard LakeflowConnect test suite + integration tests (require credentials)
# =============================================================================

class TestGmailConnector(LakeflowConnectTests):
    connector_class = GmailLakeflowConnect
    simulator_source = "gmail"
    replay_config = {
        "client_id": "simulator-client-id",
        "client_secret": "simulator-client-secret",
        "refresh_token": "simulator-refresh-token",
    }
    # Simulator corpus dates don't overlap the connector's first-call
    # window for these tables — fixture limitation, not a bug.
    allow_empty_first_read = frozenset({"messages", "threads"})

    # Extra Gamil specific integration tests.
    def test_read_profile(self):
        """Test reading user profile returns exactly 1 record with emailAddress."""
        records, offset = self.connector.read_table("profile", {}, {})
        records_list = list(records)

        assert len(records_list) == 1, "Profile should return exactly 1 record"
        assert "emailAddress" in records_list[0]
        assert "@" in records_list[0]["emailAddress"]

    def test_read_labels_has_inbox(self):
        """Test reading labels includes INBOX."""
        records, offset = self.connector.read_table("labels", {}, {})
        records_list = list(records)

        assert len(records_list) > 0, "Should have at least some labels"
        label_ids = [r.get("id", "") for r in records_list]
        assert any("INBOX" in lid for lid in label_ids), "Should have INBOX label"

    def test_read_messages_with_limit(self):
        """Test reading messages with maxResults limit."""
        records, offset = self.connector.read_table("messages", {}, {"maxResults": "5"})
        records_list = list(records)
        assert len(records_list) <= 5

    def test_read_settings(self):
        """Test reading account settings."""
        records, offset = self.connector.read_table("settings", {}, {})
        records_list = list(records)

        assert len(records_list) == 1
        assert "emailAddress" in records_list[0]

    def test_messages_with_query_filter(self):
        """Test messages table with q (query) option."""
        records, offset = self.connector.read_table(
            "messages", {}, {"q": "newer_than:30d", "maxResults": "5"}
        )
        records_list = list(records)
        assert isinstance(records_list, list)

    def test_messages_with_label_filter(self):
        """Test messages table with labelIds option."""
        records, offset = self.connector.read_table(
            "messages", {}, {"labelIds": "INBOX", "maxResults": "5"}
        )
        records_list = list(records)
        assert isinstance(records_list, list)

    def test_messages_with_format_metadata(self):
        """Test messages table with format=metadata option."""
        records, offset = self.connector.read_table(
            "messages", {}, {"format": "metadata", "maxResults": "3"}
        )
        records_list = list(records)
        if records_list:
            assert "id" in records_list[0]

    def test_threads_with_max_results(self):
        """Test threads table with maxResults option."""
        records, offset = self.connector.read_table("threads", {}, {"maxResults": "3"})
        records_list = list(records)
        assert len(records_list) <= 3

    def test_drafts_with_max_results(self):
        """Test drafts table with maxResults option."""
        records, offset = self.connector.read_table("drafts", {}, {"maxResults": "5"})
        records_list = list(records)
        assert isinstance(records_list, list)
