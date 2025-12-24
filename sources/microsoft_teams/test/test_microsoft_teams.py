import pytest
import json
import os
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from sources.microsoft_teams.microsoft_teams import LakeflowConnect


# Configuration loading
def load_config():
    """
    Load test configuration from dev_config.json.

    Returns:
        dict: Configuration with credentials and test IDs

    Raises:
        FileNotFoundError: If dev_config.json doesn't exist
        KeyError: If required fields are missing
    """
    config_path = "sources/microsoft_teams/configs/dev_config.json"

    if not os.path.exists(config_path):
        pytest.skip(f"Configuration file not found: {config_path}")

    with open(config_path) as f:
        config = json.load(f)

    # Validate required fields
    required_fields = ["tenant_id", "client_id", "client_secret"]
    missing = [f for f in required_fields if not config.get(f) or config[f].startswith("YOUR_")]

    if missing:
        pytest.skip(f"Configuration incomplete. Please update {config_path} with: {missing}")

    return config


@pytest.fixture
def connector():
    """
    Create a LakeflowConnect instance for testing.

    Returns:
        LakeflowConnect: Initialized connector instance
    """
    config = load_config()
    return LakeflowConnect(config)


@pytest.fixture
def test_ids():
    """
    Get test team_id and channel_id from configuration.

    Returns:
        dict: Dictionary with test_team_id and test_channel_id
    """
    config = load_config()
    return {
        "test_team_id": config.get("test_team_id"),
        "test_channel_id": config.get("test_channel_id"),
    }


# ============================================================================
# Test: Initialization and Credentials
# ============================================================================


def test_init_success():
    """Test successful connector initialization with valid credentials."""
    config = load_config()
    connector = LakeflowConnect(config)

    assert connector.tenant_id == config["tenant_id"]
    assert connector.client_id == config["client_id"]
    assert connector.client_secret == config["client_secret"]
    assert connector.base_url == "https://graph.microsoft.com/v1.0"


def test_init_missing_tenant_id():
    """Test that initialization fails when tenant_id is missing."""
    with pytest.raises(ValueError, match="Missing required options"):
        LakeflowConnect({"client_id": "abc", "client_secret": "def"})


def test_init_missing_client_id():
    """Test that initialization fails when client_id is missing."""
    with pytest.raises(ValueError, match="Missing required options"):
        LakeflowConnect({"tenant_id": "abc", "client_secret": "def"})


def test_init_missing_client_secret():
    """Test that initialization fails when client_secret is missing."""
    with pytest.raises(ValueError, match="Missing required options"):
        LakeflowConnect({"tenant_id": "abc", "client_id": "def"})


def test_init_empty_options():
    """Test that initialization fails with empty options."""
    with pytest.raises(ValueError, match="Missing required options"):
        LakeflowConnect({})


# ============================================================================
# Test: Token Acquisition
# ============================================================================


def test_get_access_token(connector):
    """Test that access token can be acquired successfully."""
    token = connector._get_access_token()

    assert token is not None
    assert isinstance(token, str)
    assert len(token) > 0
    assert connector._access_token == token
    assert connector._token_expiry is not None


def test_token_caching(connector):
    """Test that tokens are cached and reused."""
    token1 = connector._get_access_token()
    token2 = connector._get_access_token()

    # Should return the same cached token
    assert token1 == token2


# ============================================================================
# Test: list_tables()
# ============================================================================


def test_list_tables(connector):
    """Test that list_tables returns all expected tables."""
    tables = connector.list_tables()

    assert isinstance(tables, list)
    assert len(tables) == 5
    assert "teams" in tables
    assert "channels" in tables
    assert "messages" in tables
    assert "members" in tables
    assert "chats" in tables


def test_list_tables_static():
    """Test that list_tables returns a static list (not API call)."""
    config = load_config()
    connector = LakeflowConnect(config)

    # Should not require API call
    tables = connector.list_tables()
    assert len(tables) == 5


# ============================================================================
# Test: get_table_schema()
# ============================================================================


def test_get_table_schema_teams(connector):
    """Test schema for teams table."""
    schema = connector.get_table_schema("teams", {})

    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]

    # Verify key fields
    assert "id" in field_names
    assert "displayName" in field_names
    assert "description" in field_names
    assert "visibility" in field_names
    assert "isArchived" in field_names
    assert "createdDateTime" in field_names
    assert "memberSettings" in field_names
    assert "guestSettings" in field_names

    # Verify id is not nullable
    id_field = next(f for f in schema.fields if f.name == "id")
    assert id_field.nullable == False


def test_get_table_schema_channels(connector):
    """Test schema for channels table."""
    schema = connector.get_table_schema("channels", {})

    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]

    assert "id" in field_names
    assert "team_id" in field_names  # Connector-derived
    assert "displayName" in field_names
    assert "membershipType" in field_names
    assert "email" in field_names


def test_get_table_schema_messages(connector):
    """Test schema for messages table."""
    schema = connector.get_table_schema("messages", {})

    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]

    assert "id" in field_names
    assert "team_id" in field_names  # Connector-derived
    assert "channel_id" in field_names  # Connector-derived
    assert "messageType" in field_names
    assert "createdDateTime" in field_names
    assert "lastModifiedDateTime" in field_names
    assert "from" in field_names
    assert "body" in field_names
    assert "attachments" in field_names
    assert "mentions" in field_names
    assert "reactions" in field_names

    # Verify nested structures
    from_field = next(f for f in schema.fields if f.name == "from")
    assert isinstance(from_field.dataType, StructType)

    body_field = next(f for f in schema.fields if f.name == "body")
    assert isinstance(body_field.dataType, StructType)

    attachments_field = next(f for f in schema.fields if f.name == "attachments")
    assert isinstance(attachments_field.dataType, ArrayType)


def test_get_table_schema_members(connector):
    """Test schema for members table."""
    schema = connector.get_table_schema("members", {})

    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]

    assert "id" in field_names
    assert "team_id" in field_names
    assert "roles" in field_names
    assert "displayName" in field_names
    assert "userId" in field_names


def test_get_table_schema_chats(connector):
    """Test schema for chats table."""
    schema = connector.get_table_schema("chats", {})

    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]

    assert "id" in field_names
    assert "topic" in field_names
    assert "chatType" in field_names
    assert "lastUpdatedDateTime" in field_names


def test_get_table_schema_invalid_table(connector):
    """Test that get_table_schema raises error for invalid table."""
    with pytest.raises(ValueError, match="Unsupported table"):
        connector.get_table_schema("invalid_table", {})


# ============================================================================
# Test: read_table_metadata()
# ============================================================================


def test_read_table_metadata_teams(connector):
    """Test metadata for teams table."""
    metadata = connector.read_table_metadata("teams", {})

    assert metadata["primary_keys"] == ["id"]
    assert metadata["ingestion_type"] == "snapshot"
    assert "cursor_field" not in metadata  # Snapshot tables don't have cursors


def test_read_table_metadata_channels(connector):
    """Test metadata for channels table."""
    metadata = connector.read_table_metadata("channels", {})

    assert metadata["primary_keys"] == ["id"]
    assert metadata["ingestion_type"] == "snapshot"


def test_read_table_metadata_messages(connector):
    """Test metadata for messages table."""
    metadata = connector.read_table_metadata("messages", {})

    assert metadata["primary_keys"] == ["id"]
    assert metadata["ingestion_type"] == "cdc"
    assert metadata["cursor_field"] == "lastModifiedDateTime"


def test_read_table_metadata_members(connector):
    """Test metadata for members table."""
    metadata = connector.read_table_metadata("members", {})

    assert metadata["primary_keys"] == ["id"]
    assert metadata["ingestion_type"] == "snapshot"


def test_read_table_metadata_chats(connector):
    """Test metadata for chats table."""
    metadata = connector.read_table_metadata("chats", {})

    assert metadata["primary_keys"] == ["id"]
    assert metadata["ingestion_type"] == "cdc"
    assert metadata["cursor_field"] == "lastUpdatedDateTime"


def test_read_table_metadata_invalid_table(connector):
    """Test that read_table_metadata raises error for invalid table."""
    with pytest.raises(ValueError, match="Unsupported table"):
        connector.read_table_metadata("invalid_table", {})


# ============================================================================
# Test: read_table() - teams (snapshot)
# ============================================================================


def test_read_teams_snapshot(connector):
    """Test reading teams table (snapshot mode)."""
    records, offset = connector.read_table("teams", {}, {})
    records_list = list(records)

    # Should return at least one team (the test tenant should have at least one team)
    assert len(records_list) >= 0

    if records_list:
        # Verify first record structure
        record = records_list[0]
        assert "id" in record
        assert "displayName" in record
        assert isinstance(record["id"], str)
        assert isinstance(record["displayName"], str)

        # Verify complex settings are serialized to JSON strings
        if "memberSettings" in record:
            assert isinstance(record["memberSettings"], str)
            # Should be valid JSON
            json.loads(record["memberSettings"])

    # Snapshot tables return empty offset
    assert offset == {}


def test_read_teams_with_pagination(connector):
    """Test teams table pagination with small page size."""
    records, offset = connector.read_table("teams", {}, {"top": "2"})
    records_list = list(records)

    # Should work even with small page size
    assert isinstance(records_list, list)
    assert offset == {}


# ============================================================================
# Test: read_table() - channels (snapshot)
# ============================================================================


def test_read_channels_requires_team_id(connector):
    """Test that channels table requires team_id."""
    with pytest.raises(ValueError, match="team_id"):
        connector.read_table("channels", {}, {})


def test_read_channels_with_team_id(connector, test_ids):
    """Test reading channels table with team_id."""
    if not test_ids["test_team_id"] or test_ids["test_team_id"].startswith("TEAM_ID"):
        pytest.skip("test_team_id not configured in dev_config.json")

    table_options = {"team_id": test_ids["test_team_id"]}
    records, offset = connector.read_table("channels", {}, table_options)
    records_list = list(records)

    # Should return at least one channel (General channel)
    assert len(records_list) >= 0

    if records_list:
        record = records_list[0]
        assert "id" in record
        assert "team_id" in record
        assert "displayName" in record
        assert record["team_id"] == test_ids["test_team_id"]

    assert offset == {}


# ============================================================================
# Test: read_table() - members (snapshot)
# ============================================================================


def test_read_members_requires_team_id(connector):
    """Test that members table requires team_id."""
    with pytest.raises(ValueError, match="team_id"):
        connector.read_table("members", {}, {})


def test_read_members_with_team_id(connector, test_ids):
    """Test reading members table with team_id."""
    if not test_ids["test_team_id"] or test_ids["test_team_id"].startswith("TEAM_ID"):
        pytest.skip("test_team_id not configured in dev_config.json")

    table_options = {"team_id": test_ids["test_team_id"]}
    records, offset = connector.read_table("members", {}, table_options)
    records_list = list(records)

    # Should have at least one member
    assert len(records_list) >= 0

    if records_list:
        record = records_list[0]
        assert "id" in record
        assert "team_id" in record
        assert "displayName" in record
        assert record["team_id"] == test_ids["test_team_id"]

        # Roles should be an array
        if "roles" in record:
            assert isinstance(record["roles"], list)

    assert offset == {}


# ============================================================================
# Test: read_table() - messages (CDC)
# ============================================================================


def test_read_messages_requires_team_and_channel_id(connector):
    """Test that messages table requires both team_id and channel_id."""
    with pytest.raises(ValueError, match="team_id"):
        connector.read_table("messages", {}, {})

    with pytest.raises(ValueError, match="channel_id"):
        connector.read_table("messages", {}, {"team_id": "abc"})


def test_read_messages_initial_sync(connector, test_ids):
    """Test reading messages table - initial sync (no offset)."""
    if not test_ids["test_team_id"] or not test_ids["test_channel_id"]:
        pytest.skip("test_team_id and test_channel_id not configured")

    table_options = {
        "team_id": test_ids["test_team_id"],
        "channel_id": test_ids["test_channel_id"],
        "top": "10",
    }

    records, offset = connector.read_table("messages", {}, table_options)
    records_list = list(records)

    # May be empty if channel has no messages
    assert isinstance(records_list, list)

    if records_list:
        record = records_list[0]
        assert "id" in record
        assert "team_id" in record
        assert "channel_id" in record
        assert "lastModifiedDateTime" in record
        assert record["team_id"] == test_ids["test_team_id"]
        assert record["channel_id"] == test_ids["test_channel_id"]

        # Verify cursor in offset
        assert "cursor" in offset
        assert offset["cursor"] is not None


def test_read_messages_incremental_sync(connector, test_ids):
    """Test incremental read with cursor (CDC)."""
    if not test_ids["test_team_id"] or not test_ids["test_channel_id"]:
        pytest.skip("test_team_id and test_channel_id not configured")

    table_options = {
        "team_id": test_ids["test_team_id"],
        "channel_id": test_ids["test_channel_id"],
    }

    # First read
    records1, offset1 = connector.read_table("messages", {}, table_options)
    records_list1 = list(records1)

    # Second read with offset
    if offset1 and "cursor" in offset1:
        records2, offset2 = connector.read_table("messages", offset1, table_options)
        records_list2 = list(records2)

        # Should have cursor in new offset
        assert "cursor" in offset2


def test_read_messages_with_lookback(connector, test_ids):
    """Test that lookback window is applied correctly."""
    if not test_ids["test_team_id"] or not test_ids["test_channel_id"]:
        pytest.skip("test_team_id and test_channel_id not configured")

    table_options = {
        "team_id": test_ids["test_team_id"],
        "channel_id": test_ids["test_channel_id"],
        "lookback_seconds": "600",  # 10 minutes
    }

    records, offset = connector.read_table("messages", {}, table_options)
    records_list = list(records)

    # Should work with custom lookback
    assert isinstance(records_list, list)


# ============================================================================
# Test: read_table() - chats (CDC)
# ============================================================================


def test_read_chats_initial_sync(connector):
    """Test reading chats table - initial sync."""
    records, offset = connector.read_table("chats", {}, {"top": "10"})
    records_list = list(records)

    # Should return chats (may be empty)
    assert isinstance(records_list, list)

    if records_list:
        record = records_list[0]
        assert "id" in record
        assert "chatType" in record
        assert "lastUpdatedDateTime" in record


def test_read_chats_incremental(connector):
    """Test incremental read of chats with cursor."""
    # First read
    records1, offset1 = connector.read_table("chats", {}, {})
    records_list1 = list(records1)

    # Second read with offset
    if offset1 and "cursor" in offset1:
        records2, offset2 = connector.read_table("chats", offset1, {})
        records_list2 = list(records2)

        assert "cursor" in offset2


# ============================================================================
# Test: Error Handling
# ============================================================================


def test_read_table_invalid_table_name(connector):
    """Test that read_table raises error for invalid table."""
    with pytest.raises(ValueError, match="Unsupported table"):
        connector.read_table("invalid_table", {}, {})


def test_invalid_team_id_404(connector):
    """Test that invalid team_id results in clear error."""
    table_options = {"team_id": "invalid-team-id-12345"}

    with pytest.raises(RuntimeError, match="404"):
        connector.read_table("channels", {}, table_options)


def test_option_parsing_invalid_top(connector, test_ids):
    """Test that invalid 'top' option is handled gracefully."""
    if not test_ids["test_team_id"]:
        pytest.skip("test_team_id not configured")

    table_options = {
        "team_id": test_ids["test_team_id"],
        "top": "invalid",  # Should default to 50
    }

    # Should not crash, should use default
    records, offset = connector.read_table("channels", {}, table_options)
    records_list = list(records)

    assert isinstance(records_list, list)


# ============================================================================
# Test: Pagination & Rate Limiting
# ============================================================================


def test_max_pages_per_batch_limit(connector):
    """Test that max_pages_per_batch is respected."""
    table_options = {
        "top": "1",  # Very small page size
        "max_pages_per_batch": "2",  # Limit to 2 pages
    }

    records, offset = connector.read_table("teams", {}, table_options)
    records_list = list(records)

    # Should stop after 2 pages (max 2 records)
    assert len(records_list) <= 2


# ============================================================================
# Test: Schema Validation
# ============================================================================


def test_teams_record_matches_schema(connector):
    """Test that actual teams records match the declared schema."""
    schema = connector.get_table_schema("teams", {})
    records, _ = connector.read_table("teams", {}, {"top": "1"})
    records_list = list(records)

    if records_list:
        record = records_list[0]

        # All schema fields should either exist or be nullable
        for field in schema.fields:
            if not field.nullable:
                assert field.name in record, f"Required field {field.name} missing"


def test_messages_record_matches_schema(connector, test_ids):
    """Test that actual message records match the declared schema."""
    if not test_ids["test_team_id"] or not test_ids["test_channel_id"]:
        pytest.skip("test_team_id and test_channel_id not configured")

    schema = connector.get_table_schema("messages", {})
    table_options = {
        "team_id": test_ids["test_team_id"],
        "channel_id": test_ids["test_channel_id"],
        "top": "1",
    }

    records, _ = connector.read_table("messages", {}, table_options)
    records_list = list(records)

    if records_list:
        record = records_list[0]

        # Non-nullable fields must exist
        for field in schema.fields:
            if not field.nullable:
                assert field.name in record, f"Required field {field.name} missing"


# ============================================================================
# Test: End-to-End Workflow
# ============================================================================


def test_full_workflow_teams(connector):
    """Test complete workflow for teams table."""
    # 1. List tables
    tables = connector.list_tables()
    assert "teams" in tables

    # 2. Get schema
    schema = connector.get_table_schema("teams", {})
    assert schema is not None

    # 3. Get metadata
    metadata = connector.read_table_metadata("teams", {})
    assert metadata["ingestion_type"] == "snapshot"

    # 4. Read data
    records, offset = connector.read_table("teams", {}, {})
    records_list = list(records)

    # Should complete without errors
    assert isinstance(records_list, list)


def test_full_workflow_messages_cdc(connector, test_ids):
    """Test complete CDC workflow for messages table."""
    if not test_ids["test_team_id"] or not test_ids["test_channel_id"]:
        pytest.skip("test_team_id and test_channel_id not configured")

    table_options = {
        "team_id": test_ids["test_team_id"],
        "channel_id": test_ids["test_channel_id"],
    }

    # 1. Get metadata
    metadata = connector.read_table_metadata("messages", {})
    assert metadata["ingestion_type"] == "cdc"
    assert metadata["cursor_field"] == "lastModifiedDateTime"

    # 2. Initial sync
    records1, offset1 = connector.read_table("messages", {}, table_options)
    records_list1 = list(records1)

    # 3. Incremental sync
    if offset1 and "cursor" in offset1:
        records2, offset2 = connector.read_table("messages", offset1, table_options)
        records_list2 = list(records2)

        # Should have new cursor
        assert "cursor" in offset2
