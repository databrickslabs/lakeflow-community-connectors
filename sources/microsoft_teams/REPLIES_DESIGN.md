# Message Replies Implementation Design

## Overview

Add support for ingesting threaded message replies from Microsoft Teams channels. This enables capturing full conversation threads, not just top-level messages.

## Current State

- **messages table**: Ingests only top-level channel messages
- **API endpoint**: `GET /teams/{team_id}/channels/{channel_id}/messages`
- **Limitation**: Replies to messages (threads) are not fetched

## Proposed Solution

Add a new **`message_replies`** table that:
1. Requires a parent message ID to fetch its replies
2. Uses the `/messages/{message_id}/replies` endpoint
3. Supports CDC mode for incremental ingestion
4. Can be configured per-channel or globally

---

## API Endpoint

### Microsoft Graph API
```
GET /teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies
```

**Required Permissions**: Same as messages
- `ChannelMessage.Read.All` (Application)

**Response Structure**: Same as messages endpoint
- Returns array of `chatMessage` objects
- Supports pagination via `@odata.nextLink`
- Does NOT support `$filter` on `lastModifiedDateTime` (client-side filtering required)

**Example Response**:
```json
{
  "value": [
    {
      "id": "1234567890123",
      "replyToId": "parent_message_id",
      "messageType": "message",
      "createdDateTime": "2025-01-01T10:00:00Z",
      "lastModifiedDateTime": "2025-01-01T10:00:00Z",
      "from": {...},
      "body": {...}
    }
  ],
  "@odata.nextLink": "..."
}
```

---

## Table Schema

### `message_replies`

Same schema as `messages` table with additional connector-derived field:

| Field | Type | Description | Source |
|-------|------|-------------|--------|
| id | String | Unique reply ID (PK) | API |
| **parent_message_id** | String | Parent message ID | **Connector-derived** |
| team_id | String | Parent team ID | **Connector-derived** |
| channel_id | String | Parent channel ID | **Connector-derived** |
| replyToId | String | Always matches parent_message_id | API |
| etag | String | ETag for versioning | API |
| messageType | String | "message", "systemEventMessage" | API |
| createdDateTime | String | ISO 8601 timestamp | API |
| lastModifiedDateTime | String | Last modification (cursor) | API |
| lastEditedDateTime | String | Last edit timestamp | API |
| deletedDateTime | String | Deletion timestamp | API |
| subject | String | Reply subject | API |
| summary | String | Reply summary | API |
| importance | String | "normal", "high", "urgent" | API |
| locale | String | Language locale | API |
| webUrl | String | URL to reply in Teams | API |
| from | Struct | Sender information | API |
| body | Struct | Message content | API |
| attachments | Array[Struct] | File attachments | API |
| mentions | Array[Struct] | @mentions | API |
| reactions | Array[Struct] | Emoji reactions | API |

**Primary Key**: `id`
**Cursor Field**: `lastModifiedDateTime` (for CDC mode)
**Ingestion Type**: CDC (incremental updates)

---

## Configuration Options

### Required Options

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `team_id` | String (GUID) | Parent team identifier | `"abc-123-..."` |
| `channel_id` | String (GUID) | Parent channel identifier | `"19:xyz..."` |
| `message_id` | String | Parent message ID to fetch replies for | `"1234567890123"` |

### Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `top` | String (int) | `50` | Page size (max: 50) |
| `max_pages_per_batch` | String (int) | `100` | Safety limit for pagination |
| `start_date` | String (ISO 8601) | None | Initial cursor for first sync |
| `lookback_seconds` | String (int) | `300` | Lookback window for late updates |

---

## Implementation Strategy

### Option A: Static Message ID Configuration (Initial Implementation)

**Pros**:
- Simple to implement
- Works with existing multi-config framework
- Users have full control over which threads to track

**Cons**:
- Requires manual configuration per message thread
- Doesn't automatically discover new threads

**Configuration Example**:
```python
REPLY_CONFIGS = [
    {
        "team_id": "abc-123",
        "channel_id": "19:xyz",
        "message_id": "1234567890123"  # Specific thread to track
    },
    {
        "team_id": "abc-123",
        "channel_id": "19:xyz",
        "message_id": "9876543210987"  # Another thread
    }
]
```

### Option B: Dynamic Discovery from Messages Table (Future Enhancement)

**Pros**:
- Automatic discovery of all threads
- No manual configuration needed

**Cons**:
- Requires reading from messages table first
- More complex implementation
- Dependency on messages table data

**Approach**:
1. Read top-level messages from messages table
2. For each message, create a reply ingestion task
3. Use DLT's dependency management

---

## Recommended Approach: Option A (Static Config)

**Rationale**:
1. **Consistency**: Matches existing pattern for messages (requires channel_id)
2. **Simplicity**: Works with current multi-config framework
3. **Flexibility**: Users can choose which threads matter
4. **Performance**: Avoid fetching replies for unimportant threads
5. **Incremental**: Can add Option B as enhancement later

**Use Cases**:
- Track specific high-value discussion threads
- Monitor important announcements and their replies
- Ingest replies from a known set of pinned/important messages

---

## Implementation Tasks

### 1. Connector Changes (microsoft_teams.py)

#### 1.1 Add Table Metadata
```python
"message_replies": {
    "primary_keys": ["id"],
    "cursor_field": "lastModifiedDateTime",
    "ingestion_type": "cdc",
    "endpoint": "teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies",
    "requires_parent": ["team_id", "channel_id", "message_id"],
}
```

#### 1.2 Add Schema Definition
```python
elif table_name == "message_replies":
    # Same schema as messages with parent_message_id field
    return StructType([
        StructField("id", StringType(), False),
        StructField("parent_message_id", StringType(), False),  # NEW
        StructField("team_id", StringType(), False),
        StructField("channel_id", StringType(), False),
        # ... rest same as messages
    ])
```

#### 1.3 Add Read Method
```python
def _read_message_replies(
    self, start_offset: dict, table_options: dict[str, str]
) -> Tuple[Iterator[dict], dict]:
    """
    Read message replies (threads) for a specific message.

    Required options:
    - team_id: Parent team ID
    - channel_id: Parent channel ID
    - message_id: Parent message ID to fetch replies for

    Optional options:
    - top: Page size (max 50)
    - max_pages_per_batch: Safety limit
    - start_date: Initial cursor
    - lookback_seconds: Lookback window (default 300)
    """
    team_id = table_options.get("team_id")
    channel_id = table_options.get("channel_id")
    message_id = table_options.get("message_id")

    if not team_id or not channel_id or not message_id:
        raise ValueError(
            "table_options for 'message_replies' must include "
            "'team_id', 'channel_id', and 'message_id'"
        )

    # Build URL
    url = f"{self.base_url}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies"

    # ... rest similar to _read_messages()
    # - Parse options (top, max_pages, lookback_seconds)
    # - Fetch with pagination
    # - Add connector-derived fields: parent_message_id, team_id, channel_id
    # - Apply cursor filtering
    # - Return iterator and next offset
```

#### 1.4 Update read_table Router
```python
def read_table(self, table_name: str, start_offset: dict, table_options: dict[str, str]):
    # ... existing tables
    elif table_name == "message_replies":
        return self._read_message_replies(start_offset, table_options)
```

### 2. Documentation Updates

#### 2.1 Update README.md
- Add `message_replies` to supported tables list
- Document configuration requirements
- Add example configuration
- Explain relationship to messages table

#### 2.2 Update API Documentation
- Document `/replies` endpoint behavior
- Note limitations (no $filter support)
- Explain CDC cursor handling

### 3. Testing

#### 3.1 Unit Tests
```python
def test_read_message_replies_basic():
    """Test basic reply ingestion"""

def test_read_message_replies_with_cursor():
    """Test CDC mode with cursor"""

def test_read_message_replies_missing_message_id():
    """Test error handling for missing message_id"""
```

#### 3.2 Integration Tests
- Test with actual Teams channel with threaded messages
- Verify CDC cursor progression
- Test multi-config (multiple message_ids)

#### 3.3 End-to-End Test
```python
# Configuration
REPLY_CONFIGS = [
    {
        "team_id": "abc-123",
        "channel_id": "19:xyz",
        "message_id": "parent_msg_1"
    },
    {
        "team_id": "abc-123",
        "channel_id": "19:xyz",
        "message_id": "parent_msg_2"
    }
]

# Pipeline spec generation (similar to messages)
for config in REPLY_CONFIGS:
    pipeline_spec["objects"].append({
        "table": {
            "source_table": "message_replies",
            "destination_table": "lakeflow_connector_message_replies",
            "table_configuration": {
                **creds,
                **config,
                "top": "50",
                "max_pages_per_batch": "10"
            }
        }
    })
```

### 4. Example Pipeline Configuration

```python
# Step 1: Ingest top-level messages
INGEST_MESSAGES = True
CHANNEL_IDS = [{"team_id": "...", "channel_id": "..."}]

# Step 2: Query messages table to find important threads
# SELECT id, subject FROM messages WHERE subject LIKE '%important%'

# Step 3: Configure replies for those threads
INGEST_REPLIES = True
REPLY_CONFIGS = [
    {
        "team_id": "abc-123",
        "channel_id": "19:xyz",
        "message_id": "1234567890123"  # From messages query
    }
]
```

---

## Open Questions

1. **Naming**: `message_replies` vs `replies` vs `threads`?
   - **Recommendation**: `message_replies` (consistent with `messages`, clear parent-child relationship)

2. **Auto-discovery**: Should we add Option B (dynamic discovery) in v1?
   - **Recommendation**: No, defer to future enhancement. Start with static config.

3. **Nested replies**: Teams supports replies to replies (nested threads). Include in v1?
   - **Recommendation**: No, the API structure is flat (all replies are at same level). No special handling needed.

4. **Deleted message handling**: What if parent message is deleted?
   - **Recommendation**: API will return 404. Handle gracefully in connector, log warning, skip.

5. **Performance**: Fetching replies for many messages could be slow. Rate limiting?
   - **Recommendation**: Use same retry/rate limiting as messages (100ms delay, exponential backoff)

---

## Migration Path for Users

### Phase 1: Messages Only (Current State)
```python
INGEST_MESSAGES = True
CHANNEL_IDS = [...]
```

### Phase 2: Add Replies for Important Threads
```python
INGEST_MESSAGES = True
INGEST_REPLIES = True

CHANNEL_IDS = [...]
REPLY_CONFIGS = [...]  # Manually configure threads to track
```

### Phase 3: (Future) Auto-Discovery
```python
INGEST_MESSAGES = True
INGEST_REPLIES = True
INGEST_ALL_REPLIES = True  # Automatically fetch replies for all messages

CHANNEL_IDS = [...]
# No REPLY_CONFIGS needed - auto-discovered
```

---

## Success Criteria

1. ✅ Can ingest replies for a specific message
2. ✅ CDC mode works with cursor tracking
3. ✅ Multi-config support (multiple message_ids)
4. ✅ Proper error handling (404 for deleted messages)
5. ✅ Documentation includes clear examples
6. ✅ Tests cover basic + CDC + multi-config scenarios

---

## Timeline Estimate

- **Connector implementation**: 2-3 hours
- **Testing**: 1-2 hours
- **Documentation**: 1 hour
- **Total**: 4-6 hours of focused work

---

## Next Steps

1. Review and approve this design
2. Implement connector changes in `microsoft_teams.py`
3. Add test cases
4. Update documentation
5. Test end-to-end with real Teams data
6. Commit and push to GitHub

Ready to proceed with implementation?
