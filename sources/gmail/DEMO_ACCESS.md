# Gmail Connector - Demo Access Guide

## Quick Start for Judges

### Demo Gmail Account
```
Email: lakeflow.mail.demo@gmail.com
Password: DiscoverLakehouse
```

### Pre-populated Demo Data

The account contains **~20 emails** demonstrating:
- ✅ Customer support conversation threads (3 threads)
- ✅ Sales communications (4 emails)
- ✅ System notifications (4 emails)
- ✅ Marketing emails (3 emails)
- ✅ Draft messages (5 drafts)
- ✅ Custom Gmail labels (Customer Support, Sales, Notifications, Marketing)

## Option 1: Use Pre-configured Credentials (Fastest)

We've prepared ready-to-use OAuth credentials for testing.

### Step 1: Create Config File

Create file: `sources/gmail/configs/dev_config.json`

```json
{
  "client_id": "YOUR_CLIENT_ID_HERE",
  "client_secret": "YOUR_CLIENT_SECRET_HERE",
  "refresh_token": "YOUR_REFRESH_TOKEN_HERE",
  "user_id": "me"
}
```

**Note**: Actual credentials will be provided separately for security.

### Step 2: Run Tests

```bash
cd /path/to/lakeflow-community-connectors

# Run the test suite
pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v
```

**Expected Result**: ✅ All 6 tests pass

```
test_initialization ........................... PASSED
test_list_tables .............................. PASSED
test_get_table_schema ......................... PASSED
test_read_table_metadata ...................... PASSED
test_read_table ................................ PASSED
test_read_table_deletes ....................... PASSED
```

## Option 2: Set Up Your Own OAuth (Alternative)

If you prefer to use your own OAuth credentials:

### Step 1: Create OAuth Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or use existing
3. Enable Gmail API
4. Create OAuth 2.0 Desktop credentials
5. Configure OAuth consent screen with scope: `https://www.googleapis.com/auth/gmail.readonly`
6. Add `lakeflow.mail.demo@gmail.com` as a test user

### Step 2: Get Refresh Token

```bash
# Run the helper script
python sources/gmail/get_refresh_token.py

# Follow the prompts:
# 1. Enter your Client ID
# 2. Enter your Client Secret
# 3. Authorize in the browser (use demo account credentials)
```

This automatically creates `sources/gmail/configs/dev_config.json`.

### Step 3: Run Tests

```bash
pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v
```

## What the Connector Does

### Tables Supported (5 tables)

1. **messages** - Email messages with full content
   - Ingestion Type: CDC (incremental sync with historyId)
   - Schema: Nested structure with headers, body parts, attachments metadata

2. **threads** - Email conversation threads
   - Ingestion Type: CDC (incremental sync with historyId)
   - Schema: Contains array of messages in the thread

3. **labels** - Gmail labels (INBOX, SENT, custom labels)
   - Ingestion Type: Snapshot (full sync)
   - Schema: Label metadata and counts

4. **drafts** - Draft messages
   - Ingestion Type: Snapshot (full sync)
   - Schema: Draft with embedded message object

5. **profile** - User profile information
   - Ingestion Type: Snapshot (full sync)
   - Schema: Email address, message counts, historyId

### Key Features Demonstrated

✅ **OAuth 2.0 Authentication**: Secure token-based auth with automatic refresh
✅ **Incremental Sync (CDC)**: Uses Gmail History API for efficient updates
✅ **Full Sync**: Initial load of all historical data
✅ **Rich Schemas**: Handles nested JSON structures (headers, parts, attachments)
✅ **Error Handling**: Automatic fallback and retry logic
✅ **Rate Limiting**: Built-in delays to respect API limits

## Demo Workflow

### 1. Local Testing (5 minutes)
```bash
# Show connector implementation
head -100 sources/gmail/gmail.py

# Run tests
pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v

# All tests should pass ✅
```

### 2. Inspect Demo Data (2 minutes)
- Log into Gmail web interface with demo credentials
- Browse the emails organized by labels
- Note the variety of content (HTML, plain text, threads)

### 3. Deploy to Databricks (10 minutes)
- Use community connector CLI tool
- Configure Delta Live Tables pipeline
- Run initial ingestion (full sync)
- Query data in Unity Catalog

### 4. Demonstrate Incremental Sync (5 minutes)
```bash
# Add new emails to demonstrate CDC
python sources/gmail/demo/add_incremental_data.py

# Re-run ingestion (incremental mode)
# Only new emails are synced (efficient!)
```

## Sample SQL Queries (After Ingestion)

```sql
-- Count emails by label
SELECT
  explode(labelIds) as label,
  COUNT(*) as email_count
FROM gmail_messages
GROUP BY label
ORDER BY email_count DESC;

-- Find customer support emails
SELECT
  id,
  snippet,
  FROM_UNIXTIME(CAST(internalDate AS BIGINT)/1000) as received_date
FROM gmail_messages
WHERE array_contains(labelIds, 'Customer Support')
ORDER BY internalDate DESC;

-- Analyze email threads
SELECT
  t.id as thread_id,
  t.snippet,
  SIZE(t.messages) as message_count,
  t.historyId
FROM gmail_threads t
WHERE SIZE(t.messages) > 1
ORDER BY message_count DESC;

-- Extract email headers
SELECT
  m.id,
  header.name,
  header.value
FROM gmail_messages m
LATERAL VIEW explode(m.payload.headers) as header
WHERE header.name IN ('From', 'To', 'Subject', 'Date');

-- Profile statistics
SELECT
  emailAddress,
  messagesTotal,
  threadsTotal,
  historyId
FROM gmail_profile;
```

## Architecture Highlights

### LakeflowConnect Interface
The connector implements the standard interface:
- `__init__()` - OAuth token exchange
- `list_tables()` - Returns 5 tables
- `get_table_schema()` - Returns PySpark StructType schemas
- `read_table_metadata()` - Returns primary keys, cursor field, ingestion type
- `read_table()` - Returns iterator of records + offset dict

### Incremental Sync Pattern
```python
# Full sync: List message IDs → Fetch details → Track max historyId
if not start_offset:
    return _read_messages_full()

# Incremental sync: History API → Extract changed IDs → Fetch updates
elif "historyId" in start_offset:
    return _read_messages_incremental(start_offset["historyId"])
```

### Error Recovery
- Automatic access token refresh on 401 errors
- Fallback to full sync on 404 (expired historyId)
- Proper handling of rate limits and timeouts

## Technical Specifications

**API Used**: Gmail API v1
**Authentication**: OAuth 2.0 (refresh token flow)
**Primary Scope**: `gmail.readonly`
**Rate Limits**: 1.2M quota units/minute per project
**Cursor Field**: historyId (for CDC tables)
**Pagination**: pageToken-based

## Files to Review

### Core Implementation
- `sources/gmail/gmail.py` - Main connector (500 lines)
- `sources/gmail/gmail_api_doc.md` - API research documentation

### Testing
- `sources/gmail/test/test_gmail_lakeflow_connect.py` - Test suite integration
- Tests validate: schemas, metadata, data reading, error handling

### Documentation
- `sources/gmail/README.md` - User-facing documentation
- `sources/gmail/SETUP_GUIDE.md` - OAuth setup guide
- `sources/gmail/demo/README.md` - Demo data scripts guide

### Demo Scripts
- `sources/gmail/demo/populate_demo_data.py` - Creates initial demo data
- `sources/gmail/demo/add_incremental_data.py` - Adds data for CDC demo

## Support & Questions

For issues during evaluation:

1. **Test failures**: Ensure `dev_config.json` has valid OAuth credentials
2. **Authentication errors**: Token may have expired, regenerate using `get_refresh_token.py`
3. **API errors**: Check network connectivity and Gmail API enablement
4. **Import errors**: Ensure project root is in PYTHONPATH

## Judging Criteria Alignment

### Completeness & Functionality (50%)
✅ All 5 tables fully implemented
✅ Complete API coverage (messages, threads, labels, drafts, profile)
✅ Both CDC and snapshot modes supported
✅ Comprehensive test suite (6/6 passing)
✅ Error handling and recovery mechanisms

### Methodology & Reusability (30%)
✅ Documented development methodology (vibe coding approach)
✅ OAuth pattern reusable for other Google APIs (Calendar, Drive, etc.)
✅ History API pattern applicable to similar services
✅ Helper scripts (get_refresh_token.py) reusable
✅ Schema design patterns for nested structures

### Code Quality & Efficiency (20%)
✅ Well-structured with clear separation of concerns
✅ Efficient CDC using History API (minimal API calls)
✅ Proper error handling with automatic token refresh
✅ Comprehensive inline documentation
✅ Follows Python best practices and type hints

---

**Ready to test?** Start with Option 1 above and run the pytest command! 🚀
