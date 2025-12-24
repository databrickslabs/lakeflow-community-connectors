# Microsoft Teams Connector - Test Suite

This directory contains comprehensive tests for the Microsoft Teams community connector.

## Prerequisites

### 1. Install Dependencies

```bash
pip install pytest pytest-cov pyspark requests
```

### 2. Configure Test Credentials

Copy the configuration template and fill in your credentials:

```bash
cp sources/microsoft_teams/configs/dev_config.json.example sources/microsoft_teams/configs/dev_config.json
```

Edit `sources/microsoft_teams/configs/dev_config.json`:

```json
{
  "tenant_id": "your-azure-ad-tenant-id",
  "client_id": "your-application-client-id",
  "client_secret": "your-client-secret",
  "test_team_id": "a-real-team-id-for-testing",
  "test_channel_id": "a-real-channel-id-in-that-team"
}
```

### 3. Set Up Azure AD Application

To run tests against the real Microsoft Graph API, you need:

1. **Azure AD Tenant** with Microsoft Teams enabled
2. **App Registration** with these permissions:
   - `Team.ReadBasic.All` (Application)
   - `Channel.ReadBasic.All` (Application)
   - `ChannelMessage.Read.All` (Application)
   - `TeamMember.Read.All` (Application)
   - `Chat.Read.All` (Application)
3. **Admin Consent** granted for all permissions
4. **Client Secret** created and saved

#### How to Get test_team_id and test_channel_id:

**Option 1: Via Microsoft Graph Explorer**
1. Go to https://developer.microsoft.com/en-us/graph/graph-explorer
2. Sign in with your test account
3. Run: `GET /me/joinedTeams` → Copy a team `id`
4. Run: `GET /teams/{team-id}/channels` → Copy a channel `id`

**Option 2: Via Teams Web UI**
1. Open Teams in browser
2. Navigate to a team and channel
3. Look at URL: `https://teams.microsoft.com/...teamId=<TEAM_ID>...channelId=<CHANNEL_ID>`

## Running Tests

### Run All Tests

```bash
# From repository root
pytest sources/microsoft_teams/test/test_microsoft_teams.py -v
```

### Run Specific Test Categories

```bash
# Test initialization only
pytest sources/microsoft_teams/test/test_microsoft_teams.py -v -k "test_init"

# Test schema definitions
pytest sources/microsoft_teams/test/test_microsoft_teams.py -v -k "test_get_table_schema"

# Test metadata
pytest sources/microsoft_teams/test/test_microsoft_teams.py -v -k "test_read_table_metadata"

# Test snapshot tables (teams, channels, members)
pytest sources/microsoft_teams/test/test_microsoft_teams.py -v -k "snapshot"

# Test CDC tables (messages, chats)
pytest sources/microsoft_teams/test/test_microsoft_teams.py -v -k "cdc or incremental"

# Test error handling
pytest sources/microsoft_teams/test/test_microsoft_teams.py -v -k "error or invalid"
```

### Run with Coverage

```bash
pytest sources/microsoft_teams/test/test_microsoft_teams.py --cov=sources.microsoft_teams --cov-report=html -v
```

Coverage report will be generated in `htmlcov/index.html`.

## Test Structure

### Test Categories

1. **Initialization Tests** (`test_init_*`)
   - Valid credential handling
   - Missing credential detection
   - Error messages for invalid configs

2. **Authentication Tests** (`test_get_access_token`, `test_token_caching`)
   - OAuth 2.0 token acquisition
   - Token caching and reuse
   - Token expiry handling

3. **Interface Tests** (`test_list_tables`, `test_get_table_schema_*`, `test_read_table_metadata_*`)
   - All required interface methods
   - Schema validation for all 5 tables
   - Metadata correctness

4. **Snapshot Table Tests** (`test_read_teams_*`, `test_read_channels_*`, `test_read_members_*`)
   - Full refresh behavior
   - Parent-child relationships (team_id requirement)
   - Pagination

5. **CDC Table Tests** (`test_read_messages_*`, `test_read_chats_*`)
   - Initial sync (no offset)
   - Incremental sync (with cursor)
   - Lookback window behavior
   - Cursor tracking

6. **Error Handling Tests** (`test_*_invalid_*`, `test_*_404`)
   - Invalid table names
   - Missing required options
   - API errors (404, 401, 403)
   - Graceful fallbacks

7. **End-to-End Tests** (`test_full_workflow_*`)
   - Complete workflows for snapshot and CDC tables
   - Multi-step operations

## Test Coverage

The test suite covers:

- ✅ **Initialization**: 5 tests (valid + 4 error cases)
- ✅ **Authentication**: 2 tests (token acquisition + caching)
- ✅ **list_tables()**: 2 tests
- ✅ **get_table_schema()**: 6 tests (5 tables + error case)
- ✅ **read_table_metadata()**: 6 tests (5 tables + error case)
- ✅ **read_table() - teams**: 2 tests (snapshot + pagination)
- ✅ **read_table() - channels**: 2 tests (validation + read)
- ✅ **read_table() - members**: 2 tests (validation + read)
- ✅ **read_table() - messages**: 4 tests (validation + initial + incremental + lookback)
- ✅ **read_table() - chats**: 2 tests (initial + incremental)
- ✅ **Error Handling**: 4 tests (invalid table, 404, option parsing, rate limiting)
- ✅ **Schema Validation**: 2 tests (teams + messages)
- ✅ **End-to-End**: 2 tests (snapshot + CDC workflows)

**Total: 41 comprehensive tests**

## Expected Test Results

### Tests That Should Always Pass

These tests don't require real API access:
- All `test_init_*` tests
- `test_list_tables_static`
- All `test_get_table_schema_*` tests (except invalid table)
- All `test_read_table_metadata_*` tests (except invalid table)
- `test_*_invalid_*` tests (error cases)

### Tests That Require Real API Access

These tests will be **skipped** if credentials are not configured:
- `test_get_access_token`
- `test_token_caching`
- `test_read_teams_*`
- `test_read_channels_*` (requires `test_team_id`)
- `test_read_members_*` (requires `test_team_id`)
- `test_read_messages_*` (requires `test_team_id` + `test_channel_id`)
- `test_read_chats_*`
- `test_full_workflow_*`

### Expected Skip Messages

If `dev_config.json` is not configured, you'll see:
```
SKIPPED [1] Configuration file not found
```

If credentials are incomplete (e.g., contain "YOUR_TENANT_ID"):
```
SKIPPED [1] Configuration incomplete. Please update dev_config.json with: ['tenant_id', 'client_id', 'client_secret']
```

If test IDs are not configured:
```
SKIPPED [1] test_team_id and test_channel_id not configured in dev_config.json
```

## Troubleshooting

### Authentication Failures (401)

**Error:** `RuntimeError: Token acquisition failed: 401`

**Solutions:**
- Verify `tenant_id`, `client_id`, `client_secret` are correct
- Ensure client secret hasn't expired
- Check that the app registration exists in the specified tenant

### Permission Denied (403)

**Error:** `RuntimeError: Forbidden (403). Please verify the app has required permissions`

**Solutions:**
- Grant admin consent for all required permissions
- Verify permissions are **Application** type (not Delegated)
- Wait 5-10 minutes after granting consent for changes to propagate

### Resource Not Found (404)

**Error:** `RuntimeError: Resource not found (404)`

**Solutions:**
- Verify `test_team_id` is a valid team ID
- Verify `test_channel_id` belongs to the specified team
- Ensure the app has access to the team (might need to be added as a team app)

### Rate Limiting (429)

**Error:** Test takes a long time or fails with rate limit error

**Solutions:**
- Tests include automatic retry with `Retry-After` header
- Reduce concurrent test execution
- Use smaller `top` values for pagination tests

### Import Errors

**Error:** `ModuleNotFoundError: No module named 'sources.microsoft_teams'`

**Solutions:**
- Run tests from repository root directory
- Add repository root to PYTHONPATH:
  ```bash
  export PYTHONPATH="${PYTHONPATH}:/path/to/lakeflow-community-connectors"
  ```

## CI/CD Integration

For continuous integration, you can:

1. **Store credentials as secrets** in your CI system
2. **Generate dev_config.json** dynamically:

```bash
cat > sources/microsoft_teams/configs/dev_config.json <<EOF
{
  "tenant_id": "${AZURE_TENANT_ID}",
  "client_id": "${AZURE_CLIENT_ID}",
  "client_secret": "${AZURE_CLIENT_SECRET}",
  "test_team_id": "${TEST_TEAM_ID}",
  "test_channel_id": "${TEST_CHANNEL_ID}"
}
EOF
```

3. **Run tests** as part of build:

```bash
pytest sources/microsoft_teams/test/ -v --tb=short
```

## Test Data Considerations

### Minimal Test Data Required

For successful test execution, your test tenant should have:
- **At least 1 team** (for teams table tests)
- **At least 1 channel** in the test team (for channels table tests)
- **At least 1 member** in the test team (for members table tests)
- **Optionally: messages** in the test channel (for messages table tests)
- **Optionally: chats** (for chats table tests)

### Clean Test Environment

Best practices:
- Use a **dedicated test tenant** (not production)
- Create a **test team** specifically for connector testing
- Keep test data **minimal** (faster tests, lower API usage)
- Tests are **read-only** (no data modification)

## Performance

Typical test execution times:
- **All tests (without API)**: ~1 second
- **All tests (with API, minimal data)**: ~30-60 seconds
- **All tests (with API, large dataset)**: ~2-5 minutes

To speed up tests:
- Use `pytest -x` (stop on first failure)
- Run only changed tests
- Use smaller datasets in test tenant

## Contributing

When adding new tests:
1. Follow existing naming conventions (`test_<functionality>_<scenario>`)
2. Add docstrings explaining what the test validates
3. Use appropriate `pytest.skip()` for tests requiring configuration
4. Add error handling tests for new functionality
5. Update this README with new test categories

## Support

For issues with tests:
1. Check troubleshooting section above
2. Verify Azure AD app configuration
3. Review Microsoft Graph API documentation
4. Check test logs for detailed error messages
