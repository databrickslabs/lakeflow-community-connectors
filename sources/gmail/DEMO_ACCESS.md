# Gmail Connector - Testing Guide

## For Internal Evaluators/Judges

### Demo Account Access

A pre-configured demo Gmail account with test data is available for evaluation purposes.

**Access Credentials**: Available in the internal **Keeper vault**:
```
Vault Path: Hackathon 2026 / Gmail Connector Demo Account
```

The demo account includes:
- ~20 pre-existing emails demonstrating various use cases
- Customer support threads, sales communications, system notifications
- Custom labels and organized data
- Ready for immediate testing

---

## Testing with Your Own Gmail Account

### Why Use Your Own Account?

- **No setup required**: Use your existing emails
- **Real data**: More authentic testing experience
- **Safe**: Read-only access (`gmail.readonly` scope)
- **No automation risks**: No flagging by Google

### Prerequisites

1. **Gmail Account**: Personal Gmail or Google Workspace account with existing emails
2. **Google Cloud Access**: Ability to create OAuth 2.0 credentials
3. **Time Required**: ~10-15 minutes for OAuth setup

### Quick Start

#### Step 1: Set Up OAuth Credentials

Follow the detailed guide in [SETUP_GUIDE.md](./SETUP_GUIDE.md) to:

1. **Create Google Cloud Project** (2 min)
   - Go to https://console.cloud.google.com/
   - Create new project: "Gmail Connector Test"

2. **Enable Gmail API** (1 min)
   - Navigate to APIs & Services → Library
   - Search "Gmail API" → Enable

3. **Configure OAuth Consent Screen** (3 min)
   - Choose "External" user type
   - Add scope: `https://www.googleapis.com/auth/gmail.readonly`
   - Add yourself as test user

4. **Create OAuth Credentials** (2 min)
   - APIs & Services → Credentials
   - Create OAuth 2.0 Client ID (Desktop app type)
   - Save Client ID and Client Secret

5. **Generate Refresh Token** (2 min)
   ```bash
   python sources/gmail/get_refresh_token.py
   ```
   - Enter your Client ID and Secret
   - Authorize in browser
   - Refresh token saved automatically

#### Step 2: Create Configuration File

The helper script creates this automatically, but you can also create manually:

`sources/gmail/configs/dev_config.json`:
```json
{
  "client_id": "YOUR_CLIENT_ID_HERE",
  "client_secret": "YOUR_CLIENT_SECRET_HERE",
  "refresh_token": "YOUR_REFRESH_TOKEN_HERE",
  "user_id": "me"
}
```

⚠️ **Security Note**: This file is in `.gitignore` and will NOT be committed.

#### Step 3: Run Tests

```bash
# Install dependencies
pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client pytest

# Run tests
pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v
```

**Expected Result**: All 6 tests pass ✅

---

## What Gets Tested with Your Emails

The connector will read from your actual Gmail account:

### Data Read (All Read-Only)
- ✅ Email messages (subject, body, headers)
- ✅ Email threads/conversations
- ✅ Gmail labels (INBOX, SENT, custom labels)
- ✅ Draft messages
- ✅ Profile information (email address, message counts)

### Data NOT Modified
- ❌ No emails sent
- ❌ No emails deleted
- ❌ No labels modified
- ❌ No settings changed
- ❌ No mark as read/unread

**Scope**: Only `gmail.readonly` - completely safe!

---

## Testing Different Features

### Recommended Email Content (Use What You Have)

Your existing Gmail likely already has suitable test data. Ideal content includes:

**Minimum** (most accounts have this):
- 10+ emails in inbox
- At least 1 email thread (conversation)
- A few labels (INBOX, SENT, etc.)
- Maybe a draft or two

**Better** (for comprehensive testing):
- Email threads with multiple replies
- Custom labels you've created
- Mix of personal and automated emails (newsletters, notifications)
- Emails from different time periods
- HTML formatted emails (most modern emails)

**You don't need to create anything new** - the connector works with your existing emails!

### Test Execution Output

When you run tests, you'll see output showing YOUR real emails:

```python
# Sample output (your actual data):
{
  "id": "abc123def456",
  "threadId": "abc123def456",
  "labelIds": ["INBOX", "IMPORTANT"],
  "snippet": "Hi, following up on our meeting...",
  "internalDate": "1704067200000",
  "from": "colleague@company.com",
  "subject": "Meeting Follow-up"
}
```

This confirms the connector correctly reads and structures your Gmail data.

---

## Testing Specific Features

### Test 1: Basic Authentication
```bash
pytest sources/gmail/test/ -v -k "test_initialization"
```
Verifies OAuth works with your credentials.

### Test 2: List Your Tables
```bash
pytest sources/gmail/test/ -v -k "test_list_tables"
```
Shows the 5 available tables: messages, threads, labels, drafts, profile.

### Test 3: Read Your Messages
```bash
pytest sources/gmail/test/ -v -k "test_read_table"
```
Reads actual emails from your inbox - you'll see snippets in the output.

### Test 4: Check Your Labels
The test will discover all your labels:
- Default labels: INBOX, SENT, DRAFTS, TRASH, SPAM
- Your custom labels (if any)
- Label statistics (message counts, unread counts)

### Test 5: Incremental Sync

To test CDC mode:
1. Note your current Gmail historyId (from test output)
2. Send yourself a new email or receive one naturally
3. Re-run tests - connector detects only the new email!

---

## Privacy & Security

### What Data is Accessed?

The connector reads your Gmail data temporarily during testing:
- Data stays on your machine
- No data sent to external servers (except Gmail API)
- No data stored permanently (tests run and complete)

### OAuth Permissions

The `gmail.readonly` scope allows:
- ✅ Read emails and metadata
- ✅ Read labels and settings
- ❌ Cannot send emails
- ❌ Cannot delete or modify
- ❌ Cannot access other Google services

### Revoking Access

After testing, you can revoke access:

1. Go to https://myaccount.google.com/permissions
2. Find your OAuth app ("Gmail Connector Test")
3. Click "Remove Access"

This immediately invalidates the refresh token.

### Best Practices

1. **Use Test Account** (if available): Safer than production email
2. **Limit Scope**: Only grant `gmail.readonly` (already configured)
3. **Rotate Credentials**: Change OAuth credentials after hackathon
4. **Check Periodically**: Monitor Google account activity
5. **Clean Up**: Delete `dev_config.json` after testing

---

## Troubleshooting

### "Invalid credentials"
**Solution**: Regenerate refresh token:
```bash
python sources/gmail/get_refresh_token.py
```

### "Access blocked"
**Solution**: Add yourself as test user in OAuth consent screen
1. Google Cloud Console → OAuth consent screen
2. Test users → Add your email
3. Try authentication again

### "No data found" in tests
**Check**:
- Your Gmail account has emails (inbox not empty)
- Labels exist (at least INBOX)
- OAuth token has correct permissions

### Rate limiting
**If you see quota errors**:
- Wait 1 minute and retry
- Gmail API allows 1.2M requests/minute (plenty for testing)
- Individual test runs use ~20-50 requests

---

## What Makes a Good Test?

### Sufficient Data ✅
Your account should have:
- At least 10 emails (most people have hundreds)
- At least 1 conversation/thread
- Some labels (most accounts have several)

### Variety is Helpful ✅
- Mix of new and old emails
- Different senders and subjects
- HTML and plain text
- Attachments (metadata will be captured)

### You Don't Need Perfect Data ❌
- Don't create fake test data
- Don't organize specially for testing
- Don't clean up your inbox
- Just use what you have naturally!

**The connector works with messy, real-world email data.**

---

## Testing Checklist

Before submitting results:

- [ ] OAuth credentials configured
- [ ] All 6 tests passing
- [ ] Tested with your real Gmail account
- [ ] Verified messages table reads your emails
- [ ] Verified threads table shows conversations
- [ ] Verified labels table lists your labels
- [ ] Checked profile table shows your stats
- [ ] No errors in test output
- [ ] Confirmed read-only access (nothing modified)

---

## Example Test Session

Here's what a typical test session looks like:

```bash
$ pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v

============================== test session starts ===============================
sources/gmail/test/test_gmail_lakeflow_connect.py::test_gmail_connector

Running LakeflowConnect tests...

✅ test_initialization ................. PASSED
   OAuth authentication successful
   Access token obtained

✅ test_list_tables .................... PASSED
   Found 5 tables: messages, threads, labels, drafts, profile

✅ test_get_table_schema ............... PASSED
   All schemas valid with proper types

✅ test_read_table_metadata ............ PASSED
   Metadata correct for all tables
   CDC mode: messages, threads (with historyId cursor)
   Snapshot mode: labels, drafts, profile

✅ test_read_table ..................... PASSED
   Sample data from messages table:
   - Read 10 emails from your inbox
   - Found 5 labels
   - Detected 3 conversation threads
   - Profile: 2,347 total messages

✅ test_read_table_deletes ............. PASSED
   Skipped (not applicable for Gmail)

============================== 6 passed in 3.42s ================================
```

---

## Alternative: Testing Without Personal Account

If you prefer not to use your personal email:

### Option 1: Create Fresh Gmail Account
1. Go to https://accounts.google.com/signup
2. Create account: `your-test-name@gmail.com`
3. Send yourself 5-10 emails from another account
4. Use this account for testing

**Note**: Avoid using automated scripts to populate - Google may flag

### Option 2: Request Demo Account Access
Contact hackathon organizers for access to shared demo account.

---

## Additional Resources

- **[SETUP_GUIDE.md](./SETUP_GUIDE.md)** - Detailed OAuth setup walkthrough
- **[README.md](./README.md)** - Complete connector documentation
- **[gmail_api_doc.md](./gmail_api_doc.md)** - API research and specifications

---

## Support

**For OAuth/Setup Issues**:
1. Review [SETUP_GUIDE.md](./SETUP_GUIDE.md) troubleshooting section
2. Verify Gmail API enabled in Google Cloud Console
3. Check OAuth consent screen configuration

**For Test Failures**:
1. Ensure `dev_config.json` has valid credentials
2. Verify your Gmail account has emails
3. Check test output for specific error messages

**For Security Questions**:
- Connector only uses `gmail.readonly` scope
- No data is modified or sent externally
- You can revoke access anytime

---

**Ready to Test?** Just follow Steps 1-3 above and run the tests with your own Gmail account! The connector will work with whatever emails you already have. ✅
