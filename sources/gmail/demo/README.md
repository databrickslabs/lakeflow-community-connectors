# Gmail Connector Demo Data Scripts

This directory contains scripts to populate the demo Gmail account with realistic data for demonstrating the connector's capabilities.

## Demo Account

**Email**: DEMO_EMAIL_PLACEHOLDER
**Password**: DiscoverLakehouse

## Scripts Overview

### 1. `populate_demo_data.py` - Initial Data Population

Creates comprehensive demo data including:
- **3 customer support conversation threads** (demonstrating email threads with multiple replies)
- **4 sales-related emails** (prospecting, follow-ups, welcome emails, renewals)
- **4 system notification emails** (alerts, reports, deployment notifications)
- **3 marketing emails** (feature announcements, case studies, promotions)
- **5 draft messages** (with TODO placeholders)
- **Custom Gmail labels** (Customer Support, Sales, Notifications, Marketing)

**Total**: ~20 emails demonstrating various connector capabilities

### 2. `add_incremental_data.py` - Incremental Data for CDC Demo

Adds new emails to demonstrate incremental sync:
- **3 new customer support tickets**
- **2 sales update emails**
- **3 new system notifications**
- **1 product update email**

**Total**: ~9 new emails

Use this AFTER initial ingestion to demonstrate Change Data Capture (CDC) with Gmail's History API.

## Prerequisites

Install required dependencies:

```bash
pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client
```

## Setup Instructions

### Step 1: Create OAuth Credentials

You need OAuth credentials with **send** permission (not just read-only):

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create or select your project
3. Enable Gmail API
4. Create OAuth 2.0 credentials (Desktop app type)
5. Configure OAuth consent screen with these scopes:
   - `https://www.googleapis.com/auth/gmail.readonly`
   - `https://www.googleapis.com/auth/gmail.send`
   - `https://www.googleapis.com/auth/gmail.modify`
   - `https://www.googleapis.com/auth/gmail.labels`

### Step 2: Populate Initial Demo Data

```bash
cd /Users/fatine.boujnouni/Documents/labs/Lakeflow_Connector/lakeflow-community-connectors

# Run the population script
python sources/gmail/demo/populate_demo_data.py
```

The script will:
1. Prompt for OAuth Client ID and Client Secret
2. Open browser for authentication
3. Create demo emails and labels
4. Save credentials to `sources/gmail/demo/token.json` for reuse

**Time**: ~2-3 minutes to populate all data

### Step 3: Test the Connector Locally (Optional)

```bash
# Set up connector config (use read-only credentials)
python sources/gmail/get_refresh_token.py

# Run tests
pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v
```

### Step 4: Ingest into Databricks

Use the community connector CLI or manually configure Delta Live Tables pipeline.

### Step 5: Add Incremental Data

After initial ingestion, demonstrate CDC:

```bash
python sources/gmail/demo/add_incremental_data.py
```

This adds new emails. When you run incremental ingestion, only these new emails will be synced (efficient CDC using historyId cursor).

## What Gets Demonstrated

### Email Features
- ✅ Plain text and HTML email bodies
- ✅ Nested message structures (parts, headers, attachments metadata)
- ✅ Email threads (conversation tracking)
- ✅ Custom labels
- ✅ Draft messages
- ✅ Various email types (support, sales, notifications, marketing)

### Connector Capabilities
- ✅ **Full Sync**: Initial load of all messages
- ✅ **Incremental Sync (CDC)**: Using Gmail History API with historyId cursor
- ✅ **Snapshot Mode**: For labels, profile, drafts
- ✅ **Schema Handling**: Rich nested structures with arrays
- ✅ **Label Filtering**: Custom labels for organization

### Use Cases for Demo
1. **Customer Support Analytics**
   - Track response times
   - Analyze support ticket volumes
   - Sentiment analysis on support emails

2. **Sales Intelligence**
   - Pipeline tracking from email communications
   - Customer engagement analysis
   - Follow-up automation insights

3. **System Monitoring**
   - Alert aggregation and analysis
   - Deployment tracking
   - Performance metrics from email reports

4. **Compliance & eDiscovery**
   - Email retention and archival
   - Search and filtering capabilities
   - Audit trails

## Demo Script for Judges

### Part 1: Initial Setup (2 minutes)
```bash
# Show the connector code
cat sources/gmail/gmail.py | head -50

# Show the schemas
grep -A 20 "def _get_messages_schema" sources/gmail/gmail.py
```

### Part 2: Populate Demo Data (3 minutes)
```bash
# Run population script
python sources/gmail/demo/populate_demo_data.py

# Show what was created
python -c "
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
creds = Credentials.from_authorized_user_file('sources/gmail/demo/token.json')
service = build('gmail', 'v1', credentials=creds)
profile = service.users().getProfile(userId='me').execute()
print(f'Total messages: {profile[\"messagesTotal\"]}')
print(f'Total threads: {profile[\"threadsTotal\"]}')
print(f'Current historyId: {profile[\"historyId\"]}')
"
```

### Part 3: Test Connector (2 minutes)
```bash
# Run tests
pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v

# Should see: 6 tests passed
```

### Part 4: Show Data in Databricks (5 minutes)
- Configure and run pipeline
- Show tables in Unity Catalog
- Query the data with SQL
- Show nested structures

### Part 5: Demonstrate CDC (3 minutes)
```bash
# Add incremental data
python sources/gmail/demo/add_incremental_data.py

# Show historyId changed
# Re-run incremental ingestion
# Show only new records were synced (efficient!)
```

## Troubleshooting

### "Access blocked" error
- Make sure you added test users in OAuth consent screen
- Use the demo account email as a test user

### "Invalid credentials" error
- Re-run authentication flow
- Delete `token.json` and try again

### Rate limiting
- The scripts include 0.5s delays between API calls
- Gmail API allows 1.2M quota units/minute
- Sending ~30 emails well within limits

### "Label already exists"
- Scripts handle this gracefully
- Will reuse existing labels

## Files Generated

- `sources/gmail/demo/token.json` - OAuth credentials (for sending emails)
- `sources/gmail/configs/dev_config.json` - Connector config (for reading emails)

**⚠️ Important**: Never commit these files to Git! They contain sensitive credentials.

## Cleanup

To remove demo data from Gmail account:
1. Select all emails in Gmail web interface
2. Delete them
3. Empty trash

Or keep them for future demos!

## Questions?

Contact the connector developer or refer to:
- `sources/gmail/README.md` - Connector documentation
- `sources/gmail/SETUP_GUIDE.md` - OAuth setup guide
- `sources/gmail/gmail_api_doc.md` - API research documentation
