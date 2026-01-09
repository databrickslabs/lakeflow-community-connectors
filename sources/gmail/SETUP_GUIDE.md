# Gmail Connector Setup Guide

This guide will help you set up OAuth credentials for testing the Gmail connector.

## Quick Setup (5 minutes)

### Step 1: Get OAuth Credentials from Google Cloud

1. **Go to Google Cloud Console**
   - Visit: https://console.cloud.google.com/
   - Sign in with your Google account

2. **Create a Project** (if you don't have one)
   - Click "Select a project" → "NEW PROJECT"
   - Name: "gmail-connector-test"
   - Click "CREATE"

3. **Enable Gmail API**
   - Go to: https://console.cloud.google.com/apis/library/gmail.googleapis.com
   - Click "ENABLE"

4. **Configure OAuth Consent Screen**
   - Go to: https://console.cloud.google.com/apis/credentials/consent
   - Choose "External" → "CREATE"
   - Fill in:
     - App name: "Gmail Connector Test"
     - User support email: (your email)
     - Developer contact: (your email)
   - Click "SAVE AND CONTINUE"
   - On Scopes page: "ADD OR REMOVE SCOPES"
     - Search for: `gmail.readonly`
     - Check the box
     - Click "UPDATE" → "SAVE AND CONTINUE"
   - On Test users: "ADD USERS"
     - Add your Gmail address
     - Click "SAVE AND CONTINUE"
   - Click "BACK TO DASHBOARD"

5. **Create OAuth Client**
   - Go to: https://console.cloud.google.com/apis/credentials
   - Click "+ CREATE CREDENTIALS" → "OAuth client ID"
   - Application type: "Desktop app"
   - Name: "Gmail Connector"
   - Click "CREATE"
   - Copy the **Client ID** and **Client Secret** (you'll need these next)

### Step 2: Run Setup Script

```bash
cd /Users/fatine.boujnouni/Documents/labs/Lakeflow_Connector/lakeflow-community-connectors

# Install required package
pip install google-auth-oauthlib

# Run the setup script
python sources/gmail/get_refresh_token.py
```

The script will:
1. Ask for your Client ID and Client Secret
2. Open a browser for you to authorize
3. Create the config file automatically

### Step 3: Run Tests

```bash
pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v
```

---

## Alternative: Use Pre-configured Test Credentials (Fastest)

If someone on your team has already set up OAuth credentials, they can share:
- Client ID
- Client Secret
- Refresh Token

Just create `sources/gmail/configs/dev_config.json`:

```json
{
  "client_id": "SHARED_CLIENT_ID",
  "client_secret": "SHARED_CLIENT_SECRET",
  "refresh_token": "SHARED_REFRESH_TOKEN",
  "user_id": "me"
}
```

**Note:** The refresh token will access **their** Gmail account, not yours.

---

## Why OAuth Instead of API Keys?

Gmail API **requires** OAuth 2.0 because:

1. **Security**: Emails are private data
2. **User consent**: You need to authorize what the app can access
3. **Revocable**: You can revoke access anytime
4. **Scoped access**: Can restrict to read-only, send-only, etc.

API keys can't provide these protections for private data.

### Alternative for Google Workspace Users

If you have **Google Workspace** (not personal Gmail), you can use **Service Accounts** which don't require browser authorization:

1. Create a service account
2. Enable domain-wide delegation
3. Use service account key file

Would you like instructions for this method?

---

## Troubleshooting

### Error: "redirect_uri_mismatch"
- This is expected with the script - it handles it automatically
- If using OAuth Playground, add redirect URI in OAuth client settings

### Error: "Access blocked: This app's request is invalid"
- Make sure you added yourself as a test user in OAuth consent screen
- Verify Gmail API is enabled

### Error: "invalid_client"
- Double-check your Client ID and Client Secret
- Make sure you're using Desktop app credentials (not Web app)

---

## After Testing - Clean Up

⚠️ **IMPORTANT**: Delete config files before committing:

```bash
rm sources/gmail/configs/dev_config.json
rm sources/gmail/configs/dev_table_config.json
```

These files contain sensitive credentials and should never be in Git!
