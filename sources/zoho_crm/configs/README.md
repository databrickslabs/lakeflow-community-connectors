# Zoho CRM OAuth Setup Guide

This guide walks you through obtaining OAuth credentials for the Zoho CRM connector following [Zoho's official OAuth documentation](https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html).

## Quick Start

You need three things:
1. **client_id** - From Zoho API Console
2. **client_secret** - From Zoho API Console  
3. **refresh_token** - From OAuth authorization flow

## Step-by-Step Setup

### Step 1: Create OAuth Application in Zoho

1. Go to **[Zoho API Console](https://api-console.zoho.com/)**
2. Click **"Add Client"** → Select **"Server-based Applications"**
3. Fill in the application details:

| Field | Value |
|-------|-------|
| **Client Name** | `Databricks Zoho CRM Connector` |
| **Homepage URL** | Your Databricks workspace URL<br/>Example: `https://your-workspace.cloud.databricks.com/` |
| **Authorized Redirect URIs** | `https://<your-databricks-workspace>/login/oauth/zoho_crm.html`<br/>Example: `https://your-workspace.cloud.databricks.com/login/oauth/zoho_crm.html` |

4. Click **"Create"**
5. **Copy and save** your **Client ID** and **Client Secret**

---

### Step 2: Get Authorization Code

Now you'll authorize your application to access your Zoho CRM data.

#### Option A: Using Browser (Recommended)

1. **Build your authorization URL** by replacing the values below:

```
https://{accounts-server}/oauth/v2/auth?response_type=code&client_id={CLIENT_ID}&scope=ZohoCRM.modules.ALL,ZohoCRM.settings.ALL,ZohoCRM.settings.modules.ALL&redirect_uri={REDIRECT_URI}&access_type=offline&prompt=consent
```

**Replace these values:**
- `{accounts-server}` → Your data center's accounts URL (see table below)
- `{CLIENT_ID}` → Your Client ID from Step 1
- `{REDIRECT_URI}` → URL-encoded redirect URI from Step 1

**Data Center URLs:**

| Data Center | Accounts URL |
|-------------|--------------|
| **US** | `https://accounts.zoho.com` |
| **EU** | `https://accounts.zoho.eu` |
| **IN** | `https://accounts.zoho.in` |
| **AU** | `https://accounts.zoho.com.au` |
| **CN** | `https://accounts.zoho.com.cn` |
| **JP** | `https://accounts.zoho.jp` |

2. **Example URL for EU data center:**

```
https://accounts.zoho.eu/oauth/v2/auth?response_type=code&client_id=1000.YOUR_CLIENT_ID&scope=ZohoCRM.modules.ALL,ZohoCRM.settings.ALL,ZohoCRM.settings.modules.ALL&redirect_uri=https%3A%2F%2Fyour-workspace.cloud.databricks.com%2Flogin%2Foauth%2Fzoho_crm.html&access_type=offline&prompt=consent
```

3. **Open the URL in your browser**
4. **Log in** to your Zoho account
5. **Review and accept** the permissions
6. **Copy the authorization code** from the redirect URL

After authorization, you'll be redirected to a URL like:
```
https://your-workspace.cloud.databricks.com/login/oauth/zoho_crm.html?code=1000.abc123...&location=eu&accounts-server=https://accounts.zoho.eu
```

**⚠️ IMPORTANT:** The `code` parameter is your **authorization code** - it's valid for **2 minutes only** according to [Zoho's documentation](https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html)!

Copy the entire code value immediately (everything after `code=` and before the next `&`).

#### Option B: Using Zoho API Console (Alternative)

1. In [Zoho API Console](https://api-console.zoho.com/), find your application
2. Click **"Generate Code"**
3. Enter scopes: `ZohoCRM.modules.ALL,ZohoCRM.settings.ALL,ZohoCRM.settings.modules.ALL`
4. Set time duration: **3 minutes**
5. Click **"Create"**
6. **Copy the code immediately** - it expires in 2-3 minutes!

---

### Step 3: Exchange Authorization Code for Tokens

⏰ **Do this within 2 minutes of getting the authorization code!**

Run this curl command (replace the values in `{}`):

```bash
curl -X POST "https://{accounts-server}/oauth/v2/token" \
  -d "grant_type=authorization_code" \
  -d "client_id={CLIENT_ID}" \
  -d "client_secret={CLIENT_SECRET}" \
  -d "redirect_uri={REDIRECT_URI}" \
  -d "code={AUTHORIZATION_CODE}"
```

**Example for EU data center:**

```bash
curl -X POST "https://accounts.zoho.eu/oauth/v2/token" \
  -d "grant_type=authorization_code" \
  -d "client_id=1000.YOUR_CLIENT_ID" \
  -d "client_secret=YOUR_CLIENT_SECRET" \
  -d "redirect_uri=https://your-workspace.cloud.databricks.com/login/oauth/zoho_crm.html" \
  -d "code=1000.abc123def456..." 
```

**Successful Response:**

```json
{
  "access_token": "1000.abc123...",
  "refresh_token": "1000.def456...",
  "api_domain": "https://www.zohoapis.eu",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

**🎉 Copy the `refresh_token` value - this is what you need for the connector!**

The refresh token never expires and will be used by the connector to automatically obtain access tokens.

---

### Step 4: Configure the Connector

Add your credentials to `dev_config.json`:

```json
{
  "client_id": "1000.YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET",
  "refresh_token": "1000.YOUR_REFRESH_TOKEN_HERE",
  "redirect_uri": "https://your-workspace.cloud.databricks.com/login/oauth/zoho_crm.html",
  "base_url": "https://www.zohoapis.eu",
  "initial_load_start_date": "2024-01-01T00:00:00Z"
}
```

**Done!** The connector will automatically use the refresh token to obtain and refresh access tokens as needed.

---

## Troubleshooting

### Error: `invalid_code`

**Causes:**
- Authorization code expired (>2 minutes old)
- Authorization code already used (can only be used once)
- Wrong authorization code

**Solution:** Generate a new authorization code and exchange it immediately.

### Error: `invalid_client`

**Causes:**
- Wrong Client ID or Client Secret
- Data center mismatch (app registered in US but using EU accounts URL)

**Solution:** 
- Verify your credentials in [Zoho API Console](https://api-console.zoho.com/)
- Use the correct accounts URL for your data center

### Error: `invalid_redirect_uri`

**Cause:** Redirect URI doesn't match the one registered in Zoho API Console

**Solution:** Use the exact redirect URI from your Zoho application settings.

### Authorization code expires too quickly

According to [Zoho's documentation](https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html), authorization codes are valid for **2 minutes only**. 

**Tips:**
- Have your curl command ready before generating the code
- Copy the code quickly from the redirect URL
- Run the curl command immediately

---

## Configuration Reference

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `client_id` | OAuth Client ID from Zoho API Console | `1000.XXX...` |
| `client_secret` | OAuth Client Secret from Zoho API Console | `abc123...` |
| `refresh_token` | Refresh token from OAuth flow (never expires) | `1000.YYY...` |

### Optional Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `redirect_uri` | OAuth redirect URI (only needed if using connector's OAuth helpers) | - |
| `base_url` | Zoho CRM API base URL for your data center | `https://www.zohoapis.com` |
| `initial_load_start_date` | Starting point for the first sync. If omitted, syncs all historical data. | - |

### Base URLs by Data Center

**Production:**
- US: `https://www.zohoapis.com`
- EU: `https://www.zohoapis.eu`
- IN: `https://www.zohoapis.in`
- AU: `https://www.zohoapis.com.au`
- CN: `https://www.zohoapis.com.cn`
- JP: `https://www.zohoapis.jp`

**Sandbox:**
- US: `https://sandbox.zohoapis.com`
- EU: `https://sandbox.zohoapis.eu`
- IN: `https://sandbox.zohoapis.in`
- AU: `https://sandbox.zohoapis.com.au`
- CN: `https://sandbox.zohoapis.com.cn`
- JP: `https://sandbox.zohoapis.jp`

**Developer:**
- US: `https://developer.zohoapis.com`
- EU: `https://developer.zohoapis.eu`
- IN: `https://developer.zohoapis.in`
- AU: `https://developer.zohoapis.com.au`
- CN: `https://developer.zohoapis.com.cn`
- JP: `https://developer.zohoapis.jp`

---

## OAuth Scopes Explained

The connector requires these scopes:

| Scope | Purpose |
|-------|---------|
| `ZohoCRM.modules.ALL` | Read/write access to all CRM modules (Leads, Contacts, etc.) |
| `ZohoCRM.settings.ALL` | Access to CRM settings and configuration |
| `ZohoCRM.settings.modules.ALL` | Access to module metadata for dynamic schema discovery |

These scopes enable the connector to:
- List available modules
- Discover schemas dynamically
- Read and sync data from all modules
- Handle custom modules

---

## Security Notes

⚠️ **Keep your credentials secure:**

- Never commit `dev_config.json` to version control (it's in `.gitignore`)
- Store credentials in secure secret management systems for production
- Rotate Client Secrets periodically
- Refresh tokens don't expire but can be revoked in Zoho API Console

---

## References

- [Zoho OAuth Authorization Documentation](https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html)
- [Zoho CRM API v8 Documentation](https://www.zoho.com/crm/developer/docs/api/v8/)
- [Zoho API Console](https://api-console.zoho.com/)
- [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)
