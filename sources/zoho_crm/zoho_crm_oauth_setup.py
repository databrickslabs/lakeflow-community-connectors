# Databricks notebook source
# MAGIC %md
# MAGIC # Zoho CRM OAuth Setup
# MAGIC
# MAGIC This notebook guides you through obtaining OAuth credentials for the Zoho CRM connector.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - A Zoho CRM account
# MAGIC - A registered OAuth application in [Zoho API Console](https://api-console.zoho.com/)
# MAGIC
# MAGIC **What you'll get:**
# MAGIC - `refresh_token` - Long-lived token that never expires (the connector uses this to authenticate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Your OAuth Credentials
# MAGIC
# MAGIC Fill in your credentials from the Zoho API Console below.
# MAGIC
# MAGIC > **Don't have credentials yet?**
# MAGIC > 1. Go to [Zoho API Console](https://api-console.zoho.com/)
# MAGIC > 2. Click **"Add Client"** → **"Server-based Applications"**
# MAGIC > 3. Set **Homepage URL** to your Databricks workspace URL
# MAGIC > 4. Set **Redirect URI** to: `https://<your-workspace>/login/oauth/zoho_crm.html`
# MAGIC > 5. Click **Create** and copy the Client ID and Secret

# COMMAND ----------

# DBTITLE 1,Enter Your OAuth Credentials
# Your OAuth Client ID from Zoho API Console
CLIENT_ID = ""  # e.g., "1000.XXXXX..."

# Your OAuth Client Secret from Zoho API Console
CLIENT_SECRET = ""  # e.g., "abc123..."

# Your redirect URI (must match what's registered in Zoho API Console)
REDIRECT_URI = ""  # e.g., "https://your-workspace.cloud.databricks.com/login/oauth/zoho_crm.html"

# Your Zoho data center (choose one)
# US: "https://accounts.zoho.com"
# EU: "https://accounts.zoho.eu"
# IN: "https://accounts.zoho.in"
# AU: "https://accounts.zoho.com.au"
# CN: "https://accounts.zoho.com.cn"
# JP: "https://accounts.zoho.jp"
DATA_CENTER = "https://accounts.zoho.com"  # Change this to your data center

# COMMAND ----------

# DBTITLE 1,Validate Configuration
# Validate that all required fields are filled
errors = []
if not CLIENT_ID:
    errors.append("CLIENT_ID is required")
if not CLIENT_SECRET:
    errors.append("CLIENT_SECRET is required")
if not REDIRECT_URI:
    errors.append("REDIRECT_URI is required")
if not DATA_CENTER:
    errors.append("DATA_CENTER is required")

if errors:
    print("❌ Configuration errors:")
    for error in errors:
        print(f"   - {error}")
    raise ValueError("Please fill in all required configuration fields above")
else:
    print("✅ Configuration validated successfully!")
    print(f"   Client ID: {CLIENT_ID[:20]}...")
    print(f"   Redirect URI: {REDIRECT_URI}")
    print(f"   Data Center: {DATA_CENTER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Authorization URL
# MAGIC
# MAGIC Run the cell below to generate the authorization URL. You'll need to:
# MAGIC 1. Copy the generated URL
# MAGIC 2. Open it in a new browser tab
# MAGIC 3. Log in to your Zoho account
# MAGIC 4. Authorize the application
# MAGIC 5. Copy the authorization code from the redirect URL

# COMMAND ----------

# DBTITLE 1,Generate Authorization URL
from urllib.parse import quote

# Required scopes for Zoho CRM connector
SCOPES = "ZohoCRM.modules.ALL,ZohoCRM.settings.ALL,ZohoCRM.settings.modules.ALL"

# Build the authorization URL
auth_url = (
    f"{DATA_CENTER}/oauth/v2/auth"
    f"?response_type=code"
    f"&client_id={quote(CLIENT_ID)}"
    f"&scope={quote(SCOPES)}"
    f"&redirect_uri={quote(REDIRECT_URI)}"
    f"&access_type=offline"
    f"&prompt=consent"
)

print("=" * 80)
print("📋 AUTHORIZATION URL")
print("=" * 80)
print()
print("1. Copy the URL below and open it in a new browser tab:")
print()
print(auth_url)
print()
print("=" * 80)
print()
print("2. Log in to your Zoho account and authorize the application")
print()
print("3. After authorization, you'll be redirected to a URL like:")
print(f"   {REDIRECT_URI}?code=1000.abc123...&location=us")
print()
print("4. Copy the ENTIRE redirect URL from your browser's address bar")
print()
print("⚠️  IMPORTANT: The authorization code expires in 2 MINUTES!")
print("   Paste the URL in the next step immediately!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Exchange Authorization Code for Refresh Token
# MAGIC
# MAGIC After authorizing in the browser, you'll be redirected to a URL like:
# MAGIC ```
# MAGIC https://your-workspace.com/login/oauth/zoho_crm.html?code=1000.abc123...&location=us
# MAGIC ```
# MAGIC
# MAGIC **Paste the ENTIRE redirect URL below** - we'll extract the code automatically!
# MAGIC
# MAGIC > ⏰ **Time Limit:** You have **2 minutes** from when you were redirected!

# COMMAND ----------

# DBTITLE 1,Paste the Redirect URL
# Paste the ENTIRE redirect URL here (we'll extract the code automatically)
REDIRECT_URL_WITH_CODE = ""  # e.g., "https://your-workspace.com/login/oauth/zoho_crm.html?code=1000.abc123...&location=us"

# COMMAND ----------

# DBTITLE 1,Extract Code and Get Refresh Token
import requests
import json
from urllib.parse import urlparse, parse_qs

if not REDIRECT_URL_WITH_CODE:
    print("❌ Please paste the redirect URL in the cell above")
    raise ValueError("REDIRECT_URL_WITH_CODE is required")

# Extract the authorization code from the URL
print("🔍 Extracting authorization code from URL...")
print()

try:
    parsed_url = urlparse(REDIRECT_URL_WITH_CODE)
    query_params = parse_qs(parsed_url.query)

    if "code" not in query_params:
        print("❌ Could not find 'code' parameter in the URL")
        print(f"   URL provided: {REDIRECT_URL_WITH_CODE[:100]}...")
        print()
        print("💡 Make sure you copied the complete redirect URL that includes '?code=...'")
        raise ValueError("No 'code' parameter found in URL")

    AUTHORIZATION_CODE = query_params["code"][0]
    print(f"✅ Authorization code extracted successfully!")
    print(f"   Code: {AUTHORIZATION_CODE[:30]}...")
    print()

    # Also extract location if available
    if "location" in query_params:
        print(f"   Location: {query_params['location'][0]}")
    if "accounts-server" in query_params:
        print(f"   Accounts Server: {query_params['accounts-server'][0]}")
    print()

except Exception as e:
    print(f"❌ Error parsing URL: {e}")
    print()
    print("💡 You can also paste just the code value directly:")
    print("   AUTHORIZATION_CODE = '1000.abc123...'")
    raise

if not AUTHORIZATION_CODE:
    print("❌ Failed to extract authorization code")
    raise ValueError("AUTHORIZATION_CODE is required")

print("🔄 Exchanging authorization code for refresh token...")
print()

# Make the token exchange request
token_url = f"{DATA_CENTER}/oauth/v2/token"

data = {
    "grant_type": "authorization_code",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "redirect_uri": REDIRECT_URI,
    "code": AUTHORIZATION_CODE,
}

response = requests.post(token_url, data=data)

if response.status_code == 200:
    tokens = response.json()

    if "refresh_token" in tokens:
        print("=" * 80)
        print("🎉 SUCCESS! Refresh token obtained!")
        print("=" * 80)
        print()
        print(f"📌 API DOMAIN: {tokens.get('api_domain', 'https://www.zohoapis.com')}")
        print()
        print("The refresh token will be saved to your configuration in the next step.")
        print()

        # Store for use in next steps
        REFRESH_TOKEN = tokens["refresh_token"]
        API_DOMAIN = tokens.get("api_domain", "https://www.zohoapis.com")
    else:
        print("❌ Error: Token response missing refresh_token")
        print(f"Response: {json.dumps(tokens, indent=2)}")
        raise ValueError("Invalid token response")
else:
    error_response = response.text
    print("❌ Error exchanging code for refresh token!")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {error_response}")
    print()

    if "invalid_code" in error_response:
        print("💡 The authorization code has expired or already been used.")
        print("   Go back to Step 2 and generate a new authorization URL.")
    elif "invalid_client" in error_response:
        print("💡 Check your CLIENT_ID and CLIENT_SECRET values.")
    elif "invalid_redirect_uri" in error_response:
        print("💡 The REDIRECT_URI doesn't match what's registered in Zoho API Console.")

    raise ValueError("Token exchange failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Connection
# MAGIC
# MAGIC Let's verify that your credentials work by testing the token refresh and making a simple API call.

# COMMAND ----------

# DBTITLE 1,Test Connection
import requests

print("🔄 Testing connection to Zoho CRM...")
print()

# Test 1: Refresh the token
print("1️⃣ Testing token refresh...")
token_url = f"{DATA_CENTER}/oauth/v2/token"

refresh_response = requests.post(
    token_url, data={"refresh_token": REFRESH_TOKEN, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "grant_type": "refresh_token"}
)

if refresh_response.status_code != 200 or "access_token" not in refresh_response.json():
    print(f"❌ Token refresh failed: {refresh_response.text}")
    raise ValueError("Token refresh failed - please check your credentials and try again from Step 2")

access_token = refresh_response.json()["access_token"]
print("   ✅ Token refresh successful!")
print()

# Test 2: List modules from CRM API
print("2️⃣ Testing CRM API access...")

# Derive API URL from accounts URL
api_domain = API_DOMAIN if "API_DOMAIN" in dir() else DATA_CENTER.replace("accounts.zoho", "www.zohoapis")

modules_response = requests.get(f"{api_domain}/crm/v8/settings/modules", headers={"Authorization": f"Zoho-oauthtoken {access_token}"})

if modules_response.status_code == 200:
    modules = modules_response.json().get("modules", [])
    print(f"   ✅ API access successful! Found {len(modules)} modules.")
    print()
    print("   Sample modules:")
    for module in modules[:5]:
        print(f"      - {module.get('api_name', 'Unknown')}")
    if len(modules) > 5:
        print(f"      ... and {len(modules) - 5} more")
else:
    print(f"❌ API call failed with status {modules_response.status_code}")
    print(f"   Response: {modules_response.text[:200]}")
    raise ValueError("API verification failed - please check your credentials")

print()
print("=" * 80)
print("🎉 CONNECTION VERIFIED SUCCESSFULLY!")
print("=" * 80)
print()
print("Proceed to Step 5 to save your configuration.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Save Your Configuration
# MAGIC
# MAGIC Now that your connection is verified, let's save the configuration.
# MAGIC
# MAGIC **Security Recommendation:** For production, store sensitive values in Databricks Secrets.

# COMMAND ----------

# DBTITLE 1,Save Configuration to dev_config.json
import os

# Build the configuration
# base_url is the accounts/OAuth URL (DATA_CENTER), not the API domain
config = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "refresh_token": REFRESH_TOKEN,
    "redirect_uri": REDIRECT_URI,
    "base_url": DATA_CENTER,  # e.g., https://accounts.zoho.eu
    "initial_load_start_date": "2024-01-01T00:00:00Z",  # Optional: omit to sync all historical data
}

# Determine the config file path
# When running in Databricks, use the notebook's directory
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # For Databricks repos, construct the path to configs/dev_config.json
    config_path = "/Workspace" + os.path.dirname(notebook_path) + "/configs/dev_config.json"
except:
    # Fallback for local development
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", "dev_config.json")

print("=" * 80)
print("📋 SAVING ZOHO CRM CONNECTOR CONFIGURATION")
print("=" * 80)
print()

# Display the configuration (with masked secrets)
display_config = config.copy()
display_config["client_secret"] = config["client_secret"][:10] + "..." if config["client_secret"] else ""
display_config["refresh_token"] = config["refresh_token"][:20] + "..." if config["refresh_token"] else ""
print("Configuration:")
print()
print(json.dumps(display_config, indent=2))
print()

# Try to save the configuration file
try:
    # Ensure the directory exists
    os.makedirs(os.path.dirname(config_path), exist_ok=True)

    with open(config_path, "w") as f:
        json.dump(config, f, indent=4)

    print("=" * 80)
    print(f"✅ Configuration saved to: {config_path}")
    print("=" * 80)
except Exception as e:
    print("=" * 80)
    print(f"⚠️  Could not save to file: {e}")
    print("=" * 80)
    print()
    print("Please manually create configs/dev_config.json with the following content:")
    print()
    print(json.dumps(config, indent=4))

print()
print("💡 TIP: For production, use Databricks Secrets instead of storing credentials in files:")
print()
print('   client_id = dbutils.secrets.get(scope="zoho-crm", key="client_id")')
print('   client_secret = dbutils.secrets.get(scope="zoho-crm", key="client_secret")')
print('   refresh_token = dbutils.secrets.get(scope="zoho-crm", key="refresh_token")')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC Your Zoho CRM connector is now configured and verified. The connector will automatically handle authentication using the refresh token.
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. ✅ (Optional) Store credentials in Databricks Secrets for production use
# MAGIC 2. ✅ Configure your Lakeflow pipeline with the Zoho CRM connector
# MAGIC 3. ✅ Start ingesting data!
# MAGIC
# MAGIC ### Example Usage
# MAGIC ```python
# MAGIC # Using the connector with Lakeflow
# MAGIC from zoho_crm import LakeflowConnect
# MAGIC
# MAGIC connector = LakeflowConnect(options={
# MAGIC     "client_id": dbutils.secrets.get("zoho-crm", "client_id"),
# MAGIC     "client_secret": dbutils.secrets.get("zoho-crm", "client_secret"),
# MAGIC     "refresh_token": dbutils.secrets.get("zoho-crm", "refresh_token"),
# MAGIC     "base_url": "https://accounts.zoho.eu",  # Your data center
# MAGIC })
# MAGIC
# MAGIC # List available tables
# MAGIC tables = connector.list_tables()
# MAGIC print(f"Available tables: {[t['name'] for t in tables]}")
# MAGIC ```
