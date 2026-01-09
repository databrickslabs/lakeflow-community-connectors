# Databricks notebook source
# pylint: skip-file
# MAGIC %md
# MAGIC # Zoho CRM OAuth Setup
# MAGIC
# MAGIC This notebook helps you obtain OAuth credentials for the Zoho CRM connector.
# MAGIC
# MAGIC ### Why is this needed?
# MAGIC
# MAGIC Databricks Unity Catalog doesn't provide a built-in OAuth connection type for Zoho CRM.
# MAGIC This notebook guides you through the OAuth 2.0 authorization flow to obtain a **refresh token**,
# MAGIC which you'll then use to create a Unity Catalog connection for the Lakeflow connector.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. **Create a Zoho OAuth App** at [Zoho API Console](https://api-console.zoho.com/):
# MAGIC    - Click **Add Client** → **Server-based Applications**
# MAGIC    - Set **Homepage URL** to your Databricks workspace URL
# MAGIC    - Set **Redirect URI** to: `https://<your-workspace>/login/oauth/zoho_crm.html`
# MAGIC    - Copy the **Client ID** and **Client Secret**
# MAGIC
# MAGIC 2. Fill in the variables in Cell 2, then run cells sequentially

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Enter Your Credentials
# MAGIC
# MAGIC Fill in these values from the Zoho API Console:

# COMMAND ----------

# DBTITLE 1,Configuration - Fill These In
CLIENT_ID = ""  # From Zoho API Console
CLIENT_SECRET = ""  # From Zoho API Console
REDIRECT_URI = ""  # e.g., https://your-workspace.cloud.databricks.com/login/oauth/zoho_crm.html

# Data Center - uncomment your region:
DATA_CENTER = "https://accounts.zoho.com"  # US (default)
# DATA_CENTER = "https://accounts.zoho.eu"  # EU
# DATA_CENTER = "https://accounts.zoho.in"  # India
# DATA_CENTER = "https://accounts.zoho.com.au"  # Australia
# DATA_CENTER = "https://accounts.zoho.jp"  # Japan

# OAuth Scopes (default covers all CRM data)
SCOPES = "ZohoCRM.modules.ALL,ZohoCRM.settings.ALL,ZohoCRM.users.READ"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Authorization URL
# MAGIC
# MAGIC Run this cell, then open the URL in your browser to authorize.

# COMMAND ----------

from urllib.parse import quote

if not all([CLIENT_ID, CLIENT_SECRET, REDIRECT_URI]):
    raise ValueError("Please fill in CLIENT_ID, CLIENT_SECRET, and REDIRECT_URI above")

auth_url = (
    f"{DATA_CENTER}/oauth/v2/auth"
    f"?response_type=code"
    f"&client_id={quote(CLIENT_ID)}"
    f"&scope={quote(SCOPES)}"
    f"&redirect_uri={quote(REDIRECT_URI)}"
    f"&access_type=offline"
    f"&prompt=consent"
)

print("Open this URL in your browser:\n")
print(auth_url)
print("\n" + "=" * 80)
print("After authorizing, copy the ENTIRE redirect URL and paste it below.")
print("⚠️ The code expires in 2 MINUTES!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Exchange Code for Refresh Token
# MAGIC
# MAGIC Paste the redirect URL below and run:

# COMMAND ----------

# DBTITLE 1,Paste Redirect URL Here
REDIRECT_URL_WITH_CODE = ""  # Paste the full URL from your browser

# COMMAND ----------

import requests
from urllib.parse import urlparse, parse_qs

if not REDIRECT_URL_WITH_CODE:
    raise ValueError("Please paste the redirect URL above")

# Extract code from URL
params = parse_qs(urlparse(REDIRECT_URL_WITH_CODE).query)
if "code" not in params:
    raise ValueError("No 'code' found in URL. Copy the complete redirect URL.")

AUTHORIZATION_CODE = params["code"][0]
print(f"✅ Code extracted: {AUTHORIZATION_CODE[:30]}...")

# Exchange for refresh token
response = requests.post(
    f"{DATA_CENTER}/oauth/v2/token",
    data={
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "code": AUTHORIZATION_CODE,
    },
)

if response.status_code != 200 or "refresh_token" not in response.json():
    error = response.text
    if "invalid_code" in error:
        raise ValueError("Code expired. Re-run Step 2.")
    raise ValueError(f"Failed: {error}")

tokens = response.json()
REFRESH_TOKEN = tokens["refresh_token"]
API_DOMAIN = tokens.get("api_domain", DATA_CENTER.replace("accounts.zoho", "www.zohoapis"))
print(f"✅ Refresh token obtained!")

# Test the connection
test_response = requests.post(
    f"{DATA_CENTER}/oauth/v2/token",
    data={
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    },
)

if test_response.status_code != 200 or "access_token" not in test_response.json():
    raise ValueError(f"Token refresh failed: {test_response.text}")

access_token = test_response.json()["access_token"]

# Test API
modules_response = requests.get(
    f"{API_DOMAIN}/crm/v8/settings/modules",
    headers={"Authorization": f"Zoho-oauthtoken {access_token}"},
)
if modules_response.status_code != 200:
    raise ValueError(f"API test failed: {modules_response.text[:200]}")

modules = modules_response.json().get("modules", [])
print(f"✅ Connection verified! Found {len(modules)} modules.\n")

# Print UC Connection config
import json

uc_config = {
    "client_id": CLIENT_ID[0:10] + "...",
    "client_value_tmp": CLIENT_SECRET[0:10] + "...",
    "refresh_value_tmp": REFRESH_TOKEN[0:10] + "...",
    "base_url": DATA_CENTER,
}

print("=" * 80)
print("UNITY CATALOG CONNECTION CONFIGURATION")
print("=" * 80)
print("\nUse these values when creating your UC Connection:\n")
print(json.dumps(uc_config, indent=2))
print("\n" + "=" * 80)
print("🎉 Done! Create a Unity Catalog Connection with these credentials.")

# COMMAND ----------
