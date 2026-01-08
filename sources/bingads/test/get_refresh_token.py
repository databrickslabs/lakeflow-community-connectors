# Get Refresh Token for Bing Ads Sandbox
# Run this ONCE to obtain your refresh token
#
# Prerequisites:
# 1. Register an app in Azure Portal: https://portal.azure.com/
#    - Microsoft Entra ID → App registrations → New registration
#    - Supported account types: "Accounts in any organizational directory and personal Microsoft accounts"
#    - Redirect URI: Public client → https://login.microsoftonline.com/common/oauth2/nativeclient
# 2. Copy the Application (client) ID and paste below
#
# Reference: https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth?view=bingads-13

import webbrowser
from bingads.authorization import OAuthDesktopMobileAuthCodeGrant

CLIENT_ID = "1d0b0336-4189-46ce-bf2c-a1caf4c00e0b"  # Replace with your Application (client) ID from Azure Portal

# NOTE: Use production OAuth (no env parameter) - the token works for both prod and sandbox API calls
auth = OAuthDesktopMobileAuthCodeGrant(client_id=CLIENT_ID, env="sandbox")

print("Opening browser for Microsoft sign-in...")
print("Sign in with your Microsoft Advertising sandbox account.\n")
# print(auth.get_authorization_endpoint())
webbrowser.open(auth.get_authorization_endpoint())

print("After signing in, you'll be redirected to a blank page.")
print("Copy the FULL URL from your browser's address bar and paste it here:")
redirect_url = input("> ")

auth.request_oauth_tokens_by_response_uri(redirect_url)

print("\n" + "=" * 60)
print("SUCCESS! Your refresh token:")
print("=" * 60)
print(auth.oauth_tokens.refresh_token)
print("=" * 60)
print("\nCopy this token and set REFRESH_TOKEN in test_bingads_sandbox.py")

