#!/usr/bin/env python3
"""
Script to obtain Gmail API refresh token for testing.
This script will open a browser window for you to authorize access.

SIMPLIFIED SETUP:
1. Just run this script: python sources/gmail/get_refresh_token.py
2. Paste your Client ID and Client Secret
3. Authorize in the browser
4. Done! Config file created automatically.
"""

import json
import sys
from google_auth_oauthlib.flow import InstalledAppFlow

# Gmail API scope for read-only access
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def get_refresh_token(client_id: str, client_secret: str) -> str:
    """
    Obtain a refresh token using OAuth 2.0 flow.

    Args:
        client_id: OAuth 2.0 client ID
        client_secret: OAuth 2.0 client secret

    Returns:
        Refresh token string
    """
    # Create client config
    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost", "urn:ietf:wg:oauth:2.0:oob"]
        }
    }

    # Run OAuth flow
    flow = InstalledAppFlow.from_client_config(
        client_config,
        scopes=SCOPES
    )

    # This will open a browser window for authorization
    credentials = flow.run_local_server(
        port=0,
        authorization_prompt_message='Please visit this URL to authorize the application: {url}',
        success_message='Authorization successful! You can close this window.',
        open_browser=True
    )

    return credentials.refresh_token


def main():
    print("=" * 60)
    print("Gmail API Refresh Token Generator")
    print("=" * 60)
    print()

    # Get client ID and secret from user
    print("Please enter your OAuth 2.0 credentials:")
    print("(You can find these in Google Cloud Console > APIs & Services > Credentials)")
    print()

    client_id = input("Client ID: ").strip()
    client_secret = input("Client Secret: ").strip()

    if not client_id or not client_secret:
        print("\nError: Client ID and Client Secret are required!")
        return

    print("\n" + "=" * 60)
    print("Starting OAuth authorization flow...")
    print("A browser window will open for you to authorize access.")
    print("=" * 60)
    print()

    try:
        refresh_token = get_refresh_token(client_id, client_secret)

        print("\n" + "=" * 60)
        print("SUCCESS! Refresh token obtained:")
        print("=" * 60)
        print()
        print(refresh_token)
        print()

        # Create the config file
        config = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "user_id": "me"
        }

        config_path = "sources/gmail/configs/dev_config.json"
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        print("=" * 60)
        print(f"Config file created: {config_path}")
        print("=" * 60)
        print()
        print("You can now run the tests with:")
        print("pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v")
        print()

    except Exception as e:
        print("\n" + "=" * 60)
        print("ERROR:")
        print("=" * 60)
        print(f"\n{str(e)}")
        print()
        print("Common issues:")
        print("- Make sure Gmail API is enabled in your project")
        print("- Verify your client ID and secret are correct")
        print("- Check that your OAuth consent screen is configured")
        print()


if __name__ == "__main__":
    main()
