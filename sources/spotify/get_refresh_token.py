#!/usr/bin/env python3
"""
Spotify OAuth Helper Script

This script helps you obtain a refresh token for the Spotify Web API.
It will:
1. Read client_id and client_secret from configs/dev_config.json
2. Open your browser to authorize the application
3. Capture the authorization code via a local callback server
4. Exchange the code for access and refresh tokens
5. Automatically update dev_config.json with the refresh token

Usage:
    python get_refresh_token.py

Prerequisites:
    1. Create a Spotify application at https://developer.spotify.com/dashboard
    2. Add http://localhost:8888/callback as a Redirect URI in your app settings
    3. Update configs/dev_config.json with your client_id and client_secret
"""

import argparse
import base64
import http.server
import json
import socketserver
import urllib.parse
import webbrowser
import requests
import sys
from pathlib import Path

# Configuration
REDIRECT_URI = "http://localhost:8888/callback"
PORT = 8888
TOKEN_URL = "https://accounts.spotify.com/api/token"
AUTH_URL = "https://accounts.spotify.com/authorize"
CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"

# Scopes needed for the Spotify connector
SCOPES = [
    "user-library-read",
    "playlist-read-private",
    "playlist-read-collaborative",
    "user-follow-read",
    "user-top-read",
    "user-read-recently-played",
    "user-read-private",
    "user-read-email",
]


class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler to capture the OAuth callback."""
    
    authorization_code: str | None = None
    error: str | None = None
    
    def do_GET(self):
        """Handle the OAuth callback GET request."""
        parsed_path = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_path.query)
        
        if "code" in query_params:
            OAuthCallbackHandler.authorization_code = query_params["code"][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"""
                <html>
                <head><title>Authorization Successful</title></head>
                <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                    <h1 style="color: #1DB954;">Authorization Successful!</h1>
                    <p>You can close this window and return to the terminal.</p>
                </body>
                </html>
            """)
        elif "error" in query_params:
            OAuthCallbackHandler.error = query_params["error"][0]
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            error_msg = query_params.get("error_description", ["Unknown error"])[0]
            self.wfile.write(f"""
                <html>
                <head><title>Authorization Failed</title></head>
                <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                    <h1 style="color: #e74c3c;">Authorization Failed</h1>
                    <p>Error: {error_msg}</p>
                    <p>Please close this window and try again.</p>
                </body>
                </html>
            """.encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        """Suppress HTTP server logging."""
        pass


def get_authorization_url(client_id: str) -> str:
    """Build the Spotify authorization URL."""
    params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": " ".join(SCOPES),
        "show_dialog": "true",
    }
    return f"{AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code_for_tokens(client_id: str, client_secret: str, code: str) -> dict:
    """Exchange the authorization code for access and refresh tokens."""
    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    
    response = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
        timeout=30,
    )
    
    if response.status_code != 200:
        raise RuntimeError(f"Token exchange failed: {response.status_code} {response.text}")
    
    return response.json()


def load_config() -> dict:
    """Load configuration from dev_config.json."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Config file not found: {CONFIG_PATH}\n"
            "Please create it with your client_id and client_secret."
        )
    
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    
    client_id = config.get("client_id", "")
    client_secret = config.get("client_secret", "")
    
    if not client_id or client_id.startswith("YOUR_"):
        raise ValueError(
            "Please set a valid 'client_id' in configs/dev_config.json\n"
            "Get yours at https://developer.spotify.com/dashboard"
        )
    
    if not client_secret or client_secret.startswith("YOUR_"):
        raise ValueError(
            "Please set a valid 'client_secret' in configs/dev_config.json\n"
            "Get yours at https://developer.spotify.com/dashboard"
        )
    
    return config


def save_refresh_token(refresh_token: str) -> None:
    """Update dev_config.json with the new refresh token."""
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    
    config["refresh_token"] = refresh_token
    
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")


def main():
    parser = argparse.ArgumentParser(
        description="Obtain a Spotify refresh token for the Lakeflow connector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script reads client_id and client_secret from:
    sources/spotify/configs/dev_config.json

After authorization, the refresh_token will be automatically saved to the config file.

Prerequisites:
    1. Create a Spotify app at https://developer.spotify.com/dashboard
    2. Add http://localhost:8888/callback as a Redirect URI
    3. Update dev_config.json with your client_id and client_secret
        """,
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't automatically save the refresh token to dev_config.json",
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 60)
    print("Spotify OAuth Refresh Token Helper")
    print("=" * 60)
    
    # Load config
    print(f"\n[1/5] Loading config from {CONFIG_PATH.name}...")
    try:
        config = load_config()
        client_id = config["client_id"]
        client_secret = config["client_secret"]
        print(f"      Client ID: {client_id[:8]}...{client_id[-4:]}")
    except (FileNotFoundError, ValueError) as e:
        print(f"\n❌ Configuration error: {e}")
        sys.exit(1)
    
    # Reset class variables
    OAuthCallbackHandler.authorization_code = None
    OAuthCallbackHandler.error = None
    
    # Start local server to capture callback
    print(f"\n[2/5] Starting local callback server on port {PORT}...")
    
    with socketserver.TCPServer(("", PORT), OAuthCallbackHandler) as httpd:
        httpd.timeout = 120  # 2 minute timeout
        
        # Build and open authorization URL
        auth_url = get_authorization_url(client_id)
        print(f"\n[3/5] Opening browser for authorization...")
        print(f"      If browser doesn't open, visit this URL:\n")
        print(f"      {auth_url}\n")
        
        webbrowser.open(auth_url)
        
        # Wait for callback
        print("[4/5] Waiting for authorization (you have 2 minutes)...")
        
        while OAuthCallbackHandler.authorization_code is None and OAuthCallbackHandler.error is None:
            httpd.handle_request()
        
        if OAuthCallbackHandler.error:
            print(f"\n❌ Authorization failed: {OAuthCallbackHandler.error}")
            sys.exit(1)
        
        code = OAuthCallbackHandler.authorization_code
        print("      Authorization code received!")
    
    # Exchange code for tokens
    print("\n[5/5] Exchanging authorization code for tokens...")
    
    try:
        tokens = exchange_code_for_tokens(client_id, client_secret, code)
    except Exception as e:
        print(f"\n❌ Token exchange failed: {e}")
        sys.exit(1)
    
    refresh_token = tokens.get("refresh_token", "")
    
    # Display results
    print("\n" + "=" * 60)
    print("✅ SUCCESS! Here are your tokens:")
    print("=" * 60)
    
    print(f"\nAccess Token (expires in {tokens.get('expires_in', '?')} seconds):")
    print(f"  {tokens.get('access_token', 'N/A')[:50]}...")
    
    print(f"\nRefresh Token (save this - it doesn't expire):")
    print("-" * 60)
    print(refresh_token)
    print("-" * 60)
    
    print(f"\nGranted Scopes:")
    print(f"  {tokens.get('scope', 'N/A')}")
    
    # Auto-save refresh token
    if not args.no_save and refresh_token:
        print("\n" + "=" * 60)
        print("Saving refresh token to config...")
        print("=" * 60)
        try:
            save_refresh_token(refresh_token)
            print(f"\n✅ Refresh token saved to {CONFIG_PATH}")
        except Exception as e:
            print(f"\n⚠️  Could not save to config: {e}")
            print("    Please manually copy the refresh token above.")
    
    print("\n" + "=" * 60)
    print("Next Steps:")
    print("=" * 60)
    print("""
Run the connector tests:
    cd /Users/matt.slack/Repos/lakeflow-community-connectors
    PYTHONPATH=. pytest sources/spotify/test/test_spotify_lakeflow_connect.py -v
""")


if __name__ == "__main__":
    main()

