import msal
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# --- Configuration ---

CLIENT_SECRET = ''
CLIENT_ID = ''
TENANT_ID = ''

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
# Scopes for Outlook Mail API - include Mail.Read and offline_access for refresh token
SCOPES = ["Mail.Read"]
REDIRECT_PORT = 8080
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}"

class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"Authentication complete! You can close this window.")
        
        # Extract the code from the URL
        query = urlparse(self.path).query
        self.server.auth_code = parse_qs(query).get("code", [None])[0]

def get_refresh_token():
    # Use ConfidentialClientApplication for web apps with client secret
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )

    # 1. Build the authorization URL
    auth_url = app.get_authorization_request_url(SCOPES, redirect_uri=REDIRECT_URI)
    
    # 2. Open browser for user login
    print(f"Opening browser to: {auth_url}")
    webbrowser.open(auth_url)

    # 3. Start a local server to catch the redirect code
    server = HTTPServer(('localhost', REDIRECT_PORT), OAuthHandler)
    print(f"\nWaiting for authentication callback on {REDIRECT_URI}...")
    server.handle_request()
    
    if not server.auth_code:
        print("Failed to acquire authorization code.")
        return

    # 4. Exchange the code for tokens
    print("\nExchanging authorization code for tokens...")
    result = app.acquire_token_by_authorization_code(
        server.auth_code,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI
    )

    if "refresh_token" in result:
        print("\n" + "="*60)
        print("SUCCESS! Tokens acquired successfully")
        print("="*60)
        print(f"\nAccess Token: {result['access_token'][:50]}...")
        print(f"Refresh Token: {result['refresh_token']}")
        print(f"Expires in: {result.get('expires_in')} seconds")
        print("\n" + "="*60)
        print("Copy the refresh_token above to your dev_config.json")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("ERROR acquiring token")
        print("="*60)
        print(f"Error: {result.get('error')}")
        print(f"Description: {result.get('error_description')}")
        print(f"Correlation ID: {result.get('correlation_id')}")
        print("="*60)

if __name__ == "__main__":
    get_refresh_token()