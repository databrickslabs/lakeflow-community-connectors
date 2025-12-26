#!/usr/bin/env python3
"""
Test script to verify channel fetching for a specific team.
Run this to debug why channels aren't showing up.
"""

import requests
import json

# Replace with your actual credentials
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# Test both teams - replace with your team IDs
TEAM_IDS = [
    ("Team 1", "YOUR_TEAM_ID_1"),
    ("Team 2", "YOUR_TEAM_ID_2")
]

def get_token():
    """Get access token."""
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }
    resp = requests.post(token_url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_channels(team_id, token):
    """Fetch channels for a team."""
    url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels"
    headers = {"Authorization": f"Bearer {token}"}

    print(f"\n{'='*80}")
    print(f"Fetching channels for team: {team_id}")
    print(f"{'='*80}\n")

    resp = requests.get(url, headers=headers)
    print(f"Status Code: {resp.status_code}")
    print(f"Response Headers: {dict(resp.headers)}\n")

    if resp.status_code == 200:
        data = resp.json()
        channels = data.get("value", [])
        print(f"✓ SUCCESS! Found {len(channels)} channels:\n")
        for ch in channels:
            print(f"  - {ch.get('displayName')} (ID: {ch.get('id')})")
            print(f"    membershipType: {ch.get('membershipType')}")
            print(f"    email: {ch.get('email')}")
            print()
    else:
        print(f"✗ ERROR!")
        print(f"Response: {resp.text}")

    return resp

if __name__ == "__main__":
    try:
        print("Getting access token...")
        token = get_token()
        print("✓ Token acquired\n")

        # Test both teams
        for team_name, team_id in TEAM_IDS:
            print(f"\n{'#'*80}")
            print(f"# Testing: {team_name}")
            print(f"{'#'*80}")
            fetch_channels(team_id, token)

    except Exception as e:
        print(f"\n✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
