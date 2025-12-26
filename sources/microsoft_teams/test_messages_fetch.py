#!/usr/bin/env python3
"""
Test script to verify message fetching for all channels.
Run this to debug which channels have messages.
"""

import requests
import json

# Replace with your actual credentials
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# All channels to test - replace with your channel IDs
CHANNELS = [
    {
        "team_name": "Team 1",
        "team_id": "YOUR_TEAM_ID_1",
        "channel_name": "General",
        "channel_id": "YOUR_CHANNEL_ID_1"
    },
    {
        "team_name": "Team 2",
        "team_id": "YOUR_TEAM_ID_2",
        "channel_name": "Channel 1",
        "channel_id": "YOUR_CHANNEL_ID_2"
    }
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

def fetch_messages(team_id, channel_id, token):
    """Fetch messages for a channel."""
    url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        data = resp.json()
        messages = data.get("value", [])
        return messages, None
    else:
        return None, f"Error {resp.status_code}: {resp.text}"

if __name__ == "__main__":
    try:
        print("Getting access token...")
        token = get_token()
        print("✓ Token acquired\n")

        print("=" * 80)
        print("MESSAGES SUMMARY BY CHANNEL")
        print("=" * 80)
        print()

        total_messages = 0

        for channel_info in CHANNELS:
            team_name = channel_info["team_name"]
            channel_name = channel_info["channel_name"]
            team_id = channel_info["team_id"]
            channel_id = channel_info["channel_id"]

            print(f"📍 {team_name} > {channel_name}")
            print(f"   Team ID: {team_id}")
            print(f"   Channel ID: {channel_id}")

            messages, error = fetch_messages(team_id, channel_id, token)

            if error:
                print(f"   ❌ {error}")
            else:
                count = len(messages)
                total_messages += count
                print(f"   ✓ Found {count} message(s)")

                if count > 0:
                    print(f"\n   Message Details:")
                    for idx, msg in enumerate(messages, 1):
                        msg_id = msg.get("id", "unknown")
                        created = msg.get("createdDateTime", "unknown")
                        modified = msg.get("lastModifiedDateTime", "unknown")
                        subject = msg.get("subject", "")
                        body_preview = msg.get("body", {}).get("content", "")[:100]
                        msg_from = msg.get("from", {})
                        sender = "unknown"
                        if msg_from:
                            user = msg_from.get("user", {})
                            if user:
                                sender = user.get("displayName", "unknown")

                        print(f"   {idx}. ID: {msg_id}")
                        print(f"      Created: {created}")
                        print(f"      Modified: {modified}")
                        if subject:
                            print(f"      Subject: {subject}")
                        print(f"      From: {sender}")
                        print(f"      Preview: {body_preview.replace(chr(10), ' ')[:80]}...")
                        print()

            print()

        print("=" * 80)
        print(f"TOTAL MESSAGES ACROSS ALL CHANNELS: {total_messages}")
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
