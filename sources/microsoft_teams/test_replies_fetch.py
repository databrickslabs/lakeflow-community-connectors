#!/usr/bin/env python3
"""
Test script to verify reply fetching for specific messages.
Run this to verify which messages have replies/threads.
"""

import requests
import json

# Replace with your actual credentials
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# Messages to test - replace with actual message IDs from your teams
# You can get these from the messages table after ingesting messages
TEST_MESSAGES = [
    {
        "team_name": "Team 1",
        "team_id": "YOUR_TEAM_ID_1",
        "channel_name": "General",
        "channel_id": "YOUR_CHANNEL_ID_1",
        "message_id": "YOUR_MESSAGE_ID_1",  # Parent message to fetch replies for
        "message_subject": "Example message with replies"
    },
    {
        "team_name": "Team 2",
        "team_id": "YOUR_TEAM_ID_2",
        "channel_name": "General",
        "channel_id": "YOUR_CHANNEL_ID_2",
        "message_id": "YOUR_MESSAGE_ID_2",
        "message_subject": "Another threaded message"
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

def fetch_replies(team_id, channel_id, message_id, token):
    """Fetch replies for a message."""
    url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        data = resp.json()
        replies = data.get("value", [])
        return replies, None
    else:
        return None, f"Error {resp.status_code}: {resp.text}"

if __name__ == "__main__":
    try:
        print("Getting access token...")
        token = get_token()
        print("✓ Token acquired\n")

        print("=" * 80)
        print("MESSAGE REPLIES SUMMARY")
        print("=" * 80)
        print()

        total_replies = 0

        for msg_info in TEST_MESSAGES:
            team_name = msg_info["team_name"]
            channel_name = msg_info["channel_name"]
            message_subject = msg_info["message_subject"]
            team_id = msg_info["team_id"]
            channel_id = msg_info["channel_id"]
            message_id = msg_info["message_id"]

            print(f"📍 {team_name} > {channel_name} > {message_subject}")
            print(f"   Team ID: {team_id}")
            print(f"   Channel ID: {channel_id}")
            print(f"   Message ID: {message_id}")

            replies, error = fetch_replies(team_id, channel_id, message_id, token)

            if error:
                print(f"   ❌ {error}")
            else:
                count = len(replies)
                total_replies += count
                print(f"   ✓ Found {count} reply(ies)")

                if count > 0:
                    print(f"\n   Reply Details:")
                    for idx, reply in enumerate(replies, 1):
                        reply_id = reply.get("id", "unknown")
                        created = reply.get("createdDateTime", "unknown")
                        modified = reply.get("lastModifiedDateTime", "unknown")
                        subject = reply.get("subject", "")
                        body_preview = reply.get("body", {}).get("content", "")[:100]
                        msg_from = reply.get("from", {})
                        sender = "unknown"
                        if msg_from:
                            user = msg_from.get("user", {})
                            if user:
                                sender = user.get("displayName", "unknown")

                        print(f"   {idx}. Reply ID: {reply_id}")
                        print(f"      Created: {created}")
                        print(f"      Modified: {modified}")
                        if subject:
                            print(f"      Subject: {subject}")
                        print(f"      From: {sender}")
                        print(f"      Preview: {body_preview.replace(chr(10), ' ')[:80]}...")
                        print()

            print()

        print("=" * 80)
        print(f"TOTAL REPLIES ACROSS ALL MESSAGES: {total_replies}")
        print("=" * 80)
        print()
        print("Next steps:")
        print("1. Add these message_ids to REPLY_CONFIGS in your ingest.py")
        print("2. Run your DLT pipeline with INGEST_REPLIES = True")
        print("3. Query the message_replies table to see threaded conversations")

    except Exception as e:
        print(f"\n✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
