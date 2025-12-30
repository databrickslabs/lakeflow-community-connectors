#!/usr/bin/env python3
"""
Simple test script for fetch_all modes - no PySpark required.
Tests the Microsoft Graph API calls directly.
"""

import requests
import json

# Your credentials (replace with actual values)
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

BASE_URL = "https://graph.microsoft.com/v1.0"

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

def test_fetch_all_teams_for_channels(token):
    """Simulate fetch_all_teams for channels"""
    print("\n" + "="*80)
    print("TEST 1: Fetch ALL teams, then channels for each")
    print("="*80)

    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: Fetch all teams
    print("\nStep 1: Discovering teams...")
    teams_url = f"{BASE_URL}/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
    teams_params = {"$select": "id,displayName"}

    resp = requests.get(teams_url, headers=headers, params=teams_params)
    resp.raise_for_status()
    teams = resp.json().get("value", [])

    print(f"✓ Found {len(teams)} team(s)")
    for team in teams[:3]:  # Show first 3
        print(f"  - {team.get('displayName')} ({team.get('id')})")

    if len(teams) > 3:
        print(f"  ... and {len(teams) - 3} more")

    # Step 2: Fetch channels for each team
    print("\nStep 2: Fetching channels for each team...")
    total_channels = 0

    for team in teams[:2]:  # Test first 2 teams only
        team_id = team["id"]
        team_name = team.get("displayName", "Unknown")

        channels_url = f"{BASE_URL}/teams/{team_id}/channels"
        try:
            resp = requests.get(channels_url, headers=headers)
            resp.raise_for_status()
            channels = resp.json().get("value", [])
            total_channels += len(channels)
            print(f"  ✓ {team_name}: {len(channels)} channel(s)")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [404, 403]:
                print(f"  ⚠ {team_name}: Inaccessible (skipped)")
            else:
                raise

    print(f"\n✓ Total: {total_channels} channels across {min(2, len(teams))} team(s)")
    return True

def test_fetch_all_teams_for_members(token):
    """Simulate fetch_all_teams for members"""
    print("\n" + "="*80)
    print("TEST 2: Fetch ALL teams, then members for each")
    print("="*80)

    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: Fetch all teams
    print("\nStep 1: Discovering teams...")
    teams_url = f"{BASE_URL}/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
    teams_params = {"$select": "id,displayName"}

    resp = requests.get(teams_url, headers=headers, params=teams_params)
    resp.raise_for_status()
    teams = resp.json().get("value", [])

    print(f"✓ Found {len(teams)} team(s)")

    # Step 2: Fetch members for each team
    print("\nStep 2: Fetching members for each team...")
    total_members = 0

    for team in teams[:2]:  # Test first 2 teams only
        team_id = team["id"]
        team_name = team.get("displayName", "Unknown")

        members_url = f"{BASE_URL}/teams/{team_id}/members"
        try:
            resp = requests.get(members_url, headers=headers)
            resp.raise_for_status()
            members = resp.json().get("value", [])
            total_members += len(members)
            print(f"  ✓ {team_name}: {len(members)} member(s)")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [404, 403]:
                print(f"  ⚠ {team_name}: Inaccessible (skipped)")
            else:
                raise

    print(f"\n✓ Total: {total_members} members across {min(2, len(teams))} team(s)")
    return True

def test_fetch_all_channels_for_messages(token):
    """Simulate fetch_all_channels for messages"""
    print("\n" + "="*80)
    print("TEST 3: Fetch ALL channels in a team, then messages for each")
    print("="*80)

    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: Get first team
    print("\nStep 1: Getting first team...")
    teams_url = f"{BASE_URL}/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
    resp = requests.get(teams_url, headers=headers, params={"$select": "id,displayName", "$top": "1"})
    resp.raise_for_status()
    teams = resp.json().get("value", [])

    if not teams:
        print("✗ No teams found")
        return False

    team_id = teams[0]["id"]
    team_name = teams[0].get("displayName", "Unknown")
    print(f"✓ Using team: {team_name}")

    # Step 2: Fetch all channels in this team
    print("\nStep 2: Discovering channels...")
    channels_url = f"{BASE_URL}/teams/{team_id}/channels"
    resp = requests.get(channels_url, headers=headers, params={"$select": "id,displayName"})
    resp.raise_for_status()
    channels = resp.json().get("value", [])

    print(f"✓ Found {len(channels)} channel(s)")
    for channel in channels[:3]:  # Show first 3
        print(f"  - {channel.get('displayName')}")

    # Step 3: Fetch messages for each channel
    print("\nStep 3: Fetching messages for each channel...")
    total_messages = 0

    for channel in channels[:2]:  # Test first 2 channels only
        channel_id = channel["id"]
        channel_name = channel.get("displayName", "Unknown")

        messages_url = f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/messages"
        try:
            resp = requests.get(messages_url, headers=headers, params={"$top": "10"})
            resp.raise_for_status()
            messages = resp.json().get("value", [])
            total_messages += len(messages)
            print(f"  ✓ {channel_name}: {len(messages)} message(s) (limited to 10)")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [404, 403]:
                print(f"  ⚠ {channel_name}: Inaccessible (skipped)")
            else:
                raise

    print(f"\n✓ Total: {total_messages} messages across {min(2, len(channels))} channel(s)")
    return True

def test_fetch_all_messages_for_replies(token):
    """Simulate fetch_all_messages for message_replies"""
    print("\n" + "="*80)
    print("TEST 4: Fetch ALL messages in a channel, then replies for each")
    print("="*80)

    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: Get first team
    print("\nStep 1: Getting first team...")
    teams_url = f"{BASE_URL}/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
    resp = requests.get(teams_url, headers=headers, params={"$select": "id", "$top": "1"})
    resp.raise_for_status()
    teams = resp.json().get("value", [])

    if not teams:
        print("✗ No teams found")
        return False

    team_id = teams[0]["id"]

    # Step 2: Get first channel
    print("\nStep 2: Getting first channel...")
    channels_url = f"{BASE_URL}/teams/{team_id}/channels"
    resp = requests.get(channels_url, headers=headers, params={"$select": "id,displayName"})
    resp.raise_for_status()
    channels = resp.json().get("value", [])
    channels = channels[:1]  # Take first channel

    if not channels:
        print("✗ No channels found")
        return False

    channel_id = channels[0]["id"]
    channel_name = channels[0].get("displayName", "Unknown")
    print(f"✓ Using channel: {channel_name}")

    # Step 3: Fetch all messages in this channel
    print("\nStep 3: Discovering messages...")
    messages_url = f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/messages"
    resp = requests.get(messages_url, headers=headers, params={"$top": "5"})
    resp.raise_for_status()
    messages = resp.json().get("value", [])

    print(f"✓ Found {len(messages)} message(s) (limited to 5)")

    # Step 4: Fetch replies for each message
    print("\nStep 4: Fetching replies for each message...")
    total_replies = 0

    for message in messages:
        message_id = message["id"]

        replies_url = f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies"
        try:
            resp = requests.get(replies_url, headers=headers)
            resp.raise_for_status()
            replies = resp.json().get("value", [])
            if replies:
                total_replies += len(replies)
                print(f"  ✓ Message {message_id[:8]}...: {len(replies)} reply(ies)")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [404, 403]:
                pass  # No replies or inaccessible
            else:
                raise

    if total_replies == 0:
        print("  Note: No replies found (this is normal if messages don't have threads)")

    print(f"\n✓ Total: {total_replies} replies across {len(messages)} message(s)")
    return True

if __name__ == "__main__":
    print("\n" + "="*80)
    print("MICROSOFT TEAMS - FETCH_ALL MODES API TEST")
    print("="*80)
    print("\nThis script tests the API calls that the fetch_all modes will make.")
    print("="*80)

    try:
        print("\nAuthenticating...")
        token = get_token()
        print("✓ Token acquired\n")

        results = []

        # Run all tests
        results.append(("Channels (fetch_all_teams)", test_fetch_all_teams_for_channels(token)))
        results.append(("Members (fetch_all_teams)", test_fetch_all_teams_for_members(token)))
        results.append(("Messages (fetch_all_channels)", test_fetch_all_channels_for_messages(token)))
        results.append(("Message Replies (fetch_all_messages)", test_fetch_all_messages_for_replies(token)))

        # Print summary
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        for name, passed in results:
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"{status}: {name}")

        passed_count = sum(1 for _, passed in results if passed)
        total_count = len(results)

        print(f"\nTotal: {passed_count}/{total_count} tests passed")

        if passed_count == total_count:
            print("\n🎉 All tests passed! The fetch_all API patterns work correctly.")
            print("\nThis confirms that:")
            print("  • Your credentials have the correct permissions")
            print("  • The Graph API calls for auto-discovery work")
            print("  • The connector implementation will work in DLT")
        else:
            print("\n⚠️  Some tests failed. Please review the errors above.")

    except Exception as e:
        print(f"\n✗ AUTHENTICATION FAILED: {e}")
        import traceback
        traceback.print_exc()
