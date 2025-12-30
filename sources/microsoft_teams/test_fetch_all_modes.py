#!/usr/bin/env python3
"""
Test script to verify fetch_all modes work correctly.
This validates the automatic discovery functionality.
"""

import sys
import os

# Add parent directory to path to import the connector
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from microsoft_teams.microsoft_teams import MicrosoftTeamsConnector

# Replace with your actual credentials
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

def test_channels_fetch_all_teams():
    """Test channels with fetch_all_teams=true"""
    print("\n" + "="*80)
    print("TEST 1: Channels with fetch_all_teams=true")
    print("="*80)

    connector = MicrosoftTeamsConnector()
    connector.open(
        partition={},
        connection_config={
            "tenant_id": TENANT_ID,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
    )

    table_options = {
        "fetch_all_teams": "true",
        "max_pages_per_batch": "2"  # Limit for testing
    }

    try:
        records, offset = connector.read_table("channels", {}, table_options)
        record_list = list(records)
        print(f"✓ Success! Fetched {len(record_list)} channels across all teams")

        if record_list:
            print(f"\nSample channel:")
            sample = record_list[0]
            print(f"  ID: {sample.get('id')}")
            print(f"  Team ID: {sample.get('team_id')}")
            print(f"  Display Name: {sample.get('displayName')}")

        return True
    except Exception as e:
        print(f"✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_members_fetch_all_teams():
    """Test members with fetch_all_teams=true"""
    print("\n" + "="*80)
    print("TEST 2: Members with fetch_all_teams=true")
    print("="*80)

    connector = MicrosoftTeamsConnector()
    connector.open(
        partition={},
        connection_config={
            "tenant_id": TENANT_ID,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
    )

    table_options = {
        "fetch_all_teams": "true",
        "max_pages_per_batch": "2"  # Limit for testing
    }

    try:
        records, offset = connector.read_table("members", {}, table_options)
        record_list = list(records)
        print(f"✓ Success! Fetched {len(record_list)} members across all teams")

        if record_list:
            print(f"\nSample member:")
            sample = record_list[0]
            print(f"  ID: {sample.get('id')}")
            print(f"  Team ID: {sample.get('team_id')}")
            print(f"  Display Name: {sample.get('displayName')}")

        return True
    except Exception as e:
        print(f"✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_messages_fetch_all_channels():
    """Test messages with fetch_all_channels=true (requires team_id)"""
    print("\n" + "="*80)
    print("TEST 3: Messages with fetch_all_channels=true")
    print("="*80)
    print("NOTE: This test requires a team_id. Skipping if not provided.")

    # First, get a team_id
    connector = MicrosoftTeamsConnector()
    connector.open(
        partition={},
        connection_config={
            "tenant_id": TENANT_ID,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
    )

    # Get first team
    teams_records, _ = connector.read_table("teams", {}, {"max_pages_per_batch": "1"})
    teams_list = list(teams_records)

    if not teams_list:
        print("✗ No teams found, cannot test fetch_all_channels")
        return False

    team_id = teams_list[0]["id"]
    print(f"Using team_id: {team_id}")

    table_options = {
        "team_id": team_id,
        "fetch_all_channels": "true",
        "max_pages_per_batch": "2"  # Limit for testing
    }

    try:
        records, offset = connector.read_table("messages", {}, table_options)
        record_list = list(records)
        print(f"✓ Success! Fetched {len(record_list)} messages across all channels")

        if record_list:
            print(f"\nSample message:")
            sample = record_list[0]
            print(f"  ID: {sample.get('id')}")
            print(f"  Team ID: {sample.get('team_id')}")
            print(f"  Channel ID: {sample.get('channel_id')}")
            print(f"  Created: {sample.get('createdDateTime')}")

        return True
    except Exception as e:
        print(f"✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_message_replies_fetch_all_messages():
    """Test message_replies with fetch_all_messages=true (requires team_id and channel_id)"""
    print("\n" + "="*80)
    print("TEST 4: Message Replies with fetch_all_messages=true")
    print("="*80)
    print("NOTE: This test requires team_id and channel_id. Skipping if not provided.")

    # First, get a team_id and channel_id
    connector = MicrosoftTeamsConnector()
    connector.open(
        partition={},
        connection_config={
            "tenant_id": TENANT_ID,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
    )

    # Get first team
    teams_records, _ = connector.read_table("teams", {}, {"max_pages_per_batch": "1"})
    teams_list = list(teams_records)

    if not teams_list:
        print("✗ No teams found, cannot test fetch_all_messages")
        return False

    team_id = teams_list[0]["id"]

    # Get first channel
    channels_records, _ = connector.read_table("channels", {}, {"team_id": team_id, "max_pages_per_batch": "1"})
    channels_list = list(channels_records)

    if not channels_list:
        print("✗ No channels found, cannot test fetch_all_messages")
        return False

    channel_id = channels_list[0]["id"]
    print(f"Using team_id: {team_id}")
    print(f"Using channel_id: {channel_id}")

    table_options = {
        "team_id": team_id,
        "channel_id": channel_id,
        "fetch_all_messages": "true",
        "max_pages_per_batch": "1"  # Limit for testing (fetches fewer messages)
    }

    try:
        records, offset = connector.read_table("message_replies", {}, table_options)
        record_list = list(records)
        print(f"✓ Success! Fetched {len(record_list)} replies across all messages")

        if record_list:
            print(f"\nSample reply:")
            sample = record_list[0]
            print(f"  ID: {sample.get('id')}")
            print(f"  Parent Message ID: {sample.get('parent_message_id')}")
            print(f"  Team ID: {sample.get('team_id')}")
            print(f"  Channel ID: {sample.get('channel_id')}")
            print(f"  Created: {sample.get('createdDateTime')}")
        else:
            print("Note: No replies found (this is normal if messages don't have threads)")

        return True
    except Exception as e:
        print(f"✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("\n" + "="*80)
    print("MICROSOFT TEAMS CONNECTOR - FETCH_ALL MODES TEST SUITE")
    print("="*80)
    print("\nThis script tests the automatic discovery (fetch_all) functionality")
    print("across all tables that support parent resource discovery.")
    print("\nMake sure to configure your credentials at the top of this file!")
    print("="*80)

    # Check if credentials are configured
    if TENANT_ID == "YOUR_TENANT_ID":
        print("\n✗ ERROR: Please configure your credentials in this file first!")
        print("  Edit the TENANT_ID, CLIENT_ID, and CLIENT_SECRET variables.")
        sys.exit(1)

    results = []

    # Run all tests
    results.append(("Channels (fetch_all_teams)", test_channels_fetch_all_teams()))
    results.append(("Members (fetch_all_teams)", test_members_fetch_all_teams()))
    results.append(("Messages (fetch_all_channels)", test_messages_fetch_all_channels()))
    results.append(("Message Replies (fetch_all_messages)", test_message_replies_fetch_all_messages()))

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
        print("\n🎉 All tests passed! The fetch_all modes are working correctly.")
        sys.exit(0)
    else:
        print("\n⚠️  Some tests failed. Please review the errors above.")
        sys.exit(1)
