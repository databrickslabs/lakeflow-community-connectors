#!/usr/bin/env python3
"""
Direct connector test - validates the connector logic without PySpark/DLT.
This mimics what happens when DLT calls the connector.
"""

import sys
import os

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Now we can import from the sources directory
from sources.microsoft_teams.microsoft_teams import MicrosoftTeamsConnector

# Your credentials
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

def test_connector_channels_fetch_all():
    """Test the actual connector code for channels with fetch_all_teams"""
    print("\n" + "="*80)
    print("CONNECTOR TEST: Channels with fetch_all_teams=true")
    print("="*80)

    # Create connector instance (this is what DLT does)
    connector = MicrosoftTeamsConnector()

    # Open connection (this is what DLT does)
    connector.open(
        partition={},
        connection_config={
            "tenant_id": TENANT_ID,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
    )

    # Table options with fetch_all_teams
    table_options = {
        "fetch_all_teams": "true",
        "max_pages_per_batch": "2"  # Limit for testing
    }

    try:
        # This is what happens when DLT reads the table
        print("\nCalling connector.read_table('channels', {}, table_options)...")
        records_iter, offset = connector.read_table("channels", {}, table_options)

        # Convert iterator to list to see results
        records = list(records_iter)

        print(f"✓ SUCCESS! Got {len(records)} channel records")

        if records:
            print(f"\nSample channel record:")
            sample = records[0]
            print(f"  ID: {sample.get('id')}")
            print(f"  Team ID: {sample.get('team_id')}")
            print(f"  Display Name: {sample.get('displayName')}")
            print(f"  Membership Type: {sample.get('membershipType')}")

            print(f"\nAll teams found:")
            teams_seen = set()
            for record in records:
                team_id = record.get('team_id')
                if team_id:
                    teams_seen.add(team_id)

            print(f"  Total unique teams: {len(teams_seen)}")
            print(f"  Total channels: {len(records)}")

        return True

    except Exception as e:
        print(f"✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_connector_members_fetch_all():
    """Test the actual connector code for members with fetch_all_teams"""
    print("\n" + "="*80)
    print("CONNECTOR TEST: Members with fetch_all_teams=true")
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
        "max_pages_per_batch": "2"
    }

    try:
        print("\nCalling connector.read_table('members', {}, table_options)...")
        records_iter, offset = connector.read_table("members", {}, table_options)
        records = list(records_iter)

        print(f"✓ SUCCESS! Got {len(records)} member records")

        if records:
            print(f"\nSample member record:")
            sample = records[0]
            print(f"  ID: {sample.get('id')}")
            print(f"  Team ID: {sample.get('team_id')}")
            print(f"  Display Name: {sample.get('displayName')}")
            print(f"  Roles: {sample.get('roles')}")

        return True

    except Exception as e:
        print(f"✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_connector_messages_fetch_all_channels():
    """Test the actual connector code for messages with fetch_all_channels"""
    print("\n" + "="*80)
    print("CONNECTOR TEST: Messages with fetch_all_channels=true")
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

    # First get a team_id
    print("\nStep 1: Getting a team_id...")
    teams_iter, _ = connector.read_table("teams", {}, {"max_pages_per_batch": "1"})
    teams = list(teams_iter)

    if not teams:
        print("✗ No teams found")
        return False

    team_id = teams[0]["id"]
    print(f"✓ Using team_id: {team_id}")

    table_options = {
        "team_id": team_id,
        "fetch_all_channels": "true",
        "max_pages_per_batch": "1",  # Limit messages fetched
        "start_date": "2025-01-01T00:00:00Z"
    }

    try:
        print("\nStep 2: Calling connector.read_table('messages', {}, table_options)...")
        records_iter, offset = connector.read_table("messages", {}, table_options)
        records = list(records_iter)

        print(f"✓ SUCCESS! Got {len(records)} message records")

        if records:
            print(f"\nSample message record:")
            sample = records[0]
            print(f"  ID: {sample.get('id')}")
            print(f"  Team ID: {sample.get('team_id')}")
            print(f"  Channel ID: {sample.get('channel_id')}")
            print(f"  Created: {sample.get('createdDateTime')}")
            print(f"  Has body: {'body' in sample}")

            # Show which channels had messages
            channels_seen = set()
            for record in records:
                channel_id = record.get('channel_id')
                if channel_id:
                    channels_seen.add(channel_id)

            print(f"\n  Messages found in {len(channels_seen)} channel(s)")

        return True

    except Exception as e:
        print(f"✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_connector_replies_fetch_all_messages():
    """Test the actual connector code for message_replies with fetch_all_messages"""
    print("\n" + "="*80)
    print("CONNECTOR TEST: Message Replies with fetch_all_messages=true")
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

    # Get team_id and channel_id
    print("\nStep 1: Getting team_id and channel_id...")
    teams_iter, _ = connector.read_table("teams", {}, {"max_pages_per_batch": "1"})
    teams = list(teams_iter)

    if not teams:
        print("✗ No teams found")
        return False

    team_id = teams[0]["id"]

    channels_iter, _ = connector.read_table("channels", {}, {"team_id": team_id, "max_pages_per_batch": "1"})
    channels = list(channels_iter)

    if not channels:
        print("✗ No channels found")
        return False

    channel_id = channels[0]["id"]
    print(f"✓ Using team_id: {team_id}")
    print(f"✓ Using channel_id: {channel_id}")

    table_options = {
        "team_id": team_id,
        "channel_id": channel_id,
        "fetch_all_messages": "true",
        "max_pages_per_batch": "1",  # Limit to first page of messages
        "start_date": "2025-01-01T00:00:00Z"
    }

    try:
        print("\nStep 2: Calling connector.read_table('message_replies', {}, table_options)...")
        records_iter, offset = connector.read_table("message_replies", {}, table_options)
        records = list(records_iter)

        print(f"✓ SUCCESS! Got {len(records)} reply records")

        if records:
            print(f"\nSample reply record:")
            sample = records[0]
            print(f"  ID: {sample.get('id')}")
            print(f"  Parent Message ID: {sample.get('parent_message_id')}")
            print(f"  Team ID: {sample.get('team_id')}")
            print(f"  Channel ID: {sample.get('channel_id')}")
            print(f"  Created: {sample.get('createdDateTime')}")

            # Show which messages had replies
            messages_seen = set()
            for record in records:
                parent_id = record.get('parent_message_id')
                if parent_id:
                    messages_seen.add(parent_id)

            print(f"\n  Replies found for {len(messages_seen)} message(s)")
        else:
            print("  Note: No replies found (normal if messages don't have threads)")

        return True

    except Exception as e:
        print(f"✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("\n" + "="*80)
    print("MICROSOFT TEAMS CONNECTOR - DIRECT CONNECTOR TEST")
    print("="*80)
    print("\nThis tests the actual connector Python code that runs in DLT.")
    print("It validates the fetch_all logic inside the connector methods.")
    print("="*80)

    results = []

    # Run all tests
    results.append(("Channels (fetch_all_teams)", test_connector_channels_fetch_all()))
    results.append(("Members (fetch_all_teams)", test_connector_members_fetch_all()))
    results.append(("Messages (fetch_all_channels)", test_connector_messages_fetch_all_channels()))
    results.append(("Message Replies (fetch_all_messages)", test_connector_replies_fetch_all_messages()))

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
        print("\n🎉 All connector tests passed!")
        print("\nThe connector implementation is working correctly.")
        print("Ready to use in DLT pipelines!")
        sys.exit(0)
    else:
        print("\n⚠️  Some tests failed. Please review the errors above.")
        sys.exit(1)
