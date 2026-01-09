#!/usr/bin/env python3
"""
Script to add incremental data to the demo Gmail account.

This script adds new emails to demonstrate the connector's incremental sync
capabilities using Gmail's History API.

Use this AFTER running populate_demo_data.py and doing an initial data ingestion.

Setup:
1. Make sure populate_demo_data.py has been run
2. Run initial ingestion in Databricks
3. Run this script: python sources/gmail/demo/add_incremental_data.py
4. Run incremental ingestion in Databricks to see CDC in action

This demonstrates:
- Incremental sync with historyId cursor
- New emails being detected and synced
- Efficient change data capture (CDC)
"""

import base64
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Gmail API scopes
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify',
]

# Demo account email - actual credentials stored securely in Keeper vault
# For testing: Use your own Gmail account (same as populate_demo_data.py)
DEMO_EMAIL = "DEMO_EMAIL_PLACEHOLDER"  # Replace with your test email address


def get_gmail_service():
    """Authenticate and return Gmail API service."""
    creds = None
    token_path = Path(__file__).parent / "token.json"

    # Load existing credentials
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    # Refresh or get new credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print("\n" + "="*60)
            print("GMAIL API AUTHENTICATION")
            print("="*60)
            print("\nAuthentication required.")
            print("A browser window will open for authorization.\n")

            client_id = input("Enter OAuth Client ID: ").strip()
            client_secret = input("Enter OAuth Client Secret: ").strip()

            client_config = {
                "installed": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["http://localhost", "urn:ietf:wg:oauth:2.0:oob"]
                }
            }

            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save credentials
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)


def create_message(to, subject, body_text, body_html=None):
    """Create an email message."""
    message = MIMEMultipart('alternative')
    message['To'] = to
    message['From'] = DEMO_EMAIL
    message['Subject'] = subject

    # Add text version
    message.attach(MIMEText(body_text, 'plain'))

    # Add HTML version if provided
    if body_html:
        message.attach(MIMEText(body_html, 'html'))

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw}


def send_message(service, message):
    """Send an email message and return the sent message."""
    try:
        sent = service.users().messages().send(userId='me', body=message).execute()
        print(f"  ✓ Sent message ID: {sent['id']}")
        time.sleep(0.5)  # Rate limiting
        return sent
    except Exception as e:
        print(f"  ✗ Error sending message: {e}")
        return None


def get_label_id(service, label_name):
    """Get label ID by name."""
    try:
        labels = service.users().labels().list(userId='me').execute()
        for label in labels.get('labels', []):
            if label['name'] == label_name:
                return label['id']
    except Exception as e:
        print(f"  ✗ Error getting label: {e}")
    return None


def add_label_to_message(service, message_id, label_id):
    """Add a label to a message."""
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'addLabelIds': [label_id]}
        ).execute()
    except Exception as e:
        print(f"  ✗ Error adding label: {e}")


def add_new_support_tickets(service):
    """Add new customer support emails."""
    print("\n" + "="*60)
    print("ADDING NEW SUPPORT TICKETS")
    print("="*60 + "\n")

    support_label_id = get_label_id(service, "Customer Support")

    emails = [
        {
            "subject": "Password Reset Not Working",
            "body": "Hi Support,\n\nI clicked on 'Forgot Password' but I'm not receiving the reset email. I've checked my spam folder too.\n\nCan you help me reset my password?\n\nRegards,\nDavid Park",
            "html": "<p>Hi Support,</p><p>I clicked on <strong>'Forgot Password'</strong> but I'm not receiving the reset email. I've checked my spam folder too.</p><p>Can you help me reset my password?</p><p>Regards,<br>David Park</p>"
        },
        {
            "subject": "API Rate Limit Exceeded",
            "body": "Hello,\n\nI'm getting a 'Rate Limit Exceeded' error when calling your API. My current plan should support 10,000 requests per hour but I'm being limited at 1,000.\n\nCan you check my account settings?\n\nThanks,\nLisa Chen\nDeveloper at TechStart Inc",
            "html": "<p>Hello,</p><p>I'm getting a <strong style='color: red;'>'Rate Limit Exceeded'</strong> error when calling your API. My current plan should support 10,000 requests per hour but I'm being limited at 1,000.</p><p>Can you check my account settings?</p><p>Thanks,<br>Lisa Chen<br><em>Developer at TechStart Inc</em></p>"
        },
        {
            "subject": "Request for Data Export",
            "body": "Hi Team,\n\nPer our company's data retention policy, I need to export all our data from your platform.\n\nCan you provide:\n1. Complete data export in JSON format\n2. Metadata schema documentation\n3. Confirmation of data deletion after export\n\nThis is for compliance purposes.\n\nBest,\nRobert Martinez\nCompliance Officer",
            "html": "<div style='font-family: Arial, sans-serif;'><p>Hi Team,</p><p>Per our company's data retention policy, I need to export all our data from your platform.</p><h4>Can you provide:</h4><ol><li>Complete data export in JSON format</li><li>Metadata schema documentation</li><li>Confirmation of data deletion after export</li></ol><p><em>This is for compliance purposes.</em></p><p>Best,<br>Robert Martinez<br><strong>Compliance Officer</strong></p></div>"
        }
    ]

    for email in emails:
        print(f"Sending: {email['subject']}")
        sent = send_message(
            service,
            create_message(DEMO_EMAIL, email['subject'], email['body'], email.get('html'))
        )
        if sent and support_label_id:
            add_label_to_message(service, sent['id'], support_label_id)


def add_sales_updates(service):
    """Add new sales-related emails."""
    print("\n" + "="*60)
    print("ADDING SALES UPDATES")
    print("="*60 + "\n")

    sales_label_id = get_label_id(service, "Sales")

    emails = [
        {
            "subject": "Demo Scheduled - GlobalTech Solutions",
            "body": "Team,\n\nGreat news! We've scheduled a demo with GlobalTech Solutions for next week.\n\nDetails:\n- Company: GlobalTech Solutions\n- Contact: Jennifer Wu, VP of Engineering\n- Team Size: 200 users\n- Budget: $5K-8K per month\n- Demo Date: February 5th, 10am EST\n- Decision Timeline: End of Q1\n\nThis is a high-value opportunity. Let's make it count!\n\nAlex",
            "html": "<div style='font-family: Arial, sans-serif;'><p>Team,</p><p><strong style='color: green;'>Great news!</strong> We've scheduled a demo with GlobalTech Solutions for next week.</p><h3>Details:</h3><ul style='line-height: 1.8;'><li><strong>Company:</strong> GlobalTech Solutions</li><li><strong>Contact:</strong> Jennifer Wu, VP of Engineering</li><li><strong>Team Size:</strong> 200 users</li><li><strong>Budget:</strong> $5K-8K per month</li><li><strong>Demo Date:</strong> February 5th, 10am EST</li><li><strong>Decision Timeline:</strong> End of Q1</li></ul><p style='background: #fff3e0; padding: 15px; border-radius: 5px;'><strong>📌 This is a high-value opportunity. Let's make it count!</strong></p><p>Alex</p></div>"
        },
        {
            "subject": "Quarterly Sales Update - Q1 2024",
            "body": "Sales Team,\n\nHere's our Q1 performance update:\n\nRevenue:\n- Total: $450K (+23% QoQ)\n- New Business: $280K\n- Expansions: $170K\n\nPipeline:\n- Qualified Leads: 45\n- Demos Scheduled: 18\n- Contracts in Review: 8\n\nTop Performers:\n1. Alex Thompson - $120K\n2. Maria Garcia - $95K\n3. James Wilson - $88K\n\nKeep up the great work!\n\nSales Leadership",
            "html": "<div style='font-family: Arial, sans-serif; max-width: 600px;'><h2 style='color: #1976d2;'>📊 Quarterly Sales Update - Q1 2024</h2><p>Sales Team,</p><p>Here's our Q1 performance update:</p><div style='background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0;'><h3 style='margin-top: 0;'>Revenue:</h3><ul><li><strong>Total:</strong> $450K <span style='color: green;'>(+23% QoQ)</span></li><li><strong>New Business:</strong> $280K</li><li><strong>Expansions:</strong> $170K</li></ul></div><div style='background: #f3e5f5; padding: 20px; border-radius: 8px; margin: 20px 0;'><h3 style='margin-top: 0;'>Pipeline:</h3><ul><li><strong>Qualified Leads:</strong> 45</li><li><strong>Demos Scheduled:</strong> 18</li><li><strong>Contracts in Review:</strong> 8</li></ul></div><h3>🏆 Top Performers:</h3><ol style='font-size: 16px; line-height: 1.8;'><li><strong>Alex Thompson</strong> - $120K</li><li><strong>Maria Garcia</strong> - $95K</li><li><strong>James Wilson</strong> - $88K</li></ol><p style='text-align: center; background: #4caf50; color: white; padding: 15px; border-radius: 5px; margin-top: 30px;'><strong>Keep up the great work! 🚀</strong></p><p><em>Sales Leadership</em></p></div>"
        }
    ]

    for email in emails:
        print(f"Sending: {email['subject']}")
        sent = send_message(
            service,
            create_message(DEMO_EMAIL, email['subject'], email['body'], email.get('html'))
        )
        if sent and sales_label_id:
            add_label_to_message(service, sent['id'], sales_label_id)


def add_new_notifications(service):
    """Add new system notifications."""
    print("\n" + "="*60)
    print("ADDING NEW SYSTEM NOTIFICATIONS")
    print("="*60 + "\n")

    notifications_label_id = get_label_id(service, "Notifications")

    emails = [
        {
            "subject": "[CRITICAL] Disk Space Alert - Action Required",
            "body": "CRITICAL ALERT\n\nDisk space critically low on production server.\n\nServer: prod-db-01\nDisk Usage: 95% (47.5 GB used of 50 GB)\nThreshold: 90%\nTimestamp: 2024-01-29 08:15:33 UTC\n\nACTION REQUIRED:\n- Clean up old log files\n- Archive historical data\n- Provision additional storage\n\nAlert will escalate in 30 minutes if not resolved.\n\nMonitoring System",
            "html": "<div style='font-family: monospace; background: #ffebee; padding: 20px; border-left: 4px solid #f44336;'><h2 style='color: #c62828; margin-top: 0;'>🚨 CRITICAL ALERT</h2><p><strong>Disk space critically low on production server.</strong></p><table style='margin: 20px 0;'><tr><td><strong>Server:</strong></td><td>prod-db-01</td></tr><tr><td><strong>Disk Usage:</strong></td><td style='color: #f44336;'><strong>95%</strong> (47.5 GB used of 50 GB)</td></tr><tr><td><strong>Threshold:</strong></td><td>90%</td></tr><tr><td><strong>Timestamp:</strong></td><td>2024-01-29 08:15:33 UTC</td></tr></table><div style='background: #fff3e0; padding: 15px; border-radius: 5px; margin: 20px 0;'><h3 style='margin-top: 0;'>⚠️ ACTION REQUIRED:</h3><ul><li>Clean up old log files</li><li>Archive historical data</li><li>Provision additional storage</li></ul></div><p style='color: #f44336;'><strong>Alert will escalate in 30 minutes if not resolved.</strong></p><p><em>Monitoring System</em></p></div>"
        },
        {
            "subject": "Deployment Successful - v2.5.0",
            "body": "Deployment Summary\n\nVersion 2.5.0 has been successfully deployed to production.\n\nDeployed At: 2024-01-29 02:00:15 UTC\nDeployment Duration: 12 minutes\nDowntime: 0 seconds (zero-downtime deployment)\nStatus: SUCCESS\n\nWhat's New:\n- Performance improvements\n- Bug fixes\n- New dashboard widgets\n- API response time optimization\n\nRelease Notes: https://releases.example.com/v2.5.0\n\nAll systems operational.\n\nDeployment Bot",
            "html": "<div style='font-family: monospace; background: #e8f5e9; padding: 20px; border-left: 4px solid #4caf50;'><h2 style='color: #2e7d32; margin-top: 0;'>✅ Deployment Summary</h2><p><strong>Version 2.5.0 has been successfully deployed to production.</strong></p><table style='margin: 20px 0;'><tr><td><strong>Deployed At:</strong></td><td>2024-01-29 02:00:15 UTC</td></tr><tr><td><strong>Deployment Duration:</strong></td><td>12 minutes</td></tr><tr><td><strong>Downtime:</strong></td><td style='color: #4caf50;'><strong>0 seconds</strong> (zero-downtime deployment)</td></tr><tr><td><strong>Status:</strong></td><td style='color: #4caf50;'><strong>SUCCESS ✓</strong></td></tr></table><h3>What's New:</h3><ul><li>⚡ Performance improvements</li><li>🐛 Bug fixes</li><li>📊 New dashboard widgets</li><li>🚀 API response time optimization</li></ul><p>Release Notes: <a href='https://releases.example.com/v2.5.0'>https://releases.example.com/v2.5.0</a></p><p style='background: #4caf50; color: white; padding: 10px; text-align: center; border-radius: 5px;'><strong>All systems operational. 🎉</strong></p><p><em>Deployment Bot</em></p></div>"
        },
        {
            "subject": "Daily Backup Report - 2024-01-29",
            "body": "Daily Backup Report\n\nDate: 2024-01-29\n\nBackups Completed: 5/5\n\nDetails:\n✓ Production DB: 3.1 GB (45 min)\n✓ Staging DB: 890 MB (12 min)\n✓ User Files: 15.2 GB (2h 15 min)\n✓ Config Files: 45 MB (2 min)\n✓ Log Archives: 2.8 GB (30 min)\n\nAll backups stored securely in S3.\nRetention: 30 days for daily backups.\n\nNext backup: 2024-01-30 03:00:00 UTC\n\nBackup System",
            "html": "<div style='font-family: monospace; background: #f5f5f5; padding: 20px;'><h2 style='color: #1976d2;'>📦 Daily Backup Report</h2><p><strong>Date: 2024-01-29</strong></p><p style='font-size: 18px; color: #4caf50;'><strong>Backups Completed: 5/5 ✓</strong></p><h3>Details:</h3><table style='width: 100%; border-collapse: collapse;'><tr style='background: #e8f5e9;'><td style='padding: 10px;'>✓ Production DB</td><td style='padding: 10px;'>3.1 GB (45 min)</td></tr><tr><td style='padding: 10px;'>✓ Staging DB</td><td style='padding: 10px;'>890 MB (12 min)</td></tr><tr style='background: #e8f5e9;'><td style='padding: 10px;'>✓ User Files</td><td style='padding: 10px;'>15.2 GB (2h 15 min)</td></tr><tr><td style='padding: 10px;'>✓ Config Files</td><td style='padding: 10px;'>45 MB (2 min)</td></tr><tr style='background: #e8f5e9;'><td style='padding: 10px;'>✓ Log Archives</td><td style='padding: 10px;'>2.8 GB (30 min)</td></tr></table><p style='margin-top: 20px;'><em>All backups stored securely in S3.<br>Retention: 30 days for daily backups.</em></p><p><strong>Next backup:</strong> 2024-01-30 03:00:00 UTC</p><p><em>Backup System</em></p></div>"
        }
    ]

    for email in emails:
        print(f"Sending: {email['subject']}")
        sent = send_message(
            service,
            create_message(DEMO_EMAIL, email['subject'], email['body'], email.get('html'))
        )
        if sent and notifications_label_id:
            add_label_to_message(service, sent['id'], notifications_label_id)


def add_product_updates(service):
    """Add product update emails."""
    print("\n" + "="*60)
    print("ADDING PRODUCT UPDATES")
    print("="*60 + "\n")

    marketing_label_id = get_label_id(service, "Marketing")

    emails = [
        {
            "subject": "🎯 Product Roadmap Update - Q1 2024",
            "body": "Product Updates\n\nHere's what we're working on in Q1 2024:\n\nIn Development:\n- Mobile app (iOS & Android)\n- Advanced analytics dashboard\n- API v3 with GraphQL support\n- SSO integration (SAML & OAuth)\n\nPlanned for Q2:\n- Webhook notifications\n- Custom workflow automation\n- White-label options\n- Advanced permissions\n\nWe'd love your feedback! What features matter most to you?\n\nReply to this email or join our community forum.\n\nProduct Team",
            "html": "<div style='font-family: Arial, sans-serif; max-width: 600px;'><h1 style='color: #1976d2;'>🎯 Product Roadmap Update - Q1 2024</h1><p><strong>Here's what we're working on in Q1 2024:</strong></p><div style='background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0;'><h3 style='margin-top: 0; color: #1976d2;'>🚀 In Development:</h3><ul style='line-height: 1.8;'><li>📱 Mobile app (iOS & Android)</li><li>📊 Advanced analytics dashboard</li><li>🔌 API v3 with GraphQL support</li><li>🔐 SSO integration (SAML & OAuth)</li></ul></div><div style='background: #f3e5f5; padding: 20px; border-radius: 8px; margin: 20px 0;'><h3 style='margin-top: 0; color: #7b1fa2;'>📅 Planned for Q2:</h3><ul style='line-height: 1.8;'><li>🔔 Webhook notifications</li><li>⚙️ Custom workflow automation</li><li>🎨 White-label options</li><li>👥 Advanced permissions</li></ul></div><div style='background: #fff3e0; padding: 20px; border-radius: 8px; margin: 20px 0;'><p style='margin: 0; font-size: 16px;'><strong>💬 We'd love your feedback!</strong><br>What features matter most to you?</p></div><p>Reply to this email or join our community forum.</p><p><em>Product Team</em></p></div>"
        }
    ]

    for email in emails:
        print(f"Sending: {email['subject']}")
        sent = send_message(
            service,
            create_message(DEMO_EMAIL, email['subject'], email['body'], email.get('html'))
        )
        if sent and marketing_label_id:
            add_label_to_message(service, sent['id'], marketing_label_id)


def main():
    """Main function to add incremental data."""
    print("\n" + "="*60)
    print("GMAIL INCREMENTAL DATA SCRIPT")
    print("="*60)
    print(f"\nDemo Account: {DEMO_EMAIL}")
    print("\nThis script adds NEW data to demonstrate incremental sync:")
    print("  - 3 new customer support tickets")
    print("  - 2 sales update emails")
    print("  - 3 new system notifications")
    print("  - 1 product update email")
    print("\nTotal: ~9 new emails")
    print("\nUse this to demonstrate CDC (Change Data Capture) capabilities")
    print("with Gmail's History API and historyId cursor.")
    print("="*60 + "\n")

    print("⚠️  IMPORTANT:")
    print("   1. Run populate_demo_data.py FIRST (if you haven't already)")
    print("   2. Do an initial data ingestion in Databricks")
    print("   3. Then run THIS script")
    print("   4. Run incremental ingestion to see only new emails synced")
    print()

    response = input("Continue adding incremental data? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("\nAborted by user.")
        return

    try:
        service = get_gmail_service()

        # Get current historyId for reference
        profile = service.users().getProfile(userId='me').execute()
        current_history_id = profile.get('historyId')
        print(f"\n📍 Current historyId: {current_history_id}")
        print("   (Use this as the starting point for incremental sync)\n")

        # Add incremental data
        add_new_support_tickets(service)
        add_sales_updates(service)
        add_new_notifications(service)
        add_product_updates(service)

        # Get new historyId
        profile = service.users().getProfile(userId='me').execute()
        new_history_id = profile.get('historyId')

        print("\n" + "="*60)
        print("✓ INCREMENTAL DATA ADDED!")
        print("="*60)
        print("\n📊 Sync Details:")
        print(f"   Previous historyId: {current_history_id}")
        print(f"   New historyId: {new_history_id}")
        print(f"\n✨ Added ~9 new emails")
        print("\n🔄 Next Steps:")
        print("   1. Run incremental ingestion in Databricks")
        print("   2. Observe that only NEW emails are synced (efficient CDC)")
        print("   3. Verify historyId cursor is updated")
        print("   4. Run this script again for additional incremental batches")
        print("\n" + "="*60 + "\n")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
