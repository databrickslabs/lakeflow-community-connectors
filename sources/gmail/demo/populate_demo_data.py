#!/usr/bin/env python3
"""
Script to populate the demo Gmail account with realistic sample data.

This script creates diverse email content to demonstrate the Gmail connector's capabilities:
- Customer support threads
- Sales communications
- System notifications
- Emails with different labels
- Draft messages
- Emails with various metadata (priority, read/unread, etc.)

Setup:
1. Set up OAuth credentials for DEMO_EMAIL_PLACEHOLDER
2. Run: python sources/gmail/demo/populate_demo_data.py
"""

import base64
import json
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Gmail API scopes - need send permission to create demo data
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.labels',
]

# Demo account email - actual credentials stored securely in Keeper vault
# For evaluation: Contact hackathon organizers for demo account access
# For testing: Use your own Gmail account (see DEMO_ACCESS.md)
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
            print("\nYou need to authenticate to populate demo data.")
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


def create_message(to, subject, body_text, body_html=None, in_reply_to=None, references=None):
    """Create an email message."""
    message = MIMEMultipart('alternative')
    message['To'] = to
    message['From'] = DEMO_EMAIL
    message['Subject'] = subject

    if in_reply_to:
        message['In-Reply-To'] = in_reply_to
    if references:
        message['References'] = references

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


def create_label(service, label_name):
    """Create a custom label."""
    try:
        label = service.users().labels().create(
            userId='me',
            body={'name': label_name, 'labelListVisibility': 'labelShow', 'messageListVisibility': 'show'}
        ).execute()
        print(f"  ✓ Created label: {label_name}")
        return label['id']
    except Exception as e:
        if 'already exists' in str(e).lower():
            # Label exists, get its ID
            labels = service.users().labels().list(userId='me').execute()
            for lbl in labels.get('labels', []):
                if lbl['name'] == label_name:
                    print(f"  ℹ Label already exists: {label_name}")
                    return lbl['id']
        print(f"  ✗ Error creating label: {e}")
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


def create_draft(service, subject, body_text):
    """Create a draft message."""
    try:
        message = create_message(DEMO_EMAIL, subject, body_text)
        draft = service.users().drafts().create(
            userId='me',
            body={'message': message}
        ).execute()
        print(f"  ✓ Created draft: {subject}")
        return draft
    except Exception as e:
        print(f"  ✗ Error creating draft: {e}")
        return None


def populate_customer_support_threads(service):
    """Create customer support conversation threads."""
    print("\n" + "="*60)
    print("CREATING CUSTOMER SUPPORT THREADS")
    print("="*60 + "\n")

    # Create custom label
    support_label_id = create_label(service, "Customer Support")

    threads = [
        {
            "subject": "Login Issue - Unable to Access Dashboard",
            "messages": [
                {
                    "body": "Hi Support Team,\n\nI'm unable to log into my dashboard. I keep getting an 'Invalid credentials' error even though I'm sure my password is correct.\n\nCan you help?\n\nThanks,\nJohn Smith",
                    "html": "<p>Hi Support Team,</p><p>I'm unable to log into my dashboard. I keep getting an <strong>'Invalid credentials'</strong> error even though I'm sure my password is correct.</p><p>Can you help?</p><p>Thanks,<br>John Smith</p>"
                },
                {
                    "body": "Hi John,\n\nThank you for reaching out. I've checked your account and it appears your account was locked due to multiple failed login attempts.\n\nI've unlocked it for you. Please try logging in again and let me know if you still have issues.\n\nBest regards,\nSupport Team",
                    "html": "<p>Hi John,</p><p>Thank you for reaching out. I've checked your account and it appears your account was locked due to multiple failed login attempts.</p><p>I've unlocked it for you. Please try logging in again and let me know if you still have issues.</p><p>Best regards,<br>Support Team</p>"
                },
                {
                    "body": "Perfect! I can log in now. Thank you so much for the quick response!\n\nJohn",
                    "html": "<p>Perfect! I can log in now. Thank you so much for the quick response!</p><p>John</p>"
                }
            ]
        },
        {
            "subject": "Feature Request: Export to CSV",
            "messages": [
                {
                    "body": "Hello,\n\nIs there a way to export my data to CSV format? I need to share reports with my team who don't have access to the platform.\n\nRegards,\nSarah Johnson",
                    "html": "<p>Hello,</p><p>Is there a way to export my data to CSV format? I need to share reports with my team who don't have access to the platform.</p><p>Regards,<br>Sarah Johnson</p>"
                },
                {
                    "body": "Hi Sarah,\n\nGreat question! Yes, you can export to CSV by clicking the 'Export' button in the top right corner of any report.\n\nWe're also working on scheduled exports which will be available next month.\n\nLet me know if you need any other features!\n\nBest,\nSupport Team",
                    "html": "<p>Hi Sarah,</p><p>Great question! Yes, you can export to CSV by clicking the <strong>'Export'</strong> button in the top right corner of any report.</p><p>We're also working on <em>scheduled exports</em> which will be available next month.</p><p>Let me know if you need any other features!</p><p>Best,<br>Support Team</p>"
                }
            ]
        },
        {
            "subject": "Billing Question - Unexpected Charge",
            "messages": [
                {
                    "body": "Hi,\n\nI noticed a charge of $99 on my credit card but I thought I was on the $49/month plan. Can you explain this?\n\nMike Chen",
                    "html": "<p>Hi,</p><p>I noticed a charge of <strong>$99</strong> on my credit card but I thought I was on the <strong>$49/month</strong> plan. Can you explain this?</p><p>Mike Chen</p>"
                },
                {
                    "body": "Hi Mike,\n\nThanks for reaching out. I've reviewed your account:\n\n- Base plan: $49/month\n- Additional users (2): $25 each\n- Total: $99/month\n\nYou added two team members on January 15th which increased your monthly cost. Would you like to review your plan?\n\nBest regards,\nBilling Team",
                    "html": "<p>Hi Mike,</p><p>Thanks for reaching out. I've reviewed your account:</p><ul><li>Base plan: $49/month</li><li>Additional users (2): $25 each</li><li>Total: $99/month</li></ul><p>You added two team members on January 15th which increased your monthly cost. Would you like to review your plan?</p><p>Best regards,<br>Billing Team</p>"
                }
            ]
        }
    ]

    for thread_data in threads:
        print(f"\nCreating thread: {thread_data['subject']}")
        message_id = None
        references = None

        for idx, msg in enumerate(thread_data['messages']):
            sent = send_message(
                service,
                create_message(
                    DEMO_EMAIL,
                    f"Re: {thread_data['subject']}" if idx > 0 else thread_data['subject'],
                    msg['body'],
                    msg.get('html'),
                    in_reply_to=message_id,
                    references=references
                )
            )

            if sent:
                if support_label_id:
                    add_label_to_message(service, sent['id'], support_label_id)

                if not message_id:
                    message_id = sent['id']
                    references = f"<{message_id}>"
                else:
                    references = f"{references} <{sent['id']}>"


def populate_sales_communications(service):
    """Create sales-related emails."""
    print("\n" + "="*60)
    print("CREATING SALES COMMUNICATIONS")
    print("="*60 + "\n")

    sales_label_id = create_label(service, "Sales")

    emails = [
        {
            "subject": "Interested in Your Enterprise Plan",
            "body": "Hi there,\n\nI'm reaching out from Acme Corp. We're interested in your Enterprise plan for our team of 50 users.\n\nCould you send over pricing details and schedule a demo?\n\nBest,\nEmily Rodriguez\nCTO, Acme Corp",
            "html": "<p>Hi there,</p><p>I'm reaching out from <strong>Acme Corp</strong>. We're interested in your Enterprise plan for our team of 50 users.</p><p>Could you send over pricing details and schedule a demo?</p><p>Best,<br>Emily Rodriguez<br><em>CTO, Acme Corp</em></p>"
        },
        {
            "subject": "Following up on our call",
            "body": "Hi Emily,\n\nThank you for the great conversation yesterday! As discussed, here's a summary:\n\n- Enterprise plan: $2,500/month for 50 users\n- Includes: Priority support, SSO, custom integrations\n- Demo scheduled: Next Tuesday 2pm EST\n\nLooking forward to showing you the platform!\n\nCheers,\nAlex Thompson\nSales Director",
            "html": "<h3>Hi Emily,</h3><p>Thank you for the great conversation yesterday! As discussed, here's a summary:</p><ul><li><strong>Enterprise plan:</strong> $2,500/month for 50 users</li><li><strong>Includes:</strong> Priority support, SSO, custom integrations</li><li><strong>Demo scheduled:</strong> Next Tuesday 2pm EST</li></ul><p>Looking forward to showing you the platform!</p><p>Cheers,<br>Alex Thompson<br><em>Sales Director</em></p>"
        },
        {
            "subject": "Welcome to Our Platform! 🎉",
            "body": "Dear Valued Customer,\n\nWelcome aboard! We're thrilled to have you join our growing community.\n\nYour account is now active. Here are your next steps:\n\n1. Complete your profile\n2. Invite your team members\n3. Explore our tutorials\n4. Schedule onboarding call (optional)\n\nIf you need any help, our support team is here 24/7.\n\nBest regards,\nThe Team",
            "html": "<div style='font-family: Arial, sans-serif;'><h2>Dear Valued Customer,</h2><p>Welcome aboard! We're thrilled to have you join our growing community. 🎉</p><p>Your account is now active. Here are your next steps:</p><ol><li>Complete your profile</li><li>Invite your team members</li><li>Explore our tutorials</li><li>Schedule onboarding call (optional)</li></ol><p>If you need any help, our support team is here <strong>24/7</strong>.</p><p>Best regards,<br><em>The Team</em></p></div>"
        },
        {
            "subject": "Contract Renewal - Special Offer Inside",
            "body": "Hi there,\n\nYour annual contract is up for renewal next month. As a valued customer, we'd like to offer you:\n\n- 20% discount on annual renewal\n- Free upgrade to Pro tier for 3 months\n- Dedicated account manager\n\nThis offer expires in 7 days. Reply to lock in your savings!\n\nBest,\nCustomer Success Team",
            "html": "<p>Hi there,</p><p>Your annual contract is up for renewal next month. As a valued customer, we'd like to offer you:</p><ul><li>✅ <strong>20% discount</strong> on annual renewal</li><li>✅ Free upgrade to Pro tier for 3 months</li><li>✅ Dedicated account manager</li></ul><p><strong>This offer expires in 7 days.</strong> Reply to lock in your savings!</p><p>Best,<br>Customer Success Team</p>"
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


def populate_system_notifications(service):
    """Create system notification emails."""
    print("\n" + "="*60)
    print("CREATING SYSTEM NOTIFICATIONS")
    print("="*60 + "\n")

    notifications_label_id = create_label(service, "Notifications")

    emails = [
        {
            "subject": "[ALERT] Database Backup Completed Successfully",
            "body": "System Notification\n\nDatabase backup completed at 2024-01-28 03:00:00 UTC\n\nDetails:\n- Database: production_db\n- Size: 2.3 GB\n- Duration: 45 minutes\n- Status: SUCCESS\n- Backup location: s3://backups/2024-01-28/\n\nNext backup scheduled: 2024-01-29 03:00:00 UTC",
            "html": "<div style='font-family: monospace; background: #f5f5f5; padding: 20px;'><h3 style='color: #2e7d32;'>✓ System Notification</h3><p><strong>Database backup completed at 2024-01-28 03:00:00 UTC</strong></p><table><tr><td><strong>Database:</strong></td><td>production_db</td></tr><tr><td><strong>Size:</strong></td><td>2.3 GB</td></tr><tr><td><strong>Duration:</strong></td><td>45 minutes</td></tr><tr><td><strong>Status:</strong></td><td style='color: green;'>SUCCESS</td></tr><tr><td><strong>Backup location:</strong></td><td>s3://backups/2024-01-28/</td></tr></table><p>Next backup scheduled: 2024-01-29 03:00:00 UTC</p></div>"
        },
        {
            "subject": "[WARNING] High Memory Usage Detected",
            "body": "System Alert\n\nHigh memory usage detected on server: prod-web-01\n\nCurrent usage: 87%\nThreshold: 85%\nTimestamp: 2024-01-28 14:23:15 UTC\n\nAction: Auto-scaling triggered, spinning up additional instance.\n\nMonitor dashboard: https://monitoring.example.com",
            "html": "<div style='font-family: monospace; background: #fff3e0; padding: 20px; border-left: 4px solid #ff9800;'><h3 style='color: #f57c00;'>⚠ System Alert</h3><p><strong>High memory usage detected on server: prod-web-01</strong></p><ul><li><strong>Current usage:</strong> 87%</li><li><strong>Threshold:</strong> 85%</li><li><strong>Timestamp:</strong> 2024-01-28 14:23:15 UTC</li></ul><p><strong>Action:</strong> Auto-scaling triggered, spinning up additional instance.</p><p>Monitor dashboard: <a href='https://monitoring.example.com'>https://monitoring.example.com</a></p></div>"
        },
        {
            "subject": "Weekly Performance Report - Week 4",
            "body": "Weekly Performance Summary\n\nPeriod: Jan 22 - Jan 28, 2024\n\nKey Metrics:\n- API Requests: 2.4M (+12% vs last week)\n- Average Response Time: 145ms (-8ms)\n- Error Rate: 0.02% (↓ 0.01%)\n- Active Users: 15,234 (+234)\n- New Signups: 456\n\nTop Features Used:\n1. Dashboard (45%)\n2. Reports (28%)\n3. API Integration (18%)\n4. Data Export (9%)\n\nFull report: https://analytics.example.com/weekly",
            "html": "<div style='font-family: Arial, sans-serif;'><h2 style='color: #1976d2;'>Weekly Performance Summary 📊</h2><p><strong>Period:</strong> Jan 22 - Jan 28, 2024</p><h3>Key Metrics:</h3><table style='border-collapse: collapse; width: 100%;'><tr style='background: #e3f2fd;'><td style='padding: 10px;'><strong>API Requests</strong></td><td style='padding: 10px;'>2.4M <span style='color: green;'>(+12% vs last week)</span></td></tr><tr><td style='padding: 10px;'><strong>Average Response Time</strong></td><td style='padding: 10px;'>145ms <span style='color: green;'>(-8ms)</span></td></tr><tr style='background: #e3f2fd;'><td style='padding: 10px;'><strong>Error Rate</strong></td><td style='padding: 10px;'>0.02% <span style='color: green;'>(↓ 0.01%)</span></td></tr><tr><td style='padding: 10px;'><strong>Active Users</strong></td><td style='padding: 10px;'>15,234 <span style='color: green;'>(+234)</span></td></tr><tr style='background: #e3f2fd;'><td style='padding: 10px;'><strong>New Signups</strong></td><td style='padding: 10px;'>456</td></tr></table><h3>Top Features Used:</h3><ol><li>Dashboard (45%)</li><li>Reports (28%)</li><li>API Integration (18%)</li><li>Data Export (9%)</li></ol><p><a href='https://analytics.example.com/weekly'>View Full Report →</a></p></div>"
        },
        {
            "subject": "Security Update: SSL Certificate Renewed",
            "body": "Security Notice\n\nYour SSL certificate has been automatically renewed.\n\nDomain: api.example.com\nIssuer: Let's Encrypt\nValid From: 2024-01-28\nValid Until: 2024-04-28\nStatus: Active\n\nNo action required on your part.\n\n- Security Team",
            "html": "<div style='font-family: monospace; background: #e8f5e9; padding: 20px; border-left: 4px solid #4caf50;'><h3 style='color: #2e7d32;'>🔒 Security Notice</h3><p><strong>Your SSL certificate has been automatically renewed.</strong></p><table><tr><td><strong>Domain:</strong></td><td>api.example.com</td></tr><tr><td><strong>Issuer:</strong></td><td>Let's Encrypt</td></tr><tr><td><strong>Valid From:</strong></td><td>2024-01-28</td></tr><tr><td><strong>Valid Until:</strong></td><td>2024-04-28</td></tr><tr><td><strong>Status:</strong></td><td style='color: green;'>Active ✓</td></tr></table><p><em>No action required on your part.</em></p><p>- Security Team</p></div>"
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


def populate_marketing_emails(service):
    """Create marketing emails."""
    print("\n" + "="*60)
    print("CREATING MARKETING EMAILS")
    print("="*60 + "\n")

    marketing_label_id = create_label(service, "Marketing")

    emails = [
        {
            "subject": "🚀 New Feature: Real-time Collaboration",
            "body": "Hi there!\n\nWe're excited to announce a new feature that's going to transform how your team works together: Real-time Collaboration!\n\nWhat's New:\n- Live cursor tracking\n- Instant updates\n- Comment threads\n- Version history\n\nTry it now: https://app.example.com/collaboration\n\nHappy collaborating!\nThe Product Team",
            "html": "<div style='font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;'><h1 style='color: #1976d2;'>🚀 New Feature: Real-time Collaboration</h1><p>Hi there!</p><p>We're excited to announce a new feature that's going to <strong>transform</strong> how your team works together: <strong>Real-time Collaboration!</strong></p><h3>What's New:</h3><ul style='font-size: 16px; line-height: 1.6;'><li>✨ Live cursor tracking</li><li>⚡ Instant updates</li><li>💬 Comment threads</li><li>📜 Version history</li></ul><p style='text-align: center; margin: 30px 0;'><a href='https://app.example.com/collaboration' style='background: #1976d2; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; font-weight: bold;'>Try It Now →</a></p><p>Happy collaborating!<br><em>The Product Team</em></p></div>"
        },
        {
            "subject": "📊 Case Study: How Acme Corp Increased Productivity by 40%",
            "body": "Success Story\n\nSee how Acme Corp used our platform to transform their workflow and boost team productivity by 40%.\n\nKey Takeaways:\n- Reduced manual tasks by 60%\n- Improved collaboration across 5 departments\n- Saved 20 hours per week per team member\n- ROI achieved in 3 months\n\nRead the full case study: https://example.com/case-studies/acme\n\nWant similar results? Let's talk: https://example.com/demo",
            "html": "<div style='font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;'><h2 style='color: #2e7d32;'>📊 Success Story</h2><p style='font-size: 18px;'>See how <strong>Acme Corp</strong> used our platform to transform their workflow and boost team productivity by <strong style='color: #2e7d32; font-size: 24px;'>40%</strong>.</p><h3>Key Takeaways:</h3><div style='background: #e8f5e9; padding: 20px; border-radius: 8px; margin: 20px 0;'><ul style='font-size: 16px; line-height: 1.8;'><li>🎯 Reduced manual tasks by <strong>60%</strong></li><li>🤝 Improved collaboration across <strong>5 departments</strong></li><li>⏰ Saved <strong>20 hours per week</strong> per team member</li><li>💰 ROI achieved in <strong>3 months</strong></li></ul></div><p style='text-align: center; margin: 30px 0;'><a href='https://example.com/case-studies/acme' style='background: #2e7d32; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; font-weight: bold;'>Read Full Case Study →</a></p><p style='text-align: center;'><a href='https://example.com/demo' style='color: #1976d2;'>Want similar results? Let's talk →</a></p></div>"
        },
        {
            "subject": "Limited Time: 30% Off Annual Plans",
            "body": "Special Offer - 30% Off!\n\nFor a limited time, save 30% on all annual plans.\n\nPricing:\n- Starter: $35/mo (was $49/mo)\n- Professional: $70/mo (was $99/mo)\n- Enterprise: Custom pricing\n\nOffer expires: January 31st, 2024\n\nUpgrade now: https://example.com/pricing\n\nDon't miss out!\nSales Team",
            "html": "<div style='font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; border-radius: 10px;'><h1 style='text-align: center; font-size: 36px; margin: 0;'>🎉 Special Offer</h1><h2 style='text-align: center; font-size: 48px; margin: 10px 0;'>30% OFF!</h2><p style='text-align: center; font-size: 18px;'>For a limited time, save 30% on all annual plans.</p><div style='background: rgba(255,255,255,0.1); padding: 20px; border-radius: 8px; margin: 30px 0;'><h3>Pricing:</h3><ul style='font-size: 18px; line-height: 2;'><li><strong>Starter:</strong> $35/mo <span style='text-decoration: line-through; opacity: 0.7;'>(was $49/mo)</span></li><li><strong>Professional:</strong> $70/mo <span style='text-decoration: line-through; opacity: 0.7;'>(was $99/mo)</span></li><li><strong>Enterprise:</strong> Custom pricing</li></ul></div><p style='text-align: center; font-size: 20px;'><strong>⏰ Offer expires: January 31st, 2024</strong></p><p style='text-align: center; margin: 30px 0;'><a href='https://example.com/pricing' style='background: white; color: #667eea; padding: 15px 40px; text-decoration: none; border-radius: 5px; font-weight: bold; font-size: 18px;'>Upgrade Now →</a></p><p style='text-align: center;'><em>Don't miss out!</em><br>Sales Team</p></div>"
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


def populate_drafts(service):
    """Create draft messages."""
    print("\n" + "="*60)
    print("CREATING DRAFTS")
    print("="*60 + "\n")

    drafts = [
        {
            "subject": "Q1 Planning Meeting - Draft Agenda",
            "body": "Hi Team,\n\nHere's the draft agenda for our Q1 planning meeting next week:\n\n1. Review Q4 results (15 min)\n2. Q1 goals and OKRs (30 min)\n3. Resource allocation (20 min)\n4. Budget discussion (15 min)\n5. Q&A (10 min)\n\nPlease review and let me know if I'm missing anything.\n\n[TO DO: Add location and Zoom link]\n\nThanks!"
        },
        {
            "subject": "Feature Request Follow-up",
            "body": "Hi Sarah,\n\nThanks for your feature request about CSV exports. I wanted to follow up on a few questions:\n\n1. What format would you prefer for dates?\n2. Do you need the ability to customize columns?\n3. How often would you use this feature?\n\n[TO DO: Add screenshots of current export options]\n\nLooking forward to your feedback!"
        },
        {
            "subject": "Newsletter - January Edition",
            "body": "📬 January Newsletter\n\nWhat's New This Month:\n\n[TO DO: Add feature highlights]\n[TO DO: Add customer testimonial]\n[TO DO: Add upcoming events]\n\nTip of the Month:\n[TO DO: Add useful tip]\n\nHave feedback? Reply to this email!\n\nThe Team"
        },
        {
            "subject": "Partnership Opportunity",
            "body": "Dear [Company Name],\n\nI hope this email finds you well. I'm reaching out to explore a potential partnership between our companies.\n\n[TO DO: Research their company]\n[TO DO: Add specific partnership proposal]\n[TO DO: Add our company overview]\n\nI'd love to schedule a call to discuss this further. Are you available next week?\n\nBest regards,\n[Your Name]"
        },
        {
            "subject": "Thank You for Your Purchase!",
            "body": "Dear Customer,\n\nThank you for your recent purchase! We're thrilled to have you as a customer.\n\nOrder Details:\n[TO DO: Add order number]\n[TO DO: Add items purchased]\n[TO DO: Add total amount]\n\nYour order will ship within 2-3 business days. You'll receive tracking information once it's on its way.\n\n[TO DO: Add customer support contact info]\n\nThank you for choosing us!\n\nThe Team"
        }
    ]

    for draft in drafts:
        create_draft(service, draft['subject'], draft['body'])


def main():
    """Main function to populate demo data."""
    print("\n" + "="*60)
    print("GMAIL DEMO DATA POPULATION SCRIPT")
    print("="*60)
    print(f"\nDemo Account: {DEMO_EMAIL}")
    print("\nThis script will create:")
    print("  - 3 customer support conversation threads")
    print("  - 4 sales-related emails")
    print("  - 4 system notification emails")
    print("  - 3 marketing emails")
    print("  - 5 draft messages")
    print("  - Custom labels for organization")
    print("\nTotal: ~20 emails demonstrating various connector capabilities")
    print("="*60 + "\n")

    response = input("Continue with data population? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("\nAborted by user.")
        return

    try:
        service = get_gmail_service()

        # Populate all data types
        populate_customer_support_threads(service)
        populate_sales_communications(service)
        populate_system_notifications(service)
        populate_marketing_emails(service)
        populate_drafts(service)

        print("\n" + "="*60)
        print("✓ DEMO DATA POPULATION COMPLETE!")
        print("="*60)
        print("\nYour demo account now contains:")
        print("  ✓ Multiple conversation threads")
        print("  ✓ Emails with rich HTML content")
        print("  ✓ Custom labels (Customer Support, Sales, Notifications, Marketing)")
        print("  ✓ Draft messages")
        print("  ✓ Various email types and formats")
        print("\nYou can now:")
        print("  1. Run the connector tests")
        print("  2. Ingest data into Databricks")
        print("  3. Demo incremental sync by running add_incremental_data.py")
        print("\n" + "="*60 + "\n")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
