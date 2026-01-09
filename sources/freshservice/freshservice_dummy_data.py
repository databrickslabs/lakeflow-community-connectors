"""
Freshservice Dummy Data Generator

This module generates realistic dummy data for all Freshservice object types
as documented in freshservice_api_doc.md. Useful for testing connectors.

Includes POST API functionality to send generated data to Freshservice.
"""

from datetime import datetime, timedelta
import random
from typing import Any
import requests
import base64
import time

# ============================================================================
# FRESHSERVICE API CONFIGURATION
# ============================================================================

FRESHSERVICE_DOMAIN = "https://databricks042.freshservice.com"
FRESHSERVICE_API_KEY = ""  # <--------------------------------------------------- INSERT API KEY HERE
FRESHSERVICE_BASE_URL = f"{FRESHSERVICE_DOMAIN}/api/v2"

# Valid agent IDs in the Freshservice instance
VALID_AGENT_IDS = [58000750485, 58000750479, 58000750502]

# Valid requester IDs in the Freshservice instance
VALID_REQUESTER_IDS = [58000750484, 58000750482]

# Valid categories in Freshservice (from API error messages)
VALID_CATEGORIES = [
    "Hardware", "Software", "Network", "Other", "Office Applications",
    "Office Furniture", "Office Equipment", "Employee Benefits",
    "Employee Records and Documents", "Employee Onboarding/Offboarding",
    "Talent Management", "Employee Relations", "Workplace Access and Security",
    "Travel", "Building and Grounds Maintenance", "Legal Document Creation",
    "Legal Review - Vendor Documents", "Legal Review - Customer Documents",
    "Vendor Document Review", "Payroll", "Vendor Payment", "Customer Payment",
    "Reimbursements and Advances"
]

# Valid sub_categories per category (from API error messages)
VALID_SUB_CATEGORIES = {
    "Hardware": [],  # No sub_category for Hardware
    "Software": ["MS Office", "Adobe Reader", "Windows", "Chrome"],
    "Network": ["Access", "Connectivity"],
    "Other": [],
    "Office Applications": [],
}

# API endpoints for each object type
API_ENDPOINTS = {
    "tickets": "/tickets",
    "agents": "/agents",
    "requesters": "/requesters",
    "assets": "/assets",
    "problems": "/problems",
    "changes": "/changes",
    "releases": "/releases",
    "groups": "/groups",
    "departments": "/departments",
    "vendors": "/vendors",
    "products": "/products",
    "contracts": "/contracts",
    "purchase_orders": "/purchase_orders",
    "service_catalog_items": "/service_catalog/items",
    "solutions": "/solutions/articles",
    "locations": "/locations",
    "announcements": "/announcements",
}


def get_auth_header() -> dict[str, str]:
    """
    Generate the authentication header for Freshservice API.
    Uses Basic Authentication with API key as username and 'X' as password.
    """
    credentials = f"{FRESHSERVICE_API_KEY}:X"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    return {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }


def post_to_freshservice(endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
    """
    Send a POST request to Freshservice API.
    
    Args:
        endpoint: API endpoint (e.g., '/tickets')
        data: Dictionary containing the request body
        
    Returns:
        API response as a dictionary
    """
    url = f"{FRESHSERVICE_BASE_URL}{endpoint}"
    headers = get_auth_header()
    
    response = requests.post(url, json=data, headers=headers)
    
    if response.status_code == 429:
        # Rate limited - wait and retry
        retry_after = int(response.headers.get("Retry-After", 60))
        print(f"Rate limited. Waiting {retry_after} seconds...")
        time.sleep(retry_after)
        response = requests.post(url, json=data, headers=headers)
    
    if response.status_code not in [200, 201]:
        print(f"Error {response.status_code}: {response.text}")
        return {"error": response.status_code, "message": response.text}
    
    return response.json()


def get_random_agent_id() -> int:
    """Get a random valid agent ID."""
    return random.choice(VALID_AGENT_IDS)


def get_random_requester_id() -> int:
    """Get a random valid requester ID (using agent IDs as they are valid users)."""
    return random.choice(VALID_REQUESTER_IDS)


def generate_iso_datetime(days_ago: int = 0, hours_ago: int = 0) -> str:
    """Generate an ISO 8601 datetime string."""
    dt = datetime.utcnow() - timedelta(days=days_ago, hours=hours_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def generate_future_iso_datetime(days: int = 0, hours: int = 0) -> str:
    """Generate an ISO 8601 datetime string."""
    dt = datetime.utcnow() + timedelta(days=days, hours=hours)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_date(days_ago: int = 0) -> str:
    """Generate a date string in YYYY-MM-DD format."""
    dt = datetime.utcnow() - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%d")


# ============================================================================
# TICKETS
# ============================================================================

def generate_tickets(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy ticket data."""
    tickets = []
    
    subjects = [
        "Laptop not working",
        "Cannot access email",
        "VPN connection issues",
        "Software installation request",
        "Password reset needed",
        "Printer not printing",
        "Network connectivity issue",
        "New employee onboarding",
        "Monitor flickering",
        "Slack not syncing"
    ]
    
    # Use valid categories from Freshservice
    ticket_categories = ["Hardware", "Software", "Network", "Other"]
    
    types = ["Incident", "Service Request"]
    
    for i in range(1, count + 1):
        category = random.choice(ticket_categories)
        # Get valid sub_category for this category, or None if not available
        valid_subs = VALID_SUB_CATEGORIES.get(category, [])
        sub_category = random.choice(valid_subs) if valid_subs else None
        
        status = random.choice([2, 3, 4, 5])  # Open, Pending, Resolved, Closed
        priority = random.choice([1, 2, 3, 4])  # Low, Medium, High, Urgent
        source = random.choice([1, 2, 3, 4, 10, 15])  # Email, Portal, Phone, Chat, Slack, MS Teams
        
        created_days_ago = random.randint(1, 30)
        updated_hours_ago = random.randint(0, created_days_ago * 24)
        
        ticket = {
            "id": 1000000000 + i,
            "workspace_id": 1 if random.random() > 0.5 else None,
            "subject": random.choice(subjects),
            "description": f"<div>This is a detailed description of the issue #{i}. The user is experiencing problems and needs assistance.</div>",
            "description_text": f"This is a detailed description of the issue #{i}. The user is experiencing problems and needs assistance.",
            "type": random.choice(types),
            "status": status,
            "priority": priority,
            "source": source,
            "requester_id": get_random_requester_id(),
            "responder_id": get_random_agent_id() if status != 2 else None,
            "group_id": None,  # Will be populated if groups exist
            "department_id": None,  # Will be populated if departments exist
            "company_id": None,
            "product_id": None,
            "category": category,
            "sub_category": sub_category,
            "item_category": None,
            "impact": random.choice([1, 2, 3]) if random.random() > 0.5 else None,
            "urgency": random.choice([1, 2, 3]) if random.random() > 0.5 else None,
            "due_by": generate_iso_datetime(days_ago=-random.randint(1, 7)),
            "fr_due_by": generate_iso_datetime(days_ago=-random.randint(0, 2)),
            "is_escalated": random.random() > 0.8,
            "fr_escalated": random.random() > 0.9,
            "spam": False,
            "deleted": False,
            "email": f"user{random.randint(1, 20)}@example.com",
            "phone": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "email_config_id": None,
            "sla_policy_id": None,
            "cc_emails": [f"cc{j}@example.com" for j in range(random.randint(0, 2))],
            "fwd_emails": [],
            "reply_cc_emails": [],
            "to_emails": [],
            "attachments": [],
            "custom_fields": {},
            "tags": random.sample(["urgent", "hardware", "software", "network", "vip", "escalated"], k=random.randint(0, 3)),
            "created_at": generate_iso_datetime(days_ago=created_days_ago),
            "updated_at": generate_iso_datetime(hours_ago=updated_hours_ago)
        }
        tickets.append(ticket)
    
    return tickets


def generate_ticket_payload(ticket: dict[str, Any]) -> dict[str, Any]:
    """
    Generate a payload for creating a ticket via POST API.
    Only includes fields that are accepted by the create ticket API.
    """
    payload = {
        "subject": ticket["subject"],
        "description": ticket["description"],
        "status": ticket["status"],
        "priority": ticket["priority"],
        "source": ticket["source"],
        "requester_id": ticket["requester_id"],
        "responder_id": ticket["responder_id"],
    }
    
    # Add optional fields if present - only add responder_id if it's a valid agent ID
    # if ticket.get("responder_id") and ticket["responder_id"] in VALID_AGENT_IDS:
    #     payload["responder_id"] = ticket["responder_id"]
    if ticket.get("email"):
        payload["email"] = ticket["email"]
    if ticket.get("type"):
        payload["type"] = ticket["type"]
    if ticket.get("category"):
        payload["category"] = ticket["category"]
    # Only add sub_category if it's valid for the category
    if ticket.get("sub_category") and ticket.get("category"):
        valid_subs = VALID_SUB_CATEGORIES.get(ticket["category"], [])
        if ticket["sub_category"] in valid_subs:
            payload["sub_category"] = ticket["sub_category"]
    if ticket.get("impact"):
        payload["impact"] = ticket["impact"]
    if ticket.get("urgency"):
        payload["urgency"] = ticket["urgency"]
    if ticket.get("tags"):
        payload["tags"] = ticket["tags"]
    if ticket.get("cc_emails"):
        payload["cc_emails"] = ticket["cc_emails"]
    
    return payload


def post_ticket(ticket: dict[str, Any]) -> dict[str, Any]:
    """
    Create a ticket in Freshservice via POST API.
    
    Args:
        ticket: Ticket data dictionary
        
    Returns:
        API response containing the created ticket
    """
    payload = generate_ticket_payload(ticket)
    return post_to_freshservice("/tickets", payload)


# ============================================================================
# AGENTS
# ============================================================================

def generate_agents(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy agent data."""
    agents = []
    
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Hannah", "Ivan", "Julia"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson", "Moore"]
    job_titles = ["IT Support Specialist", "Senior Support Engineer", "Help Desk Analyst", "IT Administrator", "Support Lead"]
    time_zones = ["America/New_York", "America/Los_Angeles", "Europe/London", "Asia/Tokyo", "Australia/Sydney"]
    
    for i in range(1, count + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        agent = {
            "id": VALID_AGENT_IDS[i % len(VALID_AGENT_IDS)] if i <= len(VALID_AGENT_IDS) else VALID_AGENT_IDS[0],
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}.{last_name.lower()}{i}@company.com",
            "job_title": random.choice(job_titles),
            "phone": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "mobile_phone_number": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}" if random.random() > 0.5 else None,
            "department_ids": [],
            "reporting_manager_id": None,
            "address": f"{random.randint(100, 999)} Main St, City, State 12345",
            "time_zone": random.choice(time_zones),
            "time_format": "12h",
            "language": "en",
            "location_id": None,
            "background_information": f"Experienced support professional with {random.randint(1, 10)} years of experience.",
            "scoreboard_level_id": random.randint(1, 5),
            "member_of": [],
            "observer_of": [],
            "role_ids": [],
            "active": True,
            "occasional": random.random() > 0.8,
            "signature": f"Best regards,\n{first_name} {last_name}\nIT Support Team",
            "custom_fields": {},
            "created_at": generate_iso_datetime(days_ago=random.randint(30, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 30))
        }
        agents.append(agent)
    
    return agents


def generate_agent_payload(agent: dict[str, Any]) -> dict[str, Any]:
    """
    Generate a payload for creating an agent via POST API.
    """
    payload = {
        "first_name": agent["first_name"],
        "last_name": agent["last_name"],
        "email": agent["email"],
    }
    
    if agent.get("job_title"):
        payload["job_title"] = agent["job_title"]
    if agent.get("phone"):
        payload["phone"] = agent["phone"]
    if agent.get("time_zone"):
        payload["time_zone"] = agent["time_zone"]
    if agent.get("language"):
        payload["language"] = agent["language"]
    
    return payload


def post_agent(agent: dict[str, Any]) -> dict[str, Any]:
    """
    Create an agent in Freshservice via POST API.
    """
    payload = generate_agent_payload(agent)
    return post_to_freshservice("/agents", payload)


# ============================================================================
# REQUESTERS
# ============================================================================

def generate_requesters(count: int = 10) -> list[dict[str, Any]]:
    """Generate dummy requester data."""
    requesters = []
    
    first_names = ["Michael", "Sarah", "David", "Emily", "James", "Emma", "Robert", "Olivia", "William", "Sophia"]
    last_names = ["Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Robinson", "Clark", "Lewis"]
    job_titles = ["Software Engineer", "Product Manager", "Sales Representative", "Marketing Analyst", "HR Specialist", "Finance Manager", "Operations Lead", "Customer Success Manager"]
    
    for i in range(1, count + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        requester = {
            "id": VALID_AGENT_IDS[i % len(VALID_AGENT_IDS)] if i <= len(VALID_AGENT_IDS) else VALID_AGENT_IDS[0],
            "first_name": first_name,
            "last_name": last_name,
            "primary_email": f"{first_name.lower()}.{last_name.lower()}{i}@example.com",
            "secondary_emails": [f"{first_name.lower()}.{last_name.lower()}{i}.alt@example.com"] if random.random() > 0.7 else [],
            "job_title": random.choice(job_titles),
            "phone": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "mobile_phone_number": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}" if random.random() > 0.5 else None,
            "department_ids": [],
            "reporting_manager_id": None,
            "address": f"{random.randint(100, 999)} Oak Ave, City, State 12345" if random.random() > 0.5 else None,
            "time_zone": "America/New_York",
            "time_format": "12h",
            "language": "en",
            "location_id": None,
            "background_information": None,
            "can_see_all_tickets_from_associated_departments": random.random() > 0.7,
            "active": True,
            "vip_user": random.random() > 0.9,
            "custom_fields": {},
            "created_at": generate_iso_datetime(days_ago=random.randint(30, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 30))
        }
        requesters.append(requester)
    
    return requesters


def generate_requester_payload(requester: dict[str, Any]) -> dict[str, Any]:
    """
    Generate a payload for creating a requester via POST API.
    """
    payload = {
        "first_name": requester["first_name"],
        "primary_email": requester["primary_email"],
    }
    
    if requester.get("last_name"):
        payload["last_name"] = requester["last_name"]
    if requester.get("job_title"):
        payload["job_title"] = requester["job_title"]
    if requester.get("phone"):
        payload["phone"] = requester["phone"]
    if requester.get("time_zone"):
        payload["time_zone"] = requester["time_zone"]
    if requester.get("language"):
        payload["language"] = requester["language"]
    
    return payload


def post_requester(requester: dict[str, Any]) -> dict[str, Any]:
    """
    Create a requester in Freshservice via POST API.
    """
    payload = generate_requester_payload(requester)
    return post_to_freshservice("/requesters", payload)


# ============================================================================
# GROUPS
# ============================================================================

def generate_groups(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy group data."""
    groups = []
    
    group_names = [
        "IT Support - Level 1",
        "IT Support - Level 2",
        "Network Team",
        "Security Team",
        "Application Support",
        "Hardware Support",
        "Cloud Infrastructure",
        "DevOps Team"
    ]
    
    for i in range(1, min(count + 1, len(group_names) + 1)):
        group = {
            "id": 1000000300 + i,
            "name": group_names[i - 1],
            "description": f"This is the {group_names[i - 1]} group responsible for handling related tickets.",
            "escalate_to": None,
            "unassigned_for": "30m" if random.random() > 0.5 else None,
            "business_hours_id": None,
            "agent_ids": VALID_AGENT_IDS[:random.randint(1, len(VALID_AGENT_IDS))],
            "members": VALID_AGENT_IDS[:random.randint(1, len(VALID_AGENT_IDS))],
            "observers": [get_random_agent_id()] if random.random() > 0.7 else [],
            "leaders": [VALID_AGENT_IDS[0]],
            "auto_ticket_assign": random.random() > 0.5,
            "restricted": False,
            "approval_required": random.random() > 0.8,
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        groups.append(group)
    
    return groups


# ============================================================================
# DEPARTMENTS
# ============================================================================

def generate_departments(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy department data."""
    departments = []
    
    dept_names = [
        "Engineering",
        "Sales",
        "Marketing",
        "Human Resources",
        "Finance",
        "Operations",
        "Customer Success",
        "Legal"
    ]
    
    domains = [
        ["engineering.example.com"],
        ["sales.example.com"],
        ["marketing.example.com"],
        ["hr.example.com"],
        ["finance.example.com"],
        ["ops.example.com"],
        ["cs.example.com"],
        ["legal.example.com"]
    ]
    
    for i in range(1, min(count + 1, len(dept_names) + 1)):
        department = {
            "id": 1000000400 + i,
            "name": dept_names[i - 1],
            "description": f"The {dept_names[i - 1]} department.",
            "head_user_id": get_random_requester_id() if random.random() > 0.3 else None,
            "prime_user_id": get_random_requester_id() if random.random() > 0.5 else None,
            "domains": domains[i - 1] if random.random() > 0.5 else [],
            "custom_fields": {},
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        departments.append(department)
    
    return departments


# ============================================================================
# ASSETS
# ============================================================================

def generate_assets(count: int = 10) -> list[dict[str, Any]]:
    """Generate dummy asset data."""
    assets = []
    
    asset_names = [
        "Dell Latitude 5520",
        "MacBook Pro 14",
        "HP EliteBook 840",
        "Dell Monitor U2722D",
        "Logitech MX Master 3",
        "Cisco IP Phone 8845",
        "HP LaserJet Pro",
        "APC UPS 1500VA",
        "Lenovo ThinkPad X1",
        "Microsoft Surface Pro 9"
    ]
    
    asset_types = {
        58000460346: "Laptop",
        58000460346: "Desktop",
        58000460346: "Monitor",
        58000460346: "Peripheral",
        58000460346: "Phone",
        58000460346: "Printer",
        58000460346: "Server",
        58000460346: "Network Equipment"
    }
    
    impacts = ["low", "medium", "high"]
    usage_types = ["permanent", "loaner", "shared"]
    
    for i in range(1, count + 1):
        asset_type_id = random.choice(list(asset_types.keys()))
        
        asset = {
            "id": 1000000900 + i,
            "display_id": 1000 + i,
            "name": random.choice(asset_names),
            "description": f"Asset #{i} - Company property assigned to employee.",
            "asset_type_id": asset_type_id,
            "impact": random.choice(impacts),
            "author_type": "requester",
            "usage_type": random.choice(usage_types),
            "asset_tag": f"ASSET-{random.randint(10000, 99999)}",
            "user_id": get_random_requester_id() if random.random() > 0.2 else None,
            "department_id": None,
            "location_id": None,
            "agent_id": get_random_agent_id() if random.random() > 0.5 else None,
            "group_id": None,
            "assigned_on": generate_iso_datetime(days_ago=random.randint(1, 365)) if random.random() > 0.3 else None,
            "end_of_life": generate_date(days_ago=-random.randint(365, 1095)) if random.random() > 0.5 else None,
            "discovery_enabled": random.random() > 0.5,
            # "type_fields": {
            #     "serial_number_58000324654": f"SN{random.randint(100000000, 999999999)}",
            #     "manufacturer_58000324655": random.choice(["Dell", "HP", "Apple", "Lenovo", "Microsoft"]),
            #     "model_58000324656": f"Model-{random.randint(100, 999)}"
            # },
            "created_at": generate_iso_datetime(days_ago=random.randint(30, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 30))
        }
        assets.append(asset)
    
    return assets


def generate_asset_payload(asset: dict[str, Any]) -> dict[str, Any]:
    """
    Generate a payload for creating an asset via POST API.
    """
    payload = {
        "name": asset["name"],
        "asset_type_id": asset["asset_type_id"],
    }
    
    if asset.get("description"):
        payload["description"] = asset["description"]
    if asset.get("impact"):
        payload["impact"] = asset["impact"]
    if asset.get("asset_tag"):
        payload["asset_tag"] = asset["asset_tag"]
    if asset.get("user_id"):
        payload["user_id"] = asset["user_id"]
    if asset.get("agent_id"):
        payload["agent_id"] = asset["agent_id"]
    # if asset.get("type_fields"):
    #     payload["type_fields"] = asset["type_fields"]
    
    return payload


def post_asset(asset: dict[str, Any]) -> dict[str, Any]:
    """
    Create an asset in Freshservice via POST API.
    """
    payload = generate_asset_payload(asset)
    return post_to_freshservice("/assets", payload)


# ============================================================================
# PROBLEMS
# ============================================================================

def generate_problems(count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy problem data."""
    problems = []
    
    subjects = [
        "Recurring network latency issues",
        "Email server intermittent failures",
        "VPN disconnection pattern identified",
        "Database performance degradation",
        "Application memory leak detected"
    ]
    
    # Use valid categories from Freshservice
    problem_categories = ["Hardware", "Software", "Network", "Other"]
    
    for i in range(1, count + 1):
        category = random.choice(problem_categories)
        status = random.choice([1, 2])  # Open, Change Requested (not Closed - requires analysis fields)
        
        problem = {
            "id": 2000000000 + i,
            "workspace_id": 1 if random.random() > 0.5 else None,
            "agent_id": get_random_agent_id() if random.random() > 0.3 else None,
            "group_id": None,
            "description": f"<div>Detailed analysis of problem #{i}. Multiple incidents have been linked to this root cause.</div>",
            "description_text": f"Detailed analysis of problem #{i}. Multiple incidents have been linked to this root cause.",
            "requester_id": get_random_requester_id(),
            "subject": random.choice(subjects),
            "status": status,
            "priority": random.choice([1, 2, 3, 4]),
            "impact": random.choice([1, 2, 3]),
            "known_error": random.random() > 0.7,
            "due_by": generate_iso_datetime(days_ago=-random.randint(7, 30)) if random.random() > 0.5 else None,
            "department_id": None,
            "category": category,
            "sub_category": None,
            "item_category": None,
            "analysis_fields": {
                "root_cause": "Configuration drift in production environment" if random.random() > 0.5 else None,
                "symptoms": "Users experiencing intermittent issues",
                "impact_analysis": "Affects approximately 15% of users"
            } if random.random() > 0.5 else None,
            "custom_fields": {},
            "created_at": generate_iso_datetime(days_ago=random.randint(7, 60)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 7))
        }
        problems.append(problem)
    
    return problems


def generate_problem_payload(problem: dict[str, Any]) -> dict[str, Any]:
    """
    Generate a payload for creating a problem via POST API.
    """
    payload = {
        "subject": problem["subject"],
        "description": problem["description"],
        "requester_id": problem["requester_id"],
        "priority": problem["priority"],
        "impact": problem["impact"],
        "status": problem["status"],
    }
    
    # Only add agent_id if it's a valid agent ID
    if problem.get("agent_id") and problem["agent_id"] in VALID_AGENT_IDS:
        payload["agent_id"] = problem["agent_id"]
    if problem.get("due_by"):
        payload["due_by"] = problem["due_by"]
    if problem.get("known_error") is not None:
        payload["known_error"] = problem["known_error"]
    # Only add category if it's valid
    if problem.get("category") and problem["category"] in VALID_CATEGORIES:
        payload["category"] = problem["category"]
    
    return payload


def post_problem(problem: dict[str, Any]) -> dict[str, Any]:
    """
    Create a problem in Freshservice via POST API.
    """
    payload = generate_problem_payload(problem)
    return post_to_freshservice("/problems", payload)


# ============================================================================
# CHANGES
# ============================================================================

def generate_changes(count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy change data."""
    changes = []
    
    subjects = [
        "Upgrade production database to PostgreSQL 15",
        "Deploy new authentication service",
        "Network switch replacement in DC1",
        "Implement new backup solution",
        "Migrate email to cloud provider"
    ]
    
    # Use valid categories from Freshservice
    change_categories = ["Hardware", "Software", "Network", "Other"]
    
    for i in range(1, count + 1):
        category = random.choice(change_categories)
        status = 1  # Must start with Open (initial status per stateflow)
        change_type = random.choice([1, 2, 3, 4])
        
        planned_start = random.randint(1, 14)
        planned_end = planned_start + random.randint(1, 7)
        
        change = {
            "id": 3000000000 + i,
            "workspace_id": 1 if random.random() > 0.5 else None,
            "agent_id": get_random_agent_id(),
            "group_id": None,
            "description": f"<div>Change request #{i}: {random.choice(subjects)}</div>",
            "description_text": f"Change request #{i}: {random.choice(subjects)}",
            "requester_id": get_random_requester_id(),
            "subject": random.choice(subjects),
            "status": status,
            "priority": random.choice([1, 2, 3, 4]),
            "impact": random.choice([1, 2, 3]),
            "risk": random.choice([1, 2, 3, 4]),
            "change_type": change_type,
            "change_window_id": None,
            "planned_start_date": generate_iso_datetime(days_ago=-planned_start),
            "planned_end_date": generate_iso_datetime(days_ago=-planned_end),
            "department_id": None,
            "category": category,
            "sub_category": None,
            "item_category": None,
            "planning_fields": {
                "change_reason": "Performance improvement and security updates",
                "rollback_plan": "Restore from backup and revert configuration",
                "test_plan": "Execute automated test suite after deployment"
            } if random.random() > 0.5 else None,
            "custom_fields": {},
            "created_at": generate_iso_datetime(days_ago=random.randint(7, 30)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 7))
        }
        changes.append(change)
    
    return changes


def generate_change_payload(change: dict[str, Any]) -> dict[str, Any]:
    """
    Generate a payload for creating a change via POST API.
    """
    payload = {
        "subject": change["subject"],
        "description": change["description"],
        "requester_id": change["requester_id"],
        "priority": change["priority"],
        "impact": change["impact"],
        "risk": change["risk"],
        "change_type": change["change_type"],
        "status": change["status"],
    }
    
    # Only add agent_id if it's a valid agent ID
    if change.get("agent_id") and change["agent_id"] in VALID_AGENT_IDS:
        payload["agent_id"] = change["agent_id"]
    if change.get("planned_start_date"):
        payload["planned_start_date"] = change["planned_start_date"]
    if change.get("planned_end_date"):
        payload["planned_end_date"] = change["planned_end_date"]
    # Only add category if it's valid
    if change.get("category") and change["category"] in VALID_CATEGORIES:
        payload["category"] = change["category"]
    
    return payload


def post_change(change: dict[str, Any]) -> dict[str, Any]:
    """
    Create a change in Freshservice via POST API.
    """
    payload = generate_change_payload(change)
    return post_to_freshservice("/changes", payload)


# ============================================================================
# RELEASES
# ============================================================================

def generate_releases(count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy release data."""
    releases = []
    
    subjects = [
        "Application v2.5.0 Release",
        "Security Patch Bundle Q1 2025",
        "Platform Infrastructure Update",
        "Mobile App v3.0 Launch",
        "API Gateway Upgrade"
    ]
    
    # Use valid categories from Freshservice
    release_categories = ["Hardware", "Software", "Network", "Other"]
    
    for i in range(1, count + 1):
        category = random.choice(release_categories)
        status = random.choice([1, 2, 3, 4, 5])
        release_type = random.choice([1, 2, 3, 4])
        
        planned_start = random.randint(1, 14)
        planned_end = planned_start + random.randint(1, 7)
        
        release = {
            "id": 4000000000 + i,
            "workspace_id": 1 if random.random() > 0.5 else None,
            "agent_id": get_random_agent_id(),
            "group_id": None,
            "description": f"<div>Release #{i}: Comprehensive update including new features and bug fixes.</div>",
            "description_text": f"Release #{i}: Comprehensive update including new features and bug fixes.",
            "subject": random.choice(subjects),
            "status": status,
            "priority": random.choice([1, 2, 3, 4]),
            "release_type": release_type,
            "planned_start_date": generate_iso_datetime(days_ago=-planned_start),
            "planned_end_date": generate_iso_datetime(days_ago=-planned_end),
            "work_start_date": generate_iso_datetime(days_ago=-planned_start) if status >= 3 else None,
            "work_end_date": generate_iso_datetime(days_ago=-planned_end) if status == 5 else None,
            "department_id": None,
            "category": category,
            "sub_category": None,
            "item_category": None,
            "planning_fields": {
                "build_plan": "CI/CD pipeline with automated testing",
                "test_plan": "Full regression test suite execution"
            } if random.random() > 0.5 else None,
            "custom_fields": {},
            "created_at": generate_iso_datetime(days_ago=random.randint(14, 60)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 14))
        }
        releases.append(release)
    
    return releases


def generate_release_payload(release: dict[str, Any]) -> dict[str, Any]:
    """
    Generate a payload for creating a release via POST API.
    """
    payload = {
        "subject": release["subject"],
        "description": release["description"],
        "priority": release["priority"],
        "release_type": release["release_type"],
        "status": release["status"],
    }
    
    # Only add agent_id if it's a valid agent ID
    if release.get("agent_id") and release["agent_id"] in VALID_AGENT_IDS:
        payload["agent_id"] = release["agent_id"]
    if release.get("planned_start_date"):
        payload["planned_start_date"] = release["planned_start_date"]
    if release.get("planned_end_date"):
        payload["planned_end_date"] = release["planned_end_date"]
    # Only add category if it's valid
    if release.get("category") and release["category"] in VALID_CATEGORIES:
        payload["category"] = release["category"]
    
    return payload


def post_release(release: dict[str, Any]) -> dict[str, Any]:
    """
    Create a release in Freshservice via POST API.
    """
    payload = generate_release_payload(release)
    return post_to_freshservice("/releases", payload)


# ============================================================================
# GROUPS
# ============================================================================

def generate_group_payload(group: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating a group via POST API."""
    payload = {
        "name": group["name"],
    }
    if group.get("description"):
        payload["description"] = group["description"]
    if group.get("members"):
        payload["members"] = group["members"]
    if group.get("observers"):
        payload["observers"] = group["observers"]
    if group.get("leaders"):
        payload["leaders"] = group["leaders"]
    return payload


def post_group(group: dict[str, Any]) -> dict[str, Any]:
    """Create a group in Freshservice via POST API."""
    payload = generate_group_payload(group)
    return post_to_freshservice("/groups", payload)


# ============================================================================
# DEPARTMENTS
# ============================================================================

def generate_department_payload(department: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating a department via POST API."""
    payload = {
        "name": department["name"],
    }
    if department.get("description"):
        payload["description"] = department["description"]
    if department.get("domains"):
        payload["domains"] = department["domains"]
    return payload


def post_department(department: dict[str, Any]) -> dict[str, Any]:
    """Create a department in Freshservice via POST API."""
    payload = generate_department_payload(department)
    return post_to_freshservice("/departments", payload)


# ============================================================================
# LOCATIONS
# ============================================================================

def generate_locations(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy location data."""
    locations = []
    
    location_data = [
        ("Headquarters", "123 Main Street", "New York", "NY", "USA", "10001"),
        ("West Coast Office", "456 Tech Blvd", "San Francisco", "CA", "USA", "94102"),
        ("European Office", "789 Business Park", "London", None, "UK", "EC1A 1BB"),
        ("Asia Pacific Office", "100 Innovation Way", "Tokyo", None, "Japan", "100-0001"),
        ("Remote Hub - Austin", "200 Remote St", "Austin", "TX", "USA", "78701")
    ]
    
    for i in range(1, min(count + 1, len(location_data) + 1)):
        name, line1, city, state, country, zipcode = location_data[i - 1]
        
        location = {
            "id": 1000000600 + i,
            "name": name,
            "parent_location_id": None,
            "primary_contact_id": get_random_requester_id() if random.random() > 0.5 else None,
            "address": {
                "line1": line1,
                "line2": f"Suite {random.randint(100, 999)}" if random.random() > 0.5 else None,
                "city": city,
                "state": state,
                "country": country,
                "zipcode": zipcode
            },
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        locations.append(location)
    
    return locations


def generate_location_payload(location: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating a location via POST API."""
    payload = {
        "name": location["name"],
    }
    if location.get("address"):
        payload["address"] = location["address"]
    return payload


def post_location(location: dict[str, Any]) -> dict[str, Any]:
    """Create a location in Freshservice via POST API."""
    payload = generate_location_payload(location)
    return post_to_freshservice("/locations", payload)


# ============================================================================
# VENDORS
# ============================================================================

def generate_vendors(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy vendor data."""
    vendors = []
    
    vendor_names = [
        "Dell Technologies",
        "Microsoft Corporation",
        "Amazon Web Services",
        "Cisco Systems",
        "HP Inc.",
        "Apple Inc.",
        "Salesforce",
        "ServiceNow"
    ]
    
    for i in range(1, min(count + 1, len(vendor_names) + 1)):
        vendor = {
            "id": 5000000000 + i,
            "name": vendor_names[i - 1],
            "description": f"{vendor_names[i - 1]} - Technology vendor providing hardware and software solutions.",
            "primary_contact_id": None,
            "address": {
                "line1": f"{random.randint(1, 999)} Corporate Drive",
                "city": random.choice(["San Jose", "Redmond", "Seattle", "Austin"]),
                "state": random.choice(["CA", "WA", "TX"]),
                "country": "USA",
                "zipcode": f"{random.randint(10000, 99999)}"
            } if random.random() > 0.3 else None,
            "custom_fields": {
                "cf_vendor_tier": random.choice(["Gold", "Silver", "Bronze"])
            },
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        vendors.append(vendor)
    
    return vendors


def generate_vendor_payload(vendor: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating a vendor via POST API."""
    payload = {
        "name": vendor["name"],
    }
    if vendor.get("description"):
        payload["description"] = vendor["description"]
    if vendor.get("address"):
        payload["address"] = vendor["address"]
    return payload


def post_vendor(vendor: dict[str, Any]) -> dict[str, Any]:
    """Create a vendor in Freshservice via POST API."""
    payload = generate_vendor_payload(vendor)
    return post_to_freshservice("/vendors", payload)


# ============================================================================
# PRODUCTS
# ============================================================================

def generate_products(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy product data."""
    products = []
    
    product_data = [
        ("Dell Latitude 5520", "Dell", 58000460328),
        ("MacBook Pro 14-inch", "Apple", 58000460328),
        ("HP EliteBook 840 G8", "HP", 58000460328),
        ("Dell UltraSharp U2722D", "Dell", 58000460328),
        ("Logitech MX Master 3", "Logitech", 58000460328)
    ]
    
    statuses = ["In Production", "Retired", "Under Review"]
    procurement_modes = ["Buy", "Lease", "Rent"]
    
    for i in range(1, min(count + 1, len(product_data) + 1)):
        name, manufacturer, asset_type_id = product_data[i - 1]
        
        product = {
            "id": 6000000000 + i,
            "name": name,
            "asset_type_id": asset_type_id,
            "manufacturer": manufacturer,
            "status": random.choice(statuses),
            "mode_of_procurement": random.choice(procurement_modes),
            "depreciation_type_id": random.randint(1, 3) if random.random() > 0.5 else None,
            "description": f"{name} - Standard company-issued equipment.",
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        products.append(product)
    
    return products


def generate_product_payload(product: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating a product via POST API."""
    payload = {
        "name": product["name"],
    }
    if product.get("manufacturer"):
        payload["manufacturer"] = product["manufacturer"]
    if product.get("asset_type_id"):
        payload["asset_type_id"] = product["asset_type_id"]
    if product.get("description"):
        payload["description"] = product["description"]
    return payload


def post_product(product: dict[str, Any]) -> dict[str, Any]:
    """Create a product in Freshservice via POST API."""
    payload = generate_product_payload(product)
    return post_to_freshservice("/products", payload)


# ============================================================================
# CONTRACTS
# ============================================================================

def generate_contracts(count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy contract data."""
    contracts = []
    
    contract_names = [
        "Microsoft Enterprise Agreement",
        "AWS Support Contract",
        "Dell Hardware Maintenance",
        "Salesforce Subscription",
        "ServiceNow License Agreement"
    ]
    
    statuses = ["Active", "Expired", "Pending Renewal"]
    license_types = ["Volume", "Per User", "Per Device", "Enterprise"]
    billing_cycles = ["Annual", "Monthly", "Quarterly"]
    
    for i in range(1, min(count + 1, len(contract_names) + 1)):
        start_days_ago = random.randint(30, 365)
        end_days_from_now = random.randint(30, 730)
        
        contract = {
            "id": 7000000000 + i,
            "name": contract_names[i - 1],
            "description": f"{contract_names[i - 1]} - Enterprise contract for software and services.",
            "vendor_id": 58000186100,
            "auto_renew": random.random() > 0.5,
            "notify_expiry": True,
            "notify_before": random.choice([30, 60, 90]),
            "approver_id": get_random_agent_id() if random.random() > 0.5 else None,
            "start_date": generate_date(days_ago=start_days_ago),
            "end_date": generate_date(days_ago=-end_days_from_now),
            "cost": round(random.uniform(10000, 500000), 2),
            "status": random.choice(statuses),
            "contract_number": f"CNT-{random.randint(10000, 99999)}",
            "contract_type_id": random.randint(1, 5),
            "visible_to_id": None,
            "notify_to": ["procurement@example.com", "it-admin@example.com"],
            "custom_fields": {},
            "software_id": None,
            "license_type": random.choice(license_types),
            "billing_cycle": random.choice(billing_cycles),
            "license_key": f"XXXX-XXXX-XXXX-{random.randint(1000, 9999)}" if random.random() > 0.5 else None,
            "item_cost_details": [],
            "created_at": generate_iso_datetime(days_ago=start_days_ago),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 30))
        }
        contracts.append(contract)
    
    return contracts


def generate_contract_payload(contract: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating a contract via POST API."""
    payload = {
        "name": contract["name"],
        "vendor_id": contract["vendor_id"],
        "contract_type_id": contract["contract_type_id"],
    }
    if contract.get("start_date"):
        payload["start_date"] = contract["start_date"]
    if contract.get("end_date"):
        payload["end_date"] = contract["end_date"]
    if contract.get("cost"):
        payload["cost"] = contract["cost"]
    if contract.get("contract_number"):
        payload["contract_number"] = contract["contract_number"]
    return payload


def post_contract(contract: dict[str, Any]) -> dict[str, Any]:
    """Create a contract in Freshservice via POST API."""
    payload = generate_contract_payload(contract)
    return post_to_freshservice("/contracts", payload)


# ============================================================================
# PURCHASE ORDERS
# ============================================================================

def generate_purchase_orders(count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy purchase order data."""
    purchase_orders = []
    
    for i in range(1, count + 1):
        status = random.choice([20, 25, 30, 35, 40])  # Open, Ordered, Received, Partially Received, Cancelled
        
        purchase_order = {
            "id": 8000000000 + i,
            "name": f"PO for Q{random.randint(1, 4)} {2025} Equipment",
            "po_number": f"PO-{random.randint(10000, 99999)}",
            "vendor_id": 58000186722,
            "department_id": None,
            "created_by": get_random_agent_id(),
            "expected_delivery_date": generate_date(days_ago=-random.randint(7, 30)),
            "shipping_address": "123 Main Street, New York, NY 10001",
            "billing_same_as_shipping": True,
            "billing_address": "123 Main Street, New York, NY 10001",
            "currency_code": "USD",
            "conversion_rate": 1.0,
            "discount_percentage": random.choice([0, 5, 10, 15]) if random.random() > 0.5 else None,
            "tax_percentage": random.choice([0, 5, 7.5, 10]),
            "shipping_cost": round(random.uniform(0, 100), 2),
            "custom_fields": {},
            "purchase_items": [
                {
                    "item_type": 1,
                    "item_id": 6000000000 + random.randint(1, 5),
                    "item_name": f"Product Item {j}",
                    "quantity": random.randint(1, 10),
                    "unit_price": round(random.uniform(100, 2000), 2)
                }
                for j in range(1, random.randint(2, 5))
            ],
            "status": status,
            "created_at": generate_iso_datetime(days_ago=random.randint(7, 60)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 7))
        }
        purchase_orders.append(purchase_order)
    
    return purchase_orders


def generate_purchase_order_payload(po: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating a purchase order via POST API."""
    payload = {
        "name": po["name"],
        "po_number": po["po_number"],
        "status": po["status"],
        "billing_same_as_shipping": po["billing_same_as_shipping"],
        "shipping_address": po["shipping_address"],
        "billing_address": po["billing_address"],
    }
    if po.get("vendor_id"):
        payload["vendor_id"] = po["vendor_id"]
    if po.get("expected_delivery_date"):
        payload["expected_delivery_date"] = po["expected_delivery_date"]
    if po.get("currency_code"):
        payload["currency_code"] = po["currency_code"]
    return payload


def post_purchase_order(po: dict[str, Any]) -> dict[str, Any]:
    """Create a purchase order in Freshservice via POST API."""
    payload = generate_purchase_order_payload(po)
    return post_to_freshservice("/purchase_orders", payload)


# ============================================================================
# SERVICE CATALOG ITEMS
# ============================================================================

def generate_service_catalog_items(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy service catalog item data."""
    items = []
    
    item_data = [
        ("New Laptop Request", "Request a new laptop for work", 1200.00, 72),
        ("Software Installation", "Request software to be installed", 0.00, 24),
        ("VPN Access Request", "Request VPN access for remote work", 0.00, 4),
        ("New Employee Onboarding", "Complete IT setup for new employees", 500.00, 48),
        ("Password Reset", "Reset your password", 0.00, 1)
    ]
    
    for i in range(1, min(count + 1, len(item_data) + 1)):
        name, description, cost, delivery_time = item_data[i - 1]
        
        item = {
            "id": 9000000000 + i,
            "name": name,
            "display_id": 100 + i,
            "description": f"<div>{description}</div>",
            "short_description": description,
            "cost": cost,
            "cost_visibility": cost > 0,
            "delivery_time": delivery_time,
            "delivery_time_visibility": True,
            "category_id": random.randint(1, 5),
            "product_id": None,
            "group_visibility": 1,
            "item_type": 1,
            "ci_type_id": None,
            "visibility": 1,
            "deleted": False,
            "create_child": False,
            "configs": None,
            "icon_name": "laptop" if "Laptop" in name else "software",
            "custom_fields": [],
            "child_items": [],
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        items.append(item)
    
    return items


# Note: Service catalog items are typically configured via admin, not API POST
# The POST endpoint may not be available or may require special permissions


# ============================================================================
# SOLUTIONS (Knowledge Base Articles)
# ============================================================================

def generate_solutions(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy knowledge base article data."""
    solutions = []
    
    article_data = [
        ("How to Connect to VPN", "Step-by-step guide for VPN connection"),
        ("Password Reset Instructions", "How to reset your company password"),
        ("Setting Up Email on Mobile", "Configure email on iOS and Android"),
        ("Requesting New Equipment", "Process for requesting new hardware"),
        ("Troubleshooting Network Issues", "Common network problems and solutions")
    ]
    
    for i in range(1, min(count + 1, len(article_data) + 1)):
        title, description = article_data[i - 1]
        
        solution = {
            "id": 10000000000 + i,
            "title": title,
            "description": f"<div><h2>{title}</h2><p>{description}</p><p>This article provides detailed instructions for {title.lower()}.</p></div>",
            "description_text": f"{title}\n\n{description}\n\nThis article provides detailed instructions for {title.lower()}.",
            "status": random.choice([1, 2]),  # Draft, Published
            "approval_status": None,
            "folder_id": 58000009191,
            "category_id": random.randint(1, 5),
            "agent_id": get_random_agent_id(),
            "thumbs_up": random.randint(0, 100),
            "thumbs_down": random.randint(0, 10),
            "hits": random.randint(10, 1000),
            "tags": random.sample(["vpn", "email", "password", "network", "hardware", "howto"], k=random.randint(1, 3)),
            "keywords": [title.lower().split()[0], "help", "guide"],
            "seo_data": None,
            "attachments": [],
            "created_at": generate_iso_datetime(days_ago=random.randint(30, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 30))
        }
        solutions.append(solution)
    
    return solutions


def generate_solution_payload(solution: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating a solution article via POST API."""
    payload = {
        "title": solution["title"],
        "description": solution["description"],
        "folder_id": solution["folder_id"],
        "status": solution["status"],
    }
    if solution.get("tags"):
        payload["tags"] = solution["tags"]
    if solution.get("keywords"):
        payload["keywords"] = solution["keywords"]
    return payload


def post_solution(solution: dict[str, Any]) -> dict[str, Any]:
    """Create a solution article in Freshservice via POST API."""
    payload = generate_solution_payload(solution)
    return post_to_freshservice("/solutions/articles", payload)


# ============================================================================
# TIME ENTRIES
# ============================================================================

def generate_time_entries(ticket_id: int, count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy time entry data for a ticket."""
    time_entries = []
    
    notes = [
        "Initial investigation and troubleshooting",
        "Applied fix and tested solution",
        "Follow-up call with user",
        "Documentation update",
        "Escalation review meeting"
    ]
    
    for i in range(1, count + 1):
        hours = random.randint(0, 4)
        minutes = random.choice([0, 15, 30, 45])
        
        time_entry = {
            "id": 11000000000 + ticket_id * 100 + i,
            "ticket_id": ticket_id,
            "start_time": generate_iso_datetime(days_ago=random.randint(0, 7)),
            "timer_running": False,
            "billable": random.random() > 0.3,
            "time_spent": f"{hours:02d}:{minutes:02d}",
            "executed_at": generate_iso_datetime(days_ago=random.randint(0, 7)),
            "task_id": None,
            "note": random.choice(notes),
            "agent_id": get_random_agent_id(),
            "custom_fields": {},
            "created_at": generate_iso_datetime(days_ago=random.randint(0, 7)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 3))
        }
        time_entries.append(time_entry)
    
    return time_entries


def post_time_entry(ticket_id: int, time_entry: dict[str, Any]) -> dict[str, Any]:
    """
    Create a time entry for a ticket in Freshservice via POST API.
    """
    payload = {
        "agent_id": time_entry["agent_id"],
        "billable": time_entry["billable"],
        "time_spent": time_entry["time_spent"],
        "executed_at": time_entry["executed_at"],
    }
    
    if time_entry.get("note"):
        payload["note"] = time_entry["note"]
    
    return post_to_freshservice(f"/tickets/{ticket_id}/time_entries", payload)


# ============================================================================
# CONVERSATIONS
# ============================================================================

def generate_conversations(ticket_id: int, count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy conversation data for a ticket."""
    conversations = []
    
    bodies = [
        "Thank you for reporting this issue. We are looking into it.",
        "Can you please provide more details about when this started happening?",
        "I've identified the root cause. Applying a fix now.",
        "The issue has been resolved. Please let us know if you have any questions.",
        "This is a private note: Escalating to Level 2 support."
    ]
    
    for i in range(1, count + 1):
        is_private = random.random() > 0.7
        is_incoming = random.random() > 0.6
        body = random.choice(bodies)
        
        conversation = {
            "id": 12000000000 + ticket_id * 100 + i,
            "ticket_id": ticket_id,
            "user_id": get_random_agent_id() if not is_incoming else get_random_requester_id(),
            "body": f"<div>{body}</div>",
            "body_text": body,
            "incoming": is_incoming,
            "private": is_private,
            "source": random.choice([1, 2, 3]),  # Email, Portal, Phone
            "support_email": "support@company.freshservice.com" if not is_incoming else None,
            "to_emails": [f"user{random.randint(1, 10)}@example.com"] if not is_incoming else [],
            "from_email": f"user{random.randint(1, 10)}@example.com" if is_incoming else "support@company.freshservice.com",
            "cc_emails": [],
            "bcc_emails": [],
            "attachments": [],
            "created_at": generate_iso_datetime(days_ago=random.randint(0, 7), hours_ago=i * 2),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 3))
        }
        conversations.append(conversation)
    
    return conversations


def post_conversation(ticket_id: int, conversation: dict[str, Any]) -> dict[str, Any]:
    """
    Create a conversation (note or reply) for a ticket in Freshservice via POST API.
    """
    if conversation.get("private"):
        # Create a note
        payload = {
            "body": conversation["body"],
            "private": True,
        }
        return post_to_freshservice(f"/tickets/{ticket_id}/notes", payload)
    else:
        # Create a reply
        payload = {
            "body": conversation["body"],
        }
        return post_to_freshservice(f"/tickets/{ticket_id}/reply", payload)


# ============================================================================
# TASKS
# ============================================================================

def generate_tasks(parent_id: int, parent_type: str = "ticket", count: int = 2) -> list[dict[str, Any]]:
    """Generate dummy task data for a parent object (ticket, problem, change, or release)."""
    tasks = []
    
    task_titles = [
        "Review and analyze the issue",
        "Implement the solution",
        "Test the fix in staging",
        "Deploy to production",
        "Update documentation",
        "Notify stakeholders"
    ]
    
    # Valid notify_before values in seconds: 0, 900 (15min), 1800 (30min), 2700 (45min), 3600 (1h), 7200 (2h)
    valid_notify_before = [0, 900, 1800, 2700, 3600, 7200]
    
    for i in range(1, count + 1):
        status = random.choice([1, 2, 3])  # Open, In Progress, Completed
        
        task = {
            "id": 13000000000 + parent_id * 100 + i,
            "parent_id": parent_id,
            "parent_type": parent_type,
            "agent_id": get_random_agent_id() if random.random() > 0.3 else None,
            "group_id": None,
            "status": status,
            "title": random.choice(task_titles),
            "description": f"Task #{i} for {parent_type} #{parent_id}. Please complete this task as part of the workflow.",
            "notify_before": random.choice(valid_notify_before) if random.random() > 0.5 else 0,
            "due_date": generate_iso_datetime(days_ago=-random.randint(1, 7)),
            "closed_at": generate_iso_datetime(days_ago=random.randint(0, 3)) if status == 3 else None,
            "created_at": generate_iso_datetime(days_ago=random.randint(1, 14)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 3))
        }
        tasks.append(task)
    
    return tasks


def post_task(parent_id: int, parent_type: str, task: dict[str, Any]) -> dict[str, Any]:
    """
    Create a task for a parent object in Freshservice via POST API.
    
    Args:
        parent_id: ID of the parent object (ticket, problem, change, or release)
        parent_type: Type of parent ('ticket', 'problem', 'change', 'release')
        task: Task data dictionary
        
    Returns:
        API response containing the created task
    """
    endpoint_map = {
        "ticket": f"/tickets/{parent_id}/tasks",
        "problem": f"/problems/{parent_id}/tasks",
        "change": f"/changes/{parent_id}/tasks",
        "release": f"/releases/{parent_id}/tasks",
    }
    
    endpoint = endpoint_map.get(parent_type)
    if not endpoint:
        return {"error": f"Unknown parent type: {parent_type}"}
    
    payload = {
        "title": task["title"],
        "status": task["status"],
    }
    
    if task.get("agent_id"):
        payload["agent_id"] = task["agent_id"]
    if task.get("description"):
        payload["description"] = task["description"]
    if task.get("due_date"):
        payload["due_date"] = task["due_date"]
    if task.get("notify_before"):
        payload["notify_before"] = task["notify_before"]
    
    return post_to_freshservice(endpoint, payload)


# ============================================================================
# TICKET FIELDS
# ============================================================================

def generate_ticket_fields(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy ticket field configuration data."""
    fields = []
    
    field_data = [
        ("status", "Status", "default_status", True, True),
        ("priority", "Priority", "default_priority", True, True),
        ("subject", "Subject", "default_subject", True, True),
        ("cf_employee_id", "Employee ID", "custom_text", False, False),
        ("cf_location", "Office Location", "custom_dropdown", False, False)
    ]
    
    for i, (name, label, field_type, required, is_default) in enumerate(field_data[:count], 1):
        field = {
            "id": 14000000000 + i,
            "workspace_id": None,
            "name": name,
            "label": label,
            "description": f"Field for {label}",
            "field_type": field_type,
            "required": required,
            "required_for_closure": required,
            "position": i,
            "default": is_default,
            "visible_in_portal": True,
            "editable_in_portal": not is_default,
            "required_in_portal": required,
            "choices": [
                {"id": j, "value": f"Option {j}"}
                for j in range(1, 4)
            ] if "dropdown" in field_type else [],
            "nested_fields": [],
            "sections": [],
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        fields.append(field)
    
    return fields


# ============================================================================
# ROLES
# ============================================================================

def generate_roles(count: int = 5) -> list[dict[str, Any]]:
    """Generate dummy role data."""
    roles = []
    
    role_data = [
        ("Administrator", "Full system access", True),
        ("Agent", "Standard agent access", True),
        ("Supervisor", "Team lead access", True),
        ("Problem Manager", "Problem management access", False),
        ("Change Manager", "Change management access", False)
    ]
    
    for i, (name, description, is_default) in enumerate(role_data[:count], 1):
        role = {
            "id": 1000000700 + i,
            "name": name,
            "description": description,
            "default": is_default,
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        roles.append(role)
    
    return roles


# ============================================================================
# SLA POLICIES
# ============================================================================

def generate_sla_policies(count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy SLA policy data."""
    policies = []
    
    policy_data = [
        ("Urgent Priority SLA", "SLA for urgent tickets", True),
        ("Standard SLA", "Default SLA policy", False),
        ("VIP Support SLA", "Enhanced SLA for VIP users", False)
    ]
    
    for i, (name, description, is_default) in enumerate(policy_data[:count], 1):
        policy = {
            "id": 1000000500 + i,
            "name": name,
            "description": description,
            "position": i,
            "is_default": is_default,
            "active": True,
            "deleted": False,
            "applicable_to": {
                "ticket_type": "all",
                "priority": [1, 2, 3, 4] if is_default else [4]
            },
            "sla_targets": [
                {
                    "priority": 4,
                    "respond_within": 3600,  # 1 hour in seconds
                    "resolve_within": 14400,  # 4 hours in seconds
                    "business_hours": True,
                    "escalation_enabled": True
                },
                {
                    "priority": 3,
                    "respond_within": 7200,  # 2 hours
                    "resolve_within": 28800,  # 8 hours
                    "business_hours": True,
                    "escalation_enabled": True
                }
            ],
            "escalation": {
                "response": {
                    "level_1": {"time": 1800, "agent_ids": [VALID_AGENT_IDS[0]]}
                },
                "resolution": {
                    "level_1": {"time": 3600, "agent_ids": [VALID_AGENT_IDS[0]]}
                }
            },
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        policies.append(policy)
    
    return policies


# ============================================================================
# BUSINESS HOURS
# ============================================================================

def generate_business_hours(count: int = 2) -> list[dict[str, Any]]:
    """Generate dummy business hours configuration data."""
    configs = []
    
    config_data = [
        ("Default Business Hours", "America/New_York", True),
        ("EMEA Business Hours", "Europe/London", False)
    ]
    
    for i, (name, time_zone, is_default) in enumerate(config_data[:count], 1):
        config = {
            "id": 1000000800 + i,
            "name": name,
            "description": f"Business hours configuration for {name}",
            "is_default": is_default,
            "time_zone": time_zone,
            "service_desk_hours": {
                "monday": {"start_time": "09:00", "end_time": "17:00"},
                "tuesday": {"start_time": "09:00", "end_time": "17:00"},
                "wednesday": {"start_time": "09:00", "end_time": "17:00"},
                "thursday": {"start_time": "09:00", "end_time": "17:00"},
                "friday": {"start_time": "09:00", "end_time": "17:00"},
                "saturday": None,
                "sunday": None
            },
            "list_of_holidays": [
                {"name": "New Year's Day", "date": "2025-01-01"},
                {"name": "Independence Day", "date": "2025-07-04"},
                {"name": "Christmas Day", "date": "2025-12-25"}
            ],
            "created_at": generate_iso_datetime(days_ago=random.randint(60, 365)),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, 60))
        }
        configs.append(config)
    
    return configs


# ============================================================================
# ANNOUNCEMENTS
# ============================================================================

def generate_announcements(count: int = 3) -> list[dict[str, Any]]:
    """Generate dummy announcement data."""
    announcements = []
    
    announcement_data = [
        ("Scheduled Maintenance", "System maintenance scheduled for this weekend."),
        ("New Feature Release", "We've launched new self-service features in the portal."),
        ("Holiday Schedule", "IT support will have reduced hours during the holidays.")
    ]
    
    for i, (title, body) in enumerate(announcement_data[:count], 1):
        visible_from_days = random.randint(0, 7)
        
        announcement = {
            "id": 15000000000 + i,
            "title": title,
            "body_html": f"<div><h3>{title}</h3><p>{body}</p></div>",
            "body": f"{title}\n\n{body}",
            "visible_from": generate_future_iso_datetime(days=visible_from_days),
            "visible_till": generate_future_iso_datetime(days=-random.randint(7, 30)) if random.random() > 0.5 else None,
            "visibility": random.choice(["everyone","agents_only","grouped_visibility","requesters_only","none"]),
            "departments": [],
            "groups": [58000069979],
            "state": "active",
            "created_by": get_random_agent_id(),
            "additional_emails": [],
            "is_read": random.random() > 0.5,
            "send_email": random.random() > 0.5,
            "created_at": generate_iso_datetime(days_ago=visible_from_days),
            "updated_at": generate_iso_datetime(days_ago=random.randint(0, visible_from_days))
        }
        announcements.append(announcement)
    
    return announcements


def generate_announcement_payload(announcement: dict[str, Any]) -> dict[str, Any]:
    """Generate a payload for creating an announcement via POST API."""
    payload = {
        "title": announcement["title"],
        "body_html": announcement["body_html"],
        "visible_from": announcement["visible_from"],
    }
    if announcement.get("visible_till"):
        payload["visible_till"] = announcement["visible_till"]
    if announcement.get("visibility"):
        payload["visibility"] = announcement["visibility"]
    return payload


def post_announcement(announcement: dict[str, Any]) -> dict[str, Any]:
    """Create an announcement in Freshservice via POST API."""
    payload = generate_announcement_payload(announcement)
    return post_to_freshservice("/announcements", payload)


# ============================================================================
# MAIN FUNCTION TO GENERATE ALL DATA
# ============================================================================

def generate_all_dummy_data() -> dict[str, Any]:
    """
    Generate dummy data for all Freshservice object types.
    
    Returns a dictionary with all object types and their dummy data.
    """
    # Generate base data
    tickets = generate_tickets(count=10)
    agents = generate_agents(count=5)
    requesters = generate_requesters(count=10)
    groups = generate_groups(count=5)
    departments = generate_departments(count=5)
    assets = generate_assets(count=10)
    problems = generate_problems(count=3)
    changes = generate_changes(count=3)
    releases = generate_releases(count=3)
    locations = generate_locations(count=5)
    vendors = generate_vendors(count=5)
    products = generate_products(count=5)
    contracts = generate_contracts(count=3)
    purchase_orders = generate_purchase_orders(count=3)
    service_catalog_items = generate_service_catalog_items(count=5)
    solutions = generate_solutions(count=5)
    ticket_fields = generate_ticket_fields(count=5)
    roles = generate_roles(count=5)
    sla_policies = generate_sla_policies(count=3)
    business_hours = generate_business_hours(count=2)
    announcements = generate_announcements(count=3)
    
    # Generate child data for first few tickets
    time_entries = []
    conversations = []
    tasks = []
    
    for ticket in tickets[:3]:
        time_entries.extend(generate_time_entries(ticket["id"], count=random.randint(1, 3)))
        conversations.extend(generate_conversations(ticket["id"], count=random.randint(2, 4)))
        tasks.extend(generate_tasks(ticket["id"], "ticket", count=random.randint(1, 3)))
    
    # Generate tasks for problems and changes
    for problem in problems[:2]:
        tasks.extend(generate_tasks(problem["id"], "problem", count=random.randint(1, 2)))
    
    for change in changes[:2]:
        tasks.extend(generate_tasks(change["id"], "change", count=random.randint(2, 3)))
    
    return {
        "tickets": tickets,
        "agents": agents,
        "requesters": requesters,
        "groups": groups,
        "departments": departments,
        "assets": assets,
        "problems": problems,
        "changes": changes,
        "releases": releases,
        "locations": locations,
        "vendors": vendors,
        "products": products,
        "contracts": contracts,
        "purchase_orders": purchase_orders,
        "service_catalog_items": service_catalog_items,
        "solutions": solutions,
        "time_entries": time_entries,
        "conversations": conversations,
        "tasks": tasks,
        "ticket_fields": ticket_fields,
        "roles": roles,
        "sla_policies": sla_policies,
        "business_hours": business_hours,
        "announcements": announcements
    }


def get_dummy_data_for_object(object_name: str, **kwargs) -> list[dict[str, Any]]:
    """
    Get dummy data for a specific Freshservice object type.
    
    Args:
        object_name: Name of the object type (e.g., 'tickets', 'agents')
        **kwargs: Additional parameters passed to the generator function
        
    Returns:
        List of dummy records for the specified object type.
    """
    generators = {
        "tickets": generate_tickets,
        "agents": generate_agents,
        "requesters": generate_requesters,
        "groups": generate_groups,
        "departments": generate_departments,
        "assets": generate_assets,
        "problems": generate_problems,
        "changes": generate_changes,
        "releases": generate_releases,
        "locations": generate_locations,
        "vendors": generate_vendors,
        "products": generate_products,
        "contracts": generate_contracts,
        "purchase_orders": generate_purchase_orders,
        "service_catalog_items": generate_service_catalog_items,
        "solutions": generate_solutions,
        "ticket_fields": generate_ticket_fields,
        "roles": generate_roles,
        "sla_policies": generate_sla_policies,
        "business_hours": generate_business_hours,
        "announcements": generate_announcements
    }
    
    if object_name not in generators:
        raise ValueError(f"Unknown object type: {object_name}. Available types: {list(generators.keys())}")
    
    return generators[object_name](**kwargs)


# ============================================================================
# POST ALL DATA TO FRESHSERVICE
# ============================================================================

def post_all_dummy_data(
    tickets_count: int = 3,
    problems_count: int = 2,
    changes_count: int = 2,
    releases_count: int = 1,
    groups_count: int = 2,
    departments_count: int = 2,
    locations_count: int = 2,
    vendors_count: int = 2,
    products_count: int = 2,
    contracts_count: int = 1,
    purchase_orders_count: int = 1,
    solutions_count: int = 2,
    announcements_count: int = 1,
    assets_count: int = 2,
    requesters_count: int = 2,
    delay_between_requests: float = 0.5
) -> dict[str, list[dict[str, Any]]]:
    """
    Generate and post dummy data to Freshservice.
    
    Args:
        Various count parameters for each object type
        delay_between_requests: Delay in seconds between API requests
        
    Returns:
        Dictionary containing API responses for each object type
    """
    results = {
        "departments": [],
        "locations": [],
        "groups": [],
        "vendors": [],
        "products": [],
        "contracts": [],
        "requesters": [],
        "assets": [],
        "tickets": [],
        "problems": [],
        "changes": [],
        "releases": [],
        "solutions": [],
        "announcements": [],
        "purchase_orders": [],
        "time_entries": [],
        "conversations": [],
        "tasks": [],
    }
    
    step = 1
    total_steps = 15
    
    print("=" * 60)
    print("POSTING DUMMY DATA TO FRESHSERVICE")
    print(f"Domain: {FRESHSERVICE_DOMAIN}")
    print("=" * 60)
    
    # Create departments first (referenced by other objects)
    print(f"\n[{step}/{total_steps}] Creating {departments_count} departments...")
    departments = generate_departments(count=departments_count)
    for i, dept in enumerate(departments, 1):
        print(f"  Creating department {i}/{departments_count}: {dept['name']}...")
        response = post_department(dept)
        results["departments"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create locations
    print(f"\n[{step}/{total_steps}] Creating {locations_count} locations...")
    locations = generate_locations(count=locations_count)
    for i, loc in enumerate(locations, 1):
        print(f"  Creating location {i}/{locations_count}: {loc['name']}...")
        response = post_location(loc)
        results["locations"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create groups
    # print(f"\n[{step}/{total_steps}] Creating {groups_count} groups...")
    # groups = generate_groups(count=groups_count)
    # for i, group in enumerate(groups, 1):
    #     print(f"  Creating group {i}/{groups_count}: {group['name']}...")
    #     response = post_group(group)
    #     results["groups"].append(response)
    #     time.sleep(delay_between_requests)
    # step += 1
    
    # Create vendors
    print(f"\n[{step}/{total_steps}] Creating {vendors_count} vendors...")
    vendors = generate_vendors(count=vendors_count)
    for i, vendor in enumerate(vendors, 1):
        print(f"  Creating vendor {i}/{vendors_count}: {vendor['name']}...")
        response = post_vendor(vendor)
        results["vendors"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create products
    print(f"\n[{step}/{total_steps}] Creating {products_count} products...")
    products = generate_products(count=products_count)
    for i, product in enumerate(products, 1):
        print(f"  Creating product {i}/{products_count}: {product['name']}...")
        response = post_product(product)
        results["products"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create contracts
    print(f"\n[{step}/{total_steps}] Creating {contracts_count} contracts...")
    contracts = generate_contracts(count=contracts_count)
    for i, contract in enumerate(contracts, 1):
        print(f"  Creating contract {i}/{contracts_count}: {contract['name']}...")
        response = post_contract(contract)
        results["contracts"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create requesters
    # print(f"\n[{step}/{total_steps}] Creating {requesters_count} requesters...")
    # requesters = generate_requesters(count=requesters_count)
    # for i, requester in enumerate(requesters, 1):
    #     print(f"  Creating requester {i}/{requesters_count}: {requester['first_name']} {requester.get('last_name', '')}...")
    #     response = post_requester(requester)
    #     results["requesters"].append(response)
    #     time.sleep(delay_between_requests)
    # step += 1
    
    # Create assets
    print(f"\n[{step}/{total_steps}] Creating {assets_count} assets...")
    assets = generate_assets(count=assets_count)
    for i, asset in enumerate(assets, 1):
        print(f"  Creating asset {i}/{assets_count}: {asset['name']}...")
        response = post_asset(asset)
        results["assets"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create tickets with child objects
    print(f"\n[{step}/{total_steps}] Creating {tickets_count} tickets...")
    tickets = generate_tickets(count=tickets_count)
    for i, ticket in enumerate(tickets, 1):
        print(f"  Creating ticket {i}/{tickets_count}: {ticket['subject'][:40]}...")
        response = post_ticket(ticket)
        results["tickets"].append(response)
        time.sleep(delay_between_requests)
        
        # If ticket was created successfully, add time entries, conversations, and tasks
        if "ticket" in response:
            created_ticket_id = response["ticket"]["id"]
            
            # Create time entries for this ticket
            time_entries = generate_time_entries(created_ticket_id, count=2)
            for te in time_entries:
                print(f"    Adding time entry for ticket {created_ticket_id}...")
                te_response = post_time_entry(created_ticket_id, te)
                results["time_entries"].append(te_response)
                time.sleep(delay_between_requests)
            
            # Create a note for this ticket
            conversations = generate_conversations(created_ticket_id, count=1)
            for conv in conversations:
                conv["private"] = True  # Create as note
                print(f"    Adding note for ticket {created_ticket_id}...")
                conv_response = post_conversation(created_ticket_id, conv)
                results["conversations"].append(conv_response)
                time.sleep(delay_between_requests)
            
            # Create tasks for this ticket
            tasks = generate_tasks(created_ticket_id, "ticket", count=2)
            for task in tasks:
                print(f"    Adding task for ticket {created_ticket_id}...")
                task_response = post_task(created_ticket_id, "ticket", task)
                results["tasks"].append(task_response)
                time.sleep(delay_between_requests)
    step += 1
    
    # Create problems
    print(f"\n[{step}/{total_steps}] Creating {problems_count} problems...")
    problems = generate_problems(count=problems_count)
    for i, problem in enumerate(problems, 1):
        print(f"  Creating problem {i}/{problems_count}: {problem['subject'][:40]}...")
        response = post_problem(problem)
        results["problems"].append(response)
        time.sleep(delay_between_requests)
        
        # Add tasks for problems
        if "problem" in response:
            created_problem_id = response["problem"]["id"]
            tasks = generate_tasks(created_problem_id, "problem", count=1)
            for task in tasks:
                print(f"    Adding task for problem {created_problem_id}...")
                task_response = post_task(created_problem_id, "problem", task)
                results["tasks"].append(task_response)
                time.sleep(delay_between_requests)
    step += 1
    
    # Create changes
    print(f"\n[{step}/{total_steps}] Creating {changes_count} changes...")
    changes = generate_changes(count=changes_count)
    for i, change in enumerate(changes, 1):
        print(f"  Creating change {i}/{changes_count}: {change['subject'][:40]}...")
        response = post_change(change)
        results["changes"].append(response)
        time.sleep(delay_between_requests)
        
        # Add tasks for changes
        if "change" in response:
            created_change_id = response["change"]["id"]
            tasks = generate_tasks(created_change_id, "change", count=2)
            for task in tasks:
                print(f"    Adding task for change {created_change_id}...")
                task_response = post_task(created_change_id, "change", task)
                results["tasks"].append(task_response)
                time.sleep(delay_between_requests)
    step += 1
    
    # Create releases
    print(f"\n[{step}/{total_steps}] Creating {releases_count} releases...")
    releases = generate_releases(count=releases_count)
    for i, release in enumerate(releases, 1):
        print(f"  Creating release {i}/{releases_count}: {release['subject'][:40]}...")
        response = post_release(release)
        results["releases"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create solutions (knowledge base articles)
    print(f"\n[{step}/{total_steps}] Creating {solutions_count} solutions...")
    solutions = generate_solutions(count=solutions_count)
    for i, solution in enumerate(solutions, 1):
        print(f"  Creating solution {i}/{solutions_count}: {solution['title'][:40]}...")
        response = post_solution(solution)
        results["solutions"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create announcements
    print(f"\n[{step}/{total_steps}] Creating {announcements_count} announcements...")
    announcements = generate_announcements(count=announcements_count)
    for i, announcement in enumerate(announcements, 1):
        print(f"  Creating announcement {i}/{announcements_count}: {announcement['title'][:40]}...")
        response = post_announcement(announcement)
        results["announcements"].append(response)
        time.sleep(delay_between_requests)
    step += 1
    
    # Create purchase orders
    print(f"\n[{step}/{total_steps}] Creating {purchase_orders_count} purchase orders...")
    purchase_orders = generate_purchase_orders(count=purchase_orders_count)
    for i, po in enumerate(purchase_orders, 1):
        print(f"  Creating purchase order {i}/{purchase_orders_count}: {po['name'][:40]}...")
        response = post_purchase_order(po)
        results["purchase_orders"].append(response)
        time.sleep(delay_between_requests)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    success_counts = {}
    error_counts = {}
    
    for obj_type, responses in results.items():
        success = sum(1 for r in responses if "error" not in r)
        errors = len(responses) - success
        success_counts[obj_type] = success
        error_counts[obj_type] = errors
        status = "✓" if errors == 0 else "⚠"
        print(f"  {status} {obj_type}: {success} created, {errors} errors")
    
    print("=" * 60)
    
    return results


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    import json
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--post":
        # Post dummy data to Freshservice
        print("\n*** POSTING DATA TO FRESHSERVICE ***\n")
        
        # Parse optional counts from command line
        tickets = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        problems = int(sys.argv[3]) if len(sys.argv) > 3 else 2
        changes = int(sys.argv[4]) if len(sys.argv) > 4 else 2
        releases = int(sys.argv[5]) if len(sys.argv) > 5 else 1
        
        results = post_all_dummy_data(
            tickets_count=tickets,
            problems_count=problems,
            changes_count=changes,
            releases_count=releases
        )
        
        # Print detailed results
        print("\nDetailed Results:")
        print(json.dumps(results, indent=2, default=str))
        
    else:
        # Just generate and display dummy data
        all_data = generate_all_dummy_data()
        
        # Print summary
        print("Generated Freshservice Dummy Data:")
        print("=" * 50)
        for object_type, records in all_data.items():
            print(f"  {object_type}: {len(records)} records")
        print("=" * 50)
        
        # Print sample ticket
        print("\nSample Ticket:")
        print(json.dumps(all_data["tickets"][0], indent=2))
        
        # Print sample agent
        print("\nSample Agent:")
        print(json.dumps(all_data["agents"][0], indent=2))
        
        print("\n" + "=" * 50)
        print("To POST data to Freshservice, run:")
        print(f"  python {sys.argv[0]} --post [tickets] [problems] [changes] [releases]")
        print("\nExample:")
        print(f"  python {sys.argv[0]} --post 5 3 2 1")
        print("=" * 50)

