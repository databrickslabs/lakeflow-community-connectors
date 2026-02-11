"""
Table group definitions for SAP SuccessFactors segmented testing.

Groups tables by SuccessFactors module for parallel test execution.
"""

# Import table config to derive table names
from databricks.labs.community_connector.sources.sap_successfactors.sap_successfactors import (
    _TABLE_CONFIG,
)

# Get all table names
ALL_TABLES = list(_TABLE_CONFIG.keys())


def _categorize_tables():
    # pylint: disable=too-many-branches
    """Categorize tables into module groups based on naming patterns."""
    groups = {
        "EC-Core": [],        # Employee Central Core
        "EC-Extended": [],    # EC Foundation & Organization
        "PLT-Core": [],       # Platform Core & User Management
        "PLT-Extended": [],   # Platform Extended Features
        "Recruiting": [],     # Recruiting Module
        "Performance": [],    # Performance & Goals
        "Time-Core": [],      # Time Management Core
        "Time-Extended": [],  # Time Extended
        "Onboarding": [],     # Onboarding & Succession
        "General-Objects": [],# Budget, Fund, Grant objects
        "Specialized-1": [],  # Compliance, Custom, Attachments
        "Specialized-2": [],  # Miscellaneous
    }

    categorized = set()

    for table in ALL_TABLES:
        table_lower = table.lower()

        # EC-Core: Employee employment, compensation, benefits, payroll
        if any(pattern in table_lower for pattern in [
            'empemployment', 'empjob', 'empcompensation', 'emppay',
            'emppension', 'empwork', 'empbenefic',
            'employeecompensation', 'employeedismissal', 'employeepayroll',
            'advance', 'deduction', 'recurringdeduction', 'onetimededuction',
            'benefit', 'hireDatechange', 'personemptermination',
            'paymentinformation', 'paymentmethod', 'custompaytypeassignment', 'custompaytype'
        ]):
            groups["EC-Core"].append(table)
            categorized.add(table)
            continue

        # EC-Extended: Foundation Objects (FO*), Cost Assignment
        if table.startswith('FO') or table.lower() in ['empcostassignment', 'competencyentity']:
            groups["EC-Extended"].append(table)
            categorized.add(table)
            continue

        # PLT-Core: User, Permissions, Dynamic Groups, MDF, Picklist
        if any(pattern in table_lower for pattern in [
            'user', 'rbp', 'dynamicgroup', 'dgfield', 'dgfilter', 'dgexpression',
            'dgpeople', 'mdf', 'picklist', 'picklistv2', 'picklistoption', 'picklistvalue',
            'localizeddata', 'scimgroup', 'scimuser', 'scim_group', 'scim_user'
        ]) and 'externaluser' not in table_lower:
            # Exclude ExternalUser which goes to specialized
            if table not in ['ExternalUser']:
                groups["PLT-Core"].append(table)
                categorized.add(table)
                continue

        # PLT-Extended: Custom nav, themes, todos, success store
        if any(pattern in table_lower for pattern in [
            'custom_nav', 'theme_', 'todo', 'success_store',
            'emmonitored', 'emevent', 'execution'
        ]):
            groups["PLT-Extended"].append(table)
            categorized.add(table)
            continue

        # Recruiting: Candidates, Job Applications, Job Offers, Job Requisitions
        if any(pattern in table_lower for pattern in [
            'candidate', 'jobapplication', 'joboffer', 'jobreq', 'interview',
            'rcm', 'offerletter', 'jobofferapp'
        ]):
            groups["Recruiting"].append(table)
            categorized.add(table)
            continue

        # Performance: Forms, Goals, Achievements, Calibration, Feedback, CPM
        if any(pattern in table_lower for pattern in [
            'form_', 'form360', 'goal_', 'achievement', 'activity',
            'calibration', 'feedback', 'cpm_', 'review_route',
            'continuous_performance', 'supporter_feedback', 'talent'
        ]) and 'onboarding' not in table_lower and not table.startswith('ONB'):
            groups["Performance"].append(table)
            categorized.add(table)
            continue

        # Time-Core: Time events, Clock in/out
        if any(pattern in table_lower for pattern in [
            'time_event', 'clock_in', 'timeoff'
        ]):
            groups["Time-Core"].append(table)
            categorized.add(table)
            continue

        # Time-Extended: Calendar Session (none currently)
        # Will include any time-related that didn't match Time-Core

        # Onboarding: ONB*, Onboarding*, Journey, Successor, Nomination
        if any(pattern in table_lower for pattern in [
            'onboarding', 'onb2', 'journey', 'successor', 'nomination'
        ]):
            groups["Onboarding"].append(table)
            categorized.add(table)
            continue

        # General Objects: Budget, Fund, Grant, Project Controlling, PBC
        if any(pattern in table_lower for pattern in [
            'budget', 'fund', 'grant', 'project_controlling', 'pbc_',
            'functional_area'
        ]):
            groups["General-Objects"].append(table)
            categorized.add(table)
            continue

        # Specialized-1: Compliance, Custom fields, Attachments, I9
        if any(pattern in table_lower for pattern in [
            'compliance', 'cust_', 'custom_tasks', 'attachment', 'photo',
            'i9_', 'extension_point', 'assignedcomplianceform'
        ]):
            groups["Specialized-1"].append(table)
            categorized.add(table)
            continue

    # Specialized-2: Everything else not categorized
    for table in ALL_TABLES:
        if table not in categorized:
            groups["Specialized-2"].append(table)

    # Sort each group alphabetically
    for group_name in groups:
        groups[group_name] = sorted(groups[group_name])

    return groups


# Generate table groups
TABLE_GROUPS = _categorize_tables()

# Group descriptions
GROUP_DESCRIPTIONS = {
    "EC-Core": "Employee Central Core - Employment, Compensation, Benefits, Payroll",
    "EC-Extended": "EC Extended - Foundation Objects, Organization, Skills",
    "PLT-Core": "Platform Core - Users, Permissions, Dynamic Groups, Metadata",
    "PLT-Extended": "Platform Extended - Custom Nav, Themes, Todos, Events",
    "Recruiting": "Recruiting - Candidates, Job Applications, Offers, Requisitions",
    "Performance": "Performance & Goals - Forms, Goals, Calibration, Feedback",
    "Time-Core": "Time Management Core - Time Events, Clock In/Out",
    "Time-Extended": "Time Extended - Additional Time Features",
    "Onboarding": "Onboarding & Succession - Onboarding, Journey, Succession",
    "General-Objects": "General Objects - Budget, Fund, Grant, Project",
    "Specialized-1": "Specialized - Compliance, Custom Fields, Attachments",
    "Specialized-2": "Miscellaneous - Country, Bank, Territory, Other",
}


def get_group_tables(group_name: str) -> list:
    """Get tables for a specific group."""
    if group_name not in TABLE_GROUPS:
        raise ValueError(f"Unknown group: {group_name}. Available: {list(TABLE_GROUPS.keys())}")
    return TABLE_GROUPS[group_name]


def get_all_groups() -> list:
    """Get all group names."""
    return list(TABLE_GROUPS.keys())


def print_group_summary():
    """Print summary of all groups."""
    print("\n=== SAP SuccessFactors Table Groups ===\n")
    total = 0
    for group_name, tables in TABLE_GROUPS.items():
        count = len(tables)
        total += count
        desc = GROUP_DESCRIPTIONS.get(group_name, "")
        print(f"{group_name}: {count} tables")
        print(f"  {desc}")
        if count <= 10:
            for t in tables:
                print(f"    - {t}")
        else:
            for t in tables[:5]:
                print(f"    - {t}")
            print(f"    ... and {count - 5} more")
        print()
    print(f"Total: {total} tables across {len(TABLE_GROUPS)} groups")


if __name__ == "__main__":
    print_group_summary()
