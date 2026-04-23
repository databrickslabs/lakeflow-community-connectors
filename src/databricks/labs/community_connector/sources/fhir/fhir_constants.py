"""Constants for the FHIR R4 connector."""

DEFAULT_RESOURCES = [
    "Patient", "Observation", "Condition", "Encounter",
    "Procedure", "MedicationRequest", "DiagnosticReport",
    "AllergyIntolerance", "Immunization", "Coverage",
    "CarePlan", "Goal", "Device", "DocumentReference",
]

CURSOR_FIELD = "lastUpdated"

RETRIABLE_STATUS_CODES = {429, 500, 502, 503}  # 502: Azure/AWS
MAX_RETRIES = 5
INITIAL_BACKOFF = 5.0  # seconds; doubled after each retry (5→10→20→40→80)
PAGE_DELAY = 0.0       # seconds to sleep between paginated requests; set to 1.0 for public servers
HTTP_TIMEOUT = 60   # seconds; timeout for FHIR API requests
TOKEN_TIMEOUT = 30  # seconds; timeout for OAuth2 token requests

DEFAULT_PAGE_SIZE = 100   # _count parameter sent to FHIR server
DEFAULT_MAX_RECORDS = 1000  # max records per read_table() call
