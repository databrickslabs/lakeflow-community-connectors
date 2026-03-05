"""Constants for the FHIR R4 connector."""

DEFAULT_RESOURCES = [
    "Patient", "Observation", "Condition", "Encounter",
    "Procedure", "MedicationRequest", "DiagnosticReport",
    "AllergyIntolerance", "Immunization", "Coverage",
    "CarePlan", "Goal", "Device", "DocumentReference",
]

CURSOR_FIELD = "lastUpdated"

RETRIABLE_STATUS_CODES = {429, 500, 503}
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

DEFAULT_PAGE_SIZE = 100   # _count parameter sent to FHIR server
DEFAULT_MAX_RECORDS = 1000  # max records per read_table() call
