"""Constants for the FHIR R4 connector."""

DEFAULT_RESOURCES = [
    "Patient",
    "Observation",
    "Condition",
    "Encounter",
    "Procedure",
    "MedicationRequest",
    "DiagnosticReport",
    "AllergyIntolerance",
    "Immunization",
    "Coverage",
    "CarePlan",
    "Goal",
    "Device",
    "DocumentReference",
]

PRIMARY_KEYS = ["resource_id"]
CURSOR_FIELD = "last_updated"

RETRIABLE_STATUS_CODES = {429, 500, 502, 503}  # 502: Azure/AWS
MAX_RETRIES = 5
INITIAL_BACKOFF = 5.0  # seconds; doubled after each retry (5→10→20→40→80)
PAGE_DELAY = 0.0  # seconds to sleep between paginated requests; set to 1.0 for public servers
HTTP_TIMEOUT = 60  # seconds; timeout for FHIR API requests
TOKEN_TIMEOUT = 30  # seconds; timeout for OAuth2 token requests
MAX_RETRY_AFTER = 300  # seconds; ceiling on a server-supplied Retry-After value

DEFAULT_PAGE_SIZE = 100  # _count parameter sent to FHIR server
DEFAULT_MAX_RECORDS = 1000  # max records per read_table() call

# Bulk export constants (HL7 Bulk Data Access IG)
BULK_POLL_INTERVAL = 30  # seconds between status poll requests
BULK_EXPORT_TIMEOUT = 14400  # seconds (4 hours) max wait for export completion
BULK_NDJSON_TIMEOUT = 300  # seconds; NDJSON file download timeout (files can be GBs)
BULK_VALID_SCOPES = {"system", "patient", "group"}
BULK_VALID_INCREMENTAL_MODES = {"since", "typefilter", "full"}
BULK_MAX_REDIRECTS = 5  # max hops when following NDJSON download redirects
BULK_MIN_POLL_SLEEP = 0.5  # seconds; below this the remaining budget is spent, so stop polling
