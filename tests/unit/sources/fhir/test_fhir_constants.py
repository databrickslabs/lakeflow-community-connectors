from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    DEFAULT_RESOURCES, CURSOR_FIELD, RETRIABLE_STATUS_CODES,
    MAX_RETRIES, INITIAL_BACKOFF, DEFAULT_PAGE_SIZE, DEFAULT_MAX_RECORDS,
)

def test_default_resources_is_list_of_strings():
    assert isinstance(DEFAULT_RESOURCES, list)
    assert all(isinstance(r, str) for r in DEFAULT_RESOURCES)
    assert len(DEFAULT_RESOURCES) > 0

def test_default_resources_contains_core_types():
    for r in ["Patient", "Observation", "Encounter", "Condition"]:
        assert r in DEFAULT_RESOURCES

def test_cursor_field():
    assert CURSOR_FIELD == "lastUpdated"

def test_retriable_status_codes():
    assert {429, 500, 502, 503} == RETRIABLE_STATUS_CODES

def test_retry_config():
    assert MAX_RETRIES >= 3
    assert INITIAL_BACKOFF > 0
    assert DEFAULT_PAGE_SIZE > 0
    assert DEFAULT_MAX_RECORDS > 0
