"""Test fixtures for the OData connector.

The connector keeps a process-wide CSDL cache keyed by ``service_url``
so SDP doesn't re-fetch + re-parse multi-MB ``$metadata`` documents
for every table during pipeline INITIALIZING. Tests reuse a single
``SERVICE_URL`` across cases with different mocked ``$metadata``
bodies — clear the cache between tests so one case can't leak its
parsed tree into the next.
"""

import pytest

from databricks.labs.community_connector.sources.odata.odata import (
    _clear_capability_cache,
    _clear_metadata_cache,
)


@pytest.fixture(autouse=True)
def _reset_odata_metadata_cache():
    _clear_metadata_cache()
    # Same reuse problem for the process/file capability cache: tests share
    # SERVICE_URL across cases whose mocked servers have different $batch /
    # $expand behaviours, so one case's verdict must not leak into the next.
    _clear_capability_cache()
    yield
    _clear_metadata_cache()
    _clear_capability_cache()
