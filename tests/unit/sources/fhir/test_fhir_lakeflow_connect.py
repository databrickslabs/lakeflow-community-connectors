"""Integration test for FhirLakeflowConnect using LakeflowConnectTests.

Runs against whatever FHIR server is configured in:
    tests/unit/sources/fhir/configs/dev_config.json

The default config points at the public HAPI FHIR R4 server (auth_type=none).
To test against a SMART-enabled server, replace dev_config.json with your
SMART credentials (see README.md Authentication Setup, or run authenticate.py).
"""

from databricks.labs.community_connector.sources.fhir.fhir import FhirLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestFhirConnector(LakeflowConnectTests):
    connector_class = FhirLakeflowConnect
    sample_records = 5  # HAPI FHIR is a public server, be considerate.
