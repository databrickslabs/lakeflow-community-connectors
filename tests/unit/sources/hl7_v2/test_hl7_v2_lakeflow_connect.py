"""Tests for the HL7 v2 community connector (GCP Healthcare API).

Runs the standard LakeflowConnectTests suite against the simulator by
default (offline, deterministic, no creds required). To exercise a real
GCP HL7v2 store, set ``CONNECTOR_TEST_MODE=live`` and provide credentials
via ``CONNECTOR_TEST_CONFIG_JSON`` or ``CONNECTOR_TEST_CONFIG_PATH``.
"""

import json

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import HL7V2LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


def _generate_pem_key() -> str:
    """Build a real RSA key in PKCS#8 PEM form.

    The connector calls ``google.oauth2.service_account.Credentials.
    from_service_account_info`` in its ``__init__``, which deserializes
    the PEM eagerly. The simulator intercepts every outbound HTTP call,
    so the key is never used for actual signing — but it must parse.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem.decode("ascii")


class TestHL7V2Connector(LakeflowConnectTests):
    connector_class = HL7V2LakeflowConnect
    simulator_source = "hl7_v2"
    sample_records = 5

    @classmethod
    def _replay_config(cls):
        return {
            "source_type": "gcp",
            "project_id": "sim-project",
            "location": "us-central1",
            "dataset_id": "sim-dataset",
            "hl7v2_store_id": "sim-store",
            "service_account_json": json.dumps({
                "type": "service_account",
                "project_id": "sim-project",
                "private_key_id": "sim-key-id",
                "private_key": _generate_pem_key(),
                "client_email": "sim@sim-project.iam.gserviceaccount.com",
                "client_id": "0",
                "token_uri": "https://oauth2.googleapis.com/token",
            }),
        }
