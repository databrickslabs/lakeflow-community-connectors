import pytest

from databricks.labs.community_connector.sources.sap_successfactors.sap_successfactors import (
    SapSuccessFactorsLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


# TABLE_CONFIG declares pairs like ``ScimGroup``/``scim_group`` and
# ``CalibrationSession``/``calibration_session`` — distinct tables that hit the
# same OData entity-set URL but have *incompatible* schemas (e.g. one expects
# ``members: string`` while the other expects ``members: array<struct<...>>``).
# A single canned response can't satisfy both, so the snake_case shadows are
# excluded from the read iteration; the PascalCase entry still exercises the
# URL.
_INCOMPATIBLE_SHADOW_TABLES = {
    "calibration_session",
    "scim_group",
    "scim_user",
}


class TestSapSuccessFactorsConnector(LakeflowConnectTests):
    connector_class = SapSuccessFactorsLakeflowConnect
    simulator_source = "sap_successfactors"
    replay_config = {
        "endpoint_url": "https://simulator.example.com",
        "username": "simulator-user",
        "password": "simulator-fake-password",
    }

    # ``password`` is declared in these schemas for write-back / round-trip
    # compatibility, but the SuccessFactors OData API never returns it in
    # read responses (security-sensitive field). No corpus record can ever
    # exercise the column, so it is exempt from the column-coverage check.
    allow_null_columns = {
        "Candidate": {"password"},
        "CandidateLight": {"password"},
        "ExternalUser": {"password"},
        "User": {"password"},
    }

    def test_read_table(self):
        """Same as the harness contract, but skips the snake_case shadow tables
        whose schemas are incompatible with their PascalCase siblings."""
        tables = [
            t for t in self._non_partitioned_tables()
            if t not in _INCOMPATIBLE_SHADOW_TABLES
        ]
        if not tables:
            pytest.skip("All tables use partitioned reads")
        errors = []
        for table in tables:
            err = self._validate_read(
                table, self.connector.read_table, "read_table", is_read_table=True
            )
            if err:
                errors.append(err)
        if errors:
            pytest.fail("\n\n".join(errors))

    def test_every_column_populated_by_at_least_one_record(self):
        """Same as the harness contract, but skips the incompatible shadow tables."""
        tables = [
            t for t in self._non_partitioned_tables()
            if t not in _INCOMPATIBLE_SHADOW_TABLES
        ]
        if not tables:
            pytest.skip("All tables use partitioned reads")
        errors = []
        for table in tables:
            err = self._validate_column_population(table)
            if err:
                errors.append(err)
        if errors:
            pytest.fail("\n\n".join(errors))
