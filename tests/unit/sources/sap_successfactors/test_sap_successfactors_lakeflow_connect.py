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

    # ------------------------------------------------------------------
    # status_filter — inactive records must not be silently dropped
    # ------------------------------------------------------------------

    def _captured_params(self, monkeypatch, table, start_offset=None):
        """Run a read and return the query params handed to the HTTP layer."""
        captured = {}

        def fake_fetch(url, params, cursor_field):
            captured.update(params)
            return [], None

        monkeypatch.setattr(self.connector, "_fetch_all_pages", fake_fetch)
        self.connector.read_table(table, start_offset, {})
        return captured

    def test_user_read_requests_inactive_users(self, monkeypatch):
        """``User`` omits inactive users unless the status predicate is sent.

        Under cdc ingestion the pipeline upserts and never deletes, so a user
        dropping out of the extract is never reflected downstream — the row
        stays current forever. See SAP KBA 2166571.
        """
        params = self._captured_params(monkeypatch, "User")
        assert "status in 't','f'" in params["$filter"], (
            "User read must ask for inactive users explicitly; "
            f"got $filter={params.get('$filter')!r}"
        )

    def test_status_filter_combines_with_the_cursor_bounds(self, monkeypatch):
        """The status predicate must not displace the incremental bounds."""
        params = self._captured_params(
            monkeypatch, "User", {"cursor_value": "2020-01-01T00:00:00"}
        )
        f = params["$filter"]
        assert "lastModifiedDateTime gt datetime'2020-01-01T00:00:00'" in f
        assert "lastModifiedDateTime le datetime'" in f
        assert "status in 't','f'" in f
        assert f.count(" and ") == 2, f"expected three conjoined predicates, got {f!r}"

    def test_tables_without_a_status_filter_are_unchanged(self, monkeypatch):
        """Only tables declaring ``status_filter`` get one.

        FOLocation is the specific regression guard. Verified live: it already
        returns inactive records unfiltered, and sending it the User predicate
        (``status in 't','f'``) returns HTTP 200 with **zero rows** rather than
        an error — the t/f domain matches nothing there. A speculative
        status_filter would empty the table silently.
        """
        params = self._captured_params(monkeypatch, "FOLocation")
        assert "status" not in params.get("$filter", "")

    def test_status_filter_applies_to_snapshot_reads(self, monkeypatch):
        """An entity that hides inactive records does so regardless of
        ingestion type, so the snapshot path needs the predicate too."""
        from databricks.labs.community_connector.sources.sap_successfactors.table_metadata import (
            TABLE_CONFIG,
        )

        snapshot_table = next(
            t for t, c in TABLE_CONFIG.items() if c.get("ingestion_type") == "snapshot"
        )
        monkeypatch.setitem(
            TABLE_CONFIG[snapshot_table], "status_filter", "status in 't','f'"
        )
        params = self._captured_params(monkeypatch, snapshot_table)
        assert params.get("$filter") == "status in 't','f'"

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
