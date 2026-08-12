import requests

from databricks.labs.community_connector.sources.remberg.remberg import (
    RembergLakeflowConnect,
    _retry_after_seconds,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


def _response_with_headers(headers: dict) -> requests.Response:
    resp = requests.Response()
    resp.headers.update(headers)
    return resp


class TestRembergConnector(LakeflowConnectTests):
    connector_class = RembergLakeflowConnect
    simulator_source = "remberg"
    replay_config = {
        # The live API root is https://api.remberg.de; the simulator matches
        # on the URL path only (/v2/assets, /v1/contacts, ...), so any host
        # works here. Keep an obviously-fake one.
        "base_url": "https://simulator.remberg.example",
        "api_key": "simulator-fake-key",
    }

    # ------------------------------------------------------------------
    # remberg-specific behaviour
    # ------------------------------------------------------------------

    def test_auth_header_is_raw_key_in_lowercase_authorization(self):
        """The OpenAPI security scheme is ``type: apiKey, in: header,
        name: authorization`` — the raw key, not ``Bearer <key>``. A
        well-meaning "fix" adding a Bearer prefix would break live auth."""
        headers = self.connector._headers
        assert headers["authorization"] == "simulator-fake-key"

    def test_incremental_caps_at_init_time(self):
        """The simulator seeds future-dated records (see
        ``synthesize_future_records`` in the spec); the first read must
        exclude them via ``updatedAtUntil = _init_ts`` and park the cursor
        at init time so Trigger.AvailableNow terminates."""
        records, offset = self.connector.read_table("assets", {}, {})
        rows = list(records)
        init_iso = self.connector._init_ts_iso
        assert offset == {"cursor": init_iso}
        assert rows, "expected the seeded corpus records on the first read"
        for row in rows:
            assert row["updatedAt"] <= init_iso, (
                f"record with updatedAt={row['updatedAt']} leaked past the "
                f"init-time cap {init_iso}"
            )

    def test_max_records_per_batch_splits_range_without_loss(self):
        """``max_records_per_batch`` splits a bounded updatedAt range across
        microbatches with the range pinned in the offset; the union of the
        microbatches must cover every record exactly once."""
        seen_ids: list[str] = []
        offset: dict = {}
        for _ in range(20):
            records, next_offset = self.connector.read_table(
                "assets", offset, {"limit": "2", "max_records_per_batch": "2"}
            )
            seen_ids.extend(r["id"] for r in records)
            if next_offset == offset:
                break
            offset = next_offset
        else:
            raise AssertionError("paged incremental read did not converge")

        full_records, _ = self.connector.read_table("assets", {}, {})
        expected_ids = {r["id"] for r in full_records}
        assert len(seen_ids) == len(set(seen_ids)), (
            f"duplicate records across microbatches: {seen_ids}"
        )
        assert set(seen_ids) == expected_ids, (
            f"paged read missed records: got {sorted(seen_ids)}, "
            f"expected {sorted(expected_ids)}"
        )

    def test_lookback_not_baked_into_stored_cursor(self):
        """Lookback is a read-time widening only. The stored cursor after a
        drained range must be the range's upper bound — never the widened
        lower bound — or the cursor would walk backwards forever."""
        _, first_offset = self.connector.read_table("assets", {}, {})
        records, second_offset = self.connector.read_table(
            "assets", first_offset, {"lookback_seconds": "86400"}
        )
        list(records)
        assert second_offset == first_offset, (
            "caught-up read must return start_offset unchanged "
            "(termination contract), even with a large lookback"
        )

    def test_retry_after_takes_longest_advertised_wait(self):
        """remberg 429s advertise ``Retry-After-Burst`` / ``Retry-After-Base``
        per throttler, and 429s count against the limit — retrying after the
        shorter wait would burn a request, so the connector must honour the
        longest one."""
        resp = _response_with_headers(
            {"Retry-After-Burst": "1", "Retry-After-Base": "5"}
        )
        assert _retry_after_seconds(resp) == 5.0
        assert _retry_after_seconds(_response_with_headers({"Retry-After": "3"})) == 3.0
        assert _retry_after_seconds(_response_with_headers({})) is None
        assert _retry_after_seconds(
            _response_with_headers({"Retry-After-Base": "soon"})
        ) is None

    def test_ticket_custom_property_values_json_encoded(self):
        """``customPropertyValues[].value`` is untyped on the API (strings,
        numbers, objects...). Non-string values must be JSON-serialized so
        the StringType column stays stable."""
        raw = {
            "id": "t-1",
            "customPropertyValues": [
                {"reference": "cp_a", "value": {"k": 1}, "associationValue": [7, "x"]},
                {"reference": "cp_b", "value": "plain", "associationValue": None},
            ],
        }
        mapped = self.connector._map_record("tickets", raw)
        cpvs = mapped["customPropertyValues"]
        assert cpvs[0]["value"] == '{"k": 1}'
        assert cpvs[0]["associationValue"] == ["7", "x"]
        assert cpvs[1]["value"] == "plain"
        assert cpvs[1]["associationValue"] is None
