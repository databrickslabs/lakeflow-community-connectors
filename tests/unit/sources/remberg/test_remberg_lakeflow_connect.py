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

    def test_record_count_is_independent_of_page_size(self):
        """Reading the same table at different page sizes must return the
        same records.

        Regression test for a live data-loss bug: remberg's ``page`` is
        0-indexed (its prose docs claim 1-indexed), and the connector started
        at page 1 — silently dropping the first ``limit`` records of every
        list endpoint. It showed up as the record count varying with page
        size: a tenant with 80 work orders yielded 60 rows at ``limit=20``
        and 75 at ``limit=5``.

        Covers flat, snapshot and fan-out reads, since all three paginate.
        """
        for table in ("work_orders", "contacts", "asset_status_signals"):
            by_size = {}
            for limit in ("1", "2", "3", "1000"):
                records, _ = self.connector.read_table(table, {}, {"limit": limit})
                by_size[limit] = sorted(r["id"] for r in records)

            baseline = by_size["1000"]
            assert baseline, f"[{table}] expected seeded corpus records"
            for limit, ids in by_size.items():
                assert ids == baseline, (
                    f"[{table}] limit={limit} returned {len(ids)} records but "
                    f"limit=1000 returned {len(baseline)}. Page size must not "
                    f"change which records are read.\n"
                    f"  missing: {sorted(set(baseline) - set(ids))}\n"
                    f"  extra:   {sorted(set(ids) - set(baseline))}"
                )

    def test_first_page_is_page_zero(self):
        """The very first request of a read must ask for page 0.

        remberg is 0-indexed; asking for page 1 first skips a page. Asserted
        on the wire rather than via counts so the intent is unmistakable if
        someone later "corrects" PAGE_BASE back to 1.
        """
        seen: list[str] = []
        original = self.connector._get_json

        def spy(path, params=None, route=None, missing_ok=False):
            if params and "page" in params:
                seen.append(params["page"])
            return original(path, params=params, route=route, missing_ok=missing_ok)

        self.connector._get_json = spy
        try:
            list(self.connector.read_table("work_orders", {}, {"limit": "2"})[0])
        finally:
            self.connector._get_json = original

        assert seen and seen[0] == "0", (
            f"first request used page={seen[:1]}, expected '0' — a 1-indexed "
            "start silently drops the first page against the live API"
        )

    # ------------------------------------------------------------------
    # Fan-out (child) tables
    # ------------------------------------------------------------------

    def test_child_read_fans_out_over_every_parent_in_range(self):
        """A child table must issue one child request per parent in the
        bounded range and stamp each row with the parent it came from. The
        simulator serves the same 3-entry corpus for every parent, so a
        correct fan-out over the 5 seeded work orders yields 15 rows spread
        across all 5 parent ids."""
        parents, _ = self.connector.read_table("work_orders", {}, {})
        parent_ids = {p["id"] for p in parents}
        assert len(parent_ids) == 5, "expected the 5 seeded work orders"

        records, offset = self.connector.read_table("work_order_times", {}, {})
        rows = list(records)
        assert offset == {"cursor": self.connector._init_ts_iso}
        assert len(rows) == 15, (
            f"expected 5 parents x 3 time entries, got {len(rows)} — "
            "the fan-out stopped early or skipped parents"
        )
        assert {r["workOrderId"] for r in rows} == parent_ids

    def test_child_rows_carry_parent_cursor_as_cursor_field(self):
        """``cursor_field`` for a child table is the injected parent
        ``updatedAt``. It must be populated on every row and must match the
        parent it was fanned out from — the framework sequences on it."""
        parents, _ = self.connector.read_table("work_orders", {}, {})
        updated_by_id = {p["id"]: p["updatedAt"] for p in parents}

        records, _ = self.connector.read_table("work_order_times", {}, {})
        rows = list(records)
        assert rows
        for row in rows:
            assert row["workOrderUpdatedAt"] == updated_by_id[row["workOrderId"]], (
                "child row's cursor must be its own parent's updatedAt"
            )

    def test_child_read_throttles_on_route_not_concrete_path(self):
        """remberg rate-limits per endpoint route, and a 429 itself counts
        against the limit. The throttle must therefore bucket every
        ``/v2/work-orders/{id}/times`` call together; keying on the concrete
        path would give each parent its own bucket, space nothing, and
        429-storm on the first fan-out."""
        self.connector._last_request_at.clear()
        list(self.connector.read_table("work_order_times", {}, {})[0])

        keys = set(self.connector._last_request_at)
        assert "v2/work-orders/{id}/times" in keys, (
            f"child route not used as the throttle key; got {sorted(keys)}"
        )
        concrete = [k for k in keys if k.startswith("v2/work-orders/") and "{id}" not in k]
        assert not concrete, (
            f"per-parent throttle buckets leaked in: {concrete} — every parent "
            "would be requested with no spacing"
        )

    def test_child_max_records_per_batch_splits_without_loss(self):
        """Splitting a fan-out across microbatches must cover every child row
        exactly once. The split happens at parent granularity, so a parent's
        children are never torn across two batches."""
        def identity(row):
            return (row["workOrderId"], row["performingPersonId"], row["startTime"])

        seen: list[tuple] = []
        offset: dict = {}
        for _ in range(30):
            records, next_offset = self.connector.read_table(
                "work_order_times", offset, {"limit": "2", "max_records_per_batch": "2"}
            )
            seen.extend(identity(r) for r in records)
            if next_offset == offset:
                break
            offset = next_offset
        else:
            raise AssertionError("paged fan-out read did not converge")

        full, _ = self.connector.read_table("work_order_times", {}, {})
        expected = {identity(r) for r in full}
        assert len(seen) == len(set(seen)), f"duplicate child rows across batches: {seen}"
        assert set(seen) == expected, "paged fan-out missed child rows"

    def test_full_parent_scan_ignores_cursor_but_still_terminates(self):
        """``full_parent_scan`` drops the range's lower bound so every parent
        is revisited — the escape hatch for backfills, or if child edits turn
        out not to bump the parent's updatedAt. It must NOT bypass the
        caught-up check, or the trigger could never terminate."""
        _, first = self.connector.read_table("work_order_times", {}, {})
        records, second = self.connector.read_table(
            "work_order_times", first, {"full_parent_scan": "true"}
        )
        assert not list(records)
        assert second == first, (
            "caught-up read must return start_offset unchanged even under "
            "full_parent_scan (termination contract)"
        )

    def test_child_tolerates_parent_that_404s(self):
        """A parent listed at the start of a range can be deleted before its
        sub-resource is fetched. One vanished parent must not fail the whole
        microbatch."""
        resp = requests.Response()
        resp.status_code = 404
        resp._content = b'{"error": "Not Found"}'
        original = self.connector._request
        try:
            self.connector._request = lambda *a, **kw: resp
            body = self.connector._get_json(
                "v2/work-orders/gone/times",
                route="v2/work-orders/{id}/times",
                missing_ok=True,
            )
        finally:
            self.connector._request = original
        assert body is None
        assert self.connector._unwrap_records(body, "timeEntries") == []

    def test_ticket_conversation_creator_json_encoded(self):
        """``creator`` is an untyped object in the OpenAPI spec (no declared
        properties), so it is JSON-serialized rather than guessed at as a
        struct — the same treatment as ticket customPropertyValues."""
        mapped = self.connector._map_record(
            "ticket_conversations",
            {
                "ticketId": "t-1",
                "createdAt": "2024-02-01T09:12:00Z",
                "kind": "note",
                "creator": {"id": "u-1", "email": "a@b.c"},
                "plainText": "hello",
            },
        )
        assert mapped["creator"] == '{"id": "u-1", "email": "a@b.c"}'
        assert mapped["kind"] == "note"

    # ------------------------------------------------------------------
    # Flat sub-resource table
    # ------------------------------------------------------------------

    def test_asset_status_signals_read_as_flat_cdc_on_created_at(self):
        """``/v2/assets/status-signals`` lists every signal across assets, so
        this table is a plain CDC read with no fan-out. Its range filter is
        ``createdAt*`` — the endpoint offers no updatedAt bound — and the
        init-time cap must still exclude the seeded future records."""
        records, offset = self.connector.read_table("asset_status_signals", {}, {})
        rows = list(records)
        init_iso = self.connector._init_ts_iso
        assert offset == {"cursor": init_iso}
        assert rows, "expected the seeded status-signal corpus"
        for row in rows:
            assert row["createdAt"] <= init_iso, (
                f"record with createdAt={row['createdAt']} leaked past the cap"
            )
            # Asset info is embedded, which is what removes the need to fan
            # out over /v2/assets/{id}/status-signals.
            assert row["asset"]["id"]

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
