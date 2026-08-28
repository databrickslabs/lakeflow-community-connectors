from datetime import timedelta
from unittest.mock import patch

from databricks.labs.community_connector.sources.actitime.actitime import (
    ActitimeLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestActitimeConnector(LakeflowConnectTests):
    connector_class = ActitimeLakeflowConnect
    simulator_source = "actitime"
    replay_config = {
        # A real actiTIME Online deployment uses
        # ``https://online.actitime.com/<tenant>`` — the tenant slug is part
        # of the URL PATH, not the subdomain. We mirror that here with a
        # ``/sim`` segment so the simulator's path patterns
        # (``/{tenant}/api/v1/<table>``) match in both simulate and record
        # mode without needing a separate spec per environment.
        "base_url": "https://simulator.actitime.example/sim",
        "username": "simulator",
        "password": "simulator-fake-password",
    }
    # Empty-first-read allowances:
    #
    # * ``settings``, ``userGroups``, ``holidays``, ``approvalStatus`` —
    #   these are feature-gated on the actiTIME tenant. On the test
    #   tenant they return HTTP 404 ("api.error.unknown_error"). The
    #   connector swallows 404 on these endpoints (treat as absent
    #   table) and emits zero records; we allow the empty first read so
    #   that fact alone doesn't fail the suite. Note: ``settings`` is a
    #   ``snapshot`` and is therefore already exempt from the empty-read
    #   check, but listing it keeps the intent obvious.
    allow_empty_first_read = frozenset({
        "settings",
        "userGroups",
        "holidays",
        "approvalStatus",
    })
    # Default ``forward_days=365`` on leavetime/approvalStatus combined
    # with ``window_days=7`` walks ~57 microbatches before convergence
    # (395 days of range / 7-day window). Bump the cap so the suite's
    # convergence assertion still has headroom for a few outliers.
    read_termination_max_iterations = 100

    # ------------------------------------------------------------------
    # Issue #178 — per-user fan-out on /timetrack and /leavetime
    # ------------------------------------------------------------------

    def test_time_window_fans_out_by_user_batch(self):
        """``user_batch_size`` splits the request across multiple ``userIds=``
        GETs and the connector unions the results without duplicates.

        With ``user_batch_size=2`` the connector issues one GET per pair of
        users (each filtered by the ``userIds=`` query param the
        endpoint's spec declares as an ``op: in`` filter — see
        ``source_simulator/specs/actitime/endpoints.yaml``). The union of
        responses should:

        - cover at least two distinct ``user_id`` values (proving fan-out
          actually happened and didn't collapse to a single call), and
        - contain no duplicate rows on the composite primary key
          ``(user_id, date, task_id)`` (proving the simulator/live API
          honoured the per-batch filter and the connector didn't
          double-emit).

        Tenant-agnostic: works against the 5-user simulator corpus and
        against a live tenant of arbitrary size.
        """
        records, _ = self.connector.read_table(
            "timetrack",
            {},
            {"user_batch_size": "2", "window_days": "60", "lookback_days": "60"},
        )
        rows = list(records)
        user_ids_seen = {r["user_id"] for r in rows}
        if len(rows) == 0:
            # Live tenant might have zero timetrack entries in the
            # microbatch window; the fan-out path still ran but produced
            # no records to assert on. Don't fail in that case.
            return
        assert len(user_ids_seen) >= 2, (
            f"expected fan-out across ≥ 2 users; saw only {user_ids_seen} "
            f"(probable single-call regression)"
        )
        composite_keys = [(r["user_id"], r["date"], r["task_id"]) for r in rows]
        assert len(composite_keys) == len(set(composite_keys)), (
            "duplicate (user_id, date, task_id) rows — per-batch filter "
            "did not partition cleanly"
        )

    def test_time_window_empty_user_list_advances_cursor(self):
        """Tenant with zero users short-circuits but still advances the
        cursor so the framework makes progress instead of looping."""
        with patch.object(
            self.connector, "_read_offset_limit", return_value=iter([])
        ):
            records, next_offset = self.connector.read_table("timetrack", {}, {})
        assert list(records) == []
        assert "cursor" in next_offset, (
            "expected cursor to advance even when there are no users"
        )

    # ------------------------------------------------------------------
    # PR #176 review (Young, comment 3228778226) — max_records_per_batch
    # must not leak into internal /users discovery walks.
    # ------------------------------------------------------------------

    def test_user_rates_internal_walk_not_capped_by_max_records(self):
        """Setting a tiny ``max_records_per_batch`` on userRates must not
        truncate the internal ``/users`` discovery walk.

        The simulator corpus has 5 users with 3 rate rows each (15 total).
        Pre-fix: ``max_records_per_batch=2`` would cap the inner
        ``_read_offset_limit("users", ...)`` walk at 2 users, then fan out
        to only 2 ``/userRates/{id}`` calls — emitting 6 rate rows.
        Post-fix: the ``_raw=True`` flag gates the cap so internal lookups
        drain regardless, and the call site passes ``{}`` so userRates
        options never reach the users walk in the first place.
        """
        records, _ = self.connector.read_table(
            "userRates", {}, {"max_records_per_batch": "2"}
        )
        rows = list(records)
        if not rows:
            # Live tenant may have no userRates rows configured for the
            # service account; skip rather than flake.
            return
        distinct_users = {r["user_id"] for r in rows}
        assert len(distinct_users) >= 3, (
            f"max_records_per_batch=2 truncated the internal /users walk: "
            f"only {len(distinct_users)} distinct users observed in rate "
            f"output (expected at least 3). User ids seen: {distinct_users}"
        )

    # ------------------------------------------------------------------
    # PR #176 review (Young, comment 3228778237) — final-window boundary
    # includes _init_date itself, and forward_days extends past today for
    # leavetime/approvalStatus.
    # ------------------------------------------------------------------

    def _capture_time_window_request(self, table_name: str, table_options: dict):
        """Drive _read_time_window with a cursor at upper_bound - 1 and
        capture the dateFrom/dateTo of the next API request.

        Patches _get_json_or_none to record the params and return a body
        that emits zero rows, so we can assert on the request shape
        regardless of corpus contents.
        """
        captured: list[dict] = []

        def fake_get(path, params=None):
            captured.append({"path": path, "params": params or {}})
            if path == "users":
                # Need at least one user so _read_time_window doesn't
                # short-circuit on an empty user list before issuing the
                # time-window request we want to capture.
                return {"items": [{"id": 1}]}
            return {"data": [], "dateFrom": (params or {}).get("dateFrom"),
                    "dateTo": (params or {}).get("dateTo")}

        # Use cursor = upper_bound - 1 day so the final-window branch fires.
        if table_name == "leavetime":
            forward_days = int(table_options.get("forward_days", "365"))
            upper_bound = self.connector._init_date + timedelta(days=forward_days)
        else:
            upper_bound = self.connector._init_date
        cursor = (upper_bound - timedelta(days=1)).isoformat()

        with patch.object(self.connector, "_get_json_or_none", side_effect=fake_get):
            self.connector.read_table(
                table_name, {"cursor": cursor}, table_options
            )

        # First captured call is the /users discovery (no dateFrom), then
        # the time-window request.
        time_calls = [c for c in captured if "dateFrom" in c["params"]]
        assert time_calls, f"no time-window API call observed for {table_name}"
        return time_calls[0]["params"]

    def test_timetrack_final_window_includes_init_date(self):
        """``dateTo`` on the final window must equal ``_init_date``.

        Pre-fix: ``window_end_inclusive = window_end_date - 1`` excluded
        ``_init_date`` itself, lagging today's rows by one trigger.
        Post-fix: when ``window_end_date == upper_bound`` we include the
        boundary day.
        """
        params = self._capture_time_window_request("timetrack", {})
        expected = self.connector._init_date.isoformat()
        assert params["dateTo"] == expected, (
            f"timetrack final-window dateTo should be {expected} (today) "
            f"but got {params['dateTo']} — boundary regression"
        )

    def test_leavetime_forward_days_extends_upper_bound(self):
        """``forward_days`` on ``leavetime`` lets the trigger fetch dates
        past today (planned leave). Pre-fix the upper bound was clamped at
        ``_init_date`` and future-dated leave never surfaced."""
        params = self._capture_time_window_request(
            "leavetime", {"forward_days": "30", "window_days": "60"}
        )
        expected = (self.connector._init_date + timedelta(days=30)).isoformat()
        assert params["dateTo"] == expected, (
            f"leavetime with forward_days=30 should reach {expected} but "
            f"got {params['dateTo']} — forward_days not honored"
        )

    def test_approvalstatus_forward_days_extends_upper_bound(self):
        """Same future-fetch contract for ``approvalStatus``."""
        captured: list[dict] = []

        def fake_get(path, params=None):
            captured.append({"path": path, "params": params or {}})
            return {"items": []}

        forward = 14
        upper_bound = self.connector._init_date + timedelta(days=forward)
        cursor = (upper_bound - timedelta(days=1)).isoformat()
        with patch.object(self.connector, "_get_json_or_none", side_effect=fake_get):
            self.connector.read_table(
                "approvalStatus",
                {"cursor": cursor},
                {"forward_days": str(forward), "window_days": "60"},
            )
        calls = [c for c in captured if "dateFrom" in c["params"]]
        assert calls, "no approvalStatus API call observed"
        assert calls[0]["params"]["dateTo"] == upper_bound.isoformat(), (
            f"approvalStatus with forward_days={forward} should reach "
            f"{upper_bound.isoformat()} but got {calls[0]['params']['dateTo']}"
        )

    # ------------------------------------------------------------------
    # PR #176 review (Young, comment 3228778244) — day_offset is
    # transport state; emitted rows for the same physical record must be
    # byte-identical across overlapping windows.
    # ------------------------------------------------------------------

    def test_timetrack_schema_excludes_day_offset(self):
        """``day_offset`` must not appear in the timetrack or leavetime
        schemas. The wire field is computed per-request as ``(date -
        dateFrom).days`` and would flicker across overlapping fetches."""
        for table_name in ("timetrack", "leavetime"):
            schema = self.connector.get_table_schema(table_name, {})
            field_names = [f.name for f in schema.fields]
            assert "day_offset" not in field_names, (
                f"{table_name} schema must not expose day_offset "
                f"(transport state, not a record property). "
                f"Saw fields: {field_names}"
            )

    def test_timetrack_row_byte_identical_across_overlapping_windows(self):
        """The same ``(user_id, date, task_id)`` fetched in two windows
        with different ``dateFrom`` must emit byte-identical rows.

        Pre-fix, ``day_offset`` shifted with each window's ``dateFrom``
        (empirically observed in our own live cassette — 19 dates
        flickered across requests). Post-fix the column is gone, so
        rows must dedupe perfectly under composite-PK upsert.
        """
        # Drive the connector twice with different lookback_days to
        # force overlapping fetches with different dateFrom values.
        # Both runs target the same window_end, so the simulator returns
        # the same physical entries under different request shapes.
        records_run_a, _ = self.connector.read_table(
            "timetrack",
            {},
            {"window_days": "60", "lookback_days": "30"},
        )
        rows_a = list(records_run_a)
        records_run_b, _ = self.connector.read_table(
            "timetrack",
            {},
            {"window_days": "60", "lookback_days": "60"},
        )
        rows_b = list(records_run_b)
        if not rows_a or not rows_b:
            return  # empty tenant — fan-out path ran but no data
        # Build composite-PK -> row index so we can pair the same fact
        # across the two runs.
        index_a = {(r["user_id"], r["date"], r["task_id"]): r for r in rows_a}
        index_b = {(r["user_id"], r["date"], r["task_id"]): r for r in rows_b}
        common = set(index_a).intersection(index_b)
        assert common, (
            "no overlapping (user_id, date, task_id) records across the "
            "two runs — test cannot prove determinism"
        )
        for key in common:
            assert index_a[key] == index_b[key], (
                f"timetrack row for {key} differs between overlapping "
                f"fetches: run_a={index_a[key]} run_b={index_b[key]} — "
                f"non-determinism regression"
            )

    def test_timetrack_internal_walk_not_capped_by_max_records(self):
        """Same defence for the /timetrack fan-out path.

        ``max_records_per_batch=2`` on timetrack must not cap the internal
        ``/users`` enumeration that feeds the per-batch ``userIds=`` filter
        — otherwise users beyond index 1 would never be fanned out.
        """
        records, _ = self.connector.read_table(
            "timetrack",
            {},
            {
                "max_records_per_batch": "2",
                "user_batch_size": "10",  # force one fan-out batch
                "window_days": "60",
                "lookback_days": "60",
            },
        )
        rows = list(records)
        if not rows:
            return
        distinct_users = {r["user_id"] for r in rows}
        assert len(distinct_users) >= 3, (
            f"max_records_per_batch=2 leaked into the inner /users walk: "
            f"only {len(distinct_users)} distinct users observed in "
            f"timetrack fan-out (expected at least 3). User ids seen: "
            f"{distinct_users}"
        )
