"""Checkpoint complete CM360 report groups without shortening native reach periods."""

import hashlib
import json
import logging
import math
import time
from contextlib import contextmanager

from databricks.labs.community_connector.interface import SupportsPartitionedStream


PERIOD_STREAM_TABLE = "reach_report_period_incremental"
_MAX_MANIFEST_BYTES = 1024 * 1024
_QUERY_GROUPS = 10
_QUERY_SECONDS = 1800
_KEYS = ("profile_id", "delivery_country", "advertiser_id", "campaign_id")
logger = logging.getLogger(__name__)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _key(row):
    values = tuple(row[name] for name in _KEYS)
    if not all(isinstance(value, str) and value for value in values):
        raise ValueError("CM360 period metadata has an invalid campaign key")
    return values


def _groups(rows):
    groups = {}
    for index, row in enumerate(rows):
        bounds = ((row["period_start"], row["period_end"])
                  if row["coverage_status"] == "COMPLETE" else ("", ""))
        groups.setdefault(bounds, []).append(index)
    return [groups[bounds] for bounds in sorted(groups)]


class PeriodReachStreamMixin(SupportsPartitionedStream):
    """Source-only mixin; the Spark offset is the sole durable progress store."""

    def is_partitioned(self, table_name):
        return table_name == PERIOD_STREAM_TABLE

    def _period_identity(self, options):
        return {
            "profile_id": self._profile_id,
            "metadata_anchor": options.sync_start_date.isoformat(),
            "reporting_timezone": str(options.reporting_timezone),
            "advertiser_ids": sorted(set(options.advertiser_ids)),
            "delivery_country_codes": list(options.delivery_country_codes),
        }

    def _period_budget(self):
        if not hasattr(self, "_period_query_deadline"):
            self._period_query_deadline = time.time() + _QUERY_SECONDS
        return self._period_query_deadline

    @contextmanager
    def _period_bounded(self, deadline_epoch):
        remaining = deadline_epoch - time.time()
        if remaining <= 0:
            raise TimeoutError("CM360 period query execution deadline reached")
        old_deadline = getattr(self, "_snapshot_deadline", None)
        old_client_deadline = self._client._snapshot_deadline
        self._snapshot_deadline = time.monotonic() + remaining
        self._client._snapshot_deadline = self._snapshot_deadline
        try:
            yield
            if time.monotonic() >= self._snapshot_deadline:
                raise TimeoutError("CM360 period query execution deadline reached")
        finally:
            self._snapshot_deadline = old_deadline
            self._client._snapshot_deadline = old_client_deadline

    @staticmethod
    def _period_offset(manifest, position):
        text = _json(manifest)
        if len(text.encode()) > _MAX_MANIFEST_BYTES:
            raise ValueError("CM360 period checkpoint manifest exceeds 1MiB")
        return {"version": 1, "manifest": text, "position": position,
                "manifest_sha256": hashlib.sha256(text.encode()).hexdigest()}

    def _period_state(self, offset, options):
        if not isinstance(offset, dict) or set(offset) != {
            "version", "manifest", "position", "manifest_sha256",
        }:
            raise ValueError("CM360 period checkpoint shape is invalid")
        text = offset["manifest"]
        if (not isinstance(offset["version"], int) or isinstance(offset["version"], bool)
                or offset["version"] != 1
                or not isinstance(text, str)):
            raise ValueError("CM360 period checkpoint manifest is corrupt or unsupported")
        if (len(text.encode()) > _MAX_MANIFEST_BYTES
                or hashlib.sha256(text.encode()).hexdigest() != offset["manifest_sha256"]):
            raise ValueError("CM360 period checkpoint manifest is corrupt or unsupported")
        try:
            manifest = json.loads(text)
            groups = _groups(manifest["rows"])
            valid = (manifest["identity"] == self._period_identity(options)
                     and isinstance(manifest["cutoff"], str)
                     and bool(manifest["rows"]))
        except (ValueError, TypeError, KeyError) as error:
            raise ValueError("CM360 period checkpoint manifest is invalid") from error
        position = offset["position"]
        if (not valid or not isinstance(position, int) or isinstance(position, bool)
                or not 0 <= position <= len(groups)):
            raise ValueError("CM360 period checkpoint identity or position is invalid")
        return manifest, groups

    def latest_offset(self, table_name, table_options, start_offset=None):
        if not self.is_partitioned(table_name):
            raise NotImplementedError("Only incremental period reach is partitioned")
        options = self._period_stream_options(table_options)
        if start_offset is not None and not isinstance(start_offset, dict):
            raise ValueError("CM360 period checkpoint must be a dictionary")
        start = start_offset if start_offset is not None else {}
        cutoff = options.sync_end_date.isoformat()
        identity = _json({**self._period_identity(options), "cutoff": cutoff})
        if getattr(self, "_period_query_identity", identity) != identity:
            raise ValueError("CM360 period configuration changed within a query")
        self._period_query_identity = identity
        previous = None
        if start:
            manifest, groups = self._period_state(start, options)
            previous = manifest
            if manifest["cutoff"] == cutoff:
                if getattr(self, "_period_planned_start", None) == start:
                    return dict(self._period_planned_end)
                if not hasattr(self, "_period_query_first"):
                    self._period_query_first = start["position"]
                if (start["position"] >= min(len(groups), self._period_query_first + _QUERY_GROUPS)
                        or time.time() >= self._period_budget()):
                    return start
                end = {**start, "position": min(
                    len(groups), self._period_query_first + _QUERY_GROUPS)}
                self._period_planned_start, self._period_planned_end = dict(start), dict(end)
                return end
            if start["position"] != len(groups) or manifest["cutoff"] >= cutoff:
                raise ValueError(
                    "CM360 period cutoff cannot change before checkpoint completion"
                )
        if not hasattr(self, "_period_discovery"):
            with self._period_bounded(self._period_budget()):
                rows, profile, country_id = self._period_stream_discover(options, previous)
            if not rows:
                raise ValueError("CM360 period metadata discovery is empty")
            rows = sorted(rows, key=_key)
            if len({_key(row) for row in rows}) != len(rows):
                raise ValueError("CM360 period metadata returned duplicate campaigns")
            self._period_discovery = self._period_offset({
                "identity": self._period_identity(options), "cutoff": cutoff,
                "rows": rows, "profile": profile, "country_id": country_id,
            }, 0)
            self._period_query_first = 0
        return dict(self._period_discovery)

    def get_partitions(self, table_name, table_options, start_offset=None, end_offset=None):
        if not self.is_partitioned(table_name):
            raise NotImplementedError("Preserve existing batch-reader fallback")
        options = self._period_stream_options(table_options)
        manifest, _ = self._period_state(end_offset, options)
        if manifest["cutoff"] != options.sync_end_date.isoformat():
            raise ValueError("CM360 period partition cutoff differs from configured cutoff")
        first = 0
        if start_offset:
            previous, groups = self._period_state(start_offset, options)
            if start_offset["manifest"] == end_offset["manifest"]:
                first = start_offset["position"]
            elif (start_offset["position"] != len(groups)
                  or previous["cutoff"] >= manifest["cutoff"]):
                raise ValueError("CM360 period interval changed unfinished campaign membership")
        elif start_offset not in (None, {}):
            raise ValueError("CM360 period interval start is invalid")
        if first > end_offset["position"]:
            raise ValueError("CM360 period interval is reversed")
        if first == end_offset["position"]:
            return []
        self._period_query_first = min(getattr(self, "_period_query_first", first), first)
        if end_offset["position"] > self._period_query_first + _QUERY_GROUPS:
            raise ValueError("CM360 period interval exceeds the query group budget")
        return [{"offset": end_offset, "first": first, "deadline_epoch": self._period_budget()}]

    def read_partition(self, table_name, partition, table_options):
        if not self.is_partitioned(table_name):
            raise NotImplementedError("Only incremental period reach is partitioned")
        options = self._period_stream_options(table_options)
        manifest, groups = self._period_state(partition["offset"], options)
        first, last = partition["first"], partition["offset"]["position"]
        if (manifest["cutoff"] != options.sync_end_date.isoformat()
                or not isinstance(first, int) or isinstance(first, bool) or not 0 <= first <= last):
            raise ValueError("CM360 period partition boundaries or deadline are invalid")
        if (not isinstance(partition["deadline_epoch"], (int, float))
                or not math.isfinite(partition["deadline_epoch"])):
            raise ValueError("CM360 period partition boundaries or deadline are invalid")
        rows = []
        with self._period_bounded(partition["deadline_epoch"]):
            for position in range(first, last):
                frozen = [manifest["rows"][index] for index in groups[position]]
                started = time.monotonic()
                try:
                    values = self._period_stream_export(frozen, options, manifest)
                except Exception as error:
                    context = (
                        f"CM360 incremental group failed position={position + 1} total={len(groups)} "
                        f"interval_first={first} interval_last={last} "
                        f"elapsed_seconds={time.monotonic() - started:.3f}"
                    )
                    logger.warning("%s", context)
                    if hasattr(error, "add_note"):
                        error.add_note(context)
                    raise
                if (len(values) != len(frozen)
                        or {_key(value) for value in values} != {_key(row) for row in frozen}
                        or any(value["source_cutoff_date"] != manifest["cutoff"]
                               for value in values)):
                    raise ValueError("CM360 period interval returned unexpected campaign rows")
                rows.extend({**value, "snapshot_campaign_count": len(manifest["rows"])}
                            for value in values)
                logger.info("CM360 incremental group completed position=%d total=%d",
                            position + 1, len(groups))
        # Nothing is yielded until the entire requested interval has succeeded.
        return iter(rows)
