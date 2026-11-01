"""Live bulk-export tests against a real FHIR server (no mocks).

The mocked suites (test_fhir_bulk.py, test_fhir_lakeflow_connect_bulk.py) assert
what we *believe* a server does. These assert what one actually does, which is
the only way to catch a server disagreeing with us: a `_typeFilter` silently
dropped, a `transactionTime` in a non-UTC offset, `requiresAccessToken: true` on
a server that needs no token, or a second `$export` job per trigger.

Runtime: a HAPI `$export` answers its status poll with `Retry-After: 120`, which
the client honours per spec, so **every export costs ~2 minutes of wall clock**
regardless of `_count` or how little data comes back. The export-driving tests
are therefore opt-in:

    FHIR_LIVE_BULK=1 .venv/bin/python -m pytest tests/unit/sources/fhir/test_fhir_bulk_live.py -v

Without that variable they skip, so CI cannot hang on a flaky public endpoint.
The cheap live checks (REST ground truth, cursor canonicalisation) always run
when the server is reachable.
"""

import json
import os
from pathlib import Path

import pytest
import requests

from databricks.labs.community_connector.sources.fhir.fhir import (
    FhirLakeflowConnect,
    _canonical_ts,
    _parse_ts,
)
from databricks.labs.community_connector.sources.fhir.fhir_bulk import BulkExportClient
from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    SmartAuthClient,
    auth_headers,
)

CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"

# A resource type with no instances on the public server, used to drive the
# empty-export path. If it ever gains data the test still holds -- it asserts the
# job count, not the row count.
EMPTY_RESOURCE_TYPE = "VisionPrescription"

# Poll/timeout kept short so a stuck job fails fast instead of blocking a run.
POLL_INTERVAL = 5
EXPORT_TIMEOUT = 300

_SKIP_BULK = pytest.mark.skipif(
    os.environ.get("FHIR_LIVE_BULK") != "1",
    reason="Set FHIR_LIVE_BULK=1 to run live $export tests (~2 min each).",
)


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def base_url():
    """Base URL of a reachable FHIR server, else skip the module."""
    url = load_config()["base_url"]
    try:
        resp = requests.get(
            f"{url}/metadata",
            headers={"Accept": "application/fhir+json"},
            timeout=30,
        )
    except requests.RequestException as exc:
        pytest.skip(f"FHIR server {url} unreachable: {exc}")
    if resp.status_code != 200:
        pytest.skip(f"FHIR server {url} returned HTTP {resp.status_code} for /metadata")
    return url


def _auth_client():
    config = load_config()
    return SmartAuthClient(
        token_url=config.get("token_url", ""),
        client_id=config.get("client_id", ""),
        auth_type=config.get("auth_type", "none"),
        private_key_pem=config.get("private_key_pem", ""),
        client_secret=config.get("client_secret", ""),
        scope=config.get("scope", ""),
        kid=config.get("kid", ""),
        private_key_algorithm=config.get("private_key_algorithm", "RS384"),
    )


def _bulk_client(base):
    return BulkExportClient(
        base_url=base,
        auth_client=_auth_client(),
        scope="system",
        poll_interval=POLL_INTERVAL,
        timeout=EXPORT_TIMEOUT,
    )


def _connector(base, **overrides):
    options = {
        "base_url": base,
        "auth_type": load_config().get("auth_type", "none"),
        "bulk_export_enabled": "true",
        "resource_types": "Patient",
        "bulk_export_poll_interval": str(POLL_INTERVAL),
        "bulk_export_timeout": str(EXPORT_TIMEOUT),
    }
    options.update(overrides)
    return FhirLakeflowConnect(options)


def _record_kickoffs(connector):
    """Capture every $export kick-off URL the connector issues.

    A response hook on the client's own session, so it observes exactly what went
    on the wire -- the point of these tests is that nothing is stubbed.
    """
    kickoffs = []

    def hook(resp, *_args, **_kwargs):
        url = resp.request.url
        if "$export" in url and "poll-status" not in url:
            kickoffs.append(url)
        return resp

    connector._bulk_client._session.hooks["response"] = [hook]
    return kickoffs


def _rest_count(base, resource_type, since=None):
    """Ground-truth count via REST `_summary=count`, independent of $export."""
    params = {"_summary": "count"}
    if since:
        params["_lastUpdated"] = f"gt{since}"
    resp = requests.get(
        f"{base}/{resource_type}",
        params=params,
        headers={"Accept": "application/fhir+json"},
        timeout=60,
    )
    assert resp.status_code == 200, f"count query failed: HTTP {resp.status_code}"
    return resp.json()["total"]


# --- Manifest and download shape (one shared export) -------------------------


@pytest.fixture(scope="module")
def live_export(base_url):
    """One real kick-off -> poll -> download cycle, shared by several assertions.

    Exposes the manifest as well as the parsed rows: the manifest fields
    (`transactionTime` spelling, `requiresAccessToken`) are what the mocked tests
    can only guess at.
    """
    if os.environ.get("FHIR_LIVE_BULK") != "1":
        pytest.skip("Set FHIR_LIVE_BULK=1 to run live $export tests (~2 min each).")
    client = _bulk_client(base_url)
    status_url = client._kick_off(["Patient"], None, "since")
    try:
        manifest = client._poll_until_complete(status_url)
        resources = client._download_and_parse(manifest)
    finally:
        client._cleanup(status_url)
    return manifest, resources


@_SKIP_BULK
def test_export_returns_transaction_time_and_real_patients(base_url):
    """The documented full cycle works end to end through the public API."""
    client = _bulk_client(base_url)
    transaction_time, resources = client.export(
        resource_types=["Patient"], since=None, incremental_mode="since"
    )

    assert transaction_time, "manifest carried no transactionTime"
    # Parseable by the cursor helpers -- a transactionTime the connector cannot
    # parse would crash mid-microbatch on the next trigger.
    assert _parse_ts(transaction_time).tzinfo is not None

    patients = resources.get("Patient") or []
    assert patients, "export produced no Patient resources"
    assert all(p.get("resourceType") == "Patient" for p in patients)
    assert all(p.get("id") for p in patients)


@_SKIP_BULK
def test_manifest_transaction_time_survives_cursor_round_trip(live_export):
    """A real transactionTime must canonicalise to UTC "Z" and stay stable.

    HAPI reports a non-UTC offset with a 3-digit fraction
    (e.g. "2026-08-04T19:06:34.241-04:00"). Neither spelling appears in the
    mocked suites, and the cursor helpers were rewritten three times over
    exactly this class of input.
    """
    manifest, _ = live_export
    raw = manifest["transactionTime"]

    canonical = _canonical_ts(raw)
    assert canonical.endswith("Z"), f"cursor not canonical UTC: {canonical}"
    assert "+" not in canonical, "a '+' offset survives into _typeFilter as a space"
    # Same instant, just re-spelled.
    assert _parse_ts(canonical) == _parse_ts(raw)
    # Idempotent: the stored cursor is re-canonicalised on the next trigger.
    assert _canonical_ts(canonical) == canonical


@_SKIP_BULK
def test_download_succeeds_without_token_despite_requires_access_token(live_export):
    """`requiresAccessToken: true` on a server that needs no token.

    The public server advertises it but serves files unauthenticated. With
    auth_type=none the client must still download successfully -- and must not
    fabricate an Authorization header it has no token for.
    """
    manifest, resources = live_export
    assert manifest.get("requiresAccessToken") is True, (
        "server no longer advertises requiresAccessToken: true; "
        "this test's premise needs rechecking"
    )
    assert resources.get("Patient"), "download returned nothing despite HTTP 200"

    client = _bulk_client(load_config()["base_url"])
    headers = auth_headers(client._auth_client, "application/fhir+ndjson")
    assert headers["Accept"] == "application/fhir+ndjson"
    assert "Authorization" not in headers, "sent a bearer header with no token"


@_SKIP_BULK
def test_downloaded_ndjson_preserves_non_ascii(base_url, live_export):
    """NDJSON decoding must reproduce non-ASCII names exactly.

    The server sends `application/fhir+ndjson` with **no charset**, so
    `iter_lines(decode_unicode=True)` yields bytes and `json.loads` performs the
    UTF-8 decode. A regression to text-mode decoding would mangle multi-byte
    characters, and roughly a tenth of the Patients here have CJK or accented
    names.

    Correctness is checked against the REST representation of the same records
    rather than by scanning for U+FFFD. The public data set genuinely contains a
    record whose stored name is already mojibake (U+FFFD is in the bytes the
    server sends, and in its REST JSON too), so "no U+FFFD anywhere" is a
    property of the data, not of the decoder.
    """
    _, resources = live_export
    patients = resources.get("Patient") or []
    non_ascii = [
        p
        for p in patients
        if any(ord(ch) > 127 for ch in json.dumps(p.get("name"), ensure_ascii=False))
    ]
    if not non_ascii:
        pytest.skip("no non-ASCII names in this export")

    for patient in non_ascii[:3]:
        resp = requests.get(
            f"{base_url}/Patient/{patient['id']}",
            headers={"Accept": "application/fhir+json"},
            timeout=60,
        )
        assert resp.status_code == 200
        assert resp.json().get("name") == patient.get("name"), (
            f"NDJSON name for Patient/{patient['id']} differs from REST: {patient.get('name')!r}"
        )


# --- Incremental modes against REST ground truth -----------------------------


@_SKIP_BULK
@pytest.mark.parametrize("mode", ["since", "typefilter"])
def test_incremental_mode_filter_reaches_the_server(base_url, mode):
    """`since` and `typefilter` must both narrow the export, not silently widen it.

    Counts are compared against a REST `_summary=count` for the same bound taken
    in the same run, so the assertion holds as the public data set drifts. A
    server that ignored the bound would return the unfiltered total -- which is
    what a mocked test can never detect.
    """
    cursor = _canonical_ts("2026-08-01T00:00:00Z")
    expected = _rest_count(base_url, "Patient", since=cursor)
    unfiltered = _rest_count(base_url, "Patient")
    assert expected < unfiltered, "bound is not selective; pick an older cursor"

    connector = _connector(base_url, bulk_incremental_mode=mode)
    kickoffs = _record_kickoffs(connector)
    records, offset = connector.read_table("Patient", {"cursor": cursor}, {})
    rows = list(records)

    assert len(kickoffs) == 1, f"expected one $export, got {len(kickoffs)}"
    assert len(rows) != unfiltered, (
        f"{mode} returned the unfiltered total ({unfiltered}); the date bound was dropped"
    )
    # Compared with a tolerance, not for exact equality: this is a shared public
    # server and an export takes ~2 minutes, so rows written in between shift the
    # count. The assertion that matters is "close to the filtered count and
    # nowhere near the unfiltered one" -- a dropped filter misses by thousands.
    drift = abs(len(rows) - expected)
    assert drift <= max(25, expected // 10), (
        f"{mode} returned {len(rows)} rows, REST reports {expected} for the same "
        f"bound (unfiltered total is {unfiltered})"
    )
    assert offset["cursor"].endswith("Z")


@_SKIP_BULK
def test_typefilter_bound_is_not_corrupted_in_the_url(base_url):
    """The `_lastUpdated` bound must survive nesting inside `_typeFilter`.

    `_typeFilter` carries a query string inside a query parameter. A "+00:00"
    offset percent-encodes to "%2B" and decodes back to a space, corrupting the
    bound; the cursor is canonicalised to "Z" to avoid that.
    """
    cursor = "2026-08-01T00:00:00Z"
    connector = _connector(base_url, bulk_incremental_mode="typefilter")
    kickoffs = _record_kickoffs(connector)
    list(connector.read_table("Patient", {"cursor": cursor}, {})[0])

    url = kickoffs[0]
    assert "_typeFilter=" in url
    assert "_type=Patient" in url, "_type must accompany _typeFilter per the IG"
    assert "%2B" not in url, "a '+' reached the URL and will decode to a space"
    assert "gt2026-08-01T00" in url.replace("%3A", ":"), f"bound missing from {url}"


@_SKIP_BULK
def test_full_mode_exports_everything_and_stores_no_cursor(base_url):
    """`full` mode ignores the cursor and stores only `run_ts`."""
    unfiltered = _rest_count(base_url, "Patient")
    connector = _connector(base_url, bulk_incremental_mode="full")
    kickoffs = _record_kickoffs(connector)

    records, offset = connector.read_table("Patient", {"cursor": "2026-08-01T00:00:00Z"}, {})
    rows = list(records)

    assert len(kickoffs) == 1
    assert "_since" not in kickoffs[0] and "_typeFilter" not in kickoffs[0]
    # Tolerance rather than equality: writes land on this shared server while the
    # ~2-minute export runs. What is being asserted is that `full` ignored the
    # cursor and pulled the whole set, not a filtered slice of it.
    assert abs(len(rows) - unfiltered) <= max(25, unfiltered // 10), (
        f"full mode returned {len(rows)}, server holds {unfiltered}"
    )
    assert "cursor" not in offset, "full mode must not store a server cursor"
    assert offset["run_ts"] == connector._init_ts


# --- One export per trigger --------------------------------------------------


@_SKIP_BULK
def test_second_read_in_same_trigger_runs_no_second_export(base_url):
    """A hard cost invariant: one $export per resource type per trigger.

    On a real EHR a second job is hours of server work (and Epic enforces a
    24-hour cooldown per group), so this is measured on the wire rather than
    inferred from the offset.
    """
    connector = _connector(base_url)
    kickoffs = _record_kickoffs(connector)

    records, first_offset = connector.read_table("Patient", None, {})
    assert list(records), "first read produced no records"
    assert "run_ts" in first_offset and "cursor" in first_offset
    assert len(kickoffs) == 1

    records2, second_offset = connector.read_table("Patient", first_offset, {})
    assert list(records2) == [], "second call re-emitted rows"
    assert second_offset == first_offset
    assert len(kickoffs) == 1, "a second $export ran within one trigger"


@_SKIP_BULK
def test_next_trigger_re_exports_and_moves_cursor_forward(base_url):
    """A fresh process (new `_init_ts`) re-exports and advances the cursor.

    Guards the non-UTC `transactionTime` path specifically: comparing the new
    cursor against the stored one must neither crash nor regress.
    """
    first = _connector(base_url)
    first_records, first_offset = first.read_table("Patient", None, {})
    assert list(first_records), "first trigger produced no records"

    second = _connector(base_url)
    kickoffs = _record_kickoffs(second)
    records, second_offset = second.read_table("Patient", first_offset, {})
    list(records)

    assert len(kickoffs) == 1, "next trigger did not re-export"
    assert second_offset["run_ts"] != first_offset["run_ts"]
    assert second_offset["cursor"].endswith("Z")
    assert _parse_ts(second_offset["cursor"]) >= _parse_ts(first_offset["cursor"]), (
        "cursor regressed across triggers"
    )


@_SKIP_BULK
def test_empty_full_export_still_stamps_run_ts_so_no_second_job_runs(base_url):
    """Regression: an empty `full` export must not cost two $export jobs.

    `full` mode returned `start_offset or {}` when the export yielded no rows.
    With `start_offset=None` -- per the interface contract, the first call of the
    first run -- the returned `{}` differs from `None`, so the framework calls
    again; `{}` is falsy, so the `run_ts` guard cannot match and a second
    server-side job is kicked off for the same trigger.

    Measured against the public server before the fix: 2 jobs, one trigger.
    """
    connector = _connector(
        base_url,
        resource_types=EMPTY_RESOURCE_TYPE,
        bulk_incremental_mode="full",
    )
    kickoffs = _record_kickoffs(connector)

    records, offset = connector.read_table(EMPTY_RESOURCE_TYPE, None, {})
    assert list(records) == [], f"{EMPTY_RESOURCE_TYPE} unexpectedly has data"
    assert len(kickoffs) == 1
    # The offset must advance even with nothing to write, or the guard below has
    # nothing to match on.
    assert offset == {"run_ts": connector._init_ts}, (
        f"empty export left the offset unadvanced: {offset}"
    )

    records2, offset2 = connector.read_table(EMPTY_RESOURCE_TYPE, offset, {})
    assert list(records2) == []
    assert offset2 == offset, "offset changed on the short-circuit call"
    assert len(kickoffs) == 1, (
        f"empty full-mode export ran {len(kickoffs)} $export jobs in one trigger"
    )


@_SKIP_BULK
def test_cleanup_delete_does_not_raise_on_a_404(base_url):
    """Cleanup is best-effort. The public server answers DELETE with 404."""
    client = _bulk_client(base_url)
    status_url = client._kick_off(["Patient"], None, "since")
    client._poll_until_complete(status_url)

    client._cleanup(status_url)  # must not raise
    client._cleanup(status_url)  # already gone -- still must not raise


# --- Cheap live checks (no $export) ------------------------------------------


def test_rest_last_updated_filter_is_honoured(base_url):
    """The REST path's `_lastUpdated` bound is selective on this server.

    Cheap counterpart to the bulk filter tests, and the premise of the
    "server ignored the filter" guard in the REST read path.
    """
    unfiltered = _rest_count(base_url, "Patient")
    filtered = _rest_count(base_url, "Patient", since="2026-08-01T00:00:00Z")
    assert 0 < filtered < unfiltered, f"filter not selective: {filtered} of {unfiltered}"


def test_rest_cursor_round_trip_returns_only_newer_records(base_url):
    """Feeding a cursor back must not return records at or before it.

    If the server returned records equal to the cursor, the REST path's
    filter-ignored guard would raise on a server that is behaving correctly.
    """
    resp = requests.get(
        f"{base_url}/Patient",
        params={"_count": "100", "_sort": "_lastUpdated", "_lastUpdated": "gt2026-08-01T00:00:00Z"},
        headers={"Accept": "application/fhir+json"},
        timeout=60,
    )
    assert resp.status_code == 200
    stamps = [
        e["resource"].get("meta", {}).get("lastUpdated") for e in resp.json().get("entry", [])
    ]
    stamps = [s for s in stamps if s]
    if not stamps:
        pytest.skip("no records in the sampled window")

    cursor = max(stamps, key=_parse_ts)
    resp2 = requests.get(
        f"{base_url}/Patient",
        params={"_count": "100", "_sort": "_lastUpdated", "_lastUpdated": f"gt{cursor}"},
        headers={"Accept": "application/fhir+json"},
        timeout=60,
    )
    assert resp2.status_code == 200
    stamps2 = [
        e["resource"].get("meta", {}).get("lastUpdated") for e in resp2.json().get("entry", [])
    ]
    stamps2 = [s for s in stamps2 if s]
    cursor_dt = _parse_ts(cursor)
    assert all(_parse_ts(s) > cursor_dt for s in stamps2), (
        "server returned records at or before the cursor; the REST filter-ignored "
        "guard would raise a false positive"
    )
