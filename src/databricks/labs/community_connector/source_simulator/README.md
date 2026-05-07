# HTTP record/replay for connector tests

A pytest-integrated layer that records real HTTP traffic to a cassette on the
first run, then replays it offline on subsequent runs. Connector code is **not**
modified — only the `CONNECTOR_TEST_MODE` environment variable changes behavior.

## Why

Running the connector test suite against a real source system validates the
connector end-to-end, but it requires live credentials and isn't CI-friendly.
Once a connector has been validated against a live instance, its HTTP
interactions can be captured and replayed, so:

- CI runs without credentials
- Iterating locally doesn't burn real-API quotas
- Regressions in connector code (not the source API) surface immediately

The tradeoff: cassettes drift from the live API over time. Periodic
live re-validation is still needed — but it happens on a schedule, not on
every PR.

## Modes

Set `CONNECTOR_TEST_MODE` to one of:

| Mode     | Behavior                                                        |
| -------- | --------------------------------------------------------------- |
| `live`   | (default) tests hit the real source; no cassettes involved      |
| `record` | tests hit the real source **and** write a cassette              |
| `replay` | tests serve responses from the cassette; no network traffic     |

```bash
# record a cassette once against a live instance
CONNECTOR_TEST_MODE=record pytest tests/unit/sources/qualtrics/

# run offline from cassettes — no credentials needed
CONNECTOR_TEST_MODE=replay pytest tests/unit/sources/qualtrics/

# today's behavior (unchanged)
pytest tests/unit/sources/qualtrics/
```

## Cassette location

One cassette per test class, next to the test file:

```
tests/unit/sources/qualtrics/
└── cassettes/
    └── TestQualtricsConnector.json
```

Credentials are not stored in the repo — see the next section.

## Credentials

`LakeflowConnectTests` resolves credentials differently per mode.

### Stand-in modes (simulate / replay)

The simulator never validates credentials, so any string of the right
shape works. Two ways to supply them:

- **In-class default (preferred).** Set ``replay_config = {...}`` on
  the test class. Used uniformly across all simulate/replay runs;
  no on-disk file required.
- **Per-run override.** Drop a ``configs/replay_config.json`` next
  to the test file (gitignored). Takes precedence only when
  ``replay_config`` is unset; useful for one-off local experimentation.

A connector whose ``__init__`` parses a credential field at runtime
(e.g. Google Analytics' ``private_key`` field, parsed as PEM RSA)
should override ``_replay_config()`` and synthesize the value
in-process — see ``test_google_analytics_aggregated_lakeflow_connect.py``
for the canonical pattern.

### Live / record modes

Credentials are not stored in the repo. Supply them per run via:

1. **`CONNECTOR_TEST_CONFIG_JSON` env var** — inline JSON. Best for
   CI runners that pull from a secret store and inject the value
   into the environment without writing a file.

   ```bash
   CONNECTOR_TEST_MODE=live \
     CONNECTOR_TEST_CONFIG_JSON='{"api_token":"...","datacenter_id":"iad1"}' \
     pytest tests/unit/sources/qualtrics/
   ```

2. **`CONNECTOR_TEST_CONFIG_PATH` env var** — path to a JSON file
   at any local path the developer chooses (typically outside the
   repo, e.g. ``~/secrets/qualtrics.json``). Useful when the secret
   store materializes a temp file or for repeated local invocations.

   ```bash
   CONNECTOR_TEST_MODE=live \
     CONNECTOR_TEST_CONFIG_PATH=~/secrets/qualtrics.json \
     pytest tests/unit/sources/qualtrics/
   ```

If neither resolves, the test fails at setup with a message naming
both env vars.

## How it works

The framework patches `requests.sessions.Session.send` — the one method every
`requests.get` / `requests.post` / `Session.*` call funnels through. This
covers every connector in the repo that uses the `requests` library.

Match key per request: `(method, scheme+host+path, sorted_query, body_sha256)`.
Duplicate keys in the cassette replay round-robin so the same endpoint can be
called many times without consuming the recording.

### Record-time: keep cassettes small

Raw recordings of real APIs are huge (700+ KiB for even a small test repo),
full of paginated responses and PII. The framework trims aggressively:

- **Record trimming**: each response's records array is truncated to
  `record_replay_sample_size` (default **5**). The rest is dropped.
- **Pagination strip**: `Link: <...>; rel="next"` is removed from response
  headers; common in-body pagination fields (`next`, `next_page`,
  `@odata.nextLink`, `nextPageToken`, …) are set to `null`. Connectors stop
  paginating on replay.
- **Match-key dedup**: repeated requests with the same match key record only
  once. If you also list pagination params in `record_replay_ignore_query_params`
  (e.g. `page`, `per_page`), multi-page walks collapse to a single entry.

### Replay-time: synthesize variety

Stored samples are small. If your tests need more records, turn on the
synthesizer:

```python
class TestQualtricsConnector(LakeflowConnectTests):
    connector_class = QualtricsLakeflowConnect
    record_replay_synthesize_count = 50  # expand records at replay time
```

The synthesizer detects which fields vary across the stored samples, then
generates extra records by cloning a sample and mutating only the varying
fields: integers shift, ISO timestamps advance, UUIDs/hex IDs get randomized,
other strings get an index suffix. Type-aware, deterministic per request.

## Cassette format

```json
{
  "version": 1,
  "source": "qualtrics",
  "interactions": [
    {
      "request": {
        "method": "GET",
        "url": "https://iad1.qualtrics.com/API/v3/surveys",
        "query": {"offset": "0"},
        "body_sha256": null
      },
      "response": {
        "status_code": 200,
        "headers": {"Content-Type": "application/json"},
        "body_text": "{...}",
        "body_b64": null,
        "encoding": "utf-8",
        "url": "https://iad1.qualtrics.com/API/v3/surveys?offset=0"
      }
    }
  ]
}
```

Plain JSON — diffable in PRs, greppable, hand-editable.

## Non-deterministic query params

Connectors that compute request params from `datetime.now()`, UUIDs, or other
non-deterministic sources will mismatch on replay. Declare those params on
the test class so the matcher ignores them:

```python
class TestGithubConnector(LakeflowConnectTests):
    connector_class = GithubLakeflowConnect
    record_replay_ignore_query_params = frozenset({"since", "until"})
```

Names listed here are stripped from both the recorded interaction key and the
incoming request key before comparison. The cassette still stores the original
values for audit.

## Scrubbing

Request headers with these names are replaced with `***REDACTED***` at record
time:

- `Authorization`
- `Cookie`
- `Proxy-Authorization`
- `X-Api-Token`
- `X-Api-Key`
- `X-Auth-Token`

**OAuth flows are out of scope for this version** — they also need the
refresh-token endpoint body scrubbed. Record OAuth-using connectors in `live`
mode until that lands.

## Re-recording after a source-API change

1. Delete the old cassette: `rm tests/unit/sources/{source}/cassettes/*.json`
2. Re-run in record mode with live creds: `CONNECTOR_TEST_MODE=record pytest tests/unit/sources/{source}/`
3. Inspect the diff in the cassette JSON — it should reflect the API change
4. Commit the new cassette alongside the connector change

## Limitations (v1)

- `stream=True` responses are not supported in replay (no connector uses this today)
- OAuth token refresh is not yet scrubbed — use `live` for OAuth sources
- SDK-based connectors (Google Analytics uses `google-api-python-client`) bypass `requests` and are not intercepted
- URL match is exact on host:port — if the source returns different hostnames per run (rare), cassettes will not match

## Adding the framework to a new test

Nothing to do. `LakeflowConnectTests` inherits the fixture automatically. Any
subclass gets record/replay for free the moment you flip the env var.
