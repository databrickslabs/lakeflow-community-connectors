---
name: validate-connector-auth
description: Generate and run an auth verification test to confirm that collected credentials are valid.
disable-model-invocation: true
---

# Connector Auth Validate

## Goal

Generate and run an authentication verification test for the **{{source_name}}** connector to confirm that the credentials in `dev_config.json` are valid.

## Prerequisites

- API doc must exist at `src/databricks/labs/community_connector/sources/{{source_name}}/{{source_name}}_api_doc.md`
- `dev_config.json` must exist at `tests/unit/sources/{{source_name}}/configs/dev_config.json`. 

If `dev_config.json` does not exist, stop and report that credentials have not been collected yet and ask user to provide.

## Output

- `tests/unit/sources/{{source_name}}/auth_test.py` — a passing auth verification test

## Steps

### Step 0: Check if auth_test.py already exists

Check if `tests/unit/sources/{{source_name}}/auth_test.py` already exists.

- **If it exists:** skip to Step 3 — just run the existing test to validate credentials. Do not regenerate.
- **If it does not exist:** proceed to Step 1.

### Step 1: Read the API Doc for Auth Details

Read the authentication section of `src/databricks/labs/community_connector/sources/{{source_name}}/{{source_name}}_api_doc.md` to determine:
- The auth method (API key, Bearer token, Basic auth, OAuth, etc.)
- How credentials are passed (headers, query params, etc.)
- The base URL
- A simple read-only endpoint suitable for verifying connectivity

### Step 2: Generate the Auth Test

Generate a Python test file at `tests/unit/sources/{{source_name}}/auth_test.py`.

This script must:
1. Use `load_config` from `tests.unit.sources.test_utils` to load credentials
2. Make the **simplest possible API call** using those credentials
3. Assert the response indicates successful authentication (HTTP 200, no auth errors)
4. Print a clear success or failure message

Template:

```python
"""
Auth verification test for {SourceName} connector.
Run this script to verify your credentials are correctly configured.

Usage:
    python tests/unit/sources/{source_name}/auth_test.py
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', '..'))

from tests.unit.sources.test_utils import load_config
import requests


def test_auth():
    """Verify that credentials in dev_config.json are valid by making a simple API call."""
    config = load_config("{source_name}")

    # Build auth headers/params from config — customize based on auth method
    # Example for Bearer token:
    # headers = {"Authorization": f"Bearer {config['access_token']}"}
    # Example for API key:
    # headers = {"Authorization": f"Token token={config['api_key']}"}
    # Example for Basic auth:
    # auth = (config['email'], config['api_token'])

    response = requests.get(
        "{base_url}{verification_endpoint_path}",
        headers=headers,  # or auth=auth, etc.
        timeout=10
    )

    if response.status_code == 200:
        print(f"Authentication successful! Connected to {SourceName}.")
        print(f"   Response: {response.json()}")
        return True
    elif response.status_code == 401:
        print(f"Authentication failed: Invalid credentials (HTTP 401).")
        print(f"   Check your credentials in tests/unit/sources/{source_name}/configs/dev_config.json")
        return False
    elif response.status_code == 403:
        print(f"Authorization failed: Insufficient permissions (HTTP 403).")
        print(f"   Ensure your credentials have the required scopes/permissions.")
        return False
    else:
        print(f"Unexpected response: HTTP {response.status_code}")
        print(f"   Body: {response.text}")
        return False


if __name__ == "__main__":
    success = test_auth()
    sys.exit(0 if success else 1)
```

Customize the template based on:
- The actual auth method from the API doc
- The actual base URL and a simple verification endpoint
- The response structure (extract a useful field like username or account name to print)

### Step 3: Run the Auth Test

Run the test using the project virtual environment (Python 3.10+):

```bash
source .venv/bin/activate
python tests/unit/sources/{source_name}/auth_test.py
```

Debug if authentication fails and report the issue clearly.

---

## Quality Standards

- **Never hardcode credentials** — always load from config files via `load_config`
- **Keep the auth test minimal** — one HTTP request, clear output, no complex logic
- **Be precise about field names** — use the exact field names from the API documentation
- **Provide useful error messages** — distinguish 401 (wrong credentials) from 403 (wrong permissions)
- **Use `requests` library** unless the source has an official Python SDK that simplifies auth significantly

## Edge Cases

- **OAuth 2.0**: The `dev_config.json` may contain `refresh_token`, `client_id`, `client_secret`. The auth test may need to exchange the refresh token for an access token first.
- **Subdomain-based URLs**: Build the base URL from the `subdomain` field in config.
- **Multiple auth methods**: Use whichever method's credentials are present in `dev_config.json`.

## Git Commit on Completion

After creating the auth verification test, commit it to git before returning:

```bash
git add tests/unit/sources/{source_name}/auth_test.py
git commit -m "Add {source_name} auth test"
```

Use the exact source name in the commit message. Do not push — only commit locally.
