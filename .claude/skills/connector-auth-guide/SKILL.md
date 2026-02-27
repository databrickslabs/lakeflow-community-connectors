---
name: connector-auth-guide
description: "Single step only: set up authentication and credentials for an existing connector. Do NOT use for full connector creation — use the create-connector agent instead."
---

# Connector Auth Guide

## Goal

Set up authentication for the **{{source_name}}** connector by following the 4-step process below. Your output is:
- A `connector_spec.yaml` (connection section) at `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml`
- Credentials saved to `tests/unit/sources/{{source_name}}/configs/dev_config.json`
- A passing auth test at `tests/unit/sources/{{source_name}}/auth_test.py`

---

## Step 1: Read and Analyze Authentication Documentation

Read and parse the authentication section of the source API documentation at `src/databricks/labs/community_connector/sources/{{source_name}}/{{source_name}}_api_doc.md`. Identify:

- Authentication method(s): API key, OAuth 2.0, Basic auth, Bearer token, JWT, etc.
- Required credentials and their names (e.g., `api_key`, `client_id`, `client_secret`, `subdomain`, `access_token`)
- How credentials are passed (headers, query params, request body)
- Any scopes, permissions, or roles required
- Base URL patterns and any subdomain/tenant-specific URL structures

---

## Step 2: Generate the Connector Spec (Connection Section Only)

Using the `/generate-connector-spec` skill, create an intermediate connector spec that focuses **only on the connection and authentication section**. Follow the `templates/connector_spec_template.yaml` format. Only include parameters that are strictly necessary for authentication — the purpose at this stage is solely to verify connectivity, not to enable data extraction.

---

## Step 3: Run the Authentication Script and Direct User to Provide Parameters

Automatically run the authentication script in the background using the Bash tool with `run_in_background: true`. Use the project virtual environment (Python 3.10+ required):

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python tools/scripts/authenticate.py -s {{source_name}} -m browser
```

Monitor the output (using TaskOutput or by reading the background task output) to extract the local server URL the script prints (e.g., `http://localhost:9876`). Display the clickable URL to the user so they can open it in their browser to enter and save their credentials.

Example message to user after running:

> The authentication portal is now running. Please click the link below to enter your credentials:
>
> **http://localhost:9876**
>
> Fill in your credentials and click Save. Please confirm here once you have saved successfully.

**Wait for the user to explicitly confirm** that they have saved the credentials before proceeding to Step 4. Do not skip this gating step. If the user reports an error, help them debug before proceeding.

---

## Step 4: Generate and Run the Auth Test Script

Once the user confirms credentials are saved, generate a Python test file at `tests/unit/sources/{{source_name}}/auth_test.py`.

This script must:
1. Use `load_config` from `tests/unit/sources/test_utils.py` to load credentials
2. Make the **simplest possible API call** using those credentials (use the `verification_endpoint` from the spec)
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

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', '..'))

from tests.unit.sources.test_utils import load_config
import requests  # or httpx, or the source's SDK if applicable


def test_auth():
    """Verify that credentials in dev_config.json are valid by making a simple API call."""
    config = load_config("{source_name}")

    # Build auth headers/params using the config and following the api doc on endpoints
    # --- Customize based on auth_type from the spec ---
    # Example for Bearer token:
    # headers = {"Authorization": f"Bearer {config['access_token']}"}
    # Example for Basic auth:
    # auth = (config['email'], config['api_token'])

    # Make the simplest read-only API call
    response = requests.get(
        "{base_url}{verification_endpoint_path}",
        headers=headers,  # or auth=auth, etc.
        timeout=10
    )

    if response.status_code == 200:
        print(f"✅ Authentication successful! Connected to {SourceName}.")
        print(f"   Response: {response.json()}")
        return True
    elif response.status_code == 401:
        print(f"❌ Authentication failed: Invalid credentials (HTTP 401).")
        print(f"   Check your credentials in tests/unit/sources/{source_name}/configs/dev_config.json")
        return False
    elif response.status_code == 403:
        print(f"❌ Authorization failed: Insufficient permissions (HTTP 403).")
        print(f"   Ensure your credentials have the required scopes/permissions.")
        return False
    else:
        print(f"⚠️  Unexpected response: HTTP {response.status_code}")
        print(f"   Body: {response.text}")
        return False


if __name__ == "__main__":
    success = test_auth()
    sys.exit(0 if success else 1)
```

Customize the template based on:
- The actual `auth_type` (bearer, basic, API key in header/query param, etc.)
- The actual `base_url` and `verification_endpoint` from the spec
- Any source-specific SDK (prefer raw HTTP requests for simplicity)
- The response structure (extract a useful field like username or account name to print)

After writing the file, run it using the project virtual environment (Python 3.10+):

```bash
source .venv/bin/activate  # activate if not already active
python tests/unit/sources/{source_name}/auth_test.py
```

Debug if authentication fails and tell the user if the issue is incorrect parameters.

---

## Quality Standards

- **Never hardcode credentials** — always load from config files via `load_config`
- **Keep the auth test minimal** — one HTTP request, clear output, no complex logic
- **Be precise about field names** — use the exact field names from the API documentation
- **Provide useful error messages** — distinguish 401 (wrong credentials) from 403 (wrong permissions)
- **Use `requests` library** unless the source has an official Python SDK that simplifies auth significantly
- **Wait for user confirmation** between Step 3 and Step 4 — never skip this gating step

## Edge Cases

- **OAuth 2.0 flows**: Include the oauth field with endpoints in the connector spec; `authenticate.py` handles the browser-based flow; the spec should still capture `client_id`, `client_secret`, and the resulting `access_token`/`refresh_token` fields
- **Subdomain-based URLs**: Capture `subdomain` or `instance_url` as a credential field
- **Multiple auth methods**: Document the simplest/recommended one first, mention alternatives
- **API versioning in auth**: Note if auth tokens are version-specific
- **Missing source name**: If you cannot determine the `source_name` from context, ask the user before proceeding

## Git Commit on Completion

After creating the auth verification test, commit it to git before returning:

```bash
git add tests/unit/sources/{source_name}/auth_test.py
git commit -m "Add {source_name} auth test"
```

Use the exact source name in the commit message. Do not push — only commit locally.
