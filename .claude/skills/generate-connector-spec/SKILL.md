---
name: generate-connector-spec
description: Generate the connector spec YAML file defining connection parameters and external options allowlist.
disable-model-invocation: true
---

# Generate Connector Spec YAML

## Goal
Generate a **connector spec YAML file** for the **{{source_name}}** connector that defines the connector specification including connection parameters and external options allowlist.

## Output Contract
Produce a YAML file following the template at `templates/connector_spec_template.yaml` as `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml`.

## Requirements

### Connection Parameters Structure

The template supports two structure options based on the connector's authentication complexity:

#### Option A: Single Authentication Method (Simple Case)
Use a flat `parameters` list directly under `connection` when the connector has only one way to authenticate:

```yaml
connection:
  parameters:
    - name: api_key
      type: string
      required: true
      description: API key for authentication.
```

#### Option B: Multiple Authentication Methods
Use `auth_methods` when the connector supports multiple ways to authenticate (e.g., API key vs. OAuth, or service account vs. API secret). Shared parameters go in `parameters` at the connection level:

```yaml
connection:
  auth_methods:
    - name: api_token
      description: Authenticate using email and API token.
      parameters:
        - name: email
          type: string
          required: true
          description: User email address.
        - name: api_token
          type: string
          required: true
          description: API token for authentication.

    - name: oauth
      description: Authenticate using OAuth 2.0.
      parameters:
        - name: access_token
          type: string
          required: true
          description: OAuth 2.0 access token.

  # Shared parameters (apply to all authentication methods)
  parameters:
    - name: subdomain
      type: string
      required: true
      description: Your account subdomain.
```

### Determining Which Structure to Use
- Analyze the connector's `__init__` method in `src/databricks/labs/community_connector/sources/{{source_name}}/{{source_name}}.py`.
- Look for conditional logic that checks for mutually exclusive credentials (e.g., "if username and secret ... elif api_secret ...").
- If the connector accepts multiple authentication approaches, use Option B with `auth_methods`.
- If there's only one way to authenticate, use Option A with flat `parameters`.

### OAuth 2.0 Connections (the `oauth` block)

If the source authenticates via OAuth 2.0, add a `connection.oauth` block. This makes the connector use a Unity Catalog **COMMUNITY** connection in an OAuth auth mode instead of static credentials. The connection layer (UC, or the labs authenticate tool for local dev) runs the OAuth flow and **injects an access token into the connector's options at query time** — the user only supplies their OAuth app's `client_id` + `client_secret`, never a token.

**This is where the OAuth flow is chosen.** Selecting the right auth mode (`m2m` vs `u2m` vs `u2m_per_user`) is a research/build-time decision you make here from the source's supported grant types and the connector's intended access pattern. Once it is recorded in `oauth.flow`, the rest of the lifecycle (connection creation, deployment, end-user docs) just follows the spec — no one re-picks the mode downstream.

```yaml
connection:
  parameters:
    - name: client_id
      type: string
      required: true
      secret: true
      description: OAuth 2.0 client ID for the provider app.
    - name: client_secret
      type: string
      required: true
      secret: true
      description: OAuth 2.0 client secret issued alongside the client_id.

  oauth:
    flow: u2m                  # m2m | u2m | u2m_per_user — maps to the connection auth mode
    pkce: true                 # set false for a confidential client that doesn't need PKCE
    scopes: "read"             # space-separated scopes requested at consent
    authorization_url: "https://provider/oauth/authorize"   # u2m / u2m_per_user
    token_url: "https://provider/oauth/token"
    extra_auth_params:         # provider knobs folded into the authorize request (optional)
      access_type: offline
      prompt: consent
```

#### Choosing the `flow`

`flow` selects the auth mode and maps to the CLI's `--auth-type` / UC's `community_oauth_flow`. Pick from the grant types the source's API supports:

- **`m2m`** — the standard OAuth 2.0 **client-credentials** grant. No human and no browser: the connection posts `client_id` + `client_secret` (and `scopes`) to the `token_url` and gets back an app-level access token. Choose this when the source authenticates the *application* rather than an end user — service-to-service APIs, admin/tenant-wide tokens, anything with a "client credentials" or "server-to-server" grant. Only `token_url` (and optionally `scopes`) is needed; `authorization_url` / `pkce` / `extra_auth_params` do not apply.
- **`u2m`** — authorization-code flow where **one human authorizes once** at connection creation via a browser. Choose this when the source's data is scoped to a *user account* and a single shared identity (personal or team mailbox, one workspace login) is acceptable. Needs `authorization_url` + `token_url`.
- **`u2m_per_user`** — like `u2m` but **each end user authorizes separately**; UC resolves the right token per query at runtime. Choose this when different Databricks users querying the connection must each see their own data. Needs `authorization_url` + `token_url`.

Rule of thumb: no user identity involved → `m2m`; one user for the whole connection → `u2m`; each querying user is distinct → `u2m_per_user`. Determine the OAuth details (grant types, endpoints, scopes, whether PKCE is required) from the **source API doc**.

#### Other rules

- **Do NOT list the OAuth-issued tokens** (`access_token` / `refresh_token`) as required connection parameters — UC/the flow supplies them at runtime. List only the user-supplied app identity (`client_id`, `client_secret`) and any connector-specific options (e.g. `user_id`).
- `pkce` (default `true`) and `extra_auth_params` only affect the browser flows (`u2m` / `u2m_per_user`); set `pkce: false` for a confidential client that doesn't need it.
- The `authorization_url` / `token_url` / `scopes` are defaults baked into the spec so the user does not retype them; they can be overridden via `--options` at connection creation.
- The connector implementation should read the injected token (typically `options["access_token"]`) and treat it as opaque.
- Use this block in addition to Option A's flat `parameters` (it is not a substitute for the `parameters` list).

### Parameter Documentation
For each parameter, document:
- `name`: The parameter name as used in the options dict
- `type`: The data type (string, integer, boolean, etc.)
- `required`: Whether the parameter is required (true/false within its context)
- `description`: A brief description of the parameter's purpose

For `auth_methods`, also include:
- `name`: A short identifier for the auth method (e.g., `api_token`, `oauth`, `service_account`)
- `description`: Explanation of when/why to use this method

**Important**: Do NOT include table-specific options in the connection parameters.

### External Options Allowlist
- Review all `read_table`, `get_table_schema`, and `read_table_metadata` methods to identify table-specific options accessed from `table_options`.
- Compile all unique option names into a comma-separated string.
- Common table-specific options include:
  - Resource identifiers (e.g., `owner`, `repo`, `account_id`)
  - Filters (e.g., `state`, `status`, `type`)
  - Pagination controls (e.g., `per_page`, `max_pages_per_batch`)
  - Incremental sync options (e.g., `start_date`, `lookback_seconds`)
- If no table-specific options are used, set `external_options_allowlist` to an empty string.

### Reference Sources
- Use the connector implementation (`src/databricks/labs/community_connector/sources/{{source_name}}/{{source_name}}.py`) as the primary source of truth.
- Cross-reference with the connector's README (`src/databricks/labs/community_connector/sources/{{source_name}}/README.md`) if available.
- If inconsistency found between the 2 above, please fix the README and flag errors to user.