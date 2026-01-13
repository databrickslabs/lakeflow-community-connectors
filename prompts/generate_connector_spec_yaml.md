# Generate Connector Spec YAML

## Goal
Generate a **connector spec YAML file** for the **{{source_name}}** connector that defines the connector specification including connection parameters and external options allowlist.

## Output Contract
Produce a YAML file following the template [connector_spec_template.yaml](templates/connector_spec_template.yaml) as `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml`.

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
Use `auth_methods` and `common_parameters` when the connector supports multiple ways to authenticate (e.g., API key vs. OAuth, or service account vs. API secret):

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

  common_parameters:
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
