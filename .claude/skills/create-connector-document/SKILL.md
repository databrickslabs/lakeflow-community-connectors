---
name: create-connector-document
description: Generate public-facing documentation for a connector targeted at end users.
disable-model-invocation: true
---

# Create Public Connector Documentation

## Goal
Generate the **public-facing documentation** for the **{{source_name}}** connector, targeted at end users.

## Output Contract
Produce a Markdown file strictly following the standard template at `templates/community_connector_doc_template.md` as `src/databricks/labs/community_connector/sources/{{source_name}}/README.md`.

## Documentation Requirements

- Please use the code implementation as the source of truth.
- Use the source API documentation to cover anything missing.
- Always include a section about how to configure the parameters needed to connect to the source system.
- If the `connector_spec.yaml` declares a `connection.oauth` block, document the connection as **OAuth-based**: the user registers an OAuth app in the source and supplies its `client_id` + `client_secret` (not a token); the connection obtains the token automatically. Cover how to create the OAuth app in the source and which scopes to grant. Describe the *user experience*, not the internal flow name — **do not mention `m2m` / `u2m` / `u2m_per_user`** in end-user docs (users don't configure those; the flow is fixed by the spec). Let the spec's `flow` decide only what you describe:
  - client-credentials: the connection authenticates machine-to-machine — no browser login, no redirect URI to register.
  - interactive: creating the connection opens a browser for the user to sign in and authorize, so the OAuth-app setup section MUST document the **Authorized redirect URIs** to register, covering both ways a connection can be created:
    - **Databricks UI flow** (from the "Add Data" page): the provider redirects back to Databricks, so the user must add `https://<your-workspace-url>/login/oauth/http.html` (substituting their own workspace URL). Omitting this causes a `redirect_uri_mismatch` error in the consent popup.
    - **`community-connector` CLI flow**: the CLI listens on a local loopback, so registering the loopback host (e.g. `http://localhost`, which covers the `http://127.0.0.1:<port>/callback` the CLI picks at runtime) is sufficient — no workspace URL is involved.

  See the Gmail connector README for the reference pattern. (Only surface a flow choice if the connector genuinely supports more than one and the user must pick.)
- AVOID mentioning internal implementation terms such as function or argument names from the `LakeflowConnect`.