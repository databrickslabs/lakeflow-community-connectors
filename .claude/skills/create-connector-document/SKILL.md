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
- If the `connector_spec.yaml` declares a `connection.oauth` block, document the connection as **OAuth-based**: the user registers an OAuth app in the source and supplies its `client_id` + `client_secret` (not a token); the connection obtains the token automatically. Cover how to create the OAuth app in the source and which scopes to grant. Tailor the wording to the spec's `flow`:
  - **`m2m`** (client-credentials): a machine-to-machine token exchange — no user login, no browser, no redirect URI.
  - **`u2m` / `u2m_per_user`**: an interactive login/consent flow — creating the connection opens a browser for the user to authorize, so also document the redirect URI to register on the OAuth app.

  See the Gmail connector README for the reference pattern (a `u2m` connector).
- AVOID mentioning internal implementation terms such as function or argument names from the `LakeflowConnect`.