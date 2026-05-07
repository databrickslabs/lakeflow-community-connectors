We happily welcome contributions to Lakeflow-Community-Connectors. We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## CI on pull requests from forks

CI for this repo runs on hardened runners that exchange a per-job OIDC token for a JFrog Artifactory access token. GitHub does not issue OIDC tokens to `pull_request` events from forks, so PRs opened from a fork need an explicit maintainer approval to run CI:

1. A maintainer reviews the PR's diff (including any change under `requirements/` or `pyproject.toml`).
2. The maintainer adds the `safe-to-test` label. CI runs against the labeled commit.
3. If the contributor pushes new commits, CI does **not** auto-rerun. The maintainer reviews the new commits, removes `safe-to-test`, then re-adds it to retrigger CI.

This explicit toggle is intentional: it ensures every set of commits that runs against the privileged runner is reviewed by a maintainer first. Internal contributors pushing branches inside `databrickslabs/lakeflow-community-connectors` are not affected — their PRs run CI automatically on every push.
