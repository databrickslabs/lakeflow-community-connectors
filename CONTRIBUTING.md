We happily welcome contributions to Lakeflow-Community-Connectors. We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## CI on pull requests from forks

CI runs on a hardened runner group whose only egress to PyPI is a JFrog proxy that authenticates via the GitHub OIDC token. The OIDC token is only issued for events that run in base-repo context (`push`, internal-branch `pull_request`, `pull_request_target`), so fork PRs cannot run CI without a maintainer-mediated approval step.

To keep that approval step from becoming an arbitrary-code-execution channel, fork-PR CI is gated by an explicit `safe-to-test` label rather than by GitHub's "Require approval for outside collaborators" click. The label model makes the approval auditable, and the label is auto-stripped on every new push so each commit needs a fresh sign-off.

### Flow for fork contributors

1. Open the PR as normal. None of the CI workflows will run on first open.
2. A maintainer reviews the diff and, if they're satisfied, applies the `safe-to-test` label. CI then runs against the exact commit they labeled.
3. If you push new commits, the label is automatically removed and a maintainer must re-apply it. This is intentional — every commit on a fork PR needs a fresh sign-off.

### Flow for maintainers

Before applying `safe-to-test`, scan the diff for surfaces that execute in the privileged CI context. The most common pwn-request vectors:

- `.github/**` — workflow or action changes
- `pyproject.toml`, `requirements/**` — install-time hooks, new/changed deps
- `conftest.py` files — pytest auto-loads them at collection time
- `setup.py` / `[tool.setuptools]` entry points — install-time code
- New top-level `__init__.py` that imports as part of test discovery

If anything looks off, do not label — ask the contributor to clean it up first. Once you apply `safe-to-test`, the labeled commit is executed on the protected runner with OIDC in scope; treat the click like merging unreviewed code into a privileged context, because that's what it is.

Internal contributors pushing branches inside `databrickslabs/lakeflow-community-connectors` are not affected — their PRs run CI automatically on every push.
