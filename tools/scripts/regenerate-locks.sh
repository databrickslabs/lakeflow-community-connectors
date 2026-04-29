#!/bin/sh
# Regenerate pinned dependency files used by CI.
#
# Resolves each CI install environment into a locked requirements.txt-style
# file with exact versions for direct and transitive deps. The cutoff date
# establishes the trust horizon: uv only considers package versions
# published before that timestamp, so a post-cutoff compromise cannot be
# resolved into the lock.
#
# Hashes (`--generate-hashes`) are emitted for every pin. When pip sees a
# requirements file with hashes, it implicitly enables --require-hashes,
# refusing any install whose artifact does not match. This fails closed on
# a registry-side swap or a compromised mirror, instead of trusting PyPI's
# re-upload prohibition as the sole integrity guarantee.
#
# RUN ON A LINUX HOST with PyPI access (e.g. arca). Running on macOS will
# resolve different wheels than CI uses. Running off the corp network may
# hit proxy throttling; arca avoids both issues.
#
# Requires: uv (https://docs.astral.sh/uv/).
#   Install: curl -LsSf https://astral.sh/uv/install.sh | sh

set -eu

# Trust horizon. Bump deliberately when pulling in vetted updates — each
# bump means "we accept as trusted anything PyPI had at this timestamp."
#
# Bump policy:
#   - Monthly cadence: bump to a recent timestamp, regenerate, review the
#     diff, and merge.
#   - On-demand: bump immediately when a CVE lands in a package pinned
#     transitively in any of requirements/*.txt and the fixed version was
#     published after the current CUTOFF.
#   - The PR description must state the new CUTOFF value and the reason
#     for the bump (cadence vs. CVE — link the advisory).
CUTOFF="2026-04-22T00:00:00Z"
PYTHON_VERSION="3.10"
REQ_DIR="requirements"

if ! command -v uv >/dev/null 2>&1; then
    printf 'error: uv not found on PATH. Install with:\n' 1>&2
    printf '  curl -LsSf https://astral.sh/uv/install.sh | sh\n' 1>&2
    exit 1
fi

root="$(git rev-parse --show-toplevel)"
cd "${root}"
mkdir -p "${REQ_DIR}"

compile() {
    out="$1"
    shift
    printf '  %s\n' "${out}"
    # `--exclude-newer-package pip=...` exempts pip from the global cutoff.
    # Required when resolving via JFrog Artifactory: its PyPI proxy strips
    # upload-time metadata for some versions, so uv cannot validate them
    # against `--exclude-newer` and refuses every pip version. Pip is the
    # bootstrap and is hash-pinned in the resulting lock — the cutoff is
    # not the integrity control, the hash is. No-op on arca/direct PyPI.
    uv pip compile "$@" \
        --generate-hashes \
        --exclude-newer "${CUTOFF}" \
        --exclude-newer-package pip=2030-01-01T00:00:00Z \
        --python-version "${PYTHON_VERSION}" \
        --output-file "${out}" \
        --quiet
}

printf 'Regenerating locks (cutoff: %s, python: %s)...\n' "${CUTOFF}" "${PYTHON_VERSION}"

# Pip is pinned into every lock via `_pip.in` so the install step that
# enforces --require-hashes is itself a hash-verified package, not whatever
# the runner image happens to ship. See requirements/_pip.in for rationale.

# Root package [dev] — test-libs, test-pipeline, test-example.
compile "${REQ_DIR}/root.txt" pyproject.toml "${REQ_DIR}/_pip.in" --extra dev

# tools/community_connector [dev] — test-community-connector.
compile "${REQ_DIR}/tools.txt" tools/community_connector/pyproject.toml "${REQ_DIR}/_pip.in" --extra dev

# Pylint runs against root + tools, and also installs pylint itself. The
# extras file is tracked in the repo so the path baked into the generated
# header is deterministic — using `mktemp` here would put a random /tmp path
# into the lock header and cause spurious drift warnings on every regen.
compile "${REQ_DIR}/pylint.txt" \
    pyproject.toml \
    tools/community_connector/pyproject.toml \
    "${REQ_DIR}/_pylint-extras.in" \
    "${REQ_DIR}/_pip.in" \
    --extra dev

# Combined source-connector lock — shared across the test-source matrix in
# .github/workflows/tests.yml. Unions third-party runtime deps from every
# `sources/*/pyproject.toml` and resolves them together with the root
# pyproject's deps + dev extra (pytest, pytest-cov), so transitive
# resolution is fully pinned. Without this lock, test-source falls back to
# live PyPI against ranged constraints.
#
# A single union lock matches the one-lock-per-CI-job-context pattern of
# root.txt / tools.txt / pylint.txt. Each connector job installs a
# superset of what it strictly needs, but every job installs the same
# pinned transitive set, which is the supply-chain control that matters.
#
# We strip the `lakeflow-community-connectors` self-reference (not on
# PyPI; installed editable from the local checkout via `pip install -e .
# --no-deps`) and pipe the union via stdin (`-`) so the autogen header
# records a deterministic input path — a `mktemp` path here would change
# every run and trigger spurious lock-drift warnings.
SOURCES_DIR="src/databricks/labs/community_connector/sources"
for src_pyproject in "${SOURCES_DIR}"/*/pyproject.toml; do
    sed -n '/^dependencies *= *\[/,/^\]/{
        s/^[[:space:]]*"//
        s/",\?[[:space:]]*$//
        /^lakeflow-community-connectors/d
        /^dependencies/d
        /^\]/d
        /^[[:space:]]*$/d
        /^[[:space:]]*#/d
        p
    }' "${src_pyproject}"
done | sort -u | compile "${REQ_DIR}/sources.txt" \
    pyproject.toml \
    "${REQ_DIR}/_pip.in" \
    - \
    --extra dev

printf '\nDone. Review with: git diff %s/\n' "${REQ_DIR}"
