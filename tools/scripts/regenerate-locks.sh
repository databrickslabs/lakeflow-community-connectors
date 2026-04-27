#!/bin/sh
# Regenerate pinned dependency files used by CI.
#
# Resolves each CI install environment into a locked requirements.txt-style
# file with exact versions for direct and transitive deps. The cutoff date
# establishes the trust horizon: uv only considers package versions
# published before that timestamp, so a post-cutoff compromise cannot be
# resolved into the lock.
#
# No hashes (per the current internal guidance). PyPI's re-upload
# prohibition for existing versions is the assumed integrity guarantee.
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
CUTOFF="2026-03-19T00:00:00Z"
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
    uv pip compile "$@" \
        --exclude-newer "${CUTOFF}" \
        --python-version "${PYTHON_VERSION}" \
        --output-file "${out}" \
        --quiet
}

printf 'Regenerating locks (cutoff: %s, python: %s)...\n' "${CUTOFF}" "${PYTHON_VERSION}"

# Root package [dev] — test-libs, test-pipeline, test-example.
compile "${REQ_DIR}/root.txt" pyproject.toml --extra dev

# tools/community_connector [dev] — test-community-connector.
compile "${REQ_DIR}/tools.txt" tools/community_connector/pyproject.toml --extra dev

# Pylint runs against root + tools, and also installs pylint itself. The
# extras file is tracked in the repo so the path baked into the generated
# header is deterministic — using `mktemp` here would put a random /tmp path
# into the lock header and cause spurious drift warnings on every regen.
compile "${REQ_DIR}/pylint.txt" \
    pyproject.toml \
    tools/community_connector/pyproject.toml \
    "${REQ_DIR}/_pylint-extras.in" \
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
    - \
    --extra dev

printf '\nDone. Review with: git diff %s/\n' "${REQ_DIR}"
