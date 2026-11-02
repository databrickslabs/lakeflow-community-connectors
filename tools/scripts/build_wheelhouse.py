#!/usr/bin/env python3
"""
Build the prebuilt wheels under wheelhouse/ and check that they are up to date.

The Databricks managed ingestion wizard installs connectors from
wheelhouse/_framework/ and wheelhouse/<source_name>/, so each directory must hold
exactly one wheel built from the current source, named after the `version` in the
package's pyproject.toml.

A committed wheel is only replaced when its contents change in a way that affects
pipelines. Docs (*.md), the README-derived long description in METADATA, the
build tool version in WHEEL, and the source simulator (test-only) are ignored, so
rebuilding from unchanged code leaves the committed binary untouched.

Usage:
    python tools/scripts/build_wheelhouse.py <source_name> [<source_name> ...]
    python tools/scripts/build_wheelhouse.py --framework
    python tools/scripts/build_wheelhouse.py all          # every connector + framework

    # Exit 1 instead of writing if any wheel is missing or stale:
    python tools/scripts/build_wheelhouse.py <source_name> --check

    # Also fail if a wheel's contents changed since <ref> without a version bump:
    python tools/scripts/build_wheelhouse.py <source_name> --check --base origin/master
"""

import argparse
import io
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SOURCES_DIR = PROJECT_ROOT / "src/databricks/labs/community_connector/sources"
WHEELHOUSE_DIR = PROJECT_ROOT / "wheelhouse"
FRAMEWORK = "_framework"

# Members that don't change what a pipeline runs.
IGNORED_MEMBER_PATTERNS = [
    re.compile(r"\.md$"),
    re.compile(r"^databricks/labs/community_connector/source_simulator/"),
    # RECORD only holds hashes of the other members.
    re.compile(r"\.dist-info/RECORD$"),
]


def list_sources() -> List[str]:
    return sorted(p.parent.name for p in SOURCES_DIR.glob("*/pyproject.toml") if p.parent.is_dir())


def package_dir(target: str) -> Path:
    return PROJECT_ROOT if target == FRAMEWORK else SOURCES_DIR / target


def pyproject_path(target: str) -> Path:
    return package_dir(target) / "pyproject.toml"


def read_project_field(pyproject_text: str, field: str) -> str:
    """Read a static string field from the [project] table of a pyproject.toml."""
    in_project = False
    for line in pyproject_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            in_project = stripped == "[project]"
            continue
        if in_project:
            match = re.match(rf'^{field}\s*=\s*"([^"]+)"', stripped)
            if match:
                return match.group(1)
    raise ValueError(f"No static `{field}` in [project]")


def expected_wheel_name(pyproject_text: str) -> str:
    # Wheel filenames normalize the distribution name (PEP 427): runs of -_. become _.
    name = re.sub(r"[-_.]+", "_", read_project_field(pyproject_text, "name")).lower()
    version = read_project_field(pyproject_text, "version")
    return f"{name}-{version}-py3-none-any.whl"


def comparable_contents(wheel_bytes: bytes) -> Dict[str, bytes]:
    """Map member name -> bytes, minus ignored members and METADATA's long description."""
    contents = {}
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as zf:
        for name in zf.namelist():
            if any(p.search(name) for p in IGNORED_MEMBER_PATTERNS):
                continue
            data = zf.read(name)
            if name.endswith(".dist-info/METADATA"):
                # Headers end at the first blank line; the README body follows.
                data = data.split(b"\n\n", 1)[0]
            elif name.endswith(".dist-info/WHEEL"):
                # The setuptools version in `Generator:` varies with the isolated build env.
                data = re.sub(rb"(?m)^Generator:.*\n", b"", data)
            contents[name] = data
    return contents


def build_wheel(target: str, outdir: Path) -> Path:
    cmd = [
        sys.executable,
        "-m",
        "build",
        "--wheel",
        str(package_dir(target)),
        "--outdir",
        str(outdir),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        tail = "\n".join(result.stderr.strip().splitlines()[-10:])
        raise RuntimeError(f"`python -m build` failed for {target}:\n{tail}")
    wheels = list(outdir.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"Expected one wheel for {target}, got {[w.name for w in wheels]}")
    return wheels[0]


def git_show(ref: str, path: Path) -> Optional[bytes]:
    rel = path.relative_to(PROJECT_ROOT).as_posix()
    result = subprocess.run(
        ["git", "show", f"{ref}:{rel}"], cwd=PROJECT_ROOT, capture_output=True, check=False
    )
    return result.stdout if result.returncode == 0 else None


def git_ls(ref: str, directory: Path) -> List[str]:
    rel = directory.relative_to(PROJECT_ROOT).as_posix()
    result = subprocess.run(
        ["git", "ls-tree", "--name-only", f"{ref}", f"{rel}/"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return [Path(line).name for line in result.stdout.splitlines() if line.endswith(".whl")]


def check_version_bump(target: str, built: Path, base: str) -> Optional[str]:
    """Return an error if the wheel changed since `base` but the version didn't."""
    base_dir = WHEELHOUSE_DIR / target
    base_wheels = git_ls(base, base_dir)
    if len(base_wheels) != 1:
        return None  # New on this branch; any version is fine.
    base_bytes = git_show(base, base_dir / base_wheels[0])
    if base_bytes is None or comparable_contents(base_bytes) == comparable_contents(
        built.read_bytes()
    ):
        return None
    base_pyproject = git_show(base, pyproject_path(target))
    if base_pyproject is None:
        return None
    base_version = read_project_field(base_pyproject.decode(), "version")
    version = read_project_field(pyproject_path(target).read_text(), "version")
    if version == base_version:
        return (
            f"{target}: wheel contents changed since {base} but version is still {version}; "
            f"bump `version` in {pyproject_path(target).relative_to(PROJECT_ROOT)} "
            "(see CONTRIBUTING.md, Versioning)"
        )
    return None


def process(target: str, check: bool, base: Optional[str]) -> List[str]:
    """Build `target` and update (or check) its wheelhouse directory. Returns errors."""
    errors = []
    dest_dir = WHEELHOUSE_DIR / target
    expected = expected_wheel_name(pyproject_path(target).read_text())
    with tempfile.TemporaryDirectory() as tmp:
        built = build_wheel(target, Path(tmp))
        if built.name != expected:
            return [f"{target}: built {built.name}, expected {expected} from pyproject.toml"]

        existing = sorted(dest_dir.glob("*.whl")) if dest_dir.is_dir() else []
        up_to_date = (
            len(existing) == 1
            and existing[0].name == expected
            and comparable_contents(existing[0].read_bytes())
            == comparable_contents(built.read_bytes())
        )

        if base:
            bump_error = check_version_bump(target, built, base)
            if bump_error:
                errors.append(bump_error)

        if up_to_date:
            print(f"up to date  {target}: {expected}")
        elif check:
            found = [w.name for w in existing]
            if found == [expected]:
                problem = f"{expected} doesn't match the current source"
            else:
                problem = f"found {', '.join(found) or 'no wheel'}, expected {expected}"
            rebuild_arg = "--framework" if target == FRAMEWORK else target
            errors.append(
                f"{target}: wheelhouse is stale ({problem}); "
                f"run `python tools/scripts/build_wheelhouse.py {rebuild_arg}`"
            )
        else:
            dest_dir.mkdir(parents=True, exist_ok=True)
            for old in existing:
                old.unlink()
            (dest_dir / expected).write_bytes(built.read_bytes())
            print(
                f"rebuilt     {target}: {expected}"
                + (f" (replaced {', '.join(w.name for w in existing)})" if existing else "")
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "sources", nargs="*", help="connector names, or `all` for every connector and the framework"
    )
    parser.add_argument("--framework", action="store_true", help="build wheelhouse/_framework/")
    parser.add_argument(
        "--check", action="store_true", help="don't write; exit 1 if any wheel is missing or stale"
    )
    parser.add_argument(
        "--base", help="git ref to compare against; fail if a wheel changed without a version bump"
    )
    args = parser.parse_args()

    targets = list(args.sources)
    if "all" in targets:
        targets = list_sources() + [FRAMEWORK]
    elif args.framework:
        targets.append(FRAMEWORK)
    if not targets:
        parser.error("give at least one source name, --framework, or `all`")

    known = set(list_sources()) | {FRAMEWORK}
    unknown = [t for t in targets if t not in known]
    if unknown:
        parser.error(f"unknown source(s): {', '.join(unknown)}")

    errors = []
    for target in targets:
        try:
            errors += process(target, args.check, args.base)
        except (RuntimeError, ValueError) as exc:
            errors.append(f"{target}: {exc}")
    for error in errors:
        print(f"ERROR  {error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
