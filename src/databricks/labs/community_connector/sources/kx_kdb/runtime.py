"""Shared KDB-X / PyKX runtime bootstrap helpers."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import importlib
import logging
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable

logger = logging.getLogger(__name__)


class _PicklableLock:
    """Process-local lock that can travel with the merged connector class."""

    def __init__(self):
        self._lock = threading.Lock()

    def __enter__(self):
        return self._lock.__enter__()

    def __exit__(self, *exc_info):
        return self._lock.__exit__(*exc_info)

    def __getstate__(self):
        return {}

    def __setstate__(self, _state):
        self._lock = threading.Lock()


class _ProcessLocalDict(dict):
    """Process-local cache that serializes as empty with the merged connector class.

    The merged source ships module state to executors by value; a cache filled
    on the driver must not make an executor skip its own q or runtime setup.
    """

    def __reduce__(self):
        return (type(self), ())


class _ProcessLocalSet(set):
    """Process-local set that serializes as empty with the merged connector class."""

    def __reduce__(self):
        return (type(self), ())


DEFAULT_KDBX_INSTALLER_URL = (
    "https://portal.dl.kx.com/assets/raw/kdb-x/install_kdb/~latest~/install_kdb.sh"
)
PYKX_PIP_SPEC = "pykx==4.0.0b5"
DEFAULT_OFFLINE_BUNDLE_NAME = "l64-bundle.zip"
ARM_OFFLINE_BUNDLE_NAME = "l64arm-bundle.zip"

KDBX_SECRET_SCOPE_OPTION = "kdbx_secret_scope"
KDBX_INSTALL_BEARER_SECRET_KEY_OPTION = "kdbx_install_bearer_secret_key"
KDBX_LICENSE_B64_SECRET_KEY_OPTION = "kdbx_license_b64_secret_key"
KDBX_INSTALL_BEARER_TOKEN_OPTION = "kdbx_install_bearer_token"
KDBX_LICENSE_B64_OPTION = "kdbx_license_b64"
KDBX_LICENSE_FILE_PATH_OPTION = "kdbx_license_file_path"
KDBX_LICENSE_KIND_OPTION = "kdbx_license_kind"
KDBX_OFFLINE_BUNDLE_PATH_OPTION = "kdbx_offline_bundle_path"
KDBX_INSTALL_MODE_OPTION = "kdbx_install_mode"
PYKX_INSTALL_SPEC_OPTION = "pykx_install_spec"

_RUNTIME_LOCK = _PicklableLock()
_PREPARED_RUNTIME_KEYS: set[str] = _ProcessLocalSet()
_RUNTIME_HOME_CACHE: dict[str, Path] = _ProcessLocalDict()
_LOCAL_BUNDLE_DIR_CACHE: dict[str, Path] = _ProcessLocalDict()
_RUNTIME_HOME_OVERRIDE_ENV = "KDBX_RUNTIME_HOME"
_LICENSE_FILE_NAMES = ("kc.lic", "k4.lic", "kx.lic")
_LICENSE_KIND_FILE_NAMES = {"kc": "kc.lic", "k4": "k4.lic"}
# install_kdb.sh writes kc.lic for --b64lic and k4.lic for --k4b64lic.
_INSTALLER_LICENSE_FLAGS = {"kc.lic": "--b64lic", "k4.lic": "--k4b64lic"}
_LICENSE_ENV_VARS = frozenset({"KDB_LICENSE_B64", "KDB_K4LICENSE_B64"})
# RFC 6750 b64token characters; anything else could break the curl config syntax.
_BEARER_TOKEN_RE = re.compile(r"^[A-Za-z0-9\-._~+/]+=*$")
_REDACTED = "[REDACTED]"
# install_kdb.sh comes from KX's latest release; it gets only what it needs.
_INSTALLER_ENV_ALLOWLIST = frozenset(
    {
        "PATH",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "TMPDIR",
        "USER",
        "LOGNAME",
        "SHELL",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "no_proxy",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "CURL_CA_BUNDLE",
    }
)


@dataclass(frozen=True)
class PyKxRuntimeConfig:
    """Runtime configuration required to initialize PyKX safely."""

    license_directory: str
    installer_bearer_token: str | None = dataclasses.field(default=None, repr=False)
    license_b64: str | None = dataclasses.field(default=None, repr=False)
    installer_url: str = DEFAULT_KDBX_INSTALLER_URL
    offline_bundle_path: str | None = None
    pykx_install_spec: str | None = None
    license_file_name: str = "kc.lic"

    @property
    def uses_installer(self) -> bool:
        if self.uses_offline_bundle:
            return True
        return bool(self.installer_bearer_token and self.license_b64)

    @property
    def uses_offline_bundle(self) -> bool:
        return bool(self.offline_bundle_path and self.license_b64)

    @property
    def secrets(self) -> tuple[str, ...]:
        values = (self.installer_bearer_token, self.license_b64)
        return tuple(value for value in values if value)


def normalize_license_directory(value: str) -> str:
    """Normalize a file-or-directory license path to the containing directory."""
    candidate = Path(str(value).strip())
    normalized = candidate if candidate.suffix == "" else candidate.parent
    return str(normalized)


def build_runtime_config(
    options: dict[str, str],
    *,
    secret_resolver: Callable[[str, str], str] | None = None,
) -> PyKxRuntimeConfig:
    """Build the runtime config from connector options.

    Resolution order, highest priority first:
      1. Offline bundle install (no network) when ``kdbx_offline_bundle_path``
         is set or a bundle is found at a well-known location near the license
         directory, AND a license b64 can be resolved.
      2. Online installer bootstrap (curl + ``install_kdb.sh``) when both a
         bearer token and a license b64 are available.
      3. Legacy "license folder + preinstalled PyKX" mode otherwise.
    """
    license_directory = normalize_license_directory(str(options.get("license_volume_path", "")))
    explicit_bundle_path = str(options.get(KDBX_OFFLINE_BUNDLE_PATH_OPTION, "")).strip()
    pykx_install_spec = _option(options, PYKX_INSTALL_SPEC_OPTION)
    install_mode = _install_mode(options)
    license_kind = _license_kind(options)

    direct_install_token = str(options.get(KDBX_INSTALL_BEARER_TOKEN_OPTION, "")).strip()
    direct_license_option = str(options.get(KDBX_LICENSE_B64_OPTION, "")).strip()
    license_file_b64 = ""
    detected_license_name = None
    if not direct_license_option:
        license_file_b64, detected_license_name = _read_license_file(
            str(options.get(KDBX_LICENSE_FILE_PATH_OPTION, "")).strip()
        )
    direct_license_b64 = direct_license_option or license_file_b64
    license_file_name = _license_file_name(license_kind, detected_license_name)

    def _config(**values) -> PyKxRuntimeConfig:
        return PyKxRuntimeConfig(
            license_directory=license_directory,
            pykx_install_spec=pykx_install_spec,
            license_file_name=license_file_name,
            **values,
        )

    if direct_license_b64 and explicit_bundle_path and install_mode != "online":
        return _config(
            installer_bearer_token=direct_install_token or None,
            license_b64=direct_license_b64,
            offline_bundle_path=explicit_bundle_path,
        )

    direct_supplied = [bool(direct_install_token), bool(direct_license_option)]
    if any(direct_supplied) and not all(direct_supplied):
        raise ValueError(
            "KDB-X bootstrap requires both "
            f"{KDBX_INSTALL_BEARER_TOKEN_OPTION!r} and {KDBX_LICENSE_B64_OPTION!r} "
            "when using direct connection options."
        )
    if all(direct_supplied):
        config = _config(
            installer_bearer_token=direct_install_token,
            license_b64=direct_license_b64,
        )
        return config if install_mode == "online" else _maybe_offline(config, explicit_bundle_path)

    if license_file_b64:
        if install_mode == "online":
            raise ValueError(
                f"{KDBX_INSTALL_MODE_OPTION}='online' requires an installer bearer token "
                "and license b64, not only a license file."
            )
        return _maybe_offline(_config(license_b64=license_file_b64), explicit_bundle_path)

    secret_scope = str(options.get(KDBX_SECRET_SCOPE_OPTION, "")).strip()
    install_token_key = str(options.get(KDBX_INSTALL_BEARER_SECRET_KEY_OPTION, "")).strip()
    license_b64_key = str(options.get(KDBX_LICENSE_B64_SECRET_KEY_OPTION, "")).strip()

    supplied = [bool(secret_scope), bool(install_token_key), bool(license_b64_key)]
    if any(supplied) and not all(supplied):
        raise ValueError(
            "KDB-X bootstrap requires all of "
            f"{KDBX_SECRET_SCOPE_OPTION!r}, "
            f"{KDBX_INSTALL_BEARER_SECRET_KEY_OPTION!r}, and "
            f"{KDBX_LICENSE_B64_SECRET_KEY_OPTION!r} together."
        )

    if not all(supplied):
        return _config()

    resolver = secret_resolver or _resolve_databricks_secret
    installer_bearer_token = resolver(secret_scope, install_token_key).strip()
    license_b64 = resolver(secret_scope, license_b64_key).strip()
    if not installer_bearer_token or not license_b64:
        raise ValueError(
            "Resolved empty KDB-X bootstrap secret value. Check the configured "
            "secret scope and secret keys."
        )

    config = _config(installer_bearer_token=installer_bearer_token, license_b64=license_b64)
    return config if install_mode == "online" else _maybe_offline(config, explicit_bundle_path)


def _option(options: dict[str, str], key: str) -> str | None:
    value = str(options.get(key, "")).strip()
    return value or None


def _install_mode(options: dict[str, str]) -> str:
    value = str(options.get(KDBX_INSTALL_MODE_OPTION, "auto")).strip().lower()
    mode = value or "auto"
    if mode not in {"auto", "online", "offline"}:
        raise ValueError(
            f"Unsupported {KDBX_INSTALL_MODE_OPTION} {mode!r}. "
            "Expected 'auto', 'online', or 'offline'."
        )
    return mode


def _license_kind(options: dict[str, str]) -> str | None:
    value = str(options.get(KDBX_LICENSE_KIND_OPTION, "")).strip().lower()
    if not value:
        return None
    if value not in _LICENSE_KIND_FILE_NAMES:
        raise ValueError(
            f"Unsupported {KDBX_LICENSE_KIND_OPTION} {value!r}. Expected 'k4' for a "
            "commercial k4.lic license or 'kc' for a kc.lic license."
        )
    return value


def _license_file_name(license_kind: str | None, detected_name: str | None) -> str:
    if license_kind:
        return _LICENSE_KIND_FILE_NAMES[license_kind]
    if detected_name == "k4.lic":
        return "k4.lic"
    return "kc.lic"


def _read_license_file(path: str) -> tuple[str, str | None]:
    """Return the base64 license and its file name when it is a known license name."""
    if not path:
        return "", None
    candidate = Path(path)
    if candidate.is_dir():
        for file_name in _LICENSE_FILE_NAMES:
            license_file = candidate / file_name
            if license_file.is_file():
                return base64.b64encode(license_file.read_bytes()).decode("ascii"), file_name
        return "", None
    if candidate.is_file():
        name = candidate.name if candidate.name in _LICENSE_FILE_NAMES else None
        return base64.b64encode(candidate.read_bytes()).decode("ascii"), name
    return "", None


def _maybe_offline(config: PyKxRuntimeConfig, explicit_bundle_path: str) -> PyKxRuntimeConfig:
    """Promote a config to offline-install mode when a bundle is available.

    The offline path requires only the license b64, but the bearer token is
    preserved when available so worker-side installs can fall back to the
    online installer if a UC volume bundle cannot be localized.
    """
    if not config.license_b64:
        return config
    bundle_path = (
        explicit_bundle_path
        or _probe_offline_bundle(config.license_directory)
        or ""
    )
    if not bundle_path:
        return config
    return dataclasses.replace(config, offline_bundle_path=bundle_path)


def _probe_offline_bundle(license_directory: str) -> str | None:
    """Look for a pre-staged KDB-X install bundle near the license directory.

    Probed in order:
      - ``<license_directory>/l64-bundle.zip``
      - ``<license_directory>/../kdbx/l64-bundle.zip``
    Returns the first existing file path or ``None``.
    """
    if not license_directory:
        return None
    base = Path(license_directory)
    for candidate in _offline_bundle_candidates(base):
        try:
            if candidate.is_file():
                return str(candidate)
        except OSError:
            continue
    return None


def _offline_bundle_candidates(base: Path) -> tuple[Path, ...]:
    return tuple(
        candidate
        for bundle_name in _offline_bundle_names_for_platform()
        for candidate in (
            base / bundle_name,
            base.parent / "kdbx" / bundle_name,
        )
    )


def _bundle_path_status(paths: tuple[Path, ...]) -> str:
    statuses = []
    for path in paths:
        try:
            statuses.append(
                f"{path}: exists={path.exists()} is_file={path.is_file()}"
            )
        except OSError as exc:
            statuses.append(f"{path}: error={type(exc).__name__}: {exc}")
    return "; ".join(statuses)


def _offline_bundle_names_for_platform() -> tuple[str, ...]:
    machine = platform.machine().lower()
    if machine in {"aarch64", "arm64"}:
        return (ARM_OFFLINE_BUNDLE_NAME, DEFAULT_OFFLINE_BUNDLE_NAME)
    return (DEFAULT_OFFLINE_BUNDLE_NAME,)


def _runtime_key(config: PyKxRuntimeConfig) -> str:
    """Return a digest identifying one bootstrap configuration without keeping secrets."""
    digest = hashlib.sha256()
    for value in (
        config.license_directory,
        config.installer_bearer_token or "",
        config.license_b64 or "",
        config.license_file_name,
        config.offline_bundle_path or "",
        config.pykx_install_spec or "",
        config.installer_url,
    ):
        digest.update(value.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def prepare_pykx(config: PyKxRuntimeConfig):
    """Install/configure KDB-X and import PyKX after license setup."""
    runtime_key = _runtime_key(config)

    with _RUNTIME_LOCK:
        if config.uses_installer and runtime_key not in _PREPARED_RUNTIME_KEYS:
            logger.info("Bootstrapping KDB-X and PyKX for serverless execution.")
            _install_kdbx(config)
            _PREPARED_RUNTIME_KEYS.add(runtime_key)

        _apply_pykx_environment(config)
        importlib.invalidate_caches()

        if "pykx" in sys.modules:
            return sys.modules["pykx"]
        try:
            return importlib.import_module("pykx")
        except ModuleNotFoundError as exc:
            if exc.name != "pykx" or not config.uses_installer:
                raise

        _ensure_pykx_package(config)
        importlib.invalidate_caches()
        return importlib.import_module("pykx")


def _resolve_databricks_secret(scope: str, key: str) -> str:
    try:
        from databricks.sdk.runtime import (  # pylint: disable=import-error,no-name-in-module
            dbutils as runtime_dbutils,
        )

        return runtime_dbutils.secrets.get(scope, key)
    except Exception:
        pass

    try:
        from pyspark.dbutils import DBUtils  # pylint: disable=import-error,no-name-in-module
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError(
            "Unable to import Databricks secret helpers. Resolve KDB-X secrets on "
            "the driver before using the connector."
        ) from exc

    spark = SparkSession.getActiveSession()
    if spark is None:
        get_default_session = getattr(SparkSession, "getDefaultSession", None)
        if callable(get_default_session):
            spark = get_default_session()  # pylint: disable=not-callable
    if spark is None:
        spark = getattr(SparkSession, "_instantiatedSession", None)
    if spark is None:
        try:
            spark = SparkSession.builder.getOrCreate()  # pylint: disable=no-member
        except Exception as exc:
            raise RuntimeError(
                "A SparkSession is required to resolve KDB-X secrets for the connector."
            ) from exc
    if spark is None:
        raise RuntimeError("A SparkSession is required to resolve KDB-X secrets for the connector.")

    return DBUtils(spark).secrets.get(scope, key)


def _install_kdbx(config: PyKxRuntimeConfig) -> None:
    if not config.uses_installer:
        return
    if config.uses_offline_bundle:
        _install_kdbx_offline(config)
    else:
        _install_kdbx_online(config)


def _prepare_runtime_home() -> Path:
    runtime_home = _runtime_home_directory()
    runtime_home.mkdir(parents=True, exist_ok=True)
    # The KDB-X installer auto-selects "$HOME/.kx" as the install location and
    # aborts in non-interactive mode if it cannot create that directory.
    (runtime_home / ".kx").mkdir(parents=True, exist_ok=True)
    return runtime_home


def _install_kdbx_online(config: PyKxRuntimeConfig) -> None:
    token = _validated_bearer_token(config.installer_bearer_token)
    runtime_home = _prepare_runtime_home()

    with tempfile.TemporaryDirectory() as tmpdir:
        installer_path = Path(tmpdir) / "install_kdb.sh"
        # `-q` must come first so ~/.curlrc is ignored; the token travels on stdin.
        download = _run_subprocess(
            ["curl", "-q", "--config", "-"],
            config=config,
            label="KDB-X installer download",
            timeout=180,
            input=_curl_config(config.installer_url, token, installer_path),
            cwd=tmpdir,
            env=_child_process_env(config),
        )
        if download.returncode != 0:
            stderr_tail = _command_tail(download.stderr, secrets=config.secrets)
            raise RuntimeError(
                f"KDB-X installer download failed with rc={download.returncode}: {stderr_tail}"
            )
        _run_installer(config, installer_path, runtime_home, cwd=tmpdir, offline=False)


def _validated_bearer_token(token: str | None) -> str:
    value = str(token or "")
    if not _BEARER_TOKEN_RE.fullmatch(value):
        raise ValueError(
            "KDB-X installer bearer token contains unsupported characters; expected an "
            "RFC 6750 bearer token."
        )
    return value


def _curl_config(url: str, token: str, output_path: Path) -> str:
    lines = (
        "silent",
        "show-error",
        "location",
        "fail-with-body",
        f'oauth2-bearer = "{token}"',
        f'url = "{_curl_quote(url)}"',
        f'output = "{_curl_quote(str(output_path))}"',
    )
    return "\n".join(lines) + "\n"


def _curl_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _run_installer(
    config: PyKxRuntimeConfig,
    installer_path: Path,
    runtime_home: Path,
    *,
    cwd: str,
    offline: bool,
) -> None:
    args = ["bash", str(installer_path)]
    if offline:
        args.append("--offline")
    # Without `-y` the script falls into interactive mode and hangs on its
    # first prompt. install_kdb.sh accepts the license only as an argument.
    args.append("-y")
    args.extend(_installer_license_args(config))
    label = "install_kdb.sh --offline" if offline else "install_kdb.sh"
    install = _run_subprocess(
        args,
        config=config,
        label=label,
        timeout=1200,
        cwd=cwd,
        env=_installer_env(runtime_home),
    )
    if install.returncode != 0:
        raise RuntimeError(
            f"{label} failed with rc={install.returncode}. stdout tail: "
            f"{_command_tail(install.stdout, secrets=config.secrets)}. stderr tail: "
            f"{_command_tail(install.stderr, secrets=config.secrets)}"
        )


def _run_subprocess(
    args: list[str],
    *,
    config: PyKxRuntimeConfig,
    label: str,
    timeout: int,
    **kwargs,
) -> subprocess.CompletedProcess:
    """Run a bootstrap command without an interactive stdin or secret-bearing errors."""
    if "input" not in kwargs:
        # install_kdb.sh prompts for missing optional bundle components even
        # with -y; an inherited open stdin would block it until the timeout.
        kwargs["stdin"] = subprocess.DEVNULL
    try:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            **kwargs,
        )
    except subprocess.TimeoutExpired as exc:
        # TimeoutExpired repeats argv, which includes the license; raise
        # outside this block so the new error keeps no reference to it.
        timed_out = (_decode(exc.stdout), _decode(exc.stderr))
    raise RuntimeError(
        f"{label} timed out after {timeout} seconds. stdout tail: "
        f"{_command_tail(timed_out[0], secrets=config.secrets)}. stderr tail: "
        f"{_command_tail(timed_out[1], secrets=config.secrets)}"
    )


def _decode(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value or "")


def _installer_license_args(config: PyKxRuntimeConfig) -> list[str]:
    flag = _INSTALLER_LICENSE_FLAGS.get(config.license_file_name, "--b64lic")
    return [flag, config.license_b64 or ""]


def _installer_env(runtime_home: Path) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key in _INSTALLER_ENV_ALLOWLIST}
    # TERM overrides values such as "unknown" that break tput in the script.
    env.update({"HOME": str(runtime_home), "TERM": "dumb"})
    return env


def _child_process_env(config: PyKxRuntimeConfig, **overrides: str) -> dict[str, str]:
    """Return the parent environment without KDB license variables or secret values."""
    secrets = set(config.secrets)
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in _LICENSE_ENV_VARS and value not in secrets
    }
    env.update(overrides)
    return env


def _install_kdbx_offline(config: PyKxRuntimeConfig) -> None:
    """Install KDB-X from a pre-staged offline bundle (no network access)."""
    runtime_home = _prepare_runtime_home()
    bundle_path = _localize_offline_bundle(config.offline_bundle_path or "")

    with tempfile.TemporaryDirectory() as tmpdir:
        _extract_offline_bundle(bundle_path, Path(tmpdir))

        installer_path = Path(tmpdir) / "install_kdb.sh"
        if not installer_path.is_file():
            raise RuntimeError(
                f"Offline KDB-X bundle at {str(bundle_path)!r} does not contain "
                "install_kdb.sh."
            )
        installer_path.chmod(0o755)
        _run_installer(config, installer_path, runtime_home, cwd=tmpdir, offline=True)


def _extract_offline_bundle(bundle_path: Path, target: Path) -> None:
    try:
        with zipfile.ZipFile(bundle_path) as archive:
            members = archive.infolist()
            for member in members:
                if not _is_safe_bundle_member(member):
                    raise RuntimeError(
                        f"Offline KDB-X bundle at {str(bundle_path)!r} contains an unsafe "
                        f"member {member.filename!r}."
                    )
            archive.extractall(target, members)
    except zipfile.BadZipFile as exc:
        raise RuntimeError(
            f"Offline KDB-X bundle at {str(bundle_path)!r} is not a valid zip."
        ) from exc


def _is_safe_bundle_member(member: zipfile.ZipInfo) -> bool:
    name = member.filename
    if not name or "\\" in name or name.startswith("/"):
        return False
    if ".." in PurePosixPath(name).parts:
        return False
    return not stat.S_ISLNK(member.external_attr >> 16)


def _ensure_pykx_package(config: PyKxRuntimeConfig) -> None:
    spec = config.pykx_install_spec or PYKX_PIP_SPEC
    target = str(_runtime_home_directory() / "pykx_pkgs")
    command = [
        sys.executable,
        "-I",
        "-m",
        "pip",
        "install",
        "--quiet",
        "--ignore-installed",
        "--target",
        target,
    ]
    if not _is_wheel_or_path(spec):
        command.append("--pre")
    command.append(spec)
    pip_env = _child_process_env(config)
    # Spark workers inherit a PYTHONPATH containing JAR paths. Pip scans every
    # entry as a possible distribution and can fail with PermissionError on
    # protected Databricks JARs. The isolated interpreter and clean path keep
    # the fallback install confined to ``target``.
    pip_env.pop("PYTHONPATH", None)
    pip_env.pop("PYTHONHOME", None)
    pip_install = _run_subprocess(
        command,
        config=config,
        label=f"PyKX package install {spec!r}",
        timeout=1200,
        env=pip_env,
    )
    if pip_install.returncode != 0:
        raise RuntimeError(
            f"Failed to install PyKX package {spec!r}. stdout tail: "
            f"{_command_tail(pip_install.stdout, secrets=config.secrets)}. stderr tail: "
            f"{_command_tail(pip_install.stderr, secrets=config.secrets)}"
        )
    if target not in sys.path:
        sys.path.insert(0, target)
    existing_pythonpath = os.environ.get("PYTHONPATH", "")
    if target not in existing_pythonpath.split(os.pathsep):
        os.environ["PYTHONPATH"] = (
            f"{target}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else target
        )


def _is_wheel_or_path(spec: str) -> bool:
    normalized = str(spec or "").strip()
    return normalized.endswith(".whl") or "/" in normalized or normalized.startswith("dbfs:")


def _localize_offline_bundle(bundle_path: str) -> Path:
    """Copy an offline bundle to local temp storage before opening it as a zip."""
    source = str(bundle_path or "").strip()
    if not source:
        raise RuntimeError("Offline KDB-X install bundle path is empty.")

    local_dir = _local_bundle_directory()
    local_path = local_dir / _bundle_cache_name(source)
    if _path_is_file(local_path):
        return local_path
    # Copy to a temporary name first so an interrupted copy is never reused.
    partial_path = local_path.with_name(f"{local_path.name}.partial")

    try:
        source_path = Path(source)
        if _path_is_file(source_path):
            shutil.copyfile(source_path, partial_path)
            os.replace(partial_path, local_path)
            return local_path
    except Exception as exc:
        logger.debug("Direct copy of KDB-X bundle failed: %s", exc)

    errors = []
    for src_uri in _dbutils_source_uris(source):
        try:
            _copy_with_dbutils(src_uri, f"file:{partial_path}")
            if _path_is_file(partial_path):
                os.replace(partial_path, local_path)
                return local_path
        except Exception as exc:
            errors.append(f"{src_uri}: {exc}")

    raise RuntimeError(
        f"Offline KDB-X install bundle could not be localized from {source!r}. "
        f"source status: {_bundle_path_status((Path(source),))}. "
        f"dbutils attempts: {'; '.join(errors) if errors else 'none'}"
    )


def _bundle_cache_name(source: str) -> str:
    """Name the local copy after the source path and, when visible, its size and mtime."""
    key = source
    try:
        info = os.stat(source)
        key = f"{source}\0{info.st_size}\0{info.st_mtime_ns}"
    except OSError:
        pass
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return f"{digest}-{Path(source).name}"


def _local_bundle_directory() -> Path:
    """Return a process-owned local bundle cache directory."""
    cached = _LOCAL_BUNDLE_DIR_CACHE.get("path")
    if cached is None:
        cached = Path(tempfile.mkdtemp(prefix=f"kdbx-bundles-{os.getpid()}-"))
        cached.mkdir(parents=True, exist_ok=True)
        _LOCAL_BUNDLE_DIR_CACHE["path"] = cached
    return cached


def _path_is_file(path: Path) -> bool:
    try:
        return path.is_file()
    except OSError:
        return False


def _dbutils_source_uris(source: str) -> tuple[str, ...]:
    if source.startswith("dbfs:") or source.startswith("file:"):
        return (source,)
    if source.startswith("/"):
        return (f"file:{source}", source)
    return (source,)


def _copy_with_dbutils(src: str, dst: str) -> None:
    try:
        from databricks.sdk.runtime import (  # pylint: disable=import-error,no-name-in-module
            dbutils as runtime_dbutils,
        )

        runtime_dbutils.fs.cp(src, dst, True)
        return
    except Exception:
        pass

    try:
        from pyspark.dbutils import DBUtils  # pylint: disable=import-error,no-name-in-module
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("dbutils is not available for bundle localization") from exc

    spark = SparkSession.getActiveSession() or getattr(SparkSession, "_instantiatedSession", None)
    if spark is None:
        spark = SparkSession.builder.getOrCreate()  # pylint: disable=no-member
    DBUtils(spark).fs.cp(src, dst, True)


def _resolve_qlic_dir(config: PyKxRuntimeConfig) -> str:
    license_dir = Path(config.license_directory) if config.license_directory else None
    if license_dir and license_dir.is_dir():
        for file_name in _LICENSE_FILE_NAMES:
            if (license_dir / file_name).exists():
                return str(license_dir)

    if config.license_b64:
        return str(_materialize_license(config))

    if license_dir:
        return str(license_dir)
    target = _runtime_home_directory() / "qlic"
    target.mkdir(parents=True, exist_ok=True)
    return str(target)


def _materialize_license(config: PyKxRuntimeConfig) -> Path:
    """Write the decoded license to an owner-only file under the runtime home."""
    target = _runtime_home_directory() / "qlic"
    target.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(target, 0o700)
    license_path = target / config.license_file_name
    content = base64.b64decode(config.license_b64 or "")
    if _owner_only_file_with_content(license_path, content):
        return target
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(license_path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(content)
    os.chmod(license_path, 0o600)
    return target


def _owner_only_file_with_content(path: Path, content: bytes) -> bool:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
        return False
    return info.st_size == len(content) and path.read_bytes() == content


def _apply_pykx_environment(config: PyKxRuntimeConfig) -> None:
    runtime_home = _runtime_home_directory()
    runtime_home.mkdir(parents=True, exist_ok=True)
    target = Path(_resolve_qlic_dir(config))
    os.environ["HOME"] = str(runtime_home)
    os.environ["QLIC"] = str(target)
    os.environ["PYKX_LICENSED"] = "true"
    kx_bin = runtime_home / ".kx" / "bin"
    existing_path = os.environ.get("PATH", "")
    if kx_bin.exists():
        os.environ["PATH"] = f"{kx_bin}:{existing_path}" if existing_path else str(kx_bin)
    if not config.license_b64 and _path_exists(target):
        os.chdir(str(target))


def _path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return False


def _command_tail(value: str, limit: int = 1200, *, secrets: tuple[str, ...] = ()) -> str:
    text = str(value or "")
    for secret in secrets:
        if secret:
            text = text.replace(secret, _REDACTED)
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[-limit:]


def _runtime_home_directory() -> Path:
    """Return a writable scratch directory used as ``HOME`` during install.

    Honors ``KDBX_RUNTIME_HOME`` when set (useful for tests and pipelines that
    want a stable, well-known path). Otherwise creates a unique directory via
    ``tempfile.mkdtemp`` to guarantee writability on locked-down serverless
    runtimes where a static ``/tmp/kdbx-home`` may not be creatable. The
    chosen path is cached for the lifetime of the process.
    """
    cached = _RUNTIME_HOME_CACHE.get("path")
    if cached is not None:
        return cached

    override = os.environ.get(_RUNTIME_HOME_OVERRIDE_ENV, "").strip()
    if override:
        path = Path(override)
        path.mkdir(parents=True, exist_ok=True)
    else:
        path = Path(tempfile.mkdtemp(prefix="kdbx-home-"))

    _RUNTIME_HOME_CACHE["path"] = path
    return path
