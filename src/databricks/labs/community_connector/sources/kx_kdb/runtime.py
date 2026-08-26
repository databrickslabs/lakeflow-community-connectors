"""Shared KDB-X / PyKX runtime bootstrap helpers."""

from __future__ import annotations

import base64
import importlib
import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from databricks.labs.community_connector.sources.kx_kdb.conversion_metrics import (
    _PicklableLock,
)

logger = logging.getLogger(__name__)

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
KDBX_OFFLINE_BUNDLE_PATH_OPTION = "kdbx_offline_bundle_path"
KDBX_INSTALL_MODE_OPTION = "kdbx_install_mode"
PYKX_INSTALL_SPEC_OPTION = "pykx_install_spec"

_RUNTIME_LOCK = _PicklableLock()
_PREPARED_RUNTIME_KEYS: set[tuple[str, int, int]] = set()
_RUNTIME_HOME_CACHE: dict[str, Path] = {}
_LOCAL_BUNDLE_DIR_CACHE: dict[str, Path] = {}
_RUNTIME_HOME_OVERRIDE_ENV = "KDBX_RUNTIME_HOME"
_LICENSE_FILE_NAMES = ("kc.lic", "k4.lic", "kx.lic")


@dataclass(frozen=True)
class PyKxRuntimeConfig:
    """Runtime configuration required to initialize PyKX safely."""

    license_directory: str
    installer_bearer_token: str | None = None
    license_b64: str | None = None
    installer_url: str = DEFAULT_KDBX_INSTALLER_URL
    offline_bundle_path: str | None = None
    pykx_install_spec: str | None = None

    @property
    def uses_installer(self) -> bool:
        if self.uses_offline_bundle:
            return True
        return bool(self.installer_bearer_token and self.license_b64)

    @property
    def uses_offline_bundle(self) -> bool:
        return bool(self.offline_bundle_path and self.license_b64)


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

    direct_install_token = str(options.get(KDBX_INSTALL_BEARER_TOKEN_OPTION, "")).strip()
    direct_license_option = str(options.get(KDBX_LICENSE_B64_OPTION, "")).strip()
    license_file_b64 = ""
    if not direct_license_option:
        license_file_b64 = _read_license_file_b64(
            str(options.get(KDBX_LICENSE_FILE_PATH_OPTION, "")).strip()
        )
    direct_license_b64 = direct_license_option or license_file_b64

    if direct_license_b64 and explicit_bundle_path and install_mode != "online":
        return PyKxRuntimeConfig(
            license_directory=license_directory,
            installer_bearer_token=direct_install_token or None,
            license_b64=direct_license_b64,
            offline_bundle_path=explicit_bundle_path,
            pykx_install_spec=pykx_install_spec,
        )

    direct_supplied = [bool(direct_install_token), bool(direct_license_option)]
    if any(direct_supplied) and not all(direct_supplied):
        raise ValueError(
            "KDB-X bootstrap requires both "
            f"{KDBX_INSTALL_BEARER_TOKEN_OPTION!r} and {KDBX_LICENSE_B64_OPTION!r} "
            "when using direct connection options."
        )
    if all(direct_supplied):
        config = PyKxRuntimeConfig(
            license_directory=license_directory,
            installer_bearer_token=direct_install_token,
            license_b64=direct_license_b64,
            pykx_install_spec=pykx_install_spec,
        )
        return config if install_mode == "online" else _maybe_offline(config, explicit_bundle_path)

    if license_file_b64:
        if install_mode == "online":
            raise ValueError(
                f"{KDBX_INSTALL_MODE_OPTION}='online' requires an installer bearer token "
                "and license b64, not only a license file."
            )
        return _maybe_offline(
            PyKxRuntimeConfig(
                license_directory=license_directory,
                license_b64=license_file_b64,
                pykx_install_spec=pykx_install_spec,
            ),
            explicit_bundle_path,
        )

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
        return PyKxRuntimeConfig(
            license_directory=license_directory,
            pykx_install_spec=pykx_install_spec,
        )

    resolver = secret_resolver or _resolve_databricks_secret
    installer_bearer_token = resolver(secret_scope, install_token_key).strip()
    license_b64 = resolver(secret_scope, license_b64_key).strip()
    if not installer_bearer_token or not license_b64:
        raise ValueError(
            "Resolved empty KDB-X bootstrap secret value. Check the configured "
            "secret scope and secret keys."
        )

    config = PyKxRuntimeConfig(
        license_directory=license_directory,
        installer_bearer_token=installer_bearer_token,
        license_b64=license_b64,
        pykx_install_spec=pykx_install_spec,
    )
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


def _read_license_file_b64(path: str) -> str:
    if not path:
        return ""
    candidate = Path(path)
    if candidate.is_dir():
        for file_name in _LICENSE_FILE_NAMES:
            license_file = candidate / file_name
            if license_file.is_file():
                return base64.b64encode(license_file.read_bytes()).decode("ascii")
        return ""
    if candidate.is_file():
        return base64.b64encode(candidate.read_bytes()).decode("ascii")
    return ""


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
    return PyKxRuntimeConfig(
        license_directory=config.license_directory,
        installer_bearer_token=config.installer_bearer_token,
        license_b64=config.license_b64,
        offline_bundle_path=bundle_path,
        pykx_install_spec=config.pykx_install_spec,
    )


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


def prepare_pykx(config: PyKxRuntimeConfig):
    """Install/configure KDB-X and import PyKX after license setup."""
    runtime_key = (
        config.license_directory,
        len(config.installer_bearer_token or ""),
        len(config.license_b64 or ""),
    )

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


def _install_kdbx_online(config: PyKxRuntimeConfig) -> None:
    runtime_home = _runtime_home_directory()
    runtime_home.mkdir(parents=True, exist_ok=True)
    # The KDB-X installer auto-selects "$HOME/.kx" as the install location and
    # aborts in non-interactive mode if it cannot create that directory. On
    # serverless executors with read-only or restricted /tmp, pre-creating it
    # here avoids the "Cannot create directory" failure surfaced from
    # install_kdb.sh.
    (runtime_home / ".kx").mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        installer_path = Path(tmpdir) / "install_kdb.sh"
        download = subprocess.run(
            [
                "curl",
                "-sSLO",
                "--fail-with-body",
                "--oauth2-bearer",
                config.installer_bearer_token or "",
                config.installer_url,
            ],
            cwd=tmpdir,
            capture_output=True,
            text=True,
            timeout=180,
        )
        if download.returncode != 0:
            raise RuntimeError(
                "KDB-X installer download failed with rc="
                f"{download.returncode}: {_command_tail(download.stderr)}"
            )

        install_env = {
            **os.environ,
            "HOME": str(runtime_home),
            # Override any inherited TERM (e.g. "unknown" on serverless DLT
            # analyzers) so the installer's tput calls don't blow up.
            "TERM": "dumb",
        }
        install = subprocess.run(
            [
                "bash",
                str(installer_path),
                "-y",
                "--b64lic",
                config.license_b64 or "",
            ],
            cwd=tmpdir,
            env=install_env,
            capture_output=True,
            text=True,
            timeout=1200,
        )
        if install.returncode != 0:
            raise RuntimeError(
                "install_kdb.sh failed with rc="
                f"{install.returncode}. stdout tail: {_command_tail(install.stdout)}. "
                f"stderr tail: {_command_tail(install.stderr)}"
            )


def _install_kdbx_offline(config: PyKxRuntimeConfig) -> None:
    """Install KDB-X from a pre-staged offline bundle (no network access)."""
    runtime_home = _runtime_home_directory()
    runtime_home.mkdir(parents=True, exist_ok=True)
    (runtime_home / ".kx").mkdir(parents=True, exist_ok=True)

    bundle_path = _localize_offline_bundle(config.offline_bundle_path or "")

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            with zipfile.ZipFile(bundle_path) as zf:
                zf.extractall(tmpdir)
        except zipfile.BadZipFile as exc:
            raise RuntimeError(
                f"Offline KDB-X bundle at {str(bundle_path)!r} is not a valid zip."
            ) from exc

        installer_path = Path(tmpdir) / "install_kdb.sh"
        if not installer_path.is_file():
            raise RuntimeError(
                f"Offline KDB-X bundle at {str(bundle_path)!r} does not contain "
                "install_kdb.sh."
            )
        installer_path.chmod(0o755)

        install_env = {
            **os.environ,
            "HOME": str(runtime_home),
            "TERM": "dumb",
        }
        install = subprocess.run(
            [
                "bash",
                str(installer_path),
                "--offline",
                # Without `-y` the script falls into interactive mode and hangs
                # on its first prompt, eventually triggering ``TimeoutExpired``.
                "-y",
                "--b64lic",
                config.license_b64 or "",
            ],
            cwd=tmpdir,
            env=install_env,
            capture_output=True,
            text=True,
            timeout=1200,
        )
        if install.returncode != 0:
            raise RuntimeError(
                "install_kdb.sh --offline failed with rc="
                f"{install.returncode}. stdout tail: {_command_tail(install.stdout)}. "
                f"stderr tail: {_command_tail(install.stderr)}"
            )


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
    pip_env = dict(os.environ)
    # Spark workers inherit a PYTHONPATH containing JAR paths. Pip scans every
    # entry as a possible distribution and can fail with PermissionError on
    # protected Databricks JARs. The isolated interpreter and clean path keep
    # the fallback install confined to ``target``.
    pip_env.pop("PYTHONPATH", None)
    pip_env.pop("PYTHONHOME", None)
    pip_install = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=1200,
        env=pip_env,
    )
    if pip_install.returncode != 0:
        raise RuntimeError(
            f"Failed to install PyKX package {spec!r}. stdout tail: "
            f"{_command_tail(pip_install.stdout)}. stderr tail: "
            f"{_command_tail(pip_install.stderr)}"
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
    local_path = local_dir / Path(source).name
    if _path_is_file(local_path):
        return local_path

    try:
        source_path = Path(source)
        if _path_is_file(source_path):
            shutil.copyfile(source_path, local_path)
            return local_path
    except Exception as exc:
        logger.debug("Direct copy of KDB-X bundle failed: %s", exc)

    errors = []
    for src_uri in _dbutils_source_uris(source):
        try:
            _copy_with_dbutils(src_uri, f"file:{local_path}")
            if _path_is_file(local_path):
                return local_path
        except Exception as exc:
            errors.append(f"{src_uri}: {exc}")

    raise RuntimeError(
        f"Offline KDB-X install bundle could not be localized from {source!r}. "
        f"source status: {_bundle_path_status((Path(source),))}. "
        f"dbutils attempts: {'; '.join(errors) if errors else 'none'}"
    )


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
        target = _runtime_home_directory() / "qlic"
        target.mkdir(parents=True, exist_ok=True)
        (target / "kc.lic").write_bytes(base64.b64decode(config.license_b64))
        return str(target)

    if license_dir:
        return str(license_dir)
    target = _runtime_home_directory() / "qlic"
    target.mkdir(parents=True, exist_ok=True)
    return str(target)


def _apply_pykx_environment(config: PyKxRuntimeConfig) -> None:
    runtime_home = _runtime_home_directory()
    runtime_home.mkdir(parents=True, exist_ok=True)
    target = Path(_resolve_qlic_dir(config))
    os.environ["HOME"] = str(runtime_home)
    os.environ["QLIC"] = str(target)
    os.environ["PYKX_LICENSED"] = "true"
    if config.license_b64:
        os.environ["KDB_LICENSE_B64"] = config.license_b64
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


def _command_tail(value: str, limit: int = 1200) -> str:
    text = str(value or "").strip()
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
