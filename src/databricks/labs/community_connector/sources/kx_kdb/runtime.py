"""KDB-X / PyKX runtime setup from files in the connection's license Volume."""

from __future__ import annotations

import base64
import contextlib
import dataclasses
import fcntl
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
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from databricks.labs.community_connector.sources.kx_kdb.process_local import (
    ProcessLocalDict,
    ProcessLocalLock,
    ProcessLocalSet,
)

logger = logging.getLogger(__name__)

PYKX_REQUIREMENT = "pykx==4.0.0b5"
DEFAULT_OFFLINE_BUNDLE_NAME = "l64-bundle.zip"
ARM_OFFLINE_BUNDLE_NAME = "l64arm-bundle.zip"

KDBX_LICENSE_FILE_PATH_OPTION = "kdbx_license_file_path"
KDBX_LICENSE_KIND_OPTION = "kdbx_license_kind"
KDBX_OFFLINE_BUNDLE_PATH_OPTION = "kdbx_offline_bundle_path"
KDBX_OFFLINE_BUNDLE_SHA256_OPTION = "kdbx_offline_bundle_sha256"
# Options of the removed online installer and secret-scope paths. A pipeline
# could fill them when the connection leaves them unset, so they are rejected
# instead of ignored.
REMOVED_BOOTSTRAP_OPTIONS = (
    "kdbx_install_mode",
    "kdbx_install_bearer_token",
    "kdbx_license_b64",
    "kdbx_secret_scope",
    "kdbx_install_bearer_secret_key",
    "kdbx_license_b64_secret_key",
    "pykx_install_spec",
)

_RUNTIME_LOCK = ProcessLocalLock()
_PREPARED_RUNTIME_KEYS: set[str] = ProcessLocalSet()
_RUNTIME_HOMES: dict[str, Path] = ProcessLocalDict()
_RUNTIME_ROOT_ENV = "KDBX_RUNTIME_ROOT"
_INSTALL_MARKER = ".kdbx-installed"
_PYKX_MARKER = ".pykx-installed"
_INSTALL_WORKDIR = ".install-work"
_SUBPROCESS_TIMEOUT_SECONDS = 1200
# Longer than one install, so a waiting process fails only if the installing
# process is stuck outside its own subprocess timeout.
_NODE_LOCK_TIMEOUT_SECONDS = _SUBPROCESS_TIMEOUT_SECONDS + 300
_LICENSE_ENV_VARS = frozenset({"KDB_LICENSE_B64", "KDB_K4LICENSE_B64"})
_LICENSE_FILE_NAMES = ("kc.lic", "k4.lic", "kx.lic")
_LICENSE_KIND_FILE_NAMES = {"kc": "kc.lic", "k4": "k4.lic"}
# install_kdb.sh writes kc.lic for --b64lic and k4.lic for --k4b64lic.
_INSTALLER_LICENSE_FLAGS = {"kc.lic": "--b64lic", "k4.lic": "--k4b64lic"}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REDACTED = "[REDACTED]"
# install_kdb.sh is third-party code; it gets only what it needs.
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
    """Runtime configuration required to initialize PyKX safely.

    ``offline_bundle_identity`` is the bundle's ``(size, mtime_ns)`` seen on
    the driver, so every worker installs the same file.
    """

    license_directory: str
    license_b64: str | None = dataclasses.field(default=None, repr=False)
    license_file_name: str = "kc.lic"
    offline_bundle_path: str | None = None
    offline_bundle_identity: tuple[int, int] | None = None
    offline_bundle_sha256: str | None = None

    @property
    def uses_offline_bundle(self) -> bool:
        return bool(self.offline_bundle_path and self.license_b64)

    @property
    def secrets(self) -> tuple[str, ...]:
        return (self.license_b64,) if self.license_b64 else ()


def normalize_license_directory(value: str) -> str:
    """Normalize a file-or-directory license path to the containing directory."""
    candidate = Path(str(value).strip())
    normalized = candidate if candidate.suffix == "" else candidate.parent
    return str(normalized)


def build_runtime_config(options: dict[str, str]) -> PyKxRuntimeConfig:
    """Build the runtime config from connector options.

    KDB-X is installed only from an offline bundle inside the Unity Catalog
    Volume of ``license_volume_path``: ``kdbx_offline_bundle_path``, or a
    bundle found next to the license directory. The license is read from
    ``kdbx_license_file_path`` or the license directory in that Volume. With a
    bundle, a process that cannot import PyKX installs the pinned
    ``PYKX_REQUIREMENT`` once per node. Without a bundle, PyKX and KDB-X must
    already be in the pipeline environment, and ``QLIC`` points at the
    license directory.
    """
    present = [key for key in REMOVED_BOOTSTRAP_OPTIONS if key in options]
    if present:
        raise ValueError(
            f"KX KDB options {present} are no longer supported. KDB-X is installed "
            "only from an offline bundle in the license_volume_path Volume, or must "
            "already be in the pipeline environment."
        )

    license_directory = normalize_license_directory(str(options.get("license_volume_path", "")))
    license_kind = _license_kind(options)
    license_path = _within_license_volume(
        str(options.get(KDBX_LICENSE_FILE_PATH_OPTION, "")).strip(),
        license_directory,
        KDBX_LICENSE_FILE_PATH_OPTION,
    )
    bundle_path = _within_license_volume(
        str(options.get(KDBX_OFFLINE_BUNDLE_PATH_OPTION, "")).strip(),
        license_directory,
        KDBX_OFFLINE_BUNDLE_PATH_OPTION,
    ) or _probe_offline_bundle(license_directory)
    expected_sha256 = _sha256_option(options)
    if expected_sha256 and not bundle_path:
        raise ValueError(
            f"{KDBX_OFFLINE_BUNDLE_SHA256_OPTION} is set, but no offline KDB-X bundle "
            "was found in the license_volume_path Volume."
        )

    license_b64, detected_license_name = "", None
    if license_path or bundle_path:
        license_source = license_path or license_directory
        license_b64, detected_license_name = _read_license_file(license_source)
        if not license_b64:
            raise ValueError(
                f"No KX license file (kc.lic or k4.lic) was found at {license_source!r}."
            )

    return PyKxRuntimeConfig(
        license_directory=license_directory,
        license_b64=license_b64 or None,
        license_file_name=_license_file_name(license_kind, detected_license_name),
        offline_bundle_path=bundle_path or None,
        offline_bundle_identity=_file_identity(bundle_path) if bundle_path else None,
        offline_bundle_sha256=expected_sha256,
    )


def _within_license_volume(path: str, license_directory: str, option: str) -> str:
    """Require a bootstrap file to sit in the license directory's UC Volume.

    Pipeline table options can supply keys that the connection leaves unset,
    so bootstrap files are limited to the Volume the connection owner chose
    for ``license_volume_path``.
    """
    if not path:
        return path
    root = _license_volume_root(license_directory)
    if root is None or not _volume_contains(root, path):
        raise ValueError(
            f"{option} must be inside {root or 'the license_volume_path Volume'}, the "
            "Unity Catalog Volume of license_volume_path."
        )
    return path


def _volume_contains(root: PurePosixPath, path: str) -> bool:
    candidate = PurePosixPath(_fuse_path(path))
    return (
        candidate.is_absolute()
        and ".." not in candidate.parts
        and (candidate == root or root in candidate.parents)
    )


def _fuse_path(path: str) -> str:
    return str(path).removeprefix("dbfs:")


def _license_volume_root(license_directory: str) -> PurePosixPath | None:
    if not license_directory or license_directory == ".":
        return None
    directory = PurePosixPath(license_directory)
    if len(directory.parts) >= 5 and directory.parts[1] == "Volumes":
        return PurePosixPath(*directory.parts[:5])
    return directory.parent


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
    candidate = Path(_fuse_path(path))
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


def _sha256_option(options: dict[str, str]) -> str | None:
    value = str(options.get(KDBX_OFFLINE_BUNDLE_SHA256_OPTION, "")).strip().lower()
    if not value:
        return None
    if not _SHA256_RE.fullmatch(value):
        raise ValueError(
            f"{KDBX_OFFLINE_BUNDLE_SHA256_OPTION} must be a SHA-256 digest of 64 "
            "hexadecimal characters."
        )
    return value


def _file_identity(path: str) -> tuple[int, int] | None:
    try:
        info = os.stat(_fuse_path(path))
    except OSError:
        return None
    return info.st_size, info.st_mtime_ns


def _probe_offline_bundle(license_directory: str) -> str:
    """Look for a staged KDB-X bundle next to the license directory.

    Probed in order, keeping only paths inside the license Volume:
      - ``<license_directory>/<bundle>``
      - ``<license_directory>/../kdbx/<bundle>``
    where ``<bundle>`` is the ARM64 bundle first on ARM64 hosts. Returns the
    first existing file path, or an empty string.
    """
    root = _license_volume_root(license_directory)
    if root is None:
        return ""
    for candidate in _offline_bundle_candidates(Path(license_directory)):
        if not _volume_contains(root, str(candidate)):
            continue
        try:
            if candidate.is_file():
                return str(candidate)
        except OSError:
            continue
    return ""


def _offline_bundle_candidates(base: Path) -> tuple[Path, ...]:
    return tuple(
        candidate
        for bundle_name in _offline_bundle_names_for_platform()
        for candidate in (
            base / bundle_name,
            base.parent / "kdbx" / bundle_name,
        )
    )


def _offline_bundle_names_for_platform() -> tuple[str, ...]:
    machine = platform.machine().lower()
    if machine in {"aarch64", "arm64"}:
        return (ARM_OFFLINE_BUNDLE_NAME, DEFAULT_OFFLINE_BUNDLE_NAME)
    return (DEFAULT_OFFLINE_BUNDLE_NAME,)


def _runtime_key(config: PyKxRuntimeConfig) -> str:
    """Return a digest identifying one runtime configuration without keeping secrets."""
    digest = hashlib.sha256()
    for value in (
        config.license_directory,
        config.license_b64 or "",
        config.license_file_name,
        config.offline_bundle_path or "",
        repr(config.offline_bundle_identity),
        config.offline_bundle_sha256 or "",
    ):
        digest.update(value.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def prepare_pykx(config: PyKxRuntimeConfig):
    """Install KDB-X if configured, set the license environment, and import PyKX."""
    runtime_key = _runtime_key(config)
    with _RUNTIME_LOCK:
        if runtime_key not in _PREPARED_RUNTIME_KEYS:
            home = (
                _runtime_home(config) if config.uses_offline_bundle or config.license_b64 else None
            )
            if config.uses_offline_bundle:
                _install_once(config, home)
            _apply_pykx_environment(config, home)
            _PREPARED_RUNTIME_KEYS.add(runtime_key)
        return _import_pykx(config)


def _import_pykx(config: PyKxRuntimeConfig):
    if "pykx" in sys.modules:
        return sys.modules["pykx"]
    importlib.invalidate_caches()
    try:
        return importlib.import_module("pykx")
    except ModuleNotFoundError as exc:
        if exc.name != "pykx":
            raise
        if not config.uses_offline_bundle:
            raise RuntimeError(
                f"PyKX is not installed. Add {PYKX_REQUIREMENT!r} to the pipeline "
                "environment dependencies, or stage a KDB-X offline bundle."
            ) from exc
    # Managed-ingestion analysis workers do not receive the pipeline
    # environment, so the pinned PyKX is installed once per node instead.
    _ensure_pykx_package(config, _runtime_home(config))
    importlib.invalidate_caches()
    return importlib.import_module("pykx")


def _ensure_pykx_package(config: PyKxRuntimeConfig, home: Path) -> None:
    """Install the pinned PyKX into ``home`` unless a process on the node already did."""
    target = home / "pykx_pkgs"
    marker = home / _PYKX_MARKER
    with _node_lock(home):
        if marker.is_file():
            logger.info("PYKX_INSTALL event=reuse home=%s", home)
        else:
            if target.exists():
                # Left by an install that did not finish.
                shutil.rmtree(target)
            started = time.perf_counter()
            _run_pip_install(config, target)
            marker.write_text("installed\n", encoding="utf-8")
            logger.info(
                "PYKX_INSTALL event=install home=%s duration_ms=%s",
                home,
                round((time.perf_counter() - started) * 1000),
            )
    # Appended, so the runtime's own numpy and pandas keep precedence over the
    # copies pip resolves for PyKX.
    if str(target) not in sys.path:
        sys.path.append(str(target))


def _run_pip_install(config: PyKxRuntimeConfig, target: Path) -> None:
    command = [
        sys.executable,
        "-I",
        "-m",
        "pip",
        "install",
        "--quiet",
        "--ignore-installed",
        "--target",
        str(target),
        "--pre",
        PYKX_REQUIREMENT,
    ]
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in _LICENSE_ENV_VARS and value not in config.secrets
    }
    # Spark workers inherit a PYTHONPATH of JAR paths. Pip scans every entry
    # as a possible distribution and can fail with PermissionError on
    # protected Databricks JARs.
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    label = f"PyKX package install {PYKX_REQUIREMENT!r}"
    result = _run_subprocess(
        command, config=config, label=label, timeout=_SUBPROCESS_TIMEOUT_SECONDS, env=env
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to install PyKX package {PYKX_REQUIREMENT!r}. stdout tail: "
            f"{_command_tail(result.stdout, secrets=config.secrets)}. stderr tail: "
            f"{_command_tail(result.stderr, secrets=config.secrets)}"
        )


def _runtime_home(config: PyKxRuntimeConfig) -> Path:
    """Return the directory that holds this configuration's KDB-X install.

    The directory is shared by every Python process of the same user on a node,
    so KDB-X is installed once per node. It is used only when it is an
    owner-only directory that is not a symbolic link; otherwise a private
    temporary directory is used.
    """
    runtime_key = _runtime_key(config)
    cached = _RUNTIME_HOMES.get(runtime_key)
    if cached is not None:
        return cached
    root = Path(os.environ.get(_RUNTIME_ROOT_ENV, "").strip() or tempfile.gettempdir())
    home = _owned_directory(root / f"kdbx-{os.getuid()}-{runtime_key[:16]}")
    if home is None:
        home = Path(tempfile.mkdtemp(prefix="kdbx-home-"))
        logger.warning(
            "KDB_X_INSTALL event=private_home reason=shared directory is not owner-only home=%s",
            home,
        )
    _RUNTIME_HOMES[runtime_key] = home
    return home


def _owned_directory(path: Path) -> Path | None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(FileExistsError):
            path.mkdir(mode=0o700)
        info = os.lstat(path)
    except OSError:
        return None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        return None
    return path


@contextlib.contextmanager
def _node_lock(home: Path, timeout: float = _NODE_LOCK_TIMEOUT_SECONDS):
    descriptor = os.open(
        home / ".install.lock",
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Timed out after {timeout:.0f}s waiting for another process on "
                        f"this node to finish installing into {home}."
                    ) from None
                time.sleep(1)
        yield
    finally:
        os.close(descriptor)


def _install_once(config: PyKxRuntimeConfig, home: Path) -> None:
    """Install KDB-X into ``home`` unless another process on the node already did."""
    marker = home / _INSTALL_MARKER
    with _node_lock(home):
        if marker.is_file():
            logger.info("KDB_X_INSTALL event=reuse home=%s", home)
            return
        kx_dir = home / ".kx"
        if kx_dir.exists():
            # Left by an install that did not finish.
            shutil.rmtree(kx_dir)
        # The installer picks "$HOME/.kx" and aborts in non-interactive mode
        # if it cannot create that directory.
        kx_dir.mkdir(mode=0o700)
        started = time.perf_counter()
        _install_kdbx_offline(config, home)
        marker.write_text("installed\n", encoding="utf-8")
        logger.info(
            "KDB_X_INSTALL event=install home=%s duration_ms=%s",
            home,
            round((time.perf_counter() - started) * 1000),
        )


def _install_kdbx_offline(config: PyKxRuntimeConfig, home: Path) -> None:
    """Install KDB-X from the staged offline bundle (no network access)."""
    workdir = home / _INSTALL_WORKDIR
    if workdir.exists():
        # Left by an install whose process was killed.
        shutil.rmtree(workdir)
    workdir.mkdir(mode=0o700)
    try:
        bundle_path = _localize_offline_bundle(config, workdir)
        extract_dir = workdir / "bundle"
        extract_dir.mkdir()
        _extract_offline_bundle(bundle_path, extract_dir)

        installer_path = extract_dir / "install_kdb.sh"
        if not installer_path.is_file():
            raise RuntimeError(
                f"Offline KDB-X bundle at {config.offline_bundle_path!r} does not contain "
                "install_kdb.sh."
            )
        installer_path.chmod(0o755)
        _run_installer(config, installer_path, home, cwd=str(extract_dir))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def _run_installer(
    config: PyKxRuntimeConfig,
    installer_path: Path,
    home: Path,
    *,
    cwd: str,
) -> None:
    # Without `-y` the script falls into interactive mode and hangs on its
    # first prompt. install_kdb.sh accepts the license only as an argument.
    args = ["bash", str(installer_path), "--offline", "-y", *_installer_license_args(config)]
    label = "install_kdb.sh --offline"
    install = _run_subprocess(
        args,
        config=config,
        label=label,
        timeout=_SUBPROCESS_TIMEOUT_SECONDS,
        cwd=cwd,
        env=_installer_env(home),
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
    # install_kdb.sh prompts for missing optional bundle components even
    # with -y; an inherited open stdin would block it until the timeout.
    kwargs.setdefault("stdin", subprocess.DEVNULL)
    try:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            check=False,
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


def _installer_env(home: Path) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key in _INSTALLER_ENV_ALLOWLIST}
    # TERM overrides values such as "unknown" that break tput in the script.
    env.update({"HOME": str(home), "TERM": "dumb"})
    return env


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


def _localize_offline_bundle(config: PyKxRuntimeConfig, target_dir: Path) -> Path:
    """Copy the offline bundle to local storage and check it is the pinned file."""
    source = str(config.offline_bundle_path or "").strip()
    if not source:
        raise RuntimeError("Offline KDB-X install bundle path is empty.")
    source_path = Path(_fuse_path(source))
    _check_source_identity(config, source_path)
    local_path = target_dir / source_path.name

    errors = []
    try:
        if _path_is_file(source_path):
            shutil.copyfile(source_path, local_path)
    except OSError as exc:
        errors.append(f"direct copy: {type(exc).__name__}: {exc}")
    if not _path_is_file(local_path):
        for src_uri in _dbutils_source_uris(source):
            try:
                _copy_with_dbutils(src_uri, f"file:{local_path}")
            except Exception as exc:
                errors.append(f"{src_uri}: {exc}")
                continue
            if _path_is_file(local_path):
                break
    if not _path_is_file(local_path):
        raise RuntimeError(
            f"Offline KDB-X install bundle could not be localized from {source!r}. "
            f"source status: {_bundle_path_status(source_path)}. "
            f"attempts: {'; '.join(errors) if errors else 'none'}"
        )
    _verify_local_bundle(config, local_path)
    return local_path


def _check_source_identity(config: PyKxRuntimeConfig, source_path: Path) -> None:
    expected = config.offline_bundle_identity
    if expected is None:
        return
    try:
        info = os.stat(source_path)
    except OSError:
        return
    if (info.st_size, info.st_mtime_ns) != tuple(expected):
        raise RuntimeError(
            f"Offline KDB-X bundle {config.offline_bundle_path!r} changed after this "
            "pipeline update started; restart the update so every worker installs "
            "the same bundle."
        )


def _verify_local_bundle(config: PyKxRuntimeConfig, local_path: Path) -> None:
    expected = config.offline_bundle_identity
    size = local_path.stat().st_size
    if expected is not None and size != expected[0]:
        raise RuntimeError(
            f"Offline KDB-X bundle copy has {size} bytes, but the bundle had "
            f"{expected[0]} bytes when the pipeline update started."
        )
    if config.offline_bundle_sha256:
        digest = hashlib.sha256()
        with local_path.open("rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
        if digest.hexdigest() != config.offline_bundle_sha256:
            raise RuntimeError(
                f"Offline KDB-X bundle {config.offline_bundle_path!r} does not match "
                f"{KDBX_OFFLINE_BUNDLE_SHA256_OPTION}."
            )


def _bundle_path_status(path: Path) -> str:
    try:
        return f"{path}: exists={path.exists()} is_file={path.is_file()}"
    except OSError as exc:
        return f"{path}: error={type(exc).__name__}: {exc}"


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
    _dbutils().fs.cp(src, dst, True)


def _dbutils():
    """Return ``dbutils`` from the Databricks runtime or the current Spark session."""
    try:
        from databricks.sdk.runtime import (  # pylint: disable=import-error,no-name-in-module
            dbutils as runtime_dbutils,
        )

        return runtime_dbutils
    except Exception:
        pass

    try:
        from pyspark.dbutils import DBUtils  # pylint: disable=import-error,no-name-in-module
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("dbutils is not available in this Python process.") from exc

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
            raise RuntimeError("A SparkSession is required to use dbutils.") from exc
    if spark is None:
        raise RuntimeError("A SparkSession is required to use dbutils.")
    return DBUtils(spark)


def _apply_pykx_environment(config: PyKxRuntimeConfig, home: Path | None) -> None:
    """Set only the variables PyKX reads at import: QLIC, QHOME, and PYKX_*."""
    os.environ["QLIC"] = _resolve_qlic_dir(config, home)
    os.environ["PYKX_LICENSED"] = "true"
    if home is not None and config.uses_offline_bundle:
        kx_home = home / ".kx"
        os.environ["QHOME"] = str(kx_home)
        _prepend_path(kx_home / "bin")


def _prepend_path(directory: Path) -> None:
    if not directory.is_dir():
        return
    current = os.environ.get("PATH", "")
    entries = current.split(os.pathsep) if current else []
    if str(directory) in entries:
        return
    os.environ["PATH"] = os.pathsep.join([str(directory), *entries])


def _resolve_qlic_dir(config: PyKxRuntimeConfig, home: Path | None) -> str:
    if config.license_b64 and home is not None:
        return str(_materialize_license(config, home))
    return config.license_directory or "."


def _materialize_license(config: PyKxRuntimeConfig, home: Path) -> Path:
    """Write the decoded license to an owner-only file under the runtime home."""
    target = home / "qlic"
    target.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(target, 0o700)
    license_path = target / config.license_file_name
    content = base64.b64decode(config.license_b64 or "")
    if _owner_only_file_with_content(license_path, content):
        return target
    # Other processes on the node may be importing PyKX with this license, so
    # the file is replaced atomically instead of truncated in place.
    staged = target / f".{config.license_file_name}.{os.getpid()}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(staged, flags, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(content)
    os.chmod(staged, 0o600)
    os.replace(staged, license_path)
    return target


def _owner_only_file_with_content(path: Path, content: bytes) -> bool:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
        return False
    return info.st_size == len(content) and path.read_bytes() == content


def _command_tail(value: str, limit: int = 1200, *, secrets: tuple[str, ...] = ()) -> str:
    text = str(value or "")
    for secret in secrets:
        if secret:
            text = text.replace(secret, _REDACTED)
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[-limit:]
