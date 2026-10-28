"""Tests for KDB-X / PyKX runtime setup."""

from __future__ import annotations

import base64
import hashlib
import os
import stat
import subprocess
import sys
import types
import zipfile
from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.kx_kdb import runtime as runtime_mod
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    ARM_OFFLINE_BUNDLE_NAME,
    DEFAULT_OFFLINE_BUNDLE_NAME,
    KDBX_LICENSE_FILE_PATH_OPTION,
    KDBX_OFFLINE_BUNDLE_PATH_OPTION,
    KDBX_OFFLINE_BUNDLE_SHA256_OPTION,
    PYKX_REQUIREMENT,
    REMOVED_BOOTSTRAP_OPTIONS,
    PyKxRuntimeConfig,
    build_runtime_config,
    normalize_license_directory,
)

_LICENSE = b"license-bytes"
_LICENSE_B64 = base64.b64encode(_LICENSE).decode("ascii")

# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_runtime_state(monkeypatch, tmp_path):
    """Keep module caches and process environment from leaking between tests."""
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOMES.clear()
    monkeypatch.setenv(runtime_mod._RUNTIME_ROOT_ENV, str(tmp_path / "runtime-root"))
    for name in ("HOME", "QLIC", "QHOME", "PYKX_LICENSED", "PATH"):
        if name in os.environ:
            monkeypatch.setenv(name, os.environ[name])
        else:
            monkeypatch.delenv(name, raising=False)
    yield
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOMES.clear()


class _SubprocessRecorder:
    """Drop-in replacement for ``subprocess.run`` that records and replays."""

    def __init__(self, results):
        self._results = list(results)
        self.calls: list[dict] = []

    def __call__(self, args, **kwargs):
        rc, stdout, stderr = self._results.pop(0)
        self.calls.append({"args": list(args), **kwargs})
        return subprocess.CompletedProcess(args, rc, stdout=stdout, stderr=stderr)


def _volume(tmp_path) -> Path:
    """Return a license directory whose Volume root is ``tmp_path / 'vol'``."""
    keys = tmp_path / "vol" / "keys"
    keys.mkdir(parents=True, exist_ok=True)
    return keys


def _make_bundle(path: Path, *, with_installer: bool = True) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        if with_installer:
            archive.writestr("install_kdb.sh", "#!/bin/bash\necho stub\n")
        archive.writestr("notes.txt", "bundle stub")
    return path


def _offline_config(tmp_path, **overrides) -> PyKxRuntimeConfig:
    bundle = _make_bundle(tmp_path / "vol" / "kdbx" / DEFAULT_OFFLINE_BUNDLE_NAME)
    info = bundle.stat()
    values = {
        "license_directory": str(_volume(tmp_path)),
        "license_b64": _LICENSE_B64,
        "offline_bundle_path": str(bundle),
        "offline_bundle_identity": (info.st_size, info.st_mtime_ns),
    }
    values.update(overrides)
    return PyKxRuntimeConfig(**values)


# ---------------------------------------------------------------------------
# build_runtime_config
# ---------------------------------------------------------------------------


def test_normalize_license_directory_accepts_folder_or_file_path():
    assert normalize_license_directory("/Volumes/main/rkh/rbc_kx/key_kdbx/") == (
        "/Volumes/main/rkh/rbc_kx/key_kdbx"
    )
    assert normalize_license_directory("/Volumes/main/rkh/rbc_kx/key_kdbx/kc.lic") == (
        "/Volumes/main/rkh/rbc_kx/key_kdbx"
    )


def test_without_a_bundle_the_runtime_must_be_preinstalled():
    config = build_runtime_config({"license_volume_path": "/Volumes/main/rkh/rbc_kx/key_kdbx/"})

    assert config.license_directory == "/Volumes/main/rkh/rbc_kx/key_kdbx"
    assert config.uses_offline_bundle is False
    assert config.license_b64 is None
    assert config.offline_bundle_path is None


@pytest.mark.parametrize("option", REMOVED_BOOTSTRAP_OPTIONS)
def test_removed_bootstrap_options_are_rejected_without_echoing_values(option):
    with pytest.raises(ValueError, match="no longer supported") as excinfo:
        build_runtime_config(
            {"license_volume_path": "/Volumes/main/kx/files/keys", option: "SENTINEL-VALUE"}
        )

    assert option in str(excinfo.value)
    assert "SENTINEL-VALUE" not in str(excinfo.value)


def test_explicit_license_file_is_read_without_a_bundle(tmp_path):
    keys = _volume(tmp_path)
    (keys / "kc.lic").write_bytes(_LICENSE)

    config = build_runtime_config(
        {
            "license_volume_path": str(keys),
            KDBX_LICENSE_FILE_PATH_OPTION: str(keys / "kc.lic"),
        }
    )

    assert config.license_b64 == _LICENSE_B64
    assert config.uses_offline_bundle is False


def test_missing_explicit_license_file_is_an_error(tmp_path):
    keys = _volume(tmp_path)

    with pytest.raises(ValueError, match="No KX license file"):
        build_runtime_config(
            {
                "license_volume_path": str(keys),
                KDBX_LICENSE_FILE_PATH_OPTION: str(keys / "kc.lic"),
            }
        )


def test_explicit_bundle_pins_its_identity_and_reads_the_license_directory(tmp_path):
    keys = _volume(tmp_path)
    (keys / "kc.lic").write_bytes(_LICENSE)
    bundle = _make_bundle(tmp_path / "vol" / "bundles" / "custom.zip")

    config = build_runtime_config(
        {
            "license_volume_path": str(keys),
            KDBX_OFFLINE_BUNDLE_PATH_OPTION: str(bundle),
        }
    )

    assert config.uses_offline_bundle is True
    assert config.offline_bundle_path == str(bundle)
    assert config.offline_bundle_identity == (bundle.stat().st_size, bundle.stat().st_mtime_ns)
    assert config.license_b64 == _LICENSE_B64
    assert config.license_file_name == "kc.lic"


def test_bundle_without_a_license_is_an_error(tmp_path):
    keys = _volume(tmp_path)
    _make_bundle(keys / DEFAULT_OFFLINE_BUNDLE_NAME)

    with pytest.raises(ValueError, match="No KX license file"):
        build_runtime_config({"license_volume_path": str(keys)})


def test_bundle_is_probed_next_to_the_license_directory(tmp_path):
    keys = _volume(tmp_path)
    (keys / "kc.lic").write_bytes(_LICENSE)
    bundle = _make_bundle(keys / DEFAULT_OFFLINE_BUNDLE_NAME)

    config = build_runtime_config({"license_volume_path": str(keys)})

    assert config.offline_bundle_path == str(bundle)


def test_bundle_is_probed_in_the_sibling_kdbx_directory(tmp_path):
    keys = _volume(tmp_path)
    (keys / "kc.lic").write_bytes(_LICENSE)
    bundle = _make_bundle(tmp_path / "vol" / "kdbx" / DEFAULT_OFFLINE_BUNDLE_NAME)

    config = build_runtime_config({"license_volume_path": str(keys)})

    assert config.offline_bundle_path == str(bundle)


def test_bundle_probe_stays_inside_the_license_volume():
    candidates = runtime_mod._offline_bundle_candidates(Path("/Volumes/main/kx/files"))
    root = runtime_mod._license_volume_root("/Volumes/main/kx/files")

    inside = [str(path) for path in candidates if runtime_mod._volume_contains(root, str(path))]

    assert inside and all(path.startswith("/Volumes/main/kx/files/") for path in inside)
    assert any(str(path).startswith("/Volumes/main/kx/kdbx/") for path in candidates)


def test_probe_prefers_the_arm_bundle_on_arm64(monkeypatch, tmp_path):
    keys = _volume(tmp_path)
    _make_bundle(tmp_path / "vol" / "kdbx" / DEFAULT_OFFLINE_BUNDLE_NAME)
    arm_bundle = _make_bundle(tmp_path / "vol" / "kdbx" / ARM_OFFLINE_BUNDLE_NAME)
    monkeypatch.setattr(runtime_mod.platform, "machine", lambda: "aarch64")

    assert runtime_mod._probe_offline_bundle(str(keys)) == str(arm_bundle)


def test_probe_swallows_filesystem_errors(monkeypatch, tmp_path):
    def _raise(self):
        raise OSError("permission denied")

    monkeypatch.setattr(Path, "is_file", _raise)
    assert runtime_mod._probe_offline_bundle(str(_volume(tmp_path))) == ""


def test_sha256_option_is_validated(tmp_path):
    keys = _volume(tmp_path)
    (keys / "kc.lic").write_bytes(_LICENSE)
    bundle = _make_bundle(keys / DEFAULT_OFFLINE_BUNDLE_NAME)
    digest = hashlib.sha256(bundle.read_bytes()).hexdigest()

    config = build_runtime_config(
        {"license_volume_path": str(keys), KDBX_OFFLINE_BUNDLE_SHA256_OPTION: digest.upper()}
    )
    assert config.offline_bundle_sha256 == digest

    with pytest.raises(ValueError, match="64"):
        build_runtime_config(
            {"license_volume_path": str(keys), KDBX_OFFLINE_BUNDLE_SHA256_OPTION: "abc"}
        )


def test_sha256_without_a_bundle_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="no offline KDB-X bundle"):
        build_runtime_config(
            {
                "license_volume_path": str(_volume(tmp_path)),
                KDBX_OFFLINE_BUNDLE_SHA256_OPTION: "0" * 64,
            }
        )


def test_k4_license_file_selects_the_k4_license_name(tmp_path):
    keys = _volume(tmp_path)
    (keys / "k4.lic").write_bytes(_LICENSE)
    _make_bundle(keys / DEFAULT_OFFLINE_BUNDLE_NAME)

    config = build_runtime_config({"license_volume_path": str(keys)})

    assert config.license_file_name == "k4.lic"


def test_unknown_license_kind_is_rejected():
    with pytest.raises(ValueError, match="kdbx_license_kind"):
        build_runtime_config({"license_volume_path": "/tmp/lic", "kdbx_license_kind": "personal"})


# ---------------------------------------------------------------------------
# prepare_pykx
# ---------------------------------------------------------------------------


def _stub_import(monkeypatch, *, missing: bool = False):
    imports: list[str] = []
    fake_pykx = types.SimpleNamespace(__name__="pykx")
    monkeypatch.delitem(sys.modules, "pykx", raising=False)

    def _import_module(name):
        imports.append(name)
        if missing:
            error = ModuleNotFoundError("No module named 'pykx'")
            error.name = "pykx"
            raise error
        return fake_pykx

    monkeypatch.setattr(runtime_mod.importlib, "import_module", _import_module)
    return fake_pykx, imports


def test_preinstalled_runtime_imports_pykx_without_installing(monkeypatch, tmp_path):
    fake_pykx, imports = _stub_import(monkeypatch)
    monkeypatch.setattr(runtime_mod, "_install_once", lambda *_: pytest.fail("no install"))
    keys = _volume(tmp_path)

    result = runtime_mod.prepare_pykx(PyKxRuntimeConfig(license_directory=str(keys)))

    assert result is fake_pykx
    assert imports == ["pykx"]
    assert os.environ["QLIC"] == str(keys)
    assert os.environ["PYKX_LICENSED"] == "true"
    assert not (tmp_path / "runtime-root").exists()


def test_offline_runtime_installs_and_sets_the_environment_once_per_process(monkeypatch, tmp_path):
    _stub_import(monkeypatch)
    installs: list[Path] = []
    applies: list[Path | None] = []
    monkeypatch.setattr(runtime_mod, "_install_once", lambda _config, home: installs.append(home))
    original_apply = runtime_mod._apply_pykx_environment
    monkeypatch.setattr(
        runtime_mod,
        "_apply_pykx_environment",
        lambda config, home: applies.append(home) or original_apply(config, home),
    )
    config = _offline_config(tmp_path)

    runtime_mod.prepare_pykx(config)
    runtime_mod.prepare_pykx(config)

    assert len(installs) == 1
    assert applies == installs


def test_missing_pykx_names_the_pipeline_requirement(monkeypatch, tmp_path):
    _stub_import(monkeypatch, missing=True)
    monkeypatch.setattr(runtime_mod.subprocess, "run", _SubprocessRecorder([]))

    with pytest.raises(RuntimeError, match=PYKX_REQUIREMENT.replace(".", r"\.")):
        runtime_mod.prepare_pykx(PyKxRuntimeConfig(license_directory=str(_volume(tmp_path))))


def _stub_import_missing_until_installed(monkeypatch):
    fake_pykx = types.SimpleNamespace(__name__="pykx")
    monkeypatch.delitem(sys.modules, "pykx", raising=False)
    monkeypatch.setattr(sys, "path", list(sys.path))

    def _import_module(name):
        if not any(entry.endswith("pykx_pkgs") for entry in sys.path):
            error = ModuleNotFoundError("No module named 'pykx'")
            error.name = "pykx"
            raise error
        return fake_pykx

    monkeypatch.setattr(runtime_mod.importlib, "import_module", _import_module)
    return fake_pykx


def test_offline_runtime_installs_the_pinned_pykx_once_per_node(monkeypatch, tmp_path):
    fake_pykx = _stub_import_missing_until_installed(monkeypatch)
    monkeypatch.setattr(runtime_mod, "_install_once", lambda *_: None)
    monkeypatch.setenv("PYTHONPATH", "/databricks/jars/protected.jar")
    monkeypatch.setenv("PYTHONHOME", "/databricks/python")
    monkeypatch.setenv("KDB_LICENSE_B64", _LICENSE_B64)
    recorder = _SubprocessRecorder([(0, "Successfully installed pykx", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = _offline_config(tmp_path)

    assert runtime_mod.prepare_pykx(config) is fake_pykx
    # A second Python worker on the same node.
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOMES.clear()
    monkeypatch.setattr(sys, "path", [p for p in sys.path if not p.endswith("pykx_pkgs")])
    assert runtime_mod.prepare_pykx(config) is fake_pykx

    assert len(recorder.calls) == 1
    call = recorder.calls[0]
    target = runtime_mod._runtime_home(config) / "pykx_pkgs"
    assert call["args"][:4] == [sys.executable, "-I", "-m", "pip"]
    assert call["args"][-2:] == ["--pre", PYKX_REQUIREMENT]
    assert call["args"][call["args"].index("--target") + 1] == str(target)
    assert "--ignore-installed" in call["args"]
    assert "PYTHONPATH" not in call["env"]
    assert "PYTHONHOME" not in call["env"]
    assert "KDB_LICENSE_B64" not in call["env"]
    assert _LICENSE_B64 not in call["env"].values()
    assert call["stdin"] is subprocess.DEVNULL
    assert sys.path[-1] == str(target)
    assert os.environ["PYTHONPATH"] == "/databricks/jars/protected.jar"


def test_pykx_install_failure_is_reported_and_retried(monkeypatch, tmp_path):
    _stub_import_missing_until_installed(monkeypatch)
    monkeypatch.setattr(runtime_mod, "_install_once", lambda *_: None)
    recorder = _SubprocessRecorder([(1, "no candidate", "wheel error"), (0, "", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = _offline_config(tmp_path)

    with pytest.raises(RuntimeError, match="Failed to install PyKX.*wheel error"):
        runtime_mod.prepare_pykx(config)
    runtime_mod.prepare_pykx(config)

    assert len(recorder.calls) == 2


def test_failed_install_is_retried_on_the_next_call(monkeypatch, tmp_path):
    _stub_import(monkeypatch)
    attempts = []

    def _install(_config, _home):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("install failed")

    monkeypatch.setattr(runtime_mod, "_install_once", _install)
    config = _offline_config(tmp_path)

    with pytest.raises(RuntimeError, match="install failed"):
        runtime_mod.prepare_pykx(config)
    runtime_mod.prepare_pykx(config)

    assert len(attempts) == 2


# ---------------------------------------------------------------------------
# Runtime home and once-per-node install
# ---------------------------------------------------------------------------


def test_runtime_home_is_shared_per_configuration_and_owner_only(tmp_path):
    config = _offline_config(tmp_path)
    first = runtime_mod._runtime_home(config)
    runtime_mod._RUNTIME_HOMES.clear()
    again = runtime_mod._runtime_home(config)
    other_license = runtime_mod._runtime_home(
        PyKxRuntimeConfig(**{**config.__dict__, "license_b64": "b3RoZXI="})
    )
    other_bundle = runtime_mod._runtime_home(
        PyKxRuntimeConfig(**{**config.__dict__, "offline_bundle_identity": (1, 2)})
    )

    assert first == again
    assert len({first, other_license, other_bundle}) == 3
    assert first.parent == tmp_path / "runtime-root"
    assert first.name.startswith(f"kdbx-{os.getuid()}-")
    assert stat.S_IMODE(first.stat().st_mode) == 0o700


@pytest.mark.parametrize("unsafe", ["group_writable", "symlink"])
def test_runtime_home_falls_back_to_a_private_directory(monkeypatch, tmp_path, unsafe):
    config = _offline_config(tmp_path)
    shared = (
        tmp_path / "runtime-root" / f"kdbx-{os.getuid()}-{runtime_mod._runtime_key(config)[:16]}"
    )
    shared.parent.mkdir(parents=True)
    if unsafe == "symlink":
        target = tmp_path / "elsewhere"
        target.mkdir(mode=0o700)
        shared.symlink_to(target, target_is_directory=True)
    else:
        shared.mkdir()
        shared.chmod(0o770)
    private = tmp_path / "private"
    monkeypatch.setattr(runtime_mod.tempfile, "mkdtemp", lambda prefix: str(private))

    assert runtime_mod._runtime_home(config) == private


def test_install_once_installs_once_per_node(monkeypatch, tmp_path):
    installs = []
    monkeypatch.setattr(
        runtime_mod, "_install_kdbx_offline", lambda _config, home: installs.append(home)
    )
    config = _offline_config(tmp_path)
    home = runtime_mod._runtime_home(config)

    runtime_mod._install_once(config, home)
    # A second Python worker on the same node.
    runtime_mod._RUNTIME_HOMES.clear()
    runtime_mod._install_once(config, runtime_mod._runtime_home(config))

    assert installs == [home]
    assert (home / runtime_mod._INSTALL_MARKER).is_file()
    assert (home / ".install.lock").is_file()


def test_install_once_discards_a_partial_install(monkeypatch, tmp_path):
    config = _offline_config(tmp_path)
    home = runtime_mod._runtime_home(config)
    (home / ".kx" / "bin").mkdir(parents=True)
    (home / ".kx" / "bin" / "stale").write_text("partial")
    seen = []
    monkeypatch.setattr(
        runtime_mod,
        "_install_kdbx_offline",
        lambda _config, home: seen.append(sorted(p.name for p in (home / ".kx").iterdir())),
    )

    runtime_mod._install_once(config, home)

    assert seen == [[]]


def test_node_lock_times_out_while_another_process_holds_it(tmp_path):
    import fcntl

    home = runtime_mod._runtime_home(_offline_config(tmp_path))
    holder = os.open(home / ".install.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        with pytest.raises(TimeoutError, match="another process on this node"):
            with runtime_mod._node_lock(home, timeout=0):
                pass
    finally:
        os.close(holder)

    with runtime_mod._node_lock(home, timeout=0):
        pass


def test_offline_install_discards_a_stale_work_directory(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod.subprocess, "run", _SubprocessRecorder([(0, "", "")]))
    config = _offline_config(tmp_path)
    home = runtime_mod._runtime_home(config)
    stale = home / runtime_mod._INSTALL_WORKDIR / "bundle" / "leftover"
    stale.parent.mkdir(parents=True)
    stale.write_text("from a killed install")

    runtime_mod._install_kdbx_offline(config, home)

    assert not (home / runtime_mod._INSTALL_WORKDIR).exists()


def test_license_is_replaced_atomically(monkeypatch, tmp_path):
    config = _offline_config(tmp_path)
    home = runtime_mod._runtime_home(config)
    license_file = home / "qlic" / "kc.lic"
    license_file.parent.mkdir(mode=0o700)
    license_file.write_bytes(b"old license")
    replaced = []
    original_replace = runtime_mod.os.replace
    monkeypatch.setattr(
        runtime_mod.os,
        "replace",
        lambda src, dst: replaced.append((Path(src).name, Path(dst))) or original_replace(src, dst),
    )

    runtime_mod._materialize_license(config, home)

    assert replaced == [(f".kc.lic.{os.getpid()}.tmp", license_file)]
    assert license_file.read_bytes() == _LICENSE
    assert stat.S_IMODE(license_file.stat().st_mode) == 0o600
    assert sorted(p.name for p in license_file.parent.iterdir()) == ["kc.lic"]


def test_failed_install_leaves_no_marker(monkeypatch, tmp_path):
    config = _offline_config(tmp_path)
    home = runtime_mod._runtime_home(config)

    def _fail(_config, _home):
        raise RuntimeError("boom")

    monkeypatch.setattr(runtime_mod, "_install_kdbx_offline", _fail)

    with pytest.raises(RuntimeError, match="boom"):
        runtime_mod._install_once(config, home)
    assert not (home / runtime_mod._INSTALL_MARKER).exists()


# ---------------------------------------------------------------------------
# Offline install
# ---------------------------------------------------------------------------


def test_offline_install_runs_the_bundle_installer(monkeypatch, tmp_path):
    monkeypatch.setenv("TERM", "unknown")
    recorder = _SubprocessRecorder([(0, "ok", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = _offline_config(tmp_path)
    home = runtime_mod._runtime_home(config)

    runtime_mod._install_kdbx_offline(config, home)

    call = recorder.calls[0]
    assert call["args"][0] == "bash"
    assert Path(call["args"][1]).name == "install_kdb.sh"
    # `-y` keeps install_kdb.sh from waiting on a prompt no one will answer.
    assert call["args"][2:4] == ["--offline", "-y"]
    assert call["args"][-2:] == ["--b64lic", _LICENSE_B64]
    assert call["env"]["HOME"] == str(home)
    # An inherited TERM=unknown makes tput inside install_kdb.sh abort.
    assert call["env"]["TERM"] == "dumb"
    assert call["timeout"] == 1200
    assert call["stdin"] is subprocess.DEVNULL
    assert list(home.iterdir()) == []


def test_offline_install_reports_installer_failures(monkeypatch, tmp_path):
    recorder = _SubprocessRecorder([(7, "stdout-trail", "stderr-trail")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    config = _offline_config(tmp_path)

    with pytest.raises(
        RuntimeError, match=r"install_kdb.sh --offline failed.*stdout-trail.*stderr-trail"
    ):
        runtime_mod._install_kdbx_offline(config, runtime_mod._runtime_home(config))


def test_offline_install_requires_the_installer_in_the_bundle(tmp_path):
    bundle = _make_bundle(tmp_path / "vol" / "kdbx" / "no-installer.zip", with_installer=False)
    config = _offline_config(
        tmp_path, offline_bundle_path=str(bundle), offline_bundle_identity=None
    )

    with pytest.raises(RuntimeError, match="install_kdb.sh"):
        runtime_mod._install_kdbx_offline(config, runtime_mod._runtime_home(config))


def test_offline_install_rejects_an_invalid_zip(tmp_path):
    bogus = tmp_path / "vol" / "kdbx" / "bogus.zip"
    bogus.parent.mkdir(parents=True)
    bogus.write_text("not a zip")
    config = _offline_config(tmp_path, offline_bundle_path=str(bogus), offline_bundle_identity=None)

    with pytest.raises(RuntimeError, match="not a valid zip"):
        runtime_mod._install_kdbx_offline(config, runtime_mod._runtime_home(config))


def test_missing_bundle_cannot_be_localized(monkeypatch, tmp_path):
    monkeypatch.setattr(
        runtime_mod, "_copy_with_dbutils", lambda *_: (_ for _ in ()).throw(RuntimeError("no"))
    )
    config = _offline_config(
        tmp_path,
        offline_bundle_path=str(tmp_path / "vol" / "missing.zip"),
        offline_bundle_identity=None,
    )

    with pytest.raises(RuntimeError, match="could not be localized"):
        runtime_mod._localize_offline_bundle(config, tmp_path)


def test_bundle_changed_since_the_driver_pinned_it_is_rejected(tmp_path):
    config = _offline_config(tmp_path)
    Path(config.offline_bundle_path).write_bytes(b"replaced bundle")

    with pytest.raises(RuntimeError, match="changed after this pipeline update started"):
        runtime_mod._localize_offline_bundle(config, tmp_path)


def test_bundle_copy_with_the_wrong_size_is_rejected(monkeypatch, tmp_path):
    config = _offline_config(tmp_path)
    monkeypatch.setattr(runtime_mod, "_check_source_identity", lambda *_: None)
    Path(config.offline_bundle_path).write_bytes(b"x")

    with pytest.raises(RuntimeError, match="bytes"):
        runtime_mod._localize_offline_bundle(config, tmp_path)


def test_bundle_sha256_is_verified(tmp_path):
    config = _offline_config(tmp_path)
    digest = hashlib.sha256(Path(config.offline_bundle_path).read_bytes()).hexdigest()
    target = tmp_path / "copy"
    target.mkdir()

    good = _offline_config(tmp_path, offline_bundle_sha256=digest)
    assert runtime_mod._localize_offline_bundle(good, target).is_file()

    bad = _offline_config(tmp_path, offline_bundle_sha256="0" * 64)
    with pytest.raises(RuntimeError, match=KDBX_OFFLINE_BUNDLE_SHA256_OPTION):
        runtime_mod._localize_offline_bundle(bad, tmp_path)


def test_bundle_falls_back_to_dbutils_when_fuse_is_not_visible(monkeypatch, tmp_path):
    source = "/Volumes/main/kx/files/kdbx/l64-bundle.zip"
    copied = []

    def fake_copy(src, dst):
        copied.append((src, dst))
        _make_bundle(Path(dst.removeprefix("file:")))

    monkeypatch.setattr(runtime_mod, "_copy_with_dbutils", fake_copy)
    config = PyKxRuntimeConfig(
        license_directory="/Volumes/main/kx/files/keys",
        license_b64=_LICENSE_B64,
        offline_bundle_path=source,
    )

    localized = runtime_mod._localize_offline_bundle(config, tmp_path)

    assert localized == tmp_path / "l64-bundle.zip"
    assert copied == [(f"file:{source}", f"file:{localized}")]


# ---------------------------------------------------------------------------
# _apply_pykx_environment
# ---------------------------------------------------------------------------


def test_environment_sets_only_qlic_qhome_and_pykx_values(monkeypatch, tmp_path):
    config = _offline_config(tmp_path)
    home = runtime_mod._runtime_home(config)
    (home / ".kx" / "bin").mkdir(parents=True)
    monkeypatch.setenv("HOME", "/home/user")
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    chdir_calls: list[str] = []
    monkeypatch.setattr(runtime_mod.os, "chdir", chdir_calls.append)

    runtime_mod._apply_pykx_environment(config, home)
    runtime_mod._apply_pykx_environment(config, home)

    assert os.environ["HOME"] == "/home/user"
    assert os.environ["QLIC"] == str(home / "qlic")
    assert os.environ["QHOME"] == str(home / ".kx")
    assert os.environ["PYKX_LICENSED"] == "true"
    assert os.environ["PATH"] == f"{home / '.kx' / 'bin'}:/usr/bin:/bin"
    assert (home / "qlic" / "kc.lic").read_bytes() == _LICENSE
    assert chdir_calls == []


def test_preinstalled_environment_points_qlic_at_the_license_directory(monkeypatch, tmp_path):
    keys = _volume(tmp_path)
    monkeypatch.setenv("QHOME", "/opt/kx")
    monkeypatch.setenv("PATH", "/usr/bin")

    runtime_mod._apply_pykx_environment(PyKxRuntimeConfig(license_directory=str(keys)), None)

    assert os.environ["QLIC"] == str(keys)
    assert os.environ["QHOME"] == "/opt/kx"
    assert os.environ["PATH"] == "/usr/bin"


# ---------------------------------------------------------------------------
# _dbutils
# ---------------------------------------------------------------------------


def test_dbutils_prefers_the_databricks_runtime(monkeypatch):
    runtime_dbutils = object()
    monkeypatch.setitem(
        sys.modules, "databricks.sdk.runtime", types.SimpleNamespace(dbutils=runtime_dbutils)
    )

    assert runtime_mod._dbutils() is runtime_dbutils


@pytest.mark.parametrize("session_lookup", ["active", "default", "instantiated", "builder"])
def test_dbutils_falls_back_to_the_spark_session(monkeypatch, session_lookup):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)
    spark = object()

    class _FakeDBUtils:
        def __init__(self, session):
            self.session = session

    class _Builder:
        @staticmethod
        def getOrCreate():
            return spark

    class _FakeSparkSession:
        builder = _Builder()
        _instantiatedSession = spark if session_lookup == "instantiated" else None

        @staticmethod
        def getActiveSession():
            return spark if session_lookup == "active" else None

        @staticmethod
        def getDefaultSession():
            return spark if session_lookup == "default" else None

    monkeypatch.setitem(sys.modules, "pyspark.dbutils", types.SimpleNamespace(DBUtils=_FakeDBUtils))
    monkeypatch.setitem(
        sys.modules, "pyspark.sql", types.SimpleNamespace(SparkSession=_FakeSparkSession)
    )

    assert runtime_mod._dbutils().session is spark


def test_dbutils_raises_when_no_helper_is_available(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", None)

    with pytest.raises(RuntimeError, match="dbutils is not available"):
        runtime_mod._dbutils()


@pytest.mark.parametrize("builder_result", ["raises", "none"])
def test_dbutils_requires_a_spark_session(monkeypatch, builder_result):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)

    class _Builder:
        @staticmethod
        def getOrCreate():
            if builder_result == "raises":
                raise RuntimeError("no cluster")
            return None

    class _FakeSparkSession:
        builder = _Builder()
        _instantiatedSession = None

        @staticmethod
        def getActiveSession():
            return None

    class _UnusedDBUtils:
        def __init__(self, _spark):
            raise AssertionError("DBUtils must not be built without a SparkSession")

    monkeypatch.setitem(
        sys.modules, "pyspark.dbutils", types.SimpleNamespace(DBUtils=_UnusedDBUtils)
    )
    monkeypatch.setitem(
        sys.modules, "pyspark.sql", types.SimpleNamespace(SparkSession=_FakeSparkSession)
    )

    with pytest.raises(RuntimeError, match="SparkSession is required"):
        runtime_mod._dbutils()


# ---------------------------------------------------------------------------
# _command_tail
# ---------------------------------------------------------------------------


def test_command_tail_handles_empty_input():
    assert runtime_mod._command_tail("") == ""
    assert runtime_mod._command_tail(None) == ""  # type: ignore[arg-type]


def test_command_tail_truncates_long_input_to_limit():
    truncated = runtime_mod._command_tail("x" * 5000, limit=200)
    assert truncated == "x" * 200
