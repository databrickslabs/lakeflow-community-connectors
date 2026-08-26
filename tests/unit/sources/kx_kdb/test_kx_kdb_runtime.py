"""Tests for KDB-X / PyKX runtime bootstrap config."""

from __future__ import annotations

import base64
import os
import subprocess
import sys
import tempfile
import types
import zipfile
from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.kx_kdb import runtime as runtime_mod
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    ARM_OFFLINE_BUNDLE_NAME,
    DEFAULT_OFFLINE_BUNDLE_NAME,
    KDBX_INSTALL_BEARER_SECRET_KEY_OPTION,
    KDBX_INSTALL_BEARER_TOKEN_OPTION,
    KDBX_INSTALL_MODE_OPTION,
    KDBX_LICENSE_B64_OPTION,
    KDBX_LICENSE_B64_SECRET_KEY_OPTION,
    KDBX_OFFLINE_BUNDLE_PATH_OPTION,
    KDBX_SECRET_SCOPE_OPTION,
    PYKX_PIP_SPEC,
    PyKxRuntimeConfig,
    build_runtime_config,
    normalize_license_directory,
)

# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_runtime_cache(monkeypatch):
    """Ensure module-level caches do not leak between tests."""
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOME_CACHE.clear()
    runtime_mod._LOCAL_BUNDLE_DIR_CACHE.clear()
    monkeypatch.delenv(runtime_mod._RUNTIME_HOME_OVERRIDE_ENV, raising=False)
    yield
    runtime_mod._PREPARED_RUNTIME_KEYS.clear()
    runtime_mod._RUNTIME_HOME_CACHE.clear()
    runtime_mod._LOCAL_BUNDLE_DIR_CACHE.clear()


def _completed(args, rc=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(args=args, returncode=rc, stdout=stdout, stderr=stderr)


class _SubprocessRecorder:
    """Drop-in replacement for ``subprocess.run`` that records and replays."""

    def __init__(self, results):
        self._results = list(results)
        self.calls: list[dict] = []

    def __call__(self, args, **kwargs):
        rc, stdout, stderr = self._results.pop(0)
        self.calls.append({"args": list(args), **kwargs})
        return _completed(args, rc=rc, stdout=stdout, stderr=stderr)


# ---------------------------------------------------------------------------
# normalize_license_directory & build_runtime_config (existing + edges)
# ---------------------------------------------------------------------------


def test_normalize_license_directory_accepts_folder_or_file_path():
    assert normalize_license_directory("/Volumes/main/rkh/rbc_kx/key_kdbx/") == (
        "/Volumes/main/rkh/rbc_kx/key_kdbx"
    )
    assert normalize_license_directory("/Volumes/main/rkh/rbc_kx/key_kdbx/kc.lic") == (
        "/Volumes/main/rkh/rbc_kx/key_kdbx"
    )


def test_build_runtime_config_defaults_to_legacy_license_folder_flow():
    config = build_runtime_config({"license_volume_path": "/Volumes/main/rkh/rbc_kx/key_kdbx/"})

    assert config.license_directory == "/Volumes/main/rkh/rbc_kx/key_kdbx"
    assert config.uses_installer is False
    assert config.installer_bearer_token is None
    assert config.license_b64 is None


def test_build_runtime_config_resolves_secret_backed_kdbx_bootstrap():
    resolved = {
        ("rkh_kdbx_smoke", "installer_bearer"): "token-value",
        ("rkh_kdbx_smoke", "license_b64"): "license-value",
    }

    config = build_runtime_config(
        {
            "license_volume_path": "/Volumes/main/rkh/rbc_kx/key_kdbx/",
            KDBX_SECRET_SCOPE_OPTION: "rkh_kdbx_smoke",
            KDBX_INSTALL_BEARER_SECRET_KEY_OPTION: "installer_bearer",
            KDBX_LICENSE_B64_SECRET_KEY_OPTION: "license_b64",
        },
        secret_resolver=lambda scope, key: resolved[(scope, key)],
    )

    assert config.license_directory == "/Volumes/main/rkh/rbc_kx/key_kdbx"
    assert config.uses_installer is True
    assert config.installer_bearer_token == "token-value"
    assert config.license_b64 == "license-value"


def test_build_runtime_config_prefers_direct_secret_backed_connection_values():
    def resolver_should_not_run(*_):
        raise AssertionError("resolver should not be used")

    config = build_runtime_config(
        {
            "license_volume_path": "/Volumes/main/rkh/rbc_kx/key_kdbx/",
            KDBX_INSTALL_BEARER_TOKEN_OPTION: "token-value",
            KDBX_LICENSE_B64_OPTION: "license-value",
            KDBX_SECRET_SCOPE_OPTION: "ignored_scope",
            KDBX_INSTALL_BEARER_SECRET_KEY_OPTION: "ignored_token_key",
            KDBX_LICENSE_B64_SECRET_KEY_OPTION: "ignored_license_key",
        },
        secret_resolver=resolver_should_not_run,
    )

    assert config.uses_installer is True
    assert config.installer_bearer_token == "token-value"
    assert config.license_b64 == "license-value"


def test_build_runtime_config_reads_license_file_path(tmp_path):
    license_path = tmp_path / "kc.lic"
    license_path.write_bytes(b"license-bytes")

    config = build_runtime_config(
        {
            "license_volume_path": str(tmp_path),
            "kdbx_license_file_path": str(license_path),
        }
    )

    assert config.license_b64 == base64.b64encode(b"license-bytes").decode("ascii")


def test_build_runtime_config_rejects_partial_secret_configuration():
    with pytest.raises(ValueError, match="KDB-X bootstrap requires all"):
        build_runtime_config(
            {
                "license_volume_path": "/Volumes/main/rkh/rbc_kx/key_kdbx/",
                KDBX_SECRET_SCOPE_OPTION: "rkh_kdbx_smoke",
            },
            secret_resolver=lambda *_: "",
        )


def test_build_runtime_config_rejects_partial_direct_configuration():
    with pytest.raises(ValueError, match="requires both"):
        build_runtime_config(
            {
                "license_volume_path": "/Volumes/main/rkh/rbc_kx/key_kdbx/",
                KDBX_INSTALL_BEARER_TOKEN_OPTION: "token-value",
            }
        )


def test_build_runtime_config_rejects_empty_resolved_secret_value():
    with pytest.raises(ValueError, match="empty KDB-X bootstrap secret"):
        build_runtime_config(
            {
                "license_volume_path": "/Volumes/main/rkh/rbc_kx/key_kdbx/",
                KDBX_SECRET_SCOPE_OPTION: "rkh_kdbx_smoke",
                KDBX_INSTALL_BEARER_SECRET_KEY_OPTION: "installer_bearer",
                KDBX_LICENSE_B64_SECRET_KEY_OPTION: "license_b64",
            },
            secret_resolver=lambda *_: "   ",
        )


def test_build_runtime_config_treats_whitespace_only_direct_options_as_legacy():
    config = build_runtime_config(
        {
            "license_volume_path": "/Volumes/main/rkh/rbc_kx/key_kdbx/",
            KDBX_INSTALL_BEARER_TOKEN_OPTION: "   ",
            KDBX_LICENSE_B64_OPTION: "\t\n",
        }
    )

    assert config.uses_installer is False
    assert config.installer_bearer_token is None
    assert config.license_b64 is None


def test_build_runtime_config_with_no_license_path_normalizes_to_current_directory():
    config = build_runtime_config({})

    assert config.license_directory == "."
    assert config.uses_installer is False


# ---------------------------------------------------------------------------
# PyKxRuntimeConfig.uses_installer
# ---------------------------------------------------------------------------


def test_uses_installer_requires_both_token_and_license():
    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        installer_bearer_token="token",
        license_b64="license",
    )
    assert config.uses_installer is True


def test_uses_installer_false_when_only_token():
    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        installer_bearer_token="token",
        license_b64=None,
    )
    assert config.uses_installer is False


def test_uses_installer_false_when_neither_supplied():
    config = PyKxRuntimeConfig(license_directory="/tmp/lic")
    assert config.uses_installer is False


def test_uses_offline_bundle_true_when_path_and_license_present():
    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        license_b64="lic",
        offline_bundle_path="/tmp/l64-bundle.zip",
    )
    assert config.uses_offline_bundle is True
    assert config.uses_installer is True


def test_uses_offline_bundle_false_when_license_missing():
    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        offline_bundle_path="/tmp/l64-bundle.zip",
    )
    assert config.uses_offline_bundle is False
    assert config.uses_installer is False


# ---------------------------------------------------------------------------
# _command_tail and _runtime_home_directory
# ---------------------------------------------------------------------------


def test_command_tail_handles_empty_input():
    assert runtime_mod._command_tail("") == ""
    assert runtime_mod._command_tail(None) == ""  # type: ignore[arg-type]


def test_command_tail_returns_short_input_unchanged_and_strips_whitespace():
    assert runtime_mod._command_tail("  hello  ") == "hello"


def test_command_tail_truncates_long_input_to_limit():
    payload = "x" * 5000
    truncated = runtime_mod._command_tail(payload, limit=200)
    assert len(truncated) == 200
    assert truncated == "x" * 200


def test_runtime_home_directory_creates_unique_writable_dir_under_tempdir():
    runtime_home = runtime_mod._runtime_home_directory()

    assert isinstance(runtime_home, Path)
    assert runtime_home.is_dir()
    assert runtime_home.parent == Path(tempfile.gettempdir())
    assert runtime_home.name.startswith("kdbx-home-")


def test_runtime_home_directory_caches_choice_for_process_lifetime():
    first = runtime_mod._runtime_home_directory()
    second = runtime_mod._runtime_home_directory()

    assert first == second


def test_runtime_home_directory_honors_override_env_var(monkeypatch, tmp_path):
    override = tmp_path / "kdbx-override"
    monkeypatch.setenv(runtime_mod._RUNTIME_HOME_OVERRIDE_ENV, str(override))

    runtime_home = runtime_mod._runtime_home_directory()

    assert runtime_home == override
    assert override.is_dir()


# ---------------------------------------------------------------------------
# _apply_pykx_environment
# ---------------------------------------------------------------------------


def _patch_runtime_home(monkeypatch, runtime_home: Path) -> None:
    runtime_home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: runtime_home)


def test_apply_pykx_environment_sets_core_env_vars_and_license_b64(monkeypatch, tmp_path):
    license_dir = tmp_path / "license"
    license_dir.mkdir()
    runtime_home = tmp_path / "home"
    _patch_runtime_home(monkeypatch, runtime_home)

    chdir_calls: list[str] = []
    monkeypatch.setattr(runtime_mod.os, "chdir", lambda path: chdir_calls.append(path))
    monkeypatch.delenv("KDB_LICENSE_B64", raising=False)

    config = PyKxRuntimeConfig(
        license_directory=str(license_dir),
        installer_bearer_token="token",
        license_b64=base64.b64encode(b"license").decode("ascii"),
    )

    runtime_mod._apply_pykx_environment(config)

    assert os.environ["HOME"] == str(runtime_home)
    assert os.environ["QLIC"] == str(runtime_home / "qlic")
    assert os.environ["PYKX_LICENSED"] == "true"
    assert os.environ["KDB_LICENSE_B64"] == base64.b64encode(b"license").decode("ascii")
    assert (runtime_home / "qlic" / "kc.lic").read_bytes() == b"license"
    assert chdir_calls == []


def test_apply_pykx_environment_does_not_stat_volume_license_dir_when_b64_present(
    monkeypatch, tmp_path
):
    runtime_home = tmp_path / "home"
    _patch_runtime_home(monkeypatch, runtime_home)
    chdir_calls: list[str] = []
    monkeypatch.setattr(runtime_mod.os, "chdir", lambda path: chdir_calls.append(path))

    config = PyKxRuntimeConfig(
        license_directory="/Volumes/main/rkh/rbc_kx/key_kdbx",
        license_b64=base64.b64encode(b"license").decode("ascii"),
    )

    runtime_mod._apply_pykx_environment(config)

    assert os.environ["QLIC"] == str(runtime_home / "qlic")
    assert (runtime_home / "qlic" / "kc.lic").read_bytes() == b"license"
    assert chdir_calls == []


def test_apply_pykx_environment_does_not_set_kdb_license_b64_when_absent(monkeypatch, tmp_path):
    license_dir = tmp_path / "license"
    license_dir.mkdir()
    runtime_home = tmp_path / "home"
    _patch_runtime_home(monkeypatch, runtime_home)
    monkeypatch.setattr(runtime_mod.os, "chdir", lambda path: None)
    monkeypatch.delenv("KDB_LICENSE_B64", raising=False)

    runtime_mod._apply_pykx_environment(PyKxRuntimeConfig(license_directory=str(license_dir)))

    assert "KDB_LICENSE_B64" not in os.environ


def test_apply_pykx_environment_prepends_kx_bin_to_path_only_when_present(monkeypatch, tmp_path):
    license_dir = tmp_path / "license"
    license_dir.mkdir()
    runtime_home = tmp_path / "home"
    _patch_runtime_home(monkeypatch, runtime_home)
    monkeypatch.setattr(runtime_mod.os, "chdir", lambda path: None)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")

    runtime_mod._apply_pykx_environment(PyKxRuntimeConfig(license_directory=str(license_dir)))
    assert os.environ["PATH"] == "/usr/bin:/bin"

    kx_bin = runtime_home / ".kx" / "bin"
    kx_bin.mkdir(parents=True)

    runtime_mod._apply_pykx_environment(PyKxRuntimeConfig(license_directory=str(license_dir)))
    assert os.environ["PATH"].startswith(f"{kx_bin}:")


def test_apply_pykx_environment_skips_chdir_when_target_missing(monkeypatch, tmp_path):
    runtime_home = tmp_path / "home"
    _patch_runtime_home(monkeypatch, runtime_home)
    chdir_calls: list[str] = []
    monkeypatch.setattr(runtime_mod.os, "chdir", lambda path: chdir_calls.append(path))

    runtime_mod._apply_pykx_environment(
        PyKxRuntimeConfig(license_directory=str(tmp_path / "missing"))
    )

    assert chdir_calls == []


# ---------------------------------------------------------------------------
# prepare_pykx
# ---------------------------------------------------------------------------


def _stub_prepare_dependencies(
    monkeypatch,
    *,
    pykx_already_loaded: bool,
    import_missing_once: bool = False,
):
    install_calls: list[PyKxRuntimeConfig] = []
    ensure_calls: list[PyKxRuntimeConfig] = []
    apply_calls: list[PyKxRuntimeConfig] = []
    import_calls: list[str] = []

    monkeypatch.setattr(
        runtime_mod, "_install_kdbx", lambda config: install_calls.append(config)
    )
    monkeypatch.setattr(
        runtime_mod, "_ensure_pykx_package", lambda config: ensure_calls.append(config)
    )
    monkeypatch.setattr(
        runtime_mod, "_apply_pykx_environment", lambda config: apply_calls.append(config)
    )
    fake_pykx = types.SimpleNamespace(__name__="pykx")

    if pykx_already_loaded:
        monkeypatch.setitem(sys.modules, "pykx", fake_pykx)

        def _import_module(name):
            import_calls.append(name)
            raise AssertionError("import_module must not be called when pykx is cached")
    else:
        monkeypatch.delitem(sys.modules, "pykx", raising=False)

        def _import_module(name):
            import_calls.append(name)
            if import_missing_once and len(import_calls) == 1:
                error = ModuleNotFoundError("No module named 'pykx'")
                error.name = "pykx"
                raise error
            return fake_pykx

    monkeypatch.setattr(runtime_mod.importlib, "import_module", _import_module)

    return {
        "install_calls": install_calls,
        "ensure_calls": ensure_calls,
        "apply_calls": apply_calls,
        "import_calls": import_calls,
        "fake_pykx": fake_pykx,
    }


def test_prepare_pykx_legacy_flow_skips_installer_and_imports_module(monkeypatch):
    state = _stub_prepare_dependencies(monkeypatch, pykx_already_loaded=False)
    config = PyKxRuntimeConfig(license_directory="/tmp/lic")

    result = runtime_mod.prepare_pykx(config)

    assert result is state["fake_pykx"]
    assert state["install_calls"] == []
    assert state["ensure_calls"] == []
    assert state["apply_calls"] == [config]
    assert state["import_calls"] == ["pykx"]
    assert runtime_mod._PREPARED_RUNTIME_KEYS == set()


def test_prepare_pykx_installer_flow_runs_install_and_pip_when_pykx_missing(monkeypatch):
    state = _stub_prepare_dependencies(
        monkeypatch, pykx_already_loaded=False, import_missing_once=True
    )
    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        installer_bearer_token="token",
        license_b64="lic-b64",
    )

    runtime_mod.prepare_pykx(config)

    assert state["install_calls"] == [config]
    assert state["ensure_calls"] == [config]
    assert state["apply_calls"] == [config]
    assert (
        "/tmp/lic",
        len("token"),
        len("lic-b64"),
    ) in runtime_mod._PREPARED_RUNTIME_KEYS


def test_prepare_pykx_installer_flow_is_idempotent_for_same_runtime_key(monkeypatch):
    state = _stub_prepare_dependencies(monkeypatch, pykx_already_loaded=False)
    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        installer_bearer_token="token",
        license_b64="lic-b64",
    )

    runtime_mod.prepare_pykx(config)
    runtime_mod.prepare_pykx(config)

    assert len(state["install_calls"]) == 1
    assert state["ensure_calls"] == []
    assert state["apply_calls"] == [config, config]


def test_prepare_pykx_returns_cached_module_without_calling_import_module(monkeypatch):
    state = _stub_prepare_dependencies(monkeypatch, pykx_already_loaded=True)
    config = PyKxRuntimeConfig(license_directory="/tmp/lic")

    result = runtime_mod.prepare_pykx(config)

    assert result is state["fake_pykx"]
    assert state["import_calls"] == []


# ---------------------------------------------------------------------------
# _install_kdbx
# ---------------------------------------------------------------------------


def test_install_kdbx_returns_early_when_uses_installer_false(monkeypatch):
    recorder = _SubprocessRecorder([])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    runtime_mod._install_kdbx(PyKxRuntimeConfig(license_directory="/tmp/lic"))

    assert recorder.calls == []


def test_install_kdbx_invokes_curl_then_bash_with_expected_args(monkeypatch, tmp_path):
    runtime_home = tmp_path / "home"
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: runtime_home)
    monkeypatch.setenv("TERM", "unknown")
    recorder = _SubprocessRecorder([(0, "ok", ""), (0, "ok", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        installer_bearer_token="bearer-tok",
        license_b64="b64-lic",
    )

    runtime_mod._install_kdbx(config)

    assert runtime_home.is_dir()
    assert (runtime_home / ".kx").is_dir()
    assert len(recorder.calls) == 2

    curl_call = recorder.calls[0]
    assert curl_call["args"][0] == "curl"
    assert "--oauth2-bearer" in curl_call["args"]
    assert curl_call["args"][curl_call["args"].index("--oauth2-bearer") + 1] == "bearer-tok"
    assert curl_call["args"][-1] == config.installer_url
    assert curl_call.get("timeout") == 180

    install_call = recorder.calls[1]
    assert install_call["args"][0] == "bash"
    assert install_call["args"][-2] == "--b64lic"
    assert install_call["args"][-1] == "b64-lic"
    assert install_call.get("timeout") == 1200
    assert install_call["env"]["HOME"] == str(runtime_home)
    # Inherited TERM=unknown must be overridden so tput inside install_kdb.sh
    # does not abort.
    assert install_call["env"]["TERM"] == "dumb"
    assert install_call["cwd"] == curl_call["cwd"]
    assert Path(install_call["args"][1]).name == "install_kdb.sh"


def test_install_kdbx_raises_when_curl_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    recorder = _SubprocessRecorder([(22, "", "curl boom")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        installer_bearer_token="t",
        license_b64="l",
    )

    with pytest.raises(RuntimeError, match="installer download failed.*curl boom"):
        runtime_mod._install_kdbx(config)

    assert len(recorder.calls) == 1


def test_install_kdbx_raises_when_installer_script_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    recorder = _SubprocessRecorder(
        [
            (0, "", ""),
            (1, "stdout-trail", "stderr-trail"),
        ]
    )
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        installer_bearer_token="t",
        license_b64="l",
    )

    with pytest.raises(RuntimeError, match="install_kdb.sh failed.*stdout-trail.*stderr-trail"):
        runtime_mod._install_kdbx(config)


# ---------------------------------------------------------------------------
# _ensure_pykx_package
# ---------------------------------------------------------------------------


def test_ensure_pykx_package_invokes_pip_install(monkeypatch):
    recorder = _SubprocessRecorder([(0, "Successfully installed pykx", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    _patch_runtime_home(monkeypatch, Path("/tmp/kx-runtime-test"))
    monkeypatch.setenv("PYTHONPATH", "/databricks/jars/protected.jar")
    monkeypatch.setenv("PYTHONHOME", "/databricks/python")

    runtime_mod._ensure_pykx_package(PyKxRuntimeConfig(license_directory="/tmp/lic"))

    assert len(recorder.calls) == 1
    args = recorder.calls[0]["args"]
    assert args[:4] == [sys.executable, "-I", "-m", "pip"]
    assert "install" in args
    assert "--ignore-installed" in args
    assert "--target" in args
    assert "--pre" in args
    assert PYKX_PIP_SPEC in args
    assert "/tmp/kx-runtime-test/pykx_pkgs" in args
    assert "PYTHONPATH" not in recorder.calls[0]["env"]
    assert "PYTHONHOME" not in recorder.calls[0]["env"]
    assert sys.path[0] == "/tmp/kx-runtime-test/pykx_pkgs"
    assert os.environ["PYTHONPATH"].split(os.pathsep)[0] == "/tmp/kx-runtime-test/pykx_pkgs"


def test_ensure_pykx_package_uses_configured_wheel_without_pre(monkeypatch):
    recorder = _SubprocessRecorder([(0, "Successfully installed pykx", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    _patch_runtime_home(monkeypatch, Path("/tmp/kx-runtime-wheel"))

    runtime_mod._ensure_pykx_package(
        PyKxRuntimeConfig(
            license_directory="/tmp/lic",
            pykx_install_spec="/Volumes/wheels/pykx.whl",
        )
    )

    args = recorder.calls[0]["args"]
    assert "--pre" not in args
    assert "/Volumes/wheels/pykx.whl" in args


def test_ensure_pykx_package_raises_on_pip_failure(monkeypatch):
    recorder = _SubprocessRecorder([(1, "no candidate", "wheel build error")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)
    _patch_runtime_home(monkeypatch, Path("/tmp/kx-runtime-failure"))

    with pytest.raises(RuntimeError, match="Failed to install PyKX.*wheel build error"):
        runtime_mod._ensure_pykx_package(PyKxRuntimeConfig(license_directory="/tmp/lic"))


# ---------------------------------------------------------------------------
# _resolve_databricks_secret
# ---------------------------------------------------------------------------


def _make_dbutils(getter):
    return types.SimpleNamespace(secrets=types.SimpleNamespace(get=getter))


def test_resolve_databricks_secret_uses_sdk_runtime_when_available(monkeypatch):
    captured: list[tuple[str, str]] = []

    def _get(scope, key):
        captured.append((scope, key))
        return f"sdk:{scope}:{key}"

    fake_runtime = types.SimpleNamespace(dbutils=_make_dbutils(_get))
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", fake_runtime)

    result = runtime_mod._resolve_databricks_secret("scope-a", "key-a")

    assert result == "sdk:scope-a:key-a"
    assert captured == [("scope-a", "key-a")]


def test_resolve_databricks_secret_falls_back_to_pyspark_dbutils(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)

    captured: list[tuple[object, str, str]] = []

    class _FakeDBUtils:
        def __init__(self, spark):
            self._spark = spark
            self.secrets = types.SimpleNamespace(get=self._get)

        def _get(self, scope, key):
            captured.append((self._spark, scope, key))
            return f"pyspark:{scope}:{key}"

    sentinel_spark = object()

    class _FakeSparkSession:
        @staticmethod
        def getActiveSession():
            return sentinel_spark

    fake_dbutils_module = types.SimpleNamespace(DBUtils=_FakeDBUtils)
    fake_sql_module = types.SimpleNamespace(SparkSession=_FakeSparkSession)
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", fake_dbutils_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    result = runtime_mod._resolve_databricks_secret("scope-b", "key-b")

    assert result == "pyspark:scope-b:key-b"
    assert captured == [(sentinel_spark, "scope-b", "key-b")]


def test_resolve_databricks_secret_raises_when_no_helpers_available(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", None)

    with pytest.raises(RuntimeError, match="Unable to import Databricks secret helpers"):
        runtime_mod._resolve_databricks_secret("scope-c", "key-c")


def test_resolve_databricks_secret_uses_default_session_when_active_session_is_none(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)

    captured: list[tuple[object, str, str]] = []

    class _FakeDBUtils:
        def __init__(self, spark):
            self._spark = spark

            def get_secret(scope, key):
                captured.append((self._spark, scope, key))
                return f"default:{scope}:{key}"

            self.secrets = types.SimpleNamespace(
                get=get_secret,
            )

    sentinel_spark = object()

    class _FakeSparkSession:
        @staticmethod
        def getActiveSession():
            return None

        @staticmethod
        def getDefaultSession():
            return sentinel_spark

    monkeypatch.setitem(
        sys.modules, "pyspark.dbutils", types.SimpleNamespace(DBUtils=_FakeDBUtils)
    )
    monkeypatch.setitem(
        sys.modules, "pyspark.sql", types.SimpleNamespace(SparkSession=_FakeSparkSession)
    )

    result = runtime_mod._resolve_databricks_secret("scope-d", "key-d")

    assert result == "default:scope-d:key-d"
    assert captured == [(sentinel_spark, "scope-d", "key-d")]


def test_resolve_databricks_secret_raises_when_no_spark_session_can_be_created(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)

    class _FailingBuilder:
        def getOrCreate(self):
            raise RuntimeError("no cluster")

    class _FakeSparkSession:
        builder = _FailingBuilder()
        _instantiatedSession = None

        @staticmethod
        def getActiveSession():
            return None

    class _UnusedDBUtils:
        def __init__(self, spark):
            raise AssertionError("DBUtils must not be constructed without a spark session")

    monkeypatch.setitem(
        sys.modules, "pyspark.dbutils", types.SimpleNamespace(DBUtils=_UnusedDBUtils)
    )
    monkeypatch.setitem(
        sys.modules, "pyspark.sql", types.SimpleNamespace(SparkSession=_FakeSparkSession)
    )

    with pytest.raises(RuntimeError, match="SparkSession is required"):
        runtime_mod._resolve_databricks_secret("scope-e", "key-e")


# ---------------------------------------------------------------------------
# build_runtime_config: offline-bundle promotion
# ---------------------------------------------------------------------------


def _make_bundle(path: Path) -> Path:
    """Create a minimal valid zip at ``path`` for probe-based tests."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("README.txt", "stub bundle")
    return path


def test_build_runtime_config_promotes_to_offline_when_explicit_path_given(tmp_path):
    bundle = _make_bundle(tmp_path / "explicit-bundle.zip")
    config = build_runtime_config(
        {
            "license_volume_path": str(tmp_path / "license"),
            KDBX_INSTALL_BEARER_TOKEN_OPTION: "tok",
            KDBX_LICENSE_B64_OPTION: "lic",
            KDBX_OFFLINE_BUNDLE_PATH_OPTION: str(bundle),
        }
    )

    assert config.uses_offline_bundle is True
    assert config.offline_bundle_path == str(bundle)
    assert config.license_b64 == "lic"
    # Keep the bearer token so worker-side installs can fall back to online mode.
    assert config.installer_bearer_token == "tok"


def test_build_runtime_config_allows_offline_bundle_with_license_only(tmp_path):
    bundle = _make_bundle(tmp_path / "explicit-bundle.zip")
    config = build_runtime_config(
        {
            "license_volume_path": str(tmp_path / "license"),
            KDBX_LICENSE_B64_OPTION: "lic",
            KDBX_OFFLINE_BUNDLE_PATH_OPTION: str(bundle),
        }
    )

    assert config.uses_offline_bundle is True
    assert config.offline_bundle_path == str(bundle)
    assert config.license_b64 == "lic"
    assert config.installer_bearer_token is None


def test_build_runtime_config_promotes_to_offline_via_secret_path(tmp_path):
    bundle = _make_bundle(tmp_path / "explicit-bundle.zip")
    resolved = {
        ("scope", "tok_key"): "tok",
        ("scope", "lic_key"): "lic",
    }
    config = build_runtime_config(
        {
            "license_volume_path": str(tmp_path / "license"),
            KDBX_SECRET_SCOPE_OPTION: "scope",
            KDBX_INSTALL_BEARER_SECRET_KEY_OPTION: "tok_key",
            KDBX_LICENSE_B64_SECRET_KEY_OPTION: "lic_key",
            KDBX_OFFLINE_BUNDLE_PATH_OPTION: str(bundle),
        },
        secret_resolver=lambda scope, key: resolved[(scope, key)],
    )

    assert config.uses_offline_bundle is True
    assert config.offline_bundle_path == str(bundle)
    assert config.license_b64 == "lic"
    assert config.installer_bearer_token == "tok"


def test_build_runtime_config_online_mode_ignores_offline_bundle(tmp_path):
    bundle = _make_bundle(tmp_path / "explicit-bundle.zip")
    config = build_runtime_config(
        {
            "license_volume_path": str(tmp_path / "license"),
            KDBX_INSTALL_BEARER_TOKEN_OPTION: "tok",
            KDBX_LICENSE_B64_OPTION: "lic",
            KDBX_OFFLINE_BUNDLE_PATH_OPTION: str(bundle),
            KDBX_INSTALL_MODE_OPTION: "online",
        }
    )

    assert config.uses_installer is True
    assert config.uses_offline_bundle is False
    assert config.offline_bundle_path is None
    assert config.installer_bearer_token == "tok"
    assert config.license_b64 == "lic"


def test_build_runtime_config_online_mode_works_with_secret_options(tmp_path):
    bundle = _make_bundle(tmp_path / "explicit-bundle.zip")
    resolved = {
        ("scope", "tok_key"): "tok",
        ("scope", "lic_key"): "lic",
    }
    config = build_runtime_config(
        {
            "license_volume_path": str(tmp_path / "license"),
            KDBX_SECRET_SCOPE_OPTION: "scope",
            KDBX_INSTALL_BEARER_SECRET_KEY_OPTION: "tok_key",
            KDBX_LICENSE_B64_SECRET_KEY_OPTION: "lic_key",
            KDBX_OFFLINE_BUNDLE_PATH_OPTION: str(bundle),
            KDBX_INSTALL_MODE_OPTION: "online",
        },
        secret_resolver=lambda scope, key: resolved[(scope, key)],
    )

    assert config.uses_installer is True
    assert config.uses_offline_bundle is False
    assert config.offline_bundle_path is None
    assert config.installer_bearer_token == "tok"
    assert config.license_b64 == "lic"


def test_build_runtime_config_probes_bundle_alongside_license_dir(tmp_path):
    license_dir = tmp_path / "key_kdbx"
    license_dir.mkdir()
    bundle = _make_bundle(license_dir / DEFAULT_OFFLINE_BUNDLE_NAME)
    config = build_runtime_config(
        {
            "license_volume_path": str(license_dir),
            KDBX_INSTALL_BEARER_TOKEN_OPTION: "tok",
            KDBX_LICENSE_B64_OPTION: "lic",
        }
    )

    assert config.offline_bundle_path == str(bundle)
    assert config.uses_offline_bundle is True


def test_build_runtime_config_probes_bundle_in_sibling_kdbx_directory(tmp_path):
    license_dir = tmp_path / "key_kdbx"
    license_dir.mkdir()
    bundle = _make_bundle(tmp_path / "kdbx" / DEFAULT_OFFLINE_BUNDLE_NAME)
    config = build_runtime_config(
        {
            "license_volume_path": str(license_dir),
            KDBX_INSTALL_BEARER_TOKEN_OPTION: "tok",
            KDBX_LICENSE_B64_OPTION: "lic",
        }
    )

    assert config.offline_bundle_path == str(bundle)
    assert config.uses_offline_bundle is True


def test_build_runtime_config_does_not_guess_global_bundle_path(tmp_path):
    config = build_runtime_config(
        {
            "license_volume_path": str(tmp_path / "license-missing"),
            KDBX_INSTALL_BEARER_TOKEN_OPTION: "tok",
            KDBX_LICENSE_B64_OPTION: "lic",
        }
    )

    assert config.offline_bundle_path is None
    assert config.uses_offline_bundle is False
    assert config.uses_installer is True
    assert config.installer_bearer_token == "tok"


def test_probe_offline_bundle_returns_none_for_empty_directory():
    assert runtime_mod._probe_offline_bundle("") is None


def test_probe_offline_bundle_returns_none_when_no_candidates_exist(tmp_path):
    assert runtime_mod._probe_offline_bundle(str(tmp_path / "nope")) is None


def test_probe_offline_bundle_prefers_arm_bundle_on_arm64(monkeypatch, tmp_path):
    license_dir = tmp_path / "key_kdbx"
    license_dir.mkdir()
    kdbx_dir = tmp_path / "kdbx"
    x86_bundle = _make_bundle(kdbx_dir / DEFAULT_OFFLINE_BUNDLE_NAME)
    arm_bundle = _make_bundle(kdbx_dir / ARM_OFFLINE_BUNDLE_NAME)
    monkeypatch.setattr(runtime_mod.platform, "machine", lambda: "aarch64")

    assert runtime_mod._probe_offline_bundle(str(license_dir)) == str(arm_bundle)
    assert x86_bundle.is_file()


def test_probe_offline_bundle_swallows_filesystem_errors(monkeypatch, tmp_path):
    def _raise(self):
        raise OSError("permission denied")

    monkeypatch.setattr(Path, "is_file", _raise)
    assert runtime_mod._probe_offline_bundle(str(tmp_path)) is None


def test_maybe_offline_returns_config_unchanged_when_license_missing():
    cfg = PyKxRuntimeConfig(license_directory="/tmp/lic")
    assert runtime_mod._maybe_offline(cfg, "/tmp/bundle.zip") is cfg


# ---------------------------------------------------------------------------
# _install_kdbx dispatcher and offline install
# ---------------------------------------------------------------------------


def test_install_kdbx_dispatches_to_offline_when_configured(monkeypatch):
    online_calls: list[PyKxRuntimeConfig] = []
    offline_calls: list[PyKxRuntimeConfig] = []
    monkeypatch.setattr(
        runtime_mod, "_install_kdbx_online", lambda cfg: online_calls.append(cfg)
    )
    monkeypatch.setattr(
        runtime_mod, "_install_kdbx_offline", lambda cfg: offline_calls.append(cfg)
    )

    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        license_b64="lic",
        offline_bundle_path="/tmp/bundle.zip",
    )
    runtime_mod._install_kdbx(config)

    assert offline_calls == [config]
    assert online_calls == []


def test_install_kdbx_dispatches_to_online_when_no_bundle(monkeypatch):
    online_calls: list[PyKxRuntimeConfig] = []
    offline_calls: list[PyKxRuntimeConfig] = []
    monkeypatch.setattr(
        runtime_mod, "_install_kdbx_online", lambda cfg: online_calls.append(cfg)
    )
    monkeypatch.setattr(
        runtime_mod, "_install_kdbx_offline", lambda cfg: offline_calls.append(cfg)
    )

    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        installer_bearer_token="tok",
        license_b64="lic",
    )
    runtime_mod._install_kdbx(config)

    assert online_calls == [config]
    assert offline_calls == []


def test_install_kdbx_keeps_offline_errors_when_bundle_unavailable(monkeypatch):
    online_calls: list[PyKxRuntimeConfig] = []

    def offline_raises(_config):
        raise RuntimeError("bundle unavailable")

    monkeypatch.setattr(runtime_mod, "_install_kdbx_offline", offline_raises)
    monkeypatch.setattr(
        runtime_mod, "_install_kdbx_online", lambda cfg: online_calls.append(cfg)
    )

    config = PyKxRuntimeConfig(
        license_directory="/tmp/lic",
        installer_bearer_token="tok",
        license_b64="lic",
        offline_bundle_path="/tmp/bundle.zip",
    )
    with pytest.raises(RuntimeError, match="bundle unavailable"):
        runtime_mod._install_kdbx(config)

    assert online_calls == []


def _make_offline_bundle(zip_path: Path, *, with_installer: bool = True) -> Path:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w") as zf:
        if with_installer:
            zf.writestr("install_kdb.sh", "#!/bin/bash\necho stub\n")
        zf.writestr("notes.txt", "bundle stub")
    return zip_path


def test_install_kdbx_offline_invokes_bash_offline_install(monkeypatch, tmp_path):
    runtime_home = tmp_path / "home"
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: runtime_home)
    bundle = _make_offline_bundle(tmp_path / "l64-bundle.zip")
    recorder = _SubprocessRecorder([(0, "ok", "")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        license_b64="lic-b64",
        offline_bundle_path=str(bundle),
    )

    runtime_mod._install_kdbx_offline(config)

    assert runtime_home.is_dir()
    assert (runtime_home / ".kx").is_dir()
    assert len(recorder.calls) == 1
    call = recorder.calls[0]
    assert call["args"][0] == "bash"
    assert Path(call["args"][1]).name == "install_kdb.sh"
    assert "--offline" in call["args"]
    # `-y` is required so install_kdb.sh runs non-interactively and does not
    # hang waiting on a prompt that no one will answer.
    assert "-y" in call["args"]
    assert call["args"][-2] == "--b64lic"
    assert call["args"][-1] == "lic-b64"
    assert call["env"]["HOME"] == str(runtime_home)
    assert call["env"]["TERM"] == "dumb"
    assert call.get("timeout") == 1200


def test_localize_offline_bundle_copies_volume_path_to_temp(monkeypatch, tmp_path):
    source = _make_offline_bundle(tmp_path / "Volumes" / "bundle" / "l64-bundle.zip")
    monkeypatch.setattr(
        runtime_mod.tempfile,
        "mkdtemp",
        lambda prefix: str(tmp_path / f"{prefix}abc123"),
    )

    localized = runtime_mod._localize_offline_bundle(str(source))

    assert localized == tmp_path / f"kdbx-bundles-{os.getpid()}-abc123" / "l64-bundle.zip"
    assert localized.is_file()


def test_localize_offline_bundle_falls_back_to_dbutils(monkeypatch, tmp_path):
    source = "/Volumes/main/rkh/rbc_kx/kdbx/l64-bundle.zip"
    monkeypatch.setattr(
        runtime_mod.tempfile,
        "mkdtemp",
        lambda prefix: str(tmp_path / f"{prefix}abc123"),
    )

    copied = []

    def fake_copy(src, dst):
        copied.append((src, dst))
        target = Path(dst.removeprefix("file:"))
        _make_offline_bundle(target)

    monkeypatch.setattr(runtime_mod, "_copy_with_dbutils", fake_copy)

    localized = runtime_mod._localize_offline_bundle(source)

    assert localized.is_file()
    assert copied[0] == (f"file:{source}", f"file:{localized}")


def test_install_kdbx_offline_raises_when_bundle_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    monkeypatch.setattr(
        runtime_mod.subprocess,
        "run",
        lambda *a, **kw: pytest.fail("subprocess.run must not run when bundle is missing"),
    )

    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        license_b64="lic",
        offline_bundle_path=str(tmp_path / "missing-bundle.zip"),
    )

    with pytest.raises(RuntimeError, match="could not be localized"):
        runtime_mod._install_kdbx_offline(config)


def test_install_kdbx_offline_raises_when_zip_is_invalid(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    bogus = tmp_path / "bogus.zip"
    bogus.write_text("not a zip")

    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        license_b64="lic",
        offline_bundle_path=str(bogus),
    )

    with pytest.raises(RuntimeError, match="not a valid zip"):
        runtime_mod._install_kdbx_offline(config)


def test_install_kdbx_offline_raises_when_bundle_lacks_installer(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    bundle = _make_offline_bundle(tmp_path / "no-installer.zip", with_installer=False)

    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        license_b64="lic",
        offline_bundle_path=str(bundle),
    )

    with pytest.raises(RuntimeError, match="install_kdb.sh"):
        runtime_mod._install_kdbx_offline(config)


def test_install_kdbx_offline_raises_when_install_command_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_mod, "_runtime_home_directory", lambda: tmp_path / "home")
    bundle = _make_offline_bundle(tmp_path / "l64-bundle.zip")
    recorder = _SubprocessRecorder([(7, "stdout-trail", "stderr-trail")])
    monkeypatch.setattr(runtime_mod.subprocess, "run", recorder)

    config = PyKxRuntimeConfig(
        license_directory=str(tmp_path / "lic"),
        license_b64="lic",
        offline_bundle_path=str(bundle),
    )

    with pytest.raises(
        RuntimeError, match=r"install_kdb.sh --offline failed.*stdout-trail.*stderr-trail"
    ):
        runtime_mod._install_kdbx_offline(config)


def test_resolve_databricks_secret_raises_when_builder_returns_none(monkeypatch):
    """Defensive guard: builder.getOrCreate() returns None instead of a session."""
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)

    class _NullBuilder:
        def getOrCreate(self):
            return None

    class _FakeSparkSession:
        builder = _NullBuilder()
        _instantiatedSession = None

        @staticmethod
        def getActiveSession():
            return None

    class _UnusedDBUtils:
        def __init__(self, spark):
            raise AssertionError("DBUtils must not be constructed without a spark session")

    monkeypatch.setitem(
        sys.modules, "pyspark.dbutils", types.SimpleNamespace(DBUtils=_UnusedDBUtils)
    )
    monkeypatch.setitem(
        sys.modules, "pyspark.sql", types.SimpleNamespace(SparkSession=_FakeSparkSession)
    )

    with pytest.raises(RuntimeError, match="SparkSession is required"):
        runtime_mod._resolve_databricks_secret("scope-f", "key-f")
