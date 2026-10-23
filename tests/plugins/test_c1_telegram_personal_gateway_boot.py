"""C1 initializer registration across the real gateway boot order (no pre-written PID file)."""
from __future__ import annotations

import asyncio
import json
import importlib.util
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway import run_inbound_consumer
from gateway.restart import GATEWAY_FATAL_CONFIG_EXIT_CODE
from tests.waiting import wait_until

FAKE_BOT_TOKEN = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ_1234567890-abcd"
OWNER_ID = 8509844663


class _Manager:
    def __init__(self, loaded):
        self._plugins = {} if loaded is None else {"c1_telegram_personal": loaded}
        self.loads = []
        self._hooks = {}
        self._discovered = True

    def _load_plugin(self, manifest):
        self.loads.append(manifest)
        self._plugins["c1_telegram_personal"] = SimpleNamespace(
            manifest=manifest, enabled=True, error=None,
        )

    def has_hook(self, name):
        return bool(self._hooks.get(name))

    def iter_hook_callbacks(self, name):
        return tuple(self._hooks.get(name, ()))


def _loaded(*, source="bundled", enabled=False, error="C1 initializer requires this profile's live Gateway."):
    return SimpleNamespace(
        manifest=SimpleNamespace(name="c1_telegram_personal", source=source),
        enabled=enabled,
        error=error,
    )


@pytest.mark.parametrize(
    ("loaded", "expected_loads"),
    [
        (_loaded(), 1),
        (None, 0),
        (_loaded(enabled=True, error=None), 0),
        (_loaded(error=None), 0),
        (_loaded(error="C1 Telegram credentials or allowlist are invalid."), 0),
        (_loaded(source="user"), 0),
    ],
)
def test_retry_only_reloads_a_failed_bundled_c1_initializer(monkeypatch, loaded, expected_loads):
    manager = _Manager(loaded)
    import hermes_cli.plugins

    monkeypatch.setattr(hermes_cli.plugins, "get_plugin_manager", lambda: manager)

    assert run_inbound_consumer.retry_c1_binding_after_pid_claim() is (expected_loads == 1)
    assert len(manager.loads) == expected_loads


def test_gateway_startup_retries_c1_initializer_after_discovery(monkeypatch):
    import hermes_cli.plugins
    from gateway.run_startup import GatewayStartupMixin

    calls = []
    monkeypatch.setattr(hermes_cli.plugins, "discover_plugins", lambda *a, **k: calls.append("discover"))
    monkeypatch.setattr(
        run_inbound_consumer, "retry_c1_binding_after_pid_claim", lambda: calls.append("retry"),
    )

    GatewayStartupMixin._start_register_plugins_relay_hooks()

    assert calls[:2] == ["discover", "retry"]


def _git_commit(repo: Path, message: str) -> str:
    subprocess.run(
        ["git", "-C", str(repo), "add", "--", "gateway", "hermes_cli", "plugins"],
        check=True,
    )
    subprocess.run(
        [
            "git", "-C", str(repo), "-c", "user.name=Test",
            "-c", "user.email=test@example.invalid", "commit", "-qm", message,
        ],
        check=True,
    )
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def _load_copied_plugin(candidate: Path):
    source = Path(__file__).resolve().parents[2] / "plugins/c1_telegram_personal/__init__.py"
    target = candidate / "plugins/c1_telegram_personal/__init__.py"
    target.parent.mkdir(parents=True)
    target.write_bytes(source.read_bytes())
    spec = importlib.util.spec_from_file_location(
        "_test_c1_planb_gateway_boot_plugin", target,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _PluginContext:
    profile_name = "chief"

    def __init__(self, settings):
        self.settings = settings
        self.hooks = {}

    def get_config(self, name, default=None):
        return self.settings.get(name, default)

    def register_hook(self, name, callback):
        self.hooks[name] = callback
        return SimpleNamespace(dispose=lambda: None)

    def on_unload(self, callback):
        return SimpleNamespace(dispose=lambda: None)


def test_enabled_c1_pin_mismatch_aborts_before_adapter_connect(monkeypatch, tmp_path, caplog):
    candidate = tmp_path / "hermes"
    (candidate / "gateway").mkdir(parents=True)
    (candidate / "hermes_cli").mkdir()
    (candidate / "gateway" / "core.py").write_text("PINNED = True\n")
    gateway_file = candidate / "hermes_cli" / "gateway.py"
    gateway_file.write_text("PINNED = True\n")
    plugin = _load_copied_plugin(candidate)
    subprocess.run(["git", "-C", str(candidate), "init", "-q"], check=True)
    pin = _git_commit(candidate, "pin")

    gateway_file.write_text("PINNED = False\n")
    head = _git_commit(candidate, "protected change")
    assert head != pin

    agent_ops = tmp_path / "agent-ops"
    package = agent_ops / "tools" / "agent_ops_c1"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "hermes_production.py").write_text("PINNED = True\n")
    (package / "operator_ops.py").write_text(
        "from pathlib import Path\n"
        "import subprocess\n"
        f'_HERMES_PIN = "{pin}"\n'
        "def _gateway_pin_ok(repo):\n"
        "    root = Path(repo)\n"
        "    found = subprocess.run(\n"
        "        ['git', '-C', str(root), 'rev-parse', '--verify', f'{_HERMES_PIN}^{{commit}}'],\n"
        "        check=True, capture_output=True, text=True,\n"
        "    ).stdout.strip()\n"
        "    if found != _HERMES_PIN:\n"
        "        return False\n"
        "    return subprocess.run(\n"
        "        ['git', '-C', str(root), 'diff', '--quiet', _HERMES_PIN, '--',\n"
        "         'gateway', 'hermes_cli', 'plugins/c1_telegram_personal'],\n"
        "        check=False,\n"
        "    ).returncode == 0\n"
    )

    profile_home = tmp_path / "home" / ".hermes" / "profiles" / "chief"
    profile_home.mkdir(parents=True)
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    plugin.get_hermes_home = lambda: profile_home
    context = _PluginContext({
        "enabled": True,
        "agent_ops_repo": str(agent_ops),
        "source_repo": str(source_repo),
    })
    with pytest.raises(RuntimeError) as failure:
        plugin.register(context)
    assert str(failure.value) == "C1 Gateway source pin is not satisfied."

    loaded = SimpleNamespace(
        manifest=SimpleNamespace(name="c1_telegram_personal", source="bundled"),
        enabled=False,
        error=str(failure.value),
        module=plugin,
    )
    manager = _Manager(loaded)

    import hermes_cli.plugins
    import hermes_cli.plugins_discovery
    import hermes_constants
    import gateway.code_skew

    monkeypatch.setattr(hermes_cli.plugins, "get_plugin_manager", lambda: manager)
    (profile_home / "gateway.pid").write_text(json.dumps({"pid": os.getpid()}))
    startup_steps = []

    config = {
        "plugins": {
            "enabled": ["c1_telegram_personal"],
            "entries": {
                "c1_telegram_personal": {
                    "settings": {
                        "enabled": True,
                        "agent_ops_repo": str(agent_ops),
                        "source_repo": str(source_repo),
                    },
                },
            },
        },
    }
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: config)
    monkeypatch.setattr(
        hermes_cli.plugins_discovery, "_get_enabled_plugins",
        lambda: {"c1_telegram_personal"},
    )
    monkeypatch.setattr(
        hermes_cli.plugins_discovery, "_get_disabled_plugins", lambda: set(),
    )
    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: profile_home)
    monkeypatch.setattr(gateway.code_skew, "current_code_sha", lambda: head)

    error = run_inbound_consumer.c1_binding_startup_error()
    assert error is not None
    assert str(failure.value) in error
    assert f"HEAD={head}" in error
    assert f"expected_pin={pin}" in error
    assert "callback_count=0" in error

    from gateway.run import GatewayStartupMixin

    class Probe(GatewayStartupMixin):
        def __init__(self):
            self._shutdown_event = asyncio.Event()
            self._restart_requested = False
            self._draining = False
            self._stop_task = None
            self._exit_code = None
            self._exit_cleanly = False
            self._exit_reason = None
            self._startup_restore_in_progress = True

        def _request_clean_exit(self, reason):
            self._exit_cleanly = True
            self._exit_reason = reason
            self._shutdown_event.set()

        def _start_install_faulthandler(self):
            return None

        async def _start_log_startup_environment(self):
            return None

        def _start_check_access_policy(self):
            return False

        def _start_register_plugins_relay_hooks(self):
            startup_steps.append("retry")
            assert run_inbound_consumer.retry_c1_binding_after_pid_claim() is False
            assert manager.loads == []
            assert manager.iter_hook_callbacks(run_inbound_consumer.POST_ADMISSION_HOOK) == ()

        async def _start_prefilter_platforms(self):
            startup_steps.append("adapter_prefilter")
            raise AssertionError("adapter path must not run after C1 startup failure")

        async def _start_flush_runtime_status(self):
            return None

    probe = Probe()

    original_startup_error = run_inbound_consumer.c1_binding_startup_error

    def record_startup_error():
        startup_steps.append("binding_check")
        return original_startup_error()

    monkeypatch.setattr(
        run_inbound_consumer, "c1_binding_startup_error", record_startup_error,
    )

    def no_runtime_status(**_kwargs):
        return None

    monkeypatch.setattr("gateway.run._write_runtime_status_quiet", no_runtime_status)
    caplog.set_level(logging.ERROR, logger="gateway.run")
    assert asyncio.run(probe.start()) is True
    assert probe._exit_code == GATEWAY_FATAL_CONFIG_EXIT_CODE
    assert probe._shutdown_event.is_set()
    assert startup_steps == ["retry", "binding_check"]
    assert not (profile_home / "c1-telegram-dispatch.sqlite3").exists()
    assert not (profile_home / "c1" / "c1.sqlite3").exists()
    assert any(
        record.levelno == logging.ERROR and str(failure.value) in record.getMessage()
        for record in caplog.records
    )


def _private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)


# Spawns and reaps its own gateway in a private HOME with no platforms or network.
@pytest.mark.spawns_gateway_lookalike
def test_real_gateway_boot_writes_binding_marker_for_its_own_pid(tmp_path):
    agent_ops_repo = os.environ.get("HERMES_TEST_C1_AGENT_OPS_REPO")
    source_repo = os.environ.get("HERMES_TEST_C1_SOURCE_REPO")
    if not agent_ops_repo or not source_repo:
        pytest.skip(
            "SKIPPED_NEEDS_ENV: set HERMES_TEST_C1_AGENT_OPS_REPO and "
            "HERMES_TEST_C1_SOURCE_REPO to isolated checkouts"
        )
    hermes_repo = Path(__file__).resolve().parents[2]
    home = tmp_path / "home"
    hermes_home = home / ".hermes"
    profile_home = hermes_home / "profiles" / "chief"
    for directory in (home, hermes_home, profile_home.parent, profile_home, profile_home / "secrets"):
        _private_dir(directory)
    key_path = profile_home / "secrets" / "c1_telegram_hmac_sha256"
    key_path.write_bytes(bytes(range(32)))
    os.chmod(key_path, 0o600)
    env_file = profile_home / ".env"
    env_file.write_text(
        f"TELEGRAM_ALLOWED_USERS={OWNER_ID}\nTELEGRAM_BOT_TOKEN={FAKE_BOT_TOKEN}\n",
        encoding="utf-8",
    )
    os.chmod(env_file, 0o600)
    (profile_home / "config.yaml").write_text(
        "gateway:\n"
        "  standalone: true\n"
        "platforms:\n"
        "  telegram:\n"
        "    enabled: false\n"
        "plugins:\n"
        "  enabled:\n"
        "  - c1_telegram_personal\n"
        "  entries:\n"
        "    c1_telegram_personal:\n"
        "      settings:\n"
        "        enabled: true\n"
        f"        agent_ops_repo: {agent_ops_repo}\n"
        f"        source_repo: {source_repo}\n",
        encoding="utf-8",
    )
    blackhole = "http://127.0.0.1:9"
    env = {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": str(home),
        "XDG_STATE_HOME": str(home / "state"),
        "HERMES_HOME": str(hermes_home),
        "HERMES_GATEWAY_LOCK_DIR": str(home / "state" / "locks"),
        "HERMES_DISABLE_LAZY_INSTALLS": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        # The production guard stays enabled; only this tmp-HOME child may opt out.
        "HERMES_TEST_ONLY_ALLOW_C1_GATEWAY_BOOT_IN_TMP_HOME": "1",
        # The child's HERMES_HOME is this test's tmp_path, not a live install.
        "HERMES_STATE_DB_GUARD_BYPASS": "1",
        "HTTPS_PROXY": blackhole,
        "HTTP_PROXY": blackhole,
        "ALL_PROXY": blackhole,
    }
    output = tmp_path / "gateway-run.log"
    with output.open("wb") as stream:
        process = subprocess.Popen(
            # The live command line must read as a chief gateway for the strict PID identity check.
            [sys.executable, "-m", "hermes_cli.main", "--profile", "chief", "gateway", "run", "--no-supervise"],
            cwd=hermes_repo, env=env, stdout=stream, stderr=subprocess.STDOUT,
        )
    marker = profile_home / "c1" / "c1_telegram_personal.binding.json"
    pid_file = profile_home / "gateway.pid"
    try:
        wait_until(
            lambda: marker.exists() or process.poll() is not None,
            timeout=90,
            what="C1 binding registration",
        )
        preflight = subprocess.run(
            [sys.executable, "-m", "tools.agent_ops_c1.operator_ops", "preflight"],
            cwd=agent_ops_repo,
            env={
                "PATH": env["PATH"],
                "HOME": str(home),
                "PYTHONDONTWRITEBYTECODE": "1",
                "C1_PROFILE_HOME": str(profile_home),
                "C1_HERMES_REPO": str(hermes_repo),
                "C1_HERMES_PYTHON": sys.executable,
            },
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        expected_preflight = [
            "PASS allowlist",
            "PASS hmac_file",
            "PASS c1_db_writable",
            "PASS binding_registered",
            "PASS gateway_version",
        ]
        assert marker.exists(), (
            "gateway boot did not write the C1 binding marker:\n"
            + output.read_text(encoding="utf-8", errors="replace")[-2000:]
            + "\npreflight:\n" + preflight.stdout + preflight.stderr
        )
        assert preflight.returncode == 0, preflight.stdout + preflight.stderr
        assert preflight.stdout.splitlines() == expected_preflight
        record = json.loads(marker.read_text(encoding="utf-8"))
        pid_record = json.loads(pid_file.read_text(encoding="utf-8"))
        assert record["state"] == "registered"
        assert record["pid"] == process.pid
        assert pid_record["pid"] == process.pid
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
