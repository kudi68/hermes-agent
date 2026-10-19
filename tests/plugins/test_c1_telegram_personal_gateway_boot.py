"""C1 initializer registration across the real gateway boot order (no pre-written PID file)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway import run_inbound_consumer
from tests.waiting import wait_until

FAKE_BOT_TOKEN = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ_1234567890-abcd"
OWNER_ID = 8509844663


class _Manager:
    def __init__(self, loaded):
        self._plugins = {} if loaded is None else {"c1_telegram_personal": loaded}
        self.loads = []

    def _load_plugin(self, manifest):
        self.loads.append(manifest)
        self._plugins["c1_telegram_personal"] = SimpleNamespace(
            manifest=manifest, enabled=True, error=None,
        )


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


def _private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def test_real_gateway_boot_writes_binding_marker_for_its_own_pid(tmp_path):
    agent_ops_repo = os.environ.get("HERMES_TEST_C1_AGENT_OPS_REPO")
    source_repo = os.environ.get("HERMES_TEST_C1_SOURCE_REPO")
    if not agent_ops_repo or not source_repo:
        pytest.skip(
            "set HERMES_TEST_C1_AGENT_OPS_REPO and "
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
        "HERMES_TEST_C1_STARTUP_DONE": str(tmp_path / "startup-plugin-discovery.done"),
        # The child's HERMES_HOME is this test's tmp_path, not a live install.
        "HERMES_STATE_DB_GUARD_BYPASS": "1",
        "HTTPS_PROXY": blackhole,
        "HTTP_PROXY": blackhole,
        "ALL_PROXY": blackhole,
    }
    launcher = (
        "import os, sys\n"
        "from pathlib import Path\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "from gateway.run_startup import GatewayStartupMixin\n"
        "_original = GatewayStartupMixin._start_register_plugins_relay_hooks\n"
        "def _observed():\n"
        "    _original()\n"
        "    Path(os.environ['HERMES_TEST_C1_STARTUP_DONE']).touch()\n"
        "GatewayStartupMixin._start_register_plugins_relay_hooks = staticmethod(_observed)\n"
        "sys.argv = ['hermes', '--profile', 'chief', 'gateway', 'run', '--no-supervise']\n"
        "from hermes_cli.main import main\n"
        "sys.exit(main())\n"
    )
    output = tmp_path / "gateway-run.log"
    with output.open("wb") as stream:
        process = subprocess.Popen(
            [sys.executable, "-I", "-c", launcher, str(hermes_repo)],
            cwd=hermes_repo, env=env, stdout=stream, stderr=subprocess.STDOUT,
        )
    marker = profile_home / "c1" / "c1_telegram_personal.binding.json"
    startup_done = tmp_path / "startup-plugin-discovery.done"
    pid_file = profile_home / "gateway.pid"
    try:
        wait_until(
            lambda: marker.exists() or startup_done.exists() or process.poll() is not None,
            timeout=90,
            what="C1 binding registration or post-PID plugin discovery",
        )
        if startup_done.exists():
            assert pid_file.is_file(), "gateway completed plugin discovery without its PID file"
            pid_record = json.loads(pid_file.read_text(encoding="utf-8"))
            assert pid_record["pid"] == process.pid
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
            "PASS gateway_version_4f36202",
        ]
        assert marker.exists(), (
            "gateway completed post-PID plugin discovery without the C1 binding marker:\n"
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
