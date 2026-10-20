"""Offline initializer-to-durable-readback rehearsal for C1 Telegram."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


FAKE_BOT_TOKEN = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ_1234567890-abcd"
OWNER_ID = 8509844663
REQUEST = "回傳 kudi68/personal-ai-os 的 NEXT.md 第一行"


class _Registration:
    def __init__(self, callback):
        self.callback = callback
        self.disposed = False

    def dispose(self):
        self.disposed = True


class _PluginContext:
    profile_name = "chief"

    def __init__(self, settings):
        self.settings = settings
        self.hooks = {}
        self.unload_callbacks = []

    def get_config(self, name, default=None):
        return self.settings.get(name, default)

    def register_hook(self, name, callback):
        registration = _Registration(callback)
        self.hooks.setdefault(name, []).append(registration)
        return registration

    def on_unload(self, callback):
        self.unload_callbacks.append(callback)
        return _Registration(callback)


class _FailingUnloadContext(_PluginContext):
    def on_unload(self, _callback):
        raise RuntimeError("unload callback registration failed")


class _FakeHttpResponse:
    status_code = 200

    def json(self):
        return {"ok": True, "result": {"message_id": 991}}


class _FakeHttpClient:
    def __init__(self, sent):
        self.sent = sent

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def post(self, _url, *, json):
        self.sent.append(json)
        return _FakeHttpResponse()


class _FakeC1Dispatch:
    def __init__(self, request_id):
        self.request_id = request_id

    def check_active(self):
        return None

    def run_effect(self, effect, *args, **kwargs):
        return asyncio.run(effect(*args, **kwargs))


def _private_dir(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def _load_plugin(monkeypatch, plugin_file, profile_home, fake_secrets):
    import agent.secret_scope
    import hermes_constants

    monkeypatch.setattr(
        hermes_constants, "get_hermes_home", lambda: profile_home,
    )
    monkeypatch.setattr(
        agent.secret_scope,
        "get_secret",
        lambda name, default=None: fake_secrets.get(name, default),
    )
    spec = importlib.util.spec_from_file_location(
        "_test_c1_telegram_personal_plugin", plugin_file,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_initializer_is_disabled_without_explicit_enable(monkeypatch, tmp_path):
    plugin_file = (
        Path(__file__).resolve().parents[2]
        / "plugins/c1_telegram_personal/__init__.py"
    )
    plugin = _load_plugin(monkeypatch, plugin_file, tmp_path, {})
    context = _PluginContext({})

    assert plugin.register(context) is None
    assert context.hooks == {}
    assert context.unload_callbacks == []


def test_initializer_fails_closed_when_source_checkout_is_missing(
    monkeypatch, tmp_path,
):
    plugin_file = (
        Path(__file__).resolve().parents[2]
        / "plugins/c1_telegram_personal/__init__.py"
    )
    plugin = _load_plugin(monkeypatch, plugin_file, tmp_path, {})
    context = _PluginContext({"enabled": True})

    with pytest.raises(RuntimeError, match="C1 pinned source checkout is not configured"):
        plugin.register(context)
    assert context.hooks == {}
    assert context.unload_callbacks == []


def test_initializer_binds_trusted_request_sends_once_and_reads_pinned_source(
    monkeypatch, tmp_path,
):
    agent_ops_repo = os.environ.get("HERMES_TEST_C1_AGENT_OPS_REPO")
    source_repo = os.environ.get("HERMES_TEST_C1_SOURCE_REPO")
    if not agent_ops_repo or not source_repo:
        pytest.skip(
            "set HERMES_TEST_C1_AGENT_OPS_REPO and "
            "HERMES_TEST_C1_SOURCE_REPO to isolated checkouts"
        )

    hermes_repo = Path(__file__).resolve().parents[2]
    profile_home = tmp_path / "hermes" / "profiles" / "chief"
    for directory in (profile_home.parent.parent, profile_home.parent, profile_home):
        _private_dir(directory)
    secrets_dir = profile_home / "secrets"
    _private_dir(secrets_dir)
    key_path = secrets_dir / "c1_telegram_hmac_sha256"
    key_path.write_bytes(bytes(range(32)))
    os.chmod(key_path, 0o600)
    env_file = profile_home / ".env"
    env_file.write_text(
        f"TELEGRAM_ALLOWED_USERS={OWNER_ID}\n"
        f"TELEGRAM_BOT_TOKEN={FAKE_BOT_TOKEN}\n",
        encoding="utf-8",
    )
    os.chmod(env_file, 0o600)
    pid_file = profile_home / "gateway.pid"
    pid_file.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    os.chmod(pid_file, 0o600)

    import gateway.status

    monkeypatch.setattr(
        gateway.status,
        "get_running_pid_identity_strict",
        lambda _path: (os.getpid(), 1.0),
    )
    plugin = _load_plugin(
        monkeypatch,
        hermes_repo / "plugins/c1_telegram_personal/__init__.py",
        profile_home,
        {
            "TELEGRAM_ALLOWED_USERS": str(OWNER_ID),
            "TELEGRAM_BOT_TOKEN": FAKE_BOT_TOKEN,
        },
    )
    production, _operator_ops = plugin._agent_ops_modules(agent_ops_repo)
    original_build = production.build_trusted_binding

    def build_with_mock(**kwargs):
        kwargs["client_factory"] = lambda: _FakeHttpClient(sent)
        return original_build(**kwargs)

    sent = []
    monkeypatch.setattr(production, "build_trusted_binding", build_with_mock)
    context = _PluginContext({
        "enabled": True,
        "agent_ops_repo": agent_ops_repo,
        "source_repo": source_repo,
    })
    plugin.register(context)

    assert len(context.hooks["post_gateway_admission"]) == 1
    registration = context.hooks["post_gateway_admission"][0]
    assert not registration.disposed
    marker = profile_home / "c1" / "c1_telegram_personal.binding.json"
    assert marker.is_file()

    from gateway.run_inbound_consumer import (
        C1_TELEGRAM_REQUEST,
        _telegram_c1_identity,
    )

    now = datetime.now(timezone.utc) - timedelta(seconds=1)
    chat = SimpleNamespace(id=OWNER_ID, type="private")
    user = SimpleNamespace(id=OWNER_ID, is_bot=False)
    raw_message = SimpleNamespace(
        chat=chat, from_user=user, message_id=17, date=now, text=REQUEST,
    )
    event = SimpleNamespace(
        raw_message=raw_message, message_id="17", text=C1_TELEGRAM_REQUEST,
    )
    source_object = SimpleNamespace(
        platform=SimpleNamespace(value="telegram"),
        chat_type="dm",
        is_bot=False,
        chat_id=str(OWNER_ID),
        user_id=str(OWNER_ID),
        thread_id=None,
    )
    identity = _telegram_c1_identity(event, source_object)
    assert identity is not None
    source = {
        "platform": "telegram",
        "profile": "chief",
        "chat_type": "dm",
        "chat_id": str(OWNER_ID),
        "user_id": str(OWNER_ID),
        "thread_id": None,
        "c1_telegram_identity": identity,
    }
    outcome = registration.callback(
        session_key="agent:chief:telegram:" + str(OWNER_ID),
        platform="telegram",
        source=source,
        message_id="17",
        text=C1_TELEGRAM_REQUEST,
        telegram_identity=identity,
        c1_dispatch=_FakeC1Dispatch(identity["request_id"]),
    )
    assert outcome == {"action": "handled"}
    assert len(sent) == 1

    expected = subprocess.run(
        ["git", "-C", source_repo, "cat-file", "blob",
         "2b50253a9eb545a1d1a2a33cdb0bb53ad430582e:NEXT.md"],
        check=True, capture_output=True,
    ).stdout.decode("utf-8").splitlines()[0]
    assert sent[0]["text"] == expected

    package_name = production.__package__
    conduit_personal = __import__(
        package_name + ".conduit_personal", fromlist=["ConduitPersonalAdapter"],
    )
    gateway_adapter = __import__(
        package_name + ".gateway_c1_adapter",
        fromlist=["c1_source_event_id_for_gateway_request"],
    )
    production_module = production
    store_module = __import__(
        package_name + ".store", fromlist=["C1Store"],
    )

    reopened = store_module.C1Store(profile_home / "c1" / "c1.sqlite3")
    source_event_id = gateway_adapter.c1_source_event_id_for_gateway_request(
        identity["request_id"],
    )
    row = reopened.read_back_by_source_event(
        gateway_adapter.C1_SOURCE_NAMESPACE, source_event_id,
    )
    assert row is not None
    result = conduit_personal.ConduitPersonalAdapter(
        reopened,
        source_resolver=production_module.LocalPinnedGitSourceResolver(source_repo),
        reply_transport=conduit_personal.RejectAllReplyTransport(),
    ).read_back(row["request_id"])
    assert result.status == "DELIVERED"
    assert result.first_line == expected
    assert result.source_commit == "2b50253a9eb545a1d1a2a33cdb0bb53ad430582e"

    preflight = subprocess.run(
        [str(Path(agent_ops_repo) / "tools/agent_ops_c1/preflight.sh")],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "C1_PROFILE_HOME": str(profile_home),
            "C1_HERMES_REPO": str(hermes_repo),
            "C1_HERMES_PYTHON": sys.executable,
            "HERMES_REPO": str(hermes_repo),
        },
    )
    assert preflight.returncode == 0, preflight.stdout + preflight.stderr
    assert preflight.stdout.splitlines() == [
        "PASS allowlist",
        "PASS hmac_file",
        "PASS c1_db_writable",
        "PASS binding_registered",
        "PASS gateway_version_c4cd640",
    ]

    for cleanup in reversed(context.unload_callbacks):
        cleanup()
    assert registration.disposed
    assert not marker.exists()


def test_initializer_replaces_only_a_proven_stale_marker(monkeypatch, tmp_path):
    plugin_file = (
        Path(__file__).resolve().parents[2]
        / "plugins/c1_telegram_personal/__init__.py"
    )
    profile_home = tmp_path / "hermes" / "profiles" / "chief"
    marker = profile_home / "c1" / "c1_telegram_personal.binding.json"
    _private_dir(marker.parent)
    plugin = _load_plugin(monkeypatch, plugin_file, profile_home, {})
    stale = {
        "plugin": "c1_telegram_personal",
        "hook": "post_gateway_admission",
        "state": "registered",
        "pid": os.getpid(),
        "start_time": 1.0,
        "gateway_pin": plugin.GATEWAY_PIN,
    }
    marker.write_text(json.dumps(stale), encoding="utf-8")
    os.chmod(marker, 0o600)
    monkeypatch.setattr(
        plugin, "_process_identity_is_live", lambda _pid, _started: False,
    )

    plugin._write_marker(profile_home, os.getpid(), 2.0)

    current = json.loads(marker.read_text(encoding="utf-8"))
    assert current["pid"] == os.getpid()
    assert current["start_time"] == 2.0
    plugin._remove_marker(marker, os.getpid(), 2.0)
    assert not marker.exists()


@pytest.mark.parametrize("owner_live", [True, None])
def test_initializer_preserves_live_or_unverifiable_marker(
    monkeypatch, tmp_path, owner_live,
):
    plugin_file = (
        Path(__file__).resolve().parents[2]
        / "plugins/c1_telegram_personal/__init__.py"
    )
    profile_home = tmp_path / "hermes" / "profiles" / "chief"
    marker = profile_home / "c1" / "c1_telegram_personal.binding.json"
    _private_dir(marker.parent)
    plugin = _load_plugin(monkeypatch, plugin_file, profile_home, {})
    existing = {
        "plugin": "c1_telegram_personal",
        "hook": "post_gateway_admission",
        "state": "registered",
        "pid": os.getpid(),
        "start_time": 1.0,
        "gateway_pin": plugin.GATEWAY_PIN,
    }
    marker.write_text(json.dumps(existing), encoding="utf-8")
    os.chmod(marker, 0o600)
    original = marker.read_bytes()
    monkeypatch.setattr(
        plugin, "_process_identity_is_live",
        lambda _pid, _started: owner_live,
    )

    with pytest.raises(RuntimeError, match="active or unverifiable owner"):
        plugin._write_marker(profile_home, os.getpid(), 2.0)
    assert marker.read_bytes() == original


def test_initializer_cleans_marker_when_unload_registration_fails(
    monkeypatch, tmp_path,
):
    agent_ops_repo = os.environ.get("HERMES_TEST_C1_AGENT_OPS_REPO")
    assert agent_ops_repo
    plugin_file = (
        Path(__file__).resolve().parents[2]
        / "plugins/c1_telegram_personal/__init__.py"
    )
    profile_home = tmp_path / "hermes" / "profiles" / "chief"
    _private_dir(profile_home / "c1")
    plugin = _load_plugin(
        monkeypatch,
        plugin_file,
        profile_home,
        {
            "TELEGRAM_ALLOWED_USERS": str(OWNER_ID),
            "TELEGRAM_BOT_TOKEN": FAKE_BOT_TOKEN,
        },
    )
    production, operator_ops = plugin._agent_ops_modules(agent_ops_repo)
    monkeypatch.setattr(operator_ops, "_gateway_pin_ok", lambda _repo: True)
    monkeypatch.setattr(
        production, "build_trusted_binding", lambda **_kwargs: object(),
    )
    binding_module = __import__(
        production.__package__ + ".hermes_gateway_binding",
        fromlist=["register_post_gateway_admission"],
    )
    registration = _Registration(lambda: None)
    unregistered = []
    monkeypatch.setattr(
        binding_module,
        "register_post_gateway_admission",
        lambda _ctx, _binding: registration,
    )
    monkeypatch.setattr(
        binding_module,
        "unregister_post_gateway_admission",
        lambda value: unregistered.append(value),
    )
    monkeypatch.setattr(
        plugin, "_gateway_pid", lambda _profile: (os.getpid(), 12.0),
    )
    context = _FailingUnloadContext({
        "enabled": True,
        "agent_ops_repo": agent_ops_repo,
        "source_repo": str(tmp_path),
    })
    marker = profile_home / "c1" / "c1_telegram_personal.binding.json"

    with pytest.raises(
        RuntimeError, match="unload callback registration failed",
    ):
        plugin.register(context)

    assert unregistered == [registration]
    assert not marker.exists()
