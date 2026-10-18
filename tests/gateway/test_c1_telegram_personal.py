"""Offline C1 Telegram dispatch tests; no bot credentials or live Gateway calls.

Formal prereview probes: profile matrix, identity mismatch, missing binding,
hook error, expired input and timeout effects. All ingress uses the real runner.
"""

import asyncio
from concurrent.futures import CancelledError as FutureCancelledError
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import hmac
import json
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.event import MessageEvent
from gateway.run_inbound_consumer import (
    C1DispatchClosed,
    C1TelegramDispatch,
    C1_TELEGRAM_REQUEST,
    POST_ADMISSION_HOOK,
)
from gateway.session import SessionSource

TEST_KEY = b"MODELLED-only-telegram-c1-test-key-32-bytes"
OWNER_TELEGRAM_USER_ID = "424242"
TEST_NOW = datetime(2026, 10, 15, 10, 0, tzinfo=UTC)


async def _append_effect(effects, value):
    effects.append(value)


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _signed_event(identity):
    event = {
        "schema": "hermes-conduit-event-v1",
        "event_id": identity["event_id"],
        "request_id": identity["request_id"],
        "conversation_id": identity["conversation_id"],
        "principal": identity["principal"],
        "principal_proof_ref": identity["principal_proof_ref"],
        "text": identity["text"],
        "issued_at": identity["issued_at"],
        "expires_at": identity["expires_at"],
        "nonce": identity["nonce"],
    }
    signature = hmac.new(TEST_KEY, _canonical(event).encode("utf-8"), hashlib.sha256).hexdigest()
    return {"event": event, "signature": "sha256=" + signature}


class FakeGatewayPort:
    def __init__(self, *, uncertain=False):
        self.uncertain = uncertain
        self.sends = []

    async def send_result(self, *, conversation_id, chat_id, thread_id, request_id, text):
        attempt = {
            "conversation_id": conversation_id,
            "chat_id": chat_id,
            "thread_id": thread_id,
            "request_id": request_id,
            "text": text,
        }
        self.sends.append(attempt)
        if self.uncertain:
            raise TimeoutError("modelled delivery outcome is uncertain")
        return {"receipt_ref": "telegram-receipt-" + request_id[-12:]}


@dataclass
class FakeRecord:
    fingerprint: str
    status: str
    receipt_ref: str | None = None


class FakeDurableC1:
    """Model the existing C1 durable request identity and return-intent contract."""

    def __init__(self, gateway):
        self.gateway = gateway
        self.records = {}
        self.executions = 0
        self.before_return = lambda: None

    def handle(self, event, identity, *, c1_dispatch):
        if (
            event.get("text") != C1_TELEGRAM_REQUEST
            or event.get("principal") != "basic:kudi68"
            or event.get("request_id") != identity.get("request_id")
            or event.get("event_id") != identity.get("event_id")
            or event.get("conversation_id") != identity.get("conversation_id")
        ):
            raise ValueError("EVENT_BINDING_INVALID")

        proof = identity.get("principal_proof") or {}
        fingerprint = _canonical({"event": event, "principal_proof": proof})
        request_id = event["request_id"]
        prior = self.records.get(request_id)
        if prior is not None:
            if prior.fingerprint != fingerprint:
                raise ValueError("REQUEST_ID_CONFLICT")
            return prior

        c1_dispatch.check_active()
        record = FakeRecord(fingerprint=fingerprint, status="RETURN_INTENT")
        self.records[request_id] = record
        self.executions += 1
        self.before_return()
        try:
            # Fence the actual send, not the entire C1 workflow before preparation.
            receipt = c1_dispatch.run_effect(
                self.gateway.send_result,
                conversation_id=event["conversation_id"],
                chat_id=proof["chat_id"],
                thread_id=proof.get("thread_id"),
                request_id=request_id,
                text="NEXT first line",
            )
        except (TimeoutError, C1DispatchClosed):
            record.status = "RETURN_UNCERTAIN"
            return record

        record.status = "DELIVERED"
        record.receipt_ref = receipt["receipt_ref"]
        return record


class FakeGatewayC1Adapter:
    """Model the HMAC and Telegram principal checks before the durable C1 store."""

    def __init__(self, durable_c1):
        self.durable_c1 = durable_c1
        self.verification_attempts = 0
        self.mapped_human = None

    def handle(self, signed, identity, *, c1_dispatch, corrupt_signature=False):
        self.verification_attempts += 1
        event = signed.get("event", {})
        signature = signed.get("signature", "")
        expected = hmac.new(
            TEST_KEY, _canonical(event).encode("utf-8"), hashlib.sha256
        ).hexdigest()
        if corrupt_signature:
            signature = "sha256=" + "0" * 64
        if not hmac.compare_digest(signature.removeprefix("sha256="), expected):
            raise ValueError("SIGNATURE_INVALID")
        proof = identity.get("principal_proof") or {}
        if proof.get("user_id") != OWNER_TELEGRAM_USER_ID:
            raise ValueError("PRINCIPAL_NOT_ALLOWED")
        self.mapped_human = "human:kudi68"
        return self.durable_c1.handle(event, identity, c1_dispatch=c1_dispatch)


class FakeC1Binding:
    assurance = "MODELLED"

    def __init__(self, gateway_c1, *, corrupt_signature=False):
        self.gateway_c1 = gateway_c1
        self.corrupt_signature = corrupt_signature
        self.calls = 0
        self.identities = []

    def __call__(self, *, c1_dispatch=None, **kwargs):
        self.calls += 1
        identity = kwargs.get("telegram_identity")
        source = kwargs.get("source")
        if (
            not isinstance(identity, dict)
            or not isinstance(source, dict)
            or source.get("c1_telegram_identity") != identity
        ):
            return None
        self.identities.append(identity)
        signed = _signed_event(identity)
        c1_dispatch.check_active()
        self.gateway_c1.handle(
            signed, identity, c1_dispatch=c1_dispatch,
            corrupt_signature=self.corrupt_signature,
        )
        # C1's receipt-aware return transport already sends the result.
        return {"action": "handled"}


def _set_hook(callback):
    from hermes_cli.plugins import get_plugin_manager

    manager = get_plugin_manager()
    manager._discovered = True
    manager._hooks[POST_ADMISSION_HOOK] = [callback]
    return manager


@pytest.fixture(autouse=True)
def _isolate_hook_registry(monkeypatch):
    from hermes_cli.plugins import get_plugin_manager

    manager = get_plugin_manager()
    manager._hooks.pop(POST_ADMISSION_HOOK, None)
    manager._discovered = True
    # Model time, transport authorization and binding; these are not live-ingress proofs.
    monkeypatch.setattr("gateway.run_inbound_consumer._utc_now", lambda: TEST_NOW, raising=False)
    yield
    manager._hooks.pop(POST_ADMISSION_HOOK, None)


def _event(
    *,
    profile="chief",
    message_id=17,
    update_id=100,
    user_id=OWNER_TELEGRAM_USER_ID,
    raw_user_id=None,
    chat_id=10001,
    raw_chat_id=None,
    chat_type="private",
    source_chat_type="dm",
    text=C1_TELEGRAM_REQUEST,
    raw_text=None,
    date=None,
):
    raw_user_id = int(user_id) if raw_user_id is None else raw_user_id
    raw_chat_id = chat_id if raw_chat_id is None else raw_chat_id
    date = date or TEST_NOW
    raw = SimpleNamespace(
        chat=SimpleNamespace(id=raw_chat_id, type=chat_type),
        from_user=SimpleNamespace(id=raw_user_id, is_bot=False),
        message_id=message_id,
        text=text if raw_text is None else raw_text,
        date=date,
    )
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id=str(chat_id),
        chat_type=source_chat_type,
        user_id=str(user_id),
        thread_id="55",
        profile=profile,
        is_bot=False,
    )
    return MessageEvent(
        text=text,
        source=source,
        raw_message=raw,
        message_id=str(message_id),
        platform_update_id=update_id,
        timestamp=date,
    )


def _runner(profile="chief"):
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(
        platforms={Platform.TELEGRAM: PlatformConfig(enabled=True)},
        multiplex_profiles=False,
    )
    adapter = SimpleNamespace(send=AsyncMock())
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._primary_profile_name = profile
    runner._transport_owner = lambda _source: None
    runner.pairing_store = MagicMock()
    runner.session_store = MagicMock()
    runner._is_user_authorized_for_source = lambda _source: True
    runner._admit_bot_message_for_source = lambda _source: True
    runner._delivery_adapter_for = lambda _source: adapter
    runner._intake_adapter_for = lambda _source: adapter
    runner._scale_to_zero_note_real_inbound = lambda: None
    runner._hm_estop_gate = lambda _event, _source, _internal: None
    runner._hm_pending_reply_intercepts = AsyncMock(return_value=None)
    runner._hm_evict_idle_stale_agent = lambda _key: None
    runner._is_session_running = lambda _key: False
    runner._hm_dispatch_idle_commands = AsyncMock(return_value=(False, None))
    runner._is_telegram_topic_root_lobby = lambda _source: False
    runner._external_drain_active = False
    runner._session_key_for_source = lambda _source: "agent:chief:telegram:10001"
    runner._claim_active_session_slot = lambda _key, _source: (None, None)
    runner._hm_rescue_orphaned_fifo = lambda event, source, internal, _key: (
        event, source, internal
    )
    runner._session_state = lambda _key: SimpleNamespace(
        turn=SimpleNamespace(lease=None, agent=None, event=None, started_ts=None)
    )
    runner._persist_active_agents = lambda: None
    runner._begin_session_run_generation = lambda _key: 1
    runner._handle_message_with_agent = AsyncMock(return_value="agent-reply")
    runner._run_post_turn_hooks = AsyncMock()
    runner._restore_pending_one_turn_model_override = lambda *_args: None
    runner._clear_durable_active_turn = AsyncMock()
    runner._release_running_agent_state = lambda *_args, **_kwargs: None
    runner._release_turn_lease = lambda *_args, **_kwargs: None
    return runner


@pytest.mark.asyncio
async def test_normal_dispatch_returns_once_to_the_originating_telegram_conversation():
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    gateway_c1 = FakeGatewayC1Adapter(c1)
    binding = FakeC1Binding(gateway_c1)
    _set_hook(binding)
    runner = _runner()

    result = await runner._handle_message(_event())

    assert result is None
    runner._handle_message_with_agent.assert_not_awaited()
    assert c1.executions == 1
    assert len(gateway.sends) == 1
    assert gateway.sends[0]["chat_id"] == "10001"
    assert gateway.sends[0]["thread_id"] == "55"
    assert c1.records[next(iter(c1.records))].status == "DELIVERED"
    assert c1.records[next(iter(c1.records))].receipt_ref
    assert gateway_c1.mapped_human == "human:kudi68"


@pytest.mark.asyncio
async def test_fixed_request_is_consumed_before_busy_session_routing():
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    binding = FakeC1Binding(FakeGatewayC1Adapter(c1))
    _set_hook(binding)
    runner = _runner()
    runner._is_session_running = lambda _key: True
    runner._hm_handle_running_session_message = AsyncMock(return_value="busy-agent-path")

    result = await runner._handle_message(_event())

    assert result is None
    runner._hm_handle_running_session_message.assert_not_awaited()
    runner._handle_message_with_agent.assert_not_awaited()
    assert c1.executions == 1
    assert len(gateway.sends) == 1


@pytest.mark.asyncio
async def test_raw_fixed_request_cannot_fall_through_after_a_gateway_rewrite():
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    binding = FakeC1Binding(FakeGatewayC1Adapter(c1))
    _set_hook(binding)
    runner = _runner()

    async def rewrite(event, _source):
        event.text = "rewritten text"
        return event

    runner._hm_pre_gateway_dispatch_hook = AsyncMock(side_effect=rewrite)

    result = await runner._handle_message(_event())

    assert result is None
    runner._handle_message_with_agent.assert_not_awaited()
    assert binding.calls == 0
    assert c1.executions == 0
    assert gateway.sends == []


@pytest.mark.asyncio
async def test_other_profile_keeps_the_normal_agent_path():
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    binding = FakeC1Binding(FakeGatewayC1Adapter(c1))
    _set_hook(binding)
    runner = _runner(profile="default")

    result = await runner._handle_message(_event(profile="default"))

    assert result == "agent-reply"
    runner._handle_message_with_agent.assert_awaited_once()
    assert c1.executions == 0
    assert gateway.sends == []


@pytest.mark.asyncio
async def test_hook_timeout_consumes_the_fixed_request_without_agent_fallback(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocks_until_released(*, c1_dispatch, **_kwargs):
        entered.set()
        try:
            release.wait()
        finally:
            finished.set()

    _set_hook(blocks_until_released)
    monkeypatch.setattr(
        "gateway.run_inbound_consumer.C1_TELEGRAM_HOOK_TIMEOUT_SECONDS", 2
    )
    monkeypatch.setattr("hermes_cli.plugins._resolve_hook_callback_timeout", lambda: 0.05)
    runner = _runner()

    try:
        result = await runner._handle_message(_event())

        assert result is None
        assert entered.is_set()
        runner._handle_message_with_agent.assert_not_awaited()
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)


@pytest.mark.asyncio
async def test_mismatched_raw_telegram_identity_is_consumed_before_binding():
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    gateway_c1 = FakeGatewayC1Adapter(c1)
    binding = FakeC1Binding(gateway_c1)
    _set_hook(binding)
    runner = _runner()

    result = await runner._handle_message(
        _event(raw_user_id=424243, user_id=OWNER_TELEGRAM_USER_ID)
    )

    assert result is None
    runner._handle_message_with_agent.assert_not_awaited()
    assert binding.calls == 0
    assert gateway_c1.verification_attempts == 0
    assert c1.executions == 0
    assert gateway.sends == []


@pytest.mark.asyncio
async def test_restart_replay_is_stable_and_distinct_messages_do_not_merge():
    durable_gateway = FakeGatewayPort()
    durable_c1 = FakeDurableC1(durable_gateway)
    first_binding = FakeC1Binding(FakeGatewayC1Adapter(durable_c1))
    _set_hook(first_binding)
    runner = _runner()
    original = _event(update_id=100)

    await runner._handle_message(original)
    first_identity = first_binding.identities[0]
    first_request_id = first_identity["request_id"]

    restarted_gateway = FakeGatewayPort()
    restarted_c1 = FakeDurableC1(restarted_gateway)
    restarted_c1.records = durable_c1.records
    replay_binding = FakeC1Binding(FakeGatewayC1Adapter(restarted_c1))
    _set_hook(replay_binding)
    await runner._handle_message(_event(update_id=900))
    await runner._handle_message(_event(message_id=18, update_id=901))

    # The ingress ledger suppresses replay even before a restarted binding runs.
    assert len(replay_binding.identities) == 1
    assert replay_binding.identities[0]["request_id"] != first_request_id
    assert restarted_c1.executions == 1
    assert len(restarted_gateway.sends) == 1
    assert restarted_gateway.sends[0]["request_id"] == replay_binding.identities[0]["request_id"]
    assert len(durable_c1.records) == 2


@pytest.mark.asyncio
async def test_invalid_signature_is_rejected_before_durable_c1_execution():
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    gateway_c1 = FakeGatewayC1Adapter(c1)
    _set_hook(FakeC1Binding(gateway_c1, corrupt_signature=True))
    runner = _runner()

    result = await runner._handle_message(_event())

    assert result is None
    runner._handle_message_with_agent.assert_not_awaited()
    assert gateway_c1.verification_attempts == 1
    assert c1.executions == 0
    assert gateway.sends == []


@pytest.mark.asyncio
async def test_non_allowlisted_telegram_user_is_rejected_before_durable_c1_execution():
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    gateway_c1 = FakeGatewayC1Adapter(c1)
    _set_hook(FakeC1Binding(gateway_c1))
    runner = _runner()

    result = await runner._handle_message(_event(user_id="424243"))

    assert result is None
    runner._handle_message_with_agent.assert_not_awaited()
    assert gateway_c1.verification_attempts == 1
    assert c1.executions == 0
    assert gateway.sends == []


@pytest.mark.asyncio
async def test_return_uncertain_is_never_retried_after_binding_restart():
    first_gateway = FakeGatewayPort(uncertain=True)
    durable_c1 = FakeDurableC1(first_gateway)
    first_binding = FakeC1Binding(FakeGatewayC1Adapter(durable_c1))
    _set_hook(first_binding)
    runner = _runner()

    await runner._handle_message(_event())

    restarted_gateway = FakeGatewayPort()
    restarted_c1 = FakeDurableC1(restarted_gateway)
    restarted_c1.records = durable_c1.records
    replay_binding = FakeC1Binding(FakeGatewayC1Adapter(restarted_c1))
    _set_hook(replay_binding)
    await runner._handle_message(_event(update_id=900))

    assert durable_c1.records[first_binding.identities[0]["request_id"]].status == "RETURN_UNCERTAIN"
    assert len(first_gateway.sends) == 1
    assert restarted_c1.executions == 0
    assert restarted_gateway.sends == []
    assert replay_binding.identities == []


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime_profile", ["chief", "default", "other"])
async def test_none_source_profile_uses_the_canonical_runtime_profile(runtime_profile):
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    binding = FakeC1Binding(FakeGatewayC1Adapter(c1))
    _set_hook(binding)
    runner = _runner(profile=runtime_profile)

    result = await runner._handle_message(_event(profile=None))

    if runtime_profile == "chief":
        assert result is None
        runner._handle_message_with_agent.assert_not_awaited()
        assert c1.executions == 1
    else:
        assert result == "agent-reply"
        runner._handle_message_with_agent.assert_awaited_once()
        assert c1.executions == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime_profile,wire_profile", [("chief", "other"), ("other", "chief")])
async def test_pinned_runtime_identity_wins_over_the_wire_profile(tmp_path, runtime_profile, wire_profile):
    from gateway.session_identity import RoutingIdentity

    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    _set_hook(FakeC1Binding(FakeGatewayC1Adapter(c1)))
    runner = _runner(profile="chief")
    event = _event(profile=wire_profile)
    event.source._identity = RoutingIdentity(
        transport_profile="chief", runtime_profile=runtime_profile,
        authorization_home=tmp_path, runtime_home=tmp_path, multiplexed=False,
    )

    result = await runner._handle_message(event)

    assert c1.executions == (1 if runtime_profile == "chief" else 0)
    assert result == (None if runtime_profile == "chief" else "agent-reply")


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime_profile", ["chief", "other"])
async def test_multiplex_request_uses_the_canonical_home(tmp_path, runtime_profile):
    from gateway.run import _profile_runtime_scope
    from gateway.session_identity import RoutingIdentity
    from hermes_constants import get_hermes_home

    launch, routed = tmp_path / "launch", tmp_path / "routed"
    launch.mkdir()
    routed.mkdir()
    seen_homes = []
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    binding = FakeC1Binding(FakeGatewayC1Adapter(c1))

    def scoped_binding(*, c1_dispatch=None, **kwargs):
        seen_homes.append(get_hermes_home())
        return binding(c1_dispatch=c1_dispatch, **kwargs)

    runner = _runner(profile="default")
    runner.config.multiplex_profiles = True
    event = _event(profile=runtime_profile)
    event.source._identity = RoutingIdentity(
        transport_profile="default", runtime_profile=runtime_profile,
        authorization_home=launch, runtime_home=routed,
    )
    with _profile_runtime_scope(routed):
        _set_hook(scoped_binding)
    with _profile_runtime_scope(launch):
        result = await runner._handle_message(event)
        assert get_hermes_home() == launch

    assert seen_homes == [routed]
    assert result == (None if runtime_profile == "chief" else "agent-reply")
    assert c1.executions == (1 if runtime_profile == "chief" else 0)


@pytest.mark.asyncio
async def test_missing_binding_consumes_without_agent_fallback():
    runner = _runner()

    assert await runner._handle_message(_event()) is None

    runner._handle_message_with_agent.assert_not_awaited()
    runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()


@pytest.mark.asyncio
async def test_binding_error_consumes_without_agent_fallback():
    calls = []

    def broken(*, c1_dispatch, **_kwargs):
        calls.append("entered")
        raise RuntimeError("offline binding failure")

    _set_hook(broken)
    runner = _runner()

    assert await runner._handle_message(_event()) is None

    assert calls == ["entered"]
    runner._handle_message_with_agent.assert_not_awaited()
    runner.adapters[Platform.TELEGRAM].send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [
    timedelta(minutes=-10), timedelta(seconds=-301), timedelta(seconds=31),
    timedelta(minutes=-5, microseconds=-1), timedelta(seconds=30, microseconds=1),
])
async def test_expired_or_implausibly_future_request_never_reaches_binding(offset):
    calls = []
    _set_hook(lambda *, c1_dispatch, **kwargs: calls.append(kwargs))
    runner = _runner()

    assert await runner._handle_message(_event(date=TEST_NOW + offset)) is None

    assert calls == []
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [timedelta(minutes=-5), timedelta(seconds=30)])
async def test_request_at_time_boundaries_is_admitted(offset):
    calls = []
    _set_hook(lambda *, c1_dispatch, **kwargs: calls.append(kwargs))
    runner = _runner()

    assert await runner._handle_message(_event(date=TEST_NOW + offset)) is None

    assert len(calls) == 1
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_timeout_fences_late_callback_side_effects(monkeypatch):
    """F2 counterexample: a timed-out thread must not later send a result."""
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    binding = FakeC1Binding(FakeGatewayC1Adapter(c1))
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    dispatches = []

    def late_binding(*, c1_dispatch, **kwargs):
        dispatches.append(c1_dispatch)
        entered.set()
        try:
            if release.wait(10):
                return binding(c1_dispatch=c1_dispatch, **kwargs)
        finally:
            finished.set()

    _set_hook(late_binding)
    monkeypatch.setattr("gateway.run_inbound_consumer.C1_TELEGRAM_HOOK_TIMEOUT_SECONDS", 2)
    # Isolate the outer timeout: the real callback still runs on the to_thread worker.
    monkeypatch.setattr("hermes_cli.plugins._resolve_hook_callback_timeout", lambda: 0)
    runner = _runner()

    try:
        assert await runner._handle_message(_event()) is None
        assert entered.is_set()
        assert dispatches[0].readback() == "UNCERTAIN"
        runner._handle_message_with_agent.assert_not_awaited()
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)

    assert gateway.sends == []
    assert c1.executions == 0
    assert dispatches[0].readback() == "UNCERTAIN"

    # A new reader opens the persisted ledger without carrying the callback object.
    dispatch = dispatches[0]
    assert C1TelegramDispatch.read_request(dispatch.path.parent, dispatch.request_id)["status"] == "UNCERTAIN"

    # A restarted consumer reads the persisted uncertain state and never retries it.
    replay = FakeC1Binding(FakeGatewayC1Adapter(c1))
    _set_hook(replay)
    await _runner()._handle_message(_event(update_id=999))
    assert replay.calls == 0
    assert gateway.sends == []


@pytest.mark.asyncio
@pytest.mark.parametrize("closure", ["timeout", "cancellation"])
async def test_timeout_between_final_check_and_transport_call_cannot_send(monkeypatch, closure):
    """F2 review probe: timeout wins after the worker's final clock check.

    The readback seam holds the real durable state after intent commit.
    No clock jump, source-line hook or replacement of the gate is involved.
    """
    checked, release, finished = threading.Event(), threading.Event(), threading.Event()
    effects, dispatches, errors = [], [], []
    original_readback = C1TelegramDispatch.readback

    def held_readback(dispatch):
        state = original_readback(dispatch)
        if state == "EFFECT_STARTED" and not checked.is_set():
            checked.set()
            assert release.wait(10)
        return state

    def binding(*, c1_dispatch, **_kwargs):
        dispatches.append(c1_dispatch)
        try:
            c1_dispatch.run_effect(_append_effect, effects, "send")
        except (C1DispatchClosed, FutureCancelledError, TimeoutError):
            pass
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    monkeypatch.setattr(C1TelegramDispatch, "readback", held_readback)
    monkeypatch.setattr("gateway.run_inbound_consumer.C1_TELEGRAM_HOOK_TIMEOUT_SECONDS", 2)
    _set_hook(binding)
    runner = _runner()
    task = asyncio.create_task(runner._handle_message(_event()))
    try:
        if closure == "cancellation":
            assert await asyncio.to_thread(checked.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert await task is None
        assert checked.is_set(), "probe did not reach the final clock check"
        dispatch = dispatches[0]
        assert dispatch.readback() == "UNCERTAIN"
        assert effects == []
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)

    assert errors == []
    assert effects == [], "transport was invoked after timeout was persisted"
    assert C1TelegramDispatch.read_request(dispatch.path.parent, dispatch.request_id)["status"] == "UNCERTAIN"
    await _runner()._handle_message(_event(update_id=999))
    assert len(dispatches) == 1
    assert effects == []
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "ambiguous"])
async def test_unfenced_or_ambiguous_binding_is_never_invoked(kind):
    effects = []

    def legacy(**_kwargs):
        effects.append("unfenced")

    def fenced(*, c1_dispatch, **_kwargs):
        c1_dispatch.run_effect(_append_effect, effects, "fenced")

    manager = _set_hook(legacy if kind == "legacy" else fenced)
    if kind == "ambiguous":
        manager._hooks[POST_ADMISSION_HOOK].append(fenced)
    runner = _runner()

    assert await runner._handle_message(_event()) is None

    assert effects == []
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_callback_cannot_start_an_effect_after_message_expiry(monkeypatch):
    effects, dispatches = [], []

    def expired_preparation(*, c1_dispatch, **_kwargs):
        dispatches.append(c1_dispatch)
        monkeypatch.setattr(
            "gateway.run_inbound_consumer._utc_now", lambda: TEST_NOW + timedelta(minutes=6)
        )
        c1_dispatch.run_effect(_append_effect, effects, "expired")

    _set_hook(expired_preparation)
    runner = _runner()

    assert await runner._handle_message(_event()) is None

    assert effects == []
    assert dispatches[0].readback() == "UNCERTAIN"
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_binding_cannot_authorize_a_second_effect():
    effects, dispatches = [], []

    def twice(*, c1_dispatch, **_kwargs):
        dispatches.append(c1_dispatch)
        c1_dispatch.run_effect(_append_effect, effects, "first")
        c1_dispatch.run_effect(_append_effect, effects, "second")

    _set_hook(twice)

    assert await _runner()._handle_message(_event()) is None

    assert effects == ["first"]
    assert dispatches[0].readback() == "UNCERTAIN"


@pytest.mark.asyncio
async def test_late_effect_completion_cannot_clear_uncertain_or_retry(monkeypatch):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    effects, dispatches, errors = [], [], []
    release_timer = None

    async def in_flight_effect():
        nonlocal release_timer
        # Remote completion is independent; the async transport leaves the loop free.
        release_timer = threading.Timer(2, release.set)
        release_timer.start()
        effects.append("started before timeout")
        entered.set()
        assert await asyncio.to_thread(release.wait, 10)

    def binding(*, c1_dispatch, **_kwargs):
        dispatches.append(c1_dispatch)
        try:
            c1_dispatch.run_effect(in_flight_effect)
        except (C1DispatchClosed, FutureCancelledError, TimeoutError):
            pass
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    _set_hook(binding)
    monkeypatch.setattr("gateway.run_inbound_consumer.C1_TELEGRAM_HOOK_TIMEOUT_SECONDS", 2)
    runner = _runner()
    try:
        assert await runner._handle_message(_event()) is None
        assert entered.is_set()
        assert dispatches[0].readback() == "UNCERTAIN"
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)
        if release_timer is not None:
            release_timer.cancel()
            release_timer.join(2)

    assert errors == []
    assert dispatches[0].readback() == "UNCERTAIN"
    await _runner()._handle_message(_event(update_id=999))
    assert effects == ["started before timeout"]
    assert len(dispatches) == 1
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_persists_uncertain_and_fences_the_released_callback():
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    effects, dispatches = [], []

    def binding(*, c1_dispatch, **_kwargs):
        dispatches.append(c1_dispatch)
        entered.set()
        try:
            assert release.wait(10)
            c1_dispatch.run_effect(_append_effect, effects, "too late")
        finally:
            finished.set()

    _set_hook(binding)
    runner = _runner()
    task = asyncio.create_task(runner._handle_message(_event()))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert dispatches[0].readback() == "UNCERTAIN"
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)

    assert effects == []
    runner._handle_message_with_agent.assert_not_awaited()


def test_readback_does_not_create_a_ledger(tmp_path):
    before = set(tmp_path.iterdir())
    assert C1TelegramDispatch.read_request(tmp_path, "missing-request") is None
    assert set(tmp_path.iterdir()) == before


@pytest.mark.asyncio
async def test_corrupt_ledger_blocks_dispatch_without_agent_fallback():
    from hermes_constants import get_hermes_home

    path = get_hermes_home() / "c1-telegram-dispatch.sqlite3"
    path.write_bytes(b"not a sqlite database")
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    binding = FakeC1Binding(FakeGatewayC1Adapter(c1))
    _set_hook(binding)
    runner = _runner()

    assert await runner._handle_message(_event()) is None

    assert binding.calls == 0
    assert gateway.sends == []
    assert path.read_bytes() == b"not a sqlite database"
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_duplicate_only_claims_one_dispatch():
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    effects, dispatches = [], []

    def binding(*, c1_dispatch, **_kwargs):
        dispatches.append(c1_dispatch)
        entered.set()
        try:
            assert release.wait(10)
            c1_dispatch.run_effect(_append_effect, effects, "once")
        finally:
            finished.set()

    _set_hook(binding)
    first = asyncio.create_task(_runner()._handle_message(_event()))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        assert await _runner()._handle_message(_event(update_id=999)) is None
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)
        await first

    assert effects == ["once"]
    assert len(dispatches) == 1
    assert dispatches[0].readback() == "EFFECT_FINISHED"


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapped", [False, True])
async def test_sync_or_deferred_effect_is_rejected_before_its_body_can_run(wrapped):
    effects, dispatches = [], []

    async def deferred():
        effects.append("must stay unexecuted")

    def binding(*, c1_dispatch, **_kwargs):
        dispatches.append(c1_dispatch)
        effect = (lambda: deferred()) if wrapped else (lambda: effects.append("sync forbidden"))
        with pytest.raises(C1DispatchClosed):
            c1_dispatch.run_effect(effect)

    _set_hook(binding)

    assert await _runner()._handle_message(_event()) is None

    assert effects == []
    assert dispatches[0].readback() == "UNCERTAIN"


@pytest.mark.asyncio
async def test_binding_system_exit_is_contained_and_persisted_as_uncertain():
    dispatches = []

    def exits(*, c1_dispatch, **_kwargs):
        dispatches.append(c1_dispatch)
        raise SystemExit(1)

    _set_hook(exits)
    runner = _runner()

    assert await runner._handle_message(_event()) is None

    assert dispatches[0].readback() == "UNCERTAIN"
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_preparation_that_outlives_timeout_cannot_send_its_late_result(monkeypatch):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    gateway = FakeGatewayPort()
    c1 = FakeDurableC1(gateway)
    binding = FakeC1Binding(FakeGatewayC1Adapter(c1))
    dispatches = []

    def prepare():
        entered.set()
        assert release.wait(10)

    def delayed_return(*, c1_dispatch, **kwargs):
        dispatches.append(c1_dispatch)
        try:
            return binding(c1_dispatch=c1_dispatch, **kwargs)
        finally:
            finished.set()

    c1.before_return = prepare
    _set_hook(delayed_return)
    monkeypatch.setattr("gateway.run_inbound_consumer.C1_TELEGRAM_HOOK_TIMEOUT_SECONDS", 2)
    runner = _runner()
    try:
        assert await runner._handle_message(_event()) is None
        assert entered.is_set()
        assert c1.executions == 1  # C1 admission started before the timeout.
        assert gateway.sends == []
        assert dispatches[0].readback() == "UNCERTAIN"
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)

    assert gateway.sends == []
    assert c1.records[binding.identities[0]["request_id"]].status == "RETURN_UNCERTAIN"
    assert dispatches[0].readback() == "UNCERTAIN"
    runner._handle_message_with_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_sqlite_lock_cannot_turn_final_read_into_a_late_send(tmp_path, monkeypatch):
    """Real R1 lock contention, clocks and readback; no replaced gate."""
    import sqlite3
    import time
    from gateway import run_inbound_consumer as consumer

    monkeypatch.setattr(consumer, "C1_TELEGRAM_HOOK_TIMEOUT_SECONDS", .1)
    dispatch = await C1TelegramDispatch.create(tmp_path, {
        "request_id": "sqlite-r1", "expires_at": (TEST_NOW + timedelta(minutes=5)).isoformat(),
    })
    held, release = threading.Event(), threading.Event()
    effects, errors = [], []
    original = dispatch.readback
    triggered = False

    def locker():
        try:
            with sqlite3.connect(dispatch.path) as db:
                db.execute("BEGIN EXCLUSIVE")
                held.set()
                assert release.wait(3)
                db.rollback()
        except BaseException as exc:
            errors.append(exc)
            held.set()

    lock_thread = None

    def contended_read():
        nonlocal triggered, lock_thread
        state = original()
        if state == "EFFECT_STARTED" and not triggered:
            triggered = True
            lock_thread = threading.Thread(target=locker)
            lock_thread.start()
            assert held.wait(2)
            state = original()
        return state

    monkeypatch.setattr(dispatch, "readback", contended_read)
    loop = asyncio.get_running_loop()
    heartbeat = asyncio.Event()
    def beat():
        heartbeat.set()
        release.set()
    loop.call_later(.3, beat)
    fallback = threading.Timer(.8, release.set)
    fallback.start()
    started = time.monotonic()
    def effect():
        effects.append(time.monotonic() - started)
    async def async_effect():
        effect()
        return {"receipt_ref": "offline"}
    try:
        try:
            await asyncio.wait_for(asyncio.to_thread(dispatch.run_effect, async_effect), 3)
        except (C1DispatchClosed, TimeoutError):
            pass
    finally:
        release.set()
        fallback.cancel()
        fallback.join(2)
        if lock_thread is not None:
            await asyncio.to_thread(lock_thread.join, 2)
    assert errors == []
    assert triggered
    assert effects == [], f"send began after deadline: {effects}"
    assert heartbeat.is_set()
    assert original() == "UNCERTAIN"


@pytest.mark.asyncio
async def test_slow_sync_transport_does_not_freeze_consumer_loop(tmp_path):
    dispatch = await C1TelegramDispatch.create(tmp_path, {
        "request_id": "slow-r1", "expires_at": (TEST_NOW + timedelta(minutes=5)).isoformat(),
    })
    release = threading.Event()
    heartbeat = asyncio.Event()
    sends = []
    def blocking():
        sends.append("sync body ran")
        assert release.wait(3)
    def beat():
        heartbeat.set()
        release.set()
    asyncio.get_running_loop().call_later(.1, beat)
    fallback = threading.Timer(.5, release.set)
    fallback.start()
    try:
        with pytest.raises(C1DispatchClosed):
            await asyncio.wait_for(asyncio.to_thread(dispatch.run_effect, blocking), 3)
    finally:
        release.set()
        fallback.cancel()
        fallback.join(2)
    assert sends == []
    await asyncio.wait_for(heartbeat.wait(), 2)


@pytest.mark.asyncio
async def test_same_loop_result_shim_is_rejected_without_deadlock(tmp_path):
    """A subprocess bounds the original uninterruptible owner-loop deadlock."""
    import os
    import subprocess
    import sys
    script = '''
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from gateway.run_inbound_consumer import C1TelegramDispatch, C1DispatchClosed
async def main():
    loop = asyncio.get_running_loop()
    d = await C1TelegramDispatch.create(Path(__import__('sys').argv[1]), dict(
        request_id='deadlock', expires_at=(datetime.now(timezone.utc)+timedelta(minutes=5)).isoformat()))
    async def send():
        raise AssertionError('shim must never schedule send')
    def shim():
        asyncio.run_coroutine_threadsafe(send(), loop).result()
    try:
        await asyncio.to_thread(d.run_effect, shim)
    except C1DispatchClosed:
        print('REJECTED', flush=True)
    else:
        raise AssertionError('unsafe shim accepted')
asyncio.run(main())
'''
    try:
        result = await asyncio.to_thread(
            subprocess.run, [sys.executable, "-B", "-c", script, str(tmp_path)],
            capture_output=True, text=True, timeout=4, env=os.environ.copy(),
        )
    except subprocess.TimeoutExpired:
        pytest.fail("same-loop .result() deadlocked; isolated child killed after 4s")
    assert result.returncode == 0, result.stderr
    assert "REJECTED" in result.stdout


@pytest.mark.asyncio
async def test_owner_loop_bridge_rejects_before_scheduling_transport(tmp_path):
    dispatch = await C1TelegramDispatch.create(tmp_path, {
        "request_id": "owner-loop", "expires_at": (TEST_NOW + timedelta(minutes=5)).isoformat(),
    })
    sends = []
    with pytest.raises(C1DispatchClosed, match="async dispatch API"):
        dispatch.run_effect(_append_effect, sends, "forbidden")
    assert sends == []
    # Correct async entry stays usable; rejecting bridge misuse schedules nothing.
    await dispatch.arun_effect(_append_effect, sends, "once")
    assert sends == ["once"]
    assert await asyncio.to_thread(dispatch.readback) == "EFFECT_FINISHED"


@pytest.mark.asyncio
@pytest.mark.parametrize("swallow_cancel", [False, True])
async def test_async_io_keeps_heartbeat_and_late_receipt_uncertain(tmp_path, swallow_cancel):
    dispatch = await C1TelegramDispatch.create(tmp_path, {
        "request_id": "async-slow", "expires_at": (TEST_NOW + timedelta(minutes=5)).isoformat(),
    })
    entered, heartbeat, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    sends = []
    async def transport():
        sends.append("started")
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            if not swallow_cancel:
                raise
            return {"receipt_ref": "late-receipt-does-not-prove-delivery"}
    task = asyncio.create_task(dispatch.arun_effect(transport))
    await asyncio.wait_for(entered.wait(), 2)
    # This callback must run while I/O is still pending, not after a Timer frees it.
    asyncio.get_running_loop().call_soon(heartbeat.set)
    await asyncio.wait_for(heartbeat.wait(), 2)
    assert not task.done()
    await dispatch.aclose()
    with pytest.raises((asyncio.CancelledError, C1DispatchClosed)):
        await task
    assert cancelled.is_set()
    assert await asyncio.to_thread(dispatch.readback) == "UNCERTAIN"
    replay = await C1TelegramDispatch.create(tmp_path, {
        "request_id": "async-slow", "expires_at": (TEST_NOW + timedelta(minutes=5)).isoformat(),
    })
    with pytest.raises(C1DispatchClosed):
        await replay.arun_effect(_append_effect, sends, "retry forbidden")
    assert sends == ["started"]


@pytest.mark.asyncio
async def test_async_transport_timeout_cancels_io_and_persists_uncertain(tmp_path, monkeypatch):
    from gateway import run_inbound_consumer as consumer
    monkeypatch.setattr(consumer, "C1_TELEGRAM_HOOK_TIMEOUT_SECONDS", .1)
    dispatch = await C1TelegramDispatch.create(tmp_path, {
        "request_id": "io-timeout", "expires_at": (TEST_NOW + timedelta(minutes=5)).isoformat(),
    })
    cancelled = asyncio.Event()
    async def transport():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(dispatch.arun_effect(transport), 3)
    assert cancelled.is_set()
    assert await asyncio.to_thread(dispatch.readback) == "UNCERTAIN"


@pytest.mark.asyncio
async def test_async_transport_system_exit_is_contained(tmp_path):
    dispatch = await C1TelegramDispatch.create(tmp_path, {
        "request_id": "transport-exit", "expires_at": (TEST_NOW + timedelta(minutes=5)).isoformat(),
    })
    async def transport():
        raise SystemExit(1)
    with pytest.raises(RuntimeError, match="transport exited"):
        await dispatch.arun_effect(transport)
    assert await asyncio.to_thread(dispatch.readback) == "UNCERTAIN"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["dispatch", "timeout", "cancellation"])
async def test_saturated_executor_closes_uncertain_without_send(tmp_path, mode):
    """R4: real bridge/SQLite/cleanup in a bounded child; no production identity proof."""
    import os
    import subprocess
    import sys

    script = '''
import asyncio, sys, threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from gateway import run_inbound_consumer as m
from tests.waiting import async_wait_until

async def main():
    root, mode = Path(sys.argv[1]), sys.argv[2]
    loop = asyncio.get_running_loop()
    m.C1_TELEGRAM_HOOK_TIMEOUT_SECONDS = 2
    expiry = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    # Observe the host default capacity without resizing it.
    await asyncio.to_thread(lambda: None)
    n = loop._default_executor._max_workers
    sends = []
    async def send():
        if mode == 'dispatch':
            raise TimeoutError('offline transport refused before network send')
        sends.append('forbidden')
    if mode == 'dispatch':
        ds = [await m.C1TelegramDispatch.create(root, dict(
            request_id=str(i), expires_at=expiry)) for i in range(n)]
        barrier = threading.Barrier(n)
        # Hold the real gates until every worker has entered the bridge.
        for d in ds:
            await d._gate.acquire()
        def binding(d):
            barrier.wait(timeout=4)
            return d.run_effect(send)
        async def invoke(d):
            try:
                await asyncio.wait_for(asyncio.to_thread(binding, d), 2)
            except BaseException:
                await d.aclose()
        tasks = [asyncio.create_task(invoke(d)) for d in ds]
        await async_wait_until(lambda: sum(
            t.get_coro().__qualname__ == 'C1TelegramDispatch.arun_effect'
            for t in asyncio.all_tasks()) == n, timeout=3,
            what='all default workers waiting on their real dispatch bridge')
        # All workers now wait on the owner loop, which must execute real claims.
        for d in ds:
            d._gate.release()
        await asyncio.wait_for(asyncio.gather(*tasks), 6)
    else:
        ds = [await m.C1TelegramDispatch.create(root, dict(
            request_id=str(i), expires_at=expiry)) for i in range(n+2)]
        entered, release, worker_done = asyncio.Event(), threading.Event(), asyncio.Event()
        invoked = []
        def binding(**payload):
            invoked.append(payload['c1_dispatch'].request_id)
            if payload['c1_dispatch'].request_id != '0':
                return {'action': 'handled'}
            loop.call_soon_threadsafe(entered.set)
            try:
                release.wait(6)
                return payload['c1_dispatch'].run_effect(send)
            finally:
                loop.call_soon_threadsafe(worker_done.set)
        # Match the review's controlled consumer seams; gate/DB/bridge remain real.
        m.is_c1_telegram_personal_target = lambda *args: True
        m._telegram_c1_identity = lambda event, source: dict(request_id=event.message_id)
        m._c1_binding_callback = lambda: binding
        sys.modules['gateway.session_identity'] = SimpleNamespace(
            canonical_identity=lambda *args, **kwargs: SimpleNamespace(runtime_home=root))
        async def factory(cls, home, identity):
            return ds[int(identity['request_id'])]
        m.C1TelegramDispatch.create = classmethod(factory)
        source = SimpleNamespace(platform=SimpleNamespace(value='telegram'), to_dict=lambda: {})
        async def consume(i):
            return await m.run_c1_telegram_personal(
                SimpleNamespace(), SimpleNamespace(message_id=str(i)), source, 'session')
        first = asyncio.create_task(consume(0))
        await asyncio.wait_for(entered.wait(), 3)
        # Occupy the remaining default workers with unrelated work, as on a busy host.
        all_busy = asyncio.Event()
        barrier = threading.Barrier(n-1, action=lambda: loop.call_soon_threadsafe(all_busy.set))
        free = threading.Event()
        def unrelated():
            barrier.wait(timeout=4)
            free.wait(6)
        blockers = [asyncio.create_task(asyncio.to_thread(unrelated)) for _ in range(n-1)]
        await asyncio.wait_for(all_busy.wait(), 3)
        try:
            rejected = await asyncio.wait_for(asyncio.gather(
                *(consume(i) for i in range(1, n))), 3)
            assert rejected == [(True, None)] * (n-1), rejected
            if mode == 'cancellation':
                first.cancel()
                try:
                    await asyncio.wait_for(first, 3)
                except asyncio.CancelledError:
                    pass
                else:
                    raise AssertionError('consumer cancellation lost')
            else:
                assert await asyncio.wait_for(first, 4) == (True, None)
            # Timed-out preparation is still in flight: do not admit its follower.
            assert await asyncio.wait_for(consume(n), 3) == (True, None)
            assert invoked == ['0'], invoked
            assert [d.readback() for d in ds[:-1]] == ['UNCERTAIN'] * (n+1)
        finally:
            release.set()
            free.set()
        await asyncio.wait_for(worker_done.wait(), 3)
        await asyncio.wait_for(asyncio.gather(*blockers), 3)
        await async_wait_until(lambda: not m._C1_IN_FLIGHT.locked(), timeout=3)
        assert await asyncio.wait_for(consume(n+1), 3) == (True, None)
        assert invoked == ['0', str(n+1)], invoked
    assert all(d.readback() == 'UNCERTAIN' for d in ds)
    assert sends == [], sends
    assert await asyncio.wait_for(asyncio.to_thread(lambda: 'recovered'), 3) == 'recovered'
    print('BOUNDED UNCERTAIN NO_SEND EXECUTOR_RECOVERED', n, mode, flush=True)
asyncio.run(main())
'''
    try:
        result = await asyncio.to_thread(
            subprocess.run, [sys.executable, "-B", "-c", script, str(tmp_path), mode],
            capture_output=True, text=True, timeout=16,
            env={"PATH": os.defpath, "HOME": str(tmp_path)},
        )
    except subprocess.TimeoutExpired:
        pytest.fail("R4 shared executor circular wait; bounded child killed after 16s")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "BOUNDED UNCERTAIN NO_SEND EXECUTOR_RECOVERED" in result.stdout
