"""``post_gateway_admission``: let a plugin consume an admitted inbound message (#129958).

``GatewayInboundMixin._handle_message`` fires it once per non-internal message that survived
authorization, bot admission, pause/drain, pending-reply intercepts, the running-session lane and
idle slash-command dispatch, inside the claimed session slot. It runs under the message's ROUTED
profile scope: the receiving bot's handler binds the transport home (auth reads its ``.env``), which
differs from the runtime home when that bot serves a chat for another profile.

Fail-open: a callback that raises, times out or returns anything other than
``{"action": "handled"}`` is logged/ignored and the message proceeds to the agent, so one buggy
plugin can never blackhole a profile's traffic. The payload is a plain snapshot, never live
runner/session-store handles.

The fixed C1 request uses a separate fail-closed binding contract below: its
payload also carries a durable, deadline-bound capability for one asynchronous transport attempt.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from datetime import UTC, datetime, timedelta
import hashlib
import inspect
import json
import logging
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

POST_ADMISSION_HOOK = "post_gateway_admission"
C1_TELEGRAM_PROFILE = "chief"
C1_TELEGRAM_REQUEST = "回傳 kudi68/personal-ai-os 的 NEXT.md 第一行"
C1_TELEGRAM_HOOK_TIMEOUT_SECONDS = 45
# Telegram timestamps have second precision; tolerate a small sender/host clock skew.
C1_TELEGRAM_FUTURE_SKEW_SECONDS = 30
_C1_TELEGRAM_IDENTITY_SCHEMA = "hermes-telegram-message-identity-v1"
# Binding workers may synchronously wait on the owner loop. Their DB dependency
# must never compete for those same default-executor workers.
_C1_DB_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="c1-sqlite")
_C1_IN_FLIGHT = threading.Lock()


async def _c1_db_work(function, *args):
    context = contextvars.copy_context()
    return await asyncio.get_running_loop().run_in_executor(
        _C1_DB_EXECUTOR, partial(context.run, function, *args),
    )


def _release_c1_binding(task):
    # A timed-out worker can still be preparing. Keep admission closed until it
    # actually exits, and retrieve its late exception without starting new work.
    if not task.cancelled():
        task.exception()
    _C1_IN_FLIGHT.release()



def _routed_scope(runner: Any, source: Any):
    if not getattr(getattr(runner, "config", None), "multiplex_profiles", False):
        return contextlib.nullcontext()
    from gateway.run import _async_profile_runtime_scope
    return _async_profile_runtime_scope(runner._resolve_profile_home_for_source(source))


async def run_post_admission_hook(
    runner: Any, event: Any, source: Any, session_key: str
) -> tuple[bool, Optional[str]]:
    """Return ``(handled, reply)``; ``handled=False`` means run the ordinary agent turn."""
    try:
        async with _routed_scope(runner, source):
            from hermes_cli.lifecycle import ainvoke_hook, has_hook
            if not has_hook(POST_ADMISSION_HOOK):
                return False, None
            results = await ainvoke_hook(
                POST_ADMISSION_HOOK, session_key=session_key,
                platform=source.platform.value if source.platform else "",
                source=source.to_dict(), message_id=event.message_id, text=event.text or "",
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning("%s failed; continuing with the agent turn", POST_ADMISSION_HOOK, exc_info=True)
        return False, None
    for result in results:
        if isinstance(result, dict) and result.get("action") == "handled":
            reply = result.get("reply")
            logger.info("%s: a plugin handled the message for session %s", POST_ADMISSION_HOOK, session_key)
            return True, reply if isinstance(reply, str) and reply.strip() else None
    return False, None


def _canonical_digest(value: dict) -> str:
    payload = json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def is_c1_telegram_personal_target(event: Any, source: Any, runner: Any) -> bool:
    """Use the canonical runtime identity; a primary profile can be None on the wire."""
    if getattr(event, "internal", False):
        return False
    platform = getattr(getattr(source, "platform", None), "value", None)
    raw_text = getattr(getattr(event, "raw_message", None), "text", None)
    fixed_request = (
        platform == "telegram"
        and (
            getattr(event, "text", None) == C1_TELEGRAM_REQUEST
            or raw_text == C1_TELEGRAM_REQUEST
        )
    )
    if not fixed_request:
        return False
    from gateway.session_identity import IdentityUnresolved, canonical_identity

    identity = canonical_identity(source, runner=runner)
    if identity is None:
        # An unresolved fixed request must never become an ordinary agent turn.
        raise IdentityUnresolved("C1 Telegram runtime profile is unresolved")
    return identity.runtime_profile == C1_TELEGRAM_PROFILE


def _utc_now() -> datetime:
    return datetime.now(UTC)


class C1DispatchClosed(RuntimeError):
    """The request cannot authorize another effect; read back instead of retrying."""


class C1TelegramDispatch:
    """One-shot async I/O gate for a cooperative, trusted C1 binding.

    Use ``await create(...)`` on the consumer loop. Prepare/validate in the
    binding worker, then call ``run_effect`` there, or await ``arun_effect`` on
    the owner loop. The async callable must start one API attempt immediately:
    no chat-lock wait, pacing, retry, task creation or blocking I/O in its body.
    Cancellation cannot prove that Telegram did not receive an in-flight send.
    """

    def __init__(self, runtime_home: Path, identity: dict):
        self._effect_loop = asyncio.get_running_loop()
        self._effect_thread = threading.get_ident()
        self._gate = asyncio.Lock()
        self._closed = False
        self._effect_task = None
        self.path = Path(runtime_home) / "c1-telegram-dispatch.sqlite3"
        self.request_id = identity["request_id"]
        self.expires_at = datetime.fromisoformat(identity["expires_at"])
        self.deadline = time.monotonic() + C1_TELEGRAM_HOOK_TIMEOUT_SECONDS
        self.claimed = False

    @classmethod
    async def create(cls, runtime_home: Path, identity: dict):
        dispatch = cls(runtime_home, identity)
        await _c1_db_work(dispatch._initialize, identity)
        return dispatch

    def _initialize(self, identity):
        fingerprint = _canonical_digest(identity)
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS requests ("
                "request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, "
                "status TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            self.claimed = db.execute(
                "INSERT OR IGNORE INTO requests VALUES (?, ?, 'DISPATCHING', ?)",
                (self.request_id, fingerprint, _utc_now().isoformat()),
            ).rowcount == 1
            prior = db.execute(
                "SELECT fingerprint FROM requests WHERE request_id = ?", (self.request_id,)
            ).fetchone()
            if prior[0] != fingerprint:
                raise C1DispatchClosed("request identity conflict")

    @contextlib.contextmanager
    def _connect(self):
        # Only hashes and terminal state are persisted, never Telegram account data or text.
        with contextlib.closing(sqlite3.connect(self.path, timeout=1)) as db:
            with db:
                yield db

    def readback(self) -> str:
        return self.read_request(
            self.path.parent, self.request_id, classify_inflight=False
        )["durable_status"]

    @staticmethod
    def read_request(
        runtime_home: Path, request_id: str, *, classify_inflight: bool = True
    ) -> dict | None:
        """Read without mutation; fresh readers treat abandoned in-flight states as uncertain."""
        path = Path(runtime_home) / "c1-telegram-dispatch.sqlite3"
        if not path.is_file():
            return None
        with contextlib.closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            row = db.execute(
                "SELECT status, updated_at FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
        if row is None:
            return None
        durable_status = row[0]
        status = durable_status
        if classify_inflight and durable_status in {"DISPATCHING", "EFFECT_STARTED"}:
            status = "UNCERTAIN"
        return {
            "request_id": request_id,
            "status": status,
            "durable_status": durable_status,
            "updated_at": row[1],
        }

    def _worker_bridge(self, async_method, *args, **kwargs):
        # Reject BEFORE constructing/scheduling a coroutine on its own loop.
        if threading.get_ident() == self._effect_thread:
            raise C1DispatchClosed("use the async dispatch API on the owner loop")
        if self._effect_loop.is_closed():
            raise C1DispatchClosed("dispatch loop closed")
        return asyncio.run_coroutine_threadsafe(
            async_method(*args, **kwargs), self._effect_loop,
        ).result()

    def uncertain(self) -> None:
        self._worker_bridge(self.aclose)

    async def _close(self) -> None:
        # Close immediately, before any DB wait or effect lock. A read already
        # running on a worker cannot reopen this owner-loop fence when it returns.
        self._closed = True
        active = self._effect_task
        if active is not None and active is not asyncio.current_task() and not active.done():
            active.cancel()
        await _c1_db_work(self._mark_closed_outcome)

    async def aclose(self) -> None:
        """Close the dispatch; distinguish a proven no-send from a started effect."""
        await self._close()

    async def reject_not_sent(self) -> None:
        """Reject before sending; an already-started effect remains uncertain."""
        await self._close()

    def _mark_closed_outcome(self) -> None:
        with self._connect() as db:
            db.execute(
                "UPDATE requests SET status = CASE "
                "WHEN status = 'DISPATCHING' THEN 'REJECTED_NOT_SENT' "
                "ELSE 'UNCERTAIN' END, updated_at = ? "
                "WHERE request_id = ? AND status IN ('DISPATCHING', 'EFFECT_STARTED', 'EFFECT_FINISHED')",
                (_utc_now().isoformat(), self.request_id),
            )

    def _check_deadline(self):
        if self._closed or time.monotonic() >= self.deadline or _utc_now() >= self.expires_at:
            raise C1DispatchClosed("dispatch deadline expired or closed")

    def check_active(self) -> None:
        """Worker-only admission check; recheck time AFTER the blocking read."""
        if threading.get_ident() == self._effect_thread:
            raise C1DispatchClosed("admission preparation belongs on a worker")
        state = self._worker_bridge(_c1_db_work, self.readback)
        self._check_deadline()
        if not self.claimed or state != "DISPATCHING":
            raise C1DispatchClosed("dispatch already claimed or uncertain")

    def run_effect(self, effect, *args, **kwargs):
        """Worker bridge to the async single-attempt transport boundary."""
        return self._worker_bridge(self.arun_effect, effect, *args, **kwargs)

    def _claim_effect(self):
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return db.execute(
                "UPDATE requests SET status = 'EFFECT_STARTED', updated_at = ? "
                "WHERE request_id = ? AND status = 'DISPATCHING'",
                (_utc_now().isoformat(), self.request_id),
            ).rowcount == 1

    def _finish_effect(self):
        with self._connect() as db:
            db.execute(
                "UPDATE requests SET status = 'EFFECT_FINISHED', updated_at = ? "
                "WHERE request_id = ? AND status = 'EFFECT_STARTED'",
                (_utc_now().isoformat(), self.request_id),
            )

    async def arun_effect(self, effect, *args, **kwargs):
        """Await exactly one prepared async API attempt, never deferred work."""
        if asyncio.get_running_loop() is not self._effect_loop:
            raise C1DispatchClosed("transport must use the owner loop")
        try:
            async with self._gate:
                self._effect_task = asyncio.current_task()
                self._check_deadline()
                if not self.claimed or not inspect.iscoroutinefunction(effect):
                    raise C1DispatchClosed("transport must be an async single-attempt callable")
                if not await _c1_db_work(self._claim_effect):
                    raise C1DispatchClosed("dispatch already claimed or uncertain")
                state = await _c1_db_work(self.readback)
                # Same critical section, after ALL blocking I/O. No await between
                # this final state/time check and entry into the transport body.
                if state != "EFFECT_STARTED":
                    raise C1DispatchClosed("transport boundary closed")
                self._check_deadline()
                remaining = min(
                    self.deadline - time.monotonic(),
                    (self.expires_at - _utc_now()).total_seconds(),
                )
                async with asyncio.timeout(remaining):
                    self._check_deadline()
                    result = await effect(*args, **kwargs)
                self._check_deadline()
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise C1DispatchClosed("transport returned deferred work")
                await _c1_db_work(self._finish_effect)
                self._check_deadline()
                return result
        except SystemExit as exc:
            await self.aclose()
            raise RuntimeError("C1 transport exited") from exc
        except BaseException:
            await self.aclose()
            raise


C1_TELEGRAM_PLUGIN = "c1_telegram_personal"
C1_PID_NOT_CLAIMED_ERROR = "C1 initializer requires this profile's live Gateway."


def retry_c1_binding_after_pid_claim() -> bool:
    """Load the C1 initializer once more after this process claimed ``gateway.pid``.

    Config loading discovers plugins before the PID claim, so the initializer's live-Gateway
    check fails closed on that first pass and discovery never retries it. Only a bundled,
    failed C1 load is retried; the plugin itself re-checks the PID identity. True on success.
    """
    from hermes_cli.plugins import get_plugin_manager

    manager = get_plugin_manager()
    loaded = manager._plugins.get(C1_TELEGRAM_PLUGIN)
    if (
        loaded is None
        or loaded.enabled
        or loaded.error != C1_PID_NOT_CLAIMED_ERROR
        or loaded.manifest.source != "bundled"
    ):
        return False
    manager._load_plugin(loaded.manifest)
    retried = manager._plugins.get(C1_TELEGRAM_PLUGIN)
    if retried is not None and retried.enabled and not retried.error:
        logger.info("C1 Telegram initializer registered after the gateway PID claim")
        return True
    return False


def _c1_binding_callback():
    from hermes_cli.plugins import has_hook, iter_hook_callbacks

    if not has_hook(POST_ADMISSION_HOOK):
        return None
    callbacks = iter_hook_callbacks(POST_ADMISSION_HOOK)
    if len(callbacks) != 1:
        return None
    callback = callbacks[0]
    try:
        parameter = inspect.signature(callback).parameters.get("c1_dispatch")
    except (TypeError, ValueError):
        return None
    # **kwargs alone is not evidence that the binding implements the effect-gate contract.
    if parameter is None or parameter.kind not in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY
    ):
        return None
    return callback


def _c1_enabled_settings() -> dict | None:
    """Return the configured C1 settings only when the chief plugin is explicitly enabled."""
    from hermes_cli.config import load_config_readonly
    from hermes_cli.plugins_discovery import _get_disabled_plugins, _get_enabled_plugins
    from hermes_cli.plugins_state import _plugin_settings_entry
    from hermes_constants import get_hermes_home, profile_name_for_home

    if profile_name_for_home(get_hermes_home()) != C1_TELEGRAM_PROFILE:
        return None
    enabled = _get_enabled_plugins()
    if enabled is None or C1_TELEGRAM_PLUGIN not in enabled:
        return None
    if C1_TELEGRAM_PLUGIN in _get_disabled_plugins():
        return None
    entry = _plugin_settings_entry(load_config_readonly() or {}, C1_TELEGRAM_PLUGIN)
    if not isinstance(entry, dict):
        return None
    settings = entry.get("settings")
    if not isinstance(settings, dict):
        settings = entry.get("config")
    if not isinstance(settings, dict) or settings.get("enabled") is not True:
        return None
    return settings


def _c1_expected_gateway_pin(loaded: Any, settings: dict) -> str:
    """Read the agent-ops gate value through the bundled plugin's validated loader."""
    module = getattr(loaded, "module", None)
    load_modules = getattr(module, "_agent_ops_modules", None)
    if not callable(load_modules):
        return "UNKNOWN"
    try:
        _production, operator_ops = load_modules(settings.get("agent_ops_repo"))
        value = getattr(operator_ops, "_HERMES_PIN", None)
    except Exception:
        logger.debug("Could not read the C1 Hermes pin for startup diagnostics.", exc_info=True)
        return "UNKNOWN"
    return value if type(value) is str and value else "UNKNOWN"


def c1_binding_startup_error() -> str | None:
    """Describe the fail-closed startup condition for an enabled but unbound chief C1."""
    settings = _c1_enabled_settings()
    if settings is None or _c1_binding_callback() is not None:
        return None

    from gateway.code_skew import current_code_sha
    from hermes_cli.plugins import get_plugin_manager, iter_hook_callbacks

    manager = get_plugin_manager()
    loaded = manager._plugins.get(C1_TELEGRAM_PLUGIN)
    loaded_error = getattr(loaded, "error", None)
    callback_count = len(iter_hook_callbacks(POST_ADMISSION_HOOK))
    head = current_code_sha() or "UNKNOWN"
    expected_pin = _c1_expected_gateway_pin(loaded, settings)
    return (
        "C1 Telegram is enabled but its binding callback is unavailable "
        f"(loaded.error={loaded_error!r}, HEAD={head}, expected_pin={expected_pin}, "
        f"callback_count={callback_count})"
    )


def _invoke_c1_binding(callback, payload):
    from hermes_cli.plugins_dispatch import PluginDispatchMixin

    try:
        return PluginDispatchMixin._invoke_hook_callback(callback, payload)
    except SystemExit as exc:
        # Preserve the ordinary plugin dispatcher's isolation before reaching asyncio.
        raise RuntimeError("C1 binding exited") from exc


def _telegram_c1_identity(event: Any, source: Any) -> dict | None:
    """Build stable replay identity from the admitted Telegram Message, not update_id."""
    raw = getattr(event, "raw_message", None)
    chat = getattr(raw, "chat", None)
    user = getattr(raw, "from_user", None)
    chat_id = getattr(chat, "id", None)
    user_id = getattr(user, "id", None)
    message_id = getattr(raw, "message_id", None)
    message_date = getattr(raw, "date", None)

    if (
        type(chat_id) is not int or chat_id <= 0
        or type(user_id) is not int or user_id <= 0
        or type(message_id) is not int or message_id <= 0
        or getattr(chat, "type", None) != "private"
        or getattr(user, "is_bot", True) is not False
        or getattr(source, "chat_type", None) != "dm"
        or getattr(source, "is_bot", True) is not False
        or getattr(source, "chat_id", None) != str(chat_id)
        or getattr(source, "user_id", None) != str(user_id)
        or getattr(event, "message_id", None) != str(message_id)
        or getattr(raw, "text", None) != C1_TELEGRAM_REQUEST
        or getattr(event, "text", None) != C1_TELEGRAM_REQUEST
        or not isinstance(message_date, datetime)
        or message_date.tzinfo is None
        or message_date.utcoffset() is None
    ):
        return None

    platform = getattr(getattr(source, "platform", None), "value", None)
    if platform != "telegram":
        return None

    thread_id = getattr(source, "thread_id", None)
    if thread_id is not None and type(thread_id) is not str:
        return None

    # Telegram update_id is a delivery cursor and may change after long inactivity.
    # The per-chat message_id is the stable identity for duplicate delivery/restart.
    request_material = {
        "platform": platform,
        "profile": C1_TELEGRAM_PROFILE,
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "thread_id": thread_id,
        "message_id": str(message_id),
    }
    request_digest = _canonical_digest(request_material)
    conversation_digest = _canonical_digest({
        "platform": platform,
        "profile": C1_TELEGRAM_PROFILE,
        "chat_id": str(chat_id),
        "thread_id": thread_id,
    })
    issued_at = message_date.astimezone(UTC)
    expires_at = issued_at + timedelta(minutes=5)
    now = _utc_now()
    if now >= expires_at or issued_at > now + timedelta(seconds=C1_TELEGRAM_FUTURE_SKEW_SECONDS):
        return None
    return {
        "schema": _C1_TELEGRAM_IDENTITY_SCHEMA,
        "event_id": "gateway-" + request_digest,
        "request_id": "request-" + request_digest,
        "conversation_id": "conversation-" + conversation_digest,
        # The downstream C1 principal verifier must allowlist this Telegram
        # user_id to human:kudi68; these platform fields are not proof by themselves.
        "principal": "basic:kudi68",
        "principal_proof_ref": "telegram-proof-" + request_digest,
        "principal_proof": {
            "platform": platform,
            "user_id": str(user_id),
            "chat_id": str(chat_id),
            "thread_id": thread_id,
            "message_id": str(message_id),
        },
        "text": C1_TELEGRAM_REQUEST,
        "issued_at": issued_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "nonce": "telegram-nonce-" + request_digest,
    }


async def run_c1_telegram_personal(
    runner: Any, event: Any, source: Any, session_key: str
) -> tuple[bool, Optional[str]]:
    """Consume only the fixed chief Telegram request; never fall through on failure."""
    if not is_c1_telegram_personal_target(event, source, runner):
        return False, None
    platform = source.platform.value

    identity = _telegram_c1_identity(event, source)
    if identity is None:
        logger.warning("C1 Telegram request rejected before dispatch; consuming without agent fallback")
        return True, None

    admitted = _C1_IN_FLIGHT.acquire(blocking=False)
    dispatch = None
    binding_task = None
    try:
        async with _routed_scope(runner, source):
            from gateway.session_identity import canonical_identity
            runtime_identity = canonical_identity(source, runner=runner)
            dispatch = await C1TelegramDispatch.create(runtime_identity.runtime_home, identity)
            if not admitted:
                if dispatch.claimed:
                    await dispatch.reject_not_sent()
                logger.warning(
                    "C1 Telegram request rejected before send: busy; consumed without queue or send"
                )
                return True, None
            if not dispatch.claimed:
                logger.warning("C1 Telegram request already recorded: %s", await _c1_db_work(dispatch.readback))
                return True, None
            callback = _c1_binding_callback()
            if callback is None:
                await dispatch.reject_not_sent()
                logger.error("C1 Telegram fenced binding is unavailable; consuming without agent fallback")
                return True, None
            hook_source = source.to_dict()
            hook_source["c1_telegram_identity"] = identity
            binding_task = asyncio.create_task(
                asyncio.to_thread(
                    _invoke_c1_binding,
                    callback,
                    dict(
                        session_key=session_key,
                        platform=platform,
                        source=hook_source,
                        message_id=event.message_id,
                        text=C1_TELEGRAM_REQUEST,
                        telegram_identity=identity,
                        c1_dispatch=dispatch,
                    ),
                ),
            )
            await asyncio.wait_for(
                asyncio.shield(binding_task), timeout=C1_TELEGRAM_HOOK_TIMEOUT_SECONDS,
            )
            # EFFECT_FINISHED only records that the async transport returned;
            # delivery/receipt truth still belongs to the downstream C1 ledger.
            if await _c1_db_work(dispatch.readback) == "DISPATCHING":
                await dispatch.reject_not_sent()
    except asyncio.CancelledError:
        if dispatch is not None:
            await dispatch.aclose()
        raise
    except Exception as exc:
        if dispatch is not None:
            await dispatch.aclose()
        logger.warning(
            "C1 Telegram binding failed (%s); consuming without agent fallback",
            type(exc).__name__,
        )
    finally:
        if admitted:
            if binding_task is not None:
                if binding_task.done():
                    _release_c1_binding(binding_task)
                else:
                    binding_task.add_done_callback(_release_c1_binding)
            else:
                _C1_IN_FLIGHT.release()
    # The binding sends through its receipt-aware Gateway transport. Returning a
    # reply here would cause Hermes to send a second, unreceipted message.
    return True, None
