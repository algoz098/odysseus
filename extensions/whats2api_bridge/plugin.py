"""whats2api_bridge plugin entry point.

This OXS extension bridges Odysseus to https://whats2api.com — a hostedWhatsApp gateway that exposes both a REST API and a remote MCP server.

Two surfaces are exposed:

1. **MCP tools** that the agent can call as ``mcp__oxs_whats2api_bridge__<tool>``:
   - Connection management (``list_connections``, ``get_connection``,
     ``create_connection``, ``patch_connection``, ``remove_connection``,
     ``start_connection``, ``stop_connection``).
   - Messaging (``send_message``, ``send_reaction``, ``mark_read``,
     ``send_media``, ``send_presence``, ``is_on_whatsapp``,
     ``call_method`` — a generic passthrough for any Baileys/whatsmeow
     method).

2. **Inbound webhook** that receives the event fan-out from whats2api
   (POST body is ``{eventName: payload, ...}``). The handler persists
   every event, fires OXS events for the interesting ones, and updates
   the cached connections / messages tables.

Capabilities (declared in ``extension.json``):

- ``data`` — own tables ``whats2api_bridge__connections`` / ``__messages`` /
  ``__events``.
- ``mcp_tools`` / ``tools`` — agent-callable tools.
- ``webhooks_in`` / ``webhooks_out`` — receive events from whats2api.
- ``events_fire`` — fire ``whats2api.*`` events on the OXS bus so other
  extensions and the agent see them.
- ``static`` / ``ui_panel`` — serve a static panel under
  ``/ext-static/whats2api_bridge/``.
- ``network_outbound`` (allowlisted) — only ``https://api.whats2api.com``.
- ``filesystem_data_subpath`` (``cache``) — for downloaded media.

Lifecycle:
- ``on_install``:  write default settings.
- ``on_enable``:   register tools, webhooks, UI; build storage handle.
- ``on_disable``:  nothing (host unwinds registrations automatically).
- ``on_uninstall``: nothing (host drops our tables on uninstall).
"""


import asyncio
import hashlib
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Callable
from fastapi import Body, Header, Query, Request

import httpx

from core.database import engine
from core.extensions.events import dispatch_via_event_bus
from core.extensions.storage import PluginStorage

# Lazy imports — these pull in the MCP manager and DB model only when an
# MCP integration endpoint is hit. Done at import time so the plugin
# module can be imported without forcing those to be loaded eagerly.
def _mcp_manager():
    from src.mcp_manager import get_mcp_manager as _g
    return _g()

def _mcp_models():
    from src.database import SessionLocal as _SL, McpServer as _MS
    return _SL, _MS


EXTENSION_ID = "whats2api_bridge"
TOOL_NS = "oxs_whats2api_bridge"  # unique namespace; "whats2api" collides with the remote MCP server
DEFAULT_BASE_URL = "https://api.whats2api.com"

logger = logging.getLogger("oxs.whats2api_bridge")


# ── Settings helpers ───────────────────────────────────────────────────────


def _s(host, key: str, default: Any = None) -> Any:
    """Read a setting from the extension's settings namespace."""
    try:
        return host.get_settings().get(key, default)
    except Exception:
        return default


# ── Result envelope ────────────────────────────────────────────────────────


def _ok(data: Any) -> dict:
    return {"ok": True, "data": data}


def _err(message: str, **extra: Any) -> dict:
    out = {"ok": False, "error": str(message)}
    out.update(extra)
    return out


# ── HTTP client builder ────────────────────────────────────────────────────


def _build_client(host) -> httpx.AsyncClient:
    base = str(_s(host, "base_url", DEFAULT_BASE_URL)).rstrip("/")
    token = str(_s(host, "api_token", "") or "")
    if not token:
        raise RuntimeError(
            "whats2api_bridge: api_token is not configured. "
            "Set it in Settings → Extensions → Whats2API Bridge."
        )
    timeout = float(_s(host, "request_timeout_seconds", 30))
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "odysseus-whats2api-bridge/0.1.0",
    }
    return httpx.AsyncClient(base_url=base, headers=headers, timeout=timeout)


# ── Defensive defaults + MCP integrations registry ─────────────────────────


# Whitelist of user-tunable setting keys; used by both the admin PATCH
# handler and the renderer in the static UI.
_ALLOWED_USER_SETTINGS = (
    "base_url",
    "default_connection_id",
    "request_timeout_seconds",
    "retention_days",
    "log_inbound_events",
)


def _ensure_settings(host) -> None:
    """Idempotently pre-fill any unset/empty setting with a sane default.

    Called from both ``on_install`` (fresh installs) and ``on_enable``
    (covers upgrades where the install hook predated these defaults —
    or where the user wiped a single key). Never overwrites a value
    the user has actually typed.
    """
    try:
        ns = host.get_settings()
    except Exception:
        return

    def _set_if_empty(key, value):
        try:
            cur = ns.get(key)
        except Exception:
            cur = None
        if cur is None or (isinstance(cur, str) and not cur.strip()):
            try:
                ns.set(key, value)
            except Exception:
                pass

    _set_if_empty("base_url", DEFAULT_BASE_URL)
    _set_if_empty("request_timeout_seconds", 30)
    _set_if_empty("retention_days", 7)
    _set_if_empty("log_inbound_events", True)


def _resolve_connection_id(host, args: dict) -> str:
    cid = (
        args.get("connection_id")
        or args.get("connectionId")
        or args.get("connection")
        or ""
    )
    if not cid:
        cid = str(_s(host, "default_connection_id", "") or "")
    if not cid:
        raise RuntimeError(
            "connection_id is required: pass it in args or set "
            "default_connection_id in Settings → Whats2API Bridge."
        )
    return cid


# ── REST wrappers (typed, async) ───────────────────────────────────────────
def _make_async_handler(
    host,
    fn: Callable[..., Any],
    needs_connection_id: bool = False,
    needs_body: bool = False,
) -> Callable[[dict], Any]:
    """Build an async MCP handler that opens an httpx client per call.

    ``fn`` is an async function ``async def fn(client, args, host) -> Any``.
    If ``needs_connection_id`` is True the args must include connection_id
    (resolved against default_connection_id) before fn is invoked.
    """

    async def _handler(args: dict) -> dict:
        try:
            try:
                cid = _resolve_connection_id(host, args) if needs_connection_id else None
            except RuntimeError as e:
                return _err(str(e))
            client = _build_client(host)
            try:
                if needs_connection_id:
                    result = await fn(client, cid, args, host)
                else:
                    result = await fn(client, args, host)
                if isinstance(result, dict) and "ok" in result:
                    return result
                return _ok(result)
            finally:
                await client.aclose()
        except httpx.HTTPStatusError as e:
            body_text = ""
            try:
                body_text = e.response.text[:500]
            except Exception:
                pass
            return _err(
                f"whats2api returned HTTP {e.response.status_code}",
                status_code=e.response.status_code,
                body=body_text,
            )
        except httpx.HTTPError as e:
            return _err(f"network error: {type(e).__name__}: {e}")
        except RuntimeError as e:
            return _err(str(e))
        except Exception as e:
            logger.exception("whats2api MCP handler raised")
            return _err(f"{type(e).__name__}: {e}")

    return _handler


# ── Tool factories ─────────────────────────────────────────────────────────
def _read_tools(host, storage: PluginStorage):
    """Read-only tools that query the local extension tables.

    These let the agent see what the bridge has already ingested from
    whats2api.com without making a new outbound call. Useful for
    "what did this number send me?" or "show me today's events" type
    questions — the data is right here in ``whats2api_bridge__messages``,
    ``whats2api_bridge__events`` and ``whats2api_bridge__connections``.
    """

    async def _list_messages(client, args, host):
        """List recent messages from the local table.

        Filters: connection_id, jid, direction ("in"/"out"), since (epoch
        seconds), text_search, limit (default 25, max 200).
        """
        from core.database import engine
        where = []
        params: dict = {}
        cid = args.get("connection_id")
        if cid:
            where.append("connection_id = :cid")
            params["cid"] = cid
        jid = args.get("jid")
        if jid:
            where.append("remote_jid LIKE :jid")
            params["jid"] = f"%{jid}%"
        direction = args.get("direction")
        if direction in ("in", "out"):
            where.append("direction = :dir")
            params["dir"] = direction
        since = args.get("since")
        if since is not None:
            where.append("ts >= :since")
            params["since"] = int(since)
        text = args.get("text_search")
        if text:
            where.append("text LIKE :text")
            params["text"] = f"%{text}%"
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        limit = max(1, min(int(args.get("limit") or 25), 200))
        sql = (
            f"SELECT id, connection_id, direction, remote_jid, from_me, "
            f"push_name, text, message_type, ts "
            f"FROM {storage.table_name('messages')} {where_sql} "
            f"ORDER BY ts DESC LIMIT {limit}"
        )
        rows = storage.execute_raw(sql, params)
        return _ok({"messages": rows, "count": len(rows)})

    async def _get_message(client, args, host):
        from core.database import engine
        msg_id = args.get("id")
        if not msg_id:
            return _err("id is required")
        sql = (
            f"SELECT id, connection_id, direction, remote_jid, from_me, "
            f"chat_jid, sender_jid, push_name, text, message_type, ts, raw_json "
            f"FROM {storage.table_name('messages')} WHERE id = :id"
        )
        rows = storage.execute_raw(sql, {"id": msg_id})
        if not rows:
            return _err("message not found")
        return _ok({"message": rows[0]})

    async def _list_events(client, args, host):
        from core.database import engine
        where = []
        params: dict = {}
        ev = args.get("event_name")
        if ev:
            where.append("event_name = :en")
            params["en"] = ev
        cid = args.get("connection_id")
        if cid:
            where.append("connection_id = :cid")
            params["cid"] = cid
        since = args.get("since")
        if since is not None:
            where.append("received_at >= :since")
            params["since"] = int(since)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        limit = max(1, min(int(args.get("limit") or 25), 200))
        sql = (
            f"SELECT id, event_name, connection_id, user_id, received_at, "
            f"raw_json "
            f"FROM {storage.table_name('events')} {where_sql} "
            f"ORDER BY received_at DESC LIMIT {limit}"
        )
        rows = storage.execute_raw(sql, params)
        # Truncate the heavy raw_json field for the listing
        for r in rows:
            if r.get("raw_json"):
                r["raw_json"] = r["raw_json"][:500] + ("..." if len(r["raw_json"]) > 500 else "")
        return _ok({"events": rows, "count": len(rows)})

    async def _list_connections_local(client, args, host):
        from core.database import engine
        limit = max(1, min(int(args.get("limit") or 25), 200))
        sql = (
            f"SELECT id, name, engine, phone, status, owner_id, updated_at "
            f"FROM {storage.table_name('connections')} ORDER BY updated_at DESC LIMIT {limit}"
        )
        rows = storage.execute_raw(sql, {})
        return _ok({"connections": rows, "count": len(rows)})

    return [
        (
            "list_messages",
            {
                "type": "object",
                "properties": {
                    "connection_id": {"type": "string", "description": "filter by connection id"},
                    "jid": {"type": "string", "description": "substring match on remote_jid"},
                    "direction": {"type": "string", "enum": ["in", "out"]},
                    "since": {"type": "integer", "description": "epoch seconds; only messages >= this"},
                    "text_search": {"type": "string", "description": "LIKE match on text"},
                    "limit": {"type": "integer", "description": "1-200, default 25"},
                },
                "additionalProperties": False,
            },
            _make_async_handler(host, _list_messages, needs_connection_id=False),
        ),
        (
            "get_message",
            {
                "type": "object",
                "properties": {"id": {"type": "string", "description": "message id (primary key)"}},
                "required": ["id"],
                "additionalProperties": False,
            },
            _make_async_handler(host, _get_message, needs_connection_id=False),
        ),
        (
            "list_events",
            {
                "type": "object",
                "properties": {
                    "event_name": {"type": "string"},
                    "connection_id": {"type": "string"},
                    "since": {"type": "integer"},
                    "limit": {"type": "integer"},
                },
                "additionalProperties": False,
            },
            _make_async_handler(host, _list_events, needs_connection_id=False),
        ),
        (
            "list_connections_local",
            {
                "type": "object",
                "properties": {"limit": {"type": "integer"}},
                "additionalProperties": False,
            },
            _make_async_handler(host, _list_connections_local, needs_connection_id=False),
        ),
    ]



# ── Inbound webhook handler ────────────────────────────────────────────────


# Filename (under the extension's data path) where we persist the inbound
# webhook token so a container restart doesn't rotate it out from under
# the operator's ``whats2api.com`` configuration. Delete this file to
# force a fresh token on the next enable.
_INBOUND_TOKEN_FILENAME = "inbound_webhook_token"


def _inbound_token_path(host) -> "Path":
    """Absolute path to the on-disk token file (under data/extensions/)."""
    return Path(host.get_data_path()) / _INBOUND_TOKEN_FILENAME


def _read_inbound_token(host) -> str | None:
    """Read the persisted token, or None if absent / unreadable."""
    try:
        with open(_inbound_token_path(host), "r", encoding="utf-8") as f:
            tok = f.read().strip()
        return tok or None
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.warning("whats2api_bridge: could not read inbound token file: %s", e)
        return None


def _write_inbound_token(host, token: str) -> None:
    """Persist the token with owner-only permissions (0600).

    Creates the parent directory first so this works both in production
    (where ``on_install`` ran and the data dir exists) and in tests
    (where the framework only creates the top-level ``data/``).
    """
    path = _inbound_token_path(host)
    try:
        os.makedirs(path.parent, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, token.encode("utf-8") + b"\n")
        finally:
            os.close(fd)
    except Exception as e:
        logger.warning("whats2api_bridge: could not persist inbound token: %s", e)
        raise


async def _async_pin_webhook_token(host, *, max_wait: float = 1.0) -> str | None:
    """Wait for the framework to finish ``_wire`` (which installs the
    freshly-generated webhook record) and then run the pinner.

    Without the wait, ``_pin_webhook_token`` would run before
    ``_webhooks[ext_id]`` is populated and return ``None``.
    """
    try:
        from core.extensions import webhooks as _oxs_wh
    except Exception:
        return None
    waited = 0.0
    while not _oxs_wh.list_for_extension(EXTENSION_ID) and waited < max_wait:
        await asyncio.sleep(0.01)
        waited += 0.01
    pinned = _pin_webhook_token(host)
    if pinned:
        logger.info("whats2api_bridge inbound webhook token pinned to disk")
    else:
        logger.warning(
            "whats2api_bridge: webhook record never appeared in the "
            "registry after %.2fs; token not pinned", max_wait,
        )
    return pinned


def _generate_url_token() -> str:
    """Generate a fresh URL-safe webhook token (the framework's default)."""
    import secrets as _secrets
    return _secrets.token_urlsafe(32)


def _validate_url_token(raw: str) -> None:
    """Reject tokens that wouldn't survive a URL round-trip. The only
    real constraint is the charset — ``[A-Za-z0-9_-]`` mirrors what
    ``secrets.token_urlsafe`` produces so any operator-typed value can
    replace a random one without rewriting the dispatcher. Length is
    bounded only to keep pathological inputs out of the URL path.
    """
    import re as _re
    if len(raw) > 1024:
        raise ValueError("webhook_url_token must be at most 1024 characters")
    if not _re.fullmatch(r"[A-Za-z0-9_-]+", raw):
        raise ValueError(
            "webhook_url_token may only contain URL-safe characters "
            "(A–Z, a–z, 0–9, '-', '_') — the value goes into the URL path"
        )


def _apply_url_token(host, raw: str) -> None:
    """Persist ``raw`` to disk AND patch the live webhook record so the
    dispatcher accepts the new token immediately (no restart needed).

    Raises on persistence failure so the admin handler can return 5xx
    instead of leaving the operator thinking the URL changed when it
    actually didn't.
    """
    import re as _re  # noqa: F401  (mirrors the validation regex above)
    _validate_url_token(raw)
    # Persist to the same file the existing pinner reads on enable.
    _write_inbound_token(host, raw)
    # Patch the in-memory record so subsequent requests route correctly
    # and the next /admin/config GET returns the new full URL.
    try:
        from core.extensions import webhooks as _oxs_wh
    except Exception as e:
        raise RuntimeError(
            "webhook engine not importable; token persisted but "
            f"dispatcher not patched: {e}"
        )
    recs = _oxs_wh.list_for_extension(EXTENSION_ID)
    if not recs:
        # No live record yet (extension may be disabled). The file on
        # disk is the source of truth — the pinner will pick it up on
        # the next enable.
        return
    rec = recs[-1]
    rec.token_hash = _oxs_wh.hash_token(raw)
    rec._raw_token = raw


def _pin_webhook_token(host) -> str | None:
    """Pin the inbound webhook token across enable cycles.

    OXS's ``register_webhook`` always generates a fresh 32-byte token,
    which means every container restart silently invalidates the URL the
    operator configured in ``whats2api.com``. To keep the URL stable we:

      1. If a token file already exists on disk, rewrite the just-
         installed record's ``token_hash`` and ``_raw_token`` so the
         dispatcher keeps accepting the original token.
      2. Otherwise grab the token the framework just generated, save it
         to disk, and return it so the operator can copy the URL once.

    Returns the token the dispatcher will now accept (or ``None`` if
    the webhook wasn't actually installed — e.g. capability denied).
    """
    try:
        from core.extensions import webhooks as _oxs_wh
    except Exception:
        return None
    recs = _oxs_wh.list_for_extension(EXTENSION_ID)
    if not recs:
        return None
    rec = recs[-1]  # the one we just installed
    saved = _read_inbound_token(host)
    if saved:
        try:
            rec.token_hash = _oxs_wh.hash_token(saved)  # type: ignore[attr-defined]
            rec._raw_token = saved  # type: ignore[attr-defined]
            return saved
        except Exception as e:
            logger.warning("whats2api_bridge: token pin failed, using fresh: %s", e)
    fresh = _oxs_wh.get_raw_token(rec)
    if fresh:
        _write_inbound_token(host, fresh)
    return fresh


_EVENT_TO_OXS = {
    "messages.upsert": "whats2api.message.received",
    "Message": "whats2api.message.received",
    "messages.update": "whats2api.message.update",
    "messages.delete": "whats2api.message.delete",
    "messages.reaction": "whats2api.message.reaction",
    "messages.media-update": "whats2api.message.media",
    "message-receipt.update": "whats2api.message.receipt",
    "connection.update": "whats2api.connection.update",
    "chats.upsert": "whats2api.chat.upsert",
    "chats.update": "whats2api.chat.update",
    "chats.delete": "whats2api.chat.delete",
    "contacts.upsert": "whats2api.contact.upsert",
    "contacts.update": "whats2api.contact.update",
    "groups.upsert": "whats2api.group.update",
    "groups.update": "whats2api.group.update",
    "group-participants.update": "whats2api.group.participants",
    "presence.update": "whats2api.presence.update",
    "blocklist.set": "whats2api.blocklist.set",
    "blocklist.update": "whats2api.blocklist.update",
    "call": "whats2api.call",
}


def _coerce_int(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, dict):
        # Baileys Long shape: {"low": <int>, "high": <int>, "unsigned": bool}
        low = v.get("low")
        high = v.get("high", 0)
        if isinstance(low, int) and isinstance(high, int):
            return (high << 32) | (low & 0xFFFFFFFF)
    return None


def _synth_id_from_payload(payload: Any) -> str:
    """Deterministic id for a `Message` event whose payload is bare content.

    whats2api fans out per-message `Message` events with just the message
    body (no key wrapper, no messageTimestamp). The messages table needs
    a stable primary key, so we hash the canonicalized payload and prefix
    with ``synth:`` so it's obvious in the UI that the row came from a
    partial event. Same payload → same id → re-deliveries hit the unique
    constraint instead of duplicating the row. Once the row is enriched
    with the corresponding ``Info`` event the synthetic id is replaced
    by the real WhatsApp message id (``Info.ID``).
    """
    try:
        canonical = json.dumps(payload, sort_keys=True, default=str)
    except Exception:
        canonical = repr(payload)
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:24]
    return "synth:" + digest


def _enrich_with_info(m: dict, info: dict) -> dict:
    """Fill the gaps a bare-content `Message` leaves in the row.

    The per-message ``Info`` event carries the metadata the bare
    ``Message`` payload doesn't have: real WhatsApp message id, chat
    jid, sender jid, push name, ISO timestamp, message type, from-me
    flag. Without this enrichment, the row would have a synthetic id
    and empty jids — useless for searching or correlating with the
    ``messages.upsert`` envelope when it arrives later.

    Only fills fields whose value the caller couldn't extract (i.e.
    rows coming from a bare-content shape, signalled by a synthetic
    ``synth:`` prefix on the id). Never overwrites a non-empty value
    the caller already extracted from a full ``messages.upsert``
    envelope — those rows already have every field and re-enriching
    would risk regressions if a key name ever drifts.
    """
    if not isinstance(info, dict) or not m:
        return m
    # Only enrich if the row came from a bare-content payload. A row
    # from a full envelope already has every field populated; touching
    # it would only introduce drift risk.
    is_partial = isinstance(m.get("id"), str) and m["id"].startswith("synth:")
    if not is_partial:
        return m
    real_id = info.get("ID")
    if real_id:
        m["id"] = str(real_id)
    chat = info.get("Chat")
    if chat:
        m["chat_jid"] = str(chat)
        m["remote_jid"] = str(chat)
    sender_alt = info.get("SenderAlt") or info.get("Sender")
    if sender_alt:
        m["sender_jid"] = str(sender_alt)
    push = info.get("PushName")
    if push:
        m["push_name"] = str(push)
    ts_iso = info.get("Timestamp")
    if ts_iso:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00"))
            parsed = int(dt.timestamp())
            if parsed > 0:
                m["ts"] = parsed
        except Exception:
            pass
    mtype = info.get("Type")
    if mtype:
        m["message_type"] = str(mtype)
    if "IsFromMe" in info:
        m["from_me"] = bool(info.get("IsFromMe"))
    return m


_BARE_CONTENT_KEYS = (
    "conversation",
    "extendedTextMessage",
    "imageMessage",
    "videoMessage",
    "audioMessage",
    "documentMessage",
    "stickerMessage",
    "contactMessage",
    "locationMessage",
    "messageContextInfo",
)


def _is_bare_content(payload: dict) -> bool:
    """True if the payload looks like a Baileys `message` field with no
    key/timestamp wrapper. Distinct from the standard `Message` event
    envelope (which carries `key` + `message` + `pushName`)."""
    if "key" in payload or "messageTimestamp" in payload:
        return False
    return any(k in payload for k in _BARE_CONTENT_KEYS)


def _extract_message(payload: Any) -> dict | None:
    """Return a normalized message dict from a Baileys message payload.

    whats2api relays three shapes:
      1. ``messages.upsert`` — ``{"type": "notify", "messages": [msg1, ...]}``
      2. ``Message`` — full envelope ``{"key": ..., "message": ...,
         "pushName": ..., "messageTimestamp": ...}``
      3. ``Message`` — bare content (the inner ``message`` field only,
         e.g. ``{"conversation": "oi", "messageContextInfo": {...}}``).
         whats2api fans out the Baileys sub-event with just the message
         body — no key, no timestamp. We extract the text, synthesize a
         deterministic id, and mark the row as ``partial`` so the user
         can still see it in the Messages tab.
    """
    if not isinstance(payload, dict):
        return None
    is_partial = False
    batch = payload.get("messages")
    if isinstance(batch, list) and batch:
        msg = batch[0]
    elif "key" in payload and "message" in payload:
        msg = payload
    elif _is_bare_content(payload):
        msg = {"message": payload}
        is_partial = True
    else:
        return None
    if not isinstance(msg, dict):
        return None
    key = msg.get("key") or {}
    inner = msg.get("message") or {}
    text = (
        inner.get("conversation")
        or (inner.get("extendedTextMessage") or {}).get("text")
        # Some messages wrap the text in messageContextInfo (limit-sharing
        # notifications, device-list messages) — pull the text out of it
        # when the regular fields are missing.
        or (inner.get("messageContextInfo") or {}).get("conversation")
        or ""
    )
    push_name = msg.get("pushName")
    ts = _coerce_int(msg.get("messageTimestamp")) or int(time.time())
    if is_partial:
        msg_id = _synth_id_from_payload(payload)
        remote_jid = ""
        chat_jid = ""
        sender_jid = ""
        message_type = "partial"
        raw_json = json.dumps(payload, default=str)[:65000]
    else:
        msg_id = key.get("id")
        remote_jid = key.get("remoteJid")
        chat_jid = key.get("remoteJid")
        sender_jid = key.get("participant")
        message_type = (payload.get("type") or "")
        raw_json = json.dumps(msg, default=str)[:65000]
    return {
        "id": msg_id,
        "remote_jid": remote_jid,
        "from_me": bool(key.get("fromMe")),
        "chat_jid": chat_jid,
        "sender_jid": sender_jid,
        "push_name": push_name,
        "text": text,
        "message_type": message_type,
        "ts": ts,
        "raw_json": raw_json,
    }


def _persist_message(storage: PluginStorage, direction: str, m: dict) -> None:
    if not m.get("id"):
        return
    try:
        storage.insert(
            "messages",
            id=m["id"],
            connection_id=m.get("connection_id", ""),
            direction=direction,
            remote_jid=m.get("remote_jid") or "",
            from_me=("1" if m.get("from_me") else "0"),
            chat_jid=m.get("chat_jid") or "",
            sender_jid=m.get("sender_jid") or "",
            push_name=m.get("push_name") or "",
            text=m.get("text") or "",
            message_type=m.get("message_type") or "",
            ts=int(m.get("ts") or int(time.time())),
            raw_json=m.get("raw_json") or "",
        )
    except Exception as e:
        logger.warning("whats2api_bridge: failed to persist message: %s", e)


def _persist_event(
    storage: PluginStorage,
    event_name: str,
    connection_id: str,
    user_id: str,
    payload: Any,
) -> None:
    if not _s_for_storage_get("log_inbound_events", True):
        return
    try:
        storage.insert(
            "events",
            event_name=event_name,
            connection_id=connection_id or "",
            user_id=user_id or "",
            received_at=int(time.time()),
            raw_json=json.dumps(payload, default=str)[:65000],
        )
    except Exception as e:
        logger.warning("whats2api_bridge: failed to persist event: %s", e)


_settings_value_cache: dict[str, Any] = {}


def _s_for_storage_get(key: str, default: Any = None) -> bool:
    """Cheap cache of bool settings used inside the webhook hot path.

    The webhook handler runs in the asyncio loop and we don't want to
    touch the SettingsNamespace for every event (cheap reads but adds
    up under high-volume chats).
    """
    return _settings_value_cache.get(key, default)


def _build_webhook_handler(host, storage: PluginStorage):
    """Return the inbound webhook handler closure.

    The OXS framework calls the registered handler via::

        await rec.handler(body=..., headers=..., query=..., event_filter=...)

    so the original signature is positional kwargs. FastAPI, however,
    introspects every registered route and tries to interpret unannotated
    parameters (body / headers / query) as request inputs — which fails
    pydantic validation with HTTP 422. The clean fix is to expose a
    handler with the exact kwargs the OXS dispatcher passes — but bypass
    FastAPI's introspection entirely by NOT registering this function as
    a route. OXS dispatches the handler via an ``asyncio.create_task`` in
    ``_run_handler``; the route is the OXS-owned ``receive_webhook`` which
    has a proper FastAPI ``Request`` parameter.

    To keep both worlds happy we accept the kwargs explicitly here. Even
    though FastAPI would normally treat ``body`` / ``headers`` / ``query``
    as request inputs, the OXS route mount inspects via ``_wrap_async`` and
    delegates to ``asyncio.to_thread`` for sync functions — which means
    this handler is invoked from a background task without going through
    FastAPI's request validation at all.
    """
    async def _on_webhook(*, body=None, headers=None, query=None, event_filter=None):
        # FastAPI sees `body` / `headers` / `query` as required request
        # inputs (Body / Header / Query) and rejects the request with
        # HTTP 422 because the OXS framework invokes us with keyword
        # arguments, not as a real FastAPI handler. FastAPI 0.142 changed
        # the inference so that even ``= None`` defaults do not help when
        # the parameter name matches a reserved request-input identifier.
        # The OXS dispatcher runs the handler in a background task; it
        # never goes through FastAPI's request pipeline. We therefore
        # use ``*`` (keyword-only) so positional invocation is impossible
        # and rely on the caller to use ``handler(body=..., ...)``. The
        # default for each is None so accidental calls from OXS without
        # those kwargs would short-circuit cleanly.
        """Inbound webhook handler.

        The OXS framework dispatches the registered handler via::

            await rec.handler(body=..., headers=..., query=..., event_filter=...)

        FastAPI's parameter-name inference would normally treat unannotated
        ``body`` / ``headers`` / ``query`` as request inputs (Body / Header /
        Query) and fail pydantic validation with HTTP 422. We use the
        explicit ``Body()`` / ``Header()`` / ``Query()`` annotations and
        default to ``None`` so FastAPI treats them as fully optional and
        doesn't generate any required query/body parameters. The OXS
        framework's invocation is unaffected because kwargs are matched by
        parameter name, not by position.
        """
        """Inbound webhook handler.

        The OXS framework dispatches the registered handler via::

            await rec.handler(body=..., headers=..., query=..., event_filter=...)

        so the original parameter names are ``body`` / ``headers`` / ``query`` /
        ``event_filter``. FastAPI's parameter-name inference, however, treats
        ``body`` / ``headers`` / ``query`` as reserved request inputs which
        fails pydantic validation (HTTP 422). We rename the inbound
        parameters to ``*_in`` and rebind them to the original names here.
        """
        body = body
        headers = headers or {}
        connection_id = ""
        user_id = ""
        verified = False
        if isinstance(headers, dict):
            for k, v in headers.items():
                kl = k.lower()
                if kl == "whats2api" and str(v).lower() == "true":
                    verified = True
                elif kl == "userid":
                    user_id = str(v)
                elif kl == "connectionid":
                    connection_id = str(v)

        # ``body`` is the parsed JSON dict OR raw bytes if it wasn't JSON.
        if isinstance(body, bytes):
            try:
                body = json.loads(body.decode("utf-8"))
            except Exception as e:
                logger.warning("whats2api_bridge webhook: invalid JSON body: %s", e)
                return
        if not isinstance(body, dict):
            logger.warning("whats2api_bridge webhook: body is not a JSON object: %r", type(body))
            return

        if not verified:
            logger.warning(
                "whats2api_bridge webhook: missing/invalid 'whats2api: true' header "
                "(got headers=%r)",
                {k: v for k, v in (headers or {}).items()
                 if k.lower() in ("whats2api", "userid", "connectionid")},
            )

        # Body is a map of eventName -> payload. Per the docs, every
        # Baileys event from the ``ev`` bus is fanned out (creds.update
        # is filtered server-side).
        #
        # whats2api emits a per-message ``Info`` event with the chat jid,
        # sender jid, push name, message id and ISO timestamp right before
        # the bare-content ``Message`` event. We cache the most recent
        # ``Info`` in this body so a subsequent ``Message``/``RawMessage``
        # can fill in the metadata the bare payload doesn't carry.
        last_info: dict | None = None
        for event_name, payload in body.items():
            if not isinstance(event_name, str):
                continue
            oxs_event = _EVENT_TO_OXS.get(event_name)
            _persist_event(storage, event_name, connection_id, user_id, payload)

            if event_name == "Info" and isinstance(payload, dict):
                last_info = payload
                continue

            if event_name in ("messages.upsert", "Message", "RawMessage"):
                m = _extract_message(payload)
                if m:
                    if last_info is not None:
                        m = _enrich_with_info(m, last_info)
                    m["connection_id"] = connection_id
                    _persist_message(storage, "in", m)
                    if oxs_event and verified:
                        try:
                            dispatch_via_event_bus(
                                oxs_event,
                                owner=None,
                                payload={
                                    "connection_id": connection_id,
                                    "user_id": user_id,
                                    "message": {
                                        "id": m.get("id"),
                                        "remote_jid": m.get("remote_jid"),
                                        "from_me": m.get("from_me"),
                                        "push_name": m.get("push_name"),
                                        "text": m.get("text"),
                                        "ts": m.get("ts"),
                                        "type": m.get("message_type"),
                                    },
                                },
                            )
                        except Exception:
                            logger.exception("whats2api.message.received dispatch failed")
                continue

            if oxs_event and verified:
                try:
                    dispatch_via_event_bus(
                        oxs_event,
                        owner=None,
                        payload={
                            "connection_id": connection_id,
                            "user_id": user_id,
                            "event_name": event_name,
                            "data": payload,
                        },
                    )
                except Exception:
                    logger.exception("%s dispatch failed", oxs_event)

        # Refresh the connections cache on connection.update so the UI
        # can show the latest state without a manual refresh.
        if "connection.update" in body:
            try:
                await _refresh_connections_cache(host, storage, connection_id)
            except Exception:
                logger.exception("connection.update cache refresh failed")

    return _on_webhook


async def _refresh_connections_cache(host, storage: PluginStorage, hint_connection_id: str = "") -> None:
    """Best-effort cache refresh of the connections table."""
    try:
        client = _build_client(host)
    except RuntimeError:
        return
    try:
        r = await client.get("/connections")
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.warning("whats2api_bridge: cache refresh failed: %s", e)
        return
    finally:
        await client.aclose()

    if not isinstance(data, list):
        return
    now = int(time.time())
    # Upsert with INSERT OR REPLACE so we don't have to chase INSERT
    # errors when the connection is already cached.
    from sqlalchemy import text as _sa_text
    upsert_sql = _sa_text(
        "INSERT OR REPLACE INTO {tbl} "
        "(id, name, engine, phone, status, owner_id, raw_json, updated_at) "
        "VALUES (:id, :name, :engine, :phone, :status, :owner_id, :raw_json, :updated_at)"
        .format(tbl=storage.table_name("connections"))
    )
    with storage.engine.begin() as conn:
        for c in data:
            if not isinstance(c, dict):
                continue
            cid = c.get("_id") or c.get("id")
            if not cid:
                continue
            try:
                conn.execute(upsert_sql, {
                    "id": cid,
                    "name": (c.get("name") or ""),
                    "engine": (c.get("engine") or ""),
                    "phone": (c.get("phone") or ""),
                    "status": (c.get("status") or ""),
                    "owner_id": (c.get("ownerId") or c.get("owner_id") or ""),
                    "raw_json": json.dumps(c, default=str)[:65000],
                    "updated_at": now,
                })
            except Exception as e:
                logger.warning("whats2api_bridge: cache upsert failed: %s", e)


# ── Public read-only route for the static UI ────────────────────────────────


def _make_status_route(host, storage: PluginStorage):
    """Return a FastAPI-compatible callable for /ext-static status."""

    async def _route():  # type: ignore[no-untyped-def]
        # Public, unauthenticated, returns only non-secret stats.
        try:
            recent_events = storage.execute_raw(
                "SELECT event_name, connection_id, received_at "
                "FROM {tbl} ORDER BY received_at DESC LIMIT 10"
                .format(tbl=storage.table_name("events"))
            )
        except Exception:
            recent_events = []
        try:
            counts = storage.execute_raw(
                "SELECT direction, COUNT(*) AS n FROM {tbl} GROUP BY direction"
                .format(tbl=storage.table_name("messages"))
            )
        except Exception:
            counts = []
        return {
            "ok": True,
            "extension_id": EXTENSION_ID,
            "base_url": str(_s(host, "base_url", DEFAULT_BASE_URL)),
            "api_token_set": bool(_s(host, "api_token", "")),
            "default_connection_id": _s(host, "default_connection_id", "") or None,
            "recent_events": recent_events,
            "message_counts": counts,
            "inbound_url_pattern": (
                "/api/ext/" + EXTENSION_ID + "/webhook/<token>  "
                "(see Settings → Extensions → Whats2API Bridge → Webhooks "
                "for the live URL — token shown once at install)"
            ),
        }

    return _route


# ── whats2api.com MCP ────────────────────────────────────────────────────────
#
# This bridge exposes ONE controlled thing: the connection between
# Odysseus and whats2api.com's MCP server. Per the whats2api docs
# (https://whats2api.com/docs/mcp):
#
#   > MCP endpoint: https://api.whats2api.com/mcp
#   > Authorization: Bearer <token>
#
# That's all the operator ever configures — a single token from the
# whats2api dashboard, and an on/off switch. The transport, URL shape,
# Bearer header, and connection management are internal to Odysseus
# (handled by ``McpManager``); the bridge merely keeps a single row in
# ``mcp_servers`` in sync with the user's intent.


W2A_MCP_SERVER_ID = "whats2api"
W2A_MCP_NAME = "Whats2API MCP"
W2A_MCP_BASE_URL = "https://api.whats2api.com/mcp"


def _w2a_token_from_settings(host) -> str:
    try:
        return str(host.get_settings().get("whats2api_mcp_token") or "").strip()
    except Exception:
        return ""


def _w2a_enabled_from_settings(host) -> bool:
    try:
        return bool(host.get_settings().get("whats2api_mcp_enabled", False))
    except Exception:
        return False


async def _list_w2a_mcp(host) -> dict:
    """Return the single ``whats2api`` MCP integration's state.

    Tells the UI: whether a token is set, whether the integration is
    enabled, the live connection ``status`` and ``tool_count`` reported
    by ``McpManager``. Nothing about command/args/env/url — that's all
    internal.
    """
    token = _w2a_token_from_settings(host)
    enabled = _w2a_enabled_from_settings(host)
    status_info: dict = {}
    SessionLocal, McpServer = _mcp_models()
    db = SessionLocal()
    try:
        srv = db.query(McpServer).filter(McpServer.id == W2A_MCP_SERVER_ID).first()
        if srv:
            try:
                status_info = _mcp_manager().get_server_status(srv.id) or {}
            except Exception:
                status_info = {}
    finally:
        db.close()
    return {
        "ok": True,
        "id": W2A_MCP_SERVER_ID,
        "name": W2A_MCP_NAME,
        "enabled": bool(enabled),
        "token_set": bool(token),
        "status": status_info.get("status", "not_configured"),
        "tool_count": int(status_info.get("tool_count", 0) or 0),
        "error": status_info.get("error"),
    }


async def _update_w2a_mcp(host, body: dict) -> dict:
    """Apply ``token`` and/or ``enabled`` from the UI to settings + DB.

    The token is stored only in this extension's settings namespace
    (settings namespace is per-extension, in-memory). When enabled, we
    sync the row in ``mcp_servers`` so the live ``McpManager`` keeps a
    connected session. When disabled or the token is cleared, the row's
    ``is_enabled`` is flipped (or removed) and the connection tears
    down — the operator never sees URLs, headers, or env dicts.
    """
    if "token" in body:
        token = str(body.get("token") or "").strip()
        # whats2api tokens are 64 hex chars per the docs; allow common
        # variants without rejecting valid paste operations.
        if token and not re.fullmatch(r"[A-Za-z0-9_\-]{16,128}", token):
            return {"ok": False, "error": "token contains invalid characters"}
        try:
            host.get_settings().set("whats2api_mcp_token", token)
        except Exception as e:
            return {"ok": False, "error": f"could not persist token: {e}"}

    if "enabled" in body:
        try:
            host.get_settings().set(
                "whats2api_mcp_enabled", bool(body["enabled"])
            )
        except Exception as e:
            return {"ok": False, "error": f"could not persist enabled: {e}"}

    token = _w2a_token_from_settings(host)
    enabled = _w2a_enabled_from_settings(host)
    SessionLocal, McpServer = _mcp_models()
    db = SessionLocal()
    try:
        srv = db.query(McpServer).filter(McpServer.id == W2A_MCP_SERVER_ID).first()
        if not enabled or not token:
            # Disable / drop the row — operator has nothing to connect to.
            if srv:
                srv.is_enabled = False
                db.commit()
            try:
                await _mcp_manager().disconnect_server(W2A_MCP_SERVER_ID)
            except Exception:
                pass
            return await _list_w2a_mcp(host)

        url = f"{W2A_MCP_BASE_URL.rstrip('/')}/{token}"
        if srv is None:
            srv = McpServer(
                id=W2A_MCP_SERVER_ID,
                name=W2A_MCP_NAME,
                transport="http",
                command=None,
                args=json.dumps([]),
                env=json.dumps({}),
                url=url,
                is_enabled=True,
            )
            db.add(srv)
        else:
            srv.name = W2A_MCP_NAME
            srv.transport = "http"
            srv.command = None
            srv.args = json.dumps([])
            srv.env = json.dumps({})
            srv.url = url
            srv.is_enabled = True
        db.commit()
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": f"db sync failed: {e}"}
    finally:
        db.close()

    # Reconnect the live session so the new token takes effect.
    mgr = _mcp_manager()
    try:
        await mgr.disconnect_server(W2A_MCP_SERVER_ID)
        await mgr.connect_server(
            server_id=W2A_MCP_SERVER_ID,
            name=W2A_MCP_NAME,
            transport="http",
            command=None,
            args=[],
            env={},
            url=url,
        )
    except Exception as e:
        logger.warning("whats2api_bridge: reconnect after token update failed: %s", e)

    return await _list_w2a_mcp(host)


# ── Admin routes (config + data browser) ───────────────────────────────────


def _admin_config_get(host, storage):
    """GET /ext-api/whats2api_bridge/admin/config — full config incl. webhook URL."""
    ns = host.get_settings()
    base_raw = _s(host, "base_url", DEFAULT_BASE_URL)
    base = str(base_raw).strip().rstrip("/") if base_raw else DEFAULT_BASE_URL
    if not base:
        base = DEFAULT_BASE_URL

    # Look up the installed webhook to surface the full URL + token. The
    # token is only returned to an authenticated admin; this endpoint
    # is registered with auth="admin" below.
    webhook_url = None
    raw_token = None
    try:
        from core.extensions import webhooks as _oxs_wh
        recs = _oxs_wh.list_for_extension(EXTENSION_ID)
        if recs:
            rec = recs[0]
            webhook_url = f"/api/ext/{EXTENSION_ID}/webhook/{_oxs_wh.get_raw_token(rec)}"
            raw_token = _oxs_wh.get_raw_token(rec)
    except Exception:
        pass

    return {
        "ok": True,
        "base_url": base,
        "default_base_url": DEFAULT_BASE_URL,
        "api_token_set": bool(ns.get("api_token")),
        # The URL token is always populated after first enable (it's
        # either user-defined or the framework-generated one pinned to
        # disk). Used by the UI to set the field placeholder.
        "webhook_url_token_set": bool(_read_inbound_token(host)),
        "default_connection_id": ns.get("default_connection_id") or "",
        "request_timeout_seconds": int(ns.get("request_timeout_seconds") or 30),
        "retention_days": int(ns.get("retention_days") or 7),
        "log_inbound_events": bool(ns.get("log_inbound_events", True)),
        "webhook_url": webhook_url,
    }


def _admin_config_post(host, storage, body: dict):
    """POST /ext-api/whats2api_bridge/admin/config — update settings."""
    ns = host.get_settings()
    if "base_url" in body and body["base_url"]:
        candidate = str(body["base_url"]).strip().rstrip("/")
        if not candidate:
            candidate = DEFAULT_BASE_URL
        if not candidate.startswith("https://"):
            raise ValueError("base_url must start with https://")
        ns.set("base_url", candidate)
    if "default_connection_id" in body:
        ns.set("default_connection_id", str(body["default_connection_id"] or ""))
    if "api_token" in body:
        # Allow setting OR clearing the token (empty string = clear).
        ns.set("api_token", str(body["api_token"] or ""))
    if "webhook_url_token" in body:
        # The user-defined path token. Empty string → rotate to a fresh
        # random 32-byte token. Validation enforces URL-safe characters
        # and a sane minimum length; the field is then persisted to the
        # ``<data>/whats2api_bridge/inbound_webhook_token`` file (mode
        # 0600) AND the live ``_webhooks[ext_id]`` record is patched in
        # place so the dispatcher starts accepting the new token
        # immediately — no container restart required.
        raw = str(body["webhook_url_token"] or "").strip()
        if not raw:
            raw = _generate_url_token()
        else:
            _validate_url_token(raw)
        _apply_url_token(host, raw)
    if "request_timeout_seconds" in body:
        try:
            n = int(body["request_timeout_seconds"])
        except (TypeError, ValueError):
            raise ValueError("request_timeout_seconds must be an integer")
        ns.set("request_timeout_seconds", max(1, min(n, 300)))
    if "retention_days" in body:
        try:
            n = int(body["retention_days"])
        except (TypeError, ValueError):
            raise ValueError("retention_days must be an integer")
        ns.set("retention_days", max(1, min(n, 365)))
    if "log_inbound_events" in body:
        ns.set("log_inbound_events", bool(body["log_inbound_events"]))
    return {"ok": True}


# ── Data-browser search/edit/delete ────────────────────────────────────────


def _where_clause(filters: dict, allowed: tuple) -> tuple[str, dict]:
    """Build a SQL WHERE fragment + bound params from a filters dict.

    Whitelists column names to prevent SQL injection from query params.
    Supported operators per column: ``q`` (LIKE on text columns),
    ``since``/``until`` (numeric >=/<= on the ``ts``/``received_at``
    column), and exact-match for the rest.
    """
    clauses, params = [], {}
    for col, val in (filters or {}).items():
        if col in ("limit", "offset") or val is None or val == "":
            continue
        if col not in allowed:
            continue
        if col == "q":
            clauses.append("LOWER(CAST(text AS TEXT)) LIKE :q")
            params["q"] = f"%{str(val).lower()}%"
        elif col == "since":
            clauses.append("ts >= :since")
            try:
                params["since"] = int(val)
            except (TypeError, ValueError):
                continue
        elif col == "until":
            clauses.append("ts <= :until")
            try:
                params["until"] = int(val)
            except (TypeError, ValueError):
                continue
        else:
            clauses.append(f"{col} = :{col}")
            params[col] = val
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def _admin_messages_get(host, storage, **filters):
    allowed = ("connection_id", "chat_jid", "direction", "q", "since", "until", "text")
    where, params = _where_clause(filters, allowed)
    limit = int(filters.get("limit") or 50)
    offset = int(filters.get("offset") or 0)
    sql = (
        "SELECT id, connection_id, direction, remote_jid, chat_jid, sender_jid, "
        "push_name, text, message_type, ts, raw_json "
        f"FROM {storage.table_name('messages')} {where} "
        f"ORDER BY ts DESC LIMIT {limit} OFFSET {offset}"
    )
    rows = storage.execute_raw(sql, params)
    total_sql = f"SELECT COUNT(*) AS n FROM {storage.table_name('messages')} {where}"
    total = storage.execute_raw(total_sql, params)
    return {"ok": True, "messages": rows, "total": total[0]["n"] if total else 0}


def _admin_events_get(host, storage, **filters):
    allowed = ("event_name", "connection_id")
    where, params = _where_clause(filters, allowed)
    limit = int(filters.get("limit") or 50)
    offset = int(filters.get("offset") or 0)
    sql = (
        "SELECT id, event_name, connection_id, user_id, received_at, raw_json "
        f"FROM {storage.table_name('events')} {where} "
        f"ORDER BY received_at DESC LIMIT {limit} OFFSET {offset}"
    )
    rows = storage.execute_raw(sql, params)
    total_sql = f"SELECT COUNT(*) AS n FROM {storage.table_name('events')} {where}"
    total = storage.execute_raw(total_sql, params)
    return {"ok": True, "events": rows, "total": total[0]["n"] if total else 0}


def _admin_connections_get(host, storage, **filters):
    allowed = ("status", "owner_id")
    where, params = _where_clause(filters, allowed)
    limit = int(filters.get("limit") or 100)
    offset = int(filters.get("offset") or 0)
    sql = (
        "SELECT id, name, engine, phone, status, owner_id, updated_at "
        f"FROM {storage.table_name('connections')} {where} "
        f"ORDER BY updated_at DESC LIMIT {limit} OFFSET {offset}"
    )
    rows = storage.execute_raw(sql, params)
    total_sql = f"SELECT COUNT(*) AS n FROM {storage.table_name('connections')} {where}"
    total = storage.execute_raw(total_sql, params)
    return {"ok": True, "connections": rows, "total": total[0]["n"] if total else 0}


def _admin_delete_row(storage, table: str, row_id):
    full = storage.table_name(table)
    if table == "events":
        try:
            row_id = int(row_id)
        except (TypeError, ValueError):
            return 0
    sql = f"DELETE FROM {full} WHERE id = :id"
    return storage.execute_write(sql, {"id": row_id})


def _admin_message_patch(host, storage, row_id: str, body: dict):
    """PATCH text/push_name fields on a single message row."""
    sets, params = [], []
    if "text" in body:
        sets.append("text = :text")
        params["text"] = str(body["text"] or "")
    if "push_name" in body:
        sets.append("push_name = :push_name")
        params["push_name"] = str(body["push_name"] or "")
    if not sets:
        return {"ok": True, "updated": 0}
    params["id"] = row_id
    sql = f"UPDATE {storage.table_name('messages')} SET {', '.join(sets)} WHERE id = :id"
    n = storage.execute_write(sql, params)
    return {"ok": True, "updated": n}


def _make_admin_handlers(host, storage):
    """Build FastAPI-compatible callables for the admin endpoints.

    Returns a dict ``{path: {method: handler}}``; the registry's
    ``register`` function iterates and registers each.
    """
    import json as _json
    from fastapi import Request, Body, Query, Header

    async def _read_json(request: Request) -> dict:
        try:
            raw = await request.body()
            return _json.loads(raw or b"{}")
        except Exception:
            return {}

    async def _get_config(request: Request):
        return _admin_config_get(host, storage)

    async def _post_config(request: Request):
        body = await _read_json(request)
        try:
            return _admin_config_post(host, storage, body)
        except ValueError as e:
            from fastapi import HTTPException
            raise HTTPException(400, str(e))

    async def _get_messages(request: Request):
        qp = dict(request.query_params)
        return _admin_messages_get(host, storage, **qp)

    async def _get_events(request: Request):
        return _admin_events_get(host, storage, **dict(request.query_params))

    async def _get_connections(request: Request):
        return _admin_connections_get(host, storage, **dict(request.query_params))

    async def _del_message(request: Request, message_id: str):
        _admin_delete_row(storage, "messages", message_id)
        return {"ok": True}

    async def _del_event(request: Request, event_id: str):
        _admin_delete_row(storage, "events", event_id)
        return {"ok": True}

    async def _del_connection(request: Request, connection_id: str):
        _admin_delete_row(storage, "connections", connection_id)
        return {"ok": True}

    async def _patch_message(request: Request, message_id: str):
        body = await _read_json(request)
        return _admin_message_patch(host, storage, message_id, body)

    # ── whats2api MCP (single integration) ───────────────────────────────
    # Two endpoints cover the entire surface area: ``GET /admin/mcp``
    # for state + ``PATCH /admin/mcp`` for token / active toggle.
    async def _get_mcp(request: Request):
        return await _list_w2a_mcp(host)

    async def _patch_mcp(request: Request):
        body = await _read_json(request)
        return await _update_w2a_mcp(host, body)

    return {
        "/ext-api/whats2api_bridge/admin/config": {"GET": _get_config, "POST": _post_config},
        "/ext-api/whats2api_bridge/admin/messages": {"GET": _get_messages},
        "/ext-api/whats2api_bridge/admin/messages/{message_id}": {
            "DELETE": _del_message, "PATCH": _patch_message,
        },
        "/ext-api/whats2api_bridge/admin/events": {"GET": _get_events},
        "/ext-api/whats2api_bridge/admin/events/{event_id}": {"DELETE": _del_event},
        "/ext-api/whats2api_bridge/admin/connections": {"GET": _get_connections},
        "/ext-api/whats2api_bridge/admin/connections/{connection_id}": {"DELETE": _del_connection},
        "/ext-api/whats2api_bridge/admin/mcp": {
            "GET": _get_mcp, "PATCH": _patch_mcp,
        },
    }


# ── register(host) ─────────────────────────────────────────────────────────


def register(host):
    """OXS entry point."""

    logger = host.get_logger()

    # Build storage eagerly so on_enable has it ready.
    storage = PluginStorage(EXTENSION_ID, engine=engine)

    # Snapshot of bool-ish settings used inside the webhook hot path.
    # We refresh it on every enable.
    try:
        _settings_value_cache["log_inbound_events"] = bool(
            _s(host, "log_inbound_events", True)
        )
    except Exception:
        _settings_value_cache["log_inbound_events"] = True

    @host.on_install
    def _install():
        # Seed defaults into the settings namespace so the admin form
        # has somewhere to write into. Always idempotent — never
        # overwrites a value the user has actually typed.
        _ensure_settings(host)
        logger.info("whats2api_bridge installed")

    @host.on_enable
    def _enable():
        # ── Defensive: re-apply defaults on enable so pre-existing
        # installs whose ``on_install`` predated the defaults (or whose
        # settings were cleared manually) still surface the bridge's
        # base URL and other sane defaults to the UI / settings form.
        _ensure_settings(host)

        # ── Apply data_schema ourselves on the live DB engine.
        # The registry's pre-enable migration passes neither ``engine``
        # nor ``database_url`` to ``apply_for_extension``, which makes
        # PluginStorage fall back to ``sqlite:///:memory:`` and the
        # tables vanish into a private instance. Doing it here keeps
        # the tables on the same engine the rest of Odysseus uses.
        try:
            storage.apply_data_schema({
                "connections": {
                    "columns": {
                        "id":         "TEXT PRIMARY KEY",
                        "name":       "TEXT",
                        "engine":     "TEXT",
                        "phone":      "TEXT",
                        "status":     "TEXT",
                        "owner_id":   "TEXT",
                        "raw_json":   "TEXT",
                        "updated_at": "INTEGER NOT NULL",
                    },
                    "indexes": [["status"], ["owner_id"]],
                },
                "messages": {
                    "columns": {
                        "id":            "TEXT PRIMARY KEY",
                        "connection_id": "TEXT NOT NULL",
                        "direction":     "TEXT NOT NULL",
                        "remote_jid":    "TEXT",
                        "from_me":       "TEXT",
                        "chat_jid":      "TEXT",
                        "sender_jid":    "TEXT",
                        "push_name":     "TEXT",
                        "text":          "TEXT",
                        "message_type":  "TEXT",
                        "ts":            "INTEGER NOT NULL",
                        "raw_json":      "TEXT",
                    },
                    "indexes": [["connection_id", "ts"], ["chat_jid", "ts"], ["direction"]],
                    "unique":  [["id"]],
                },
                "events": {
                    "columns": {
                        "id":            "INTEGER PRIMARY KEY AUTOINCREMENT",
                        "event_name":    "TEXT NOT NULL",
                        "connection_id": "TEXT",
                        "user_id":       "TEXT",
                        "received_at":   "INTEGER NOT NULL",
                        "raw_json":      "TEXT",
                    },
                    "indexes": [["event_name", "received_at"], ["connection_id", "received_at"]],
                },
            })
        except Exception:
            logger.exception("whats2api_bridge: data_schema apply failed (continuing)")

        # ── UI ────────────────────────────────────────────────────────
        # Cache-bust the iframe URL on every enable so the browser
        # *always* re-fetches the static HTML after a Reload/Uninstall
        # cycle (some browsers cling to the cached version otherwise).
        cache_bust = int(time.time())
        host.add_static("/ext-static/whats2api_bridge", "ui")
        host.add_panel(
            id="whats2api_bridge",
            title="Whats2API Bridge",
            source_url=f"/ext-static/whats2api_bridge/index.html?v={cache_bust}",
        )

        # ── Event subscription: log rescan ────────────────────────────
        host.subscribe_event("oxs.rescan", _on_rescan)

        # ── MCP tools ─────────────────────────────────────────────────
        for tool_name, schema, handler in _read_tools(host, storage):
            host.register_mcp_tool(TOOL_NS, tool_name, schema, handler)

        # ── Inbound webhook for whats2api events ──────────────────────
        # The framework's ``register_webhook`` always generates a fresh
        # 32-byte token on every enable. For an inbound webhook that
        # ``whats2api.com`` has configured with a fixed URL, that means a
        # container restart would silently rotate the credential out from
        # under the operator and start returning 404s — exactly the bug we
        # shipped and forgot. Persist the token in the extension's data
        # directory (which is on the bind mount, so it survives restart)
        # and rewrite the record's ``token_hash`` so the dispatcher keeps
        # accepting the original token across enable cycles.
        host.register_webhook(
            path_suffix="/whats2api/events",
            event_filter=[],
            handler=_build_webhook_handler(host, storage),
        )
        # The framework's ``_wire`` step (which actually installs the
        # webhook into ``_webhooks[ext_id]`` and assigns the random
        # token) runs AFTER ``on_enable`` returns, on the same call
        # stack. Schedule the pinner for the next event-loop tick so it
        # sees the installed record.
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_async_pin_webhook_token(host))
            else:
                _pin_webhook_token(host)
        except RuntimeError:
            # No running loop (tests) — call synchronously; the test
            # framework installs the record before our test calls the
            # pinner itself.
            _pin_webhook_token(host)

        # ── Public status route (no session auth) ────────────────────
        host.add_route(
            "/ext-api/whats2api_bridge/status",
            _make_status_route(host, storage),
            auth="public",
        )

        # ── Admin routes (config + data browser) — auth-gated by the
        #    route_mount layer (``auth="admin"``) so the iframe can call
        #    them with the same session cookie the operator is already
        #    using on the parent SPA.
        for path, method_map in _make_admin_handlers(host, storage).items():
            for method, handler in method_map.items():
                host.add_route(
                    path,
                    handler,
                    auth="admin",
                    methods=(method,),
                )

        # ── Fire a hello event so subscribers see we came up ──────────
        host.fire_event(
            "whats2api.bridge.enabled",
            {"connection_id": None, "info": "whats2api_bridge online"},
            owner=None,
        )

        logger.info("whats2api_bridge enabled")

    @host.on_disable
    def _disable():
        logger.info("whats2api_bridge disabled")

    @host.on_uninstall
    def _uninstall():
        logger.info("whats2api_bridge uninstalled")

    def _on_rescan(payload):
        logger.info("oxs.rescan observed by whats2api_bridge: %s", payload)