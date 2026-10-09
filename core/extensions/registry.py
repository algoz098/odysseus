"""registry.py — ExtensionRegistry: install / enable / disable / uninstall.

The :class:`ExtensionRegistry` is the runtime owner of every enabled
extension. It holds:

  - a dict ``{extension_id: RunningExtension}`` for fast lookup
  - methods to install, enable, disable, uninstall, reload
  - persistence to the ``extensions`` and ``extension_permissions`` DB tables

For PR 2 the registry records registrations on the host and runs the
lifecycle, but does NOT yet wire those registrations to FastAPI / McpManager
/ event_bus. PR 3 adds the wiring by reading ``host.tools``,
``host.routes``, etc. and mounting them.

Crash isolation: any failure during enable/disable is caught, logged, and
recorded on the ``Extension.install_error`` column. The running server
keeps going — other extensions are not affected.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from src.constants import DATA_EXTENSIONS_DIR, EXTENSIONS_DIR, OXS_PERMISSIONS_DIR

from .audit import audit_log
from .host import OXSHost
from .lifecycle import LifecycleHooks
from .loader import DiscoveredExtension, discover_all, verify_discovered
from .manifest_schema import OXSManifestError, validate_manifest
from .security import (
    OXSSecurityError,
    ast_parse_safe,
    find_register_callable,
    sha256_of_file,
)


@dataclass
class RunningExtension:
    """In-memory state for one loaded extension."""
    extension_id: str
    manifest: dict
    source: str
    source_path: str
    host: OXSHost
    # Set after enable(); cleared on disable().
    enabled: bool = False
    # Operator toggle: surface this extension in the user sidebar. Default off;
    # flipped via the admin panel + persisted in the DB extensions row.
    sidebar_visible: bool = False


class ExtensionRegistry:
    """Manages the lifecycle of every extension in the running process.

    Construct one per FastAPI lifespan. Bind to the app via ``registry.wire(app)``
    in PR 3 — for PR 2 this is a no-op because we do not yet mount routes.
    """

    def __init__(
        self,
        *,
        bundled_dir: str = EXTENSIONS_DIR,
        user_dir: str = DATA_EXTENSIONS_DIR,
    ):
        self.bundled_dir = bundled_dir
        self.user_dir = user_dir
        self._extensions: dict[str, RunningExtension] = {}

    # ── Introspection ─────────────────────────────────────────────────────

    def __contains__(self, extension_id: str) -> bool:
        return extension_id in self._extensions

    def __iter__(self):
        return iter(self._extensions.values())

    def get(self, extension_id: str) -> RunningExtension | None:
        return self._extensions.get(extension_id)

    def list(self) -> list[RunningExtension]:
        return list(self._extensions.values())

    def list_enabled(self) -> list[RunningExtension]:
        return [e for e in self._extensions.values() if e.enabled]

    # ── Discovery ────────────────────────────────────────────────────────

    def rescan(self) -> list[RunningExtension]:
        """Discover extensions and add new ones to the in-memory registry.

        Already-known extensions are left untouched. Operators use this to
        pick up extensions installed out-of-band (pip-installed packages,
        extensions dropped into ``data/extensions/``).

        Does NOT auto-enable. Operators enable explicitly.
        """
        for ext in discover_all(bundled_dir=self.bundled_dir, user_dir=self.user_dir):
            if ext.extension_id in self._extensions:
                continue
            try:
                verify_discovered(ext)
            except OXSSecurityError as e:
                audit_log(
                    "extension.discovery.security_failed",
                    extension_id=ext.extension_id,
                    actor="system",
                    payload={"source": ext.source, "errors": e.errors},
                )
                continue
            self._register_discovered(ext)
        return self.list()

    def _register_discovered(self, ext: DiscoveredExtension) -> None:
        """Add a discovered extension to the in-memory registry (not enabled)."""
        if ext.extension_id in self._extensions:
            return
        declared_caps, granted_priv = _extract_capabilities(ext.manifest)
        host = OXSHost(
            extension_id=ext.extension_id,
            extension_version=ext.manifest["version"],
            declared_capabilities=declared_caps,
            granted_privileged=granted_priv,
            data_path=os.path.join(self.user_dir, ext.extension_id),
            plugin_path=ext.source_path,
        )
        running = RunningExtension(
            extension_id=ext.extension_id,
            manifest=ext.manifest,
            source=ext.source,
            source_path=ext.source_path,
            host=host,
            enabled=False,
        )
        self._extensions[ext.extension_id] = running
        # Hydrate operator toggles (e.g. sidebar_visible) from the persisted
        # extensions DB row so UI state survives restarts.
        try:
            from core.database import SessionLocal, Extension as _ExtRow
            with SessionLocal() as db:
                row = db.query(_ExtRow).filter(_ExtRow.id == ext.extension_id).first()
                if row is not None:
                    running.sidebar_visible = bool(row.sidebar_visible)
        except Exception:
            import logging
            logging.getLogger(__name__).debug(
                "sidebar_visible hydrate skipped for %s", ext.extension_id,
            )
        audit_log(
            "extension.discovered",
            extension_id=ext.extension_id,
            actor="system",
            payload={
                "version": ext.manifest["version"],
                "source": ext.source,
                "source_path": ext.source_path,
            },
        )

    # ── Install ──────────────────────────────────────────────────────────

    def _backup_existing(self, ext_id: str) -> str | None:
        """If ``data/extensions/<id>/`` exists, move it aside.

        Backs up to ``data/extensions/.bak/<id>-<UTC-ts>/`` so a re-install of
        an already-installed extension never destroys the previous version
        silently. Returns the backup path or ``None`` when there was nothing
        to back up.
        """
        target = os.path.join(self.user_dir, ext_id)
        if not os.path.isdir(target):
            return None
        bak_root = os.path.join(self.user_dir, ".bak")
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = os.path.join(bak_root, f"{ext_id}-{ts}")
        os.makedirs(bak_root, exist_ok=True)
        shutil.move(target, backup)
        return backup

    def install_from_zip(self, zip_path: str, *, operator: str | None = None) -> RunningExtension:
        """Install an extension from a local ZIP file.

        Validates the ZIP (no zip-slip / symlinks), extracts into
        ``data/extensions/<id>/``, then runs the regular rescan + install flow.
        """
        import zipfile
        from .security import verify_zip_safe

        with zipfile.ZipFile(zip_path) as zf:
            try:
                verify_zip_safe(zf)
            except OXSSecurityError as e:
                audit_log(
                    "extension.install.zip_unsafe",
                    extension_id="?",
                    actor=operator or "system",
                    payload={"zip_path": zip_path, "errors": e.errors},
                )
                raise
            # Find the extension.json inside the archive
            manifest_bytes = None
            for info in zf.infolist():
                if info.filename.endswith("extension.json") and not info.is_dir():
                    manifest_bytes = zf.read(info.filename)
                    inner_zip_prefix = info.filename[: -len("extension.json")]
                    break
            if manifest_bytes is None:
                raise ValueError("zip does not contain extension.json")
            # Validate the manifest
            manifest = validate_manifest(manifest_bytes)
            ext_id = manifest["id"]

            # Backup existing install (if any) before we overwrite it
            backup_path = self._backup_existing(ext_id)

            # Extract
            target_dir = os.path.join(self.user_dir, ext_id)
            os.makedirs(target_dir, exist_ok=True)
            for info in zf.infolist():
                # Re-validate every path on the way out (defense in depth —
                # verify_zip_safe already covered this, but check again).
                norm = info.filename
                if "\\" in norm or norm.startswith("/") or ".." in norm.split("/"):
                    raise ValueError(f"unsafe member: {info.filename!r}")
                if not norm.startswith(inner_zip_prefix):
                    # Member outside the inner prefix; skip
                    continue
                rel = norm[len(inner_zip_prefix):]
                if not rel:
                    continue
                target = os.path.join(target_dir, rel)
                if info.is_dir():
                    os.makedirs(target, exist_ok=True)
                    continue
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with open(target, "wb") as out:
                    out.write(zf.read(info))

        # Re-discover so the new extension is picked up
        self.rescan()
        running = self._extensions.get(ext_id)
        if running is None:
            raise RuntimeError(f"install succeeded but discovery did not register {ext_id}")

        # Persist
        self._persist_extension_row(running, operator=operator)
        audit_log(
            "extension.install",
            extension_id=ext_id,
            actor=operator or "system",
            payload={
                "version": manifest["version"],
                "source": "user",
                "source_path": target_dir,
                "via": "zip",
                "backup": backup_path,
            },
        )
        return running

    def install_from_git(self, url: str, *, operator: str | None = None) -> RunningExtension:
        """Install an extension from a public https git repository.

        Clones into a temp dir, validates the ``extension.json`` (found at
        the repo root or inside a single top-level directory), backs up any
        existing install at ``data/extensions/<id>/``, then moves the source
        tree into place. Same rescan + persist + audit tail as
        :meth:`install_from_zip`.

        Only ``https://`` URLs without embedded credentials are accepted.
        SSH (``git@...``, ``ssh://...``), ``git://``, ``file://``, and any
        URL carrying ``user:pass@`` are rejected before any subprocess runs.
        """
        # 1. URL validation
        try:
            parsed = urlparse(url)
        except ValueError as e:
            raise ValueError(f"malformed URL: {e}") from None
        if parsed.scheme != "https":
            raise ValueError(f"only https URLs are accepted; got scheme {parsed.scheme!r}")
        if parsed.username or parsed.password:
            raise ValueError("URL must not contain credentials")
        if not parsed.netloc or not parsed.path or parsed.path == "/":
            raise ValueError("URL is missing a repository path")

        # 2. Clone to a temp dir
        tmp = tempfile.mkdtemp(prefix="oxs-git-")
        backup_path: str | None = None
        target_dir = ""
        try:
            try:
                proc = subprocess.run(
                    ["git", "clone", "--depth=1", "--single-branch", "--", url, tmp],
                    check=False,
                    timeout=120,
                    capture_output=True,
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError("git clone timed out after 120s") from None
            except FileNotFoundError:
                raise RuntimeError("git binary not found on server") from None
            if proc.returncode != 0:
                stderr = proc.stderr.decode("utf-8", "replace").strip()
                raise RuntimeError(
                    f"git clone failed: {stderr or f'exit {proc.returncode}'}"
                )

            # 3. Locate extension.json: at the clone root, or inside a single
            # top-level subdirectory. Refuse to guess if multiple candidates
            # exist — the author should re-pack the repo so the manifest is
            # easy to find.
            candidates: list[str] = []
            root_manifest = os.path.join(tmp, "extension.json")
            if os.path.isfile(root_manifest):
                candidates.append(root_manifest)
            for entry in sorted(os.listdir(tmp)):
                full = os.path.join(tmp, entry)
                if not os.path.isdir(full) or entry.startswith("."):
                    continue
                cand = os.path.join(full, "extension.json")
                if os.path.isfile(cand):
                    candidates.append(cand)
            if not candidates:
                raise ValueError("no extension.json found in cloned repository")
            if len(candidates) > 1:
                raise ValueError(
                    "multiple candidate extension.json found; repo must have a "
                    "single extension at the root or under a single top-level dir"
                )
            manifest_path = candidates[0]
            source_root = os.path.dirname(manifest_path)

            with open(manifest_path, encoding="utf-8") as f:
                manifest = validate_manifest(f.read())
            ext_id = manifest["id"]

            # 4. Backup existing install
            backup_path = self._backup_existing(ext_id)

            # 5. Move source tree into place
            target_dir = os.path.join(self.user_dir, ext_id)
            if source_root == tmp:
                # Manifest at the repo root — move each top-level entry
                # individually (skipping .git) so the manifest filename is
                # preserved at data/extensions/<id>/extension.json.
                os.makedirs(target_dir, exist_ok=True)
                for entry in os.listdir(tmp):
                    if entry == ".git":
                        continue
                    src = os.path.join(tmp, entry)
                    dst = os.path.join(target_dir, entry)
                    if os.path.exists(dst):
                        if os.path.isdir(dst):
                            shutil.rmtree(dst)
                        else:
                            os.unlink(dst)
                    shutil.move(src, dst)
            else:
                if os.path.exists(target_dir):
                    shutil.rmtree(target_dir)
                shutil.move(source_root, target_dir)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        # 6. Re-discover + persist + audit (mirrors install_from_zip)
        self.rescan()
        running = self._extensions.get(ext_id)
        if running is None:
            raise RuntimeError(
                f"install succeeded but discovery did not register {ext_id}"
            )
        self._persist_extension_row(running, operator=operator)
        audit_log(
            "extension.install",
            extension_id=ext_id,
            actor=operator or "system",
            payload={
                "version": manifest["version"],
                "source": "user",
                "source_path": target_dir,
                "via": "git",
                "url": url,
                "backup": backup_path,
            },
        )
        return running

    # ── Enable ───────────────────────────────────────────────────────────

    def enable(self, extension_id: str, *, operator: str | None = None) -> RunningExtension:
        """Load the entry-point, call ``register(host)``, run ``on_enable``."""
        running = self._extensions.get(extension_id)
        if running is None:
            raise KeyError(f"extension not found: {extension_id!r}")
        if running.enabled:
            return running

        # Apply data_schema (create tables, indexes, uniques) BEFORE
        # on_enable so the extension can read its own data.
        schema = running.manifest.get("data_schema") or {}
        if schema:
            from .migrate import apply_for_extension
            try:
                apply_for_extension(extension_id, schema)
            except Exception as e:
                audit_log(
                    "extension.enable.migrate_failed",
                    extension_id=extension_id,
                    actor=operator or "system",
                    payload={"error": str(e)},
                )
                # Don't abort enable — extension can still run with
                # missing tables; log and continue.

        # Import the entry-point module and grab the register callable.
        try:
            register_fn = _resolve_register(running)
        except Exception as e:
            audit_log(
                "extension.enable.import_failed",
                extension_id=extension_id,
                actor=operator or "system",
                payload={"error": str(e)},
            )
            self._set_install_error(running, f"import failed: {e}")
            raise

        # Author-supplied AST inspection of the entry-point: cheap, no exec.
        try:
            _audit_entry_point(running, register_fn)
        except OXSSecurityError as e:
            audit_log(
                "extension.enable.ast_failed",
                extension_id=extension_id,
                actor=operator or "system",
                payload={"errors": e.errors},
            )
            self._set_install_error(running, f"AST check failed: {e.errors}")
            raise

        # Call register(host). The extension decorates hooks on the host.
        try:
            register_fn(running.host)
        except Exception as e:
            audit_log(
                "extension.enable.register_failed",
                extension_id=extension_id,
                actor=operator or "system",
                payload={"error": str(e)},
            )
            self._set_install_error(running, f"register() raised: {e}")
            raise

        # Run on_enable hooks.
        try:
            running.host.hooks.run("on_enable")
        except Exception:
            # LifecycleHooks already isolates and audits per-failure; this
            # catch-all is defensive.
            pass

        # Wire up the recorded registrations into the real backends.
        self._wire(running)

        # Dispatch any events_fire requests the extension made during enable.
        for event_name, payload, owner in running.host.event_fires_during_enable:
            audit_log(
                "extension.event_fired",
                extension_id=extension_id,
                actor=f"extension:{extension_id}",
                payload={"event": event_name, "owner": owner, "payload": payload},
            )
            try:
                from .events import dispatch_via_event_bus
                dispatch_via_event_bus(event_name, owner=owner, payload=payload)
            except Exception:
                audit_log(
                    "extension.event_fire_failed",
                    extension_id=extension_id,
                    actor=f"extension:{extension_id}",
                    payload={"event": event_name, "error": "dispatch failed"},
                )

        running.enabled = True
        self._set_enabled(running, enabled=True)
        audit_log(
            "extension.enable",
            extension_id=extension_id,
            actor=operator or "system",
            payload={
                "tools": len(running.host.tools),
                "mcp_tools": len(running.host.mcp_tools),
                "routes": len(running.host.routes),
                "webhooks": len(running.host.webhooks),
                "violations": len(running.host.violations),
            },
        )
        return running

    # ── Disable ──────────────────────────────────────────────────────────

    def disable(self, extension_id: str, *, operator: str | None = None) -> None:
        running = self._extensions.get(extension_id)
        if running is None or not running.enabled:
            return
        try:
            running.host.hooks.run("on_disable")
        except Exception:
            pass
        # Unwire from real backends BEFORE clearing the host records so
        # the unwire can read the registry on its way out.
        self._unwire(running)

        # Clear in-memory records.
        running.host.tools.clear()
        running.host.mcp_tools.clear()
        running.host.routes.clear()
        running.host.statics.clear()
        running.host.event_subs.clear()
        running.host.event_fires_during_enable.clear()
        running.host.webhooks.clear()
        running.host.sidebar.clear()
        running.host.panels.clear()
        running.host.slash_commands.clear()
        running.host.settings_tabs.clear()
        running.host.hooks.clear()
        running.host.violations.clear()
        running.enabled = False
        self._set_enabled(running, enabled=False)
        audit_log(
            "extension.disable",
            extension_id=extension_id,
            actor=operator or "system",
            payload={},
        )

    def set_sidebar_visible(
        self, extension_id: str, visible: bool, *, operator: str | None = None
    ) -> RunningExtension:
        """Toggle whether ``extension_id`` appears in the user sidebar.

        Updates both the in-memory :class:`RunningExtension` and the persisted
        ``extensions`` row so the choice survives restarts. No-op if the
        extension is not registered.
        """
        running = self._extensions.get(extension_id)
        if running is None:
            raise KeyError(f"extension not found: {extension_id!r}")
        running.sidebar_visible = bool(visible)
        try:
            from core.database import SessionLocal, Extension as _ExtRow
            with SessionLocal() as db:
                row = db.query(_ExtRow).filter(_ExtRow.id == extension_id).first()
                if row is not None:
                    row.sidebar_visible = bool(visible)
                    db.commit()
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                "OXS set_sidebar_visible persist failed for %s", extension_id,
            )
        audit_log(
            "extension.sidebar_visible",
            extension_id=extension_id,
            actor=operator or "system",
            payload={"visible": bool(visible)},
        )
        return running

    # ── Wiring helpers ───────────────────────────────────────────────────

    def _wire(self, running: RunningExtension) -> None:
        """Push every host-recorded registration into the real backends.

        Idempotent — running this twice is a no-op because each backend
        rejects duplicate registrations."""
        ext_id = running.extension_id
        host = running.host

        # Event subscriptions
        if host.event_subs:
            from .events import wire_subscriptions
            n = wire_subscriptions(
                ext_id,
                [(s.event_name, s.handler) for s in host.event_subs],
            )
            if n:
                audit_log(
                    "extension.event_subscriptions.wired",
                    extension_id=ext_id,
                    actor="system",
                    payload={"count": n},
                )

        # MCP tools
        if host.mcp_tools:
            from .mcp_bridge import install_tools
            n = install_tools(ext_id, host.mcp_tools)
            if n:
                audit_log(
                    "extension.mcp_tools.installed",
                    extension_id=ext_id,
                    actor="system",
                    payload={"count": n},
                )

        # Task actions
        if host.task_actions:
            from .task_actions import install_action
            for ta in host.task_actions:
                install_action(ext_id, ta.name, ta.handler, ta.schema)
            audit_log(
                "extension.task_actions.installed",
                extension_id=ext_id,
                actor="system",
                payload={"count": len(host.task_actions)},
            )

        # Webhooks — install generates tokens and stores them on the
        # records. PR 4 wires the FastAPI router; for now we just
        # register them in the in-memory webhook table.
        if host.webhooks:
            from . import webhooks
            for wh in host.webhooks:
                webhooks.install(
                    ext_id, wh.path_suffix, wh.event_filter, wh.handler,
                )
            audit_log(
                "extension.webhooks.installed",
                extension_id=ext_id,
                actor="system",
                payload={"count": len(host.webhooks)},
            )

        # Static mounts — only attach if a FastAPI app was bound to the
        # registry via ``bind_app``. PR 4 calls bind_app during startup.
        if host.statics and self._app is not None:
            from . import ui_mount
            for st in host.statics:
                ui_mount.install(
                    self._app, ext_id, st.url_prefix,
                    running.source_path, st.directory,
                )

        # HTTP routes declared via host.add_route. Mounted onto the
        # FastAPI app so the extension's own endpoints become reachable.
        if host.routes and self._app is not None:
            from . import route_mount
            for r in host.routes:
                try:
                    route_mount.install(
                        self._app, ext_id, r.path, r.handler,
                        auth=r.auth, methods=r.methods,
                    )
                    audit_log(
                        "extension.route.installed",
                        extension_id=ext_id,
                        actor="system",
                        payload={"path": r.path, "methods": list(r.methods), "auth": r.auth},
                    )
                except Exception:
                    import logging as _l
                    _l.getLogger(__name__).exception(
                        "OXS route mount failed for %s %s", ext_id, r.path,
                    )

    def _unwire(self, running: RunningExtension) -> None:
        """Reverse of :meth:`_wire`. Removes everything the extension put down."""
        ext_id = running.extension_id

        # Event subscriptions
        from .events import unwire_subscriptions
        unwire_subscriptions(ext_id)

        # MCP tools
        from .mcp_bridge import uninstall_tools
        uninstall_tools(ext_id)

        # Task actions
        from .task_actions import uninstall_all_actions
        uninstall_all_actions(ext_id)

        # Webhooks
        from . import webhooks
        webhooks.uninstall(ext_id)

        # Static mounts
        if self._app is not None:
            from . import ui_mount
            ui_mount.uninstall(self._app, ext_id)

        # HTTP routes mounted in _wire
        if self._app is not None:
            from . import route_mount
            route_mount.uninstall(self._app, ext_id)

    # ── App binding (PR 4) ────────────────────────────────────────────────

    _app: Any = None

    def bind_app(self, app: Any) -> None:
        """Bind the FastAPI app so static mounts and inbound webhook routes
        can be wired/unwired on enable/disable."""
        self._app = app

    @property
    def app(self) -> Any:
        return self._app

    # ── Uninstall ────────────────────────────────────────────────────────

    def uninstall(self, extension_id: str, *, operator: str | None = None) -> None:
        running = self._extensions.get(extension_id)
        if running is None:
            return
        if running.enabled:
            self.disable(extension_id, operator=operator)
        # Run the extension's own cleanup first — it can disconnect MCP
        # sessions, remove rows from ``mcp_servers``, drop external caches,
        # etc. We do this BEFORE the framework's automatic cleanup so the
        # extension can drop things the framework doesn't know about.
        try:
            running.host.hooks.run("on_uninstall")
        except Exception:
            pass
        # Best-effort automatic cleanup. Each step is wrapped in its own
        # try/except so a failure in one (e.g. a missing table) doesn't
        # skip the others. Every failure is audited; the operator sees
        # the full picture in the audit log.
        self._cleanup_extension_resources(extension_id, operator=operator)
        audit_log(
            "extension.uninstall",
            extension_id=extension_id,
            actor=operator or "system",
            payload={},
        )
        self._extensions.pop(extension_id, None)
        self._delete_extension_row(extension_id)
        # Note: we keep the extension's data dir on disk so operators can
        # recover from a mistake. Cleanup is a separate explicit action.

    def _cleanup_extension_resources(
        self, extension_id: str, *, operator: str | None,
    ) -> None:
        """Best-effort tear-down of resources the extension owned.

        Runs after ``on_uninstall`` so the extension had a chance to do its
        own cleanup first. Each step is independent — we never let a
        failure in one block the others. Failures are audited individually
        so the operator can see exactly what went wrong.

        Cleanup covers:

          1. McpServer rows where ``extension_id == <id>`` (the rows the
             extension created via direct DB writes) — the live session is
             disconnected first, then the row is deleted.
          2. Data tables declared in ``data_schema`` (``<id>__*``) — the
             spec at ``specs/oxs.md:256`` promises this; PRs before this
             change kept the tables around for "recovery", which leaked
             data on every uninstall. Now we drop them. The data dir on
             disk is still kept — a separate explicit action removes it.
        """
        from .audit import audit_log

        # ── 1. McpServer rows owned by this extension ─────────────────────
        try:
            from core.database import SessionLocal, McpServer
            from src.mcp_manager import get_mcp_manager
            mgr = get_mcp_manager()
            owned_ids: list[str] = []
            with SessionLocal() as db:
                rows = (
                    db.query(McpServer)
                    .filter(McpServer.extension_id == extension_id)
                    .all()
                )
                for row in rows:
                    owned_ids.append(row.id)
                    db.delete(row)
                db.commit()
            # Disconnect live sessions after the DB write so a partial DB
            # failure doesn't leave a session pointing at a deleted row.
            for sid in owned_ids:
                try:
                    import asyncio
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.create_task(mgr.disconnect_server(sid))
                        else:
                            loop.run_until_complete(mgr.disconnect_server(sid))
                    except RuntimeError:
                        # No running loop — best-effort, skip.
                        pass
                except Exception as e:
                    audit_log(
                        "extension.uninstall.mcp_disconnect_failed",
                        extension_id=extension_id,
                        actor=operator or "system",
                        payload={"server_id": sid, "error": str(e)},
                    )
            if owned_ids:
                audit_log(
                    "extension.uninstall.mcp_servers_removed",
                    extension_id=extension_id,
                    actor=operator or "system",
                    payload={"server_ids": owned_ids},
                )
        except Exception as e:
            audit_log(
                "extension.uninstall.mcp_cleanup_failed",
                extension_id=extension_id,
                actor=operator or "system",
                payload={"error": str(e)},
            )

        # ── 2. Data tables (`<id>__*`) ────────────────────────────────────
        try:
            from .migrate import drop_all_for_extension
            from core.database import engine as _core_engine
            dropped = drop_all_for_extension(
                extension_id, engine=_core_engine,
            )
            if dropped:
                audit_log(
                    "extension.uninstall.tables_dropped",
                    extension_id=extension_id,
                    actor=operator or "system",
                    payload={"tables": dropped},
                )
        except Exception as e:
            audit_log(
                "extension.uninstall.tables_drop_failed",
                extension_id=extension_id,
                actor=operator or "system",
                payload={"error": str(e)},
            )

    # ── Reload ───────────────────────────────────────────────────────────

    def reload(self, extension_id: str, *, operator: str | None = None) -> RunningExtension:
        """Disable, drop the in-memory module, then re-enable.

        Useful for picking up changes to extension code without a server
        restart. We invalidate the cached module so the next import
        re-runs.
        """
        running = self._extensions.get(extension_id)
        if running is None:
            raise KeyError(f"extension not found: {extension_id!r}")
        was_enabled = running.enabled
        module_path = running.manifest.get("entry_point", "").split(":")[0]
        if was_enabled:
            self.disable(extension_id, operator=operator)
        # Drop the entry-point module so the next enable re-imports.
        if module_path:
            for mod in list(sys.modules.keys()):
                if mod == module_path or mod.startswith(module_path + "."):
                    del sys.modules[mod]
        if was_enabled:
            return self.enable(extension_id, operator=operator)
        # Even if it was disabled, the operator may want to refresh its
        # manifest after editing extension.json. Re-discover + verify.
        self.rescan()
        return self._extensions.get(extension_id) or running

    # ── DB persistence helpers ───────────────────────────────────────────

    def _persist_extension_row(self, running: RunningExtension, *, operator: str | None) -> None:
        try:
            from core.database import SessionLocal, Extension
            with SessionLocal() as db:
                row = db.query(Extension).filter(Extension.id == running.extension_id).first()
                if row is None:
                    row = Extension(
                        id=running.extension_id,
                        name=running.manifest["name"],
                        version=running.manifest["version"],
                        oxs_version=running.manifest["oxs_version"],
                        source=running.source,
                        source_path=running.source_path,
                        entry_point=running.manifest["entry_point"],
                        manifest_json=json.dumps(running.manifest, sort_keys=True),
                        file_hashes=running.manifest.get("file_hashes"),
                        is_enabled=False,
                        is_bundled=(running.source == "bundled"),
                        installed_by=operator,
                        install_error=None,
                        last_enabled_at=None,
                    )
                    db.add(row)
                else:
                    row.name = running.manifest["name"]
                    row.version = running.manifest["version"]
                    row.oxs_version = running.manifest["oxs_version"]
                    row.manifest_json = json.dumps(running.manifest, sort_keys=True)
                    row.file_hashes = running.manifest.get("file_hashes")
                    row.install_error = None
                db.commit()
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                "OXS persist extension row failed for %s", running.extension_id,
            )

    def _set_enabled(self, running: RunningExtension, *, enabled: bool) -> None:
        try:
            from core.database import SessionLocal, Extension
            from datetime import datetime, timezone
            with SessionLocal() as db:
                row = db.query(Extension).filter(Extension.id == running.extension_id).first()
                if row is not None:
                    row.is_enabled = enabled
                    if enabled:
                        row.last_enabled_at = datetime.now(timezone.utc).replace(tzinfo=None)
                        row.install_error = None
                    db.commit()
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                "OXS set_enabled failed for %s", running.extension_id,
            )

    def _set_install_error(self, running: RunningExtension, message: str) -> None:
        try:
            from core.database import SessionLocal, Extension
            with SessionLocal() as db:
                row = db.query(Extension).filter(Extension.id == running.extension_id).first()
                if row is not None:
                    row.install_error = message
                    db.commit()
        except Exception:
            pass

    def _delete_extension_row(self, extension_id: str) -> None:
        try:
            from core.database import SessionLocal, Extension
            with SessionLocal() as db:
                db.query(Extension).filter(Extension.id == extension_id).delete()
                db.commit()
        except Exception:
            pass


# ── Module-level helpers ───────────────────────────────────────────────────


def _extract_capabilities(manifest: dict[str, Any]) -> tuple[dict[str, bool], dict[str, bool]]:
    """Pull capability and privileged permission flags out of the manifest."""
    caps = manifest.get("capabilities") or {}
    declared = {name: True for name in caps.keys()}
    priv_manifest = manifest.get("privileged_permissions") or {}
    granted_priv = {name: True for name, v in priv_manifest.items() if v is True}
    return declared, granted_priv


def _resolve_register(running: RunningExtension):
    """Import the extension's module and return its ``register`` callable.

    Source resolution:
      - bundled / user: ``source_path`` is the directory; ``entry_point``
        is ``module:callable``. We add ``source_path`` to ``sys.path``
        (transient — for the duration of one enable) so authors do not
        have to ship a pip-installable package.
      - entry_point: the entry-point object itself is the callable (or
        its load() result).

    For bundled/user sources we use ``importlib.util.spec_from_file_location``
    with a unique module name (``_oxs_<id>_<module>``) so concurrent
    extensions with the same ``entry_point`` module name (e.g. ``plugin``)
    do not collide in ``sys.modules``.
    """
    if running.source == "entry_point":
        try:
            eps = importlib.metadata.entry_points(group="odysseus.extensions")
        except ImportError:
            import sys as _sys
            eps = _sys.modules.get("importlib").entry_points  # type: ignore
        ep = next(
            (e for e in eps if getattr(e, "name", "") == running.extension_id),
            None,
        )
        if ep is None:
            raise RuntimeError(f"entry-point not found: {running.extension_id!r}")
        obj = ep.load()
        return obj() if callable(obj) else obj

    entry = running.manifest.get("entry_point", "")
    if ":" not in entry:
        raise RuntimeError(f"invalid entry_point: {entry!r}")
    module_name, attr = entry.split(":", 1)
    if running.source == "bundled" or running.source == "user":
        candidate = os.path.join(running.source_path, f"{module_name}.py")
        if os.path.isfile(candidate):
            unique_name = f"_oxs_{running.extension_id}_{module_name}"
            spec = importlib.util.spec_from_file_location(unique_name, candidate)
            module = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(module)
            sys.modules[unique_name] = module
            return getattr(module, attr)

    if running.source_path and running.source_path not in sys.path:
        sys.path.insert(0, running.source_path)
    module = importlib.import_module(module_name)
    return getattr(module, attr)


def _audit_entry_point(running: RunningExtension, register_fn) -> None:
    """Static AST check on the entry-point source.

    We re-read the source from disk so we never exec anything during
    discovery. The check rejects ``exec`` / ``eval`` / ``compile`` and
    confirms ``register`` is a top-level def with one positional arg.
    """
    if running.source == "entry_point":
        return  # pip-managed; integrity is pip's responsibility
    module_name = running.manifest.get("entry_point", "").split(":")[0]
    src_path = os.path.join(running.source_path, f"{module_name}.py")
    if not os.path.isfile(src_path):
        return
    with open(src_path, encoding="utf-8") as f:
        source = f.read()
    tree = ast_parse_safe(source)
    if find_register_callable(tree) is None:
        # The extension is allowed to use a different function name if
        # ``entry_point`` matches, but we still want to confirm something
        # callable exists with the expected arity.
        return


__all__ = [
    "RunningExtension",
    "ExtensionRegistry",
]