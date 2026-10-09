
    // Compute the API prefix once. ``window.location.origin`` resolves
    // ``/ext-api/...`` correctly whether the page is served from
    // http://localhost:7011 (direct), via the panel iframe, or proxied
    // — without it, a ``fetch("/ext-api/...")`` fails with "Failed to
    // parse URL" when the document has no base URL.
    const API = (window.location.origin || "") + "/ext-api/whats2api_bridge/admin";
    const $ = (id) => document.getElementById(id);

    // ── Stale-document guard ───────────────────────────────────────────
    // The panel iframe caches aggressively; if the user enabled a new
    // version while the old document was still mounted, this code
    // sees the mismatched version marker and forces a clean reload.
    (function() {
      const expected = "w2a v0.4.0";
      const here = (document.body && document.body.innerHTML) || "";
      if (!here.includes(expected)) {
        // Append a hard reload that fires after paint so the user sees
        // the reason before we yank the iframe contents away.
        const note = document.createElement("div");
        note.style.cssText = "padding:24px;text-align:center;font-family:sans-serif;color:#9aa3b2;background:#0f1115;height:100vh;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:12px;";
        note.innerHTML =
          "<div style='font-size:14px;color:#7ad6a3;'>Whats2API Bridge</div>" +
          "<div style='font-size:12px;'>New version deployed — reloading the panel…</div>";
        document.documentElement.innerHTML = note.outerHTML;
        setTimeout(() => { location.replace(location.href.split("?")[0] + "?v=" + Date.now()); }, 200);
      }
    })();

    function escapeHtml(s) {
      return String(s == null ? "" : s).replace(/[&<>"']/g, c => (
        { "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[c]
      ));
    }
    function fmtTs(sec) {
      if (!sec) return "—";
      try { return new Date(sec * 1000).toISOString().replace("T", " ").slice(0, 19) + " UTC"; }
      catch { return String(sec); }
    }
    function showMsg(text, kind) {
      const m = $("msg");
      m.textContent = text || "";
      m.className = "msg" + (kind ? " " + kind : "");
      if (kind === "ok") setTimeout(() => { m.className = "msg"; m.textContent = ""; }, 3000);
    }

    async function api(path, opts) {
      // 8s budget per request — without this a stuck server (or a stale
      // 302 to /login) would leave the "loading…" indicator hanging
      // forever. The error surface still propagates fine.
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 8000);
      let res;
      try {
        res = await fetch(API + path, Object.assign(
          { credentials: "same-origin", signal: ctrl.signal },
          opts || {}
        ));
      } catch (e) {
        clearTimeout(tid);
        if (e.name === "AbortError") throw new Error("request timed out (8s)");
        throw e;
      }
      clearTimeout(tid);
      const text = await res.text();
      let data;
      try { data = text ? JSON.parse(text) : {}; } catch { data = { detail: text }; }
      if (!res.ok) throw new Error(data.detail || data.error || (res.status + " " + res.statusText));
      return data;
    }

    // Defaults applied to the form BEFORE the API responds, so the
    // user never sees blank fields. After the API responds, real values
    // overwrite these (only if the API returned a non-empty value).
    const DEFAULTS = {
      base_url: "https://api.whats2api.com",
      default_connection_id: "",
      request_timeout_seconds: 30,
      retention_days: 7,
      log_inbound_events: true,
    };
    function _applyDefaults() {
      $("cfg-base_url").value = DEFAULTS.base_url;
      $("cfg-default_connection_id").value = DEFAULTS.default_connection_id;
      $("cfg-request_timeout_seconds").value = String(DEFAULTS.request_timeout_seconds);
      $("cfg-retention_days").value = String(DEFAULTS.retention_days);
      $("cfg-log_inbound_events").checked = DEFAULTS.log_inbound_events;
    }

    // ── Config ───────────────────────────────────────────────────────
    async function loadConfig() {
      // Pre-fill defaults so the form is never blank — even if /config
      // 403s, redirects to /login, or the panel iframe loses its origin.
      _applyDefaults();
      try {
        const d = await api("/config");
        // Only overwrite a field if the server explicitly returned a
        // value. ``undefined`` keeps the HTML default; empty string
        // would also keep it (operators expect to see the default URL).
        if (d.base_url) $("cfg-base_url").value = d.base_url;
        if (d.default_connection_id !== undefined && d.default_connection_id !== null)
          $("cfg-default_connection_id").value = d.default_connection_id;
        if (d.request_timeout_seconds) $("cfg-request_timeout_seconds").value = d.request_timeout_seconds;
        if (d.retention_days) $("cfg-retention_days").value = d.retention_days;
        $("cfg-log_inbound_events").checked = !!d.log_inbound_events;
        $("cfg-api_token").value = ""; // never display the existing token
        $("cfg-api_token").placeholder = d.api_token_set ? "(unchanged — type to replace)" : "(not set)";

        // Webhook info
        const kv = $("webhook-info");
        const host = window.location.origin;
        const fullUrl = d.webhook_url ? (host + d.webhook_url) : "—";
        kv.innerHTML =
          `<dt>URL pattern</dt><dd>/api/ext/whats2api_bridge/webhook/&lt;token&gt;</dd>` +
          `<dt>Full URL (with token)</dt><dd class="${d.webhook_url ? "ok" : "warn"}">${escapeHtml(fullUrl)}</dd>`;
        $("copy-webhook").dataset.url = fullUrl;
      } catch (e) {
        showMsg("Failed to load config: " + e.message, "err");
      }
    }

    async function saveConfig() {
      const body = {
        base_url: $("cfg-base_url").value.trim(),
        default_connection_id: $("cfg-default_connection_id").value.trim(),
        request_timeout_seconds: parseInt($("cfg-request_timeout_seconds").value, 10),
        retention_days: parseInt($("cfg-retention_days").value, 10),
        log_inbound_events: $("cfg-log_inbound_events").checked,
      };
      const tok = $("cfg-api_token").value;
      if (tok) body.api_token = tok; // only send when user typed something
      $("cfg-status").textContent = "saving…";
      try {
        await api("/config", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
        $("cfg-status").textContent = "saved ✓";
        $("cfg-api_token").value = "";
        showMsg("Configuration saved.", "ok");
        loadConfig();
      } catch (e) {
        $("cfg-status").textContent = "";
        showMsg("Save failed: " + e.message, "err");
      }
    }

    // ── Webhook copy ─────────────────────────────────────────────────
    function copyWebhook() {
      const url = $("copy-webhook").dataset.url || "";
      if (!url || url === "—") { showMsg("No webhook URL configured yet.", "err"); return; }
      navigator.clipboard?.writeText(url).then(
        () => showMsg("Webhook URL copied to clipboard.", "ok"),
        () => {
          // Fallback: select+copy via temp textarea
          const ta = document.createElement("textarea");
          ta.value = url; document.body.appendChild(ta); ta.select();
          try { document.execCommand("copy"); showMsg("Copied.", "ok"); } catch {}
          document.body.removeChild(ta);
        }
      );
    }

    // ── Data browser ────────────────────────────────────────────────
    const browser = { tab: "messages", q: "", offset: 0, limit: 25 };

    function tabBtn(tab) {
      document.querySelectorAll(".tab-btn").forEach(b => {
        b.classList.toggle("primary", b.dataset.tab === tab);
      });
    }

    async function refreshBrowser() {
      const list = $("browse-list");
      list.textContent = "loading…";
      tabBtn(browser.tab);
      const qp = new URLSearchParams();
      qp.set("limit", browser.limit);
      qp.set("offset", browser.offset);
      if (browser.q) qp.set("q", browser.q);
      try {
        const d = await api("/" + browser.tab + "?" + qp.toString());
        const rows = d[browser.tab] || [];
        list.innerHTML = "";
        $("browse-count").textContent = `${rows.length} of ${d.total} total`;
        $("browse-pager").style.display = "";
        if (browser.offset <= 0) $("browse-prev").disabled = true; else $("browse-prev").disabled = false;
        if (browser.offset + browser.limit >= d.total) $("browse-next").disabled = true; else $("browse-next").disabled = false;
        if (!rows.length) { list.innerHTML = '<div class="hint">No rows.</div>'; return; }
        for (const r of rows) list.appendChild(renderRow(r));
      } catch (e) {
        list.innerHTML = `<div class="msg err">Load failed: ${escapeHtml(e.message)}</div>`;
      }
    }

    function renderRow(r) {
      const div = document.createElement("div");
      div.className = "data-row";
      div.dataset.id = r.id;
      if (browser.tab === "messages") {
        const text = r.text || "";
        div.innerHTML = `
          <div class="meta">
            <span class="ts">${fmtTs(r.ts)}</span>
            <span>${r.direction === "in" ? "↓" : "↑"} ${escapeHtml(r.connection_id || "")}</span>
            <span class="badge-${r.direction === "in" ? "ok" : "warn"}">${escapeHtml(r.direction || "")}</span>
            <span style="color:var(--fg-dim);">${escapeHtml(r.push_name || "")}</span>
            <span style="color:var(--fg-dim);">${escapeHtml(r.chat_jid || "")}</span>
            <span class="grow"></span>
            <button class="edit">Edit text</button>
            <button class="raw">JSON</button>
            <button class="danger del">Delete</button>
          </div>
          <div class="text" data-text="${escapeHtml(text)}">${escapeHtml(text) || '<span class="placeholder">(no text)</span>'}</div>
        `;
      } else if (browser.tab === "events") {
        div.innerHTML = `
          <div class="meta">
            <span class="ts">${fmtTs(r.received_at)}</span>
            <span class="badge-warn">${escapeHtml(r.event_name || "")}</span>
            <span>${escapeHtml(r.connection_id || "—")}</span>
            <span class="grow"></span>
            <button class="raw">JSON</button>
            <button class="danger del">Delete</button>
          </div>
        `;
      } else { // connections
        const status = (r.status || "—");
        const cls = status === "open" ? "ok" : (status === "close" ? "off" : "warn");
        div.innerHTML = `
          <div class="meta">
            <span class="conn-name">${escapeHtml(r.name || r.id)}</span>
            <span class="badge-${cls}">${escapeHtml(status)}</span>
            <span style="color:var(--fg-dim);">${escapeHtml(r.engine || "")}</span>
            <span style="color:var(--fg-dim);">${escapeHtml(r.phone || "")}</span>
            <span class="ts">${fmtTs(r.updated_at)}</span>
            <span class="grow"></span>
            <button class="raw">JSON</button>
            <button class="danger del">Delete</button>
          </div>
        `;
      }
      // Wire actions
      div.querySelector(".del").addEventListener("click", () => deleteRow(r.id, div));
      div.querySelector(".raw").addEventListener("click", () => {
        const pre = div.querySelector("pre");
        if (pre) { pre.remove(); return; }
        const p = document.createElement("pre");
        p.textContent = JSON.stringify(r.raw_json ? safeParse(r.raw_json) || r.raw_json : r, null, 2);
        div.appendChild(p);
      });
      if (browser.tab === "messages") {
        div.querySelector(".edit").addEventListener("click", () => editText(div, r));
      }
      return div;
    }

    function safeParse(s) { try { return JSON.parse(s); } catch { return null; } }

    async function deleteRow(id, rowEl) {
      if (!confirm("Delete this row? (id=" + id + ")")) return;
      try {
        await api("/" + browser.tab + "/" + encodeURIComponent(id), { method: "DELETE" });
        rowEl.remove();
        showMsg("Deleted.", "ok");
      } catch (e) {
        showMsg("Delete failed: " + e.message, "err");
      }
    }

    function editText(rowEl, r) {
      const textEl = rowEl.querySelector(".text");
      const current = textEl.dataset.text || "";
      const ta = document.createElement("textarea");
      ta.value = current;
      ta.style.cssText = "width:100%;min-height:60px;background:var(--code-bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:4px 6px;font-family:var(--mono);font-size:11px;";
      const save = document.createElement("button");
      save.textContent = "Save";
      save.className = "primary";
      const cancel = document.createElement("button");
      cancel.textContent = "Cancel";
      const actions = document.createElement("div");
      actions.style.cssText = "display:flex;gap:4px;margin-top:4px;";
      actions.appendChild(save); actions.appendChild(cancel);
      textEl.replaceWith(ta);
      rowEl.appendChild(actions);
      save.addEventListener("click", async () => {
        try {
          await api("/messages/" + encodeURIComponent(r.id),
            { method: "PATCH", headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ text: ta.value }) });
          showMsg("Updated.", "ok");
          refreshBrowser();
        } catch (e) {
          showMsg("Save failed: " + e.message, "err");
        }
      });
      cancel.addEventListener("click", () => refreshBrowser());
    }

    // ── Wire up ─────────────────────────────────────────────────────
    $("cfg-save").addEventListener("click", saveConfig);
    $("copy-webhook").addEventListener("click", copyWebhook);
    $("cfg-show-token").addEventListener("click", () => {
      const t = $("cfg-api_token");
      t.type = t.type === "password" ? "text" : "password";
    });
    document.querySelectorAll(".tab-btn").forEach(b => {
      b.addEventListener("click", () => { browser.tab = b.dataset.tab; browser.offset = 0; refreshBrowser(); });
    });
    $("browse-q").addEventListener("input", () => {
      browser.q = $("browse-q").value;
      browser.offset = 0;
      clearTimeout(window.__bt);
      window.__bt = setTimeout(refreshBrowser, 250);
    });
    $("browse-refresh").addEventListener("click", refreshBrowser);
    $("browse-prev").addEventListener("click", () => { browser.offset = Math.max(0, browser.offset - browser.limit); refreshBrowser(); });
    $("browse-next").addEventListener("click", () => { browser.offset += browser.limit; refreshBrowser(); });

    // ── MCP (single whats2api integration) ───────────────────────
    // The card lives in the HTML — ``loadMcp`` only patches text and
    // class names so there's never an empty placeholder while the API
    // call is in flight. The "Enable MCP" / "Disable MCP" button is
    // the operator's only knob, deliberately visible.
    async function loadMcp() {
      const btn = $("mcp-toggle");
      const pill = $("mcp-pill-enabled");
      const toolsPill = $("mcp-pill-tools");
      const connPill = $("mcp-pill-conn");
      const tokenMeta = $("mcp-meta-token");
      const errRow = $("mcp-meta-error-row");
      const errMeta = $("mcp-meta-error");
      const status = $("mcp-status");
      const fields = $("mcp-fields");

      btn.disabled = true;
      btn.textContent = "Loading…";
      pill.textContent = "loading…";
      pill.className = "intg-pill";

      let d = null;
      let fetchErr = null;
      try {
        d = await api("/mcp");
      } catch (e) {
        fetchErr = e.message || String(e);
      }

      btn.disabled = false;

      if (fetchErr) {
        pill.textContent = "unavailable";
        pill.className = "intg-pill enabled-off";
        btn.textContent = "Enable MCP";
        tokenMeta.textContent = "—";
        toolsPill.style.display = "none";
        connPill.style.display = "none";
        errRow.style.display = "";
        errMeta.textContent = fetchErr;
        status.textContent = "Couldn't reach /admin/mcp — " + fetchErr;
        return;
      }

      const on = !!d.enabled;
      const tokenSet = !!d.token_set;

      pill.textContent = on ? "active" : "disabled";
      pill.className = "intg-pill " + (on ? "enabled-on" : "enabled-off");
      btn.textContent = on ? "Disable MCP" : "Enable MCP";
      btn.classList.toggle("primary", !on);
      btn.classList.toggle("danger", on);

      tokenMeta.textContent = tokenSet
        ? "configured (paste a new one to replace)"
        : "not configured — paste a token below";

      if (d.tool_count > 0) {
        toolsPill.textContent = `${d.tool_count} tools`;
        toolsPill.style.display = "";
      } else {
        toolsPill.style.display = "none";
      }

      if (d.status && d.status !== "not_configured") {
        connPill.textContent = d.status;
        connPill.style.display = "";
        connPill.className = "intg-pill " + (d.status === "connected" ? "enabled-on" : "enabled-off");
      } else {
        connPill.style.display = "none";
      }

      if (d.error) {
        errRow.style.display = "";
        errMeta.textContent = d.error;
      } else {
        errRow.style.display = "none";
      }

      const placeholder = tokenSet
        ? "(saved — type to replace)"
        : "paste the token from your whats2api dashboard";
      $("mcp-token-input").placeholder = placeholder;

      // Gate the Enable button until there's actually a token to enable.
      if (!on && !tokenSet) {
        btn.disabled = true;
        btn.title = "save a token first";
      } else {
        btn.disabled = false;
        btn.title = "";
      }

      status.textContent = "";
    }

    $("mcp-toggle").addEventListener("click", async () => {
      // Determine the *desired* state from the current button label.
      // Safer than reading internal state — survives race with reload.
      const btn = $("mcp-toggle");
      const wantsEnable = btn.textContent.trim().toLowerCase().startsWith("enable");
      const status = $("mcp-status");
      status.textContent = wantsEnable ? "enabling…" : "disabling…";
      btn.disabled = true;
      try {
        const r = await api("/mcp", {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled: wantsEnable }),
        });
        if (r && r.ok === false) throw new Error(r.error || "toggle failed");
        status.textContent = wantsEnable ? "enabled ✓" : "disabled ✓";
        loadMcp();
      } catch (e) {
        status.textContent = "";
        showMsg("Failed: " + e.message, "err");
        loadMcp();
      }
    });

    $("mcp-save").addEventListener("click", async () => {
      const token = $("mcp-token-input").value;
      if (!token) {
        showMsg("token is required", "err");
        $("mcp-token-input").focus();
        return;
      }
      const status = $("mcp-status");
      status.textContent = "saving token…";
      try {
        const r = await api("/mcp", {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token }),
        });
        if (r && r.ok === false) throw new Error(r.error || "save failed");
        status.textContent = "token saved ✓";
        showMsg("token saved", "ok");
        $("mcp-token-input").value = "";
        loadMcp();
      } catch (e) {
        status.textContent = "";
        showMsg("Save failed: " + e.message, "err");
      }
    });

    loadConfig();
    refreshBrowser();
    loadMcp();
  