// App shell: header (cluster status, search, mode switch, mode-specific
// controls, clock), the tree/main/detail grid, and the status bar. The DOM
// skeleton is built once; steady-state updates touch textContent only, so
// focus in the search box survives the 1s tick.

import { store, config } from "../lib/state.mjs";
import { fmtClock, parseClock, debounce, ICONS } from "../lib/util.mjs";

const MODES = [
  ["locks", "Locks"],
  ["expiry", "Expiry window"],
  ["telemetry", "Telemetry"],
  ["log", "Log"],
];

const VIEW_TAG = { locks: "la-lock-table", expiry: "la-lock-table", telemetry: "la-charts", log: "la-log-view" };

class LaApp extends HTMLElement {
  connectedCallback() {
    const s = store.state;
    this.innerHTML = `<div class="app">
      <header class="topbar">
        <div class="cluster-status">
          <span class="dot"></span>
          <span class="leader" id="la-leader">connecting…</span>
          <span id="la-quorum"></span>
        </div>
        <div class="search-wrap">${ICONS.search}
          <input class="input" id="la-search" spellcheck="false"
                 placeholder="/cluster/members/00000   tag:leader   holder:node-3">
        </div>
        <div class="seg" id="la-mode">
          ${MODES.map(([v, label]) => `
            <label class="seg-opt"><input type="radio" name="mode" value="${v}"><span>${label}</span></label>`).join("")}
        </div>
        <div class="mode-extra" id="la-expiry" hidden>
          <input class="input" id="la-at" spellcheck="false">
          <span class="sep">±</span>
          <select class="input" id="la-tol">
            ${config.toleranceOptionsSec.map((t) => `<option value="${t}">${t < 60 ? t + "s" : (t / 60) + "m"}</option>`).join("")}
          </select>
        </div>
        <div class="mode-extra" id="la-range" hidden>
          <input class="input" id="la-from" spellcheck="false">
          <span class="sep">→</span>
          <input class="input" id="la-to" spellcheck="false">
        </div>
        <span class="watch-badge" id="la-watch"></span>
        <span class="clock" id="la-clock"></span>
      </header>
      <div class="shell" id="la-shell">
        <la-tree></la-tree>
        <div class="main">
          <div id="la-view" style="display:flex;flex-direction:column;flex:1;min-height:0"></div>
          <div class="statusbar">
            <span id="la-count"></span>
            <span class="spacer"></span>
            <span class="toast" id="la-toast"></span>
            <span id="la-hint"></span>
          </div>
        </div>
        <la-detail id="la-detail" style="display:none"></la-detail>
      </div>
      <la-break-dialog></la-break-dialog>
    </div>`;

    this.$ = (id) => this.querySelector("#" + id);

    // control wiring (values come from persisted state)
    const search = this.$("la-search");
    search.value = s.query;
    search.oninput = debounce(() => store.set({ query: search.value }), 250);

    for (const radio of this.querySelectorAll("input[name=mode]")) {
      radio.checked = radio.value === s.mode;
      radio.onchange = () => {
        const patch = { mode: radio.value, selectedId: null, detail: null };
        // A stale (past) expiry target shows an empty table; re-arm it.
        if (radio.value === "expiry") {
          const atMs = parseClock(store.state.atText, Date.now());
          if (atMs === null || atMs < Date.now()) {
            patch.atText = fmtClock(Date.now() + config.expiryDefaultOffsetMs);
            this.$("la-at").value = patch.atText;
          }
        }
        store.set(patch);
      };
    }
    const at = this.$("la-at"), tol = this.$("la-tol"), from = this.$("la-from"), to = this.$("la-to");
    at.value = s.atText;
    at.onchange = () => store.set({ atText: at.value });
    tol.value = String(s.tolSec);
    tol.onchange = () => store.set({ tolSec: Number(tol.value) });
    from.value = s.fromText;
    from.onchange = () => store.set({ fromText: from.value });
    to.value = s.toText;
    to.onchange = () => store.set({ toText: to.value });

    this._mode = null;
    this._unsub = store.subscribe((st) => this.update(st));
    this.update(s);
  }
  disconnectedCallback() { this._unsub?.(); }

  update(st) {
    if (st.mode !== this._mode) {
      this._mode = st.mode;
      this.$("la-view").innerHTML = `<${VIEW_TAG[st.mode]} style="display:flex;flex-direction:column;flex:1;min-height:0"></${VIEW_TAG[st.mode]}>`;
      this.$("la-expiry").hidden = st.mode !== "expiry";
      this.$("la-range").hidden = st.mode !== "log";
    }

    const c = st.cluster;
    if (c) {
      this.$("la-leader").textContent = `${c.nodes.length} nodes`;
      const held = st.locksAll.filter((l) => l.state === "held").length;
      const segCount = c.nodes.reduce((n, x) => n + (x.segmentCount ?? 0), 0);
      this.$("la-quorum").textContent = `· ${held} held · ${segCount} segments`;
    }
    this.$("la-clock").textContent = fmtClock(st.now);

    const hot = st.locksAll.filter((l) =>
      st.watched.has(l.id) && l.state === "held" && l.expiresAtMs - st.now < config.watchWarnMs).length;
    this.$("la-watch").innerHTML = st.watched.size
      ? `${st.watched.size} watched${hot ? ` <span class="hot">· ${hot} expiring</span>` : ""}`
      : "";

    this.$("la-shell").style.gridTemplateColumns = st.selectedId !== null ? "196px 1fr 320px" : "196px 1fr";
    this.$("la-detail").style.display = st.selectedId !== null ? "" : "none";

    this.$("la-count").textContent = st.mode === "log"
      ? `${st.events.length} events`
      : `${st.locks.length} of ${st.locksAll.length} locks`;
    this.$("la-hint").textContent = st.error || "tag: holder: /path";
    this.$("la-toast").textContent = st.toast;
  }
}

customElements.define("la-app", LaApp);
