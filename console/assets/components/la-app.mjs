// App shell: header (cluster status, search, mode switch, mode-specific
// controls, clock), the tree/main/detail grid, and the status bar. The DOM
// skeleton is built once; steady-state updates touch textContent only, so
// focus in the search box survives the 1s tick.

import { store, config } from "../lib/state.mjs";
import { fmtClock, parseClock, debounce, ICONS } from "../lib/util.mjs";
import { resizableColumns } from "../lib/resizable.mjs";

/** @typedef {import("../lib/types.mjs").ClusterNode} ClusterNode */
/** @typedef {import("../lib/types.mjs").Lock} Lock */
/** @typedef {import("../lib/types.mjs").StoreState} StoreState */
/** @typedef {import("../lib/types.mjs").ViewMode} ViewMode */
/** @typedef {import("../lib/resizable.mjs").ResizableColumns} ResizableColumns */

/** @type {[ViewMode, string][]} */
const MODES = [
  ["locks", "Locks"],
  ["expiry", "Expiry window"],
  ["telemetry", "Telemetry"],
  ["log", "Log"],
];

/** @type {Record<ViewMode, string>} */
const VIEW_TAG = { locks: "la-lock-table", expiry: "la-lock-table", telemetry: "la-charts", log: "la-log-view" };

class LaApp extends HTMLElement {
  /** @type {(() => void) | undefined} */
  _unsub;
  /** @type {ViewMode | null} */
  _mode = null;
  /** @type {ResizableColumns | undefined} */
  _panes;
  /** @type {boolean | null} */
  _hadSelection = null;
  /** Skeleton nodes, memoised by id on first lookup. @type {Map<string, HTMLElement>} */
  _refs = new Map();

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
          <button class="btn btn-ghost" id="la-range-now"
                  title="Back to the live trailing window" hidden>now</button>
        </div>
        <span class="watch-badge" id="la-watch"></span>
        <span class="clock" id="la-clock"></span>
      </header>
      <div class="shell" id="la-shell">
        <la-tree></la-tree>
        <div class="pane-divider" id="la-div-left" data-col="0"></div>
        <div class="main">
          <div id="la-view" style="display:flex;flex-direction:column;flex:1;min-height:0"></div>
          <div class="statusbar">
            <span id="la-count"></span>
            <span class="spacer"></span>
            <span class="toast" id="la-toast"></span>
            <span id="la-hint"></span>
          </div>
        </div>
        <div class="pane-divider" id="la-div-right" data-col="1"></div>
        <la-detail id="la-detail" style="display:none"></la-detail>
      </div>
      <la-break-dialog></la-break-dialog>
    </div>`;

    // The skeleton above is this component's own markup, so a missing id is a
    // programming error, not a runtime condition — $() throws rather than
    // silently no-oping the way `querySelector(...)?.textContent =` would.
    this._refs.clear();

    // The two shell panes resize through the same helper as the data grids.
    // widths = [tree, detail]; the 6px divider tracks and the collapsible
    // detail pane live in the template, not in the persisted numbers. The
    // side panes are clamped so they cannot be dragged shut.
    this._panes = resizableColumns({
      host: this.$("la-shell"), headEl: this.$("la-shell"),
      cssVar: "--shell-cols", key: "paneWidths",
      defaults: [196, 320], min: [140, 240],
      selector: ".pane-divider",
      template: (w) => store.state.selectedId !== null
        ? `${w[0]}px 6px 1fr 6px ${w[1]}px`
        : `${w[0]}px 6px 1fr`,
    });

    // control wiring (values come from persisted state)
    const search = /** @type {HTMLInputElement} */ (this.$("la-search"));
    search.value = s.query;
    search.oninput = debounce(() => store.set({ query: search.value }), 250);

    for (const radio of this.querySelectorAll("input[name=mode]")) {
      const input = /** @type {HTMLInputElement} */ (radio);
      input.checked = input.value === s.mode;
      input.onchange = () => {
        const mode = /** @type {ViewMode} */ (input.value);
        /** @type {Partial<StoreState>} */
        const patch = { mode, selectedId: null, detail: null };
        // A stale (past) expiry target shows an empty table; re-arm it.
        if (mode === "expiry") {
          const atMs = parseClock(store.state.atText, Date.now());
          if (atMs === null || atMs < Date.now()) {
            patch.atText = fmtClock(Date.now() + config.expiryDefaultOffsetMs);
            at.value = patch.atText;
          }
        }
        store.set(patch);
      };
    }
    const at = /** @type {HTMLInputElement} */ (this.$("la-at"));
    const tol = /** @type {HTMLSelectElement} */ (this.$("la-tol"));
    const from = /** @type {HTMLInputElement} */ (this.$("la-from"));
    const to = /** @type {HTMLInputElement} */ (this.$("la-to"));
    at.value = s.atText;
    at.onchange = () => store.set({ atText: at.value });
    tol.value = String(s.tolSec);
    tol.onchange = () => store.set({ tolSec: Number(tol.value) });
    from.value = s.fromText;
    from.onchange = () => store.set({ fromText: from.value, logRangePinned: true });
    to.value = s.toText;
    to.onchange = () => store.set({ toText: to.value, logRangePinned: true });
    // Reset affordance: drop the pin and re-anchor the trailing window
    // immediately (the events poller then keeps it fresh every 2s).
    const rangeNow = /** @type {HTMLButtonElement} */ (this.$("la-range-now"));
    rangeNow.onclick = () => store.set({
      logRangePinned: false,
      fromText: fmtClock(Date.now() - config.logDefaultWindowMs),
      toText: fmtClock(Date.now()),
    });

    this._mode = null;
    this._hadSelection = null;
    this._unsub = store.subscribe((st) => this.update(st));
    this.update(s);
  }
  disconnectedCallback() { this._unsub?.(); this._panes?.dispose(); }

  /**
   * Look up one of this component's own skeleton nodes by id, memoising the
   * result so the 1s tick does not re-query the DOM.
   * @param {string} id
   * @returns {HTMLElement}
   */
  $(id) {
    const cached = this._refs.get(id);
    if (cached) return cached;
    const el = this.querySelector("#" + id);
    if (!(el instanceof HTMLElement)) throw new Error(`la-app: missing #${id}`);
    this._refs.set(id, el);
    return el;
  }

  /**
   * @param {StoreState} st
   * @returns {void}
   */
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

    // The "now" button only has meaning against a pinned range. In trailing
    // mode the poller freshens fromText/toText; mirror them into the inputs,
    // except the one the operator is mid-edit in (onchange hasn't fired yet).
    this.$("la-range-now").hidden = !st.logRangePinned;
    if (!st.logRangePinned) {
      const fromEl = /** @type {HTMLInputElement} */ (this.$("la-from"));
      const toEl = /** @type {HTMLInputElement} */ (this.$("la-to"));
      if (document.activeElement !== fromEl) fromEl.value = st.fromText;
      if (document.activeElement !== toEl) toEl.value = st.toText;
    }

    // A held lock with no expiresAtMs cannot be "expiring soon", so it must
    // not count as hot: NaN comparisons are false, but the intent is explicit.
    const hot = st.locksAll.filter((l) =>
      st.watched.has(l.id) && l.state === "held"
      && l.expiresAtMs != null && l.expiresAtMs - st.now < config.watchWarnMs).length;
    this.$("la-watch").innerHTML = st.watched.size
      ? `${st.watched.size} watched${hot ? ` <span class="hot">· ${hot} expiring</span>` : ""}`
      : "";

    // The grid template only changes shape when the detail pane appears or
    // disappears; re-applying on every tick would clobber a drag in progress,
    // because the drag writes --shell-cols before it reaches the store.
    const hasSelection = st.selectedId !== null;
    if (hasSelection !== this._hadSelection) {
      this._hadSelection = hasSelection;
      this._panes?.apply();
    }
    this.$("la-detail").style.display = hasSelection ? "" : "none";
    this.$("la-div-right").style.display = hasSelection ? "" : "none";

    this.$("la-count").textContent = st.mode === "log"
      ? `${st.events.length} events`
      : `${st.locks.length} of ${st.locksAll.length} locks`;
    this.$("la-hint").textContent = st.error || "tag: holder: /path";
    this.$("la-toast").textContent = st.toast;
  }
}

customElements.define("la-app", LaApp);
