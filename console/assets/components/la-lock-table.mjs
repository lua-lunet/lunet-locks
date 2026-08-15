// Lock rows: icon, path, holder, expires, held-for, labels.
// In expiry mode the expires column shows the signed delta from the target time.

import { store, config } from "../lib/state.mjs";
import { esc, fmtDur, parseClock, ICONS } from "../lib/util.mjs";
import { resizableColumns } from "../lib/resizable.mjs";

/** @typedef {import("../lib/types.mjs").Lock} Lock */
/** @typedef {import("../lib/resizable.mjs").ResizableColumns} ResizableColumns */

// Column widths in px: icon, lock, holder, expires, held. Labels take the
// remaining width (the trailing minmax() in styles.css). Defaults are sized
// to the real data: a holder UUID is 36 chars (~270px at 12px mono), the
// longest demo path ~36 chars (~280px).
const DEFAULT_COLS = [20, 280, 270, 104, 68];
const MIN_COL_PX = 48;

class LaLockTable extends HTMLElement {
  /** @type {(() => void) | undefined} */
  _unsub;
  /** @type {(() => void) | undefined} */
  _unsubTick;
  /** @type {ResizableColumns | undefined} */
  _cols;
  /** Skeleton nodes, memoised by selector on first lookup. @type {Map<string, HTMLElement>} */
  _refs = new Map();

  connectedCallback() {
    this.innerHTML = `
      <div class="rows">
        <div class="grid-head">
          <div></div>
          <div class="hcell">lock<span class="col-resize" data-col="1"></span></div>
          <div class="hcell">holder<span class="col-resize" data-col="2"></span></div>
          <div class="hcell">expires<span class="col-resize" data-col="3"></span></div>
          <div class="hcell">held<span class="col-resize" data-col="4"></span></div>
          <div>labels</div>
        </div>
        <div class="body"></div>
      </div>`;
    // Own markup, so every $() lookup below resolves; results are memoised.
    this._cols = resizableColumns({
      host: this, headEl: this.$(".grid-head"),
      cssVar: "--cols", key: "colWidths",
      defaults: DEFAULT_COLS, min: MIN_COL_PX,
    });
    // Full row rebuilds only when the data or the render inputs change; the
    // 1s `now` tick just refreshes the TTL/held-for text in place.
    this._unsub = store.subscribe(() => this.render(),
      ["locks", "selectedId", "watched", "mode", "atText", "tolSec"]);
    this._unsubTick = store.subscribe(() => this.tick(), ["now"]);
    this.onclick = (e) => {
      const row = e.target instanceof Element ? e.target.closest(".lock-row") : null;
      if (row instanceof HTMLElement) store.set({ selectedId: Number(row.dataset.id) });
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); this._unsubTick?.(); this._cols?.dispose(); }

  /**
   * Look up one of this component's own skeleton nodes, memoising the result
   * so the 1s tick does not re-query the DOM.
   * @param {string} selector
   * @returns {HTMLElement}
   */
  $(selector) {
    const cached = this._refs.get(selector);
    if (cached) return cached;
    const el = this.querySelector(selector);
    if (!(el instanceof HTMLElement)) throw new Error(`la-lock-table: missing ${selector}`);
    this._refs.set(selector, el);
    return el;
  }

  /**
   * The `now`-dependent display values for one row, shared by render() and
   * the 1s tick() so the two never drift.
   * @param {Lock} l
   * @param {number} now
   * @param {number | null} atMs Expiry-mode target, null in other modes.
   * @returns {{ ttlText: string, ttlColor: string, heldText: string, iconColor: string, urgent: boolean, hot: boolean }}
   */
  _timing(l, now, atMs) {
    const { watched, tolSec } = store.state;
    // A lock the cluster reports as held but with no expiry recorded has no
    // remaining lease to render: treat it like the free case for the numeric
    // columns rather than printing "NaNs".
    const held = l.state === "held";
    const timed = held && l.expiresAtMs != null;
    const rem = timed ? /** @type {number} */ (l.expiresAtMs) - now : 0;
    const urgent = timed && rem < 12000;
    const hot = watched.has(l.id) && timed && rem < config.watchWarnMs;

    let ttlText, ttlColor;
    if (timed && atMs != null) {
      const d = (/** @type {number} */ (l.expiresAtMs) - atMs) / 1000;
      ttlText = (d >= 0 ? "+" : "") + d.toFixed(1) + "s";
      ttlColor = Math.abs(d) < tolSec / 2 ? "var(--color-accent)" : "var(--color-neutral-400)";
    } else if (timed) {
      ttlText = fmtDur(rem);
      ttlColor = urgent ? "var(--color-accent)" : "var(--color-neutral-400)";
    } else if (held) {
      ttlText = "—";
      ttlColor = "var(--color-neutral-400)";
    } else {
      ttlText = "free";
      ttlColor = "var(--color-accent-600)";
    }

    return {
      ttlText,
      ttlColor,
      heldText: held && l.takenAtMs != null ? fmtDur(now - l.takenAtMs) : "—",
      iconColor: held ? (urgent ? "var(--color-accent)" : "var(--color-neutral-500)") : "var(--color-accent)",
      urgent,
      hot,
    };
  }

  /**
   * The 1s clock tick: update only the time-dependent cells of the existing
   * rows (TTL, held-for, urgency tint) — the row markup itself is untouched.
   * @returns {void}
   */
  tick() {
    const { locks, now, mode, atText } = store.state;
    const atMs = mode === "expiry" ? parseClock(atText, now) : null;
    const byId = new Map(locks.map((l) => [l.id, l]));
    for (const row of this.$(".body").children) {
      if (!(row instanceof HTMLElement) || row.dataset.id === undefined) continue;
      const l = byId.get(Number(row.dataset.id));
      if (!l) continue;
      const t = this._timing(l, now, atMs);
      const icon = /** @type {HTMLElement} */ (row.children[0]);
      const ttl = /** @type {HTMLElement} */ (row.children[3]);
      const heldFor = /** @type {HTMLElement} */ (row.children[4]);
      icon.style.color = t.iconColor;
      ttl.textContent = t.ttlText;
      ttl.style.color = t.ttlColor;
      heldFor.textContent = t.heldText;
      row.classList.toggle("watched-hot", t.hot);
    }
  }

  /** @returns {void} */
  render() {
    const { locks, now, selectedId, watched, mode, atText } = store.state;
    const atMs = mode === "expiry" ? parseClock(atText, now) : null;

    const rows = locks.map((l) => {
      const held = l.state === "held";
      const t = this._timing(l, now, atMs);
      const cut = l.name.lastIndexOf("/");
      const dir = l.name.slice(0, cut + 1);
      const leaf = l.name.slice(cut + 1);

      const classes = ["lock-row",
        selectedId === l.id ? "selected" : "",
        t.hot ? "watched-hot" : ""].filter(Boolean).join(" ");

      return `<div class="${classes}" data-id="${l.id}">
        <div class="icon" style="color:${t.iconColor}">${held ? ICONS.lock : ICONS.lockOpen}</div>
        <div class="name">
          <span class="dir">${esc(dir)}</span><span class="leaf">${esc(leaf)}</span>
          ${watched.has(l.id) ? `<span style="color:var(--color-accent);flex:none">${ICONS.bell}</span>` : ""}
        </div>
        <div class="cell" style="color:${held ? "var(--color-text)" : "var(--color-neutral-600)"}">${esc(held ? l.holder : "—")}</div>
        <div class="cell" style="color:${t.ttlColor}">${t.ttlText}</div>
        <div class="cell" style="color:var(--color-neutral-500)">${t.heldText}</div>
        <div class="labels">${l.labels.map((x) => `<span class="tag">${esc(x)}</span>`).join("")}</div>
      </div>`;
    }).join("");

    // Only the row markup is replaced; the header, scroll container, and any
    // in-progress column drag survive the 1s ticks, and scrollTop is restored
    // in case the browser resets it.
    const scrollTop = this.$(".rows").scrollTop;
    this.$(".body").innerHTML = rows || '<div style="padding:24px;color:var(--color-neutral-500);font-family:var(--font-mono);font-size:12px">no locks match</div>';
    this.$(".rows").scrollTop = scrollTop;
  }
}

customElements.define("la-lock-table", LaLockTable);
