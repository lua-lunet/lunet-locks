// Lock rows: icon, path, holder, expires, held-for, labels.
// In expiry mode the expires column shows the signed delta from the target time.

import { store, config } from "../lib/state.mjs";
import { esc, fmtDur, parseClock, ICONS } from "../lib/util.mjs";

/** @typedef {import("../lib/types.mjs").Lock} Lock */

// Column widths in px: icon, lock, holder, expires, held. Labels take the
// remaining width. Defaults are sized to the real data: a holder UUID is 36
// chars (~270px at 12px mono), the longest demo path ~36 chars (~280px).
const DEFAULT_COLS = [20, 280, 270, 104, 68];
const MIN_COL_PX = 48;

class LaLockTable extends HTMLElement {
  /** @type {(() => void) | undefined} */
  _unsub;
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
    this._applyCols(store.state.colWidths ?? DEFAULT_COLS);
    this.$(".grid-head").onmousedown = (e) => this._startResize(e);
    this._unsub = store.subscribe(() => this.render());
    this.onclick = (e) => {
      const row = e.target instanceof Element ? e.target.closest(".lock-row") : null;
      if (row instanceof HTMLElement) store.set({ selectedId: Number(row.dataset.id) });
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

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
   * @param {number[]} widths
   * @returns {void}
   */
  _applyCols(widths) {
    this.style.setProperty("--cols", widths.map((px) => px + "px").join(" ") + " minmax(120px, 1fr)");
  }

  /**
   * @param {MouseEvent} e
   * @returns {void}
   */
  _startResize(e) {
    const handle = e.target instanceof Element ? e.target.closest(".col-resize") : null;
    if (!(handle instanceof HTMLElement)) return;
    e.preventDefault();
    const col = Number(handle.dataset.col);
    const startX = e.clientX;
    const widths = [...(store.state.colWidths ?? DEFAULT_COLS)];
    const startW = widths[col];
    /** @param {MouseEvent} ev */
    const onMove = (ev) => {
      widths[col] = Math.max(MIN_COL_PX, Math.round(startW + ev.clientX - startX));
      this._applyCols(widths);
    };
    const onUp = () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.body.style.userSelect = "";
      store.set({ colWidths: widths }); // persists to sessionStorage
    };
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }

  /** @returns {void} */
  render() {
    const { locks, now, selectedId, watched, mode, atText, tolSec } = store.state;
    const atMs = mode === "expiry" ? parseClock(atText, now) : null;

    const rows = locks.map((l) => {
      // A lock the cluster reports as held but with no expiry recorded has no
      // remaining lease to render: treat it like the free case for the numeric
      // columns rather than printing "NaNs".
      const held = l.state === "held";
      const timed = held && l.expiresAtMs != null;
      const rem = timed ? /** @type {number} */ (l.expiresAtMs) - now : 0;
      const urgent = timed && rem < 12000;
      const cut = l.name.lastIndexOf("/");
      const dir = l.name.slice(0, cut + 1);
      const leaf = l.name.slice(cut + 1);
      const hot = watched.has(l.id) && timed && rem < config.watchWarnMs;

      let ttlText, ttlColor;
      if (timed && mode === "expiry" && atMs != null) {
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

      const iconColor = held ? (urgent ? "var(--color-accent)" : "var(--color-neutral-500)") : "var(--color-accent)";
      const classes = ["lock-row",
        selectedId === l.id ? "selected" : "",
        hot ? "watched-hot" : ""].filter(Boolean).join(" ");

      return `<div class="${classes}" data-id="${l.id}">
        <div class="icon" style="color:${iconColor}">${held ? ICONS.lock : ICONS.lockOpen}</div>
        <div class="name">
          <span class="dir">${esc(dir)}</span><span class="leaf">${esc(leaf)}</span>
          ${watched.has(l.id) ? `<span style="color:var(--color-accent);flex:none">${ICONS.bell}</span>` : ""}
        </div>
        <div class="cell" style="color:${held ? "var(--color-text)" : "var(--color-neutral-600)"}">${esc(held ? l.holder : "—")}</div>
        <div class="cell" style="color:${ttlColor}">${ttlText}</div>
        <div class="cell" style="color:var(--color-neutral-500)">${held && l.takenAtMs != null ? fmtDur(now - l.takenAtMs) : "—"}</div>
        <div class="labels">${l.labels.map((t) => `<span class="tag">${esc(t)}</span>`).join("")}</div>
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
